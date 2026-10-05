#!/usr/bin/env python3
"""Opt-in isolated setup for the real benchmark on disposable hosted GitHub runners.

Plan-only by default. Resources are owned by this job's unique Compose project.
No existing server, cloud resource or persistent deployment is accepted.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import signal
import socket
import subprocess
import sys
import time
from urllib.request import urlopen
import uuid

import commit_update_bench as bench


def hosted_root(root):
    if (sys.platform != "linux" or os.environ.get("GITHUB_ACTIONS") != "true"
            or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"):
        raise ValueError("execution is limited to a disposable GitHub-hosted Linux runner")
    run, attempt = os.environ.get("GITHUB_RUN_ID", ""), os.environ.get("GITHUB_RUN_ATTEMPT", "")
    if not run.isdecimal() or not attempt.isdecimal():
        raise ValueError("job ownership identifiers are missing")
    expected = Path(os.environ["RUNNER_TEMP"]).resolve(strict=True) / f"mst2-real-{run}-{attempt}"
    if root.absolute() != expected or root.is_symlink():
        raise ValueError("owned root must be the exact unique job directory under RUNNER_TEMP")
    return expected, f"m2perf-{run}-{attempt}"


def toml(data):
    """Serialize the trusted server template, including its token-table array."""
    lines = []
    def value(item):
        if isinstance(item, bool):
            return "true" if item else "false"
        if isinstance(item, (str, int)):
            return json.dumps(item, ensure_ascii=False)
        if isinstance(item, list):
            return "[" + ", ".join(value(x) for x in item) + "]"
        raise TypeError("unsupported template value")
    def table(items, path, array=False):
        if path:
            name = ".".join(path)
            lines.append(("[[" + name + "]]" if array else "[" + name + "]"))
        for key, item in items.items():
            if not re.fullmatch(r"[A-Za-z0-9_]+", key):
                raise ValueError("unexpected template key")
            if not isinstance(item, dict) and not (isinstance(item, list) and item and isinstance(item[0], dict)):
                lines.append(key + " = " + value(item))
        for key, item in items.items():
            if isinstance(item, dict):
                table(item, path + [key])
            elif isinstance(item, list) and item and isinstance(item[0], dict):
                for entry in item:
                    table(entry, path + [key], True)
    table(data, [])
    return "\n".join(lines) + "\n"


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def dependencies(source, project, ports, deadline):
    raw = bench.command(["docker", "compose", "-f", str(source / "docker/docker-compose.test.yml"),
                         "config", "--format", "json"], deadline)
    config = json.loads(raw)
    selected = {name: config["services"][name] for name in ("postgres", "redis", "rustfs", "rustfs-init")}
    for name, service in selected.items():
        if "build" in service or "volumes" in service or "container_name" in service:
            raise AssertionError("dependency template unexpectedly escapes disposable job storage")
        service["networks"] = {"default": None}
        service.pop("ports", None)
        if name in ports:
            target = {"postgres": 5432, "redis": 6379, "rustfs": 9000}[name]
            service["ports"] = [{"target": target, "published": str(ports[name]),
                                  "host_ip": "127.0.0.1", "protocol": "tcp"}]
    return {"services": selected, "networks": {"default": {"name": project + "-network"}}}


def stop_owned(root, project, deadline, process=None):
    state_path = root / "owned.json"
    if not state_path.exists():
        return
    state = json.loads(state_path.read_text())
    if state["project"] != project:
        raise AssertionError("cleanup ownership differs from this job")
    compose = root / "dependencies.json"
    if hashlib.sha256(compose.read_bytes()).hexdigest() != state["compose_sha256"]:
        raise AssertionError("cleanup Compose configuration differs from owned startup")
    service = state.get("service")
    if service and Path(f"/proc/{service['pid']}").exists():
        pid = service["pid"]
        proc = Path(f"/proc/{pid}")
        started = proc.joinpath("stat").read_text().rsplit(") ", 1)[1].split()[19]
        argv = [x.decode() for x in proc.joinpath("cmdline").read_bytes().split(b"\0") if x]
        if (started != service["starttime"] or argv[:3] != [state["binary"], "--config", str(root / "service.toml")]
                or os.getpgid(pid) != pid):
            raise AssertionError("refusing cleanup of a replaced or unrelated service")
        os.killpg(pid, signal.SIGTERM)
        if process:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(pid, signal.SIGKILL)
                process.wait(timeout=5)
        else:
            for _ in range(50):
                if not proc.exists():
                    break
                time.sleep(0.1)
            if proc.exists():
                # Recheck its start identity before escalation after waiting.
                if proc.joinpath("stat").read_text().rsplit(") ", 1)[1].split()[19] != service["starttime"]:
                    raise AssertionError("service PID was replaced during cleanup")
                os.killpg(pid, signal.SIGKILL)
    bench.command(["docker", "compose", "-p", project, "-f", str(compose), "down", "--volumes"], deadline)
    state.pop("service", None)
    state_path.write_text(json.dumps(state))
    print(json.dumps({"record": "owned_cleanup", "project": project, "correctness": "PASS"}), flush=True)


def execute(options):
    root, project = hosted_root(options.run_root)
    if root.exists():
        raise ValueError("disposable job directory must not already exist")
    if not re.fullmatch(r"[0-9a-f]{40}", options.mega_sha):
        raise ValueError("server source must be an immutable full SHA-1")
    absolute = datetime.fromisoformat(options.session_deadline_utc.replace("Z", "+00:00"))
    if absolute.tzinfo is None or absolute.utcoffset().total_seconds() != 0:
        raise ValueError("shared deadline must be explicit UTC")
    deadline = time.monotonic() + min(14400, absolute.timestamp() - time.time()) - 120
    source = options.mega_source.resolve(strict=True)
    source_sha = bench.git(source, deadline, "rev-parse", "HEAD").decode().strip()
    if source_sha != options.mega_sha or bench.git(source, deadline, "status", "--porcelain").strip():
        raise AssertionError("server checkout differs from the reviewed immutable source")
    binary = options.mega_binary.resolve(strict=True)
    if not binary.is_relative_to(source / "target"):
        raise ValueError("server binary must be built inside the immutable server checkout")
    bench.driver_binding(options)
    ports = {name: free_port() for name in ("postgres", "redis", "rustfs", "http")}
    if len(set(ports.values())) != 4:
        raise AssertionError("ephemeral ports collided before resource creation")
    compose = dependencies(source, project, ports, deadline)
    existing = bench.command(["docker", "ps", "-aq", "--filter", "label=com.docker.compose.project=" + project], deadline)
    existing_network = bench.command(["docker", "network", "ls", "-q", "--filter", "name=^" + project + "-network$"], deadline)
    if existing.strip() or existing_network.strip():
        raise AssertionError("refusing to reuse or clean up an existing Compose project/network")
    root.mkdir(mode=0o700)
    compose_path = root / "dependencies.json"
    compose_path.write_text(json.dumps(compose))
    state = {"project": project, "binary": str(binary), "mega_source_sha": source_sha,
             "compose_sha256": hashlib.sha256(compose_path.read_bytes()).hexdigest()}
    state_path = root / "owned.json"
    state_path.write_text(json.dumps(state))
    process = None
    log = None
    try:
        bench.command(["docker", "compose", "-p", project, "-f", str(compose_path),
                       "up", "-d", "--wait", "--wait-timeout", "180"], min(deadline, time.monotonic() + 240))
        db = "mst2_bench_" + uuid.uuid4().hex
        env = bench.clean_env({"PGHOST": "127.0.0.1", "PGPORT": str(ports["postgres"]),
                               "PGUSER": "mega2", "PGPASSWORD": "mega2_test_password", "PGDATABASE": "mega2"})
        bench.command(["psql", "-X", "-v", "ON_ERROR_STOP=1", "-c", "CREATE DATABASE " + db], deadline, env=env)
        env["PGDATABASE"] = db
        git_token, token = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
        for name, secret in (("git-token", git_token), ("mst2-token", token)):
            print("::add-mask::" + secret, flush=True)
            file = root / name
            file.write_text(secret)
            file.chmod(0o600)
        config = bench.tomllib.loads((source / "config/config-storage-only.toml").read_text())
        config["base_dir"] = str(root / "service-data")
        config["log"].update(print_std=False, with_ansi=False)
        config["database"].update(db_url=f"postgres://mega2:mega2_test_password@127.0.0.1:{ports['postgres']}/{db}",
                                  max_connection=8, min_connection=1, acquire_timeout=60, connect_timeout=30)
        config["redis"]["url"] = f"redis://127.0.0.1:{ports['redis']}"
        config["monorepo"].update(root_dirs=["third-party", "project"], object_format="sha1", push_policy="trunk")
        config["pack"].update(pack_decode_mem_size="512M", pack_decode_cache_path=str(root / "pack-cache"))
        config["object_storage"]["s3"].update(endpoint_url=f"http://127.0.0.1:{ports['rustfs']}", bucket="mega2")
        config["git"].update(push_auth="token", ssh_receive_pack=False,
                             push_tokens=[{"name": "owned-benchmark", "token": "${file:" + str(root / "git-token") + "}",
                                           "paths": ["/project"]}])
        instance = str(uuid.uuid4())
        config["mst2"] = {"enabled": True, "instance_uuid": instance, "publication_enabled": True,
                           "auth_token": "${file:" + str(root / "mst2-token") + "}"}
        for name in ("oci", "agent_capture", "storage_events"):
            config[name] = {"enabled": False}
        config_path = root / "service.toml"
        config_path.write_text(toml(config))
        config_path.chmod(0o600)
        service_env = bench.clean_env({"MEGA_BASE_DIR": config["base_dir"], "MEGA_CACHE_DIR": str(root / "cache"),
                                       "MEGA_GIT_OBJECT_CACHE_PREFIX": project})
        prefix = [str(binary), "--config", str(config_path)]
        bench.command(prefix + ["config", "validate"], deadline, env=service_env)
        bench.command(prefix + ["service", "init", "--yes"], deadline, env=service_env)
        log = (root / "service-private.log").open("wb")
        process = subprocess.Popen(prefix + ["service", "http", "--host", "127.0.0.1", "-p", str(ports["http"])],
                                   stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                                   env=service_env, start_new_session=True)
        state["service"] = {"pid": process.pid, "starttime": Path(f"/proc/{process.pid}/stat").read_text().rsplit(") ", 1)[1].split()[19]}
        state_path.write_text(json.dumps(state))
        base = f"http://127.0.0.1:{ports['http']}"
        ready_until = min(deadline, time.monotonic() + 180)
        while True:
            if process.poll() is not None or time.monotonic() >= ready_until:
                raise RuntimeError("owned service failed readiness")
            try:
                with urlopen(base + "/api/v2/snapshots/capabilities", timeout=2) as response:
                    if response.status == 200:
                        break
            except OSError:
                pass
            time.sleep(0.2)
        initial = bench.command(["git", "ls-remote", base + "/project", "refs/heads/main"], deadline).decode().split()
        if len(initial) != 2 or initial[1] != "refs/heads/main":
            raise AssertionError("owned service did not initialize exactly one project main")
        env.update(M2_TOKEN=token, M2_GIT_TOKEN=git_token)
        remaining = deadline - time.monotonic()
        actual_rounds = min(options.rounds, 3) if remaining < 3600 else options.rounds
        actual_profile = "smoke" if options.profile == "medium" and remaining < 1200 else options.profile
        print(json.dumps({"record": "owned_server_build", "source_sha": source_sha,
                          "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "project": project,
                          "requested_profile": options.profile, "actual_profile": actual_profile,
                          "requested_rounds": options.rounds, "actual_rounds": actual_rounds,
                          "remaining_session_seconds": remaining}), flush=True)
        os.environ.update(env)
        args = bench.parser().parse_args([
            "--execute", "--isolated-deployment", "--base-url", base, "--git-url", base + "/project",
            "--database", db, "--instance-id", instance, "--expect-initial-commit", initial[0],
            "--service-pid", str(process.pid), "--driver", str(options.driver.resolve()),
            "--driver-sha256", options.driver_sha256, "--run-root", str(root / "measurements"),
            "--profile", actual_profile, "--rounds", str(actual_rounds),
            "--session-deadline-utc", options.session_deadline_utc])
        bench.execute(args)
    finally:
        try:
            stop_owned(root, project, time.monotonic() + 90, process)
        finally:
            if log:
                log.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--cleanup", action="store_true")
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--mega-source", type=Path)
    parser.add_argument("--mega-sha")
    parser.add_argument("--mega-binary", type=Path)
    parser.add_argument("--driver", type=Path)
    parser.add_argument("--driver-sha256")
    parser.add_argument("--session-deadline-utc")
    parser.add_argument("--profile", choices=("smoke", "medium"), default="medium")
    parser.add_argument("--rounds", type=int, choices=range(3, 11), default=5)
    opts = parser.parse_args()
    try:
        if opts.execute and opts.cleanup:
            raise ValueError("execute and cleanup are separate operations")
        if opts.cleanup:
            owned, compose_project = hosted_root(opts.run_root)
            stop_owned(owned, compose_project, time.monotonic() + 90)
        elif not opts.execute:
            print(json.dumps({"execute": False, "profile": opts.profile, "rounds": opts.rounds,
                              "resources": "one unique disposable hosted-runner Compose project; no cloud resources",
                              "max_session_seconds": 14400, "persistent_service_changes": False}))
        else:
            required = (opts.mega_source, opts.mega_sha, opts.mega_binary, opts.driver,
                        opts.driver_sha256, opts.session_deadline_utc)
            if not all(required):
                raise ValueError("all immutable build and shared deadline arguments are required")
            def interrupted(signum, frame):
                raise KeyboardInterrupt
            signal.signal(signal.SIGTERM, interrupted)
            execute(opts)
    except (Exception, KeyboardInterrupt) as error:
        print(json.dumps({"execution_failed": True, "error_type": type(error).__name__}), file=sys.stderr)
        raise SystemExit(1)
