#!/usr/bin/env python3
"""Opt-in isolated setup for the real benchmark on disposable hosted GitHub runners.

Plan-only by default. Resources are owned by this job's unique Compose project.
No existing server, cloud resource or persistent deployment is accepted.
"""

import argparse
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
import commit_update_budget as budget_module
import commit_update_projection as projection_module


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


def initialize_owned_native(database, instance, env, deadline):
    """Maintenance bootstrap for this job's fresh DB, before starting writers.

    service init does not invoke Mega2's maintenance-only native initializer.
    Match its root/epoch/sequence contract, with a stricter empty-fixture gate.
    This does not create a READY certificate; the first real push must do that.
    """
    if (not re.fullmatch(r"mst2_bench_[0-9a-f]{32}", database)
            or env.get("PGDATABASE") != database
            or env.get("PGHOST") != "127.0.0.1"):
        raise ValueError("native bootstrap requires the exact newly owned loopback database")
    if str(uuid.UUID(instance)) != instance:
        raise ValueError("native bootstrap requires a canonical instance UUID")
    sql = f"""BEGIN;
SET LOCAL search_path = public;
SELECT pg_advisory_xact_lock(1297043024, 1229867349);
DO $owned_native$
DECLARE root mega_refs%ROWTYPE;
BEGIN
 IF current_database() <> '{database}' THEN
  RAISE EXCEPTION 'owned database differs';
 END IF;
 IF (SELECT count(*) FROM queue_control WHERE id=1) <> 1 THEN
  RAISE EXCEPTION 'fresh queue control is missing';
 END IF;
 PERFORM id FROM queue_control WHERE id=1 FOR UPDATE;
 IF EXISTS (SELECT 1 FROM push_queue)
    OR EXISTS (SELECT 1 FROM mst2_native_head)
    OR EXISTS (SELECT 1 FROM mst2_native_publication)
    OR EXISTS (SELECT 1 FROM mst2_namespace_seq)
    OR EXISTS (SELECT 1 FROM mst2_publication)
    OR EXISTS (SELECT 1 FROM mst2_publication_outbox)
    OR EXISTS (SELECT 1 FROM mst2_queue_noop_receipt) THEN
  RAISE EXCEPTION 'native bootstrap requires an unused publication fixture';
 END IF;
 SELECT * INTO STRICT root FROM mega_refs
  WHERE path='/' AND ref_name='refs/heads/main' AND NOT is_cl FOR UPDATE;
 IF root.ref_commit_hash !~ '^[0-9a-f]{{40}}$'
    OR root.ref_tree_hash !~ '^[0-9a-f]{{40}}$'
    OR (SELECT count(*) FROM mega_commit
        WHERE commit_id=root.ref_commit_hash AND tree=root.ref_tree_hash) <> 1
    OR (SELECT count(*) FROM mega_tree WHERE tree_id=root.ref_tree_hash) <> 1 THEN
  RAISE EXCEPTION 'fresh native root is missing or mismatched';
 END IF;
 INSERT INTO mst2_native_head
  (namespace,instance_id,sequence,writer_epoch,root_commit,root_tree,state)
 VALUES ('/','{instance}',0,1,root.ref_commit_hash,root.ref_tree_hash,'INITIALIZING');
END $owned_native$;
COMMIT;"""
    bench.command(["psql", "-X", "-q", "-v", "ON_ERROR_STOP=1"],
                  deadline, env=env, data=sql.encode())
    # Independently read back the actual stored root and initial head before
    # any writer is started. No verified object or publication is fabricated.
    rows = bench.query(bench.IDENTITY_SQL, deadline, env=env)
    # service init stores only the global ref. The Git HTTP route lazily
    # materializes /project; its full identity fence still runs after ls-remote.
    if not isinstance(rows, list):
        raise AssertionError("fresh global root ref is missing")
    roots = [row for row in rows if isinstance(row, dict) and row.get("path") == "/"]
    if len(roots) != 1:
        raise AssertionError("exactly one fresh global main ref is required")
    root = roots[0]
    if (root["database"] != database or root["commit_tree"] != root["tree"]
            or not re.fullmatch(r"[0-9a-f]{40}", root["commit"])
            or not re.fullmatch(r"[0-9a-f]{40}", root["tree"])):
        raise AssertionError("fresh global database or stored commit/tree differs")
    raw = bytes.fromhex(root["raw_tree"])
    if hashlib.sha1(b"tree " + str(len(raw)).encode() + b"\0" + raw).hexdigest() != root["tree"]:
        raise AssertionError("fresh global raw tree does not match its Git object ID")
    identity = {"global_commit": root["commit"], "global_tree": root["tree"]}
    native = bench.query(bench.NATIVE_SQL, deadline, env=env)
    if (not native or native["sequence"] != 0 or native["state"] != "INITIALIZING"
            or native["certificate_receipt_id"] is not None):
        raise AssertionError("fresh native head is not the initial epoch/sequence")
    bench.validate_native(native, identity, instance, False)
    return {"record": "owned_native_initialization", "instance_id": instance,
            "sequence": 0, "writer_epoch": 1, "state": "INITIALIZING",
            "global_commit": identity["global_commit"],
            "global_tree": identity["global_tree"], "correctness": "PASS",
            "production_service_init_wired": False}


def bootstrap_ready_baseline(root, base, env, instance, deadline):
    """Publish one real setup commit so the first workspace has a READY view.

    Native resolve deliberately rejects an INITIALIZING head.  The maintenance
    bootstrap above only creates that guarded head; a real Git push must create
    its first certificate and transition it to READY before ScorpioFS can mount
    the baseline workspace.  This setup commit is outside the measured matrix;
    the first measured push still exercises the normal commit-update path.
    """
    git_token = env.get("M2_GIT_TOKEN")
    if not git_token:
        raise ValueError("native baseline requires the owned Git token")
    git_env = bench.clean_env({
        "GIT_CONFIG_COUNT": "3",
        "GIT_CONFIG_KEY_0": "http.extraHeader",
        "GIT_CONFIG_VALUE_0": "Authorization: Bearer " + git_token,
        "GIT_CONFIG_KEY_1": "http.followRedirects",
        "GIT_CONFIG_VALUE_1": "false",
        "GIT_CONFIG_KEY_2": "credential.helper",
        "GIT_CONFIG_VALUE_2": "",
    })
    checkout = root / "native-baseline"
    if checkout.exists() or checkout.is_symlink():
        raise AssertionError("native baseline checkout path already exists")
    bench.command(["git", "clone", "--no-checkout", "--single-branch", "--branch", "main",
                   base + "/project", str(checkout)], deadline, env=git_env)
    bench.git(checkout, deadline, "config", "user.name", "MST2 setup baseline", env=git_env)
    bench.git(checkout, deadline, "config", "user.email", "mst2-setup@example.invalid", env=git_env)
    bench.git(checkout, deadline, "commit", "--allow-empty", "-m", "MST2 setup baseline",
              env=git_env)
    commit = bench.git(checkout, deadline, "rev-parse", "HEAD", env=git_env).decode().strip()
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise AssertionError("native baseline commit is not canonical SHA-1")
    bench.git(checkout, deadline, "push", "--no-thin", "origin",
              f"{commit}:refs/heads/main", env=git_env)

    while time.monotonic() < deadline:
        native = bench.query(bench.NATIVE_SQL, deadline, env=env)
        if native and native.get("state") == "READY":
            rows = bench.query(bench.IDENTITY_SQL, deadline, env=env)
            identity = bench.validate_identity(rows, commit,
                                                bench.git(checkout, deadline, "rev-parse", "HEAD^{tree}", env=git_env).decode().strip(),
                                                env["PGDATABASE"])
            bench.validate_native(native, identity, instance, True)
            return {"record": "owned_native_baseline", "commit": commit,
                    "sequence": native["sequence"], "correctness": "PASS"}
        time.sleep(min(.2, max(0, deadline - time.monotonic())))
    raise TimeoutError("native baseline publication did not become READY before setup deadline")


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


def verify_workspace_cleanup(measurements, deadline):
    # Normal execution owns and joins the worker/daemon children before this
    # server cleanup. A fallback must not claim PASS if abrupt interruption
    # bypassed those owners; persisted PID text is not signal authority.
    if measurements.exists():
        from workspace_update_daemon import mounts_under
        if measurements.is_symlink() or mounts_under(measurements):
            raise AssertionError("owned workspace cleanup left native mounts")
        for round_root in measurements.iterdir():
            if not re.fullmatch(r"round-[0-9]{2}", round_root.name):
                continue
            if round_root.is_symlink() or not round_root.is_dir():
                raise AssertionError("owned round cleanup path changed")
            for name in ("owned-workspace-daemon.json", "owned-workspace-worker.json"):
                receipt = round_root / name
                if not receipt.exists():
                    raise AssertionError("owned workspace cleanup receipt is missing")
                if receipt.exists():
                    if receipt.is_symlink() or not receipt.is_file() or receipt.stat().st_size > 4096:
                        raise AssertionError("owned workspace cleanup receipt changed")
                    record = json.loads(receipt.read_text())
                    if (set(record) != {"pid", "starttime", "cleanup_complete"}
                            or type(record["pid"]) is not int or record["pid"] <= 0
                            or type(record["starttime"]) is not str or not record["starttime"].isdecimal()
                            or record["cleanup_complete"] is not True
                            or budget_module.group_members(record["pid"], record["starttime"])):
                        raise AssertionError("owned workspace cleanup was incomplete")
        if time.monotonic() >= deadline:
            raise TimeoutError("owned workspace cleanup verification exceeded its original deadline")


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
    workspace_error = None
    try:
        verify_workspace_cleanup(root / "measurements", deadline)
    except Exception as error:
        workspace_error = error
    service = state.get("service")
    if service:
        pid = service["pid"]
        if service.get("pgid") != pid or service.get("sid") != pid:
            raise AssertionError("owned new-session group binding is missing")
        proc = Path(f"/proc/{pid}")
        reaped = process is not None and process.returncode is not None
        if not reaped and proc.exists():
            fields = proc.joinpath("stat").read_text().rsplit(") ", 1)[1].split()
            started = fields[19]
            argv = [x.decode() for x in proc.joinpath("cmdline").read_bytes().split(b"\0") if x]
            if (started != service["starttime"]
                    or (fields[0] not in ("Z", "X")
                        and argv[:3] != [state["binary"], "--config", str(root / "service.toml")])
                    or os.getpgid(pid) != pid or os.getsid(pid) != pid):
                raise AssertionError("refusing cleanup of a replaced or unrelated service")
        elif not reaped and not proc.exists() and budget_module.group_members(pid, service["starttime"]):
            raise AssertionError("refusing to signal a group whose owned leader is missing")
        budget_module.stop_group(pid, service["starttime"], deadline, process)
    bench.command(["docker", "compose", "-p", project, "-f", str(compose), "down", "--volumes"], deadline)
    containers = bench.command(["docker", "ps", "-aq", "--filter",
                                "label=com.docker.compose.project=" + project], deadline)
    network = bench.command(["docker", "network", "ls", "-q", "--filter",
                             "name=^" + project + "-network$"], deadline)
    if containers.strip() or network.strip():
        raise AssertionError("owned cleanup left project containers or network")
    if time.monotonic() >= deadline:
        raise TimeoutError("owned cleanup inventory exceeded original deadline")
    state.pop("service", None)
    state_path.write_text(json.dumps(state))
    if time.monotonic() >= deadline:
        raise TimeoutError("owned cleanup metadata exceeded original deadline")
    if workspace_error is not None:
        raise workspace_error
    print(json.dumps({"record": "owned_cleanup", "project": project, "correctness": "PASS"}), flush=True)


def owned_service_exit(process):
    """Observe readiness/exit without releasing the owned leader's PID."""
    if process.returncode is not None or not isinstance(process, budget_module.PinnedProcess):
        raise AssertionError("owned service leader must remain pinned")
    observed = os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
    if observed is None:
        return None
    return observed.si_status if observed.si_code == os.CLD_EXITED else -observed.si_status


def graceful_owned(root, project, deadline, process):
    """Only this newly owned service group; final fallback retains the same H."""
    state_path = root / "owned.json"
    state = json.loads(state_path.read_text())
    service = state.get("service", {})
    if (state["project"] != project or service.get("pid") != process.pid
            or service.get("pgid") != process.pid or service.get("sid") != process.pid
            or hashlib.sha256((root / "dependencies.json").read_bytes()).hexdigest() != state["compose_sha256"]):
        raise AssertionError("graceful stop ownership differs from this job")
    proc = Path(f"/proc/{process.pid}")
    fields = proc.joinpath("stat").read_text().rsplit(") ", 1)[1].split()
    argv = [x.decode() for x in proc.joinpath("cmdline").read_bytes().split(b"\0") if x]
    if (fields[19] != service["starttime"] or fields[0] in ("Z", "X")
            or argv[:3] != [state["binary"], "--config", str(root / "service.toml")]
            or os.getpgid(process.pid) != process.pid or os.getsid(process.pid) != process.pid
            or owned_service_exit(process) is not None):
        raise AssertionError("refusing graceful stop of a changed or exited service")
    if time.monotonic() >= deadline:
        raise TimeoutError("no original budget remains for projection drain")
    os.killpg(process.pid, signal.SIGINT)
    until = min(deadline, time.monotonic() + 7)
    while budget_module.group_members(process.pid, service["starttime"]) and time.monotonic() < until:
        time.sleep(min(.01, max(0, until - time.monotonic())))
    if (budget_module.group_members(process.pid, service["starttime"])
            or owned_service_exit(process) != 0 or time.monotonic() >= deadline):
        raise TimeoutError("owned service did not complete its bounded graceful drain")
    budget_module.reap_owned(process, deadline)
    state.pop("service")
    state_path.write_text(json.dumps(state))


def persist_failure_record(run_root, error):
    """Persist a closed failure record without touching private diagnostics.

    The benchmark measurements directory is preferred once it exists.  Early
    setup failures fall back to the owned run root so the workflow can collect
    one small, safe artifact even when the measurement directory was never
    created.  Refuse symlinks and use ``O_NOFOLLOW`` for the final file.
    """
    record = bench.failure_record(error)
    root = Path(run_root)
    try:
        if root.is_symlink() or (root.exists() and not root.is_dir()):
            return
        measurements = root / "measurements"
        if (measurements.exists() and measurements.is_dir()
                and not measurements.is_symlink()):
            parent = measurements
        else:
            parent = root
        parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if parent.is_symlink() or not parent.is_dir():
            return
        target = parent / "failure.json"
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(str(target), flags, 0o600)
        try:
            payload = json.dumps(record, sort_keys=True, separators=(",", ":"),
                                 ensure_ascii=True).encode("ascii")
            view = memoryview(payload + b"\n")
            while view:
                written = os.write(fd, view)
                if written <= 0:
                    return
                view = view[written:]
        finally:
            os.close(fd)
    except (OSError, TypeError, ValueError):
        # Reporting must never hide the original benchmark failure.
        return


def execute(options):
    root, project = hosted_root(options.run_root)
    if root.exists():
        raise ValueError("disposable job directory must not already exist")
    if not re.fullmatch(r"[0-9a-f]{40}", options.mega_sha):
        raise ValueError("server source must be an immutable full SHA-1")
    budget = budget_module.from_options(options)
    if getattr(options, "projection_traces", False):
        original = projection_module.window_anchor(options.session_started_utc, options.session_deadline_utc)
        if budget.cleanup_deadline > original + 1:
            raise ValueError("projection work deadline exceeds its original dispatch anchor")
    deadline = budget.stage_deadline("setup")
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
        with bench.phase("dependency_startup"):
            bench.command(["docker", "compose", "-p", project, "-f", str(compose_path),
                           "up", "-d", "--wait", "--wait-timeout", "180"],
                          min(deadline, time.monotonic() + 240))
        db = "mst2_bench_" + uuid.uuid4().hex
        env = bench.clean_env({"PGHOST": "127.0.0.1", "PGPORT": str(ports["postgres"]),
                               "PGUSER": "mega2", "PGPASSWORD": "mega2_test_password", "PGDATABASE": "mega2"})
        with bench.phase("database_create"):
            bench.command(["psql", "-X", "-v", "ON_ERROR_STOP=1", "-c", "CREATE DATABASE " + db],
                          deadline, env=env)
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
        if getattr(options, "projection_traces", False):
            config["mst2"]["projection_observation_enabled"] = True
        for name in ("oci", "agent_capture", "storage_events"):
            config[name] = {"enabled": False}
        config_path = root / "service.toml"
        config_path.write_text(toml(config))
        config_path.chmod(0o600)
        service_env = bench.clean_env({"MEGA_BASE_DIR": config["base_dir"], "MEGA_CACHE_DIR": str(root / "cache"),
                                       "MEGA_GIT_OBJECT_CACHE_PREFIX": project})
        prefix = [str(binary), "--config", str(config_path)]
        with bench.phase("server_config_validate"):
            bench.command(prefix + ["config", "validate"], deadline, env=service_env)
        with bench.phase("server_service_init"):
            bench.command(prefix + ["service", "init", "--yes"], deadline, env=service_env)
        with bench.phase("owned_native_initialization"):
            print(json.dumps(initialize_owned_native(db, instance, env, deadline)), flush=True)
        log = (root / "service-private.log").open("wb")
        with bench.phase("server_process_start"):
            process = budget_module.PinnedProcess(prefix + ["service", "http", "--host", "127.0.0.1", "-p", str(ports["http"])],
                                       stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                                       env=service_env, start_new_session=True)
        started = None
        try:
            started = budget_module.process_start(process.pid)
            state["service"] = {"pid": process.pid, "pgid": process.pid, "sid": process.pid,
                                "starttime": started}
            state_path.write_text(json.dumps(state))
        except BaseException:
            # Startup identity/state failures must not bypass owned finally.
            # The direct Popen child is still unreaped and pins its group ID.
            if started is None:
                budget_module.abort_startup(process, budget.cleanup_deadline)
            else:
                budget_module.stop_group(process.pid, started, budget.cleanup_deadline, process)
            raise
        base = f"http://127.0.0.1:{ports['http']}"
        ready_until = min(deadline, time.monotonic() + 180)
        with bench.phase("server_readiness"):
            while True:
                if owned_service_exit(process) is not None or time.monotonic() >= ready_until:
                    raise RuntimeError("owned service failed readiness")
                try:
                    with urlopen(base + "/api/v2/snapshots/capabilities",
                                 timeout=min(2, max(.001, ready_until - time.monotonic()))) as response:
                        if response.status == 200:
                            break
                except OSError:
                    pass
                time.sleep(min(.2, max(0, ready_until - time.monotonic())))
        with bench.phase("initial_git_identity_seed"):
            initial = bench.command(["git", "ls-remote", base + "/project", "refs/heads/main"], deadline).decode().split()
        if len(initial) != 2 or initial[1] != "refs/heads/main":
            raise AssertionError("owned service did not initialize exactly one project main")
        env.update(M2_TOKEN=token, M2_GIT_TOKEN=git_token)
        with bench.phase("owned_native_baseline"):
            baseline = bootstrap_ready_baseline(root, base, env, instance, deadline)
            print(json.dumps(baseline), flush=True)
        with bench.phase("initial_git_identity"):
            initial = bench.command(["git", "ls-remote", base + "/project", "refs/heads/main"],
                                    deadline, env=bench.clean_env({
                                        "GIT_CONFIG_COUNT": "3",
                                        "GIT_CONFIG_KEY_0": "http.extraHeader",
                                        "GIT_CONFIG_VALUE_0": "Authorization: Bearer " + git_token,
                                        "GIT_CONFIG_KEY_1": "http.followRedirects",
                                        "GIT_CONFIG_VALUE_1": "false",
                                        "GIT_CONFIG_KEY_2": "credential.helper",
                                        "GIT_CONFIG_VALUE_2": "",
                                    })).decode().split()
        if len(initial) != 2 or initial[1] != "refs/heads/main" or initial[0] != baseline["commit"]:
            raise AssertionError("native baseline did not publish exactly one project main")
        budget.require(options.rounds * budget_module.ROUND_SECONDS
                       + budget_module.REPORT_RESERVE + budget_module.CLEANUP_RESERVE
                       + budget_module.MARGIN)
        print(json.dumps({"record": "owned_server_build", "source_sha": source_sha,
                          "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "project": project,
                          "requested_profile": options.profile, "actual_profile": options.profile,
                          "requested_rounds": options.rounds, "actual_rounds": options.rounds,
                          "remaining_session_seconds": budget.cleanup_deadline - time.monotonic()}), flush=True)
        os.environ.update(env)
        args = bench.parser().parse_args([
            "--execute", "--isolated-deployment", "--base-url", base, "--git-url", base + "/project",
            "--database", db, "--instance-id", instance, "--expect-initial-commit", initial[0],
            "--service-pid", str(process.pid), "--driver", str(options.driver.resolve()),
            "--driver-sha256", options.driver_sha256, "--run-root", str(root / "measurements"),
            "--profile", options.profile, "--rounds", str(options.rounds),
            "--session-deadline-utc", options.session_deadline_utc])
        args.budget = budget
        if getattr(options, "projection_traces", False):
            args.projection_traces = True
            args.finalize_projection = lambda original_deadline: graceful_owned(root, project, original_deadline, process)
        if time.monotonic() >= deadline:
            raise TimeoutError("owned setup exceeded its fixed stage budget")
        with bench.phase("commit_update_benchmark"):
            bench.execute(args)
    finally:
        try:
            stop_owned(root, project, budget.cleanup_deadline, process)
        finally:
            if log:
                log.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--cleanup", action="store_true")
    parser.add_argument("--projection-traces", action="store_true")
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--mega-source", type=Path)
    parser.add_argument("--mega-sha")
    parser.add_argument("--mega-binary", type=Path)
    parser.add_argument("--driver", type=Path)
    parser.add_argument("--driver-sha256")
    parser.add_argument("--session-deadline-utc")
    parser.add_argument("--session-started-utc")
    budget_module.add_recovery_argument(parser)
    parser.add_argument("--work-cleanup-deadline-monotonic", type=float,
                        default=os.environ.get("MST2_WORK_CLEANUP_DEADLINE_MONOTONIC"))
    parser.add_argument("--profile", choices=("smoke", "medium"), default="medium")
    parser.add_argument("--rounds", type=int, choices=range(3, 11), default=3)
    opts = parser.parse_args()
    try:
        if opts.execute and opts.cleanup:
            raise ValueError("execute and cleanup are separate operations")
        if opts.cleanup:
            owned, compose_project = hosted_root(opts.run_root)
            if (owned / "owned.json").exists():
                stop_owned(owned, compose_project, budget_module.from_options(opts).cleanup_deadline)
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
        persist_failure_record(opts.run_root, error)
        print(json.dumps(bench.failure_record(error)), file=sys.stderr)
        raise SystemExit(1)
