#!/usr/bin/env python3
"""Bounded in-cluster driver. Preserve failures, verify against fixed manifest."""
import argparse
import concurrent.futures
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import random
import signal
import shutil
import subprocess
import sys
import time
import urllib.request
import uuid


def command(argv, timeout=900, **kw):
    return subprocess.check_output(argv, timeout=timeout, **kw)


def http(path, body=None, method="GET"):
    request = urllib.request.Request("http://127.0.0.1:2725/antares" + path,
        data=None if body is None else json.dumps(body).encode(), method=method,
        headers={"Content-Type": "application/json"})
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request, timeout=900) as r:
        b = r.read()
        return json.loads(b) if b else None


def load_readiness():
    spec = importlib.util.spec_from_file_location("readiness", "/bench-medium/first-directory-ready.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def require_shallow_gate(path, target, repo):
    gate = json.loads(path.read_text())
    if (gate.get("status") != "success" or gate.get("head") != target
            or gate.get("repo") != repo or gate.get("depth") != 1
            or not gate.get("strict_object_set_verified")):
        raise RuntimeError("measure requires a successful strict gate for the fixed repository tip")
    return gate


def publish_fixture_commit(fixture_git, repo, row, env):
    def git(*args, **kw):
        return command(["git", "--git-dir=" + str(fixture_git), *args], env=env, **kw).decode().strip()

    tree = git("rev-parse", row["commit"] + "^{tree}")
    tip = git("ls-remote", repo, "refs/heads/main").split()
    parent = ["-p", tip[0]] if tip else []
    commit = git("commit-tree", tree, *parent,
                 input=f"medium fixture {row['files']} files\n".encode())
    git("-c", "http.postBuffer=1073741824", "push", repo, commit + ":refs/heads/main")
    git("fetch", repo, "refs/heads/main")
    observed = git("rev-parse", "FETCH_HEAD")
    if git("rev-parse", observed + "^{tree}") != tree:
        raise RuntimeError("server content tree differs from immutable fixture")
    if git("ls-remote", repo, "refs/heads/main").split()[0] != observed:
        raise RuntimeError("server ref changed while freezing the fixture")
    return dict(row, fixture_commit=row.get("fixture_commit", row["commit"]),
                commit=observed, tree=tree)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("action", choices=["seed", "gate", "measure", "verify"])
    ap.add_argument("--files", type=int, choices=[50000, 100000, 200000], required=True)
    ap.add_argument("--rounds", type=int, default=5)
    ap.add_argument("--cluster", required=True)
    ap.add_argument("--cache", choices=["existing-server", "client-cold", "end-to-end-cold", "warm"], default="existing-server")
    ap.add_argument("--backend", choices=["both", "git", "scorpiofs"], default="both")
    ap.add_argument("--label", default="default")
    ap.add_argument("--bootstrap-initial", action="store_true")
    ap.add_argument("--out", type=Path, default=Path("/data/results"))
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    repo = "http://mega2:8000/project"
    rows = [json.loads(x) for x in open("/fixture/manifest.jsonl")][:args.files]
    refs = json.load(open("/fixture/manifest.refs.json"))
    target = next(x["commit"] for x in refs if x["files"] == args.files)
    fixture_git = "/fixture/repo.git"
    prefix = f"{args.cluster}-{args.files}-{args.action}-{args.cache}-{args.label}"
    logfile = args.out / (prefix + ".jsonl")
    if logfile.exists():
        raise SystemExit("refusing to overwrite results " + str(logfile))

    def emit(row):
        row.update(cluster=args.cluster, files=args.files, commit=target, cache=args.cache,
                   utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        with logfile.open("a") as out:
            out.write(json.dumps(row) + "\n")
        print(json.dumps(row), flush=True)

    if args.action == "seed":
        # Source is an immutable repository embedded in the fixture image.
        env = dict(os.environ, GIT_TERMINAL_PROMPT="0")
        history_env = dict(env, GIT_AUTHOR_NAME="medium-benchmark", GIT_AUTHOR_EMAIL="medium@bench.invalid",
                           GIT_COMMITTER_NAME="medium-benchmark", GIT_COMMITTER_EMAIL="medium@bench.invalid",
                           GIT_AUTHOR_DATE="2026-10-03T00:00:00+0000", GIT_COMMITTER_DATE="2026-10-03T00:00:00+0000")
        remote = command(["git", "ls-remote", repo, "refs/heads/main"]).decode().split()
        current = next((r["files"] for r in refs if remote and r["commit"] == remote[0]), 0)
        if remote and current == 0:
            if not args.bootstrap_initial:
                raise RuntimeError("unknown remote tip; explicit isolated bootstrap required")
            audit = "/data/bootstrap-audit"
            oid = command(["git", "-C", audit, "rev-parse", "HEAD"]).decode().strip()
            tree = [x.split() for x in command(["git", "-C", audit, "ls-tree", "-r", "-l", "HEAD"]).decode().splitlines()]
            expected = [["100644", "blob", "afff6026cbd0c96177c593ff71f92a563f354c3b", "39", ".gitkeep"]]
            subject = command(["git", "-C", audit, "log", "-1", "--format=%s"]).decode().strip()
            if oid != remote[0] or tree != expected or subject != "Init Mega Directory":
                raise RuntimeError("bootstrap audit differs from known fresh initialization")
            # Trunk policy requires first-parent ancestry. Reuse immutable fixture trees,
            # but parent its history to this cluster's audited initialization commit.
            # Import the already verified bootstrap object via local Git transport.
            # Negotiating an unrelated fixture history against this HTTP server is unsupported.
            command(["git", "--git-dir="+fixture_git, "fetch", "--depth", "1", audit, "HEAD"])
            shallow = Path(fixture_git)/"shallow"
            if shallow.exists():
                for boundary in shallow.read_text().splitlines():
                    raw = command(["git", "--git-dir="+fixture_git, "cat-file", "-p", boundary]).decode()
                    if any(line.startswith("parent ") for line in raw.splitlines()):
                        raise RuntimeError("cannot remove a real shallow ancestry boundary")
                # These are actual root commits, so the imported ancestry is complete.
                # The backend receive-pack parser does not support the shallow header.
                shallow.unlink()
            parent = oid
            derived = []
            for row in refs:
                tree_oid = command(["git", "--git-dir="+fixture_git, "rev-parse", row["commit"]+"^{tree}"]).decode().strip()
                commit = command(["git", "--git-dir="+fixture_git, "commit-tree", tree_oid, "-p", parent],
                                 input=f"medium fixture {row['files']} files\n".encode(), env=history_env).decode().strip()
                derived.append({"files": row["files"], "commit": commit, "tree": tree_oid})
                parent = commit
            refs = derived
            Path("/fixture/manifest.refs.json").write_text(json.dumps(refs, indent=2)+"\n")
            target = next(x["commit"] for x in refs if x["files"] == args.files)
            emit({"phase": "isolated_bootstrap_audit", "initial_commit": oid, "initial_tree": tree,
                  "derived_history": refs, "content_trees_unchanged": True})
        for index, row in enumerate(refs):
            if row["files"] <= current:
                continue
            if row["files"] > args.files:
                break
            start = time.monotonic()
            refs[index] = publish_fixture_commit(fixture_git, repo, row, history_env)
            Path("/fixture/manifest.refs.json").write_text(json.dumps(refs, indent=2) + "\n")
            emit({"phase": "seed_push", "batch_files": row["files"],
                  "canonical_commit": refs[index]["commit"], "content_tree_verified": True,
                  "ms": (time.monotonic()-start)*1000})
        target = next(x["commit"] for x in refs if x["files"] == args.files)
        observed = command(["git", "ls-remote", repo, "refs/heads/main"]).decode().split()[0]
        if observed != target:
            raise RuntimeError("seed ref mismatch")
        emit({"phase": "seed", "status": "success"})
        return

    def check_ref():
        observed = command(["git", "ls-remote", repo, "refs/heads/main"], timeout=30).decode().split()[0]
        if observed != target:
            raise RuntimeError("remote fixed ref changed")

    def mount():
        m = http("/mounts", {"path": "/project", "job_id": "medium-" + uuid.uuid4().hex}, "POST")
        p = Path(m["mountpoint"])
        if not os.path.ismount(p):
            raise RuntimeError("API success without FUSE mount")
        return m, p

    def read(root, selected):
        start = time.monotonic_ns()
        data = [(root / r["path"]).read_bytes() for r in selected]
        ms = (time.monotonic_ns() - start) / 1e6
        finished = time.monotonic()
        for row, buf in zip(selected, data):
            if len(buf) != row["size"] or hashlib.sha256(buf).hexdigest() != row["sha256"]:
                raise RuntimeError("read hash mismatch: " + row["path"])
            if (root/row["path"]).stat().st_mode & 0o777 != row["mode"]:
                raise RuntimeError("mode mismatch: " + row["path"])
        return {"ms": ms, "bytes": sum(map(len, data)), "count": len(selected), "verified": True, "finished_s": finished}

    def traverse(root):
        start = time.monotonic()
        seen = set()
        for base, dirs, files in os.walk(root):
            dirs[:] = [x for x in dirs if x not in (".git", ".libra")]
            for f in files:
                seen.add(str((Path(base)/f).relative_to(root)))
        ms = (time.monotonic() - start) * 1000
        expected = {row["path"] for row in rows}
        if seen != expected:
            raise RuntimeError(f"traversal count mismatch actual={len(seen)} expected={len(expected)}")
        return {"ms": ms, "count": len(seen), "verified": True}

    check_ref()
    gate_path = args.out / f"{args.cluster}-{args.files}-git-shallow-gate.json"
    if args.action in ("gate", "verify"):
        if not gate_path.exists():
            command([sys.executable, "/bench-medium/git-shallow-gate.py", "--repo", repo,
                     "--oracle", fixture_git, "--commit", target, "--out", str(gate_path)], timeout=900)
        gate = require_shallow_gate(gate_path, target, repo)
        emit({"phase": "git_shallow_gate", "result": gate,
              "status": "success", "excluded_from_measurement": True})
        checkout = Path("/data/oracle-" + str(args.files))
        if not checkout.exists():
            command(["git", "clone", "--depth", "1", repo, str(checkout)])
        actual = command(["git", "-C", str(checkout), "rev-parse", "HEAD"]).decode().strip()
        if actual != target:
            raise RuntimeError("oracle checkout mismatch")
        if args.action == "verify":
            subset = args.out / (prefix + "-manifest.jsonl")
            subset.write_text("".join(json.dumps(r)+"\n" for r in rows))
            command(["python3", "/bench-medium/medium-monorepo.py", "verify", "--root", str(checkout),
                     "--manifest", str(subset)], timeout=1800)
        emit({"phase": "git_tree", "result": traverse(checkout), "status": "success"})
        m, root = mount()
        try:
            state = http("/worktrees/" + m["mount_id"] + "/state")
            base = state.get("base_revision")
            refresh = http("/worktrees/" + m["mount_id"] + "/refresh",
                           {"expected_base_revision": base, "require_clean": True}, "POST")
            if refresh.get("disposition") not in ("already_at_target", "switched"):
                raise RuntimeError("clean refresh failed: " + json.dumps(refresh))
            finalize = http("/worktrees/" + m["mount_id"] + "/commit-finalize",
                            {"expected_base_revision": refresh.get("base_revision"), "committed_paths": []}, "POST")
            if finalize.get("state") != "ready":
                raise RuntimeError("empty finalize failed: " + json.dumps(finalize))
            emit({"phase": "control_plane", "refresh": refresh, "finalize": finalize, "status": "success"})
            emit({"phase": "scorpio_tree", "result": traverse(root), "status": "success"})
            for start in range(0, len(rows), 1000) if args.action == "verify" else (0,):
                read(root, rows[start:start+1000])
                if args.action == "verify" and (start+1000) % 10000 == 0:
                    emit({"phase": "content_verify_progress", "verified_files": min(start+1000,len(rows)), "status": "success"})
            # First-write regression with a unique path, entirely inside temporary upper.
            link = root / "medium-link"
            link.symlink_to(rows[1]["path"])
            if not link.is_symlink() or link.read_bytes() != (root/rows[1]["path"]).read_bytes():
                raise RuntimeError("symlink regression")
            link.unlink()
            empty = root / "medium-empty-directory"
            empty.mkdir()
            empty.rmdir()
            p = root / "medium-write-regression.txt"
            p.write_bytes(b"a"*4096)
            p.write_bytes(b"short")
            if p.read_bytes() != b"short":
                raise RuntimeError("truncate regression")
            renamed = p.with_name("medium-renamed.txt")
            p.rename(renamed)
            renamed.unlink()
            victim = root / rows[0]["path"]
            original = victim.read_bytes()
            with victim.open("wb"):
                pass
            if victim.read_bytes() or victim.stat().st_size:
                raise RuntimeError("first lower copy-up O_TRUNC regression")
            victim.write_bytes(original)
            if victim.read_bytes() != original:
                raise RuntimeError("lower rewrite regression")
            victim.unlink()
            if victim.exists():
                raise RuntimeError("whiteout regression")
            emit({"phase": "correctness", "status": "success", "full_content_verified": args.action == "verify"})
        finally:
            http("/mounts/" + m["mount_id"], method="DELETE")
        return

    require_shallow_gate(gate_path, target, repo)
    readiness = load_readiness()
    rng = random.Random(20261003)
    sample = rng.sample(rows, 1100)
    for number in range(1, args.rounds + 1):
        for backend in (["scorpiofs", "git"] if number % 2 else ["git", "scorpiofs"]):
            if args.backend != "both" and backend != args.backend:
                continue
            check_ref()
            work = args.out / f"{prefix}-round-{number}-{backend}"
            work.mkdir()
            config = argparse.Namespace(work=work, api="http://127.0.0.1:2725/antares",
                repo=repo, scope="/project", commit=target, timeout=900, cleanup_timeout=30, expected_dir=["svc01"])
            config.retain_mount = backend == "scorpiofs"
            if args.cache in ("client-cold", "end-to-end-cold"):
                pid = int(Path("/data/daemon.pid").read_text())
                if Path(f"/proc/{pid}/exe").resolve().name != "scorpio":
                    raise RuntimeError("refuse to kill unexpected daemon pid")
                reset = time.monotonic()
                os.kill(pid, signal.SIGTERM)
                deadline = reset + 30
                while Path(f"/proc/{pid}").exists() and time.monotonic() < deadline:
                    # Zombie indicates termination; init/wrapper will reap it.
                    try:
                        process_state = Path(f"/proc/{pid}/stat").read_text()
                    except FileNotFoundError:
                        break
                    if process_state.split(") ", 1)[1].startswith("Z"):
                        break
                    time.sleep(.1)
                else:
                    if Path(f"/proc/{pid}").exists():
                        raise RuntimeError("daemon did not stop before cache reset")
                fresh = Path("/data/cold-" + uuid.uuid4().hex)
                env = dict(os.environ)
                for key, suffix in (("WORKSPACE", "mount"), ("STORE_PATH", "store"),
                                    ("ANTARES_UPPER_ROOT", "upper"), ("ANTARES_CL_ROOT", "cl"),
                                    ("ANTARES_MOUNT_ROOT", "mounts")):
                    path = fresh / suffix
                    path.mkdir(parents=True, exist_ok=True)
                    env["SCORPIO_"+key] = str(path)
                env["SCORPIO_ANTARES_STATE_FILE"] = str(fresh/"state.toml")
                log = (fresh/"daemon.log").open("wb")
                process = subprocess.Popen(["/usr/local/bin/docker-entrypoint.sh", "serve", "--http-addr", "0.0.0.0:2725"],
                                            env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                log.close()
                Path("/data/daemon.pid").write_text(str(process.pid))
                deadline = time.monotonic() + 30
                while True:
                    try:
                        http("/health")
                        break
                    except Exception:
                        if time.monotonic() > deadline or process.poll() is not None:
                            raise RuntimeError("cold daemon failed health gate")
                        time.sleep(.1)
                os.sync()
                Path("/proc/sys/vm/drop_caches").write_text("3\n")
                emit({"phase": "client_cache_reset", "round": number, "backend": backend,
                      "new_cas": str(fresh/"store"), "reset_ms": (time.monotonic()-reset)*1000,
                      "excluded_from_readiness_timer": True})
            workflow_start = time.monotonic()
            result = readiness.scorpio_round(config, "ready") if backend == "scorpiofs" else readiness.git_round(config, "ready")
            emit({"phase": "directory_ready", "round": number, **result})
            if result["status"] != "success":
                raise RuntimeError("readiness failed; stop invalid subsequent measurements")
            m = None
            try:
                if backend == "scorpiofs":
                    m, root = {"mount_id": result["mount_id"]}, work/"ready-mount"
                else:
                    root = work / "ready-git"
                if args.cache == "warm":
                    read(root, sample)
                for count in (100, 1000):
                    selected = sample[:100] if count == 100 else sample[100:]
                    first = read(root, selected)
                    cumulative_ms = (first.pop("finished_s")-workflow_start)*1000
                    repeat = read(root, selected)
                    repeat.pop("finished_s")
                    emit({"phase": "read", "round": number, "backend": backend,
                          "first": first, "repeat": repeat, "status": "success",
                          "workflow_elapsed_to_first_read_ms": cumulative_ms,
                          "workflow_distinct_files_read": 100 if count == 100 else 1100,
                          "workflow_prior_repeat_files": 0 if count == 100 else 100,
                          "same_workspace_as_readiness": True})
                emit({"phase": "traverse", "round": number, "backend": backend,
                      "result": traverse(root), "status": "success"})
                large = next(r for r in rows if r["size"] >= 1048576+32768)
                with (root/large["path"]).open("rb") as f:
                    t = time.monotonic_ns()
                    f.seek(1048576-32768)
                    buf = f.read(65536)
                    ms = (time.monotonic_ns()-t)/1e6
                reference = command(["git", "--git-dir="+fixture_git, "show", target+":"+large["path"]])
                if buf != reference[1015808:1015808+65536]:
                    raise RuntimeError("range read mismatch")
                emit({"phase": "range_read", "round": number, "backend": backend,
                      "ms": ms, "bytes": len(buf), "offset": 1015808,
                      "crosses_1mib_chunk_boundary": True, "verified": True, "status": "success"})
            finally:
                if m:
                    http("/mounts/" + m["mount_id"], method="DELETE")
                    emit({"phase": "cleanup", "round": number, "backend": backend, "status": "success"})
                if backend == "git":
                    shutil.rmtree(root)
    check_ref()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(json.dumps({"status": "failed", "error": str(error)}), flush=True)
        raise SystemExit(1)
        raise
