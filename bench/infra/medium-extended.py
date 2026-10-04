#!/usr/bin/env python3
"""Multi-workspace, VCS and concurrent workloads; no remote pushes."""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import subprocess
import threading
import time
import urllib.request
import uuid


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("action", choices=["workspaces", "development", "concurrency", "soak"])
    p.add_argument("--cluster", required=True)
    p.add_argument("--files", type=int, default=200000)
    p.add_argument("--rounds", type=int, default=5)
    p.add_argument("--duration", type=int, default=60)
    a = p.parse_args()
    rows = [json.loads(x) for x in open("/fixture/manifest.jsonl")][:a.files]
    target = next(x["commit"] for x in json.load(open("/fixture/manifest.refs.json")) if x["files"] == a.files)
    repo = "http://mega2:8000/project"
    out = Path("/data/results")
    out.mkdir(exist_ok=True)
    logfile = out / f"{a.cluster}-{a.files}-{a.action}.jsonl"
    if logfile.exists():
        raise SystemExit("preserve existing output")
    lock = threading.Lock()
    env = dict(os.environ, LIBRA_SCORPIOFS_ENDPOINT="http://127.0.0.1:2725/antares",
               GIT_TERMINAL_PROMPT="0", LIBRA_FETCH_IDLE_TIMEOUT_MS="900000")
    for k in tuple(env):
        if k.lower() in ("http_proxy", "https_proxy", "all_proxy"):
            del env[k]

    def emit(row):
        row.update(cluster=a.cluster, files=a.files, commit=target, utc=time.time())
        with lock, logfile.open("a") as f:
            f.write(json.dumps(row)+"\n")

    def cmd(argv, cwd=None, timeout=900):
        start = time.monotonic_ns()
        r = subprocess.run(argv, cwd=cwd, capture_output=True, env=env, timeout=timeout)
        ms = (time.monotonic_ns()-start)/1e6
        if r.returncode:
            raise RuntimeError(f"command failed rc={r.returncode}: {argv[0]} {argv[1:3]}: " + r.stderr.decode(errors="replace")[-2000:])
        return ms, r.stdout

    def api(path, body=None, method="GET"):
        req = urllib.request.Request("http://127.0.0.1:2725/antares"+path,
            data=None if body is None else json.dumps(body).encode(), method=method,
            headers={"Content-Type": "application/json"})
        with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req, timeout=900) as r:
            return json.loads(r.read() or b"null")

    def mount():
        m = api("/mounts", {"path": "/project", "job_id": "extended-"+uuid.uuid4().hex}, "POST")
        root = Path(m["mountpoint"])
        if not os.path.ismount(root):
            raise RuntimeError("no FUSE mount")
        return m, root

    def physical():
        paths = [x for x in Path("/data").iterdir() if x.name.startswith(("scorpio", "cold-")) and x.is_dir()]
        return sum(int(cmd(["du", "-sB1", str(x)])[1].split()[0]) for x in paths)

    def check(root, sample):
        t = time.monotonic_ns()
        buffers = [(root/r["path"]).read_bytes() for r in sample]
        ms = (time.monotonic_ns()-t)/1e6
        for r, b in zip(sample, buffers):
            if hashlib.sha256(b).hexdigest() != r["sha256"]:
                raise RuntimeError("hash mismatch: "+r["path"])
        return {"ms": ms, "bytes": sum(map(len, buffers)), "file_count": len(sample), "verified": True}

    tip = cmd(["git", "ls-remote", repo, "refs/heads/main"])[1].decode().split()[0]
    if tip != target:
        raise RuntimeError("ref mismatch")
    sample = random.Random(20261003).sample(rows, 50)
    failed = False
    if a.action == "workspaces":
        for round_no in range(a.rounds):
            order = ["scorpiofs-api", "git-worktree", "git-independent"]
            if round_no % 2:
                order.reverse()
            for backend in order:
                mounts = []
                work = out / f"multi-{round_no}-{backend}"
                work.mkdir()
                try:
                    before = physical() if backend == "scorpiofs-api" else 0
                    base = work/"w0"
                    for number in range(5):
                        t = time.monotonic_ns()
                        if backend == "scorpiofs-api":
                            m, root = mount()
                            mounts.append(m)
                        elif number == 0 or backend == "git-independent":
                            root = work/f"w{number}"
                            cmd(["git", "clone", "--depth", "1", repo, str(root)])
                        else:
                            root = work/f"w{number}"
                            cmd(["git", "-C", str(base), "worktree", "add", "--detach", str(root), "HEAD"])
                        ms = (time.monotonic_ns()-t)/1e6
                        # Access condition same for all, outside create timer.
                        if not (root/"svc01").is_dir():
                            raise RuntimeError("missing service directory")
                        check(root, sample)
                        allocated = physical()-before if backend == "scorpiofs-api" else int(cmd(["du", "-sB1", str(work)])[1].split()[0])
                        emit({"phase": "workspaces", "round": round_no+1, "backend": backend,
                              "workspace_count": number+1, "create_ms": ms, "physical_bytes": allocated,
                              "logical_bytes_per_workspace": sum(r["size"] for r in rows), "status": "success"})
                except Exception as e:
                    emit({"phase": "workspaces", "round": round_no+1, "backend": backend, "status": "failed", "error": str(e)})
                    failed = True
                finally:
                    for m in reversed(mounts):
                        api("/mounts/"+m["mount_id"], method="DELETE")
                    shutil.rmtree(work)
                if failed:
                    raise RuntimeError("workspace gate failed")
    elif a.action == "development":
        for number in range(a.rounds):
            for backend in (["libra", "git"] if number%2 == 0 else ["git", "libra"]):
                work = out/f"dev-{number}-{backend}"
                work.mkdir()
                root = work/"worktree"
                linked = False
                try:
                    if backend == "git":
                        ms, _ = cmd(["git", "clone", "--depth", "1", repo, str(root)])
                        cmd(["git", "config", "user.name", "medium"], cwd=root)
                        cmd(["git", "config", "user.email", "medium@bench.invalid"], cwd=root)
                    else:
                        ms, _ = cmd(["libra", "clone", "--depth", "1", "--no-checkout", repo, str(work/"base")])
                        cmd(["libra", "config", "set", "user.name", "medium"], cwd=work/"base")
                        cmd(["libra", "config", "set", "user.email", "medium@bench.invalid"], cwd=work/"base")
                        attach, _ = cmd(["libra", "worktree", "add", "--backend", "scorpiofs", "-b",
                                         "dev-"+uuid.uuid4().hex, str(root)], cwd=work/"base")
                        linked = True
                        if not os.path.ismount(root) or not (root/".libra").exists():
                            raise RuntimeError("Libra attach missing mount or VCS identity")
                        emit({"phase": "libra_attach", "round": number+1, "ms": attach, "status": "success"})
                    emit({"phase": "vcs_clone", "round": number+1, "backend": backend, "ms": ms, "status": "success"})
                    check(root, sample)
                    for count in (0, 1, 100):
                        for r in rows[:count]:
                            (root/r["path"]).write_bytes(b"medium changed\n")
                        ms, status = cmd([backend, "status", "--porcelain"], cwd=root)
                        lines = [x for x in status.decode().splitlines() if x.strip()]
                        if len(lines) != count:
                            raise RuntimeError(f"status count mismatch {len(lines)} vs {count}")
                        emit({"phase": "status", "backend": backend, "round": number+1, "changes": count, "ms": ms, "status": "success"})
                    ms, _ = cmd([backend, "add", "--", *[r["path"] for r in rows[:100]]], cwd=root)
                    emit({"phase": "add", "backend": backend, "round": number+1, "ms": ms, "status": "success"})
                    ms, _ = cmd([backend, "commit", "-m", "medium isolated local changes"], cwd=root)
                    emit({"phase": "commit", "backend": backend, "round": number+1, "ms": ms, "status": "success"})
                    _, status = cmd([backend, "status", "--porcelain"], cwd=root)
                    if status.strip():
                        raise RuntimeError("local commit left changes")
                except Exception as e:
                    emit({"phase": "development", "backend": backend, "round": number+1, "status": "failed", "error": str(e)})
                    failed = True
                finally:
                    if linked:
                        cmd(["libra", "worktree", "umount", str(root)], cwd=work/"base")
                    # On failed attach there may be a late mount: unmount through API before deletion.
                    if root.exists() and os.path.ismount(root):
                        raise RuntimeError("cannot delete mounted development directory")
                    shutil.rmtree(work)
                if failed:
                    raise RuntimeError("development failed; preserve evidence, no retries with workarounds")
    else:
        levels = (4,) if a.action == "soak" else (1, 4)
        for backend in ("scorpiofs", "git"):
            for workers in levels:
                mounts = []
                roots = []
                work = out/f"load-{backend}-{workers}"
                work.mkdir()
                try:
                    for i in range(workers):
                        if backend == "scorpiofs":
                            m, root = mount()
                            mounts.append(m)
                        else:
                            root = work/f"w{i}"
                            cmd(["git", "clone", "--depth", "1", repo, str(root)])
                        check(root, sample)
                        roots.append(root)
                    barrier = threading.Barrier(workers)
                    stop_at = time.monotonic() + a.duration

                    def worker(i):
                        barrier.wait()
                        n = 0
                        while time.monotonic() < stop_at:
                            start = time.monotonic()
                            result = check(roots[i], sample)
                            list((roots[i]/"svc01").iterdir())
                            emit({"phase": a.action, "backend": backend, "workers": workers,
                                  "worker": i, "request": n, "read": result, "status": "success"})
                            n += 1
                            if a.action == "soak":
                                time.sleep(max(0, .5-(time.monotonic()-start)))
                        return n

                    start = time.monotonic()
                    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                        counts = list(pool.map(worker, range(workers)))
                    emit({"phase": "load_summary", "backend": backend, "workers": workers,
                          "completed": sum(counts), "elapsed_s": time.monotonic()-start,
                          "target_duration_s": a.duration, "status": "success"})
                finally:
                    for m in mounts:
                        api("/mounts/"+m["mount_id"], method="DELETE")
                    shutil.rmtree(work)
    after = cmd(["git", "ls-remote", repo, "refs/heads/main"])[1].decode().split()[0]
    if after != target:
        raise RuntimeError("fixed remote ref changed")


if __name__ == "__main__":
    main()
