#!/usr/bin/env python3
import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request
import uuid


def http_json(url, method="GET", body=None, timeout=60):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        url, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(
        request, timeout=timeout
    ) as response:
        payload = response.read()
        return json.loads(payload) if payload else None


def mounted(path):
    target = str(path.resolve())
    with open("/proc/self/mountinfo", encoding="utf-8") as stream:
        return any(line.split()[4] == target for line in stream)


def listing_probe(path, expected_dirs, deadline):
    probe = (
        "import json,sys; from pathlib import Path; p=Path(sys.argv[1]); "
        "print(json.dumps({e.name:e.is_dir() for e in p.iterdir()}))"
    )
    while time.monotonic() < deadline:
        try:
            timeout = min(3, remaining(deadline))
            if not timeout:
                break
            data = subprocess.check_output(
                ["python3", "-c", probe, str(path)],
                text=True,
                timeout=timeout,
                stderr=subprocess.DEVNULL,
            )
            names = json.loads(data)
            if isinstance(names, dict) and expected_dirs.issubset(names) and all(
                names[name] is True for name in expected_dirs
            ):
                stamp = time.monotonic()
                if stamp <= deadline:
                    return stamp, sorted(names)
        except (OSError, subprocess.SubprocessError, ValueError):
            pass
        time.sleep(min(0.025, remaining(deadline)))
    raise TimeoutError("root directory visibility deadline exceeded")


def round_result(backend, label):
    return {
        "backend": backend, "label": label, "status": "failed", "valid": False,
        "visibility_status": "not_observed", "completion_status": "failed",
        "cleanup_status": "not_required",
    }


def finish_result(result):
    result["valid"] = (
        result["visibility_status"] == "success"
        and result["completion_status"] == "success"
        and (result["cleanup_status"] in {"success", "not_required"}
             or (result["cleanup_status"] == "deferred" and result.get("cleanup_deferred")))
    )
    result["status"] = "success" if result["valid"] else "failed"
    return result


def remaining(deadline):
    return max(0, deadline - time.monotonic())


def record_visibility(result, stamp, start, deadline, names):
    if stamp > deadline:
        raise TimeoutError("root directory visibility deadline exceeded")
    result.update(
        visibility_status="success",
        first_directory_ms=round((stamp - start) * 1000, 3),
        root_entries=names,
    )


def scorpio_round(args, label):
    path = args.work / (label + "-mount")
    path.mkdir()
    start = time.monotonic()
    deadline = start + args.timeout
    mount_id = None
    create = listing = None
    result = round_result("scorpiofs", label)
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    stage = "mount registration"
    try:
        create = pool.submit(
            http_json,
            args.api + "/mounts",
            "POST",
            {
                "path": args.scope,
                "job_id": label + "-" + str(uuid.uuid4()),
                "mountpoint": str(path),
            },
            args.timeout,
        )
        result["cleanup_status"] = "unknown"
        while time.monotonic() < deadline and not mounted(path):
            if create.done():
                create.result()
            time.sleep(min(0.025, remaining(deadline)))
        if not mounted(path):
            raise TimeoutError("no FUSE mount registered before deadline")
        stage = "root directory visibility"
        listing = pool.submit(listing_probe, path, set(args.expected_dir), deadline)
        stamp, names = listing.result(timeout=remaining(deadline))
        # Visibility is evidence in its own right, even if completion later fails.
        record_visibility(result, stamp, start, deadline, names)
        stage = "API completion"
        created = create.result(timeout=remaining(deadline))
        mount_id = created["mount_id"]
        if not isinstance(mount_id, str) or not mount_id:
            raise ValueError("API returned no mount_id")
        result.update(
            completion_status="success",
            api_complete_ms=round((time.monotonic() - start) * 1000, 3),
            mount_id=mount_id,
            clone_required=False,
            index_required=False,
        )
    except Exception as error:
        result["error"] = str(error) or stage + " deadline exceeded"
    finally:
        cleanup_start = time.monotonic()
        result["measurement_elapsed_ms"] = round((cleanup_start - start) * 1000, 3)
        cleanup_timeout = getattr(args, "cleanup_timeout", 30)
        cleanup_deadline = cleanup_start + cleanup_timeout
        result["cleanup_timeout_s"] = cleanup_timeout
        if mount_id is None and create is not None:
            try:
                # Late API success may supply the only usable cleanup handle.
                created = create.result(timeout=remaining(cleanup_deadline))
                mount_id = created["mount_id"]
                if not isinstance(mount_id, str) or not mount_id:
                    raise ValueError("API returned no mount_id")
                result.update(mount_id=mount_id, api_completed_during_cleanup=True)
            except Exception as error:
                result["cleanup_status"] = "unknown" if create.done() else "failed"
                result["cleanup_error"] = "mount ID unavailable: " + (str(error) or "cleanup deadline exceeded")
        if mount_id and getattr(args, "retain_mount", False) and result["completion_status"] == "success":
            result.update(cleanup_status="deferred", cleanup_deferred=True)
        elif mount_id:
            try:
                if not remaining(cleanup_deadline):
                    raise TimeoutError("mount deletion cleanup deadline exceeded")
                deletion = pool.submit(
                    http_json, args.api + "/mounts/" + mount_id, "DELETE",
                    timeout=remaining(cleanup_deadline),
                )
                deletion.result(timeout=remaining(cleanup_deadline))
                result["cleanup_status"] = "success"
            except Exception as error:
                result["cleanup_status"] = "failed"
                result["cleanup_error"] = str(error) or "mount deletion cleanup deadline exceeded"
        if listing is not None:
            listing.cancel()
        # Never extend either deadline with the executor context manager's wait.
        pool.shutdown(wait=False, cancel_futures=True)
        result["cleanup_elapsed_ms"] = round((time.monotonic() - cleanup_start) * 1000, 3)
    return finish_result(result)


def git_round(args, label):
    path = args.work / (label + "-git")
    log_path = args.work / (label + "-git.log")
    start = time.monotonic()
    result = round_result("git", label)
    env = os.environ.copy()
    for key in tuple(env):
        if key.lower() in {"http_proxy", "https_proxy", "all_proxy"}:
            env.pop(key)
    with log_path.open("wb") as log:
        process = None
        try:
            process = subprocess.Popen(
                ["git", "clone", "--depth", "1", args.repo, str(path)],
                stdout=log, stderr=subprocess.STDOUT, env=env,
            )
            deadline = start + args.timeout
            stamp = None
            while time.monotonic() < deadline:
                if process.poll() not in (None, 0):
                    raise RuntimeError("git clone exited with " + str(process.returncode))
                try:
                    stamp, names = listing_probe(
                        path, set(args.expected_dir), min(deadline, time.monotonic() + 0.2)
                    )
                    record_visibility(result, stamp, start, deadline, names)
                    break
                except TimeoutError:
                    pass
            if stamp is None:
                raise TimeoutError("git root directory visibility deadline exceeded")
            process.wait(timeout=remaining(deadline))
            result["clone_complete_ms"] = round((time.monotonic() - start) * 1000, 3)
            if process.returncode:
                raise RuntimeError("git clone exited with " + str(process.returncode))
            oid = subprocess.check_output(
                ["git", "-C", str(path), "rev-parse", "HEAD"], text=True,
                timeout=remaining(deadline),
            ).strip()
            result["commit"] = oid
            if oid != args.commit:
                raise RuntimeError("git clone tip changed: " + oid)
            result["completion_status"] = "success"
        except Exception as error:
            result["error"] = str(error) or "git completion deadline exceeded"
        finally:
            cleanup_start = time.monotonic()
            result["measurement_elapsed_ms"] = round((cleanup_start - start) * 1000, 3)
            if process is not None and process.poll() is None:
                try:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=10)
                    result["cleanup_status"] = "success"
                except Exception as error:
                    result["cleanup_status"] = "failed"
                    result["cleanup_error"] = str(error)
            result["cleanup_elapsed_ms"] = round((time.monotonic() - cleanup_start) * 1000, 3)
    return finish_result(result)


def git_tip(repo):
    advertisement = subprocess.check_output(
        ["git", "ls-remote", repo, "refs/heads/main"], text=True, timeout=30
    )
    return advertisement.split()[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", default="http://127.0.0.1:37251/antares")
    parser.add_argument("--repo", required=True)
    parser.add_argument("--scope", default="/project")
    parser.add_argument("--commit", required=True)
    parser.add_argument("--expected-dir", action="append", required=True)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--cleanup-timeout", type=float, default=30)
    parser.add_argument("--rounds", type=int, default=3)
    args = parser.parse_args()
    args.work.mkdir(parents=True, exist_ok=False)
    output = args.work / "first-directory.jsonl"
    failed = False
    with output.open("w", encoding="utf-8") as stream:
        for number in range(args.rounds):
            order = (scorpio_round, git_round) if number % 2 == 0 else (git_round, scorpio_round)
            for run in order:
                label = "round-" + str(number + 1)
                result = round_result("scorpiofs" if run is scorpio_round else "git", label)
                try:
                    if git_tip(args.repo) != args.commit:
                        raise RuntimeError("repository tip changed before measurement")
                except Exception as error:
                    result.update(ref_status="failed", ref_error=str(error), error=str(error))
                else:
                    result = run(args, label)
                    try:
                        if git_tip(args.repo) != args.commit:
                            raise RuntimeError("repository tip changed during measurement")
                        result["ref_status"] = "success"
                    except Exception as error:
                        result.update(ref_status="failed", ref_error=str(error))
                        result.setdefault("error", str(error))
                result["valid"] = (
                    bool(result.get("valid")) and result.get("ref_status") == "success"
                    and result.get("visibility_status") == "success"
                    and result.get("completion_status") == "success"
                    and result.get("cleanup_status") in {"success", "not_required"}
                )
                result["status"] = "success" if result["valid"] else "failed"
                line = json.dumps(result, ensure_ascii=False)
                print(line, flush=True)
                stream.write(line + "\n")
                stream.flush()
                failed |= not result["valid"]
                if failed:
                    raise SystemExit(1)
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
