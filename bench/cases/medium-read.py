#!/usr/bin/env python3
"""Measure verified first/repeated reads on fresh MST/2 mounts and Git checkouts."""

import argparse
import hashlib
import json
from pathlib import Path
import random
import subprocess
import sys
import time
import urllib.request
import uuid


def http_json(url, body=None, method="GET"):
    request = urllib.request.Request(
        url, data=None if body is None else json.dumps(body).encode(),
        method=method, headers={"Content-Type": "application/json"},
    )
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(
        request, timeout=60
    ) as response:
        raw = response.read()
        return json.loads(raw) if raw else None


def read_worker(root, sample_file):
    paths = json.loads(Path(sample_file).read_text())["paths"]
    start = time.monotonic_ns()
    payloads = [(Path(root) / path).read_bytes() for path in paths]
    elapsed_ms = (time.monotonic_ns() - start) / 1_000_000
    # Hash after timing; retain bytes so verification does not reread the files.
    print(json.dumps({"elapsed_ms": elapsed_ms,
                      "bytes": sum(map(len, payloads)),
                      "hashes": [hashlib.sha256(body).hexdigest() for body in payloads]}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api", default="http://127.0.0.1:37255/antares")
    parser.add_argument("--repo", required=True, help="Git URL for fixed-ref checks")
    parser.add_argument("--scope", default="/project/bench50k")
    parser.add_argument("--commit", required=True)
    parser.add_argument("--work", type=Path, required=True,
                        help="directory containing round-N-git from readiness runs")
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--count", type=int, default=100)
    args = parser.parse_args()
    sample_file = args.work / "read-sample.json"
    output = args.work / "read-results.jsonl"
    if sample_file.exists() or output.exists():
        parser.error("read outputs already exist; keep previous samples intact")
    names = subprocess.check_output(
        ["git", "-C", str(args.work / "round-1-git"), "ls-files", "-z"]
    ).decode().rstrip("\0").split("\0")
    paths = sorted(random.Random(20261003).sample(names, args.count))
    sample_file.write_text(json.dumps({"seed": 20261003, "paths": paths}, indent=2) + "\n")

    def verify_ref(root):
        commit = subprocess.check_output(
            ["git", "-C", str(root), "rev-parse", "HEAD"], text=True, timeout=10
        ).strip()
        count = len(subprocess.check_output(
            ["git", "-C", str(root), "ls-files", "-z"], timeout=10
        ).rstrip(b"\0").split(b"\0"))
        remote = subprocess.check_output(
            ["git", "ls-remote", args.repo, "refs/heads/main"], text=True, timeout=30
        ).split()[0]
        if commit != args.commit or remote != args.commit or count != len(names):
            raise RuntimeError("fixed commit or tracked-file count changed")

    def read(root):
        return json.loads(subprocess.check_output(
            [sys.executable, __file__, "--worker", str(root), str(sample_file)],
            text=True, timeout=60,
        ))

    failed = False
    with output.open("x") as stream:
        for number in range(1, args.rounds + 1):
            git_root = args.work / ("round-" + str(number) + "-git")
            verify_ref(git_root)
            # Build the oracle from an earlier fixed checkout, outside both timers.
            expected = [hashlib.sha256((args.work / "round-1-git" / path).read_bytes())
                        .hexdigest() for path in paths]
            order = ["scorpiofs", "git"] if number % 2 else ["git", "scorpiofs"]
            for backend in order:
                created = None
                record = {"round": number, "backend": backend, "status": "failed",
                          "file_count": len(paths), "tracked_files": len(names),
                          "commit": args.commit, "cache_condition": "existing_server_and_os_cache"}
                try:
                    if backend == "scorpiofs":
                        created = http_json(args.api + "/mounts", {
                            "path": args.scope, "job_id": "read100-" + str(uuid.uuid4()),
                        }, "POST")
                        root = Path(created["mountpoint"])
                        record["first_condition"] = "first_access_in_fresh_mount"
                    else:
                        root = git_root
                        record["first_condition"] = "first_timed_access_after_clone"
                    first, repeat = read(root), read(root)
                    for phase, result in [("first", first), ("repeat", repeat)]:
                        record[phase + "_ms"] = round(result["elapsed_ms"], 3)
                        record[phase + "_hashes"] = result["hashes"]
                        if result["hashes"] != expected:
                            raise AssertionError(phase + " content differs from Git oracle")
                    record.update(status="success", bytes=first["bytes"],
                                  content_verified=True)
                    verify_ref(git_root)
                except Exception as error:
                    record.update(status="failed", error=str(error))
                finally:
                    if created:
                        try:
                            http_json(args.api + "/mounts/" + created["mount_id"], method="DELETE")
                            record["cleanup_status"] = "success"
                        except Exception as error:
                            record.update(status="failed", cleanup_status="failed",
                                          cleanup_error=str(error))
                    line = json.dumps(record)
                    print(line, flush=True)
                    stream.write(line + "\n")
                    stream.flush()
                failed |= record["status"] != "success"
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--worker":
        read_worker(sys.argv[2], sys.argv[3])
    else:
        main()
