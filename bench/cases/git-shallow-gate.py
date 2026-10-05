#!/usr/bin/env python3
"""Check a depth-limited clone against a fixed local Git oracle; never push."""
import argparse
import json
import subprocess
import tempfile
from pathlib import Path


def verify_clone(clone, oracle, commit="HEAD", depth=1, timeout=300):
    def git(path, *args):
        return subprocess.check_output(
            ["git", "-C", str(path), *args], timeout=timeout,
        ).decode().strip()

    def require(condition, message):
        if not condition:
            raise RuntimeError(message)

    expected = git(oracle, "rev-parse", commit)
    tree = git(oracle, "rev-parse", commit + "^{tree}")
    ancestry = git(oracle, "rev-list", "--parents", expected).splitlines()
    require(all(len(row.split()) <= 2 for row in ancestry),
            "the benchmark gate requires a linear oracle history")
    history = [row.split()[0] for row in ancestry]
    included = history[:depth]
    allowed = set(included)
    for oid in included:
        allowed.add(git(oracle, "rev-parse", oid + "^{tree}"))
        entries = git(oracle, "ls-tree", "-r", "-t", "-z", oid)
        allowed.update(entry.split("\t", 1)[0].split()[2]
                       for entry in entries.split("\0") if entry)
    actual_objects = set(git(clone, "cat-file", "--batch-all-objects",
                             "--batch-check=%(objectname)").splitlines())
    result = dict(
        head=git(clone, "rev-parse", "HEAD"),
        tree=git(clone, "rev-parse", "HEAD^{tree}"),
        shallow=git(clone, "rev-parse", "--is-shallow-repository"),
        commits=git(clone, "rev-list", "HEAD").splitlines(),
        files=len(git(clone, "ls-tree", "-r", "-z", "HEAD").split("\0")) - 1,
        pack=git(clone, "count-objects", "-v"),
        expected_object_count=len(allowed), actual_object_count=len(actual_objects),
    )
    require(result["head"] == expected, "HEAD differs from the fixed oracle")
    require(result["tree"] == tree, "file contents or modes differ from the oracle")
    require(result["commits"] == included, "clone includes the wrong commit depth")
    shallow_file = Path(git(clone, "rev-parse", "--path-format=absolute", "--git-path", "shallow"))
    boundaries = set(shallow_file.read_text().splitlines()) if shallow_file.exists() else set()
    if len(history) > depth:
        require(result["shallow"] == "true" and boundaries == {included[-1]},
                "incorrect shallow boundary")
    else:
        # Native Git can mark the actual root shallow when depth equals the
        # complete history length. That cuts no parent edges and is valid.
        roots = {row for row in ancestry if len(row.split()) == 1}
        require(boundaries <= roots, "incorrect shallow boundary in complete history")
    result["boundaries"] = sorted(boundaries)
    require(actual_objects == allowed,
            f"object set differs from oracle: {len(actual_objects - allowed)} extra, "
            f"{len(allowed - actual_objects)} missing")
    git(clone, "diff", "--exit-code", "HEAD")
    git(clone, "fsck", "--no-reflogs")
    result["strict_object_set_verified"] = True
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo", required=True)
    p.add_argument("--oracle", type=Path, required=True)
    p.add_argument("--commit", default="HEAD", help="Fixed oracle ref to verify")
    p.add_argument("--protocol", type=int, choices=(0, 2), default=2)
    p.add_argument("--depth", type=int, default=1)
    p.add_argument("--timeout", type=int, default=300)
    p.add_argument("--out", type=Path, required=True)
    a = p.parse_args()
    if a.depth < 1 or a.timeout < 1:
        p.error("depth and timeout must be positive")
    if a.out.exists():
        p.error("preserve existing gate evidence")
    result = {"repo": a.repo, "protocol": a.protocol, "depth": a.depth, "status": "running"}

    try:
        with tempfile.TemporaryDirectory(prefix="git-shallow-gate-") as temp:
            clone = Path(temp) / "clone"
            subprocess.run(
                ["git", "-c", f"protocol.version={a.protocol}", "clone", "--depth",
                 str(a.depth), a.repo, str(clone)], check=True, timeout=a.timeout,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            result.update(verify_clone(clone, a.oracle, a.commit, a.depth, a.timeout))
            result["status"] = "success"
    except Exception as error:
        result.update(status="failed", error=str(error))
        if isinstance(error, subprocess.CalledProcessError) and error.stderr:
            result["stderr"] = error.stderr.decode(errors="replace")
        raise
    finally:
        a.out.parent.mkdir(parents=True, exist_ok=True)
        a.out.write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
