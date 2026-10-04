#!/usr/bin/env python3
"""Check a depth-limited clone against a fixed local Git oracle; never push."""
import argparse
import json
import subprocess
import tempfile
from pathlib import Path


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo", required=True)
    p.add_argument("--oracle", type=Path, required=True)
    p.add_argument("--protocol", type=int, choices=(0, 2), default=2)
    p.add_argument("--depth", type=int, default=1)
    p.add_argument("--timeout", type=int, default=300)
    p.add_argument("--out", type=Path, required=True)
    a = p.parse_args()
    if a.depth < 1 or a.timeout < 1:
        p.error("depth and timeout must be positive")
    if a.out.exists():
        p.error("preserve existing gate evidence")
    result = {"protocol": a.protocol, "depth": a.depth, "status": "running"}

    def git(path, *args):
        return subprocess.check_output(
            ["git", "-C", str(path), *args], timeout=a.timeout,
        ).decode().strip()

    try:
        expected = git(a.oracle, "rev-parse", "HEAD")
        tree = git(a.oracle, "rev-parse", "HEAD^{tree}")
        history = git(a.oracle, "rev-list", "HEAD").splitlines()
        with tempfile.TemporaryDirectory(prefix="git-shallow-gate-") as temp:
            clone = Path(temp) / "clone"
            subprocess.run(
                ["git", "-c", f"protocol.version={a.protocol}", "clone", "--depth",
                 str(a.depth), a.repo, str(clone)], check=True, timeout=a.timeout,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            result.update(
                head=git(clone, "rev-parse", "HEAD"),
                tree=git(clone, "rev-parse", "HEAD^{tree}"),
                shallow=git(clone, "rev-parse", "--is-shallow-repository"),
                commits=git(clone, "rev-list", "HEAD").splitlines(),
                files=len(git(clone, "ls-tree", "-r", "--name-only", "HEAD").splitlines()),
                pack=git(clone, "count-objects", "-v"),
            )
            assert result["head"] == expected, "HEAD differs from the fixed oracle"
            assert result["tree"] == tree, "file contents or modes differ from the oracle"
            assert result["commits"] == history[:a.depth], "clone includes the wrong commit depth"
            assert result["shallow"] == str(len(history) > a.depth).lower(), "incorrect shallow boundary"
            if len(history) > a.depth:
                absent = subprocess.run(
                    ["git", "-C", str(clone), "cat-file", "-e", history[a.depth]],
                    timeout=a.timeout, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                )
                assert absent.returncode != 0, "pack includes an excluded ancestor commit"
            git(clone, "diff", "--exit-code", "HEAD")
            git(clone, "fsck", "--no-reflogs")
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
