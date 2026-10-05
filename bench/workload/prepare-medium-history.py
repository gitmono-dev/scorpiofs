#!/usr/bin/env python3
"""Commit actual file subsets in bounded batches, freezing 100k/200k refs."""
import argparse
import json
import os
from pathlib import Path
import subprocess


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--manifest", type=Path, required=True)
    p.add_argument("--batch", type=int, default=10000)
    args = p.parse_args()
    if (args.root / ".git").exists():
        raise SystemExit("refusing to overwrite existing history")
    env = dict(os.environ, GIT_AUTHOR_DATE="2026-10-03T00:00:00+0000",
               GIT_COMMITTER_DATE="2026-10-03T00:00:00+0000")

    def git(*argv, data=None):
        return subprocess.check_output(["git", "-C", str(args.root), *argv], input=data, env=env)

    git("init", "-b", "main")
    git("config", "user.name", "medium-benchmark")
    git("config", "user.email", "medium@bench.invalid")
    rows = [json.loads(line) for line in args.manifest.open()]
    refs = []
    for offset in range(0, len(rows), args.batch):
        batch = rows[offset:offset + args.batch]
        git("add", "--pathspec-from-file=-", "--pathspec-file-nul",
            data=b"".join(row["path"].encode() + b"\0" for row in batch))
        count = offset + len(batch)
        git("commit", "-m", f"medium fixture {count} files")
        ref = git("rev-parse", "HEAD").decode().strip()
        refs.append({"files": count, "commit": ref})
        if count in (100000, 200000):
            git("tag", f"medium-{count}")
        print(json.dumps(refs[-1]), flush=True)
    git("gc", "--prune=now")
    git("fsck", "--full")
    args.manifest.with_suffix(".refs.json").write_text(json.dumps(refs, indent=2) + "\n")


if __name__ == "__main__":
    main()
