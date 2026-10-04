#!/usr/bin/env python3
"""Publish a smaller audited fixture tree as a forward commit in the isolated run."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--files", type=int, required=True)
p.add_argument("--cluster", required=True)
a = p.parse_args()
repo = "/fixture/repo.git"
remote = "http://mega2:8000/project"
refs_path = Path("/fixture/manifest.refs.json")
refs = json.loads(refs_path.read_text())
env = dict(os.environ, GIT_AUTHOR_NAME="medium-benchmark", GIT_AUTHOR_EMAIL="medium@bench.invalid",
           GIT_COMMITTER_NAME="medium-benchmark", GIT_COMMITTER_EMAIL="medium@bench.invalid",
           GIT_TERMINAL_PROMPT="0")
for name in tuple(env):
    if name.lower() in ("http_proxy", "https_proxy", "all_proxy"):
        del env[name]

def git(*argv, input=None):
    return subprocess.check_output(["git", "--git-dir="+repo, *argv], input=input, env=env).decode().strip()

parent = git("ls-remote", remote, "refs/heads/main").split()[0]
old = next(r for r in refs if r["commit"] == parent)
if old["files"] <= a.files:
    raise SystemExit("reduction requires a known larger current tip")
row = next(r for r in refs if r["files"] == a.files)
tree = git("rev-parse", row["commit"]+"^{tree}")
if len(git("ls-tree", "-r", "--name-only", tree).splitlines()) != a.files:
    raise SystemExit("fixture tree count mismatch")
backup = refs_path.with_name("manifest.refs.before-reduction.json")
if backup.exists():
    raise SystemExit("preserve existing reduction evidence")
backup.write_text(refs_path.read_text())
commit = git("commit-tree", tree, "-p", parent, input=f"Reduce isolated benchmark to {a.files} files\n".encode())
start = time.monotonic()
git("-c", "http.postBuffer=1073741824", "push", remote, commit+":refs/heads/main")
git("fetch", remote, "refs/heads/main")
canonical = git("rev-parse", "FETCH_HEAD")
if git("rev-parse", canonical+"^{tree}") != tree:
    raise SystemExit("server content tree differs from reduction fixture")
if git("ls-remote", remote, "refs/heads/main").split()[0] != canonical:
    raise SystemExit("new ref mismatch")
row.update(previous_fixture_commit=row["commit"], commit=canonical, tree=tree, reduced_from_commit=parent)
refs_path.write_text(json.dumps(refs, indent=2)+"\n")
result = {"cluster": a.cluster, "files": a.files, "parent": parent, "commit": canonical,
          "client_commit": commit,
          "tree": tree, "push_ms": (time.monotonic()-start)*1000,
          "forward_history": True, "fixture_content_unchanged": True, "utc": time.time()}
Path("/data/results/reduction.json").write_text(json.dumps(result, indent=2)+"\n")
print(json.dumps(result), flush=True)
