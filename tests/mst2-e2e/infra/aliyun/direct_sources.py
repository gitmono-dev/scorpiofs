"""Transport exact shallow Git objects, without credentials or ancestor history."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile

MAX_BUNDLE = 512 * 1024 * 1024
MAX_EXPANDED = 2 * 1024 * 1024 * 1024
LABELS = {"scorpiofs", "mega2", "client-a", "client-b"}


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def git(root, *arguments, data=None):
    result = subprocess.run(["git", "-C", str(root), *arguments], input=data, capture_output=True, timeout=120)
    if result.returncode:
        raise ValueError("EXACT_SOURCE_GIT_FAILED")
    return result.stdout


def create(sources, archive):
    if set(sources) != LABELS:
        raise ValueError("EXACT_FOUR_SOURCES_REQUIRED")
    archive = Path(archive)
    if archive.exists():
        raise ValueError("FRESH_SOURCE_BUNDLE_REQUIRED")
    with tempfile.TemporaryDirectory() as temporary:
        folder = Path(temporary)
        entries = {}
        for label in sorted(sources):
            source = sources[label]
            root, commit = Path(source["path"]), source["sha"]
            if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
                raise ValueError("IMMUTABLE_SOURCE_REQUIRED")
            tree = git(root, "rev-parse", commit + "^{tree}").decode().strip()
            objects = {commit, tree}
            for row in git(root, "ls-tree", "-r", "-t", "-z", commit).split(b"\0"):
                if row:
                    mode, kind, oid = row.split(b"\t", 1)[0].split()
                    if kind not in (b"tree", b"blob"):
                        raise ValueError("SUBMODULE_SOURCE_NOT_SUPPORTED")
                    objects.add(oid.decode())
            pack = folder / (label + ".pack")
            with pack.open("xb") as output:
                result = subprocess.run(["git", "-C", str(root), "pack-objects", "--stdout"],
                    input=("\n".join(sorted(objects)) + "\n").encode(), stdout=output,
                    stderr=subprocess.PIPE, timeout=300)
            if result.returncode or pack.stat().st_size > MAX_BUNDLE:
                raise ValueError("SOURCE_PACK_FAILED_OR_TOO_LARGE")
            entries[label] = {"sha": commit, "tree": tree, "pack_sha256": digest(pack), "pack_bytes": pack.stat().st_size}
        # This root bootstrap helper is the exact file included in the pinned harness.
        helper = git(Path(sources["scorpiofs"]["path"]), "show",
            sources["scorpiofs"]["sha"] + ":tests/mst2-e2e/infra/aliyun/direct_sources.py")
        (folder / "bootstrap.py").write_bytes(helper)
        manifest = {"revision": 1, "sources": entries, "bootstrap_sha256": hashlib.sha256(helper).hexdigest()}
        (folder / "manifest.json").write_text(json.dumps(manifest, sort_keys=True) + "\n", encoding="utf-8")
        with tarfile.open(archive, "w:gz") as stream:
            for path in sorted(folder.iterdir()):
                stream.add(path, arcname=path.name, recursive=False)
    if archive.stat().st_size > MAX_BUNDLE:
        raise ValueError("SOURCE_BUNDLE_TOO_LARGE")
    return {"sha256": digest(archive), "bytes": archive.stat().st_size,
            "bootstrap_sha256": manifest["bootstrap_sha256"], "sources": entries}


def restore(archive, output, expected):
    archive, output = Path(archive), Path(output)
    if output.exists() or archive.stat().st_size > MAX_BUNDLE:
        raise ValueError("FRESH_BOUNDED_SOURCE_DESTINATION_REQUIRED")
    if set(expected) != LABELS or any(re.fullmatch(r"[0-9a-f]{40}", sha) is None for sha in expected.values()):
        raise ValueError("EXACT_SOURCE_PINS_REQUIRED")
    output.mkdir(parents=True, mode=0o750)
    with tempfile.TemporaryDirectory(dir=output.parent) as temporary, tarfile.open(archive, "r:gz") as bundle:
        members = bundle.getmembers()
        names = {label + ".pack" for label in LABELS} | {"manifest.json", "bootstrap.py"}
        if (len(members) != len(names) or {member.name for member in members} != names
                or any(not member.isfile() or member.size > MAX_BUNDLE for member in members)
                or sum(member.size for member in members) > MAX_EXPANDED):
            raise ValueError("INVALID_SOURCE_BUNDLE_MEMBERS")
        raw = bundle.extractfile("manifest.json").read(65537)
        if len(raw) > 65536:
            raise ValueError("SOURCE_MANIFEST_TOO_LARGE")
        manifest = json.loads(raw)
        if set(manifest) != {"revision", "sources", "bootstrap_sha256"} or manifest["revision"] != 1 or set(manifest["sources"]) != LABELS:
            raise ValueError("INVALID_SOURCE_MANIFEST")
        for label in sorted(LABELS):
            entry = manifest["sources"][label]
            if (set(entry) != {"sha", "tree", "pack_sha256", "pack_bytes"} or entry["sha"] != expected[label]
                    or re.fullmatch(r"[0-9a-f]{40}", entry["tree"]) is None):
                raise ValueError("SOURCE_IDENTITY_MISMATCH")
            pack = Path(temporary) / (label + ".pack")
            with bundle.extractfile(label + ".pack") as source, pack.open("xb") as target:
                while chunk := source.read(1024 * 1024):
                    target.write(chunk)
            if pack.stat().st_size != entry["pack_bytes"] or digest(pack) != entry["pack_sha256"]:
                raise ValueError("SOURCE_PACK_MISMATCH")
            repo = output / label
            subprocess.run(["git", "init", "-q", "-b", "main", str(repo)], check=True, timeout=30)
            with pack.open("rb") as source:
                process = subprocess.run(["git", "-C", str(repo), "index-pack", "--stdin"],
                    stdin=source, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120)
            if process.returncode:
                raise ValueError("SOURCE_PACK_INDEX_FAILED")
            # The raw signed commit and every tree/blob are real. Only its ancestors
            # are omitted, using Git's ordinary shallow boundary representation.
            (repo / ".git/shallow").write_text(entry["sha"] + "\n", encoding="ascii")
            git(repo, "config", "core.autocrlf", "false")
            git(repo, "update-ref", "refs/heads/main", entry["sha"])
            git(repo, "reset", "--hard", entry["sha"])
            if (git(repo, "rev-parse", "HEAD^{tree}").decode().strip() != entry["tree"]
                    or git(repo, "status", "--porcelain").strip()):
                raise ValueError("RESTORED_SOURCE_NOT_EXACT_OR_CLEAN")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--pins", required=True)
    args = parser.parse_args()
    restore(args.bundle, args.output, json.loads(args.pins))
