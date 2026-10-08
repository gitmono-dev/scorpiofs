"""Transport exact sources and complete pinned dependency Git histories."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
import time

MAX_BUNDLE = 512 * 1024 * 1024
MAX_EXPANDED = 2 * 1024 * 1024 * 1024
LABELS = {"scorpiofs", "mega2", "client-a", "client-b"}
DEPENDENCIES = {
    "rk8s": {"url": "https://github.com/rk8s-dev/rk8s",
             "sha": "01dee0288279ee9cd885c2a0714fed421e735cff",
             "tree": "2f1367b210779c21a7fb991297b6c0217ee49323"},
    "mst2-codec": {"url": "https://github.com/gitmono-dev/mst2-codec",
                   "sha": "dc5ec7650a221ec07a62f4dea4be9ddf17ad924e",
                   "tree": "79aa9b7c05a1499fd7d6a4fab34e47818bc3879f"},
}
HISTORY_MODE = "complete-ancestor-closure"
MAX_DEPENDENCY_OBJECTS = 200000
MAX_DEPENDENCY_COMMITS = 10000
MAX_DEPENDENCY_OBJECT_BYTES = 2 * 1024 * 1024 * 1024
DEPENDENCY_RECEIPT_FILE = "dependency-git/receipt.json"
PERFORMANCE_FILE = "source-git-performance.json"
PERFORMANCE_LIMIT = 65536
RESOURCE_FIELDS = {"cpu_user_ms", "cpu_system_ms", "peak_rss_bytes", "io_read_bytes", "io_write_bytes"}


def _operations(phase):
    if phase == "pack":
        return ([(label, operation) for label in sorted(LABELS)
                 for operation in ("rev-parse", "ls-tree", "pack-objects")]
                + [(label, operation) for label in sorted(DEPENDENCIES)
                   for operation in ("rev-parse", "rev-parse", "ls-tree", "rev-list", "cat-file", "pack-objects")]
                + [("scorpiofs", "show")])
    if phase == "restore":
        return ([(label, operation) for label in sorted(LABELS)
                 for operation in ("init", "index-pack", "config", "update-ref", "reset", "rev-parse", "status")]
                + [(label, operation) for label in sorted(DEPENDENCIES)
                   for operation in ("init", "index-pack", "update-ref", "rev-parse", "rev-parse", "fsck", "rev-list", "cat-file")])
    raise ValueError("INVALID_SOURCE_PERFORMANCE")


def validate_performance(value, *, phase=None, require_success=False):
    """Validate a closed receipt without accepting command text or private data."""
    fields = {"revision", "scope", "phase", "status", "resource_metrics_status", "operations", *RESOURCE_FIELDS}
    if (type(value) is not dict or set(value) != fields or type(value["revision"]) is not int
            or value["revision"] != 1 or value["scope"] != "source-transport"
            or value["phase"] not in ("pack", "restore") or phase is not None and value["phase"] != phase
            or value["status"] not in ("success", "failed")
            or require_success and value["status"] != "success"
            or value["resource_metrics_status"] != "unavailable"
            or any(value[name] is not None for name in RESOURCE_FIELDS)):
        raise ValueError("INVALID_SOURCE_PERFORMANCE")
    expected, rows = _operations(value["phase"]), value["operations"]
    if type(rows) is not list or len(rows) > len(expected):
        raise ValueError("INVALID_SOURCE_PERFORMANCE")
    for index, row in enumerate(rows):
        if (type(row) is not dict or set(row) != {"source", "operation", "wall_ms", "status", "exit_status"}
                or (row["source"], row["operation"]) != expected[index]
                or type(row["wall_ms"]) not in (int, float) or not math.isfinite(row["wall_ms"])
                or row["wall_ms"] < 0 or row["status"] not in ("success", "nonzero_exit", "timeout", "spawn_failed")):
            raise ValueError("INVALID_SOURCE_PERFORMANCE")
        status, code = row["status"], row["exit_status"]
        if ((status == "success" and (type(code) is not int or code != 0))
                or (status == "nonzero_exit" and (type(code) is not int or code == 0))
                or (status in ("timeout", "spawn_failed") and code is not None)
                or status != "success" and index != len(rows) - 1):
            raise ValueError("INVALID_SOURCE_PERFORMANCE")
    if value["status"] == "success" and (len(rows) != len(expected)
            or any(row["status"] != "success" for row in rows)):
        raise ValueError("INVALID_SOURCE_PERFORMANCE")
    return value


class SourceGitFailure(ValueError):
    """Fixed failure text and numeric evidence; no argv, environment or output."""

    def __init__(self, code, performance):
        self.source_git_performance = performance
        super().__init__(code)


class _TransportPerformance:
    """Standard-library only: this module runs before the harness is restored."""

    def __init__(self, phase):
        self.phase, self.operations = phase, []

    def receipt(self, success):
        return validate_performance({"revision": 1, "scope": "source-transport", "phase": self.phase,
            "status": "success" if success else "failed", "resource_metrics_status": "unavailable",
            **dict.fromkeys(RESOURCE_FIELDS), "operations": list(self.operations)})

    def run(self, operation, source, argv, *, failure_code="EXACT_SOURCE_GIT_FAILED", **options):
        index = len(self.operations)
        if index >= len(_operations(self.phase)) or (source, operation) != _operations(self.phase)[index]:
            raise ValueError("INVALID_SOURCE_PERFORMANCE_OPERATION")
        started, status, code = time.perf_counter_ns(), "spawn_failed", None
        try:
            result = subprocess.run(argv, **options)
            code = result.returncode
            status = "success" if code == 0 else "nonzero_exit"
        except subprocess.TimeoutExpired:
            status = "timeout"
        except OSError:
            pass
        finally:
            self.operations.append({"source": source, "operation": operation,
                "wall_ms": (time.perf_counter_ns() - started) / 1_000_000,
                "status": status, "exit_status": code})
        if status != "success":
            raise SourceGitFailure(failure_code, self.receipt(False)) from None
        return result


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def git(root, *arguments, data=None, performance=None, source=None):
    argv = ["git", "-C", str(root), *arguments]
    result = (subprocess.run(argv, input=data, capture_output=True, timeout=120) if performance is None else
              performance.run(arguments[0], source, argv, input=data, capture_output=True, timeout=120))
    if result.returncode:
        raise ValueError("EXACT_SOURCE_GIT_FAILED")
    return result.stdout


def dependency_repository(output, label):
    if label not in DEPENDENCIES:
        raise ValueError("INVALID_DEPENDENCY_LABEL")
    return Path(output) / "dependency-git" / (label + ".git")


def dependency_sources(value):
    if (type(value) is not dict or set(value) != set(DEPENDENCIES)
            or any(type(path) is not str or not Path(path).is_absolute() for path in value.values())):
        raise ValueError("EXACT_ABSOLUTE_DEPENDENCY_SOURCES_REQUIRED")
    return dict(value)


def _hex(value, length):
    return type(value) is str and re.fullmatch(r"[0-9a-f]{" + str(length) + "}", value) is not None


def _pack_entry(entry):
    return (type(entry) is dict and _hex(entry.get("sha"), 40) and _hex(entry.get("tree"), 40)
            and _hex(entry.get("pack_sha256"), 64) and type(entry.get("pack_bytes")) is int
            and 0 < entry["pack_bytes"] <= MAX_BUNDLE)


def validate_manifest(manifest, expected=None):
    """Closed, path-free identities and bounded object inventories."""
    if (type(manifest) is not dict or set(manifest) != {"revision", "sources", "dependencies", "bootstrap_sha256"}
            or type(manifest["revision"]) is not int or manifest["revision"] != 2
            or not _hex(manifest["bootstrap_sha256"], 64)
            or type(manifest["sources"]) is not dict or set(manifest["sources"]) != LABELS
            or type(manifest["dependencies"]) is not dict or set(manifest["dependencies"]) != set(DEPENDENCIES)):
        raise ValueError("INVALID_SOURCE_MANIFEST")
    for label, entry in manifest["sources"].items():
        if (not _pack_entry(entry) or set(entry) != {"sha", "tree", "pack_sha256", "pack_bytes"}
                or expected is not None and entry["sha"] != expected[label]):
            raise ValueError("SOURCE_IDENTITY_MISMATCH")
    validate_dependencies(manifest["dependencies"])
    return manifest


def validate_dependencies(value):
    if type(value) is not dict or set(value) != set(DEPENDENCIES):
        raise ValueError("EXACT_PINNED_DEPENDENCIES_REQUIRED")
    fields = {"url", "sha", "tree", "pack_sha256", "pack_bytes", "history_mode",
              "object_count", "object_bytes", "commit_count"}
    for label, entry in value.items():
        if (not _pack_entry(entry) or set(entry) != fields
                or any(entry[key] != value for key, value in DEPENDENCIES[label].items())
                or entry["history_mode"] != HISTORY_MODE
                or any(type(entry[key]) is not int or not 0 < entry[key] <= cap
                       for key, cap in (("object_count", MAX_DEPENDENCY_OBJECTS),
                                        ("object_bytes", MAX_DEPENDENCY_OBJECT_BYTES),
                                        ("commit_count", MAX_DEPENDENCY_COMMITS)))
                or entry["commit_count"] > entry["object_count"]):
            raise ValueError("DEPENDENCY_IDENTITY_OR_INVENTORY_MISMATCH")
    if sum(entry["object_bytes"] for entry in value.values()) > MAX_DEPENDENCY_OBJECT_BYTES:
        raise ValueError("DEPENDENCY_OBJECTS_TOO_LARGE")
    return value


def validate_dependency_receipt(value, workspace, expected_dependencies):
    if (type(value) is not dict or set(value) != {"revision", "dependencies"}
            or type(value["revision"]) is not int or value["revision"] != 1):
        raise ValueError("INVALID_DEPENDENCY_RECEIPT")
    validate_dependencies(value["dependencies"])
    validate_dependencies(expected_dependencies)
    if value["dependencies"] != expected_dependencies:
        raise ValueError("DEPENDENCY_RECEIPT_BINDING_MISMATCH")
    workspace = Path(workspace)
    paths = [workspace, workspace / "dependency-git"]
    receipt = workspace / DEPENDENCY_RECEIPT_FILE
    if receipt.is_symlink() or not receipt.is_file() or receipt.stat().st_size > 65536:
        raise ValueError("INVALID_DEPENDENCY_RECEIPT_FILE")
    for label in DEPENDENCIES:
        repo = dependency_repository(workspace, label)
        paths.extend([repo, repo / "objects", repo / "refs"])
        if (repo / "shallow").exists() or (repo / "shallow").is_symlink():
            raise ValueError("COMPLETE_DEPENDENCY_HISTORY_REQUIRED")
        for name in ("HEAD", "config"):
            if (repo / name).is_symlink() or not (repo / name).is_file():
                raise ValueError("INVALID_BARE_DEPENDENCY_LAYOUT")
    if any(path.is_symlink() or not path.is_dir() for path in paths):
        raise ValueError("INVALID_BARE_DEPENDENCY_LAYOUT")
    return value


def _tree_objects(root, commit, performance, label):
    objects = {commit}
    for row in git(root, "ls-tree", "-r", "-t", "-z", commit,
                   performance=performance, source=label).split(b"\0"):
        if row:
            mode, kind, oid = row.split(b"\t", 1)[0].split()
            if kind not in (b"tree", b"blob") or mode == b"160000":
                raise ValueError("SUBMODULE_SOURCE_NOT_SUPPORTED")
            objects.add(oid.decode("ascii"))
    return objects


def _inventory(root, commit, performance, label, folder):
    listing, sizes = folder / (label + ".objects"), folder / (label + ".sizes")
    with listing.open("xb") as output:
        performance.run("rev-list", label, ["git", "-C", str(root), "rev-list", "--objects", "--no-object-names", commit],
                        stdout=output, stderr=subprocess.PIPE, timeout=120)
    if listing.stat().st_size > MAX_DEPENDENCY_OBJECTS * 41:
        raise ValueError("DEPENDENCY_OBJECT_COUNT_EXCEEDED")
    objects = listing.read_bytes().splitlines()
    if (not objects or len(objects) > MAX_DEPENDENCY_OBJECTS or len(set(objects)) != len(objects)
            or any(re.fullmatch(rb"[0-9a-f]{40}", oid) is None for oid in objects)):
        raise ValueError("INVALID_DEPENDENCY_OBJECTS")
    with sizes.open("xb") as output:
        performance.run("cat-file", label,
                        ["git", "-C", str(root), "cat-file", "--batch-check=%(objectname) %(objecttype) %(objectsize)"],
                        input=b"\n".join(objects) + b"\n", stdout=output, stderr=subprocess.PIPE, timeout=120)
    if sizes.stat().st_size > MAX_DEPENDENCY_OBJECTS * 80:
        raise ValueError("DEPENDENCY_OBJECT_INVENTORY_TOO_LARGE")
    rows = sizes.read_bytes().splitlines()
    if len(rows) != len(objects):
        raise ValueError("INVALID_DEPENDENCY_OBJECT_INVENTORY")
    total, commits, pinned_commit = 0, 0, False
    for oid, row in zip(objects, rows):
        fields = row.split()
        if (len(fields) != 3 or fields[0] != oid or fields[1] not in (b"commit", b"tree", b"blob")
                or re.fullmatch(rb"[0-9]{1,20}", fields[2]) is None):
            raise ValueError("INVALID_DEPENDENCY_OBJECT_INVENTORY")
        total += int(fields[2])
        commits += fields[1] == b"commit"
        pinned_commit |= oid.decode("ascii") == commit and fields[1] == b"commit"
        if total > MAX_DEPENDENCY_OBJECT_BYTES or commits > MAX_DEPENDENCY_COMMITS:
            raise ValueError("DEPENDENCY_OBJECTS_TOO_LARGE")
    if not pinned_commit:
        raise ValueError("PINNED_DEPENDENCY_COMMIT_REQUIRED")
    listing.unlink()
    sizes.unlink()
    return objects, {"object_count": len(objects), "object_bytes": total, "commit_count": commits}


def _pack(root, label, objects, folder, performance):
    path = folder / (label + ".pack")
    with path.open("xb") as output:
        performance.run("pack-objects", label, ["git", "-C", str(root), "pack-objects", "--stdout"],
                        input=b"\n".join(sorted(objects)) + b"\n", stdout=output,
                        stderr=subprocess.PIPE, timeout=300, failure_code="SOURCE_PACK_FAILED_OR_TOO_LARGE")
    if path.stat().st_size > MAX_BUNDLE:
        raise ValueError("SOURCE_PACK_FAILED_OR_TOO_LARGE")
    return {"pack_sha256": digest(path), "pack_bytes": path.stat().st_size}


def create(sources, archive, dependencies):
    performance = _TransportPerformance("pack")
    try:
        receipt = _create(sources, archive, dependency_sources(dependencies), performance)
    except Exception as error:
        error.source_git_performance = performance.receipt(False)
        raise
    receipt["source_pack_performance"] = performance.receipt(True)
    return receipt


def _create(sources, archive, dependencies, performance):
    if type(sources) is not dict or set(sources) != LABELS:
        raise ValueError("EXACT_FOUR_SOURCES_REQUIRED")
    archive = Path(archive)
    if archive.exists():
        raise ValueError("FRESH_SOURCE_BUNDLE_REQUIRED")
    with tempfile.TemporaryDirectory() as temporary:
        folder = Path(temporary)
        entries = {}
        for label in sorted(sources):
            source = sources[label]
            if (type(source) is not dict or set(source) != {"path", "sha"}
                    or type(source["path"]) is not str or not _hex(source["sha"], 40)):
                raise ValueError("IMMUTABLE_SOURCE_REQUIRED")
            root, commit = Path(source["path"]), source["sha"]
            tree = git(root, "rev-parse", commit + "^{tree}", performance=performance, source=label).decode().strip()
            objects = _tree_objects(root, commit, performance, label) | {tree}
            entries[label] = {"sha": commit, "tree": tree,
                              **_pack(root, label, [oid.encode("ascii") for oid in objects], folder, performance)}
        dependency_entries = {}
        for label in sorted(DEPENDENCIES):
            root, identity = Path(dependencies[label]), DEPENDENCIES[label]
            if git(root, "rev-parse", "--is-shallow-repository", performance=performance, source=label).strip() != b"false":
                raise ValueError("COMPLETE_DEPENDENCY_HISTORY_REQUIRED")
            tree = git(root, "rev-parse", identity["sha"] + "^{tree}", performance=performance, source=label).decode().strip()
            if tree != identity["tree"]:
                raise ValueError("DEPENDENCY_TREE_MISMATCH")
            _tree_objects(root, identity["sha"], performance, label)
            objects, inventory = _inventory(root, identity["sha"], performance, label, folder)
            dependency_entries[label] = {**identity, "history_mode": HISTORY_MODE, **inventory,
                                         **_pack(root, label, objects, folder, performance)}
        # This root bootstrap helper is the exact file included in the pinned harness.
        helper = git(Path(sources["scorpiofs"]["path"]), "show",
            sources["scorpiofs"]["sha"] + ":tests/mst2-e2e/infra/aliyun/direct_sources.py",
            performance=performance, source="scorpiofs")
        (folder / "bootstrap.py").write_bytes(helper)
        manifest = validate_manifest({"revision": 2, "sources": entries, "dependencies": dependency_entries,
                                      "bootstrap_sha256": hashlib.sha256(helper).hexdigest()})
        (folder / "manifest.json").write_text(json.dumps(manifest, sort_keys=True) + "\n", encoding="utf-8")
        with tarfile.open(archive, "w:gz") as stream:
            for path in sorted(folder.iterdir()):
                stream.add(path, arcname=path.name, recursive=False)
    if archive.stat().st_size > MAX_BUNDLE:
        raise ValueError("SOURCE_BUNDLE_TOO_LARGE")
    return {"sha256": digest(archive), "bytes": archive.stat().st_size,
            "bootstrap_sha256": manifest["bootstrap_sha256"], "sources": entries, "dependencies": dependency_entries}


def restore(archive, output, expected):
    archive, output = Path(archive), Path(output)
    if output.exists() or archive.stat().st_size > MAX_BUNDLE:
        raise ValueError("FRESH_BOUNDED_SOURCE_DESTINATION_REQUIRED")
    if type(expected) is not dict or set(expected) != LABELS or any(not _hex(sha, 40) for sha in expected.values()):
        raise ValueError("EXACT_SOURCE_PINS_REQUIRED")
    output.mkdir(parents=True, mode=0o750)
    performance, success = _TransportPerformance("restore"), False
    try:
        manifest = _restore(archive, output, expected, performance)
        success = True
        return manifest
    except Exception as error:
        error.source_git_performance = performance.receipt(False)
        raise
    finally:
        path = output / PERFORMANCE_FILE
        with path.open("x", encoding="ascii") as stream:
            json.dump(performance.receipt(success), stream, sort_keys=True)
            stream.write("\n")
        path.chmod(0o600)


def _restore(archive, output, expected, performance):
    with tempfile.TemporaryDirectory(dir=output.parent) as temporary, tarfile.open(archive, "r:gz") as bundle:
        members = bundle.getmembers()
        names = {label + ".pack" for label in LABELS | set(DEPENDENCIES)} | {"manifest.json", "bootstrap.py"}
        if (len(members) != len(names) or {member.name for member in members} != names
                or any(not member.isfile() or not 0 <= member.size <= MAX_BUNDLE for member in members)
                or sum(member.size for member in members) > MAX_EXPANDED):
            raise ValueError("INVALID_SOURCE_BUNDLE_MEMBERS")
        raw = bundle.extractfile("manifest.json").read(65537)
        if len(raw) > 65536:
            raise ValueError("SOURCE_MANIFEST_TOO_LARGE")
        manifest = validate_manifest(json.loads(raw), expected)
        helper = bundle.extractfile("bootstrap.py").read(1024 * 1024 + 1)
        if len(helper) > 1024 * 1024 or hashlib.sha256(helper).hexdigest() != manifest["bootstrap_sha256"]:
            raise ValueError("BOOTSTRAP_DIGEST_MISMATCH")
        for label in sorted(LABELS):
            entry = manifest["sources"][label]
            pack = _extract_pack(bundle, label, entry, Path(temporary))
            repo = output / label
            performance.run("init", label, ["git", "init", "-q", "-b", "main", str(repo)],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
            with pack.open("rb") as source:
                process = performance.run("index-pack", label, ["git", "-C", str(repo), "index-pack", "--stdin"],
                    stdin=source, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120,
                    failure_code="SOURCE_PACK_INDEX_FAILED")
            if process.returncode:
                raise ValueError("SOURCE_PACK_INDEX_FAILED")
            # The raw signed commit and every tree/blob are real. Only its ancestors
            # are omitted, using Git's ordinary shallow boundary representation.
            (repo / ".git/shallow").write_text(entry["sha"] + "\n", encoding="ascii")
            git(repo, "config", "core.autocrlf", "false", performance=performance, source=label)
            git(repo, "update-ref", "refs/heads/main", entry["sha"], performance=performance, source=label)
            git(repo, "reset", "--hard", entry["sha"], performance=performance, source=label)
            if (git(repo, "rev-parse", "HEAD^{tree}", performance=performance, source=label).decode().strip() != entry["tree"]
                    or git(repo, "status", "--porcelain", performance=performance, source=label).strip()):
                raise ValueError("RESTORED_SOURCE_NOT_EXACT_OR_CLEAN")
        for label in sorted(DEPENDENCIES):
            entry = manifest["dependencies"][label]
            pack = _extract_pack(bundle, label, entry, Path(temporary))
            repo = dependency_repository(output, label)
            repo.parent.mkdir(mode=0o750, exist_ok=True)
            performance.run("init", label, ["git", "init", "--bare", "-q", "-b", "main", str(repo)],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
            with pack.open("rb") as source:
                performance.run("index-pack", label, ["git", "-C", str(repo), "index-pack", "--stdin", "--strict"],
                                stdin=source, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120,
                                failure_code="DEPENDENCY_PACK_INDEX_FAILED")
            git(repo, "update-ref", "refs/heads/main", entry["sha"], performance=performance, source=label)
            if git(repo, "rev-parse", "--is-shallow-repository", performance=performance, source=label).strip() != b"false":
                raise ValueError("COMPLETE_DEPENDENCY_HISTORY_REQUIRED")
            if git(repo, "rev-parse", "HEAD^{tree}", performance=performance, source=label).decode().strip() != entry["tree"]:
                raise ValueError("DEPENDENCY_TREE_MISMATCH")
            git(repo, "fsck", "--strict", "--no-reflogs", "--no-progress", entry["sha"],
                performance=performance, source=label)
            _, inventory = _inventory(repo, entry["sha"], performance, label, Path(temporary))
            if any(inventory[key] != entry[key] for key in inventory):
                raise ValueError("DEPENDENCY_INVENTORY_MISMATCH")
        receipt = output / DEPENDENCY_RECEIPT_FILE
        with receipt.open("x", encoding="ascii") as stream:
            json.dump({"revision": 1, "dependencies": manifest["dependencies"]}, stream, sort_keys=True)
            stream.write("\n")
        receipt.chmod(0o600)
    return manifest


def _extract_pack(bundle, label, entry, folder):
    member = bundle.getmember(label + ".pack")
    if member.size != entry["pack_bytes"]:
        raise ValueError("SOURCE_PACK_MISMATCH")
    pack = folder / (label + ".pack")
    with bundle.extractfile(member) as source, pack.open("xb") as target:
        while chunk := source.read(1024 * 1024):
            target.write(chunk)
    if pack.stat().st_size != entry["pack_bytes"] or digest(pack) != entry["pack_sha256"]:
        raise ValueError("SOURCE_PACK_MISMATCH")
    return pack


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--pins", required=True)
    args = parser.parse_args()
    restore(args.bundle, args.output, json.loads(args.pins))
