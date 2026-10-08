"""Transport exact shallow Git objects, without credentials or ancestor history."""

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
PERFORMANCE_FILE = "source-git-performance.json"
PERFORMANCE_LIMIT = 65536
RESOURCE_FIELDS = {"cpu_user_ms", "cpu_system_ms", "peak_rss_bytes", "io_read_bytes", "io_write_bytes"}


def _operations(phase):
    if phase == "pack":
        return [(label, operation) for label in sorted(LABELS)
                for operation in ("rev-parse", "ls-tree", "pack-objects")] + [("scorpiofs", "show")]
    if phase == "restore":
        return [(label, operation) for label in sorted(LABELS)
                for operation in ("init", "index-pack", "config", "update-ref", "reset", "rev-parse", "status")]
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


def create(sources, archive):
    performance = _TransportPerformance("pack")
    try:
        receipt = _create(sources, archive, performance)
    except Exception as error:
        error.source_git_performance = performance.receipt(False)
        raise
    receipt["source_pack_performance"] = performance.receipt(True)
    return receipt


def _create(sources, archive, performance):
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
            tree = git(root, "rev-parse", commit + "^{tree}", performance=performance, source=label).decode().strip()
            objects = {commit, tree}
            for row in git(root, "ls-tree", "-r", "-t", "-z", commit,
                           performance=performance, source=label).split(b"\0"):
                if row:
                    mode, kind, oid = row.split(b"\t", 1)[0].split()
                    if kind not in (b"tree", b"blob"):
                        raise ValueError("SUBMODULE_SOURCE_NOT_SUPPORTED")
                    objects.add(oid.decode())
            pack = folder / (label + ".pack")
            with pack.open("xb") as output:
                result = performance.run("pack-objects", label, ["git", "-C", str(root), "pack-objects", "--stdout"],
                    input=("\n".join(sorted(objects)) + "\n").encode(), stdout=output,
                    stderr=subprocess.PIPE, timeout=300, failure_code="SOURCE_PACK_FAILED_OR_TOO_LARGE")
            if result.returncode or pack.stat().st_size > MAX_BUNDLE:
                raise ValueError("SOURCE_PACK_FAILED_OR_TOO_LARGE")
            entries[label] = {"sha": commit, "tree": tree, "pack_sha256": digest(pack), "pack_bytes": pack.stat().st_size}
        # This root bootstrap helper is the exact file included in the pinned harness.
        helper = git(Path(sources["scorpiofs"]["path"]), "show",
            sources["scorpiofs"]["sha"] + ":tests/mst2-e2e/infra/aliyun/direct_sources.py",
            performance=performance, source="scorpiofs")
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
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--pins", required=True)
    args = parser.parse_args()
    restore(args.bundle, args.output, json.loads(args.pins))
