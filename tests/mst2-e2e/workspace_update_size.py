"""Benchmark fixture admission; these ceilings never widen production proofs."""

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import time


PROFILES = {
    "smoke": (8, 1, 8, 1024),
    "medium": (16, 4, 16, 8192),
    # About 100k logical entries in /project, including the directory alias.
    "large": (96, 8, 128, 8192),
    # Five cohorts grow the repository while bounding each incremental rewrite.
    "history-large": (165, 4, 128, 8192),
}
HISTORY_VERSIONS = 10
HISTORY_COHORTS = 5
HISTORY_LIMITS = {"source_entry_references_upper": 262144,
                  "resident_metadata_pages_upper": 16384,
                  "resident_metadata_bytes_upper": 256 * 1024 * 1024,
                  "page_certificates_upper": 16384,
                  "source_attestations_upper": 16384,
                  "rooted_source_inventory_upper": 65536}
ORACLE_MANIFEST_LIMIT = 32 * 1024 * 1024
EVIDENCE_ROW_LIMIT = 64 * 1024 * 1024
EVIDENCE_FILE_LIMIT = 1024 * 1024 * 1024
EVIDENCE_RECORD_LIMIT = 128
COMPACT_RECORD_LIMIT = 2 * 1024 * 1024
CAT_FILE_ITEMS = 1024
CAT_FILE_BODY_BYTES = 8 * 1024 * 1024
HARD_LIMITS = {"metadata_pages_upper": 4096, "metadata_edges_upper": 16384,
               "metadata_payload_bytes_upper": 64 * 1024 * 1024,
               "logical_entries": 131072}


def shape(profile):
    if type(profile) is not str or profile not in PROFILES:
        raise ValueError("unknown bounded fixture profile")
    return PROFILES[profile]


def history_modules(version):
    """Canonical cold tree and rotating, equally sized module cohorts."""
    if type(version) is not str or re.fullmatch(r"v(?:[1-9]|10)", version) is None:
        raise ValueError("unknown measured history version")
    modules = shape("history-large")[0]
    if modules < HISTORY_COHORTS or modules % HISTORY_COHORTS:
        raise ValueError("history modules must divide into five equal cohorts")
    if version == "v1":
        return tuple(range(modules))
    width = modules // HISTORY_COHORTS
    start = ((int(version[1:]) - 2) % HISTORY_COHORTS) * width
    return tuple(range(start, start + width))


def admit_backend(profile, isolated_backends):
    report = plan(profile)
    if profile in ("large", "history-large") and isolated_backends is not True:
        raise ValueError("large fixtures require fresh isolated backends per round; shared namespaces exceed retained source history ceilings")
    return report


def runner_label(value):
    if type(value) is not str or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", value) is None:
        raise ValueError("runner label must be 1..64 ASCII letters, digits, underscores, dots or hyphens")
    return value


def campaign_disk_plan(profile):
    """Admission reservation for all seven lanes, not measured disk usage.

    Git detached checkouts, fixture repositories and retired client/backend
    directories survive stop(). Count them all; allow no compression, hardlink,
    reflink, server representation or deleted-container credit. Per-entry and
    scratch allowances are reservation policy, not a physical-size proof.
    """
    report = plan(profile)
    modules, buckets, files, body_size = shape(profile)
    logical = report["logical_content_bytes"]
    entries = report["logical_entries"]
    cold_unique = modules * buckets * files * body_size + 129 * 32 + (
        65536 if profile == "smoke" else 2097152)
    versions = HISTORY_VERSIONS if profile == "history-large" else 4
    updates = ((versions - 1) * len(history_modules("v2")) * buckets * files * body_size
               if profile == "history-large" else (17 if profile == "smoke" else 129) * body_size)
    history = cold_unique + updates
    metadata = report["metadata_payload_bytes_upper"]
    manifests = report["oracle_manifest_bytes_upper"]
    lanes, fixture_repositories = 7, 4
    components = {
        "retained_git_detached_checkouts": lanes * versions * (logical + entries * 4096),
        "retained_git_object_databases": lanes * (history + entries * 4096),
        "fixture_checkouts_and_git_history": fixture_repositories * (logical + history + entries * 8192),
        "client_cas_metadata_controls": lanes * (history + versions * (4 * manifests + 3 * metadata)
                                                  + entries * 4096),
        "backend_objects_pack_and_metadata": lanes * (4 * history + versions * 4 * metadata
                                                       + entries * 4096),
        "concurrent_database_and_transfer_scratch": 2 * (2 * history + entries * 8192),
        "evidence_export_scratch": 4 * 1024 * 1024 * 1024,
        "dependency_images_build_and_free_space_headroom": 16 * 1024 * 1024 * 1024,
    }
    return {"profile": profile, "lanes": lanes, "versions_per_lane": versions,
        "retained_git_detached_checkouts": lanes * versions,
        "fixture_repositories": fixture_repositories,
        "cold_unique_content_bytes": cold_unique, "retained_unique_content_bytes": history,
        "components_bytes": components, "minimum_free_bytes": sum(components.values()),
        "basis": "conservative admission reservation; retained paths, uncompressed unique bodies, per-entry overhead and scratch; not observed native disk use",
        "check": "run-root parent filesystem before builds and again before campaign resources; space is not reserved against other writers",
        "runner_requirement": "existing Linux Actions runner with sufficient free space, selected explicitly by runner_label; the standard runner is not assumed sufficient"}


def admit_campaign_disk(profile, run_root):
    """Read available disk without creating resources or changing runner policy."""
    report = campaign_disk_plan(profile)
    parent = Path(run_root).absolute().parent.resolve(strict=True)
    if not parent.is_dir():
        raise ValueError("campaign disk check requires an existing run-root parent directory")
    available = shutil.disk_usage(parent).free
    report.update(filesystem_path=str(parent), available_free_bytes=available)
    if available < report["minimum_free_bytes"]:
        raise ValueError("campaign requires at least " + str(report["minimum_free_bytes"])
                         + " free bytes on the run-root filesystem; available " + str(available))
    return report


def radix_bound(entries):
    """MTP2/1 partition sizes without digests; count duplicates conservatively.

    Basename bytes and entry kinds alone determine the canonical partition and
    encoded sizes (20-byte header, 128-entry/16384-byte leaf, 41-byte child).
    We count every logical directory separately, including aliases, instead of
    taking credit for page deduplication. This is an envelope, not a wire proof.
    """
    entries = sorted(entries)
    size = 20 + sum(3 + len(name) + (32 if directory else 40)
                    for name, directory in entries)
    directory_edges = sum(directory for _, directory in entries)
    if len(entries) <= 128 and size <= 16384:
        return 1, directory_edges, size
    names = [name for name, _ in entries]
    prefix = names[0]
    for name in names[1:]:
        end = 0
        while end < min(len(prefix), len(name)) and prefix[end] == name[end]:
            end += 1
        prefix = prefix[:end]
    groups, terminal = {}, None
    for entry in entries:
        if entry[0] == prefix:
            terminal = entry
        else:
            groups.setdefault(entry[0][len(prefix)], []).append(entry)
    if len(groups) + (terminal is not None) < 2 or len(prefix) > 255:
        raise ValueError("fixture has an invalid canonical radix shape")
    pages, edges = 1, len(groups) + int(terminal is not None and terminal[1])
    payload = 20 + 2 + len(prefix) + 1 + 41 * len(groups)
    if terminal is not None:
        payload += 3 + len(terminal[0]) + (32 if terminal[1] else 40)
    for group in groups.values():
        child_pages, child_edges, child_bytes = radix_bound(group)
        pages += child_pages
        edges += child_edges
        payload += child_bytes
    return pages, edges, payload


def _admit(report):
    # Include the server's global / -> project directory wrapper as well as the
    # selected scope. Aliases and every retained scenario are counted in full.
    for key, limit in HARD_LIMITS.items():
        if report[key] > limit:
            raise ValueError("fixture exceeds production " + key + " ceiling")
    if report["oracle_manifest_bytes_upper"] > ORACLE_MANIFEST_LIMIT:
        raise ValueError("fixture exceeds benchmark oracle manifest ceiling")
    return report


def plan(profile):
    modules, buckets, files, size = shape(profile)
    # A renamed directory has the longest name in any of v1-v4. A fresh v1
    # clears the Git index, so repetitions replace, rather than accumulate it.
    shapes = [([(b"project", True)], 1),
              ([(b"r10", True), (b"alias-r10-m007", True),
                (b"empty-a", True), (b"empty-b", True)], 1),
              ([(b"renamed-m001" if m == 1 else f"m{m:03}".encode(), True)
                for m in range(modules)] + [(b"wide", True), (b"large.bin", False)], 1),
              ([(f"d{b:02}".encode(), True) for b in range(buckets)], modules + 1),
              ([(f"f{f:03}".encode(), False) for f in range(files)], (modules + 1) * buckets),
              ([(f"f{f:03}".encode(), False) for f in range(129)], 1),
              ([], 2)]
    pages = edges = payload = 0
    for entries, count in shapes:
        p, e, b = radix_bound(entries)
        pages += p * count
        edges += e * count
        payload += b * count
    logical_files = (modules + 1) * buckets * files + 130
    directories = 5 + (modules + 1) * (buckets + 1)
    # Fixed fixture ASCII paths fit 64 bytes in all rounds and scenarios. This
    # bounds the actual compact local manifest, independently of body size.
    record = {"rel_path": "x" * 64, "fs_kind": "executable", "size": 2097152,
              "content_digest": "sha256:" + "f" * 64}
    manifest_upper = 64 + logical_files * (len(json.dumps(record)) + 1) + directories * 68
    report = _admit({"profile": profile, "scope": "/project", "logical_files": logical_files,
        "logical_directories": directories, "selected_scope_entries": logical_files + directories - 1,
        "logical_entries": logical_files + directories,
        "metadata_pages_upper": pages, "metadata_edges_upper": edges,
        "metadata_payload_bytes_upper": payload,
        "oracle_manifest_bytes_upper": manifest_upper,
        "logical_content_bytes": (modules + 1) * buckets * files * size + 129 * 32
                                  + (65536 if profile == "smoke" else 2097152),
        "admission": "conservative fixture envelope; actual manifest checked before publication",
        "completion_phase": "full-verified", "production_limits": dict(HARD_LIMITS)})
    if profile == "history-large":
        cohort_modules = len(history_modules("v2"))
        # Frozen server 75a1d081 reuses COMMITTED/LIVE native attestations by
        # exact Git tree OID, page lifetime and source revision/body, inside
        # the same native profile and isolated backend namespace. Unchanged
        # module subtrees therefore add no source-entry dictionary rows. Count
        # every changed module's directory pointers and file entries, every
        # ancestor's full direct entries, and an extra complete alias subtree
        # on every increment. This gives no alias or changed-page dedup credit.
        # rooted_metadata_projection.rs:646-656; qualified_metadata_rooted.rs:640;
        # qualified_metadata_source_read.sql:46. Retained views keep roots live.
        increment_source_entries = ((cohort_modules + 1) * buckets * (files + 1)
                                    + modules + 7)
        # Reserve the canonical seed separately. Metadata, certificates and
        # attestations retain the larger ten-complete-trees envelope.
        retained = {
            "source_entry_references_upper": (report["logical_entries"]
                + (HISTORY_VERSIONS - 1) * increment_source_entries + 1024),
            "resident_metadata_pages_upper": HISTORY_VERSIONS * pages + 64,
            "resident_metadata_bytes_upper": HISTORY_VERSIONS * payload + 64 * 1024,
            "page_certificates_upper": HISTORY_VERSIONS * pages + 64,
            "source_attestations_upper": HISTORY_VERSIONS * (directories + 1) + 64,
            "rooted_source_inventory_upper": HISTORY_VERSIONS * (directories + 1) + 64,
        }
        for key, limit in HISTORY_LIMITS.items():
            if retained[key] > limit:
                raise ValueError("fixture exceeds production retained " + key + " ceiling")
        report["history_admission"] = {"versions": HISTORY_VERSIONS,
            "cohort_count": HISTORY_COHORTS,
            "modules_rewritten_per_increment": cohort_modules,
            "modules_preserved_per_increment": modules - cohort_modules,
            "source_files_rewritten_per_increment": cohort_modules * buckets * files,
            "source_bytes_rewritten_per_increment": cohort_modules * buckets * files * size,
            "source_dictionary_entries_per_increment_upper": increment_source_entries,
            "full_oracle_walks_per_lane": HISTORY_VERSIONS * (HISTORY_VERSIONS + 5) // 2,
            **retained, "production_limits": dict(HISTORY_LIMITS),
            "basis": "cold full tree plus nine exact changed cohorts and complete ancestor/alias entries; unchanged Git tree attestation reuse within the same native profile and isolated namespace with prior roots COMMITTED and LIVE; metadata reserves ten full trees plus seed; no GC or reconstruction credit",
            "proof_json_quota": "actual canonical proof JSON remains subject to the server's independent 256 MiB per-relation limit"}
    return report


def validate_manifest(expected):
    """Recheck the actual selected namespace before any native publication."""
    if type(expected) is not dict or set(expected) != {"files", "directories"}:
        raise ValueError("fixture manifest shape is invalid")
    directories = expected["directories"]
    files = expected["files"]
    if type(files) is not list or type(directories) is not list:
        raise ValueError("fixture manifest collections are invalid")
    if len(files) + len(directories) > HARD_LIMITS["logical_entries"]:
        raise ValueError("fixture exceeds production logical_entries ceiling")
    if (not all(type(path) is str for path in directories)
            or "" not in directories or len(set(directories)) != len(directories)):
        raise ValueError("fixture manifest directories are invalid")
    entries = {path: [] for path in directories}
    seen = set()

    def add(path, directory):
        if type(path) is not str or path in seen or not path or len(path.encode()) > 4096:
            raise ValueError("fixture manifest path is invalid")
        parts = path.split("/")
        if any(not part or part in (".", "..") or "\0" in part
               or len(part.encode()) > 255 for part in parts):
            raise ValueError("fixture manifest basename is invalid")
        parent = "/".join(parts[:-1])
        if parent not in entries:
            raise ValueError("fixture manifest parent is absent")
        seen.add(path)
        entries[parent].append((parts[-1].encode(), directory))

    for path in directories:
        if path:
            add(path, True)
    for file in files:
        if type(file) is not dict or set(file) != {"rel_path", "fs_kind", "size", "content_digest"}:
            raise ValueError("fixture manifest file shape is invalid")
        if (file["fs_kind"] not in ("regular", "executable", "symlink")
                or type(file["size"]) is not int or not 0 <= file["size"] <= CAT_FILE_BODY_BYTES
                or type(file["content_digest"]) is not str
                or re.fullmatch(r"sha256:[0-9a-f]{64}", file["content_digest"]) is None):
            raise ValueError("fixture manifest body exceeds benchmark batch ceiling")
        add(file["rel_path"], False)
    pages, edges, payload = radix_bound([(b"project", True)])
    for values in entries.values():
        p, e, b = radix_bound(values)
        pages += p
        edges += e
        payload += b
    raw = json.dumps(expected, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf8")
    report = _admit({"logical_files": len(files), "logical_directories": len(directories),
        "logical_entries": len(files) + len(directories), "metadata_pages_upper": pages,
        "metadata_edges_upper": edges, "metadata_payload_bytes_upper": payload,
        "oracle_manifest_bytes_upper": len(raw) + 1})
    return raw, report


def consume_regular(path, cap, consume):
    """Pin every ancestor and the exact regular inode while reading."""
    path = Path(path).absolute()
    chain = [*reversed(path.parents)]
    identities = {}
    for directory in chain:
        info = directory.lstat()
        if not stat.S_ISDIR(info.st_mode):
            raise AssertionError("safe evidence ancestor is not a real directory")
        identities[directory] = (info.st_dev, info.st_ino)
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > cap:
        raise AssertionError("safe evidence file is not a bounded independent regular file")
    parent_fd = None
    file_fd = None
    try:
        if os.name == "posix":
            flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
            parent_fd = os.open(path.anchor, flags)
            current = Path(path.anchor)
            for part in path.parts[1:-1]:
                child = os.open(part, flags, dir_fd=parent_fd)
                os.close(parent_fd)
                parent_fd = child
                current /= part
                info = os.fstat(parent_fd)
                if (info.st_dev, info.st_ino) != identities[current]:
                    raise AssertionError("safe evidence ancestor was replaced")
            file_fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=parent_fd)
        else:
            file_fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_BINARY", 0))
        opened = os.fstat(file_fd)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise AssertionError("safe evidence file was replaced before read")
        with os.fdopen(file_fd, "rb", closefd=False) as stream:
            result = consume(stream)
        after = os.fstat(file_fd)
        current = path.lstat()
        fields = lambda v: (v.st_dev, v.st_ino, v.st_size, v.st_mtime_ns, v.st_ctime_ns, v.st_nlink)
        path_fields = lambda v: (v.st_dev, v.st_ino, v.st_size, v.st_mtime_ns, v.st_nlink)
        # Windows fstat and lstat expose different ctime meanings. Compare
        # descriptor change-time with itself and pathname birth-time with
        # itself; Linux retains the full descriptor/path change-time check.
        path_changed = (fields(opened) != fields(current) if os.name == "posix" else
                        path_fields(opened) != path_fields(current) or before.st_ctime_ns != current.st_ctime_ns)
        if fields(opened) != fields(after) or path_changed:
            raise AssertionError("safe evidence file changed during read")
        for directory, identity in identities.items():
            info = directory.lstat()
            if not stat.S_ISDIR(info.st_mode) or (info.st_dev, info.st_ino) != identity:
                raise AssertionError("safe evidence ancestor changed during read")
        return result
    finally:
        if file_fd is not None:
            os.close(file_fd)
        if parent_fd is not None:
            os.close(parent_fd)



def file_sha256(path, deadline, cap=EVIDENCE_FILE_LIMIT):
    """Hash bounded evidence in chunks from one pinned file descriptor."""
    def digest(stream):
        hasher, total = hashlib.sha256(), 0
        while True:
            if time.monotonic() >= deadline:
                raise TimeoutError("evidence hash exceeded its stage deadline")
            chunk = stream.read(1024 * 1024)
            if not chunk:
                return hasher.hexdigest()
            total += len(chunk)
            if total > cap:
                raise AssertionError("evidence hash exceeds its file ceiling")
            hasher.update(chunk)
    return consume_regular(path, cap, digest)
