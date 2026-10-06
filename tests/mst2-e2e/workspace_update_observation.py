"""Bounded evidence from the actual workspace daemon, never read authority.

Live results are provisional. Final acceptance requires the actual process exit
and the sink's unique last footer, plus the persisted owner/cache bindings.
"""

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import struct
import uuid

FILE_BYTES = 8 * 1024 * 1024
RECORD_BYTES = 32 * 1024
RECORD_LIMIT = 256
FOOTER_BYTES = 4096
U64_MAX = (1 << 64) - 1
BINDING_FIELDS = frozenset({
    "record", "revision", "run_id", "workspace_id", "generation",
    "logical_request_id", "resolve_trace_receipt", "descriptor_bytes_hex",
    "instance_id", "namespace_view_id", "snapshot_id", "scope",
    "publication_sequence", "store", "content_store",
})
RECEIPT_FIELDS = frozenset({
    "logical_request_id", "attempt_ids", "final_attempt_id", "retry_count",
})
FOOTER_FIELDS = frozenset({
    "record", "revision", "run_id", "accepted_records", "received_records",
    "written_records", "written_bytes", "producers_closed", "drained",
    "daemon_exit_code", "complete", "first_error",
})
OWNER_FIELDS = frozenset({
    "revision", "workspace_id", "snapshot_id", "scope", "auth_domain",
})
AUTHORITY_FIELDS = frozenset({"revision", "domain", "scope", "snapshot_id"})


class ObservationRejected(AssertionError):
    """Fixed diagnostic text; do not copy payloads, paths or credentials."""


def reject():
    raise ObservationRejected("workspace daemon observation rejected")


def integer(value, maximum=U64_MAX):
    if type(value) is not int or not 0 <= value <= maximum:
        reject()
    return value


def shape(value, fields):
    if type(value) is not dict or set(value) != fields:
        reject()


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            reject()
        result[key] = value
    return result


def parse(raw):
    try:
        # Rust emits UTF-8 JSON. A BOM or escaped invalid Unicode cannot create
        # an alternate identity accepted only by Python's permissive parser.
        text = raw.decode("utf-8") if type(raw) is bytes else raw
        return json.loads(text, object_pairs_hook=_pairs,
                          parse_constant=lambda _: reject())
    except (ValueError, UnicodeError, RecursionError, TypeError):
        reject()


def canonical_uuid(value):
    try:
        parsed = uuid.UUID(value)
    except (ValueError, TypeError, AttributeError):
        reject()
    if type(value) is not str or str(parsed) != value or not parsed.int:
        reject()
    return parsed


def canonical_digest(value):
    if type(value) is not str or re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None:
        reject()
    return bytes.fromhex(value[7:])


def canonical_domain(value):
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        reject()
    return value


def canonical_path(value):
    if type(value) is not str or not value.startswith("/") or "\0" in value:
        reject()
    parts = value.split("/")[1:]
    if not parts or any(part in ("", ".", "..") for part in parts):
        reject()
    try:
        value.encode("utf-8")
    except UnicodeError:
        reject()
    return Path(value)


def scope_bytes(scope):
    if type(scope) is not str or not scope.startswith("/") or "\0" in scope:
        reject()
    try:
        encoded = scope.encode("utf-8")
    except UnicodeError:
        reject()
    parts = scope[1:].split("/") if scope != "/" else []
    if (len(encoded) > 4096 or len(parts) > 256
            or any(part in ("", ".", "..") or len(part.encode()) > 255 for part in parts)):
        reject()
    return encoded


def scope_id(scope):
    # src/snapshot/auth.rs hash_fields: each field has a BE u64 byte length.
    encoded = scope_bytes(scope)
    return hashlib.sha256(b"mega.scorpio.scope.v1\0" + struct.pack(">Q", len(encoded)) + encoded).hexdigest()


def descriptor_bytes(record):
    """Validate and return the pinned MSD2 canonical descriptor bytes."""
    value = record["descriptor_bytes_hex"]
    if (type(value) is not str or len(value) > 2 * (98 + 4096)
            or re.fullmatch(r"(?:[0-9a-f]{2})+", value) is None):
        reject()
    raw = bytes.fromhex(value)
    scope = scope_bytes(record["scope"])
    if len(raw) != 98 + len(scope):
        reject()
    expected = (b"MSD2" + struct.pack("<HH", 2, 1)
                + canonical_uuid(record["instance_id"]).bytes
                + canonical_digest(record["namespace_view_id"])
                + struct.pack("<H", len(scope)) + scope
                + struct.pack("<HHHH", 1, 1, 0, 0))
    if raw[:-32] != expected:
        reject()
    if hashlib.sha256(b"mega.mst2.descriptor\0" + raw).digest() != canonical_digest(record["snapshot_id"]):
        reject()
    return raw


def validate_binding(record, run_id):
    shape(record, BINDING_FIELDS)
    if (record["record"] != "workspace_resolve_binding"
            or integer(record["revision"], 1) != 1 or record["run_id"] != run_id):
        reject()
    canonical_uuid(run_id)
    canonical_uuid(record["workspace_id"])
    canonical_uuid(record["generation"])
    # AuthorizedSnapshotContext::decimal restricts publication counters to i64.
    integer(record["publication_sequence"], (1 << 63) - 1)
    descriptor_bytes(record)
    logical = f"ws:{run_id}:{record['workspace_id']}"
    if record["logical_request_id"] != logical:
        reject()
    receipt = record["resolve_trace_receipt"]
    shape(receipt, RECEIPT_FIELDS)
    attempts = receipt["attempt_ids"]
    if type(attempts) is not list or not 1 <= len(attempts) <= 4:
        reject()
    expected = [f"{logical}:a{number}" for number in range(1, len(attempts) + 1)]
    if (receipt["logical_request_id"] != logical or attempts != expected
            or receipt["final_attempt_id"] != expected[-1]
            or integer(receipt["retry_count"], 3) != len(attempts) - 1):
        reject()
    canonical_path(record["store"])
    canonical_path(record["content_store"])
    return record


class WorkspaceObservationCollector:
    """A single actual sink inode and its append-only provisional prefix.

    ``expect_binding`` takes the actual create/status result. ``expected_domain``
    may additionally bind a separately captured cache partition. Without it the
    persisted domain remains partition evidence, never an authorization grant.
    """

    def __init__(self, path, cache_root, run_id, daemon_uid, expected_domain=None):
        self.path = canonical_path(os.fspath(path))
        self.cache_root = canonical_path(os.fspath(cache_root))
        self.run_id = str(canonical_uuid(run_id))
        self.daemon_uid = integer(daemon_uid, (1 << 32) - 1)
        self.expected_domain = None if expected_domain is None else canonical_domain(expected_domain)
        self.expected = {}
        self._identities = {}
        self._prefix = b""
        self._final = False

    def expect_binding(self, workspace):
        if self._final or type(workspace) is not dict:
            reject()
        fields = ("workspace_id", "generation", "snapshot_id")
        try:
            expected = {field: workspace[field] for field in fields}
        except KeyError:
            reject()
        canonical_uuid(expected["workspace_id"])
        canonical_uuid(expected["generation"])
        canonical_digest(expected["snapshot_id"])
        for field in ("namespace_view_id", "scope", "publication_sequence", "descriptor_bytes_hex"):
            if field in workspace:
                expected[field] = workspace[field]
        key = expected["workspace_id"]
        if key in self.expected and self.expected[key] != expected:
            reject()
        if key not in self.expected and len(self.expected) == RECORD_LIMIT:
            reject()
        self.expected[key] = expected

    def _identity(self, path, info):
        identity = (info.st_dev, info.st_ino)
        if path in self._identities and self._identities[path] != identity:
            reject()
        self._identities[path] = identity

    def _open_parent(self, path):
        parent = None
        try:
            flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
            parent = os.open("/", flags)
            current = Path("/")
            self._identity(current, os.fstat(parent))
            for component in path.parts[1:-1]:
                child = os.open(component, flags, dir_fd=parent)
                previous, parent = parent, child
                os.close(previous)
                current /= component
                self._identity(current, os.fstat(parent))
            return parent
        except BaseException:
            if parent is not None:
                os.close(parent)
            raise

    def _read(self, path, maximum, private=False, stable=True):
        if os.name != "posix" or not hasattr(os, "O_NOFOLLOW"):
            reject()
        path = canonical_path(os.fspath(path))
        parent = file = verified_parent = None
        try:
            parent = self._open_parent(path)
            file = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
                           dir_fd=parent)
            before = os.fstat(file)
            if (not stat.S_ISREG(before.st_mode) or before.st_uid != self.daemon_uid
                    or before.st_nlink != 1 or before.st_size > maximum
                    or private and stat.S_IMODE(before.st_mode) != 0o600):
                reject()
            self._identity(path, before)
            chunks, total = [], 0
            while True:
                chunk = os.read(file, min(65536, maximum + 1 - total))
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
                if total > maximum:
                    reject()
            after = os.fstat(file)
            if (before.st_dev, before.st_ino, before.st_uid, before.st_mode, before.st_nlink) != (
                    after.st_dev, after.st_ino, after.st_uid, after.st_mode, after.st_nlink):
                reject()
            if total > after.st_size or after.st_size > maximum:
                reject()
            if stable and ((before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                    after.st_size, after.st_mtime_ns, after.st_ctime_ns) or total != after.st_size):
                reject()
            # Reopen from root: an old parent FD can still name the original
            # file after its directory was moved away from the canonical path.
            verified_parent = self._open_parent(path)
            current = os.stat(path.name, dir_fd=verified_parent, follow_symlinks=False)
            if (after.st_dev, after.st_ino, after.st_uid, after.st_mode, after.st_nlink) != (
                    current.st_dev, current.st_ino, current.st_uid, current.st_mode, current.st_nlink):
                reject()
            if stable and (after.st_size, after.st_mtime_ns, after.st_ctime_ns) != (
                    current.st_size, current.st_mtime_ns, current.st_ctime_ns):
                reject()
            if current.st_size < total or current.st_size > maximum:
                reject()
            return b"".join(chunks)
        except (OSError, ValueError):
            reject()
        finally:
            if file is not None:
                os.close(file)
            if parent is not None:
                os.close(parent)
            if verified_parent is not None:
                os.close(verified_parent)

    def _authority(self, directory, domain, scope, snapshot):
        authority = parse(self._read(directory / "authority.json", RECORD_BYTES))
        shape(authority, AUTHORITY_FIELDS)
        if authority != {"revision": 1, "domain": domain, "scope": scope, "snapshot_id": snapshot}:
            reject()
        integer(authority["revision"], 1)

    def _owner(self, record):
        store = canonical_path(record["store"])
        owner = parse(self._read(store / "workspace.json", RECORD_BYTES))
        shape(owner, OWNER_FIELDS)
        domain = canonical_domain(owner["auth_domain"])
        if self.expected_domain is not None and domain != self.expected_domain:
            reject()
        if owner != {"revision": 1, "workspace_id": record["workspace_id"],
                     "snapshot_id": record["snapshot_id"], "scope": record["scope"], "auth_domain": domain}:
            reject()
        integer(owner["revision"], 1)
        scope = self.cache_root / "snapshots" / domain / scope_id(record["scope"])
        expected = scope / record["snapshot_id"][7:] / "owners" / record["workspace_id"]
        if store != expected or canonical_path(record["content_store"]) != scope / "blobs":
            reject()
        self._authority(scope, domain, record["scope"], None)
        self._authority(store, domain, record["scope"], record["snapshot_id"])
        self._authority(scope / "blobs", domain, record["scope"], None)

    def _collect(self, final):
        if self._final:
            reject()
        raw = self._read(self.path, FILE_BYTES, private=True, stable=final)
        if not raw.startswith(self._prefix):
            reject()
        cut = raw.rfind(b"\n") + 1
        complete, partial = raw[:cut], raw[cut:]
        if len(partial) > RECORD_BYTES or final and partial:
            reject()
        records, footer, payload_bytes = [], None, 0
        seen, stores, generations = set(), set(), set()
        for payload in complete.split(b"\n")[:-1]:
            line = payload + b"\n"
            # JSONL is delimited only by LF; CR and other line separators are
            # not alternate delimiters for the Rust writer's canonical output.
            if len(line) > RECORD_BYTES + 1 or b"\r" in line:
                reject()
            value = parse(line[:-1])
            if type(value) is dict and value.get("record") == "workspace_observation_footer":
                if footer is not None or len(line) > FOOTER_BYTES:
                    reject()
                shape(value, FOOTER_FIELDS)
                footer = value
                continue
            if footer is not None or len(records) == RECORD_LIMIT:
                reject()
            validate_binding(value, self.run_id)
            if (value["workspace_id"] in seen or value["store"] in stores
                    or value["generation"] in generations):
                reject()
            seen.add(value["workspace_id"])
            stores.add(value["store"])
            generations.add(value["generation"])
            self._owner(value)
            records.append(value)
            payload_bytes += len(line)
        if payload_bytes > FILE_BYTES - FOOTER_BYTES:
            reject()
        self._prefix = raw
        return records, footer, payload_bytes, partial

    def live(self):
        """Complete binding lines only; a live result never certifies finality."""
        records, footer, _, partial = self._collect(False)
        if footer is not None and partial:
            reject()
        return records

    def finalize(self, actual_daemon_exit_code):
        if type(actual_daemon_exit_code) is not int or actual_daemon_exit_code != 0:
            reject()
        records, footer, payload_bytes, _ = self._collect(True)
        if footer is None:
            reject()
        if (footer["record"] != "workspace_observation_footer"
                or integer(footer["revision"], 1) != 1 or footer["run_id"] != self.run_id
                or type(footer["daemon_exit_code"]) is not int or footer["daemon_exit_code"] != 0
                or footer["first_error"] is not None
                or any(footer[field] is not True for field in ("complete", "producers_closed", "drained"))):
            reject()
        for field in ("accepted_records", "received_records", "written_records"):
            if integer(footer[field], RECORD_LIMIT) != len(records):
                reject()
        if integer(footer["written_bytes"], FILE_BYTES - FOOTER_BYTES) != payload_bytes:
            reject()
        if {record["workspace_id"] for record in records} != set(self.expected):
            reject()
        for record in records:
            if any(record[field] != value or type(record[field]) is not type(value)
                   for field, value in self.expected[record["workspace_id"]].items()):
                reject()
        self._final = True
        return records
