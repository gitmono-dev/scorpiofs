"""Bounded, fail-closed collection of the actual typed native resolve sink."""

import hashlib
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import stat
import struct
import time
import uuid


RECORD_BYTES = 32 * 1024
RECORD_LIMIT = 64
FILE_BYTES = RECORD_BYTES * RECORD_LIMIT
STATUS_BYTES = 4096
U64_MAX = (1 << 64) - 1
WORK_FIELDS = frozenset({
    "directories_rebuilt", "reused_subtree_roots", "directory_root_pages_built",
    "directory_root_page_bytes_built", "directory_root_pages_reused",
    "directory_root_page_bytes_reused", "directory_entries_scanned",
    "scope_path_entries_examined", "tree_fetches", "verified_blob_hits",
    "verified_blob_misses", "raw_bytes_fetched", "raw_bytes_hashed",
})
FIXED = {
    "observation_revision": 1, "phase": "resolve_directory_projection",
    "source_domain": "native-git", "schema_version": 2, "metadata_codec": 1,
    "materialization_policy": 1, "fs_semantics": 1, "access_projection": 0,
    "verification_revision": 2, "projection_revision": 1,
    "page_counter_scope": "returned-directory-root-pages", "codec_radix_work": "NOT_EXPOSED",
    "message": "native resolve directory projection succeeded",
}
PAYLOAD_FIELDS = frozenset(FIXED) | WORK_FIELDS | frozenset({
    "request_id", "instance_id", "root_commit_oid", "root_tree_oid",
    "native_certificate_receipt_id", "native_writer_epoch", "native_publication_sequence",
    "scope", "namespace_view_id", "snapshot_id", "metadata_root", "projection_elapsed_micros",
})
STATUS_FIELDS = frozenset({
    "writer_revision", "sink_instance", "accepted_records", "written_sequence",
    "written_records", "written_bytes", "rolling_sha256", "first_error_code", "closed",
})
ENVELOPE_FIELDS = frozenset({
    "writer_revision", "sink_instance", "record_sequence", "payload", "payload_sha256",
})
RECEIPT_FIELDS = frozenset({
    "logical_request_id", "attempt_ids", "final_attempt_id", "retry_count",
})


class TraceRejected(AssertionError):
    """Fixed messages only; never record contents, paths or credentials."""


class AckPending(Exception):
    """A valid live sink has not yet acknowledged every accepted record."""


def reject():
    raise TraceRejected("native projection observation rejected")


def integer(value, maximum=U64_MAX, positive=False):
    if type(value) is not int or not (int(positive) <= value <= maximum):
        reject()
    return value


def shape(value, fields):
    if type(value) is not dict or set(value) != fields:
        reject()


def pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            reject()
        result[key] = value
    return result


def parse(raw):
    try:
        return json.loads(raw, object_pairs_hook=pairs,
                          parse_constant=lambda _: reject())
    except (ValueError, UnicodeError, RecursionError):
        reject()


def digest(raw):
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def canonical_digest(value):
    if type(value) is not str or not re.fullmatch(r"sha256:[0-9a-f]{64}", value):
        reject()
    return bytes.fromhex(value[7:])


def canonical_uuid(value):
    try:
        result = uuid.UUID(value)
    except (ValueError, TypeError, AttributeError):
        reject()
    if type(value) is not str or str(result) != value or not result.int:
        reject()
    return result


def token(value, maximum=128):
    if type(value) is not str or not re.fullmatch(r"[A-Za-z0-9_.:/-]{1," + str(maximum) + "}", value):
        reject()


def descriptor_bytes(payload):
    # This experiment is confined to the explicit /project scope. MSD2 uses
    # little-endian u16 fields; independently rederive its canonical bytes.
    if payload["scope"] != "/project":
        reject()
    scope = payload["scope"].encode()
    return (b"MSD2" + struct.pack("<HH", 2, 1)
            + canonical_uuid(payload["instance_id"]).bytes
            + canonical_digest(payload["namespace_view_id"])
            + struct.pack("<H", len(scope)) + scope + struct.pack("<HHHH", 1, 1, 0, 0)
            + canonical_digest(payload["metadata_root"]))


def validate_payload(payload):
    shape(payload, PAYLOAD_FIELDS)
    for field, expected in FIXED.items():
        if type(payload[field]) is not type(expected) or payload[field] != expected:
            reject()
    for field in WORK_FIELDS | {"projection_elapsed_micros"}:
        integer(payload[field])
    for field in ("native_certificate_receipt_id", "native_writer_epoch", "native_publication_sequence"):
        integer(payload[field], positive=True)
    token(payload["request_id"])
    canonical_uuid(payload["instance_id"])
    for field in ("root_commit_oid", "root_tree_oid"):
        if type(payload[field]) is not str or not re.fullmatch(r"sha1:[0-9a-f]{40}", payload[field]):
            reject()
    expected_view = digest(b"mega.mst2.namespaceview\0" + payload["root_commit_oid"][5:].encode())
    if payload["namespace_view_id"] != expected_view:
        reject()
    if payload["snapshot_id"] != digest(b"mega.mst2.descriptor\0" + descriptor_bytes(payload)):
        reject()


def receipt_ids(receipt, logical_id):
    shape(receipt, RECEIPT_FIELDS)
    token(logical_id, 125)
    attempts = receipt["attempt_ids"]
    if type(attempts) is not list or not 1 <= len(attempts) <= 4:
        reject()
    expected = [f"{logical_id}:a{number}" for number in range(1, len(attempts) + 1)]
    if (receipt["logical_request_id"] != logical_id or attempts != expected
            or receipt["final_attempt_id"] != expected[-1]
            or integer(receipt["retry_count"], 3) != len(attempts) - 1):
        reject()
    return attempts


def window_anchor(started_utc, deadline_utc, admission=False):
    """T0 is the actual dispatch, D=T0+235m and H=T0+220m; never restart."""
    try:
        started = datetime.fromisoformat(started_utc.replace("Z", "+00:00"))
        deadline = datetime.fromisoformat(deadline_utc.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        reject()
    for value in (started, deadline):
        if value.tzinfo is None or value.utcoffset().total_seconds() != 0:
            reject()
    now = datetime.now(timezone.utc)
    if (deadline != started + timedelta(minutes=235) or now < started
            or now >= started + timedelta(minutes=220)
            or admission and now - started > timedelta(minutes=15)):
        reject()
    return time.monotonic() + (started + timedelta(minutes=220) - now).total_seconds()


class ProjectionCollector:
    def __init__(self, cache):
        self.cache = Path(cache).absolute()
        self.directory_bindings = {}
        self.record_identity = None
        self.allowed = set()
        self.registered = {}
        self.receipts = {}
        self.finals = {}
        self.last_bytes = b""
        self._directory(self.cache, private=False)
        logs = self.cache / "logs"
        self._directory(logs, private=False)
        parent = logs / "mst2-native-projection"
        self._directory(parent)
        entries = list(parent.iterdir())
        if len(entries) != 1:
            reject()
        self.root = entries[0]
        self.instance = str(canonical_uuid(self.root.name))
        self._directory(self.root)

    def _directory(self, path, private=True):
        # Reject symlinks throughout the actual configured anchor chain.
        for component in [*reversed(path.parents), path]:
            info = component.lstat()
            if not stat.S_ISDIR(info.st_mode):
                reject()
        info = path.lstat()
        if private and os.name == "posix" and (info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700):
            reject()
        identity = (info.st_dev, info.st_ino)
        if path in self.directory_bindings and self.directory_bindings[path] != identity:
            reject()
        self.directory_bindings[path] = identity

    def _read(self, name, cap, stable=False):
        for path in self.directory_bindings:
            self._directory(path, private=path not in (self.cache, self.cache / "logs"))
        path = self.root / name
        root_fd = None
        if os.name == "posix":
            # Open every directory relative to its pinned parent. Checking
            # lstat followed by a whole-path open alone permits ancestor swaps.
            flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
            root_fd = os.open(self.root.anchor, flags)
            try:
                current = Path(self.root.anchor)
                for component in self.root.parts[1:]:
                    child = os.open(component, flags, dir_fd=root_fd)
                    os.close(root_fd)
                    root_fd = child
                    current /= component
                    info = os.fstat(root_fd)
                    expected = self.directory_bindings.get(current)
                    if expected is not None and (info.st_dev, info.st_ino) != expected:
                        reject()
            except BaseException:
                os.close(root_fd)
                raise
        try:
            return self._read_at(name, path, cap, stable, root_fd)
        finally:
            if root_fd is not None:
                os.close(root_fd)

    def _read_at(self, name, path, cap, stable, root_fd):
        before = os.stat(name, dir_fd=root_fd, follow_symlinks=False) if root_fd is not None else path.lstat()
        if not stat.S_ISREG(before.st_mode) or before.st_size > cap:
            reject()
        if os.name == "posix" and (before.st_uid != os.geteuid() or stat.S_IMODE(before.st_mode) != 0o600):
            reject()
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(name, flags, dir_fd=root_fd) if root_fd is not None else os.open(path, flags)
        try:
            opened = os.fstat(fd)
            identity = (opened.st_dev, opened.st_ino)
            if identity != (before.st_dev, before.st_ino):
                reject()
            if stable and self.record_identity not in (None, identity):
                reject()
            if stable:
                self.record_identity = identity
            with os.fdopen(fd, "rb", closefd=False) as stream:
                raw = stream.read(cap + 1)
            if len(raw) > cap:
                reject()
            after = os.stat(name, dir_fd=root_fd, follow_symlinks=False) if root_fd is not None else path.lstat()
            if identity != (after.st_dev, after.st_ino):
                if stable:
                    reject()
                raise AckPending()
            return raw
        finally:
            os.close(fd)

    def snapshot(self, closed=False):
        first = self._read("status.json", STATUS_BYTES)
        status = parse(first)
        shape(status, STATUS_FIELDS)
        if status["writer_revision"] != 1 or type(status["writer_revision"]) is not int or status["sink_instance"] != self.instance:
            reject()
        for name in ("accepted_records", "written_sequence", "written_records"):
            integer(status[name], RECORD_LIMIT)
        integer(status["written_bytes"], FILE_BYTES)
        if integer(status["first_error_code"], 255) != 0 or type(status["closed"]) is not bool:
            reject()
        canonical_digest(status["rolling_sha256"])
        if (status["written_sequence"] != status["written_records"]
                or status["written_records"] > status["accepted_records"]):
            reject()
        raw = self._read("records.jsonl", FILE_BYTES, stable=True)
        if self._read("status.json", STATUS_BYTES) != first:
            raise AckPending()
        if (status["written_bytes"] < len(self.last_bytes)
                or not raw.startswith(self.last_bytes)):
            reject()
        acknowledged = raw[:status["written_bytes"]]
        if len(acknowledged) != status["written_bytes"] or digest(acknowledged) != status["rolling_sha256"]:
            reject()
        lines = acknowledged.splitlines(keepends=True)
        if len(lines) != status["written_records"]:
            reject()
        payloads = {}
        for number, line in enumerate(lines, 1):
            if not line.endswith(b"\n") or len(line) > RECORD_BYTES:
                reject()
            envelope = parse(line)
            shape(envelope, ENVELOPE_FIELDS)
            if (type(envelope["writer_revision"]) is not int or envelope["writer_revision"] != 1
                    or envelope["sink_instance"] != self.instance
                    or integer(envelope["record_sequence"], RECORD_LIMIT, True) != number):
                reject()
            canonical_digest(envelope["payload_sha256"])
            prefix = (f'{{"writer_revision":1,"sink_instance":"{self.instance}",'
                      f'"record_sequence":{number},"payload":').encode()
            suffix = (',"payload_sha256":"' + envelope["payload_sha256"] + '"}\n').encode()
            if not line.startswith(prefix) or not line.endswith(suffix):
                reject()
            if digest(line[len(prefix):-len(suffix)]) != envelope["payload_sha256"]:
                reject()
            payload = envelope["payload"]
            validate_payload(payload)
            request = payload["request_id"]
            if request not in self.allowed or request in payloads:
                reject()
            payloads[request] = payload
        self.last_bytes = acknowledged
        if (len(raw) != status["written_bytes"] or status["accepted_records"] != status["written_records"]):
            if status["closed"]:
                reject()
            raise AckPending()
        if closed and not status["closed"]:
            raise AckPending()
        return status, payloads

    def _wait(self, deadline, closed=False, required_id=None):
        until = min(deadline, time.monotonic() + 5)
        while time.monotonic() < until:
            try:
                snapshot = self.snapshot(closed)
                if time.monotonic() >= until:
                    raise TraceRejected("native projection acknowledgement exceeded its original deadline")
                if required_id is not None and required_id not in snapshot[1]:
                    if snapshot[0]["closed"]:
                        reject()
                    raise AckPending()
                return snapshot
            except AckPending:
                time.sleep(min(.01, max(0, until - time.monotonic())))
        raise TraceRejected("native projection durable acknowledgement incomplete")

    def register(self, measured, logical_id):
        """Admit actual validated receipt IDs before reading shared trace files."""
        attempts = receipt_ids(measured.get("resolve_trace_receipt"), logical_id)
        if self.allowed.intersection(attempts):
            reject()
        self.allowed.update(attempts)
        self.registered[logical_id] = tuple(attempts)
        self.receipts[logical_id] = parse(json.dumps(measured["resolve_trace_receipt"]))

    def collect(self, measured, native, identity, logical_id, deadline):
        self.register(measured, logical_id)
        return self.collect_registered(measured, native, identity, logical_id, deadline)

    def collect_registered(self, measured, native, identity, logical_id, deadline):
        attempts = receipt_ids(measured.get("resolve_trace_receipt"), logical_id)
        if self.registered.get(logical_id) != tuple(attempts) or attempts[-1] in self.finals:
            reject()
        final = attempts[-1]
        status, payloads = self._wait(deadline, required_id=final)
        payload = payloads[final]
        bindings = {
            "instance_id": native["instance_id"], "root_commit_oid": "sha1:" + identity["global_commit"],
            "root_tree_oid": "sha1:" + identity["global_tree"],
            "native_certificate_receipt_id": native["certificate_receipt_id"],
            "native_writer_epoch": native["writer_epoch"], "native_publication_sequence": native["sequence"],
            "namespace_view_id": identity["namespace_view_id"], "scope": "/project",
            "snapshot_id": measured["snapshot_id"],
        }
        if any(type(value) is not type(payload[key]) or payload[key] != value for key, value in bindings.items()):
            reject()
        if (measured["namespace_view_id"] != payload["namespace_view_id"]
                or integer(measured["publication_sequence"], positive=True) != payload["native_publication_sequence"]
                or measured.get("descriptor_bytes_hex") != descriptor_bytes(payload).hex()):
            reject()
        self.finals[final] = payload
        return {"instrumentation_mode": "typed-projection-writer-v1", "payload": payload,
                "prior_attempt_observations": [payloads[x] for x in attempts[:-1] if x in payloads],
                "sink_instance": self.instance, "durable_written_sequence": status["written_sequence"]}

    def finish(self, expected, deadline):
        status, payloads = self._wait(deadline, closed=True)
        if (len(self.finals) != expected
                or set(self.finals) != {attempts[-1] for attempts in self.registered.values()}
                or any(payloads.get(key) != value for key, value in self.finals.items())):
            reject()
        return status

    def closed_evidence(self, expected, deadline):
        """Read the actual closed files and registered live receipt authority."""
        status = self.finish(expected, deadline)
        raw_status = self._read("status.json", STATUS_BYTES)
        raw_records = self._read("records.jsonl", FILE_BYTES, stable=True)
        if (parse(raw_status) != status or raw_records != self.last_bytes
                or time.monotonic() >= deadline):
            reject()
        return {"records_jsonl": raw_records.decode("utf-8"),
                "status_json": raw_status.decode("utf-8"),
                "registered_receipts": [parse(json.dumps(value)) for value in self.receipts.values()],
                "expected_final_count": expected}
