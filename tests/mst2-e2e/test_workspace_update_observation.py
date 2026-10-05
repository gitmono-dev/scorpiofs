"""Adversarial collector tests; actual file checks run only on POSIX."""

import copy
import hashlib
import json
import os
from pathlib import Path
import struct
import tempfile
import unittest
from unittest import mock
import uuid

import workspace_update_observation as observation

RUN = "11111111-2222-4333-8444-555555555555"
WORKSPACE = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
GENERATION = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"
DOMAIN = "03" * 32
DESCRIPTOR = (
    "4d5344320200010011111111222243338444555555555555"
    + "01" * 32 + "08002f70726f6a6563740100010000000000" + "02" * 32
)
SID = "sha256:757bc99567e655e87de1b6720ad59aca7c767dfab54d2e08cf81806c3dd1c1b2"
SCOPE_HASH = "429ad7dd8f9ae12f1c0e395acd014e02f276d666be7328e6e6253a0d53e20d68"


def encoded(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def binding(cache="/cache", workspace=WORKSPACE, generation=GENERATION):
    logical = f"ws:{RUN}:{workspace}"
    scope = f"{cache}/snapshots/{DOMAIN}/{SCOPE_HASH}"
    return {
        "record": "workspace_resolve_binding", "revision": 1, "run_id": RUN,
        "workspace_id": workspace, "generation": generation,
        "logical_request_id": logical,
        "resolve_trace_receipt": {
            "logical_request_id": logical, "attempt_ids": [logical + ":a1"],
            "final_attempt_id": logical + ":a1", "retry_count": 0,
        },
        "descriptor_bytes_hex": DESCRIPTOR, "instance_id": RUN,
        "namespace_view_id": "sha256:" + "01" * 32, "snapshot_id": SID,
        "scope": "/project", "publication_sequence": 7,
        "store": f"{scope}/{SID[7:]}/owners/{workspace}",
        "content_store": scope + "/blobs",
    }


def footer(records, payload=None):
    if payload is None:
        payload = b"".join(encoded(record) + b"\n" for record in records)
    return {
        "record": "workspace_observation_footer", "revision": 1, "run_id": RUN,
        "accepted_records": len(records), "received_records": len(records),
        "written_records": len(records), "written_bytes": len(payload),
        "producers_closed": True, "drained": True, "daemon_exit_code": 0,
        "complete": True, "first_error": None,
    }


def documents(record):
    scope = str(Path(record["content_store"]).parent)
    authority = {"revision": 1, "domain": DOMAIN, "scope": record["scope"], "snapshot_id": None}
    view = dict(authority, snapshot_id=record["snapshot_id"])
    owner = {"revision": 1, "workspace_id": record["workspace_id"],
             "snapshot_id": record["snapshot_id"], "scope": record["scope"], "auth_domain": DOMAIN}
    return {
        Path(record["store"]) / "workspace.json": encoded(owner),
        Path(scope) / "authority.json": encoded(authority),
        Path(record["store"]) / "authority.json": encoded(view),
        Path(record["content_store"]) / "authority.json": encoded(authority),
    }


class ByteCollector(observation.WorkspaceObservationCollector):
    """Exercise parsing with explicit byte inputs, never claim file evidence."""

    def __init__(self, records=(), expected_domain=DOMAIN):
        super().__init__("/sink.jsonl", "/cache", RUN, 123, expected_domain)
        self.inputs = {}
        for record in records:
            self.inputs.update(documents(record))
        self.inputs[self.path] = b""

    def _read(self, path, maximum, private=False, stable=True):
        value = self.inputs.get(path)
        if value is None or len(value) > maximum:
            observation.reject()
        return value

    def stream(self, records, tail=True):
        payload = b"".join(encoded(record) + b"\n" for record in records)
        self.inputs[self.path] = payload + (encoded(footer(records, payload)) + b"\n" if tail else b"")


class RejectAssertions:
    def rejects(self, function, *args):
        with self.assertRaises(observation.ObservationRejected) as raised:
            function(*args)
        self.assertEqual(str(raised.exception), "workspace daemon observation rejected")


class ProtocolTests(RejectAssertions, unittest.TestCase):
    def test_golden_descriptor_uses_domain_separated_snapshot_identity(self):
        record = binding()
        raw = observation.descriptor_bytes(record)
        self.assertEqual(len(raw), 106)
        self.assertEqual(observation.scope_id("/project"), SCOPE_HASH)
        self.assertNotEqual("sha256:" + hashlib.sha256(raw).hexdigest(), SID)
        self.assertEqual(observation.validate_binding(record, RUN), record)
        for sequence in (0, (1 << 63) - 1):
            record["publication_sequence"] = sequence
            observation.validate_binding(record, RUN)

    def test_descriptor_rejects_constants_length_scope_and_unbound_digest(self):
        for offset in (0, 4, 6, 8, 24, 56, 58, 66, 68, 70, 72, 105):
            with self.subTest(offset=offset):
                record = binding()
                raw = bytearray.fromhex(record["descriptor_bytes_hex"])
                raw[offset] ^= 1
                record["descriptor_bytes_hex"] = raw.hex()
                self.rejects(observation.validate_binding, record, RUN)
        for suffix in ("00", "", "FF"):
            record = binding()
            record["descriptor_bytes_hex"] = DESCRIPTOR[:-2] + suffix
            self.rejects(observation.validate_binding, record, RUN)
        record = binding()
        record["snapshot_id"] = "sha256:" + hashlib.sha256(bytes.fromhex(DESCRIPTOR)).hexdigest()
        self.rejects(observation.validate_binding, record, RUN)

    def test_json_rejects_duplicate_nested_keys_nonfinite_bom_and_invalid_utf8(self):
        for value in (b'{"x":1,"x":2}', b'{"nested":{"x":1,"x":2}}',
                      b'{"x":NaN}', b'{"x":Infinity}', b'\xef\xbb\xbf{}', b'{"x":"\xff"}'):
            self.rejects(observation.parse, value)

    def test_exact_shape_and_integer_types_are_required(self):
        for field, value in (("revision", True), ("revision", 1.0), ("publication_sequence", False),
                             ("publication_sequence", -1), ("publication_sequence", 1 << 63),
                             ("publication_sequence", 1 << 64),
                             ("scope", "/project/.."), ("store", "/cache/../owner"),
                             ("content_store", "/cache//blobs")):
            with self.subTest(field=field, value=value):
                record = binding()
                record[field] = value
                self.rejects(observation.validate_binding, record, RUN)
        for field in observation.BINDING_FIELDS:
            record = binding()
            del record[field]
            self.rejects(observation.validate_binding, record, RUN)
        record = dict(binding(), bearer_token="sensitive")
        self.rejects(observation.validate_binding, record, RUN)

    def test_uuid_digest_and_scope_are_canonical(self):
        for value in ("0" * 32, str(uuid.UUID(int=0)), WORKSPACE.upper(), WORKSPACE.replace("-", ""), 1):
            self.rejects(observation.canonical_uuid, value)
        for value in (SID.upper(), SID[7:], "sha256:" + "f" * 63, True):
            self.rejects(observation.canonical_digest, value)
        for value in ("", "relative", "/a/", "/a//b", "/a/.", "/a/..", "/a\0b", "/\ud800",
                      "/" + "a" * 256, "/" + "/".join(["a"] * 257)):
            self.rejects(observation.scope_bytes, value)
        self.assertEqual(observation.scope_bytes("/"), b"/")

    def test_receipt_requires_every_actual_attempt_and_terminal_attempt(self):
        record = binding()
        receipt = record["resolve_trace_receipt"]
        logical = record["logical_request_id"]
        receipt.update(attempt_ids=[logical + f":a{n}" for n in range(1, 5)],
                       final_attempt_id=logical + ":a4", retry_count=3)
        observation.validate_binding(record, RUN)
        for change in ({"attempt_ids": []}, {"attempt_ids": [logical + ":a2"]},
                       {"attempt_ids": [logical + ":a1"] * 4},
                       {"attempt_ids": [logical + f":a{n}" for n in range(1, 6)]},
                       {"final_attempt_id": logical + ":a1"}, {"retry_count": 2},
                       {"retry_count": True}, {"logical_request_id": "other"}, {"unexpected": 1}):
            bad = copy.deepcopy(record)
            bad["resolve_trace_receipt"].update(change)
            self.rejects(observation.validate_binding, bad, RUN)


class StreamTests(RejectAssertions, unittest.TestCase):
    def collector(self, records=None):
        records = [binding()] if records is None else records
        collector = ByteCollector(records)
        for record in records:
            collector.expect_binding(record)
        return collector, records

    def test_live_ignores_partial_line_then_accepts_complete_append_without_finality(self):
        collector, records = self.collector()
        line = encoded(records[0]) + b"\n"
        collector.inputs[collector.path] = line[:-1]
        self.assertEqual(collector.live(), [])
        collector.inputs[collector.path] = line
        self.assertEqual(collector.live(), records)
        self.rejects(collector.finalize, 0)
        collector.stream(records)
        self.assertEqual(collector.finalize(0), records)
        self.rejects(collector.live)
        self.rejects(collector.finalize, 0)

    def test_every_previously_seen_byte_is_append_only_including_partial_line(self):
        collector, records = self.collector()
        collector.inputs[collector.path] = b'{"record":'
        collector.live()
        collector.stream(records)
        collector.inputs[collector.path] = b" " + collector.inputs[collector.path]
        self.rejects(collector.live)
        collector, records = self.collector()
        collector.stream(records, tail=False)
        collector.live()
        collector.inputs[collector.path] = b""
        self.rejects(collector.live)

    def test_footer_and_actual_successful_exit_are_both_required(self):
        for exit_code in (None, True, 0.0, -9, 1):
            collector, records = self.collector()
            collector.stream(records)
            self.rejects(collector.finalize, exit_code)
        collector, records = self.collector()
        collector.stream(records, tail=False)
        self.rejects(collector.finalize, 0)
        collector.inputs[collector.path] += encoded(footer(records))
        self.rejects(collector.finalize, 0)

    def test_footer_must_be_unique_last_and_have_exact_shape(self):
        for suffix in (b"\n", encoded(footer([binding()])) + b"\n", encoded(binding()) + b"\n", b" "):
            collector, records = self.collector()
            collector.stream(records)
            collector.inputs[collector.path] += suffix
            self.rejects(collector.finalize, 0)
        for field in observation.FOOTER_FIELDS:
            collector, records = self.collector()
            payload = encoded(records[0]) + b"\n"
            tail = footer(records)
            del tail[field]
            collector.inputs[collector.path] = payload + encoded(tail) + b"\n"
            self.rejects(collector.finalize, 0)
        collector, records = self.collector()
        tail = dict(footer(records), unknown=True)
        collector.inputs[collector.path] = encoded(records[0]) + b"\n" + encoded(tail) + b"\n"
        self.rejects(collector.finalize, 0)

    def test_footer_flags_types_counts_bytes_and_error_cannot_be_relaxed(self):
        changes = [(field, False) for field in ("complete", "drained", "producers_closed")]
        changes += [(field, 1) for field in ("complete", "drained", "producers_closed")]
        changes += [(field, 0) for field in ("accepted_records", "received_records", "written_records", "written_bytes")]
        changes += [("revision", True), ("accepted_records", True), ("daemon_exit_code", True),
                    ("daemon_exit_code", 1), ("first_error", "footer_sync"), ("first_error", {}),
                    ("run_id", WORKSPACE)]
        for field, value in changes:
            with self.subTest(field=field, value=value):
                collector, records = self.collector()
                tail = footer(records)
                tail[field] = value
                collector.inputs[collector.path] = encoded(records[0]) + b"\n" + encoded(tail) + b"\n"
                self.rejects(collector.finalize, 0)

    def test_expected_actual_bindings_are_an_exact_set_with_exact_field_types(self):
        collector, records = self.collector()
        collector.stream([])
        self.rejects(collector.finalize, 0)
        collector = ByteCollector([binding()])
        collector.stream([binding()])
        self.rejects(collector.finalize, 0)
        for field, value in (("snapshot_id", "sha256:" + "ff" * 32),
                             ("generation", WORKSPACE), ("publication_sequence", True),
                             ("scope", "/other")):
            collector = ByteCollector([binding()])
            expected = binding()
            expected[field] = value
            collector.expect_binding(expected)
            collector.stream([binding()])
            self.rejects(collector.finalize, 0)

    def test_duplicate_workspace_or_generation_and_conflicting_expectation_fail(self):
        first = binding()
        second = binding(workspace=str(uuid.uuid4()))
        for records in ([first, first], [first, second]):
            collector = ByteCollector(records)
            collector.stream(records)
            self.rejects(collector.live)
        collector, records = self.collector()
        changed = dict(records[0], snapshot_id="sha256:" + "ff" * 32)
        self.rejects(collector.expect_binding, changed)

    def test_zero_records_can_finish_only_with_zero_expected_and_valid_footer(self):
        collector, _ = self.collector([])
        collector.stream([])
        self.assertEqual(collector.finalize(0), [])

    def test_only_lf_delimits_records_and_exact_lf_bytes_are_counted(self):
        collector, records = self.collector()
        collector.stream(records)
        collector.inputs[collector.path] = collector.inputs[collector.path].replace(b"\n", b"\r\n")
        self.rejects(collector.finalize, 0)
        collector, records = self.collector()
        spaced = b" " + encoded(records[0]) + b"\n"
        collector.inputs[collector.path] = spaced + encoded(footer(records)) + b"\n"
        self.rejects(collector.finalize, 0)

    def test_stream_bounds_reject_oversize_partial_record_footer_and_file(self):
        for raw in (b" " * (observation.RECORD_BYTES + 1),
                    b" " * (observation.RECORD_BYTES + 1) + b"\n",
                    b" " * (observation.FILE_BYTES + 1)):
            collector, _ = self.collector()
            collector.inputs[collector.path] = raw
            self.rejects(collector.live)
        collector, records = self.collector()
        tail = encoded(footer(records))
        collector.inputs[collector.path] = encoded(records[0]) + b"\n" + b" " * observation.FOOTER_BYTES + tail + b"\n"
        self.rejects(collector.finalize, 0)

    def test_record_count_limit_and_expected_limit_are_independent(self):
        records = [binding(workspace=str(uuid.uuid4()), generation=str(uuid.uuid4()))
                   for _ in range(observation.RECORD_LIMIT + 1)]
        collector = ByteCollector(records)
        for record in records[:-1]:
            collector.expect_binding(record)
        self.rejects(collector.expect_binding, records[-1])
        collector.stream(records)
        self.rejects(collector.live)

    def test_persisted_owner_authorities_partition_and_paths_must_all_match(self):
        record = binding()
        for path in documents(record):
            for change in ({"revision": True}, {"unknown": 1}, {"scope": "/other"},
                           {"snapshot_id": "sha256:" + "ff" * 32}):
                collector = ByteCollector([record])
                value = observation.parse(collector.inputs[path])
                value.update(change)
                collector.inputs[path] = encoded(value)
                collector.stream([record])
                self.rejects(collector.live)
        for change in ({"store": record["store"] + "-other"},
                       {"content_store": record["content_store"] + "-other"}):
            wrong = dict(record, **change)
            collector = ByteCollector([wrong])
            collector.stream([wrong])
            self.rejects(collector.live)
        collector = ByteCollector([record], expected_domain="ff" * 32)
        collector.stream([record])
        self.rejects(collector.live)

    def test_maximum_valid_scope_metadata_exceeds_footer_budget_and_is_accepted(self):
        record = binding()
        scope = "/" + "/".join(["a" * 255] * 16)
        self.assertEqual(len(scope), 4096)
        raw = bytes.fromhex(DESCRIPTOR)
        raw = raw[:56] + struct.pack("<H", len(scope)) + scope.encode() + raw[66:]
        record.update(scope=scope, descriptor_bytes_hex=raw.hex(),
                      snapshot_id="sha256:" + hashlib.sha256(b"mega.mst2.descriptor\0" + raw).hexdigest())
        root = f"/cache/snapshots/{DOMAIN}/{observation.scope_id(scope)}"
        record.update(store=f"{root}/{record['snapshot_id'][7:]}/owners/{WORKSPACE}", content_store=root + "/blobs")
        collector = ByteCollector([record])
        self.assertTrue(any(len(value) > observation.FOOTER_BYTES for value in collector.inputs.values()))
        collector.expect_binding(record)
        collector.stream([record])
        self.assertEqual(collector.finalize(0), [record])


@unittest.skipUnless(os.name == "posix", "actual nofollow/UID/inode checks require POSIX")
class FileTests(RejectAssertions, unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="workspace-observation-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.sink = self.root / "sink.jsonl"
        self.cache = self.root / "cache"
        self.record = binding(self.cache.as_posix())
        self.payload = encoded(self.record) + b"\n"
        for path, value in documents(self.record).items():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(value)
            path.chmod(0o644)
        self.sink.write_bytes(self.payload + encoded(footer([self.record])) + b"\n")
        self.sink.chmod(0o600)
        self.collector = self.new_collector()

    def new_collector(self, uid=None, path=None):
        collector = observation.WorkspaceObservationCollector(path or self.sink, self.cache, RUN,
                                                              os.getuid() if uid is None else uid, DOMAIN)
        collector.expect_binding(self.record)
        return collector

    def test_actual_regular_600_sink_and_644_metadata_finish(self):
        self.assertEqual(self.collector.finalize(0), [self.record])

    def test_wrong_sink_mode_or_actual_uid_fails(self):
        self.sink.chmod(0o644)
        self.rejects(self.collector.live)
        self.sink.chmod(0o600)
        self.rejects(self.new_collector(os.getuid() + 1).live)

    def test_sink_leaf_parent_and_authority_symlinks_are_never_followed(self):
        link = self.root / "link.jsonl"
        link.symlink_to(self.sink)
        self.rejects(self.new_collector(path=link).live)
        parent = self.root / "alias"
        parent.symlink_to(self.root, target_is_directory=True)
        self.rejects(self.new_collector(path=parent / self.sink.name).live)
        authority = Path(self.record["store"]) / "authority.json"
        saved = authority.with_name("saved.json")
        authority.rename(saved)
        authority.symlink_to(saved)
        self.rejects(self.collector.live)

    def test_fifo_and_hardlinked_sink_are_rejected(self):
        fifo = self.root / "fifo"
        os.mkfifo(fifo, 0o600)
        self.rejects(self.new_collector(path=fifo).live)
        os.link(self.sink, self.root / "hardlink")
        self.rejects(self.collector.live)

    def test_same_path_inode_replacement_and_parent_replacement_fail(self):
        self.collector.live()
        replacement = self.root / "replacement"
        replacement.write_bytes(self.sink.read_bytes())
        replacement.chmod(0o600)
        os.replace(replacement, self.sink)
        self.rejects(self.collector.finalize, 0)
        collector = self.new_collector()
        collector.live()
        store = Path(self.record["store"])
        saved = store.with_name(store.name + "-saved")
        store.rename(saved)
        store.mkdir()
        for source in saved.iterdir():
            (store / source.name).write_bytes(source.read_bytes())
        self.rejects(collector.finalize, 0)

    def test_real_append_from_partial_to_final_is_accepted(self):
        self.sink.write_bytes(self.payload[:-1])
        self.assertEqual(self.collector.live(), [])
        with self.sink.open("ab") as sink:
            sink.write(b"\n")
        self.assertEqual(self.collector.live(), [self.record])
        with self.sink.open("ab") as sink:
            sink.write(encoded(footer([self.record])) + b"\n")
        self.assertEqual(self.collector.finalize(0), [self.record])

    def test_same_inode_prefix_overwrite_fails(self):
        self.collector.live()
        with self.sink.open("r+b") as sink:
            sink.write(b" ")
        self.rejects(self.collector.finalize, 0)

    def test_actual_file_budget_and_stable_final_eof_are_enforced(self):
        with self.sink.open("wb") as sink:
            sink.truncate(observation.FILE_BYTES + 1)
        self.rejects(self.collector.live)
        self.sink.write_bytes(self.payload + encoded(footer([self.record])) + b"\n")
        real_read = os.read
        changed = False

        def concurrent_append(fd, length):
            nonlocal changed
            result = real_read(fd, length)
            if not changed:
                changed = True
                with self.sink.open("ab") as sink:
                    sink.write(b" ")
            return result

        with mock.patch.object(os, "read", concurrent_append):
            self.rejects(self.new_collector().finalize, 0)

    def test_owner_actual_link_count_is_required(self):
        owner = Path(self.record["store"]) / "workspace.json"
        os.link(owner, self.root / "owner-link")
        self.rejects(self.collector.live)

    def test_same_size_concurrent_rewrite_of_valid_sink_or_owner_bytes_fails(self):
        real_read = os.read
        for path in (self.sink, Path(self.record["store"]) / "workspace.json"):
            with self.subTest(path=path.name):
                original = path.stat()
                content = path.read_bytes()
                changed = False

                def concurrent_rewrite(fd, length):
                    nonlocal changed
                    result = real_read(fd, length)
                    opened = os.fstat(fd)
                    if not changed and (opened.st_dev, opened.st_ino) == (original.st_dev, original.st_ino):
                        changed = True
                        path.write_bytes(content)
                        os.utime(path, ns=(original.st_atime_ns, original.st_mtime_ns + 1_000_000_000))
                    return result

                with mock.patch.object(os, "read", concurrent_rewrite):
                    self.rejects(self.new_collector().finalize, 0)
                self.assertTrue(changed)

    def test_current_leaf_must_still_be_the_opened_inode_after_real_read(self):
        real_read = os.read
        owner = Path(self.record["store"]) / "workspace.json"
        for operation, path in (("live", self.sink), ("finalize", self.sink), ("finalize", owner)):
            with self.subTest(operation=operation, path=path.name):
                original = path.stat()
                replacement = path.with_name(path.name + ".replacement")
                replacement.write_bytes(path.read_bytes())
                replacement.chmod(original.st_mode & 0o777)
                changed = False

                def concurrent_replace(fd, length):
                    nonlocal changed
                    result = real_read(fd, length)
                    opened = os.fstat(fd)
                    if not changed and (opened.st_dev, opened.st_ino) == (original.st_dev, original.st_ino):
                        changed = True
                        os.replace(replacement, path)
                    return result

                collector = self.new_collector()
                with mock.patch.object(os, "read", concurrent_replace):
                    function = getattr(collector, operation)
                    self.rejects(function, *([0] if operation == "finalize" else []))
                self.assertTrue(changed)

    def test_cold_read_rejects_parent_moved_away_from_canonical_sink_path(self):
        real_read = os.read
        for operation in ("live", "finalize"):
            with self.subTest(operation=operation):
                directory = self.root / operation
                directory.mkdir()
                sink = directory / "sink.jsonl"
                content = self.sink.read_bytes()
                sink.write_bytes(content)
                sink.chmod(0o600)
                original = sink.stat()
                changed = False

                def concurrent_parent_move(fd, length):
                    nonlocal changed
                    result = real_read(fd, length)
                    opened = os.fstat(fd)
                    if not changed and (opened.st_dev, opened.st_ino) == (original.st_dev, original.st_ino):
                        changed = True
                        directory.rename(directory.with_name(operation + "-moved"))
                        directory.mkdir()
                        sink.write_bytes(content)
                        sink.chmod(0o600)
                    return result

                collector = self.new_collector(path=sink)
                with mock.patch.object(os, "read", concurrent_parent_move):
                    self.rejects(getattr(collector, operation), *([0] if operation == "finalize" else []))
                self.assertTrue(changed)


if __name__ == "__main__":
    unittest.main()
