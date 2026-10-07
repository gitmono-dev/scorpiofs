"""Actual bounded trace files, retry bindings, acknowledgements and closure."""

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import tempfile
import shutil
import subprocess
import threading
import time
import unittest
from unittest.mock import patch
import uuid

import commit_update_projection as projection
import commit_update_ci as ci
import commit_update_budget as budget


class ProjectionCollectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.cache = Path(self.temp.name) / "cache"
        self.cache.mkdir(mode=0o700)
        self.logs = self.cache / "logs"
        self.logs.mkdir()
        self.parent = self.logs / "mst2-native-projection"
        self.parent.mkdir(mode=0o700)
        self.sink = str(uuid.uuid4())
        self.root = self.parent / self.sink
        self.root.mkdir(mode=0o700)
        self.instance = str(uuid.uuid4())
        self.logical = "mst2:test:r1:v1:resolve"
        self.payload = dict(projection.FIXED)
        self.payload.update({key: 0 for key in projection.WORK_FIELDS})
        self.payload.update({
            "request_id": self.logical + ":a1", "instance_id": self.instance,
            "root_commit_oid": "sha1:" + "a" * 40, "root_tree_oid": "sha1:" + "b" * 40,
            "native_certificate_receipt_id": 11, "native_writer_epoch": 1,
            "native_publication_sequence": 3, "scope": "/project",
            "namespace_view_id": projection.digest(b"mega.mst2.namespaceview\0" + b"a" * 40),
            "metadata_root": "sha256:" + "c" * 64, "projection_elapsed_micros": 42,
        })
        self.payload["snapshot_id"] = projection.digest(b"mega.mst2.descriptor\0" + projection.descriptor_bytes(self.payload))
        self.native = {"instance_id": self.instance, "certificate_receipt_id": 11,
                       "writer_epoch": 1, "sequence": 3}
        self.identity = {"global_commit": "a" * 40, "global_tree": "b" * 40,
                         "namespace_view_id": self.payload["namespace_view_id"]}
        self.measured = {
            "resolve_trace_receipt": {"logical_request_id": self.logical,
                                      "attempt_ids": [self.logical + ":a1"],
                                      "final_attempt_id": self.logical + ":a1", "retry_count": 0},
            "snapshot_id": self.payload["snapshot_id"], "namespace_view_id": self.payload["namespace_view_id"],
            "publication_sequence": 3, "descriptor_bytes_hex": projection.descriptor_bytes(self.payload).hex(),
        }

    def line(self, payload, sequence):
        body = json.dumps(payload, separators=(",", ":")).encode()
        prefix = (f'{{"writer_revision":1,"sink_instance":"{self.sink}",'
                  f'"record_sequence":{sequence},"payload":').encode()
        return prefix + body + (',"payload_sha256":"' + projection.digest(body) + '"}\n').encode()

    def write(self, payloads, closed=False, raw=None, **status_changes):
        raw = raw if raw is not None else b"".join(self.line(p, n) for n, p in enumerate(payloads, 1))
        record = self.root / "records.jsonl"
        # Keep the actual records inode throughout live appends.
        with record.open("wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        record.chmod(0o600)
        status = {"writer_revision": 1, "sink_instance": self.sink, "accepted_records": len(payloads),
                  "written_sequence": len(payloads), "written_records": len(payloads),
                  "written_bytes": len(raw), "rolling_sha256": projection.digest(raw),
                  "first_error_code": 0, "closed": closed}
        status.update(status_changes)
        temporary = self.root / "fixture-status.tmp"
        with temporary.open("wb") as stream:
            stream.write(json.dumps(status).encode())
            stream.flush()
            os.fsync(stream.fileno())
        temporary.chmod(0o600)
        os.replace(temporary, self.root / "status.json")
        if os.name == "posix":
            fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

    def collect(self, collector):
        return collector.collect(self.measured, self.native, self.identity, self.logical, time.monotonic() + 1)

    def test_live_durable_ack_then_real_closed_status_is_required(self):
        self.write([self.payload])
        collector = projection.ProjectionCollector(self.cache)
        result = self.collect(collector)
        self.assertEqual(result["payload"], self.payload)
        self.assertEqual(result["durable_written_sequence"], 1)
        with self.assertRaises(projection.TraceRejected):
            collector.finish(1, time.monotonic() + .02)
        self.write([self.payload], closed=True)
        self.assertTrue(collector.finish(1, time.monotonic() + 1)["closed"])

    def test_previous_success_with_lost_response_is_only_prior_attempt_diagnostic(self):
        first = dict(self.payload)
        first["root_commit_oid"] = "sha1:" + "d" * 40
        first["namespace_view_id"] = projection.digest(b"mega.mst2.namespaceview\0" + b"d" * 40)
        first["snapshot_id"] = projection.digest(b"mega.mst2.descriptor\0" + projection.descriptor_bytes(first))
        final = dict(self.payload, request_id=self.logical + ":a2")
        self.measured["resolve_trace_receipt"].update(attempt_ids=[self.logical + ":a1", self.logical + ":a2"],
                                                       final_attempt_id=self.logical + ":a2", retry_count=1)
        self.write([first, final], closed=True)
        collector = projection.ProjectionCollector(self.cache)
        result = self.collect(collector)
        self.assertEqual(result["payload"], final)
        self.assertEqual(result["prior_attempt_observations"], [first])
        collector.finish(1, time.monotonic() + 1)

    def paired_inputs(self):
        logical = "mst2:test:client-b:r1:v1:resolve"
        payload = dict(self.payload, request_id=logical + ":a1")
        measured = json.loads(json.dumps(self.measured))
        measured["resolve_trace_receipt"] = {
            "logical_request_id": logical, "attempt_ids": [logical + ":a1"],
            "final_attempt_id": logical + ":a1", "retry_count": 0}
        return logical, payload, measured

    def test_two_admitted_clients_can_read_the_same_real_acknowledged_trace_file(self):
        logical, second, measured = self.paired_inputs()
        for payloads in ([self.payload, second], [second, self.payload]):
            with self.subTest(first=payloads[0]["request_id"]):
                self.write(payloads, closed=True)
                collector = projection.ProjectionCollector(self.cache)
                collector.register(self.measured, self.logical)
                collector.register(measured, logical)
                first = collector.collect_registered(self.measured, self.native, self.identity,
                                                     self.logical, time.monotonic() + 1)
                other = collector.collect_registered(measured, self.native, self.identity,
                                                     logical, time.monotonic() + 1)
                self.assertEqual(first["payload"], self.payload)
                self.assertEqual(other["payload"], second)
                self.assertTrue(collector.finish(2, time.monotonic() + 1)["closed"])

    def test_paired_registration_preserves_unknown_duplicate_and_full_collection_rejection(self):
        logical, second, measured = self.paired_inputs()
        for failure in ("unknown", "duplicate", "register-twice", "uncollected", "receipt-drift", "collect-twice"):
            with self.subTest(failure=failure):
                payloads = [self.payload, second]
                if failure == "unknown":
                    payloads.append(dict(second, request_id="unknown:a1"))
                if failure == "duplicate":
                    payloads.append(second)
                self.write(payloads, closed=True)
                collector = projection.ProjectionCollector(self.cache)
                collector.register(self.measured, self.logical)
                collector.register(measured, logical)
                with self.assertRaises(projection.TraceRejected):
                    if failure == "register-twice":
                        collector.register(measured, logical)
                    else:
                        collector.collect_registered(self.measured, self.native, self.identity,
                                                     self.logical, time.monotonic() + 1)
                        if failure == "uncollected":
                            collector.finish(1, time.monotonic() + 1)
                        elif failure == "receipt-drift":
                            changed = json.loads(json.dumps(measured))
                            changed["resolve_trace_receipt"].update(
                                attempt_ids=[logical + ":a1", logical + ":a2"],
                                final_attempt_id=logical + ":a2", retry_count=1)
                            collector.collect_registered(changed, self.native, self.identity,
                                                         logical, time.monotonic() + 1)
                        elif failure == "collect-twice":
                            collector.collect_registered(self.measured, self.native, self.identity,
                                                         self.logical, time.monotonic() + 1)

    def test_waits_for_real_delayed_durable_record_within_original_deadline(self):
        self.write([])
        collector = projection.ProjectionCollector(self.cache)
        def acknowledge():
            time.sleep(.03)
            self.write([self.payload])
        worker = threading.Thread(target=acknowledge)
        worker.start()
        try:
            self.assertEqual(self.collect(collector)["payload"], self.payload)
        finally:
            worker.join(timeout=1)

    def test_sticky_errors_closed_gaps_and_bad_status_are_terminal(self):
        for changes in ({"first_error_code": 1}, {"first_error_code": True},
                        {"written_sequence": 2}, {"accepted_records": 65},
                        {"written_bytes": 0}, {"rolling_sha256": "sha256:" + "0" * 64},
                        {"closed": 1}, {"unknown": 0}, {"accepted_records": 2, "closed": True}):
            with self.subTest(changes=changes):
                self.write([self.payload], **changes)
                with self.assertRaises(projection.TraceRejected):
                    self.collect(projection.ProjectionCollector(self.cache))

    def test_closed_shape_payload_types_profiles_and_binding_are_checked(self):
        variants = [dict(self.payload, credentials="secret"), dict(self.payload, tree_fetches=True),
                    dict(self.payload, raw_bytes_hashed=1 << 64), dict(self.payload, schema_version=True),
                    dict(self.payload, root_tree_oid="sha256:" + "b" * 64),
                    dict(self.payload, native_certificate_receipt_id=0),
                    dict(self.payload, scope="/project/"), dict(self.payload, snapshot_id="sha256:" + "d" * 64),
                    dict(self.payload, native_publication_sequence=4)]
        for value in variants:
            with self.subTest(value=value):
                self.write([value])
                with self.assertRaises(projection.TraceRejected):
                    self.collect(projection.ProjectionCollector(self.cache))
        self.write([self.payload])
        self.measured["descriptor_bytes_hex"] = "00"
        with self.assertRaises(projection.TraceRejected):
            self.collect(projection.ProjectionCollector(self.cache))

    def test_raw_payload_hash_duplicate_keys_truncation_and_duplicate_request_fail(self):
        line = self.line(self.payload, 1)
        bad = [line.replace(b'"tree_fetches":0', b'"tree_fetches":1'),
               line.replace(b'"tree_fetches":0', b'"tree_fetches":0,"tree_fetches":0'),
               line.replace(b'"writer_revision":1', b'"writer_revision":1,"writer_revision":1'),
               line[:-2] + b"\n"]
        for raw in bad:
            self.write([self.payload], raw=raw)
            with self.assertRaises(projection.TraceRejected):
                self.collect(projection.ProjectionCollector(self.cache))
        self.write([self.payload, self.payload])
        with self.assertRaises(projection.TraceRejected):
            self.collect(projection.ProjectionCollector(self.cache))

    def test_file_and_line_caps_and_changed_record_inode_fail(self):
        self.write([self.payload], raw=b"x" * (projection.FILE_BYTES + 1))
        with self.assertRaises(projection.TraceRejected):
            self.collect(projection.ProjectionCollector(self.cache))
        self.write([self.payload], raw=b" " * projection.RECORD_BYTES + self.line(self.payload, 1))
        with self.assertRaises(projection.TraceRejected):
            self.collect(projection.ProjectionCollector(self.cache))
        self.write([self.payload])
        collector = projection.ProjectionCollector(self.cache)
        self.collect(collector)
        replacement = self.root / "other"
        replacement.write_bytes((self.root / "records.jsonl").read_bytes())
        replacement.chmod(0o600)
        os.replace(replacement, self.root / "records.jsonl")
        with self.assertRaises(projection.TraceRejected):
            collector.snapshot()

    def test_unknown_request_bad_receipt_or_duplicate_logical_call_fail(self):
        self.write([dict(self.payload, request_id="unknown:a1")])
        with self.assertRaises(projection.TraceRejected):
            self.collect(projection.ProjectionCollector(self.cache))
        self.write([self.payload])
        for changes in ({"retry_count": True}, {"final_attempt_id": "wrong:a1"},
                        {"attempt_ids": [self.logical + ":a2"]}, {"extra": 0}):
            original = self.measured["resolve_trace_receipt"]
            self.measured["resolve_trace_receipt"] = dict(original, **changes)
            with self.assertRaises(projection.TraceRejected):
                self.collect(projection.ProjectionCollector(self.cache))
            self.measured["resolve_trace_receipt"] = original
        collector = projection.ProjectionCollector(self.cache)
        self.collect(collector)
        with self.assertRaises(projection.TraceRejected):
            self.collect(collector)

    @unittest.skipUnless(os.name == "posix", "actual Unix symlink and mode checks")
    def test_symlink_private_modes_and_multiple_sink_directories_fail(self):
        self.write([self.payload])
        self.parent.chmod(0o755)
        with self.assertRaises(projection.TraceRejected):
            projection.ProjectionCollector(self.cache)
        self.parent.chmod(0o700)
        (self.root / "records.jsonl").rename(self.root / "actual")
        (self.root / "records.jsonl").symlink_to(self.root / "actual")
        with self.assertRaises(projection.TraceRejected):
            self.collect(projection.ProjectionCollector(self.cache))
        (self.parent / str(uuid.uuid4())).mkdir(mode=0o700)
        with self.assertRaises(projection.TraceRejected):
            projection.ProjectionCollector(self.cache)

    def test_explicit_dispatch_anchors_do_not_reset_on_queue_or_recovery(self):
        start = datetime.now(timezone.utc)
        deadline = start + timedelta(minutes=235)
        anchor = projection.window_anchor(start.isoformat(), deadline.isoformat(), admission=True)
        self.assertAlmostEqual(anchor - time.monotonic(), 220 * 60, delta=.1)
        for first, last in ((None, None), (start.isoformat(), ""),
                            (start.replace(tzinfo=None).isoformat(), deadline.isoformat()),
                            (start.isoformat(), (deadline + timedelta(seconds=1)).isoformat())):
            with self.assertRaises(projection.TraceRejected):
                projection.window_anchor(first, last, admission=True)
        queued = start - timedelta(minutes=16)
        with self.assertRaises(projection.TraceRejected):
            projection.window_anchor(queued.isoformat(), (queued + timedelta(minutes=235)).isoformat(), admission=True)
        self.assertLess(projection.window_anchor(queued.isoformat(), (queued + timedelta(minutes=235)).isoformat()), anchor)


@unittest.skipUnless(os.name == "posix" and Path("/proc").is_dir() and shutil.which("cc"),
                     "actual owned Linux process and C fixture")
class OwnedProjectionDrainTests(unittest.TestCase):
    def start(self, root, ignore=False):
        source = root / "fixture.c"
        source.write_text('''#include <signal.h>
#include <stdio.h>
#include <unistd.h>
static volatile sig_atomic_t stopped;
static void stop(int sig) { (void)sig; stopped = 1; }
int main(int argc, char **argv) {
  if (argc != 3) return 2;
  signal(SIGINT, ''' + ("SIG_IGN" if ignore else "stop") + ''');
  FILE *stream = fopen(argv[2], "a"); if (!stream) return 3;
  fputs("ready\\n", stream); fflush(stream); fsync(fileno(stream));
  while (!stopped) usleep(1000);
  fputs("closed\\n", stream); fflush(stream); fsync(fileno(stream));
  fclose(stream); return 0;
}
''')
        binary = root / "owned-fixture"
        subprocess.run(["cc", str(source), "-o", str(binary)], check=True, timeout=10, capture_output=True)
        config = root / "service.toml"
        config.write_text("")
        dependencies = root / "dependencies.json"
        dependencies.write_text("{}")
        process = budget.PinnedProcess([str(binary), "--config", str(config)], start_new_session=True,
                                       stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        started = budget.process_start(process.pid)
        self.addCleanup(lambda: budget.stop_group(process.pid, started, time.monotonic() + 2, process)
                        if process.returncode is None else None)
        until = time.monotonic() + 2
        while "ready" not in config.read_text() and time.monotonic() < until:
            time.sleep(.01)
        self.assertIn("ready", config.read_text())
        state = {"project": "owned-test", "binary": str(binary),
                 "compose_sha256": projection.digest(dependencies.read_bytes())[7:],
                 "service": {"pid": process.pid, "pgid": process.pid, "sid": process.pid, "starttime": started}}
        (root / "owned.json").write_text(json.dumps(state))
        return process, config, state

    def test_actual_owned_sigint_drain_pins_leader_until_final_group_check_and_reap(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            process, config, state = self.start(root)
            self.assertIsNone(ci.owned_service_exit(process))
            self.assertIsNone(process.returncode)
            ci.graceful_owned(root, state["project"], time.monotonic() + 2, process)
            self.assertEqual(process.returncode, 0)
            self.assertIn("closed", config.read_text())
            self.assertNotIn("service", json.loads((root / "owned.json").read_text()))

    def test_changed_ownership_refuses_sigint_and_ignored_sigint_has_no_new_deadline(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            process, config, state = self.start(root, ignore=True)
            with self.assertRaises(AssertionError):
                ci.graceful_owned(root, "other-project", time.monotonic() + 1, process)
            self.assertIsNone(ci.owned_service_exit(process))
            started = time.monotonic()
            with self.assertRaises(TimeoutError):
                ci.graceful_owned(root, state["project"], started + .05, process)
            self.assertLess(time.monotonic() - started, .3)
            self.assertNotIn("closed", config.read_text())
            budget.stop_group(process.pid, state["service"]["starttime"], time.monotonic() + 1, process)


if __name__ == "__main__":
    unittest.main()
