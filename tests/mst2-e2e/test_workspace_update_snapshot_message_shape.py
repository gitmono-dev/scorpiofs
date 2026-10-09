"""Closed create-failure diagnostics with real HTTP and safe export coverage."""

from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest

import commit_update_bench as bench
import commit_update_ci as ci
import workspace_update_campaign_export as export_module
import workspace_update_worker as worker


PREFIX = "workspace 11111111-2222-4333-8444-666666666666: "
PRIVATE = "https://private.invalid/path?token=PRIVATE_SENTINEL"


class SnapshotMessageShapeTests(unittest.TestCase):
    def setUp(self):
        self.response = {"code": "SNAPSHOT_ERROR", "message": ""}
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
                raw = json.dumps(owner.response).encode()
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={"poll_interval": 0.01}, daemon=True)
        self.thread.start()
        self.addCleanup(self.close)

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def request_error(self, detail, *, code="SNAPSHOT_ERROR", prefix=PREFIX):
        self.response = {"code": code, "message": prefix + detail}
        client = worker._NoRedirectHTTP("http://127.0.0.1:" + str(self.server.server_port))
        with self.assertRaises(worker.WorkerError) as raised:
            client.request("POST", "/v3/workspaces", time.monotonic() + 5, {})
        return raised.exception

    def test_actual_http_distinguishes_three_shapes_without_retaining_detail(self):
        cases = [
            ("snapshot request deadline exceeded", "exact_request_deadline"),
            ("network: " + PRIVATE, "network_prefix"),
            ("service temporarily unavailable " + PRIVATE, "other_temporary_unavailable"),
        ]
        for detail, shape in cases:
            with self.subTest(shape=shape):
                error = self.request_error("TemporaryUnavailable: " + detail)
                record = bench.failure_record(error)
                self.assertEqual(record, {
                    "execution_failed": True, "error_type": "WorkerError",
                    "error_code": "worker_http_status_5xx", "backend_code": "SNAPSHOT_ERROR",
                    "snapshot_code": "TemporaryUnavailable", "snapshot_message_shape": shape,
                })
                for text in (str(error), json.dumps(record)):
                    self.assertNotIn(PRIVATE, text)
                    self.assertNotIn("11111111", text)
                    self.assertNotIn(detail, text)

    def test_deadline_match_is_exact_and_network_match_is_only_a_prefix(self):
        for detail in ("snapshot request deadline exceeded " + PRIVATE,
                       "snapshot request deadline exceeded\n",
                       "network:", "NETWORK: " + PRIVATE, " network: " + PRIVATE):
            with self.subTest(detail=detail):
                error = self.request_error("TemporaryUnavailable: " + detail)
                self.assertEqual(error.snapshot_message_shape, "other_temporary_unavailable")

    def test_invalid_envelopes_and_other_codes_have_no_shape(self):
        cases = [
            {"detail": "ObjectUnavailable: network: " + PRIVATE},
            {"detail": "UnknownCode: snapshot request deadline exceeded"},
            {"detail": "TemporaryUnavailable: network: " + PRIVATE, "prefix": "workspace private: "},
            {"detail": "TemporaryUnavailable: network: " + PRIVATE, "prefix": ""},
            {"detail": "TemporaryUnavailable: network: " + PRIVATE, "code": "WORKSPACE_IO"},
            {"detail": "TemporaryUnavailable: network: " + PRIVATE, "code": "PRIVATE_SENTINEL"},
        ]
        for case in cases:
            with self.subTest(case=case):
                error = self.request_error(**case)
                self.assertIsNone(error.snapshot_message_shape)
                self.assertNotIn("snapshot_message_shape", bench.failure_record(error))

    def test_parser_rejects_non_string_and_malformed_messages(self):
        for message in (None, [], {}, 5, PREFIX + "TemporaryUnavailable",
                        PREFIX + "temporaryUnavailable: network: " + PRIVATE):
            with self.subTest(message=message):
                self.assertIsNone(worker._snapshot_message_shape_from_workspace_message(message))

    def test_constructor_and_serializer_drop_unknown_or_inapplicable_shapes(self):
        for shape in (PRIVATE, None, [], {}, 1, True):
            with self.subTest(shape=shape):
                error = worker.WorkerError(PRIVATE, backend_code="SNAPSHOT_ERROR",
                    snapshot_code="TemporaryUnavailable", snapshot_message_shape=shape)
                self.assertIsNone(error.snapshot_message_shape)
                # Exercise the export boundary independently of constructor validation.
                error.snapshot_message_shape = shape
                self.assertNotIn("snapshot_message_shape", bench.failure_record(error))
                self.assertNotIn("snapshot_message_shape", bench.failure_record(bench.PhaseFailure("create", error)))
        for backend, snapshot in ((None, "TemporaryUnavailable"),
                                  ("WORKSPACE_IO", "TemporaryUnavailable"),
                                  ("SNAPSHOT_ERROR", "ObjectUnavailable")):
            error = worker.WorkerError(PRIVATE, backend_code=backend, snapshot_code=snapshot,
                                       snapshot_message_shape="network_prefix")
            self.assertIsNone(error.snapshot_message_shape)
            error.snapshot_message_shape = "network_prefix"
            self.assertNotIn("snapshot_message_shape", bench.failure_record(error))

    def test_phase_group_and_persisted_safe_export_keep_only_closed_shape(self):
        error = self.request_error("TemporaryUnavailable: network: " + PRIVATE)
        worker._tag_worker_stage(error, "create")
        phase = bench.PhaseFailure("shipped_workspace_and_git_measurement", error)
        record = bench.failure_record(phase)
        self.assertEqual(record["snapshot_message_shape"], "network_prefix")
        self.assertEqual(record["worker_stage"], "create")
        group = ExceptionGroup(PRIVATE, [phase, RuntimeError(PRIVATE)])
        self.assertEqual(bench.failure_record(group)["failures"][0], record)
        with tempfile.TemporaryDirectory() as temp:
            root, safe = Path(temp) / "owned", Path(temp) / "safe"
            (root / "measurements").mkdir(parents=True)
            ci.persist_failure_record(root, group)
            deadline = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
            copied = export_module.export(root, safe, deadline, complete_allowed=False)
            self.assertEqual(set(copied), {"measurements/failure.json"})
            exported = (safe / "measurements/failure.json").read_bytes()
            self.assertEqual(json.loads(exported), bench.failure_record(group))
            self.assertNotIn(b"PRIVATE_SENTINEL", exported)
            self.assertFalse(json.loads((safe / "safe-export.json").read_bytes())["complete_campaign"])


if __name__ == "__main__":
    unittest.main()
