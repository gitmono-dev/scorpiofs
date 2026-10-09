"""Create-only diagnostics cannot silently become benchmark evidence."""
import json
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

import workspace_update_backend_proof as proofs
import workspace_update_campaign as campaign
import workspace_update_campaign_export as exporter
from workspace_update_worker import WorkerError


def sources():
    result = {key: "1" * (40 if key.endswith("_sha") or key.endswith("_tree") else 64)
              for key in proofs.SOURCE_FIELDS - {"rustc_version", "cargo_version", "harness_files_sha256"}}
    result.update(rustc_version="rustc 1.98.1 fixture", cargo_version="cargo 1.98.1 fixture",
                  harness_files_sha256={path: "2" * 64 for path in proofs.SCRIPT_PATHS})
    return result


class CreateOnlyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.lane = SimpleNamespace(root=self.root, worker=Mock(), backend=SimpleNamespace(ports={"http": 54321}),
            daemon=SimpleNamespace(process=SimpleNamespace(pid=123), started="456", uid=1000))
        self.commit, self.tree = "a" * 40, "b" * 40
        self.capture = SimpleNamespace(evidence={"runtime": {"identity": {"project_commit": self.commit,
            "project_tree": self.tree}, "service_pid": 321, "service_starttime": "654"}, "sources": sources()})
        self.deadline = time.monotonic() + 60

    def invoke(self):
        return campaign.request_endpoint_diagnostic(self.lane, self.capture, self.commit, self.tree,
                                                     self.deadline, self.deadline)

    def test_returned_create_still_stops_before_any_git_oracle_measurement(self):
        with self.assertRaises(campaign.RequestEndpointDiagnosticFinished):
            self.invoke()
        self.lane.worker._create_workspace.assert_called_once_with(self.deadline)
        self.lane.worker.measure.assert_not_called()
        context = json.loads((self.root / "request-diagnostic-context.json").read_bytes())
        self.assertEqual(context["create_outcome"], "returned")
        self.assertFalse(context["formal_performance_accepted"])
        self.assertEqual(context["published_commit"], self.commit)

    def test_failed_native_create_keeps_original_closed_failure_without_response_text(self):
        error = WorkerError("PRIVATE_SENTINEL", error_code="worker_http_status_5xx",
                            backend_code="SNAPSHOT_ERROR", snapshot_code="TemporaryUnavailable",
                            snapshot_message_shape="exact_request_deadline")
        self.lane.worker._create_workspace.side_effect = error
        with self.assertRaises(WorkerError) as caught:
            self.invoke()
        self.assertIs(caught.exception, error)
        raw = (self.root / "request-diagnostic-context.json").read_bytes()
        self.assertNotIn(b"PRIVATE_SENTINEL", raw)
        self.assertEqual(json.loads(raw)["create_outcome"], "failed")
        self.lane.worker.measure.assert_not_called()

    def test_different_publication_is_rejected_before_native_create(self):
        self.capture.evidence["runtime"]["identity"]["project_commit"] = "c" * 40
        with self.assertRaises(proofs.ProofRejected):
            self.invoke()
        self.lane.worker._create_workspace.assert_not_called()
        self.assertFalse((self.root / "request-diagnostic-context.json").exists())


class ExportBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "owned"
        self.root.mkdir()
        self.output = Path(self.temp.name) / "safe"
        from datetime import datetime, timedelta, timezone
        self.deadline = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()

    def marker(self):
        (self.root / "request-diagnostic-mode.json").write_text(json.dumps(exporter.REQUEST_DIAGNOSTIC_MODE))

    def test_requested_mode_exports_as_partial_even_before_a_native_request(self):
        self.marker()
        exporter.export(self.root, self.output, self.deadline)
        manifest = json.loads((self.output / "safe-export.json").read_bytes())
        self.assertFalse(manifest["complete_campaign"])
        self.assertEqual(set(manifest["files_sha256"]), {"request-diagnostic-mode.json"})

    def test_diagnostic_mode_is_rejected_before_complete_campaign_replay(self):
        self.marker()
        (self.root / "campaign.json").write_text("{}")
        with self.assertRaises(proofs.ProofRejected):
            exporter.validate_complete(self.root)

    def test_trace_without_mode_is_rejected_and_never_copied(self):
        leaf = self.root / exporter.REQUEST_DIAGNOSTIC_LEAF
        leaf.mkdir(parents=True)
        (leaf / "request-diagnostic.json").write_text('{"PRIVATE_SENTINEL":true}')
        with self.assertRaises(proofs.ProofRejected):
            exporter.export(self.root, self.output, self.deadline)
        self.assertFalse(self.output.exists())

    def test_empty_closed_trace_can_export_early_startup_partial(self):
        self.marker()
        leaf = self.root / exporter.REQUEST_DIAGNOSTIC_LEAF
        leaf.mkdir(parents=True)
        trace = {"revision": 1, "diagnostic_only": True, "formal_performance_accepted": False,
                 "valid": True, "invalid_reason": None, "closed": True, "owner": {"pid": 1,
                 "starttime_ticks": "1", "uid": 0}, "listener_port": 1, "upstream_port": 1,
                 "limits": {"connections": 1, "records": 1, "header_bytes": 16384, "buffer_bytes": 65536},
                 "connections": [], "records": []}
        (leaf / "request-diagnostic.json").write_text(json.dumps(trace, separators=(",", ":")) + "\n")
        exporter.export(self.root, self.output, self.deadline)
        manifest = json.loads((self.output / "safe-export.json").read_bytes())
        self.assertFalse(manifest["complete_campaign"])
        self.assertIn("measurements/fair/round-01/client-a/request-diagnostic.json", manifest["files_sha256"])

    def test_diagnostics_are_allowlisted_only_for_first_A_lane(self):
        for filename in exporter.REQUEST_DIAGNOSTIC_FILES:
            self.assertTrue(exporter.allowed(exporter.REQUEST_DIAGNOSTIC_LEAF / filename))
            for other in ("measurements/fair/round-01/client-b", "measurements/fair/round-02/client-a",
                          "measurements/diagnostic/round-01/client-b"):
                self.assertFalse(exporter.allowed(Path(other) / filename))


if __name__ == "__main__":
    unittest.main()
