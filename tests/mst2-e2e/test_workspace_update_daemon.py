"""Cleanup rejection never suppresses independent owned-resource cleanup."""

import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest import mock

import commit_update_ci as ci


class WorkspaceCleanupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.round = self.root / "measurements/round-01"
        self.round.mkdir(parents=True)

    def receipts(self, value=None):
        value = value or {"pid": 12345, "starttime": "789", "cleanup_complete": True}
        for name in ("owned-workspace-daemon.json", "owned-workspace-worker.json"):
            (self.round / name).write_text(json.dumps(value))

    def test_pending_missing_and_unfinished_children_cannot_claim_cleanup(self):
        values = [None, {"pid": None, "starttime": None, "cleanup_complete": False},
                  {"pid": 12345, "starttime": "789", "cleanup_complete": False},
                  {"pid": True, "starttime": "789", "cleanup_complete": True}]
        with mock.patch("workspace_update_daemon.mounts_under", return_value=[]), \
                mock.patch.object(ci.budget_module, "group_members", return_value=[]):
            for value in values:
                for path in self.round.iterdir():
                    path.unlink()
                if value is not None:
                    self.receipts(value)
                with self.assertRaises(AssertionError):
                    ci.verify_workspace_cleanup(self.root / "measurements", time.monotonic() + 1)

    def test_final_receipts_also_require_actual_empty_process_groups_and_no_mounts(self):
        self.receipts()
        with mock.patch("workspace_update_daemon.mounts_under", return_value=[]), \
                mock.patch.object(ci.budget_module, "group_members", return_value=[]) as groups:
            ci.verify_workspace_cleanup(self.root / "measurements", time.monotonic() + 1)
            self.assertEqual(groups.call_count, 2)
        for mount, group in [(["actual mount"], []), ([], [54321])]:
            with mock.patch("workspace_update_daemon.mounts_under", return_value=mount), \
                    mock.patch.object(ci.budget_module, "group_members", return_value=group):
                with self.assertRaises(AssertionError):
                    ci.verify_workspace_cleanup(self.root / "measurements", time.monotonic() + 1)

    def test_workspace_rejection_still_cleans_confirmed_owned_compose_and_never_prints_pass(self):
        import hashlib
        compose = self.root / "dependencies.json"
        compose.write_bytes(b"exact owned dependencies")
        state = {"project": "owned-project", "compose_sha256": hashlib.sha256(compose.read_bytes()).hexdigest()}
        (self.root / "owned.json").write_text(json.dumps(state))
        calls = []

        def command(args, deadline):
            calls.append(args)
            return b""

        with mock.patch("workspace_update_daemon.mounts_under", return_value=[]), \
                mock.patch.object(ci.bench, "command", side_effect=command), \
                mock.patch("builtins.print") as printed:
            with self.assertRaisesRegex(AssertionError, "receipt is missing"):
                ci.stop_owned(self.root, "owned-project", time.monotonic() + 1)
            self.assertEqual(len(calls), 3)
            self.assertIn("down", calls[0])
            self.assertIn("--volumes", calls[0])
            printed.assert_not_called()


if __name__ == "__main__":
    unittest.main()
