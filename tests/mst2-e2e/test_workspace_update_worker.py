import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock
import uuid

from workspace_update_worker import (
    STATUS_FIELDS,
    WorkerError,
    WorkerSession,
    _NoRedirectHTTP,
    _check_deadline,
    _status,
)


def valid_status(**changes):
    value = {
        "workspace_id": "11111111-2222-4333-8444-666666666666",
        "generation": "22222222-2222-4333-8444-666666666666",
        "snapshot_id": "sha256:" + "a" * 64,
        "mountpoint": "/private/workspaces-v3/11111111-2222-4333-8444-666666666666/mount",
        "mount_state": "mounted",
        "metadata_ready": True,
        "hydration_state": "complete",
        "dirty_state": "unknown",
        "lease_state": "granted_locally",
        "local_pin_state": "complete_snapshot",
        "last_error": None,
    }
    value.update(changes)
    return value


class WorkerShapeTests(unittest.TestCase):
    def test_status_requires_exact_wire_shape_and_canonical_identity(self):
        self.assertEqual(set(_status(valid_status())), STATUS_FIELDS)
        for key, value in (("metadata_ready", 1), ("snapshot_id", "sha256:" + "A" * 64),
                           ("workspace_id", str(uuid.UUID(int=0)))):
            invalid = valid_status(**{key: value})
            with self.assertRaises(WorkerError):
                _status(invalid)

    def test_absolute_deadline_rejects_before_network_or_sleep(self):
        with self.assertRaises(TimeoutError):
            _check_deadline(time.monotonic() - 1)
        client = _NoRedirectHTTP("http://127.0.0.1:1")
        with self.assertRaises(TimeoutError):
            client.request("GET", "/health", time.monotonic() - 1)


class WorkerReceiptTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.workspace = self.root / "workspaces-v3"
        self.workspace.mkdir(mode=0o700)
        self.git = self.root / "git.git"
        self.git.mkdir(mode=0o700)
        self.workers = []
        self.addCleanup(self.cleanup_workers)

    def cleanup_workers(self):
        for worker in self.workers:
            try:
                worker.abort(time.monotonic() + 10)
            except Exception:
                pass

    def worker(self):
        worker = WorkerSession(self.root, "http://127.0.0.1:1", self.workspace, self.git,
                               "http://127.0.0.1:1/project", {},
                               deadline=time.monotonic() + 10, env={"PATH": os.environ.get("PATH", "")},
                               daemon_uid=os.getuid() if hasattr(os, "getuid") else 0)
        self.workers.append(worker)
        return worker

    def test_pending_receipt_is_written_before_any_child(self):
        worker = self.worker()
        receipt = json.loads((self.root / "owned-workspace-worker.json").read_text())
        self.assertFalse(receipt["cleanup_complete"])
        if sys.platform == "linux":
            self.assertIsInstance(receipt["pid"], int)
            self.assertIsInstance(receipt["starttime"], str)
        else:
            self.assertIsNone(receipt["pid"])
            self.assertIsNone(receipt["starttime"])

    def test_child_identity_and_completion_require_empty_owned_group(self):
        worker = self.worker()
        if os.name != "posix":
            self.skipTest("native process-group receipt is POSIX-only")
        output = worker._owned_command([sys.executable, "-c", "print('worker')"], time.monotonic() + 10)
        self.assertEqual(output.strip(), b"worker")
        pending = json.loads((self.root / "owned-workspace-worker.json").read_text())
        if sys.platform == "linux":
            self.assertIsInstance(pending["pid"], int)
            self.assertIsInstance(pending["starttime"], str)
        else:
            self.assertIsNone(pending["pid"])
            self.assertIsNone(pending["starttime"])
            self.skipTest("native process-group receipt is Linux-only")
        self.assertFalse(pending["cleanup_complete"])
        worker._stop_anchor(time.monotonic() + 10)
        worker._complete_receipt()
        complete = json.loads((self.root / "owned-workspace-worker.json").read_text())
        self.assertTrue(complete["cleanup_complete"])

    def test_commands_join_one_worker_owned_process_group(self):
        if sys.platform != "linux":
            self.skipTest("native process-group join is Linux-only")
        worker = self.worker()
        anchor_pid = worker._anchor_identity[0]
        observed = worker._owned_command(
            [sys.executable, "-c", "import os; print(os.getpgrp())"],
            time.monotonic() + 10,
        )
        self.assertEqual(int(observed.strip()), anchor_pid)
        worker._stop_anchor(time.monotonic() + 10)
        worker._complete_receipt()

    def test_timeout_kills_blocking_command_and_descendant(self):
        if sys.platform != "linux":
            self.skipTest("owned RPC descendant fence is Linux-only")
        worker = self.worker()
        marker = self.root / "blocking-grandchild.pid"
        script = (
            "import subprocess,sys,time; "
            "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(600)']); "
            "open(sys.argv[1],'w').write(str(p.pid)); time.sleep(600)"
        )
        anchor_pid, anchor_start = worker._anchor_identity
        with self.assertRaises((TimeoutError, WorkerError)):
            worker._owned_command([sys.executable, "-c", script, str(marker)],
                                  time.monotonic() + 1)
        try:
            worker.abort(time.monotonic() + 10)
        except WorkerError:
            # The test daemon endpoint is intentionally absent; the abort
            # path must still fence the anchor group before reporting it.
            pass
        if marker.exists():
            grandchild = int(marker.read_text())
            for _ in range(100):
                if not Path(f"/proc/{grandchild}").exists():
                    break
                time.sleep(.01)
            self.assertFalse(Path(f"/proc/{grandchild}").exists())
        self.assertEqual(__import__("commit_update_budget").group_members(anchor_pid, anchor_start), [])

    def test_complete_receipt_refuses_active_group_or_mount(self):
        worker = self.worker()
        worker._last_identity = (os.getpid(), "1")
        with mock.patch.object(worker, "_groups_empty", return_value=False):
            with self.assertRaises(WorkerError):
                worker._complete_receipt()


if __name__ == "__main__":
    unittest.main()
