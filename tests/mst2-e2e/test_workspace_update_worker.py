import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock
import uuid

from workspace_update_worker import (
    HTTP_BODY_LIMIT,
    HYDRATION_SUBSTAGES,
    ORACLE_MANIFEST_LIMIT,
    RETENTION_SUBSTAGES,
    STATUS_FIELDS,
    WORKER_STAGES,
    WorkerError,
    WorkerSession,
    _NoRedirectHTTP,
    _check_deadline,
    _status_error_codes,
    _status_hydration_substage,
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
    def test_worker_error_codes_are_fixed_and_status_values_are_not_embedded(self):
        status = WorkerError("worker HTTP status 503 was not accepted")
        self.assertEqual(status.error_code, "worker_http_status_5xx")
        self.assertNotIn("503", status.error_code)
        self.assertEqual(WorkerError("worker HTTP status 409 was not accepted").error_code,
                         "worker_http_status_4xx")
        self.assertEqual(WorkerError("worker HTTP status 302 was not accepted").error_code,
                         "worker_http_status_other")
        self.assertEqual(WorkerError("daemon leaked token", error_code="token").error_code,
                         "worker_error")
        for message, expected in [
            ("workspace hydration failed", "workspace_hydration_failed"),
            ("retained old file differs from its fixed snapshot", "workspace_retention_invalid"),
            ("workspace FUSE mount identity is missing or ambiguous", "workspace_mount_invalid"),
            ("Linux mount ownership inventory is unavailable", "worker_process_invalid"),
            ("workspace mounts remained after explicit retirement", "worker_cleanup_invalid"),
        ]:
            self.assertEqual(WorkerError(message).error_code, expected)

    def test_backend_error_code_is_closed(self):
        self.assertEqual(WorkerError("private", backend_code="SNAPSHOT_ERROR").backend_code,
                         "SNAPSHOT_ERROR")
        self.assertIsNone(WorkerError("private", backend_code="private-token").backend_code)

    def test_snapshot_error_code_is_closed(self):
        self.assertEqual(WorkerError("private", snapshot_code="ObjectUnavailable").snapshot_code,
                         "ObjectUnavailable")
        self.assertIsNone(WorkerError("private", snapshot_code="private-token").snapshot_code)

    def test_status_error_codes_are_closed_and_discard_details(self):
        self.assertEqual(_status_error_codes(valid_status(last_error="WORKSPACE_IO: /private/path")),
                         ("WORKSPACE_IO", None))
        self.assertEqual(_status_error_codes(valid_status(last_error="ObjectUnavailable: private body")),
                         (None, "ObjectUnavailable"))
        self.assertEqual(_status_error_codes(valid_status(last_error=(
            "workspace 11111111-2222-4333-8444-666666666666: LeaseExpired: private body"))),
                         (None, "LeaseExpired"))
        self.assertEqual(_status_error_codes(valid_status(last_error="PrivateToken: secret")),
                         (None, None))
        self.assertEqual(_status_error_codes(valid_status(last_error=None)), (None, None))

    def test_status_hydration_substage_is_closed_and_separate_from_snapshot_code(self):
        self.assertEqual(_status_hydration_substage(
            valid_status(last_error="IntegrityError: dependency_audit")),
            "dependency_audit")
        self.assertEqual(_status_hydration_substage(
            valid_status(last_error="Internal: hydration_task")),
            "hydration_task")
        self.assertIsNone(_status_hydration_substage(
            valid_status(last_error="IntegrityError: /private/path")))
        self.assertEqual(HYDRATION_SUBSTAGES, {
            "metadata_closure", "cas_resume_audit", "small_object_fetch",
            "large_content_fetch", "hydration_commit", "snapshot_links",
            "dependency_audit", "hydration_task",
        })

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

    def test_worker_stage_is_closed_and_preserves_nested_specific_stage(self):
        worker = WorkerSession.__new__(WorkerSession)
        self.assertEqual(WORKER_STAGES, {
            "create", "hydrate", "poll", "oracle", "retained", "git", "destroy", "cleanup",
        })
        with self.assertRaises(WorkerError) as failed:
            with worker._stage("hydrate"):
                with worker._stage("poll"):
                    raise WorkerError("private response body", error_code="worker_http_status_5xx")
        self.assertEqual(failed.exception.worker_stage, "poll")
        self.assertEqual(WorkerError("private", stage="private").worker_stage, None)
        self.assertEqual(WorkerError("private", stage=[]).worker_stage, None)

    def test_retention_substage_is_closed(self):
        self.assertEqual(RETENTION_SUBSTAGES, {
            "retain_path", "upper_check", "sentinel_write", "retained_fd",
            "old_view_oracle", "old_view_fd", "old_view_sentinel", "final_view_oracle",
        })
        self.assertEqual(WorkerError("private", retention_substage="old_view_oracle").retention_substage,
                         "old_view_oracle")
        self.assertIsNone(WorkerError("private", retention_substage="private-path").retention_substage)
        worker = WorkerSession.__new__(WorkerSession)
        with self.assertRaises(WorkerError) as failed:
            with worker._retention_substage("old_view_oracle"):
                raise WorkerError("private response body")
        self.assertEqual(failed.exception.retention_substage, "old_view_oracle")


class WorkerFullProfileTests(unittest.TestCase):
    """Exercise the worker against a control endpoint that rejects lazy."""

    def setUp(self):
        self.create_status = valid_status(hydration_state="idle", local_pin_state="incomplete")
        self.poll_statuses = [valid_status()]
        self.requests = []
        profile = self

        class FullOnlyHandler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_POST(self):
                self.respond()

            def do_GET(self):
                self.respond()

            def respond(self):
                raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                body = json.loads(raw) if raw else None
                profile.requests.append((self.command, self.path, body))
                workspace_path = "/v3/workspaces/" + profile.create_status["workspace_id"]
                code = 200
                if self.command == "POST" and self.path == "/v3/workspaces":
                    if type(body) is not dict or body.get("delivery") != "full":
                        code, value = 422, {"error": "delivery unsupported"}
                    else:
                        value = profile.create_status
                elif self.command == "POST" and self.path == "/error":
                    code, value = 500, {
                        "code": "SNAPSHOT_ERROR",
                        "message": "workspace 11111111-2222-4333-8444-666666666666: ObjectUnavailable: private details",
                    }
                elif self.command == "POST" and self.path == "/unknown-snapshot":
                    code, value = 500, {
                        "code": "SNAPSHOT_ERROR",
                        "message": "workspace 11111111-2222-4333-8444-666666666666: PrivateToken: private details",
                    }
                elif self.command == "GET" and self.path == workspace_path and profile.poll_statuses:
                    value = profile.poll_statuses.pop(0)
                else:
                    code, value = 404, {"error": "unexpected request"}
                encoded = json.dumps(value).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), FullOnlyHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.01},
                                       daemon=True)
        self.thread.start()
        self.addCleanup(self.close_server)

    def close_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def worker(self):
        worker = WorkerSession.__new__(WorkerSession)
        worker.http = _NoRedirectHTTP("http://127.0.0.1:" + str(self.server.server_port))
        worker._workspace_ids = []
        # This control-plane test has no native FUSE mount. Identity equality
        # and the complete wire shape still use the production validators.
        worker._assert_status_path = mock.Mock()
        return worker

    def test_full_only_create_waits_for_automatic_hydration_and_complete_snapshot_pin(self):
        worker = self.worker()
        deadline = time.monotonic() + 5
        # Full create starts hydration asynchronously.  The worker must only
        # observe status; an explicit /hydrate would be an unfair second
        # hydration and can race the automatically started task.
        self.poll_statuses = [valid_status(hydration_state="running", local_pin_state="incomplete"),
                              valid_status()]
        started = time.monotonic()
        first = worker._create_workspace(deadline)
        status, metadata_ms, complete_ms = worker._hydrate(first, deadline, started,
                                                          initial_metadata_ms=7.25)
        workspace_path = "/v3/workspaces/" + first["workspace_id"]
        self.assertEqual(self.requests, [
            ("POST", "/v3/workspaces", {"target": {"kind": "latest"}, "scope": "/project",
                                       "delivery": "full", "upper_policy": "private"}),
            ("GET", workspace_path, None), ("GET", workspace_path, None),
        ])
        self.assertEqual(status, valid_status())
        self.assertEqual(worker._workspace_ids, [first["workspace_id"]])
        self.assertEqual(metadata_ms, 7.25)
        self.assertGreaterEqual(complete_ms, 0)

    def test_http_backend_error_code_is_captured_without_response_message(self):
        worker = self.worker()
        with self.assertRaises(WorkerError) as failed:
            worker.http.request("POST", "/error", time.monotonic() + 5, {}, expected=(200,))
        self.assertEqual(failed.exception.error_code, "worker_http_status_5xx")
        self.assertEqual(failed.exception.backend_code, "SNAPSHOT_ERROR")
        self.assertEqual(failed.exception.snapshot_code, "ObjectUnavailable")
        self.assertNotIn("private details", str(failed.exception))

        with self.assertRaises(WorkerError) as unknown:
            worker.http.request("POST", "/unknown-snapshot", time.monotonic() + 5, {}, expected=(200,))
        self.assertEqual(unknown.exception.backend_code, "SNAPSHOT_ERROR")
        self.assertIsNone(unknown.exception.snapshot_code)

    def test_full_profile_rejects_fixed_identity_changes_while_polling(self):
        changes = {"workspace_id": "33333333-2222-4333-8444-666666666666",
                   "generation": "33333333-2222-4333-8444-666666666666",
                   "snapshot_id": "sha256:" + "b" * 64,
                   "mountpoint": "/private/workspaces-v3/changed/mount"}
        for field, value in changes.items():
            with self.subTest(field=field):
                self.requests.clear()
                changed = valid_status(**{field: value})
                self.poll_statuses = [changed]
                worker = self.worker()
                deadline = time.monotonic() + 5
                first = worker._create_workspace(deadline)
                with self.assertRaises(WorkerError) as failed:
                    worker._hydrate(first, deadline, time.monotonic())
                self.assertEqual(failed.exception.error_code, "workspace_identity_invalid")
                self.assertEqual(failed.exception.worker_stage, "poll")
                self.assertEqual(worker._workspace_ids, [first["workspace_id"]])
                self.assertEqual(len(self.requests), 2)

    def test_full_create_already_complete_does_not_issue_hydrate_request(self):
        self.create_status = valid_status()
        self.poll_statuses = []
        worker = self.worker()
        started = time.monotonic()
        first = worker._create_workspace(time.monotonic() + 5)
        status, metadata_ms, complete_ms = worker._hydrate(first, time.monotonic() + 5, started)
        self.assertEqual(status, first)
        self.assertGreaterEqual(metadata_ms, 0)
        self.assertGreaterEqual(complete_ms, 0)
        self.assertEqual(self.requests, [
            ("POST", "/v3/workspaces", {"target": {"kind": "latest"}, "scope": "/project",
                                       "delivery": "full", "upper_policy": "private"}),
        ])

    def test_full_profile_rejects_create_without_fixed_snapshot(self):
        self.create_status = valid_status(snapshot_id=None)
        worker = self.worker()
        with self.assertRaises(WorkerError) as failed:
            worker._create_workspace(time.monotonic() + 5)
        self.assertEqual(failed.exception.error_code, "workspace_identity_invalid")
        self.assertEqual(failed.exception.worker_stage, "create")
        self.assertEqual(worker._workspace_ids, [])
        self.assertEqual(self.requests[0][2]["delivery"], "full")


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

    def test_medium_manifest_uses_local_oracle_budget(self):
        worker = self.worker()
        files = [{"rel_path": f"file-{index:06d}", "fs_kind": "regular", "size": 0,
                  "content_digest": "sha256:" + "0" * 64} for index in range(40000)]
        expected = {"files": files, "directories": [""]}
        path = self.root / "medium-expected.json"
        raw = json.dumps(expected, separators=(",", ":")).encode("utf-8")
        self.assertGreater(len(raw), HTTP_BODY_LIMIT)
        self.assertLess(len(raw), ORACLE_MANIFEST_LIMIT)
        path.write_bytes(raw)
        self.assertEqual(len(worker._load_expected(path)["files"]), len(files))

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
                proc = Path(f"/proc/{grandchild}")
                if not proc.exists():
                    break
                try:
                    state = proc.joinpath("stat").read_text().rsplit(") ", 1)[1].split()[0]
                except (FileNotFoundError, ProcessLookupError):
                    break
                # PID 1 on a hosted/containerized runner may retain an orphan
                # as a zombie after the owned process group is dead. That is
                # no longer executable work and is covered by group_members.
                if state in ("Z", "X"):
                    break
                time.sleep(.01)
            proc = Path(f"/proc/{grandchild}")
            if proc.exists():
                try:
                    state = proc.joinpath("stat").read_text().rsplit(") ", 1)[1].split()[0]
                except (FileNotFoundError, ProcessLookupError):
                    state = None
                self.assertIn(state, (None, "Z", "X"))
        self.assertEqual(__import__("commit_update_budget").group_members(anchor_pid, anchor_start), [])

    def test_complete_receipt_refuses_active_group_or_mount(self):
        worker = self.worker()
        worker._last_identity = (os.getpid(), "1")
        with mock.patch.object(worker, "_groups_empty", return_value=False):
            with self.assertRaises(WorkerError):
                worker._complete_receipt()


if __name__ == "__main__":
    unittest.main()
