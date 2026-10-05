import concurrent.futures
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock


SCRIPT = Path(__file__).with_name("first-directory-ready.py")
spec = importlib.util.spec_from_file_location("first_directory_ready", SCRIPT)
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)


class Clock:
    def __init__(self):
        self.now = 100.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        if seconds < 0:
            raise AssertionError("negative wait")
        self.now += seconds


class ClockFuture(concurrent.futures.Future):
    """A running request that advances only a fake clock, never a real thread."""
    def __init__(self, clock, ready_at, value=None, error=None):
        super().__init__()
        self.clock = clock
        self.ready_at = ready_at
        self.value = value
        self.error = error
        self.waits = []
        self.set_running_or_notify_cancel()

    def done(self):
        if not super().done() and self.ready_at is not None and self.clock.now >= self.ready_at:
            if self.error is not None:
                self.set_exception(self.error)
            else:
                self.set_result(self.value)
        return super().done()

    def result(self, timeout=None):
        self.waits.append(timeout)
        if not self.done():
            if timeout is None:
                raise AssertionError("unbounded pending Future wait")
            if self.ready_at is None or self.ready_at > self.clock.now + timeout:
                self.clock.sleep(timeout)
                raise concurrent.futures.TimeoutError()
            self.clock.sleep(max(0, self.ready_at - self.clock.now))
            self.done()
        return super().result(timeout=0)


class Executor:
    def __init__(self, clock, create, listing):
        self.clock = clock
        self.create = create
        self.listing = listing
        self.submissions = []
        self.futures = []
        self.deletion = None
        self.shutdown = mock.Mock(side_effect=self._shutdown)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.shutdown(wait=True)

    def _shutdown(self, wait=True, **kwargs):
        if wait:
            for future in self.futures:
                if not future.done():
                    future.result()  # Detect the production context-manager's implicit wait.

    def submit(self, function, *args, **kwargs):
        self.submissions.append((function, args, kwargs))
        if len(args) > 1 and args[1] == "POST":
            future = self.create
        elif len(args) > 1 and args[1] == "DELETE":
            if self.deletion is not None:
                self.futures.append(self.deletion)
                return self.deletion
            try:
                value = function(*args, **kwargs)
                future = ClockFuture(self.clock, self.clock.now, value=value)
            except Exception as error:
                future = ClockFuture(self.clock, self.clock.now, error=error)
        else:
            future = self.listing
        self.futures.append(future)
        return future


class HarnessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.work = Path(self.temp.name)
        self.clock = Clock()
        self.args = SimpleNamespace(
            work=self.work, timeout=1.0, cleanup_timeout=0.5,
            api="http://mock.invalid/antares", scope="/project", repo="mock-repo",
            commit="expected-commit", expected_dir=["big1m"],
        )
        self.monotonic = mock.patch.object(harness.time, "monotonic", self.clock.monotonic)
        self.sleep = mock.patch.object(harness.time, "sleep", self.clock.sleep)
        self.monotonic.start()
        self.sleep.start()
        self.addCleanup(self.monotonic.stop)
        self.addCleanup(self.sleep.stop)
        for owner, name in (
            (harness.subprocess, "Popen"), (harness.subprocess, "check_output"),
            (harness.subprocess, "run"), (harness.urllib.request, "build_opener"),
        ):
            guard = mock.patch.object(owner, name, side_effect=AssertionError("unmocked external action"))
            guard.start()
            self.addCleanup(guard.stop)

    def assert_invalid(self, result):
        self.assertEqual(result["status"], "failed")
        self.assertFalse(result["valid"])

    def test_listing_requires_all_expected_directories_and_boolean_types(self):
        for entries in ({"unrelated": True}, {"big1m": False}, {"big1m": "false"}):
            with self.subTest(entries=entries), mock.patch.object(
                harness.subprocess, "check_output", return_value=json.dumps(entries)
            ):
                with self.assertRaises(TimeoutError):
                    harness.listing_probe(self.work, {"big1m"}, self.clock.now + 0.1)

    def test_listing_retries_until_verified_and_returns_evidence(self):
        with mock.patch.object(harness.subprocess, "check_output", side_effect=[
            subprocess.TimeoutExpired("probe", 0.1), '{"big1m": false}',
            '{"big1m": true, "other": false}',
        ]):
            stamp, names = harness.listing_probe(self.work, {"big1m"}, self.clock.now + 1)
        self.assertAlmostEqual(stamp, 100.05)
        self.assertEqual(names, ["big1m", "other"])

    def test_listing_poll_sleep_does_not_extend_deadline(self):
        with mock.patch.object(harness.subprocess, "check_output", return_value='{"big1m": false}'):
            with self.assertRaises(TimeoutError):
                harness.listing_probe(self.work, {"big1m"}, 100.01)
        self.assertAlmostEqual(self.clock.now, 100.01)

    def test_listing_evidence_after_deadline_is_not_success(self):
        def output(*args, **kwargs):
            self.clock.sleep(0.2)
            return '{"big1m": true}'

        with mock.patch.object(harness.subprocess, "check_output", side_effect=output):
            with self.assertRaises(TimeoutError):
                harness.listing_probe(self.work, {"big1m"}, 100.1)

    def git_case(self, failure=None, kill_fallback=False, visible=True):
        process = mock.Mock()
        process.returncode = None
        process.poll.side_effect = lambda: process.returncode
        waits = []

        def wait(timeout=None):
            waits.append(timeout)
            if len(waits) == 1:
                if failure == "timeout":
                    self.clock.sleep(timeout)
                    raise subprocess.TimeoutExpired("clone", timeout)
                self.clock.sleep(0.1)
                process.returncode = 128 if failure == "nonzero" else 0
            elif kill_fallback and len(waits) == 2:
                self.clock.sleep(timeout)
                raise subprocess.TimeoutExpired("terminate", timeout)
            else:
                process.returncode = -9 if kill_fallback else -15
            return process.returncode

        process.wait.side_effect = wait
        process.kill.side_effect = lambda: setattr(process, "returncode", -9)
        if failure == "early":
            process.returncode = 128

        def probe(*args):
            self.clock.sleep(0.05 if visible else 0.2)
            if not visible:
                raise TimeoutError("no entries")
            return self.clock.now, ["big1m"]

        with mock.patch.object(harness.subprocess, "Popen", return_value=process), \
             mock.patch.object(harness, "listing_probe", side_effect=probe) as listing, \
             mock.patch.object(harness.subprocess, "check_output", return_value=(
                 "other-commit\n" if failure == "commit" else "expected-commit\n"
             )):
            result = harness.git_round(self.args, "git-case")
        return result, process, listing

    def test_git_early_exit_is_invalid_without_visibility(self):
        result, process, listing = self.git_case("early")
        self.assert_invalid(result)
        self.assertNotIn("first_directory_ms", result)
        self.assertEqual(result["visibility_status"], "not_observed")
        self.assertEqual(result["completion_status"], "failed")
        listing.assert_not_called()
        process.terminate.assert_not_called()

    def test_git_late_failures_preserve_visible_evidence(self):
        for failure in ("timeout", "nonzero", "commit"):
            with self.subTest(failure=failure):
                result, _, _ = self.git_case(failure)
                self.assert_invalid(result)
                self.assertAlmostEqual(result["first_directory_ms"], 50.0)
                self.assertEqual(result["root_entries"], ["big1m"])
                self.assertEqual(result["visibility_status"], "success")
                self.assertEqual(result["completion_status"], "failed")

    def test_git_terminate_timeout_falls_back_to_kill(self):
        result, process, _ = self.git_case("timeout", kill_fallback=True)
        self.assert_invalid(result)
        self.assertEqual(result["cleanup_status"], "success")
        process.terminate.assert_called_once_with()
        process.kill.assert_called_once_with()
        self.assertEqual(process.wait.call_count, 3)
        self.assertIsNotNone(process.wait.call_args.kwargs.get("timeout"))

    def test_git_cleanup_failure_invalidates_and_preserves_visibility(self):
        process = mock.Mock(returncode=None)
        process.poll.return_value = None
        process.wait.side_effect = subprocess.TimeoutExpired("clone", 1)
        process.kill.side_effect = OSError("cannot kill")
        with mock.patch.object(harness.subprocess, "Popen", return_value=process), \
             mock.patch.object(harness, "listing_probe", return_value=(100.05, ["big1m"])):
            result = harness.git_round(self.args, "kill-error")
        self.assert_invalid(result)
        self.assertEqual(result["first_directory_ms"], 50.0)
        self.assertEqual(result["cleanup_status"], "failed")
        self.assertIn("cannot kill", result["cleanup_error"])

    def test_git_listing_deadline_is_invalid_and_terminates_clone(self):
        result, process, _ = self.git_case(visible=False)
        self.assert_invalid(result)
        self.assertNotIn("first_directory_ms", result)
        self.assertEqual(result["cleanup_status"], "success")
        process.terminate.assert_called_once_with()

    def test_git_spawn_failure_returns_invalid_sample(self):
        with mock.patch.object(harness.subprocess, "Popen", side_effect=OSError("git unavailable")):
            result = harness.git_round(self.args, "spawn-error")
        self.assert_invalid(result)
        self.assertIn("git unavailable", result["error"])
        self.assertEqual(result["cleanup_status"], "not_required")

    def test_git_success_is_visible_completed_and_valid(self):
        result, process, _ = self.git_case()
        self.assertEqual(result["status"], "success")
        self.assertTrue(result["valid"])
        self.assertEqual(result["visibility_status"], "success")
        self.assertEqual(result["completion_status"], "success")
        self.assertEqual(result["cleanup_status"], "not_required")
        process.terminate.assert_not_called()

    def scorpio_case(self, api_delay=0.1, api_error=None, mount=True,
                     listing_error=None, delete_error=None):
        start = self.clock.now
        created = ClockFuture(
            self.clock, None if api_delay is None else start + api_delay,
            value={"mount_id": "mount-123"}, error=api_error,
        )
        listing = ClockFuture(
            self.clock, start + 0.05,
            value=(start + 0.05, ["big1m"]), error=listing_error,
        )
        pool = Executor(self.clock, created, listing)
        with mock.patch.object(harness.concurrent.futures, "ThreadPoolExecutor", return_value=pool), \
             mock.patch.object(harness, "mounted", return_value=mount), \
             mock.patch.object(harness, "http_json", side_effect=delete_error) as http:
            result = harness.scorpio_round(self.args, "scorpio-case")
        return result, created, pool, http

    def test_scorpio_success_is_valid_only_after_delete(self):
        result, _, _, http = self.scorpio_case()
        self.assertTrue(result["valid"])
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["first_directory_ms"], 50.0)
        self.assertEqual(result["completion_status"], "success")
        self.assertEqual(result["cleanup_status"], "success")
        self.assertEqual(http.call_args.args[:2], (self.args.api + "/mounts/mount-123", "DELETE"))

    def test_scorpio_no_mount_is_invalid_and_cleans_known_id(self):
        result, _, _, http = self.scorpio_case(mount=False)
        self.assert_invalid(result)
        self.assertNotIn("first_directory_ms", result)
        self.assertIn("no FUSE mount", result["error"])
        self.assertEqual(result["cleanup_status"], "success")
        http.assert_called_once()

    def test_scorpio_api_failure_before_mount_is_invalid(self):
        result, _, _, http = self.scorpio_case(
            api_delay=0, api_error=RuntimeError("POST rejected"), mount=False,
        )
        self.assert_invalid(result)
        self.assertIn("POST rejected", result["error"])
        self.assertNotIn("first_directory_ms", result)
        http.assert_not_called()

    def test_scorpio_listing_timeout_is_invalid_and_cleans_mount(self):
        result, _, _, http = self.scorpio_case(listing_error=TimeoutError("listing deadline"))
        self.assert_invalid(result)
        self.assertEqual(result["visibility_status"], "not_observed")
        self.assertNotIn("first_directory_ms", result)
        self.assertEqual(result["cleanup_status"], "success")
        http.assert_called_once()

    def test_scorpio_visible_before_api_failure_preserves_evidence(self):
        result, _, _, _ = self.scorpio_case(api_error=RuntimeError("API failed late"))
        self.assert_invalid(result)
        self.assertEqual(result["first_directory_ms"], 50.0)
        self.assertEqual(result["visibility_status"], "success")
        self.assertEqual(result["completion_status"], "failed")
        self.assertIn("API failed late", result["error"])

    def test_scorpio_visible_before_api_timeout_has_bounded_cleanup_window(self):
        result, created, pool, http = self.scorpio_case(api_delay=None)
        self.assert_invalid(result)
        self.assertEqual(result["first_directory_ms"], 50.0)
        self.assertEqual(result["visibility_status"], "success")
        self.assertEqual(result["completion_status"], "failed")
        self.assertEqual(result["cleanup_status"], "failed")
        self.assertAlmostEqual(result["measurement_elapsed_ms"], 1000.0)
        self.assertAlmostEqual(result["cleanup_elapsed_ms"], 500.0)
        self.assertAlmostEqual(self.clock.now, 101.5)
        self.assertTrue(all(wait is not None for wait in created.waits))
        pool.shutdown.assert_called_once_with(wait=False, cancel_futures=True)
        http.assert_not_called()

    def test_scorpio_late_api_id_is_deleted_after_measurement_deadline(self):
        result, _, pool, http = self.scorpio_case(api_delay=1.2, mount=False)
        self.assert_invalid(result)
        self.assertNotIn("first_directory_ms", result)
        self.assertEqual(result["mount_id"], "mount-123")
        self.assertEqual(result["cleanup_status"], "success")
        self.assertGreaterEqual(result["measurement_elapsed_ms"], 1000.0)
        self.assertLessEqual(result["cleanup_elapsed_ms"], 500.0)
        self.assertAlmostEqual(self.clock.now, 101.2)
        pool.shutdown.assert_called_once_with(wait=False, cancel_futures=True)
        http.assert_called_once()

    def test_scorpio_visible_before_late_api_success_remains_invalid(self):
        result, _, _, http = self.scorpio_case(api_delay=1.2)
        self.assert_invalid(result)
        self.assertEqual(result["first_directory_ms"], 50.0)
        self.assertEqual(result["completion_status"], "failed")
        self.assertEqual(result["cleanup_status"], "success")
        http.assert_called_once()

    def test_scorpio_delete_failure_separately_invalidates_completed_sample(self):
        result, _, _, _ = self.scorpio_case(delete_error=RuntimeError("DELETE rejected"))
        self.assert_invalid(result)
        self.assertEqual(result["first_directory_ms"], 50.0)
        self.assertEqual(result["visibility_status"], "success")
        self.assertEqual(result["completion_status"], "success")
        self.assertEqual(result["cleanup_status"], "failed")
        self.assertIn("DELETE rejected", result["cleanup_error"])
        self.assertNotIn("error", result)

    def test_scorpio_pending_delete_cannot_extend_cleanup_window(self):
        created = ClockFuture(self.clock, 100.1, value={"mount_id": "mount-123"})
        listing = ClockFuture(self.clock, 100.05, value=(100.05, ["big1m"]))
        pool = Executor(self.clock, created, listing)
        pool.deletion = ClockFuture(self.clock, None)
        with mock.patch.object(harness.concurrent.futures, "ThreadPoolExecutor", return_value=pool), \
             mock.patch.object(harness, "mounted", return_value=True):
            result = harness.scorpio_round(self.args, "delete-timeout")
        self.assert_invalid(result)
        self.assertEqual(result["first_directory_ms"], 50.0)
        self.assertEqual(result["completion_status"], "success")
        self.assertEqual(result["cleanup_status"], "failed")
        self.assertEqual(result["cleanup_elapsed_ms"], 500.0)
        self.assertAlmostEqual(self.clock.now, 100.6)
        pool.shutdown.assert_called_once_with(wait=False, cancel_futures=True)

    def test_scorpio_pending_listing_does_not_extend_measurement_or_cleanup(self):
        created = ClockFuture(self.clock, 100.1, value={"mount_id": "mount-123"})
        listing = ClockFuture(self.clock, None)
        pool = Executor(self.clock, created, listing)
        with mock.patch.object(harness.concurrent.futures, "ThreadPoolExecutor", return_value=pool), \
             mock.patch.object(harness, "mounted", return_value=True), \
             mock.patch.object(harness, "http_json") as http:
            result = harness.scorpio_round(self.args, "listing-timeout")
        self.assert_invalid(result)
        self.assertNotIn("first_directory_ms", result)
        self.assertEqual(result["measurement_elapsed_ms"], 1000.0)
        self.assertEqual(result["cleanup_status"], "success")
        pool.shutdown.assert_called_once_with(wait=False, cancel_futures=True)
        http.assert_called_once()

    def test_http_json_uses_mocked_urllib_and_disables_proxies(self):
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = b'{"mount_id":"m"}'
        opener = mock.Mock()
        opener.open.return_value = response
        with mock.patch.object(harness.urllib.request, "build_opener", return_value=opener) as build:
            self.assertEqual(harness.http_json("http://mock.invalid", "POST", {"path": "/p"}, 2),
                             {"mount_id": "m"})
        request = opener.open.call_args.args[0]
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(json.loads(request.data), {"path": "/p"})
        self.assertEqual(opener.open.call_args.kwargs["timeout"], 2)
        self.assertEqual(build.call_args.args[0].proxies, {})

    def run_main(self, tips, record=None, rounds=1):
        work = self.work / "main-output"
        argv = [str(SCRIPT), "--repo", "mock-repo", "--commit", "expected-commit",
                "--expected-dir", "big1m", "--work", str(work), "--rounds", str(rounds)]
        if record is None:
            record = {"backend": "scorpiofs", "label": "round-1", "status": "success",
                      "valid": True, "visibility_status": "success", "completion_status": "success",
                      "cleanup_status": "success", "first_directory_ms": 50.0}
        with mock.patch.object(sys, "argv", argv), \
             mock.patch.object(harness, "git_tip", side_effect=tips), \
             mock.patch.object(harness, "scorpio_round", return_value=record.copy()) as scorpio, \
             mock.patch.object(harness, "git_round", return_value={**record, "backend": "git"}) as git, \
             mock.patch("sys.stdout", new_callable=io.StringIO):
            with self.assertRaises(SystemExit) as exit_info:
                harness.main()
        rows = [json.loads(line) for line in (work / "first-directory.jsonl").read_text().splitlines()]
        return exit_info.exception.code, rows, scorpio, git

    def test_main_ref_changed_before_sample_writes_invalid_jsonl_without_running(self):
        code, rows, scorpio, git = self.run_main(["other-commit"])
        self.assertEqual(code, 1)
        self.assertEqual(len(rows), 1)
        self.assert_invalid(rows[0])
        self.assertEqual(rows[0]["ref_status"], "failed")
        scorpio.assert_not_called()
        git.assert_not_called()

    def test_main_ref_changed_after_sample_preserves_visibility_in_jsonl(self):
        code, rows, scorpio, git = self.run_main(["expected-commit", "other-commit"])
        self.assertEqual(code, 1)
        self.assert_invalid(rows[0])
        self.assertEqual(rows[0]["first_directory_ms"], 50.0)
        self.assertEqual(rows[0]["ref_status"], "failed")
        scorpio.assert_called_once()
        git.assert_not_called()

    def test_main_rechecks_ref_before_each_sample(self):
        code, rows, _, git = self.run_main(["expected-commit", "expected-commit", "other-commit"])
        self.assertEqual(code, 1)
        self.assertEqual(len(rows), 2)
        self.assertTrue(rows[0]["valid"])
        self.assert_invalid(rows[1])
        git.assert_not_called()

    def test_main_ref_lookup_error_still_writes_jsonl(self):
        code, rows, _, _ = self.run_main([RuntimeError("ref lookup failed")])
        self.assertEqual(code, 1)
        self.assert_invalid(rows[0])
        self.assertIn("ref lookup failed", rows[0]["ref_error"])

    def test_main_invalid_completed_or_cleanup_sample_never_fake_green(self):
        for field in ("completion_status", "cleanup_status"):
            with self.subTest(field=field):
                record = {"backend": "scorpiofs", "label": "round-1", "status": "success",
                          "valid": False, "visibility_status": "success", "completion_status": "success",
                          "cleanup_status": "success", "first_directory_ms": 50.0, field: "failed"}
                # Each case gets a distinct work directory; main deliberately refuses reuse.
                self.work = Path(self.temp.name) / field
                self.work.mkdir()
                code, rows, _, git = self.run_main(["expected-commit"] * 4, record)
                self.assertEqual(code, 1)
                self.assert_invalid(rows[0])
                self.assertEqual(rows[0]["first_directory_ms"], 50.0)
                git.assert_not_called()

    def test_main_success_writes_both_backends_and_exits_zero(self):
        code, rows, scorpio, git = self.run_main(["expected-commit"] * 4)
        self.assertEqual(code, 0)
        self.assertEqual([row["backend"] for row in rows], ["scorpiofs", "git"])
        self.assertTrue(all(row["valid"] for row in rows))
        scorpio.assert_called_once()
        git.assert_called_once()


if __name__ == "__main__":
    unittest.main()
