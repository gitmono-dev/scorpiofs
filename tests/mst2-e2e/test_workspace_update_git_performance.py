"""Real Git capture, safe bounded exports and unchanged owned error paths."""

from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import commit_update_budget as budget
import workspace_update_build as builds
import workspace_update_git_performance as perf


class GitPerformanceTests(unittest.TestCase):
    def environment(self, path):
        return {perf.PATH_ENV: str(path)}

    def synthetic(self, path, status=0):
        # Windows/wall-only is an explicit synthetic schema fixture, never a
        # Linux performance sample. Native Linux capture has its own test.
        with patch.object(perf.sys, "platform", "win32"):
            with perf.measure(["git", "status"], env=self.environment(path)) as metric:
                metric.complete(status)

    def test_disabled_and_non_git_commands_are_unchanged(self):
        args = ["git", "--version"]
        with patch.dict(os.environ, {}, clear=True), perf.measure(args) as metric:
            self.assertIs(metric.args, args)
            metric.complete(0)
        with perf.measure(["echo", "git"], env={perf.PATH_ENV: "relative/bad"}) as metric:
            self.assertEqual(metric.args, ["echo", "git"])
            metric.complete(0)

    def test_actual_git_captures_safe_events_and_original_return_interface(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "metrics.jsonl"
            token = "Bearer DO_NOT_EXPORT_SECRET"
            env = dict(os.environ, **self.environment(path))
            argv = ["git", "-c", "http.extraHeader=" + token, "--version"]
            status, out, error = budget.run_process(argv, time.monotonic() + 20, env=env)
            self.assertEqual(status, 0)
            self.assertTrue(out.startswith(b"git version "))
            self.assertEqual(error, b"")
            seal = perf.finalize(path)
            self.assertEqual(perf.finalize(path), seal)
            rows = perf.validate_stream(path)
            self.assertEqual([r["event"] for r in rows], ["begin", "end", "seal"])
            self.assertEqual(rows[1]["status"], "completed")
            self.assertEqual(rows[1]["operation"], "version")
            self.assertNotIn(token, path.read_text())
            self.assertNotIn(str(path), path.read_text())
            if sys.platform == "linux":
                self.assertIsNotNone(rows[1]["resources"])
            else:
                self.assertIsNone(rows[1]["resources"])
            self.assertEqual(list(Path(temp).glob(".git-time-*")), [])

    def test_actual_git_failure_keeps_exit_status_and_build_exception(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "metrics.jsonl"
            env = dict(os.environ, **self.environment(path))
            args = ["git", "-C", str(Path(temp) / "missing"), "rev-parse", "HEAD"]
            status, _, _ = budget.run_process(args, time.monotonic() + 10, env=env)
            self.assertNotEqual(status, 0)
            with self.assertRaises(subprocess.CalledProcessError) as caught:
                builds.output(args, time.monotonic() + 10, env=env)
            self.assertEqual(caught.exception.returncode, status)
            rows = perf.validate_stream(path, require_complete=False)
            self.assertEqual([r["exit_status"] for r in rows if r["event"] == "end"], [status, status])
            self.assertEqual([r["status"] for r in rows if r["event"] == "end"], ["failed", "failed"])

    def test_timeout_and_cleanup_failure_propagate_the_identical_exception(self):
        for failure in (TimeoutError("private failure"), AssertionError("private cleanup identity")):
            with self.subTest(kind=type(failure).__name__), tempfile.TemporaryDirectory() as temp:
                path = Path(temp) / "metrics.jsonl"
                env = self.environment(path)
                with patch.object(budget, "_run_process", side_effect=failure):
                    with self.assertRaises(type(failure)) as caught:
                        budget.run_process(["git", "status"], time.monotonic() + 10, env=env)
                self.assertIs(caught.exception, failure)
                terminal = perf.validate_stream(path, require_complete=False)[-1]
                self.assertEqual(terminal["status"], "timeout" if isinstance(failure, TimeoutError) else "failed")
                self.assertIsNone(terminal["resources"])
                self.assertNotIn("private", path.read_text())

    def test_abort_preserves_timeout_before_an_owner_translates_it(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "metrics.jsonl"
            with self.assertRaisesRegex(RuntimeError, "owner wrapper"):
                with perf.measure(["git", "fetch"], env=self.environment(path)) as metric:
                    metric.abort(TimeoutError("do not retain this text"))
                    raise RuntimeError("owner wrapper")
            self.assertEqual(perf.validate_stream(path, False)[-1]["status"], "timeout")

    def test_collection_and_disposal_errors_do_not_mask_owned_cleanup_failure(self):
        failure = AssertionError("owned group identity mismatch")
        original_close = perf._Metric.close
        def broken_close(metric):
            original_close(metric)
            raise OSError("temporary resource cleanup failed")
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "metrics.jsonl"
            with patch.object(perf._Metric, "finish", side_effect=OSError("evidence append failed")), \
                    patch.object(perf._Metric, "close", new=broken_close):
                with self.assertRaises(AssertionError) as caught:
                    with perf.measure(["git", "status"], self.environment(path)):
                        raise failure
            self.assertIs(caught.exception, failure)

    def test_context_overrides_stale_environment_and_restores_nested_state(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "metrics.jsonl"
            base = {"stage": "setup", "phase": "setup", "round": None, "client": None, "version": None}
            stale = dict(self.environment(path), **{perf.CONTEXT_ENV: json.dumps(base)})
            original = os.environ.get(perf.CONTEXT_ENV)
            with perf.context(phase="fair", round=2, client="b"):
                with perf.context(stage="publication", version="v10"):
                    self.assertEqual(json.loads(os.environ[perf.CONTEXT_ENV])["phase"], "fair")
                    with patch.object(perf.sys, "platform", "win32"), perf.measure(["git", "push"], stale) as metric:
                        metric.complete(0)
                self.assertIsNone(json.loads(os.environ[perf.CONTEXT_ENV])["version"])
            self.assertEqual(os.environ.get(perf.CONTEXT_ENV), original)
            self.assertEqual(perf.validate_stream(path, False)[-1]["context"],
                             dict(base, phase="fair", round=2, client="b", stage="publication", version="v10"))
            for labels in ({"version": "private-token"}, {"round": True}, {"argv": "secret"}):
                with self.assertRaises(ValueError), perf.context(**labels):
                    pass

    def test_only_closed_verbs_survive_config_and_path_arguments(self):
        for argv, expected in ((["git", "-C", "private/repo", "-c", "x=secret", "fetch"], "fetch"),
                               (["git", "--git-dir=private", "worktree", "add"], "worktree"),
                               (["git", "private-custom-alias"], "other"),
                               (["git", "--version"], "version")):
            self.assertEqual(perf.operation(argv), expected)
        self.assertTrue(perf.is_git([r"C:\Program Files\Git\cmd\git.exe", "status"]))
        self.assertFalse(perf.is_git(["git-credential", "get"]))

    def test_gnu_time_uses_its_own_report_and_explicit_kernel_units(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            timer = root / "time"
            timer.write_bytes(b"test-only timer placeholder")
            path = root / "metrics.jsonl"
            with patch.object(perf.sys, "platform", "linux"), patch.object(perf, "_TIME", str(timer)), \
                    patch.object(perf.os, "access", return_value=True):
                with perf.measure(["git", "status"], self.environment(path)) as metric:
                    self.assertEqual(metric.args[:5], [str(timer), "-q", "-f", perf._FORMAT, "-o"])
                    self.assertEqual(metric.args[-3:], ["--", "git", "status"])
                    Path(metric.report_path).write_text("mst2-git-time-v1\t1,23\t0,04\t0,05\t42\t2\t3\t4\t5\t6\t7\n")
                    metric.complete(0)
            resources = perf.validate_stream(path, False)[-1]["resources"]
            self.assertEqual(resources["child_elapsed_ms"], 1230)
            self.assertEqual(resources["user_cpu_seconds"], .04)
            self.assertEqual(resources["max_rss_kib"], 42)
            self.assertEqual(resources["filesystem_outputs_blocks"], 5)
            self.assertIn("not bytes", perf.RESOURCE_UNITS["filesystem_outputs_blocks"])

    def test_linux_missing_or_malformed_resource_report_fails_closed(self):
        for report in (None, "malformed\n"):
            with self.subTest(report=report), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                timer = root / "time"
                timer.write_bytes(b"test-only timer placeholder")
                path = root / "metrics.jsonl"
                with patch.object(perf.sys, "platform", "linux"), patch.object(perf, "_TIME", str(timer)), \
                        patch.object(perf.os, "access", return_value=True):
                    with self.assertRaisesRegex(ValueError, "collection did not complete"):
                        with perf.measure(["git", "status"], self.environment(path)) as metric:
                            if report is not None:
                                Path(metric.report_path).write_text(report)
                            metric.complete(0)
                row = perf.validate_stream(path, False)[-1]
                self.assertEqual(row["status"], "measurement_failed")
                self.assertIsNone(row["resources"])
                self.assertEqual(list(root.glob(".git-time-*")), [])

    def test_linux_missing_timer_records_failure_before_startup(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "metrics.jsonl"
            with patch.object(perf.sys, "platform", "linux"), patch.object(perf, "_TIME", str(Path(temp) / "absent")):
                with self.assertRaisesRegex(ValueError, "requires GNU time"):
                    with perf.measure(["git", "status"], self.environment(path)):
                        self.fail("must not start unmeasured Linux Git")
            self.assertEqual(perf.validate_stream(path, False)[-1]["status"], "measurement_failed")

    def test_concurrent_append_preserves_every_complete_line_and_unique_pair(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "metrics.jsonl"
            with patch.object(perf.sys, "platform", "win32"):
                def append(_):
                    with perf.measure(["git", "rev-parse"], self.environment(path)) as metric:
                        metric.complete(0)
                with ThreadPoolExecutor(max_workers=4) as pool:
                    list(pool.map(append, range(40)))
            perf.finalize(path)
            rows = perf.validate_stream(path)
            self.assertEqual(len(rows), 81)
            self.assertEqual(perf.summarize(rows)["started"], 40)

    def test_seal_binds_counts_hash_and_forbids_additional_writers(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "metrics.jsonl"
            self.synthetic(path)
            rows = perf.validate_stream(path, False)
            with self.assertRaisesRegex(ValueError, "sealed"):
                perf.validate_records(rows)
            seal = perf.finalize(path)
            self.assertEqual(seal["sha256"], hashlib.sha256(b"".join(perf._canonical(row) for row in rows)).hexdigest())
            with self.assertRaisesRegex(ValueError, "sealed"):
                self.synthetic(path)
            perf.validate_stream(path)
            altered = json.loads(json.dumps(perf.validate_stream(path)))
            altered[1]["wall_ms"] += 1
            with self.assertRaisesRegex(ValueError, "seal differs"):
                perf.validate_records(altered)
            altered = json.loads(json.dumps(perf.validate_stream(path)))
            altered[-1]["status_counts"]["completed"] = True
            with self.assertRaises(ValueError):
                perf.validate_records(altered)

    def test_partial_requires_known_begins_and_cannot_claim_complete(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "metrics.jsonl"
            self.synthetic(path)
            rows = perf.validate_stream(path, False)
            path.write_bytes(perf._canonical(rows[0]))
            perf.finalize(path)
            partial = perf.validate_stream(path, False)
            summary = perf.summarize(partial)
            self.assertTrue(summary["sealed"])
            self.assertFalse(summary["balanced"])
            self.assertEqual(summary["dangling"], 1)
            with self.assertRaises(ValueError):
                perf.validate_stream(path)
            with self.assertRaisesRegex(ValueError, "orphan"):
                perf.validate_records([rows[1]], False)

    def test_unknown_fields_duplicate_keys_torn_and_oversized_rows_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "metrics.jsonl"
            self.synthetic(path)
            rows = perf.validate_stream(path, False)
            bad = dict(rows[0], argv=["private"])
            with self.assertRaises(ValueError):
                perf.validate_records([bad], False)
            for raw in (b'{"event":"begin","event":"end"}\n', b"{}", b" " * (perf.MAX_LINE_BYTES + 1) + b"\n"):
                path.write_bytes(raw)
                with self.assertRaises(ValueError):
                    perf.validate_stream(path, False)
            with patch.object(perf, "MAX_RECORDS", 1), self.assertRaises(ValueError):
                perf.validate_records(rows, False)

    def test_hardlink_sink_is_rejected_without_modifying_either_name(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            original = root / "original"
            original.write_bytes(b"preserve me")
            link = root / "metrics.jsonl"
            os.link(original, link)
            with self.assertRaises(ValueError):
                self.synthetic(link)
            self.assertEqual(original.read_bytes(), b"preserve me")

    @unittest.skipUnless(os.name == "posix", "POSIX symlink directory fence")
    def test_symlink_sink_or_parent_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            real = root / "real"
            real.mkdir()
            linked_parent = root / "link"
            linked_parent.symlink_to(real, target_is_directory=True)
            with self.assertRaises((OSError, ValueError)):
                self.synthetic(linked_parent / "metrics.jsonl")
            sink = root / "metrics.jsonl"
            target = root / "target"
            target.write_bytes(b"unchanged")
            sink.symlink_to(target)
            with self.assertRaises((OSError, ValueError)):
                self.synthetic(sink)
            self.assertEqual(target.read_bytes(), b"unchanged")

    def test_summary_has_resource_absence_distribution_and_explicit_overhead(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "metrics.jsonl"
            self.synthetic(path)
            self.synthetic(path, 128)
            perf.finalize(path)
            summary = perf.summarize(perf.validate_stream(path))
            group = summary["groups"][0]
            self.assertEqual(group["wall_ms"]["count"], 2)
            self.assertEqual(group["status_counts"]["completed"], 1)
            self.assertEqual(group["status_counts"]["failed"], 1)
            self.assertEqual(group["resources"]["max_rss_kib"]["count"], 0)
            self.assertIsNone(group["resources"]["max_rss_kib"]["total"])
            self.assertTrue(summary["instrumented"])
            self.assertIn("comparison timers include", summary["measurement"])
            self.assertIn("terminal-event append is excluded", summary["measurement"])
            with patch.object(perf, "MAX_SUMMARY_BYTES", 8), self.assertRaises(ValueError):
                perf.summarize(perf.validate_stream(path))

    @unittest.skipUnless(sys.platform == "linux", "actual Linux per-command GNU time and timeout cleanup")
    def test_native_linux_timeout_keeps_owned_cleanup_and_never_fakes_resources(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "metrics.jsonl"
            env = dict(os.environ, **self.environment(path))
            # A temporary Git alias sleeps in the same owned process group.
            with self.assertRaises(TimeoutError):
                budget.run_process(["git", "-c", "alias.wait=!sleep 30", "wait"],
                                   time.monotonic() + 2, env=env)
            row = perf.validate_stream(path, False)[-1]
            self.assertEqual(row["status"], "timeout")
            self.assertIsNone(row["exit_status"])


if __name__ == "__main__":
    unittest.main()
