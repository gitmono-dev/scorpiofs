"""Paired budget, matrix and both-owner cleanup failure contracts."""

import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import commit_update_budget as budget
import commit_update_ci as ci
import workspace_update_bench as bench


class PairedContractsTests(unittest.TestCase):
    def test_exact_two_build_and_three_pair_reserves_fit_235_minutes(self):
        with patch.object(budget.time, "time", return_value=0), patch.object(budget.time, "monotonic", return_value=0):
            session = budget.SessionBudget("1970-01-01T03:55:00Z", 3, paired=True)
        self.assertEqual(sum(session.stages.values()) + 3 * budget.ROUND_SECONDS + budget.REPORT_RESERVE
                         + budget.CLEANUP_RESERVE + budget.MARGIN + budget.EXTERNAL_RESERVE, 215 * 60)
        with patch("sys.stdout", new_callable=io.StringIO):
            for start, stage, end in ((20, "server-build", 55), (55, "client-a-build", 75),
                                      (75, "client-b-build", 95), (95, "fences", 105), (105, "setup", 115)):
                with patch.object(budget.time, "monotonic", return_value=start * 60):
                    self.assertEqual(session.stage_deadline(stage), end * 60)
            for number, start in ((1, 115), (2, 140), (3, 165)):
                with patch.object(budget.time, "monotonic", return_value=start * 60):
                    self.assertEqual(session.round_deadline(number), (start + 25) * 60)

    def test_late_stage_or_pair_cannot_borrow_cleanup_or_recovery_time(self):
        for recovery in (False, True):
            with patch.object(budget.time, "time", return_value=0), patch.object(budget.time, "monotonic", return_value=0):
                session = budget.SessionBudget("1970-01-01T03:55:00Z", 3,
                                               recover_original_window=recovery, paired=True)
            with patch.object(budget.time, "monotonic", return_value=20 * 60 + 1):
                with self.assertRaises(TimeoutError):
                    session.stage_deadline("server-build")
            with patch.object(budget.time, "monotonic", return_value=115 * 60 + 1):
                with self.assertRaises(TimeoutError):
                    session.round_deadline(1)
            with self.assertRaises(ValueError):
                session.stage_deadline("client-build")

    def test_paired_mode_cannot_reuse_single_client_budget_or_shrink_rounds(self):
        with patch.object(budget.time, "time", return_value=0), patch.object(budget.time, "monotonic", return_value=0):
            single = budget.SessionBudget("1970-01-01T03:55:00Z", 3)
            for rounds in (2, 4, True):
                with self.assertRaises(ValueError):
                    budget.SessionBudget("1970-01-01T03:55:00Z", rounds, paired=True)
        with self.assertRaises(ValueError):
            budget.from_options(SimpleNamespace(budget=single, paired=True))

    def test_both_build_commands_inherit_the_actual_shared_stage_deadline(self):
        for stage in ("client-a-build", "client-b-build"):
            session = SimpleNamespace(stage_deadline=Mock(return_value=123.5))
            argv = ["budget", "--paired", "--stage", stage,
                    "--session-deadline-utc", "1970-01-01T03:55:00Z", "--", "owned-build"]
            with self.subTest(stage=stage), patch("sys.argv", argv), \
                    patch.dict(os.environ, {"MST2_BUILD_DEADLINE_MONOTONIC": "999"}), \
                    patch.object(budget, "from_options", return_value=session), \
                    patch.object(budget, "run_process", return_value=(0, None, None)) as run:
                self.assertEqual(budget.main(), 0)
            session.stage_deadline.assert_called_once_with(stage)
            self.assertEqual(run.call_args.args, (["owned-build"], 123.5))
            self.assertEqual(run.call_args.kwargs["env"]["MST2_BUILD_DEADLINE_MONOTONIC"], "123.5")
            self.assertFalse(run.call_args.kwargs["capture"])

    def test_external_upload_cannot_renew_the_original_absolute_window(self):
        original = "1970-01-01T03:55:00Z"
        with patch.object(budget.time, "time", return_value=220 * 60):
            self.assertEqual(budget.require_external_time(original, 10 * 60), 15 * 60)
        for now, reserve in ((225 * 60, 10 * 60), (235 * 60, 0), (236 * 60, 0)):
            with self.subTest(now=now, reserve=reserve), patch.object(budget.time, "time", return_value=now):
                with self.assertRaises(TimeoutError):
                    budget.require_external_time(original, reserve)
        for reserve in (True, -1, 15 * 60 + 1, float("nan")):
            with self.subTest(reserve=reserve), self.assertRaises(ValueError):
                budget.require_external_time(original, reserve)

    def test_actual_workflow_run_metadata_preserves_original_window_and_escapes_public_inputs(self):
        workflow = Path(__file__).resolve().parents[2] / ".github/workflows/mst2-workspace-update.yml"
        script = workflow.read_text().split('python3 -B - "$safe/run.json" <<\'PY\'\n', 1)[1].split("          PY", 1)[0]
        env = {"GITHUB_RUN_ID": 'public"run\\name', "GITHUB_RUN_ATTEMPT": "2",
               "SCORPIO_SHA": "a" * 40, "MEGA_SHA": "b" * 40,
               "PROFILE": "medium", "ROUNDS": "3", "COMPARISON": "paired",
               "STARTED_INPUT": "2026-10-07T01:00:00Z", "DEADLINE_INPUT": "2026-10-07T04:55:00Z"}
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp) / "run.json"
            subprocess.run([sys.executable, "-B", "-", str(target)], env=env,
                           input=textwrap.dedent(script).encode(), check=True, capture_output=True)
            record = json.loads(target.read_text())
        self.assertEqual(record, {"run_id": env["GITHUB_RUN_ID"], "attempt": "2",
                                  "harness_sha": "a" * 40, "mega_sha": "b" * 40,
                                  "profile": "medium", "rounds": 3, "comparison": "paired",
                                  "session_started_utc": env["STARTED_INPUT"],
                                  "session_deadline_utc": env["DEADLINE_INPUT"]})

    def records(self):
        return [{"round": number, "version": version, "client": label,
                 "fixed_commit": str((number, version)), "identity": {"fixed": True},
                 "native_publication": {"sequence": number}, "oracle_manifest_sha256": "a" * 64,
                 "publication_started_monotonic": number * 100, "client_order": ["a", "b"]}
                for number in (1, 2, 3) for version in bench.common.SCENARIOS for label in ("a", "b")]

    def test_complete_count_alone_cannot_hide_a_missing_client_or_other_commit_oracle(self):
        records = self.records()
        bench.validate_matrix(records, 3, ["a", "b"])
        for field in ("fixed_commit", "oracle_manifest_sha256", "publication_started_monotonic"):
            changed = self.records()
            changed[1][field] = "another value"
            with self.assertRaises(AssertionError):
                bench.validate_matrix(changed, 3, ["a", "b"])
        missing = self.records()
        missing[-1] = missing[0].copy()
        with self.assertRaises(AssertionError):
            bench.validate_matrix(missing, 3, ["a", "b"])

    def test_abort_attempts_all_actual_owners_under_one_deadline_despite_errors(self):
        deadline = time.monotonic() + 1
        lanes = [SimpleNamespace(resources=Mock(), worker=Mock(), daemon=Mock()) for _ in range(2)]
        for owner in (lanes[1].resources.close, lanes[1].worker.abort, lanes[1].daemon.abort):
            owner.side_effect = RuntimeError("owned failure")
        refs = [(l.resources, l.worker, l.daemon) for l in lanes]
        errors = bench.abort_lanes(lanes, deadline)
        self.assertEqual(len(errors), 3)
        for resource, worker, daemon in refs:
            resource.close.assert_called_once_with(deadline)
            worker.abort.assert_called_once_with(deadline)
            daemon.abort.assert_called_once_with(deadline)

    def test_actual_outer_finalizer_requires_cleanup_and_log_close_with_original_deadline(self):
        for failure in (None, "cleanup", "log", "deadline"):
            with self.subTest(failure=failure):
                events = []
                process = object()
                root = Path("owned")
                log = Mock()
                def stop(*args):
                    self.assertEqual(args, (root, "owned", 10, process))
                    events.append("cleanup")
                    if failure == "cleanup":
                        raise AssertionError("leftover network")
                def close():
                    events.append("log-close")
                    if failure == "log":
                        raise OSError("close failed")
                log.close.side_effect = close
                with patch.object(ci, "stop_owned", side_effect=stop), \
                        patch.object(ci.time, "monotonic", return_value=10 if failure == "deadline" else 9):
                    if failure:
                        with self.assertRaises((AssertionError, OSError, TimeoutError)):
                            ci.finalize_owned_campaign(root, "owned", 10, process, log)
                    else:
                        ci.finalize_owned_campaign(root, "owned", 10, process, log)
                self.assertEqual(events, ["cleanup"] if failure == "cleanup" else ["cleanup", "log-close"])

    def test_cleanup_requires_both_nested_receipts_and_rejects_failed_or_live_group(self):
        for failure in (None, "missing", "incomplete", "live", "symlink"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                for label in ("a", "b"):
                    lane = root / "round-01" / ("client-" + label)
                    lane.mkdir(parents=True)
                    for name in ("daemon", "worker"):
                        receipt = lane / ("owned-workspace-" + name + ".json")
                        receipt.write_text(json.dumps({"pid": 123, "starttime": "456", "cleanup_complete": True}))
                target = root / "round-01/client-b/owned-workspace-worker.json"
                if failure == "missing":
                    target.unlink()
                if failure == "incomplete":
                    target.write_text(json.dumps({"pid": 123, "starttime": "456", "cleanup_complete": False}))
                with patch("workspace_update_daemon.mounts_under", return_value=[]), \
                        patch.object(ci.budget_module, "group_members", return_value=[123] if failure == "live" else []), \
                        patch.object(Path, "is_symlink", side_effect=lambda: failure == "symlink"):
                    if failure:
                        with self.assertRaises(AssertionError):
                            ci.verify_workspace_cleanup(root, time.monotonic() + 1, paired=True)
                    else:
                        ci.verify_workspace_cleanup(root, time.monotonic() + 1, paired=True)


if __name__ == "__main__":
    unittest.main()
