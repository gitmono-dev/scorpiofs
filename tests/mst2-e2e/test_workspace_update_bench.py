"""Full scenario orchestration and fixed deadlines without a deployment."""

from contextlib import ExitStack
import io
import json
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import workspace_update_bench as bench


class WorkspaceUpdateBenchTests(unittest.TestCase):
    def run_matrix(self, fail_batch=False):
        with tempfile.TemporaryDirectory() as temp, ExitStack() as stack:
            root = Path(temp)
            options = SimpleNamespace(
                isolated_deployment=True, publication_mode="native", projection_traces=True,
                finalize_projection=Mock(), base_url="http://127.0.0.1:9000",
                git_url="http://127.0.0.1:9000/project", database="mst2_bench_" + "a" * 32,
                instance_id="11111111-2222-4333-8444-555555555555",
                expect_initial_commit="a" * 40, run_root=root / "measurements",
                driver=root / "scorpio", driver_sha256="b" * 64, profile="medium", rounds=3,
                deadline_seconds=14400)
            now = time.monotonic()
            deadlines = [now + 100, now + 200, now + 300]
            budget = SimpleNamespace(measurement_deadline=now + 1000,
                                     cleanup_deadline=now + 1500,
                                     round_deadline=Mock(side_effect=deadlines),
                                     report_deadline=Mock(return_value=now + 1200))
            current = {"commit": options.expect_initial_commit, "sequence": 0}

            def command(args, *_args, **_kwargs):
                if args[:2] == ["git", "ls-remote"]:
                    return (current["commit"] + "\trefs/heads/main\n").encode()
                if args[:2] == ["git", "clone"]:
                    Path(args[-1]).mkdir()
                return b"git version test\n"

            def git(_repo, _deadline, *args, **_kwargs):
                if args[0] == "push":
                    current["commit"] = args[-1].split(":", 1)[0]
                    current["sequence"] += 1
                return options.expect_initial_commit.encode()

            calls = []

            def create_version(_repo, round_number, version, _smoke, deadline):
                calls.append((round_number, version, deadline))
                return f"{len(calls):040x}", "c" * 40

            daemon, worker, projection = Mock(), Mock(), Mock()
            daemon.url, daemon.workspace_root, daemon.uid = "http://127.0.0.1:9001", root, 1000
            daemon.binding.side_effect = lambda *_args: {"logical_request_id": str(len(calls))}
            daemon.finish.return_value = {"cleanup_complete": True}
            worker.stop.return_value = {"final_retained_view_audit_ms": 1}
            measured = []

            def measure(expected_path, commit, side_order, version, round_number, deadline):
                self.assertEqual(json.loads(expected_path.read_text()), {"files": [], "directories": [""]})
                measured.append((round_number, version, deadline))
                if fail_batch and version == "v4":
                    raise TimeoutError("fourth scenario exhausted the original round deadline")
                return {"actual_status": {}, "old_views": [],
                        "scorpio": {key: 1 for key in ("metadata_ready_ms", "durable_complete_ms",
                                    "durable_verified_ms", "retain_view_ms", "old_view_audit_ms", "side_total_ms")},
                        "git": {"verified_ms": 1, "checkout_verified_ms": 1}}

            worker.measure.side_effect = measure
            def finalize_projection(_deadline):
                self.assertEqual(worker.stop.call_count, 3)
                self.assertEqual(daemon.finish.call_count, 3)
                emitted = (options.run_root / "measurements.jsonl").read_text().splitlines()
                self.assertEqual([json.loads(line)["record"] for line in emitted], ["environment"])

            options.finalize_projection.side_effect = finalize_projection
            projection.collect.return_value = {
                "payload": {key: 1 for key in ("projection_elapsed_micros", *bench.WORK_FIELDS)}}
            projection.finish.return_value = {"correctness": "PASS"}
            patches = [patch.object(bench.budget_module, "from_options", return_value=budget),
                       patch.object(bench, "file_digest", return_value="b" * 64),
                       patch.object(bench, "WorkspaceDaemon", return_value=daemon),
                       patch.object(bench, "WorkerSession", return_value=worker),
                       patch.object(bench, "ProjectionCollector", return_value=projection),
                       patch.object(bench.common, "endpoint_pair"),
                       patch.object(bench.common, "driver_binding"),
                       patch.object(bench.common, "service_binding", return_value={"projection_cache": str(root)}),
                       patch.object(bench.common, "command", side_effect=command),
                       patch.object(bench.common, "git", side_effect=git),
                       patch.object(bench.common, "create_version", side_effect=create_version),
                       patch.object(bench.common, "expected_manifest", return_value={"files": [], "directories": [""]}),
                       patch.object(bench.common, "query", side_effect=lambda sql, _deadline:
                                    [{"path": "/project", "tree": "c" * 40}]
                                    if sql == bench.common.IDENTITY_SQL else {"sequence": current["sequence"]}),
                       patch.object(bench.common, "validate_identity", side_effect=lambda _rows, commit, *_args:
                                    {"namespace_view_id": "fixed-view", "commit": commit}),
                       patch.object(bench.common, "validate_native"),
                       patch.dict("os.environ", {"M2_TOKEN": "test", "M2_GIT_TOKEN": "test"}),
                       patch("sys.stdout", new_callable=io.StringIO)]
            entered = [stack.enter_context(item) for item in patches]
            if fail_batch:
                with self.assertRaises(bench.common.PhaseFailure) as failed:
                    bench.execute(options)
                self.assertEqual(bench.common.failure_record(failed.exception)["error_type"], "TimeoutError")
            else:
                bench.execute(options)
            return SimpleNamespace(options=options, budget=budget, deadlines=deadlines,
                                   calls=calls, measured=measured, worker=worker, daemon=daemon,
                                   projection=projection,
                                   records=[json.loads(line) for line in entered[-1].getvalue().splitlines()])

    def test_all_four_scenarios_share_each_round_deadline_and_finalize_all_sinks(self):
        run = self.run_matrix()
        expected = [(number, version, run.deadlines[number - 1])
                    for number in (1, 2, 3) for version in ("v1", "v2", "v3", "v4")]
        self.assertEqual(run.calls, expected)
        self.assertEqual(run.measured, expected)
        self.assertEqual([call.args for call in run.budget.round_deadline.call_args_list], [(1,), (2,), (3,)])
        self.assertEqual(run.worker.stop.call_count, 3)
        self.assertEqual(run.daemon.finish.call_count, 3)
        run.options.finalize_projection.assert_called_once_with(run.budget.report_deadline.return_value)
        self.assertEqual(run.projection.collect.call_count, 12)
        run.projection.finish.assert_called_once_with(12, run.budget.report_deadline.return_value)
        rounds = [record for record in run.records if record["record"] == "round"]
        summaries = [record for record in run.records if record["record"] == "summary"]
        self.assertEqual(len(rounds), 12)
        self.assertTrue(all(record["correctness"] == "PASS" for record in rounds))
        self.assertEqual([(record["version"], record["samples"]) for record in summaries],
                         [("v1", 3), ("v2", 3), ("v3", 3), ("v4", 3)])
        self.assertEqual(run.records[-1]["round_scenarios"], 12)

    def test_batch_timeout_cleans_both_owners_and_cannot_publish_partial_success(self):
        run = self.run_matrix(fail_batch=True)
        self.assertEqual(run.measured, [(1, version, run.deadlines[0])
                                       for version in ("v1", "v2", "v3", "v4")])
        run.worker.abort.assert_called_once_with(run.budget.cleanup_deadline)
        run.daemon.abort.assert_called_once_with(run.budget.cleanup_deadline)
        run.options.finalize_projection.assert_not_called()
        run.projection.finish.assert_not_called()
        run.budget.report_deadline.assert_not_called()
        self.assertEqual([record["record"] for record in run.records], ["environment"])


if __name__ == "__main__":
    unittest.main()
