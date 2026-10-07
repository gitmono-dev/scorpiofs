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
import commit_update_ci as ci
import workspace_update_profile as read_profile


class WorkspaceUpdateBenchTests(unittest.TestCase):
    def run_matrix(self, fail_batch=False, paired=False, sink_failure=False, startup_failure=False,
                   campaign_failure=False, campaign_expiry=False, final_emit_delay=None,
                   diagnostic=False, missing_profile=False):
        with tempfile.TemporaryDirectory() as temp, ExitStack() as stack:
            root = Path(temp)
            options = SimpleNamespace(
                isolated_deployment=True, publication_mode="native", projection_traces=True,
                finalize_projection=Mock(), finalize_campaign=Mock(), base_url="http://127.0.0.1:9000",
                git_url="http://127.0.0.1:9000/project", database="mst2_bench_" + "a" * 32,
                instance_id="11111111-2222-4333-8444-555555555555",
                expect_initial_commit="a" * 40, run_root=root / "measurements",
                driver=root / "scorpio", driver_sha256="b" * 64, profile="medium", rounds=3,
                deadline_seconds=14400, paired=paired)
            if diagnostic:
                options.workspace_read_profile = True
            now = time.monotonic()
            deadlines = [now + 100, now + 200, now + 300]
            budget = SimpleNamespace(measurement_deadline=now + 1000,
                                     cleanup_deadline=now + 1500,
                                     round_deadline=Mock(side_effect=deadlines),
                                     report_deadline=Mock(return_value=now + 1200))
            budget.paired = paired
            emission_expired = [False]
            actual_monotonic, actual_open = time.monotonic, Path.open

            class DelayedOutput(io.StringIO):
                complete = False
                def write(self, value):
                    self.complete = self.complete or '"record": "complete"' in value
                    return super().write(value)
                def flush(self):
                    super().flush()
                    if self.complete and final_emit_delay == "stdout-flush":
                        emission_expired[0] = True

            class DelayedAppend:
                def __init__(self, stream):
                    self.stream, self.complete = stream, False
                def __enter__(self):
                    self.stream.__enter__()
                    return self
                def __exit__(self, *args):
                    return self.stream.__exit__(*args)
                def __getattr__(self, name):
                    return getattr(self.stream, name)
                def write(self, value):
                    self.complete = '"record": "complete"' in value
                    result = self.stream.write(value)
                    if self.complete and final_emit_delay == "file-write":
                        emission_expired[0] = True
                    return result
                def flush(self):
                    self.stream.flush()
                    if self.complete and final_emit_delay == "file-flush":
                        emission_expired[0] = True

            def delayed_open(path, *args, **kwargs):
                stream = actual_open(path, *args, **kwargs)
                return (DelayedAppend(stream) if final_emit_delay and args and args[0] == "a"
                        and path == options.run_root / "measurements.jsonl" else stream)
            clients = [SimpleNamespace(label=label, driver=root / ("scorpio-" + label),
                                       driver_sha256=("b" if label == "a" else "c") * 64,
                                       receipt=None, build={"source_sha": ("d" if label == "a" else "e") * 40})
                       for label in ("a", "b")]
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
            daemon_instances, worker_instances, collectors = [], [], []

            def measure(expected_path, commit, side_order, version, round_number, deadline, label="single"):
                self.assertEqual(json.loads(expected_path.read_text()), {"files": [], "directories": [""]})
                measured.append((round_number, version, deadline) + ((label, side_order, commit) if paired else ()))
                if fail_batch and version == "v4":
                    raise TimeoutError("fourth scenario exhausted the original round deadline")
                result = {"actual_status": {}, "old_views": [],
                        "scorpio": {key: 1 for key in ("metadata_ready_ms", "durable_complete_ms",
                                    "durable_verified_ms", "retain_view_ms", "old_view_audit_ms", "side_total_ms")},
                        "git": {"verified_ms": 1, "checkout_verified_ms": 1}}
                if diagnostic and not missing_profile:
                    from test_workspace_update_profile import evidence
                    result["scorpio"]["read_profile"] = (read_profile.not_measured("unsupported")
                                                          if label == "a" else evidence())
                return result

            def daemon_factory(binary, _digest, lane_root, *_args, **kwargs):
                label = lane_root.name.removeprefix("client-")
                if startup_failure and label == "b":
                    raise TimeoutError("second client startup exhausted the admitted deadline")
                obj = Mock()
                obj.read_profile_enabled = kwargs.get("read_profile", False)
                obj.url, obj.workspace_root, obj.uid = "http://127.0.0.1:9001", lane_root, 1000
                obj.store = lane_root / "scorpio-store"
                obj.process.pid, obj.started = 123 + len(daemon_instances), "456"
                obj.binding.side_effect = lambda *_args: {"logical_request_id": label + str(len(calls))}
                obj.finish.return_value = {"actual_exit_code": 0, "bindings": 4, "footer_complete": True,
                                           "native_mounts_remaining": 0}
                daemon_instances.append(obj)
                return obj

            def worker_factory(lane_root, *_args, **_kwargs):
                obj = Mock()
                obj.read_profile_mode = _kwargs.get("read_profile_mode", "disabled")
                label = lane_root.name.removeprefix("client-")
                obj.measure.side_effect = lambda *args: measure(*args, label=label)
                obj.stop.return_value = {"retained": 4, "verified": True, "final_retained_view_audit_ms": 1}
                worker_instances.append(obj)
                return obj

            def resource_factory(*_args):
                obj = Mock()
                obj.start.return_value = obj
                obj.snapshot.return_value = {"pid": 123}
                obj.close.return_value = {"pid": 123}
                collectors.append(obj)
                return obj

            worker.measure.side_effect = measure
            def finalize_projection(_deadline):
                self.assertEqual(sum(w.stop.call_count for w in worker_instances) if paired else worker.stop.call_count,
                                 6 if paired else 3)
                self.assertEqual(sum(d.finish.call_count for d in daemon_instances) if paired else daemon.finish.call_count,
                                 6 if paired else 3)
                emitted = (options.run_root / "measurements.jsonl").read_text().splitlines()
                self.assertEqual([json.loads(line)["record"] for line in emitted], ["environment"])

            options.finalize_projection.side_effect = finalize_projection
            def finalize_campaign(original_deadline):
                self.assertEqual(original_deadline, budget.cleanup_deadline)
                projection.finish.assert_called_once_with(24 if paired else 12, budget.report_deadline.return_value)
                emitted = (options.run_root / "measurements.jsonl").read_text().splitlines()
                self.assertEqual([json.loads(line)["record"] for line in emitted], ["environment"])
                if campaign_failure:
                    raise AssertionError("owned Docker inventory remains")
                if campaign_expiry:
                    budget.cleanup_deadline = time.monotonic() - 1
            options.finalize_campaign.side_effect = finalize_campaign
            projection.collect_registered.return_value = {
                "payload": {key: 1 for key in ("projection_elapsed_micros", *bench.WORK_FIELDS)}}
            projection.finish.return_value = {"correctness": "PASS"}
            if sink_failure:
                projection.finish.side_effect = AssertionError("projection footer did not ACK the complete matrix")
            patches = [patch.object(bench.budget_module, "from_options", return_value=budget),
                       patch.object(bench, "file_digest", return_value="b" * 64),
                       patch.object(bench, "WorkspaceDaemon", **({"side_effect": daemon_factory} if paired else {"return_value": daemon})),
                       patch.object(bench, "WorkerSession", **({"side_effect": worker_factory} if paired else {"return_value": worker})),
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
                       patch("sys.stdout", new_callable=DelayedOutput)]
            if final_emit_delay:
                patches[-1:-1] = [patch.object(Path, "open", delayed_open),
                                  patch.object(bench.time, "monotonic", side_effect=lambda:
                                               budget.cleanup_deadline + 1 if emission_expired[0] else actual_monotonic())]
            if paired:
                patches[-1:-1] = [patch.object(bench.builds, "clients", return_value=clients),
                                  patch("workspace_update_resources.ProcessResources", side_effect=resource_factory),
                                  patch("workspace_update_resources.disk_usage", return_value={"allocated_bytes": 1}),
                                  patch("workspace_update_resources.io_delta", return_value={"rchar": 1})]
            capability_probe = Mock(side_effect=lambda lane, *_args: "unsupported" if lane.label == "a" else "enabled")
            patches[-1:-1] = [patch.object(bench.builds, "read_profile_mode", capability_probe)]
            entered = [stack.enter_context(item) for item in patches]
            if final_emit_delay:
                with self.assertRaisesRegex(TimeoutError, "evidence (write|output) exceeded") as failed:
                    bench.execute(options)
                ci.persist_failure_record(root, failed.exception)
            elif fail_batch or startup_failure or sink_failure or campaign_failure or campaign_expiry or missing_profile:
                if startup_failure or sink_failure or campaign_failure or campaign_expiry:
                    with self.assertRaises((TimeoutError, AssertionError)):
                        bench.execute(options)
                else:
                    with self.assertRaises(bench.common.PhaseFailure) as failed:
                        bench.execute(options)
                    self.assertEqual(bench.common.failure_record(failed.exception)["error_type"],
                                     "ProfileError" if missing_profile else "TimeoutError")
            else:
                bench.execute(options)
            return SimpleNamespace(options=options, budget=budget, deadlines=deadlines,
                                   calls=calls, measured=measured, worker=worker, daemon=daemon,
                                   projection=projection,
                                   daemon_instances=daemon_instances, worker_instances=worker_instances,
                                   collectors=collectors,
                                   capability_probe=capability_probe,
                                   file_records=[json.loads(line) for line in (options.run_root / "measurements.jsonl").read_text().splitlines()],
                                   failure=json.loads((options.run_root / "failure.json").read_text()) if final_emit_delay else None,
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
        self.assertEqual(run.projection.collect_registered.call_count, 12)
        run.projection.finish.assert_called_once_with(12, run.budget.report_deadline.return_value)
        run.options.finalize_campaign.assert_called_once_with(run.budget.cleanup_deadline)
        rounds = [record for record in run.records if record["record"] == "round"]
        summaries = [record for record in run.records if record["record"] == "summary"]
        self.assertEqual(len(rounds), 12)
        self.assertTrue(all(record["correctness"] == "PASS" for record in rounds))
        self.assertEqual([(record["version"], record["samples"]) for record in summaries],
                         [("v1", 3), ("v2", 3), ("v3", 3), ("v4", 3)])
        self.assertEqual(run.records[-1]["round_scenarios"], 12)
        self.assertTrue(run.records[-1]["campaign_cleanup_complete"])

    def test_default_off_preserves_unprofiled_records_and_never_probes_client_capabilities(self):
        run = self.run_matrix(paired=True)
        run.capability_probe.assert_not_called()
        for record in run.records:
            self.assertNotIn("measurement_interpretation", record)
            self.assertNotIn("performance_comparison_allowed", record)
            if record["record"] == "round":
                self.assertNotIn("read_profile", record["scorpio"])
        self.assertTrue(all(not daemon.read_profile_enabled for daemon in run.daemon_instances))
        self.assertTrue(all(worker.read_profile_mode == "disabled" for worker in run.worker_instances))

    def test_explicit_diagnostic_matrix_never_labels_unsupported_baseline_as_zero_measurement(self):
        run = self.run_matrix(paired=True, diagnostic=True)
        self.assertEqual(run.capability_probe.call_count, 2)
        self.assertEqual(run.records[0]["workspace_read_profile_modes"], {"a": "unsupported", "b": "enabled"})
        for record in run.records:
            self.assertEqual(record["measurement_interpretation"], read_profile.DIAGNOSTIC_INTERPRETATION)
            self.assertIs(record["performance_comparison_allowed"], False)
            if record["record"] == "round":
                value = record["scorpio"]["read_profile"]
                self.assertEqual(value["status"], "NOT_MEASURED" if record["client"] == "a" else "MEASURED")
                if record["client"] == "a":
                    self.assertNotIn("delta", value)
        self.assertEqual([daemon.read_profile_enabled for daemon in run.daemon_instances], [False, True] * 3)
        self.assertEqual([worker.read_profile_mode for worker in run.worker_instances], ["unsupported", "enabled"] * 3)
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "measurements.jsonl"
            path.write_text("".join(json.dumps(record) + "\n" for record in run.records))
            self.assertEqual(read_profile.validate_artifact(path, diagnostic_required=True), 24)

    def test_missing_requested_diagnostics_fail_the_matrix_before_any_round_is_published(self):
        run = self.run_matrix(paired=True, diagnostic=True, missing_profile=True)
        self.assertEqual([record["record"] for record in run.records], ["environment"])
        run.projection.finish.assert_not_called()
        run.options.finalize_campaign.assert_not_called()

    def test_batch_timeout_cleans_both_owners_and_cannot_publish_partial_success(self):
        run = self.run_matrix(fail_batch=True)
        self.assertEqual(run.measured, [(1, version, run.deadlines[0])
                                       for version in ("v1", "v2", "v3", "v4")])
        run.worker.abort.assert_called_once_with(run.budget.cleanup_deadline)
        run.daemon.abort.assert_called_once_with(run.budget.cleanup_deadline)
        run.options.finalize_projection.assert_not_called()
        run.projection.finish.assert_not_called()
        run.options.finalize_campaign.assert_not_called()
        run.budget.report_deadline.assert_not_called()
        self.assertEqual([record["record"] for record in run.records], ["environment"])

    def test_paired_clients_share_publications_oracle_and_budget_with_independent_owners(self):
        run = self.run_matrix(paired=True)
        self.assertEqual(len(run.calls), 12)
        self.assertEqual(len(run.measured), 24)
        records = [r for r in run.records if r["record"] == "round"]
        for number in (1, 2, 3):
            for version in bench.common.SCENARIOS:
                samples = [r for r in records if r["round"] == number and r["version"] == version]
                self.assertEqual({r["client"] for r in samples}, {"a", "b"})
                self.assertEqual(samples[0]["fixed_commit"], samples[1]["fixed_commit"])
                self.assertEqual(samples[0]["oracle_manifest_sha256"], samples[1]["oracle_manifest_sha256"])
                self.assertEqual(samples[0]["publication_started_monotonic"], samples[1]["publication_started_monotonic"])
                self.assertEqual({r["side_order"] for r in samples}, {"scorpio-first", "git-first"})
        self.assertEqual([r["client_order"][0] for r in records].count("a"), 12)
        for label in ("a", "b"):
            self.assertEqual([r["side_order"] for r in records if r["client"] == label].count("scorpio-first"), 6)
        self.assertTrue(all(call[2] == run.deadlines[call[0] - 1] for call in run.measured))
        self.assertEqual(len(run.daemon_instances), 6)
        self.assertEqual(len({d.workspace_root for d in run.daemon_instances}), 6)
        self.assertTrue(all(d.finish.call_count == 1 and d.abort.call_count == 0 for d in run.daemon_instances))
        self.assertTrue(all(c.close.call_count == 1 for c in run.collectors))
        self.assertEqual(len([r for r in run.records if r["record"] == "summary"]), 8)
        self.assertEqual(len([r for r in run.records if r["record"] == "paired_summary"]), 4)
        self.assertFalse(any('"p95"' in json.dumps(r) for r in run.records))
        run.projection.finish.assert_called_once_with(24, run.budget.report_deadline.return_value)
        run.options.finalize_campaign.assert_called_once_with(run.budget.cleanup_deadline)
        self.assertEqual(run.records[-1]["round_scenarios"], 24)
        trace_calls = [call[0] for call in run.projection.method_calls if call[0] in ("register", "collect_registered")]
        self.assertEqual(trace_calls, [name for _ in range(12)
                                       for name in ("register", "register", "collect_registered", "collect_registered")])

    def test_paired_second_startup_failure_aborts_the_first_client_without_success(self):
        run = self.run_matrix(paired=True, startup_failure=True)
        self.assertEqual(run.calls, [])
        run.worker_instances[0].abort.assert_called_once_with(run.budget.cleanup_deadline)
        run.daemon_instances[0].abort.assert_called_once_with(run.budget.cleanup_deadline)
        run.collectors[0].close.assert_called_once_with(run.budget.cleanup_deadline)
        self.assertEqual([r["record"] for r in run.records], ["environment"])

    def test_outer_campaign_cleanup_failure_cannot_publish_round_or_complete_pass(self):
        run = self.run_matrix(paired=True, campaign_failure=True)
        self.assertEqual(len(run.measured), 24)
        run.options.finalize_campaign.assert_called_once_with(run.budget.cleanup_deadline)
        self.assertEqual([r["record"] for r in run.records], ["environment"])

    def test_final_campaign_cleanup_crossing_original_deadline_cannot_publish_pass(self):
        run = self.run_matrix(paired=True, campaign_expiry=True)
        self.assertEqual(len(run.measured), 24)
        self.assertEqual([r["record"] for r in run.records], ["environment"])

    def test_delayed_last_evidence_write_or_flush_fails_and_removes_complete_from_safe_file(self):
        for checkpoint in ("file-write", "file-flush", "stdout-flush"):
            with self.subTest(checkpoint=checkpoint):
                run = self.run_matrix(paired=True, final_emit_delay=checkpoint)
                self.assertEqual(len(run.measured), 24)
                run.options.finalize_campaign.assert_called_once_with(run.budget.cleanup_deadline)
                self.assertFalse(any(r["record"] == "complete" for r in run.file_records))
                self.assertEqual(run.failure, {"error_type": "TimeoutError", "execution_failed": True})

    def test_paired_batch_failure_aborts_both_independent_clients_without_partial_pass(self):
        run = self.run_matrix(paired=True, fail_batch=True)
        self.assertEqual(len(run.calls), 4)
        self.assertTrue(all(w.abort.call_args.args == (run.budget.cleanup_deadline,) for w in run.worker_instances))
        self.assertTrue(all(d.abort.call_args.args == (run.budget.cleanup_deadline,) for d in run.daemon_instances))
        run.projection.finish.assert_not_called()
        self.assertEqual([r["record"] for r in run.records], ["environment"])

    def test_paired_projection_footer_failure_cannot_promote_any_record_to_pass(self):
        run = self.run_matrix(paired=True, sink_failure=True)
        self.assertEqual(len(run.measured), 24)
        self.assertTrue(all(d.finish.call_count == 1 for d in run.daemon_instances))
        self.assertEqual([r["record"] for r in run.records], ["environment"])


if __name__ == "__main__":
    unittest.main()
