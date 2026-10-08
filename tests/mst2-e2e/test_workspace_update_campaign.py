"""Campaign completeness and original-window failure boundaries."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import subprocess
import time
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from unittest.mock import Mock
from types import SimpleNamespace
from contextlib import ExitStack

import commit_update_budget as budgets
import workspace_update_campaign as campaign
import workspace_update_backend as backend_module


def workload():
    manifest = {"files": [{"rel_path": "f", "fs_kind": "regular", "size": 7,
                           "content_digest": "sha256:" + "1" * 64}], "directories": ["", "empty"]}
    def oracle(git=False, dirty=False):
        return {"verified_files": 1 + int(dirty), "verified_directories": 1 if git else 2,
            "verified_bytes": 7 + (len(campaign.DIRTY_BYTES) if dirty else 0), "regular_read_calls": 2,
            "oracle_walk_and_hash_ms": .4, "isolated_oracle_process_ms": .8,
            "raw_empty_tree_directories_omitted_by_git": ["empty"] if git else []}
    def view(record):
        return {**record["workspace_binding"], "fd_verified": True,
                "dirty_upper_verified": True, "oracle": oracle(dirty=True)}
    records = []
    for index, version in enumerate(campaign.common.SCENARIOS, 1):
        binding = {"workspace_id": "workspace-" + str(index), "generation": "generation-" + str(index),
                   "snapshot_id": "snapshot-" + str(index)}
        record = {"version": version, "round": 1, "fixed_commit": str(index) * 40,
                  "workspace_binding": binding, "manifest": manifest}
        record["result"] = {"version": version, "round": 1, "actual_status": binding,
            "git": {"commit": record["fixed_commit"], "oracle": oracle(git=True)},
            "scorpio": {"oracle": oracle()}, "old_views": [view(old) for old in records]}
        records.append(record)
    final = {"retained": 4, "verified": True, "views": [view(record) for record in records],
             "final_retained_view_audit_ms": 1.2}
    return records, final


class CampaignBudgetTests(unittest.TestCase):
    def setUp(self):
        self.utc = datetime(2026, 10, 7, tzinfo=timezone.utc)
        self.clock = [1000.]
        self.patches = [patch.object(budgets.time, "time", return_value=self.utc.timestamp()),
                        patch.object(budgets.time, "monotonic", side_effect=lambda: self.clock[0])]
        for context in self.patches:
            context.start()
            self.addCleanup(context.stop)
        self.budget = budgets.IsolatedCampaignBudget((self.utc + timedelta(minutes=235)).isoformat(), 3)

    def test_preflight_uses_remaining_original_slot_and_never_reanchors(self):
        h = self.budget.cleanup_deadline
        self.clock[0] += 8 * 60
        first = self.budget.stage_deadline("preflight")
        self.clock[0] += 6 * 60
        second = self.budget.stage_deadline("preflight")
        self.assertEqual(first, 1000 + 15 * 60)
        self.assertEqual(first, second)
        self.assertEqual(h, self.budget.cleanup_deadline)
        self.clock[0] += 2 * 60
        with self.assertRaises(TimeoutError):
            self.budget.stage_deadline("preflight")

    def test_lifetimes_and_diagnostic_fit_original_window_with_report_cleanup_export(self):
        self.clock[0] += 105 * 60
        for number in range(1, 4):
            deadline = self.budget.round_deadline(number)
            self.assertEqual(deadline - self.clock[0], 25 * 60)
            self.clock[0] += 24 * 60
            self.budget.close_phase(deadline)
        deadline = self.budget.diagnostic_deadline()
        self.assertEqual(deadline - self.clock[0], 20 * 60)
        self.clock[0] += 19 * 60
        self.budget.close_phase(deadline)
        self.assertLessEqual(self.budget.report_deadline(), self.budget.cleanup_deadline - 15 * 60)

    def test_missing_round_or_double_admission_cannot_enter_diagnostic(self):
        with self.assertRaises(ValueError):
            self.budget.diagnostic_deadline()
        deadline = self.budget.round_deadline(1)
        with self.assertRaises(ValueError):
            self.budget.round_deadline(1)
        self.budget.close_phase(deadline)
        with self.assertRaises(ValueError):
            self.budget.round_deadline(3)

    def test_round_cleanup_beyond_phase_cap_rejects_without_resetting_h(self):
        h = self.budget.cleanup_deadline
        deadline = self.budget.round_deadline(1)
        self.clock[0] = deadline
        with self.assertRaises(TimeoutError):
            self.budget.close_phase(deadline)
        self.assertEqual(h, self.budget.cleanup_deadline)

    def test_slow_fair_work_does_not_borrow_diagnostic_or_cleanup_reserve(self):
        self.clock[0] += 106 * 60
        with self.assertRaises(TimeoutError):
            self.budget.round_deadline(1)
        self.budget._next_round = 4
        self.clock[0] = self.budget.cleanup_deadline - 39 * 60
        with self.assertRaises(TimeoutError):
            self.budget.diagnostic_deadline()

    def test_recovery_and_single_mode_are_rejected(self):
        for args in ((True, True), (False, False)):
            with self.assertRaises(ValueError):
                budgets.IsolatedCampaignBudget((self.utc + timedelta(minutes=235)).isoformat(), 3,
                    recover_original_window=args[0], paired=args[1])

    def test_existing_budget_cannot_cross_isolation_mode(self):
        options = SimpleNamespace(budget=self.budget, paired=True, isolated_backends=False)
        with self.assertRaises(ValueError):
            budgets.from_options(options)


class CampaignWorkloadTests(unittest.TestCase):
    def test_producer_worker_footer_collector_and_teardown_failures_never_emit_complete(self):
        import test_workspace_update_campaign_export as replay_tests
        import workspace_update_resources as resource_module
        for boundary in ("worker", "footer", "collector", "teardown", "success"):
            with self.subTest(boundary=boundary):
                replay = replay_tests.CampaignExportTests(methodName="runTest")
                replay.setUp()
                try:
                    fixture = replay.proof_fixture()
                    lanes = {label: replay.replay_lane(fixture, label=label) for label in ("a", "b")}
                    captures = {label: lane[2] for label, lane in lanes.items()}
                    runtimes = {}
                    for label, capture in captures.items():
                        value = deepcopy(capture.evidence["runtime"])
                        for source, target in (("identity_rows", "identity_rows_json"), ("identity", "identity_json"), ("native", "native_json")):
                            value[target] = campaign.proofs.canonical(value.pop(source))
                        value["dependency_container_ids"] = tuple(value["dependency_container_ids"])
                        runtimes[label] = backend_module.RuntimeBinding(**value)
                    root = Path(replay.temp.name) / ("producer-" + boundary)
                    (root / "measurements/fair").mkdir(parents=True)
                    h = time.monotonic() + 10000
                    deadline = h - 1000
                    primary = RuntimeError("injected " + boundary + " failure")
                    stopped, daemons = [], {}
                    owners = []
                    for label in ("a", "b"):
                        closed = lanes[label][0][0]["semantic_provenance"]["lane"]["projection_sink"]
                        collector = SimpleNamespace(snapshot=lambda: ({"written_records": 0}, {}),
                            register=Mock(), collect_registered=Mock(return_value={}),
                            closed_evidence=Mock(side_effect=primary if boundary == "collector" and label == "a" else None,
                                                return_value=closed))
                        def stop(_h, *, operation_deadline=None, label=label):
                            stopped.append(label)
                            if boundary == "teardown" and label == "a" and operation_deadline is not None:
                                raise primary
                        owners.append(SimpleNamespace(client=label, initial_commit="c" * 40,
                            base_url=runtimes[label].base_url, git_url=runtimes[label].git_url,
                            instance_id=runtimes[label].instance_id, pg_env={"M2_TOKEN": "test-only-token"}, git_env={},
                            projection_collector=collector, publish_seed=Mock(return_value=runtimes[label]),
                            verify_runtime=Mock(return_value=runtimes[label]), finalize_projection=Mock(), stop=stop))
                    clients = [SimpleNamespace(label=label, driver=Path(captures[label].evidence["client_build"]["binary"]),
                        driver_sha256=captures[label].evidence["client_build"]["binary_sha256"], receipt=fixture.builds[label])
                        for label in ("a", "b")]
                    class FakeDaemon:
                        def __init__(self, binary, _sha, lane_root, *_args, **_kwargs):
                            self.label = next(client.label for client in clients if client.driver == binary)
                            self.root = lane_root
                            self.url, self.workspace_root, self.uid = "http://127.0.0.1:1", lane_root / "workspaces", 1000
                            self.process, self.started, self.index = SimpleNamespace(pid=1), "999", 0
                            daemons[self.label] = self
                        def check_owner(self, **_kwargs):
                            pass
                        def binding(self, *_args):
                            return lanes[self.label][0][self.index]["workspace_binding"]
                        def finish(self, _deadline):
                            if boundary == "footer" and self.label == "a":
                                raise primary
                            (self.root / "workspace-observation.jsonl").write_bytes(lanes[self.label][5])
                            return {"actual_exit_code": 0, "bindings": 4, "footer_complete": True, "native_mounts_remaining": 0}
                        def abort(self, _deadline):
                            pass
                    class FakeWorker:
                        def __init__(self, lane_root, *_args, **_kwargs):
                            self.label = lane_root.name[-1]
                            self.index = 0
                        def measure(self, _manifest, _commit, _side, version, number, _deadline):
                            daemons[self.label].index = self.index
                            record = lanes[self.label][0][self.index]
                            self.index += 1
                            return {"version": version, "round": number, "actual_status": record["actual_status"],
                                "scorpio": record["scorpio"], "git": record["git"], "old_views": record["old_views"]}
                        def stop(self, _deadline):
                            if boundary == "worker" and self.label == "a":
                                raise primary
                            return lanes[self.label][0][0]["round_final_retained_views"]
                        def abort(self, _deadline):
                            pass
                    class FakeResources:
                        def __init__(self, *_args):
                            pass
                        def start(self):
                            return self
                        def close(self, _deadline):
                            return {}
                    group = SimpleNamespace(root=root, cleanup_deadline=h, start_pair=Mock(return_value=owners),
                                            options=SimpleNamespace(profile="smoke"))
                    with ExitStack() as stack:
                        for obj, name, replacement in (
                            (campaign, "canonical_seed", Mock(return_value={"parent": "c" * 40, "commit": "d" * 40, "tree": "a" * 40})),
                            (campaign.common, "command", Mock(return_value=b"")),
                            (campaign.common, "create_version", Mock(return_value=("b" * 40, "a" * 40))),
                            (campaign.common, "expected_manifest", Mock(return_value=lanes["a"][0][0]["manifest"])),
                            (campaign.builds, "validate", Mock()), (campaign.builds, "read_profile_mode", Mock(return_value="disabled")),
                            (campaign, "WorkspaceDaemon", FakeDaemon), (campaign, "WorkerSession", FakeWorker),
                            (resource_module, "ProcessResources", FakeResources),
                            (campaign.measurement, "resources_before", Mock(return_value=None)),
                            (campaign.measurement, "resources_after", Mock(return_value={})),
                            (campaign, "publication", Mock(side_effect=lambda owner, *_args: (runtimes[owner.client],
                                lanes[owner.client][0][0]["publication"], 1.0))),
                            (campaign.proofs, "capture_lane_runtime", Mock(side_effect=lambda owner, *_args, **_kwargs: captures[owner.client])),
                            (campaign, "cleanup_receipts", Mock(side_effect=lambda lane_root, *_args: lanes[lane_root.name[-1]][0][0]["cleanup_receipts"]))):
                            stack.enter_context(patch.object(obj, name, replacement))
                        if boundary == "success":
                            records = campaign.run_phase(group, clients,
                                {label: captures[label].evidence["sources"] for label in captures},
                                "fair", 1, deadline, root)
                            self.assertEqual(len(records), 8)
                            self.assertTrue(all(isinstance(row["manifest"], campaign.ManifestFacts) for row in records))
                            wire = [json.loads(line) for line in (root / "measurements/fair/measurements.jsonl").read_text().splitlines()]
                            self.assertEqual(len(wire), 8)
                            self.assertTrue(all(type(row["manifest"]) is dict for row in wire))
                            for compact, emitted in zip(records, wire):
                                self.assertEqual(compact["manifest"].fingerprint, campaign.proofs.digest(emitted["manifest"]))
                        else:
                            with self.assertRaises(RuntimeError) as caught:
                                campaign.run_phase(group, clients, {label: captures[label].evidence["sources"] for label in captures},
                                                   "fair", 1, deadline, root)
                            self.assertIs(caught.exception, primary)
                    self.assertIn("a", stopped)
                    self.assertIn("b", stopped)
                    self.assertEqual(group.cleanup_deadline, h)
                    self.assertEqual((root / "measurements/fair/measurements.jsonl").exists(), boundary == "success")
                    self.assertFalse((root / "campaign.json").exists())
                finally:
                    replay.doCleanups()

    def test_producer_second_backend_start_failure_retires_both_admitted_owners_without_complete(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(budgets.time, "time", return_value=0), \
                patch.object(budgets.time, "monotonic", return_value=1000):
            root = Path(directory) / "owned"
            deadline = "1970-01-01T03:55:00Z"
            budget = budgets.IsolatedCampaignBudget(deadline, 3)
            options = SimpleNamespace(run_root=root, mega_sha="1" * 40)
            primary = RuntimeError("second backend failed")
            def start(owner, _deadline):
                if owner.client == "b":
                    raise primary
                owner.state = "running"
            with patch.object(backend_module.ci, "hosted_root", return_value=(root, "owned-campaign")), \
                 patch.object(backend_module.OwnedBackend, "start", new=start):
                group = backend_module.BackendGroup(options, budget)
                (root / "measurements/fair").mkdir(parents=True)
                h = budget.cleanup_deadline
                with self.assertRaises(RuntimeError) as caught:
                    campaign.run_phase(group, [], {}, "fair", 1, budget.round_deadline(1), root)
                self.assertIs(caught.exception, primary)
                self.assertEqual([owner.state for owner in group.backends], ["retired", "retired"])
                self.assertTrue(group.closed)
                self.assertEqual(h, budget.cleanup_deadline)
                self.assertFalse((root / "measurements/fair/measurements.jsonl").exists())
                self.assertFalse((root / "campaign.json").exists())

    def test_partial_pair_failure_retains_interrupt_and_both_cleanup_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "measurements/fair").mkdir(parents=True)
            first_error, second_error = RuntimeError("first cleanup"), RuntimeError("second cleanup")
            owners = [SimpleNamespace(stop=Mock(side_effect=first_error)),
                      SimpleNamespace(stop=Mock(side_effect=second_error))]
            group = SimpleNamespace(root=root, start_pair=Mock(return_value=owners), cleanup_deadline=1e20)
            primary = KeyboardInterrupt()
            with patch.object(campaign, "canonical_seed", side_effect=primary):
                with self.assertRaises(BaseExceptionGroup) as caught:
                    campaign.run_phase(group, [], {}, "fair", 1, 1e20, root)
            self.assertIn(primary, caught.exception.exceptions)
            self.assertIn(first_error, caught.exception.exceptions)
            self.assertIn(second_error, caught.exception.exceptions)
            self.assertTrue(all(owner.stop.call_count == 1 for owner in owners))

    def test_missing_duplicate_diagnostic_a_or_boolean_round_matrix_rejects(self):
        fair = [{"phase": "fair", "round": n, "version": v, "client": c}
                for n in range(1, 4) for v in campaign.common.SCENARIOS for c in ("a", "b")]
        for records in (fair[:-1], fair + [fair[0]], [{**fair[0], "round": True}, *fair[1:]]):
            with self.assertRaises(AssertionError):
                campaign.validate_matrix(records, "fair")
        diagnostic = [{"phase": "diagnostic", "round": 1, "version": v, "client": "a"}
                      for v in campaign.common.SCENARIOS]
        with self.assertRaises(AssertionError):
            campaign.validate_matrix(diagnostic, "diagnostic")

    def test_real_git_seed_is_generated_once_with_one_common_linear_parent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "initial"
            source.mkdir()
            def git(*args):
                return subprocess.check_output(["git", "-C", str(source), *args]).decode().strip()
            git("init", "--quiet", "--initial-branch=main")
            git("-c", "user.name=seed", "-c", "user.email=seed@example.invalid", "-c", "commit.gpgsign=false",
                "commit", "--allow-empty", "--quiet", "-m", "initial")
            parent = git("rev-parse", "HEAD")
            owned = root / "owned"
            owned.mkdir()
            group = SimpleNamespace(root=owned, seed=None, options=SimpleNamespace(bootstrap_commit_time=1700000000))
            owner = SimpleNamespace(initial_commit=parent, git_url=str(source), git_env=campaign.common.clean_env())
            seed = campaign.canonical_seed(group, owner, time.monotonic() + 30)
            fixture = owned / "canonical-seed"
            parents = subprocess.check_output(["git", "-C", str(fixture), "rev-list", "--parents", "-n", "1", seed["commit"]]).decode().split()
            self.assertEqual(parents, [seed["commit"], parent])
            body = subprocess.check_output(["git", "-C", str(fixture), "cat-file", "commit", seed["commit"]])
            self.assertIn(b"1700000000 +0000", body)
            from workspace_update_campaign_export import validate_seed
            self.assertEqual(validate_seed(seed), seed)
            tampered = deepcopy(seed)
            tampered["commit_body_hex"] = (body + b"extra parent or message").hex()
            with self.assertRaises(AssertionError):
                validate_seed(tampered)
            self.assertIs(campaign.canonical_seed(group, owner, time.monotonic() + 30), seed)
            wrong = SimpleNamespace(initial_commit="0" * 40)
            with self.assertRaises(AssertionError):
                campaign.canonical_seed(group, wrong, time.monotonic() + 30)

    def test_all_18_walks_and_retained_fd_upper_checks_are_required(self):
        records, final = workload()
        self.assertEqual(campaign.validate_workload(records, final), 18)

    def test_omitted_or_reordered_previous_view_is_rejected(self):
        for change in (lambda records: records[3]["result"]["old_views"].pop(),
                       lambda records: records[3]["result"]["old_views"].reverse()):
            records, final = workload()
            change(records)
            with self.assertRaises(AssertionError):
                campaign.validate_workload(records, final)

    def test_old_fd_and_dirty_upper_false_or_boolean_count_reject(self):
        for field, value in (("fd_verified", False), ("dirty_upper_verified", False)):
            records, final = workload()
            records[1]["result"]["old_views"][0][field] = value
            with self.assertRaises(AssertionError):
                campaign.validate_workload(records, final)
        records, final = workload()
        records[0]["result"]["scorpio"]["oracle"]["verified_files"] = True
        with self.assertRaises(AssertionError):
            campaign.validate_workload(records, final)

    def test_git_empty_directory_exclusion_does_not_weaken_scorpio(self):
        records, final = workload()
        records[0]["result"]["scorpio"]["oracle"]["verified_directories"] = 1
        with self.assertRaises(AssertionError):
            campaign.validate_workload(records, final)

    def test_complete_current_manifest_bytes_and_final_views_cannot_be_dropped(self):
        for field in ("verified_bytes", "verified_files"):
            records, final = workload()
            records[0]["result"]["git"]["oracle"][field] -= 1
            with self.assertRaises(AssertionError):
                campaign.validate_workload(records, final)
        records, final = workload()
        final["views"].pop()
        with self.assertRaises(AssertionError):
            campaign.validate_workload(records, final)

    def test_wrong_current_git_commit_or_workspace_identity_rejects(self):
        for change in (lambda r: r[0]["result"]["git"].update(commit="9" * 40),
                       lambda r: r[0]["result"]["actual_status"].update(snapshot_id="different")):
            records, final = workload()
            # Avoid fixture aliasing between actual status and expected binding.
            records = deepcopy(records)
            records[0]["result"]["actual_status"] = dict(records[0]["result"]["actual_status"])
            change(records)
            with self.assertRaises(AssertionError):
                campaign.validate_workload(records, final)

    def test_strict_canonical_bootstrap_timestamp(self):
        self.assertEqual(campaign.commit_time("4294967295"), 4294967295)
        self.assertEqual(campaign.commit_time("0"), 0)
        for value in ("4294967296", "-1", "+1", "1.0", "١", " 1", "1\n", True):
            with self.assertRaises(ValueError):
                campaign.commit_time(value)

    def test_fair_append_refuses_profile_and_diagnostic_labels_every_record(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "measurements.jsonl"
            with self.assertRaises(AssertionError):
                campaign.append(path, {"record": "round", "scorpio": {"read_profile": {}}}, "fair", 1e20)
            campaign.append(path, {"record": "environment"}, "diagnostic", 1e20)
            row = json.loads(path.read_text())
            self.assertFalse(row["performance_comparison_allowed"])
            self.assertEqual(row["measurement_interpretation"], campaign.profiles.DIAGNOSTIC_INTERPRETATION)


if __name__ == "__main__":
    unittest.main()
