"""Safe artifact boundaries and independent phase validation."""

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import hashlib
import uuid
from copy import deepcopy
from dataclasses import replace
import test_workspace_update_profile as profile_fixtures

import workspace_update_campaign_export as export_module
import workspace_update_profile as profiles
import test_workspace_update_backend_proof as proof_fixtures
import test_workspace_update_campaign as workload_fixtures


class CampaignExportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "owned"
        self.root.mkdir()
        self.safe = Path(self.temp.name) / "safe"
        self.deadline = (datetime.now(timezone.utc) + timedelta(minutes=15)).isoformat()

    def write(self, relative, value, *, jsonl=False):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        raw = (json.dumps(value) + "\n").encode()
        path.write_bytes(raw)
        return path

    def test_private_configs_tokens_logs_and_fixture_never_enter_allowlist(self):
        for relative in ("backends/fair-r01-a/service.toml", "backends/fair-r01-a/mst2-token",
                         "measurements/fair/round-01/client-a/scorpio-private.log",
                         "measurements/fair/round-01/client-a/scorpio.toml",
                         "measurements/fair/round-01/fixture/token",
                         "measurements/fair/round-01/client-a/scorpio-store/blob"):
            self.write(relative, {"PRIVATE_SENTINEL": True})
        self.write("failure.json", {"execution_failed": True, "error_type": "TimeoutError"})
        copied = export_module.export(self.root, self.safe, self.deadline)
        self.assertEqual(set(copied), {"failure.json"})
        for path in self.safe.rglob("*"):
            if path.is_file():
                self.assertNotIn(b"PRIVATE_SENTINEL", path.read_bytes())
        self.assertFalse(json.loads((self.safe / "safe-export.json").read_text())["complete_campaign"])

    def test_mandatory_diagnostic_and_unprofiled_fair_partial_environment_are_separate(self):
        self.write("measurements/fair/measurements.jsonl", {"record": "environment", "phase": "fair"})
        self.write("measurements/diagnostic/measurements.jsonl", {"record": "environment", "phase": "diagnostic",
            "measurement_interpretation": profiles.DIAGNOSTIC_INTERPRETATION, "performance_comparison_allowed": False})
        copied = export_module.export(self.root, self.safe, self.deadline)
        self.assertIn("measurements/fair/measurements.jsonl", copied)
        self.assertIn("measurements/diagnostic/measurements.jsonl", copied)
        self.assertFalse(json.loads((self.safe / "safe-export.json").read_text())["complete_campaign"])

    def test_partial_diagnostic_without_marker_is_rejected(self):
        self.write("measurements/diagnostic/measurements.jsonl", {"record": "environment"})
        with self.assertRaises(profiles.ProfileError):
            export_module.export(self.root, self.safe, self.deadline)

    def test_nonexistent_early_owned_root_exports_no_false_completion(self):
        empty = Path(self.temp.name) / "not-started"
        copied = export_module.export(empty, self.safe, self.deadline)
        self.assertEqual(copied, {})
        self.assertFalse(json.loads((self.safe / "safe-export.json").read_text())["complete_campaign"])

    def test_early_build_failure_keeps_actual_allowlisted_run_sources_and_original_window(self):
        started = datetime.now(timezone.utc)
        env = {"GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "2", "SCORPIO_SHA": "a" * 40, "GITHUB_SHA": "a" * 40,
            "MEGA_SHA": "b" * 40, "BASELINE_SHA": export_module.campaign.BASELINE, "CANDIDATE_SHA": export_module.campaign.CANDIDATE,
            "PROFILE": "medium", "ROUNDS": "3", "COMPARISON": "isolated", "BOOTSTRAP_COMMIT_TIME": "1700000000",
            "STARTED_INPUT": started.isoformat(), "DEADLINE_INPUT": (started + timedelta(minutes=235)).isoformat(),
            "MST2_WORK_CLEANUP_DEADLINE_MONOTONIC": "999999", "PRIVATE_TOKEN": "PRIVATE_SENTINEL",
            "RUNNER_TEMP": "/tmp", "MST2_OWNED_ROOT": "/tmp/mst2-real-123-2"}
        with patch.dict(os.environ, env, clear=True):
            metadata = export_module.run_metadata_from_env()
        missing = Path(self.temp.name) / "build-failed-before-owned-start"
        export_module.export(missing, self.safe, env["DEADLINE_INPUT"], run_metadata=metadata)
        actual = json.loads((self.safe / "run.json").read_text())
        self.assertEqual(actual["run_id"], "123")
        self.assertEqual(actual["session_started_utc"], env["STARTED_INPUT"])
        self.assertEqual(set(actual), export_module.RUN_FIELDS)
        self.assertNotIn("PRIVATE_SENTINEL", (self.safe / "run.json").read_text())
        self.assertFalse(json.loads((self.safe / "safe-export.json").read_text())["complete_campaign"])

    def test_destination_cannot_be_reused_or_inside_owned_root(self):
        for output in (self.root / "export", self.root):
            with self.assertRaises(ValueError):
                export_module.export(self.root, output, self.deadline)

    def test_hardlinked_allowed_file_is_rejected(self):
        source = self.write("failure.json", {"execution_failed": True})
        os.link(source, Path(self.temp.name) / "borrowed.json")
        with self.assertRaises(AssertionError):
            export_module.export(self.root, self.safe, self.deadline)

    @unittest.skipIf(os.name == "nt", "Windows symlink privilege is not assumed")
    def test_symlink_allowed_file_and_ancestor_are_rejected(self):
        outside = Path(self.temp.name) / "outside.json"
        outside.write_text("{}\n")
        (self.root / "failure.json").symlink_to(outside)
        with self.assertRaises(AssertionError):
            export_module.export(self.root, self.safe, self.deadline)

    def test_expired_utc_rejects_before_output_creation(self):
        past = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
        with self.assertRaises(TimeoutError):
            export_module.export(self.root, self.safe, past)
        self.assertFalse(self.safe.exists())

    def test_complete_receipt_with_six_owners_cannot_export_as_success(self):
        self.write("campaign.json", {"record": "isolated_campaign_complete", "correctness": "PASS",
            "fair_records": 24, "diagnostic_records": 4, "fair_full_oracle_walks": 108,
            "diagnostic_full_oracle_walks": 18, "cleanup": {"owners": 6, "closed": True}})
        with self.assertRaises(AssertionError):
            export_module.export(self.root, self.safe, self.deadline)
        self.assertFalse(self.safe.exists())

    def test_allowlist_requires_exact_phase_round_label_and_manifest(self):
        for path in ("measurements/fair/round-04/v1-expected.json",
                     "measurements/diagnostic/round-02/v1-expected.json",
                     "measurements/diagnostic/round-01/client-a/workspace-observation.jsonl",
                     "measurements/fair/round-01/client-c/owned-workspace-worker.json",
                     "measurements/fair/round-01/v5-expected.json"):
            self.assertFalse(export_module.allowed(path), path)
        self.assertTrue(export_module.allowed("measurements/diagnostic/round-01/client-b/workspace-observation.jsonl"))

    def replay_lane(self, fixture, phase="fair", number=1, label="a"):
        key = f"{phase}-r{number:02}-{label}"
        owner_root = getattr(self, "fixture_owned_root", "/owned") + "/backends/" + key
        runtime = fixture.runtime(label, phase=phase, round=number, base_dir=owner_root + "/service-data",
            cache_dir=owner_root + "/cache", pack_cache_dir=owner_root + "/pack-cache",
            project=getattr(self, "fixture_project", "project") + "-" + key,
            cache_prefix="prefix-" + key, service_pid=number * 1000 + (1 if label == "a" else 2) + (10000 if phase == "diagnostic" else 0),
            service_starttime=str(990 + number),
            projection_sink_root=owner_root + "/cache/logs/mst2-native-projection/" + str(uuid.uuid5(uuid.NAMESPACE_DNS, "sink-" + key)),
            projection_sink_instance=str(uuid.uuid5(uuid.NAMESPACE_DNS, "sink-" + key)))
        instance = str(uuid.uuid5(uuid.NAMESPACE_DNS, "instance-" + key))
        native = runtime.native
        native.update(instance_id=instance, certificate_instance=instance)
        rows = runtime.identity_rows
        for row in rows:
            row["database"] = "db-" + key
        runtime = replace(runtime, instance_id=instance, database="db-" + key,
            dependency_container_ids=tuple(hashlib.sha256((key + str(i)).encode()).hexdigest() for i in range(4)),
            projection_sink_inode=number * 100 + (1 if label == "a" else 2) + (1000 if phase == "diagnostic" else 0),
            native_json=export_module.proofs.canonical(native), identity_rows_json=export_module.proofs.canonical(rows))
        captured = fixture.capture(label, runtime=runtime)
        workload, final = workload_fixtures.workload()
        lanes, payloads = [], []
        for index in range(4):
            lane = fixture.lane(captured)
            lane["version"] = index + 1
            binding = lane["workspace_binding"]
            binding["workspace_id"] = str(uuid.uuid5(uuid.NAMESPACE_DNS, "campaign-view-" + key + str(index)))
            binding["generation"] = str(uuid.uuid5(uuid.NAMESPACE_DNS, "campaign-generation-" + key + str(index)))
            binding["store"] += "/" + str(index)
            logical = "ws:" + binding["run_id"] + ":" + binding["workspace_id"]
            binding["logical_request_id"] = logical
            binding["resolve_trace_receipt"] = {"logical_request_id": logical, "attempt_ids": [logical + ":a1"],
                "final_attempt_id": logical + ":a1", "retry_count": 0}
            payload = export_module.observation.parse(lane["projection_sink"]["records_jsonl"])["payload"]
            payload["request_id"] = logical + ":a1"
            payloads.append(payload)
            manifest_path = self.root / f"measurements/{phase}/round-{number:02}/v{index+1}-expected.json"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            raw = (export_module.proofs.canonical(workload[index]["manifest"]) + "\n").encode()
            manifest_path.write_bytes(raw)
            lane["oracle_manifest_sha256"] = hashlib.sha256(raw).hexdigest()
            lane["workspace_binding_sha256"] = export_module.proofs.digest(binding)
            lanes.append(lane)
        for lane in lanes:
            fixture.set_sink(lane, payloads, captured.evidence["runtime"])
            lane["projection_sink"].update(registered_receipts=[l["workspace_binding"]["resolve_trace_receipt"] for l in lanes],
                                            expected_final_count=4)
        leaf = self.root / f"measurements/{phase}/round-{number:02}/client-{label}"
        leaf.mkdir(exist_ok=True)
        bindings_raw = b"".join((export_module.proofs.canonical(lane["workspace_binding"]) + "\n").encode() for lane in lanes)
        footer = {"record": "workspace_observation_footer", "revision": 1, "run_id": lanes[0]["workspace_binding"]["run_id"],
            "accepted_records": 4, "received_records": 4, "written_records": 4, "written_bytes": len(bindings_raw),
            "producers_closed": True, "drained": True, "daemon_exit_code": 0, "complete": True, "first_error": None}
        sink_raw = bindings_raw + (export_module.proofs.canonical(footer) + "\n").encode()
        (leaf / "workspace-observation.jsonl").write_bytes(sink_raw)
        cleanup = {}
        for name in ("owned-workspace-daemon.json", "owned-workspace-worker.json"):
            value = {"pid": 99999, "starttime": "777", "cleanup_complete": True}
            raw = (export_module.proofs.canonical(value) + "\n").encode()
            (leaf / name).write_bytes(raw)
            cleanup[name] = {"record": value, "sha256": hashlib.sha256(raw).hexdigest()}
        records = []
        for index, lane in enumerate(lanes):
            proof = export_module.proofs.validate_lane_measurement(lane, captured)
            result = workload[index]["result"]
            result["round"] = number
            result["actual_status"] = lane["workspace_binding"]
            result["git"].update(commit=lane["fixed_commit"], verified_ms=6.0)
            result["scorpio"].update(metadata_ready_ms=3.0, durable_complete_ms=4.0, durable_verified_ms=5.0)
            if phase == "diagnostic":
                result["scorpio"]["read_profile"] = profile_fixtures.evidence()
            result["old_views"] = []
            for previous in records:
                old = deepcopy(final["views"][0])
                old.update({key: previous["workspace_binding"][key] for key in ("workspace_id", "generation", "snapshot_id")})
                result["old_views"].append(old)
            record = {"record": "round", "revision": 2, "correctness": "PASS", "lane_full_oracle_walks": 18,
                "phase": phase, "round": number, "client": label, "version": "v" + str(index + 1),
                "fixed_commit": lane["fixed_commit"], "path_tree": lane["path_tree"],
                "oracle_manifest_sha256": lane["oracle_manifest_sha256"], "manifest": workload[index]["manifest"],
                "manifest_relative_path": f"measurements/{phase}/round-{number:02}/v{index+1}-expected.json",
                "publication": lane["publication"], "operation_started_monotonic": lane["publication"]["visible_monotonic"] + 1,
                "operation_finished_monotonic": lane["publication"]["visible_monotonic"] + 2,
                "workspace_binding": lane["workspace_binding"], "actual_status": result["actual_status"],
                "scorpio": result["scorpio"], "git": result["git"], "old_views": result["old_views"],
                "semantic_provenance": proof.evidence, "lane_proof_sha256": export_module.proofs.digest(proof.evidence),
                "workspace_sink_sha256": hashlib.sha256(sink_raw).hexdigest(), "cleanup_receipts": cleanup}
            records.append(record)
        final["views"] = []
        for record in records:
            old = deepcopy(workload_fixtures.workload()[1]["views"][0])
            old.update({key: record["workspace_binding"][key] for key in ("workspace_id", "generation", "snapshot_id")})
            final["views"].append(old)
        for record in records:
            record["round_final_retained_views"] = final
        owner = {"root": owner_root, **{key: captured.evidence["runtime"][key]
            for key in ("phase", "round", "client", "project", "database", "instance_id", "service_pid", "service_starttime")}}
        export_module.validate_saved_lane(self.root, records, owner, captured.evidence["sources"], captured.evidence["client_build"])
        return records, owner, captured, leaf, bindings_raw, sink_raw

    def proof_fixture(self):
        fixture_class = proof_fixtures.BackendProofTests
        fixture_class.setUpClass()
        self.addCleanup(fixture_class.doClassCleanups)
        return fixture_class(methodName="runTest")

    def test_closed_lane_replay_binds_actual_files_manifests_footer_build_owner_and_all_walks(self):
        # Typed fixtures test replay; they never constitute native execution.
        records, owner, captured, leaf, bindings_raw, sink_raw = self.replay_lane(self.proof_fixture())
        for change in ("owner", "source", "manifest", "footer"):
            altered_owner, altered_sources = deepcopy(owner), deepcopy(captured.evidence["sources"])
            original_manifest = (self.root / records[0]["manifest_relative_path"]).read_bytes()
            if change == "owner":
                altered_owner["database"] = "different"
            elif change == "source":
                altered_sources["server_source_sha"] = "0" * 40
            elif change == "manifest":
                (self.root / records[0]["manifest_relative_path"]).write_bytes(b"{}\n")
            else:
                (leaf / "workspace-observation.jsonl").write_bytes(bindings_raw)
            with self.assertRaises(AssertionError):
                export_module.validate_saved_lane(self.root, records, altered_owner, altered_sources, captured.evidence["client_build"])
            (self.root / records[0]["manifest_relative_path"]).write_bytes(original_manifest)
            (leaf / "workspace-observation.jsonl").write_bytes(sink_raw)

    def test_offline_lane_replay_rejects_malformed_config_and_compose_hashes(self):
        records, _owner, *_ = self.replay_lane(self.proof_fixture())
        original = records[0]["semantic_provenance"]
        for key in ("config_sha256", "compose_sha256"):
            for invalid in (True, "0" * 63, "g" * 64):
                with self.subTest(key=key, invalid=invalid):
                    altered = deepcopy(original)
                    altered["capture"]["runtime"][key] = invalid
                    with self.assertRaises(export_module.proofs.ProofRejected):
                        export_module.proofs.validate_lane_values(altered["lane"], altered["capture"])

    def test_full_28_record_export_accepts_z_deadline_with_normalized_owners_and_rejects_tampering(self):
        fixture = self.proof_fixture()
        sources = deepcopy(fixture.sources)
        server = {"revision": 1, "label": "server", "source": "/immutable/server", "source_sha": "1" * 40,
            "cargo_lock_sha256": "4" * 64, "binary": "/immutable/server/target/release/mega2", "binary_sha256": "3" * 64,
            "build_argv": export_module.builds.build_argv(Path("/immutable/server"), "mega2"),
            "build_env": export_module.builds.BUILD_ENV, "rustc_version": sources["a"]["rustc_version"],
            "cargo_version": sources["a"]["cargo_version"]}
        server_path = Path(self.temp.name) / "server-build.json"
        server_raw = (export_module.proofs.canonical(server) + "\n").encode()
        server_path.write_bytes(server_raw)
        oid = lambda kind, raw: hashlib.sha1(kind.encode() + b" " + str(len(raw)).encode() + b"\0" + raw).hexdigest()
        tree_raw = b""
        tree = oid("tree", tree_raw)
        parent_raw = (f"tree {tree}\nauthor Fixture <fixture@example.invalid> 1700000000 +0800\n"
                      "committer Fixture <fixture@example.invalid> 1700000000 +0800\n\ninitial\n").encode()
        parent = oid("commit", parent_raw)
        commit_raw = (f"tree {tree}\nparent {parent}\nauthor MST2 setup baseline <mst2-setup@example.invalid> 1700000000 +0000\n"
            "committer MST2 setup baseline <mst2-setup@example.invalid> 1700000000 +0000\n\nMST2 canonical native seed\n").encode()
        seed = {"parent": parent, "commit": oid("commit", commit_raw), "tree": tree,
            "parent_commit_body_hex": parent_raw.hex(), "commit_body_hex": commit_raw.hex(), "tree_body_hex": "",
            "commit_body_sha256": hashlib.sha256(commit_raw).hexdigest(), "tree_body_sha256": hashlib.sha256(tree_raw).hexdigest(),
            "bootstrap_commit_time": 1700000000}
        self.write("canonical-seed.json", seed)
        owners, inventory = [], []
        phase_rows = {"fair": [], "diagnostic": []}
        start = datetime.now(timezone.utc)
        deadline = (start + timedelta(minutes=235)).isoformat().replace("+00:00", "Z")
        self.fixture_owned_root = "/tmp/mst2-real-123-1"
        self.fixture_project = "m2perf-123-1"
        metadata = {"revision": 1, "run_id": "123", "attempt": "1", "harness_sha": sources["a"]["harness_source_sha"],
            "mega_sha": server["source_sha"], "baseline_sha": sources["a"]["client_source_sha"],
            "candidate_sha": sources["b"]["client_source_sha"], "profile": "medium", "rounds": 3,
            "comparison": "isolated", "bootstrap_commit_time": 1700000000, "session_started_utc": start.isoformat(),
            "session_deadline_utc": deadline, "cleanup_deadline_monotonic": 999999., "owned_root": self.fixture_owned_root}
        self.write("run.json", metadata)
        for phase in phase_rows:
            env = {"record": "environment", "phase": phase, "sources": sources, "server_build": server,
                "session_started_utc": start.isoformat(), "session_deadline_utc": deadline,
                "cleanup_deadline_monotonic": 999999., "run_id": "123", "run_attempt": "1", "profile": "medium",
                "rounds": 3 if phase == "fair" else 1}
            if phase == "diagnostic":
                env.update(measurement_interpretation=profiles.DIAGNOSTIC_INTERPRETATION, performance_comparison_allowed=False)
            phase_rows[phase].append(env)
            for number in (range(1, 4) if phase == "fair" else (1,)):
                for label in (("a", "b") if phase == "fair" else ("b",)):
                    records, owner, _capture, *_ = self.replay_lane(fixture, phase, number, label)
                    owner.update(state="retired", operation_deadline_monotonic=99999., initial_path_commit=parent)
                    owners.append(owner)
                    inventory.append({"owner": owner, "client_cleanup": records[0]["cleanup_receipts"],
                                      "dependency_containers_remaining": 0, "dependency_networks_remaining": 0})
                    for record in records:
                        if phase == "diagnostic":
                            record.update(measurement_interpretation=profiles.DIAGNOSTIC_INTERPRETATION, performance_comparison_allowed=False)
                        phase_rows[phase].append(record)
            complete = {"record": "complete", "phase": phase, "correctness": "PASS", "campaign_cleanup_complete": True,
                "round_scenarios": 24 if phase == "fair" else 4, "full_oracle_walks": 108 if phase == "fair" else 18}
            if phase == "diagnostic":
                complete.update(measurement_interpretation=profiles.DIAGNOSTIC_INTERPRETATION, performance_comparison_allowed=False)
            phase_rows[phase].append(complete)
            path = self.root / "measurements" / phase / "measurements.jsonl"
            path.write_bytes(b"".join((export_module.proofs.canonical(row) + "\n").encode() for row in phase_rows[phase]))
        # Producer stores normalized UTC; dispatch/run/environment retain Z.
        owned = {"revision": 1, "project": self.fixture_project,
            "session_deadline_utc": (start + timedelta(minutes=235)).isoformat(),
            "cleanup_deadline_monotonic": 999999., "measurement_deadline_monotonic": 999999.
            - export_module.budgets.CAMPAIGN_REPORT - export_module.budgets.CLEANUP_RESERVE
            - export_module.budgets.CAMPAIGN_MARGIN, "backends": owners, "closed": True}
        owned_path = self.write("backend-owners.json", owned)
        value = {"revision": 1, "record": "isolated_campaign_complete", "correctness": "PASS",
            "session_started_utc": start.isoformat(), "session_deadline_utc": deadline, "cleanup_deadline_monotonic": 999999.,
            "sources": sources, "canonical_seed": seed, "server_build": server,
            "server_build_receipt_sha256": hashlib.sha256(server_raw).hexdigest(),
            "fair_records": 24, "diagnostic_records": 4, "fair_full_oracle_walks": 108, "diagnostic_full_oracle_walks": 18,
            "cleanup": {"owners": 7, "closed": True, "inventory": inventory, "backend_owners_sha256": hashlib.sha256(owned_path.read_bytes()).hexdigest()},
            "phase_measurements_sha256": {phase: hashlib.sha256((self.root / "measurements" / phase / "measurements.jsonl").read_bytes()).hexdigest()
                                          for phase in phase_rows}, "elapsed_execution_seconds": 1., "performance_claims": "TEST_FIXTURE_NOT_NATIVE_RESULT"}
        campaign_path = self.write("campaign.json", value)
        receipts = {"a": fixture.builds["a"], "b": fixture.builds["b"], "server": server_path}
        with patch.object(export_module.campaign, "BASELINE", sources["a"]["client_source_sha"]), \
             patch.object(export_module.campaign, "CANDIDATE", sources["b"]["client_source_sha"]):
            copied = export_module.export(self.root, self.safe, deadline, list(receipts.items()), run_metadata=metadata)
            self.assertTrue(json.loads((self.safe / "safe-export.json").read_text())["complete_campaign"])
            self.assertIn("campaign.json", copied)
            self.assertEqual(len(export_module.validate_complete(self.safe)["cleanup"]["inventory"]), 7)
            for mutation in ("source", "seed", "six_owners"):
                changed = deepcopy(value)
                if mutation == "source":
                    changed["sources"]["a"]["server_source_sha"] = "0" * 40
                elif mutation == "seed":
                    changed["canonical_seed"]["commit_body_hex"] += "00"
                else:
                    changed["cleanup"]["owners"] = 6
                self.write("campaign.json", changed)
                with self.assertRaises(AssertionError):
                    export_module.validate_complete(self.root, receipts)
            campaign_path.write_text(json.dumps(value) + "\n")
            for mutation in ("environment_run_id", "owner_root", "diagnostic_not_measured", "owners_shape",
                             "owners_revision", "owners_project", "owners_d", "owners_h", "owners_measurement",
                             "inventory_client_cleanup"):
                changed = deepcopy(value)
                changed_owners = deepcopy(owned)
                changed_rows = deepcopy(phase_rows)
                if mutation == "environment_run_id":
                    changed_rows["fair"][0]["run_id"] = "456"
                elif mutation == "owner_root":
                    changed_owners["backends"][0]["root"] = "/tmp/mst2-real-456-1/backends/fair-r01-a"
                    changed["cleanup"]["inventory"][0]["owner"]["root"] = changed_owners["backends"][0]["root"]
                elif mutation == "diagnostic_not_measured":
                    changed_rows["diagnostic"][1]["scorpio"]["read_profile"] = profiles.not_measured("disabled")
                elif mutation == "owners_shape":
                    changed_owners["unexpected"] = True
                elif mutation == "owners_revision":
                    changed_owners["revision"] = 2
                elif mutation == "owners_project":
                    changed_owners["project"] = "m2perf-456-1"
                elif mutation == "owners_d":
                    changed_owners["session_deadline_utc"] = (start + timedelta(minutes=236)).isoformat()
                elif mutation == "owners_h":
                    changed_owners["cleanup_deadline_monotonic"] += 1
                elif mutation == "owners_measurement":
                    changed_owners["measurement_deadline_monotonic"] += 1
                else:
                    changed["cleanup"]["inventory"][0]["client_cleanup"]["owned-workspace-daemon.json"]["sha256"] = "0" * 64
                for phase in phase_rows:
                    path = self.root / "measurements" / phase / "measurements.jsonl"
                    path.write_bytes(b"".join((export_module.proofs.canonical(row) + "\n").encode() for row in changed_rows[phase]))
                    changed["phase_measurements_sha256"][phase] = hashlib.sha256(path.read_bytes()).hexdigest()
                self.write("backend-owners.json", changed_owners)
                changed["cleanup"]["backend_owners_sha256"] = hashlib.sha256(owned_path.read_bytes()).hexdigest()
                self.write("campaign.json", changed)
                with self.subTest(mutation=mutation), self.assertRaises(AssertionError):
                    export_module.validate_complete(self.root, receipts)

    def test_original_d_is_checked_between_reads_and_after_final_export(self):
        self.write("failure.json", {"execution_failed": True})
        with patch.object(export_module.budgets, "require_external_time", side_effect=[20, 20, TimeoutError("D exhausted")]):
            with self.assertRaises(TimeoutError):
                export_module.export(self.root, self.safe, self.deadline)
        self.assertFalse((self.safe / "safe-export.json").exists())


if __name__ == "__main__":
    unittest.main()
