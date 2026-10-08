"""Small deterministic admission and streaming checks; never generate large."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import commit_update_bench as bench
import commit_update_ci as ci
import workspace_update_campaign as campaign
import workspace_update_campaign_export as export
import workspace_update_size as size
import workspace_update_build as builds
import workspace_update_bench as measurement


def manifest():
    return {"directories": ["", "source", "alias", "empty"], "files": [
        {"rel_path": directory + "/f", "fs_kind": "regular", "size": 8,
         "content_digest": "sha256:" + "1" * 64} for directory in ("source", "alias")]}


class FixtureSizeTests(unittest.TestCase):
    def test_history_plan_covers_fivefold_tree_and_rotating_cohorts_under_frozen_limits(self):
        report = size.plan("history-large")
        self.assertEqual((report["logical_files"], report["logical_directories"], report["logical_entries"]),
                         (85122, 835, 85957))
        history = report["history_admission"]
        self.assertEqual((history["versions"], history["source_files_rewritten_per_increment"],
                          history["source_bytes_rewritten_per_increment"], history["full_oracle_walks_per_lane"]),
                         (10, 16896, 132 * 1024 ** 2, 75))
        self.assertEqual((history["cohort_count"], history["modules_rewritten_per_increment"],
                          history["modules_preserved_per_increment"]), (5, 33, 132))
        self.assertEqual(history["source_dictionary_entries_per_increment_upper"], 17716)
        self.assertEqual(history["source_entry_references_upper"], 246425)
        self.assertEqual(history["source_entry_references_upper"], report["logical_entries"]
                         + 9 * history["source_dictionary_entries_per_increment_upper"] + 1024)
        self.assertEqual(history["resident_metadata_pages_upper"], 10 * report["metadata_pages_upper"] + 64)
        for key, limit in size.HISTORY_LIMITS.items():
            self.assertLessEqual(history[key], limit)
        for key in size.HISTORY_LIMITS:
            with patch.dict(size.HISTORY_LIMITS, {key: 0}), self.assertRaises(ValueError):
                size.plan("history-large")
        # A current tree can fit while ten immutable source dictionaries do not.
        with patch.dict(size.PROFILES, {"history-large": (240, 4, 128, 8192)}), \
                self.assertRaisesRegex(ValueError, "retained source_entry"):
            size.plan("history-large")

    def test_history_disk_reservation_counts_every_new_body_and_seventy_worktrees(self):
        fixture = size.plan("history-large")
        report = size.campaign_disk_plan("history-large")
        self.assertEqual((report["lanes"], report["versions_per_lane"], report["retained_git_detached_checkouts"]), (7, 10, 70))
        self.assertEqual(report["retained_unique_content_bytes"] - report["cold_unique_content_bytes"],
                         9 * 132 * 1024 ** 2)
        self.assertGreaterEqual(report["components_bytes"]["retained_git_detached_checkouts"],
                                70 * fixture["logical_content_bytes"])
        self.assertEqual(report["minimum_free_bytes"], sum(report["components_bytes"].values()))
        self.assertGreater(report["minimum_free_bytes"], 190 * 1024 ** 3)
        self.assertLess(report["minimum_free_bytes"], 300 * 1024 ** 3)
        with self.assertRaises(ValueError):
            size.admit_backend("history-large", False)
        self.assertEqual(size.admit_backend("history-large", True)["profile"], "history-large")

    def test_history_cohorts_cover_all_modules_once_then_repeat_without_reusing_bytes(self):
        self.assertEqual(size.history_modules("v1"), tuple(range(165)))
        cohorts = [size.history_modules(f"v{version}") for version in range(2, 7)]
        self.assertTrue(all(len(cohort) == 33 for cohort in cohorts))
        self.assertEqual(tuple(module for cohort in cohorts for module in cohort), tuple(range(165)))
        self.assertEqual(size.history_modules("v7"), cohorts[0])
        self.assertEqual(size.history_modules("v10"), cohorts[3])
        for invalid in (None, True, "v0", "v01", "v11", "V2"):
            with self.assertRaises(ValueError):
                size.history_modules(invalid)
        with patch.dict(size.PROFILES, {"history-large": (164, 4, 128, 8192)}), self.assertRaises(ValueError):
            size.plan("history-large")

    def test_existing_runner_label_is_explicit_and_strictly_bounded(self):
        for label in ("ubuntu-latest", "perf-linux-large", "self-hosted", "a.b_1", "a" * 64):
            self.assertEqual(size.runner_label(label), label)
        for label in (None, True, "", "a" * 65, "../runner", "[self-hosted,linux]", "$(cmd)", "a b", "x\ny", "中文"):
            with self.assertRaises(ValueError):
                size.runner_label(label)

    def test_actions_root_preserves_unique_job_ownership_for_each_existing_runner_type(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory).resolve(strict=True)
            root = parent / "mst2-real-77-2"
            environment = {"GITHUB_ACTIONS": "true", "RUNNER_TEMP": str(parent),
                           "GITHUB_RUN_ID": "77", "GITHUB_RUN_ATTEMPT": "2"}
            for runner in ("github-hosted", "self-hosted"):
                with patch.dict(ci.os.environ, dict(environment, RUNNER_ENVIRONMENT=runner)), \
                        patch.object(ci.sys, "platform", "linux"):
                    self.assertEqual(ci.hosted_root(root), (root, "m2perf-77-2"))
                    for wrong in (parent / "unowned", root / "child", root.parent.parent):
                        with self.assertRaises(ValueError):
                            ci.hosted_root(wrong)
            for changes in ({"GITHUB_ACTIONS": "false"}, {"RUNNER_ENVIRONMENT": "unknown"}, {"GITHUB_RUN_ID": "bad"}):
                with patch.dict(ci.os.environ, dict(environment, RUNNER_ENVIRONMENT="self-hosted") | changes), \
                        patch.object(ci.sys, "platform", "linux"):
                    with self.assertRaises(ValueError):
                        ci.hosted_root(root)
            self.assertFalse(root.exists())

    def test_large_disk_reservation_covers_all_retained_checkouts_and_unique_history(self):
        fixture = size.plan("large")
        report = size.campaign_disk_plan("large")
        self.assertEqual((report["lanes"], report["versions_per_lane"], report["retained_git_detached_checkouts"]), (7, 4, 28))
        self.assertGreaterEqual(report["components_bytes"]["retained_git_detached_checkouts"], 28 * fixture["logical_content_bytes"])
        self.assertEqual(report["minimum_free_bytes"], sum(report["components_bytes"].values()))
        self.assertEqual(report["retained_unique_content_bytes"] - report["cold_unique_content_bytes"], 129 * 8192)
        self.assertGreater(report["minimum_free_bytes"], 14 * 1024 ** 3)

    def test_large_insufficient_disk_rejected_before_build_source_or_service_side_effects(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "not-created"
            required = size.campaign_disk_plan("large")["minimum_free_bytes"]
            with patch.object(ci, "hosted_root", return_value=(root, "owned")), \
                    patch.object(size.shutil, "disk_usage", return_value=SimpleNamespace(free=required - 1)), \
                    patch.object(builds, "clients") as clients, patch.object(builds, "load") as server, \
                    patch.object(campaign, "source_expectations") as sources, \
                    patch.object(campaign.backends, "BackendGroup") as backend, \
                    patch.object(bench, "create_version") as generation, patch.object(bench, "command") as commands:
                with self.assertRaisesRegex(ValueError, "free bytes.*available"):
                    ci.execute(SimpleNamespace(profile="large", run_root=root, paired=True, isolated_backends=True,
                        baseline_sha=builds.DEFAULT_BASELINE, candidate_sha=builds.DEFAULT_CANDIDATE))
            for effect in (clients, server, sources, backend, generation, commands):
                effect.assert_not_called()
            self.assertFalse(root.exists())
            with patch.object(size.shutil, "disk_usage", return_value=SimpleNamespace(free=required)) as disk:
                admitted = size.admit_campaign_disk("large", root)
            disk.assert_called_once_with(root.parent.resolve(strict=True))
            self.assertEqual(admitted["available_free_bytes"], required)
            self.assertFalse(root.exists())

    def test_large_shared_namespace_rejected_before_resource_or_endpoint_access(self):
        options = SimpleNamespace(profile="large", isolated_backends=False)
        with patch.object(ci, "hosted_root") as resources, patch.object(bench, "endpoint_pair") as endpoint:
            with self.assertRaises(ValueError):
                ci.execute(options)
            with self.assertRaises(ValueError):
                measurement.execute(options)
        resources.assert_not_called()
        endpoint.assert_not_called()
        self.assertEqual(size.admit_backend("large", True)["profile"], "large")
        for profile in ("smoke", "medium"):
            self.assertEqual(size.admit_backend(profile, False)["profile"], profile)

    def test_phase_hash_streams_without_whole_file_reads_and_enforces_cap_and_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "phase.jsonl"
            raw = b"x" * (2 * 1024 * 1024 + 3)
            path.write_bytes(raw)
            expected = hashlib.sha256(raw).hexdigest()
            with patch.object(Path, "read_bytes", side_effect=AssertionError("whole-file read forbidden")):
                self.assertEqual(size.file_sha256(path, 1e20), expected)
            with self.assertRaises(AssertionError):
                size.file_sha256(path, 1e20, cap=len(raw) - 1)
            with self.assertRaises(TimeoutError):
                size.file_sha256(path, 0)

    def test_historical_modes_keep_local_objects_inside_metered_owned_service_data(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = {"base_dir": str(root / "service-data"), "object_storage": {
                "storage_type": "s3", "s3": {"bucket": "unrelated"}}}
            ci.configure_owned_local_storage(config, root)
            self.assertEqual(config["object_storage"]["storage_type"], "local")
            objects = Path(config["object_storage"]["local"]["root_dir"])
            self.assertTrue(objects.is_relative_to(root / "service-data"))
            self.assertNotIn("s3", config["object_storage"])
            self.assertFalse(objects.exists())
            config["base_dir"] = str(root.parent / "other-service")
            with self.assertRaises(AssertionError):
                ci.configure_owned_local_storage(config, root)
            with self.assertRaises(AssertionError):
                ci.configure_owned_local_storage({}, Path("relative-owner"))

    def test_explicit_immutable_pair_accepts_new_commits_and_rejects_mutable_or_equal_refs(self):
        self.assertEqual(builds.comparison_pair("a" * 40, "b" * 40), ["a" * 40, "b" * 40])
        for baseline, candidate in (("main", "b" * 40), ("a" * 7, "b" * 40),
                                    ("A" * 40, "b" * 40), ("a" * 40, "a" * 40),
                                    (None, "b" * 40)):
            with self.assertRaises(ValueError):
                builds.comparison_pair(baseline, candidate)

    def test_explicit_pair_must_match_actual_build_receipts_and_export_sources(self):
        options = SimpleNamespace(paired=True, rounds=3, build_a=Path("a.json"), build_b=Path("b.json"),
                                  baseline_sha="a" * 40, candidate_sha="b" * 40)
        lanes = [SimpleNamespace(driver=Path(label), build={"source": label, "source_sha": label * 40,
                    "rustc_version": "rustc", "cargo_version": "cargo"}) for label in ("a", "b")]
        with patch.object(builds, "load", side_effect=lanes):
            self.assertEqual(builds.clients(options, 1e20), lanes)
        options.candidate_sha = "c" * 40
        with patch.object(builds, "load", side_effect=lanes):
            with self.assertRaises(AssertionError):
                builds.clients(options, 1e20)
        metadata = {"revision": 1, "run_id": "1", "attempt": "1", "harness_sha": "d" * 40,
            "mega_sha": "e" * 40, "baseline_sha": "a" * 40, "candidate_sha": "b" * 40,
            "profile": "large", "rounds": 3, "comparison": "isolated", "bootstrap_commit_time": 1,
            "session_started_utc": "2026-10-08T00:00:00Z", "session_deadline_utc": "2026-10-08T03:55:00Z",
            "cleanup_deadline_monotonic": 1e20, "owned_root": "/tmp/mst2-real-1-1"}
        complete = {key: metadata[key] for key in ("session_started_utc", "session_deadline_utc", "cleanup_deadline_monotonic")}
        complete.update(canonical_seed={"bootstrap_commit_time": 1}, sources={label: {
            "harness_source_sha": "d" * 40, "server_source_sha": "e" * 40,
            "client_source_sha": label * 40} for label in ("a", "b")})
        export.validate_run_metadata(metadata, complete)
        complete["sources"]["b"]["client_source_sha"] = "c" * 40
        with self.assertRaises(AssertionError):
            export.validate_run_metadata(metadata, complete)

    def test_independent_git_oracle_batches_by_body_bytes_and_item_count(self):
        bodies = {bytes([97 + i]) * 40: body for i, body in enumerate((b"aaa", b"bbb", b"cccc", b"ddddd"))}
        calls = []
        def git(_repo, _deadline, *args, data=None):
            if args[0] == "ls-tree":
                return b"\0".join(b"100644 blob " + oid + b"\tf" + bytes([48 + i])
                                   for i, oid in enumerate(bodies)) + b"\0"
            oids = data.splitlines()
            self.assertLessEqual(len(oids), 2)
            if args[1] == "--batch-check":
                return b"".join(oid + b" blob " + str(len(bodies[oid])).encode() + b"\n" for oid in oids)
            self.assertLessEqual(sum(len(bodies[oid]) for oid in oids), 6)
            calls.append(oids)
            return b"".join(oid + b" blob " + str(len(bodies[oid])).encode() + b"\n"
                            + bodies[oid] + b"\n" for oid in oids)
        with patch.object(bench, "git", side_effect=git), patch.object(size, "CAT_FILE_ITEMS", 2), \
                patch.object(size, "CAT_FILE_BODY_BYTES", 6):
            actual = bench.expected_manifest(Path("unused"), "a" * 40, 1e20)
        self.assertEqual([len(call) for call in calls], [2, 1, 1])
        self.assertEqual([file["content_digest"] for file in actual["files"]],
                         ["sha256:" + hashlib.sha256(body).hexdigest() for body in bodies.values()])

    def test_independent_git_oracle_rejects_single_body_before_reading_it(self):
        oid = b"a" * 40
        def git(_repo, _deadline, *args, data=None):
            if args[0] == "ls-tree":
                return b"100644 blob " + oid + b"\tf\0"
            self.assertEqual(args[1], "--batch-check")
            return oid + b" blob 7\n"
        with patch.object(bench, "git", side_effect=git), patch.object(size, "CAT_FILE_BODY_BYTES", 6):
            with self.assertRaises(ValueError):
                bench.expected_manifest(Path("unused"), "a" * 40, 1e20)

    def test_large_plan_counts_alias_and_empty_dirs_without_generating_files(self):
        with patch.object(bench, "git") as git:
            plan = size.plan("large")
        git.assert_not_called()
        self.assertEqual(plan["logical_files"], 99458)
        self.assertEqual(plan["logical_directories"], 878)
        self.assertEqual(plan["logical_entries"], 100336)
        for key, limit in size.HARD_LIMITS.items():
            self.assertLessEqual(plan[key], limit)
        self.assertLess(plan["oracle_manifest_bytes_upper"], size.ORACLE_MANIFEST_LIMIT)
        self.assertEqual(plan["completion_phase"], "full-verified")

    def test_radix_entry_and_byte_overflow_include_branch_pages_and_edges(self):
        entries = [(f"f{i:03}".encode(), False) for i in range(128)]
        self.assertEqual(size.radix_bound(entries), (1, 0, 20 + 128 * 47))
        # 129 fNNN entries partition into f0NN and f1NN, not one leaf.
        self.assertEqual(size.radix_bound(entries + [(b"f128", False)]),
                         (3, 2, 20 + 2 + 1 + 1 + 82 + 40 + 129 * 47))
        long_names = [(f"{i:03}".encode() + b"x" * 252, True) for i in range(128)]
        pages, edges, payload = size.radix_bound(long_names)
        self.assertEqual((pages, edges), (13, 140))
        self.assertGreater(payload, 16384)

    def test_actual_namespace_counts_alias_logically_and_checks_real_bytes(self):
        expected = manifest()
        raw, report = size.validate_manifest(expected)
        self.assertEqual(json.loads(raw), expected)
        self.assertEqual(report["logical_entries"], 6)
        self.assertEqual(report["metadata_pages_upper"], 5)
        self.assertEqual(report["oracle_manifest_bytes_upper"], len(raw) + 1)
        with patch.object(size, "ORACLE_MANIFEST_LIMIT", len(raw)):
            with self.assertRaises(ValueError):
                size.validate_manifest(expected)

    def test_unknown_profile_and_each_independent_production_limit_fail_closed(self):
        for profile in ("million", True, None):
            with self.assertRaises(ValueError):
                size.plan(profile)
        for key in size.HARD_LIMITS:
            with patch.dict(size.HARD_LIMITS, {key: 0}):
                with self.assertRaises(ValueError):
                    size.plan("smoke")

    def test_actual_manifest_rejects_alias_omissions_collisions_and_invalid_paths(self):
        for bad_path in ("missing/f", "source/../f", "/f", "source/f", "a" * 256):
            expected = manifest()
            expected["files"][1]["rel_path"] = bad_path
            with self.assertRaises(ValueError):
                size.validate_manifest(expected)

    def test_manifest_facts_preserve_full_fingerprint_and_oracle_totals(self):
        expected = manifest()
        facts = campaign.ManifestFacts(expected)
        self.assertEqual((facts.files, facts.bytes, facts.directories, facts.git_directories), (2, 16, 4, 3))
        changed = manifest()
        changed["files"][0]["content_digest"] = "sha256:" + "2" * 64
        self.assertNotEqual(facts.fingerprint, campaign.ManifestFacts(changed).fingerprint)
        self.assertFalse(hasattr(facts, "manifest"))

    def test_streamed_rows_bound_line_count_and_total_while_hashing_same_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rows.jsonl"
            raw = b'{"a":1}\n{"a":2}\n'
            path.write_bytes(raw)
            seen = []
            self.assertEqual(export.scan_rows(path, seen.append), hashlib.sha256(raw).hexdigest())
            self.assertEqual(seen, [{"a": 1}, {"a": 2}])
            for name, cap in (("EVIDENCE_ROW_LIMIT", 7), ("EVIDENCE_RECORD_LIMIT", 1),
                              ("EVIDENCE_FILE_LIMIT", len(raw) - 1)):
                with patch.object(size, name, cap):
                    with self.assertRaises(AssertionError):
                        export.scan_rows(path, lambda _row: None)
            path.write_bytes(raw[:-1])
            with self.assertRaises(AssertionError):
                export.scan_rows(path, lambda _row: None)

    def test_streamed_copy_and_scan_detect_substitution_and_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, output = root / "source", root / "out"
            source.write_bytes(b'{"a":1}\n')
            output.mkdir()
            bindings = {}
            with patch.object(export.budgets, "require_external_time") as deadline:
                digest = export.copy_safe(source, output, "measurements.jsonl", bindings, "deadline")
            self.assertEqual(digest, hashlib.sha256(source.read_bytes()).hexdigest())
            self.assertEqual((output / "measurements.jsonl").read_bytes(), source.read_bytes())
            self.assertGreater(deadline.call_count, 0)
            def mutate(_row):
                source.write_bytes(b'{"a":2}\n')
            with self.assertRaises(AssertionError):
                export.scan_rows(source, mutate)


if __name__ == "__main__":
    unittest.main()
