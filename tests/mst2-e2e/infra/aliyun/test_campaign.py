"""Cloud-free checks of launch fences, failed cleanup and evidence bindings."""

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import campaign as cloud


def configuration():
    return {"region": "cn-hangzhou", "zone": "cn-hangzhou-h", "image_id": "ubuntu-reviewed-amd64",
            "instance_type": "ecs.u1-c1m4.2xlarge", "vpc_cidr": "172.26.0.0/16", "vswitch_cidr": "172.26.1.0/24",
            "ssh_operator_cidrs": ["8.8.4.4/32"], "ssh_public_key": "ssh-ed25519 QUJDREVG",
            "ssh_identity_file": str(Path(tempfile.gettempdir()).resolve() / "existing-identity"),
            "harness_ref": "test/reviewed-infra", "harness_sha": "e" * 40, "profile": "large"}


class CampaignTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name) / "new-campaign"
        self.state = cloud.plan(configuration(), self.directory)
        self.campaign = cloud.Campaign(self.directory)

    def start_window(self):
        self.campaign.record(**cloud.schedule(datetime.now(timezone.utc)))

    def test_plan_is_local_and_rejects_reusing_state(self):
        self.assertEqual(self.state["status"], "PREPARED_LOCAL_ONLY")
        self.assertNotIn("session_started_utc", self.state)
        self.assertEqual(list(self.directory.iterdir()), [self.directory / "campaign.json"])
        with self.assertRaisesRegex(ValueError, "FRESH_STATE"):
            cloud.plan(configuration(), self.directory)

    def test_history_profile_reaches_the_same_bounded_isolated_workflow(self):
        value = configuration() | {"profile": "history-large"}
        state = cloud.plan(value, Path(self.temp.name) / "history-campaign")
        controller = cloud.Campaign(Path(self.temp.name) / "history-campaign")
        controller.record(**cloud.schedule(datetime.now(timezone.utc)))
        inputs = controller.workflow_inputs()
        self.assertEqual(inputs["profile"], "history-large")
        self.assertEqual(inputs["comparison"], "isolated")
        self.assertEqual(inputs["rounds"], "3")
        self.assertEqual(inputs["recover_original_window"], "false")
        self.assertEqual(state["status"], "PREPARED_LOCAL_ONLY")

    def test_state_inside_repository_is_rejected_before_creation(self):
        with self.assertRaisesRegex(ValueError, "OUTSIDE_REPOSITORY"):
            cloud.external_directory(cloud.HERE / "runtime-state")

    def test_invalid_network_and_secret_fields_are_rejected(self):
        for change in ({"vswitch_cidr": "10.1.0.0/24"}, {"ssh_operator_cidrs": ["0.0.0.0/0"]},
                       {"harness_sha": "main"}, {"ssh_public_key": "-----BEGIN PRIVATE KEY-----"},
                       {"access_key": "never-accepted"}, {"zone": "cn-beijing-a"}):
            value = configuration() | change
            with self.subTest(change=list(change)):
                with self.assertRaises(ValueError):
                    cloud.config(value)

    def test_original_deadline_cannot_be_reset(self):
        started = datetime(2026, 10, 8, 1, 0, tzinfo=timezone.utc)
        value = cloud.schedule(started)
        self.assertEqual(cloud.utc(value["hard_release_utc"]) - cloud.utc(value["session_started_utc"]), timedelta(hours=4))
        self.campaign.record(**value)
        with patch.object(cloud, "now", return_value=started + timedelta(minutes=16)):
            with self.assertRaisesRegex(ValueError, "DEADLINE_EXPIRED"):
                self.campaign.remaining("preflight_deadline_utc", 300)
        with patch.object(cloud, "now", return_value=started + timedelta(minutes=14)):
            self.assertEqual(self.campaign.remaining("preflight_deadline_utc", 300), 60)

    def test_api_accepts_empty_204_without_exporting_error_output(self):
        tools = cloud.Tools()
        with patch.object(tools, "call", return_value=b""):
            self.assertIsNone(tools.api("dispatch", data={}))
        result = type("Result", (), {"returncode": 1, "stdout": b"TOKEN", "stderr": b"TOKEN"})()
        with patch.object(cloud.subprocess, "run", return_value=result):
            with self.assertRaisesRegex(RuntimeError, "^TOOL_NONZERO_EXIT$"):
                tools.call(["not-executed"])

    def test_run_selection_checks_unique_label_and_exact_harness(self):
        self.start_window()
        good = {"id": 123, "display_title": self.campaign.title(), "head_sha": "e" * 40}
        other = good | {"id": 456, "display_title": "another campaign"}
        with patch.object(self.campaign.tools, "api", return_value={"workflow_runs": [other, good]}):
            self.assertEqual(self.campaign.discover_run(), good)
        with patch.object(self.campaign.tools, "api", return_value={"workflow_runs": [good, good]}):
            with self.assertRaisesRegex(ValueError, "AMBIGUOUS_DISPATCH"):
                self.campaign.discover_run()

    def test_failed_benchmark_preserves_evidence_and_still_fails_the_campaign(self):
        self.start_window()
        row = {"status": "completed", "conclusion": "failure", "run_attempt": 1}
        with patch.object(self.campaign, "discover_run", return_value=row):
            with patch.object(self.campaign, "collect") as collect:
                with self.assertRaisesRegex(ValueError, "BENCHMARK_WORKFLOW_FAILED"):
                    self.campaign.monitor()
        collect.assert_called_once()
        self.assertEqual(self.campaign.state["github_conclusion"], "failure")

    def test_wrong_harness_fails_before_runner_token_or_apply(self):
        with patch.object(self.campaign.tools, "api", return_value={"sha": "f" * 40}) as api:
            with patch.object(self.campaign.tools, "call") as call:
                with self.assertRaisesRegex(ValueError, "HARNESS_REF_MOVED"):
                    self.campaign.run()
        self.assertEqual(api.call_count, 1)
        call.assert_not_called()

    def test_partial_apply_failure_enters_cleanup_without_restarting_clock(self):
        def api(path, **_):
            if "/commits/" in path:
                return {"sha": "e" * 40}
            if "registration-token" in path:
                return {"token": "one-use-private-token"}
            return {"runners": [], "total_count": 0}

        def tf(*arguments, **_):
            if arguments[0] == "apply":
                raise RuntimeError("PARTIAL_APPLY")
            return b""

        with patch.object(self.campaign.tools, "api", side_effect=api):
            with patch.object(self.campaign, "tf", side_effect=tf):
                with patch.object(self.campaign, "admit_cloud_shape"):
                    with patch.object(self.campaign, "cleanup") as cleanup:
                        with self.assertRaisesRegex(RuntimeError, "PARTIAL_APPLY"):
                            self.campaign.run()
        cleanup.assert_called_once()
        state = json.loads(self.campaign.path.read_bytes())
        self.assertNotIn("one-use-private-token", json.dumps(state))
        self.assertEqual(cloud.utc(state["hard_release_utc"]) - cloud.utc(state["session_started_utc"]), timedelta(hours=4))

    def test_inventory_consumes_all_pages_and_refuses_missing_tail(self):
        pages = [{"TotalCount": 2, "Disks": {"Disk": [{"DiskId": "d-first"}]}},
                 {"TotalCount": 2, "Disks": {"Disk": [{"DiskId": "d-last"}]}}]
        with patch.object(self.campaign.tools, "call", side_effect=[json.dumps(v).encode() for v in pages]) as call:
            result = self.campaign.inventory("ecs", "DescribeDisks", "Disks", "Disk", ["--DiskName", "owned"])
        self.assertEqual(len(result), 2)
        self.assertEqual(call.call_args.args[0][-3], "2")
        pages[-1]["Disks"]["Disk"] = []
        with patch.object(self.campaign.tools, "call", side_effect=[json.dumps(v).encode() for v in pages]):
            with self.assertRaisesRegex(ValueError, "INVENTORY_INCOMPLETE"):
                self.campaign.inventory("ecs", "DescribeDisks", "Disks", "Disk", [])

    def test_failed_run_cancel_does_not_skip_compute_destroy(self):
        self.start_window()
        self.campaign.record(github_run_id="123")
        calls = []

        def command(args, **_):
            calls.append(args)
            return b""

        with patch.object(self.campaign.tools, "api", side_effect=RuntimeError("offline")):
            with patch.object(self.campaign.tools, "call", side_effect=command):
                with patch.object(self.campaign, "audit", return_value={"ECS": []}):
                    with self.assertRaisesRegex(RuntimeError, "CLOUD_CLEANUP"):
                        self.campaign.cleanup()
        self.assertTrue(any("destroy" in args for args in calls))
        self.assertEqual(self.campaign.state["status"], "CLEANUP_FAILED")

    def test_runner_cleanup_requires_both_name_and_label(self):
        label = self.state["runner_label"]
        unrelated = {"id": 2, "name": "other", "labels": [{"name": label}]}
        owned = {"id": 1, "name": label, "labels": [{"name": label}]}
        with patch.object(self.campaign.tools, "api", return_value={"runners": [owned, unrelated], "total_count": 2}):
            with patch.object(self.campaign.tools, "call", return_value=b"") as call:
                self.campaign.remove_runner()
        self.assertEqual(call.call_count, 1)
        self.assertTrue(call.call_args.args[0][-1].endswith("/1"))

    def test_native_workflow_inputs_preserve_frozen_comparison(self):
        self.start_window()
        inputs = self.campaign.workflow_inputs()
        self.assertEqual(inputs["mega_sha"], cloud.SERVER)
        self.assertEqual(inputs["candidate_sha"], cloud.CANDIDATE)
        self.assertEqual(inputs["baseline_sha"], cloud.BASELINE)
        self.assertEqual((inputs["comparison"], inputs["rounds"], inputs["recover_original_window"]), ("isolated", "3", "false"))
        self.assertEqual(inputs["session_started_utc"], self.campaign.state["session_started_utc"])

    def test_oss_inventory_accepts_real_cli_xml_json_shapes_and_rejects_truncation(self):
        bucket = "exact-campaign-bucket"
        for container, count in ((None, 0), ({"Bucket": {"Name": bucket}}, 1),
                                 ({"Bucket": [{"Name": "other"}, {"Name": bucket}]}, 1)):
            self.assertEqual(len(cloud.oss_buckets({"IsTruncated": "false", "Buckets": container}, bucket)), count)
        for flag in ("true", True, 0, None):
            with self.assertRaises(ValueError):
                cloud.oss_buckets({"IsTruncated": flag, "Buckets": None}, bucket)

    def test_expired_operation_stops_pagination_without_granting_more_time(self):
        self.campaign.operation_deadline = cloud.time.monotonic() - 1
        with patch.object(self.campaign.tools, "call") as call:
            with self.assertRaisesRegex(ValueError, "OPERATION_DEADLINE"):
                self.campaign.inventory("ecs", "DescribeDisks", "Disks", "Disk", [])
        call.assert_not_called()

    def test_runtime_shape_rejects_wrong_image_before_creating_resources(self):
        types = {"InstanceTypes": {"InstanceType": [{"InstanceTypeId": "ecs.u1-c1m4.2xlarge", "CpuCoreCount": 8, "MemorySize": 32}]}}
        images = {"Images": {"Image": [{"ImageId": "ubuntu-reviewed-amd64", "Architecture": "arm64", "Platform": "Ubuntu", "Status": "Available"}]}}
        with patch.object(self.campaign.tools, "call", side_effect=[json.dumps(v).encode() for v in (types, images)]):
            with self.assertRaisesRegex(ValueError, "UBUNTU_IMAGE_REQUIRED"):
                self.campaign.admit_cloud_shape()


class EvidenceTests(unittest.TestCase):
    setUp = CampaignTests.setUp
    start_window = CampaignTests.start_window
    def evidence(self):
        self.start_window()
        self.campaign.record(github_run_id="123", github_attempt="1", github_conclusion="failure")
        root = self.directory / "safe-evidence"
        root.mkdir()
        metadata = {"revision": 1, "run_id": "123", "attempt": "1", "harness_sha": "e" * 40,
                    "mega_sha": cloud.SERVER, "baseline_sha": cloud.BASELINE, "candidate_sha": cloud.CANDIDATE,
                    "profile": "large", "rounds": 3, "comparison": "isolated", "bootstrap_commit_time": 1700000000,
                    "session_started_utc": self.campaign.state["session_started_utc"],
                    "session_deadline_utc": self.campaign.state["session_deadline_utc"],
                    "cleanup_deadline_monotonic": 1000., "owned_root": "/runner/_temp/mst2-real-123-1"}
        body = json.dumps(metadata).encode()
        (root / "run.json").write_bytes(body)
        manifest = {"revision": 1, "files_sha256": {"run.json": hashlib.sha256(body).hexdigest()},
                    "complete_campaign": False, "private_logs_exported": False}
        cloud.save(root / "safe-export.json", manifest)
        return root

    def test_partial_failure_artifact_is_bound_but_never_claims_success(self):
        root = self.evidence()
        cloud.validate_evidence(root, self.campaign.state)
        self.campaign.record(github_conclusion="success")
        with self.assertRaisesRegex(ValueError, "SUCCESS_REQUIRES_COMPLETE"):
            cloud.validate_evidence(root, self.campaign.state)

    def test_tampered_bytes_or_extra_private_files_are_rejected(self):
        root = self.evidence()
        original = (root / "run.json").read_bytes()
        (root / "run.json").write_bytes(original + b" ")
        with self.assertRaisesRegex(ValueError, "HASH_MISMATCH"):
            cloud.validate_evidence(root, self.campaign.state)
        (root / "run.json").write_bytes(original)
        (root / "service.toml").write_text("private")
        with self.assertRaisesRegex(ValueError, "UNEXPECTED_EXPORT"):
            cloud.validate_evidence(root, self.campaign.state)


if __name__ == "__main__":
    unittest.main()
