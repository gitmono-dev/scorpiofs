"""Cloud-free checks of native ECS launch, ownership, deadlines and cleanup."""

import base64
from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import direct_campaign as cloud


START = datetime(2026, 10, 8, 1, 0, tzinfo=timezone.utc)


def configuration():
    root = Path(tempfile.gettempdir()).resolve()
    return {"region": "cn-hangzhou", "zone": "cn-hangzhou-h", "image_id": "ubuntu_24_04_x64_reviewed_image",
            "instance_type": "ecs.u1-c1m4.2xlarge", "vpc_cidr": "172.26.0.0/16", "vswitch_cidr": "172.26.1.0/24",
            "harness_sha": "e" * 40, "profile": "history-large",
            "scorpiofs_source": str(root / "scorpiofs"), "mega2_source": str(root / "mega2")}


class FakeTools:
    terraform = "fake-terraform"
    aliyun = "fake-aliyun"

    def __init__(self, responses=(), handler=None):
        self.responses = iter(responses)
        self.handler = handler
        self.calls = []

    def call(self, arguments, **options):
        self.calls.append((arguments, options))
        result = self.handler(arguments, options) if self.handler else next(self.responses)
        if isinstance(result, BaseException):
            raise result
        return json.dumps(result).encode() if isinstance(result, (dict, list)) else result


class CampaignTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name) / "new-campaign"
        self.state = cloud.plan(configuration(), self.directory)
        self.campaign = cloud.Campaign(self.directory)

    def start_window(self):
        self.campaign.record(**cloud.schedule(START))

    def resources(self):
        self.campaign.record(resources={"instance_id": "i-owned"})

    def invocation(self, **changes):
        row = {"InstanceId": "i-owned", "InvokeId": "t-owned", "InvocationStatus": "Success",
               "InvokeRecordStatus": "Finished", "ExitCode": 0, "Dropped": 0, "Output": '{"status":"ok"}'}
        row.update(changes)
        return {"Invocation": {"InvocationResults": {"InvocationResult": [row]}}}

    def run_command(self, response):
        self.resources()
        self.campaign.tools = FakeTools([{"InvokeId": "t-owned", "CommandId": "c-owned"}, response])
        return self.campaign.command("#!/bin/bash\ntrue\n", 10, "test")

    def test_three_tiers_share_the_direct_execution_contract(self):
        for tier in ("smoke", "medium", "history-large"):
            with self.subTest(tier=tier):
                cfg = configuration() | {"profile": tier}
                self.assertEqual(cloud.config(cfg), cfg)
                directory = Path(self.temp.name) / tier
                state = cloud.plan(cfg, directory)
                self.assertEqual(state["execution_provider"], "aliyun-direct")
                self.assertEqual(state["config"]["profile"], tier)

    def test_plan_performs_no_cloud_or_process_calls_and_requires_fresh_state(self):
        with patch.object(cloud.Tools, "call") as call, patch.object(cloud.subprocess, "run") as process:
            state = cloud.plan(configuration(), Path(self.temp.name) / "local-only")
        call.assert_not_called()
        process.assert_not_called()
        self.assertEqual(state["status"], "PREPARED_LOCAL_ONLY")
        self.assertNotIn("session_started_utc", state)
        self.assertEqual(list(self.directory.iterdir()), [self.directory / "campaign.json"])
        with self.assertRaisesRegex(ValueError, "FRESH_STATE"):
            cloud.plan(configuration(), self.directory)

    def test_state_inside_repository_is_rejected_before_creation(self):
        target = cloud.HERE / "must-not-exist-direct-state"
        with self.assertRaisesRegex(ValueError, "OUTSIDE_REPOSITORY"):
            cloud.plan(configuration(), target)
        self.assertFalse(target.exists())

    def test_old_ssh_github_and_secret_fields_are_rejected(self):
        for key in ("ssh_operator_cidrs", "ssh_public_key", "ssh_identity_file", "runner_label", "harness_ref",
                    "github_run_id", "github_token", "access_key", "secret_key"):
            with self.subTest(key=key):
                with self.assertRaisesRegex(ValueError, "INVALID_CONFIG_FIELDS"):
                    cloud.config(configuration() | {key: "forbidden"})

    def test_invalid_cloud_shape_network_and_mutable_sources_are_rejected(self):
        for change in ({"vswitch_cidr": "10.1.0.0/24"}, {"harness_sha": "main"}, {"profile": "large"},
                       {"zone": "cn-beijing-a"}, {"instance_type": "ecs.t6-c1m4.large"},
                       {"scorpiofs_source": "relative/source"}):
            with self.subTest(change=change):
                with self.assertRaises(ValueError):
                    cloud.config(configuration() | change)

    def test_schedule_preserves_one_original_four_hour_window(self):
        value = cloud.schedule(START)
        offsets = {"session_started_utc": 0, "preflight_deadline_utc": 15, "work_cleanup_deadline_utc": 220,
                   "collection_deadline_utc": 233, "session_deadline_utc": 235, "hard_release_utc": 240}
        self.assertEqual(set(value), set(offsets))
        for key, minutes in offsets.items():
            self.assertEqual(cloud.utc(value[key]), START + timedelta(minutes=minutes))
        self.campaign.record(**value)
        with patch.object(cloud, "now", return_value=START + timedelta(minutes=14)):
            self.assertEqual(self.campaign.remaining("preflight_deadline_utc", 300), 60)
        with patch.object(cloud, "now", return_value=START + timedelta(minutes=16)):
            with self.assertRaisesRegex(ValueError, "ORIGINAL_DEADLINE_EXPIRED"):
                self.campaign.remaining("preflight_deadline_utc", 300)

    def test_reloading_state_rejects_any_extended_deadline(self):
        window = cloud.schedule(START)
        for key in set(window) - {"session_started_utc"}:
            with self.subTest(key=key):
                self.campaign.record(**window)
                self.campaign.record(**{key: cloud.stamp(cloud.utc(window[key]) + timedelta(seconds=1))})
                with self.assertRaisesRegex(ValueError, "IMMUTABLE_WINDOW_MISMATCH"):
                    cloud.Campaign(self.directory)

    def test_expired_operation_stops_before_tool_call(self):
        self.campaign.operation_deadline = cloud.time.monotonic() - 1
        with patch.object(self.campaign.tools, "call") as call:
            with self.assertRaisesRegex(ValueError, "OPERATION_DEADLINE"):
                self.campaign.inventory("ecs", "DescribeDisks", "Disks", "Disk", [])
        call.assert_not_called()

    def test_tool_failures_do_not_relay_private_output(self):
        result = type("Result", (), {"returncode": 1, "stdout": b"PRIVATE_TOKEN", "stderr": b"PRIVATE_TOKEN"})()
        with patch.object(cloud.subprocess, "run", return_value=result):
            with self.assertRaisesRegex(RuntimeError, "^TOOL_NONZERO_EXIT$"):
                cloud.Tools().call(["not-executed"])

    def test_cloud_command_binds_exact_ids_and_requests_plaintext(self):
        self.assertEqual(self.run_command(self.invocation()), {"status": "ok"})
        launch, result = [call[0] for call in self.campaign.tools.calls]
        self.assertEqual(launch[:3], ["fake-aliyun", "ecs", "RunCommand"])
        self.assertEqual(launch[launch.index("--InstanceId.1") + 1], "i-owned")
        self.assertEqual(launch[launch.index("--ContentEncoding") + 1], "Base64")
        self.assertEqual(base64.b64decode(launch[launch.index("--CommandContent") + 1]), b"#!/bin/bash\ntrue\n")
        self.assertEqual(result[result.index("--InvokeId") + 1], "t-owned")
        self.assertEqual(result[result.index("--InstanceId") + 1], "i-owned")
        self.assertEqual(result[result.index("--ContentEncoding") + 1], "PlainText")
        self.assertEqual(self.campaign.state["last_invoke_id"], "t-owned")

    def test_cloud_command_rejects_other_instances_or_invocations(self):
        for change in ({"InstanceId": "i-other"}, {"InvokeId": "t-other"}):
            with self.subTest(change=change):
                with self.assertRaisesRegex(ValueError, "INVOCATION_IDENTITY_MISMATCH"):
                    self.run_command(self.invocation(**change))

    def test_cloud_command_rejects_terminal_failures_without_waiting(self):
        for status in ("Invalid", "Aborted", "Failed", "Error", "Timeout", "Cancelled", "Terminated"):
            with self.subTest(status=status):
                with self.assertRaisesRegex(ValueError, "CLOUD_COMMAND_FAILED"):
                    self.run_command(self.invocation(InvocationStatus=status))
        for status in ("Failed", "PartialFailed", "Stopped"):
            with self.subTest(record_status=status):
                with self.assertRaisesRegex(ValueError, "CLOUD_COMMAND_FAILED"):
                    self.run_command(self.invocation(InvokeRecordStatus=status))

    def test_cloud_command_rejects_exit_failure_dropped_or_oversize_output(self):
        for change, code in (({"ExitCode": 1}, "FAILED_OR_TRUNCATED"), ({"Dropped": 1}, "FAILED_OR_TRUNCATED"),
                             ({"Output": "x" * 32769}, "CLOUD_OUTPUT_TOO_LARGE")):
            with self.subTest(change=list(change)):
                with self.assertRaisesRegex(ValueError, code):
                    self.run_command(self.invocation(**change))

    def test_exact_invocation_rejects_pagination_and_ambiguous_rows(self):
        response = self.invocation()
        response["Invocation"]["NextToken"] = "unexpected-second-page"
        with self.assertRaisesRegex(ValueError, "EXACT_INVOCATION_PAGINATED"):
            self.run_command(response)
        response = self.invocation()
        response["Invocation"]["InvocationResults"]["InvocationResult"] *= 2
        with self.assertRaisesRegex(ValueError, "AMBIGUOUS_INVOCATION"):
            self.run_command(response)

    def test_command_polls_until_instance_success_and_total_invocation_finished(self):
        self.resources()
        self.campaign.tools = FakeTools([{"InvokeId": "t-owned"}, self.invocation(InvocationStatus="Running"),
                                        self.invocation(InvokeRecordStatus="Running"), self.invocation()])
        with patch.object(cloud.time, "sleep") as sleep:
            self.assertEqual(self.campaign.command("true", 10, "test"), {"status": "ok"})
        self.assertEqual(len(self.campaign.tools.calls), 4)
        self.assertEqual(sleep.call_count, 2)

    def test_invalid_command_bounds_fail_before_dispatch(self):
        self.resources()
        for script, seconds in (("true", 0), ("true", 901), ("x" * 24001, 10)):
            with self.subTest(seconds=seconds, size=len(script)):
                with patch.object(self.campaign.tools, "call") as call:
                    with self.assertRaisesRegex(ValueError, "INVALID_CLOUD_COMMAND_BOUND"):
                        self.campaign.command(script, seconds, "test")
                call.assert_not_called()

    def test_inventory_consumes_all_pages_and_refuses_missing_tail(self):
        pages = [{"TotalCount": 2, "Disks": {"Disk": [{"DiskId": "d-first"}]}},
                 {"TotalCount": 2, "Disks": {"Disk": [{"DiskId": "d-last"}]}}]
        self.campaign.tools = FakeTools(pages)
        rows = self.campaign.inventory("ecs", "DescribeDisks", "Disks", "Disk", ["--DiskName", "owned"])
        self.assertEqual([row["DiskId"] for row in rows], ["d-first", "d-last"])
        second = self.campaign.tools.calls[1][0]
        self.assertEqual(second[second.index("--PageNumber") + 1], "2")
        pages[-1]["Disks"]["Disk"] = []
        self.campaign.tools = FakeTools(pages)
        with self.assertRaisesRegex(ValueError, "INVENTORY_INCOMPLETE"):
            self.campaign.inventory("ecs", "DescribeDisks", "Disks", "Disk", [])

    def test_ram_inventory_consumes_marker_pages_and_matches_exact_names(self):
        pages = [{"Roles": {"Role": [{"RoleName": "other"}]}, "IsTruncated": True, "Marker": "next-page"},
                 {"Roles": {"Role": [{"RoleName": "owned"}, {"RoleName": "owned-suffix"}]}, "IsTruncated": False}]
        self.campaign.tools = FakeTools(pages)
        rows = self.campaign.ram_inventory("ListRoles", "Roles", "Role", "RoleName", "owned")
        self.assertEqual(rows, [{"RoleName": "owned"}])
        second = self.campaign.tools.calls[1][0]
        self.assertEqual(second[second.index("--Marker") + 1], "next-page")
        self.campaign.tools = FakeTools([{"Policies": {"Policy": []}, "IsTruncated": False}])
        self.campaign.ram_inventory("ListPolicies", "Policies", "Policy", "PolicyName", "owned")
        args = self.campaign.tools.calls[0][0]
        self.assertEqual(args[args.index("--PolicyType") + 1], "Custom")

    def test_ram_inventory_rejects_missing_repeated_or_invalid_pagination(self):
        for pages, code in (([{"IsTruncated": True}], "INVALID_RAM_MARKER"),
                            ([{"IsTruncated": True, "Marker": "same"}] * 2, "INVALID_RAM_MARKER"),
                            ([{"IsTruncated": "false"}], "INVALID_RAM_PAGINATION")):
            with self.subTest(pages=pages):
                self.campaign.tools = FakeTools([{"Roles": {"Role": []}, **page} for page in pages])
                with self.assertRaisesRegex(ValueError, code):
                    self.campaign.ram_inventory("ListRoles", "Roles", "Role", "RoleName", "owned")

    def test_oss_inventory_matches_exact_bucket_and_refuses_truncated_results(self):
        bucket = "exact-campaign-bucket"
        for container, count in ((None, 0), ({"Bucket": {"Name": bucket}}, 1),
                                 ({"Bucket": [{"Name": "other"}, {"Name": bucket}]}, 1)):
            self.assertEqual(len(cloud.oss_buckets({"IsTruncated": "false", "Buckets": container}, bucket)), count)
        for flag in ("true", True, 0, None):
            with self.assertRaises(ValueError):
                cloud.oss_buckets({"IsTruncated": flag, "Buckets": None}, bucket)

    def test_cleanup_stops_exact_instance_before_objects_and_terraform(self):
        self.resources()
        owned = {"InstanceId": "i-owned", "InstanceName": "scorpiofs-" + self.state["campaign_id"]}
        events = []
        self.campaign.tools = FakeTools(handler=lambda args, _: events.append(args[2]) or b"{}")
        with patch.object(self.campaign, "owned_instances", side_effect=[[owned], []]), \
                patch.object(self.campaign, "tf", side_effect=lambda *args, **_: events.append(args[0]) or b""), \
                patch.object(self.campaign, "audit", side_effect=lambda: events.append("audit") or {"ECS": [], "RAM": []}):
            self.campaign.cleanup()
        self.assertEqual(events, ["DeleteInstance", "api", "api", "destroy", "audit"])
        deletes = [args for args, _ in self.campaign.tools.calls if "delete-object" in args]
        self.assertEqual({args[args.index("--key") + 1] for args in deletes},
                         {"sources/" + self.state["campaign_id"] + "/sources.tar.gz",
                          "runs/" + self.state["campaign_id"] + "/safe-evidence.tar.gz"})
        self.assertEqual(self.campaign.state["status"], "CLEANED")

    def test_failed_inventory_or_unbound_instance_still_attempts_terraform_and_audit(self):
        for inventory in (RuntimeError("offline"), [{"InstanceId": "i-other", "InstanceName": "unrelated"}]):
            with self.subTest(inventory=inventory):
                with patch.object(self.campaign, "owned_instances", side_effect=inventory if isinstance(inventory, Exception) else [inventory]), \
                        patch.object(self.campaign.tools, "call") as call, \
                        patch.object(self.campaign, "tf", return_value=b"") as tf, \
                        patch.object(self.campaign, "audit", return_value={"ECS": []}) as audit:
                    self.campaign.cleanup()
                call.assert_not_called()
                tf.assert_called_once_with("destroy", "-auto-approve", "-input=false", cap=240)
                audit.assert_called_once()

    def test_failed_destroy_and_residual_audit_cannot_claim_cleaned(self):
        with patch.object(self.campaign, "owned_instances", side_effect=RuntimeError("offline")), \
                patch.object(self.campaign, "tf", side_effect=RuntimeError("destroy-failed")) as tf, \
                patch.object(self.campaign, "audit", return_value={"RAMRoles": [{"RoleName": "owned"}]}) as audit:
            with self.assertRaisesRegex(ValueError, "CLOUD_CLEANUP_REQUIRES_ATTENTION"):
                self.campaign.cleanup()
        self.assertEqual(tf.call_count, 2)
        audit.assert_called_once()
        self.assertEqual(self.campaign.state["status"], "CLEANUP_FAILED")
        self.assertIn("RESIDUAL_AUDIT_FAILED", self.campaign.state["cleanup_errors"])

    def test_partial_apply_failure_cleans_and_audits_with_original_window(self):
        actions = []

        def tf(*args, **_):
            actions.append(args[0])
            if args[0] == "apply":
                raise RuntimeError("PARTIAL_APPLY")
            return b""

        with patch.object(cloud.direct_sources, "create", return_value={"sha256": "f" * 64}), \
                patch.object(self.campaign, "admit_cloud_shape"), \
                patch.object(cloud.shutil, "copytree", side_effect=lambda _, path, **__: path.mkdir()), \
                patch.object(cloud, "now", return_value=START), \
                patch.object(self.campaign, "tf", side_effect=tf), \
                patch.object(self.campaign, "owned_instances", return_value=[]), \
                patch.object(self.campaign.tools, "call", return_value=b"{}"), \
                patch.object(self.campaign, "audit", return_value={"ECS": [], "RAMRoles": [], "RAMPolicies": []}) as audit:
            with self.assertRaisesRegex(RuntimeError, "PARTIAL_APPLY"):
                self.campaign.run()
        self.assertEqual(actions, ["init", "validate", "plan", "apply", "destroy"])
        audit.assert_called_once()
        self.assertEqual(self.campaign.state["status"], "CLEANED")
        self.assertEqual(self.campaign.state["campaign_result"], "FAILED_OR_INTERRUPTED")
        for key, value in cloud.schedule(START).items():
            self.assertEqual(self.campaign.state[key], value)
        with self.assertRaisesRegex(ValueError, "CAMPAIGN_CANNOT_BE_RESTARTED"):
            self.campaign.run()

    def test_runtime_shape_rejects_wrong_image_before_creating_resources(self):
        types = {"InstanceTypes": {"InstanceType": [{"InstanceTypeId": "ecs.u1-c1m4.2xlarge", "CpuCoreCount": 8, "MemorySize": 32}]}}
        images = {"Images": {"Image": [{"ImageId": "ubuntu-reviewed-amd64", "Architecture": "arm64", "Platform": "Ubuntu", "Status": "Available"}]}}
        self.campaign.tools = FakeTools([types, images])
        with self.assertRaisesRegex(ValueError, "UBUNTU_IMAGE_REQUIRED"):
            self.campaign.admit_cloud_shape()
        self.assertEqual(len(self.campaign.tools.calls), 2)


class EvidenceArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.archive = self.root / "safe.tar.gz"
        self.output = self.root / "evidence"

    def write_archive(self, entries):
        with tarfile.open(self.archive, "w:gz") as archive:
            for name, body, kind in entries:
                info = tarfile.TarInfo(name)
                info.type = kind
                info.size = len(body) if kind == tarfile.REGTYPE else 0
                info.linkname = "../outside" if kind in (tarfile.SYMTYPE, tarfile.LNKTYPE) else ""
                archive.addfile(info, io.BytesIO(body) if info.isfile() else None)

    def test_regular_nested_safe_files_are_extracted_exactly_once(self):
        self.write_archive([("safe-export.json", b"{}", tarfile.REGTYPE),
                            ("measurements/fair/measurements.jsonl", b"{}\n", tarfile.REGTYPE)])
        cloud.extract_evidence(self.archive, self.output)
        self.assertEqual((self.output / "measurements/fair/measurements.jsonl").read_bytes(), b"{}\n")
        with self.assertRaisesRegex(ValueError, "FRESH_EVIDENCE_DIRECTORY_REQUIRED"):
            cloud.extract_evidence(self.archive, self.output)

    def test_traversal_absolute_backslash_and_drive_names_are_rejected_before_writes(self):
        for name in ("../outside", "/absolute", "a/../../outside", "a\\outside", "C:/outside", "a//b", "a/./b"):
            with self.subTest(name=name):
                self.write_archive([(name, b"private", tarfile.REGTYPE)])
                with self.assertRaisesRegex(ValueError, "UNSAFE_EVIDENCE_PATH"):
                    cloud.extract_evidence(self.archive, self.output)
                self.assertFalse(self.output.exists())

    def test_links_and_directory_members_are_rejected(self):
        for kind in (tarfile.SYMTYPE, tarfile.LNKTYPE, tarfile.DIRTYPE):
            with self.subTest(kind=kind):
                self.write_archive([("bad", b"", kind)])
                with self.assertRaisesRegex(ValueError, "UNSAFE_EVIDENCE_PATH"):
                    cloud.extract_evidence(self.archive, self.output)
                self.assertFalse(self.output.exists())

    def test_duplicate_empty_and_excess_member_archives_are_rejected(self):
        for entries in ([], [("same", b"x", tarfile.REGTYPE)] * 2,
                        [(str(index), b"", tarfile.REGTYPE) for index in range(258)]):
            with self.subTest(count=len(entries)):
                self.write_archive(entries)
                with self.assertRaisesRegex(ValueError, "INVALID_EVIDENCE_MEMBERS"):
                    cloud.extract_evidence(self.archive, self.output)
                self.assertFalse(self.output.exists())

    def test_expanded_bytes_are_bounded_before_any_extraction(self):
        self.write_archive([("one", b"12345", tarfile.REGTYPE), ("two", b"67890", tarfile.REGTYPE)])
        with patch.object(cloud, "MAX_EXPANDED_EVIDENCE", 9):
            with self.assertRaisesRegex(ValueError, "EXPANDED_EVIDENCE_TOO_LARGE"):
                cloud.extract_evidence(self.archive, self.output)
        self.assertFalse(self.output.exists())


if __name__ == "__main__":
    unittest.main()
