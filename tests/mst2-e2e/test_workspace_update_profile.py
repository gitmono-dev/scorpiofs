"""Read-profile admission, boundary-active work and publication contracts."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import tempfile
import subprocess
import sys
import textwrap
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import commit_update_bench as common
import workspace_update_build as builds
import workspace_update_profile as profile
import workspace_update_worker as worker_module
import workspace_update_daemon as daemon_module
from workspace_update_worker import WorkerSession, _NoRedirectHTTP


WORKSPACE = "11111111-2222-4333-8444-666666666666"
GENERATION = "22222222-2222-4333-8444-666666666666"


def response(sequence=7, started=100, finished=110):
    return {"workspace_id": WORKSPACE, "generation": GENERATION, "profile": {
        "revision": 1, "sequence": sequence, "sample_started_ns": started,
        "sample_finished_ns": finished, "workers_active": 0, "overflow": False,
        "native_kernel_copy_measured": False, "upper_reply_copy_measured": False,
        "directory_stream_delivery_measured": False,
        "metrics": [{"metric": name, "value": 0} for name in profile.METRICS],
        "phases": [{"phase": name, "calls": 0, "active": 0, "wall_ns": 0, "max_wall_ns": 0}
                   for name in profile.PHASES],
        "operations": [{"operation": name, "times": {field: 0 for field in profile.OPERATION_FIELDS}}
                       for name in profile.OPERATIONS],
    }}


def metric(raw, name, value):
    next(entry for entry in raw["profile"]["metrics"] if entry["metric"] == name)["value"] = value


def operation(raw, name, **values):
    next(entry for entry in raw["profile"]["operations"] if entry["operation"] == name)["times"].update(values)


def phase(raw, name, **values):
    next(entry for entry in raw["profile"]["phases"] if entry["phase"] == name).update(values)


def checkpoints():
    before, after = response(), response(11, 1010, 1020)
    metric(before, "worker_admitted", 4)
    before["profile"]["workers_active"] = 2
    operation(before, "read", calls=5, completed=2, errors=1, dropped=1, active=2,
              returned_bytes=300, wall_ns=500, max_wall_ns=300)
    phase(before, "small_cas", calls=6, active=2, wall_ns=700, max_wall_ns=500)
    # Two calls that began before the first checkpoint complete/drop within
    # this interval. There are no new reads. Delta calls need not equal the
    # completed/drop deltas when work was active at a checkpoint boundary.
    metric(after, "worker_admitted", 4)
    metric(after, "worker_error", 1)
    metric(after, "worker_cancelled_pending", 1)
    metric(after, "worker_detached", 1)
    metric(after, "small_cas_read_bytes", 25)
    operation(after, "read", calls=5, completed=3, errors=1, dropped=2, active=0,
              returned_bytes=325, wall_ns=2000, max_wall_ns=1000, empty_read_replies=0)
    phase(after, "small_cas", calls=6, active=1, wall_ns=1700, max_wall_ns=1000)
    return (profile.parse_checkpoint(before, WORKSPACE, GENERATION),
            profile.parse_checkpoint(after, WORKSPACE, GENERATION))


def evidence():
    return profile.measured(*checkpoints(), 20, 30, 800)


class ProfileAdmissionTests(unittest.TestCase):
    def test_errors_drops_and_boundary_active_workers_keep_actual_counters(self):
        value = evidence()
        profile.validate_evidence(value)
        delta = value["delta"]
        self.assertEqual(delta["operations"]["read"]["calls"], 0)
        self.assertEqual(delta["operations"]["read"]["completed"], 1)
        self.assertEqual(delta["operations"]["read"]["dropped"], 1)
        self.assertEqual(delta["operations"]["read"]["active_before"], 2)
        self.assertEqual(delta["operations"]["read"]["active_after"], 0)
        self.assertEqual(delta["workers_active_before"], 2)
        self.assertEqual(delta["workers_active_after"], 0)
        self.assertEqual(delta["metrics"]["worker_error"], 1)
        self.assertEqual(delta["metrics"]["worker_cancelled_pending"], 1)
        self.assertEqual(delta["metrics"]["worker_detached"], 1)
        self.assertEqual(value["checkpoint_overhead_ns"], 50)
        self.assertEqual(value["profiled_current_oracle_ns"], 800)
        # Concurrent/nested phase and operation sums can exceed elapsed time.
        self.assertGreater(delta["operations"]["read"]["wall_ns"], delta["sample_interval_ns"])
        self.assertEqual(delta["phases"]["small_cas"]["active_after"], 1)
        self.assertNotIn("completed", value["before"]["phases"]["small_cas"])

    def test_public_evidence_contains_no_private_binding_or_remote_payload(self):
        value = evidence()
        encoded = json.dumps(value)
        for private in (WORKSPACE, GENERATION, "workspace_id", "generation", "snapshot_id", "mountpoint", "token"):
            self.assertNotIn(private, encoded)
        checkpoint = profile.parse_checkpoint(response(), WORKSPACE, GENERATION)
        self.assertEqual(checkpoint.workspace_id, WORKSPACE)
        self.assertEqual(checkpoint.generation, GENERATION)

    def test_foreign_workspace_or_generation_and_extra_payloads_are_rejected(self):
        for change in ("workspace", "generation", "extra", "noncanonical", "profile_extra"):
            raw = response()
            if change == "workspace":
                raw["workspace_id"] = GENERATION
            elif change == "generation":
                raw["generation"] = WORKSPACE
            elif change == "noncanonical":
                raw["workspace_id"] = WORKSPACE.replace("11111111", "AAAAAAAA")
            elif change == "profile_extra":
                raw["profile"]["token"] = "private bearer token"
            else:
                raw["snapshot_id"] = "sha256:" + "a" * 64
            with self.subTest(change=change), self.assertRaises(profile.ProfileError) as failed:
                profile.parse_checkpoint(raw, WORKSPACE, GENERATION)
            self.assertEqual(str(failed.exception), "workspace read profile is invalid")

    def test_names_sets_duplicate_entries_and_schema_revision_are_closed(self):
        for group, key in (("metrics", "metric"), ("phases", "phase"), ("operations", "operation")):
            for change in ("missing", "duplicate", "unknown", "extra", "not_list"):
                raw = response()
                entries = raw["profile"][group]
                if change == "missing":
                    entries.pop()
                elif change == "duplicate":
                    entries[-1] = deepcopy(entries[0])
                elif change == "unknown":
                    entries[0][key] = "private/path"
                elif change == "extra":
                    entries[0]["sid"] = "private"
                else:
                    raw["profile"][group] = {}
                with self.subTest(group=group, change=change), self.assertRaises(profile.ProfileError):
                    profile.parse_checkpoint(raw, WORKSPACE, GENERATION)
        for version in (0, 2, True, "1"):
            raw = response()
            raw["profile"]["revision"] = version
            with self.subTest(version=version), self.assertRaises(profile.ProfileError):
                profile.parse_checkpoint(raw, WORKSPACE, GENERATION)

    def test_every_counter_rejects_bool_float_negative_and_u64_overflow(self):
        setters = [lambda raw, v: metric(raw, "worker_error", v),
                   lambda raw, v: raw["profile"].update(workers_active=v),
                   lambda raw, v: operation(raw, "read", returned_bytes=v),
                   lambda raw, v: phase(raw, "small_cas", wall_ns=v),
                   lambda raw, v: raw["profile"].update(sequence=v)]
        for setter in setters:
            for number in (True, 1.0, -1, 1 << 64, "1", None):
                raw = response()
                setter(raw, number)
                with self.subTest(number=number, setter=setter), self.assertRaises(profile.ProfileError):
                    profile.parse_checkpoint(raw, WORKSPACE, GENERATION)

    def test_false_measurement_buckets_and_overflow_cannot_claim_measurement(self):
        for field in ("overflow", *profile.BOUNDARY_FLAGS):
            for flag in (True, 0, 1, "false", None):
                raw = response()
                raw["profile"][field] = flag
                with self.subTest(field=field, flag=flag), self.assertRaises(profile.ProfileError):
                    profile.parse_checkpoint(raw, WORKSPACE, GENERATION)

    def test_operation_partition_error_empty_reply_and_phase_active_invariants(self):
        mutations = [lambda raw: operation(raw, "read", calls=2, completed=1),
                     lambda raw: operation(raw, "read", calls=1, completed=1, errors=2),
                     lambda raw: operation(raw, "read", calls=1, completed=1, errors=1, empty_read_replies=1),
                     lambda raw: operation(raw, "open", calls=1, completed=1, empty_read_replies=1),
                     lambda raw: operation(raw, "read", returned_bytes=1),
                     lambda raw: operation(raw, "read", wall_ns=3, max_wall_ns=4),
                     lambda raw: phase(raw, "small_cas", calls=1, active=2),
                     lambda raw: phase(raw, "small_cas", calls=1, wall_ns=3, max_wall_ns=4),
                     lambda raw: raw["profile"].update(workers_active=1)]
        for mutate in mutations:
            raw = response()
            mutate(raw)
            with self.subTest(mutate=mutate), self.assertRaises(profile.ProfileError):
                profile.parse_checkpoint(raw, WORKSPACE, GENERATION)

    def test_sequence_sample_intervals_and_counter_regression_are_rejected(self):
        first = response()
        metric(first, "small_cas_read_bytes", 10)
        before = profile.parse_checkpoint(first, WORKSPACE, GENERATION)
        for change in ("sequence", "overlap", "counter", "phase", "operation", "max_wall", "reversed"):
            raw = response(8, 200, 210)
            metric(raw, "small_cas_read_bytes", 10)
            original = deepcopy(first)
            if change == "sequence":
                raw["profile"]["sequence"] = 7
            elif change == "overlap":
                raw["profile"]["sample_started_ns"] = 109
            elif change == "counter":
                metric(raw, "small_cas_read_bytes", 9)
            elif change == "phase":
                phase(original, "small_cas", calls=1)
            elif change == "operation":
                operation(original, "read", calls=1, completed=1)
            elif change == "max_wall":
                phase(original, "small_cas", calls=1, wall_ns=10, max_wall_ns=10)
                phase(raw, "small_cas", calls=1, wall_ns=10, max_wall_ns=9)
            else:
                raw["profile"]["sample_finished_ns"] = 199
            with self.subTest(change=change), self.assertRaises(profile.ProfileError):
                profile.measured(profile.parse_checkpoint(original, WORKSPACE, GENERATION),
                                 profile.parse_checkpoint(raw, WORKSPACE, GENERATION), 1, 1, 1)
        with self.assertRaises(profile.ProfileError):
            profile.parse_checkpoint(response(sequence=0), WORKSPACE, GENERATION)
        with self.assertRaises(profile.ProfileError):
            profile.measured(before, profile.parse_checkpoint(first, WORKSPACE, GENERATION), 1, 1, 1)

    def test_published_delta_and_overhead_cannot_be_tampered_or_extend_the_schema(self):
        for change in ("delta", "bool", "float", "extra", "duration", "duration_overflow", "private"):
            value = evidence()
            if change == "delta":
                value["delta"]["metrics"]["worker_error"] = 0
            elif change == "bool":
                value["delta"]["metrics"]["worker_error"] = True
            elif change == "float":
                value["delta"]["metrics"]["worker_error"] = 1.0
            elif change == "extra":
                value["delta"]["phases"]["small_cas"]["elapsed_ns"] = 1
            elif change == "duration":
                value["checkpoint_overhead_ns"] = 49
            elif change == "duration_overflow":
                value["profiled_current_oracle_ns"] = 1 << 64
            else:
                value["before"]["path"] = "private"
            with self.subTest(change=change), self.assertRaises(profile.ProfileError):
                profile.validate_evidence(value)

    def test_disabled_and_unsupported_results_are_missing_measurements_without_zero_delta(self):
        for mode in ("disabled", "unsupported"):
            value = profile.not_measured(mode)
            profile.validate_evidence(value)
            self.assertEqual(value, {"status": "NOT_MEASURED", "reason": mode})
            self.assertNotIn("delta", value)
            with self.assertRaises(profile.ProfileError):
                profile.validate_evidence(dict(value, metrics={"read": 0}))

    def test_multiple_threads_parse_independent_immutable_checkpoint_pairs(self):
        raw = response()
        def parse(index):
            before = profile.parse_checkpoint(raw, WORKSPACE, GENERATION)
            next_raw = response(8 + index, 200 + index * 10, 210 + index * 10)
            metric(next_raw, "worker_error", index)
            after = profile.parse_checkpoint(next_raw, WORKSPACE, GENERATION)
            value = profile.measured(before, after, 2, 3, 50)
            value["before"]["metrics"]["worker_error"] = 999
            return after.counters["metrics"]["worker_error"], before.counters["metrics"]["worker_error"]
        with ThreadPoolExecutor(max_workers=8) as pool:
            self.assertEqual(list(pool.map(parse, range(16))), [(index, 0) for index in range(16)])
        self.assertEqual(next(x for x in raw["profile"]["metrics"] if x["metric"] == "worker_error")["value"], 0)

    def test_artifact_validation_rejects_duplicate_keys_missing_diagnostics_and_private_fields(self):
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp) / "measurements.jsonl"
            row = {"record": "round", "measurement_interpretation": profile.DIAGNOSTIC_INTERPRETATION,
                   "performance_comparison_allowed": False, "scorpio": {"read_profile": evidence()}}
            target.write_text(json.dumps(row) + "\n")
            self.assertEqual(profile.validate_artifact(target, diagnostic_required=True), 1)
            invalid = [json.dumps(dict(row, performance_comparison_allowed=True)),
                       json.dumps(dict(row, scorpio={})),
                       json.dumps({"record": "summary"}),
                       '{"record":"round","record":"summary"}',
                       '{"record":"summary","private":NaN}']
            removed = deepcopy(row)
            del removed["measurement_interpretation"]
            invalid.append(json.dumps(removed))
            value = deepcopy(row)
            value["scorpio"]["read_profile"]["token"] = "private"
            invalid.append(json.dumps(value))
            for line in invalid:
                target.write_text(line + "\n")
                with self.subTest(line=line[:60]), self.assertRaises(profile.ProfileError):
                    profile.validate_artifact(target, diagnostic_required=True)
            target.write_text('{"record":"environment"}\n{"record":"round","scorpio":{}}\n')
            self.assertEqual(profile.validate_artifact(target), 0)
            with self.assertRaises(profile.ProfileError):
                profile.validate_artifact(target, diagnostic_required=True)


class CapabilityTests(unittest.TestCase):
    def test_option_is_explicit_and_default_does_not_execute_any_probe(self):
        parser = argparse.ArgumentParser()
        builds.add_arguments(parser)
        self.assertFalse(parser.parse_args([]).workspace_read_profile)
        self.assertTrue(parser.parse_args(["--workspace-read-profile"]).workspace_read_profile)
        with patch.object(builds, "validate") as validate, patch.object(builds.common, "command") as command:
            self.assertEqual(builds.read_profile_mode(object(), False, 1), "disabled")
            validate.assert_not_called()
            command.assert_not_called()

    def test_opt_in_probe_binds_actual_binary_and_old_v3_is_explicitly_unsupported(self):
        lane = SimpleNamespace(driver=Path("pinned-binary"))
        for output, expected in [(b"Options:\n      --workspace-read-profile\n", "enabled"),
                                 (b"Options:\n      --workspace-observation-jsonl <PATH>\n", "unsupported"),
                                 (b"The prose mentions --workspace-read-profile.\n", "unsupported"),
                                 (b"      --workspace-read-profile-extra\n", "unsupported")]:
            with self.subTest(output=output), patch.object(builds, "validate") as validate, \
                    patch.object(builds.common, "command", return_value=output) as command:
                self.assertEqual(builds.read_profile_mode(lane, True, time.monotonic() + 60), expected)
                self.assertEqual(validate.call_count, 2)
                self.assertEqual(command.call_args.args[0], [str(lane.driver), "serve", "--help"])
        with patch.object(builds, "validate"), patch.object(builds.common, "command", return_value=b"x" * 65537):
            with self.assertRaises(ValueError):
                builds.read_profile_mode(lane, True, time.monotonic() + 60)

    def test_actual_daemon_argv_receives_profile_flag_only_on_explicit_opt_in(self):
        # Fake platform ownership/permissions/sink admission, exercising the
        # real daemon config/argv assembly without running a native binary.
        for enabled in (False, True):
            with self.subTest(enabled=enabled), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                binary = root / "scorpio"
                binary.write_bytes(b"pinned fake artifact; not executable")
                process = Mock(pid=123, returncode=None)
                with patch.object(daemon_module, "mounts_under", return_value=[]), \
                        patch.object(daemon_module, "WorkspaceObservationCollector"), \
                        patch.object(daemon_module.os, "getuid", return_value=1000, create=True), \
                        patch.object(daemon_module.os, "fchmod", create=True), \
                        patch.object(daemon_module.os, "chmod"), \
                        patch.object(daemon_module.budget, "PinnedProcess", return_value=process) as spawn, \
                        patch.object(daemon_module.budget, "process_start", return_value="456"), \
                        patch.object(daemon_module.WorkspaceDaemon, "check_owner"):
                    daemon = daemon_module.WorkspaceDaemon(binary, daemon_module.file_digest(binary), root,
                        "http://127.0.0.1:8000", "private-token", WORKSPACE, {}, time.monotonic() + 60,
                        read_profile=enabled)
                    daemon.log.close()
                    self.assertEqual("--workspace-read-profile" in spawn.call_args.args[0], enabled)
                    self.assertEqual(spawn.call_args.args[0], daemon.argv)


class WorkflowPublicationTests(unittest.TestCase):
    @staticmethod
    def workflow():
        return (Path(__file__).resolve().parents[2] / ".github/workflows/mst2-real-update.yml").read_text()

    def test_actual_upload_requires_safe_validation_success_as_well_as_time_admission(self):
        workflow = self.workflow()
        stage = workflow.split("      - name: Stage safe benchmark evidence\n", 1)[1].split("      - name:", 1)[0]
        upload = workflow.split("      - name: Upload safe benchmark evidence\n", 1)[1].split("      - name:", 1)[0]
        self.assertIn("        id: safe_evidence\n", stage)
        self.assertIn("        if: always() && steps.safe_evidence.outcome == 'success' && steps.upload_admission.outcome == 'success'\n", upload)

    def test_actual_safe_copy_validator_checks_nested_files_and_fails_before_upload(self):
        workflow = self.workflow()
        script = workflow.split('python3 -B - "$safe/measurements" <<\'PY\'\n', 1)[1].split("          PY", 1)[0]
        env = dict(os.environ, PYTHONPATH=str(Path(__file__).parent), READ_PROFILE_INPUT="true")
        row = {"record": "round", "measurement_interpretation": profile.DIAGNOSTIC_INTERPRETATION,
               "performance_comparison_allowed": False, "scorpio": {"read_profile": evidence()}}
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "measurements.jsonl").write_text(json.dumps(row) + "\n")
            nested = root / "round-01/client-b"
            nested.mkdir(parents=True)
            leaf = nested / "measurements.jsonl"
            leaf.write_text(json.dumps(row) + "\n")
            self.assertEqual(profile.validate_artifact_tree(root, diagnostic_required=True), 2)
            result = subprocess.run([sys.executable, "-B", "-", str(root)], input=textwrap.dedent(script).encode(),
                                    env=env, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            malformed = deepcopy(row)
            malformed["scorpio"]["read_profile"]["delta"]["metrics"]["worker_error"] = -1
            leaf.write_text(json.dumps(malformed) + "\n")
            with self.assertRaises(profile.ProfileError):
                profile.validate_artifact_tree(root, diagnostic_required=True)
            result = subprocess.run([sys.executable, "-B", "-", str(root)], input=textwrap.dedent(script).encode(),
                                    env=env, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn("private", result.stderr.decode())

    def test_actual_workflow_metadata_labels_opt_in_and_preserves_default_record_shape(self):
        script = self.workflow().split('python3 -B - "$safe/run.json" <<\'PY\'\n', 1)[1].split("          PY", 1)[0]
        env = {"GITHUB_RUN_ID": "public-run", "GITHUB_RUN_ATTEMPT": "1", "SCORPIO_SHA": "a" * 40,
               "MEGA_SHA": "b" * 40, "PROFILE": "medium", "ROUNDS": "3", "COMPARISON": "paired",
               "STARTED_INPUT": "2026-10-07T01:00:00Z", "DEADLINE_INPUT": "2026-10-07T04:55:00Z"}
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp) / "run.json"
            for enabled in ("false", "true"):
                subprocess.run([sys.executable, "-B", "-", str(target)], input=textwrap.dedent(script).encode(),
                               env=dict(env, READ_PROFILE_INPUT=enabled), check=True, capture_output=True)
                value = json.loads(target.read_text())
                if enabled == "false":
                    self.assertNotIn("measurement_interpretation", value)
                    self.assertNotIn("performance_comparison_allowed", value)
                else:
                    self.assertEqual(value["measurement_interpretation"], profile.DIAGNOSTIC_INTERPRETATION)
                    self.assertIs(value["performance_comparison_allowed"], False)


class ActualHttpCheckpointTests(unittest.TestCase):
    def setUp(self):
        self.lock = threading.Lock()
        self.requests = []
        self.sequence = 1
        self.payload_change = None
        self.payload_bytes = None
        outer = self
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                with outer.lock:
                    outer.requests.append(self.path)
                    index = outer.sequence
                    outer.sequence += 1
                raw = response(index, index * 100, index * 100 + 1)
                if outer.payload_change:
                    outer.payload_change(raw)
                body = outer.payload_bytes if outer.payload_bytes is not None else json.dumps(raw).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            def log_message(self, *_args):
                pass
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.worker = WorkerSession.__new__(WorkerSession)
        self.worker.http = _NoRedirectHTTP("http://127.0.0.1:" + str(self.server.server_port))
        self.status = {"workspace_id": WORKSPACE, "generation": GENERATION, "mount_state": "mounted",
                       "metadata_ready": True}

    def test_actual_http_checkpoint_uses_bound_endpoint_and_rejects_wrong_generation(self):
        checkpoint, duration = self.worker._read_profile_checkpoint(self.status, time.monotonic() + 10)
        self.assertEqual(checkpoint.workspace_id, WORKSPACE)
        self.assertEqual(self.requests, ["/v3/workspaces/" + WORKSPACE + "/read-profile"])
        self.assertGreaterEqual(duration, 0)
        self.payload_change = lambda raw: raw.update(generation=WORKSPACE)
        with self.assertRaises(profile.ProfileError) as failed:
            self.worker._read_profile_checkpoint(self.status, time.monotonic() + 10)
        self.assertEqual(common.failure_record(failed.exception)["error_code"], "workspace_read_profile_invalid")
        self.assertEqual(common.failure_record(failed.exception)["worker_stage"], "read_profile")

    def test_actual_http_duplicate_keys_cannot_hide_a_private_or_changed_profile(self):
        encoded = json.dumps(response(1, 100, 110))
        # Append a duplicate binding before the final brace. The HTTP parser
        # must reject it before the profile parser can select one of them.
        self.payload_bytes = (encoded[:-1] + ',"generation":"private/path"}').encode()
        with self.assertRaises(worker_module.WorkerError) as failed:
            self.worker._read_profile_checkpoint(self.status, time.monotonic() + 10)
        failure = common.failure_record(failed.exception)
        self.assertEqual(failure["worker_stage"], "read_profile")
        self.assertNotIn("private/path", json.dumps(failure))

    def test_checkpoints_are_outside_oracle_and_unsupported_default_make_no_http_request(self):
        worker = self.worker
        worker.daemon_uid = 0
        worker._expected_path, worker._expected_digest = Path("oracle"), "a" * 64
        worker._create_workspace = Mock(return_value=self.status)
        worker._hydrate = Mock(return_value=(self.status, 1, 2))
        worker._assert_status_path = Mock(return_value=Path("private-mount"))
        worker._retain_view = Mock()
        worker._views = [object()]
        for mode in ("disabled", "unsupported", "enabled"):
            self.requests.clear()
            worker.read_profile_mode = mode
            expected_requests = 1 if mode == "enabled" else 0
            def oracle(*_args, **_kwargs):
                self.assertEqual(len(self.requests), expected_requests)
                return {"verified": True}
            worker._oracle = Mock(side_effect=oracle)
            with patch.object(worker_module, "_mount_record", return_value={}):
                result = worker._measure_scorpio({}, time.monotonic() + 10)
            self.assertEqual(len(self.requests), 2 if mode == "enabled" else 0)
            if mode == "disabled":
                self.assertNotIn("read_profile", result)
            elif mode == "unsupported":
                self.assertEqual(result["read_profile"], profile.not_measured("unsupported"))
            else:
                profile.validate_evidence(result["read_profile"])
                self.assertEqual(set(result["actual_status"]), set(self.status))
                self.assertGreaterEqual(result["durable_verified_ms"], result["verification_endpoint_ms"])

    def test_slow_checkpoint_http_is_not_subtracted_from_raw_wall_or_included_in_oracle_timer(self):
        worker = self.worker
        worker.read_profile_mode = "enabled"
        worker.daemon_uid = 0
        worker._expected_path, worker._expected_digest = Path("oracle"), "a" * 64
        worker._create_workspace = Mock(return_value=self.status)
        worker._hydrate = Mock(return_value=(self.status, 1, 2))
        worker._assert_status_path = Mock(return_value=Path("private-mount"))
        worker._retain_view = Mock()
        worker._views = [object()]
        self.payload_change = lambda _raw: time.sleep(0.05)
        worker._oracle = Mock(return_value={"verified": True})
        with patch.object(worker_module, "_mount_record", return_value={}):
            result = worker._measure_scorpio({}, time.monotonic() + 10)
        value = result["read_profile"]
        self.assertGreater(value["checkpoint_overhead_ns"], 70_000_000)
        self.assertLess(value["profiled_current_oracle_ns"], value["checkpoint_overhead_ns"] / 2)
        self.assertGreater(result["durable_verified_ms"], 35)
        self.assertGreater(result["side_total_ms"], 70)
        self.assertLess(result["verification_endpoint_ms"], 35)


if __name__ == "__main__":
    unittest.main()
