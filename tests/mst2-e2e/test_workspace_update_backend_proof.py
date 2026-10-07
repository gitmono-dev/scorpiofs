"""Adversarial proof tests; no campaign dispatch, server build or native Cargo."""

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch
import uuid

import commit_update_projection as projection
import workspace_update_backend as backend_module
import workspace_update_backend_proof as proof
import workspace_update_build as builds


def sha(data):
    return hashlib.sha256(data).hexdigest()


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args]).decode().strip()


class BackendProofTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        cls.root = Path(cls.temp.name)
        cls.harness = cls.root / "harness"
        cls.harness.mkdir()
        (cls.harness / "Cargo.lock").write_text("fixed lock\n", encoding="utf-8")
        for name in proof.SCRIPT_PATHS:
            path = cls.harness / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(name + "\n", encoding="utf-8")
        cls.commit(cls.harness)
        cls.builds = {}
        cls.sources = {}
        for client in ("a", "b"):
            source = cls.root / ("client-" + client)
            source.mkdir()
            (source / "Cargo.lock").write_text("fixed lock\n", encoding="utf-8")
            (source / "Cargo.toml").write_text("test fixture " + client + "\n", encoding="utf-8")
            (source / ".gitignore").write_text("target/\n", encoding="utf-8")
            cls.commit(source)
            binary = source / "target/release/scorpio"
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"immutable binary " + client.encode())
            record = {
                "revision": 1, "label": client, "source": str(source.resolve()),
                "source_sha": git(source, "rev-parse", "HEAD"),
                "cargo_lock_sha256": sha((source / "Cargo.lock").read_bytes()),
                "binary": str(binary.resolve()), "binary_sha256": sha(binary.read_bytes()),
                "build_argv": builds.build_argv(source.resolve()), "build_env": builds.BUILD_ENV,
                "rustc_version": "rustc 1.90.0 fixture", "cargo_version": "cargo 1.90.0 fixture",
            }
            receipt = cls.root / ("build-" + client + ".json")
            receipt.write_text(json.dumps(record), encoding="utf-8")
            cls.builds[client] = receipt
            cls.sources[client] = {
                "server_source_sha": "1" * 40, "server_source_tree": "2" * 40,
                "server_binary_sha256": "3" * 64, "server_cargo_lock_sha256": "4" * 64,
                "client_source_sha": record["source_sha"],
                "client_cargo_lock_sha256": record["cargo_lock_sha256"],
                "client_binary_sha256": record["binary_sha256"],
                "rustc_version": record["rustc_version"], "cargo_version": record["cargo_version"],
                "harness_source_sha": git(cls.harness, "rev-parse", "HEAD"),
                "harness_source_tree": git(cls.harness, "rev-parse", "HEAD^{tree}"),
                "harness_files_sha256": {name: sha((cls.harness / name).read_bytes()) for name in proof.SCRIPT_PATHS},
            }

    @staticmethod
    def commit(root):
        git(root, "init", "--quiet")
        # The measured loader deliberately strips global Git configuration.
        git(root, "config", "core.autocrlf", "false")
        git(root, "add", ".")
        git(root, "-c", "user.name=Proof Fixture", "-c", "user.email=fixture@example.invalid",
            "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "fixture")

    def runtime(self, client, *, scope_tree="a" * 40, path_commit="b" * 40, **changes):
        raw = b"40000 project\0" + bytes.fromhex(scope_tree) + b"100644 owner\0" + hashlib.sha1(client.encode()).digest()
        root_tree = hashlib.sha1(b"tree " + str(len(raw)).encode() + b"\0" + raw).hexdigest()
        root_commit = hashlib.sha1(("root-" + client).encode()).hexdigest()
        instance = str(uuid.uuid5(uuid.NAMESPACE_DNS, "instance-" + client))
        identity = {"project_commit": path_commit, "project_tree": scope_tree,
                    "global_commit": root_commit, "global_tree": root_tree,
                    "namespace_view_id": projection.digest(b"mega.mst2.namespaceview\0" + root_commit.encode())}
        rows = [{"path": "/", "commit": root_commit, "tree": root_tree, "database": "db_" + client,
                 "commit_tree": root_tree, "raw_tree": raw.hex()},
                {"path": "/project", "commit": path_commit, "tree": scope_tree,
                 "database": "db_" + client, "commit_tree": scope_tree, "raw_tree": None}]
        number = 11 if client == "a" else 27
        native = {
            "instance_id": instance, "sequence": number, "writer_epoch": 1,
            "root_commit": root_commit, "root_tree": root_tree, "state": "READY",
            "certificate_receipt_id": number, "certificate_commit": root_commit,
            "certificate_tree": root_tree, "certificate_sequence": number,
            "certificate_instance": instance, "path_commit": path_commit, "path_tree": scope_tree,
            "origin_path": "/project", "origin_ref": "refs/heads/main", "origin_sequence": number,
            "certificate_id": number, "certificate_namespace": "/", "certificate_epoch": 1,
            "old_root_commit": "c" * 40, "old_path_commit": "d" * 40,
            "receipt_id": number, "receipt_namespace": "/project", "old_oid": "c" * 40,
            "new_oid": path_commit, "receipt_epoch": 1, "writer_kind": "trunk_push",
            "request_digest": "sha256:" + "e" * 64, "request_digest_version": 1,
            "native_certificate_version": 1, "outbox_id": number,
            "outbox_namespace": "/project", "outbox_sequence": number,
        }
        kwargs = {
            "revision": 1, "phase": "fair", "round": 1, "client": client,
            "project": "project_" + client, "database": "db_" + client,
            "instance_id": instance, "base_url": "http://127.0.0.1:" + ("8001" if client == "a" else "8002"),
            "git_url": "http://127.0.0.1:" + ("8001" if client == "a" else "8002") + "/git/project.git",
            "server_source_sha": "1" * 40, "server_source_tree": "2" * 40,
            "server_binary_sha256": "3" * 64, "server_cargo_lock_sha256": "4" * 64,
            "service_pid": 1001 if client == "a" else 1002, "service_starttime": "999",
            "config_sha256": "5" * 64, "compose_sha256": "6" * 64,
            "cache_prefix": "prefix_" + client, "base_dir": "/owned/" + client + "/base",
            "cache_dir": "/owned/" + client + "/cache", "pack_cache_dir": "/owned/" + client + "/pack",
            "dependency_container_ids": tuple(sha((client + service).encode())
                                              for service in ("postgres", "redis", "rustfs", "rustfs-init")),
            "projection_sink_instance": str(uuid.uuid5(uuid.NAMESPACE_DNS, "sink-" + client)),
            "projection_sink_root": "/owned/" + client + "/cache/logs/mst2-native-projection/" + str(uuid.uuid5(uuid.NAMESPACE_DNS, "sink-" + client)),
            "projection_sink_device": 1, "projection_sink_inode": 8001 if client == "a" else 8002,
            "identity_rows_json": proof.canonical(rows), "identity_json": proof.canonical(identity),
            "native_json": proof.canonical(native),
        }
        kwargs.update(changes)
        return backend_module.RuntimeBinding(**kwargs)

    def capture(self, client, runtime=None, sources=None):
        runtime = runtime or self.runtime(client)
        backend = object.__new__(backend_module.OwnedBackend)
        with patch.object(backend_module.OwnedBackend, "verify_runtime", return_value=runtime) as verify:
            result = proof.capture_lane_runtime(backend, self.builds[client], sources or self.sources[client],
                                               time.monotonic() + 30, harness_root=self.harness)
            verify.assert_called_once()
            return result

    def lane(self, captured):
        capture = captured.evidence
        runtime = capture["runtime"]
        client = runtime["client"]
        run_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, "run-" + client))
        workspace_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, "workspace-" + client))
        logical = f"ws:{run_id}:{workspace_id}"
        payload = dict(projection.FIXED)
        payload.update({key: 0 for key in projection.WORK_FIELDS})
        payload.update({"request_id": logical + ":a1", "instance_id": runtime["instance_id"],
                        "root_commit_oid": "sha1:" + runtime["identity"]["global_commit"],
                        "root_tree_oid": "sha1:" + runtime["identity"]["global_tree"],
                        "native_certificate_receipt_id": runtime["native"]["certificate_receipt_id"],
                        "native_writer_epoch": 1, "native_publication_sequence": runtime["native"]["sequence"],
                        "namespace_view_id": runtime["identity"]["namespace_view_id"], "scope": "/project",
                        "metadata_root": "sha256:" + "f" * 64, "projection_elapsed_micros": 123})
        payload["snapshot_id"] = projection.digest(b"mega.mst2.descriptor\0" + projection.descriptor_bytes(payload))
        receipt = {"logical_request_id": logical, "attempt_ids": [logical + ":a1"],
                   "final_attempt_id": logical + ":a1", "retry_count": 0}
        binding = {"record": "workspace_resolve_binding", "revision": 1, "run_id": run_id,
                   "workspace_id": workspace_id, "generation": str(uuid.uuid5(uuid.NAMESPACE_DNS, "generation-" + client)),
                   "logical_request_id": logical, "resolve_trace_receipt": receipt,
                   "descriptor_bytes_hex": projection.descriptor_bytes(payload).hex(),
                   "instance_id": runtime["instance_id"], "namespace_view_id": payload["namespace_view_id"],
                   "snapshot_id": payload["snapshot_id"], "scope": "/project",
                   "publication_sequence": runtime["native"]["sequence"],
                   "store": "/workspace/" + client + "/store", "content_store": "/workspace/" + client + "/cas"}
        record = {"revision": 1, "phase": runtime["phase"], "round": runtime["round"], "client": client,
                  "version": 1, "fixed_commit": runtime["identity"]["project_commit"],
                  "path_tree": runtime["identity"]["project_tree"], "oracle_manifest_sha256": "0" * 64,
                  "sources": capture["sources"], "identity_rows": runtime["identity_rows"],
                  "identity": runtime["identity"], "native_publication": runtime["native"],
                  "workspace_binding": binding, "workspace_binding_sha256": proof.digest(binding),
                  "timings_ms": {"git_push_ms": 1.0, "publication_visible_ms": 2.0,
                                 "metadata_ready_ms": 3.0, "durable_complete_ms": 4.0,
                                 "durable_verified_ms": 5.0, "git_verified_ms": 6.0},
                  "publication": {"started_utc": "2026-10-07T00:00:00Z",
                                  "visible_utc": "2026-10-07T00:00:01Z",
                                  "started_monotonic": 10.0 if client == "a" else 25.0,
                                  "visible_monotonic": 11.0 if client == "a" else 26.0}}
        record["native_proof_sha256"] = proof.digest({key: record[key] for key in
                                                     ("identity_rows", "identity", "native_publication")})
        self.set_sink(record, [payload], runtime)
        return record

    def set_sink(self, record, payloads, runtime):
        # Rust's hashed payload is its original byte sequence, not a reserialized
        # canonical dictionary. Keep a deliberately noncanonical key order.
        sink = runtime["projection_sink_instance"]
        lines = []
        for number, payload in enumerate(payloads, 1):
            body = json.dumps(payload, separators=(",", ":")).encode()
            prefix = (f'{{"writer_revision":1,"sink_instance":"{sink}",'
                      f'"record_sequence":{number},"payload":').encode()
            lines.append(prefix + body + (',"payload_sha256":"' + projection.digest(body) + '"}\n').encode())
        raw = b"".join(lines)
        status = {"writer_revision": 1, "sink_instance": sink, "accepted_records": len(lines),
                  "written_sequence": len(lines), "written_records": len(lines),
                  "written_bytes": len(raw), "rolling_sha256": projection.digest(raw),
                  "first_error_code": 0, "closed": True}
        record["projection_sink"] = {"records_jsonl": raw.decode(), "status_json": json.dumps(status),
                                     "registered_receipts": [deepcopy(record["workspace_binding"]["resolve_trace_receipt"])],
                                     "expected_final_count": 1}

    def change_payload(self, record, captured, field, value, *, rederive=False):
        payload = projection.parse(record["projection_sink"]["records_jsonl"])["payload"]
        payload[field] = value
        if rederive:
            payload["snapshot_id"] = projection.digest(b"mega.mst2.descriptor\0" + projection.descriptor_bytes(payload))
            record["workspace_binding"].update(snapshot_id=payload["snapshot_id"],
                                                descriptor_bytes_hex=projection.descriptor_bytes(payload).hex())
            record["workspace_binding_sha256"] = proof.digest(record["workspace_binding"])
        self.set_sink(record, [payload], captured.evidence["runtime"])

    def test_independently_validated_deployment_local_divergence_is_accepted(self):
        a, b = self.capture("a"), self.capture("b")
        left, right = self.lane(a), self.lane(b)
        for key in ("instance_id", "namespace_view_id", "snapshot_id", "workspace_id", "generation",
                    "publication_sequence"):
            self.assertNotEqual(left["workspace_binding"][key], right["workspace_binding"][key])
        self.assertNotEqual(left["identity"], right["identity"])
        right["publication"].update(started_utc="2026-10-07T00:03:00Z", visible_utc="2026-10-07T00:03:01Z")
        result = proof.compare_pair(proof.validate_lane_measurement(left, a), proof.validate_lane_measurement(right, b))
        self.assertEqual(result["semantic"]["metadata_root"], "sha256:" + "f" * 64)
        self.assertEqual(len(set(result["backend_receipt_sha256"])), 2)

    def test_semantic_mismatch_rejected_after_individual_proof_passes(self):
        a, b = self.capture("a"), self.capture("b")
        left = proof.validate_lane_measurement(self.lane(a), a)
        for field in ("fixed_commit", "path_tree", "oracle_manifest_sha256", "metadata_root"):
            with self.subTest(field=field):
                if field == "fixed_commit":
                    other = self.capture("b", runtime=self.runtime("b", path_commit="9" * 40))
                elif field == "path_tree":
                    other = self.capture("b", runtime=self.runtime("b", scope_tree="9" * 40))
                else:
                    other = b
                right = self.lane(other)
                if field == "metadata_root":
                    self.change_payload(right, b, field, "sha256:" + "9" * 64, rederive=True)
                elif field == "oracle_manifest_sha256":
                    right[field] = "9" * 64
                validated = proof.validate_lane_measurement(right, other)
                with self.assertRaises(proof.ProofRejected):
                    proof.compare_pair(left, validated)

    def test_fixed_scope_and_policy_are_checked_before_pair_equality(self):
        capture = self.capture("a")
        for field in proof.POLICY_FIELDS | {"scope"}:
            with self.subTest(field=field):
                lane = self.lane(capture)
                current = projection.parse(lane["projection_sink"]["records_jsonl"])["payload"][field]
                value = current + 1 if type(current) is int else "/other"
                self.change_payload(lane, capture, field, value)
                with self.assertRaises(proof.ProofRejected):
                    proof.validate_lane_measurement(lane, capture)

    def test_source_and_build_tampering_rejected_at_capture(self):
        for field in ("client_source_sha", "client_binary_sha256", "client_cargo_lock_sha256",
                      "server_source_sha", "server_source_tree", "harness_source_sha", "harness_source_tree"):
            with self.subTest(field=field):
                changed = deepcopy(self.sources["a"])
                changed[field] = "0" * len(changed[field])
                with self.assertRaises(proof.ProofRejected):
                    self.capture("a", sources=changed)
        changed = deepcopy(self.sources["a"])
        changed["harness_files_sha256"][next(iter(proof.SCRIPT_PATHS))] = "0" * 64
        with self.assertRaises(proof.ProofRejected):
            self.capture("a", sources=changed)

    def test_boolean_or_forged_receipts_never_substitute_actual_probes(self):
        lane = {"validated": True}
        for fake in (True, lane, {"runtime": self.runtime("a"), "validated": True}):
            with self.assertRaises(proof.ProofRejected):
                proof.validate_lane_measurement(lane, fake)
            with self.assertRaises(proof.ProofRejected):
                proof.compare_pair(fake, fake)
        with self.assertRaises(TypeError):
            proof.CapturedLane()
        with self.assertRaises(TypeError):
            proof.LaneProof()
        with self.assertRaises(proof.ProofRejected):
            proof.capture_lane_runtime(lane, self.builds["a"], self.sources["a"],
                                       time.monotonic() + 30, harness_root=self.harness)

    def test_immutable_capture_and_proof_return_defensive_copies(self):
        capture = self.capture("a")
        changed = capture.evidence
        changed["runtime"]["identity"]["project_commit"] = "0" * 40
        self.assertEqual(capture.evidence["runtime"]["identity"]["project_commit"], "b" * 40)
        validated = proof.validate_lane_measurement(self.lane(capture), capture)
        changed = validated.evidence
        changed["semantic"]["metadata_root"] = "sha256:" + "0" * 64
        self.assertEqual(validated.evidence["semantic"]["metadata_root"], "sha256:" + "f" * 64)
        with self.assertRaises(TypeError):
            capture._json = "{}"

    def test_native_bool_negative_and_float_counters_rejected(self):
        for field in proof.NATIVE_COUNTERS:
            for value in (True, -1, 1.0):
                with self.subTest(field=field, value=value):
                    native = self.runtime("a").native
                    native[field] = value
                    runtime = self.runtime("a", native_json=proof.canonical(native))
                    with self.assertRaises(proof.ProofRejected):
                        self.capture("a", runtime=runtime)

    def test_diagnostic_is_only_round_one_client_b(self):
        capture = self.capture("b", runtime=self.runtime("b", phase="diagnostic", round=1))
        proof.validate_lane_measurement(self.lane(capture), capture)
        for client, number in (("a", 1), ("a", 2), ("b", 2), ("b", 3), ("b", True), ("b", 0)):
            with self.subTest(client=client, number=number):
                with self.assertRaises(proof.ProofRejected):
                    self.capture(client, runtime=self.runtime(client, phase="diagnostic", round=number))

    def test_dependencies_require_exactly_four_distinct_container_ids(self):
        original = self.runtime("a").dependency_container_ids
        for ids in ((), original[:1], original[:3], original + (sha(b"extra"),),
                    (original[0], original[0], original[2], original[3])):
            with self.subTest(ids=ids):
                with self.assertRaises(proof.ProofRejected):
                    self.capture("a", runtime=self.runtime("a", dependency_container_ids=ids))

    def test_concurrent_pair_rejects_same_pid_with_different_starttime(self):
        a = self.capture("a")
        b = self.capture("b", runtime=self.runtime("b", service_pid=self.runtime("a").service_pid,
                                                  service_starttime="1000"))
        with self.assertRaises(proof.ProofRejected):
            proof.compare_pair(proof.validate_lane_measurement(self.lane(a), a),
                               proof.validate_lane_measurement(self.lane(b), b))

    def test_pair_rejects_same_sink_directory_device_and_inode(self):
        a = self.capture("a")
        actual = self.runtime("a")
        b = self.capture("b", runtime=self.runtime("b", projection_sink_device=actual.projection_sink_device,
                                                  projection_sink_inode=actual.projection_sink_inode))
        with self.assertRaises(proof.ProofRejected):
            proof.compare_pair(proof.validate_lane_measurement(self.lane(a), a),
                               proof.validate_lane_measurement(self.lane(b), b))

    def test_typed_counters_reject_bool_negative_float_even_with_fresh_hashes(self):
        capture = self.capture("a")
        for field in projection.WORK_FIELDS | {"projection_elapsed_micros", "native_certificate_receipt_id",
                                               "native_writer_epoch", "native_publication_sequence"}:
            for value in (True, -1, 1.0):
                with self.subTest(field=field, value=value):
                    lane = self.lane(capture)
                    self.change_payload(lane, capture, field, value)
                    with self.assertRaises(proof.ProofRejected):
                        proof.validate_lane_measurement(lane, capture)

    def test_closed_sink_hash_status_and_shape_tampering_rejected(self):
        capture = self.capture("a")
        for field, value in (("closed", False), ("closed", 1), ("first_error_code", True),
                             ("accepted_records", True), ("written_records", -1), ("written_sequence", 1.0),
                             ("written_bytes", 0), ("rolling_sha256", "sha256:" + "0" * 64),
                             ("sink_instance", str(uuid.uuid4()))):
            with self.subTest(field=field):
                lane = self.lane(capture)
                status = projection.parse(lane["projection_sink"]["status_json"])
                status[field] = value
                lane["projection_sink"]["status_json"] = json.dumps(status)
                with self.assertRaises(proof.ProofRejected):
                    proof.validate_lane_measurement(lane, capture)
        for mutation in ("record-byte", "payload-hash", "duplicate-status-key", "extra-lane-field", "missing-source-hash"):
            with self.subTest(mutation=mutation):
                lane = self.lane(capture)
                if mutation == "record-byte":
                    lane["projection_sink"]["records_jsonl"] = lane["projection_sink"]["records_jsonl"].replace('"projection_elapsed_micros":123', '"projection_elapsed_micros":124')
                elif mutation == "payload-hash":
                    raw = lane["projection_sink"]["records_jsonl"]
                    line = projection.parse(raw)
                    raw = raw.replace(line["payload_sha256"], "sha256:" + "0" * 64)
                    status = projection.parse(lane["projection_sink"]["status_json"])
                    status["rolling_sha256"] = projection.digest(raw.encode())
                    lane["projection_sink"].update(records_jsonl=raw, status_json=json.dumps(status))
                elif mutation == "duplicate-status-key":
                    lane["projection_sink"]["status_json"] = lane["projection_sink"]["status_json"].replace('{', '{"closed":false,', 1)
                elif mutation == "extra-lane-field":
                    lane["validated"] = True
                else:
                    del lane["sources"]["client_cargo_lock_sha256"]
                with self.assertRaises(proof.ProofRejected):
                    proof.validate_lane_measurement(lane, capture)

    def test_descriptor_sid_native_source_and_identity_proof_tampering_rejected(self):
        capture = self.capture("a")
        for failure in ("descriptor", "sid", "native-hash", "binding-hash", "native-binding", "source", "raw-tree"):
            with self.subTest(failure=failure):
                lane = self.lane(capture)
                if failure == "descriptor":
                    lane["workspace_binding"]["descriptor_bytes_hex"] = "00" + lane["workspace_binding"]["descriptor_bytes_hex"][2:]
                    lane["workspace_binding_sha256"] = proof.digest(lane["workspace_binding"])
                elif failure == "sid":
                    lane["workspace_binding"]["snapshot_id"] = "sha256:" + "0" * 64
                    lane["workspace_binding_sha256"] = proof.digest(lane["workspace_binding"])
                elif failure == "native-hash":
                    lane["native_proof_sha256"] = "0" * 64
                elif failure == "binding-hash":
                    lane["workspace_binding_sha256"] = "0" * 64
                elif failure == "native-binding":
                    self.change_payload(lane, capture, "native_certificate_receipt_id", 90)
                elif failure == "source":
                    lane["sources"]["client_binary_sha256"] = "0" * 64
                else:
                    lane["identity_rows"][0]["raw_tree"] = "00"
                with self.assertRaises(proof.ProofRejected):
                    proof.validate_lane_measurement(lane, capture)

    def test_numeric_timing_and_lane_types_rejected(self):
        capture = self.capture("a")
        for field in proof.TIME_FIELDS:
            for value in (True, -1, float("nan"), float("inf"), "1"):
                with self.subTest(field=field, value=value):
                    lane = self.lane(capture)
                    lane["timings_ms"][field] = value
                    with self.assertRaises(proof.ProofRejected):
                        proof.validate_lane_measurement(lane, capture)
        for key, value in (("revision", True), ("round", True), ("version", True), ("version", -1)):
            lane = self.lane(capture)
            lane[key] = value
            with self.assertRaises(proof.ProofRejected):
                proof.validate_lane_measurement(lane, capture)

    def test_retry_records_require_full_hashes_known_unique_attempts_and_all_finals(self):
        capture = self.capture("a")
        lane = self.lane(capture)
        payload = projection.parse(lane["projection_sink"]["records_jsonl"])["payload"]
        logical = lane["workspace_binding"]["logical_request_id"]
        lane["workspace_binding"]["resolve_trace_receipt"].update(
            attempt_ids=[logical + ":a1", logical + ":a2"], final_attempt_id=logical + ":a2", retry_count=1)
        lane["workspace_binding_sha256"] = proof.digest(lane["workspace_binding"])
        final = dict(payload, request_id=logical + ":a2")
        self.set_sink(lane, [payload, final], capture.evidence["runtime"])
        proof.validate_lane_measurement(lane, capture)
        for failure in ("missing-final", "duplicate", "unknown", "bad-prior-counter", "receipt-bool", "expected-bool"):
            with self.subTest(failure=failure):
                changed = deepcopy(lane)
                payloads = [deepcopy(payload), deepcopy(final)]
                if failure == "missing-final":
                    payloads.pop()
                elif failure == "duplicate":
                    payloads.append(deepcopy(payload))
                elif failure == "unknown":
                    payloads.append(dict(payload, request_id="unknown:a1"))
                elif failure == "bad-prior-counter":
                    payloads[0]["tree_fetches"] = True
                self.set_sink(changed, payloads, capture.evidence["runtime"])
                if failure == "receipt-bool":
                    changed["projection_sink"]["registered_receipts"][0]["retry_count"] = True
                elif failure == "expected-bool":
                    changed["projection_sink"]["expected_final_count"] = True
                with self.assertRaises(proof.ProofRejected):
                    proof.validate_lane_measurement(changed, capture)

    def test_independent_backend_ownership_all_dimensions_required(self):
        a = self.capture("a")
        left = proof.validate_lane_measurement(self.lane(a), a)
        for key in ("project", "database", "instance_id", "base_url", "git_url", "cache_prefix",
                    "service_pid", "dependency_container_ids", "projection_sink_instance", "projection_sink_root",
                    "base_dir", "cache_dir", "pack_cache_dir"):
            with self.subTest(key=key):
                runtime = self.runtime("b")
                change = {key: getattr(self.runtime("a"), key)}
                if key == "projection_sink_root":
                    change.update(cache_dir=self.runtime("a").cache_dir,
                                  projection_sink_instance=self.runtime("a").projection_sink_instance)
                if key in {"cache_dir", "projection_sink_instance"}:
                    cache_dir = change.get("cache_dir", runtime.cache_dir)
                    sink_id = change.get("projection_sink_instance", runtime.projection_sink_instance)
                    change["projection_sink_root"] = cache_dir + "/logs/mst2-native-projection/" + sink_id
                if key in {"database", "instance_id"}:
                    # Match actual same deployment proof completely so the
                    # failure is pair ownership, not malformed native input.
                    if key == "database":
                        rows = runtime.identity_rows
                        for row in rows:
                            row["database"] = change[key]
                        change["identity_rows_json"] = proof.canonical(rows)
                    else:
                        native = runtime.native
                        native["instance_id"] = native["certificate_instance"] = change[key]
                        change["native_json"] = proof.canonical(native)
                b = self.capture("b", runtime=self.runtime("b", **change))
                right = proof.validate_lane_measurement(self.lane(b), b)
                with self.assertRaises(proof.ProofRejected):
                    proof.compare_pair(left, right)


if __name__ == "__main__":
    unittest.main()
