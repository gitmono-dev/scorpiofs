"""Independent-backend semantic and provenance proofs for the v3 campaign.

Capture the actual publication before advancing its head, then validate the
closed typed sink after its writer retires. Workspace/worker sink closure and
full oracle execution are separate campaign obligations; a digest is not an
oracle run. Pure replay validators never mint live runtime authority.
"""

from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import re

import commit_update_bench as common
import commit_update_projection as projection
import workspace_update_build as builds
import workspace_update_observation as observation


SCRIPT_PATHS = frozenset({
    ".github/workflows/mst2-real-update.yml",
    *{"tests/mst2-e2e/infra/aliyun/" + name + ".py"
      for name in ("direct_campaign", "direct_remote", "direct_sources")},
    *{"tests/mst2-e2e/commit_update_" + name + ".py"
      for name in ("bench", "budget", "ci", "projection")},
    *{"tests/mst2-e2e/workspace_update_" + name + ".py"
      for name in ("backend", "backend_proof", "bench", "build", "campaign", "campaign_export", "daemon",
                   "directory", "execution", "git_performance", "observation", "oracle", "profile", "request_diagnostic", "resources", "size", "worker")},
})
SOURCE_FIELDS = frozenset({
    "server_source_sha", "server_source_tree", "server_binary_sha256",
    "server_cargo_lock_sha256", "client_source_sha", "client_cargo_lock_sha256",
    "client_binary_sha256", "rustc_version", "cargo_version", "harness_source_sha",
    "harness_source_tree", "harness_files_sha256",
})
RUNTIME_FIELDS = frozenset({
    "revision", "phase", "round", "client", "project", "database", "instance_id",
    "base_url", "git_url", "server_source_sha", "server_source_tree",
    "server_binary_sha256", "server_cargo_lock_sha256", "service_pid",
    "service_starttime", "config_sha256", "compose_sha256", "cache_prefix",
    "base_dir", "cache_dir", "pack_cache_dir", "dependency_container_ids",
    "projection_sink_instance", "projection_sink_root", "projection_sink_device", "projection_sink_inode",
    "identity_rows", "identity", "native",
})
IDENTITY_FIELDS = frozenset({
    "project_commit", "project_tree", "global_commit", "global_tree", "namespace_view_id",
})
ROW_FIELDS = frozenset({"path", "commit", "tree", "database", "commit_tree", "raw_tree"})
NATIVE_FIELDS = frozenset({
    "instance_id", "sequence", "writer_epoch", "root_commit", "root_tree", "state",
    "certificate_receipt_id", "certificate_commit", "certificate_tree",
    "certificate_sequence", "certificate_instance", "path_commit", "path_tree",
    "origin_path", "origin_ref", "origin_sequence", "certificate_id",
    "certificate_namespace", "certificate_epoch", "old_root_commit", "old_path_commit",
    "receipt_id", "receipt_namespace", "old_oid", "new_oid", "receipt_epoch",
    "writer_kind", "request_digest", "request_digest_version", "native_certificate_version",
    "outbox_id", "outbox_namespace", "outbox_sequence",
})
NATIVE_COUNTERS = frozenset({
    "sequence", "writer_epoch", "certificate_receipt_id", "certificate_sequence",
    "origin_sequence", "certificate_id", "certificate_epoch", "receipt_id",
    "receipt_epoch", "request_digest_version", "native_certificate_version",
    "outbox_id", "outbox_sequence",
})
TIME_FIELDS = frozenset({
    "git_push_ms", "publication_visible_ms", "metadata_ready_ms",
    "durable_complete_ms", "durable_verified_ms", "git_verified_ms",
})
PUBLICATION_FIELDS = frozenset({
    "started_utc", "visible_utc", "started_monotonic", "visible_monotonic",
})
LANE_FIELDS = frozenset({
    "revision", "phase", "round", "client", "version", "fixed_commit", "path_tree",
    "oracle_manifest_sha256", "sources", "identity_rows", "identity",
    "native_publication", "native_proof_sha256", "workspace_binding",
    "workspace_binding_sha256", "projection_sink", "timings_ms", "publication",
})
SINK_FIELDS = frozenset({
    "records_jsonl", "status_json", "registered_receipts", "expected_final_count",
})
POLICY_FIELDS = frozenset({
    "source_domain", "schema_version", "metadata_codec", "materialization_policy",
    "fs_semantics", "access_projection", "verification_revision", "projection_revision",
})
I64_MAX = (1 << 63) - 1


class ProofRejected(AssertionError):
    """Fixed diagnostics only; never echo records, paths or credentials."""


def reject():
    raise ProofRejected("independent backend measurement proof rejected")


def require(condition):
    if not condition:
        reject()


def shape(value, fields):
    require(type(value) is dict and set(value) == fields)


def exact(actual, expected):
    require(type(actual) is type(expected))
    if type(expected) is dict:
        shape(actual, set(expected))
        for key, value in expected.items():
            exact(actual[key], value)
    elif type(expected) is list:
        require(len(actual) == len(expected))
        for left, right in zip(actual, expected):
            exact(left, right)
    else:
        require(actual == expected)


def integer(value, maximum=I64_MAX, positive=False):
    require(type(value) is int and int(positive) <= value <= maximum)
    return value


def finite(value):
    require(type(value) in (int, float) and math.isfinite(value) and value >= 0)
    return value


def hex_digest(value, length=64):
    require(type(value) is str and re.fullmatch(r"[0-9a-f]{" + str(length) + "}", value))
    return value


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def sources(value):
    shape(value, SOURCE_FIELDS)
    for key in SOURCE_FIELDS - {"rustc_version", "cargo_version", "harness_files_sha256"}:
        hex_digest(value[key], 40 if key.endswith(("source_sha", "source_tree")) else 64)
    for key, prefix in (("rustc_version", "rustc "), ("cargo_version", "cargo ")):
        require(type(value[key]) is str and value[key].startswith(prefix)
                and len(value[key]) <= 512 and not any(c in value[key] for c in "\r\n\0"))
    shape(value["harness_files_sha256"], SCRIPT_PATHS)
    for value_hash in value["harness_files_sha256"].values():
        hex_digest(value_hash)


def native_identity(rows, identity, native, database, instance):
    shape(identity, IDENTITY_FIELDS)
    for key in IDENTITY_FIELDS - {"namespace_view_id"}:
        hex_digest(identity[key], 40)
    projection.canonical_digest(identity["namespace_view_id"])
    require(type(rows) is list and len(rows) == 2)
    for row in rows:
        shape(row, ROW_FIELDS)
        require(type(row["path"]) is str and row["path"] in {"/", "/project"}
                and type(row["database"]) is str and row["database"] == database)
        for key in ("commit", "tree", "commit_tree"):
            hex_digest(row[key], 40)
        if row["path"] == "/":
            require(type(row["raw_tree"]) is str and len(row["raw_tree"]) <= 2 * 1024 * 1024
                    and re.fullmatch(r"(?:[0-9a-f]{2})+", row["raw_tree"]))
        else:
            require(row["raw_tree"] is None)
    exact(common.validate_identity(rows, identity["project_commit"],
                                   identity["project_tree"], database), identity)
    shape(native, NATIVE_FIELDS)
    for key in NATIVE_COUNTERS:
        integer(native[key], positive=True)
    for key in ("root_commit", "root_tree", "certificate_commit", "certificate_tree",
                "path_commit", "path_tree", "old_root_commit", "old_path_commit", "old_oid", "new_oid"):
        hex_digest(native[key], 40)
    for key in ("instance_id", "certificate_instance"):
        projection.canonical_uuid(native[key])
    for key in NATIVE_FIELDS - NATIVE_COUNTERS:
        require(type(native[key]) is str)
    projection.canonical_digest(native["request_digest"])
    common.validate_native(native, identity, instance, True)


class _ImmutableEvidence:
    __slots__ = ("_json",)

    def __init__(self, *_args, **_kwargs):
        raise TypeError("proof receipts are created only by validation")

    def __setattr__(self, _name, _value):
        raise TypeError("proof receipts are immutable")

    @property
    def evidence(self):
        # Return a fresh tree, not a mutable reference to retained authority.
        return projection.parse(self._json)


class CapturedLane(_ImmutableEvidence):
    __slots__ = ()


class LaneProof(_ImmutableEvidence):
    """Semantic/provenance receipt, not full workload or performance acceptance."""

    __slots__ = ()


def _receipt(kind, value):
    result = object.__new__(kind)
    object.__setattr__(result, "_json", canonical(value))
    return result


def capture_lane_runtime(backend, client_build_receipt, source_expectations, deadline, *, harness_root):
    """Mint a publication capability from real owner/build/source probes.

    This call must precede advancing the backend head. Its immutable capability
    can then accompany that lane's later closed sink. Explicit fixed source
    expectations are supplied by the campaign, not inferred from a branch.
    """
    from workspace_update_backend import OwnedBackend, RuntimeBinding
    try:
        require(type(backend) is OwnedBackend)
        sources(source_expectations)
        binding = backend.verify_runtime(deadline)
        require(type(binding) is RuntimeBinding)
        runtime = {key: getattr(binding, key) for key in RUNTIME_FIELDS}
        runtime["dependency_container_ids"] = list(runtime["dependency_container_ids"])
        shape(runtime, RUNTIME_FIELDS)
        integer(runtime["revision"], 1, True)
        integer(runtime["round"], 3, True)
        require(type(runtime["phase"]) is str and runtime["phase"] in {"fair", "diagnostic"}
                and type(runtime["client"]) is str and runtime["client"] in {"a", "b"})
        require(runtime["phase"] != "diagnostic"
                or runtime["round"] == 1 and runtime["client"] == "b")
        for key in ("project", "database", "base_url", "git_url", "cache_prefix",
                    "base_dir", "cache_dir", "pack_cache_dir", "projection_sink_root"):
            require(type(runtime[key]) is str and 1 <= len(runtime[key]) <= 4096
                    and not any(c in runtime[key] for c in "\r\n\0"))
        projection.canonical_uuid(runtime["instance_id"])
        integer(runtime["service_pid"], positive=True)
        projection.canonical_uuid(runtime["projection_sink_instance"])
        integer(runtime["projection_sink_device"])
        integer(runtime["projection_sink_inode"], positive=True)
        hex_digest(runtime["config_sha256"])
        hex_digest(runtime["compose_sha256"])
        exact(runtime["projection_sink_root"], runtime["cache_dir"].rstrip("/")
              + "/logs/mst2-native-projection/" + runtime["projection_sink_instance"])
        require(type(runtime["service_starttime"]) is str
                and re.fullmatch(r"[1-9][0-9]{0,19}", runtime["service_starttime"]))
        require(type(binding.dependency_container_ids) is tuple
                and len(runtime["dependency_container_ids"]) == 4
                and len(set(runtime["dependency_container_ids"])) == len(runtime["dependency_container_ids"]))
        for value in runtime["dependency_container_ids"]:
            hex_digest(value)
        for key in ("server_source_sha", "server_source_tree", "server_binary_sha256", "server_cargo_lock_sha256"):
            exact(runtime[key], source_expectations[key])
        native_identity(runtime["identity_rows"], runtime["identity"], runtime["native"],
                        runtime["database"], runtime["instance_id"])
        build = builds.load(client_build_receipt, runtime["client"], deadline).build
        shape(build, builds.FIELDS)
        integer(build["revision"], 1, True)
        for key, build_key in (("client_source_sha", "source_sha"), ("client_cargo_lock_sha256", "cargo_lock_sha256"),
                               ("client_binary_sha256", "binary_sha256"), ("rustc_version", "rustc_version"),
                               ("cargo_version", "cargo_version")):
            exact(source_expectations[key], build[build_key])
        raw_build = Path(client_build_receipt).read_bytes()
        require(len(raw_build) <= 16384)
        exact(projection.parse(raw_build), build)
        harness, _lock = builds.fixed_source(harness_root, source_expectations["harness_source_sha"], deadline)
        tree = builds.output(["git", "-C", str(harness), "rev-parse", "HEAD^{tree}"], deadline).decode().strip()
        exact(tree, source_expectations["harness_source_tree"])
        for name in SCRIPT_PATHS:
            path = harness / name
            require(path.is_file() and not path.is_symlink()
                    and not any(parent.is_symlink() for parent in path.parents))
            exact(builds.sha256(path, deadline), source_expectations["harness_files_sha256"][name])
        builds.remaining(deadline)
        return _receipt(CapturedLane, {"runtime": runtime, "sources": source_expectations,
                        "client_build": build, "client_build_receipt_sha256": hashlib.sha256(raw_build).hexdigest()})
    except (AssertionError, AttributeError, KeyError, TypeError, ValueError, OSError):
        reject()


def closed_projection_sink(value, binding, runtime):
    """Validate all records/retries, raw payload hashes and the closed writer."""
    shape(value, SINK_FIELDS)
    require(type(value["records_jsonl"]) is str and type(value["status_json"]) is str)
    raw = value["records_jsonl"].encode("utf-8")
    raw_status = value["status_json"].encode("utf-8")
    require(len(raw) <= projection.FILE_BYTES and len(raw_status) <= projection.STATUS_BYTES)
    status = projection.parse(raw_status)
    projection.shape(status, projection.STATUS_FIELDS)
    integer(status["writer_revision"], 1, True)
    exact(status["sink_instance"], runtime["projection_sink_instance"])
    for name in ("accepted_records", "written_sequence", "written_records"):
        integer(status[name], projection.RECORD_LIMIT)
    integer(status["written_bytes"], projection.FILE_BYTES)
    integer(status["first_error_code"], 255)
    require(status["first_error_code"] == 0 and status["closed"] is True)
    require(status["written_sequence"] == status["written_records"] == status["accepted_records"]
            and status["written_bytes"] == len(raw) and status["rolling_sha256"] == projection.digest(raw))
    receipts = value["registered_receipts"]
    require(type(receipts) is list and 1 <= len(receipts) <= projection.RECORD_LIMIT)
    integer(value["expected_final_count"], projection.RECORD_LIMIT, True)
    require(value["expected_final_count"] == len(receipts))
    admitted = set()
    finals = set()
    current_receipt = binding["resolve_trace_receipt"]
    for receipt in receipts:
        projection.shape(receipt, projection.RECEIPT_FIELDS)
        attempts = projection.receipt_ids(receipt, receipt["logical_request_id"])
        require(not admitted.intersection(attempts))
        admitted.update(attempts)
        finals.add(attempts[-1])
    require(any(receipt == current_receipt for receipt in receipts))
    lines = raw.splitlines(keepends=True)
    require(len(lines) == status["written_records"])
    payloads = {}
    payload_hashes = {}
    for number, line in enumerate(lines, 1):
        require(line.endswith(b"\n") and len(line) <= projection.RECORD_BYTES)
        envelope = projection.parse(line)
        projection.shape(envelope, projection.ENVELOPE_FIELDS)
        integer(envelope["writer_revision"], 1, True)
        exact(envelope["sink_instance"], runtime["projection_sink_instance"])
        require(integer(envelope["record_sequence"], projection.RECORD_LIMIT, True) == number)
        projection.canonical_digest(envelope["payload_sha256"])
        prefix = (f'{{"writer_revision":1,"sink_instance":"{runtime["projection_sink_instance"]}",'
                  f'"record_sequence":{number},"payload":').encode()
        suffix = (',"payload_sha256":"' + envelope["payload_sha256"] + '"}\n').encode()
        require(line.startswith(prefix) and line.endswith(suffix)
                and projection.digest(line[len(prefix):-len(suffix)]) == envelope["payload_sha256"])
        payload = envelope["payload"]
        projection.validate_payload(payload)
        require(payload["instance_id"] == runtime["instance_id"]
                and payload["request_id"] in admitted and payload["request_id"] not in payloads)
        payloads[payload["request_id"]] = payload
        payload_hashes[payload["request_id"]] = envelope["payload_sha256"]
    require(finals <= set(payloads))
    payload = payloads[current_receipt["final_attempt_id"]]
    native = runtime["native"]
    identity = runtime["identity"]
    bindings = {
        "instance_id": runtime["instance_id"], "root_commit_oid": "sha1:" + identity["global_commit"],
        "root_tree_oid": "sha1:" + identity["global_tree"],
        "native_certificate_receipt_id": native["certificate_receipt_id"],
        "native_writer_epoch": native["writer_epoch"], "native_publication_sequence": native["sequence"],
        "namespace_view_id": identity["namespace_view_id"], "scope": "/project",
        "snapshot_id": binding["snapshot_id"],
    }
    for key, expected in bindings.items():
        exact(payload[key], expected)
    exact(binding["namespace_view_id"], payload["namespace_view_id"])
    exact(binding["publication_sequence"], payload["native_publication_sequence"])
    exact(binding["descriptor_bytes_hex"], projection.descriptor_bytes(payload).hex())
    return {"payload": payload, "records_sha256": hashlib.sha256(raw).hexdigest(),
            "status_sha256": hashlib.sha256(raw_status).hexdigest(),
            "payload_sha256": payload_hashes[current_receipt["final_attempt_id"]],
            "payload_hashes": payload_hashes, "status": status}


def publication(value):
    shape(value, PUBLICATION_FIELDS)
    stamps = []
    for key in ("started_utc", "visible_utc"):
        text = value[key]
        require(type(text) is str and re.fullmatch(
            r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z", text))
        stamps.append(datetime.fromisoformat(text[:-1] + "+00:00").astimezone(timezone.utc))
    require(stamps[0] <= stamps[1])
    require(finite(value["started_monotonic"]) <= finite(value["visible_monotonic"]))


def validate_lane_measurement(record, captured):
    """Close one lane proof against its captured actual immutable publication."""
    try:
        require(type(captured) is CapturedLane)
        return _receipt(LaneProof, validate_lane_values(record, captured.evidence))
    except (AssertionError, AttributeError, KeyError, TypeError, ValueError, OverflowError):
        reject()


def validate_lane_values(record, capture):
    """Replay values without minting a live runtime or measurement capability."""
    try:
        shape(capture, {"runtime", "sources", "client_build", "client_build_receipt_sha256"})
        runtime = capture["runtime"]
        shape(runtime, RUNTIME_FIELDS)
        integer(runtime["revision"], 1, True)
        integer(runtime["round"], 3, True)
        require(runtime["phase"] in ("fair", "diagnostic") and runtime["client"] in ("a", "b"))
        require(runtime["phase"] != "diagnostic" or runtime["round"] == 1 and runtime["client"] == "b")
        for key in ("config_sha256", "compose_sha256"):
            hex_digest(runtime[key])
        integer(runtime["service_pid"], positive=True)
        integer(runtime["projection_sink_device"])
        integer(runtime["projection_sink_inode"], positive=True)
        require(type(runtime["service_starttime"]) is str and re.fullmatch(r"[1-9][0-9]{0,19}", runtime["service_starttime"]))
        for key in ("project", "database", "base_url", "git_url", "cache_prefix", "base_dir", "cache_dir", "pack_cache_dir", "projection_sink_root"):
            require(type(runtime[key]) is str and 1 <= len(runtime[key]) <= 4096 and not any(c in runtime[key] for c in "\r\n\0"))
        require(type(runtime["dependency_container_ids"]) is list and len(runtime["dependency_container_ids"]) == 4
                and len(set(runtime["dependency_container_ids"])) == 4)
        for value in runtime["dependency_container_ids"]:
            hex_digest(value)
        projection.canonical_uuid(runtime["instance_id"])
        projection.canonical_uuid(runtime["projection_sink_instance"])
        exact(runtime["projection_sink_root"], runtime["cache_dir"].rstrip("/") + "/logs/mst2-native-projection/" + runtime["projection_sink_instance"])
        sources(capture["sources"])
        hex_digest(capture["client_build_receipt_sha256"])
        shape(capture["client_build"], builds.FIELDS)
        for key in ("server_source_sha", "server_source_tree", "server_binary_sha256", "server_cargo_lock_sha256"):
            exact(runtime[key], capture["sources"][key])
        for source_key, build_key in (("client_source_sha", "source_sha"), ("client_cargo_lock_sha256", "cargo_lock_sha256"),
                ("client_binary_sha256", "binary_sha256"), ("rustc_version", "rustc_version"), ("cargo_version", "cargo_version")):
            exact(capture["sources"][source_key], capture["client_build"][build_key])
        exact(capture["client_build"]["label"], runtime["client"])
        shape(record, LANE_FIELDS)
        integer(record["revision"], 1, True)
        integer(record["version"], 10, True)
        for key in ("phase", "round", "client"):
            exact(record[key], runtime[key])
        sources(record["sources"])
        exact(record["sources"], capture["sources"])
        hex_digest(record["fixed_commit"], 40)
        hex_digest(record["path_tree"], 40)
        hex_digest(record["oracle_manifest_sha256"])
        for key, runtime_key in (("identity_rows", "identity_rows"), ("identity", "identity"),
                                 ("native_publication", "native")):
            exact(record[key], runtime[runtime_key])
        exact(record["fixed_commit"], runtime["identity"]["project_commit"])
        exact(record["path_tree"], runtime["identity"]["project_tree"])
        native_identity(record["identity_rows"], record["identity"], record["native_publication"],
                        runtime["database"], runtime["instance_id"])
        native_proof = {key: record[key] for key in ("identity_rows", "identity", "native_publication")}
        exact(hex_digest(record["native_proof_sha256"]), digest(native_proof))
        binding = record["workspace_binding"]
        observation.validate_binding(binding, binding["run_id"])
        exact(binding["instance_id"], runtime["instance_id"])
        exact(hex_digest(record["workspace_binding_sha256"]), digest(binding))
        sink = closed_projection_sink(record["projection_sink"], binding, runtime)
        shape(record["timings_ms"], TIME_FIELDS)
        for value in record["timings_ms"].values():
            finite(value)
        require(record["timings_ms"]["metadata_ready_ms"] <= record["timings_ms"]["durable_complete_ms"]
                <= record["timings_ms"]["durable_verified_ms"])
        publication(record["publication"])
        semantic = {"fixed_commit": record["fixed_commit"], "path_tree": record["path_tree"],
                    "scope": binding["scope"], "oracle_manifest_sha256": record["oracle_manifest_sha256"],
                    "metadata_root": sink["payload"]["metadata_root"],
                    "policy": {key: sink["payload"][key] for key in POLICY_FIELDS}}
        return {"lane": record, "capture": capture,
                "semantic": semantic, "closed_projection": sink}
    except (AssertionError, AttributeError, KeyError, TypeError, ValueError, OverflowError):
        reject()


def compare_pair(first, second):
    """Accept semantic equality after each lane's semantic/provenance proof."""
    try:
        require(type(first) is LaneProof and type(second) is LaneProof)
        return compare_pair_values(first.evidence, second.evidence)
    except (AssertionError, AttributeError, KeyError, TypeError, ValueError):
        reject()


def compare_pair_values(left, right):
    """Replay the complete pair checks without granting live owner authority."""
    try:
        exact(left, validate_lane_values(left["lane"], left["capture"]))
        exact(right, validate_lane_values(right["lane"], right["capture"]))
        require({left["lane"]["client"], right["lane"]["client"]} == {"a", "b"})
        for key in ("phase", "round", "version"):
            exact(left["lane"][key], right["lane"][key])
        require(left["lane"]["phase"] == "fair")
        exact(left["semantic"], right["semantic"])
        left_runtime, right_runtime = left["capture"]["runtime"], right["capture"]["runtime"]
        for key in ("project", "database", "instance_id", "base_url", "git_url", "cache_prefix",
                    "projection_sink_instance", "projection_sink_root"):
            require(left_runtime[key] != right_runtime[key])
        # Fair pair owners are alive together; a changed starttime cannot make
        # the same PID represent two concurrently independent processes.
        require(left_runtime["service_pid"] != right_runtime["service_pid"])
        require((left_runtime["projection_sink_device"], left_runtime["projection_sink_inode"])
                != (right_runtime["projection_sink_device"], right_runtime["projection_sink_inode"]))
        require(not set(left_runtime["dependency_container_ids"]).intersection(right_runtime["dependency_container_ids"]))
        for left_path in (left_runtime[key] for key in ("base_dir", "cache_dir", "pack_cache_dir")):
            for right_path in (right_runtime[key] for key in ("base_dir", "cache_dir", "pack_cache_dir")):
                left_dir, right_dir = PurePosixPath(left_path), PurePosixPath(right_path)
                require(left_dir.is_absolute() and right_dir.is_absolute()
                        and not left_dir.is_relative_to(right_dir) and not right_dir.is_relative_to(left_dir))
        common_sources = SOURCE_FIELDS - {"client_source_sha", "client_binary_sha256"}
        for key in common_sources:
            exact(left["capture"]["sources"][key], right["capture"]["sources"][key])
        require(left["capture"]["sources"]["client_source_sha"] != right["capture"]["sources"]["client_source_sha"])
        for key in ("source", "binary"):
            require(left["capture"]["client_build"][key] != right["capture"]["client_build"][key])
        return {"proof_scope": "SEMANTIC_PROVENANCE_ONLY_NOT_WORKLOAD_ACCEPTANCE",
                "semantic": left["semantic"], "lane_proof_sha256": [digest(left), digest(right)],
                "backend_receipt_sha256": [digest(left_runtime), digest(right_runtime)]}
    except (AssertionError, AttributeError, KeyError, TypeError, ValueError):
        reject()
