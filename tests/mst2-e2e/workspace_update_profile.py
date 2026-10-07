"""Closed numeric checkpoints for explicitly enabled v3 read diagnostics.

The identity is private to admission; published checkpoints contain no workspace
IDs, generations, paths, SIDs, tokens or response messages. Cumulative timers
can overlap across threads and phases. Their deltas are counter changes across
the sampled interval, not an exclusive breakdown of the oracle's wall time.
"""

from copy import deepcopy
from dataclasses import dataclass
import json
from pathlib import Path
import re
import uuid


U64_MAX = (1 << 64) - 1
METRICS = (
    "owner_cache_hit", "owner_cache_miss", "cache_get_probes", "cache_insert_probes",
    "cache_evictions", "node_file_clones", "node_directory_clones",
    "directory_load_skipped", "directory_load", "directory_reply_entries_built",
    "directory_reply_name_bytes", "metadata_local_pages", "metadata_wire_pages",
    "small_cas_calls", "small_cas_read_attempts", "small_cas_read_bytes", "small_cas_read_eof",
    "small_cas_read_interrupted", "small_cas_whole_hash_bytes", "small_cas_append_bytes",
    "large_cas_calls", "large_cas_append_bytes", "large_cas_read_bytes", "large_whole_hash_bytes",
    "large_chunk_hash_bytes", "large_index_hit", "large_index_built", "large_strict_fallback",
    "reply_owners", "reply_owner_bytes", "reply_payload_copy_bytes", "worker_admitted",
    "worker_backing_hit", "worker_backing_miss", "worker_error", "worker_panic",
    "worker_rejected", "worker_lease_rejected", "worker_cancelled_pending", "worker_detached",
    "worker_queue_ns", "worker_wall_ns",
)
PHASES = (
    "lower_getattr_mapping", "lower_read", "directory_load", "lease_before_read",
    "membership_and_lease", "validate_proven_file", "cache_get", "cache_insert",
    "reply_owner", "small_cas", "small_cas_open", "small_cas_fstat", "small_cas_read",
    "small_cas_hash", "large_cas",
)
OPERATIONS = (
    "lookup", "getattr", "open", "read", "readlink", "opendir", "readdir",
    "readdirplus", "release", "releasedir",
)
PHASE_FIELDS = frozenset({"calls", "active", "wall_ns", "max_wall_ns"})
OPERATION_FIELDS = frozenset({
    "calls", "completed", "errors", "dropped", "active", "returned_bytes",
    "wall_ns", "max_wall_ns", "empty_read_replies",
})
BOUNDARY_FLAGS = frozenset({
    "native_kernel_copy_measured", "upper_reply_copy_measured", "directory_stream_delivery_measured",
})
SNAPSHOT_FIELDS = frozenset({
    "revision", "sequence", "sample_started_ns", "sample_finished_ns", "overflow",
    "workers_active", "metrics", "phases", "operations", *BOUNDARY_FLAGS,
})
MODES = frozenset({"disabled", "unsupported", "enabled"})
DIAGNOSTIC_INTERPRETATION = "INSTRUMENTED_DIAGNOSTIC_NOT_FREE_PERFORMANCE_BASELINE"


class ProfileError(RuntimeError):
    """Static failure vocabulary; private payloads never become error messages."""

    error_code = "workspace_read_profile_invalid"
    worker_stage = "read_profile"

    def __init__(self):
        super().__init__("workspace read profile is invalid")


def _require(valid):
    if not valid:
        raise ProfileError()


def _shape(value, fields):
    _require(type(value) is dict and set(value) == fields)


def _u64(value):
    _require(type(value) is int and 0 <= value <= U64_MAX)
    return value


def _identity(value):
    _require(type(value) is str and re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", value) is not None)
    _require(str(uuid.UUID(value)) == value)
    return value


def _phase(value):
    _shape(value, PHASE_FIELDS)
    for number in value.values():
        _u64(number)
    _require(value["active"] <= value["calls"] and value["max_wall_ns"] <= value["wall_ns"])
    if value["calls"] == 0:
        _require(value["wall_ns"] == 0)


def _operation(name, value):
    _shape(value, OPERATION_FIELDS)
    for number in value.values():
        _u64(number)
    # Errors are completed futures; cancellation/drop is a separate partition.
    _require(value["calls"] == value["completed"] + value["dropped"] + value["active"])
    _require(value["errors"] <= value["completed"] and value["max_wall_ns"] <= value["wall_ns"])
    _require(value["empty_read_replies"] <= value["completed"] - value["errors"])
    if name != "read":
        _require(value["empty_read_replies"] == 0)
    if value["completed"] == 0:
        _require(value["returned_bytes"] == 0)
    if value["completed"] + value["dropped"] == 0:
        _require(value["wall_ns"] == 0)


def _entries(raw, names, key, fields):
    _require(type(raw) is list and len(raw) == len(names))
    result = {}
    for entry in raw:
        _shape(entry, {key, *fields})
        name = entry[key]
        _require(type(name) is str and name in names and name not in result)
        result[name] = {field: entry[field] for field in fields}
    _require(set(result) == set(names))
    return result


def _numeric_snapshot(value):
    _shape(value, SNAPSHOT_FIELDS)
    for field in ("revision", "sequence", "sample_started_ns", "sample_finished_ns", "workers_active"):
        _u64(value[field])
    _require(value["revision"] == 1 and value["sequence"] >= 1)
    _require(value["sample_started_ns"] <= value["sample_finished_ns"])
    # Saturated counters cannot yield trustworthy subtraction. Never accept
    # them as zero activity, and never claim unmeasured delivery/copy buckets.
    for field in ("overflow", *BOUNDARY_FLAGS):
        _require(value[field] is False)
    _shape(value["metrics"], set(METRICS))
    for number in value["metrics"].values():
        _u64(number)
    _require(value["workers_active"] <= value["metrics"]["worker_admitted"])
    _shape(value["phases"], set(PHASES))
    for times in value["phases"].values():
        _phase(times)
    _shape(value["operations"], set(OPERATIONS))
    for name, times in value["operations"].items():
        _operation(name, times)


@dataclass(frozen=True)
class Checkpoint:
    workspace_id: str
    generation: str
    counters: dict


def parse_checkpoint(raw, workspace_id, generation):
    """Bind the exact HTTP schema to the workspace actually being verified."""
    _identity(workspace_id)
    _identity(generation)
    _shape(raw, {"workspace_id", "generation", "profile"})
    _identity(raw["workspace_id"])
    _identity(raw["generation"])
    _require(raw["workspace_id"] == workspace_id and raw["generation"] == generation)
    _shape(raw["profile"], SNAPSHOT_FIELDS)
    profile = deepcopy(raw["profile"])
    entries = _entries(profile["metrics"], METRICS, "metric", {"value"})
    profile["metrics"] = {name: entry["value"] for name, entry in entries.items()}
    profile["phases"] = _entries(profile["phases"], PHASES, "phase", PHASE_FIELDS)
    _require(type(profile["operations"]) is list and len(profile["operations"]) == len(OPERATIONS))
    operations = {}
    for entry in profile["operations"]:
        _shape(entry, {"operation", "times"})
        name = entry["operation"]
        _require(type(name) is str and name in OPERATIONS and name not in operations)
        operations[name] = entry["times"]
    profile["operations"] = operations
    _numeric_snapshot(profile)
    return Checkpoint(workspace_id, generation, profile)


def _changes(before, after, fields):
    result = {}
    for field in fields:
        if field in {"active", "max_wall_ns"}:
            # These are gauges/high-water marks, not subtractable interval
            # counts. A prior operation may complete between the snapshots.
            if field == "max_wall_ns":
                _require(after[field] >= before[field])
            result[field + "_before"] = before[field]
            result[field + "_after"] = after[field]
        else:
            _require(after[field] >= before[field])
            result[field] = after[field] - before[field]
    return result


def _delta(before, after):
    _require(after["sequence"] > before["sequence"])
    _require(after["sample_started_ns"] >= before["sample_finished_ns"])
    metrics = _changes(before["metrics"], after["metrics"], METRICS)
    phases = {name: _changes(before["phases"][name], after["phases"][name], PHASE_FIELDS)
              for name in PHASES}
    operations = {name: _changes(before["operations"][name], after["operations"][name], OPERATION_FIELDS)
                  for name in OPERATIONS}
    return {"metrics": metrics, "phases": phases, "operations": operations,
            "workers_active_before": before["workers_active"], "workers_active_after": after["workers_active"],
            "sample_interval_ns": after["sample_started_ns"] - before["sample_finished_ns"]}


def not_measured(mode):
    _require(type(mode) is str and mode in {"disabled", "unsupported"})
    return {"status": "NOT_MEASURED", "reason": mode}


def measured(before, after, before_duration_ns, after_duration_ns, oracle_elapsed_ns):
    _require(type(before) is Checkpoint and type(after) is Checkpoint)
    _identity(before.workspace_id)
    _identity(before.generation)
    _require((before.workspace_id, before.generation) == (after.workspace_id, after.generation))
    _numeric_snapshot(before.counters)
    _numeric_snapshot(after.counters)
    result = {"status": "MEASURED", "scope": "current_full_oracle", "interpretation": "DIAGNOSTIC_ONLY",
              "checkpoint_before_duration_ns": _u64(before_duration_ns),
              "checkpoint_after_duration_ns": _u64(after_duration_ns),
              "checkpoint_overhead_ns": _u64(before_duration_ns + after_duration_ns),
              "profiled_current_oracle_ns": _u64(oracle_elapsed_ns),
              "before": deepcopy(before.counters), "after": deepcopy(after.counters),
              "delta": _delta(before.counters, after.counters)}
    validate_evidence(result)
    return result


def validate_evidence(value):
    """Recheck the closed published shape, including any delta tampering."""
    _require(type(value) is dict)
    if value.get("status") == "NOT_MEASURED":
        _shape(value, {"status", "reason"})
        _require(type(value["reason"]) is str and value["reason"] in {"disabled", "unsupported"})
        return
    _shape(value, {"status", "scope", "interpretation", "checkpoint_before_duration_ns",
                   "checkpoint_after_duration_ns", "checkpoint_overhead_ns", "profiled_current_oracle_ns",
                   "before", "after", "delta"})
    _require(value["status"] == "MEASURED" and value["scope"] == "current_full_oracle"
             and value["interpretation"] == "DIAGNOSTIC_ONLY")
    _u64(value["checkpoint_before_duration_ns"])
    _u64(value["checkpoint_after_duration_ns"])
    _u64(value["checkpoint_overhead_ns"])
    _u64(value["profiled_current_oracle_ns"])
    _require(value["checkpoint_overhead_ns"] == value["checkpoint_before_duration_ns"]
             + value["checkpoint_after_duration_ns"])
    _numeric_snapshot(value["before"])
    _numeric_snapshot(value["after"])
    expected = _delta(value["before"], value["after"])
    # Equality alone would admit bools and floats masquerading as integers.
    def exact(actual, wanted):
        _require(type(actual) is type(wanted))
        if type(wanted) is dict:
            _shape(actual, set(wanted))
            for key in wanted:
                exact(actual[key], wanted[key])
        else:
            _require(actual == wanted)
    exact(value["delta"], expected)


def validate_artifact(path, *, diagnostic_required=False):
    """Check profile fields in the saved measurement copy before publication.

    Historic unprofiled records are accepted without manufacturing a profile.
    Duplicate JSON keys are rejected before a tampered value could be hidden.
    """
    def pairs(entries):
        result = {}
        for key, value in entries:
            _require(key not in result)
            result[key] = value
        return result

    def constant(_value):
        raise ProfileError()

    _require(type(diagnostic_required) is bool)
    profiles = 0
    diagnostic = diagnostic_required
    try:
        with Path(path).open("rb") as stream:
            while True:
                line = stream.readline(16 * 1024 * 1024 + 1)
                if not line:
                    break
                _require(len(line) <= 16 * 1024 * 1024)
                row = json.loads(line, object_pairs_hook=pairs, parse_constant=constant)
                _require(type(row) is dict)
                scorpio = row.get("scorpio")
                has_profile = type(scorpio) is dict and "read_profile" in scorpio
                labelled = row.get("measurement_interpretation") == DIAGNOSTIC_INTERPRETATION
                diagnostic = diagnostic or labelled or has_profile
                if diagnostic and row.get("record") in {"environment", "round", "summary", "paired_summary", "complete"}:
                    _require(labelled and row.get("performance_comparison_allowed") is False)
                if type(scorpio) is dict and "read_profile" in scorpio:
                    validate_evidence(scorpio["read_profile"])
                    profiles += 1
                elif diagnostic and row.get("record") == "round":
                    raise ProfileError()
    except (OSError, ValueError, UnicodeError):
        raise ProfileError() from None
    return profiles


def validate_artifact_tree(root, *, diagnostic_required=False):
    """Validate every copied measurements file, including nested allowlisted files."""
    root = Path(root)
    _require(type(diagnostic_required) is bool)
    _require(root.is_dir() and not root.is_symlink())
    profiles = 0
    for path in root.rglob("measurements.jsonl"):
        _require(path.is_file() and not path.is_symlink())
        parent = path.parent
        while parent != root:
            _require(not parent.is_symlink())
            parent = parent.parent
        profiles += validate_artifact(path, diagnostic_required=diagnostic_required)
    return profiles
