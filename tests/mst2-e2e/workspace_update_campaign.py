"""Three fair isolated A/B rounds and a fresh B diagnostic in one UTC window.

Every lane reads its own actual native and workspace sinks. Completion follows
the full byte oracles, retained descriptors/upper audits, sink closure, owned
process retirement and complete dependency inventory. This module does not
dispatch a workflow or manufacture native READY metadata.
"""

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import statistics
import sys
import time
from types import SimpleNamespace
import uuid

import commit_update_bench as common
import commit_update_budget as budgets
import commit_update_ci as ci
import commit_update_projection as projection
import workspace_update_backend as backends
import workspace_update_backend_proof as proofs
import workspace_update_build as builds
import workspace_update_bench as measurement
import workspace_update_observation as observation
import workspace_update_oracle as oracle
import workspace_update_profile as profiles
import workspace_update_execution as execution
from workspace_update_daemon import WorkspaceDaemon, mounts_under
from workspace_update_worker import WorkerSession, DIRTY_BYTES, DIRTY_SENTINEL

BASELINE = builds.DEFAULT_BASELINE
CANDIDATE = builds.DEFAULT_CANDIDATE
ORACLE_FIELDS = frozenset({"verified_files", "verified_directories", "verified_bytes",
    "regular_read_calls", "oracle_walk_and_hash_ms", "isolated_oracle_process_ms",
    "raw_empty_tree_directories_omitted_by_git"})


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def commit_time(value):
    if type(value) is not str or re.fullmatch(r"[0-9]+", value) is None:
        raise ValueError("bootstrap time requires ASCII uint32 seconds")
    number = int(value)
    if number > (1 << 32) - 1:
        raise ValueError("bootstrap time exceeds uint32 seconds")
    return number


def write_json(path, value, deadline):
    backends._check(deadline)
    backends._write(Path(path), value, new=True)
    backends._check(deadline)


def append(path, value, phase, deadline):
    backends._check(deadline)
    row = deepcopy(value)
    row["phase"] = phase
    if phase == "diagnostic":
        row.update(measurement_interpretation=profiles.DIAGNOSTIC_INTERPRETATION,
                   performance_comparison_allowed=False)
        if row.get("record") == "round":
            profiles.validate_evidence(row["scorpio"]["read_profile"])
    elif "measurement_interpretation" in row or "read_profile" in row.get("scorpio", {}):
        raise AssertionError("fair measurements must have the optional profiler disabled")
    payload = (proofs.canonical(row) + "\n").encode("ascii")
    path = Path(path)
    if path.is_symlink() or (path.exists() and path.stat().st_nlink != 1):
        raise AssertionError("campaign evidence file was substituted")
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0)
    with os.fdopen(os.open(path, flags, 0o600), "ab") as stream:
        stream.seek(0, os.SEEK_END)
        start = stream.tell()
        try:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
            backends._check(deadline)
        except BaseException:
            stream.truncate(start)
            raise


def source_expectations(options, client, deadline, harness):
    source, server_lock = builds.fixed_source(options.mega_source, options.mega_sha, deadline)
    hroot, _ = builds.fixed_source(harness, options.harness_sha, deadline)
    value = {
        "server_source_sha": options.mega_sha,
        "server_source_tree": common.git(source, deadline, "rev-parse", "HEAD^{tree}").decode().strip(),
        "server_binary_sha256": builds.sha256(options.mega_binary, deadline),
        "server_cargo_lock_sha256": server_lock,
        "client_source_sha": client.build["source_sha"],
        "client_cargo_lock_sha256": client.build["cargo_lock_sha256"],
        "client_binary_sha256": client.driver_sha256,
        "rustc_version": client.build["rustc_version"], "cargo_version": client.build["cargo_version"],
        "harness_source_sha": options.harness_sha,
        "harness_source_tree": common.git(hroot, deadline, "rev-parse", "HEAD^{tree}").decode().strip(),
        "harness_files_sha256": {name: builds.sha256(hroot / name, deadline) for name in proofs.SCRIPT_PATHS},
    }
    proofs.sources(value)
    return value


def canonical_seed(group, owner, deadline):
    """Create once from the actual canonical initial path parent, never force."""
    if group.seed is not None:
        if owner.initial_commit != group.seed["parent"]:
            raise AssertionError("fresh backend initial path parent differs from canonical seed")
        return group.seed
    fixture = group.root / "canonical-seed"
    common.command(["git", "clone", "--no-checkout", "--single-branch", "--branch", "main",
                    owner.git_url, str(fixture)], deadline, env=owner.git_env)
    parent = common.git(fixture, deadline, "rev-parse", "HEAD").decode().strip()
    tree = common.git(fixture, deadline, "rev-parse", "HEAD^{tree}").decode().strip()
    if parent != owner.initial_commit:
        raise AssertionError("canonical seed clone differs from actual initial path parent")
    env = common.clean_env({"GIT_AUTHOR_NAME": "MST2 setup baseline", "GIT_COMMITTER_NAME": "MST2 setup baseline",
        "GIT_AUTHOR_EMAIL": "mst2-setup@example.invalid", "GIT_COMMITTER_EMAIL": "mst2-setup@example.invalid",
        "GIT_AUTHOR_DATE": "@" + str(group.options.bootstrap_commit_time) + " +0000",
        "GIT_COMMITTER_DATE": "@" + str(group.options.bootstrap_commit_time) + " +0000"})
    commit = common.git(fixture, deadline, "commit-tree", tree, "-p", parent, env=env,
                        data=b"MST2 canonical native seed\n").decode().strip()
    common.git(fixture, deadline, "update-ref", "refs/heads/main", commit, parent)
    commit_body = common.git(fixture, deadline, "cat-file", "commit", commit)
    tree_body = common.git(fixture, deadline, "cat-file", "tree", tree)
    parent_body = common.git(fixture, deadline, "cat-file", "commit", parent)
    group.seed = {"parent": parent, "commit": commit, "tree": tree,
                  "parent_commit_body_hex": parent_body.hex(), "commit_body_hex": commit_body.hex(),
                  "tree_body_hex": tree_body.hex(),
                  "commit_body_sha256": hashlib.sha256(commit_body).hexdigest(),
                  "tree_body_sha256": hashlib.sha256(tree_body).hexdigest(),
                  "bootstrap_commit_time": group.options.bootstrap_commit_time}
    write_json(group.root / "canonical-seed.json", group.seed, deadline)
    return group.seed


class ManifestFacts:
    """Internal compact oracle facts, bound to the entire canonical manifest.

    This object is never accepted from evidence JSON. Export derives it from a
    parsed embedded manifest, then checks its fingerprint against the separately
    pinned manifest file before validating the full retained-view workload.
    """
    __slots__ = ("fingerprint", "files", "directories", "git_directories", "bytes", "omitted")

    def __init__(self, expected, *, fingerprint=True):
        full, materialized, omitted = oracle.directory_sets(expected)
        self.fingerprint = proofs.digest(expected) if fingerprint else None
        self.files = len(expected["files"])
        self.directories, self.git_directories = len(full), len(materialized)
        self.bytes = sum(file["size"] for file in expected["files"])
        self.omitted = sorted(omitted)


def validate_oracle(value, expected, *, git=False, dirty=False):
    proofs.shape(value, ORACLE_FIELDS)
    facts = expected if isinstance(expected, ManifestFacts) else ManifestFacts(expected, fingerprint=False)
    proofs.exact(value["verified_files"], facts.files + int(dirty))
    proofs.exact(value["verified_directories"], facts.git_directories if git else facts.directories)
    proofs.exact(value["verified_bytes"], facts.bytes
                 + (len(DIRTY_BYTES) if dirty else 0))
    proofs.exact(value["raw_empty_tree_directories_omitted_by_git"], facts.omitted if git else [])
    proofs.integer(value["regular_read_calls"])
    for key in ("oracle_walk_and_hash_ms", "isolated_oracle_process_ms"):
        proofs.finite(value[key])


def validate_view(view, previous):
    proofs.shape(view, {"workspace_id", "generation", "snapshot_id", "fd_verified", "dirty_upper_verified", "oracle"})
    for key in ("workspace_id", "generation", "snapshot_id"):
        proofs.exact(view[key], previous["workspace_binding"][key])
    proofs.exact(view["fd_verified"], True)
    proofs.exact(view["dirty_upper_verified"], True)
    validate_oracle(view["oracle"], previous["manifest"], dirty=True)


def full_oracle_walks(profile):
    count = len(common.scenarios(profile))
    return 3 * count + count * (count - 1) // 2


def validate_workload(records, final, profile="smoke"):
    """Bind every full walk, old identity, dirty upper and FD per lane."""
    versions = list(common.scenarios(profile))
    count = len(versions)
    proofs.require(len(records) == count and [r["version"] for r in records] == versions)
    walks = 0
    for index, record in enumerate(records):
        result = record["result"]
        proofs.exact(result["version"], record["version"])
        proofs.exact(result["round"], record["round"])
        proofs.exact(result["git"]["commit"], record["fixed_commit"])
        for key in ("workspace_id", "generation", "snapshot_id"):
            proofs.exact(result["actual_status"][key], record["workspace_binding"][key])
        validate_oracle(result["scorpio"]["oracle"], record["manifest"])
        validate_oracle(result["git"]["oracle"], record["manifest"], git=True)
        walks += 2
        proofs.require(type(result["old_views"]) is list and len(result["old_views"]) == index)
        for old, previous in zip(result["old_views"], records[:index]):
            validate_view(old, previous)
            walks += 1
    proofs.shape(final, {"retained", "verified", "views", "final_retained_view_audit_ms"})
    proofs.exact(final["retained"], count)
    proofs.exact(final["verified"], True)
    proofs.require(type(final["views"]) is list and len(final["views"]) == count)
    proofs.finite(final["final_retained_view_audit_ms"])
    for view, previous in zip(final["views"], records):
        validate_view(view, previous)
        walks += 1
    proofs.require(walks == full_oracle_walks(profile))
    return walks


def cleanup_receipts(root, deadline):
    backends._check(deadline)
    root = backends._real_path(root)
    if mounts_under(root):
        raise AssertionError("campaign client still has native mounts")
    result = {}
    for name in ("owned-workspace-daemon.json", "owned-workspace-worker.json"):
        path = root / name
        if path.is_symlink() or not path.is_file() or path.stat().st_nlink != 1 or path.stat().st_size > 4096:
            raise AssertionError("campaign cleanup receipt is missing or substituted")
        value = observation.parse(path.read_bytes())
        proofs.shape(value, {"pid", "starttime", "cleanup_complete"})
        proofs.integer(value["pid"], positive=True)
        proofs.require(type(value["starttime"]) is str and re.fullmatch(r"[1-9][0-9]*", value["starttime"]))
        proofs.exact(value["cleanup_complete"], True)
        if budgets.group_members(value["pid"], value["starttime"]):
            raise AssertionError("owned campaign client process group is not empty")
        result[name] = {"record": value, "sha256": backends._digest(path)}
    backends._check(deadline)
    return result


def publication(owner, fixture, commit, tree, previous, deadline):
    before = owner.verify_runtime(deadline)
    proofs.exact(before.identity, previous.identity)
    started, started_utc = time.monotonic(), utc_now()
    common.git(fixture, deadline, "push", "--no-thin", owner.git_url,
               f"{commit}:refs/heads/main", env=owner.git_env)
    pushed = time.monotonic()
    after = owner.verify_runtime(deadline)
    proofs.exact(after.identity["project_commit"], commit)
    proofs.exact(after.identity["project_tree"], tree)
    common.validate_native(after.native, after.identity, owner.instance_id, True, previous.identity)
    visible = time.monotonic()
    return after, {"started_utc": started_utc, "visible_utc": utc_now(),
                   "started_monotonic": started, "visible_monotonic": visible}, (pushed - started) * 1000


def lane_record(entry, closed, expected_sources):
    capture, binding, result = entry["capture"], entry["workspace_binding"], entry["result"]
    runtime = capture.evidence["runtime"]
    record = {"revision": 1, "phase": entry["phase"], "round": entry["round"], "client": entry["client"],
        "version": int(entry["version"][1:]),
        "fixed_commit": entry["fixed_commit"], "path_tree": entry["path_tree"],
        "oracle_manifest_sha256": entry["oracle_manifest_sha256"], "sources": expected_sources,
        "identity_rows": runtime["identity_rows"], "identity": runtime["identity"],
        "native_publication": runtime["native"],
        "workspace_binding": binding, "workspace_binding_sha256": proofs.digest(binding),
        "projection_sink": closed, "publication": entry["publication"],
        "timings_ms": {"git_push_ms": entry["git_push_ms"],
            "publication_visible_ms": (entry["publication"]["visible_monotonic"] - entry["publication"]["started_monotonic"]) * 1000,
            **{key: result["scorpio"][key] for key in ("metadata_ready_ms", "durable_complete_ms", "durable_verified_ms")},
            "git_verified_ms": result["git"]["verified_ms"]}}
    record["native_proof_sha256"] = proofs.digest({key: record[key] for key in ("identity_rows", "identity", "native_publication")})
    return proofs.validate_lane_measurement(record, capture)


def prepare_history(fixture, round_root, profile, number, seed, deadline, *, fixture_round):
    """Build the real ten-commit chain before either lane starts measuring."""
    prepared, receipts, previous = {}, [], None
    for version in common.scenarios(profile):
        commit, tree = common.create_version(fixture, fixture_round, version, profile, deadline)
        expected = common.expected_manifest(fixture, commit, deadline)
        common.fixture_size.validate_manifest(expected)
        receipt = common.commit_receipt(fixture, commit, version, expected, previous, deadline)
        proofs.exact(receipt["parent"], receipts[-1]["commit"] if receipts else seed)
        receipts.append(receipt)
        write_json(round_root / f"{version}-expected.json", expected, deadline)
        prepared[version] = (commit, tree)
        previous = expected
    write_json(round_root / "git-history.json", {"revision": 1, "profile": profile,
        "round": number, "seed_commit": seed, "commits": receipts}, deadline)
    return prepared


def run_phase(group, clients, sources, phase, number, deadline, harness):
    owners, active, records, validated = [], [], [], []
    path = group.root / "measurements" / phase
    round_root = path / f"round-{number:02}"
    round_root.mkdir(mode=0o700)
    try:
        if phase == "fair":
            owners = list(group.start_pair(number, deadline))
        else:
            owner = group.admit("diagnostic", 1, "b", deadline)
            owners.append(owner)
            owner.start(deadline)
        seed = canonical_seed(group, owners[0], deadline)
        current = {}
        for owner in owners:
            if owner.initial_commit != seed["parent"]:
                raise AssertionError("independent backend canonical parent mismatch")
            current[owner.client] = owner.publish_seed(group.root / "canonical-seed", seed["parent"], seed["commit"], seed["tree"], deadline)
            status, payloads = owner.projection_collector.snapshot()
            if status["written_records"] != 0 or payloads:
                raise AssertionError("fresh backend unexpectedly resolved a measured projection")
        if phase == "fair":
            backends.assert_isolated(current["a"], current["b"])
        fixture = round_root / "fixture"
        common.command(["git", "clone", "--no-hardlinks", "--no-checkout", "--single-branch", "--branch", "main",
                        str(group.root / "canonical-seed"), str(fixture)], deadline)
        scenarios = common.scenarios(group.options.profile)
        prepared = (prepare_history(fixture, round_root, group.options.profile, number, seed["commit"], deadline,
                                    fixture_round=number if phase == "fair" else 4)
                    if group.options.profile == "history-large" else None)
        for client, owner in zip(clients, owners):
            lane_root = round_root / ("client-" + client.label)
            lane_root.mkdir(mode=0o700)
            lane = SimpleNamespace(client=client, backend=owner, root=lane_root,
                daemon=None, worker=None, resources=None, entries=[])
            active.append(lane)
            builds.validate(client, deadline)
            common.command(["git", "init", "--bare", str(lane_root / "git.git")], deadline)
            mode = builds.read_profile_mode(client, phase == "diagnostic", deadline)
            if phase == "diagnostic" and mode != "enabled":
                raise AssertionError("fixed diagnostic B lacks requested read profiling")
            lane.daemon = WorkspaceDaemon(client.driver, client.driver_sha256, lane_root, owner.base_url,
                owner.pg_env["M2_TOKEN"], str(uuid.uuid4()), common.clean_env(), deadline,
                read_profile=phase == "diagnostic")
            lane.worker = WorkerSession(lane_root, lane.daemon.url, lane.daemon.workspace_root,
                lane_root / "git.git", owner.git_url, owner.git_env, deadline=deadline,
                env=common.clean_env(), daemon_uid=lane.daemon.uid,
                read_profile_mode=mode)
            from workspace_update_resources import ProcessResources
            lane.resources = ProcessResources(lane.daemon.process.pid, lane.daemon.started, lane.daemon.uid).start()
        for scenario_number, version in enumerate(scenarios, 1):
            manifest_path = round_root / f"{version}-expected.json"
            if prepared is not None:
                commit, tree = prepared[version]
                expected = observation.parse(manifest_path.read_bytes())
            else:
                commit, tree = common.create_version(fixture, number if phase == "fair" else 4, version,
                                                     group.options.profile, deadline)
                expected = common.expected_manifest(fixture, commit, deadline)
                common.fixture_size.validate_manifest(expected)
                write_json(manifest_path, expected, deadline)
            manifest_digest = backends._digest(manifest_path)
            ordered = active if (number + scenario_number) % 2 == 0 else list(reversed(active))
            for lane in ordered:
                builds.validate(lane.client, deadline)
                lane.daemon.check_owner(socket_required=True)
                owner = lane.backend
                runtime, published, push_ms = publication(owner, fixture, commit, tree, current[lane.client.label], deadline)
                current[lane.client.label] = runtime
                capture = proofs.capture_lane_runtime(owner, lane.client.receipt, sources[lane.client.label],
                                                     deadline, harness_root=harness)
                side = "scorpio-first" if (number + scenario_number + (0 if lane.client.label == "a" else 1)) % 2 == 0 else "git-first"
                before = measurement.resources_before(lane, deadline)
                operation_started = time.monotonic()
                result = lane.worker.measure(manifest_path, commit, side, version, number, deadline)
                operation_finished = time.monotonic()
                resources = measurement.resources_after(lane, before, deadline)
                if phase == "diagnostic":
                    profiles.validate_evidence(result["scorpio"]["read_profile"])
                elif "read_profile" in result["scorpio"]:
                    raise AssertionError("fair worker unexpectedly enabled diagnostics")
                binding = lane.daemon.binding(result["actual_status"], {
                    "instance_id": owner.instance_id, "namespace_view_id": runtime.identity["namespace_view_id"],
                    "scope": "/project", "publication_sequence": runtime.native["sequence"]}, deadline)
                owner.projection_collector.register(binding, binding["logical_request_id"])
                trace = owner.projection_collector.collect_registered(binding, runtime.native, runtime.identity,
                    binding["logical_request_id"], deadline)
                after = owner.verify_runtime(deadline)
                proofs.exact(after.identity, runtime.identity)
                proofs.exact(after.native, runtime.native)
                builds.validate(lane.client, deadline)
                lane.daemon.check_owner(socket_required=True)
                if backends._digest(manifest_path) != manifest_digest:
                    raise AssertionError("canonical oracle manifest changed during measurement")
                entry = {"phase": phase, "round": number, "version": version, "client": lane.client.label,
                    "fixed_commit": commit, "path_tree": tree, "manifest": expected,
                    "manifest_relative_path": str(manifest_path.relative_to(group.root)).replace("\\", "/"),
                    "oracle_manifest_sha256": manifest_digest, "workspace_binding": binding,
                    "capture": capture, "result": result, "publication": published, "git_push_ms": push_ms,
                    "client_order": [x.client.label for x in ordered], "side_order": side,
                    "operation_started_monotonic": operation_started, "operation_finished_monotonic": operation_finished,
                    "resources": resources, "server_projection": trace}
                lane.entries.append(entry)
        for lane in active:
            final = lane.worker.stop(deadline)
            lane.worker = None
            walks = validate_workload(lane.entries, final, group.options.profile)
            resource_final = lane.resources.close(deadline)
            lane.resources = None
            daemon_status = lane.daemon.finish(deadline)
            lane.daemon = None
            proofs.exact(daemon_status, {"actual_exit_code": 0, "bindings": len(scenarios),
                                        "footer_complete": True, "native_mounts_remaining": 0})
            cleanup = cleanup_receipts(lane.root, deadline)
            lane.backend.finalize_projection(deadline)
            closed = lane.backend.projection_collector.closed_evidence(len(scenarios), deadline)
            for entry in lane.entries:
                proof = lane_record(entry, closed, sources[lane.client.label])
                validated.append(proof)
                record = {key: deepcopy(value) for key, value in entry.items()
                          if key not in ("capture", "result", "manifest")}
                # Full manifests are shared across the two lanes until all
                # original full-byte/retained-view assertions have passed.
                record["manifest"] = entry["manifest"]
                record.update(record="round", revision=2, correctness="PASS",
                    scorpio=entry["result"]["scorpio"], git=entry["result"]["git"], old_views=entry["result"]["old_views"],
                    actual_status=entry["result"]["actual_status"],
                    semantic_provenance=proof.evidence, lane_proof_sha256=proofs.digest(proof.evidence),
                    workspace_sink=daemon_status, workspace_sink_sha256=backends._digest(lane.root / "workspace-observation.jsonl"),
                    cleanup_receipts=cleanup, round_final_retained_views=final,
                    round_daemon_resources=resource_final, lane_full_oracle_walks=walks,
                    lane_publication_to_two_sides_verified_wall_ms=(entry["operation_finished_monotonic"] - entry["publication"]["started_monotonic"]) * 1000,
                    wall_timing_scope="this lane's push start through its Scorpio/Git operations, retention and old audits; excludes final closure",
                    operation_kind="commit-to-new-mounted-view-with-retained-previous-views")
                records.append(record)
            lane.entries.clear()
            lane.backend.stop(group.cleanup_deadline, operation_deadline=deadline)
        if phase == "fair":
            for version_number in range(1, len(scenarios) + 1):
                pair = [p for p in validated if p.evidence["lane"]["version"] == version_number]
                proofs.compare_pair(*pair)
        for record in records:
            append(path / "measurements.jsonl", record, phase, deadline)
            # Preserve the complete original wire record, then retain only
            # compact oracle facts across rounds and for final matrix checks.
            record["manifest"] = ManifestFacts(record["manifest"])
        backends._check(deadline)
        return records
    finally:
        errors = measurement.abort_lanes(active, group.cleanup_deadline)
        for owner in reversed(owners):
            try:
                owner.stop(group.cleanup_deadline)
            except BaseException as error:
                errors.append(error)
        if errors:
            primary = sys.exception()
            if primary is None:
                raise BaseExceptionGroup("isolated phase cleanup incomplete", errors)
            raise BaseExceptionGroup("isolated phase failed and cleanup was incomplete", [primary, *errors]) from None


def validate_matrix(records, phase, profile="smoke"):
    proofs.require(phase in ("fair", "diagnostic"))
    for record in records:
        proofs.require(type(record["round"]) is int and type(record["version"]) is str
                       and type(record["client"]) is str and type(record["phase"]) is str)
    expected = {(phase, r, v, c) for r in (range(1, 4) if phase == "fair" else (1,))
                for v in common.scenarios(profile) for c in (("a", "b") if phase == "fair" else ("b",))}
    keys = [(r["phase"], r["round"], r["version"], r["client"]) for r in records]
    proofs.require(len(keys) == len(expected) and set(keys) == expected)
    for record in records:
        proofs.exact(record["correctness"], "PASS")
        proofs.exact(record["lane_full_oracle_walks"], full_oracle_walks(profile))
        evidence = record["semantic_provenance"]
        proofs.exact(record["lane_proof_sha256"], proofs.digest(evidence))
        proofs.exact(record["fixed_commit"], evidence["semantic"]["fixed_commit"])
        proofs.exact(record["oracle_manifest_sha256"], evidence["semantic"]["oracle_manifest_sha256"])
        if phase == "fair":
            proofs.require("read_profile" not in record["scorpio"])
        else:
            profiles.validate_evidence(record["scorpio"]["read_profile"])
            proofs.exact(record["scorpio"]["read_profile"]["status"], "MEASURED")
    return full_oracle_walks(profile) * (6 if phase == "fair" else 1)


def summaries(records, profile="smoke"):
    result = []
    for version in common.scenarios(profile):
        pairs = []
        for number in range(1, 4):
            pair = {r["client"]: r for r in records if r["round"] == number and r["version"] == version}
            a, b = pair["a"]["scorpio"]["durable_verified_ms"], pair["b"]["scorpio"]["durable_verified_ms"]
            pairs.append({"round": number, "client_order": pair["a"]["client_order"],
                "a_verified_ms": a, "b_verified_ms": b, "b_minus_a_ms": b - a,
                "b_over_a": b / a if a else None,
                "a_git_verified_ms": pair["a"]["git"]["verified_ms"],
                "b_git_verified_ms": pair["b"]["git"]["verified_ms"],
                "git_b_minus_a_ms": pair["b"]["git"]["verified_ms"] - pair["a"]["git"]["verified_ms"],
                "git_b_over_a": pair["b"]["git"]["verified_ms"] / pair["a"]["git"]["verified_ms"]
                                  if pair["a"]["git"]["verified_ms"] else None})
        values = [p["b_minus_a_ms"] for p in pairs]
        result.append({"record": "paired_summary", "version": version, "pairs": pairs,
            "b_minus_a_ms": {"median": statistics.median(values), "min": min(values), "max": max(values)},
            "git_b_minus_a_ms": measurement.sample_summary([p["git_b_minus_a_ms"] for p in pairs], True),
            "b_over_a": measurement.sample_summary([p["b_over_a"] for p in pairs], True)
                         if all(p["b_over_a"] is not None for p in pairs) else "NOT_MEASURED_ZERO_DENOMINATOR",
            "git_b_over_a": measurement.sample_summary([p["git_b_over_a"] for p in pairs], True)
                             if all(p["git_b_over_a"] is not None for p in pairs) else "NOT_MEASURED_ZERO_DENOMINATOR",
            "sample_limit": "three paired samples; median/range only, no p95, significance or general Git superiority"})
    return result


def execute(options):
    execution.bind_options(options)
    fixture_admission = common.fixture_size.admit_backend(options.profile, True)
    if options.profile in ("large", "history-large"):
        root, _ = ci.hosted_root(options.run_root)
        fixture_admission["campaign_disk"] = common.fixture_size.admit_campaign_disk(options.profile, root)
    pair = builds.comparison_pair(options.baseline_sha, options.candidate_sha)
    if (not options.paired or options.rounds != 3 or not options.projection_traces
            or options.workspace_read_profile or options.recover_original_window):
        raise ValueError("isolated campaign requires fair paired three rounds and a separate mandatory B diagnostic")
    if (type(options.bootstrap_commit_time) is not int or not 0 <= options.bootstrap_commit_time <= (1 << 32) - 1
            or re.fullmatch(r"[0-9a-f]{40}", options.harness_sha or "") is None):
        raise ValueError("immutable harness and canonical bootstrap time are required")
    budget = budgets.from_options(options)
    if type(budget) is not budgets.IsolatedCampaignBudget:
        raise ValueError("isolated campaign needs its parent phase budget")
    original = projection.window_anchor(options.session_started_utc, options.session_deadline_utc, admission=False)
    if budget.cleanup_deadline > original + 1:
        raise ValueError("campaign cannot move its dispatch anchor")
    deadline = budget.stage_deadline("setup")
    clients = builds.clients(options, deadline)
    if [client.build["source_sha"] for client in clients] != pair:
        raise AssertionError("campaign A and B differ from the requested immutable comparison")
    server = builds.load(options.server_build_receipt, "server", deadline)
    if (server.build["source_sha"] != options.mega_sha or server.driver != Path(options.mega_binary).resolve(strict=True)
            or server.build["source"] != str(Path(options.mega_source).resolve(strict=True))
            or any(server.build[key] != clients[0].build[key] for key in ("rustc_version", "cargo_version"))):
        raise AssertionError("server actual build receipt differs from the common toolchain and fixed source")
    harness = Path(__file__).resolve().parents[2]
    sources = {client.label: source_expectations(options, client, deadline, harness) for client in clients}
    for source in sources.values():
        proofs.exact(source["server_binary_sha256"], server.driver_sha256)
        proofs.exact(source["server_cargo_lock_sha256"], server.build["cargo_lock_sha256"])
    # Preparation is inside setup; actual per-round startup/seed remains in its
    # 25-minute lifetime. Pulling dependency layers is not a timed cache flush.
    group = backends.BackendGroup(options, budget)
    execution_context = execution.identity()
    execution_metadata = execution.metadata_fields(execution_context)
    group.seed = None
    started = time.monotonic()
    fair, diagnostic = [], []
    try:
        measurements = group.root / "measurements"
        measurements.mkdir(mode=0o700)
        for phase in ("fair", "diagnostic"):
            (measurements / phase).mkdir(mode=0o700)
            append(measurements / phase / "measurements.jsonl", {
                "record": "environment", "revision": 2, "profile": options.profile,
                "fixture_admission": fixture_admission,
                "rounds": 3 if phase == "fair" else 1, "sources": sources,
                "architecture": "workspace-v3", "publication_mode": "native",
                "runner_os": platform.system(), "runner_kernel_release": platform.release(),
                "runner_machine": platform.machine(), "runner_logical_cpus": os.cpu_count(),
                "run_id": execution_context["run_id"], "run_attempt": execution_context["attempt"],
                **execution_metadata,
                "backend_isolation": "fresh separately owned PG/Redis/Local object storage/process/cache per lane and round",
                "cache_conditions": "host page caches, CPU scheduling and dependency image layers uncontrolled; no host cache flush",
                "session_started_utc": options.session_started_utc, "session_deadline_utc": options.session_deadline_utc,
                "cleanup_deadline_monotonic": budget.cleanup_deadline,
                "read_profile_scope": "warm current full oracle after durable hydrate only" if phase == "diagnostic" else "disabled",
                "server_build": server.build,
                "unexposed_measurements": ["transport_body_bytes", "transport_request_counts", "PSS", "kernel_copy", "whole_Git_oracle_group_RSS"]}, phase, deadline)
        # Pull each required image once without starting any service or using
        # an unowned project. Every later container is owned by BackendGroup.
        compose = ci.dependencies(Path(options.mega_source), group.project + "-preparation",
                                  {name: 1 for name in ("postgres", "redis", "rustfs", "http")}, deadline)
        for image in sorted({service["image"] for service in compose["services"].values()}):
            common.command(["docker", "image", "pull", image], deadline)
        backends._check(deadline)
        for number in range(1, 4):
            phase_deadline = budget.round_deadline(number)
            fair.extend(run_phase(group, clients, sources, "fair", number, phase_deadline, harness))
            budget.close_phase(phase_deadline)
        phase_deadline = budget.diagnostic_deadline()
        diagnostic = run_phase(group, [clients[1]], sources, "diagnostic", 1, phase_deadline, harness)
        budget.close_phase(phase_deadline)
        deadline = budget.report_deadline()
        builds.validate(server, deadline)
        for client in clients:
            builds.validate(client, deadline)
            proofs.exact(source_expectations(options, client, deadline, harness), sources[client.label])
        fair_walks = validate_matrix(fair, "fair", options.profile)
        diagnostic_walks = validate_matrix(diagnostic, "diagnostic", options.profile)
        for row in summaries(fair, options.profile):
            append(measurements / "fair/measurements.jsonl", row, "fair", deadline)
        backends._check(deadline)
        cleanup_limit = budget.cleanup_stage_deadline()
        group.close()
        cleanup = verify_cleanup_from_disk(options, operation_deadline=cleanup_limit)
        proofs.exact(cleanup["owners"], 7)
        proofs.require({(item["owner"]["phase"], item["owner"]["round"], item["owner"]["client"])
                        for item in cleanup["inventory"]} == expected_owner_keys())
        for phase, records, walks in (("fair", fair, fair_walks), ("diagnostic", diagnostic, diagnostic_walks)):
            append(measurements / phase / "measurements.jsonl", {
                "record": "complete", "revision": 2, "correctness": "PASS", "round_scenarios": len(records),
                "full_oracle_walks": walks, "workspace_daemons": 6 if phase == "fair" else 1,
                "campaign_cleanup_complete": True}, phase, cleanup_limit)
            profiles.validate_artifact_tree(measurements / phase, diagnostic_required=phase == "diagnostic")
        campaign = {"revision": 1, "record": "isolated_campaign_complete", "correctness": "PASS",
            **execution_metadata,
            "session_started_utc": options.session_started_utc, "session_deadline_utc": options.session_deadline_utc,
            "cleanup_deadline_monotonic": budget.cleanup_deadline, "sources": sources, "canonical_seed": group.seed,
            "server_build": server.build,
            "server_build_receipt_sha256": backends._digest(Path(options.server_build_receipt)),
            "fair_records": len(fair), "diagnostic_records": len(diagnostic), "fair_full_oracle_walks": fair_walks,
            "diagnostic_full_oracle_walks": diagnostic_walks, "cleanup": cleanup,
            "phase_measurements_sha256": {phase: common.fixture_size.file_sha256(
                                              measurements / phase / "measurements.jsonl", cleanup_limit)
                                          for phase in ("fair", "diagnostic")},
            "elapsed_execution_seconds": time.monotonic() - started,
            "performance_claims": "NOT_EVALUATED; raw fair evidence and independent diagnostic only"}
        write_json(group.root / "campaign.json", campaign, cleanup_limit)
        return campaign
    finally:
        primary = sys.exception()
        try:
            group.close()
        except BaseException as cleanup_error:
            if primary is None:
                raise
            raise BaseExceptionGroup("isolated campaign failed and cleanup was incomplete",
                                     [primary, cleanup_error]) from None


def verify_cleanup_from_disk(options, *, operation_deadline=None):
    """Verify recorded owners; persisted PID text never authorizes signalling."""
    root, project = ci.hosted_root(options.run_root)
    budget = budgets.from_options(options)
    deadline = budget.cleanup_deadline if operation_deadline is None else min(budget.cleanup_deadline, operation_deadline)
    backends._check(deadline)
    path = root / "backend-owners.json"
    if not path.exists():
        return {"owners": 0, "closed": True, "not_started": True}
    backends._real_path(path)
    value = observation.parse(path.read_bytes())
    proofs.shape(value, {"revision", "project", "session_deadline_utc", "cleanup_deadline_monotonic",
                         "measurement_deadline_monotonic", "backends", "closed"})
    proofs.exact(value["revision"], 1)
    proofs.exact(value["project"], project)
    proofs.exact(value["session_deadline_utc"], budget.deadline_utc)
    proofs.exact(value["cleanup_deadline_monotonic"], budget.cleanup_deadline)
    proofs.exact(value["measurement_deadline_monotonic"], budget.measurement_deadline)
    proofs.exact(value["closed"], True)
    owners = value["backends"]
    proofs.require(type(owners) is list and len(owners) <= 7)
    keys = set()
    inventory = []
    for owner in owners:
        proofs.shape(owner, {"phase", "round", "client", "project", "database", "instance_id", "root",
            "state", "operation_deadline_monotonic", "service_pid", "service_starttime", "initial_path_commit"})
        key = (owner["phase"], owner["round"], owner["client"])
        proofs.require(type(owner["round"]) is int and type(owner["phase"]) is str and type(owner["client"]) is str)
        proofs.require(key not in keys and key in {*(('fair', n, c) for n in range(1, 4) for c in ('a', 'b')), ('diagnostic', 1, 'b')})
        keys.add(key)
        leaf = root / "backends" / f"{owner['phase']}-r{owner['round']:02}-{owner['client']}"
        proofs.exact(owner["root"], str(leaf))
        proofs.exact(owner["project"], project + "-" + leaf.name)
        proofs.exact(owner["state"], "retired")
        if owner["service_pid"] is not None and budgets.group_members(owner["service_pid"], owner["service_starttime"]):
            raise AssertionError("owned campaign backend process group is not empty")
        for argv in (["docker", "ps", "-aq", "--filter", "label=com.docker.compose.project=" + owner["project"]],
                     ["docker", "network", "ls", "-q", "--filter", "name=^" + owner["project"] + "-network$"]):
            if common.command(argv, deadline).strip():
                raise AssertionError("campaign cleanup left owned dependency resources")
        client_root = root / "measurements" / owner["phase"] / f"round-{owner['round']:02}" / ("client-" + owner["client"])
        receipt = cleanup_receipts(client_root, deadline)
        inventory.append({"owner": owner, "client_cleanup": receipt,
                          "dependency_containers_remaining": 0, "dependency_networks_remaining": 0})
    if mounts_under(root):
        raise AssertionError("campaign cleanup left owned native mounts")
    backends._check(deadline)
    return {"owners": len(owners), "closed": True, "inventory": inventory,
            "backend_owners_sha256": backends._digest(path)}


def expected_owner_keys():
    return {*(('fair', number, client) for number in range(1, 4) for client in ('a', 'b')),
            ('diagnostic', 1, 'b')}
