"""Real publication to shipped v3 workspace, with retained old views and Git."""

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import re
import sys
import time
import uuid
import statistics
from types import SimpleNamespace
from itertools import product

import commit_update_bench as common
import commit_update_budget as budget_module
from commit_update_projection import ProjectionCollector, WORK_FIELDS
from workspace_update_daemon import WorkspaceDaemon, file_digest
from workspace_update_worker import WorkerSession
import workspace_update_build as builds
import workspace_update_profile as read_profile


def sample_summary(values, paired):
    if paired:
        return {"median": statistics.median(values), "min": min(values), "max": max(values)}
    return {"p50": common.percentile(values, 50), "p95": common.percentile(values, 95)}


def validate_matrix(records, rounds, labels):
    expected = {(number, version, label) for number in range(1, rounds + 1)
                for version in common.SCENARIOS for label in labels}
    keys = [(r["round"], r["version"], r["client"]) for r in records]
    if len(keys) != len(expected) or set(keys) != expected:
        raise AssertionError("the complete requested client/scenario matrix is required")
    for number in range(1, rounds + 1):
        for version in common.SCENARIOS:
            pair = [r for r in records if r["round"] == number and r["version"] == version]
            first = pair[0]
            for record in pair[1:]:
                if any(record[key] != first[key] for key in (
                        "fixed_commit", "identity", "native_publication", "oracle_manifest_sha256",
                        "publication_started_monotonic", "client_order")):
                    raise AssertionError("paired clients did not measure the same publication and oracle")


def abort_lanes(active, deadline):
    errors = []
    for lane in reversed(active):
        # Each cleanup is attempted even if the previous owner fails.
        for name, method in (("resources", "close"), ("worker", "abort"), ("daemon", "abort")):
            owner = getattr(lane, name, None)
            if owner is not None:
                try:
                    getattr(owner, method)(deadline)
                    setattr(lane, name, None)
                except BaseException as error:
                    errors.append(error)
    return errors


def resources_before(lane, deadline):
    if lane.resources is None:
        return None
    disk = lane_disk(lane, deadline)
    return {"disk": disk, "process": lane.resources.snapshot(deadline)}


def resources_after(lane, before, deadline):
    if before is None:
        return {"daemon_RSS": "NOT_EXPOSED", "daemon_io": "NOT_EXPOSED", "disk": "NOT_EXPOSED"}
    from workspace_update_resources import io_delta
    after = lane.resources.snapshot(deadline)
    return {"daemon_process": after, "daemon_io_delta": io_delta(before["process"], after),
            "disk_before": before["disk"],
            "disk_after": lane_disk(lane, deadline),
            "timing_scope": "daemon interval includes both side operations and retained audits; disk outside side timers",
            "worker_oracle_group_RSS": "NOT_EXPOSED"}


def lane_disk(lane, deadline):
    from workspace_update_resources import disk_usage
    seen = set()
    return {name: disk_usage(path, deadline=deadline, seen=seen if name.startswith("git_") else None)
            if path.exists() or path.is_symlink() else "NOT_CREATED"
            for name, path in (("scorpio_store", lane.daemon.store), ("git_odb", lane.root / "git.git"),
                               ("git_worktrees", lane.root / "git-worktrees"))}


def execute(options):
    fixture_admission = common.fixture_size.admit_backend(options.profile, False)
    if (not options.isolated_deployment or options.publication_mode != "native"
            or not getattr(options, "projection_traces", False)
            or not callable(getattr(options, "finalize_projection", None))
            or not callable(getattr(options, "finalize_campaign", None))):
        raise ValueError("v3 measurement requires the owned native publication runner, both sinks and campaign cleanup")
    common.endpoint_pair(options.base_url, options.git_url)
    if (not re.fullmatch(r"mst2_bench_[0-9a-f]{32}", options.database)
            or str(uuid.UUID(options.instance_id)) != options.instance_id
            or not uuid.UUID(options.instance_id).int
            or not re.fullmatch(r"[0-9a-f]{40}", options.expect_initial_commit)):
        raise ValueError("v3 measurement requires a fixed isolated native identity")
    budget = budget_module.from_options(options)
    clients = builds.clients(options, budget.measurement_deadline)
    diagnostic = getattr(options, "workspace_read_profile", False)
    if type(diagnostic) is not bool:
        raise ValueError("read profiling requires an explicit boolean opt-in")
    profile_modes = ({client.label: builds.read_profile_mode(client, True, budget.measurement_deadline)
                      for client in clients} if diagnostic else {})
    paired = len(clients) == 2
    started = time.monotonic()
    measurement_limit = min(budget.measurement_deadline, started + options.deadline_seconds)
    deadline = measurement_limit
    owner = common.service_binding(options)
    projection = ProjectionCollector(owner["projection_cache"])
    root = options.run_root.parent.resolve(strict=True) / options.run_root.name
    checkout = Path(__file__).resolve().parents[2]
    if root.exists() or root.is_relative_to(checkout):
        raise ValueError("measurement root must be a new owned directory outside the checkout")
    token, git_token = os.environ.get("M2_TOKEN"), os.environ.get("M2_GIT_TOKEN")
    if not token or not git_token:
        raise ValueError("owned MST/2 and Git authentication are required")
    git_env = common.clean_env({"GIT_CONFIG_COUNT": "3", "GIT_CONFIG_KEY_0": "http.extraHeader",
                               "GIT_CONFIG_VALUE_0": "Authorization: Bearer " + git_token,
                               "GIT_CONFIG_KEY_1": "http.followRedirects", "GIT_CONFIG_VALUE_1": "false",
                               "GIT_CONFIG_KEY_2": "credential.helper", "GIT_CONFIG_VALUE_2": ""})

    def tip():
        raw = common.command(["git", "ls-remote", options.git_url, "refs/heads/main"], deadline, env=git_env)
        rows = raw.decode().splitlines()
        if len(rows) != 1 or rows[0].split()[1] != "refs/heads/main":
            raise AssertionError("isolated service has an ambiguous project main ref")
        return rows[0].split()[0]

    if tip() != options.expect_initial_commit:
        raise AssertionError("initial isolated commit moved before workload mutation")
    rows = common.query(common.IDENTITY_SQL, deadline)
    initial = next(row for row in rows if row["path"] == "/project")
    identity = common.validate_identity(rows, options.expect_initial_commit, initial["tree"], options.database)
    common.validate_native(common.query(common.NATIVE_SQL, deadline), identity, options.instance_id, False)
    root.mkdir(mode=0o700)
    fixture = root / "fixture"
    common.command(["git", "clone", "--no-checkout", "--single-branch", "--branch", "main",
                    options.git_url, str(fixture)], deadline, env=git_env)
    if common.git(fixture, deadline, "rev-parse", "HEAD").decode().strip() != options.expect_initial_commit:
        raise AssertionError("independent fixture clone differs from the fixed native commit")
    records, daemon_statuses = [], []
    current = options.expect_initial_commit
    output = root / "measurements.jsonl"

    def emit(record):
        if time.monotonic() >= deadline:
            raise TimeoutError("benchmark evidence exceeded its original stage deadline")
        if diagnostic and record.get("record") == "round":
            read_profile.validate_evidence(record["scorpio"]["read_profile"])
        payload = json.dumps(record, sort_keys=True)
        with output.open("a", encoding="utf-8") as stream:
            previous_size = stream.tell()
            try:
                stream.write(payload + "\n")
                stream.flush()
                if time.monotonic() >= deadline:
                    raise TimeoutError("benchmark evidence write exceeded its original stage deadline")
                print(payload, flush=True)
                if time.monotonic() >= deadline:
                    raise TimeoutError("benchmark evidence output exceeded its original stage deadline")
            except BaseException:
                # Remove an incomplete or late append from safe file evidence;
                # the CI caller persists a closed failure record for the run.
                stream.truncate(previous_size)
                stream.flush()
                raise

    environment = {"record": "environment", "profile": options.profile, "rounds": options.rounds,
          "fixture_admission": fixture_admission,
          "runner_os": platform.system(), "runner_kernel_release": platform.release(),
          "runner_machine": platform.machine(), "runner_logical_cpus": os.cpu_count(),
          "scenarios": common.SCENARIOS,
          "architecture": "workspace-v3", "publication_mode": "native", "service_binding": owner,
          "instrumentation_mode": "shipped-workspace-and-typed-projection-sinks",
          "comparison_mode": "paired" if paired else "single",
          "clients": [{"label": client.label, "binary_sha256": client.driver_sha256,
                       "build": client.build} for client in clients],
          "scorpio_binary_sha256": clients[0].driver_sha256 if not paired else "NOT_APPLICABLE",
          "runner_sha256": file_digest(Path(__file__)),
          "worker_sha256": file_digest(checkout / "tests/mst2-e2e/workspace_update_worker.py"),
          "oracle_sha256": file_digest(checkout / "tests/mst2-e2e/workspace_update_oracle.py"),
          "git_version": common.command(["git", "--version"], deadline).decode().strip(),
          "cache_conditions": "each client has fresh daemon/store/CAS/domain and Git bare ODB per round, shared only across its own updates; OS and shared server cache uncontrolled; client and side order alternate",
          "shared_server_order_cost": "first resolve may initialize projection/cache; both traces and order are retained; second client publication delay includes prior client measurement",
          "fixture_content": "distinct deterministic 32-byte blocks repeated to source-like synthetic file lengths",
          "git_baseline": "real cold clone --depth=1 with checkout; incremental fetch into the same ODB and new detached worktrees; previous checkouts retained; no Git dirty sentinel/files are created",
          "git_durability_synchronization": "NOT_MEASURED; no additional fsync imposed on the default checkout baseline",
          "oracle_timing_scope": "oracle_walk_and_hash_ms covers namespace walk, metadata checks, file reads, SHA256 and path revalidation; isolated_oracle_process_ms additionally includes child startup/imports, manifest read/hash/parse and result IPC; both remain inside full-side verified times",
          "resource_measurement_scope": "sampled RSS peak is each operation interval; VmHWM is the daemon round lifetime high-water mark; idle sampled intervals are not retained; store disk is aggregate and excludes mounted views",
          "unexposed_measurements": ["transport_request_counts", "transport_body_bytes", "weighted_byte_budget",
                                     "worker_oracle_group_RSS", "active_process_count", "PSS", "idle_sampled_RSS_intervals",
                                     "CAS_disk_breakdown", "stage_RSS"] + ([] if paired else ["daemon_RSS", "disk_growth"]),
          "started_utc": datetime.now(timezone.utc).isoformat()}
    if diagnostic:
        environment.update(
            measurement_interpretation="INSTRUMENTED_DIAGNOSTIC_NOT_FREE_PERFORMANCE_BASELINE",
            performance_comparison_allowed=False,
            workspace_read_profile_modes=profile_modes,
            read_profile_scope="current full oracle only; excludes create, hydration, retain_view and old-view audits; checkpoint interval may include boundary-active work; cumulative phases and workers overlap across threads",
            read_profile_timing_scope="before checkpoint precedes oracle timer, after checkpoint follows verified endpoint; original cumulative wall timers are raw and are not reduced by checkpoint overhead; profiler cost remains in reads; instrumented A/B and Git ratios cannot establish uninstrumented performance",
        )
    emit(environment)

    for round_number in range(1, options.rounds + 1):
        deadline = min(budget.round_deadline(round_number), measurement_limit)
        group = root / f"round-{round_number:02}"
        group.mkdir(mode=0o700)
        active = []
        round_records = []
        try:
            for client in clients:
                lane_root = group / ("client-" + client.label) if paired else group
                if paired:
                    lane_root.mkdir(mode=0o700)
                lane = SimpleNamespace(client=client, root=lane_root, daemon=None, worker=None, resources=None)
                active.append(lane)
                builds.validate(client, deadline)
                git_store = lane_root / "git.git"
                common.command(["git", "init", "--bare", str(git_store)], deadline)
                lane.daemon = WorkspaceDaemon(client.driver, client.driver_sha256, lane_root, options.base_url,
                                             token, str(uuid.uuid4()), common.clean_env(), deadline,
                                             **({"read_profile": True} if profile_modes.get(client.label) == "enabled" else {}))
                lane.worker = WorkerSession(lane_root, lane.daemon.url, lane.daemon.workspace_root, git_store,
                                            options.git_url, git_env, deadline=deadline,
                                            env=common.clean_env(), daemon_uid=lane.daemon.uid,
                                            **({"read_profile_mode": profile_modes[client.label]} if diagnostic else {}))
                if paired:
                    from workspace_update_resources import ProcessResources
                    lane.resources = ProcessResources(lane.daemon.process.pid, lane.daemon.started,
                                                      lane.daemon.uid).start()
            for number, version in enumerate(common.SCENARIOS, 1):
                for lane in active:
                    builds.validate(lane.client, deadline)
                    lane.daemon.check_owner(socket_required=True)
                if common.service_binding(options) != owner or tip() != current:
                    raise AssertionError("service or current commit changed before publication")
                with common.phase("fixture_and_git_oracle"):
                    commit, tree = common.create_version(fixture, round_number, version,
                                                         options.profile, deadline)
                    expected = common.expected_manifest(fixture, commit, deadline)
                    manifest_raw, _ = common.fixture_size.validate_manifest(expected)
                expected_path = group / f"{version}-expected.json"
                expected_path.write_bytes(manifest_raw)
                expected_digest = file_digest(expected_path)
                publish_start = time.monotonic()
                with common.phase("git_publication_push"):
                    common.git(fixture, deadline, "push", "--no-thin", options.git_url,
                               f"{commit}:refs/heads/main", env=git_env)
                push_ms = (time.monotonic() - publish_start) * 1000
                previous = identity
                with common.phase("updated_publication_identity"):
                    identity = common.validate_identity(common.query(common.IDENTITY_SQL, deadline),
                                                        commit, tree, options.database)
                    native = common.query(common.NATIVE_SQL, deadline)
                    common.validate_native(native, identity, options.instance_id, True, previous)
                visible_ms = (time.monotonic() - publish_start) * 1000
                current = commit
                ordered = active if (round_number + number) % 2 == 0 else list(reversed(active))
                measured = []
                for lane in ordered:
                    builds.validate(lane.client, deadline)
                    lane.daemon.check_owner(socket_required=True)
                    side_order = "scorpio-first" if (round_number + number + clients.index(lane.client)) % 2 == 0 else "git-first"
                    before = resources_before(lane, deadline)
                    operation_start = time.monotonic()
                    with common.phase("shipped_workspace_and_git_measurement"):
                        result = lane.worker.measure(expected_path, commit, side_order, version, round_number, deadline)
                        if diagnostic:
                            evidence = result["scorpio"].get("read_profile")
                            read_profile.validate_evidence(evidence)
                            if ((profile_modes[lane.client.label] == "enabled" and evidence["status"] != "MEASURED")
                                    or (profile_modes[lane.client.label] == "unsupported"
                                        and evidence != read_profile.not_measured("unsupported"))):
                                raise read_profile.ProfileError()
                    operation_end = time.monotonic()
                    resources = resources_after(lane, before, deadline)
                    builds.validate(lane.client, deadline)
                    if file_digest(expected_path) != expected_digest:
                        raise AssertionError("paired oracle manifest changed across operations")
                    measured.append((lane, result, side_order, operation_start, operation_end, resources))
                wall_end = time.monotonic()
                bound = []
                for lane, result, side_order, operation_start, operation_end, resources in measured:
                    status = result["actual_status"]
                    binding = lane.daemon.binding(status, {
                        "instance_id": options.instance_id,
                        "namespace_view_id": identity["namespace_view_id"],
                        "scope": "/project", "publication_sequence": native["sequence"],
                    }, deadline)
                    projection.register(binding, binding["logical_request_id"])
                    bound.append((lane, result, side_order, operation_start, operation_end, resources, binding))
                for lane, result, side_order, operation_start, operation_end, resources, binding in bound:
                    # Sink reads and server acknowledgement matching consume
                    # the same wall budget, after all immediate timed oracles.
                    trace = projection.collect_registered(binding, native, identity, binding["logical_request_id"], deadline)
                    after = common.validate_identity(common.query(common.IDENTITY_SQL, deadline), commit, tree, options.database)
                    if after != identity or tip() != current or common.service_binding(options) != owner:
                        raise AssertionError("fixed commit or native service changed across measurements")
                    record = {"record": "round", "round": round_number, "version": version,
                          "client": lane.client.label, "client_binary_sha256": lane.client.driver_sha256,
                          "client_source_sha": lane.client.build["source_sha"] if paired else "NOT_EXPOSED",
                          "client_order": [entry.client.label for entry in ordered],
                          "oracle_manifest_sha256": expected_digest,
                          "publication_started_monotonic": publish_start,
                          "client_operation_started_monotonic": operation_start,
                          "client_operation_finished_monotonic": operation_end,
                          "resources": resources,
                          "scenario": common.SCENARIOS[version],
                          "fixed_commit": commit, "identity": identity, "native_publication": native,
                          "git_push_ms": push_ms, "publication_visible_ms": visible_ms,
                          "publication_to_both_sides_verified_wall_ms": (wall_end - publish_start) * 1000,
                          "wall_timing_scope": "common publication push start through all clients and their Git operations; includes earlier client wait, retained audits, resource observations and harness; not single-client product latency",
                          "wall_timing_exclusions": [
                              "projection.collect and sink acknowledgement",
                              "post-measurement identity/ref validation",
                              "final retained-view audit in worker.stop",
                              "workspace/daemon cleanup",
                          ],
                          "scorpio_timing_scope": "workspace create through durable complete status and current full streamed oracle; durable_verified_ms ends at the current oracle and excludes retain_view and old-view audit",
                          "git_timing_scope": "Git fetch through detached worktree creation and full checkout oracle; verified_ms includes fetch and rev-parse",
                          "publication_and_client_metadata_stage_sum_ms": visible_ms + result["scorpio"]["metadata_ready_ms"],
                          "publication_and_client_durable_stage_sum_ms": visible_ms + result["scorpio"]["durable_complete_ms"],
                          "stage_sum_scope": "sum of publication observation and client segments, not a continuous end-to-end wall timer",
                          "side_order": side_order, "scorpio": result["scorpio"], "git": result["git"],
                          "workspace_binding": binding, "server_projection": trace,
                          "old_views": result["old_views"], "fuse_mount": "PASS",
                          "correctness": "PROVISIONAL_PENDING_BOTH_SINK_FINALIZATION"}
                    if diagnostic:
                        record["measurement_interpretation"] = "INSTRUMENTED_DIAGNOSTIC_NOT_FREE_PERFORMANCE_BASELINE"
                        record["performance_comparison_allowed"] = False
                    round_records.append(record)
            for lane in active:
                old_proof = lane.worker.stop(deadline)
                lane.worker = None
                round_resources = lane.resources.close(deadline) if lane.resources is not None else "NOT_EXPOSED"
                lane.resources = None
                daemon_status = lane.daemon.finish(deadline)
                lane.daemon = None
                daemon_statuses.append({"round": round_number, "client": lane.client.label, **daemon_status})
                for record in round_records:
                    if record["client"] == lane.client.label:
                        record["workspace_sink"] = daemon_status
                        record["round_final_retained_views"] = old_proof
                        record["round_daemon_resources"] = round_resources
                        record["final_retained_view_audit_ms"] = old_proof["final_retained_view_audit_ms"]
                        records.append(record)
        finally:
            # Preserve the primary error while still attempting both owners.
            errors = abort_lanes(active, budget.cleanup_deadline)
            if errors and sys.exc_info()[0] is None:
                raise AssertionError("owned v3 round cleanup was incomplete") from None
    validate_matrix(records, options.rounds, [client.label for client in clients])
    deadline = budget.report_deadline()
    options.finalize_projection(deadline)
    projection_status = projection.finish(len(records), deadline)
    if time.monotonic() >= deadline:
        raise TimeoutError("both sinks were not finalized within the original report deadline")
    evidence = list(records)
    for client, version in product(clients, common.SCENARIOS):
        samples = [record for record in records if record["version"] == version and record["client"] == client.label]
        summary = {"record": "summary", "version": version,
                   "client": client.label, "scenario": common.SCENARIOS[version], "samples": len(samples)}
        if diagnostic:
            summary["measurement_interpretation"] = "INSTRUMENTED_DIAGNOSTIC_NOT_FREE_PERFORMANCE_BASELINE"
            summary["performance_comparison_allowed"] = False
        metrics = {
            "publication_visible_ms": [r["publication_visible_ms"] for r in samples],
            "scorpio_metadata_ready_ms": [r["scorpio"]["metadata_ready_ms"] for r in samples],
            "scorpio_durable_complete_ms": [r["scorpio"]["durable_complete_ms"] for r in samples],
            "scorpio_verified_ms": [r["scorpio"]["durable_verified_ms"] for r in samples],
            "scorpio_retain_view_ms": [r["scorpio"]["retain_view_ms"] for r in samples],
            "scorpio_old_view_audit_ms": [r["scorpio"]["old_view_audit_ms"] for r in samples],
            "scorpio_final_retained_view_audit_ms": [r["final_retained_view_audit_ms"] for r in samples],
            "scorpio_side_total_ms": [r["scorpio"]["side_total_ms"] for r in samples],
            "git_verified_ms": [r["git"]["verified_ms"] for r in samples],
            "git_checkout_verified_ms": [r["git"]["checkout_verified_ms"] for r in samples],
        }
        for metric in ("projection_elapsed_micros", *sorted(WORK_FIELDS)):
            metrics[metric] = [r["server_projection"]["payload"][metric] for r in samples]
        for name, values in metrics.items():
            summary[name] = sample_summary(values, paired)
        evidence.append(summary)
    if paired:
        for version in common.SCENARIOS:
            pairs = []
            for number in range(1, options.rounds + 1):
                pair = {r["client"]: r for r in records if r["round"] == number and r["version"] == version}
                a, b = pair["a"]["scorpio"]["durable_verified_ms"], pair["b"]["scorpio"]["durable_verified_ms"]
                pairs.append({"round": number, "client_order": pair["a"]["client_order"],
                              "a_verified_ms": a, "b_verified_ms": b, "b_minus_a_ms": b - a,
                              "b_over_a": b / a if a > 0 else None})
            pair_summary = {"record": "paired_summary", "version": version, "samples": len(pairs), "pairs": pairs,
                             "b_minus_a_ms": sample_summary([p["b_minus_a_ms"] for p in pairs], True),
                             "sample_limit": "three paired samples; report median/range, no reliable p95 or general Git superiority"}
            if diagnostic:
                pair_summary["measurement_interpretation"] = "INSTRUMENTED_DIAGNOSTIC_NOT_FREE_PERFORMANCE_BASELINE"
                pair_summary["performance_comparison_allowed"] = False
            evidence.append(pair_summary)
    if time.monotonic() >= deadline:
        raise TimeoutError("campaign report exceeded its original report deadline")
    options.finalize_campaign(budget.cleanup_deadline)
    # The complete campaign includes owned server/Compose/log cleanup.  Final
    # evidence uses that same admitted cleanup anchor, never a fresh window.
    deadline = budget.cleanup_deadline
    if time.monotonic() >= deadline:
        raise TimeoutError("campaign cleanup exceeded its original deadline")
    for record in records:
        record["correctness"] = "PASS"
    for record in evidence:
        emit(record)
    completion = {"record": "complete", "round_scenarios": len(records), "workspace_daemons": daemon_statuses,
          "projection_writer_status": projection_status, "elapsed_seconds": time.monotonic() - started,
          "campaign_cleanup_complete": True, "correctness": "PASS"}
    if diagnostic:
        completion.update(measurement_interpretation=read_profile.DIAGNOSTIC_INTERPRETATION,
                          performance_comparison_allowed=False)
    emit(completion)
