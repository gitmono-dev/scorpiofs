"""Real publication to shipped v3 workspace, with retained old views and Git."""

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time
import uuid

import commit_update_bench as common
import commit_update_budget as budget_module
from commit_update_projection import ProjectionCollector, WORK_FIELDS
from workspace_update_daemon import WorkspaceDaemon, file_digest
from workspace_update_worker import WorkerSession


def execute(options):
    if (not options.isolated_deployment or options.publication_mode != "native"
            or not getattr(options, "projection_traces", False)
            or not callable(getattr(options, "finalize_projection", None))):
        raise ValueError("v3 measurement requires the owned native publication runner and both sinks")
    common.endpoint_pair(options.base_url, options.git_url)
    common.driver_binding(options)
    if (not re.fullmatch(r"mst2_bench_[0-9a-f]{32}", options.database)
            or str(uuid.UUID(options.instance_id)) != options.instance_id
            or not uuid.UUID(options.instance_id).int
            or not re.fullmatch(r"[0-9a-f]{40}", options.expect_initial_commit)):
        raise ValueError("v3 measurement requires a fixed isolated native identity")
    budget = budget_module.from_options(options)
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
        with output.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, sort_keys=True) + "\n")
        print(json.dumps(record, sort_keys=True), flush=True)

    emit({"record": "environment", "profile": options.profile, "rounds": options.rounds,
          "scenarios": common.SCENARIOS,
          "architecture": "workspace-v3", "publication_mode": "native", "service_binding": owner,
          "instrumentation_mode": "shipped-workspace-and-typed-projection-sinks",
          "scorpio_binary_sha256": file_digest(options.driver),
          "runner_sha256": file_digest(Path(__file__)),
          "worker_sha256": file_digest(checkout / "tests/mst2-e2e/workspace_update_worker.py"),
          "git_version": common.command(["git", "--version"], deadline).decode().strip(),
          "cache_conditions": "fresh Scorpio daemon/store and Git bare ODB per round; shared across updates; OS cache uncontrolled; side order alternates",
          "fixture_content": "distinct deterministic 32-byte blocks repeated to source-like synthetic file lengths",
          "git_baseline": "depth=1 cold fetch; incremental fetch into shared bare ODB; each commit gets a new detached worktree; previous detached worktrees retained; no Git dirty sentinel/files are created",
          "git_durability_synchronization": "NOT_MEASURED; no additional fsync imposed on the default checkout baseline",
          "oracle_timing_scope": "oracle_walk_and_hash_ms covers namespace walk, metadata checks, file reads, SHA256 and path revalidation; isolated_oracle_process_ms additionally includes child startup/imports, manifest read/hash/parse and result IPC; both remain inside full-side verified times",
          "unexposed_measurements": ["transport_request_counts", "transport_body_bytes", "RSS", "weighted_byte_budget", "disk_growth"],
          "started_utc": datetime.now(timezone.utc).isoformat()})

    for round_number in range(1, options.rounds + 1):
        deadline = min(budget.round_deadline(round_number), measurement_limit)
        group = root / f"round-{round_number:02}"
        group.mkdir(mode=0o700)
        git_store = group / "git.git"
        common.command(["git", "init", "--bare", str(git_store)], deadline)
        daemon = worker = None
        round_records = []
        try:
            run_id = str(uuid.uuid4())
            daemon = WorkspaceDaemon(options.driver, options.driver_sha256, group, options.base_url,
                                     token, run_id, common.clean_env(), deadline)
            worker = WorkerSession(group, daemon.url, daemon.workspace_root, git_store,
                                   options.git_url, git_env, deadline=deadline,
                                   env=common.clean_env(), daemon_uid=daemon.uid)
            for number, version in enumerate(common.SCENARIOS, 1):
                common.driver_binding(options)
                daemon.check_owner(socket_required=True)
                if common.service_binding(options) != owner or tip() != current:
                    raise AssertionError("service or current commit changed before publication")
                with common.phase("fixture_and_git_oracle"):
                    commit, tree = common.create_version(fixture, round_number, version,
                                                         options.profile == "smoke", deadline)
                    expected = common.expected_manifest(fixture, commit, deadline)
                expected_path = group / f"{version}-expected.json"
                expected_path.write_text(json.dumps(expected))
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
                side_order = "scorpio-first" if (round_number + number) % 2 == 0 else "git-first"
                with common.phase("shipped_workspace_and_git_measurement"):
                    result = worker.measure(expected_path, commit, side_order, version, round_number, deadline)
                wall_end = time.monotonic()
                status = result["actual_status"]
                binding = daemon.binding(status, {
                    "instance_id": options.instance_id,
                    "namespace_view_id": identity["namespace_view_id"],
                    "scope": "/project", "publication_sequence": native["sequence"],
                }, deadline)
                # Sink reads and server acknowledgement matching consume the
                # original wall budget, after both immediate timed oracles.
                trace = projection.collect(binding, native, identity, binding["logical_request_id"], deadline)
                after = common.validate_identity(common.query(common.IDENTITY_SQL, deadline), commit, tree, options.database)
                if after != identity or tip() != current or common.service_binding(options) != owner:
                    raise AssertionError("fixed commit or native service changed across measurements")
                record = {"record": "round", "round": round_number, "version": version,
                          "scenario": common.SCENARIOS[version],
                          "fixed_commit": commit, "identity": identity, "native_publication": native,
                          "git_push_ms": push_ms, "publication_visible_ms": visible_ms,
                          "publication_to_both_sides_verified_wall_ms": (wall_end - publish_start) * 1000,
                          "wall_timing_scope": "publication push start through both alternated side operations and their immediate full oracles; includes both sides and harness work",
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
                round_records.append(record)
            old_proof = worker.stop(deadline)
            worker = None
            daemon_status = daemon.finish(deadline)
            daemon = None
            daemon_statuses.append(daemon_status)
            for record in round_records:
                record["workspace_sink"] = daemon_status
                record["round_final_retained_views"] = old_proof
                record["final_retained_view_audit_ms"] = old_proof["final_retained_view_audit_ms"]
                records.append(record)
        finally:
            # Preserve the primary error while still attempting both owners.
            errors = []
            if worker is not None:
                try:
                    worker.abort(budget.cleanup_deadline)
                except BaseException as error:
                    errors.append(error)
            if daemon is not None:
                try:
                    daemon.abort(budget.cleanup_deadline)
                except BaseException as error:
                    errors.append(error)
            if errors and sys.exc_info()[0] is None:
                raise AssertionError("owned v3 round cleanup was incomplete") from None
    if len(records) != options.rounds * len(common.SCENARIOS):
        raise AssertionError("the full requested cold/single/rename/batch matrix is required")
    deadline = budget.report_deadline()
    options.finalize_projection(deadline)
    projection_status = projection.finish(len(records), deadline)
    for record in records:
        record["correctness"] = "PASS"
        emit(record)
    for version in common.SCENARIOS:
        samples = [record for record in records if record["version"] == version]
        summary = {"record": "summary", "version": version,
                   "scenario": common.SCENARIOS[version], "samples": len(samples)}
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
            summary[name] = {"p50": common.percentile(values, 50), "p95": common.percentile(values, 95)}
        emit(summary)
    emit({"record": "complete", "round_scenarios": len(records), "workspace_daemons": daemon_statuses,
          "projection_writer_status": projection_status, "elapsed_seconds": time.monotonic() - started,
          "correctness": "PASS"})
