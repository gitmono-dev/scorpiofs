"""Copy a closed allowlist of independent fair/diagnostic evidence, never logs."""

import argparse
import hashlib
import json
import os
from pathlib import Path
from pathlib import PurePosixPath, PureWindowsPath
import re
import stat
from datetime import timedelta

import commit_update_budget as budgets
import workspace_update_backend_proof as proofs
import workspace_update_build as builds
import workspace_update_campaign as campaign
import workspace_update_observation as observation
import workspace_update_profile as profiles
import workspace_update_size as fixture_size
import workspace_update_execution as execution
import workspace_update_git_performance as git_performance
import workspace_update_directory as directory_probe
from workspace_update_size import consume_regular

ROOT_FILES = {"campaign.json", "canonical-seed.json", "backend-owners.json", "failure.json"}
CLIENT_FILES = {"workspace-observation.jsonl", "owned-workspace-daemon.json", "owned-workspace-worker.json"}
OWNER_FIELDS = {"phase", "round", "client", "project", "database", "instance_id", "root", "state",
                "operation_deadline_monotonic", "service_pid", "service_starttime", "initial_path_commit"}
RUN_FIELDS = {"revision", "run_id", "attempt", "harness_sha", "mega_sha", "baseline_sha", "candidate_sha",
              "profile", "rounds", "comparison", "bootstrap_commit_time", "session_started_utc",
              "session_deadline_utc", "cleanup_deadline_monotonic", "owned_root"}
DIRECT_RUN_FIELDS = RUN_FIELDS | execution.DIRECT_FIELDS
GIT_PERFORMANCE_FILES = {"git-performance.jsonl", "git-performance-summary.json"}


def run_metadata_from_env():
    context = execution.identity()
    value = {"revision": 1, "run_id": context["run_id"], "attempt": context["attempt"],
        "harness_sha": os.environ["SCORPIO_SHA"], "mega_sha": os.environ["MEGA_SHA"],
        "baseline_sha": os.environ["BASELINE_SHA"], "candidate_sha": os.environ["CANDIDATE_SHA"],
        "profile": os.environ["PROFILE"], "rounds": int(os.environ["ROUNDS"]), "comparison": os.environ["COMPARISON"],
        "bootstrap_commit_time": campaign.commit_time(os.environ["BOOTSTRAP_COMMIT_TIME"]),
        "session_started_utc": os.environ["STARTED_INPUT"], "session_deadline_utc": os.environ["DEADLINE_INPUT"],
        "cleanup_deadline_monotonic": float(os.environ["MST2_WORK_CLEANUP_DEADLINE_MONOTONIC"]),
        "owned_root": os.environ["MST2_OWNED_ROOT"]}
    if context["execution_provider"] == "github-actions":
        proofs.exact(value["harness_sha"], os.environ["GITHUB_SHA"])
        proofs.exact(value["owned_root"], str(PurePosixPath(os.environ["RUNNER_TEMP"]) / f"mst2-real-{value['run_id']}-{value['attempt']}"))
    else:
        value.update(execution.metadata_fields(context))
        for key in ("owned_root", "session_started_utc", "session_deadline_utc"):
            proofs.exact(value[key], context[key])
    validate_run_metadata(value)
    return value


def validate_run_metadata(value, complete=None):
    direct = type(value) is dict and "execution_provider" in value
    proofs.shape(value, DIRECT_RUN_FIELDS if direct else RUN_FIELDS)
    if direct:
        execution.validate_metadata(value)
    proofs.exact(value["revision"], 1)
    for key in ("run_id", "attempt"):
        proofs.require(type(value[key]) is str and re.fullmatch(r"[1-9][0-9]{0,19}", value[key]))
    for key in ("harness_sha", "mega_sha", "baseline_sha", "candidate_sha"):
        proofs.hex_digest(value[key], 40)
    builds.comparison_pair(value["baseline_sha"], value["candidate_sha"])
    proofs.exact(value["rounds"], 3)
    proofs.exact(value["comparison"], "isolated")
    fixture_size.plan(value["profile"])
    proofs.integer(value["bootstrap_commit_time"], (1 << 32) - 1)
    proofs.finite(value["cleanup_deadline_monotonic"])
    if not direct:
        proofs.require(type(value["owned_root"]) is str and PurePosixPath(value["owned_root"]).is_absolute()
                       and PurePosixPath(value["owned_root"]).name == f"mst2-real-{value['run_id']}-{value['attempt']}")
    proofs.exact(budgets.utc(value["session_deadline_utc"]), budgets.utc(value["session_started_utc"]) + timedelta(minutes=235))
    if complete is not None:
        proofs.exact("execution_provider" in complete, direct)
        if direct:
            for key in execution.DIRECT_FIELDS:
                proofs.exact(value[key], complete[key])
        for key in ("session_started_utc", "session_deadline_utc", "cleanup_deadline_monotonic"):
            proofs.exact(value[key], complete[key])
        proofs.exact(value["bootstrap_commit_time"], complete["canonical_seed"]["bootstrap_commit_time"])
        for label in ("a", "b"):
            proofs.exact(value["harness_sha"], complete["sources"][label]["harness_source_sha"])
            proofs.exact(value["mega_sha"], complete["sources"][label]["server_source_sha"])
            proofs.exact(value["baseline_sha" if label == "a" else "candidate_sha"],
                         complete["sources"][label]["client_source_sha"])
    return value


def allowed(relative):
    parts = Path(relative).parts
    if len(parts) == 1:
        return parts[0] in ROOT_FILES
    if parts[0] != "measurements" or parts[1] not in ("fair", "diagnostic"):
        return False
    if len(parts) == 3:
        return parts[2] in {"measurements.jsonl", "failure.json"}
    if len(parts) not in (4, 5) or re.fullmatch(r"round-0[1-3]", parts[2]) is None:
        return False
    if parts[1] == "diagnostic" and parts[2] != "round-01":
        return False
    if len(parts) == 4:
        return parts[3] == "git-history.json" or re.fullmatch(r"v(?:[1-9]|10)-expected\.json", parts[3]) is not None
    return (parts[3] in (("client-a", "client-b") if parts[1] == "fair" else ("client-b",))
            and parts[4] in CLIENT_FILES)


def read_regular(path, cap=fixture_size.ORACLE_MANIFEST_LIMIT):
    def read(stream):
        raw = stream.read(cap + 1)
        if len(raw) > cap:
            raise AssertionError("safe evidence file changed during read")
        return raw
    return consume_regular(path, cap, read)


def scan_rows(path, consume, *, deadline_utc=None):
    """Hash and validate one bounded line at a time from one pinned inode."""
    def scan(stream):
        digest, count, total = hashlib.sha256(), 0, 0
        while True:
            if deadline_utc is not None:
                budgets.require_external_time(deadline_utc)
            line = stream.readline(fixture_size.EVIDENCE_ROW_LIMIT + 1)
            if not line:
                break
            count += 1
            total += len(line)
            if (not line.endswith(b"\n") or len(line) > fixture_size.EVIDENCE_ROW_LIMIT
                    or total > fixture_size.EVIDENCE_FILE_LIMIT
                    or count > fixture_size.EVIDENCE_RECORD_LIMIT):
                raise AssertionError("campaign evidence stream exceeds bound or is incomplete")
            value = observation.parse(line)
            proofs.require(type(value) is dict)
            digest.update(line)
            consume(value)
        return digest.hexdigest()
    return consume_regular(path, fixture_size.EVIDENCE_FILE_LIMIT, scan)


def rows(path):
    raw = read_regular(path)
    if raw and not raw.endswith(b"\n"):
        raise AssertionError("campaign evidence has an incomplete JSONL line")
    values = []
    for line in raw.split(b"\n")[:-1]:
        if len(line) > fixture_size.EVIDENCE_ROW_LIMIT:
            raise AssertionError("campaign evidence line exceeds bound")
        value = observation.parse(line)
        proofs.require(type(value) is dict)
        values.append(value)
    return raw, values


def git_performance_evidence(path, *, require_complete=False, deadline_utc=None):
    """Validate the actual bounded stream before producing a public summary."""
    if deadline_utc is not None:
        budgets.require_external_time(deadline_utc)
    raw = read_regular(path, git_performance.MAX_BYTES)
    proofs.require(not raw or raw.endswith(b"\n"))
    values = []
    for line in raw.splitlines(keepends=True):
        if deadline_utc is not None:
            budgets.require_external_time(deadline_utc)
        proofs.require(line.endswith(b"\n") and len(line) <= git_performance.MAX_LINE_BYTES)
        proofs.require(len(values) < git_performance.MAX_RECORDS)
        value = observation.parse(line)
        proofs.exact(line, (proofs.canonical(value) + "\n").encode("ascii"))
        values.append(value)
    validated = git_performance.validate_records(values, require_complete=require_complete)
    return raw, git_performance.summarize(validated)


def validate_git_performance_export(root, *, require_complete=False, deadline_utc=None):
    """Recompute every public metric; an uploaded summary is never trusted."""
    root = Path(root)
    _, expected = git_performance_evidence(root / "git-performance.jsonl",
        require_complete=require_complete, deadline_utc=deadline_utc)
    actual = observation.parse(read_regular(root / "git-performance-summary.json", git_performance.MAX_SUMMARY_BYTES))
    proofs.exact(actual, expected)
    return expected


def write_safe(output, relative, raw, bindings):
    """Write only beneath the pinned fresh export directory."""
    target = output / relative
    proofs.require(target.is_relative_to(output) and ".." not in Path(relative).parts)
    for directory in [*reversed(target.parent.parents), target.parent]:
        if directory.is_relative_to(output):
            directory.mkdir(mode=0o700, exist_ok=True)
        info = directory.lstat()
        proofs.require(stat.S_ISDIR(info.st_mode))
        identity = (info.st_dev, info.st_ino)
        if directory in bindings:
            proofs.exact(identity, bindings[directory])
        else:
            bindings[directory] = identity
    parent_fd = None
    fd = None
    try:
        if os.name == "posix":
            flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
            parent_fd = os.open(target.anchor, flags)
            current = Path(target.anchor)
            for part in target.parts[1:-1]:
                child = os.open(part, flags, dir_fd=parent_fd)
                os.close(parent_fd)
                parent_fd = child
                current /= part
                info = os.fstat(parent_fd)
                proofs.exact((info.st_dev, info.st_ino), bindings[current])
            fd = os.open(target.name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent_fd)
        else:
            fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0), 0o600)
        with os.fdopen(fd, "wb", closefd=False) as stream:
            result = raw(stream) if callable(raw) else stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        for directory, identity in bindings.items():
            info = directory.lstat()
            proofs.require(stat.S_ISDIR(info.st_mode))
            proofs.exact((info.st_dev, info.st_ino), identity)
    finally:
        if fd is not None:
            os.close(fd)
        if parent_fd is not None:
            os.close(parent_fd)
    return result


def copy_safe(source, output, relative, bindings, deadline_utc):
    """Copy a potentially large JSONL file without retaining its contents."""
    def write(destination):
        def copy(stream):
            digest, total = hashlib.sha256(), 0
            while True:
                budgets.require_external_time(deadline_utc)
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    return digest.hexdigest()
                total += len(chunk)
                if total > fixture_size.EVIDENCE_FILE_LIMIT:
                    raise AssertionError("campaign evidence copy exceeds bound")
                digest.update(chunk)
                destination.write(chunk)
        return consume_regular(source, fixture_size.EVIDENCE_FILE_LIMIT, copy)
    return write_safe(output, relative, write, bindings)


def workspace_sink(path, records, profile="smoke"):
    raw, values = rows(path)
    count = len(campaign.common.scenarios(profile))
    proofs.require(len(values) == count + 1 and len(records) == count)
    bindings, footer = values[:-1], values[-1]
    for key in ("workspace_id", "generation", "store"):
        proofs.require(len({binding[key] for binding in bindings}) == count)
    proofs.shape(footer, observation.FOOTER_FIELDS)
    proofs.exact(footer["record"], "workspace_observation_footer")
    proofs.exact(footer["revision"], 1)
    for key in ("complete", "producers_closed", "drained"):
        proofs.exact(footer[key], True)
    proofs.exact(footer["daemon_exit_code"], 0)
    proofs.exact(footer["first_error"], None)
    for key in ("accepted_records", "received_records", "written_records"):
        proofs.exact(footer[key], count)
    proofs.exact(footer["written_bytes"], sum(len(line) + 1 for line in raw.split(b"\n")[:-2]))
    for binding, record in zip(bindings, records):
        observation.validate_binding(binding, footer["run_id"])
        proofs.exact(binding, record["workspace_binding"])
        proofs.exact(record["workspace_sink_sha256"], hashlib.sha256(raw).hexdigest())
    return raw


def validate_directory_readiness(record, manifest):
    """Replay directory results against the same fixed manifest and operation."""
    operation_start = proofs.finite(record["operation_started_monotonic"])
    operation_end = proofs.finite(record["operation_finished_monotonic"])
    for side, verified in (("scorpio", "durable_verified_ms"), ("git", "verified_ms")):
        result = record[side]
        directory_probe.validate_record(result["directory_probe"], manifest, git_checkout=side == "git")
        started = proofs.finite(result["operation_started_monotonic"])
        finished = proofs.finite(result["operation_finished_monotonic"])
        ready = proofs.finite(result["directory_ready_ms"])
        verified_ms = proofs.finite(result[verified])
        proofs.require(operation_start <= started <= finished <= operation_end)
        proofs.require(result["directory_probe"]["timings_ms"]["total"] <= ready <= verified_ms
                       <= (finished - started) * 1000)
    scorpio, git = record["scorpio"], record["git"]
    proofs.require(proofs.finite(scorpio["metadata_ready_ms"]) <= scorpio["directory_ready_ms"])
    proofs.exact(git["commit"], record["fixed_commit"])
    proofs.require(proofs.finite(git["fetch_ms"]) <= git["directory_ready_ms"])
    first = record["version"] == "v1"
    proofs.exact(git["baseline_kind"], "shallow-clone" if first else "incremental-fetch-worktree")
    proofs.exact(git["clone_depth"], 1 if first else None)
    proofs.exact(git["repository_is_shallow"], True if first else None)
    proofs.exact(git["head_history_commits"], 1 if first else None)
    if first:
        proofs.require(proofs.finite(git["clone_ms"]) <= git["fetch_ms"])
    else:
        proofs.exact(git["clone_ms"], None)


def validate_saved_lane(root, records, owner, expected_sources, client_build, profile="smoke"):
    entries = []
    for record in records:
        evidence = record["semantic_provenance"]
        proofs.shape(evidence, {"lane", "capture", "semantic", "closed_projection"})
        proofs.exact(proofs.digest(evidence), record["lane_proof_sha256"])
        lane, capture = evidence["lane"], evidence["capture"]
        proofs.exact(evidence, proofs.validate_lane_values(lane, capture))
        proofs.shape(lane, proofs.LANE_FIELDS)
        proofs.shape(capture, {"runtime", "sources", "client_build", "client_build_receipt_sha256"})
        proofs.shape(capture["runtime"], proofs.RUNTIME_FIELDS)
        proofs.sources(capture["sources"])
        proofs.exact(lane["sources"], capture["sources"])
        runtime = capture["runtime"]
        proofs.exact(capture["sources"], expected_sources)
        proofs.exact(capture["client_build"], client_build)
        for key, owner_key in (("phase", "phase"), ("round", "round"), ("client", "client"),
                ("project", "project"), ("database", "database"), ("instance_id", "instance_id"),
                ("service_pid", "service_pid"), ("service_starttime", "service_starttime")):
            proofs.exact(runtime[key], owner[owner_key])
        for key, suffix in (("base_dir", "service-data"), ("cache_dir", "cache"), ("pack_cache_dir", "pack-cache")):
            proofs.exact(runtime[key], owner["root"].rstrip("/") + "/" + suffix)
        proofs.native_identity(lane["identity_rows"], lane["identity"], lane["native_publication"], runtime["database"], runtime["instance_id"])
        for lane_key, runtime_key in (("identity_rows", "identity_rows"), ("identity", "identity"), ("native_publication", "native")):
            proofs.exact(lane[lane_key], runtime[runtime_key])
        proofs.exact(lane["native_proof_sha256"], proofs.digest({key: lane[key] for key in ("identity_rows", "identity", "native_publication")}))
        proofs.exact(lane["workspace_binding"], record["workspace_binding"])
        proofs.exact(lane["workspace_binding_sha256"], proofs.digest(lane["workspace_binding"]))
        closed = proofs.closed_projection_sink(lane["projection_sink"], lane["workspace_binding"], runtime)
        proofs.exact(closed, evidence["closed_projection"])
        semantic = {"fixed_commit": lane["fixed_commit"], "path_tree": lane["path_tree"],
            "scope": lane["workspace_binding"]["scope"], "oracle_manifest_sha256": lane["oracle_manifest_sha256"],
            "metadata_root": closed["payload"]["metadata_root"],
            "policy": {key: closed["payload"][key] for key in proofs.POLICY_FIELDS}}
        proofs.exact(evidence["semantic"], semantic)
        for key in ("phase", "round", "client", "fixed_commit", "path_tree", "oracle_manifest_sha256"):
            proofs.exact(lane[key], record[key])
        proofs.exact(lane["version"], list(campaign.common.scenarios(profile)).index(record["version"]) + 1)
        relative = record["manifest_relative_path"]
        proofs.exact(relative, f"measurements/{record['phase']}/round-{record['round']:02}/{record['version']}-expected.json")
        raw = read_regular(root / relative, fixture_size.ORACLE_MANIFEST_LIMIT)
        proofs.exact(hashlib.sha256(raw).hexdigest(), record["oracle_manifest_sha256"])
        manifest = observation.parse(raw)
        if isinstance(record["manifest"], campaign.ManifestFacts):
            proofs.exact(proofs.digest(manifest), record["manifest"].fingerprint)
        else:
            proofs.exact(manifest, record["manifest"])
        validate_directory_readiness(record, manifest)
        proofs.publication(record["publication"])
        proofs.exact(lane["publication"], record["publication"])
        proofs.require(record["publication"]["visible_monotonic"] <= proofs.finite(record["operation_started_monotonic"])
                       <= proofs.finite(record["operation_finished_monotonic"]))
        for value in lane["timings_ms"].values():
            proofs.finite(value)
        proofs.require(lane["timings_ms"]["metadata_ready_ms"] <= lane["timings_ms"]["durable_complete_ms"] <= lane["timings_ms"]["durable_verified_ms"])
        proofs.exact(lane["timings_ms"]["git_verified_ms"], record["git"]["verified_ms"])
        for key in ("metadata_ready_ms", "durable_complete_ms", "durable_verified_ms"):
            proofs.exact(lane["timings_ms"][key], record["scorpio"][key])
        entries.append({**record, "result": {"version": record["version"], "round": record["round"],
            "actual_status": record["actual_status"], "scorpio": record["scorpio"], "git": record["git"],
            "old_views": record["old_views"]}})
    proofs.require(all(record["round_final_retained_views"] == records[0]["round_final_retained_views"] for record in records))
    campaign.validate_workload(entries, records[0]["round_final_retained_views"], profile)
    leaf = root / "measurements" / records[0]["phase"] / f"round-{records[0]['round']:02}" / ("client-" + records[0]["client"])
    workspace_sink(leaf / "workspace-observation.jsonl", records, profile)
    for name in ("owned-workspace-daemon.json", "owned-workspace-worker.json"):
        raw = read_regular(leaf / name, 4096)
        receipt = records[0]["cleanup_receipts"][name]
        proofs.exact(receipt["sha256"], hashlib.sha256(raw).hexdigest())
        proofs.exact(receipt["record"], observation.parse(raw))
        proofs.exact(receipt["record"]["cleanup_complete"], True)


def validate_history(root, phase, number, seed_commit, records, profile):
    """Replay raw Git parent links and real changes against saved full oracles."""
    folder = Path(root) / "measurements" / phase / f"round-{number:02}"
    history = observation.parse(read_regular(folder / "git-history.json", 1024 * 1024))
    proofs.shape(history, {"revision", "profile", "round", "seed_commit", "commits"})
    proofs.exact(history["revision"], 1)
    proofs.exact(history["profile"], profile)
    proofs.exact(history["round"], number)
    proofs.exact(history["seed_commit"], seed_commit)
    versions = list(campaign.common.scenarios(profile))
    proofs.require(type(history["commits"]) is list and len(history["commits"]) == len(versions))
    parent, previous, seen = seed_commit, None, set()
    for version, receipt in zip(versions, history["commits"]):
        proofs.shape(receipt, {"version", "commit", "tree", "parent", "commit_body_hex", "change_counts", "source_changes"})
        proofs.exact(receipt["version"], version)
        proofs.exact(receipt["parent"], parent)
        proofs.hex_digest(receipt["commit"], 40)
        proofs.hex_digest(receipt["tree"], 40)
        text = receipt["commit_body_hex"]
        proofs.require(type(text) is str and len(text) <= 128 * 1024
                       and re.fullmatch(r"(?:[0-9a-f]{2})+", text) is not None)
        body = bytes.fromhex(text)
        digest = hashlib.sha1(b"commit " + str(len(body)).encode() + b"\0" + body).hexdigest()
        proofs.exact(receipt["commit"], digest)
        headers = body.split(b"\n\n", 1)[0].split(b"\n")
        proofs.exact([line for line in headers if line.startswith(b"tree ")], [("tree " + receipt["tree"]).encode()])
        proofs.exact([line for line in headers if line.startswith(b"parent ")], [("parent " + parent).encode()])
        proofs.require(digest not in seen)
        seen.add(digest)
        raw = read_regular(folder / f"{version}-expected.json", fixture_size.ORACLE_MANIFEST_LIMIT)
        expected = observation.parse(raw)
        fixture_size.validate_manifest(expected)
        proofs.exact(receipt["change_counts"], campaign.common.manifest_changes(expected, previous))
        proofs.exact(receipt["source_changes"], campaign.common.history_change(previous, expected, version))
        lane_records = [record for record in records if record["round"] == number and record["version"] == version]
        proofs.exact(len(lane_records), 2 if phase == "fair" else 1)
        for record in lane_records:
            proofs.exact(record["fixed_commit"], digest)
            proofs.exact(record["path_tree"], receipt["tree"])
            proofs.exact(record["oracle_manifest_sha256"], hashlib.sha256(raw).hexdigest())
        parent, previous = digest, expected
    return history


def validate_seed(value):
    proofs.shape(value, {"parent", "commit", "tree", "parent_commit_body_hex", "commit_body_hex", "tree_body_hex",
                         "commit_body_sha256", "tree_body_sha256", "bootstrap_commit_time"})
    timestamp = value["bootstrap_commit_time"]
    proofs.integer(timestamp, (1 << 32) - 1)
    bodies = {}
    for name, oid_key, kind in (("parent_commit", "parent", "commit"), ("commit", "commit", "commit"), ("tree", "tree", "tree")):
        text = value[name + "_body_hex"]
        proofs.require(type(text) is str and len(text) <= 8192 and re.fullmatch(r"(?:[0-9a-f]{2})*", text) is not None)
        raw = bytes.fromhex(text)
        oid = hashlib.sha1(kind.encode() + b" " + str(len(raw)).encode() + b"\0" + raw).hexdigest()
        proofs.exact(value[oid_key], oid)
        bodies[name] = raw
    expected = (f"tree {value['tree']}\nparent {value['parent']}\n"
        f"author MST2 setup baseline <mst2-setup@example.invalid> {timestamp} +0000\n"
        f"committer MST2 setup baseline <mst2-setup@example.invalid> {timestamp} +0000\n\n"
        "MST2 canonical native seed\n").encode()
    proofs.exact(bodies["commit"], expected)
    proofs.require(bodies["parent_commit"].startswith(("tree " + value["tree"] + "\n").encode()))
    proofs.exact(value["commit_body_sha256"], hashlib.sha256(bodies["commit"]).hexdigest())
    proofs.exact(value["tree_body_sha256"], hashlib.sha256(bodies["tree"]).hexdigest())
    return value


def validate_complete(root, build_receipts=None, *, run_metadata=None, deadline_utc=None):
    """Replay complete artifacts; this is not authority to start a backend."""
    root = Path(root)
    value = observation.parse(read_regular(root / "campaign.json", 2 * 1024 * 1024))
    complete_fields = {"revision", "record", "correctness", "session_started_utc", "session_deadline_utc",
        "cleanup_deadline_monotonic", "sources", "canonical_seed", "server_build", "server_build_receipt_sha256",
        "fair_records", "diagnostic_records", "fair_full_oracle_walks", "diagnostic_full_oracle_walks", "cleanup",
        "phase_measurements_sha256", "elapsed_execution_seconds", "performance_claims"}
    proofs.shape(value, complete_fields | (execution.DIRECT_FIELDS if "execution_provider" in value else set()))
    proofs.exact(value["revision"], 1)
    proofs.shape(value["sources"], {"a", "b"})
    proofs.shape(value["phase_measurements_sha256"], {"fair", "diagnostic"})
    proofs.require(value["record"] == "isolated_campaign_complete" and value["correctness"] == "PASS")
    proofs.exact(value["cleanup"]["owners"], 7)
    proofs.exact(value["cleanup"]["closed"], True)
    metadata = validate_run_metadata(run_metadata if run_metadata is not None else
        observation.parse(read_regular(root / "run.json", 16384)), value)
    profile = metadata["profile"]
    scenarios = campaign.common.scenarios(profile)
    count, walks = len(scenarios), campaign.full_oracle_walks(profile)
    proofs.exact(value["fair_records"], 6 * count)
    proofs.exact(value["diagnostic_records"], count)
    proofs.exact(value["fair_full_oracle_walks"], 6 * walks)
    proofs.exact(value["diagnostic_full_oracle_walks"], walks)
    seed = validate_seed(observation.parse(read_regular(root / "canonical-seed.json", 65536)))
    proofs.exact(seed, value["canonical_seed"])
    build_receipts = build_receipts or {label: root / ("server-build.json" if label == "server" else "client-" + label + "-build.json")
                                        for label in ("a", "b", "server")}
    proofs.require(set(build_receipts) == {"a", "b", "server"})
    actual_builds = {}
    actual_build_hashes = {}
    for label, path in build_receipts.items():
        raw = read_regular(path, 16384)
        build = observation.parse(raw)
        proofs.shape(build, builds.FIELDS)
        proofs.exact(build["revision"], 1)
        proofs.exact(build["label"], label)
        proofs.exact(build["build_env"], builds.BUILD_ENV)
        # Replay the producer's path syntax, independently of this machine.
        source_path = (PurePosixPath(build["source"]) if build["source"].startswith("/")
                       else PureWindowsPath(build["source"]))
        proofs.exact(build["build_argv"], builds.build_argv(source_path, "mega2" if label == "server" else "scorpio"))
        actual_builds[label] = build
        actual_build_hashes[label] = hashlib.sha256(raw).hexdigest()
        if label == "server":
            proofs.exact(hashlib.sha256(raw).hexdigest(), value["server_build_receipt_sha256"])
            proofs.exact(build, value["server_build"])
    for label in ("a", "b"):
        source, build = value["sources"][label], actual_builds[label]
        proofs.sources(source)
        proofs.exact(build["source_sha"], metadata["baseline_sha" if label == "a" else "candidate_sha"])
        for source_key, build_key in (("client_source_sha", "source_sha"), ("client_cargo_lock_sha256", "cargo_lock_sha256"),
                ("client_binary_sha256", "binary_sha256"), ("rustc_version", "rustc_version"), ("cargo_version", "cargo_version")):
            proofs.exact(source[source_key], build[build_key])
        for source_key, build_key in (("server_source_sha", "source_sha"), ("server_cargo_lock_sha256", "cargo_lock_sha256"),
                ("server_binary_sha256", "binary_sha256"), ("rustc_version", "rustc_version"), ("cargo_version", "cargo_version")):
            proofs.exact(source[source_key], actual_builds["server"][build_key])
    owners_raw = read_regular(root / "backend-owners.json", 65536)
    owners = observation.parse(owners_raw)
    proofs.shape(owners, {"revision", "project", "session_deadline_utc", "cleanup_deadline_monotonic",
                          "measurement_deadline_monotonic", "backends", "closed"})
    proofs.exact(owners["revision"], 1)
    proofs.exact(owners["project"], f"m2perf-{metadata['run_id']}-{metadata['attempt']}")
    proofs.exact(budgets.utc(owners["session_deadline_utc"]), budgets.utc(value["session_deadline_utc"]))
    proofs.exact(owners["cleanup_deadline_monotonic"], value["cleanup_deadline_monotonic"])
    proofs.exact(owners["measurement_deadline_monotonic"], value["cleanup_deadline_monotonic"]
                 - budgets.CAMPAIGN_REPORT - budgets.CLEANUP_RESERVE - budgets.CAMPAIGN_MARGIN)
    proofs.exact(value["cleanup"]["backend_owners_sha256"], hashlib.sha256(owners_raw).hexdigest())
    proofs.exact(owners["closed"], True)
    proofs.require(type(owners["backends"]) is list and len(owners["backends"]) == 7)
    proofs.require({(owner["phase"], owner["round"], owner["client"]) for owner in owners["backends"]}
                   == campaign.expected_owner_keys())
    for owner in owners["backends"]:
        proofs.shape(owner, OWNER_FIELDS)
        proofs.exact(owner["state"], "retired")
        proofs.exact(owner["initial_path_commit"], seed["parent"])
        proofs.require(type(owner["round"]) is int and type(owner["phase"]) is str and type(owner["client"]) is str)
        proofs.exact(owner["root"], metadata["owned_root"].rstrip("/")
                     + f"/backends/{owner['phase']}-r{owner['round']:02}-{owner['client']}")
        proofs.exact(owner["project"], owners["project"]
                     + f"-{owner['phase']}-r{owner['round']:02}-{owner['client']}")
    owner_map = {(owner["phase"], owner["round"], owner["client"]): owner for owner in owners["backends"]}
    for key in ("project", "database", "instance_id", "root"):
        proofs.require(len({owner[key] for owner in owners["backends"]}) == 7)
    proofs.require(len(value["cleanup"]["inventory"]) == 7)
    proofs.require({(item["owner"]["phase"], item["owner"]["round"], item["owner"]["client"])
                    for item in value["cleanup"]["inventory"]} == campaign.expected_owner_keys())
    for item in value["cleanup"]["inventory"]:
        owner = item["owner"]
        proofs.exact(owner, owner_map[(owner["phase"], owner["round"], owner["client"])])
        proofs.exact(item["dependency_containers_remaining"], 0)
        proofs.exact(item["dependency_networks_remaining"], 0)
    inventory_map = {(item["owner"]["phase"], item["owner"]["round"], item["owner"]["client"]): item
                     for item in value["cleanup"]["inventory"]}
    phases = {}
    for phase in ("fair", "diagnostic"):
        all_rows = []
        def retain(row):
            if row.get("record") == "round":
                manifest = row.pop("manifest")
                facts = campaign.ManifestFacts(manifest)
                if len(proofs.canonical(row).encode("utf8")) > fixture_size.COMPACT_RECORD_LIMIT:
                    raise AssertionError("compact campaign evidence exceeds bound")
                row["manifest"] = facts
            elif len(proofs.canonical(row).encode("utf8")) > fixture_size.COMPACT_RECORD_LIMIT:
                raise AssertionError("campaign control evidence exceeds bound")
            all_rows.append(row)
        digest = scan_rows(root / "measurements" / phase / "measurements.jsonl", retain,
                           deadline_utc=deadline_utc)
        proofs.exact(digest, value["phase_measurements_sha256"][phase])
        profiles.validate_artifact_tree(root / "measurements" / phase, diagnostic_required=phase == "diagnostic")
        records = [row for row in all_rows if row.get("record") == "round"]
        for record in records:
            proofs.exact(record["semantic_provenance"]["capture"]["client_build_receipt_sha256"], actual_build_hashes[record["client"]])
        environment = [row for row in all_rows if row.get("record") == "environment"]
        proofs.require(len(environment) == 1 and all_rows[0] == environment[0] and all_rows[-1]["record"] == "complete")
        proofs.exact(environment[0]["sources"], value["sources"])
        proofs.exact(environment[0]["server_build"], value["server_build"])
        for key in ("session_started_utc", "session_deadline_utc", "cleanup_deadline_monotonic"):
            proofs.exact(environment[0][key], value[key])
        proofs.exact(environment[0]["run_id"], metadata["run_id"])
        proofs.exact(environment[0]["run_attempt"], metadata["attempt"])
        if "execution_provider" in metadata:
            for key in execution.DIRECT_FIELDS:
                proofs.exact(environment[0][key], metadata[key])
        proofs.exact(environment[0]["profile"], metadata["profile"])
        proofs.exact(environment[0]["rounds"], 3 if phase == "fair" else 1)
        campaign.validate_matrix(records, phase, profile)
        proofs.require(len([row for row in all_rows if row.get("record") == "complete"]) == 1)
        complete = [row for row in all_rows if row.get("record") == "complete"][0]
        proofs.exact(complete["round_scenarios"], count * (6 if phase == "fair" else 1))
        proofs.exact(complete["full_oracle_walks"], walks * (6 if phase == "fair" else 1))
        proofs.exact(complete["correctness"], "PASS")
        proofs.exact(complete["campaign_cleanup_complete"], True)
        for number in (range(1, 4) if phase == "fair" else (1,)):
            if profile == "history-large":
                validate_history(root, phase, number, seed["commit"], records, profile)
            for label in (("a", "b") if phase == "fair" else ("b",)):
                lane = sorted([record for record in records if record["round"] == number and record["client"] == label],
                              key=lambda row: list(scenarios).index(row["version"]))
                validate_saved_lane(root, lane, owner_map[(phase, number, label)], value["sources"][label], actual_builds[label], profile)
                for record in lane:
                    proofs.exact(inventory_map[(phase, number, label)]["client_cleanup"], record["cleanup_receipts"])
        expected_summaries = ([dict(summary, phase="fair") for summary in campaign.summaries(records, profile)]
                              if phase == "fair" else [])
        proofs.exact([row for row in all_rows if row.get("record") == "paired_summary"], expected_summaries)
        phases[phase] = records
    for number in range(1, 4):
        for version in scenarios:
            pair = [r for r in phases["fair"] if r["round"] == number and r["version"] == version]
            proofs.compare_pair_values(*(record["semantic_provenance"] for record in pair))
    return value


def export(root, output, deadline_utc, receipts=(), *, run_metadata=None, git_performance_path=None,
           complete_allowed=True):
    budgets.require_external_time(deadline_utc)
    proofs.require(type(complete_allowed) is bool)
    root, output = Path(root).absolute(), Path(output).absolute()
    if output.exists() or output.is_symlink() or output.is_relative_to(root):
        raise ValueError("safe export destination must be fresh and outside the owned campaign")
    # The owned root can be absent on a build failure; still save closed run
    # metadata and available immutable build receipts without claiming success.
    if root.exists():
        if root.is_symlink() or not root.is_dir():
            raise AssertionError("safe export owned root changed")
        if (root / "campaign.json").exists():
            validate_complete(root, dict(receipts), run_metadata=run_metadata, deadline_utc=deadline_utc)
    git_evidence = None
    if git_performance_path is not None:
        source = Path(os.path.abspath(git_performance_path))
        proofs.require(not source.is_relative_to(Path(os.path.abspath(root)))
                       and not source.is_relative_to(Path(os.path.abspath(output))))
        git_evidence = git_performance_evidence(source,
            require_complete=(root / "campaign.json").exists() and complete_allowed, deadline_utc=deadline_utc)
    output.mkdir(mode=0o700)
    bindings = {}
    for directory in [*reversed(output.parents), output]:
        info = directory.lstat()
        proofs.require(stat.S_ISDIR(info.st_mode))
        bindings[directory] = (info.st_dev, info.st_ino)
    copied = {}
    if git_evidence is not None:
        raw, summary = git_evidence
        for name, content in (("git-performance.jsonl", raw), ("git-performance-summary.json",
                (proofs.canonical(summary) + "\n").encode("ascii"))):
            write_safe(output, name, content, bindings)
            copied[name] = hashlib.sha256(content).hexdigest()
            budgets.require_external_time(deadline_utc)
        validate_git_performance_export(output, require_complete=(root / "campaign.json").exists() and complete_allowed,
                                        deadline_utc=deadline_utc)
    if run_metadata is not None:
        validate_run_metadata(run_metadata)
        raw = (proofs.canonical(run_metadata) + "\n").encode("ascii")
        write_safe(output, "run.json", raw, bindings)
        copied["run.json"] = hashlib.sha256(raw).hexdigest()
        budgets.require_external_time(deadline_utc)
    if root.exists():
        for directory, dirs, names in os.walk(root, followlinks=False):
            parent = Path(directory)
            for name in dirs:
                if (parent / name).is_symlink():
                    raise AssertionError("safe export refuses substituted directory")
            # Private trees need not be traversed and never enter the allowlist.
            if parent == root:
                dirs[:] = [name for name in dirs if name == "measurements"]
            elif parent.name.startswith("client-"):
                dirs[:] = []
            elif parent.name.startswith("round-"):
                dirs[:] = [name for name in dirs if name.startswith("client-")]
            for name in names:
                source = parent / name
                relative = source.relative_to(root)
                if not allowed(relative):
                    continue
                budgets.require_external_time(deadline_utc)
                if name == "measurements.jsonl":
                    copied[relative.as_posix()] = copy_safe(source, output, relative, bindings, deadline_utc)
                else:
                    raw = read_regular(source)
                    write_safe(output, relative, raw, bindings)
                    copied[relative.as_posix()] = hashlib.sha256(raw).hexdigest()
                budgets.require_external_time(deadline_utc)
    for label, receipt in receipts:
        if Path(receipt).exists():
            raw = read_regular(receipt, 16384)
            value = observation.parse(raw)
            proofs.require(value["label"] == label)
            target = output / ("server-build.json" if label == "server" else "client-" + label + "-build.json")
            write_safe(output, target.relative_to(output), raw, bindings)
            copied[target.name] = hashlib.sha256(raw).hexdigest()
            budgets.require_external_time(deadline_utc)
    if (output / "campaign.json").exists():
        validate_complete(output, deadline_utc=deadline_utc)
    for phase in ("fair", "diagnostic"):
        subtree = output / "measurements" / phase
        if subtree.exists():
            profiles.validate_artifact_tree(subtree, diagnostic_required=phase == "diagnostic")
    budgets.require_external_time(deadline_utc)
    write_safe(output, "safe-export.json", (proofs.canonical({"revision": 1, "files_sha256": copied,
        "complete_campaign": (output / "campaign.json").exists() and complete_allowed,
        "private_logs_exported": False}) + "\n").encode("ascii"), bindings)
    budgets.require_external_time(deadline_utc)
    return copied


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--session-deadline-utc", required=True)
    parser.add_argument("--build-a", type=Path)
    parser.add_argument("--build-b", type=Path)
    parser.add_argument("--server-build", type=Path)
    parser.add_argument("--with-run-metadata", action="store_true")
    parser.add_argument("--git-performance", type=Path)
    parser.add_argument("--partial", action="store_true")
    options = parser.parse_args()
    return export(options.run_root, options.output, options.session_deadline_utc,
                  [(label, path) for label, path in (("a", options.build_a), ("b", options.build_b),
                                                    ("server", options.server_build)) if path],
                  run_metadata=run_metadata_from_env() if options.with_run_metadata else None,
                  git_performance_path=options.git_performance, complete_allowed=not options.partial)


if __name__ == "__main__":
    main()
