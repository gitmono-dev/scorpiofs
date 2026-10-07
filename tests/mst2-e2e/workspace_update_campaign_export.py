"""Copy a closed allowlist of independent fair/diagnostic evidence, never logs."""

import argparse
import hashlib
import json
import os
from pathlib import Path
from pathlib import PurePosixPath
import re
import stat
from datetime import timedelta

import commit_update_budget as budgets
import workspace_update_backend_proof as proofs
import workspace_update_build as builds
import workspace_update_campaign as campaign
import workspace_update_observation as observation
import workspace_update_profile as profiles

ROOT_FILES = {"campaign.json", "canonical-seed.json", "backend-owners.json", "failure.json"}
CLIENT_FILES = {"workspace-observation.jsonl", "owned-workspace-daemon.json", "owned-workspace-worker.json"}
OWNER_FIELDS = {"phase", "round", "client", "project", "database", "instance_id", "root", "state",
                "operation_deadline_monotonic", "service_pid", "service_starttime", "initial_path_commit"}
RUN_FIELDS = {"revision", "run_id", "attempt", "harness_sha", "mega_sha", "baseline_sha", "candidate_sha",
              "profile", "rounds", "comparison", "bootstrap_commit_time", "session_started_utc",
              "session_deadline_utc", "cleanup_deadline_monotonic", "owned_root"}


def run_metadata_from_env():
    value = {"revision": 1, "run_id": os.environ["GITHUB_RUN_ID"], "attempt": os.environ["GITHUB_RUN_ATTEMPT"],
        "harness_sha": os.environ["SCORPIO_SHA"], "mega_sha": os.environ["MEGA_SHA"],
        "baseline_sha": os.environ["BASELINE_SHA"], "candidate_sha": os.environ["CANDIDATE_SHA"],
        "profile": os.environ["PROFILE"], "rounds": int(os.environ["ROUNDS"]), "comparison": os.environ["COMPARISON"],
        "bootstrap_commit_time": campaign.commit_time(os.environ["BOOTSTRAP_COMMIT_TIME"]),
        "session_started_utc": os.environ["STARTED_INPUT"], "session_deadline_utc": os.environ["DEADLINE_INPUT"],
        "cleanup_deadline_monotonic": float(os.environ["MST2_WORK_CLEANUP_DEADLINE_MONOTONIC"]),
        "owned_root": os.environ["MST2_OWNED_ROOT"]}
    proofs.exact(value["harness_sha"], os.environ["GITHUB_SHA"])
    proofs.exact(value["owned_root"], str(PurePosixPath(os.environ["RUNNER_TEMP"]) / f"mst2-real-{value['run_id']}-{value['attempt']}"))
    validate_run_metadata(value)
    return value


def validate_run_metadata(value, complete=None):
    proofs.shape(value, RUN_FIELDS)
    proofs.exact(value["revision"], 1)
    for key in ("run_id", "attempt"):
        proofs.require(type(value[key]) is str and re.fullmatch(r"[1-9][0-9]{0,19}", value[key]))
    for key in ("harness_sha", "mega_sha", "baseline_sha", "candidate_sha"):
        proofs.hex_digest(value[key], 40)
    proofs.exact(value["baseline_sha"], campaign.BASELINE)
    proofs.exact(value["candidate_sha"], campaign.CANDIDATE)
    proofs.exact(value["rounds"], 3)
    proofs.exact(value["comparison"], "isolated")
    proofs.require(value["profile"] in ("smoke", "medium"))
    proofs.integer(value["bootstrap_commit_time"], (1 << 32) - 1)
    proofs.finite(value["cleanup_deadline_monotonic"])
    proofs.require(type(value["owned_root"]) is str and PurePosixPath(value["owned_root"]).is_absolute()
                   and PurePosixPath(value["owned_root"]).name == f"mst2-real-{value['run_id']}-{value['attempt']}")
    proofs.exact(budgets.utc(value["session_deadline_utc"]), budgets.utc(value["session_started_utc"]) + timedelta(minutes=235))
    if complete is not None:
        for key in ("session_started_utc", "session_deadline_utc", "cleanup_deadline_monotonic"):
            proofs.exact(value[key], complete[key])
        proofs.exact(value["bootstrap_commit_time"], complete["canonical_seed"]["bootstrap_commit_time"])
        for label in ("a", "b"):
            proofs.exact(value["harness_sha"], complete["sources"][label]["harness_source_sha"])
            proofs.exact(value["mega_sha"], complete["sources"][label]["server_source_sha"])
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
        return re.fullmatch(r"v[1-4]-expected\.json", parts[3]) is not None
    return (parts[3] in (("client-a", "client-b") if parts[1] == "fair" else ("client-b",))
            and parts[4] in CLIENT_FILES)


def read_regular(path, cap=32 * 1024 * 1024):
    """Pin every ancestor and the exact regular inode while reading."""
    path = Path(path).absolute()
    chain = [*reversed(path.parents)]
    identities = {}
    for directory in chain:
        info = directory.lstat()
        if not stat.S_ISDIR(info.st_mode):
            raise AssertionError("safe evidence ancestor is not a real directory")
        identities[directory] = (info.st_dev, info.st_ino)
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > cap:
        raise AssertionError("safe evidence file is not a bounded independent regular file")
    parent_fd = None
    file_fd = None
    try:
        if os.name == "posix":
            flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
            parent_fd = os.open(path.anchor, flags)
            current = Path(path.anchor)
            for part in path.parts[1:-1]:
                child = os.open(part, flags, dir_fd=parent_fd)
                os.close(parent_fd)
                parent_fd = child
                current /= part
                info = os.fstat(parent_fd)
                if (info.st_dev, info.st_ino) != identities[current]:
                    raise AssertionError("safe evidence ancestor was replaced")
            file_fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=parent_fd)
        else:
            file_fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_BINARY", 0))
        opened = os.fstat(file_fd)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise AssertionError("safe evidence file was replaced before read")
        with os.fdopen(file_fd, "rb", closefd=False) as stream:
            raw = stream.read(cap + 1)
        after = os.fstat(file_fd)
        current = path.lstat()
        fields = lambda v: (v.st_dev, v.st_ino, v.st_size, v.st_mtime_ns, v.st_ctime_ns, v.st_nlink)
        path_fields = lambda v: (v.st_dev, v.st_ino, v.st_size, v.st_mtime_ns, v.st_nlink)
        # Windows fstat and lstat expose different ctime meanings. Compare
        # descriptor change-time with itself and pathname birth-time with
        # itself; Linux retains the full descriptor/path change-time check.
        path_changed = (fields(opened) != fields(current) if os.name == "posix" else
                        path_fields(opened) != path_fields(current) or before.st_ctime_ns != current.st_ctime_ns)
        if len(raw) > cap or fields(opened) != fields(after) or path_changed:
            raise AssertionError("safe evidence file changed during read")
        for directory, identity in identities.items():
            info = directory.lstat()
            if not stat.S_ISDIR(info.st_mode) or (info.st_dev, info.st_ino) != identity:
                raise AssertionError("safe evidence ancestor changed during read")
        return raw
    finally:
        if file_fd is not None:
            os.close(file_fd)
        if parent_fd is not None:
            os.close(parent_fd)


def rows(path):
    raw = read_regular(path)
    if raw and not raw.endswith(b"\n"):
        raise AssertionError("campaign evidence has an incomplete JSONL line")
    values = []
    for line in raw.split(b"\n")[:-1]:
        if len(line) > 16 * 1024 * 1024:
            raise AssertionError("campaign evidence line exceeds bound")
        value = observation.parse(line)
        proofs.require(type(value) is dict)
        values.append(value)
    return raw, values


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
            stream.write(raw)
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


def workspace_sink(path, records):
    raw, values = rows(path)
    proofs.require(len(values) == 5 and len(records) == 4)
    bindings, footer = values[:-1], values[-1]
    for key in ("workspace_id", "generation", "store"):
        proofs.require(len({binding[key] for binding in bindings}) == 4)
    proofs.shape(footer, observation.FOOTER_FIELDS)
    proofs.exact(footer["record"], "workspace_observation_footer")
    proofs.exact(footer["revision"], 1)
    for key in ("complete", "producers_closed", "drained"):
        proofs.exact(footer[key], True)
    proofs.exact(footer["daemon_exit_code"], 0)
    proofs.exact(footer["first_error"], None)
    for key in ("accepted_records", "received_records", "written_records"):
        proofs.exact(footer[key], 4)
    proofs.exact(footer["written_bytes"], sum(len(line) + 1 for line in raw.split(b"\n")[:-2]))
    for binding, record in zip(bindings, records):
        observation.validate_binding(binding, footer["run_id"])
        proofs.exact(binding, record["workspace_binding"])
        proofs.exact(record["workspace_sink_sha256"], hashlib.sha256(raw).hexdigest())
    return raw


def validate_saved_lane(root, records, owner, expected_sources, client_build):
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
        proofs.exact(lane["version"], list(campaign.common.SCENARIOS).index(record["version"]) + 1)
        relative = record["manifest_relative_path"]
        proofs.exact(relative, f"measurements/{record['phase']}/round-{record['round']:02}/{record['version']}-expected.json")
        raw = read_regular(root / relative, 16 * 1024 * 1024)
        proofs.exact(hashlib.sha256(raw).hexdigest(), record["oracle_manifest_sha256"])
        manifest = observation.parse(raw)
        proofs.exact(manifest, record["manifest"])
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
    campaign.validate_workload(entries, records[0]["round_final_retained_views"])
    leaf = root / "measurements" / records[0]["phase"] / f"round-{records[0]['round']:02}" / ("client-" + records[0]["client"])
    workspace_sink(leaf / "workspace-observation.jsonl", records)
    for name in ("owned-workspace-daemon.json", "owned-workspace-worker.json"):
        raw = read_regular(leaf / name, 4096)
        receipt = records[0]["cleanup_receipts"][name]
        proofs.exact(receipt["sha256"], hashlib.sha256(raw).hexdigest())
        proofs.exact(receipt["record"], observation.parse(raw))
        proofs.exact(receipt["record"]["cleanup_complete"], True)


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


def validate_complete(root, build_receipts=None, *, run_metadata=None):
    """Replay complete artifacts; this is not authority to start a backend."""
    root = Path(root)
    value = observation.parse(read_regular(root / "campaign.json", 2 * 1024 * 1024))
    proofs.shape(value, {"revision", "record", "correctness", "session_started_utc", "session_deadline_utc",
        "cleanup_deadline_monotonic", "sources", "canonical_seed", "server_build", "server_build_receipt_sha256",
        "fair_records", "diagnostic_records", "fair_full_oracle_walks", "diagnostic_full_oracle_walks", "cleanup",
        "phase_measurements_sha256", "elapsed_execution_seconds", "performance_claims"})
    proofs.exact(value["revision"], 1)
    proofs.shape(value["sources"], {"a", "b"})
    proofs.shape(value["phase_measurements_sha256"], {"fair", "diagnostic"})
    proofs.require(value["record"] == "isolated_campaign_complete" and value["correctness"] == "PASS")
    proofs.exact(value["fair_records"], 24)
    proofs.exact(value["diagnostic_records"], 4)
    proofs.exact(value["fair_full_oracle_walks"], 108)
    proofs.exact(value["diagnostic_full_oracle_walks"], 18)
    proofs.exact(value["cleanup"]["owners"], 7)
    proofs.exact(value["cleanup"]["closed"], True)
    metadata = validate_run_metadata(run_metadata if run_metadata is not None else
        observation.parse(read_regular(root / "run.json", 16384)), value)
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
        proofs.exact(build["build_argv"], builds.build_argv(Path(build["source"]), "mega2" if label == "server" else "scorpio"))
        actual_builds[label] = build
        actual_build_hashes[label] = hashlib.sha256(raw).hexdigest()
        if label == "server":
            proofs.exact(hashlib.sha256(raw).hexdigest(), value["server_build_receipt_sha256"])
            proofs.exact(build, value["server_build"])
    for label in ("a", "b"):
        source, build = value["sources"][label], actual_builds[label]
        proofs.sources(source)
        proofs.exact(build["source_sha"], campaign.BASELINE if label == "a" else campaign.CANDIDATE)
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
        raw, all_rows = rows(root / "measurements" / phase / "measurements.jsonl")
        proofs.exact(hashlib.sha256(raw).hexdigest(), value["phase_measurements_sha256"][phase])
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
        proofs.exact(environment[0]["profile"], metadata["profile"])
        proofs.exact(environment[0]["rounds"], 3 if phase == "fair" else 1)
        campaign.validate_matrix(records, phase)
        proofs.require(len([row for row in all_rows if row.get("record") == "complete"]) == 1)
        complete = [row for row in all_rows if row.get("record") == "complete"][0]
        proofs.exact(complete["round_scenarios"], 24 if phase == "fair" else 4)
        proofs.exact(complete["full_oracle_walks"], 108 if phase == "fair" else 18)
        proofs.exact(complete["correctness"], "PASS")
        proofs.exact(complete["campaign_cleanup_complete"], True)
        for number in (range(1, 4) if phase == "fair" else (1,)):
            for label in (("a", "b") if phase == "fair" else ("b",)):
                lane = sorted([record for record in records if record["round"] == number and record["client"] == label],
                              key=lambda row: row["version"])
                validate_saved_lane(root, lane, owner_map[(phase, number, label)], value["sources"][label], actual_builds[label])
                for record in lane:
                    proofs.exact(inventory_map[(phase, number, label)]["client_cleanup"], record["cleanup_receipts"])
        phases[phase] = records
    for number in range(1, 4):
        for version in campaign.common.SCENARIOS:
            pair = [r for r in phases["fair"] if r["round"] == number and r["version"] == version]
            proofs.compare_pair_values(*(record["semantic_provenance"] for record in pair))
    return value


def export(root, output, deadline_utc, receipts=(), *, run_metadata=None):
    budgets.require_external_time(deadline_utc)
    root, output = Path(root).absolute(), Path(output).absolute()
    if output.exists() or output.is_symlink() or output.is_relative_to(root):
        raise ValueError("safe export destination must be fresh and outside the owned campaign")
    # The owned root can be absent on a build failure; still save closed run
    # metadata and available immutable build receipts without claiming success.
    if root.exists():
        if root.is_symlink() or not root.is_dir():
            raise AssertionError("safe export owned root changed")
        if (root / "campaign.json").exists():
            validate_complete(root, dict(receipts), run_metadata=run_metadata)
    output.mkdir(mode=0o700)
    bindings = {}
    for directory in [*reversed(output.parents), output]:
        info = directory.lstat()
        proofs.require(stat.S_ISDIR(info.st_mode))
        bindings[directory] = (info.st_dev, info.st_ino)
    copied = {}
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
        validate_complete(output)
    for phase in ("fair", "diagnostic"):
        subtree = output / "measurements" / phase
        if subtree.exists():
            profiles.validate_artifact_tree(subtree, diagnostic_required=phase == "diagnostic")
    budgets.require_external_time(deadline_utc)
    write_safe(output, "safe-export.json", (proofs.canonical({"revision": 1, "files_sha256": copied,
        "complete_campaign": (output / "campaign.json").exists(), "private_logs_exported": False}) + "\n").encode("ascii"), bindings)
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
    options = parser.parse_args()
    return export(options.run_root, options.output, options.session_deadline_utc,
                  [(label, path) for label, path in (("a", options.build_a), ("b", options.build_b),
                                                    ("server", options.server_build)) if path],
                  run_metadata=run_metadata_from_env() if options.with_run_metadata else None)


if __name__ == "__main__":
    main()
