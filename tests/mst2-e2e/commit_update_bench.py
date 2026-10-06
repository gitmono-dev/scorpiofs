#!/usr/bin/env python3
"""Measure real commit updates against an explicitly isolated, running Mega2.

Default is plan-only. Execute never starts/stops services, creates cloud or DB
resources, changes server configuration, force-pushes, or deletes old results.
Build the shipped scorpio binary first. Tokens and PostgreSQL credentials
are inherited through M2_TOKEN/M2_GIT_TOKEN and PG* environment variables.
"""

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import sys
import time
from urllib.parse import urlsplit
import uuid

import commit_update_budget as budget_module
import commit_update_projection as projection_module

try:
    import tomllib
except ModuleNotFoundError:
    from pip._vendor import tomli as tomllib


# Duplicated deliberately at this boundary: the benchmark must remain able
# to serialize a safe record even when the worker module failed to import.
# Unknown values are discarded rather than copied from an exception object.
SAFE_WORKER_ERROR_CODES = frozenset({
    "worker_error", "worker_input_invalid", "worker_http_request_too_large",
    "worker_http_redirect_rejected", "worker_http_body_too_large",
    "worker_http_content_length_invalid", "worker_http_body_truncated",
    "worker_http_status_rejected", "worker_http_status_4xx",
    "worker_http_status_5xx", "worker_http_status_other",
    "worker_http_response_invalid",
    "worker_http_request_failed", "worker_json_invalid",
    "workspace_status_invalid", "workspace_mount_invalid",
    "workspace_identity_invalid", "workspace_hydration_failed",
    "workspace_retention_invalid", "workspace_oracle_failed",
    "worker_process_invalid", "worker_command_failed",
    "worker_receipt_invalid", "worker_cleanup_invalid", "git_baseline_invalid",
})
SAFE_WORKER_STAGES = frozenset({
    "create", "hydrate", "poll", "oracle", "retained", "git", "destroy", "cleanup",
})
SAFE_RETENTION_SUBSTAGES = frozenset({
    "retain_path", "upper_check", "sentinel_write", "retained_fd",
    "old_view_oracle", "old_view_fd", "old_view_sentinel", "final_view_oracle",
})
SAFE_BACKEND_ERROR_CODES = frozenset({
    "INVALID_CONFIG", "SNAPSHOT_ERROR", "WORKSPACE_BUSY", "WORKSPACE_DIRTY",
    "WORKSPACE_IO", "WORKSPACE_NOT_FOUND", "WORKSPACE_NOT_READY",
    "WORKSPACE_UNKNOWN",
})
SAFE_SNAPSHOT_CODES = frozenset({
    "ScopeInvalid", "InvalidRequest", "LimitExceeded", "Unauthenticated",
    "ScopeForbidden", "ViewNotFound", "SnapshotNotReady", "SnapshotGone",
    "PathNotFound", "NotDirectory", "UnsupportedEntry", "LeaseUnknown",
    "LeaseExpired", "CursorInvalid", "CursorStale", "ProofBudgetExceeded",
    "DigestMismatch", "IntegrityError", "ObjectUnavailable", "RangeNotSupported",
    "SymlinkTraversal", "DurableViewConflict", "TemporaryUnavailable", "Internal",
})


def _safe_worker_error_code(error):
    code = getattr(error, "error_code", None)
    return code if code in SAFE_WORKER_ERROR_CODES else None


def _safe_worker_stage(error):
    stage = getattr(error, "worker_stage", None)
    return stage if type(stage) is str and stage in SAFE_WORKER_STAGES else None


def _safe_retention_substage(error):
    substage = getattr(error, "retention_substage", None)
    return substage if type(substage) is str and substage in SAFE_RETENTION_SUBSTAGES else None


def _safe_backend_error_code(error):
    code = getattr(error, "backend_code", None)
    return code if code in SAFE_BACKEND_ERROR_CODES else None


def _safe_snapshot_code(error):
    code = getattr(error, "snapshot_code", None)
    return code if code in SAFE_SNAPSHOT_CODES else None


IDENTITY_SQL = """BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL search_path = public;
SELECT json_agg(row_to_json(x)) FROM (
 SELECT r.path, r.ref_commit_hash AS commit, r.ref_tree_hash AS tree,
        current_database() AS database, c.tree AS commit_tree,
        CASE WHEN r.path='/' THEN encode(t.sub_trees,'hex') ELSE NULL END AS raw_tree
 FROM mega_refs r LEFT JOIN mega_tree t ON t.tree_id=r.ref_tree_hash
 LEFT JOIN mega_commit c ON c.commit_id=r.ref_commit_hash
 WHERE r.ref_name='refs/heads/main' AND r.path IN ('/', '/project') AND NOT r.is_cl
 ORDER BY r.path
) x;
COMMIT;"""

NATIVE_SQL = """BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL search_path = public;
SELECT COALESCE((SELECT row_to_json(x) FROM (
 SELECT h.instance_id, h.sequence, h.writer_epoch, h.root_commit, h.root_tree,
        h.state, h.certificate_receipt_id, n.root_commit AS certificate_commit,
        n.root_tree AS certificate_tree, n.sequence AS certificate_sequence,
        n.instance_id AS certificate_instance, n.path_commit, n.path_tree,
        n.origin_path, n.origin_ref, p.sequence AS origin_sequence,
        n.receipt_id AS certificate_id, n.namespace AS certificate_namespace,
        n.writer_epoch AS certificate_epoch, n.old_root_commit, n.old_path_commit,
        p.id AS receipt_id, p.namespace AS receipt_namespace, p.old_oid, p.new_oid,
        p.writer_epoch AS receipt_epoch, p.writer_kind, p.request_digest,
        p.request_digest_version,
        p.native_certificate_version, o.id AS outbox_id,
        o.namespace AS outbox_namespace, o.sequence AS outbox_sequence
 FROM mst2_native_head h
 LEFT JOIN mst2_native_publication n ON n.receipt_id=h.certificate_receipt_id
 LEFT JOIN mst2_publication p ON p.id=n.receipt_id
 LEFT JOIN mst2_publication_outbox o ON o.operation_id=p.operation_id
 WHERE h.namespace='/'
) x), 'null'::json);
COMMIT;"""


class CommandFailure(RuntimeError):
    """Closed diagnostic fields; never command arguments or child messages."""

    def __init__(self, program, status, _stderr):
        self.details = {"command": program, "exit_status": status}
        super().__init__(program + " failed")


class PhaseFailure(AssertionError):
    """A fixed harness phase, without child output or exception text."""

    def __init__(self, phase, error):
        self.phase = phase
        self.failure_type = type(error).__name__
        self.details = error.details if isinstance(error, CommandFailure) else {}
        # WorkerError exposes only a closed, non-sensitive code.  Keep this
        # separate from ``details`` so existing CommandFailure records remain
        # byte-for-byte compatible.
        self.error_code = _safe_worker_error_code(error)
        self.worker_stage = _safe_worker_stage(error)
        self.retention_substage = _safe_retention_substage(error)
        self.backend_code = _safe_backend_error_code(error)
        self.snapshot_code = _safe_snapshot_code(error)
        super().__init__(phase + " failed")


@contextmanager
def phase(name):
    try:
        yield
    except PhaseFailure:
        raise
    except Exception as error:
        raise PhaseFailure(name, error) from None


def failure_record(error):
    record = {"execution_failed": True, "error_type": type(error).__name__}
    if isinstance(error, PhaseFailure):
        record.update(error_type=error.failure_type, phase=error.phase)
        if error.error_code is not None:
            record["error_code"] = error.error_code
        if error.worker_stage is not None:
            record["worker_stage"] = error.worker_stage
        if error.retention_substage is not None:
            record["retention_substage"] = error.retention_substage
        if error.backend_code is not None:
            record["backend_code"] = error.backend_code
        if error.snapshot_code is not None:
            record["snapshot_code"] = error.snapshot_code
        record.update(error.details)
    elif isinstance(error, CommandFailure):
        record.update(error.details)
    else:
        code = _safe_worker_error_code(error)
        if code is not None:
            record["error_code"] = code
        stage = _safe_worker_stage(error)
        if stage is not None:
            record["worker_stage"] = stage
        retention_substage = _safe_retention_substage(error)
        if retention_substage is not None:
            record["retention_substage"] = retention_substage
        backend_code = _safe_backend_error_code(error)
        if backend_code is not None:
            record["backend_code"] = backend_code
        snapshot_code = _safe_snapshot_code(error)
        if snapshot_code is not None:
            record["snapshot_code"] = snapshot_code
    return record


def clean_env(extra=None):
    env = {k: os.environ[k] for k in ("HOME", "USER", "LOGNAME", "PATH", "LANG", "LC_ALL", "TZ")
           if k in os.environ}
    env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL="/dev/null",
               GIT_TERMINAL_PROMPT="0", NO_PROXY="127.0.0.1,localhost",
               no_proxy="127.0.0.1,localhost")
    if extra:
        env.update(extra)
    return env


def command(args, deadline, env=None, data=None):
    # Each child owns a process group; a stalled Git/HTTP child cannot survive
    # the shared wall-clock deadline. Captured errors never echo bearer headers.
    status, out, error_output = budget_module.run_process(
        args, min(deadline, time.monotonic() + 1800), env=env or clean_env(), data=data)
    if status:
        name = Path(str(args[0])).name.removesuffix(".exe")
        # The shipped server is an owned executable in the setup phase. Keep
        # its label closed so safe failure evidence identifies the failing
        # command without copying arguments, paths, or child stderr.
        program = name if name in {"git", "psql", "docker", "mega2", "scorpio"} else "external_command"
        raise CommandFailure(program, status, error_output)
    return out


def git(repo, deadline, *args, env=None, data=None):
    return command(["git", "-C", str(repo), *args], deadline, env=env, data=data)


def fsync_tree(root, deadline):
    """Flush regular files and directories, without following symlinks."""
    files = directories = size = 0
    if root.is_symlink() or not root.is_dir():
        raise AssertionError("flush root must be a real private directory")
    def fail(error):
        raise error
    for base, _, names in os.walk(root, topdown=False, followlinks=False, onerror=fail):
        for name in names:
            if time.monotonic() >= deadline:
                raise TimeoutError("Git durable worktree flush exceeded the shared deadline")
            path = Path(base) / name
            mode = path.lstat().st_mode
            if stat.S_ISLNK(mode):
                continue  # Its inode/entry is committed by the parent dir fsync.
            if not stat.S_ISREG(mode):
                raise AssertionError("unexpected special node in private Git baseline")
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                size += os.fstat(fd).st_size
                os.fsync(fd)
            finally:
                os.close(fd)
            files += 1
        fd = os.open(base, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        directories += 1
    return {"regular_files": files, "directories": directories, "file_bytes": size}


def endpoint_pair(base_url, git_url):
    base, remote = urlsplit(base_url), urlsplit(git_url)
    for endpoint in (base, remote):
        if (endpoint.scheme != "http" or endpoint.hostname != "127.0.0.1" or not endpoint.port
                or endpoint.username or endpoint.password or endpoint.query or endpoint.fragment):
            raise ValueError("only explicit credential-free loopback HTTP endpoints are accepted")
    if base.netloc != remote.netloc or base.path not in ("", "/") or remote.path != "/project":
        raise ValueError("Git and MST/2 must address the same isolated service /project")
    return base.port


def service_binding(options):
    """Bind HTTP socket, process start identity, effective DB and MST instance."""
    proc = Path(f"/proc/{options.service_pid}")
    started = proc.joinpath("stat").read_text().rsplit(") ", 1)[1].split()[19]
    argv = [x.decode() for x in proc.joinpath("cmdline").read_bytes().split(b"\0") if x]
    if "--config" not in argv or any(x.startswith("--profile") for x in argv):
        raise AssertionError("service must load one explicit literal config, without profile merging")
    config_path = Path(argv[argv.index("--config") + 1]).resolve(strict=True)
    raw = config_path.read_bytes()
    config = tomllib.loads(raw.decode())
    inherited = dict(x.decode().split("=", 1) for x in proc.joinpath("environ").read_bytes().split(b"\0") if x)
    allowed = {"MEGA_DATABASE__DB_URL", "MEGA_REDIS__URL", "MEGA_BASE_DIR", "MEGA_CACHE_DIR",
               "MEGA_GIT_OBJECT_CACHE_PREFIX"}
    if "MEGA_PROFILE" in inherited or any(k.startswith("MEGA_") and k not in allowed for k in inherited):
        raise AssertionError("unreviewed service environment overrides literal config")
    db = urlsplit(inherited.get("MEGA_DATABASE__DB_URL", config["database"]["db_url"]))
    if (db.scheme not in ("postgres", "postgresql") or db.query or db.fragment
            or db.hostname != os.environ.get("PGHOST") or db.port != int(os.environ["PGPORT"])
            or db.username != os.environ.get("PGUSER")
            or db.path != "/" + options.database or os.environ.get("PGDATABASE") != options.database):
        raise AssertionError("read-only identity DB does not match the running service")
    if (not config["mst2"]["enabled"] or config["mst2"]["instance_uuid"] != options.instance_id
            or bool(config["mst2"]["publication_enabled"]) != (options.publication_mode == "native")):
        raise AssertionError("service MST/2 instance/publication mode differs from explicit arguments")
    if config["monorepo"]["object_format"] != "sha1" or config["monorepo"]["push_policy"] != "trunk":
        raise AssertionError("benchmark requires SHA-1 trunk ingest")
    projection = getattr(options, "projection_traces", False)
    if projection and (config["mst2"].get("projection_observation_enabled") is not True
                       or not inherited.get("MEGA_CACHE_DIR")):
        raise AssertionError("typed projection sink must use the owned service's explicit cache")
    sockets = set()
    for fd in proc.joinpath("fd").iterdir():
        try:
            target = fd.readlink().as_posix()
        except FileNotFoundError:
            continue
        if target.startswith("socket:["):
            sockets.add(target)
    port = endpoint_pair(options.base_url, options.git_url)
    listener = False
    for line in Path("/proc/net/tcp").read_text().splitlines()[1:]:
        fields = line.split()
        address, number = fields[1].split(":")
        if address == "0100007F" and int(number, 16) == port and fields[3] == "0A":
            listener = f"socket:[{fields[9]}]" in sockets
    if not listener:
        raise AssertionError("requested loopback listener is not owned by the explicit service PID")
    binding = {"pid": options.service_pid, "starttime_ticks": started,
            "exe": str(proc.joinpath("exe").resolve()),
            "config_sha256": hashlib.sha256(raw).hexdigest()}
    if projection:
        binding["projection_cache"] = inherited["MEGA_CACHE_DIR"]
    return binding


def query(sql, deadline, env=None):
    if env is None:
        env = clean_env({k: v for k, v in os.environ.items() if k.startswith("PG")})
    raw = command(["psql", "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1"],
                  deadline, env=env, data=sql.encode())
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # Never echo SQL results, connection strings or credentials in CI.
        phase = "native publication" if sql == NATIVE_SQL else "Git identity"
        raise AssertionError(f"{phase} fence returned invalid JSON") from None


def validate_identity(rows, commit, tree, database):
    if len(rows) != 2 or {row["path"] for row in rows} != {"/", "/project"}:
        raise AssertionError("exact global and project main refs are required")
    rows = {row["path"]: row for row in rows}
    if any(row["database"] != database or row["commit_tree"] != row["tree"] for row in rows.values()):
        raise AssertionError("database or stored commit/tree differs")
    if rows["/project"]["commit"] != commit or rows["/project"]["tree"] != tree:
        raise AssertionError("actual project main is stale or differs from the pushed Git commit")
    raw = bytes.fromhex(rows["/"]["raw_tree"])
    if hashlib.sha1(b"tree " + str(len(raw)).encode() + b"\0" + raw).hexdigest() != rows["/"]["tree"]:
        raise AssertionError("global raw tree does not match its Git object ID")
    project = []
    cursor = 0
    while cursor < len(raw):
        separator = raw.index(b" ", cursor)
        end_name = raw.index(b"\0", separator + 1)
        if end_name + 21 > len(raw):
            raise AssertionError("truncated global tree")
        if raw[separator + 1:end_name] == b"project":
            project.append((raw[cursor:separator], raw[end_name + 1:end_name + 21].hex()))
        cursor = end_name + 21
    if project != [(b"40000", tree)]:
        raise AssertionError("global tree /project is not the fixed local Git tree")
    root = rows["/"]
    return {"project_commit": commit, "project_tree": tree, "global_commit": root["commit"],
            "global_tree": root["tree"], "namespace_view_id": "sha256:" +
            hashlib.sha256(b"mega.mst2.namespaceview\0" + root["commit"].encode()).hexdigest()}


def validate_native(native, identity, instance_id, after_push, previous=None):
    if (not native or native["instance_id"] != instance_id
            or native["root_commit"] != identity["global_commit"]
            or native["root_tree"] != identity["global_tree"]
            or native["writer_epoch"] != 1 or not isinstance(native["sequence"], int)
            or native["sequence"] < 0):
        raise AssertionError("native head does not bind the same global commit/tree/instance")
    if not after_push and native["state"] == "INITIALIZING" and native["certificate_receipt_id"] is None:
        return
    if (native["state"] != "READY" or not native["certificate_receipt_id"] or not native["outbox_id"]
            or native["certificate_commit"] != identity["global_commit"]
            or native["certificate_tree"] != identity["global_tree"]
            or native["certificate_instance"] != instance_id
            or native["certificate_sequence"] != native["sequence"]
            or native["path_commit"] != identity["project_commit"]
            or native["path_tree"] != identity["project_tree"]
            or native["origin_path"] != "/project" or native["origin_ref"] != "refs/heads/main"
            or native["native_certificate_version"] != 1
            or native["certificate_id"] != native["certificate_receipt_id"]
            or native["receipt_id"] != native["certificate_receipt_id"]
            or native["certificate_namespace"] != "/"
            or native["certificate_epoch"] != native["writer_epoch"]
            or native["receipt_epoch"] != native["writer_epoch"]
            or native["writer_kind"] != "trunk_push"
            or native["request_digest_version"] != 1
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", native["request_digest"] or "")
            or native["old_oid"] is None or native["old_oid"] != native["old_root_commit"]
            or native["new_oid"] is None or native["new_oid"] != native["path_commit"]
            or native["old_path_commit"] == native["path_commit"]
            or native["receipt_namespace"] != native["origin_path"]
            or native["outbox_namespace"] != "/project"
            or native["outbox_sequence"] != native["origin_sequence"]):
        raise AssertionError("native publication certificate/receipt/outbox is incomplete or mismatched")
    if previous and (native["old_root_commit"] != previous["global_commit"]
                     or native["old_path_commit"] != previous["project_commit"]):
        raise AssertionError("native publication does not continue the previously observed fixed root/path")


def expected_manifest(repo, commit, deadline):
    records = git(repo, deadline, "ls-tree", "-rz", "-t", commit).split(b"\0")
    parsed = []
    directories = [""]
    unique = {}
    for record in filter(None, records):
        meta, path = record.split(b"\t", 1)
        mode, kind, oid = meta.split()
        if kind == b"tree":
            directories.append(path.decode())
            continue
        if kind != b"blob" or mode not in (b"100644", b"100755", b"120000"):
            raise AssertionError("unexpected Git entry kind")
        parsed.append((path.decode(), mode, oid))
        unique[oid] = None
    # A single Git cat-file invocation avoids one process per file and is
    # independent of MST/2 metadata/content digest implementations.
    raw = git(repo, deadline, "cat-file", "--batch", data=b"\n".join(unique) + b"\n")
    cursor = 0
    for oid in unique:
        end = raw.index(b"\n", cursor)
        got, kind, count = raw[cursor:end].split()
        size = int(count)
        body = raw[end + 1:end + 1 + size]
        if got != oid or kind != b"blob" or len(body) != size or raw[end + 1 + size:end + 2 + size] != b"\n":
            raise AssertionError("truncated or wrong independent Git blob")
        unique[oid] = (size, "sha256:" + hashlib.sha256(body).hexdigest())
        cursor = end + 2 + size
    return {"files": [{"rel_path": path, "fs_kind": "symlink" if mode == b"120000" else
                       "executable" if mode == b"100755" else "regular", "size": unique[oid][0],
                       "content_digest": unique[oid][1]} for path, mode, oid in parsed],
            "directories": directories}


def verify_worktree(worktree, expected, deadline=None):
    wanted = {f["rel_path"]: f for f in expected["files"]}
    found = set()
    def fail(error):
        raise error
    for base, directories, names in os.walk(worktree, followlinks=False, onerror=fail):
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError("Git byte oracle exceeded its operation budget")
        if Path(base) == worktree:
            directories[:] = [d for d in directories if d != ".git"]
            names = [n for n in names if n != ".git"]
        links = [d for d in directories if (Path(base) / d).is_symlink()]
        directories[:] = [d for d in directories if d not in links]
        names.extend(links)
        for name in names:
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("Git byte oracle exceeded its operation budget")
            path = Path(base) / name
            rel = path.relative_to(worktree).as_posix()
            if rel not in wanted:
                raise AssertionError("unexpected Git worktree entry")
            file = wanted[rel]
            mode = path.lstat().st_mode
            if file["fs_kind"] == "symlink":
                if not stat.S_ISLNK(mode):
                    raise AssertionError("Git symlink mode differs from the fixed commit")
                data = os.fsencode(os.readlink(path))
            else:
                if not stat.S_ISREG(mode) or bool(mode & 0o111) != (file["fs_kind"] == "executable"):
                    raise AssertionError("Git regular/executable mode differs from the fixed commit")
                data = path.read_bytes()
            if len(data) != file["size"] or "sha256:" + hashlib.sha256(data).hexdigest() != file["content_digest"]:
                raise AssertionError("Git worktree content differs from the fixed commit")
            found.add(rel)
    if found != set(wanted):
        raise AssertionError("Git worktree omitted expected content")


def create_version(repo, round_number, version, smoke, deadline):
    prefix = f"r{round_number:02}"
    user = clean_env({"GIT_AUTHOR_NAME": "MST2 benchmark", "GIT_COMMITTER_NAME": "MST2 benchmark",
                      "GIT_AUTHOR_EMAIL": "benchmark@example.invalid", "GIT_COMMITTER_EMAIL": "benchmark@example.invalid"})
    if version == "v1":
        git(repo, deadline, "read-tree", "--empty")
        modules, buckets, files, size = (8, 1, 8, 1024) if smoke else (64, 8, 32, 16384)
        for module in range(modules):
            for bucket in range(buckets):
                directory = repo / prefix / f"m{module:03}" / f"d{bucket:02}"
                directory.mkdir(parents=True)
                for number in range(files):
                    if time.monotonic() >= deadline:
                        raise TimeoutError("fixture generation exceeded the shared deadline")
                    name = f"{prefix}/m{module:03}/d{bucket:02}/f{number:03}"
                    block = hashlib.sha256(name.encode()).digest()
                    (repo / name).write_bytes(block * (size // len(block)))
        wide = repo / prefix / "wide"
        wide.mkdir()
        for number in range(129):
            (wide / f"f{number:03}").write_bytes(hashlib.sha256(f"{prefix}/wide/{number}".encode()).digest())
        (repo / prefix / "large.bin").write_bytes(hashlib.sha256(prefix.encode()).digest() *
                                                  ((65536 if smoke else 2097152) // 32))
        git(repo, deadline, "add", "--", prefix)
    elif version == "v2":
        path = repo / prefix / "m000/d00/f000"
        body = bytearray(path.read_bytes())
        body[0] ^= 1
        path.write_bytes(body)
        git(repo, deadline, "add", "--", f"{prefix}/m000/d00/f000")
    else:
        git(repo, deadline, "mv", "--", f"{prefix}/m001", f"{prefix}/renamed-m001")
    parent = git(repo, deadline, "rev-parse", "HEAD").decode().strip()
    tree = git(repo, deadline, "write-tree").decode().strip()
    # Preserve raw empty directories and a logical directory alias in every
    # Git commit. Git checkout omits empty trees; MST/2 full closure must not.
    empty = git(repo, deadline, "mktree", data=b"").decode().strip()
    alias = git(repo, deadline, "rev-parse", f"{tree}:{prefix}/m007").decode().strip()
    entries = [r for r in git(repo, deadline, "ls-tree", "-z", tree).split(b"\0") if r]
    entries += [f"040000 tree {empty}\tempty-a".encode(), f"040000 tree {empty}\tempty-b".encode(),
                f"040000 tree {alias}\talias-{prefix}-m007".encode()]
    tree = git(repo, deadline, "mktree", "-z", data=b"\0".join(entries) + b"\0").decode().strip()
    commit = git(repo, deadline, "commit-tree", tree, "-p", parent, env=user,
                 data=f"MST2 benchmark round {round_number} {version}\n".encode()).decode().strip()
    git(repo, deadline, "update-ref", "refs/heads/main", commit, parent)
    return commit, tree


def percentile(values, percentage):
    return sorted(values)[math.ceil(len(values) * percentage / 100) - 1]


def durable_verified(operation, oracle):
    """The common end point includes each side's immediate full-byte oracle."""
    started = time.monotonic()
    measured = operation()
    oracle_started = time.monotonic()
    oracle(measured)
    measured["durable_byte_oracle_ms"] = (time.monotonic() - oracle_started) * 1000
    measured["durable_verified_ms"] = (time.monotonic() - started) * 1000
    return measured


def driver_binding(options):
    if not re.fullmatch(r"[0-9a-f]{64}", options.driver_sha256):
        raise ValueError("a fixed measurement binary SHA-256 is required")
    if options.driver.is_symlink() or not options.driver.is_file():
        raise ValueError("measurement binary must be a real file")
    if hashlib.sha256(options.driver.read_bytes()).hexdigest() != options.driver_sha256:
        raise AssertionError("measurement binary differs from the reviewed fixed artifact")


def execute(options):
    # Execution uses only the shipped v3 daemon and retained native workspaces.
    from workspace_update_bench import execute as execute_v3
    return execute_v3(options)


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--execute", action="store_true")
    p.add_argument("--isolated-deployment", action="store_true")
    p.add_argument("--base-url", required=True)
    p.add_argument("--git-url", required=True)
    p.add_argument("--database", required=True)
    p.add_argument("--instance-id", required=True)
    p.add_argument("--expect-initial-commit", required=True)
    p.add_argument("--service-pid", type=int, required=True)
    p.add_argument("--driver", type=Path, required=True)
    p.add_argument("--driver-sha256", required=True)
    p.add_argument("--run-root", type=Path, required=True)
    p.add_argument("--publication-mode", choices=("native",), default="native")
    p.add_argument("--projection-traces", action="store_true")
    p.add_argument("--profile", choices=("medium", "smoke"), default="medium")
    p.add_argument("--rounds", type=int, choices=range(3, 11), default=3)
    p.add_argument("--deadline-seconds", type=int, choices=range(60, 14401), default=14400)
    p.add_argument("--session-deadline-utc")
    p.add_argument("--work-cleanup-deadline-monotonic", type=float,
                   default=os.environ.get("MST2_WORK_CLEANUP_DEADLINE_MONOTONIC"))
    return p


if __name__ == "__main__":
    opts = parser().parse_args()
    if not opts.execute:
        endpoint_pair(opts.base_url, opts.git_url)
        print(json.dumps({"execute": False, "profile": opts.profile, "rounds": opts.rounds,
                          "scenarios": ["v1-cold", "v2-single-file", "v3-subtree-rename"],
                          "max_wall_seconds": min(opts.deadline_seconds, 14400),
                          "service_or_resource_changes": False,
                          "git_baseline": "cold depth=1 fetch, incremental shared bare ODB fetch, new detached worktree per fixed commit, retained old worktrees, shared streamed oracle",
                          "publication_mode": opts.publication_mode}, indent=2))
    else:
        try:
            execute(opts)
        except Exception as error:
            print(json.dumps(failure_record(error)), file=sys.stderr)
            raise SystemExit(1)
