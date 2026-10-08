"""Bounded, credential-free performance events for actual harness Git commands.

GNU time observes one Git process and its waited-for children. Its filesystem
counts are kernel block-operation counters, not bytes or network traffic. The
existing owning wrapper still controls process groups, deadlines and cleanup.
"""

from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import statistics
import subprocess
import sys
import tempfile
import time
import uuid


PATH_ENV = "MST2_GIT_PERFORMANCE_PATH"
CONTEXT_ENV = "MST2_GIT_PERFORMANCE_CONTEXT"
MAX_BYTES = 64 * 1024 * 1024
MAX_RECORDS = 100000
MAX_LINE_BYTES = 4096
MAX_SUMMARY_BYTES = 32 * 1024 * 1024
CONTEXT_FIELDS = {"stage", "phase", "round", "client", "version"}
STAGES = {"preflight", "server-build", "client-a-build", "client-b-build", "fences",
          "setup", "measure", "report", "cleanup", "source", "fixture", "oracle",
          "git", "publication", "validation"}
PHASES = {"setup", "fair", "diagnostic", "report", "cleanup", "preflight", "build"}
OPERATIONS = {"add", "cat-file", "checkout", "clone", "commit", "commit-tree", "config",
              "diff", "fetch", "for-each-ref", "hash-object", "index-pack", "init", "log",
              "ls-files", "ls-remote", "ls-tree", "merge-base", "mktree", "mv", "pack-objects",
              "push", "read-tree", "reset", "rev-list", "rev-parse", "rm", "show", "status",
              "symbolic-ref", "update-index", "update-ref", "version", "worktree", "write-tree",
              "other"}
STATUSES = {"completed", "failed", "timeout", "interrupted", "startup_failed", "measurement_failed"}
RESOURCE_FIELDS = ("child_elapsed_ms", "user_cpu_seconds", "system_cpu_seconds", "max_rss_kib",
                   "minor_faults", "major_faults", "filesystem_inputs_blocks", "filesystem_outputs_blocks",
                   "voluntary_context_switches", "involuntary_context_switches")
EVENT_FIELDS = {"revision", "event", "operation_id", "utc", "operation", "context", "status",
                "exit_status", "wall_ms", "resources", "resource_collection", "preparation_ms",
                "collection_ms", "start_event_write_ms"}
SEAL_FIELDS = {"revision", "event", "utc", "rows", "started", "ended", "pending", "status_counts", "sha256"}
MEASUREMENT = ("instrumented Git: wall includes preparation, start-event append, report collection and "
               "owned-process cleanup; terminal-event append is excluded; comparison timers include "
               "GNU time startup and collection; GNU time elapsed/CPU resolution is 10 ms")
RESOURCE_UNITS = {"child_elapsed_ms": "milliseconds (10 ms resolution)",
                  "user_cpu_seconds": "seconds (0.01 s resolution)",
                  "system_cpu_seconds": "seconds (0.01 s resolution)", "max_rss_kib": "KiB",
                  "minor_faults": "faults", "major_faults": "faults",
                  "filesystem_inputs_blocks": "kernel input block operations (not bytes)",
                  "filesystem_outputs_blocks": "kernel output block operations (not bytes)",
                  "voluntary_context_switches": "switches", "involuntary_context_switches": "switches"}
_CONTEXT = ContextVar("git_performance_context", default={})
_TIME = "/usr/bin/time"
_FORMAT = "mst2-git-time-v1\t%e\t%U\t%S\t%M\t%R\t%F\t%I\t%O\t%w\t%c"


def _canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")


def _utc():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _validate_utc(value):
    if type(value) is not str or not re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z", value):
        raise ValueError("invalid Git performance UTC")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _finite(value, maximum=1e12):
    return type(value) in (int, float) and math.isfinite(value) and 0 <= value <= maximum


def _count(value):
    return type(value) is int and 0 <= value <= (1 << 63) - 1


def _validate_context(value):
    if (type(value) is not dict or set(value) != CONTEXT_FIELDS
            or type(value["stage"]) is not str or value["stage"] not in STAGES
            or type(value["phase"]) is not str or value["phase"] not in PHASES
            or value["client"] not in (None, "a", "b", "git")
            or value["version"] not in (None, *(f"v{i}" for i in range(1, 11)))
            or value["round"] is not None and (type(value["round"]) is not int or not 1 <= value["round"] <= 3)):
        raise ValueError("invalid Git performance context")
    return value


def _pairs(items):
    value = {}
    for key, item in items:
        if key in value:
            raise ValueError("duplicate Git performance field")
        value[key] = item
    return value


def _json(raw):
    return json.loads(raw, object_pairs_hook=_pairs,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite Git performance value")))


def _environment_context(env=None):
    effective = os.environ if env is None else env
    raw = effective.get(CONTEXT_ENV, os.environ.get(CONTEXT_ENV))
    base = {"stage": "setup", "phase": "setup", "round": None, "client": None, "version": None}
    if raw is not None:
        if type(raw) is not str or len(raw) > 1024:
            raise ValueError("invalid Git performance context environment")
        base = dict(_validate_context(_json(raw)))
    base.update(_CONTEXT.get())
    return _validate_context(base)


@contextmanager
def context(**labels):
    """Closed labels override stale child-env snapshots and propagate to children."""
    if not set(labels) <= CONTEXT_FIELDS:
        raise ValueError("unknown Git performance context field")
    merged = _environment_context()
    merged.update(labels)
    _validate_context(merged)
    previous = os.environ.get(CONTEXT_ENV)
    token = _CONTEXT.set(dict(_CONTEXT.get(), **labels))
    os.environ[CONTEXT_ENV] = json.dumps(merged, sort_keys=True, separators=(",", ":"))
    try:
        yield
    finally:
        _CONTEXT.reset(token)
        if previous is None:
            os.environ.pop(CONTEXT_ENV, None)
        else:
            os.environ[CONTEXT_ENV] = previous


def is_git(args):
    return bool(args) and isinstance(args[0], (str, os.PathLike)) and re.split(r"[/\\]", os.fspath(args[0]))[-1].lower() in ("git", "git.exe")


def operation(args):
    """Classify only a closed Git verb; never retain argv values or config text."""
    values = list(args[1:])
    i = 0
    while i < len(values):
        item = str(values[i])
        if item in ("-C", "-c", "--git-dir", "--work-tree", "--namespace", "--config-env"):
            i += 2
        elif item.startswith(("--git-dir=", "--work-tree=", "--namespace=", "--config-env=")):
            i += 1
        elif item in ("--no-pager", "--paginate", "--bare", "--literal-pathspecs", "--no-optional-locks"):
            i += 1
        elif item in ("--version", "-v"):
            return "version"
        else:
            return item if item in OPERATIONS else "other"
    return "other"


def _reject_link(info):
    return stat.S_ISLNK(info.st_mode) or bool(getattr(info, "st_reparse_tag", 0))


def _open(path, *, write=False):
    """Walk directory fds on POSIX so substituted parent symlinks cannot win."""
    path = Path(path)
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("Git performance sink must be an absolute regular path")
    flags = os.O_RDWR | os.O_APPEND | os.O_CREAT if write else os.O_RDONLY
    flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_BINARY", 0)
    parent_fd = None
    try:
        if os.name == "posix":
            parent_fd = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            for part in path.parts[1:-1]:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
                os.close(parent_fd)
                parent_fd = child
            info = os.fstat(parent_fd)
            if info.st_mode & 0o022:
                raise ValueError("Git performance sink parent is writable by another user")
            fd = os.open(path.name, flags, 0o600, dir_fd=parent_fd)
        else:
            for item in reversed((path.parent, *path.parent.parents)):
                if _reject_link(item.lstat()):
                    raise ValueError("Git performance sink parent is a link")
            try:
                existing = path.lstat()
            except FileNotFoundError:
                existing = None
            if existing is not None and _reject_link(existing):
                raise ValueError("Git performance sink is a link")
            fd = os.open(path, flags, 0o600)
    finally:
        if parent_fd is not None:
            os.close(parent_fd)
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > MAX_BYTES
                or _reject_link(info) or os.name == "posix" and (info.st_uid != os.getuid() or info.st_mode & 0o022)):
            raise ValueError("Git performance sink is not an owned bounded regular file")
        return fd
    except BaseException:
        os.close(fd)
        raise


@contextmanager
def _lock(fd, readonly=False):
    until = time.monotonic() + .5
    while True:
        try:
            if os.name == "posix":
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            else:
                import msvcrt
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_NBRLCK if readonly else msvcrt.LK_NBLCK, 1)
            break
        except BlockingIOError:
            if time.monotonic() >= until:
                raise TimeoutError("Git performance sink append lock exceeded its bound") from None
            time.sleep(.001)
        except OSError:
            if os.name == "posix" or time.monotonic() >= until:
                raise
            time.sleep(.001)
    try:
        yield
    finally:
        if os.name == "posix":
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_UN)
        else:
            import msvcrt
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)


def _tail(fd, size):
    os.lseek(fd, max(0, size - MAX_LINE_BYTES), os.SEEK_SET)
    raw = os.read(fd, MAX_LINE_BYTES)
    if not raw.endswith(b"\n"):
        raise ValueError("Git performance stream has a torn tail")
    return _json(raw.splitlines()[-1])


def _append_locked(fd, record):
    info = os.fstat(fd)
    raw = _canonical(record)
    if (info.st_nlink != 1 or not stat.S_ISREG(info.st_mode) or info.st_size + len(raw) > MAX_BYTES
            or len(raw) > MAX_LINE_BYTES
            or os.name == "posix" and (info.st_uid != os.getuid() or info.st_mode & 0o022)):
        raise ValueError("Git performance append exceeds the bounded regular stream")
    if info.st_size:
        tail = _tail(fd, info.st_size)
        if type(tail) is not dict:
            raise ValueError("invalid Git performance tail")
        if tail.get("event") == "seal":
            raise ValueError("Git performance stream is sealed")
    if os.write(fd, raw) != len(raw):
        raise OSError("incomplete Git performance append")


def _append(fd, record):
    with _lock(fd):
        _append_locked(fd, record)


def validate_record(record):
    if type(record) is not dict or set(record) != EVENT_FIELDS or type(record["revision"]) is not int or record["revision"] != 1:
        raise ValueError("invalid Git performance event fields")
    _validate_utc(record["utc"])
    _validate_context(record["context"])
    if (type(record["operation_id"]) is not str or not re.fullmatch(r"[0-9a-f]{32}", record["operation_id"])
            or type(record["operation"]) is not str or record["operation"] not in OPERATIONS
            or record["resource_collection"] not in ("gnu-time", "wall-only")):
        raise ValueError("invalid Git performance event identity")
    if record["event"] == "begin":
        if record["status"] != "started" or any(record[key] is not None for key in
                ("exit_status", "wall_ms", "resources", "preparation_ms", "collection_ms", "start_event_write_ms")):
            raise ValueError("invalid Git performance begin event")
        return record
    if (record["event"] != "end" or type(record["status"]) is not str or record["status"] not in STATUSES
            or any(not _finite(record[key]) for key in ("wall_ms", "preparation_ms", "collection_ms", "start_event_write_ms"))
            or record["exit_status"] is not None and (type(record["exit_status"]) is not int or not -(1 << 31) <= record["exit_status"] < (1 << 32))):
        raise ValueError("invalid Git performance terminal event")
    if record["status"] == "completed" and record["exit_status"] != 0 or record["status"] == "failed" and record["exit_status"] == 0:
        raise ValueError("Git performance status contradicts exit status")
    resources = record["resources"]
    if resources is not None:
        if (record["resource_collection"] != "gnu-time" or type(resources) is not dict or set(resources) != set(RESOURCE_FIELDS)
                or any(not _finite(resources[key]) for key in RESOURCE_FIELDS[:3])
                or any(not _count(resources[key]) for key in RESOURCE_FIELDS[3:])):
            raise ValueError("invalid per-command Git resource metrics")
    if record["status"] == "completed" and record["resource_collection"] == "gnu-time" and resources is None:
        raise ValueError("completed Linux Git command lacks resource metrics")
    if record["preparation_ms"] + record["collection_ms"] > record["wall_ms"] + .01 or record["start_event_write_ms"] > record["preparation_ms"] + .01:
        raise ValueError("Git performance collection timing contradicts wall time")
    return record


def _status_counts(events):
    return {key: sum(row["event"] == "end" and row["status"] == key for row in events) for key in sorted(STATUSES)}


def _valid_status_counts(value):
    return type(value) is dict and set(value) == STATUSES and all(_count(item) for item in value.values())


def validate_records(records, require_complete=True):
    if type(require_complete) is not bool or type(records) is not list or len(records) > MAX_RECORDS:
        raise ValueError("Git performance stream exceeds record bounds")
    starts, ended = {}, set()
    digest = hashlib.sha256()
    seal = None
    for index, row in enumerate(records):
        raw = _canonical(row)
        if len(raw) > MAX_LINE_BYTES:
            raise ValueError("Git performance event exceeds line bound")
        if type(row) is dict and row.get("event") == "seal":
            if (set(row) != SEAL_FIELDS or row["revision"] != 1 or type(row["revision"]) is not int
                    or index != len(records) - 1 or row["sha256"] != digest.hexdigest()
                    or row["rows"] != index or row["started"] != len(starts) or row["ended"] != len(ended)
                    or row["pending"] != len(starts) - len(ended) or not _valid_status_counts(row["status_counts"])
                    or row["status_counts"] != _status_counts(records[:index])
                    or any(not _count(row[key]) for key in ("rows", "started", "ended", "pending"))):
                raise ValueError("Git performance seal differs from its preceding stream")
            _validate_utc(row["utc"])
            seal = row
            continue
        validate_record(row)
        key = row["operation_id"]
        if row["event"] == "begin":
            if key in starts:
                raise ValueError("duplicate Git performance invocation")
            starts[key] = row
        else:
            if key not in starts or key in ended:
                raise ValueError("orphan or duplicate Git performance terminal event")
            start = starts[key]
            if (any(row[key] != start[key] for key in ("operation", "context", "resource_collection"))
                    or _validate_utc(row["utc"]) < _validate_utc(start["utc"])):
                raise ValueError("Git performance invocation binding changed")
            ended.add(key)
        digest.update(raw)
    if require_complete and (seal is None or len(starts) != len(ended) or not starts):
        raise ValueError("complete Git performance evidence requires a sealed balanced nonempty stream")
    return records


def _decode(raw):
    if len(raw) > MAX_BYTES or raw and not raw.endswith(b"\n"):
        raise ValueError("Git performance stream is oversized or has a torn tail")
    lines = raw.splitlines()
    if len(lines) > MAX_RECORDS:
        raise ValueError("Git performance stream exceeds record bounds")
    rows = []
    for line in lines:
        if not line or len(line) + 1 > MAX_LINE_BYTES:
            raise ValueError("Git performance stream has an invalid line")
        row = _json(line)
        if _canonical(row) != line + b"\n":
            raise ValueError("Git performance stream is not canonical")
        rows.append(row)
    return rows


def validate_stream(path, require_complete=True):
    fd = _open(path)
    try:
        with _lock(fd, readonly=True):
            os.lseek(fd, 0, os.SEEK_SET)
            with os.fdopen(os.dup(fd), "rb") as stream:
                raw = stream.read(MAX_BYTES + 1)
            return validate_records(_decode(raw), require_complete)
    finally:
        os.close(fd)


read_records = validate_stream


def finalize(path):
    """Seal only after the campaign owner has stopped every possible writer."""
    fd = _open(path, write=True)
    try:
        with _lock(fd):
            os.lseek(fd, 0, os.SEEK_SET)
            with os.fdopen(os.dup(fd), "rb") as stream:
                raw = stream.read(MAX_BYTES + 1)
            rows = validate_records(_decode(raw), require_complete=False)
            if rows and rows[-1]["event"] == "seal":
                return rows[-1]
            started = sum(row["event"] == "begin" for row in rows)
            ended = sum(row["event"] == "end" for row in rows)
            seal = {"revision": 1, "event": "seal", "utc": _utc(), "rows": len(rows),
                    "started": started, "ended": ended, "pending": started - ended,
                    "status_counts": _status_counts(rows), "sha256": hashlib.sha256(raw).hexdigest()}
            _append_locked(fd, seal)
            os.fsync(fd)
            return seal
    finally:
        os.close(fd)


def _stats(values):
    values = sorted(values)
    return {"count": len(values), "min": values[0] if values else None,
            "median": statistics.median(values) if values else None,
            "p95": values[math.ceil(len(values) * .95) - 1] if values else None,
            "max": values[-1] if values else None, "total": sum(values) if values else None}


def summarize(records):
    validate_records(records, require_complete=False)
    events = [row for row in records if row["event"] != "seal"]
    grouped = {}
    for row in events:
        key = (row["operation"], json.dumps(row["context"], sort_keys=True))
        grouped.setdefault(key, []).append(row)
    groups = []
    for key, rows in sorted(grouped.items()):
        terminals = [row for row in rows if row["event"] == "end"]
        groups.append({"operation": key[0], "context": rows[0]["context"],
                       "started": sum(row["event"] == "begin" for row in rows), "ended": len(terminals),
                       "status_counts": _status_counts(rows), "wall_ms": _stats([row["wall_ms"] for row in terminals]),
                       "preparation_ms": _stats([row["preparation_ms"] for row in terminals]),
                       "collection_ms": _stats([row["collection_ms"] for row in terminals]),
                       "start_event_write_ms": _stats([row["start_event_write_ms"] for row in terminals]),
                       "resources": {field: _stats([row["resources"][field] for row in terminals if row["resources"] is not None])
                                     for field in RESOURCE_FIELDS}})
    started = sum(row["event"] == "begin" for row in events)
    ended = sum(row["event"] == "end" for row in events)
    summary = {"revision": 1, "instrumented": True, "measurement": MEASUREMENT, "resource_units": RESOURCE_UNITS,
            "sealed": bool(records and records[-1]["event"] == "seal"), "balanced": started == ended,
            "started": started, "ended": ended, "dangling": started - ended,
            "status_counts": _status_counts(events), "groups": groups}
    if len(_canonical(summary)) > MAX_SUMMARY_BYTES:
        raise ValueError("Git performance summary exceeds its byte bound")
    return summary


class _Metric:
    def __init__(self, args, env):
        self.args = args
        self.exit_status = None
        self.aborted = None
        self.fd = None
        self.report_fd = None
        self.report_path = None
        self.start = time.monotonic()
        self.preparation_ms = self.start_event_write_ms = 0.0
        self.enabled = False
        effective = os.environ if env is None else env
        sink = effective.get(PATH_ENV, os.environ.get(PATH_ENV))
        if not is_git(args) or sink is None:
            return
        if type(sink) is not str or not sink:
            raise ValueError("invalid Git performance sink environment")
        self.context = _environment_context(env)
        self.collection = "gnu-time" if sys.platform == "linux" else "wall-only"
        self.operation = operation(args)
        self.id = uuid.uuid4().hex
        self.fd = _open(sink, write=True)
        self.enabled = True
        try:
            row = self._event("begin", "started")
            before = time.monotonic()
            _append(self.fd, row)
            self.start_event_write_ms = (time.monotonic() - before) * 1000
            if self.collection == "gnu-time":
                if not Path(_TIME).is_file() or not os.access(_TIME, os.X_OK):
                    raise ValueError("Linux Git resource collection requires GNU time")
                self.report_fd, self.report_path = tempfile.mkstemp(prefix=".git-time-", dir=str(Path(sink).parent))
                self.args = [_TIME, "-q", "-f", _FORMAT, "-o", self.report_path, "--", *args]
            self.preparation_ms = (time.monotonic() - self.start) * 1000
        except BaseException:
            self.preparation_ms = (time.monotonic() - self.start) * 1000
            try:
                self.finish("measurement_failed")
            finally:
                self.close()
            raise

    def _event(self, event, status):
        return {"revision": 1, "event": event, "operation_id": self.id, "utc": _utc(),
                "operation": self.operation, "context": self.context, "status": status,
                "exit_status": None, "wall_ms": None, "resources": None, "resource_collection": self.collection,
                "preparation_ms": None, "collection_ms": None, "start_event_write_ms": None}

    def complete(self, status):
        if type(status) is not int or not -(1 << 31) <= status < (1 << 32):
            raise ValueError("invalid Git performance command status")
        if self.exit_status is not None:
            raise ValueError("Git performance command completed twice")
        self.exit_status = status

    def abort(self, error):
        """Preserve the original category before an owner wraps its exception."""
        if self.aborted is not None:
            return
        if isinstance(error, (TimeoutError, subprocess.TimeoutExpired)):
            self.aborted = "timeout"
        elif isinstance(error, (KeyboardInterrupt, SystemExit)):
            self.aborted = "interrupted"
        elif isinstance(error, (FileNotFoundError, PermissionError)):
            self.aborted = "startup_failed"
        else:
            self.aborted = "failed"

    def _resources(self):
        if self.report_fd is None:
            return None
        info = os.fstat(self.report_fd)
        current = Path(self.report_path).lstat()
        if (info.st_ino != current.st_ino or info.st_dev != current.st_dev or info.st_nlink != 1
                or not stat.S_ISREG(info.st_mode) or info.st_size > 1024 or _reject_link(current)):
            raise ValueError("Git resource report identity changed")
        os.lseek(self.report_fd, 0, os.SEEK_SET)
        raw = os.read(self.report_fd, 1025)
        if not raw:
            return None
        fields = raw.decode("ascii").strip().split("\t")
        if len(fields) != 11 or fields[0] != "mst2-git-time-v1":
            raise ValueError("invalid GNU time resource report")
        values = fields[1:]
        if (any(re.fullmatch(r"[0-9]+(?:[.,][0-9]{1,6})?", value) is None for value in values[:3])
                or any(re.fullmatch(r"[0-9]+", value) is None for value in values[3:])):
            raise ValueError("invalid GNU time resource value")
        result = dict(zip(RESOURCE_FIELDS, [*(float(value.replace(",", ".")) for value in values[:3]),
                                          *(int(value) for value in values[3:])]))
        result["child_elapsed_ms"] *= 1000
        return result

    def finish(self, failure=None):
        if not self.enabled:
            return
        before = time.monotonic()
        collection_error = False
        try:
            resources = self._resources()
        except (OSError, UnicodeError, ValueError):
            resources = None
            collection_error = True
        missing = self.collection == "gnu-time" and resources is None
        status = self.aborted or failure or ("completed" if self.exit_status == 0 else "failed")
        if failure is None and self.aborted is None and (self.exit_status is None or collection_error or missing):
            status = "measurement_failed"
        row = self._event("end", status)
        row.update(exit_status=self.exit_status, resources=resources,
                   preparation_ms=self.preparation_ms, start_event_write_ms=self.start_event_write_ms,
                   collection_ms=(time.monotonic() - before) * 1000)
        row["wall_ms"] = (time.monotonic() - self.start) * 1000
        validate_record(row)
        _append(self.fd, row)
        if status == "measurement_failed" and failure is None:
            raise ValueError("Git performance resource collection did not complete")

    def close(self):
        try:
            if self.report_fd is not None:
                os.close(self.report_fd)
            if self.report_path is not None:
                try:
                    Path(self.report_path).unlink()
                except FileNotFoundError:
                    pass
        finally:
            if self.fd is not None:
                os.close(self.fd)


@contextmanager
def measure(args, env=None):
    """Wrap only Git. Never signal, reap, change deadlines, or retain secrets."""
    metric = _Metric(args, env)
    try:
        try:
            yield metric
        except BaseException as error:
            if isinstance(error, subprocess.CalledProcessError) and metric.exit_status is None:
                metric.complete(error.returncode)
            if metric.aborted is None:
                metric.abort(error)
            try:
                metric.finish(metric.aborted)
            except BaseException:
                # Owned cleanup's original failure must remain the exception.
                pass
            raise
        else:
            metric.finish()
    finally:
        if sys.exc_info()[0] is None:
            metric.close()
        else:
            try:
                metric.close()
            except BaseException:
                pass
