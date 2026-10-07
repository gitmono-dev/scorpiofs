"""Numeric, external Linux resource measurements for the owned v3 daemon."""

import os
from pathlib import Path
import re
import stat
import threading
import time


IO_FIELDS = ("rchar", "wchar", "syscr", "syscw", "read_bytes", "write_bytes",
             "cancelled_write_bytes")


class ResourceMeasurementError(RuntimeError):
    """Closed error codes, never a proc path, private body or configuration."""


def _deadline(deadline):
    if deadline is not None and time.monotonic() >= deadline:
        raise TimeoutError("resource observation exceeded its original deadline")


def _numbers(text, fields, unit=""):
    values = {}
    for line in text.splitlines():
        key, separator, value = line.partition(":")
        if key not in fields:
            continue
        match = re.fullmatch(r"\s*([0-9]+)" + (r"\s+" + unit if unit else "") + r"\s*", value)
        if not separator or key in values or match is None:
            raise ResourceMeasurementError("invalid_process_metrics")
        values[key] = int(match[1])
    if set(values) != set(fields):
        raise ResourceMeasurementError("incomplete_process_metrics")
    return values


class ProcessResources:
    """Constant-memory RSS sampling; VmHWM remains a daemon lifetime metric."""

    def __init__(self, pid, expected_starttime, expected_uid, proc_root=Path("/proc"),
                 interval_seconds=0.02):
        if (type(pid) is not int or pid <= 0 or type(expected_uid) is not int
                or expected_uid < 0 or not re.fullmatch(r"[1-9][0-9]*", str(expected_starttime))
                or not 0.001 <= interval_seconds <= 1):
            raise ValueError("invalid resource owner or sampling interval")
        self.pid, self.started, self.uid = pid, str(expected_starttime), expected_uid
        self.proc_root = Path(proc_root)
        self.interval = interval_seconds
        self._lock, self._stop = threading.Lock(), threading.Event()
        self._thread = None
        self._error = None
        self._latest = None
        self._peak, self._count, self._interval_start = 0, 0, None

    def _identity(self):
        directory = self.proc_root / str(self.pid)
        fields = directory.joinpath("stat").read_text(encoding="ascii").rsplit(") ", 1)[1].split()
        if (len(fields) < 20 or fields[19] != self.started or fields[0] in ("Z", "X")
                or directory.stat().st_uid != self.uid):
            raise ResourceMeasurementError("owned_process_changed")
        return directory

    def _read(self):
        try:
            directory = self._identity()
            memory = _numbers(directory.joinpath("status").read_text(encoding="ascii"),
                              ("VmRSS", "VmHWM"), "kB")
            counters = _numbers(directory.joinpath("io").read_text(encoding="ascii"), IO_FIELDS)
            self._identity()
            if memory["VmHWM"] < memory["VmRSS"]:
                raise ResourceMeasurementError("invalid_process_metrics")
            return {"pid": self.pid, "starttime_ticks": self.started, "uid": self.uid,
                    "monotonic_ns": time.monotonic_ns(), "rss_bytes": memory["VmRSS"] * 1024,
                    "vm_hwm_bytes": memory["VmHWM"] * 1024, "io": counters}
        except ResourceMeasurementError:
            raise
        except (OSError, UnicodeError, IndexError, ValueError):
            raise ResourceMeasurementError("process_metrics_unavailable") from None

    def _record(self):
        # Reads are serialized with snapshot/reset so a delayed sample cannot
        # be attributed to a subsequent measurement interval.
        sample = self._read()
        self._latest = sample
        self._peak = max(self._peak, sample["rss_bytes"])
        self._count += 1
        if self._interval_start is None:
            self._interval_start = sample["monotonic_ns"]

    def _run(self):
        while not self._stop.wait(self.interval):
            with self._lock:
                try:
                    self._record()
                except ResourceMeasurementError as error:
                    self._error = error
                    self._stop.set()

    def start(self):
        if self._thread is not None:
            raise RuntimeError("resource sampler already started")
        with self._lock:
            self._record()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def snapshot(self, deadline=None):
        _deadline(deadline)
        if self._thread is None:
            raise RuntimeError("resource sampler was not started")
        with self._lock:
            if self._error is not None:
                raise self._error
            self._record()
            result = dict(self._latest, interval_started_ns=self._interval_start,
                          sampled_rss_peak_bytes=self._peak, sample_count=self._count)
            self._peak, self._count = self._latest["rss_bytes"], 1
            self._interval_start = self._latest["monotonic_ns"]
            _deadline(deadline)
            return result

    def close(self, deadline=None):
        if self._thread is None:
            raise RuntimeError("resource sampler was not started")
        self._stop.set()
        remaining = 1 if deadline is None else max(0, min(1, deadline - time.monotonic()))
        self._thread.join(timeout=remaining)
        if self._thread.is_alive():
            raise ResourceMeasurementError("resource_sampler_not_stopped")
        return self.snapshot(deadline)


def io_delta(before, after):
    if (any(before[key] != after[key] for key in ("pid", "starttime_ticks", "uid"))
            or after["monotonic_ns"] < before["monotonic_ns"]):
        raise ResourceMeasurementError("resource_interval_owner_changed")
    result = {key: after["io"][key] - before["io"][key] for key in IO_FIELDS}
    if any(value < 0 for value in result.values()):
        raise ResourceMeasurementError("resource_counter_regressed")
    return result


def _mountpoints(proc_root):
    result = set()
    for line in Path(proc_root).joinpath("self/mountinfo").read_text(encoding="utf-8").splitlines():
        fields = line.split()
        if len(fields) < 10 or "-" not in fields:
            raise ResourceMeasurementError("invalid_mount_metrics")
        value = re.sub(r"\\(040|011|012|134)",
                       lambda match: chr(int(match[1], 8)), fields[4])
        point = Path(value)
        if not point.is_absolute():
            raise ResourceMeasurementError("invalid_mount_metrics")
        result.add(point)
    return result


def disk_usage(root, excluded=(), deadline=None, proc_root=Path("/proc"), *, seen=None):
    """Quiescent metadata walk, excluding nested mounts and symlink targets.

    Apparent/allocated file stocks are not physical writes or an atomic
    filesystem snapshot. The shared inode set also deduplicates hard links.
    """
    _deadline(deadline)
    root = Path(root)
    if root.is_symlink():
        raise ResourceMeasurementError("invalid_disk_root")
    root = root.resolve(strict=True)
    identity = root.stat()
    if not stat.S_ISDIR(identity.st_mode):
        raise ResourceMeasurementError("invalid_disk_root")
    # Excluding mountpoints by pathname before lstat avoids touching live FUSE
    # namespaces, including same-device bind mounts.
    omitted = _mountpoints(proc_root) | {Path(os.path.abspath(path)) for path in excluded}
    _deadline(deadline)
    if root in omitted:
        raise ResourceMeasurementError("mounted_disk_root")
    result = {"regular_apparent_bytes": 0, "regular_allocated_bytes": 0,
              "directory_allocated_bytes": 0, "symlink_apparent_bytes": 0,
              "symlink_allocated_bytes": 0, "files": 0, "directories": 0,
              "symlinks": 0, "duplicate_inodes": 0, "excluded_subtrees": 0,
              "special_files": 0}
    pending = [root]
    if seen is None:
        seen = set()
    while pending:
        _deadline(deadline)
        path = pending.pop()
        if path != root and path in omitted:
            result["excluded_subtrees"] += 1
            continue
        info = path.lstat()
        if info.st_dev != identity.st_dev:
            result["excluded_subtrees"] += 1
            continue
        key = (info.st_dev, info.st_ino)
        if key in seen:
            result["duplicate_inodes"] += 1
            continue
        seen.add(key)
        allocated = getattr(info, "st_blocks", None)
        if allocated is None:
            raise ResourceMeasurementError("disk_allocation_unavailable")
        allocated *= 512
        if stat.S_ISREG(info.st_mode):
            result["files"] += 1
            result["regular_apparent_bytes"] += info.st_size
            result["regular_allocated_bytes"] += allocated
        elif stat.S_ISDIR(info.st_mode):
            result["directories"] += 1
            result["directory_allocated_bytes"] += allocated
            descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                opened = os.fstat(descriptor)
                if (opened.st_dev, opened.st_ino) != key:
                    raise ResourceMeasurementError("disk_directory_changed")
                with os.scandir(descriptor) as entries:
                    for entry in entries:
                        _deadline(deadline)
                        pending.append(path / entry.name)
            finally:
                os.close(descriptor)
        elif stat.S_ISLNK(info.st_mode):
            result["symlinks"] += 1
            result["symlink_apparent_bytes"] += info.st_size
            result["symlink_allocated_bytes"] += allocated
        else:
            result["special_files"] += 1
    final = root.lstat()
    if (final.st_dev, final.st_ino, final.st_mode) != (identity.st_dev, identity.st_ino, identity.st_mode):
        raise ResourceMeasurementError("disk_root_changed")
    _deadline(deadline)
    return result
