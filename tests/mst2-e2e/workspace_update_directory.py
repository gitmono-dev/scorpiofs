"""A real root/nested directory-open and readdir probe, without file reads."""

import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import time

from workspace_update_oracle import directory_sets


KIND = "root-and-nested-readdir"
TIMING_FIELDS = {"root_open", "root_readdir", "nested_open", "nested_readdir", "total"}
FIELDS = {"revision", "kind", "git_checkout", "opened_directories", "root", "nested", "timings_ms"}


def _hash_names(names):
    return hashlib.sha256(json.dumps(sorted(names), ensure_ascii=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _children(expected, directories, parent):
    prefix = parent + "/" if parent else ""
    paths = [*directories, *(file["rel_path"] for file in expected["files"])]
    return sorted({path[len(prefix):] for path in paths if path.startswith(prefix)
                   and path != parent and "/" not in path[len(prefix):]})


def plan(expected, *, git_checkout=False):
    if type(git_checkout) is not bool:
        raise ValueError("invalid directory probe Git mode")
    full, materialized, _ = directory_sets(expected)
    candidates = sorted(materialized - {""})
    nested = candidates[0] if candidates else None
    directories = materialized if git_checkout else full
    return {"git_checkout": git_checkout, "nested_path": nested,
            "root_names": _children(expected, directories, ""),
            "nested_names": _children(expected, directories, nested) if nested is not None else None}


def _semantics(value):
    nested = value["nested_path"]
    return {"revision": 1, "kind": KIND, "git_checkout": value["git_checkout"],
            "opened_directories": 2 if nested is not None else 1,
            "root": {"entries": len(value["root_names"]), "names_sha256": _hash_names(value["root_names"])},
            "nested": ({"path_sha256": hashlib.sha256(nested.encode("utf-8")).hexdigest(),
                        "entries": len(value["nested_names"]), "names_sha256": _hash_names(value["nested_names"])}
                       if nested is not None else None)}


def expected_record(expected, *, git_checkout=False):
    """Semantic fields only; real timings are never fabricated from a manifest."""
    return _semantics(plan(expected, git_checkout=git_checkout))


def validate_plan_record(value, expected_plan):
    validate_record(value)
    _validate_plan(expected_plan)
    if {key: value[key] for key in FIELDS - {"timings_ms"}} != _semantics(expected_plan):
        raise ValueError("directory probe differs from its fixed plan")
    return value


def validate_record(value, expected=None, *, git_checkout=None):
    if (type(value) is not dict or set(value) != FIELDS or type(value["revision"]) is not int or value["revision"] != 1
            or value["kind"] != KIND or type(value["git_checkout"]) is not bool
            or type(value["opened_directories"]) is not int or value["opened_directories"] not in (1, 2)
            or git_checkout is not None and value["git_checkout"] is not git_checkout):
        raise ValueError("invalid directory probe shape")
    for name, keys in (("root", {"entries", "names_sha256"}),
                       ("nested", {"path_sha256", "entries", "names_sha256"})):
        row = value[name]
        if name == "nested" and row is None:
            if value["opened_directories"] != 1:
                raise ValueError("directory probe omitted its nested directory")
            continue
        if (type(row) is not dict or set(row) != keys or type(row["entries"]) is not int
                or not 0 <= row["entries"] <= 1000000
                or any(type(row[key]) is not str or re.fullmatch(r"[0-9a-f]{64}", row[key]) is None for key in keys - {"entries"})
                or name == "nested" and value["opened_directories"] != 2):
            raise ValueError("invalid directory probe namespace evidence")
    timings = value["timings_ms"]
    if (type(timings) is not dict or set(timings) != TIMING_FIELDS
            or any(type(item) not in (int, float) or not math.isfinite(item) or not 0 <= item <= 1e10 for item in timings.values())
            or sum(timings[key] for key in TIMING_FIELDS - {"total"}) > timings["total"] + .01
            or value["nested"] is None and (timings["nested_open"] != 0 or timings["nested_readdir"] != 0)):
        raise ValueError("invalid directory probe timings")
    if expected is not None:
        wanted = expected_record(expected, git_checkout=value["git_checkout"])
        if {key: value[key] for key in FIELDS - {"timings_ms"}} != wanted:
            raise ValueError("directory probe differs from the expected namespace")
    return value


def _deadline(deadline):
    if type(deadline) not in (int, float) or not math.isfinite(deadline) or time.monotonic() >= deadline:
        raise TimeoutError("directory probe exceeded its original deadline")


def _validate_plan(value):
    if type(value) is not dict or set(value) != {"git_checkout", "nested_path", "root_names", "nested_names"} or type(value["git_checkout"]) is not bool:
        raise ValueError("invalid directory probe plan")
    nested = value["nested_path"]
    if nested is not None and (type(nested) is not str or not nested or nested.startswith("/")
                               or any(part in ("", ".", "..") for part in nested.split("/"))):
        raise ValueError("invalid directory probe nested path")
    for key in ("root_names", "nested_names"):
        names = value[key]
        if key == "nested_names" and nested is None and names is None:
            continue
        if (type(names) is not list or len(names) > 1000000 or any(type(name) is not str or not name
                or name in (".", "..") or "/" in name or "\0" in name for name in names)
                or names != sorted(set(names))):
            raise ValueError("invalid directory probe expected names")


def verify(root, value, deadline):
    """Run in the owned child: enter both directories, list names, read no body."""
    _validate_plan(value)
    _deadline(deadline)
    root = Path(root)
    timings = dict.fromkeys(TIMING_FIELDS, 0.0)
    started = time.monotonic()
    current = os.open(".", os.O_RDONLY | os.O_DIRECTORY) if os.name == "posix" else None
    root_fd = nested_fd = None
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        before = time.monotonic()
        if root.is_symlink() or not stat.S_ISDIR(root.lstat().st_mode):
            raise ValueError("directory probe root is not a real directory")
        if os.name == "posix":
            root_fd = os.open(root, flags)
            os.fchdir(root_fd)
        timings["root_open"] = (time.monotonic() - before) * 1000
        _deadline(deadline)
        before = time.monotonic()
        names = os.listdir(root_fd if os.name == "posix" else root)
        if value["git_checkout"]:
            names = [name for name in names if name != ".git"]
        if sorted(names) != value["root_names"]:
            raise ValueError("directory probe root entries differ")
        timings["root_readdir"] = (time.monotonic() - before) * 1000
        _deadline(deadline)
        if value["nested_path"] is not None:
            before = time.monotonic()
            if os.name == "posix":
                nested_fd = os.dup(root_fd)
                for part in value["nested_path"].split("/"):
                    _deadline(deadline)
                    child = os.open(part, flags, dir_fd=nested_fd)
                    os.close(nested_fd)
                    nested_fd = child
                os.fchdir(nested_fd)
            else:
                nested_path = root
                for part in value["nested_path"].split("/"):
                    nested_path = nested_path / part
                    if nested_path.is_symlink() or not stat.S_ISDIR(nested_path.lstat().st_mode):
                        raise ValueError("directory probe nested entry is not a real directory")
            timings["nested_open"] = (time.monotonic() - before) * 1000
            _deadline(deadline)
            before = time.monotonic()
            names = os.listdir(nested_fd if os.name == "posix" else nested_path)
            if sorted(names) != value["nested_names"]:
                raise ValueError("directory probe nested entries differ")
            timings["nested_readdir"] = (time.monotonic() - before) * 1000
            _deadline(deadline)
        timings["total"] = (time.monotonic() - started) * 1000
        return validate_record(dict(_semantics(value), timings_ms=timings))
    finally:
        if current is not None:
            os.fchdir(current)
            os.close(current)
        if nested_fd is not None:
            os.close(nested_fd)
        if root_fd is not None:
            os.close(root_fd)
