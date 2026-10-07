"""The same streamed file oracle with explicit directory semantics on both sides."""

import hashlib
import os
from pathlib import Path
import stat
import time


READ_BYTES = 64 * 1024


def _check_deadline(deadline):
    if time.monotonic() >= deadline:
        raise TimeoutError("workspace content oracle exceeded its operation deadline")


def _relative(value, root=False):
    if type(value) is not str or (not value and not root):
        raise AssertionError("invalid oracle relative path")
    if value and (value.startswith("/") or any(part in ("", ".", "..") for part in value.split("/"))):
        raise AssertionError("invalid oracle relative path")
    return value


def directory_sets(expected):
    """Git omits raw empty trees; compare its checkout to actual file ancestors.

    Scorpio must expose the entire descriptor tree. Return the difference for
    separate reporting; never make extra or missing directories invisible.
    """
    full = {_relative(path, root=True) for path in expected["directories"]}
    if "" not in full or len(full) != len(expected["directories"]):
        raise AssertionError("invalid oracle directory manifest")
    materialized = {""}
    files = set()
    for file in expected["files"]:
        rel = _relative(file["rel_path"])
        if rel in files or rel in full:
            raise AssertionError("ambiguous oracle entry")
        files.add(rel)
        parts = rel.split("/")
        materialized.update("/".join(parts[:index]) for index in range(1, len(parts)))
    if not materialized.issubset(full):
        raise AssertionError("oracle file ancestor is absent")
    for directory in full - {""}:
        if directory.rpartition("/")[0] not in full:
            raise AssertionError("oracle directory ancestor is absent")
    return full, materialized, full - materialized


def verify_workspace(root, expected, deadline, *, git_checkout=False):
    """Include this call in each operation's timer, immediately after completion.

    Verify all kinds, execute bits, lengths, SHA256 digests and the exact
    directory set. Read regular files through O_NOFOLLOW descriptors in fixed
    64 KiB chunks; never fetch arbitrary file bodies into a single buffer.
    """
    root = Path(root)
    full_directories, git_directories, omitted = directory_sets(expected)
    wanted_directories = git_directories if git_checkout else full_directories
    files = {file["rel_path"]: file for file in expected["files"]}
    found_files, found_directories = set(), set()
    read_bytes = 0
    read_calls = 0

    def walk_error(error):
        raise error

    for base, directories, names, parent in os.fwalk(root, follow_symlinks=False, onerror=walk_error):
        _check_deadline(deadline)
        relative = Path(base).relative_to(root).as_posix()
        relative = "" if relative == "." else relative
        if relative not in wanted_directories:
            raise AssertionError("unexpected workspace directory")
        found_directories.add(relative)
        if git_checkout and not relative:
            # A detached Git worktree has a root administration file. This
            # exception belongs only to the Git baseline, never to Scorpio.
            if ".git" in names and not stat.S_ISREG(os.stat(
                    ".git", dir_fd=parent, follow_symlinks=False).st_mode):
                raise AssertionError("detached Git administration entry must be a regular file")
            names = [name for name in names if name != ".git"]
        links = [name for name in directories
                 if stat.S_ISLNK(os.stat(name, dir_fd=parent, follow_symlinks=False).st_mode)]
        directories[:] = [name for name in directories if name not in links]
        names.extend(links)
        for name in names:
            _check_deadline(deadline)
            rel = f"{relative}/{name}" if relative else name
            if rel not in files or rel in found_files:
                raise AssertionError("unexpected workspace entry")
            file = files[rel]
            before = os.stat(name, dir_fd=parent, follow_symlinks=False)
            digest = hashlib.sha256()
            count = 0
            if file["fs_kind"] == "symlink":
                if not stat.S_ISLNK(before.st_mode):
                    raise AssertionError("workspace symlink kind mismatch")
                raw = os.fsencode(os.readlink(name, dir_fd=parent))
                digest.update(raw)
                count = len(raw)
            else:
                if (file["fs_kind"] not in ("regular", "executable")
                        or not stat.S_ISREG(before.st_mode)
                        or bool(before.st_mode & 0o111) != (file["fs_kind"] == "executable")
                        or before.st_size != file["size"]):
                    raise AssertionError("workspace file kind or size mismatch")
                fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
                             dir_fd=parent)
                try:
                    opened = os.fstat(fd)
                    if (not stat.S_ISREG(opened.st_mode)
                            or (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino)):
                        raise AssertionError("workspace entry changed while opening")
                    while True:
                        _check_deadline(deadline)
                        chunk = os.read(fd, READ_BYTES)
                        read_calls += 1
                        if not chunk:
                            break
                        count += len(chunk)
                        if count > file["size"]:
                            raise AssertionError("workspace file grew during verification")
                        digest.update(chunk)
                    after = os.fstat(fd)
                    if (opened.st_size, opened.st_mtime_ns, opened.st_ctime_ns) != (
                            after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                        raise AssertionError("workspace file changed during verification")
                finally:
                    os.close(fd)
            _check_deadline(deadline)
            after_path = os.stat(name, dir_fd=parent, follow_symlinks=False)
            if (before.st_dev, before.st_ino, before.st_mode, before.st_size,
                    before.st_mtime_ns, before.st_ctime_ns) != (
                    after_path.st_dev, after_path.st_ino, after_path.st_mode, after_path.st_size,
                    after_path.st_mtime_ns, after_path.st_ctime_ns):
                raise AssertionError("workspace path changed during verification")
            if count != file["size"] or "sha256:" + digest.hexdigest() != file["content_digest"]:
                raise AssertionError("workspace bytes differ from the fixed commit")
            read_bytes += count
            found_files.add(rel)
    if found_files != set(files) or found_directories != wanted_directories:
        raise AssertionError("workspace omitted fixed snapshot entries")
    _check_deadline(deadline)
    return {
        "verified_files": len(found_files), "verified_directories": len(found_directories),
        "verified_bytes": read_bytes, "regular_read_calls": read_calls,
        "raw_empty_tree_directories_omitted_by_git": sorted(omitted) if git_checkout else [],
    }
