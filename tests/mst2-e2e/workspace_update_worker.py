"""Owned v3 workspace update and fair Git baseline driver.

This module is deliberately an evidence driver rather than a convenience
client.  Every operation is bound to one absolute deadline, every control
response is shape checked, and the filesystem oracle runs inside the measured
operation.  The worker owns no service: the caller owns the daemon and may
shut it down after :meth:`stop` has retired the workspaces.
"""

from copy import deepcopy
import base64
import binascii
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import select
import stat
import subprocess
import sys
import time
from urllib.parse import quote, urlsplit
import uuid

import commit_update_budget as budget
from workspace_update_daemon import mounts_under


STATUS_FIELDS = frozenset({
    "workspace_id", "generation", "snapshot_id", "mountpoint", "mount_state",
    "metadata_ready", "hydration_state", "dirty_state", "lease_state",
    "local_pin_state", "last_error",
})
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
SID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
POLL_SECONDS = 0.025
HTTP_BODY_LIMIT = 2 * 1024 * 1024
COMMAND_OUTPUT_LIMIT = 8 * 1024 * 1024
DIRTY_SENTINEL = ".scorpiofs-worker-dirty-upper-sentinel"
DIRTY_BYTES = b"workspace-v3-worker-dirty-upper\n"


def _owned_mounts(root):
    if sys.platform != "linux":
        return []
    try:
        return mounts_under(root)
    except (FileNotFoundError, OSError):
        raise WorkerError("Linux mount ownership inventory is unavailable") from None


class WorkerError(RuntimeError):
    """Closed diagnostics for a failed worker operation."""


def _check_deadline(deadline):
    if type(deadline) not in (int, float) or time.monotonic() >= deadline:
        raise TimeoutError("workspace update exceeded its absolute operation deadline")


def _canonical_uuid(value, label):
    if type(value) is not str or UUID_RE.fullmatch(value) is None:
        raise WorkerError(f"invalid {label}")
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError):
        raise WorkerError(f"invalid {label}") from None
    if parsed.int == 0 or str(parsed) != value:
        raise WorkerError(f"invalid {label}")
    return value


def _json_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise WorkerError("duplicate JSON object key")
        result[key] = value
    return result


def _decode_json(raw):
    if type(raw) is not bytes or len(raw) > HTTP_BODY_LIMIT:
        raise WorkerError("HTTP JSON body is invalid")
    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=_json_pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(WorkerError("invalid JSON constant")))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, WorkerError):
        raise WorkerError("HTTP JSON body is invalid") from None


def _decode_anchor_json(raw):
    if type(raw) is not bytes or len(raw) > COMMAND_OUTPUT_LIMIT + HTTP_BODY_LIMIT:
        raise WorkerError("worker anchor JSON body is invalid")
    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=_json_pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(WorkerError("invalid JSON constant")))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, WorkerError):
        raise WorkerError("worker anchor JSON body is invalid") from None


def _loopback_url(url, *, path=None):
    parsed = urlsplit(url)
    if (parsed.scheme != "http" or parsed.hostname != "127.0.0.1"
            or parsed.port is None or parsed.port < 1 or parsed.port > 65535
            or parsed.username is not None or parsed.password is not None
            or parsed.query or parsed.fragment):
        raise ValueError("worker accepts only explicit credential-free loopback HTTP")
    if path is not None and parsed.path not in ("", "/"):
        raise ValueError("daemon URL must not include a request path")
    return parsed


class _NoRedirectHTTP:
    """Small HTTP client with no redirect handling and one absolute deadline."""

    def __init__(self, base_url):
        parsed = _loopback_url(base_url, path="/")
        self.host = parsed.hostname
        self.port = parsed.port

    def request(self, method, path, deadline, body=None, expected=(200,)):
        _check_deadline(deadline)
        if type(path) is not str or not path.startswith("/") or "?" in path or "#" in path:
            raise ValueError("worker HTTP path is invalid")
        payload = None
        headers = {"Accept": "application/json", "Connection": "close"}
        if body is not None:
            payload = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
            if len(payload) > HTTP_BODY_LIMIT:
                raise WorkerError("worker HTTP request is too large")
            headers["Content-Type"] = "application/json"
            headers["Content-Length"] = str(len(payload))
        timeout = min(30.0, max(0.001, deadline - time.monotonic()))
        connection = http.client.HTTPConnection(self.host, self.port, timeout=timeout)
        try:
            connection.request(method, path, body=payload, headers=headers)
            response = connection.getresponse()
            if 300 <= response.status < 400:
                raise WorkerError("worker HTTP redirects are rejected")
            length = response.getheader("Content-Length")
            if length is not None:
                try:
                    if int(length) < 0 or int(length) > HTTP_BODY_LIMIT:
                        raise WorkerError("worker HTTP body is too large")
                except ValueError:
                    raise WorkerError("worker HTTP content length is invalid") from None
            # Read in bounded chunks while shortening the socket timeout to
            # the same absolute deadline on every iteration.  A peer that
            # drips one byte per timeout interval cannot extend this request.
            raw_parts = []
            total = 0
            expected_length = int(length) if length is not None else None
            while True:
                _check_deadline(deadline)
                remaining = max(0.001, deadline - time.monotonic())
                socket = getattr(connection, "sock", None)
                if socket is not None:
                    socket.settimeout(remaining)
                chunk = response.read(min(64 * 1024, HTTP_BODY_LIMIT + 1 - total))
                if not chunk:
                    break
                raw_parts.append(chunk)
                total += len(chunk)
                if total > HTTP_BODY_LIMIT:
                    break
                if expected_length is not None and total >= expected_length:
                    break
            raw = b"".join(raw_parts)
            if len(raw) > HTTP_BODY_LIMIT:
                raise WorkerError("worker HTTP body is too large")
            if expected_length is not None and len(raw) != expected_length:
                raise WorkerError("worker HTTP body length is truncated")
            if response.status not in expected:
                raise WorkerError(f"worker HTTP status {response.status} was not accepted")
            if response.status != 204:
                ctype = (response.getheader("Content-Type") or "").split(";", 1)[0].strip().lower()
                if ctype != "application/json":
                    raise WorkerError("worker HTTP response is not JSON")
                value = _decode_json(raw)
            else:
                if raw:
                    raise WorkerError("worker HTTP no-content response has a body")
                value = None
            _check_deadline(deadline)
            return value
        except (TimeoutError, WorkerError):
            raise
        except (OSError, http.client.HTTPException):
            raise WorkerError("worker HTTP request failed") from None
        finally:
            connection.close()


def _status(value):
    if type(value) is not dict or set(value) != STATUS_FIELDS:
        raise WorkerError("workspace status shape is invalid")
    _canonical_uuid(value["workspace_id"], "workspace id")
    _canonical_uuid(value["generation"], "workspace generation")
    sid = value["snapshot_id"]
    if sid is not None and (type(sid) is not str or SID_RE.fullmatch(sid) is None):
        raise WorkerError("workspace snapshot id is invalid")
    if (type(value["mountpoint"]) is not str or not value["mountpoint"].startswith("/")
            or "\0" in value["mountpoint"]):
        raise WorkerError("workspace mountpoint is invalid")
    for key, choices in {
        "mount_state": {"creating", "mounted", "retiring", "unmounted", "failed"},
        "hydration_state": {"idle", "running", "complete", "cancelled", "failed"},
        "dirty_state": {"clean", "dirty", "unknown"},
        "lease_state": {"not_resolved", "granted_locally", "expired", "failed"},
        "local_pin_state": {"incomplete", "complete_snapshot", "file_closure_only",
                             "revoking", "released", "unknown"},
    }.items():
        if type(value[key]) is not str or value[key] not in choices:
            raise WorkerError(f"workspace {key} is invalid")
    if type(value["metadata_ready"]) is not bool:
        raise WorkerError("workspace metadata_ready is invalid")
    if value["last_error"] is not None and type(value["last_error"]) is not str:
        raise WorkerError("workspace last_error is invalid")
    return value


def _safe_mount_path(root, mountpoint):
    root = Path(root)
    mount = Path(mountpoint)
    if not mount.is_absolute() or mount.is_symlink():
        raise WorkerError("workspace mountpoint is not a private absolute path")
    try:
        relative = mount.relative_to(root)
    except ValueError:
        raise WorkerError("workspace mountpoint escaped the owned root") from None
    if not relative.parts or any(part in ("", ".", "..") for part in relative.parts):
        raise WorkerError("workspace mountpoint path is invalid")
    cursor = root
    for part in relative.parts:
        cursor = cursor / part
        try:
            if cursor.is_symlink():
                raise WorkerError("workspace mountpoint path contains a symlink")
        except OSError:
            raise WorkerError("workspace mountpoint disappeared") from None
    return mount


def _mount_record(path, uid):
    """Verify Linux FUSE identity, retaining device/inode evidence."""
    try:
        info = path.stat()
    except OSError:
        raise WorkerError("workspace mountpoint cannot be stat'ed") from None
    if not stat.S_ISDIR(info.st_mode):
        raise WorkerError("workspace mountpoint is not a directory")
    record = {"available": False, "verified": False, "dev": info.st_dev,
              "ino": info.st_ino, "uid": info.st_uid}
    if sys.platform != "linux":
        record["reason"] = "mountinfo-unavailable"
        return record
    try:
        lines = Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines()
    except OSError:
        raise WorkerError("Linux mountinfo is unavailable") from None
    target = str(path)
    matches = []
    for line in lines:
        fields = line.split()
        if len(fields) < 7 or fields[4] != target or "-" not in fields:
            continue
        separator = fields.index("-")
        if separator + 3 >= len(fields):
            continue
        matches.append((fields, separator))
    if len(matches) != 1:
        raise WorkerError("workspace FUSE mount identity is missing or ambiguous")
    fields, separator = matches[0]
    fs_type, source, super_options = fields[separator + 1:separator + 4]
    options = fields[5].split(",") + fields[separator + 3].split(",")
    expected_uid = f"user_id={uid}"
    if fs_type != "fuse" or source != "scorpiofs-v3" or expected_uid not in options:
        raise WorkerError("workspace FUSE mount identity differs from v3 owner")
    if info.st_uid != uid:
        raise WorkerError("workspace mountpoint owner differs from daemon uid")
    record.update({"available": True, "verified": True, "fstype": fs_type,
                   "source": source, "user_id": uid, "super_options": super_options})
    return record


def _fd_digest(fd, expected_size, deadline):
    digest = hashlib.sha256()
    count = 0
    offset = 0
    while True:
        _check_deadline(deadline)
        chunk = os.pread(fd, 64 * 1024, offset) if hasattr(os, "pread") else _read_fd_at(fd, offset)
        if not chunk:
            break
        digest.update(chunk)
        count += len(chunk)
        offset += len(chunk)
        if count > expected_size:
            raise WorkerError("retained old file grew")
    if count != expected_size:
        raise WorkerError("retained old file size changed")
    return "sha256:" + digest.hexdigest()


def _read_fd_at(fd, offset):
    current = os.lseek(fd, 0, os.SEEK_CUR)
    try:
        os.lseek(fd, offset, os.SEEK_SET)
        return os.read(fd, 64 * 1024)
    finally:
        os.lseek(fd, current, os.SEEK_SET)


class WorkerSession:
    """Own one daemon round's workspace mounts and Git comparison checkouts."""

    def __init__(self, round_root, daemon_url, workspace_root, git_store, git_url,
                 git_env, *, deadline, env, daemon_uid):
        self.root = Path(round_root).resolve(strict=True)
        self.workspace_root = Path(workspace_root).resolve(strict=True)
        self.git_store = Path(git_store).resolve(strict=True)
        self.git_url = git_url
        self.git_env = dict(git_env or {})
        self.env = dict(env or os.environ)
        self.daemon_uid = daemon_uid
        self.default_deadline = deadline
        self.http = _NoRedirectHTTP(daemon_url)
        self._validate_roots()
        self.receipt_path = self.root / "owned-workspace-worker.json"
        if self.receipt_path.exists() or self.receipt_path.is_symlink():
            raise WorkerError("worker ownership receipt already exists")
        self._anchor_process = None
        self._anchor_identity = None
        self._anchor_buffer = bytearray()
        self._active_process = None
        self._active_identity = None
        self._last_identity = None
        self._git_fetched = False
        self._git_index = 0
        self._oracle_index = 0
        self._workspace_ids = []
        self._views = []
        self._git_worktrees = []
        # The receipt is reserved before the first anchor spawn.  On Linux the
        # anchor owns one new session for every short-lived Git child; this
        # means an interrupted fetch cannot escape through a second process
        # group after its leader exits.
        self._write_receipt(None, None, False)
        try:
            self._start_anchor()
        except BaseException:
            self._stop_anchor(time.monotonic() + 10)
            raise

    def _validate_roots(self):
        if (self.root.is_symlink() or self.workspace_root.is_symlink()
                or self.git_store.is_symlink() or not self.workspace_root.is_dir()
                or not self.git_store.is_dir()):
            raise WorkerError("worker roots are not private real directories")
        root_info = self.root.stat()
        if (os.name == "posix" and ((hasattr(os, "geteuid") and root_info.st_uid != os.geteuid())
                                     or stat.S_IMODE(root_info.st_mode) != 0o700)):
            raise WorkerError("worker round root is not a private 0700 directory")
        _loopback_url(self.git_url)
        if type(self.daemon_uid) is not int or self.daemon_uid < 0:
            raise WorkerError("worker daemon uid is invalid")

    def _write_receipt(self, pid, starttime, complete):
        if pid is not None and (type(pid) is not int or pid <= 0):
            raise WorkerError("worker receipt pid is invalid")
        if starttime is not None and (type(starttime) is not str or not starttime.isdecimal()):
            raise WorkerError("worker receipt starttime is invalid")
        payload = {"pid": pid, "starttime": starttime, "cleanup_complete": bool(complete)}
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        flags |= getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(self.receipt_path, flags, 0o600)
        try:
            if hasattr(os, "fchmod"):
                os.fchmod(fd, 0o600)
            else:
                os.chmod(self.receipt_path, 0o600)
            raw = json.dumps(payload, separators=(",", ":")).encode("ascii")
            os.write(fd, raw)
            os.fsync(fd)
        finally:
            os.close(fd)

    def _start_anchor(self):
        if os.name != "posix":
            return
        supervisor = (
            "import base64,json,subprocess,sys\n"
            "sys.stdout.write(json.dumps({'ready':True},separators=(',',':'))+'\\n');sys.stdout.flush()\n"
            "for raw in sys.stdin:\n"
            "    try:\n"
            "        request=json.loads(raw);data=base64.b64decode(request['data']) if request.get('data') is not None else None\n"
            "        child=subprocess.Popen(request['args'],stdin=subprocess.PIPE if data is not None else subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=request['env'],start_new_session=False)\n"
            "        stdout,stderr=child.communicate(data)\n"
            "        result={'status':child.returncode,'stdout':base64.b64encode(stdout).decode('ascii'),'stderr':base64.b64encode(stderr).decode('ascii')} if len(stdout)<=8388608 and len(stderr)<=8388608 else {'status':125,'stdout':'','stderr':''}\n"
            "    except BaseException:\n"
            "        result={'status':125,'stdout':'','stderr':''}\n"
            "    sys.stdout.write(json.dumps(result,separators=(',',':'))+'\\n');sys.stdout.flush()\n"
        )
        command = [sys.executable, "-c", supervisor]
        popen = budget.PinnedProcess if sys.platform == "linux" and hasattr(os, "waitid") else subprocess.Popen
        process = popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                        stderr=subprocess.DEVNULL, env=self.env, start_new_session=True)
        started = None
        if sys.platform == "linux" and hasattr(os, "waitid"):
            until = time.monotonic() + 5
            while time.monotonic() < until:
                try:
                    started = budget.process_start(process.pid)
                    break
                except FileNotFoundError:
                    time.sleep(.005)
        if sys.platform == "linux" and hasattr(os, "waitid") and started is None:
            self._terminate_portable(process, time.monotonic() + 10)
            raise WorkerError("worker anchor start identity is unavailable")
        self._anchor_process = process
        self._anchor_identity = (process.pid, started)
        self._last_identity = (process.pid, started)
        self._write_receipt(process.pid, started, False)
        if self._anchor_read(time.monotonic() + 5) != {"ready": True}:
            raise WorkerError("worker process-group anchor did not become ready")

    def _anchor_group_ok(self):
        if self._anchor_identity is None:
            return True
        pid, started = self._anchor_identity
        process = self._anchor_process
        if process is None:
            return False
        if not self._leader_alive(pid, started, process):
            raise WorkerError("worker process-group anchor exited unexpectedly")
        if os.name == "posix":
            try:
                if os.getpgid(pid) != pid or os.getsid(pid) != pid:
                    raise WorkerError("worker process-group anchor identity changed")
            except ProcessLookupError:
                raise WorkerError("worker process-group anchor disappeared") from None
        if sys.platform == "linux" and started is not None:
            budget.group_members(pid, started)
        return True

    def _stop_anchor(self, deadline):
        process = self._anchor_process
        identity = self._anchor_identity
        if process is None or identity is None:
            return
        pid, started = identity
        if sys.platform == "linux" and started is not None:
            # budget.stop_group verifies the unreaped leader identity before
            # TERM/KILL and reaps it only after descendants are gone.  The
            # receipt itself is never a signal authority.
            budget.stop_group(pid, started, deadline, process)
        elif self._leader_alive(pid, started, process):
            self._terminate_portable(process, deadline)
        self._anchor_process = None
        for stream in (getattr(process, "stdin", None), getattr(process, "stdout", None),
                       getattr(process, "stderr", None)):
            if stream is not None:
                stream.close()

    @staticmethod
    def _leader_alive(pid, started, process):
        if sys.platform == "linux" and started is not None and hasattr(os, "waitid"):
            try:
                if not Path(f"/proc/{pid}").exists():
                    return False
                # WNOWAIT observes an exit without releasing the PID/group
                # identity that the cleanup fence still owns.
                return os.waitid(os.P_PID, pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is None
            except (FileNotFoundError, ProcessLookupError):
                return False
        return process.poll() is None

    def _owned_command(self, args, deadline, *, env=None, data=None):
        _check_deadline(deadline)
        if not args or any(type(value) is not str for value in args):
            raise ValueError("owned command arguments must be strings")
        self._anchor_group_ok()
        # Keep the anchor binding in the receipt while a short-lived child is
        # spawned.  A pending/null receipt is reserved only for the very first
        # anchor spawn above.
        if self._anchor_identity is not None:
            self._write_receipt(*self._anchor_identity, False)
        if self._anchor_process is None or self._anchor_process.stdin is None:
            raise WorkerError("worker process-group anchor is unavailable")
        request = {"args": args, "env": env or self.env,
                   "data": base64.b64encode(data).decode("ascii") if data is not None else None}
        raw = json.dumps(request, separators=(",", ":"), ensure_ascii=True).encode("utf-8") + b"\n"
        if len(raw) > HTTP_BODY_LIMIT:
            raise WorkerError("owned command request is too large")
        self._active_process = self._anchor_process
        self._active_identity = self._anchor_identity
        try:
            self._anchor_process.stdin.write(raw)
            self._anchor_process.stdin.flush()
            result = self._anchor_read(deadline)
            if (type(result) is not dict or set(result) != {"status", "stdout", "stderr"}
                    or type(result["status"]) is not int
                    or type(result["stdout"]) is not str or type(result["stderr"]) is not str):
                raise WorkerError("worker command response shape is invalid")
            try:
                output = base64.b64decode(result["stdout"], validate=True)
                errors = base64.b64decode(result["stderr"], validate=True)
            except (ValueError, binascii.Error):
                raise WorkerError("worker command response encoding is invalid") from None
            if len(output) > COMMAND_OUTPUT_LIMIT or len(errors) > COMMAND_OUTPUT_LIMIT:
                raise WorkerError("owned command output is too large")
            if result["status"] != 0:
                raise WorkerError("owned Git command failed")
            _check_deadline(deadline)
            return output
        except (TimeoutError, BrokenPipeError, EOFError, OSError):
            self._stop_anchor(deadline)
            raise WorkerError("worker process-group anchor failed") from None
        finally:
            self._active_process = None
            self._active_identity = None

    def _anchor_read(self, deadline):
        while True:
            marker = self._anchor_buffer.find(b"\n")
            if marker >= 0:
                line = bytes(self._anchor_buffer[:marker])
                del self._anchor_buffer[:marker + 1]
                return _decode_anchor_json(line)
            _check_deadline(deadline)
            if self._anchor_process is None or self._anchor_process.stdout is None:
                raise WorkerError("worker process-group anchor output is unavailable")
            descriptor = self._anchor_process.stdout.fileno()
            remaining = max(0.001, deadline - time.monotonic())
            ready, _, _ = select.select([descriptor], [], [], remaining)
            if not ready:
                raise TimeoutError("owned command exceeded its absolute deadline")
            chunk = os.read(descriptor, 64 * 1024)
            if not chunk:
                raise EOFError("worker process-group anchor exited")
            self._anchor_buffer.extend(chunk)
            if len(self._anchor_buffer) > COMMAND_OUTPUT_LIMIT + HTTP_BODY_LIMIT:
                raise WorkerError("worker anchor response is too large")

    @staticmethod
    def _terminate_portable(process, deadline):
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=max(0.001, min(5.0, deadline - time.monotonic())))
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=max(0.001, deadline - time.monotonic()))

    def _git(self, deadline, *args):
        return self._owned_command(["git", *args], deadline, env=self.git_env)

    def _load_expected(self, path):
        path = Path(path)
        if path.is_symlink() or not path.is_file():
            raise WorkerError("expected manifest is not a regular file")
        raw = path.read_bytes()
        if len(raw) > HTTP_BODY_LIMIT:
            raise WorkerError("expected manifest is too large")
        try:
            value = json.loads(raw.decode("utf-8"), object_pairs_hook=_json_pairs)
        except (UnicodeDecodeError, json.JSONDecodeError, WorkerError):
            raise WorkerError("expected manifest is invalid") from None
        if type(value) is not dict or set(value) != {"files", "directories"}:
            raise WorkerError("expected manifest shape is invalid")
        # directory_sets performs the complete canonical-path check.
        from workspace_update_oracle import directory_sets
        directory_sets(value)
        return value

    def _oracle(self, root, expected, deadline, *, git_checkout=False):
        """Run the complete filesystem oracle through the owned anchor.

        The anchor's session/group is already fenced by ``_owned_command``.
        Keeping this walk in a short-lived child prevents a stuck FUSE read
        from blocking the benchmark parent, while the parent retains one
        absolute deadline and can terminate the entire anchor group.
        """
        _check_deadline(deadline)
        self._oracle_index += 1
        path = self.root / f".workspace-oracle-{self._oracle_index:04d}.json"
        if path.exists() or path.is_symlink():
            raise WorkerError("oracle manifest path already exists")
        raw = json.dumps(expected, sort_keys=True, separators=(",", ":")).encode("utf-8")
        if len(raw) > HTTP_BODY_LIMIT:
            raise WorkerError("oracle manifest is too large")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(path, flags, 0o600)
        try:
            os.write(fd, raw)
            os.fsync(fd)
        finally:
            os.close(fd)
        code = (
            "import json,sys\n"
            "from pathlib import Path\n"
            "from workspace_update_oracle import verify_workspace\n"
            "try:\n"
            " p=Path(sys.argv[1]); expected=json.loads(p.read_text(encoding='utf-8'))\n"
            " value=verify_workspace(sys.argv[2],expected,float(sys.argv[3]),git_checkout=sys.argv[4]=='1')\n"
            " print(json.dumps({'ok':True,'result':value},separators=(',',':')),flush=True)\n"
            "except BaseException as error:\n"
            " print(json.dumps({'ok':False,'error_type':type(error).__name__,'error':str(error)},separators=(',',':')),flush=True)\n"
        )
        env = dict(self.env)
        module_dir = str(Path(__file__).resolve().parent)
        current = env.get("PYTHONPATH")
        env["PYTHONPATH"] = module_dir if not current else module_dir + os.pathsep + current
        try:
            output = self._owned_command(
                [sys.executable, "-c", code, str(path), str(root), str(deadline),
                 "1" if git_checkout else "0"], deadline, env=env)
            response = _decode_anchor_json(output.rstrip(b"\n"))
            if type(response) is not dict or type(response.get("ok")) is not bool:
                raise WorkerError("isolated oracle response shape is invalid")
            if not response["ok"]:
                if response.get("error_type") == "TimeoutError":
                    raise TimeoutError(response.get("error") or "workspace oracle timed out")
                raise WorkerError(response.get("error") or "workspace oracle failed")
            result = response.get("result")
            if type(result) is not dict:
                raise WorkerError("isolated oracle result shape is invalid")
            return result
        finally:
            try:
                path.unlink()
            except FileNotFoundError:
                pass

    def _create_workspace(self, deadline):
        raw = self.http.request("POST", "/v3/workspaces", deadline,
                                {"target": {"kind": "latest"}, "scope": "/project",
                                 "delivery": "lazy", "upper_policy": "private"})
        status = _status(raw)
        if status["snapshot_id"] is None:
            raise WorkerError("workspace create omitted its fixed snapshot id")
        self._assert_status_path(status)
        self._workspace_ids.append(status["workspace_id"])
        return status

    def _assert_status_path(self, status):
        mount = _safe_mount_path(self.workspace_root, status["mountpoint"])
        if mount.parent.name != status["workspace_id"]:
            raise WorkerError("workspace mountpoint does not bind its workspace id")
        return mount

    def _fixed_status(self, first, value):
        value = _status(value)
        for key in ("workspace_id", "generation", "snapshot_id", "mountpoint"):
            if value[key] != first[key]:
                raise WorkerError("workspace identity changed while polling")
        self._assert_status_path(value)
        return value

    def _hydrate(self, first, deadline, timing_start, initial_metadata_ms=None):
        status = self._fixed_status(first, self.http.request(
            "POST", "/v3/workspaces/" + quote(first["workspace_id"], safe="") + "/hydrate",
            deadline, expected=(200,)))
        metadata_ms = initial_metadata_ms
        complete_ms = None
        while True:
            _check_deadline(deadline)
            status = self._fixed_status(first, self.http.request(
                "GET", "/v3/workspaces/" + quote(first["workspace_id"], safe=""), deadline))
            if status["mount_state"] == "mounted" and status["metadata_ready"]:
                if metadata_ms is None:
                    metadata_ms = (time.monotonic() - timing_start) * 1000
            if (status["mount_state"] == "failed" or status["hydration_state"] in {"failed", "cancelled"}
                    or status["last_error"] is not None):
                raise WorkerError("workspace hydration failed")
            if (status["mount_state"] == "mounted" and status["metadata_ready"]
                    and status["hydration_state"] == "complete"
                    and status["local_pin_state"] == "complete_snapshot"
                    and status["lease_state"] == "granted_locally"
                    and status["last_error"] is None):
                complete_ms = (time.monotonic() - timing_start) * 1000
                return status, metadata_ms, complete_ms
            pause = min(POLL_SECONDS, max(0, deadline - time.monotonic()))
            if pause <= 0:
                raise TimeoutError("workspace hydration did not become durable before its deadline")
            time.sleep(pause)

    def _open_retained_fd(self, mount, expected, deadline):
        candidates = [file for file in expected["files"] if file.get("fs_kind") in ("regular", "executable")]
        if not candidates:
            fd = os.open(mount, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
                         | getattr(os, "O_CLOEXEC", 0))
            return fd, "directory", None, None
        file = candidates[0]
        path = mount.joinpath(*file["rel_path"].split("/"))
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0))
        digest = _fd_digest(fd, file["size"], deadline)
        if digest != file["content_digest"]:
            os.close(fd)
            raise WorkerError("retained old file differs from its fixed snapshot")
        return fd, "file", file["rel_path"], file["content_digest"]

    def _retain_view(self, status, expected, deadline):
        mount = self._assert_status_path(status)
        upper = mount.parent / "upper"
        if upper.is_symlink() or not upper.is_dir():
            raise WorkerError("workspace upper is not a private directory")
        upper_info = upper.stat()
        if upper_info.st_uid != self.daemon_uid:
            raise WorkerError("workspace upper owner differs from daemon uid")
        sentinel = upper / DIRTY_SENTINEL
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
        fd = os.open(sentinel, flags, 0o600)
        try:
            os.write(fd, DIRTY_BYTES)
            os.fsync(fd)
        finally:
            os.close(fd)
        upper_fd = os.open(upper, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
                           | getattr(os, "O_CLOEXEC", 0))
        try:
            os.fsync(upper_fd)
        finally:
            os.close(upper_fd)
        retained_fd, fd_kind, fd_rel, fd_digest = self._open_retained_fd(mount, expected, deadline)
        self._views.append({"status": dict(status), "expected": deepcopy(expected), "fd": retained_fd,
                            "fd_kind": fd_kind, "fd_rel": fd_rel, "fd_digest": fd_digest,
                            "sentinel": sentinel, "mountpoint": mount})

    def _audit_view(self, view, deadline):
        expected = deepcopy(view["expected"])
        if any(file["rel_path"] == DIRTY_SENTINEL for file in expected["files"]):
            raise WorkerError("dirty sentinel collides with committed content")
        expected["files"].append({"rel_path": DIRTY_SENTINEL, "fs_kind": "regular",
                                  "size": len(DIRTY_BYTES),
                                  "content_digest": "sha256:" + hashlib.sha256(DIRTY_BYTES).hexdigest()})
        oracle = self._oracle(view["mountpoint"], expected, deadline)
        if view["fd_kind"] == "file":
            if _fd_digest(view["fd"], next(file["size"] for file in view["expected"]["files"]
                                             if file["rel_path"] == view["fd_rel"]), deadline) != view["fd_digest"]:
                raise WorkerError("retained old file descriptor changed")
        else:
            info = os.fstat(view["fd"])
            if not stat.S_ISDIR(info.st_mode):
                raise WorkerError("retained old directory descriptor changed")
        if view["sentinel"].read_bytes() != DIRTY_BYTES:
            raise WorkerError("dirty upper sentinel changed")
        return {"workspace_id": view["status"]["workspace_id"],
                "generation": view["status"]["generation"], "snapshot_id": view["status"]["snapshot_id"],
                "fd_verified": True, "dirty_upper_verified": True, "oracle": oracle}

    def _measure_scorpio(self, expected, deadline):
        started = time.monotonic()
        first = self._create_workspace(deadline)
        initial_metadata_ms = ((time.monotonic() - started) * 1000
                               if first["mount_state"] == "mounted" and first["metadata_ready"] else None)
        status, metadata_ms, complete_ms = self._hydrate(first, deadline, started, initial_metadata_ms)
        mount = self._assert_status_path(status)
        mount_identity = _mount_record(mount, self.daemon_uid)
        verify_start = time.monotonic()
        oracle = self._oracle(mount, expected, deadline)
        verified_ms = (time.monotonic() - started) * 1000
        verification_endpoint_ms = (time.monotonic() - verify_start) * 1000
        # Retain the just-verified view before auditing earlier views. The
        # current side's durable timer ends at its own complete byte oracle;
        # retention setup and retained-view audits remain visible as separate
        # wall-clock work.
        retain_start = time.monotonic()
        self._retain_view(status, expected, deadline)
        retain_view_ms = (time.monotonic() - retain_start) * 1000
        old_audit_start = time.monotonic()
        old = [self._audit_view(view, deadline) for view in self._views[:-1]]
        old_audit_ms = (time.monotonic() - old_audit_start) * 1000
        side_total_ms = (time.monotonic() - started) * 1000
        return {
            "actual_status": dict(status), "complete_status": dict(status),
            "metadata_ready_ms": metadata_ms, "durable_complete_ms": complete_ms,
            "durable_verified_ms": verified_ms, "oracle": oracle,
            "mount": mount_identity, "verification_endpoint_ms": verification_endpoint_ms,
            "retain_view_ms": retain_view_ms, "old_views": old,
            "old_view_audit_ms": old_audit_ms, "side_total_ms": side_total_ms,
        }

    def _measure_git(self, expected, commit, deadline):
        if type(commit) is not str or COMMIT_RE.fullmatch(commit) is None:
            raise ValueError("fixed Git commit must be a lowercase SHA-1")
        self._git_index += 1
        ref = f"refs/mst2-workspace/{self._git_index:04d}-{commit}"
        git_start = time.monotonic()
        fetch_start = time.monotonic()
        args = ["--git-dir", str(self.git_store), "fetch", "--no-tags"]
        if not self._git_fetched:
            args.append("--depth=1")
        args.extend([self.git_url, "refs/heads/main:" + ref])
        self._git(deadline, *args)
        self._git_fetched = True
        fetched = self._git(deadline, "--git-dir", str(self.git_store), "rev-parse", ref).decode().strip()
        if fetched != commit:
            raise WorkerError("Git target ref differs from the fixed commit")
        fetch_ms = (time.monotonic() - fetch_start) * 1000
        path = self.root / "git-worktrees" / f"{self._git_index:04d}-{commit[:12]}"
        path.parent.mkdir(mode=0o700, exist_ok=True)
        if path.exists() or path.is_symlink():
            raise WorkerError("Git detached worktree path already exists")
        self._git(deadline, "--git-dir", str(self.git_store), "worktree", "add", "--detach",
                  str(path), commit)
        self._git_worktrees.append(path)
        oracle = self._oracle(path, expected, deadline, git_checkout=True)
        verified_ms = (time.monotonic() - git_start) * 1000
        return {"commit": commit, "worktree": str(path), "fetch_ms": fetch_ms,
                "verified_ms": verified_ms, "side_total_ms": verified_ms,
                # Keep the old key for consumers that have not migrated to
                # the unambiguous full-side timing name yet.
                "checkout_verified_ms": verified_ms, "oracle": oracle}

    def measure(self, expected_path, commit, side_order, version, round_number, deadline):
        if side_order not in {"scorpio-first", "git-first"}:
            raise ValueError("invalid measurement side order")
        _check_deadline(deadline)
        expected = self._load_expected(expected_path)
        if type(version) is not str or not version or type(round_number) is not int:
            raise ValueError("measurement labels are invalid")
        results = {}
        if side_order == "scorpio-first":
            results["scorpio"] = self._measure_scorpio(expected, deadline)
            results["git"] = self._measure_git(expected, commit, deadline)
        else:
            results["git"] = self._measure_git(expected, commit, deadline)
            results["scorpio"] = self._measure_scorpio(expected, deadline)
        _check_deadline(deadline)
        return {"actual_status": results["scorpio"]["actual_status"],
                "complete_status": results["scorpio"]["complete_status"],
                "scorpio": {key: value for key, value in results["scorpio"].items() if key != "old_views"},
                "git": results["git"], "old_views": results["scorpio"]["old_views"],
                "version": version, "round": round_number}

    def _destroy_all(self, deadline):
        errors = []
        for workspace_id in list(reversed(self._workspace_ids)):
            try:
                self.http.request("POST", "/v3/workspaces/" + quote(workspace_id, safe="") + "/destroy",
                                  deadline, {"discard_dirty": True}, expected=(204,))
            except BaseException as error:
                errors.append(error)
        if errors:
            raise errors[0]

    def _close_views(self):
        for view in self._views:
            fd = view.pop("fd", None)
            if fd is not None:
                os.close(fd)

    def _groups_empty(self):
        if self._anchor_identity is not None:
            pid, started = self._anchor_identity
            if self._anchor_process is not None and self._leader_alive(pid, started, self._anchor_process):
                if sys.platform == "linux" and started is not None:
                    # The anchor itself is intentionally alive until stop or
                    # abort.  Completion is therefore possible only after its
                    # explicit group drain.
                    return False
                return False
            if sys.platform == "linux" and started is not None:
                return not budget.group_members(pid, started)
            return True
        if self._last_identity is None:
            return True
        pid, started = self._last_identity
        if sys.platform == "linux" and started is not None:
            return not budget.group_members(pid, started)
        return True

    def _complete_receipt(self):
        if not self._groups_empty() or _owned_mounts(self.workspace_root):
            raise WorkerError("worker cleanup cannot be marked complete while owned work remains")
        if self._last_identity is None:
            raise WorkerError("worker has no bound child identity")
        self._write_receipt(self._last_identity[0], self._last_identity[1], True)

    def stop(self, deadline):
        _check_deadline(deadline)
        final_audit_start = time.monotonic()
        final = [self._audit_view(view, deadline) for view in self._views]
        final_retained_view_audit_ms = (time.monotonic() - final_audit_start) * 1000
        try:
            self._destroy_all(deadline)
            if _owned_mounts(self.workspace_root):
                raise WorkerError("workspace mounts remained after explicit retirement")
            self._close_views()
            self._stop_anchor(deadline)
            self._complete_receipt()
            return {"retained": len(final), "verified": True, "views": final,
                    "final_retained_view_audit_ms": final_retained_view_audit_ms}
        except BaseException:
            raise

    def abort(self, deadline):
        errors = []
        try:
            if self._active_process is not None and self._active_identity is not None:
                self._stop_anchor(deadline)
            try:
                self._destroy_all(deadline)
            except BaseException as error:
                errors.append(error)
            self._close_views()
            self._stop_anchor(deadline)
            if not _owned_mounts(self.workspace_root) and self._groups_empty():
                if self._last_identity is not None:
                    self._write_receipt(self._last_identity[0], self._last_identity[1], True)
        except BaseException as error:
            errors.append(error)
        if errors:
            raise errors[0]
