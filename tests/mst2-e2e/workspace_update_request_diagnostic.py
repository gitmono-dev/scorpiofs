"""Bounded, byte-transparent loopback observation for a diagnostic-only create.

Request headers are observed only after their bytes were written to the upstream
socket. This is not proof that the service read them. Response bytes and EOF are
connection observations; they are never assigned to a request or HTTP status.
The caller stops the daemon, closes this relay within its original deadline,
then stops the backend. No request timeout, retry, header or body is changed.
"""

import copy
import json
import math
import os
from pathlib import Path
import re
import select
import socket
import threading
import time
from urllib.parse import urlsplit


HEADER_BYTES = 16 * 1024
BUFFER_BYTES = 64 * 1024
BODY_BYTES = 2 * 1024 * 1024
MAX_CONNECTIONS = 16
MAX_RECORDS = 128
INVALID_REASONS = frozenset({
    "owner_unbound", "owner_rejected", "unsupported_framing", "header_limit",
    "body_length_limit", "truncated_request", "record_budget", "connection_budget",
    "deadline", "socket_error", "relay_start_failed",
})
CLOSE_REASONS = frozenset({"eof", "relay_shutdown", "socket_error"})
ENDPOINTS = frozenset({"capabilities", "resolve", "other"})
TOKEN = re.compile(rb"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")


class RequestDiagnosticError(RuntimeError):
    """A fixed diagnostic error; raw socket, owner and file errors stay private."""

    def __init__(self, code):
        if code not in INVALID_REASONS | {"output_exists", "output_failed", "invalid_schema"}:
            code = "invalid_schema"
        self.code = code
        super().__init__(code)


def _integer(value, minimum=0, maximum=(1 << 63) - 1):
    return type(value) is int and minimum <= value <= maximum


def _shape(value, fields):
    if type(value) is not dict or set(value) != set(fields):
        raise RequestDiagnosticError("invalid_schema")


def validate(value):
    """Validate the whole closed export, including failed/early-startup traces."""
    _shape(value, {"revision", "diagnostic_only", "formal_performance_accepted", "valid",
                   "invalid_reason", "closed", "owner", "listener_port", "upstream_port",
                   "limits", "connections", "records"})
    if (type(value["revision"]) is not int or value["revision"] != 1
            or value["diagnostic_only"] is not True or value["formal_performance_accepted"] is not False
            or type(value["valid"]) is not bool or type(value["closed"]) is not bool
            or not _integer(value["listener_port"], 1, 65535)
            or not _integer(value["upstream_port"], 1, 65535)):
        raise RequestDiagnosticError("invalid_schema")
    reason = value["invalid_reason"]
    if ((value["valid"] and reason is not None)
            or (not value["valid"] and (type(reason) is not str or reason not in INVALID_REASONS))
            or value["closed"] is not True):
        raise RequestDiagnosticError("invalid_schema")
    limits = value["limits"]
    _shape(limits, {"connections", "records", "header_bytes", "buffer_bytes"})
    if (not _integer(limits["connections"], 1, MAX_CONNECTIONS)
            or not _integer(limits["records"], 1, MAX_RECORDS)
            or limits["header_bytes"] != HEADER_BYTES or type(limits["header_bytes"]) is not int
            or limits["buffer_bytes"] != BUFFER_BYTES or type(limits["buffer_bytes"]) is not int):
        raise RequestDiagnosticError("invalid_schema")
    owner = value["owner"]
    if owner is not None:
        _shape(owner, {"pid", "starttime_ticks", "uid"})
        if (not _integer(owner["pid"], 1) or not _integer(owner["uid"], 0, (1 << 32) - 1)
                or type(owner["starttime_ticks"]) is not str
                or re.fullmatch(r"[1-9][0-9]{0,19}", owner["starttime_ticks"]) is None):
            raise RequestDiagnosticError("invalid_schema")
    elif value["valid"] or value["connections"] or value["records"]:
        raise RequestDiagnosticError("invalid_schema")
    if (type(value["connections"]) is not list or len(value["connections"]) > limits["connections"]
            or type(value["records"]) is not list or len(value["records"]) > limits["records"]):
        raise RequestDiagnosticError("invalid_schema")
    connections = {}
    for connection in value["connections"]:
        _shape(connection, {"connection_id", "daemon_local_address", "daemon_local_port",
                            "relay_local_address", "relay_local_port", "daemon_socket_inode",
                            "accepted_monotonic_ns", "closed", "close_reason", "closed_monotonic_ns"})
        cid = connection["connection_id"]
        if (not _integer(cid, 1, limits["connections"]) or cid in connections
                or connection["daemon_local_address"] != "127.0.0.1"
                or connection["relay_local_address"] != "127.0.0.1"
                or not _integer(connection["daemon_local_port"], 1, 65535)
                or connection["relay_local_port"] != value["listener_port"]
                or type(connection["relay_local_port"]) is not int
                or not _integer(connection["daemon_socket_inode"], 1)
                or not _integer(connection["accepted_monotonic_ns"], 1)
                or type(connection["closed"]) is not bool):
            raise RequestDiagnosticError("invalid_schema")
        if connection["closed"]:
            if (type(connection["close_reason"]) is not str or connection["close_reason"] not in CLOSE_REASONS
                    or not _integer(connection["closed_monotonic_ns"], connection["accepted_monotonic_ns"])):
                raise RequestDiagnosticError("invalid_schema")
        elif (connection["close_reason"] is not None or connection["closed_monotonic_ns"] is not None
              or value["closed"]):
            raise RequestDiagnosticError("invalid_schema")
        connections[cid] = connection
    if set(connections) != set(range(1, len(connections) + 1)):
        raise RequestDiagnosticError("invalid_schema")
    requests, first_bytes, eof, previous = {}, set(), set(), 0
    for record in value["records"]:
        if type(record) is not dict:
            raise RequestDiagnosticError("invalid_schema")
        event = record.get("event")
        fields = {"event", "connection_id", "monotonic_ns"}
        if event == "request_headers_forwarded":
            fields |= {"request_index", "endpoint"}
        elif event == "eof":
            fields |= {"side"}
        elif event != "first_upstream_bytes":
            raise RequestDiagnosticError("invalid_schema")
        _shape(record, fields)
        cid = record["connection_id"]
        if (not _integer(cid, 1) or cid not in connections
                or not _integer(record["monotonic_ns"], connections[cid]["accepted_monotonic_ns"])
                or record["monotonic_ns"] < previous
                or (connections[cid]["closed"]
                    and record["monotonic_ns"] > connections[cid]["closed_monotonic_ns"])):
            raise RequestDiagnosticError("invalid_schema")
        previous = record["monotonic_ns"]
        if event == "request_headers_forwarded":
            if (type(record["endpoint"]) is not str or record["endpoint"] not in ENDPOINTS
                    or not _integer(record["request_index"], 1)
                    or record["request_index"] != requests.get(cid, 0) + 1):
                raise RequestDiagnosticError("invalid_schema")
            requests[cid] = record["request_index"]
        elif event == "first_upstream_bytes":
            if cid in first_bytes:
                raise RequestDiagnosticError("invalid_schema")
            first_bytes.add(cid)
        elif type(record["side"]) is not str or record["side"] not in {"daemon", "upstream"} or (cid, record["side"]) in eof:
            raise RequestDiagnosticError("invalid_schema")
        else:
            eof.add((cid, record["side"]))
    return value


class _RequestHeaders:
    """Observe bytes already forwarded; retain at most one bounded header."""

    def __init__(self, emit, invalidate):
        self.emit, self.invalidate = emit, invalidate
        self.header = bytearray()
        self.body_remaining = 0
        self.request_index = 0
        self.invalid = False

    def refuse(self, reason):
        self.invalid = True
        self.header.clear()
        self.invalidate(reason)

    def feed(self, data):
        offset = 0
        while offset < len(data) and not self.invalid:
            if self.body_remaining:
                used = min(self.body_remaining, len(data) - offset)
                self.body_remaining -= used
                offset += used
                continue
            # Byte iteration stops at the exact header boundary; bodies are never retained.
            self.header.append(data[offset])
            offset += 1
            if len(self.header) > HEADER_BYTES:
                self.refuse("header_limit")
            elif self.header.endswith(b"\r\n\r\n"):
                self._complete()

    def _complete(self):
        lines = bytes(self.header[:-4]).split(b"\r\n")
        self.header.clear()
        request = lines[0].split(b" ")
        if (len(request) != 3 or not TOKEN.fullmatch(request[0])
                or request[2] not in {b"HTTP/1.0", b"HTTP/1.1"}
                or not request[1] or any(byte < 33 or byte > 126 for byte in request[1])
                or request[0] == b"CONNECT"):
            self.refuse("unsupported_framing")
            return
        lengths = []
        for line in lines[1:]:
            name, separator, value = line.partition(b":")
            if not separator or not TOKEN.fullmatch(name) or b"\r" in value or b"\n" in value:
                self.refuse("unsupported_framing")
                return
            name, value = name.lower(), value.strip(b" \t")
            connection_tokens = {token.strip(b" \t").lower() for token in value.split(b",")} if name == b"connection" else set()
            if name in {b"transfer-encoding", b"upgrade"} or (name == b"connection" and b"upgrade" in connection_tokens):
                self.refuse("unsupported_framing")
                return
            if name == b"content-length":
                if not re.fullmatch(rb"[0-9]{1,10}", value):
                    self.refuse("body_length_limit")
                    return
                lengths.append(int(value))
        if len(lengths) > 1:
            self.refuse("unsupported_framing")
            return
        length = lengths[0] if lengths else 0
        if length > BODY_BYTES:
            self.refuse("body_length_limit")
            return
        self.body_remaining = length
        self.request_index += 1
        endpoint = {(b"GET", b"/api/v2/snapshots/capabilities"): "capabilities",
                    (b"POST", b"/api/v2/snapshots/resolve"): "resolve"}.get(tuple(request[:2]), "other")
        self.emit(self.request_index, endpoint)

    def finish(self):
        if not self.invalid and (self.header or self.body_remaining):
            self.refuse("truncated_request")


class RequestRelay:
    def __init__(self, upstream, output_path, deadline, *, max_connections=8, max_records=64):
        parsed = urlsplit(upstream)
        try:
            port = parsed.port
        except ValueError:
            raise ValueError("fixed loopback upstream required") from None
        if (parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or parsed.username is not None
                or parsed.password is not None or not _integer(port, 1, 65535)
                or parsed.path not in {"", "/"} or parsed.query or parsed.fragment
                or type(deadline) not in (int, float) or not math.isfinite(deadline)
                or not _integer(max_connections, 1, MAX_CONNECTIONS)
                or not _integer(max_records, 1, MAX_RECORDS)):
            raise ValueError("invalid bounded request diagnostic configuration")
        self.upstream_port, self.path, self.deadline = port, Path(output_path), float(deadline)
        self.max_connections, self.max_records = max_connections, max_records
        self._lock, self._close_lock = threading.RLock(), threading.Lock()
        self._stop, self._bound = threading.Event(), threading.Event()
        self._listener = None
        self._accept_thread = None
        self._threads, self._sockets = [], set()
        self._owner, self._callback = None, None
        self._connections, self._records = [], []
        self._invalid_reason = None
        self._saved = None
        self._close_error = None
        self.listener_port = None

    @property
    def owner(self):
        with self._lock:
            return copy.deepcopy(self._owner)

    @property
    def url(self):
        if self.listener_port is None:
            raise RequestDiagnosticError("relay_start_failed")
        return f"http://127.0.0.1:{self.listener_port}"

    def _invalidate(self, reason):
        with self._lock:
            if self._invalid_reason is None:
                self._invalid_reason = reason

    def start(self):
        if self._listener is not None or self._stop.is_set():
            raise RequestDiagnosticError("relay_start_failed")
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.bind(("127.0.0.1", 0))
            listener.listen(self.max_connections)
            listener.setblocking(False)
        except OSError:
            listener.close()
            self._invalidate("relay_start_failed")
            raise RequestDiagnosticError("relay_start_failed") from None
        self._listener = listener
        self.listener_port = listener.getsockname()[1]
        self._accept_thread = threading.Thread(target=self._accept, daemon=True)
        self._accept_thread.start()
        return self

    def bind_daemon(self, daemon):
        try:
            owner = {"pid": daemon.process.pid, "starttime_ticks": daemon.started, "uid": daemon.uid}
            if (self._listener is None or self._owner is not None or self._stop.is_set()
                    or not _integer(owner["pid"], 1) or not _integer(owner["uid"], 0, (1 << 32) - 1)
                    or type(owner["starttime_ticks"]) is not str
                    or not re.fullmatch(r"[1-9][0-9]{0,19}", owner["starttime_ticks"])
                    or not callable(daemon.check_owner)):
                raise RequestDiagnosticError("owner_rejected")
            self._callback = daemon.check_owner
            self._check_identity(owner)
            with self._lock:
                self._owner = owner
                self._bound.set()
        except Exception:
            self._callback = None
            self._invalidate("owner_rejected")
            raise RequestDiagnosticError("owner_rejected") from None
        return self

    def _check_identity(self, owner=None):
        owner = self._owner if owner is None else owner
        if owner is None or self._callback is None:
            raise RequestDiagnosticError("owner_unbound")
        self._callback(socket_required=True)
        directory = Path("/proc") / str(owner["pid"])
        fields = (directory / "stat").read_text().rsplit(") ", 1)[1].split()
        if fields[19] != owner["starttime_ticks"] or directory.stat().st_uid != owner["uid"]:
            raise RequestDiagnosticError("owner_rejected")

    def _accepted_inode(self, connection):
        self._check_identity()
        peer, local = connection.getpeername(), connection.getsockname()
        if peer[0] != "127.0.0.1" or local != ("127.0.0.1", self.listener_port):
            raise RequestDiagnosticError("owner_rejected")
        directory = Path("/proc") / str(self._owner["pid"])
        matches = []
        for line in (directory / "net/tcp").read_text().splitlines()[1:]:
            fields = line.split()
            if (fields[1] == f"0100007F:{peer[1]:04X}"
                    and fields[2] == f"0100007F:{local[1]:04X}" and fields[3] == "01"):
                matches.append(fields)
        if len(matches) != 1 or int(matches[0][7]) != self._owner["uid"] or int(matches[0][9]) <= 0:
            raise RequestDiagnosticError("owner_rejected")
        inode = int(matches[0][9])
        owned = False
        for fd in (directory / "fd").iterdir():
            try:
                if os.readlink(fd) == f"socket:[{inode}]":
                    owned = True
            except FileNotFoundError:
                continue
        self._check_identity()
        if not owned:
            raise RequestDiagnosticError("owner_rejected")
        return inode

    def _emit(self, event, cid, **fields):
        with self._lock:
            if self._invalid_reason is not None:
                return
        if event in {"request_headers_forwarded", "first_upstream_bytes"}:
            try:
                self._check_identity()
            except Exception:
                self._invalidate("owner_rejected")
                return
        with self._lock:
            if len(self._records) >= self.max_records:
                self._invalidate("record_budget")
                return
            self._records.append(dict(event=event, connection_id=cid, monotonic_ns=time.monotonic_ns(), **fields))

    def _accept(self):
        while not self._stop.is_set():
            if time.monotonic() >= self.deadline:
                self._invalidate("deadline")
                self._stop.set()
                return
            if not self._bound.wait(.05):
                continue
            try:
                ready, _, _ = select.select([self._listener], [], [], .05)
                if not ready:
                    continue
                connection, peer = self._listener.accept()
                if len(self._connections) >= self.max_connections:
                    connection.close()
                    self._invalidate("connection_budget")
                    return
                try:
                    inode = self._accepted_inode(connection)
                except Exception:
                    connection.close()
                    self._invalidate("owner_rejected")
                    return
                connection.setblocking(False)
                with self._lock:
                    cid = len(self._connections) + 1
                    row = {"connection_id": cid, "daemon_local_address": "127.0.0.1",
                           "daemon_local_port": peer[1], "relay_local_address": "127.0.0.1",
                           "relay_local_port": self.listener_port, "daemon_socket_inode": inode,
                           "accepted_monotonic_ns": time.monotonic_ns(), "closed": False,
                           "close_reason": None, "closed_monotonic_ns": None}
                    self._connections.append(row)
                    self._sockets.add(connection)
                    thread = threading.Thread(target=self._forward, args=(connection, row), daemon=True)
                    self._threads.append(thread)
                    thread.start()
            except (OSError, ValueError):
                if not self._stop.is_set():
                    self._invalidate("socket_error")
                return

    def _forward(self, daemon, row):
        upstream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        with self._lock:
            self._sockets.add(upstream)
        reason = "relay_shutdown"
        observer = _RequestHeaders(
            lambda index, endpoint: self._emit("request_headers_forwarded", row["connection_id"],
                                               request_index=index, endpoint=endpoint), self._invalidate)
        observer_finished = False
        queues = None
        try:
            remaining = self.deadline - time.monotonic()
            if remaining <= 0:
                self._invalidate("deadline")
                return
            upstream.settimeout(remaining)
            upstream.connect(("127.0.0.1", self.upstream_port))
            upstream.setblocking(False)
            sockets = (daemon, upstream)
            queues = {daemon: bytearray(), upstream: bytearray()}
            ended, shut = set(), set()
            first_upstream = False
            while not self._stop.is_set():
                remaining = self.deadline - time.monotonic()
                if remaining <= 0:
                    self._invalidate("deadline")
                    break
                if daemon in ended and not queues[upstream] and not observer_finished:
                    observer.finish()
                    observer_finished = True
                if len(ended) == 2 and not any(queues.values()):
                    reason = "eof"
                    break
                for source, target in ((daemon, upstream), (upstream, daemon)):
                    if source in ended and not queues[target] and target not in shut:
                        if source is daemon and not observer_finished:
                            observer.finish()
                            observer_finished = True
                        target.shutdown(socket.SHUT_WR)
                        shut.add(target)
                reads = [s for s in sockets if s not in ended and len(queues[sockets[1] if s is daemon else daemon]) < BUFFER_BYTES]
                writes = [s for s in sockets if queues[s]]
                readable, writable, _ = select.select(reads, writes, [], min(.05, remaining))
                for target in writable:
                    try:
                        count = target.send(queues[target])
                    except BlockingIOError:
                        continue
                    if count <= 0:
                        raise OSError()
                    if target is upstream:
                        observer.feed(memoryview(queues[target])[:count])
                    del queues[target][:count]
                for source in readable:
                    target = upstream if source is daemon else daemon
                    try:
                        data = source.recv(min(32768, BUFFER_BYTES - len(queues[target])))
                    except BlockingIOError:
                        continue
                    if not data:
                        ended.add(source)
                        self._emit("eof", row["connection_id"], side="daemon" if source is daemon else "upstream")
                    else:
                        if source is upstream and not first_upstream:
                            first_upstream = True
                            self._emit("first_upstream_bytes", row["connection_id"])
                        queues[target].extend(data)
        except (OSError, ValueError):
            if not self._stop.is_set():
                reason = "socket_error"
                self._invalidate("socket_error")
        finally:
            if queues is not None and queues[upstream]:
                self._invalidate("truncated_request")
            if not observer_finished:
                observer.finish()
                observer_finished = True
            with self._lock:
                for stream in (daemon, upstream):
                    stream.close()
                    self._sockets.discard(stream)
                row.update(closed=True, close_reason=reason, closed_monotonic_ns=time.monotonic_ns())

    def close(self, deadline):
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise RequestDiagnosticError("deadline")
        until = min(float(deadline), self.deadline)
        with self._close_lock:
            if self._saved is not None:
                if self._close_error:
                    raise RequestDiagnosticError(self._close_error)
                return copy.deepcopy(self._saved)
            self._stop.set()
            self._bound.set()
            if self._owner is None:
                self._invalidate("owner_unbound")
            with self._lock:
                for stream in list(self._sockets):
                    try:
                        stream.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
            if self._accept_thread is not None:
                self._accept_thread.join(max(0, until - time.monotonic()))
            for thread in list(self._threads):
                thread.join(max(0, until - time.monotonic()))
            complete = (time.monotonic() <= until and (self._accept_thread is None or not self._accept_thread.is_alive())
                        and all(not thread.is_alive() for thread in self._threads))
            if not complete:
                self._invalidate("deadline")
                self._close_error = "deadline"
            with self._lock:
                if self._listener is not None:
                    self._listener.close()
                for stream in list(self._sockets):
                    stream.close()
                self._sockets.clear()
                value = {"revision": 1, "diagnostic_only": True, "formal_performance_accepted": False,
                         "valid": self._invalid_reason is None and complete,
                         "invalid_reason": self._invalid_reason, "closed": complete, "owner": self.owner,
                         "listener_port": self.listener_port, "upstream_port": self.upstream_port,
                         "limits": {"connections": self.max_connections, "records": self.max_records,
                                    "header_bytes": HEADER_BYTES, "buffer_bytes": BUFFER_BYTES},
                         "connections": copy.deepcopy(self._connections), "records": copy.deepcopy(self._records)}
            validate(value)
            raw = (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode("ascii")
            try:
                fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
                with os.fdopen(fd, "wb") as stream:
                    stream.write(raw)
            except FileExistsError:
                raise RequestDiagnosticError("output_exists") from None
            except OSError:
                raise RequestDiagnosticError("output_failed") from None
            self._saved = value
            if self._close_error:
                raise RequestDiagnosticError(self._close_error)
            return copy.deepcopy(value)
