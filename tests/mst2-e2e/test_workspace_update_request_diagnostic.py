import json
import os
from pathlib import Path
import socket
import tempfile
import threading
import time
import unittest

import workspace_update_request_diagnostic as diagnostic


class _Daemon:
    def __init__(self, check=True):
        self.process = type("P", (), {"pid": os.getpid()})()
        self.started = str(_proc_starttime(os.getpid()))
        self.uid = os.getuid() if hasattr(os, "getuid") else 0
        self.check_calls = 0
        self.check = check

    def check_owner(self, socket_required=False):
        self.check_calls += 1
        if not self.check:
            raise RuntimeError("owner changed")


def _proc_starttime(pid):
    fields = Path("/proc") .joinpath(str(pid), "stat").read_text().rsplit(") ", 1)[1].split()
    return fields[19]


class _Upstream:
    def __init__(self):
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.port = self.listener.getsockname()[1]
        self.requests = []
        self.received = []
        self.done = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self):
        try:
            conn, _ = self.listener.accept()
            with conn:
                conn.settimeout(3)
                for _ in range(2):
                    data = bytearray()
                    while b"\r\n\r\n" not in data:
                        data.extend(conn.recv(1))
                    head = bytes(data)
                    length = int(next((line.split(b":", 1)[1] for line in head.split(b"\r\n")
                                       if line.lower().startswith(b"content-length:")), b"0"))
                    body = conn.recv(length) if length else b""
                    self.received.append(head + body)
                    conn.sendall(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")
        except (OSError, ValueError):
            pass
        finally:
            self.done.set()

    def close(self):
        self.listener.close()
        self.thread.join(3)


@unittest.skipIf(not hasattr(os, "getuid") or not Path("/proc").is_dir(), "requires Linux owner binding")
class RequestDiagnosticTests(unittest.TestCase):
    def relay(self, upstream):
        directory = Path(tempfile.mkdtemp())
        return diagnostic.RequestRelay("http://127.0.0.1:%d" % upstream.port, directory / "request-diagnostic.json",
                                       time.monotonic() + 5, max_connections=2, max_records=16), directory

    def test_forwards_split_headers_keepalive_without_private_bytes(self):
        upstream = _Upstream()
        try:
            relay, directory = self.relay(upstream)
            daemon = _Daemon()
            relay.start().bind_daemon(daemon)
            client = socket.create_connection(("127.0.0.1", relay.listener_port), timeout=2)
            first = (b"GET /api/v2/snapshots/capabilities HTTP/1.1\r\nHost: secret.local\r\n"
                     b"X-Secret: never-export\r\nConnection: keep-alive\r\n\r\n")
            second = (b"POST /api/v2/snapshots/resolve HTTP/1.1\r\nHost: secret.local\r\n"
                      b"Content-Length: 5\r\n\r\nhello")
            client.sendall(first[:17]); client.sendall(first[17:]); client.recv(128)
            client.sendall(second[:13]); client.sendall(second[13:]); client.recv(128); client.close()
            value = relay.close(time.monotonic() + 4)
            self.assertTrue(value["valid"])
            self.assertEqual(value["owner"]["pid"], os.getpid())
            self.assertEqual([x["endpoint"] for x in value["records"] if x["event"] == "request_headers_forwarded"],
                             ["capabilities", "resolve"])
            upstream.thread.join(2)
            self.assertEqual(upstream.received, [first, second])
            raw = (directory / "request-diagnostic.json").read_bytes()
            self.assertNotIn(b"secret.local", raw)
            self.assertNotIn(b"never-export", raw)
            self.assertTrue(upstream.done.wait(2))
        finally:
            upstream.close()

    def test_owner_rejection_has_closed_invalid_schema_and_no_endpoint(self):
        upstream = _Upstream()
        try:
            relay, directory = self.relay(upstream)
            relay.start().bind_daemon(_Daemon(check=False))
        except diagnostic.RequestDiagnosticError as error:
            self.assertEqual(error.code, "owner_rejected")
            value = relay.close(time.monotonic() + 4)
            self.assertFalse(value["valid"])
            self.assertIsNone(value["owner"])
            self.assertEqual(value["records"], [])
            self.assertEqual(json.loads((directory / "request-diagnostic.json").read_bytes()), value)
        finally:
            upstream.close()

    def test_limits_and_close_are_bounded_and_file_is_exclusive(self):
        upstream = _Upstream()
        try:
            relay, directory = self.relay(upstream)
            relay.start().bind_daemon(_Daemon())
            value = relay.close(time.monotonic() + 4)
            self.assertTrue(value["closed"])
            raw = (directory / "request-diagnostic.json").read_bytes()
            self.assertEqual(relay.close(time.monotonic() + 4), value)
            self.assertEqual((directory / "request-diagnostic.json").read_bytes(), raw)
            self.assertEqual(diagnostic.validate(value), value)
        finally:
            upstream.close()

class RequestDiagnosticSchemaTests(unittest.TestCase):
    def test_schema_rejects_ownerless_valid_trace(self):
        with self.assertRaises(diagnostic.RequestDiagnosticError):
            diagnostic.validate({"revision": 1, "diagnostic_only": True, "formal_performance_accepted": False,
                                 "valid": True, "invalid_reason": None, "closed": True, "owner": None,
                                 "listener_port": 1, "upstream_port": 1,
                                 "limits": {"connections": 1, "records": 1, "header_bytes": diagnostic.HEADER_BYTES,
                                            "buffer_bytes": diagnostic.BUFFER_BYTES},
                                 "connections": [], "records": []})

    def test_unsupported_transfer_encoding_is_rejected_before_endpoint_event(self):
        events, errors = [], []
        parser = diagnostic._RequestHeaders(lambda *args: events.append(args), errors.append)
        parser.feed(b"POST /api/v2/snapshots/resolve HTTP/1.1\r\nHost: hidden\r\n"
                    b"Transfer-Encoding: chunked\r\n\r\n")
        self.assertEqual(errors, ["unsupported_framing"])
        self.assertEqual(events, [])

    def test_upgrade_token_with_spacing_is_rejected_before_endpoint_event(self):
        events, errors = [], []
        parser = diagnostic._RequestHeaders(lambda *args: events.append(args), errors.append)
        parser.feed(b"GET /api/v2/snapshots/capabilities HTTP/1.1\r\nHost: hidden\r\n"
                    b"Connection: keep-alive, upgrade\r\n\r\n")
        self.assertEqual(errors, ["unsupported_framing"])
        self.assertEqual(events, [])


if __name__ == "__main__":
    unittest.main()
