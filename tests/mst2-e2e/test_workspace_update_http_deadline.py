"""Exercise real HTTP parsing with a scripted socket and virtual elapsed time."""

import io
import json
import unittest
from unittest.mock import patch

import commit_update_bench as common
import workspace_update_worker as worker


def headers(length=2):
    return ('HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n'
            'Content-Length: ' + str(length) + '\r\nConnection: close\r\n\r\n').encode()


class Clock:
    now = 1000.0


class ScriptedReader(io.RawIOBase):
    def __init__(self, socket, packets):
        self.socket, self.packets = socket, list(packets)

    def readable(self):
        return True

    def readinto(self, buffer):
        if not self.packets:
            return 0
        delay, data = self.packets.pop(0)
        self.socket.wait(delay, 'read')
        size = min(len(buffer), len(data))
        buffer[:size] = data[:size]
        if size < len(data):
            self.packets.insert(0, (0, data[size:]))
        return size


class ScriptedSocket:
    def __init__(self, clock, packets, send_delays):
        self.clock, self.send_delays = clock, list(send_delays)
        self.timeout = None
        self.waits = []
        self.closed = False
        self.reader = ScriptedReader(self, packets)

    def wait(self, delay, operation):
        self.waits.append({'operation': operation, 'started': self.clock.now,
                           'timeout': self.timeout, 'delay': delay, 'socket_closed': self.closed})
        if delay > self.timeout:
            self.clock.now += self.timeout
            raise TimeoutError('PRIVATE_SOCKET_TEXT')
        self.clock.now += delay

    def settimeout(self, timeout):
        if timeout <= 0:
            raise AssertionError('transport installed a nonpositive timeout')
        self.timeout = timeout

    def setsockopt(self, *args):
        pass

    def sendall(self, data):
        self.wait(self.send_delays.pop(0) if self.send_delays else 0, 'send')

    def makefile(self, mode, buffering=-1):
        if mode != 'rb':
            raise AssertionError('unexpected socket file mode')
        return self.reader if buffering == 0 else io.BufferedReader(self.reader)

    def close(self):
        # Like a real socket with an open makefile, close does not invalidate
        # the response's file descriptor until that file has also closed.
        self.closed = True


class HTTPDeadlineTests(unittest.TestCase):
    def request(self, *, budget=90, connect_delay=0, send_delays=(), packets=None, body=None):
        self.clock = Clock()
        self.socket = ScriptedSocket(self.clock, packets or [(0, headers() + b'{}')], send_delays)
        self.connect_timeouts = []

        def connect(address, timeout, source_address):
            self.connect_timeouts.append(timeout)
            self.socket.settimeout(timeout)
            self.socket.wait(connect_delay, 'connect')
            return self.socket

        with patch.object(worker.time, 'monotonic', side_effect=lambda: self.clock.now), \
                patch.object(worker.http.client.socket, 'create_connection', side_effect=connect):
            return worker._NoRedirectHTTP('http://127.0.0.1:12345').request(
                'POST', '/v3/workspaces', self.clock.now + budget, body=body)

    def reads(self):
        return [item for item in self.socket.waits if item['operation'] == 'read']

    def test_slow_headers_use_actual_remaining_budget_after_connect_and_request(self):
        # Thirty-one virtual seconds must succeed within the original 90s
        # operation. The actual raw header read sees 85s, not the connect cap
        # or a newly anchored 90s. No real-time sleep or network is involved.
        result = self.request(connect_delay=2, send_delays=(1, 2), body={'delivery': 'full'},
                              packets=[(31, headers() + b'{}')])
        self.assertEqual(result, {})
        self.assertEqual(self.connect_timeouts, [30])
        self.assertEqual(self.reads()[0]['timeout'], 85)
        self.assertEqual(self.clock.now, 1036)
        self.assertTrue(self.socket.reader.closed)

    def test_short_operation_caps_connect_and_subtracts_elapsed_time_before_headers(self):
        self.assertEqual(self.request(budget=5, connect_delay=1, send_delays=(1,),
                                      packets=[(1, headers() + b'{}')]), {})
        self.assertEqual(self.connect_timeouts, [5])
        self.assertEqual(self.reads()[0]['timeout'], 3)
        self.assertEqual(self.clock.now, 1003)

    def test_connect_retains_30_second_cap_with_long_operation_budget(self):
        with self.assertRaises(worker.HTTPConnectTimeout):
            self.request(budget=120, connect_delay=31)
        self.assertEqual(self.connect_timeouts, [30])
        self.assertEqual(self.clock.now, 1030)
        self.assertEqual(self.reads(), [])

    def test_short_connect_cannot_extend_original_deadline(self):
        with self.assertRaises(worker.HTTPConnectTimeout):
            self.request(budget=5, connect_delay=6)
        self.assertEqual(self.connect_timeouts, [5])
        self.assertEqual(self.clock.now, 1005)

    def test_request_send_uses_remaining_operation_budget(self):
        with self.assertRaises(worker.HTTPRequestTimeout):
            self.request(budget=5, connect_delay=1, send_delays=(5,))
        self.assertEqual(self.clock.now, 1005)
        self.assertEqual(self.reads(), [])

    def test_dripping_header_lines_cannot_reset_deadline(self):
        with self.assertRaises(worker.HTTPHeaderTimeout):
            self.request(budget=60, packets=[(40, b'HTTP/1.1 200 OK\r\n'),
                                             (25, b'Content-Length: 2\r\n')])
        self.assertEqual([item['timeout'] for item in self.reads()], [60, 20])
        self.assertEqual(self.clock.now, 1060)

    def test_body_retains_budget_after_connection_close_detaches_socket(self):
        self.assertEqual(self.request(budget=120, connect_delay=2,
                                      packets=[(31, headers()), (31, b'{}')]), {})
        self.assertEqual([item['timeout'] for item in self.reads()], [118, 87])
        self.assertTrue(self.reads()[-1]['socket_closed'])
        self.assertTrue(self.socket.reader.closed)
        self.assertEqual(self.clock.now, 1064)

    def test_dripping_body_bytes_cannot_extend_original_deadline(self):
        with self.assertRaises(worker.HTTPBodyTimeout):
            self.request(budget=10, packets=[(0, headers(4)), (3, b'n'), (3, b'u'),
                                             (3, b'l'), (3, b'l')])
        self.assertEqual([item['timeout'] for item in self.reads()], [10, 10, 7, 4, 1])
        self.assertEqual(self.clock.now, 1010)

    def test_expired_operation_rejects_before_connect(self):
        with self.assertRaises(TimeoutError) as failed:
            self.request(budget=0)
        self.assertIs(type(failed.exception), TimeoutError)
        self.assertEqual(self.connect_timeouts, [])

    def test_response_finishing_at_deadline_is_never_accepted(self):
        for packets, error_type in [([(5, headers() + b'{}')], worker.HTTPHeaderTimeout),
                                    ([(2, headers()), (3, b'{}')], worker.HTTPBodyTimeout)]:
            with self.subTest(stage=error_type.__name__), self.assertRaises(error_type):
                self.request(budget=5, packets=packets)
            self.assertEqual(self.clock.now, 1005)

    def test_closed_timeout_types_survive_existing_failure_record_without_private_text(self):
        cases = [(worker.HTTPConnectTimeout, {'connect_delay': 6}),
                 (worker.HTTPRequestTimeout, {'send_delays': (6,)}),
                 (worker.HTTPHeaderTimeout, {'packets': [(6, headers())]}),
                 (worker.HTTPBodyTimeout, {'packets': [(0, headers()), (6, b'{}')]})]
        session = worker.WorkerSession.__new__(worker.WorkerSession)
        for error_type, options in cases:
            with self.subTest(stage=error_type.__name__), self.assertRaises(error_type) as failed:
                with session._stage('create'):
                    self.request(budget=5, **options)
            self.assertIsInstance(failed.exception, TimeoutError)
            record = common.failure_record(failed.exception)
            self.assertEqual(record, {'execution_failed': True, 'error_type': error_type.__name__,
                                      'worker_stage': 'create'})
            self.assertNotIn('PRIVATE', json.dumps(record) + str(failed.exception))


if __name__ == '__main__':
    unittest.main()
