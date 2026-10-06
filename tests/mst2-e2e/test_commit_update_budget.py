"""Budget admissions and actual Linux process cleanup, without a deployment."""

from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import commit_update_bench as bench
import commit_update_budget as budget
import commit_update_ci as ci


class SessionBudgetTests(unittest.TestCase):
    def test_communicate_wait_keeps_leader_pinned_until_explicit_group_cleanup(self):
        process = object.__new__(budget.PinnedProcess)
        process._child_created = False
        process.reap_pinned = True
        process.returncode = None
        process.pid = 123
        process.args = ["owned"]
        observation = SimpleNamespace(si_code=1, si_status=0)
        with patch.multiple(budget.os, P_PID=1, WEXITED=2, WNOHANG=4, WNOWAIT=8,
                            CLD_EXITED=1, create=True), \
                patch.object(budget.os, "waitid", return_value=observation, create=True) as observe, \
                patch.object(subprocess.Popen, "wait", return_value=0) as reap:
            self.assertEqual(process.wait(timeout=1), 0)
            self.assertEqual(process._wait(timeout=1), 0)
            self.assertEqual(process.wait(timeout=1), 0)
            self.assertIsNone(process.returncode)
            reap.assert_not_called()
            observe.assert_called_with(1, 123, 2 | 4 | 8)
            budget.reap_owned(process, time.monotonic() + 1)
            reap.assert_called_once()
            self.assertFalse(process.reap_pinned)

    def test_timeout_never_kills_reaped_or_replaced_leader_group(self):
        for changed in ("reaped", "replaced"):
            class Child:
                pid = 123
                returncode = None
                def communicate(self, *args, **kwargs):
                    if not hasattr(self, "term_sent"):
                        self.term_sent = True
                        raise subprocess.TimeoutExpired("owned", 1)
                    if changed == "reaped":
                        self.returncode = 0
                    return b"", b""
            child = Child()
            scans = iter(([123], AssertionError("owned group leader identity was replaced")))
            def members(*_):
                result = next(scans)
                if isinstance(result, Exception):
                    raise result
                return result
            with patch.object(budget.sys, "platform", "linux"), \
                    patch.object(budget.signal, "SIGKILL", 9, create=True), \
                    patch.object(budget, "PinnedProcess", return_value=child), \
                    patch.object(budget, "process_start", return_value="1"), \
                    patch.object(budget, "group_members", side_effect=members), \
                    patch.object(budget, "reap_exited"), \
                    patch.object(budget.os, "killpg", create=True) as send:
                with self.assertRaises(AssertionError):
                    budget.run_process(["owned"], time.monotonic() + 30)
                send.assert_called_once_with(123, budget.signal.SIGTERM)

    def test_open_pipe_timeout_reaps_exited_leader_without_extending_wait_or_masking_failure(self):
        class Child:
            pid = 123
            returncode = None
            stdin = None
            stdout = io.BytesIO()
            stderr = io.BytesIO()
            def communicate(self, *args, **kwargs):
                raise subprocess.TimeoutExpired("owned", 1)
            def release_reap(self):
                self.released = True
            def wait(self, timeout=None):
                self.wait_timeout = timeout
                self.returncode = 0
        child = Child()
        with patch.object(budget.sys, "platform", "linux"), \
                patch.object(budget, "PinnedProcess", return_value=child), \
                patch.object(budget, "process_start", return_value="1"), \
                patch.object(budget, "group_members", return_value=[]), \
                patch.object(budget.signal, "SIGKILL", 9, create=True), \
                patch.multiple(budget.os, P_PID=1, WEXITED=2, WNOHANG=4, WNOWAIT=8, create=True), \
                patch.object(budget.os, "waitid", return_value=SimpleNamespace(si_status=0), create=True), \
                patch.object(budget.os, "killpg", create=True) as send:
            with self.assertRaisesRegex(TimeoutError, "owned child did not reap"):
                budget.run_process(["owned"], time.monotonic() + 30)
            send.assert_not_called()
        self.assertTrue(child.released)
        self.assertEqual(child.wait_timeout, 0)
        self.assertEqual(child.returncode, 0)

    def test_group_timeout_reaps_exited_direct_leader_and_preserves_timeout(self):
        child = SimpleNamespace(pid=123, returncode=None)
        waits = []
        def wait(timeout=None):
            waits.append(timeout)
            child.returncode = 0
        child.wait = wait
        with patch.object(budget.sys, "platform", "linux"), \
                patch.object(budget, "group_members", return_value=[456]), \
                patch.object(budget.signal, "SIGKILL", 9, create=True), \
                patch.object(budget.os, "killpg", create=True), \
                patch.multiple(budget.os, P_PID=1, WEXITED=2, WNOHANG=4, WNOWAIT=8, create=True), \
                patch.object(budget.os, "waitid", return_value=SimpleNamespace(si_status=0), create=True):
            with self.assertRaisesRegex(TimeoutError, "owned group remained active"):
                budget.stop_group(123, "1", time.monotonic() - 1, child)
        self.assertEqual(waits, [0])

    @unittest.skipUnless(sys.platform == "linux", "actual Linux escaped open-pipe timeout")
    def test_exited_leader_with_escaped_pipe_holder_is_reaped_without_signalling_escape(self):
        import signal
        with tempfile.TemporaryDirectory() as temp:
            pids = Path(temp) / "pids"
            script = textwrap.dedent("""\
                import os, signal, sys, time
                leader = os.getpid()
                child = os.fork()
                if child == 0:
                    os.setsid()
                    signal.signal(signal.SIGTERM, signal.SIG_IGN)
                    with open(sys.argv[1], 'w') as stream:
                        stream.write(str(leader) + ' ' + str(os.getpid()))
                    while True: time.sleep(.01)
                while not os.path.exists(sys.argv[1]): time.sleep(.01)
                """)
            deadline = time.monotonic() + 2
            try:
                with patch.object(budget.os, "killpg", wraps=os.killpg) as send:
                    with self.assertRaises(TimeoutError):
                        budget.run_process([sys.executable, "-c", script, str(pids)], deadline)
                leader, child = map(int, pids.read_text().split())
                self.assertFalse(Path(f"/proc/{leader}").exists())
                self.assertTrue(Path(f"/proc/{child}").exists())
                self.assertTrue(all(call.args[0] != child for call in send.call_args_list))
                self.assertLess(time.monotonic(), deadline + .15)
            finally:
                if pids.exists():
                    _, child = map(int, pids.read_text().split())
                    try:
                        os.kill(child, signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    @unittest.skipUnless(sys.platform == "linux", "actual Linux communicate KeyboardInterrupt")
    def test_keyboard_interrupt_keeps_leader_pinned_until_same_group_descendant_cleanup(self):
        import signal
        import threading
        with tempfile.TemporaryDirectory() as temp:
            pids = Path(temp) / "pids"
            script = textwrap.dedent("""\
                import os, signal, sys, time
                leader = os.getpid()
                child = os.fork()
                if child == 0:
                    signal.signal(signal.SIGTERM, signal.SIG_IGN)
                    with open(sys.argv[1], 'w') as stream:
                        stream.write(str(leader) + ' ' + str(os.getpid()))
                    while True: time.sleep(.01)
                while not os.path.exists(sys.argv[1]): time.sleep(.01)
                """)
            original = budget.stop_group
            observed = []
            def stop(pgid, started, deadline, process=None):
                self.assertIsNone(process.returncode)
                self.assertTrue(Path(f"/proc/{pgid}").exists())
                observed.append(pgid)
                return original(pgid, started, deadline, process)
            timer = threading.Timer(.3, os.kill, args=(os.getpid(), signal.SIGINT))
            deadline = time.monotonic() + 3
            try:
                timer.start()
                with patch.object(budget, "stop_group", side_effect=stop):
                    with self.assertRaises(KeyboardInterrupt):
                        budget.run_process([sys.executable, "-c", script, str(pids)], deadline)
                leader, child = map(int, pids.read_text().split())
                self.assertEqual(observed, [leader])
                self.assertFalse(Path(f"/proc/{leader}").exists())
                proc = Path(f"/proc/{child}/stat")
                self.assertTrue(not proc.exists() or proc.read_text().rsplit(") ", 1)[1].split()[0] == "Z")
                self.assertLess(time.monotonic(), deadline)
            finally:
                timer.cancel()
                timer.join()
                if pids.exists():
                    _, child = map(int, pids.read_text().split())
                    try:
                        os.kill(child, signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    def test_build_setup_round_admission_uses_one_anchor_and_never_grants_more_time(self):
        with patch.object(budget.time, "time", return_value=0), \
                patch.object(budget.time, "monotonic", return_value=0):
            shared = budget.SessionBudget("1970-01-01T03:55:00Z", 3)
        self.assertEqual(shared.cleanup_deadline, 220 * 60)
        self.assertEqual(shared.measurement_deadline, 200 * 60)
        for now, stage, expected in [(10, "server-build", 45), (45, "client-build", 65),
                                     (65, "fences", 75), (75, "setup", 85)]:
            with patch.object(budget.time, "monotonic", return_value=now * 60):
                self.assertEqual(shared.stage_deadline(stage), expected * 60)
        for number, now in [(1, 85), (2, 110), (3, 135)]:
            with patch.object(budget.time, "monotonic", return_value=now * 60):
                self.assertEqual(shared.round_deadline(number), (now + 25) * 60)
        with patch.object(budget.time, "monotonic", return_value=160 * 60):
            self.assertEqual(shared.report_deadline(), 170 * 60)
        for stage in budget.STAGES:
            with patch.object(budget.time, "monotonic", return_value=219 * 60):
                with self.assertRaises(TimeoutError):
                    shared.stage_deadline(stage)
        for number in (1, 2, 3):
            with patch.object(budget.time, "monotonic", return_value=219 * 60):
                with self.assertRaises(TimeoutError):
                    shared.round_deadline(number)
        self.assertEqual(shared.cleanup_deadline, 220 * 60)
        self.assertIs(budget.from_options(SimpleNamespace(budget=shared)), shared)

    def test_short_recovery_rejects_medium_before_build_or_owned_resource_mutation(self):
        deadline = (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat()
        shared = budget.SessionBudget(deadline, 3)
        with patch.object(budget.subprocess, "Popen") as start:
            with self.assertRaises(TimeoutError):
                budget.run_process(["unused"], shared.stage_deadline("server-build"))
            start.assert_not_called()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "unused"
            opts = SimpleNamespace(run_root=root, mega_sha="1" * 40, budget=shared)
            with patch.object(ci, "hosted_root", return_value=(root, "owned")), \
                    patch.object(ci, "dependencies") as deps:
                with self.assertRaises(TimeoutError):
                    ci.execute(opts)
                deps.assert_not_called()
            self.assertFalse(root.exists())

    def test_verified_end_point_rejects_tampering_and_contains_immediate_full_byte_oracle(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            body = b"fixed bytes"
            target = root / "file"
            target.write_bytes(body)
            kind = "executable" if target.stat().st_mode & 0o111 else "regular"
            expected = {"files": [{"rel_path": "file", "fs_kind": kind, "size": len(body),
                                  "content_digest": "sha256:" + hashlib.sha256(body).hexdigest()}]}
            order = []
            def operation():
                order.append("durable")
                return {"durable_complete_ms": 1}
            def oracle(_):
                order.append("oracle")
                bench.verify_worktree(root, expected, time.monotonic() + 30)
            with patch.object(bench.time, "monotonic", side_effect=[1, 2, 3, 4, 5, 6, 7]):
                measured = bench.durable_verified(operation, oracle)
            self.assertEqual(order, ["durable", "oracle"])
            self.assertEqual(measured["durable_byte_oracle_ms"], 4000)
            self.assertEqual(measured["durable_verified_ms"], 6000)
            target.write_bytes(b"wrong bytes")
            with self.assertRaises(AssertionError):
                bench.durable_verified(operation, oracle)

    @unittest.skipUnless(sys.platform == "linux", "actual Linux process-group ownership")
    def test_actual_capture_and_inherited_io_keep_exited_leader_pinned_through_group_cleanup(self):
        original_stop = budget.stop_group
        for capture in (True, False):
            seen = []
            def stop(pgid, started, deadline, process=None):
                self.assertIsNone(process.returncode)
                stat = Path(f"/proc/{pgid}/stat").read_text().rsplit(") ", 1)[1].split()
                self.assertEqual(stat[0], "Z")
                self.assertEqual(stat[19], started)
                seen.append(pgid)
                return original_stop(pgid, started, deadline, process)
            with patch.object(budget, "stop_group", side_effect=stop):
                result = budget.run_process([sys.executable, "-c", "pass"],
                                            time.monotonic() + 5, capture=capture)
            self.assertEqual(result, (0, b"", b"") if capture else (0, None, None))
            self.assertEqual(len(seen), 1)
            self.assertFalse(Path(f"/proc/{seen[0]}").exists())

    @unittest.skipUnless(sys.platform == "linux", "actual Linux process-group ownership")
    def test_actual_timeout_kills_ignoring_descendant_and_reaps_within_original_deadline(self):
        with tempfile.TemporaryDirectory() as temp:
            pid_file = Path(temp) / "pids"
            script = textwrap.dedent("""\
                import os, signal, sys, time
                signal.signal(signal.SIGTERM, signal.SIG_IGN)
                child = os.fork()
                if child == 0:
                    while True: time.sleep(.05)
                with open(sys.argv[1], 'w') as stream:
                    stream.write(str(os.getpid()) + ' ' + str(child))
                while True: time.sleep(.05)
                """)
            deadline = time.monotonic() + 2
            with self.assertRaises(TimeoutError):
                bench.command([sys.executable, "-c", script, str(pid_file)], deadline)
            self.assertLess(time.monotonic(), deadline + .15)
            pids = [int(value) for value in pid_file.read_text().split()]
            for pid in pids:
                proc = Path(f"/proc/{pid}/stat")
                self.assertTrue(not proc.exists() or proc.read_text().rsplit(") ", 1)[1].split()[0] == "Z",
                                "no running child survives the actual timeout")

    @unittest.skipUnless(sys.platform == "linux", "actual Linux owned service finally")
    def test_actual_failed_owned_setup_finally_terminates_service_and_uses_fixed_cleanup_deadline(self):
        self.actual_owned_failure("setup")

    @unittest.skipUnless(sys.platform == "linux", "actual Linux owned round timeout finally")
    def test_actual_round_child_timeout_runs_owned_finally_without_renewing_cleanup(self):
        self.actual_owned_failure("round")

    @unittest.skipUnless(sys.platform == "linux", "actual Linux exited leader cleanup")
    def test_owned_reaped_leader_cannot_hide_live_descendant_that_ignores_term(self):
        self.actual_owned_failure("leader")

    @unittest.skipUnless(sys.platform == "linux", "actual Linux startup identity failure")
    def test_failed_owned_startup_identity_still_reaps_before_owned_finally(self):
        self.actual_owned_failure("startup")

    @unittest.skipUnless(sys.platform == "linux", "actual Linux startup abort ownership")
    def test_command_startup_identity_errors_reap_the_direct_child_without_renewing_deadline(self):
        original = budget.PinnedProcess
        for error in (PermissionError("fixture identity access"), FileNotFoundError("fixture identity missing")):
            spawned = []
            def start(*args, **kwargs):
                process = original(*args, **kwargs)
                spawned.append(process)
                return process
            deadline = time.monotonic() + 2
            with patch.object(budget, "PinnedProcess", side_effect=start), \
                    patch.object(budget, "process_start", side_effect=error):
                with self.assertRaises(PermissionError if isinstance(error, PermissionError) else AssertionError):
                    budget.run_process([sys.executable, "-c", "import time; time.sleep(30)"], deadline)
            self.assertIsNotNone(spawned[0].returncode)
            self.assertFalse(Path(f"/proc/{spawned[0].pid}").exists())
            self.assertLess(time.monotonic(), deadline)
            for pipe in (spawned[0].stdout, spawned[0].stderr):
                pipe.close()

    @unittest.skipUnless(sys.platform == "linux", "actual Linux short child reap")
    def test_missing_exited_startup_leader_is_reaped_without_signalling_an_unbound_group(self):
        original = budget.PinnedProcess
        spawned = []
        def start(*args, **kwargs):
            process = original(*args, **kwargs)
            # EOF proves the short child exited, without poll/wait releasing its
            # PID before the production startup error path reaps it.
            self.assertEqual(process.stdout.read(), b"")
            spawned.append(process)
            return process
        deadline = time.monotonic() + 2
        with patch.object(budget, "PinnedProcess", side_effect=start), \
                patch.object(budget, "process_start", side_effect=FileNotFoundError), \
                patch.object(budget.os, "getpgid", side_effect=ProcessLookupError), \
                patch.object(budget.os, "killpg") as signal_group:
            self.assertEqual(budget.run_process([sys.executable, "-c", "pass"], deadline), (0, b"", b""))
            signal_group.assert_not_called()
        self.assertEqual(spawned[0].returncode, 0)
        self.assertFalse(Path(f"/proc/{spawned[0].pid}").exists())
        self.assertLess(time.monotonic(), deadline)

    @unittest.skipUnless(sys.platform == "linux", "actual Linux group scan checkpoint")
    def test_empty_group_scan_finishing_after_deadline_cannot_report_cleanup_success(self):
        original = budget.group_members
        deadline = time.monotonic() + .05
        def late_scan(*args):
            members = original(*args)
            time.sleep(.07)
            return members
        with patch.object(budget, "group_members", side_effect=late_scan), \
                patch.object(budget.os, "killpg") as signal_group:
            with self.assertRaises(TimeoutError):
                budget.stop_group(-1, "0", deadline)
            signal_group.assert_not_called()

    @unittest.skipUnless(sys.platform == "linux", "actual Linux successful leader cleanup")
    def test_successful_short_command_still_cleans_child_that_closed_inherited_pipes(self):
        with tempfile.TemporaryDirectory() as temp:
            pid_file = Path(temp) / "child"
            script = textwrap.dedent("""\
                import os, signal, sys, time
                child = os.fork()
                if child == 0:
                    signal.signal(signal.SIGTERM, signal.SIG_IGN)
                    for fd in (0, 1, 2): os.close(fd)
                    with open(sys.argv[1], 'w') as stream: stream.write(str(os.getpid()))
                    while True: time.sleep(.05)
                while not os.path.exists(sys.argv[1]): time.sleep(.01)
                """)
            deadline = time.monotonic() + 8
            self.assertEqual(bench.command([sys.executable, "-c", script, str(pid_file)], deadline), b"")
            child = Path(f"/proc/{int(pid_file.read_text())}/stat")
            self.assertTrue(not child.exists() or child.read_text().rsplit(") ", 1)[1].split()[0] == "Z")
            self.assertLess(time.monotonic(), deadline)

    def actual_owned_failure(self, failure):
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp) / "source"
            (source / "target").mkdir(parents=True)
            (source / "config").mkdir()
            binary = source / "target" / "owned-service"
            program = b"#include <unistd.h>\nint main(void) { for (;;) pause(); }\n"
            if failure == "leader":
                program = textwrap.dedent("""\
                    #include <unistd.h>
                    #include <signal.h>
                    #include <stdio.h>
                    #include <stdlib.h>
                    int main(int argc, char **argv) {
                        int sync[2]; char byte; pipe(sync);
                        if (fork() == 0) {
                            signal(SIGTERM, SIG_IGN);
                            close(0); close(1); close(2);
                            char path[8192]; snprintf(path, sizeof(path), "%s.child", argv[2]);
                            FILE *file = fopen(path, "w"); fprintf(file, "%d", getpid()); fclose(file);
                            write(sync[1], "x", 1); for (;;) pause();
                        }
                        read(sync[0], &byte, 1); return 0;
                    }
                    """).encode()
            subprocess.run(["cc", "-x", "c", "-o", str(binary), "-"], input=program,
                           check=True, capture_output=True)
            template = {"base_dir": "unused", "log": {}, "database": {}, "redis": {},
                        "monorepo": {}, "pack": {}, "object_storage": {"s3": {}}, "git": {}}
            (source / "config/config-storage-only.toml").write_text(ci.toml(template))
            driver = source / "driver"
            driver.write_bytes(b"fixed")
            root = Path(temp) / "owned"
            shared = budget.SessionBudget((datetime.now(timezone.utc) + timedelta(hours=3)).isoformat(), 3)
            shared.cleanup_deadline = time.monotonic() + 8
            setup_deadline = time.monotonic() + (1 if failure == "setup" else 5)
            opts = SimpleNamespace(run_root=root, mega_sha="1" * 40, mega_source=source,
                                   mega_binary=binary, driver=driver,
                                   driver_sha256=hashlib.sha256(b"fixed").hexdigest(), budget=shared,
                                   session_deadline_utc=shared.deadline_utc, profile="medium", rounds=3)
            cleanup_deadlines = []
            service_pids = []
            original_stop = ci.stop_owned
            original_command = bench.command
            def stop(*args):
                cleanup_deadlines.append(args[2])
                service_pids.append(args[3].pid)
                return original_stop(*args)
            def command(args, deadline, **_):
                self.assertLessEqual(deadline, shared.cleanup_deadline)
                if "ls-remote" in args:
                    return b"2" * 40 + b"\trefs/heads/main\n"
                return b""
            class Ready:
                status = 200
                def __enter__(self):
                    return self
                def __exit__(self, *_):
                    return False
            def round_timeout(args):
                self.assertIs(args.budget, shared)
                deadline = min(time.monotonic() + 2, args.budget.cleanup_deadline - 1)
                original_command([sys.executable, "-c", "import time; time.sleep(30)"], deadline)
            ready = {"return_value": Ready()} if failure == "round" else {"side_effect": OSError}
            original_process_start = budget.process_start
            def process_start(pid):
                if failure == "startup":
                    raise PermissionError("fixture startup identity failure")
                return original_process_start(pid)
            # Docker/DB are fixture stubs; the service Popen, /proc identity,
            # owned finally, TERM/group cleanup and reap are actual Linux code.
            with patch.dict(os.environ), \
                    patch.object(ci, "hosted_root", return_value=(root, "owned")), \
                    patch.object(shared, "stage_deadline", return_value=setup_deadline), \
                    patch.object(shared, "require"), \
                    patch.object(ci.bench, "git", side_effect=[b"1" * 40, b""]), \
                    patch.object(ci.bench, "command", side_effect=command), \
                    patch.object(ci, "dependencies", return_value={"services": {}}), \
                    patch.object(ci, "initialize_owned_native", return_value={"correctness": "PASS"}), \
                    patch.object(ci, "urlopen", **ready), \
                    patch.object(ci.bench, "execute", side_effect=round_timeout), \
                    patch.object(budget, "process_start", side_effect=process_start), \
                    patch.object(ci, "stop_owned", side_effect=stop), \
                    patch("sys.stdout", new_callable=io.StringIO):
                expected_error = (PermissionError if failure == "startup"
                                  else bench.PhaseFailure if failure == "round" else RuntimeError)
                with self.assertRaises(expected_error):
                    ci.execute(opts)
            self.assertEqual(cleanup_deadlines, [shared.cleanup_deadline])
            state = json.loads((root / "owned.json").read_text())
            self.assertNotIn("service", state)
            self.assertFalse(Path(f"/proc/{service_pids[0]}").exists())
            if failure == "leader":
                child = Path(f"/proc/{int((root / 'service.toml.child').read_text())}/stat")
                self.assertTrue(not child.exists() or child.read_text().rsplit(") ", 1)[1].split()[0] == "Z")
            self.assertLess(time.monotonic(), shared.cleanup_deadline)

    def test_cleanup_inventory_failure_keeps_owned_state_and_never_renews_deadline(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            compose = b"{}"
            (root / "dependencies.json").write_bytes(compose)
            state = {"project": "owned", "compose_sha256": hashlib.sha256(compose).hexdigest()}
            (root / "owned.json").write_text(json.dumps(state))
            deadline = time.monotonic() + 1
            seen = []
            def command(args, supplied, **_):
                seen.append(supplied)
                return b"leftover-owned-container" if "ps" in args else b""
            with patch.object(ci.bench, "command", side_effect=command), \
                    patch("sys.stdout", new_callable=io.StringIO) as output:
                with self.assertRaises(AssertionError):
                    ci.stop_owned(root, "owned", deadline)
                self.assertNotIn('"correctness": "PASS"', output.getvalue())
            self.assertEqual(seen, [deadline, deadline, deadline])
            self.assertEqual(json.loads((root / "owned.json").read_text()), state)

    def test_cleanup_final_inventory_and_metadata_checkpoints_never_emit_late_pass(self):
        for checkpoint in ("inventory", "metadata"):
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                compose = b"{}"
                (root / "dependencies.json").write_bytes(compose)
                state = {"project": "owned", "compose_sha256": hashlib.sha256(compose).hexdigest()}
                (root / "owned.json").write_text(json.dumps(state))
                # Inventory and state serialization each have a final checkpoint.
                times = [0, 2] if checkpoint == "metadata" else [2]
                with patch.object(ci.bench, "command", return_value=b""), \
                        patch.object(ci.time, "monotonic", side_effect=times), \
                        patch("sys.stdout", new_callable=io.StringIO) as output:
                    with self.assertRaises(TimeoutError):
                        ci.stop_owned(root, "owned", 1)
                    self.assertNotIn('"correctness": "PASS"', output.getvalue())


if __name__ == "__main__":
    unittest.main()
