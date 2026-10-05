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

    def actual_owned_failure(self, failure):
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp) / "source"
            (source / "target").mkdir(parents=True)
            (source / "config").mkdir()
            binary = source / "target" / "owned-service"
            subprocess.run(["cc", "-x", "c", "-o", str(binary), "-"],
                           input=b"#include <unistd.h>\nint main(void) { for (;;) pause(); }\n",
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
            ready = {"side_effect": OSError} if failure == "setup" else {"return_value": Ready()}
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
                    patch.object(ci, "stop_owned", side_effect=stop), \
                    patch("sys.stdout", new_callable=io.StringIO):
                with self.assertRaises(RuntimeError if failure == "setup" else bench.PhaseFailure):
                    ci.execute(opts)
            self.assertEqual(cleanup_deadlines, [shared.cleanup_deadline])
            state = json.loads((root / "owned.json").read_text())
            self.assertNotIn("service", state)
            self.assertFalse(Path(f"/proc/{service_pids[0]}").exists())
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


if __name__ == "__main__":
    unittest.main()
