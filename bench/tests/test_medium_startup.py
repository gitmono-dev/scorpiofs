import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


spec = importlib.util.spec_from_file_location("cluster_runner", Path(__file__).resolve().parents[1] / "infra/run-medium-cluster.py")
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class StartupTests(unittest.TestCase):
    def run_preflight(self, fail_capabilities):
        commands = []
        def kubectl(argv, **kwargs):
            commands.append(argv)
            failed = "seed" in argv or (fail_capabilities and "snapshots/capabilities" in argv[-1])
            return subprocess.CompletedProcess(argv, int(failed))
        with tempfile.TemporaryDirectory() as temp, \
                mock.patch.object(sys, "argv", ["runner", "--kubeconfig", "test", "--cluster", "test",
                                               "--stage", "preflight", "--out", temp]), \
                mock.patch.object(runner.subprocess, "run", side_effect=kubectl), \
                self.assertRaisesRegex(RuntimeError, "step failed"):
            runner.main()
        return commands

    def test_runner_storage_and_protocol_ready_before_seed(self):
        commands = self.run_preflight(False)
        self.assertIn("pod/medium-runner", commands[0])
        self.assertIn("job/rustfs-init", commands[1])
        self.assertIn("snapshots/capabilities", commands[2][-1])
        self.assertIn("seed", commands[3])

    def test_protocol_failure_prevents_seed(self):
        commands = self.run_preflight(True)
        self.assertEqual(len(commands), 3)
        self.assertFalse(any("seed" in argv for argv in commands))


if __name__ == "__main__":
    unittest.main()
