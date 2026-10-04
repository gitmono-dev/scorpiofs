import contextlib
import datetime as dt
import importlib.util
import io
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


BENCH = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gate = load("shallow_gate", BENCH / "cases/git-shallow-gate.py")
driver = load("medium_driver", BENCH / "infra/medium-cloud-run.py")
controller = load("ack_controller", BENCH / "infra/ack-medium.py")
auditor = load("resource_auditor", BENCH / "infra/audit-medium-resources.py")


class ShallowGateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.oracle = self.root / "oracle"
        self.oracle.mkdir()
        self.git(self.oracle, "init", "-b", "main")
        self.git(self.oracle, "config", "user.name", "Gate Test")
        self.git(self.oracle, "config", "user.email", "gate@example.invalid")
        self.git(self.oracle, "config", "commit.gpgsign", "false")
        self.commits = []
        self.blobs = []
        for index in range(3):
            for child in self.oracle.glob("*.txt"):
                child.unlink()
            (self.oracle / f"file-{index}.txt").write_text(f"unique content {index}\n")
            self.git(self.oracle, "add", "-A")
            self.git(self.oracle, "commit", "-m", f"fixture {index}")
            self.commits.append(self.git(self.oracle, "rev-parse", "HEAD"))
            self.blobs.append(self.git(self.oracle, "rev-parse", f"HEAD:file-{index}.txt"))

    @staticmethod
    def git(path, *args, data=None):
        env = dict(os.environ, GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull)
        return subprocess.check_output(["git", "-C", str(path), *args], input=data,
                                       env=env, stderr=subprocess.PIPE).decode().strip()

    def clone(self, depth):
        destination = self.root / f"depth-{depth}"
        subprocess.run(["git", "clone", "--depth", str(depth), self.oracle.as_uri(),
                        str(destination)], check=True, stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE)
        return destination

    def test_depth_one_and_two_have_exact_object_sets(self):
        for depth in (1, 2):
            with self.subTest(depth=depth):
                result = gate.verify_clone(self.clone(depth), self.oracle, depth=depth)
                self.assertTrue(result["strict_object_set_verified"])
                self.assertEqual(result["actual_object_count"], 3 * depth)

    def test_parent_only_blob_is_rejected_without_extra_commit(self):
        clone = self.clone(1)
        body = subprocess.check_output(["git", "-C", str(self.oracle),
                                        "cat-file", "blob", self.blobs[1]])
        self.git(clone, "hash-object", "-w", "--stdin", data=body)
        with self.assertRaisesRegex(RuntimeError, "1 extra"):
            gate.verify_clone(clone, self.oracle)

    def test_complete_history_at_and_beyond_root(self):
        for depth in (3, 5):
            with self.subTest(depth=depth):
                result = gate.verify_clone(self.clone(depth), self.oracle, depth=depth)
                self.assertEqual(result["commits"], list(reversed(self.commits)))
                self.assertEqual(result["actual_object_count"], 9)

    def test_parent_only_tree_is_rejected(self):
        clone = self.clone(1)
        tree = self.git(self.oracle, "rev-parse", self.commits[1] + "^{tree}")
        body = subprocess.check_output(["git", "-C", str(self.oracle), "cat-file", "tree", tree])
        self.git(clone, "hash-object", "-t", "tree", "-w", "--stdin", data=body)
        with self.assertRaisesRegex(RuntimeError, "extra"):
            gate.verify_clone(clone, self.oracle)

    def test_overdeep_clone_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "wrong commit depth"):
            gate.verify_clone(self.clone(2), self.oracle)

    def test_optimized_python_still_rejects_wrong_oracle(self):
        output = self.root / "result.json"
        result = subprocess.run([sys.executable, "-O", str(BENCH / "cases/git-shallow-gate.py"),
                                 "--repo", self.oracle.as_uri(), "--oracle", str(self.oracle),
                                 "--commit", self.commits[0], "--out", str(output)],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(json.loads(output.read_text())["status"], "failed")


class DriverGateTests(unittest.TestCase):
    def test_missing_failed_stale_or_incomplete_gate_blocks_measurement(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "gate.json"
            with self.assertRaises(FileNotFoundError):
                driver.require_shallow_gate(path, "fixed", "repo")
            valid = dict(status="success", head="fixed", repo="repo", depth=1,
                         strict_object_set_verified=True)
            for key, value in (("status", "failed"), ("head", "stale"), ("repo", "other"),
                               ("depth", 2), ("strict_object_set_verified", False)):
                with self.subTest(key=key):
                    path.write_text(json.dumps(dict(valid, **{key: value})))
                    with self.assertRaises(RuntimeError):
                        driver.require_shallow_gate(path, "fixed", "repo")
            path.write_text(json.dumps(valid))
            self.assertEqual(driver.require_shallow_gate(path, "fixed", "repo"), valid)

    def test_driver_failure_exits_nonzero(self):
        with tempfile.TemporaryDirectory() as temp:
            argv = ["medium-cloud-run.py", "seed", "--cluster", "test", "--files", "50000",
                    "--out", temp]
            with mock.patch.object(sys, "argv", argv), \
                    mock.patch("builtins.open", side_effect=RuntimeError("fixture read failed")), \
                    contextlib.redirect_stdout(io.StringIO()), self.assertRaises(SystemExit) as error:
                runpy.run_path(str(BENCH / "infra/medium-cloud-run.py"), run_name="__main__")
            self.assertEqual(error.exception.code, 1)

    def test_expired_deadline_cannot_provision(self):
        past = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)
        with self.assertRaisesRegex(RuntimeError, "deadline reached"):
            controller.require_time_remaining({"deadline_utc": past.isoformat()})
        with self.assertRaisesRegex(RuntimeError, "deadline is required"):
            controller.require_time_remaining({})
        future = dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1)
        controller.require_time_remaining({"deadline_utc": future.isoformat()})

    def test_unrecorded_cluster_from_timed_out_create_blocks_cleanup(self):
        with tempfile.TemporaryDirectory() as temp:
            state_path = Path(temp) / "state.json"
            output = Path(temp) / "audit.json"
            inventory = {key: {item: []} for key, item in (
                ("Disks", "Disk"), ("Instances", "Instance"), ("NatGateways", "NatGateway"),
                ("EipAddresses", "EipAddress"), ("LoadBalancers", "LoadBalancer"),
                ("SecurityGroups", "SecurityGroup"), ("Snapshots", "Snapshot"),
                ("ScalingGroups", "ScalingGroup"))}
            state_path.write_text(json.dumps(dict(
                clusters=[], run_id="test", region="test-region",
                preflight=dict(disks_before=inventory, ecs_before=inventory))))

            def api(*args):
                if args[:3] == ("cs", "GET", "/clusters"):
                    return [dict(cluster_id="unrecorded", name="mst2-medium-a-test")]
                return inventory

            argv = ["audit", "--state", str(state_path), "--out", str(output), "--mark-clean"]
            with mock.patch.object(sys, "argv", argv), \
                    mock.patch.object(auditor.runpy, "run_path", return_value={"cli": api}), \
                    self.assertRaises(SystemExit):
                auditor.main()
            result = json.loads(output.read_text())
            self.assertEqual(result["remaining_counts"]["clusters"], 1)
            self.assertFalse(result["cleanup_verified"])
            self.assertNotIn("cleanup_verified", json.loads(state_path.read_text()))


if __name__ == "__main__":
    unittest.main()
