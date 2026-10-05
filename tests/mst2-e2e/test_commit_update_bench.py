"""Acceptance checks for the real runner's fences, Git oracle and durability."""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).with_name("commit_update_bench.py")
SPEC = importlib.util.spec_from_file_location("real_bench", SOURCE)
BENCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCH)
CI_SPEC = importlib.util.spec_from_file_location("real_bench_ci", SOURCE.with_name("commit_update_ci.py"))
CI = importlib.util.module_from_spec(CI_SPEC)
CI_SPEC.loader.exec_module(CI)


class CommitUpdateBenchTests(unittest.TestCase):
    def test_ci_setup_plan_cannot_create_local_resources(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "nonexistent-owned-root"
            output = subprocess.check_output([os.sys.executable, str(SOURCE.with_name("commit_update_ci.py")),
                                              "--run-root", str(root)])
            self.assertFalse(json.loads(output)["execute"])
            self.assertFalse(root.exists())
            with patch.dict(os.environ, {"GITHUB_ACTIONS": "false"}):
                with self.assertRaises(ValueError):
                    CI.hosted_root(root)

    def test_ci_compose_has_unique_network_and_rejects_host_bind_storage(self):
        services = {name: {"image": "pinned-upstream", "networks": {"default": None}}
                    for name in ("postgres", "redis", "rustfs", "rustfs-init")}
        config = {"services": services, "networks": {"default": {"name": "shared-old-network"}}}
        ports = {"postgres": 39001, "redis": 39002, "rustfs": 39003}
        with patch.object(CI.bench, "command", return_value=json.dumps(config).encode()):
            safe = CI.dependencies(Path("unused"), "owned-project", ports, time.monotonic() + 60)
        self.assertEqual(safe["networks"]["default"]["name"], "owned-project-network")
        self.assertEqual(safe["services"]["postgres"]["ports"][0]["host_ip"], "127.0.0.1")
        services["postgres"]["volumes"] = ["/existing/data:/var/lib/postgresql"]
        with patch.object(CI.bench, "command", return_value=json.dumps(config).encode()):
            with self.assertRaises(AssertionError):
                CI.dependencies(Path("unused"), "owned-project", ports, time.monotonic() + 60)

    def test_ci_cleanup_rejects_replaced_configuration_before_mutation(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "dependencies.json").write_text("tampered")
            (root / "owned.json").write_text(json.dumps({"project": "owned", "compose_sha256": "0" * 64}))
            with patch.object(CI.bench, "command") as command:
                with self.assertRaises(AssertionError):
                    CI.stop_owned(root, "owned", time.monotonic() + 60)
                command.assert_not_called()

    def test_ci_literal_template_roundtrip_preserves_nested_token_configuration(self):
        config = {"base_dir": "/private/job space", "mst2": {"enabled": True, "instance_uuid": "uuid"},
                  "git": {"push_tokens": [{"name": "job", "token": "${file:/private/token}", "paths": ["/project"]}]},
                  "lfs": {"local": {"lfs_file_path": "${base_dir}/lfs"}}}
        self.assertEqual(CI.bench.tomllib.loads(CI.toml(config)), config)

    def test_remote_service_cannot_be_confused_with_loopback_git_target(self):
        self.assertEqual(BENCH.endpoint_pair("http://127.0.0.1:39091", "http://127.0.0.1:39091/project"), 39091)
        for base, remote in [("http://127.0.0.1:39091", "http://127.0.0.1:39092/project"),
                             ("http://example.invalid:39091", "http://example.invalid:39091/project"),
                             ("http://user:secret@127.0.0.1:39091", "http://127.0.0.1:39091/project")]:
            with self.assertRaises(ValueError):
                BENCH.endpoint_pair(base, remote)

    def test_plan_only_never_reads_service_pid_or_creates_results(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "new-results"
            output = subprocess.check_output([
                os.sys.executable, str(SOURCE), "--base-url", "http://127.0.0.1:39091",
                "--git-url", "http://127.0.0.1:39091/project", "--database", "benchmark_only",
                "--instance-id", "uuid-not-read-in-plan", "--expect-initial-commit", "not-read-in-plan",
                "--service-pid", "99999999", "--driver", str(Path(temp) / "absent-driver"),
                "--driver-sha256", "not-read-in-plan",
                "--run-root", str(root)])
            plan = json.loads(output)
            self.assertFalse(plan["execute"])
            self.assertEqual(plan["rounds"], 5)
            self.assertEqual(plan["max_wall_seconds"], 14400)
            self.assertFalse(root.exists())

    def test_wrong_binary_hash_fails_before_workload_mutation(self):
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as temp:
            driver = Path(temp) / "driver"
            driver.write_bytes(b"fixed reviewed binary")
            options = SimpleNamespace(driver=driver, driver_sha256=hashlib.sha256(driver.read_bytes()).hexdigest())
            BENCH.driver_binding(options)
            driver.write_bytes(b"different binary")
            with self.assertRaises(AssertionError):
                BENCH.driver_binding(options)

    def test_identity_fails_for_stale_project_and_wrong_global_tree(self):
        commit, tree = "1" * 40, "2" * 40
        raw = b"40000 project\0" + bytes.fromhex(tree)
        root_tree = hashlib.sha1(b"tree " + str(len(raw)).encode() + b"\0" + raw).hexdigest()
        rows = [{"path": "/", "commit": "3" * 40, "tree": root_tree, "commit_tree": root_tree,
                 "database": "benchmark_only", "raw_tree": raw.hex()},
                {"path": "/project", "commit": commit, "tree": tree, "commit_tree": tree,
                 "database": "benchmark_only", "raw_tree": None}]
        identity = BENCH.validate_identity(rows, commit, tree, "benchmark_only")
        self.assertEqual(identity["project_commit"], commit)
        with self.assertRaises(AssertionError):
            BENCH.validate_identity(rows, "4" * 40, tree, "benchmark_only")
        rows[0]["raw_tree"] = (b"40000 unrelated\0" + bytes.fromhex(tree)).hex()
        with self.assertRaises(AssertionError):
            BENCH.validate_identity(rows, commit, tree, "benchmark_only")

    def test_native_certificate_requires_same_root_and_outbox(self):
        identity = {"global_commit": "a", "global_tree": "b", "project_commit": "c", "project_tree": "d"}
        native = {"instance_id": "instance", "root_commit": "a", "root_tree": "b", "state": "READY",
                  "writer_epoch": 1, "certificate_id": 7, "receipt_id": 7, "certificate_namespace": "/",
                  "certificate_epoch": 1, "receipt_epoch": 1, "writer_kind": "trunk_push",
                  "request_digest_version": 1, "request_digest": "sha256:" + "0" * 64,
                  "old_oid": "old-root", "old_root_commit": "old-root", "old_path_commit": "old-path",
                  "new_oid": "c", "receipt_namespace": "/project",
                  "certificate_receipt_id": 7, "outbox_id": 9, "certificate_commit": "a",
                  "certificate_tree": "b", "certificate_instance": "instance", "certificate_sequence": 2,
                  "sequence": 2, "path_commit": "c", "path_tree": "d", "origin_path": "/project",
                  "origin_ref": "refs/heads/main", "native_certificate_version": 1,
                  "outbox_namespace": "/project", "outbox_sequence": 11, "origin_sequence": 11}
        BENCH.validate_native(native, identity, "instance", True)
        for field, value in [("outbox_id", None), ("certificate_commit", "stale"), ("certificate_sequence", 1),
                             ("certificate_namespace", "/project"), ("receipt_epoch", 2),
                             ("writer_epoch", 2), ("sequence", -1), ("writer_kind", "other"),
                             ("request_digest_version", 0), ("request_digest", "sha256:" + "A" * 64),
                             ("old_oid", "unrelated"), ("new_oid", "stale"), ("old_path_commit", "c"),
                             ("receipt_namespace", "/unrelated")]:
            wrong = dict(native, **{field: value})
            with self.assertRaises(AssertionError):
                BENCH.validate_native(wrong, identity, "instance", True)

    def test_real_git_changes_one_body_then_renames_without_new_body(self):
        with tempfile.TemporaryDirectory() as temp:
            repo = Path(temp) / "fixture"
            deadline = time.monotonic() + 120
            subprocess.run(["git", "init", "-q", "-b", "main", str(repo)], check=True)
            env = BENCH.clean_env({"GIT_AUTHOR_NAME": "test", "GIT_COMMITTER_NAME": "test",
                                   "GIT_AUTHOR_EMAIL": "test@example.invalid", "GIT_COMMITTER_EMAIL": "test@example.invalid"})
            BENCH.git(repo, deadline, "commit", "--allow-empty", "-qm", "seed", env=env)
            manifests = []
            for version in ("v1", "v2", "v3"):
                commit, _ = BENCH.create_version(repo, 1, version, True, deadline)
                manifests.append(BENCH.expected_manifest(repo, commit, deadline))
            sets = [{f["content_digest"] for f in manifest["files"]} for manifest in manifests]
            self.assertEqual(len(sets[1] - sets[0]), 1)
            self.assertEqual(sets[2], sets[1])
            self.assertEqual(len(manifests[0]["files"]), len(manifests[2]["files"]))
            self.assertTrue(any(f["rel_path"].startswith("r01/renamed-m001/") for f in manifests[2]["files"]))
            self.assertIn("empty-a", manifests[2]["directories"])
            self.assertIn("empty-b", manifests[2]["directories"])
            self.assertIn("alias-r01-m007", manifests[2]["directories"])
            # A second repetition needs a distinct cold content set and tree.
            commit, _ = BENCH.create_version(repo, 2, "v1", True, deadline)
            new = BENCH.expected_manifest(repo, commit, deadline)
            self.assertTrue(all(f["rel_path"].startswith(("r02/", "alias-r02-m007/")) for f in new["files"]))
            self.assertFalse(sets[0] & {f["content_digest"] for f in new["files"]})

    def test_independent_git_worktree_oracle_rejects_tampered_bytes_and_extra_entries(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = root / "file"
            path.write_bytes(b"original")
            # Windows has no Unix execute-bit contract, so derive the local
            # kind for this byte-integrity test independently of Git checkout.
            kind = "executable" if path.stat().st_mode & 0o111 else "regular"
            expected = {"files": [{"rel_path": "file", "fs_kind": kind, "size": 8,
                                  "content_digest": "sha256:" + hashlib.sha256(b"original").hexdigest()}]}
            BENCH.verify_worktree(root, expected)
            path.write_bytes(b"tampered")
            with self.assertRaises(AssertionError):
                BENCH.verify_worktree(root, expected)
            path.write_bytes(b"original")
            (root / "extra").write_bytes(b"extra")
            with self.assertRaises(AssertionError):
                BENCH.verify_worktree(root, expected)

    def test_timeout_terminates_whole_child_group_then_kills_if_term_is_ignored(self):
        class Hung:
            pid = 12345
            calls = 0
            def communicate(self, data=None, timeout=None):
                self.calls += 1
                if self.calls <= 2:
                    raise subprocess.TimeoutExpired("ignored", timeout)
                return b"", b""
        child = Hung()
        with patch.object(BENCH.subprocess, "Popen", return_value=child), \
                patch.object(BENCH.os, "killpg", create=True) as killpg, \
                patch.object(BENCH.signal, "SIGKILL", 9, create=True):
            with self.assertRaises(TimeoutError):
                BENCH.command(["driver"], time.monotonic() + 30)
            self.assertEqual([call.args[1] for call in killpg.call_args_list],
                             [BENCH.signal.SIGTERM, BENCH.signal.SIGKILL])
            self.assertTrue(all(call.args[0] == 12345 for call in killpg.call_args_list))
            self.assertEqual(child.calls, 3)

    @unittest.skipUnless(hasattr(os, "O_DIRECTORY") and hasattr(os, "O_NOFOLLOW"), "Linux durability primitive")
    def test_durable_flush_includes_regular_files_and_directories_without_following_symlinks(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "worktree"
            root.mkdir()
            (root / "nested").mkdir()
            (root / "nested/data").write_bytes(b"fixed commit")
            (root / "link").symlink_to("nested/data")
            result = BENCH.fsync_tree(root, time.monotonic() + 30)
            self.assertEqual(result, {"regular_files": 1, "directories": 2, "file_bytes": 12})
            alias = Path(temp) / "alias"
            alias.symlink_to(root, target_is_directory=True)
            with self.assertRaises(AssertionError):
                BENCH.fsync_tree(alias, time.monotonic() + 30)


if __name__ == "__main__":
    unittest.main()
