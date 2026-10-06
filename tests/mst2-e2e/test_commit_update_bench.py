"""Acceptance checks for the real runner's fences, Git oracle and durability."""

import hashlib
from datetime import datetime, timedelta, timezone
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
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
from workspace_update_worker import WorkerError


class CommitUpdateBenchTests(unittest.TestCase):
    def test_owned_native_bootstrap_rejects_other_database_host_and_noncanonical_instance(self):
        database = "mst2_bench_" + "a" * 32
        instance = "6ab219b0-4275-45ba-9d7b-7b0b633018cd"
        env = {"PGHOST": "127.0.0.1", "PGDATABASE": database}
        cases = [("existing_database", instance, env),
                 (database, instance, dict(env, PGDATABASE="other")),
                 (database, instance, dict(env, PGHOST="remote.example")),
                 (database, instance.upper(), env),
                 (database, "' OR 1=1 --", env)]
        with patch.object(CI.bench, "command") as command:
            for db, uuid, connection in cases:
                with self.assertRaises(ValueError):
                    CI.initialize_owned_native(db, uuid, connection, time.monotonic() + 30)
            command.assert_not_called()

    def test_owned_native_bootstrap_independent_readback_rejects_stale_root_or_epoch(self):
        database = "mst2_bench_" + "a" * 32
        instance = "6ab219b0-4275-45ba-9d7b-7b0b633018cd"
        tree = "2" * 40
        raw = b"40000 project\0" + bytes.fromhex(tree)
        root_tree = hashlib.sha1(b"tree " + str(len(raw)).encode() + b"\0" + raw).hexdigest()
        rows = [{"path": "/", "commit": "3" * 40, "tree": root_tree, "commit_tree": root_tree,
                 "database": database, "raw_tree": raw.hex()}]
        native = {"instance_id": instance, "root_commit": "3" * 40, "root_tree": root_tree,
                  "writer_epoch": 1, "sequence": 0, "state": "INITIALIZING", "certificate_receipt_id": None}
        env = {"PGHOST": "127.0.0.1", "PGDATABASE": database}
        with patch.object(CI.bench, "command"), patch.object(CI.bench, "query", side_effect=[rows, native]):
            record = CI.initialize_owned_native(database, instance, env, time.monotonic() + 30)
            self.assertEqual(record["global_commit"], rows[0]["commit"])
            self.assertEqual(record["global_tree"], root_tree)
            self.assertEqual(record["correctness"], "PASS")
        # Before the Git route has materialized /project, only the bootstrap
        # fence can pass. The normal post-readiness fence remains stricter.
        with self.assertRaises(AssertionError):
            BENCH.validate_identity(rows, "1" * 40, tree, database)
        for wrong in [None, dict(native, root_commit="4" * 40), dict(native, writer_epoch=2),
                      dict(native, instance_id="different"), dict(native, sequence=1),
                      dict(native, state="READY"), dict(native, certificate_receipt_id=1)]:
            with patch.object(CI.bench, "command"), patch.object(CI.bench, "query", side_effect=[rows, wrong]):
                with self.assertRaises(AssertionError):
                    CI.initialize_owned_native(database, instance, env, time.monotonic() + 30)

        for wrong_rows in [None, [], rows * 2, [dict(rows[0], path="/project")],
                           [dict(rows[0], database="other")],
                           [dict(rows[0], commit_tree="4" * 40)],
                           [dict(rows[0], raw_tree=(raw + b"tampered").hex())]]:
            with patch.object(CI.bench, "command"), patch.object(CI.bench, "query", side_effect=[wrong_rows, native]):
                with self.assertRaises(AssertionError):
                    CI.initialize_owned_native(database, instance, env, time.monotonic() + 30)

    def test_failure_records_expose_fixed_phase_without_private_exception_text(self):
        with self.assertRaises(BENCH.PhaseFailure) as failed:
            with BENCH.phase("owned_native_initialization"):
                raise AssertionError("private-token-do-not-log")
        record = BENCH.failure_record(failed.exception)
        self.assertEqual(record, {"execution_failed": True, "error_type": "AssertionError",
                                  "phase": "owned_native_initialization"})
        self.assertNotIn("private-token", json.dumps(record))
        with self.assertRaises(BENCH.PhaseFailure) as nested:
            with BENCH.phase("commit_update_benchmark"):
                with BENCH.phase("updated_publication_identity"):
                    raise KeyError("private-connection-string")
        self.assertEqual(BENCH.failure_record(nested.exception)["phase"], "updated_publication_identity")

    def test_worker_failure_record_carries_only_a_closed_error_code(self):
        private = "private-token /run/secret response body"
        inner = BENCH.PhaseFailure(
            "shipped_workspace_and_git_measurement",
            WorkerError(private, error_code="worker_http_status_rejected"),
        )
        outer = BENCH.PhaseFailure("commit_update_benchmark", inner)
        self.assertEqual(BENCH.failure_record(outer)["error_code"],
                         "worker_http_status_rejected")
        with self.assertRaises(BENCH.PhaseFailure) as failed:
            with BENCH.phase("commit_update_benchmark"):
                with BENCH.phase("shipped_workspace_and_git_measurement"):
                    raise WorkerError(private, error_code="worker_http_status_rejected")
        record = BENCH.failure_record(failed.exception)
        self.assertEqual(record["error_code"], "worker_http_status_rejected")
        self.assertNotIn(private, json.dumps(record))
        self.assertNotIn("/run/secret", json.dumps(record))

        unknown = WorkerError(private, error_code="private-code")
        self.assertEqual(unknown.error_code, "worker_error")
        self.assertEqual(BENCH.failure_record(unknown)["error_code"], "worker_error")

    def test_worker_failure_record_carries_only_a_closed_substage(self):
        private = "private-token /run/secret response body"
        error = WorkerError(private, error_code="worker_http_status_5xx")
        error.worker_stage = "poll"
        with self.assertRaises(BENCH.PhaseFailure) as failed:
            with BENCH.phase("commit_update_benchmark"):
                with BENCH.phase("shipped_workspace_and_git_measurement"):
                    raise error
        record = BENCH.failure_record(failed.exception)
        self.assertEqual(record["error_code"], "worker_http_status_5xx")
        self.assertEqual(record["worker_stage"], "poll")
        self.assertNotIn(private, json.dumps(record))
        self.assertNotIn("/run/secret", json.dumps(record))

        error.worker_stage = "private-path"
        self.assertNotIn("worker_stage", BENCH.failure_record(error))

    def test_worker_failure_record_carries_only_a_closed_backend_code(self):
        error = WorkerError("private response body", error_code="worker_http_status_5xx",
                            backend_code="SNAPSHOT_ERROR")
        record = BENCH.failure_record(error)
        self.assertEqual(record["backend_code"], "SNAPSHOT_ERROR")
        self.assertNotIn("private response body", json.dumps(record))
        unknown = WorkerError("private response body", backend_code="private-token")
        self.assertNotIn("backend_code", BENCH.failure_record(unknown))

    def test_ci_persists_only_safe_failure_record(self):
        private = "private-token /run/secret response body"
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            measurements = root / "measurements"
            measurements.mkdir()
            CI.persist_failure_record(root, WorkerError(private,
                                                        error_code="worker_http_status_rejected"))
            path = measurements / "failure.json"
            self.assertTrue(path.is_file())
            raw = path.read_text(encoding="ascii")
            self.assertNotIn(private, raw)
            self.assertNotIn("/run/secret", raw)
            self.assertEqual(json.loads(raw), {
                "error_code": "worker_http_status_rejected",
                "error_type": "WorkerError",
                "execution_failed": True,
            })

    def test_command_failure_keeps_only_closed_fields(self):
        private = b'private-token and child stderr must never be copied into the record\n'
        with self.assertRaises(BENCH.PhaseFailure) as failed:
            with BENCH.phase("scorpio_sync"):
                raise BENCH.CommandFailure("scorpio", 1, private)
        self.assertEqual(BENCH.failure_record(failed.exception), {
            "execution_failed": True, "error_type": "CommandFailure", "phase": "scorpio_sync",
            "command": "scorpio", "exit_status": 1,
        })
        self.assertNotIn("private-token", json.dumps(BENCH.failure_record(failed.exception)))
        error = BENCH.CommandFailure("git", 128, private)
        self.assertEqual(BENCH.failure_record(error), {
            "execution_failed": True, "error_type": "CommandFailure", "command": "git", "exit_status": 128,
        })

    def test_query_reports_safe_phase_without_echoing_invalid_output(self):
        for sql, phase in [(BENCH.IDENTITY_SQL, "Git identity"), (BENCH.NATIVE_SQL, "native publication")]:
            with patch.object(BENCH, "command", return_value=b"private-token-do-not-log"):
                with self.assertRaises(AssertionError) as error:
                    BENCH.query(sql, time.monotonic() + 30)
                self.assertIn(phase, str(error.exception))
                self.assertNotIn("private-token", str(error.exception))
        with patch.object(BENCH, "command", return_value=b"null\n"):
            self.assertIsNone(BENCH.query(BENCH.NATIVE_SQL, time.monotonic() + 30))

    def test_workflow_recovery_preserves_deadline_and_rejects_extension_before_setup(self):
        workflow = SOURCE.parents[2] / ".github/workflows/mst2-workspace-update.yml"
        script = textwrap.dedent(workflow.read_text(encoding="utf-8").split("python3 -B - <<'PY'\n", 1)[1].split("\n          PY", 1)[0])
        started = datetime.now(timezone.utc) - timedelta(minutes=1)
        deadline = (started + timedelta(minutes=235)).isoformat()
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "github-env"
            env = dict(os.environ, GITHUB_ENV=str(output), RUNNER_TEMP=temp,
                       GITHUB_RUN_ID="123", GITHUB_RUN_ATTEMPT="2",
                       STARTED_INPUT=started.isoformat(), DEADLINE_INPUT=deadline,
                       PYTHONPATH=str(SOURCE.parent))
            for _ in range(2):
                subprocess.run([os.sys.executable, "-c", script], check=True, env=env, capture_output=True)
                self.assertIn("MST2_SESSION_STARTED=" + started.isoformat(), output.read_text())
                self.assertIn("MST2_SESSION_DEADLINE=" + deadline, output.read_text())
                output.unlink()
            for bad in [dict(env, DEADLINE_INPUT=(started + timedelta(minutes=236)).isoformat()),
                        dict(env, STARTED_INPUT=(started - timedelta(minutes=20)).isoformat(),
                             DEADLINE_INPUT=(started + timedelta(minutes=215)).isoformat()),
                        dict(env, DEADLINE_INPUT="2000-01-01T00:00:00Z"),
                        dict(env, DEADLINE_INPUT="2099-01-01T00:00:00Z"),
                        dict(env, DEADLINE_INPUT=deadline[:-6]),
                        dict(env, DEADLINE_INPUT=deadline[:-6] + "+08:00"),
                        dict(env, STARTED_INPUT="")]:
                result = subprocess.run([os.sys.executable, "-c", script], env=bad, capture_output=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(output.exists())

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
            self.assertEqual(plan["rounds"], 3)
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
            returncode = None
            calls = 0
            def communicate(self, data=None, timeout=None):
                self.calls += 1
                if self.calls <= 2:
                    raise subprocess.TimeoutExpired("ignored", timeout)
                return b"", b""
        child = Hung()
        with patch.object(BENCH.budget_module.subprocess, "Popen", return_value=child), \
                patch.object(BENCH.budget_module, "PinnedProcess", return_value=child), \
                patch.object(BENCH.budget_module, "process_start", return_value="1"), \
                patch.object(BENCH.budget_module, "group_members", return_value=[12345]), \
                patch.object(BENCH.budget_module, "stop_group") as stop_group, \
                patch.object(BENCH.os, "killpg", create=True) as killpg, \
                patch.object(BENCH.budget_module.signal, "SIGKILL", 9, create=True):
            with self.assertRaises(TimeoutError):
                BENCH.command(["driver"], time.monotonic() + 30)
            self.assertEqual([call.args[1] for call in killpg.call_args_list],
                             [BENCH.budget_module.signal.SIGTERM, BENCH.budget_module.signal.SIGKILL])
            self.assertTrue(all(call.args[0] == 12345 for call in killpg.call_args_list))
            self.assertEqual(child.calls, 3)
            self.assertEqual(stop_group.call_args.args[:2], (12345, "1"))
            self.assertIs(stop_group.call_args.args[3], child)

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
