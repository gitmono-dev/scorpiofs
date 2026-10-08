from dataclasses import replace
from datetime import datetime, timedelta, timezone
from copy import deepcopy
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest.mock import Mock, patch
from contextlib import ExitStack, redirect_stdout
from types import SimpleNamespace

import workspace_update_backend as backend


class BackendOwnershipTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.root = self.base / "campaign"
        self.source = self.base / "server"
        self.source.joinpath("target/release").mkdir(parents=True)
        self.source.joinpath("config").mkdir()
        self.binary = self.source / "target/release/mega2"
        self.binary.write_bytes(b"fixed server executable")
        self.source.joinpath("Cargo.lock").write_bytes(b"fixed lock")
        template = {"log": {}, "database": {}, "redis": {}, "monorepo": {},
                    "pack": {}, "object_storage": {"s3": {}}, "git": {}}
        self.source.joinpath("config/config-storage-only.toml").write_text(backend.ci.toml(template))
        self.options = SimpleNamespace(run_root=self.root, mega_source=self.source,
                                       mega_binary=self.binary, mega_sha="1" * 40)
        self.clock = {"now": 100.0}
        self.patch("workspace_update_backend.time.monotonic", side_effect=lambda: self.clock["now"])
        self.hosted = self.patch("workspace_update_backend.ci.hosted_root", return_value=(self.root, "m2perf-77-1"))
        deadline = (datetime.now(timezone.utc) + timedelta(minutes=230)).isoformat()
        self.budget = backend.budget_module.SessionBudget(deadline, 3, paired=True)
        self.group = backend.BackendGroup(self.options, self.budget)

    def patch(self, target, **kwargs):
        started = patch(target, **kwargs)
        result = started.start()
        self.addCleanup(started.stop)
        return result

    def admit(self, client="a", phase="fair", number=1, deadline=1000):
        return self.group.admit(phase, number, client, deadline)

    def materialize_owner(self, owner, state="running"):
        owner.root.mkdir()
        info = owner.root.stat()
        owner.root_identity = (info.st_dev, info.st_ino)
        backend._write(owner.root / "owned.json", {"project": owner.project}, new=True)
        owner.owned_digest = backend._digest(owner.root / "owned.json")
        backend._write_text(owner.root / "service.toml", "fixed config", new=True)
        backend._write(owner.root / "dependencies.json", {"owned": owner.project}, new=True)
        owner.config_digest = backend._digest(owner.root / "service.toml")
        owner.compose_digest = backend._digest(owner.root / "dependencies.json")
        owner._transition(state)
        return owner

    def fake_binding(self, owner):
        return backend.RuntimeBinding(
            revision=1, phase=owner.phase, round=owner.number, client=owner.client,
            project=owner.project, database=owner.database, instance_id=owner.instance_id,
            base_url="http://127.0.0.1:" + ("1234" if owner.client == "a" else "1235"),
            git_url="http://127.0.0.1:" + ("1234" if owner.client == "a" else "1235") + "/project",
            server_source_sha="1" * 40, server_source_tree="2" * 40,
            server_binary_sha256="3" * 64, server_cargo_lock_sha256="4" * 64,
            service_pid=100 if owner.client == "a" else 101, service_starttime="42",
            config_sha256="5" * 64, compose_sha256="6" * 64, cache_prefix=owner.project,
            base_dir=str(owner.root / "service-data"), cache_dir=str(owner.root / "cache"),
            pack_cache_dir=str(owner.root / "pack-cache"),
            projection_sink_instance=owner.instance_id, projection_sink_root=str(owner.root / "cache/logs/mst2-native-projection" / owner.instance_id),
            projection_sink_device=1, projection_sink_inode=100 if owner.client == "a" else 101,
            dependency_container_ids=tuple(("a" if owner.client == "a" else "b") + str(i) * 63 for i in range(4)),
            identity_rows_json="[]", identity_json='{"project_commit":"' + "7" * 40 + '","project_tree":"' + "8" * 40 + '"}',
            native_json="{}")

    def test_registers_owner_and_original_phase_before_startup(self):
        owner = self.admit()
        saved = json.loads(self.group.state_path.read_text())
        self.assertEqual(saved["backends"][0]["state"], "admitted")
        self.assertEqual(saved["backends"][0]["operation_deadline_monotonic"], 1000)
        self.assertEqual(saved["cleanup_deadline_monotonic"], self.budget.cleanup_deadline)
        self.assertFalse(owner.root.exists())

    def test_rejects_duplicate_invalid_or_reanchored_admission(self):
        first = self.admit()
        cases = [("fair", 1, "a", 1000), ("fair", True, "b", 1000),
                 ("diagnostic", 1, "a", 1000), ("fair", 4, "b", 1000),
                 ("fair", 2, "b", self.budget.measurement_deadline + 1)]
        for args in cases:
            with self.subTest(args=args), self.assertRaises(ValueError):
                self.group.admit(*args)
        with patch.object(backend.ci, "stop_owned"):
            first.stop(self.budget.cleanup_deadline)
        owner = self.group.admit("diagnostic", 1, "b", 1000)
        self.assertEqual(owner.key, ("diagnostic", 1, "b"))

    def test_active_round_blocks_next_round_and_diagnostic_before_registration(self):
        first, second = self.admit(), self.admit("b")
        original = self.group.state_path.read_bytes()
        with patch.object(backend.common, "command") as command:
            for phase, number, client in (("fair", 2, "a"), ("fair", 2, "b"), ("diagnostic", 1, "b")):
                with self.subTest(phase=phase, client=client), self.assertRaisesRegex(ValueError, "must retire"):
                    self.group.admit(phase, number, client, 1000)
            command.assert_not_called()
        self.assertEqual(self.group.state_path.read_bytes(), original)
        self.assertEqual(len(self.group.backends), 2)
        self.assertEqual(list(self.root.joinpath("backends").iterdir()), [])
        with patch.object(backend.ci, "stop_owned"):
            first.stop(self.budget.cleanup_deadline)
        with self.assertRaisesRegex(ValueError, "must retire"):
            self.group.admit("fair", 2, "a", 1000)
        with patch.object(backend.ci, "stop_owned"):
            second.stop(self.budget.cleanup_deadline)
        next_a = self.group.admit("fair", 2, "a", 1000)
        next_b = self.group.admit("fair", 2, "b", 1000)
        self.assertEqual(len([b for b in self.group.backends if b.state != "retired"]), 2)
        with self.assertRaisesRegex(ValueError, "must retire"):
            self.group.admit("diagnostic", 1, "b", 1000)
        with patch.object(backend.ci, "stop_owned"):
            next_a.stop(self.budget.cleanup_deadline)
            next_b.stop(self.budget.cleanup_deadline)
        diagnostic = self.group.admit("diagnostic", 1, "b", 1000)
        self.assertEqual([b for b in self.group.backends if b.state != "retired"], [diagnostic])
        with self.assertRaisesRegex(ValueError, "must retire"):
            self.group.admit("fair", 3, "a", 1000)

    def test_all_operations_reject_extension_of_admitted_deadline(self):
        owner = self.admit()
        with self.assertRaises(AttributeError):
            owner.operation_deadline = 1001
        operations = [lambda: owner.start(1001), lambda: owner.verify_runtime(1001),
                      lambda: owner.tip(1001), lambda: owner.publish_seed(self.base, "1" * 40, "2" * 40, "3" * 40, 1001)]
        for operation in operations:
            with self.subTest(operation=operation), self.assertRaisesRegex(ValueError, "admitted phase"):
                operation()

    def test_budget_or_registry_replacement_prevents_side_effect(self):
        owner = self.admit()
        original = self.group.state_path.read_bytes()
        self.group.state_path.write_bytes(original + b" ")
        with patch.object(backend.common, "command") as command:
            with self.assertRaisesRegex(AssertionError, "ownership"):
                owner.start(1000)
            command.assert_not_called()
        self.group.state_path.write_bytes(original)
        self.budget.cleanup_deadline += 1
        with self.assertRaisesRegex(AssertionError, "original window"):
            owner.start(1000)

    def test_cleanup_attempts_every_owner_and_retains_retry_obligation(self):
        first, second = self.admit(), self.admit("b")
        calls = []
        def stop(owner):
            def run(deadline):
                calls.append((owner.client, deadline))
                if owner is second:
                    raise KeyboardInterrupt()
                owner._transition("retired")
            return run
        with patch.object(first, "stop", side_effect=stop(first)), patch.object(second, "stop", side_effect=stop(second)):
            with self.assertRaises(BaseExceptionGroup) as caught:
                self.group.close()
        self.assertIsInstance(caught.exception.exceptions[0], KeyboardInterrupt)
        self.assertEqual(calls, [("b", self.budget.cleanup_deadline), ("a", self.budget.cleanup_deadline)])
        self.assertFalse(self.group.closed)
        self.assertEqual(first.state, "retired")

    def test_second_startup_failure_preserves_primary_when_cleanup_also_fails(self):
        primary = RuntimeError("second startup failed")
        with patch.object(backend.OwnedBackend, "start", side_effect=[None, primary]), patch.object(backend.OwnedBackend, "stop", side_effect=TimeoutError("cleanup failed")) as stopped:
            with self.assertRaises(RuntimeError) as caught:
                self.group.start_pair(1, 1000)
        self.assertIs(caught.exception, primary)
        self.assertEqual(stopped.call_count, 2)
        self.assertFalse(self.group.closed)
        self.assertEqual(len(self.group.backends), 2)

    def test_context_manager_preserves_primary_and_closes_on_success(self):
        with patch.object(self.group, "close", side_effect=TimeoutError("cleanup")):
            primary = RuntimeError("work failed")
            with self.assertRaises(RuntimeError) as caught:
                with self.group:
                    raise primary
            self.assertIs(caught.exception, primary)
        with self.group:
            pass
        self.assertTrue(self.group.closed)
        self.group.close()

    def test_cleanup_rejects_new_anchor_and_late_state_is_not_accepted(self):
        owner = self.admit()
        with self.assertRaisesRegex(ValueError, "anchor"):
            owner.stop(self.budget.cleanup_deadline + 1)
        self.clock["now"] = self.budget.cleanup_deadline
        with patch.object(backend.ci, "stop_owned") as stop:
            with self.assertRaises(TimeoutError):
                owner.stop(self.budget.cleanup_deadline)
            stop.assert_not_called()
        with self.assertRaises(TimeoutError):
            self.group._persist()

    def test_state_transition_checks_deadline_after_durable_write(self):
        owner = self.admit()
        original = backend._write
        def slow_write(*args, **kwargs):
            original(*args, **kwargs)
            self.clock["now"] = self.budget.cleanup_deadline
        with patch.object(backend, "_write", side_effect=slow_write):
            with self.assertRaises(TimeoutError):
                owner._transition("ready")
        self.assertEqual(owner.state, "admitted")

    def test_admission_crossing_phase_deadline_retains_registered_cleanup_owner(self):
        original = self.group._persist
        def slow_persist():
            original()
            self.clock["now"] = 1000
        with patch.object(self.group, "_persist", side_effect=slow_persist):
            with self.assertRaises(TimeoutError):
                self.admit()
        self.assertEqual(len(self.group.backends), 1)
        self.assertEqual(json.loads(self.group.state_path.read_text())["backends"][0]["state"], "admitted")
        with patch.object(backend.ci, "stop_owned") as stopped:
            self.group.close()
            stopped.assert_called_once()

    def test_replaced_child_manifest_blocks_only_its_cleanup(self):
        first = self.materialize_owner(self.admit())
        second = self.materialize_owner(self.admit("b"))
        second.root.joinpath("owned.json").write_text('{"project":"borrowed"}')
        with patch.object(backend.ci, "stop_owned") as stopped:
            with self.assertRaises(BaseExceptionGroup):
                self.group.close()
        self.assertEqual(stopped.call_count, 1)
        self.assertEqual(stopped.call_args.args[0], first.root)
        self.assertEqual(first.state, "retired")
        self.assertEqual(second.state, "running")

    def test_changed_config_compose_and_source_are_rejected(self):
        owner = self.materialize_owner(self.admit())
        owner.process = SimpleNamespace(pid=123)
        for path in (owner.root / "service.toml", owner.root / "dependencies.json"):
            original = path.read_bytes()
            path.write_bytes(original + b" ")
            with patch.object(owner, "_source"), patch.object(owner, "_probe_dependencies") as probe:
                with self.assertRaisesRegex(AssertionError, "configuration changed"):
                    owner.verify_runtime(1000)
                probe.assert_not_called()
            path.write_bytes(original)
        def git(_source, _deadline, *args):
            return {("rev-parse", "HEAD"): b"1" * 40, ("rev-parse", "HEAD^{tree}"): b"2" * 40,
                    ("status", "--porcelain"): b""}[args]
        with patch.object(backend.common, "git", side_effect=git):
            owner._source(1000)
            self.binary.write_bytes(b"replaced executable")
            with self.assertRaisesRegex(AssertionError, "changed during"):
                owner._source(1000)

    def test_token_writer_preserves_exact_bare_bytes_and_exclusive_ownership(self):
        path = self.base / "token"
        token = "opaque_credential-123"
        backend._write_text(path, token, new=True)
        self.assertEqual(path.read_bytes(), token.encode())
        with self.assertRaises(FileExistsError):
            backend._write_text(path, "replacement", new=True)
        self.assertEqual(path.read_bytes(), token.encode())
        if os.name == "posix":
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_state_writer_rejects_hardlinks_before_truncating_borrowed_file(self):
        original = self.base / "original"
        original.write_bytes(b"do not overwrite")
        alias = self.base / "borrowed-state"
        try:
            os.link(original, alias)
        except OSError:
            self.skipTest("hardlink creation unavailable")
        with self.assertRaisesRegex(AssertionError, "hard link"):
            backend._write(alias, {"owned": True})
        self.assertEqual(original.read_bytes(), b"do not overwrite")

    def test_symlinked_source_child_or_ancestor_is_rejected(self):
        link = self.base / "source-link"
        try:
            link.symlink_to(self.source, target_is_directory=True)
        except OSError:
            self.skipTest("symlink creation unavailable")
        owner = self.admit()
        self.options.mega_source = link
        with self.assertRaisesRegex(AssertionError, "symlink"):
            owner._source(1000)
        self.options.mega_source = self.source
        self.materialize_owner(owner)
        replacement = self.base / "borrowed-root"
        owner.root.rename(replacement)
        owner.root.symlink_to(replacement, target_is_directory=True)
        with patch.object(backend.ci, "stop_owned") as stopped:
            with self.assertRaisesRegex(AssertionError, "symlink"):
                owner.stop(self.budget.cleanup_deadline)
            stopped.assert_not_called()
        owner.root.unlink()
        replacement.rename(owner.root)
        original_backends = self.root / "backends"
        moved = self.base / "borrowed-backends"
        original_backends.rename(moved)
        original_backends.symlink_to(moved, target_is_directory=True)
        with self.assertRaisesRegex(AssertionError, "symlink"):
            owner.stop(self.budget.cleanup_deadline)

    def test_same_path_directory_replacement_is_rejected(self):
        owner = self.materialize_owner(self.admit())
        owner.root.rename(self.base / "old-owner")
        owner.root.mkdir()
        with patch.object(backend.ci, "stop_owned") as stop:
            with self.assertRaisesRegex(AssertionError, "directory was replaced"):
                owner.stop(self.budget.cleanup_deadline)
            stop.assert_not_called()

    def test_seed_mismatch_never_pushes_or_initializes_ready(self):
        owner = self.materialize_owner(self.admit())
        owner.git_url, owner.git_env = "http://127.0.0.1:1234/project", {}
        parent, commit, tree = "1" * 40, "2" * 40, "3" * 40
        for observed_parent, observed_tree, parents in [("4" * 40, tree, [commit, parent]),
                (parent, "4" * 40, [commit, parent]), (parent, tree, [commit, "4" * 40])]:
            with self.subTest(parent=observed_parent, tree=observed_tree, parents=parents):
                def git(_fixture, _deadline, *args, **kwargs):
                    if args[0] == "push":
                        self.fail("a mismatched seed reached push")
                    return (observed_tree if args[0] == "rev-parse" else " ".join(parents)).encode()
                with patch.object(owner, "tip", return_value=observed_parent), patch.object(backend.common, "git", side_effect=git), patch.object(backend.ci, "initialize_owned_native") as initialize:
                    with self.assertRaises(AssertionError):
                        owner.publish_seed(self.base, parent, commit, tree, 1000)
                    initialize.assert_not_called()

    def test_seed_uses_ordinary_push_and_bounded_publication_wait(self):
        owner = self.materialize_owner(self.admit())
        owner.git_url, owner.git_env = "http://127.0.0.1:1234/project", {}
        parent, commit, tree = "1" * 40, "7" * 40, "8" * 40
        binding = self.fake_binding(owner)
        calls = []
        def git(_fixture, _deadline, *args, **kwargs):
            calls.append(args)
            return tree.encode() if args[0] == "rev-parse" else (commit + " " + parent).encode()
        with patch.object(owner, "tip", return_value=parent), patch.object(backend.common, "git", side_effect=git), patch.object(owner, "verify_runtime", side_effect=[backend.NativePublicationPending(), binding]), patch.object(backend.time, "sleep"):
            result = owner.publish_seed(self.base, parent, commit, tree, 1000)
        self.assertEqual(result, binding)
        self.assertEqual(calls[-1], ("push", "--no-thin", owner.git_url, commit + ":refs/heads/main"))
        self.assertEqual(owner.state, "ready")
        owner.state = "running"
        with patch.object(owner, "tip", return_value=parent), patch.object(backend.common, "git", side_effect=git), patch.object(owner, "verify_runtime", side_effect=backend.NativePublicationPending()), patch.object(backend.time, "sleep", side_effect=lambda _: self.clock.update(now=1000)):
            with self.assertRaises(TimeoutError):
                owner.publish_seed(self.base, parent, commit, tree, 1000)
        self.assertEqual(owner.state, "running")

    def test_seed_cannot_return_proof_after_ready_persistence_crosses_phase_deadline(self):
        owner = self.materialize_owner(self.admit())
        owner.git_url, owner.git_env = "http://127.0.0.1:1234/project", {}
        parent, commit, tree = "1" * 40, "7" * 40, "8" * 40
        original = owner._transition
        def delayed_transition(state):
            original(state)
            self.clock["now"] = 1000
        def git(_fixture, _deadline, *args, **kwargs):
            return tree.encode() if args[0] == "rev-parse" else (commit + " " + parent).encode()
        with patch.object(owner, "tip", return_value=parent), patch.object(backend.common, "git", side_effect=git), patch.object(owner, "verify_runtime", return_value=self.fake_binding(owner)), patch.object(owner, "_transition", side_effect=delayed_transition):
            with self.assertRaises(TimeoutError):
                owner.publish_seed(self.base, parent, commit, tree, 1000)
        self.assertLess(self.clock["now"], self.budget.cleanup_deadline)
        with patch.object(backend.ci, "stop_owned") as stopped:
            self.group.close()
            stopped.assert_called_once()

    def test_forged_or_partially_shared_runtime_cannot_prove_isolation(self):
        first, second = self.admit(), self.admit("b")
        a, b = self.fake_binding(first), self.fake_binding(second)
        backend.assert_isolated(a, b)
        for name in ("project", "database", "instance_id", "service_pid", "cache_prefix", "base_dir", "cache_dir", "pack_cache_dir", "projection_sink_instance", "projection_sink_root"):
            with self.subTest(shared=name), self.assertRaises(AssertionError):
                backend.assert_isolated(a, replace(b, **{name: getattr(a, name)}))
        with self.assertRaises(AssertionError):
            backend.assert_isolated(a, replace(b, dependency_container_ids=a.dependency_container_ids))
        with self.assertRaises(AssertionError):
            backend.assert_isolated(a, replace(b, cache_dir=a.cache_dir + "/nested"))
        with self.assertRaises(ValueError):
            backend.assert_isolated(SimpleNamespace(**a.__dict__), b)

    def test_runtime_json_getters_cannot_mutate_original_proof(self):
        binding = self.fake_binding(self.admit())
        identity = binding.identity
        identity["project_commit"] = "9" * 40
        self.assertEqual(binding.identity["project_commit"], "7" * 40)

    def dependency_inventory(self, owner):
        owner.ports = {"postgres": 1001, "redis": 1002, "rustfs": 1003}
        result = []
        for number, service in enumerate(sorted(backend.SERVICES), 1):
            ports = {}
            if service in owner.ports:
                target = {"postgres": "5432/tcp", "redis": "6379/tcp", "rustfs": "9000/tcp"}[service]
                ports[target] = [{"HostIp": "127.0.0.1", "HostPort": str(owner.ports[service])}]
            result.append({"Id": str(number) * 64,
                "Config": {"Labels": {"com.docker.compose.project": owner.project, "com.docker.compose.service": service}},
                "State": {"Running": service != "rustfs-init", "ExitCode": 0}, "NetworkSettings": {"Ports": ports}})
        return result

    def test_dependency_probe_pins_actual_ids_owners_endpoints_and_integer_exit(self):
        owner = self.admit()
        inventory = self.dependency_inventory(owner)
        def command(argv, _deadline):
            if argv[1] == "ps":
                return "\n".join(x["Id"] for x in inventory).encode()
            return json.dumps(inventory).encode()
        with patch.object(backend.common, "command", side_effect=command):
            ids = owner._probe_dependencies(1000)
            self.assertEqual(ids, tuple(sorted(x["Id"] for x in inventory)))
            original = deepcopy(inventory)
            mutations = [lambda: inventory[0]["Config"]["Labels"].update({"com.docker.compose.project": "borrowed"}),
                         lambda: inventory[0]["NetworkSettings"]["Ports"]["5432/tcp"][0].update(HostIp="0.0.0.0"),
                         lambda: inventory[-1]["State"].update(ExitCode=False),
                         lambda: inventory[0]["State"].update(Running=1),
                         lambda: inventory[0].update(Id="9" * 64),
                         lambda: inventory[0].update(Config=None)]
            for mutate in mutations:
                inventory[:] = deepcopy(original)
                mutate()
                with self.subTest(mutate=mutate), self.assertRaises(AssertionError):
                    owner._probe_dependencies(1000)

    def runtime_fixture(self):
        owner = self.materialize_owner(self.admit())
        owner.process = SimpleNamespace(pid=123, returncode=None)
        owner.started = "42"
        owner.source, owner.binary = self.source, self.binary
        owner.source_tree, owner.lock_digest, owner.binary_digest = "2" * 40, "3" * 64, "4" * 64
        owner.base_url, owner.git_url = "http://127.0.0.1:1234", "http://127.0.0.1:1234/project"
        owner.service_env = {"MEGA_BASE_DIR": str(owner.root / "service-data"), "MEGA_CACHE_DIR": str(owner.root / "cache"),
                             "MEGA_GIT_OBJECT_CACHE_PREFIX": owner.project}
        owner.pg_env = {"PGDATABASE": owner.database}
        sink = owner.root / "cache/logs/mst2-native-projection" / str(uuid.uuid4())
        sink.mkdir(parents=True, mode=0o700)
        sink.parent.chmod(0o700)
        commit, tree, root_commit = "7" * 40, "8" * 40, "9" * 40
        raw = b"40000 project\0" + bytes.fromhex(tree)
        root_tree = hashlib.sha1(b"tree " + str(len(raw)).encode() + b"\0" + raw).hexdigest()
        rows = [{"path": "/", "commit": root_commit, "tree": root_tree, "database": owner.database,
                 "commit_tree": root_tree, "raw_tree": raw.hex()},
                {"path": "/project", "commit": commit, "tree": tree, "database": owner.database, "commit_tree": tree, "raw_tree": None}]
        native = {"instance_id": owner.instance_id, "sequence": 1, "writer_epoch": 1, "root_commit": root_commit,
                  "root_tree": root_tree, "state": "READY", "certificate_receipt_id": 1, "certificate_commit": root_commit,
                  "certificate_tree": root_tree, "certificate_sequence": 1, "certificate_instance": owner.instance_id,
                  "path_commit": commit, "path_tree": tree, "origin_path": "/project", "origin_ref": "refs/heads/main",
                  "origin_sequence": 1, "certificate_id": 1, "certificate_namespace": "/", "certificate_epoch": 1,
                  "old_root_commit": "a" * 40, "old_path_commit": "b" * 40, "receipt_id": 1, "receipt_namespace": "/project",
                  "old_oid": "a" * 40, "new_oid": commit, "receipt_epoch": 1, "writer_kind": "trunk_push",
                  "request_digest": "sha256:" + "c" * 64, "request_digest_version": 1, "native_certificate_version": 1,
                  "outbox_id": 1, "outbox_namespace": "/project", "outbox_sequence": 1}
        self.patch("workspace_update_backend.OwnedBackend._source")
        self.patch("workspace_update_backend.OwnedBackend._probe_dependencies", return_value=("1" * 64, "2" * 64, "3" * 64, "4" * 64))
        self.patch("workspace_update_backend.OwnedBackend.tip", return_value=commit)
        self.patch("workspace_update_backend.ci.owned_service_exit", return_value=None)
        self.patch("workspace_update_backend.budget_module.process_start", return_value="42")
        self.patch("workspace_update_backend.os.getpgid", return_value=123, create=True)
        self.patch("workspace_update_backend.os.getsid", return_value=123, create=True)
        environment = self.patch("workspace_update_backend._process_environment", return_value=dict(owner.service_env))
        service = self.patch("workspace_update_backend.common.service_binding", return_value={"starttime_ticks": "42", "exe": str(self.binary),
            "config_sha256": owner.config_digest, "projection_cache": str(owner.root / "cache")})
        query = self.patch("workspace_update_backend.common.query", side_effect=lambda sql, _deadline, **kwargs: rows if sql == backend.common.IDENTITY_SQL else native)
        return owner, rows, native, sink, environment, service, query

    def test_runtime_probe_binds_real_sink_and_private_process_overrides(self):
        owner, rows, native, sink, environment, service, query = self.runtime_fixture()
        binding = owner.verify_runtime(1000)
        self.assertEqual(binding.projection_sink_instance, sink.name)
        self.assertNotEqual(binding.projection_sink_instance, owner.instance_id)
        self.assertEqual((binding.projection_sink_device, binding.projection_sink_inode), (sink.stat().st_dev, sink.stat().st_ino))
        service.assert_called_once()
        self.assertEqual(service.call_args.args[1], owner.pg_env)
        self.assertEqual(binding.identity["project_commit"], "7" * 40)
        self.assertEqual(binding.native["instance_id"], owner.instance_id)
        for key in ("MEGA_BASE_DIR", "MEGA_CACHE_DIR", "MEGA_GIT_OBJECT_CACHE_PREFIX", "MEGA_REDIS__URL"):
            environment.return_value = dict(owner.service_env, **{key: "borrowed"})
            with self.subTest(override=key), self.assertRaisesRegex(AssertionError, "overrides changed"):
                owner.verify_runtime(1000)
        environment.return_value = dict(owner.service_env)
        sink.rename(sink.with_name("saved-sink"))
        sink.mkdir(mode=0o700)
        with self.assertRaises(AssertionError):
            owner.verify_runtime(1000)

    def test_runtime_native_boolean_counters_and_malformed_rows_fail_closed(self):
        owner, rows, native, sink, environment, service, query = self.runtime_fixture()
        for key in ("sequence", "writer_epoch", "certificate_sequence", "receipt_epoch", "outbox_id"):
            original = native[key]
            native[key] = True
            with self.subTest(counter=key), self.assertRaisesRegex(AssertionError, "counters are malformed"):
                owner.verify_runtime(1000)
            native[key] = original
        for invalid in (None, [None], [{"path": "/project"}], [{"path": "/project", "tree": "invalid"}]):
            query.side_effect = lambda sql, _deadline, **kwargs: invalid if sql == backend.common.IDENTITY_SQL else native
            with self.subTest(rows=invalid), self.assertRaises(AssertionError):
                owner.verify_runtime(1000)

    def test_runtime_rejects_exited_or_replaced_process_before_identity_queries(self):
        owner, rows, native, sink, environment, service, query = self.runtime_fixture()
        owner.process.returncode = 0
        with self.assertRaisesRegex(AssertionError, "live pinned process"):
            owner.verify_runtime(1000)
        query.assert_not_called()
        owner.process.returncode = None
        with patch.object(backend.budget_module, "process_start", return_value="replaced"):
            with self.assertRaisesRegex(AssertionError, "live pinned process"):
                owner.verify_runtime(1000)
        query.assert_not_called()


    def startup_mocks(self, failing_boundary=None, capabilities=None):
        deployed = {"value": False}
        self.patch("workspace_update_backend.ci.free_port", side_effect=range(10001, 10101))
        self.patch("workspace_update_backend.ci.dependencies", return_value={"services": {}})
        self.patch("workspace_update_backend.ci.initialize_owned_native", side_effect=RuntimeError("native init") if failing_boundary == "native_init" else None)
        self.patch("workspace_update_backend.ci.owned_service_exit", return_value=None)
        self.patch("workspace_update_backend.budget_module.process_start", side_effect=RuntimeError("process identity") if failing_boundary == "process_identity" else None, return_value="42")
        process = SimpleNamespace(pid=123, returncode=None)
        self.patch("workspace_update_backend.budget_module.PinnedProcess", side_effect=RuntimeError("launch") if failing_boundary == "process_launch" else None, return_value=process)
        abort = self.patch("workspace_update_backend.budget_module.abort_startup")
        stop_group = self.patch("workspace_update_backend.budget_module.stop_group")
        if capabilities is None:
            capabilities = {"protocol_versions": [2], "features": dict.fromkeys(
                ("strict_publication", "directory", "lookup", "metadata_pages", "raw_blob",
                 "small_objects", "chunk_reads", "full_hydration"), True)}
        body = capabilities if isinstance(capabilities, bytes) else json.dumps(capabilities).encode()
        self.patch("workspace_update_backend.budget_module.run_process", return_value=(0, body, b""))
        def git(_source, _deadline, *args):
            return {("rev-parse", "HEAD"): b"1" * 40, ("rev-parse", "HEAD^{tree}"): b"2" * 40,
                    ("status", "--porcelain"): b""}[args]
        self.patch("workspace_update_backend.common.git", side_effect=git)
        def command(argv, _deadline, **kwargs):
            if argv[0] == "docker":
                if "up" in argv:
                    # Ownership must exist before Docker may partially create resources.
                    owners = [p for p in self.root.glob("backends/*/owned.json")]
                    self.assertTrue(owners)
                    deployed["value"] = True
                    if failing_boundary == "compose_up":
                        raise RuntimeError("compose up")
                return b""
            if argv[0] == "psql" and failing_boundary == "database":
                raise RuntimeError("database")
            if argv[-2:] == ["config", "validate"] and failing_boundary == "config_validate":
                raise RuntimeError("config")
            if argv[-3:] == ["service", "init", "--yes"] and failing_boundary == "service_init":
                raise RuntimeError("service init")
            return b""
        self.patch("workspace_update_backend.common.command", side_effect=command)
        self.patch("workspace_update_backend.OwnedBackend._probe_dependencies", side_effect=RuntimeError("dependency probe") if failing_boundary == "dependency_probe" else None, return_value=tuple(str(i) * 64 for i in range(4)))
        self.patch("workspace_update_backend.OwnedBackend.tip", return_value="7" * 40)
        self.patch("workspace_update_backend.OwnedBackend.verify_runtime", side_effect=RuntimeError("runtime proof") if failing_boundary == "runtime_proof" else None)
        stopped = self.patch("workspace_update_backend.ci.stop_owned")
        return stopped, abort, stop_group

    def test_every_partial_startup_boundary_remains_registered_and_retires(self):
        for boundary in ("compose_up", "dependency_probe", "database", "config_validate", "service_init", "native_init", "process_launch", "process_identity", "runtime_proof"):
            with self.subTest(boundary=boundary):
                # Each case uses a fresh campaign and unmodified original anchor.
                if self.group.closed:
                    self.root = self.base / ("campaign-" + boundary)
                    self.options.run_root = self.root
                    self.hosted.return_value = (self.root, "m2perf-77-1")
                    self.group = backend.BackendGroup(self.options, self.budget)
                with ExitStack() as stack:
                    # Keep each boundary's helper patches local to the iteration.
                    def local_patch(target, **kwargs):
                        return stack.enter_context(patch(target, **kwargs))
                    with patch.object(self, "patch", side_effect=local_patch):
                        stopped, abort, stop_group = self.startup_mocks(boundary)
                    owner = self.admit()
                    with redirect_stdout(io.StringIO()), self.assertRaises(RuntimeError):
                        owner.start(1000)
                    self.assertEqual(len(self.group.backends), 1)
                    self.assertTrue(owner.root.joinpath("owned.json").is_file())
                    self.group.close()
                    stopped.assert_called_once_with(owner.root, owner.project, self.budget.cleanup_deadline, owner.process)
                    self.assertEqual(owner.state, "retired")
                    if boundary == "process_identity":
                        abort.assert_called_once_with(owner.process, self.budget.cleanup_deadline)

    def test_startup_tokens_are_bare_and_service_uses_owned_unique_overrides(self):
        self.startup_mocks()
        owner = self.admit()
        with redirect_stdout(io.StringIO()):
            owner.start(1000)
        for file, name in (("git-token", "M2_GIT_TOKEN"), ("mst2-token", "M2_TOKEN")):
            self.assertEqual(owner.root.joinpath(file).read_bytes(), owner.pg_env[name].encode())
        overrides = {k: v for k, v in owner.service_env.items() if k.startswith("MEGA_")}
        self.assertEqual(overrides, {"MEGA_BASE_DIR": str(owner.root / "service-data"),
                                    "MEGA_CACHE_DIR": str(owner.root / "cache"),
                                    "MEGA_GIT_OBJECT_CACHE_PREFIX": owner.project})
        self.assertEqual(json.loads(owner.root.joinpath("owned.json").read_text())["service"],
                         {"pid": 123, "pgid": 123, "sid": 123, "starttime": "42"})
        config = backend.common.tomllib.loads(owner.root.joinpath("service.toml").read_text())
        self.assertEqual(config["object_storage"], {
            "storage_type": "local",
            "local": {"root_dir": str(owner.root / "service-data" / "objects")},
        })
        self.group.close()

    def test_missing_or_invalid_delivery_capabilities_fail_before_measured_work_and_retain_cleanup(self):
        complete = {"protocol_versions": [2], "features": dict.fromkeys(
            ("strict_publication", "directory", "lookup", "metadata_pages", "raw_blob",
             "small_objects", "chunk_reads", "full_hydration"), True)}
        invalid = [b"invalid-json", b"x" * 65537, [], {"protocol_versions": [2]},
                   {"protocol_versions": [3], "features": {}},
                   {"protocol_versions": [2], "features": {"raw_blob": False}},
                   {**complete, "protocol_versions": [2.0]},
                   {**complete, "features": {**complete["features"], "chunk_reads": 1}},
                   {**complete, "features": {**complete["features"], "full_hydration": False}}]
        for index, capabilities in enumerate(invalid):
            with self.subTest(capabilities=index), ExitStack() as stack:
                self.root = self.base / ("invalid-capabilities-" + str(index))
                self.options.run_root = self.root
                self.hosted.return_value = (self.root, "m2perf-77-1")
                self.group = backend.BackendGroup(self.options, self.budget)
                with patch.object(self, "patch", side_effect=lambda target, **kwargs:
                                  stack.enter_context(patch(target, **kwargs))):
                    stopped, _, _ = self.startup_mocks(capabilities=capabilities)
                owner = self.admit()
                with redirect_stdout(io.StringIO()), self.assertRaisesRegex(AssertionError, "capabilities"):
                    owner.start(1000)
                backend.OwnedBackend.tip.assert_not_called()
                backend.OwnedBackend.verify_runtime.assert_not_called()
                self.assertTrue(owner.root.joinpath("owned.json").is_file())
                self.group.close()
                stopped.assert_called_once_with(owner.root, owner.project, self.budget.cleanup_deadline, owner.process)
                self.assertEqual(owner.state, "retired")

    def test_capabilities_retry_uses_owned_child_and_original_readiness_deadline(self):
        self.startup_mocks()
        complete = backend.budget_module.run_process.return_value
        deadlines = []
        def probe(argv, deadline, **kwargs):
            self.assertEqual(argv, [backend.sys.executable, "-I", "-c",
                                    backend.CAPABILITY_PROBE, "10004"])
            self.assertEqual(kwargs, {"env": backend.common.clean_env()})
            deadlines.append(deadline)
            if len(deadlines) == 1:
                self.clock["now"] = 278.0
                return (3, b"", b"")
            return complete
        backend.budget_module.run_process.side_effect = probe
        owner = self.admit()
        with redirect_stdout(io.StringIO()), patch.object(backend.time, "sleep"):
            owner.start(1000)
        self.assertEqual(deadlines, [105.0, 280.0])
        self.group.close()

    def test_capabilities_child_timeout_preserves_registered_cleanup_and_stops_before_seed(self):
        stopped, _, _ = self.startup_mocks()
        backend.budget_module.run_process.side_effect = TimeoutError("probe absolute deadline")
        owner = self.admit()
        with redirect_stdout(io.StringIO()), self.assertRaisesRegex(TimeoutError, "absolute deadline"):
            owner.start(1000)
        self.assertEqual(backend.budget_module.run_process.call_args.args[1], 105.0)
        backend.OwnedBackend.tip.assert_not_called()
        backend.OwnedBackend.verify_runtime.assert_not_called()
        self.assertTrue(owner.root.joinpath("owned.json").is_file())
        self.group.close()
        stopped.assert_called_once_with(owner.root, owner.project, self.budget.cleanup_deadline, owner.process)
        self.assertEqual(owner.state, "retired")

    def test_capabilities_unexpected_child_failure_stops_before_seed(self):
        self.startup_mocks()
        backend.budget_module.run_process.return_value = (1, b"", b"")
        owner = self.admit()
        with redirect_stdout(io.StringIO()), self.assertRaisesRegex(AssertionError, "probe failed"):
            owner.start(1000)
        backend.OwnedBackend.tip.assert_not_called()
        backend.OwnedBackend.verify_runtime.assert_not_called()
        self.group.close()

    def test_failed_pid_persistence_and_abort_preserve_direct_child_retry_authority(self):
        stopped, abort, stop_group = self.startup_mocks()
        stop_group.side_effect = [TimeoutError("first abort failed"), None]
        owner = self.admit()
        original_write = backend._write
        primary = RuntimeError("PID persistence failed")
        def fail_pid_write(path, value, **kwargs):
            if path.name == "owned.json" and "service" in value:
                raise primary
            return original_write(path, value, **kwargs)
        with patch.object(backend, "_write", side_effect=fail_pid_write), redirect_stdout(io.StringIO()):
            with self.assertRaises(RuntimeError) as caught:
                owner.start(1000)
        self.assertIs(caught.exception, primary)
        self.assertTrue(owner.startup_abort_pending)
        self.assertNotIn("service", json.loads(owner.root.joinpath("owned.json").read_text()))
        self.group.close()
        self.assertFalse(owner.startup_abort_pending)
        self.assertEqual(stop_group.call_count, 2)
        for call in stop_group.call_args_list:
            self.assertEqual(call.args, (123, "42", self.budget.cleanup_deadline, owner.process))
        stopped.assert_called_once()
        self.assertIsNone(owner.log)


if __name__ == "__main__":
    unittest.main()
