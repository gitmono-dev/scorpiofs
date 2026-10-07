"""Actual Git/source/binary drift rejection; release compilation is simulated."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import workspace_update_build as builds


class ClientBuildTests(unittest.TestCase):
    def source(self, root, name):
        source = root / name
        source.mkdir()
        self.git(source, "init", "-q")
        (source / ".gitignore").write_text("/target\n")
        (source / "Cargo.toml").write_text('[package]\nname="scorpiofs"\nversion="0.1.0"\n')
        (source / "Cargo.lock").write_text("version = 4\n# " + name + "\n")
        self.git(source, "add", ".")
        self.git(source, "-c", "user.name=benchmark", "-c", "user.email=bench@example.invalid",
                 "commit", "-qm", name)
        return source, self.git(source, "rev-parse", "HEAD").decode().strip()

    @staticmethod
    def git(source, *args):
        return subprocess.check_output(["git", "-C", str(source), *args], env=builds.common.clean_env())

    def receipt(self, root, label):
        source, sha = self.source(root, "source-" + label)
        original_output, original_run = builds.output, subprocess.run
        deadline = time.monotonic() + 30

        def output(argv, *args):
            if argv[0] in ("cargo", "rustc"):
                return (argv[0] + " 1.90.0\n").encode()
            return original_output(argv, *args)

        def run(argv, **kwargs):
            if argv[0] != "cargo":
                return original_run(argv, **kwargs)
            self.assertIn("--locked", argv)
            self.assertIn("--release", argv)
            self.assertEqual(kwargs["env"]["CARGO_BUILD_JOBS"], "2")
            self.assertGreater(kwargs["timeout"], 0)
            binary = source / "target/release" / ("mega2" if label == "server" else "scorpio")
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"simulated built artifact " + label.encode())
            return subprocess.CompletedProcess(argv, 0)

        path = root / (label + "-build.json")
        with patch.object(builds, "output", side_effect=output), patch.object(builds.subprocess, "run", side_effect=run):
            record = builds.build(source, sha, label, path, deadline)
        return path, record, deadline

    def test_two_real_checkouts_and_distinct_binaries_bind_the_same_toolchain(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            a, ra, deadline = self.receipt(root, "a")
            b, rb, _ = self.receipt(root, "b")
            lanes = builds.clients(SimpleNamespace(paired=True, rounds=3, build_a=a, build_b=b), deadline)
            self.assertNotEqual(lanes[0].driver, lanes[1].driver)
            self.assertNotEqual(ra["source_sha"], rb["source_sha"])
            self.assertNotEqual(ra["binary_sha256"], rb["binary_sha256"])
            self.assertEqual(ra["build_env"], rb["build_env"])
            builds.validate(lanes[0], deadline)
            builds.validate(lanes[1], deadline)

    def test_server_receipt_binds_actual_mega2_artifact_and_same_locked_release_flags(self):
        with tempfile.TemporaryDirectory() as temp:
            path, record, deadline = self.receipt(Path(temp), "server")
            lane = builds.load(path, "server", deadline)
            self.assertEqual(lane.driver.name, "mega2")
            self.assertEqual(record["build_argv"][-2:], ["--bin", "mega2"])
            self.assertEqual(record["build_env"], builds.BUILD_ENV)
            record["build_argv"][-1] = "scorpio"
            path.write_text(json.dumps(record))
            with self.assertRaises(AssertionError):
                builds.load(path, "server", deadline)

    def test_replaced_binary_lock_or_source_head_is_rejected(self):
        for change in ("binary", "lock", "head"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temp:
                path, record, deadline = self.receipt(Path(temp), "a")
                if change == "binary":
                    Path(record["binary"]).write_bytes(b"another executable")
                elif change == "lock":
                    (Path(record["source"]) / "Cargo.lock").write_text("version = 3\n")
                else:
                    source = Path(record["source"])
                    (source / "tracked").write_text("next version\n")
                    self.git(source, "add", "tracked")
                    self.git(source, "-c", "user.name=benchmark", "-c", "user.email=bench@example.invalid",
                             "commit", "-qm", "next")
                with self.assertRaises(AssertionError):
                    builds.load(path, "a", deadline)

    def test_explicit_paired_driver_cannot_silently_override_either_build_receipt(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            a, ra, deadline = self.receipt(root, "a")
            b, rb, _ = self.receipt(root, "b")
            options = SimpleNamespace(paired=True, rounds=3, build_a=a, build_b=b,
                                      driver=Path(ra["binary"]), driver_sha256=ra["binary_sha256"])
            builds.clients(options, deadline)
            for field, value in (("driver", Path(rb["binary"])), ("driver_sha256", "0" * 64)):
                with self.subTest(field=field):
                    changed = SimpleNamespace(**vars(options))
                    setattr(changed, field, value)
                    with self.assertRaisesRegex(ValueError, "explicit paired driver"):
                        builds.clients(changed, deadline)

    def test_swapped_label_foreign_binary_or_unlocked_build_flags_are_rejected(self):
        for change in ("label", "foreign_binary", "flags", "toolchain"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                path, record, deadline = self.receipt(root, "a")
                if change == "label":
                    record["label"] = "b"
                elif change == "foreign_binary":
                    other = root / "foreign"
                    other.write_bytes(Path(record["binary"]).read_bytes())
                    record["binary"] = str(other)
                elif change == "flags":
                    record["build_argv"].remove("--locked")
                else:
                    record["cargo_version"] = "unbound build"
                path.write_text(json.dumps(record))
                with self.assertRaises((ValueError, AssertionError)):
                    builds.load(path, "a", deadline)

    def test_rewriting_an_admitted_receipt_is_detected_before_reuse(self):
        with tempfile.TemporaryDirectory() as temp:
            path, record, deadline = self.receipt(Path(temp), "a")
            lane = builds.load(path, "a", deadline)
            record["rustc_version"] = "rustc 1.91.0"
            path.write_text(json.dumps(record))
            with self.assertRaisesRegex(AssertionError, "after admission"):
                builds.validate(lane, deadline)

    def test_bindings_cannot_use_a_fresh_deadline_after_the_shared_stage_expires(self):
        with tempfile.TemporaryDirectory() as temp:
            path, _, _ = self.receipt(Path(temp), "a")
            with self.assertRaises(TimeoutError):
                builds.load(path, "a", time.monotonic() - 1)

    @unittest.skipUnless(os.name == "posix", "actual POSIX release symlink substitution")
    def test_release_symlink_is_rejected_even_when_its_bytes_match(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path, record, deadline = self.receipt(root, "a")
            binary = Path(record["binary"])
            outside = root / "outside"
            outside.write_bytes(binary.read_bytes())
            binary.unlink()
            binary.symlink_to(outside)
            with self.assertRaises(ValueError):
                builds.load(path, "a", deadline)


if __name__ == "__main__":
    unittest.main()
