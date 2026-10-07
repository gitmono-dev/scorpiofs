import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from workspace_update_resources import (IO_FIELDS, ProcessResources,
                                        ResourceMeasurementError, disk_usage, io_delta)


class ProcessResourceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.proc = self.root / "123"
        self.proc.mkdir()
        self.write_metrics()
        self.sampler = ProcessResources(123, "42", self.proc.stat().st_uid, self.root, 1)

    def write_metrics(self, rss=10, hwm=100, counter=5, started="42"):
        self.proc.joinpath("stat").write_text("123 (a name ) inside) " +
                                           " ".join(["S"] + ["0"] * 18 + [started]))
        self.proc.joinpath("status").write_text(f"Name:\tfixture\nVmRSS:\t{rss} kB\nVmHWM:\t{hwm} kB\n")
        self.proc.joinpath("io").write_text("".join(f"{field}: {counter}\n" for field in IO_FIELDS))

    def test_sampled_peak_reset_and_lifetime_hwm_are_distinct(self):
        self.sampler.start()
        self.addCleanup(self.sampler.close)
        before = self.sampler.snapshot()
        self.write_metrics(rss=50, counter=12)
        with self.sampler._lock:
            self.sampler._record()
        self.write_metrics(rss=8, counter=15)
        after = self.sampler.snapshot()
        self.assertEqual(after["sampled_rss_peak_bytes"], 50 * 1024)
        self.assertEqual(after["rss_bytes"], 8 * 1024)
        self.assertEqual(after["vm_hwm_bytes"], 100 * 1024)
        self.assertEqual(io_delta(before, after), dict.fromkeys(IO_FIELDS, 10))
        self.assertEqual(self.sampler.snapshot()["sampled_rss_peak_bytes"], 8 * 1024)
        self.assertGreaterEqual(after["sample_count"], 3)

    def test_pid_reuse_is_rejected_and_sampler_is_stopped(self):
        self.sampler.start()
        self.write_metrics(started="43")
        with self.assertRaisesRegex(ResourceMeasurementError, "owned_process_changed"):
            self.sampler.close()
        self.assertFalse(self.sampler._thread.is_alive())

    def test_identity_change_during_read_is_rejected(self):
        real = self.sampler._identity
        calls = 0

        def changed():
            nonlocal calls
            calls += 1
            if calls == 2:
                self.write_metrics(started="44")
            return real()

        with patch.object(self.sampler, "_identity", side_effect=changed):
            with self.assertRaisesRegex(ResourceMeasurementError, "owned_process_changed"):
                self.sampler.start()
        self.assertIsNone(self.sampler._thread)

    def test_missing_duplicate_negative_or_wrong_unit_metrics_fail_closed(self):
        for text in ("VmRSS: 10 kB\n", "VmRSS: 10 kB\nVmRSS: 10 kB\nVmHWM: 100 kB\n",
                     "VmRSS: -1 kB\nVmHWM: 100 kB\n", "VmRSS: 10 MB\nVmHWM: 100 kB\n"):
            with self.subTest(text=text):
                self.proc.joinpath("status").write_text(text)
                with self.assertRaises(ResourceMeasurementError):
                    self.sampler._read()

    def test_thread_error_is_propagated_without_continuing_zero_samples(self):
        self.sampler.interval = 0.001
        self.sampler.start()
        self.proc.joinpath("io").unlink()
        self.assertTrue(self.sampler._stop.wait(1))
        with self.assertRaisesRegex(ResourceMeasurementError, "process_metrics_unavailable"):
            self.sampler.close()
        self.assertFalse(self.sampler._thread.is_alive())

    def test_io_regression_and_different_owner_rejected(self):
        self.sampler.start()
        self.addCleanup(self.sampler.close)
        before = self.sampler.snapshot()
        after = dict(before, pid=124)
        with self.assertRaisesRegex(ResourceMeasurementError, "owner_changed"):
            io_delta(before, after)
        after = dict(before, io=dict.fromkeys(IO_FIELDS, 4))
        with self.assertRaisesRegex(ResourceMeasurementError, "counter_regressed"):
            io_delta(before, after)

    def test_close_rejects_final_read_that_crosses_original_deadline(self):
        self.sampler.start()
        clock = {"now": 0}
        original = self.sampler._read

        def delayed_read():
            result = original()
            clock["now"] = 2
            return result

        with patch("workspace_update_resources.time.monotonic", side_effect=lambda: clock["now"]):
            with patch.object(self.sampler, "_read", side_effect=delayed_read):
                with self.assertRaises(TimeoutError):
                    self.sampler.close(deadline=1)
        self.assertFalse(self.sampler._thread.is_alive())

    def test_disk_mount_inventory_cannot_extend_original_deadline(self):
        clock = {"now": 0}

        def delayed_inventory(_root):
            clock["now"] = 2
            return set()

        with patch("workspace_update_resources.time.monotonic", side_effect=lambda: clock["now"]):
            with patch("workspace_update_resources._mountpoints", side_effect=delayed_inventory):
                with self.assertRaises(TimeoutError):
                    disk_usage(self.root, deadline=1, proc_root=self.root)

    @unittest.skipUnless(Path("/proc/self/stat").is_file(), "requires native Linux proc metrics")
    def test_actual_linux_process_binding_and_metric_units(self):
        started = Path("/proc/self/stat").read_text().rsplit(") ", 1)[1].split()[19]
        sampler = ProcessResources(os.getpid(), started, os.getuid()).start()
        try:
            before = sampler.snapshot()
            self.proc.joinpath("body").write_bytes(b"x" * 65536)
            after = sampler.snapshot()
            self.assertGreater(after["rss_bytes"], 0)
            self.assertGreaterEqual(after["vm_hwm_bytes"], after["rss_bytes"])
            self.assertGreaterEqual(io_delta(before, after)["wchar"], 65536)
        finally:
            sampler.close()
        self.assertFalse(sampler._thread.is_alive())


@unittest.skipUnless(os.name == "posix", "allocated blocks require a Unix filesystem")
class DiskResourceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.root = self.base / "stock"
        self.root.mkdir()
        self.proc = self.base / "proc"
        self.proc.joinpath("self").mkdir(parents=True)
        self.proc.joinpath("self/mountinfo").write_text("")

    def test_sparse_files_hardlinks_and_symlink_targets(self):
        sparse = self.root / "sparse"
        with sparse.open("wb") as stream:
            stream.truncate(1024 * 1024)
        os.link(sparse, self.root / "alias")
        outside = self.base / "outside"
        outside.mkdir()
        outside.joinpath("ignored").write_bytes(b"outside")
        self.root.joinpath("link").symlink_to(outside, target_is_directory=True)
        with patch.object(Path, "read_bytes", side_effect=AssertionError("body must not be read")):
            result = disk_usage(self.root, proc_root=self.proc)
        self.assertEqual(result["files"], 1)
        self.assertEqual(result["duplicate_inodes"], 1)
        self.assertEqual(result["symlinks"], 1)
        self.assertEqual(result["regular_apparent_bytes"], 1024 * 1024)
        self.assertEqual(result["regular_allocated_bytes"], sparse.stat().st_blocks * 512)
        self.assertNotIn("path", result)

    def test_nested_mount_is_excluded_before_lstat_including_escaped_space(self):
        mount = self.root / "mounted tree"
        mount.mkdir()
        mount.joinpath("do-not-count").write_bytes(b"x")
        encoded = str(mount).replace(" ", r"\040")
        self.proc.joinpath("self/mountinfo").write_text(f"10 1 0:1 / {encoded} rw - fuse fuse rw\n")
        original = Path.lstat

        def guarded(path, *args, **kwargs):
            if path == mount:
                raise AssertionError("live mount was touched")
            return original(path, *args, **kwargs)

        with patch.object(Path, "lstat", guarded):
            result = disk_usage(self.root, proc_root=self.proc)
        self.assertEqual(result["files"], 0)
        self.assertEqual(result["excluded_subtrees"], 1)

    def test_symlink_root_and_elapsed_deadline_rejected(self):
        alias = self.base / "root-alias"
        alias.symlink_to(self.root, target_is_directory=True)
        with self.assertRaisesRegex(ResourceMeasurementError, "invalid_disk_root"):
            disk_usage(alias, proc_root=self.proc)
        with self.assertRaises(TimeoutError):
            disk_usage(self.root, deadline=time.monotonic() - 1, proc_root=self.proc)

    def test_shared_inode_set_deduplicates_separate_git_roots(self):
        object_root, worktree = self.root / "odb", self.root / "checkout"
        object_root.mkdir()
        worktree.mkdir()
        object_root.joinpath("object").write_bytes(b"same allocation")
        os.link(object_root / "object", worktree / "file")
        seen = set()
        first = disk_usage(object_root, proc_root=self.proc, seen=seen)
        second = disk_usage(worktree, proc_root=self.proc, seen=seen)
        self.assertEqual(first["regular_apparent_bytes"], 15)
        self.assertEqual(second["regular_apparent_bytes"], 0)
        self.assertEqual(second["duplicate_inodes"], 1)

    def test_final_root_stat_cannot_extend_original_deadline(self):
        clock, calls = {"now": 0}, {"root": 0}
        original = Path.lstat

        def delayed(path, *args, **kwargs):
            result = original(path, *args, **kwargs)
            if path == self.root:
                calls["root"] += 1
                if calls["root"] == 3:
                    clock["now"] = 2
            return result

        with patch("workspace_update_resources.time.monotonic", side_effect=lambda: clock["now"]):
            with patch.object(Path, "lstat", delayed):
                with self.assertRaises(TimeoutError):
                    disk_usage(self.root, deadline=1, proc_root=self.proc)
        self.assertEqual(calls["root"], 3)


if __name__ == "__main__":
    unittest.main()
