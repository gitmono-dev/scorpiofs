"""A metadata-only probe must actually enter and enumerate the fixed namespace."""

from copy import deepcopy
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

import workspace_update_directory as directory


class DirectoryProbeTests(unittest.TestCase):
    def fixture(self, root, git=False):
        (root / "nested").mkdir()
        (root / "nested/file").write_bytes(b"must not read body")
        if git:
            (root / ".git").write_text("gitdir: separate test-only store")
        else:
            (root / "empty").mkdir()
            (root / "nested/empty").mkdir()
        return {"directories": ["", "empty", "nested", "nested/empty"],
                "files": [{"rel_path": "nested/file"}]}

    def test_real_root_and_nested_open_readdir_match_both_namespace_semantics_without_body_reads(self):
        for git in (False, True):
            with self.subTest(git=git), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                expected = self.fixture(root, git)
                before = Path.cwd()
                with patch.object(Path, "read_bytes", side_effect=AssertionError("must not read file bodies")):
                    report = directory.verify(root, directory.plan(expected, git_checkout=git), time.monotonic() + 10)
                directory.validate_record(report, expected, git_checkout=git)
                self.assertEqual(Path.cwd(), before)
                self.assertEqual(report["opened_directories"], 2)
                self.assertEqual(report["root"]["entries"], 1 if git else 2)
                self.assertEqual(report["nested"]["entries"], 1 if git else 2)
                self.assertNotIn("nested", json.dumps({key: value for key, value in report["nested"].items()}))
                self.assertNotIn("must not read body", json.dumps(report))

    def test_missing_or_extra_root_or_nested_names_fail_before_claiming_ready(self):
        for mutation in ("extra-root", "extra-nested", "missing-file", "missing-empty"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                expected = self.fixture(root)
                if mutation == "extra-root":
                    (root / "extra").mkdir()
                elif mutation == "extra-nested":
                    (root / "nested/extra").mkdir()
                elif mutation == "missing-file":
                    (root / "nested/file").unlink()
                else:
                    (root / "empty").rmdir()
                with self.assertRaises(ValueError):
                    directory.verify(root, directory.plan(expected), time.monotonic() + 10)

    def test_tiny_root_only_fixture_does_not_fabricate_nested_probe(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "file").write_bytes(b"body")
            expected = {"directories": [""], "files": [{"rel_path": "file"}]}
            report = directory.verify(root, directory.plan(expected), time.monotonic() + 10)
            self.assertEqual(report["opened_directories"], 1)
            self.assertIsNone(report["nested"])
            self.assertEqual(report["timings_ms"]["nested_open"], 0)
            directory.validate_record(report, expected)

    def test_schema_counts_hashes_booleans_timings_and_expected_binding_are_strict(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            expected = self.fixture(root)
            report = directory.verify(root, directory.plan(expected), time.monotonic() + 10)
            for change in (lambda r: r.update(path="private"), lambda r: r.update(git_checkout=1),
                           lambda r: r["root"].update(entries=True),
                           lambda r: r["nested"].update(names_sha256="0" * 64),
                           lambda r: r["timings_ms"].update(total=-1),
                           lambda r: r["timings_ms"].update(root_open=float("nan")),
                           lambda r: r["timings_ms"].update(root_open=1e8),
                           lambda r: r.update(nested=None, opened_directories=1)):
                altered = deepcopy(report)
                change(altered)
                with self.assertRaises(ValueError):
                    directory.validate_record(altered, expected)

    def test_expired_absolute_deadline_prevents_open_and_root_probe(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            expected = self.fixture(root)
            with patch.object(directory.os, "open") as opened, self.assertRaises(TimeoutError):
                directory.verify(root, directory.plan(expected), time.monotonic() - 1)
            opened.assert_not_called()

    @unittest.skipUnless(os.name == "posix", "POSIX nofollow directory entry")
    def test_nested_symlink_cannot_claim_entering_fixed_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            expected = self.fixture(root)
            real = root / "real"
            (root / "nested").rename(real)
            (root / "nested").symlink_to(real, target_is_directory=True)
            expected["directories"].append("real")
            expected["files"].append({"rel_path": "real/file"})
            expected["directories"].append("real/empty")
            with self.assertRaises((OSError, ValueError)):
                directory.verify(root, directory.plan(expected), time.monotonic() + 10)


if __name__ == "__main__":
    unittest.main()
