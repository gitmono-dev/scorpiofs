import hashlib
import os
from pathlib import Path
import tempfile
import time
import unittest

from workspace_update_oracle import READ_BYTES, directory_sets, verify_workspace


@unittest.skipUnless(hasattr(os, "fwalk") and hasattr(os, "O_NOFOLLOW"), "requires POSIX directory descriptors")
class WorkspaceOracleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.raw = b"streamed fixture" * (READ_BYTES // 2)
        self.root.joinpath("dir").mkdir()
        self.root.joinpath("dir/file").write_bytes(self.raw)
        self.expected = {"directories": ["", "dir", "empty", "empty/nested"], "files": [{
            "rel_path": "dir/file", "fs_kind": "regular", "size": len(self.raw),
            "content_digest": "sha256:" + hashlib.sha256(self.raw).hexdigest(),
        }]}

    def verify(self, git=False):
        return verify_workspace(self.root, self.expected, time.monotonic() + 5, git_checkout=git)

    def test_empty_trees_are_required_for_scorpio_and_explicitly_reported_for_git(self):
        with self.assertRaises(AssertionError):
            self.verify()
        self.root.joinpath(".git").write_text("gitdir: fixture")
        report = self.verify(True)
        self.assertEqual(report["raw_empty_tree_directories_omitted_by_git"], ["empty", "empty/nested"])
        self.assertEqual(report["verified_bytes"], len(self.raw))
        self.assertGreater(report["regular_read_calls"], len(self.raw) // READ_BYTES)
        self.root.joinpath(".git").unlink()
        self.root.joinpath("empty/nested").mkdir(parents=True)
        self.assertEqual(self.verify()["verified_directories"], 4)

    def test_extra_empty_directories_and_unexpected_git_file_are_rejected(self):
        self.root.joinpath("empty/nested").mkdir(parents=True)
        self.root.joinpath("extra").mkdir()
        with self.assertRaises(AssertionError):
            self.verify()
        self.root.joinpath("extra").rmdir()
        self.root.joinpath(".git").write_text("unexpected Scorpio entry")
        with self.assertRaises(AssertionError):
            self.verify()

    def test_git_administration_directory_is_not_exempt_from_the_oracle(self):
        self.root.joinpath(".git").mkdir()
        self.root.joinpath(".git/hidden").write_bytes(b"unexpected directory contents")
        with self.assertRaises(AssertionError):
            self.verify(True)

    def test_executable_and_directory_symlink_content_are_verified(self):
        path = self.root.joinpath("dir/file")
        path.chmod(0o755)
        self.expected["files"][0]["fs_kind"] = "executable"
        self.root.joinpath("link").symlink_to("dir", target_is_directory=True)
        self.expected["files"].append({
            "rel_path": "link", "fs_kind": "symlink", "size": 3,
            "content_digest": "sha256:" + hashlib.sha256(b"dir").hexdigest(),
        })
        self.assertEqual(self.verify(True)["verified_files"], 2)
        path.chmod(0o644)
        with self.assertRaises(AssertionError):
            self.verify(True)

    def test_same_length_byte_damage_and_expired_deadline_are_rejected(self):
        self.root.joinpath("dir/file").write_bytes(b"X" + self.raw[1:])
        with self.assertRaises(AssertionError):
            self.verify(True)
        with self.assertRaises(TimeoutError):
            verify_workspace(self.root, self.expected, time.monotonic() - 1, git_checkout=True)


class DirectoryManifestTests(unittest.TestCase):
    def test_duplicate_entries_and_missing_ancestors_are_rejected(self):
        for manifest in [
            {"directories": ["", ""], "files": []},
            {"directories": ["", "a/b"], "files": []},
            {"directories": [""], "files": [{"rel_path": "a/file"}]},
            {"directories": [""], "files": [{"rel_path": "../file"}]},
        ]:
            with self.assertRaises(AssertionError):
                directory_sets(manifest)


if __name__ == "__main__":
    unittest.main()
