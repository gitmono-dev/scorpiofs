"""Exact Git object transport and archive boundaries, without cloud access."""
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
import tarfile

import direct_sources as sources


class DirectSourceTests(unittest.TestCase):
    def test_actual_signed_object_identity_and_clean_shallow_tree_survive_transport(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            repo = folder / 'origin'
            subprocess.run(['git', 'init', '-q', '-b', 'main', str(repo)], check=True)
            helper = repo / 'tests/mst2-e2e/infra/aliyun/direct_sources.py'
            helper.parent.mkdir(parents=True)
            helper.write_bytes(Path(sources.__file__).read_bytes())
            (repo / 'Cargo.lock').write_text('fixture\n')
            sources.git(repo, 'add', '.')
            args = ('-c', 'user.name=fixture', '-c', 'user.email=fixture@example.invalid', '-c', 'commit.gpgsign=false')
            sources.git(repo, *args, 'commit', '-qm', 'first')
            (repo / 'body').write_bytes(bytes(range(256)) * 32)
            sources.git(repo, 'add', '.')
            sources.git(repo, *args, 'commit', '-qm', 'second')
            sha = sources.git(repo, 'rev-parse', 'HEAD').decode().strip()
            bundle = folder / 'source.tar.gz'
            report = sources.create({label: {'path': str(repo), 'sha': sha} for label in sources.LABELS}, bundle)
            restored = folder / 'restored'
            sources.restore(bundle, restored, dict.fromkeys(sources.LABELS, sha))
            for label in sources.LABELS:
                self.assertEqual(sources.git(restored / label, 'rev-parse', 'HEAD').decode().strip(), sha)
                self.assertEqual(sources.git(restored / label, 'rev-list', '--count', 'HEAD').strip(), b'1')
                self.assertFalse(sources.git(restored / label, 'status', '--porcelain').strip())
                self.assertEqual((restored / label / 'body').read_bytes(), (repo / 'body').read_bytes())
            self.assertEqual(report['sha256'], sources.digest(bundle))
            with self.assertRaises(ValueError):
                sources.restore(bundle, folder / 'wrong', dict.fromkeys(sources.LABELS, '1' * 40))

    def test_unexpected_member_or_link_cannot_create_a_checkout(self):
        for bad in ('../escape', 'link'):
            with tempfile.TemporaryDirectory() as temporary:
                folder = Path(temporary)
                archive = folder / 'bad.tar.gz'
                with tarfile.open(archive, 'w:gz') as stream:
                    info = tarfile.TarInfo(bad)
                    if bad == 'link':
                        info.type, info.linkname = tarfile.SYMTYPE, '/etc/passwd'
                        stream.addfile(info)
                    else:
                        info.size = 1
                        stream.addfile(info, io.BytesIO(b'x'))
                with self.assertRaises(ValueError):
                    sources.restore(archive, folder / 'out', dict.fromkeys(sources.LABELS, '1' * 40))
                self.assertFalse((folder / 'escape').exists())


if __name__ == '__main__':
    unittest.main()
