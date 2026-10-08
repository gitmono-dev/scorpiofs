"""Exact Git object transport and archive boundaries, without cloud access."""
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import tarfile

import direct_sources as sources


class DirectSourceTests(unittest.TestCase):
    def bundle(self, folder):
        repo = folder / 'origin'
        subprocess.run(['git', 'init', '-q', '-b', 'main', str(repo)], check=True)
        helper = repo / 'tests/mst2-e2e/infra/aliyun/direct_sources.py'
        helper.parent.mkdir(parents=True)
        helper.write_bytes(Path(sources.__file__).read_bytes())
        (repo / 'Cargo.lock').write_text('fixture\n')
        sources.git(repo, 'add', '.')
        sources.git(repo, '-c', 'user.name=fixture', '-c', 'user.email=fixture@example.invalid',
                    '-c', 'commit.gpgsign=false', 'commit', '-qm', 'first')
        sha = sources.git(repo, 'rev-parse', 'HEAD').decode().strip()
        archive = folder / 'source.tar.gz'
        report = sources.create({label: {'path': str(repo), 'sha': sha} for label in sources.LABELS}, archive)
        return archive, sha, report

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
            packed = sources.validate_performance(report['source_pack_performance'], phase='pack', require_success=True)
            self.assertEqual(len(packed['operations']), 13)
            self.assertEqual(sum(row['operation'] == 'pack-objects' for row in packed['operations']), 4)
            receipt = sources.validate_performance(json.loads((restored / sources.PERFORMANCE_FILE).read_bytes()),
                                                   phase='restore', require_success=True)
            self.assertEqual(len(receipt['operations']), 28)
            self.assertEqual(sum(row['operation'] == 'index-pack' for row in receipt['operations']), 4)
            self.assertEqual(sum(row['operation'] == 'reset' for row in receipt['operations']), 4)
            for performance in (packed, receipt):
                self.assertTrue(all(performance[name] is None for name in sources.RESOURCE_FIELDS))
                self.assertTrue(all(row['wall_ms'] >= 0 and row['exit_status'] == 0
                                    for row in performance['operations']))
            with tarfile.open(bundle, 'r:gz') as stream:
                self.assertEqual(set(stream.getnames()), {label + '.pack' for label in sources.LABELS}
                                 | {'manifest.json', 'bootstrap.py'})
                manifest = json.load(stream.extractfile('manifest.json'))
                self.assertEqual(set(manifest), {'revision', 'sources', 'bootstrap_sha256'})
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

    def test_bootstrap_is_standalone_and_keeps_metrics_outside_source_checkouts(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            archive, sha, _ = self.bundle(folder)
            helper = folder / 'bootstrap.py'
            with tarfile.open(archive, 'r:gz') as stream:
                helper.write_bytes(stream.extractfile('bootstrap.py').read())
            output = folder / 'standalone'
            subprocess.run([sys.executable, '-I', str(helper), '--bundle', str(archive), '--output', str(output),
                            '--pins', json.dumps(dict.fromkeys(sources.LABELS, sha))], check=True, timeout=60)
            receipt = json.loads((output / sources.PERFORMANCE_FILE).read_bytes())
            sources.validate_performance(receipt, phase='restore', require_success=True)
            self.assertEqual(set(path.name for path in output.iterdir()), sources.LABELS | {sources.PERFORMANCE_FILE})
            for label in sources.LABELS:
                self.assertFalse(sources.git(output / label, 'status', '--porcelain').strip())

    def test_pack_failure_retains_numeric_receipt_without_private_command_output(self):
        secret = 'Authorization: Bearer SECRET_PRIVATE_PATH'
        def run(argv, **options):
            code = 7 if 'pack-objects' in argv else 0
            body = b'1' * 40 + b'\n' if 'rev-parse' in argv else b''
            return subprocess.CompletedProcess(argv, code, body, secret.encode())
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(sources.subprocess, 'run', side_effect=run):
            folder = Path(temporary)
            with self.assertRaises(sources.SourceGitFailure) as failed:
                sources.create({label: {'path': secret, 'sha': '1' * 40} for label in sources.LABELS}, folder / 'source.tar.gz')
            receipt = failed.exception.source_git_performance
            sources.validate_performance(receipt, phase='pack')
            self.assertEqual(receipt['operations'][-1]['operation'], 'pack-objects')
            self.assertEqual(receipt['operations'][-1]['status'], 'nonzero_exit')
            self.assertEqual(receipt['operations'][-1]['exit_status'], 7)
            self.assertNotIn(secret, str(failed.exception) + json.dumps(receipt))

    def test_index_and_reset_failures_write_closed_restore_receipt(self):
        secret = 'SECRET_PRIVATE_PATH_AND_STDERR'
        actual_run = subprocess.run
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            archive, sha, _ = self.bundle(folder)
            for operation in ('index-pack', 'reset'):
                with self.subTest(operation=operation):
                    def run(argv, **options):
                        if operation in argv:
                            return subprocess.CompletedProcess(argv, 9, secret.encode(), secret.encode())
                        return actual_run(argv, **options)
                    output = folder / operation
                    with mock.patch.object(sources.subprocess, 'run', side_effect=run):
                        with self.assertRaises(sources.SourceGitFailure) as failed:
                            sources.restore(archive, output, dict.fromkeys(sources.LABELS, sha))
                    receipt = json.loads((output / sources.PERFORMANCE_FILE).read_bytes())
                    sources.validate_performance(receipt, phase='restore')
                    self.assertEqual(receipt, failed.exception.source_git_performance)
                    self.assertEqual(receipt['operations'][-1]['operation'], operation)
                    self.assertEqual(receipt['operations'][-1]['status'], 'nonzero_exit')
                    self.assertNotIn(secret, str(failed.exception) + json.dumps(receipt))

    def test_timeout_and_spawn_failure_do_not_echo_exception_details(self):
        secret = 'PRIVATE_ARG_OR_OUTPUT'
        for error, status in ((subprocess.TimeoutExpired(['git', secret], 1, output=secret.encode()), 'timeout'),
                              (FileNotFoundError(secret), 'spawn_failed')):
            with self.subTest(status=status), mock.patch.object(sources.subprocess, 'run', side_effect=error):
                performance = sources._TransportPerformance('pack')
                with self.assertRaises(sources.SourceGitFailure) as failed:
                    performance.run('rev-parse', 'client-a', ['git', '-C', secret, 'rev-parse', 'HEAD'])
                receipt = failed.exception.source_git_performance
                sources.validate_performance(receipt, phase='pack')
                self.assertEqual(receipt['operations'][-1]['status'], status)
                self.assertIsNone(receipt['operations'][-1]['exit_status'])
                self.assertNotIn(secret, str(failed.exception) + json.dumps(receipt))

    def test_performance_receipt_rejects_extra_fields_nonfinite_time_or_fake_success(self):
        empty = sources._TransportPerformance('restore').receipt(False)
        for value in ({**empty, 'argv': 'SECRET'}, {**empty, 'status': 'success'},
                      {**empty, 'operations': [{'source': 'client-a', 'operation': 'init', 'wall_ms': float('nan'),
                                              'status': 'success', 'exit_status': 0}]}):
            with self.assertRaises(ValueError):
                sources.validate_performance(value)


if __name__ == '__main__':
    unittest.main()
