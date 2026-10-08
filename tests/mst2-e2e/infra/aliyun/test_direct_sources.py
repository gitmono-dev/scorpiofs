"""Exact Git object transport and archive boundaries, without cloud access."""
import io
import hashlib
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
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.dependency_paths, self.identities, self.ancestor_blobs = {}, {}, {}
        for label, identity in sources.DEPENDENCIES.items():
            repo = Path(temporary.name) / label
            subprocess.run(['git', 'init', '-q', '-b', 'main', str(repo)], check=True)
            body = repo / 'body'
            body.write_text('ancestor-only contents\n')
            sources.git(repo, 'add', '.')
            self.commit(repo, 'ancestor')
            self.ancestor_blobs[label] = sources.git(repo, 'rev-parse', 'HEAD:body').decode().strip()
            body.write_text('pinned contents\n')
            sources.git(repo, 'add', '.')
            self.commit(repo, 'pinned')
            self.dependency_paths[label] = str(repo)
            self.identities[label] = {'url': identity['url'],
                                     'sha': sources.git(repo, 'rev-parse', 'HEAD').decode().strip(),
                                     'tree': sources.git(repo, 'rev-parse', 'HEAD^{tree}').decode().strip()}
        patch = mock.patch.object(sources, 'DEPENDENCIES', self.identities)
        patch.start()
        self.addCleanup(patch.stop)

    def commit(self, repo, message):
        sources.git(repo, '-c', 'user.name=fixture', '-c', 'user.email=fixture@example.invalid',
                    '-c', 'commit.gpgsign=false', 'commit', '-qm', message)

    def helper_bytes(self):
        text = Path(sources.__file__).read_text()
        start, end = text.index('DEPENDENCIES = '), text.index('HISTORY_MODE = ')
        return (text[:start] + 'DEPENDENCIES = ' + repr(self.identities) + '\n' + text[end:]).encode()

    def create(self, inputs, archive):
        return sources.create(inputs, archive, self.dependency_paths)

    def bundle(self, folder):
        repo = folder / 'origin'
        subprocess.run(['git', 'init', '-q', '-b', 'main', str(repo)], check=True)
        helper = repo / 'tests/mst2-e2e/infra/aliyun/direct_sources.py'
        helper.parent.mkdir(parents=True)
        helper.write_bytes(self.helper_bytes())
        (repo / 'Cargo.lock').write_text('fixture\n')
        sources.git(repo, 'add', '.')
        sources.git(repo, '-c', 'user.name=fixture', '-c', 'user.email=fixture@example.invalid',
                    '-c', 'commit.gpgsign=false', 'commit', '-qm', 'first')
        sha = sources.git(repo, 'rev-parse', 'HEAD').decode().strip()
        archive = folder / 'source.tar.gz'
        report = self.create({label: {'path': str(repo), 'sha': sha} for label in sources.LABELS}, archive)
        return archive, sha, report

    def test_actual_signed_object_identity_and_clean_shallow_tree_survive_transport(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            repo = folder / 'origin'
            subprocess.run(['git', 'init', '-q', '-b', 'main', str(repo)], check=True)
            helper = repo / 'tests/mst2-e2e/infra/aliyun/direct_sources.py'
            helper.parent.mkdir(parents=True)
            helper.write_bytes(self.helper_bytes())
            (repo / 'Cargo.lock').write_text('fixture\n')
            sources.git(repo, 'add', '.')
            args = ('-c', 'user.name=fixture', '-c', 'user.email=fixture@example.invalid', '-c', 'commit.gpgsign=false')
            sources.git(repo, *args, 'commit', '-qm', 'first')
            (repo / 'body').write_bytes(bytes(range(256)) * 32)
            sources.git(repo, 'add', '.')
            sources.git(repo, *args, 'commit', '-qm', 'second')
            sha = sources.git(repo, 'rev-parse', 'HEAD').decode().strip()
            bundle = folder / 'source.tar.gz'
            report = self.create({label: {'path': str(repo), 'sha': sha} for label in sources.LABELS}, bundle)
            restored = folder / 'restored'
            sources.restore(bundle, restored, dict.fromkeys(sources.LABELS, sha))
            for label in sources.LABELS:
                self.assertEqual(sources.git(restored / label, 'rev-parse', 'HEAD').decode().strip(), sha)
                self.assertEqual(sources.git(restored / label, 'rev-list', '--count', 'HEAD').strip(), b'1')
                self.assertFalse(sources.git(restored / label, 'status', '--porcelain').strip())
                self.assertEqual((restored / label / 'body').read_bytes(), (repo / 'body').read_bytes())
            self.assertEqual(report['sha256'], sources.digest(bundle))
            for label, identity in self.identities.items():
                repo = sources.dependency_repository(restored, label)
                self.assertEqual(sources.git(repo, 'rev-parse', '--is-bare-repository').strip(), b'true')
                self.assertEqual(sources.git(repo, 'rev-parse', '--is-shallow-repository').strip(), b'false')
                self.assertEqual(sources.git(repo, 'rev-parse', 'HEAD').decode().strip(), identity['sha'])
                self.assertEqual(sources.git(repo, 'rev-list', '--count', 'HEAD').strip(), b'2')
                self.assertEqual(sources.git(repo, 'cat-file', '-p', self.ancestor_blobs[label]), b'ancestor-only contents\n')
                self.assertEqual(report['dependencies'][label]['commit_count'], 2)
                self.assertFalse((repo / 'shallow').exists())
            dependency_receipt = json.loads((restored / sources.DEPENDENCY_RECEIPT_FILE).read_bytes())
            self.assertEqual(sources.validate_dependency_receipt(dependency_receipt, restored, report['dependencies']),
                             {'revision': 1, 'dependencies': report['dependencies']})
            with self.assertRaisesRegex(ValueError, 'INVALID_DEPENDENCY_RECEIPT'):
                sources.validate_dependency_receipt(dependency_receipt | {'path': 'SECRET'}, restored, report['dependencies'])
            wrong = json.loads(json.dumps(report['dependencies']))
            wrong[sorted(wrong)[0]]['object_count'] += 1
            with self.assertRaisesRegex(ValueError, 'DEPENDENCY_RECEIPT_BINDING_MISMATCH'):
                sources.validate_dependency_receipt(dependency_receipt, restored, wrong)
            shallow = sources.dependency_repository(restored, sorted(self.identities)[0]) / 'shallow'
            shallow.write_text(identity['sha'] + '\n')
            with self.assertRaisesRegex(ValueError, 'COMPLETE_DEPENDENCY_HISTORY_REQUIRED'):
                sources.validate_dependency_receipt(dependency_receipt, restored, report['dependencies'])
            shallow.unlink()
            packed = sources.validate_performance(report['source_pack_performance'], phase='pack', require_success=True)
            self.assertEqual(len(packed['operations']), 25)
            self.assertEqual(sum(row['operation'] == 'pack-objects' for row in packed['operations']), 6)
            receipt = sources.validate_performance(json.loads((restored / sources.PERFORMANCE_FILE).read_bytes()),
                                                   phase='restore', require_success=True)
            self.assertEqual(len(receipt['operations']), 44)
            self.assertEqual(sum(row['operation'] == 'index-pack' for row in receipt['operations']), 6)
            self.assertEqual(sum(row['operation'] == 'reset' for row in receipt['operations']), 4)
            for performance in (packed, receipt):
                self.assertTrue(all(performance[name] is None for name in sources.RESOURCE_FIELDS))
                self.assertTrue(all(row['wall_ms'] >= 0 and row['exit_status'] == 0
                                    for row in performance['operations']))
            with tarfile.open(bundle, 'r:gz') as stream:
                self.assertEqual(set(stream.getnames()), {label + '.pack' for label in sources.LABELS | set(self.identities)}
                                 | {'manifest.json', 'bootstrap.py'})
                manifest = json.load(stream.extractfile('manifest.json'))
                self.assertEqual(set(manifest), {'revision', 'sources', 'dependencies', 'bootstrap_sha256'})
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
            self.assertEqual(set(path.name for path in output.iterdir()), sources.LABELS | {sources.PERFORMANCE_FILE, 'dependency-git'})
            for label in sources.LABELS:
                self.assertFalse(sources.git(output / label, 'status', '--porcelain').strip())

    def rewrite_bundle(self, archive, target, transform):
        with tarfile.open(archive, 'r:gz') as stream:
            bodies = {member.name: stream.extractfile(member).read() for member in stream.getmembers()}
        manifest = json.loads(bodies['manifest.json'])
        transform(manifest, bodies)
        bodies['manifest.json'] = json.dumps(manifest).encode()
        with tarfile.open(target, 'w:gz') as stream:
            for name, body in bodies.items():
                info = tarfile.TarInfo(name)
                info.size = len(body)
                stream.addfile(info, io.BytesIO(body))

    def test_shallow_dependency_is_rejected_without_inventing_ancestor_closure(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            _, sha, _ = self.bundle(folder)
            label = sorted(self.identities)[0]
            shallow = folder / 'shallow'
            subprocess.run(['git', 'clone', '-q', '--depth=1', Path(self.dependency_paths[label]).as_uri(), str(shallow)],
                           check=True, capture_output=True)
            self.dependency_paths[label] = str(shallow)
            with self.assertRaisesRegex(ValueError, 'COMPLETE_DEPENDENCY_HISTORY_REQUIRED') as failure:
                self.create({name: {'path': str(folder / 'origin'), 'sha': sha} for name in sources.LABELS},
                            folder / 'rejected.tar.gz')
            receipt = sources.validate_performance(failure.exception.source_git_performance, phase='pack')
            self.assertEqual(receipt['status'], 'failed')
            self.assertEqual(receipt['operations'][-1]['source'], label)
            self.assertEqual(receipt['operations'][-1]['operation'], 'rev-parse')
            self.assertFalse((folder / 'rejected.tar.gz').exists())

    def test_manifest_rejects_pin_url_inventory_caps_extra_fields_and_old_revision(self):
        with tempfile.TemporaryDirectory() as temporary:
            archive, _, _ = self.bundle(Path(temporary))
            with tarfile.open(archive, 'r:gz') as stream:
                manifest = json.load(stream.extractfile('manifest.json'))
            label = sorted(self.identities)[0]
            changes = ({'sha': '1' * 40}, {'url': 'https://unreviewed.invalid/repo'}, {'tree': '1' * 40},
                       {'history_mode': 'shallow'}, {'object_count': True}, {'object_count': 0},
                       {'object_count': sources.MAX_DEPENDENCY_OBJECTS + 1},
                       {'object_bytes': sources.MAX_DEPENDENCY_OBJECT_BYTES + 1},
                       {'commit_count': sources.MAX_DEPENDENCY_COMMITS + 1},
                       {'pack_bytes': sources.MAX_BUNDLE + 1}, {'private_path': 'SECRET'})
            for change in changes:
                with self.subTest(change=change):
                    value = json.loads(json.dumps(manifest))
                    value['dependencies'][label].update(change)
                    with self.assertRaises(ValueError):
                        sources.validate_manifest(value)
            for change in ({'revision': 1}, {'credentials': 'SECRET'}, {'dependencies': {}}):
                with self.assertRaises(ValueError):
                    sources.validate_manifest(manifest | change)
            for value in ({}, self.dependency_paths | {'unknown': 'extra'},
                          self.dependency_paths | {label: 'relative'},
                          self.dependency_paths | {label: {'path': 'nested'}}):
                with self.assertRaisesRegex(ValueError, 'DEPENDENCY_SOURCES_REQUIRED'):
                    sources.dependency_sources(value)

    def test_dependency_pack_hash_bootstrap_hash_and_inventory_mismatch_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            archive, sha, _ = self.bundle(folder)
            label = sorted(self.identities)[0]
            def corrupt_pack(manifest, bodies):
                bodies[label + '.pack'] = b'x' * len(bodies[label + '.pack'])
            def corrupt_helper(manifest, bodies):
                bodies['bootstrap.py'] += b'\n# tampered\n'
            def corrupt_inventory(manifest, bodies):
                manifest['dependencies'][label]['object_count'] += 1
            for index, (transform, code) in enumerate(((corrupt_pack, 'SOURCE_PACK_MISMATCH'),
                                                      (corrupt_helper, 'BOOTSTRAP_DIGEST_MISMATCH'),
                                                      (corrupt_inventory, 'DEPENDENCY_INVENTORY_MISMATCH'))):
                with self.subTest(code=code):
                    target, output = folder / (str(index) + '.tar.gz'), folder / ('out-' + str(index))
                    self.rewrite_bundle(archive, target, transform)
                    with self.assertRaisesRegex(ValueError, code):
                        sources.restore(target, output, dict.fromkeys(sources.LABELS, sha))
                    receipt = sources.validate_performance(json.loads((output / sources.PERFORMANCE_FILE).read_bytes()),
                                                           phase='restore')
                    self.assertEqual(receipt['status'], 'failed')

    def test_missing_dependency_parent_is_rejected_even_with_updated_pack_checksum(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            archive, sha, _ = self.bundle(folder)
            label = sorted(self.identities)[0]
            identity, repo = self.identities[label], Path(self.dependency_paths[label])
            objects = {identity['sha'], identity['tree']}
            for row in sources.git(repo, 'ls-tree', '-r', '-t', '-z', identity['sha']).split(b'\0'):
                if row:
                    objects.add(row.split(b'\t', 1)[0].split()[2].decode())
            incomplete = sources.git(repo, 'pack-objects', '--stdout', data=('\n'.join(sorted(objects)) + '\n').encode())
            def replace(manifest, bodies):
                bodies[label + '.pack'] = incomplete
                manifest['dependencies'][label].update(pack_bytes=len(incomplete),
                    pack_sha256=hashlib.sha256(incomplete).hexdigest())
            target, output = folder / 'incomplete.tar.gz', folder / 'out'
            self.rewrite_bundle(archive, target, replace)
            with self.assertRaises(sources.SourceGitFailure) as failure:
                sources.restore(target, output, dict.fromkeys(sources.LABELS, sha))
            receipt = sources.validate_performance(failure.exception.source_git_performance, phase='restore')
            self.assertEqual(receipt['operations'][-1]['source'], label)
            self.assertIn(receipt['operations'][-1]['operation'], ('index-pack', 'fsck'))
            self.assertFalse((sources.dependency_repository(output, label) / 'shallow').exists())

    def test_dependency_inventory_byte_cap_stops_before_pack_and_retains_performance(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            _, sha, _ = self.bundle(folder)
            with mock.patch.object(sources, 'MAX_DEPENDENCY_OBJECT_BYTES', 1):
                with self.assertRaisesRegex(ValueError, 'DEPENDENCY_OBJECTS_TOO_LARGE') as failure:
                    self.create({name: {'path': str(folder / 'origin'), 'sha': sha} for name in sources.LABELS},
                                folder / 'over-cap.tar.gz')
            receipt = sources.validate_performance(failure.exception.source_git_performance, phase='pack')
            self.assertEqual(receipt['operations'][-1]['operation'], 'cat-file')
            self.assertFalse((folder / 'over-cap.tar.gz').exists())

    def test_pack_failure_retains_numeric_receipt_without_private_command_output(self):
        secret = 'Authorization: Bearer SECRET_PRIVATE_PATH'
        def run(argv, **options):
            code = 7 if 'pack-objects' in argv else 0
            body = b'1' * 40 + b'\n' if 'rev-parse' in argv else b''
            return subprocess.CompletedProcess(argv, code, body, secret.encode())
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(sources.subprocess, 'run', side_effect=run):
            folder = Path(temporary)
            with self.assertRaises(sources.SourceGitFailure) as failed:
                self.create({label: {'path': secret, 'sha': '1' * 40} for label in sources.LABELS}, folder / 'source.tar.gz')
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
