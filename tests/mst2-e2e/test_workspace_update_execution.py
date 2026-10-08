"""Offline execution ownership checks; never query real instance metadata."""

from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import commit_update_bench as common
import commit_update_ci as ci
import workspace_update_execution as execution
import workspace_update_campaign_export as export
import workspace_update_build as builds


class ExecutionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        root = Path(self.temp.name).resolve()
        self.stack.enter_context(patch.object(execution, 'DATA_ROOT', root / 'data'))
        self.stack.enter_context(patch.object(execution, 'RECEIPT_ROOT', root / 'control'))
        start = datetime.now(timezone.utc) - timedelta(minutes=1)
        self.context = {'revision': 1, 'execution_provider': 'aliyun-direct',
            'campaign_id': 'v3-' + 'a' * 20, 'instance_id': 'i-bp1abcdefgh12345', 'attempt': '1',
            'run_uid': 1001, 'run_gid': 1001, 'data_root': execution.DATA_ROOT.as_posix(),
            'data_device': '/dev/vdb', 'data_uuid': '11111111-2222-3333-4444-555555555555',
            'owned_root': (execution.DATA_ROOT / 'work' / ('mst2-direct-v3-' + 'a' * 20 + '-1')).as_posix(),
            'session_started_utc': start.isoformat(),
            'session_deadline_utc': (start + timedelta(minutes=235)).isoformat(),
            'hard_release_utc': (start + timedelta(minutes=240)).isoformat()}
        self.receipt = execution.RECEIPT_ROOT / ('direct-execution-' + self.context['campaign_id'] + '-1.json')
        self.environment = {'MST2_EXECUTION_RECEIPT': str(self.receipt),
            'HOME': str(execution.DATA_ROOT / 'test-home'),
            'CARGO_HOME': str(execution.DATA_ROOT / 'cargo'),
            'RUSTUP_HOME': str(execution.DATA_ROOT / 'rustup'),
            'TMPDIR': str(execution.DATA_ROOT / 'tmp'),
            'STARTED_INPUT': self.context['session_started_utc'],
            'DEADLINE_INPUT': self.context['session_deadline_utc']}
        execution._METADATA_CACHE.clear()

    def live(self):
        self.stack.enter_context(patch.dict(execution.os.environ, self.environment, clear=True))
        self.stack.enter_context(patch.object(execution.sys, 'platform', 'linux'))
        self.stack.enter_context(patch.object(execution.os, 'geteuid', return_value=1001, create=True))
        self.stack.enter_context(patch.object(execution.os, 'getegid', return_value=1001, create=True))
        account = SimpleNamespace(pw_uid=1001, pw_gid=1001, pw_dir=str(execution.DATA_ROOT / 'test-home'))
        self.stack.enter_context(patch.object(execution, 'pwd', SimpleNamespace(getpwnam=Mock(return_value=account))))
        self.read = self.stack.enter_context(patch.object(execution, 'read_receipt', return_value=(self.context, 'f' * 64)))
        self.storage = self.stack.enter_context(patch.object(execution, 'check_storage'))
        self.metadata = self.stack.enter_context(patch.object(execution, 'metadata_instance_id', return_value=self.context['instance_id']))
        self.stack.enter_context(patch.object(execution, '_boot_id', return_value='11111111-2222-3333-4444-555555555555'))

    def test_linux_metadata_replays_on_the_control_platform(self):
        value = deepcopy(self.context)
        value.update(data_root='/srv/scorpiofs-benchmark',
            owned_root='/srv/scorpiofs-benchmark/work/mst2-direct-' + value['campaign_id'] + '-1')
        with patch.object(execution, 'DATA_ROOT', Path('/srv/scorpiofs-benchmark')):
            self.assertEqual(execution.validate_context(value), value)

    def test_direct_identity_is_real_and_imds_is_cached_without_skipping_local_checks(self):
        self.live()
        first, second = execution.identity(), execution.identity()
        self.assertEqual(first, second)
        self.assertEqual(first['execution_provider'], 'aliyun-direct')
        self.assertRegex(first['run_id'], r'[1-9][0-9]{17}')
        self.assertEqual(first['run_id'], execution.run_id_for_campaign(self.context['campaign_id']))
        self.metadata.assert_called_once_with()
        self.assertEqual((self.read.call_count, self.storage.call_count), (2, 2))
        root, project = ci.hosted_root(Path(self.context['owned_root']))
        self.assertEqual(root.as_posix(), self.context['owned_root'])
        self.assertEqual(project, 'm2perf-' + first['run_id'] + '-1')
        self.assertFalse(any(key.startswith('GITHUB_') for key in execution.os.environ))
        with self.assertRaises(ValueError):
            ci.hosted_root(root / 'another')

    def test_actual_instance_and_user_must_match_root_receipt(self):
        self.live()
        self.metadata.return_value = 'i-anotherinstance'
        with self.assertRaisesRegex(ValueError, 'instance_mismatch'):
            execution.identity()
        execution._METADATA_CACHE.clear()
        self.metadata.return_value = self.context['instance_id']
        with patch.object(execution.os, 'geteuid', return_value=0), self.assertRaisesRegex(ValueError, 'user_mismatch'):
            execution.identity()

    def test_direct_forbids_fake_actions_and_wrong_data_environment(self):
        self.live()
        for key, value in (('GITHUB_ACTIONS', 'true'), ('GITHUB_RUN_ID', '123'),
                           ('GITHUB_SHA', 'a' * 40), ('RUNNER_ENVIRONMENT', 'self-hosted'),
                           ('HOME', '/root'), ('CARGO_HOME', '/tmp/cargo'), ('CARGO_TARGET_DIR', '/shared/target')):
            with self.subTest(key=key), patch.dict(execution.os.environ, {key: value}), self.assertRaises(ValueError):
                execution.identity()
        self.metadata.assert_not_called()

    def test_all_windows_and_cli_options_bind_to_original_receipt(self):
        self.live()
        options = SimpleNamespace(session_started_utc=self.context['session_started_utc'],
                                  session_deadline_utc=self.context['session_deadline_utc'])
        execution.bind_options(options)
        options.session_deadline_utc = self.context['hard_release_utc']
        with self.assertRaisesRegex(ValueError, 'window_mismatch'):
            execution.bind_options(options)
        for key, value in (('STARTED_INPUT', self.context['hard_release_utc']),
                           ('DEADLINE_INPUT', self.context['hard_release_utc']),
                           ('MST2_WORK_CLEANUP_DEADLINE_MONOTONIC', 'nan'),
                           ('MST2_WORK_CLEANUP_DEADLINE_MONOTONIC', str(time.monotonic() + 999999))):
            with patch.dict(execution.os.environ, {key: value}), self.assertRaises(ValueError):
                execution.identity()

    def test_cleanup_can_run_after_preflight_but_never_after_hard_release(self):
        self.live()
        start = datetime.now(timezone.utc) - timedelta(minutes=230)
        for key, minutes in (('session_started_utc', 0), ('session_deadline_utc', 235), ('hard_release_utc', 240)):
            self.context[key] = (start + timedelta(minutes=minutes)).isoformat()
        with patch.dict(execution.os.environ, {'STARTED_INPUT': self.context['session_started_utc'],
                                             'DEADLINE_INPUT': self.context['session_deadline_utc']}):
            self.assertEqual(execution.identity()['instance_id'], self.context['instance_id'])
        self.context['hard_release_utc'] = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
        with self.assertRaisesRegex(ValueError, 'window_expired'):
            execution.identity()

    def test_context_rejects_duplicate_shape_wrong_roots_or_extended_window(self):
        execution.validate_context(self.context)
        for key, value in (('revision', True), ('campaign_id', 'other'), ('attempt', '4'),
                           ('run_uid', 0), ('run_gid', True), ('instance_id', 'server'),
                           ('data_device', '/dev/../vdb'), ('data_uuid', '../uuid'),
                           ('data_root', '/tmp'), ('owned_root', self.context['owned_root'] + '/child'),
                           ('hard_release_utc', self.context['session_deadline_utc'])):
            bad = dict(self.context, **{key: value})
            with self.subTest(key=key), self.assertRaises(ValueError):
                execution.validate_context(bad)

    def test_receipt_requires_root_regular_unwritable_file_and_trusted_parents(self):
        for mode, uid, gid in ((stat.S_IFLNK | 0o777, 0, 0), (stat.S_IFREG | 0o666, 0, 0),
                               (stat.S_IFREG | 0o644, 1001, 0), (stat.S_IFREG | 0o644, 0, 1001)):
            path = Mock()
            path.lstat.return_value = SimpleNamespace(st_mode=mode, st_uid=uid, st_gid=gid)
            with self.subTest(mode=mode, uid=uid, gid=gid), self.assertRaises(ValueError):
                execution._root_path(path)

    def test_work_parent_is_benchmark_owned_real_directory_on_the_actual_data_mount(self):
        mount = SimpleNamespace(st_dev=7)
        good = SimpleNamespace(st_mode=stat.S_IFDIR | 0o700, st_uid=1001, st_gid=1001, st_dev=7)
        device = Path('/dev/vdb')
        bad = [SimpleNamespace(**{**good.__dict__, key: value}) for key, value in
               (('st_mode', stat.S_IFLNK | 0o777), ('st_mode', stat.S_IFDIR | 0o777),
                ('st_uid', 0), ('st_gid', 0), ('st_dev', 8))]
        for directory in [good, *bad]:
            with self.subTest(directory=directory), ExitStack() as stack:
                stack.enter_context(patch.object(execution, '_root_path', return_value=mount))
                stack.enter_context(patch.object(execution.os.path, 'ismount', return_value=True))
                stack.enter_context(patch.object(Path, 'resolve', return_value=device))
                stack.enter_context(patch.object(Path, 'stat', return_value=SimpleNamespace(
                    st_mode=stat.S_IFBLK | 0o600, st_rdev=7)))
                stack.enter_context(patch.object(Path, 'exists', return_value=False))
                stack.enter_context(patch.object(Path, 'is_symlink', return_value=False))
                def info(path):
                    return directory if path == execution.DATA_ROOT / 'work' else good
                stack.enter_context(patch.object(Path, 'lstat', autospec=True, side_effect=info))
                if directory is good:
                    execution.check_storage(self.context)
                else:
                    with self.assertRaisesRegex(ValueError, 'storage_owner_mismatch'):
                        execution.check_storage(self.context)

    def read_mock(self, raw, *, changed=False, nlink=1):
        info = SimpleNamespace(st_mode=stat.S_IFREG | 0o644, st_uid=0, st_gid=0, st_nlink=nlink,
            st_dev=1, st_ino=2, st_size=len(raw), st_mtime_ns=1, st_ctime_ns=1)
        self.stack.enter_context(patch.object(execution, '_root_path', return_value=info))
        self.stack.enter_context(patch.object(execution.os, 'open', return_value=3))
        self.stack.enter_context(patch.object(execution.os, 'close'))
        self.stack.enter_context(patch.object(execution.os, 'fdopen', return_value=io.BytesIO(raw)))
        other = SimpleNamespace(**{**info.__dict__, 'st_ino': 9}) if changed else info
        self.stack.enter_context(patch.object(execution.os, 'fstat', side_effect=[info, other]))
        self.stack.enter_context(patch.object(Path, 'lstat', return_value=info))

    def test_receipt_read_is_capped_unique_and_bound_to_exact_control_path(self):
        raw = json.dumps(self.context).encode()
        self.read_mock(raw)
        value, digest = execution.read_receipt(self.receipt)
        self.assertEqual(value, self.context)
        self.assertEqual(digest, hashlib.sha256(raw).hexdigest())

    def test_receipt_rejects_hardlink_mutation_duplicate_and_oversized_json(self):
        for issue in ('hardlink', 'mutation', 'duplicate', 'oversized'):
            with self.subTest(issue=issue), ExitStack() as stack:
                old = self.stack
                self.stack = stack
                raw = json.dumps(self.context).encode()
                if issue == 'duplicate':
                    raw = raw[:-1] + b',"revision":1}'
                if issue == 'oversized':
                    raw = b'x' * 32769
                self.read_mock(raw, changed=issue == 'mutation', nlink=2 if issue == 'hardlink' else 1)
                try:
                    with self.assertRaises(ValueError):
                        execution.read_receipt(self.receipt)
                finally:
                    self.stack = old

    def response(self, data, url):
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.geturl.return_value = url
        response.read.return_value = data
        return response

    def test_imdsv2_uses_exact_aliyun_urls_headers_and_no_proxy_or_redirect_fallback(self):
        opener = Mock()
        opener.open.side_effect = [self.response(b'private-token', execution.IMDS_ROOT + 'api/token'),
            self.response(self.context['instance_id'].encode(), execution.IMDS_ROOT + 'meta-data/instance-id')]
        with patch.object(execution, 'build_opener', return_value=opener) as build:
            self.assertEqual(execution.metadata_instance_id(), self.context['instance_id'])
        handlers = build.call_args.args
        self.assertEqual(handlers[0].proxies, {})
        self.assertIsInstance(handlers[1], execution._NoRedirect)
        token, instance = [call.args[0] for call in opener.open.call_args_list]
        self.assertEqual(token.full_url, 'http://100.100.100.200/latest/api/token')
        self.assertEqual(token.get_method(), 'PUT')
        self.assertEqual(token.get_header('X-aliyun-ecs-metadata-token-ttl-seconds'), '60')
        self.assertEqual(instance.get_header('X-aliyun-ecs-metadata-token'), 'private-token')
        self.assertTrue(all(call.kwargs['timeout'] == 2 for call in opener.open.call_args_list))

    def test_metadata_failures_hide_token_and_never_fall_back_to_v1(self):
        for failure in (TimeoutError('private-token'), self.response(b'private-token\n', execution.IMDS_ROOT + 'api/token'),
                        self.response(b'private-token', 'https://example.invalid/redirect')):
            opener = Mock()
            if isinstance(failure, Exception):
                opener.open.side_effect = failure
            else:
                opener.open.return_value = failure
            with patch.object(execution, 'build_opener', return_value=opener), \
                    self.assertRaisesRegex(ValueError, '^execution_metadata_failed$') as error:
                execution.metadata_instance_id()
            self.assertNotIn('private-token', str(error.exception))
            self.assertEqual(opener.open.call_count, 1)

    def test_direct_run_metadata_binds_provider_instance_and_original_window(self):
        self.live()
        extra = {'SCORPIO_SHA': 'a' * 40, 'MEGA_SHA': 'b' * 40,
            'BASELINE_SHA': builds.DEFAULT_BASELINE, 'CANDIDATE_SHA': builds.DEFAULT_CANDIDATE,
            'PROFILE': 'history-large', 'ROUNDS': '3', 'COMPARISON': 'isolated',
            'BOOTSTRAP_COMMIT_TIME': '1700000000', 'MST2_OWNED_ROOT': self.context['owned_root'],
            'MST2_WORK_CLEANUP_DEADLINE_MONOTONIC': str(time.monotonic() + 100)}
        with patch.dict(execution.os.environ, extra):
            value = export.run_metadata_from_env()
        self.assertEqual(set(value), export.DIRECT_RUN_FIELDS)
        self.assertEqual(value['execution_provider'], 'aliyun-direct')
        self.assertEqual(value['instance_id'], self.context['instance_id'])
        export.validate_run_metadata(value)
        for key, wrong in (('run_id', '1'), ('instance_id', 'fake'), ('hard_release_utc', value['session_deadline_utc']),
                           ('owned_root', value['owned_root'] + '/child'), ('execution_provider', 'github-actions')):
            with self.subTest(key=key), self.assertRaises((ValueError, AssertionError)):
                export.validate_run_metadata({**value, key: wrong})

    def test_build_environment_keeps_data_disk_toolchain_paths_and_drops_secrets_or_shared_target(self):
        value = {'HOME': '/srv/scorpiofs-benchmark/test-home', 'CARGO_HOME': '/srv/scorpiofs-benchmark/cargo',
            'RUSTUP_HOME': '/srv/scorpiofs-benchmark/rustup', 'TMPDIR': '/srv/scorpiofs-benchmark/tmp',
            'CARGO_TARGET_DIR': '/shared/target', 'M2_TOKEN': 'private-secret'}
        with patch.dict(common.os.environ, value, clear=True):
            clean = common.clean_env()
        for key in ('HOME', 'CARGO_HOME', 'RUSTUP_HOME', 'TMPDIR'):
            self.assertEqual(clean[key], value[key])
        self.assertNotIn('CARGO_TARGET_DIR', clean)
        self.assertNotIn('M2_TOKEN', clean)

    def test_actual_build_driver_passes_data_disk_environment_to_rustup_and_cargo(self):
        source = Path(self.temp.name) / 'source'
        binary = source / 'target/release/scorpio'
        binary.parent.mkdir(parents=True)
        binary.write_bytes(b'offline-build-artifact')
        env = {key: self.environment[key] for key in ('HOME', 'CARGO_HOME', 'RUSTUP_HOME', 'TMPDIR')}
        env.update(CARGO_TARGET_DIR='/shared/target', M2_TOKEN='private-secret')
        with patch.dict(common.os.environ, env, clear=True), \
                patch.object(builds, 'fixed_source', return_value=(source, 'f' * 64)), \
                patch.object(builds, 'output', side_effect=[b'rustc offline\n', b'cargo offline\n']) as versions, \
                patch.object(builds.subprocess, 'run') as cargo:
            builds.build(source, 'a' * 40, 'a', Path(self.temp.name) / 'build.json', time.monotonic() + 5)
        environments = [call.args[2] for call in versions.call_args_list] + [cargo.call_args.kwargs['env']]
        for child in environments:
            for key in ('HOME', 'CARGO_HOME', 'RUSTUP_HOME', 'TMPDIR'):
                self.assertEqual(child[key], env[key])
            self.assertNotIn('CARGO_TARGET_DIR', child)
            self.assertNotIn('M2_TOKEN', child)


if __name__ == '__main__':
    unittest.main()
