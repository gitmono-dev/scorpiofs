"""Failure and exclusive ownership regressions without cloud or build calls."""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import errno
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import direct_remote as remote

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import commit_update_budget as budgets
import commit_update_projection as projection
import workspace_update_campaign_export as exporter
import workspace_update_execution as execution
import workspace_update_git_performance as metrics
import workspace_update_size as size


class RemoteFailureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.evidence = self.root / 'evidence'
        self.evidence.mkdir()
        self.config = {'workspace': str(self.root / 'sources'), 'status_path': str(self.evidence / 'status.json'),
            'campaign_id': 'v3-' + 'a' * 20, 'instance_id': 'i-owned', 'harness_sha': 'b' * 40,
            'profile': 'history-large', 'mega_sha': 'c' * 40, 'baseline_sha': 'd' * 40,
            'candidate_sha': 'e' * 40, 'session_started_utc': '2026-10-08T00:00:00Z',
            'session_deadline_utc': '2026-10-08T03:55:00Z', 'evidence_prefix': 'runs/owned/'}
        self.context = {'owned_root': str(self.root / 'owned')}
        self.progress = {'stage': 'execution-admission', 'failure': None}

    def exercise(self, *, save_error=False):
        def export(root, output, *args, **kwargs):
            output.mkdir()
            (output / 'safe-export.json').write_text(json.dumps({'complete_campaign': False}), encoding='utf-8')
        with patch.dict(os.environ, {}, clear=False), \
                patch.object(projection, 'window_anchor', return_value=time.monotonic() + 60), \
                patch.object(budgets, 'IsolatedCampaignBudget'), \
                patch.object(budgets, 'run_process', return_value=(0, b'', b'')) as run, \
                patch.object(budgets, 'require_external_time'), \
                patch.object(size, 'admit_backend'), patch.object(size, 'admit_campaign_disk'), \
                patch.object(exporter, 'export', side_effect=export) as exported, \
                patch.object(exporter, 'run_metadata_from_env', return_value={}), \
                patch.object(remote, 'bucket', return_value=Mock()), \
                patch('sys.stderr', new_callable=io.StringIO) as errors:
            if save_error:
                with patch.object(remote, 'save', side_effect=FileExistsError(errno.EEXIST, 'PRIVATE_VALUE')):
                    result = remote.execute(self.config, self.context, self.progress)
            else:
                result = remote.execute(self.config, self.context, self.progress)
        return result, run, exported, errors.getvalue()

    def test_existing_metrics_never_start_build_or_append_and_still_cleanup(self):
        path = self.evidence / 'git-performance.jsonl'
        path.write_bytes(b'PRIVATE_PREEXISTING_DATA\n')
        result, run, exported, errors = self.exercise()
        self.assertEqual(result, 1)
        self.assertEqual(path.read_bytes(), b'PRIVATE_PREEXISTING_DATA\n')
        self.assertEqual(run.call_count, 1)
        self.assertIn('--cleanup', run.call_args.args[0])
        self.assertIsNone(exported.call_args.kwargs['git_performance_path'])
        self.assertFalse(exported.call_args.kwargs['complete_allowed'])
        final = json.loads(Path(self.config['status_path']).read_bytes())
        self.assertEqual(final['status'], 'FAILED')
        self.assertEqual(final['primary_failure']['error_type'], 'FileExistsError')
        self.assertEqual(final['primary_failure']['error_errno'], errno.EEXIST)
        self.assertIsInstance(final['primary_failure']['remote_source_line'], int)
        self.assertNotIn('PRIVATE', json.dumps(final) + errors)

    def test_status_write_failure_cannot_skip_cleanup_or_replace_primary_stage(self):
        result, run, exported, errors = self.exercise(save_error=True)
        self.assertEqual(result, 1)
        self.assertEqual(run.call_count, 1)
        self.assertIn('--cleanup', run.call_args.args[0])
        self.assertFalse(exported.call_args.kwargs['complete_allowed'])
        self.assertEqual(self.progress['failure']['failed_stage'], 'server-build')
        self.assertEqual(self.progress['failure']['error_errno'], errno.EEXIST)
        self.assertNotIn('PRIVATE_VALUE', errors)

    def test_competing_entry_never_mutates_owner_status_or_starts_execution(self):
        configuration = self.root / 'config.json'
        configuration.write_text(json.dumps(self.config), encoding='utf-8')
        status = Path(self.config['status_path'])
        status.write_bytes(b'OWNER_RUNNING\n')
        with patch.object(remote, 'execution_claim', side_effect=remote.ActiveExecution()), \
                patch.object(remote, 'execute') as execute, patch.object(remote, 'save') as save, \
                patch('sys.stderr', new_callable=io.StringIO):
            self.assertEqual(remote.main(['execute', '--config', str(configuration)]), 1)
        execute.assert_not_called()
        save.assert_not_called()
        self.assertEqual(status.read_bytes(), b'OWNER_RUNNING\n')

    def test_outer_failure_reports_actual_phase_and_no_exception_text(self):
        configuration = self.root / 'config.json'
        configuration.write_text(json.dumps(self.config), encoding='utf-8')
        @contextmanager
        def claim(config):
            yield self.context
        def fail(config, context, progress):
            progress['stage'] = 'cleanup'
            raise FileExistsError(errno.EEXIST, 'PRIVATE_VALUE', '/PRIVATE_PATH')
        with patch.object(remote, 'execution_claim', side_effect=claim), patch.object(remote, 'execute', side_effect=fail):
            self.assertEqual(remote.main(['execute', '--config', str(configuration)]), 1)
        value = json.loads(Path(self.config['status_path']).read_bytes())
        self.assertEqual(value['stage'], 'cleanup')
        self.assertEqual(value['primary_failure']['failed_stage'], 'cleanup')
        self.assertEqual(value['outer_failure']['error_errno'], errno.EEXIST)
        self.assertNotIn('PRIVATE', json.dumps(value))

    @unittest.skipUnless(sys.platform == 'linux', 'actual Linux flock required')
    def test_read_only_root_receipt_lock_excludes_second_entry_then_releases(self):
        path = self.root / 'receipt.json'
        raw = b'{"immutable":"receipt"}\n'
        path.write_bytes(raw)
        path.chmod(0o644)
        if os.geteuid() != 0:
            if shutil.which('sudo') is None:
                self.skipTest('fixture requires root ownership')
            result = subprocess.run(['sudo', '-n', 'chown', '0:0', str(path)], capture_output=True, timeout=10)
            if result.returncode:
                self.skipTest('fixture requires passwordless chown')
        context = {'campaign_id': self.config['campaign_id'], 'instance_id': self.config['instance_id'],
                   'execution_receipt_sha256': hashlib.sha256(raw).hexdigest()}
        config = self.config | {'execution_receipt': str(path)}
        with patch.dict(os.environ, {'MST2_EXECUTION_RECEIPT': str(path)}), \
                patch.object(execution, 'identity', return_value=context):
            with remote.execution_claim(config):
                with self.assertRaises(remote.ActiveExecution):
                    with remote.execution_claim(config):
                        self.fail('second entry acquired the live owner lock')
            with remote.execution_claim(config):
                pass


class DependencyPreparationTests(unittest.TestCase):
    def setUp(self):
        import direct_sources
        self.sources = direct_sources
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.workspace = self.root / 'workspace'
        self.receipt = self.workspace / direct_sources.DEPENDENCY_RECEIPT_FILE
        self.receipt.parent.mkdir(parents=True)
        self.receipt.write_text('{"revision":1,"dependencies":{}}', encoding='utf-8')
        self.cargo = self.root / 'cargo'
        self.cargo.mkdir()
        self.cargo_config = self.cargo / 'config.toml'
        self.cargo_config.write_text('[source.crates-io]\nreplace-with="rsproxy-sparse"\n'
            '[source.rsproxy-sparse]\nregistry="sparse+https://rsproxy.cn/index/"\n'
            '[http]\nmultiplexing=false\n', encoding='utf-8')
        for label in ('a', 'b'):
            path = self.workspace / ('client-' + label)
            path.mkdir()
            (path / 'Cargo.lock').write_bytes(b'fixed locked source\n')
        self.config = {'source': {'dependencies': {}}, 'baseline_sha': 'a' * 40, 'candidate_sha': 'b' * 40,
            'preflight_deadline_utc': (datetime.now(timezone.utc) + timedelta(minutes=10)).strftime('%Y-%m-%dT%H:%M:%SZ')}
        self.user = SimpleNamespace(pw_uid=123, pw_gid=456)

    def prepare(self, run):
        with patch.object(remote, 'DATA', self.root), \
                patch.object(self.sources, 'validate_dependency_receipt') as validate, \
                patch.object(remote.os, 'chown', create=True), \
                patch.object(budgets, 'run_process', side_effect=run) as process:
            result = remote.prepare_dependencies(self.config, self.workspace, self.user)
        return result, validate, process

    def test_actual_client_fetches_are_locked_and_use_only_verified_file_mirrors(self):
        import tomllib
        started = time.monotonic()
        result, validate, process = self.prepare(lambda *args, **kwargs: (0, b'', b''))
        self.assertEqual(process.call_count, 2)
        self.assertEqual(result['target'], 'x86_64-unknown-linux-gnu')
        self.assertEqual(set(result['clients']), {'a', 'b'})
        validate.assert_called_once()
        for call in process.call_args_list:
            argv = call.args[0]
            self.assertIn('--locked', argv)
            self.assertIn('fetch', argv)
            self.assertNotIn('build', argv)
            self.assertGreater(call.args[1], started)
            self.assertLessEqual(call.args[1], time.monotonic() + 300)
            self.assertTrue(call.kwargs['capture'])
        value = tomllib.loads(self.cargo_config.read_text(encoding='utf-8'))
        for label, identity in self.sources.DEPENDENCIES.items():
            self.assertEqual(value['source']['pinned-' + label],
                {'git': identity['url'], 'rev': identity['sha'], 'replace-with': 'local-' + label})
            self.assertEqual(value['source']['local-' + label],
                {'git': self.sources.dependency_repository(self.workspace, label).as_uri(), 'rev': identity['sha']})

    def test_lock_change_is_rejected_immediately(self):
        def mutate(*args, **kwargs):
            (self.workspace / 'client-a/Cargo.lock').write_bytes(b'changed')
            return 0, b'', b''
        with self.assertRaisesRegex(ValueError, 'CLIENT_LOCK_CHANGED'):
            self.prepare(mutate)

    def test_group_runner_uses_original_remaining_window_without_extension(self):
        self.config['preflight_deadline_utc'] = (datetime.now(timezone.utc) + timedelta(seconds=25)).strftime('%Y-%m-%dT%H:%M:%SZ')
        before = time.monotonic()
        _, _, process = self.prepare(lambda *args, **kwargs: (0, b'', b''))
        for call in process.call_args_list:
            self.assertGreater(call.args[1], before)
            self.assertLessEqual(call.args[1], before + 25)

    def test_group_timeout_aborts_without_starting_second_client(self):
        run = Mock(side_effect=TimeoutError('owned child exceeded its operation budget'))
        with self.assertRaises(TimeoutError):
            self.prepare(run)
        self.assertEqual(run.call_count, 1)

    def test_failed_fetch_aborts_without_starting_second_client(self):
        run = Mock(return_value=(1, b'', b'PRIVATE_CARGO_ERROR'))
        with self.assertRaisesRegex(RuntimeError, '^LOCKED_DEPENDENCY_FETCH_FAILED$'):
            self.prepare(run)
        self.assertEqual(run.call_count, 1)

    def test_expired_original_preflight_never_fetches(self):
        self.config['preflight_deadline_utc'] = '2026-01-01T00:00:00Z'
        run = Mock()
        with self.assertRaisesRegex(TimeoutError, 'ORIGINAL_PREFLIGHT_EXPIRED'):
            self.prepare(run)
        run.assert_not_called()

    def test_unexpected_cargo_configuration_is_rejected_before_fetch(self):
        self.cargo_config.write_text('[http]\nmultiplexing=true\n', encoding='utf-8')
        run = Mock()
        with self.assertRaisesRegex(ValueError, 'INVALID_CARGO_CONFIGURATION'):
            self.prepare(run)
        run.assert_not_called()


if __name__ == '__main__':
    unittest.main()
