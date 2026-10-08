"""Offline validation of runner installation; never register or start a runner."""

from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
from pathlib import Path
import subprocess
import stat
import tarfile
import tempfile
import unittest
from unittest.mock import Mock, patch

import install_runner as runner


START = datetime(2026, 10, 8, tzinfo=timezone.utc)
TOKEN = 'fixture_registration_secret_0123456789'


def payload():
    return {'revision': 1, 'campaign_id': 'r20261008-a001',
            'repository': 'gitmono-dev/scorpiofs', 'runner_label': 'scorpiofs-r20261008-a001',
            'register_token': TOKEN, 'runner_url': runner.RUNNER_URL,
            'runner_sha256': runner.RUNNER_SHA256,
            'session_started_utc': START.isoformat(),
            'session_deadline_utc': (START + timedelta(minutes=235)).isoformat(),
            'hard_release_utc': (START + timedelta(minutes=240)).isoformat()}


def ready():
    result = {key: value for key, value in payload().items() if key in
              ('campaign_id', 'runner_label', 'session_started_utc', 'session_deadline_utc', 'hard_release_utc')}
    result.update(revision=1, status='ready', data_root=str(runner.DATA_ROOT),
                  runner_arch='linux-x64', runner_user='benchmark', runner_uid=1001, runner_gid=1001,
                  disk_device='/dev/vdb', disk_uuid='12345678-1234-1234-1234-123456789abc',
                  disk_free_bytes=runner.MIN_FREE_BYTES, logical_cpus=8, memory_bytes=32 * 1024 ** 3)
    return result


def archive(path, entries=None):
    if entries is None:
        entries = [(name, tarfile.REGTYPE, b'fixture', 0o755) for name in
                   ('config.sh', 'run.sh', 'bin/Runner.Listener')]
    with tarfile.open(path, mode='w:gz') as output:
        for name, kind, body, mode in entries:
            info = tarfile.TarInfo(name)
            info.type, info.mode = kind, mode
            if kind == tarfile.REGTYPE:
                info.size = len(body)
            if kind in (tarfile.SYMTYPE, tarfile.LNKTYPE):
                info.linkname = body.decode()
            output.addfile(info, io.BytesIO(body) if kind == tarfile.REGTYPE else None)


class PayloadTests(unittest.TestCase):
    def parse(self, value):
        return runner.parse_payload(json.dumps(value).encode(), START + timedelta(minutes=1))

    def test_exact_release_repository_unique_label_and_original_window(self):
        self.assertEqual(self.parse(payload()), payload())

    def test_unknown_missing_duplicate_boolean_or_untrusted_inputs_reject(self):
        mutations = [('repository', 'someone/scorpiofs'), ('runner_label', 'self-hosted'),
                     ('revision', True), ('campaign_id', '../other'),
                     ('runner_sha256', '0' * 64), ('runner_url', 'https://example.invalid/runner'),
                     ('register_token', 'secret\n--replace'), ('extra', 'field')]
        for key, value in mutations:
            with self.subTest(key=key):
                candidate = payload()
                candidate[key] = value
                with self.assertRaises(runner.InstallError):
                    self.parse(candidate)
        candidate = payload()
        del candidate['register_token']
        with self.assertRaises(runner.InstallError):
            self.parse(candidate)
        duplicate = json.dumps(payload())[:-1] + ',"revision":1}'
        with self.assertRaisesRegex(runner.InstallError, 'duplicate_input_field'):
            runner.parse_payload(duplicate.encode(), START)

    def test_window_cannot_be_extended_restarted_or_admitted_late(self):
        for key in ('session_deadline_utc', 'hard_release_utc'):
            candidate = payload()
            candidate[key] = (runner.utc(candidate[key]) + timedelta(seconds=1)).isoformat()
            with self.assertRaisesRegex(runner.InstallError, 'invalid_window'):
                self.parse(candidate)
        for now in (START - timedelta(seconds=1), START + timedelta(minutes=15)):
            with self.assertRaisesRegex(runner.InstallError, 'preflight_window_expired'):
                runner.parse_payload(json.dumps(payload()).encode(), now)

    def test_json_size_encoding_and_timezone_reject_without_echoing_input(self):
        for raw in (b'', b'x' * (runner.MAX_INPUT + 1), b'\xff', b'{'):
            with self.assertRaises(runner.InstallError) as error:
                runner.parse_payload(raw, START)
            if raw:
                self.assertNotIn(raw.decode(errors='replace'), str(error.exception))
        candidate = payload()
        candidate['session_started_utc'] = '2026-10-08T00:00:00'
        with self.assertRaisesRegex(runner.InstallError, 'invalid_window'):
            self.parse(candidate)

    def test_bootstrap_receipt_binds_campaign_arch_user_disk_and_all_deadlines(self):
        self.assertIs(runner.validate_ready(payload(), ready()).get('revision'), 1)
        for key, value in [('campaign_id', 'another-run'), ('runner_label', 'self-hosted'),
                           ('runner_arch', 'linux-arm64'), ('runner_user', 'root'),
                           ('data_root', '/tmp'), ('runner_uid', True),
                           ('disk_device', '/dev/../../etc/passwd'), ('disk_uuid', '../bad'),
                           ('disk_free_bytes', runner.MIN_FREE_BYTES - 1),
                           ('logical_cpus', 4), ('memory_bytes', 16 * 1024 ** 3),
                           ('hard_release_utc', '2030-01-01T00:00:00Z')]:
            with self.subTest(key=key):
                candidate = ready()
                candidate[key] = value
                with self.assertRaises(runner.InstallError):
                    runner.validate_ready(payload(), candidate)


class ArchiveTests(unittest.TestCase):
    def test_valid_archive_extracts_required_executable_files(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            tar = root / 'runner.tar.gz'
            archive(tar)
            destination = root / 'runner'
            destination.mkdir()
            runner.safe_extract(tar, destination)
            self.assertEqual((destination / 'bin/Runner.Listener').read_bytes(), b'fixture')
            self.assertEqual((destination / 'config.sh').read_bytes(), b'fixture')

    def test_traversal_absolute_links_and_special_members_reject_before_any_extract(self):
        members = [('../escape', tarfile.REGTYPE), ('/absolute', tarfile.REGTYPE),
                   ('dir/../../escape', tarfile.REGTYPE), ('dir\\escape', tarfile.REGTYPE),
                   ('C:/escape', tarfile.REGTYPE), ('link', tarfile.SYMTYPE),
                   ('hard', tarfile.LNKTYPE), ('fifo', tarfile.FIFOTYPE), ('device', tarfile.CHRTYPE)]
        for name, kind in members:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                destination = root / 'runner'
                destination.mkdir()
                tar = root / 'runner.tar.gz'
                archive(tar, [('config.sh', tarfile.REGTYPE, b'ok', 0o755), (name, kind, b'bad', 0o755)])
                with self.assertRaises(runner.InstallError):
                    runner.safe_extract(tar, destination)
                self.assertEqual(list(destination.iterdir()), [])
                self.assertFalse((root / 'escape').exists())

    def test_duplicates_missing_entrypoints_and_size_limits_reject(self):
        for entries, limit in [
                ([('config.sh', tarfile.REGTYPE, b'a', 0o755)] * 2, runner.MAX_EXTRACTED),
                ([('other', tarfile.REGTYPE, b'a', 0o755)], runner.MAX_EXTRACTED),
                ([('other', tarfile.REGTYPE, b'abc', 0o755)], 1)]:
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                destination = root / 'runner'
                destination.mkdir()
                tar = root / 'runner.tar.gz'
                archive(tar, entries)
                with patch.object(runner, 'MAX_EXTRACTED', limit), self.assertRaises(runner.InstallError):
                    runner.safe_extract(tar, destination)
                self.assertEqual(list(destination.iterdir()), [])

    def test_existing_directory_contents_are_never_reused(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'existing').write_bytes(b'keep')
            with self.assertRaisesRegex(runner.InstallError, 'runner_directory_not_empty'):
                runner.safe_extract(root / 'absent', root)
            self.assertEqual((root / 'existing').read_bytes(), b'keep')

    def test_entrypoints_must_be_regular_and_executable_in_the_archive(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            destination = root / 'runner'
            destination.mkdir()
            tar = root / 'runner.tar.gz'
            archive(tar, [(name, tarfile.REGTYPE, b'fixture', 0o644) for name in
                          ('config.sh', 'run.sh', 'bin/Runner.Listener')])
            with self.assertRaisesRegex(runner.InstallError, 'runner_archive_missing_entrypoint'):
                runner.safe_extract(tar, destination)
            self.assertEqual(list(destination.iterdir()), [])

    def test_only_pinned_node_symlinks_to_regular_archive_targets_are_allowed(self):
        for link, target in runner.RUNNER_LINKS.items():
            with self.subTest(link=link), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                destination = root / 'runner'
                destination.mkdir()
                tar = root / 'runner.tar.gz'
                target_path = (Path(link).parent.parent / target.removeprefix('../')).as_posix()
                base = [(name, tarfile.REGTYPE, b'fixture', 0o755) for name in
                        ('config.sh', 'run.sh', 'bin/Runner.Listener')]
                archive(tar, base + [(Path(link).parent.as_posix(), tarfile.DIRTYPE, b'', 0o755),
                                     (target_path, tarfile.REGTYPE, b'javascript', 0o755),
                                     (link, tarfile.SYMTYPE, target.encode(), 0o777)])
                with ExitStack() as stack:
                    # Native Linux CI verifies link creation and resolution;
                    # Windows filesystems may not support unprivileged links.
                    create = (stack.enter_context(patch.object(runner.os, 'symlink'))
                              if runner.os.name != 'posix' else None)
                    runner.safe_extract(tar, destination)
                if create is None:
                    self.assertTrue((destination / link).is_symlink())
                    self.assertEqual(runner.os.readlink(destination / link), target)
                    self.assertEqual((destination / link).read_bytes(), b'javascript')
                else:
                    create.assert_called_once_with(target, destination / link)
                self.assertEqual((destination / target_path).read_bytes(), b'javascript')

    def test_pinned_symlink_wrong_or_missing_target_rejects_before_extract(self):
        link, expected = next(iter(runner.RUNNER_LINKS.items()))
        for target in (expected, '../../outside'):
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                destination = root / 'runner'
                destination.mkdir()
                tar = root / 'runner.tar.gz'
                base = [(name, tarfile.REGTYPE, b'fixture', 0o755) for name in
                        ('config.sh', 'run.sh', 'bin/Runner.Listener')]
                archive(tar, base + [(link, tarfile.SYMTYPE, target.encode(), 0o777)])
                with self.assertRaisesRegex(runner.InstallError, 'unsafe_runner_archive'):
                    runner.safe_extract(tar, destination)
                self.assertEqual(list(destination.iterdir()), [])


class DownloadTests(unittest.TestCase):
    def response(self, content, url='https://release-assets.githubusercontent.com/pinned'):
        result = Mock()
        result.__enter__ = Mock(return_value=result)
        result.__exit__ = Mock(return_value=False)
        result.geturl.return_value = url
        result.read.side_effect = [content, b'']
        return result

    def test_download_checks_digest_and_uses_https_official_origin(self):
        candidate = payload()
        candidate['runner_sha256'] = hashlib.sha256(b'pinned archive').hexdigest()
        with tempfile.TemporaryDirectory() as temp, \
                patch.object(runner, 'remaining', return_value=60), \
                patch.object(runner, 'urlopen', return_value=self.response(b'pinned archive')) as fetch:
            path = Path(temp) / 'download'
            runner.download_archive(candidate, path)
            self.assertEqual(path.read_bytes(), b'pinned archive')
            fetch.assert_called_once_with(runner.RUNNER_URL, timeout=60)

    def test_mismatch_redirect_and_size_bound_never_reach_extraction(self):
        for url, content, limit in [('https://example.invalid/evil', b'wrong', runner.MAX_ARCHIVE),
                                    ('http://github.com/evil', b'wrong', runner.MAX_ARCHIVE),
                                    (runner.RUNNER_URL, b'wrong', runner.MAX_ARCHIVE),
                                    (runner.RUNNER_URL, b'large', 1)]:
            with tempfile.TemporaryDirectory() as temp, patch.object(runner, 'remaining', return_value=60), \
                    patch.object(runner, 'urlopen', return_value=self.response(content, url)), \
                    patch.object(runner, 'MAX_ARCHIVE', limit), self.assertRaises(runner.InstallError):
                runner.download_archive(payload(), Path(temp) / 'download')


class CommandTests(unittest.TestCase):
    def test_config_failure_and_timeout_never_expose_token_or_child_output(self):
        for result in (1, subprocess.TimeoutExpired(['config.sh'], 1, output=TOKEN)):
            with patch.object(runner, 'remaining', return_value=1), \
                    patch.object(runner.os, 'killpg', create=True) as kill_group, \
                    patch.object(runner.signal, 'SIGKILL', 9, create=True), \
                    patch.object(runner.subprocess, 'Popen') as execute:
                child = Mock(pid=123, returncode=None)
                execute.return_value = child
                if isinstance(result, Exception):
                    child.wait.side_effect = [result, 0]
                else:
                    child.wait.return_value = result
                with self.assertRaisesRegex(runner.InstallError, '^runner_command_failed$') as error:
                    runner.private_command(['config.sh'], payload(), {'ACTIONS_RUNNER_INPUT_TOKEN': TOKEN})
                self.assertNotIn(TOKEN, str(error.exception))
                self.assertIs(execute.call_args.kwargs['stdout'], subprocess.DEVNULL)
                self.assertIs(execute.call_args.kwargs['stderr'], subprocess.DEVNULL)
                self.assertTrue(execute.call_args.kwargs['start_new_session'])
                if isinstance(result, Exception):
                    kill_group.assert_called_once_with(123, 9)
                    self.assertEqual(child.wait.call_args_list[-1].kwargs, {'timeout': 5})
                else:
                    kill_group.assert_not_called()

    def test_installer_builds_exact_ephemeral_flags_and_absolute_stop_timer(self):
        with tempfile.TemporaryDirectory() as temp, ExitStack() as stack:
            data = Path(temp) / 'data'
            data.mkdir()
            root = data / 'runner'
            units = Path(temp) / 'units'
            units.mkdir()
            for name, value in [('DATA_ROOT', data), ('RUNNER_ROOT', root), ('SYSTEMD_ROOT', units)]:
                stack.enter_context(patch.object(runner, name, value))
            account = ready()
            stack.enter_context(patch.object(runner, 'verify_host', return_value=account))
            stack.enter_context(patch.object(runner, 'remaining', return_value=60))
            stack.enter_context(patch.object(runner.time, 'time', return_value=START.timestamp() + 60))
            stack.enter_context(patch.object(runner.os, 'chown', create=True))
            def command(argv, _payload, _environment=None):
                if '--unattended' in argv:
                    self.assertTrue((units / runner.STOP_TIMER).is_file())
                    self.assertTrue((units / runner.STOP_SERVICE).is_file())
                    self.assertFalse((units / runner.RUNNER_SERVICE).exists())
            commands = stack.enter_context(patch.object(runner, 'private_command', side_effect=command))
            def download(_payload, path):
                archive(path)
            stack.enter_context(patch.object(runner, 'download_archive', side_effect=download))
            result = runner.install(payload())
            configuration, supplied, environment = next(call.args for call in commands.call_args_list
                                                        if '--unattended' in call.args[0])
            self.assertEqual(configuration[:4], ['/usr/sbin/runuser', '-u', 'benchmark', '--'])
            for flag in ('--unattended', '--ephemeral', '--disableupdate', '--no-default-labels'):
                self.assertIn(flag, configuration)
            self.assertNotIn('--replace', configuration)
            self.assertEqual(configuration[configuration.index('--labels') + 1], payload()['runner_label'])
            self.assertNotIn('--token', configuration)
            self.assertNotIn(TOKEN, configuration)
            self.assertEqual(environment['ACTIONS_RUNNER_INPUT_TOKEN'], TOKEN)
            self.assertEqual(configuration[configuration.index('--url') + 1], 'https://github.com/gitmono-dev/scorpiofs')
            self.assertEqual(supplied, payload())
            self.assertFalse(any(name.startswith('GITHUB_') for name in environment))
            for name in ('CARGO_HOME', 'RUSTUP_HOME', 'TMPDIR'):
                self.assertTrue(Path(environment[name]).is_relative_to(data))
            self.assertTrue(environment['PATH'].startswith(str(data / 'cargo/bin') + ':'))
            service = (units / runner.RUNNER_SERVICE).read_text()
            self.assertIn('User=benchmark\nGroup=benchmark', service)
            self.assertIn('HOME=' + str(data / 'runner-home'), service)
            self.assertIn('ExecStart=' + str(root / 'run.sh'), service)
            self.assertIn('KillMode=control-group', service)
            timer = (units / runner.STOP_TIMER).read_text()
            self.assertIn('OnCalendar=2026-10-08 03:55:00 UTC', timer)
            self.assertIn('Persistent=true', timer)
            self.assertEqual(commands.call_args_list[-2].args[0], ['/usr/bin/systemctl', 'start', runner.RUNNER_SERVICE])
            for path in data.rglob('*'):
                if path.is_file():
                    self.assertNotIn(TOKEN.encode(), path.read_bytes())
            for path in units.iterdir():
                self.assertNotIn(TOKEN, path.read_text())
            self.assertNotIn(TOKEN, json.dumps(result))

    def test_bootstrap_path_rejects_links_wrong_owner_and_writable_receipts(self):
        for mode, uid in [(stat.S_IFLNK | 0o777, 0), (stat.S_IFREG | 0o666, 0),
                          (stat.S_IFREG | 0o644, 1001)]:
            path = Mock()
            path.lstat.return_value = Mock(st_mode=mode, st_uid=uid)
            with self.assertRaisesRegex(runner.InstallError, 'untrusted_bootstrap_path'):
                runner.secure_root_path(path)

    def test_non_linux_host_rejects_before_any_install_effect(self):
        with patch.object(runner.sys, 'platform', 'win32'):
            with self.assertRaisesRegex(runner.InstallError, 'unsupported_install_host'):
                runner.verify_host(payload())

    def test_elapsed_preflight_does_not_start_a_configuration_child(self):
        with patch.object(runner, 'remaining', side_effect=runner.InstallError('preflight_window_expired')), \
                patch.object(runner.subprocess, 'Popen') as child:
            with self.assertRaisesRegex(runner.InstallError, 'runner_command_failed'):
                runner.private_command(['config.sh'], payload())
            child.assert_not_called()

    def test_existing_runner_or_units_fail_before_download_or_registration(self):
        for existing in ('runner', 'unit'):
            with tempfile.TemporaryDirectory() as temp, ExitStack() as stack:
                data, units = Path(temp) / 'data', Path(temp) / 'units'
                data.mkdir()
                units.mkdir()
                root = data / 'runner'
                if existing == 'runner':
                    root.mkdir()
                else:
                    (units / runner.STOP_TIMER).write_text('owned elsewhere')
                for name, value in [('DATA_ROOT', data), ('RUNNER_ROOT', root), ('SYSTEMD_ROOT', units)]:
                    stack.enter_context(patch.object(runner, name, value))
                stack.enter_context(patch.object(runner, 'verify_host', return_value=ready()))
                stack.enter_context(patch.object(runner, 'remaining', return_value=60))
                download = stack.enter_context(patch.object(runner, 'download_archive'))
                execute = stack.enter_context(patch.object(runner, 'private_command'))
                with self.assertRaises(runner.InstallError):
                    runner.install(payload())
                download.assert_not_called()
                execute.assert_not_called()

    def test_main_reports_closed_error_even_when_unexpected_exception_contains_secret(self):
        input_stream = Mock(buffer=io.BytesIO(json.dumps(payload()).encode()))
        for error in (runner.InstallError('runner_command_failed'), RuntimeError(TOKEN)):
            input_stream.buffer.seek(0)
            with patch.object(runner.sys, 'stdin', input_stream), \
                    patch.object(runner, 'parse_payload', return_value=payload()), \
                    patch.object(runner, 'install', side_effect=error), \
                    patch.object(runner.sys, 'stdout', new_callable=io.StringIO) as output, \
                    patch.object(runner.sys, 'stderr', new_callable=io.StringIO) as errors:
                self.assertEqual(runner.main(), 1)
                self.assertEqual(output.getvalue(), '')
                self.assertNotIn(TOKEN, errors.getvalue())
                self.assertEqual(json.loads(errors.getvalue())['status'], 'failed')


if __name__ == '__main__':
    unittest.main()
