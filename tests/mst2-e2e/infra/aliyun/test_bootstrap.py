"""Execute the maintenance gate with real Bash and isolated host command doubles."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import direct_remote as remote


class MaintenanceBootstrapTests(unittest.TestCase):
    units = ('apt-daily.timer', 'apt-daily-upgrade.timer',
             'apt-daily.service', 'apt-daily-upgrade.service')

    @classmethod
    def setUpClass(cls):
        git_bash = Path('C:/Program Files/Git/bin/bash.exe')
        cls.bash = str(git_bash) if os.name == 'nt' and git_bash.is_file() else shutil.which('bash')
        if not cls.bash:
            raise RuntimeError('Bash is required to exercise the bootstrap gate')

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.bin = self.root / 'bin'
        self.state = self.root / 'state'
        self.bin.mkdir()
        self.state.mkdir()
        self.log = self.root / 'commands.log'
        self.env = {**os.environ, 'PATH': str(self.bin) + os.pathsep + os.environ['PATH'],
                    'FAKE_STATE_ROOT': self.state.as_posix(), 'FAKE_COMMAND_LOG': self.log.as_posix()}
        for unit in self.units:
            (self.state / (unit + '.enabled')).write_text('enabled')
            (self.state / (unit + '.active')).write_text('active' if unit.endswith('.timer') else 'inactive')
        self.command('systemctl', '''
printf '%s\\n' "$*" >> "$FAKE_COMMAND_LOG"
action=$1; shift
case "$action" in
  stop)
    test "${FAKE_STOP_FAILURE:-0}" = 0 || exit 1
    for unit in "$@"; do
      case "$unit" in *.timer) ;; *) exit 90 ;; esac
      printf inactive > "$FAKE_STATE_ROOT/$unit.active"
    done ;;
  mask)
    for unit in "$@"; do
      test "${FAKE_MASK_IGNORED:-}" = "$unit" || printf masked > "$FAKE_STATE_ROOT/$unit.enabled"
    done ;;
  show)
    property=$1; unit=$3
    test "${FAKE_QUERY_FAILURE:-}" != "$unit" || exit 1
    case "$property" in
      --property=UnitFileState) cat "$FAKE_STATE_ROOT/$unit.enabled" ;;
      --property=ActiveState) cat "$FAKE_STATE_ROOT/$unit.active" ;;
      *) exit 91 ;;
    esac ;;
  *) exit 92 ;;
esac
''')
        self.command('apt-config', '''
test "$1" = shell && test "$2" = value || exit 93
case "$3" in
  APT::Periodic::"${FAKE_APT_MISSING:-absent}") exit 0 ;;
  APT::Periodic::"${FAKE_APT_NONZERO:-absent}") printf "value='1'\\n" ;;
  *) printf "value='0'\\n" ;;
esac
''')
        self.command('apt-get', '''
test "$NEEDRESTART_MODE" = l || exit 94
printf 'apt-get %s\\n' "$*" >> "$FAKE_COMMAND_LOG"
''')
        self.command('install', '''
printf 'install %s\\n' "$*" >> "$FAKE_COMMAND_LOG"
/usr/bin/install "$@"
''')
        template = (Path(__file__).parent / 'terraform/bootstrap.sh.tftpl').read_text(encoding='utf-8')
        block = template[template.index('readonly MAINTENANCE_UNITS='):template.index('for program in lsblk')]
        block = block.replace('$${', '${').replace('/etc/apt', self.root.as_posix() + '/etc/apt')
        block = block.replace('/etc/needrestart', self.root.as_posix() + '/etc/needrestart')
        self.script = ('set -Eeuo pipefail\n'
                       'fail() { printf "refused: %s\\n" "$1" >&2; exit 1; }\n'
                       + block + '\napt-get update -y\n')

    def command(self, name, body):
        path = self.bin / name
        path.write_text('#!/usr/bin/env bash\nset -euo pipefail\n' + body, encoding='utf-8', newline='\n')
        path.chmod(0o755)

    def run_gate(self, *arguments, **environment):
        return subprocess.run([self.bash, '-s', '--', *arguments], input=self.script,
                              text=True, capture_output=True, cwd=self.root,
                              env={**self.env, **environment}, timeout=10)

    def refuse(self, result, reason):
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(reason, result.stderr)
        self.assertNotIn('apt-get', self.log.read_text() if self.log.exists() else '')

    def test_isolation_stops_only_timers_masks_all_units_and_precedes_apt(self):
        result = self.run_gate()
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.log.read_text().splitlines()
        self.assertEqual(calls[0], 'stop apt-daily.timer apt-daily-upgrade.timer')
        self.assertEqual(calls[1], 'mask ' + ' '.join(self.units))
        self.assertEqual(calls[-1], 'apt-get update -y')
        self.assertEqual([line for line in calls if line.startswith('stop ')], [calls[0]])

    def test_running_dpkg_owner_is_never_stopped_or_admitted(self):
        for unit in self.units[2:]:
            with self.subTest(unit=unit):
                (self.state / (unit + '.active')).write_text('active')
                self.refuse(self.run_gate(), 'maintenance_unit_not_quiescent')
                self.assertNotIn('stop ' + unit, self.log.read_text())
                (self.state / (unit + '.active')).write_text('inactive')

    def test_transitional_or_unknown_unit_state_is_rejected(self):
        result = self.run_gate()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.log.unlink()
        for index, value in enumerate(('active', 'activating', 'deactivating', 'reloading', 'unknown', '')):
            with self.subTest(state=value):
                path = self.state / (self.units[index % len(self.units)] + '.active')
                path.write_text(value)
                self.refuse(self.run_gate('--check-maintenance-isolation'), 'maintenance_unit_not_quiescent')
                path.write_text('inactive')

    def test_ineffective_mask_or_failed_query_is_rejected(self):
        for unit in self.units:
            with self.subTest(unit=unit):
                (self.state / (unit + '.enabled')).write_text('enabled')
                self.refuse(self.run_gate(FAKE_MASK_IGNORED=unit), 'maintenance_unit_not_masked')
        self.refuse(self.run_gate(FAKE_QUERY_FAILURE=self.units[-1]), 'maintenance_unit_not_masked')

    def test_timer_stop_failure_never_reaches_apt(self):
        result = self.run_gate(FAKE_STOP_FAILURE='1')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.log.read_text().strip(), 'stop apt-daily.timer apt-daily-upgrade.timer')

    def test_missing_or_enabled_effective_apt_policy_is_rejected(self):
        self.refuse(self.run_gate(FAKE_APT_NONZERO='Unattended-Upgrade'), 'apt_periodic_policy_not_disabled')
        self.refuse(self.run_gate(FAKE_APT_MISSING='Enable'), 'apt_periodic_policy_not_disabled')

    def test_effective_needrestart_override_or_syntax_error_is_rejected(self):
        config = self.root / 'etc/needrestart/conf.d/zz-later.conf'
        config.parent.mkdir(parents=True)
        for content in ("$nrconf{restart} = 'a';\n", 'invalid perl syntax !\n'):
            with self.subTest(content=content):
                config.write_text(content)
                self.refuse(self.run_gate(), 'needrestart_policy_not_list_only')

    def test_readonly_gate_accepts_failed_masked_unit_and_rejects_drift(self):
        result = self.run_gate()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.log.unlink()
        (self.state / (self.units[-1] + '.active')).write_text('failed')
        result = self.run_gate('--check-maintenance-isolation')
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.log.read_text()
        self.assertNotIn('stop ', calls)
        self.assertNotIn('mask ', calls)
        self.assertNotIn('apt-get', calls)
        self.assertNotIn('install ', calls)
        self.log.unlink()
        (self.state / (self.units[-1] + '.enabled')).write_text('enabled')
        self.refuse(self.run_gate('--check-maintenance-isolation'), 'maintenance_unit_not_masked')

    def test_unknown_argument_cannot_reconfigure_host(self):
        self.refuse(self.run_gate('--unknown'), 'unsupported_bootstrap_argument')
        self.assertFalse(self.log.exists())


class MaintenanceInstallTests(unittest.TestCase):
    def setUp(self):
        self.config = {'bootstrap_ready_path': '/ready', 'campaign_id': 'v3-' + 'a' * 20,
                       'profile': 'medium', 'session_started_utc': '2026-10-08T00:00:00Z',
                       'session_deadline_utc': '2026-10-08T03:55:00Z', 'hard_release_utc': '2026-10-08T04:00:00Z'}
        self.ready = {'revision': 2, 'status': 'ready', 'campaign_id': self.config['campaign_id'],
                      'test_tier': 'medium', 'execution_mode': 'cloud-assistant',
                      **{key: self.config[key] for key in ('session_started_utc', 'session_deadline_utc', 'hard_release_utc')}}

    def test_current_host_gate_precedes_metadata_or_execution_receipt(self):
        with patch.object(remote.os, 'getuid', create=True, return_value=0), \
                patch.object(Path, 'read_bytes', return_value=json.dumps(self.ready).encode()), \
                patch.object(remote.subprocess, 'run') as run, \
                patch.object(remote, 'metadata', side_effect=RuntimeError('after gate')) as metadata, \
                patch.object(remote, 'save') as save:
            with self.assertRaisesRegex(RuntimeError, 'after gate'):
                remote.install(self.config)
        run.assert_called_once_with(['/usr/local/sbin/scorpiofs-benchmark-bootstrap',
                                     '--check-maintenance-isolation'], check=True, timeout=10)
        metadata.assert_called_once()
        save.assert_not_called()

    def test_failed_or_timed_out_gate_has_no_execution_side_effect(self):
        for error in (subprocess.CalledProcessError(1, ['gate']), subprocess.TimeoutExpired(['gate'], 10)):
            with self.subTest(error=type(error).__name__), \
                    patch.object(remote.os, 'getuid', create=True, return_value=0), \
                    patch.object(Path, 'read_bytes', return_value=json.dumps(self.ready).encode()), \
                    patch.object(remote.subprocess, 'run', side_effect=error), \
                    patch.object(remote, 'metadata') as metadata, patch.object(remote, 'save') as save:
                with self.assertRaises(type(error)):
                    remote.install(self.config)
                metadata.assert_not_called()
                save.assert_not_called()


if __name__ == '__main__':
    unittest.main()
