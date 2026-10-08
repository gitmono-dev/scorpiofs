"""Install one pinned ephemeral Actions runner from a private stdin payload."""

from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
from urllib.parse import urlsplit
from urllib.request import urlopen

try:
    import pwd
except ImportError:  # Pure validation/extraction tests also run on Windows.
    pwd = None


DATA_ROOT = Path('/srv/scorpiofs-benchmark')
RUNNER_ROOT = DATA_ROOT / 'runner'
READY_PATH = Path('/var/lib/scorpiofs-benchmark/bootstrap-ready.json')
SYSTEMD_ROOT = Path('/etc/systemd/system')
RUNNER_SERVICE = 'scorpiofs-benchmark-runner.service'
STOP_SERVICE = 'scorpiofs-benchmark-stop.service'
STOP_TIMER = 'scorpiofs-benchmark-stop.timer'
RUNNER_VERSION = '2.338.0'
RUNNER_URL = ('https://github.com/actions/runner/releases/download/v2.338.0/'
              'actions-runner-linux-x64-2.338.0.tar.gz')
RUNNER_SHA256 = 'af4b794c1bc41d73d40535e3fe092a39f9679cd8d965954c2aca25a05ca41d32'
MIN_FREE_BYTES = 116412113924
MAX_INPUT = 32 * 1024
MAX_ARCHIVE = 512 * 1024 * 1024
MAX_EXTRACTED = 2 * 1024 * 1024 * 1024
RUNNER_LINKS = {f'externals/node{version}/bin/{name}': target
                for version in (20, 24) for name, target in
                [('corepack', '../lib/node_modules/corepack/dist/corepack.js'),
                 ('npx', '../lib/node_modules/npm/bin/npx-cli.js'),
                 ('npm', '../lib/node_modules/npm/bin/npm-cli.js')]}
FIELDS = frozenset({'revision', 'campaign_id', 'repository', 'runner_label',
                    'register_token', 'runner_url', 'runner_sha256',
                    'session_started_utc', 'session_deadline_utc', 'hard_release_utc'})


class InstallError(Exception):
    """Only closed codes leave this process; child argv and payload never do."""


def require(condition, code):
    if not condition:
        raise InstallError(code)


def utc(value):
    require(type(value) is str and len(value) <= 40, 'invalid_window')
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        raise InstallError('invalid_window') from None
    require(result.tzinfo is not None and result.utcoffset() == timedelta(0), 'invalid_window')
    return result


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate_input_field')
        result[key] = value
    return result


def parse_payload(raw, now=None):
    require(type(raw) is bytes and 0 < len(raw) <= MAX_INPUT, 'invalid_input_size')
    try:
        payload = json.loads(raw.decode('utf8'), object_pairs_hook=unique_object)
    except (ValueError, UnicodeError):
        raise InstallError('invalid_json') from None
    require(type(payload) is dict and set(payload) == FIELDS, 'invalid_input_fields')
    require(type(payload['revision']) is int and payload['revision'] == 1, 'invalid_revision')
    campaign = payload['campaign_id']
    require(type(campaign) is str and re.fullmatch(r'[a-z][a-z0-9-]{7,47}', campaign), 'invalid_campaign')
    require(payload['repository'] == 'gitmono-dev/scorpiofs', 'invalid_repository')
    require(payload['runner_label'] == 'scorpiofs-' + campaign, 'invalid_runner_label')
    require(payload['runner_url'] == RUNNER_URL and payload['runner_sha256'] == RUNNER_SHA256,
            'untrusted_runner_release')
    token = payload['register_token']
    require(type(token) is str and re.fullmatch(r'[A-Za-z0-9_-]{20,256}', token), 'invalid_registration_token')
    started, deadline, release = [utc(payload[key]) for key in
        ('session_started_utc', 'session_deadline_utc', 'hard_release_utc')]
    require(deadline - started == timedelta(minutes=235)
            and release - started == timedelta(minutes=240), 'invalid_window')
    now = datetime.now(timezone.utc) if now is None else now
    require(started <= now < started + timedelta(minutes=15), 'preflight_window_expired')
    return payload


def validate_ready(payload, ready):
    require(type(ready) is dict, 'invalid_bootstrap_receipt')
    expected = {'revision': 1, 'status': 'ready', 'data_root': str(DATA_ROOT),
                'runner_arch': 'linux-x64', 'runner_user': 'benchmark'}
    for key in ('campaign_id', 'runner_label', 'session_started_utc',
                'session_deadline_utc', 'hard_release_utc'):
        expected[key] = payload[key]
    require(all(ready.get(key) == value and type(ready.get(key)) is type(value)
                for key, value in expected.items()), 'bootstrap_binding_mismatch')
    for key in ('runner_uid', 'runner_gid', 'disk_free_bytes', 'logical_cpus', 'memory_bytes'):
        require(type(ready.get(key)) is int and ready[key] > 0, 'invalid_bootstrap_receipt')
    require(ready['disk_free_bytes'] >= MIN_FREE_BYTES, 'insufficient_data_disk')
    require(ready['logical_cpus'] >= 8 and ready['memory_bytes'] >= 30 * 1024 ** 3,
            'insufficient_runner_capacity')
    require(type(ready.get('disk_uuid')) is str
            and re.fullmatch(r'[0-9a-fA-F-]{16,64}', ready['disk_uuid']), 'invalid_disk_identity')
    require(type(ready.get('disk_device')) is str
            and re.fullmatch(r'/dev/[A-Za-z0-9/_-]{1,100}', ready['disk_device']), 'invalid_disk_identity')
    return ready


def secure_root_path(path, directory=False):
    info = path.lstat()
    require(info.st_uid == 0 and not info.st_mode & 0o022
            and (stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)),
            'untrusted_bootstrap_path')
    return info


def verify_host(payload):
    require(sys.platform == 'linux' and platform.machine().lower() in ('x86_64', 'amd64')
            and os.geteuid() == 0 and pwd is not None, 'unsupported_install_host')
    for path in (Path('/var'), Path('/var/lib'), READY_PATH.parent, Path('/srv'), DATA_ROOT):
        secure_root_path(path, directory=True)
    secure_root_path(READY_PATH)
    descriptor = os.open(READY_PATH, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        with os.fdopen(descriptor, 'rb') as stream:
            raw = stream.read(MAX_INPUT + 1)
        require(len(raw) <= MAX_INPUT, 'invalid_bootstrap_receipt')
        ready = validate_ready(payload, json.loads(raw.decode('utf8'), object_pairs_hook=unique_object))
    except (ValueError, UnicodeError):
        raise InstallError('invalid_bootstrap_receipt') from None
    account = pwd.getpwnam('benchmark')
    require((account.pw_uid, account.pw_gid) == (ready['runner_uid'], ready['runner_gid']),
            'runner_account_mismatch')
    require(account.pw_dir == str(DATA_ROOT / 'runner-home'), 'runner_account_mismatch')
    require(os.path.ismount(DATA_ROOT), 'data_disk_not_mounted')
    device = Path(ready['disk_device']).resolve(strict=True)
    by_uuid = Path('/dev/disk/by-uuid') / ready['disk_uuid']
    require(by_uuid.resolve(strict=True) == device and stat.S_ISBLK(device.stat().st_mode)
            and device.stat().st_rdev == DATA_ROOT.stat().st_dev, 'data_disk_identity_mismatch')
    require(shutil.disk_usage(DATA_ROOT).free >= MIN_FREE_BYTES, 'insufficient_data_disk')
    memory = re.search(r'^MemTotal:\s+([0-9]+)\s+kB$', Path('/proc/meminfo').read_text(), re.M)
    require(memory is not None and int(memory.group(1)) * 1024 == ready['memory_bytes']
            and os.cpu_count() == ready['logical_cpus'], 'runner_capacity_mismatch')
    return ready


def remaining(payload):
    seconds = utc(payload['session_started_utc']).timestamp() + 15 * 60 - time.time()
    require(seconds > 0, 'preflight_window_expired')
    return seconds


def download_archive(payload, destination):
    digest = hashlib.sha256()
    total = 0
    with urlopen(payload['runner_url'], timeout=min(60, remaining(payload))) as response:
        final = urlsplit(response.geturl())
        require(final.scheme == 'https' and final.hostname in
                ('github.com', 'release-assets.githubusercontent.com', 'objects.githubusercontent.com')
                and not final.username and not final.password, 'untrusted_download_redirect')
        with destination.open('xb') as output:
            while True:
                remaining(payload)
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                require(total <= MAX_ARCHIVE, 'runner_archive_too_large')
                digest.update(chunk)
                output.write(chunk)
    require(digest.hexdigest() == payload['runner_sha256'], 'runner_checksum_mismatch')


def safe_extract(archive_path, destination):
    require(destination.is_dir() and not destination.is_symlink()
            and not any(destination.iterdir()), 'runner_directory_not_empty')
    with tarfile.open(archive_path, mode='r:gz') as archive:
        members, paths, total, count = [], set(), 0, 0
        for member in archive:
            count += 1
            require(count <= 100000 and len(member.name) <= 4096, 'unsafe_runner_archive')
            path = PurePosixPath(member.name)
            require(not path.is_absolute() and '..' not in path.parts and '\\' not in member.name
                    and ':' not in member.name and '\x00' not in member.name
                    and (member.isfile() or member.isdir() or member.issym()
                         and RUNNER_LINKS.get(str(path)) == member.linkname), 'unsafe_runner_archive')
            if not path.parts:
                require(member.isdir(), 'unsafe_runner_archive')
                continue
            require(path not in paths and member.size >= 0, 'unsafe_runner_archive')
            paths.add(path)
            total += member.size
            require(total <= MAX_EXTRACTED, 'runner_archive_too_large')
            members.append((member, path))
        entries = {str(path): member for member, path in members}
        require(all(name in entries and entries[name].isfile() and entries[name].mode & 0o111
                    for name in ('config.sh', 'run.sh', 'bin/Runner.Listener')),
                'runner_archive_missing_entrypoint')
        for member, relative in members:
            if member.issym():
                # The six pinned Node entrypoints each point exactly one
                # level up to a regular file inside their own Node tree.
                target = relative.parent.parent / member.linkname.removeprefix('../')
                require(str(target) in entries and entries[str(target)].isfile(), 'unsafe_runner_archive')
        for member, relative in members:
            if member.issym():
                continue
            target = destination.joinpath(*relative.parts)
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
            if member.isdir():
                target.mkdir(exist_ok=True, mode=0o755)
            else:
                with archive.extractfile(member) as source, target.open('xb') as output:
                    copied = 0
                    while chunk := source.read(1024 * 1024):
                        copied += len(chunk)
                        require(copied <= member.size, 'unsafe_runner_archive')
                        output.write(chunk)
                    require(copied == member.size, 'unsafe_runner_archive')
                target.chmod(member.mode & 0o755)
        for member, relative in members:
            if member.issym():
                os.symlink(member.linkname, destination.joinpath(*relative.parts))
        require(all((destination / name).is_file()
                    and (os.name != 'posix' or (destination / name).stat().st_mode & 0o111)
                    for name in ('config.sh', 'run.sh', 'bin/Runner.Listener')),
                'runner_archive_missing_entrypoint')


def private_command(argv, payload, environment=None):
    # Child failures must never expose argv, environment or output.
    process = None
    try:
        remaining(payload)
        process = subprocess.Popen(argv, cwd=RUNNER_ROOT, env=environment,
                                   stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                   stderr=subprocess.DEVNULL, start_new_session=True)
        code = process.wait(timeout=remaining(payload))
    except (OSError, subprocess.SubprocessError, InstallError, KeyboardInterrupt):
        # wait(timeout) keeps a live direct leader unreaped. Signal only its
        # newly owned group, then reap it; never leave configure running.
        if process is not None and process.returncode is None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=5)
            except (OSError, subprocess.SubprocessError):
                pass
        raise InstallError('runner_command_failed') from None
    require(code == 0, 'runner_command_failed')


def owned_directory(path, ready):
    path.mkdir(mode=0o700, exist_ok=True)
    require(path.is_dir() and not path.is_symlink() and path.stat().st_dev == DATA_ROOT.stat().st_dev,
            'unsafe_runner_storage')
    os.chown(path, ready['runner_uid'], ready['runner_gid'])


def write_unit(name, content):
    target = SYSTEMD_ROOT / name
    require(not target.exists() and not target.is_symlink(), 'runner_unit_already_exists')
    with target.open('x', encoding='utf8') as stream:
        stream.write(content)
    target.chmod(0o644)


def install(payload):
    ready = verify_host(payload)
    remaining(payload)
    require(not RUNNER_ROOT.exists() and not RUNNER_ROOT.is_symlink(), 'runner_directory_already_exists')
    require(all(not (SYSTEMD_ROOT / name).exists() and not (SYSTEMD_ROOT / name).is_symlink()
                for name in (RUNNER_SERVICE, STOP_SERVICE, STOP_TIMER)), 'runner_unit_already_exists')
    RUNNER_ROOT.mkdir(mode=0o700)
    archive_fd, archive_name = tempfile.mkstemp(prefix='.runner-download-', suffix='.tar.gz', dir=DATA_ROOT)
    os.close(archive_fd)
    archive = Path(archive_name)
    archive.unlink()  # Downloader creates exclusively, never follows a substituted file.
    try:
        download_archive(payload, archive)
        safe_extract(archive, RUNNER_ROOT)
    finally:
        archive.unlink(missing_ok=True)
    # Keep the new runner root inaccessible to its user until extraction and
    # ownership changes are complete.
    for path in [*RUNNER_ROOT.rglob('*'), RUNNER_ROOT]:
        require(not path.is_symlink() or RUNNER_LINKS.get(path.relative_to(RUNNER_ROOT).as_posix())
                == os.readlink(path), 'unsafe_runner_archive')
        os.chown(path, ready['runner_uid'], ready['runner_gid'], follow_symlinks=False)
    environment = {'PATH': str(DATA_ROOT / 'cargo/bin') + ':/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
                   'HOME': str(DATA_ROOT / 'runner-home'), 'USER': 'benchmark', 'LOGNAME': 'benchmark',
                   'LANG': 'C.UTF-8', 'CARGO_HOME': str(DATA_ROOT / 'cargo'),
                   'RUSTUP_HOME': str(DATA_ROOT / 'rustup'), 'TMPDIR': str(DATA_ROOT / 'tmp')}
    for name in ('runner-home', 'cargo', 'rustup', 'tmp', 'work'):
        owned_directory(DATA_ROOT / name, ready)
    deadline = utc(payload['session_deadline_utc'])
    write_unit(STOP_SERVICE, '[Unit]\nDescription=Stop the original ScorpioFS campaign\n'
               '[Service]\nType=oneshot\nExecStart=/usr/bin/systemctl stop ' + RUNNER_SERVICE + '\n')
    write_unit(STOP_TIMER, '[Unit]\nDescription=Original campaign deadline\n[Timer]\nOnCalendar='
               + deadline.strftime('%Y-%m-%d %H:%M:%S UTC') + '\nAccuracySec=1s\nPersistent=true\n'
               'Unit=' + STOP_SERVICE + '\n[Install]\nWantedBy=timers.target\n')
    private_command(['/usr/bin/systemctl', 'daemon-reload'], payload)
    private_command(['/usr/bin/systemctl', 'enable', '--now', STOP_TIMER], payload)
    # v2.338.0 CommandSettings consumes ACTIONS_RUNNER_INPUT_* from memory,
    # masks secret values and removes these variables before configuration.
    configuration_env = dict(environment, ACTIONS_RUNNER_INPUT_TOKEN=payload['register_token'])
    private_command(['/usr/sbin/runuser', '-u', 'benchmark', '--', str(RUNNER_ROOT / 'config.sh'),
                     '--unattended', '--ephemeral', '--disableupdate', '--no-default-labels',
                     '--labels', payload['runner_label'], '--name', payload['runner_label'],
                     '--url', 'https://github.com/' + payload['repository'],
                     '--work', str(DATA_ROOT / 'work')], payload, configuration_env)
    del configuration_env
    # The exchanged runner credentials belong to Actions itself. The one-time
    # registration token is never included in our files, argv or exceptions.
    run_seconds = max(1, math.floor(deadline.timestamp() - time.time()))
    write_unit(RUNNER_SERVICE, '[Unit]\nDescription=Dedicated ephemeral ScorpioFS benchmark runner\n'
               'After=network-online.target\nWants=network-online.target\n[Service]\n'
               'User=benchmark\nGroup=benchmark\nWorkingDirectory=' + str(RUNNER_ROOT) + '\n'
               'ExecStart=' + str(RUNNER_ROOT / 'run.sh') + '\nRestart=no\nKillMode=control-group\n'
               'TimeoutStopSec=30\nRuntimeMaxSec=' + str(run_seconds) + '\n'
               + ''.join('Environment="' + name + '=' + environment[name] + '"\n'
                         for name in environment))
    private_command(['/usr/bin/systemctl', 'daemon-reload'], payload)
    private_command(['/usr/bin/systemctl', 'start', RUNNER_SERVICE], payload)
    private_command(['/usr/bin/systemctl', 'is-active', '--quiet', RUNNER_SERVICE, STOP_TIMER], payload)
    return {'revision': 1, 'status': 'runner_started', 'campaign_id': payload['campaign_id'],
            'runner_label': payload['runner_label'], 'runner_version': RUNNER_VERSION,
            'session_deadline_utc': payload['session_deadline_utc'],
            'hard_release_utc': payload['hard_release_utc']}


def main():
    try:
        payload = parse_payload(sys.stdin.buffer.read(MAX_INPUT + 1))
        result = install(payload)
    except InstallError as error:
        print(json.dumps({'revision': 1, 'status': 'failed', 'code': error.args[0]}), file=sys.stderr)
        return 1
    except Exception:
        print(json.dumps({'revision': 1, 'status': 'failed', 'code': 'runner_install_failed'}), file=sys.stderr)
        return 1
    print(json.dumps(result))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
