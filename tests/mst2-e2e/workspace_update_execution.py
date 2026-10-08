"""Actual Actions or root-attested Aliyun execution ownership, without emulation."""

from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import sys
import time
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

try:
    import pwd
except ImportError:
    pwd = None


DATA_ROOT = Path('/srv/scorpiofs-benchmark')
RECEIPT_ROOT = Path('/var/lib/scorpiofs-benchmark')
IMDS_ROOT = 'http://100.100.100.200/latest/'
CONTEXT_FIELDS = frozenset({'revision', 'execution_provider', 'campaign_id', 'instance_id',
    'attempt', 'run_uid', 'run_gid', 'data_root', 'data_device', 'data_uuid', 'owned_root',
    'session_started_utc', 'session_deadline_utc', 'hard_release_utc'})
DIRECT_FIELDS = frozenset({'execution_provider', 'campaign_id', 'instance_id', 'run_uid',
    'run_gid', 'data_root', 'data_device', 'data_uuid', 'hard_release_utc',
    'execution_receipt_sha256'})
_METADATA_CACHE = {}


def require(condition, code):
    if not condition:
        raise ValueError(code)


def utc(value):
    require(type(value) is str and len(value) <= 40, 'invalid_execution_window')
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        raise ValueError('invalid_execution_window') from None
    require(parsed.tzinfo is not None and parsed.utcoffset() == timedelta(0), 'invalid_execution_window')
    return parsed


def run_id_for_campaign(campaign):
    require(type(campaign) is str and re.fullmatch(r'v3-[0-9a-f]{20}', campaign), 'invalid_execution_campaign')
    return str(10 ** 17 + int(campaign[3:], 16) % (9 * 10 ** 17))


def validate_context(value):
    """Pure receipt replay never grants permission to run on a host."""
    require(type(value) is dict and set(value) == CONTEXT_FIELDS, 'invalid_execution_receipt')
    require(type(value['revision']) is int and value['revision'] == 1, 'invalid_execution_receipt')
    require(value['execution_provider'] == 'aliyun-direct', 'invalid_execution_provider')
    run_id_for_campaign(value['campaign_id'])
    require(type(value['instance_id']) is str and re.fullmatch(r'i-[a-z0-9]{4,64}', value['instance_id']),
            'invalid_execution_instance')
    require(value['attempt'] in ('1', '2', '3') and type(value['attempt']) is str, 'invalid_execution_attempt')
    require(all(type(value[key]) is int and value[key] > 0 for key in ('run_uid', 'run_gid')),
            'invalid_execution_user')
    require(value['data_root'] == DATA_ROOT.as_posix(), 'invalid_execution_storage')
    require(type(value['data_device']) is str and re.fullmatch(r'/dev/vd[b-z]', value['data_device']),
            'invalid_execution_storage')
    require(type(value['data_uuid']) is str and re.fullmatch(r'[0-9a-fA-F-]{16,64}', value['data_uuid']),
            'invalid_execution_storage')
    expected = DATA_ROOT / 'work' / ('mst2-direct-' + value['campaign_id'] + '-' + value['attempt'])
    require(value['owned_root'] == expected.as_posix(), 'invalid_execution_owned_root')
    start, deadline, release = [utc(value[key]) for key in
        ('session_started_utc', 'session_deadline_utc', 'hard_release_utc')]
    require(deadline - start == timedelta(minutes=235) and release - start == timedelta(minutes=240),
            'invalid_execution_window')
    return value


def _root_path(path, directory=False):
    info = path.lstat()
    require(info.st_uid == 0 and info.st_gid == 0 and not info.st_mode & 0o022
            and (stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)),
            'untrusted_execution_receipt_path')
    return info


def _unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate_execution_receipt_field')
        result[key] = value
    return result


def read_receipt(path):
    require(path.is_absolute(), 'invalid_execution_receipt_path')
    for parent in reversed(path.parents):
        _root_path(parent, directory=True)
    before = _root_path(path)
    require(before.st_nlink == 1, 'untrusted_execution_receipt_path')
    descriptor = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
    try:
        opened = os.fstat(descriptor)
        require((opened.st_dev, opened.st_ino) == (before.st_dev, before.st_ino),
                'execution_receipt_replaced')
        require(stat.S_ISREG(opened.st_mode) and opened.st_uid == opened.st_gid == 0
                and opened.st_nlink == 1 and not opened.st_mode & 0o022,
                'untrusted_execution_receipt_path')
        with os.fdopen(descriptor, 'rb', closefd=False) as stream:
            raw = stream.read(32769)
        after = os.fstat(descriptor)
        current = path.lstat()
        fields = lambda item: (item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns,
                               item.st_ctime_ns, item.st_uid, item.st_gid, item.st_mode, item.st_nlink)
        require(fields(opened) == fields(after) == fields(current), 'execution_receipt_changed')
    finally:
        os.close(descriptor)
    require(0 < len(raw) <= 32768, 'invalid_execution_receipt_size')
    try:
        value = json.loads(raw.decode('utf8'), object_pairs_hook=_unique)
    except (UnicodeError, json.JSONDecodeError):
        raise ValueError('invalid_execution_receipt_json') from None
    validate_context(value)
    expected = RECEIPT_ROOT / ('direct-execution-' + value['campaign_id'] + '-' + value['attempt'] + '.json')
    require(path == expected, 'invalid_execution_receipt_path')
    return value, hashlib.sha256(raw).hexdigest()


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, url):
        return None


def metadata_instance_id():
    """Use IMDSv2 only, with no environment proxies or redirect fallback."""
    try:
        opener = build_opener(ProxyHandler({}), _NoRedirect())
        token_url = IMDS_ROOT + 'api/token'
        token_request = Request(token_url, data=b'', method='PUT',
            headers={'X-aliyun-ecs-metadata-token-ttl-seconds': '60'})
        with opener.open(token_request, timeout=2) as response:
            require(response.geturl() == token_url, 'execution_metadata_failed')
            token = response.read(4097)
        require(0 < len(token) <= 4096 and all(33 <= byte <= 126 for byte in token),
                'execution_metadata_failed')
        instance_url = IMDS_ROOT + 'meta-data/instance-id'
        request = Request(instance_url, headers={'X-aliyun-ecs-metadata-token': token.decode('ascii')})
        with opener.open(request, timeout=2) as response:
            require(response.geturl() == instance_url, 'execution_metadata_failed')
            raw = response.read(129)
        require(0 < len(raw) <= 128, 'execution_metadata_failed')
        instance = raw.decode('ascii').strip()
        require(re.fullmatch(r'i-[a-z0-9]{4,64}', instance), 'execution_metadata_failed')
        return instance
    except Exception:
        # Metadata responses and tokens never leave this boundary in errors.
        raise ValueError('execution_metadata_failed') from None


def _boot_id():
    value = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    require(re.fullmatch(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', value),
            'invalid_execution_boot_identity')
    return value


def _instance_for_receipt(digest):
    # The immutable ECS identity cannot change within a process and boot.
    # Local receipt, UID, mount and time checks still run on every admission.
    key = (digest, _boot_id())
    if key not in _METADATA_CACHE:
        _METADATA_CACHE[key] = metadata_instance_id()
    return _METADATA_CACHE[key]


def check_storage(value):
    for path in reversed(DATA_ROOT.parents):
        _root_path(path, directory=True)
    info = _root_path(DATA_ROOT, directory=True)
    require(os.path.ismount(DATA_ROOT), 'execution_data_disk_not_mounted')
    device = Path(value['data_device']).resolve(strict=True)
    uuid = Path('/dev/disk/by-uuid') / value['data_uuid']
    require(uuid.resolve(strict=True) == device and stat.S_ISBLK(device.stat().st_mode)
            and device.stat().st_rdev == info.st_dev, 'execution_data_disk_mismatch')
    for name in ('test-home', 'cargo', 'rustup', 'tmp', 'work'):
        item = (DATA_ROOT / name).lstat()
        require(stat.S_ISDIR(item.st_mode) and not item.st_mode & 0o022
                and (item.st_uid, item.st_gid, item.st_dev) ==
                    (value['run_uid'], value['run_gid'], info.st_dev), 'execution_storage_owner_mismatch')
    root = Path(value['owned_root'])
    if root.exists() or root.is_symlink():
        item = root.lstat()
        require(stat.S_ISDIR(item.st_mode) and not item.st_mode & 0o022
                and (item.st_uid, item.st_gid, item.st_dev) ==
                    (value['run_uid'], value['run_gid'], info.st_dev), 'execution_owned_root_mismatch')


def identity():
    """Live Aliyun admission always checks the current machine and UID."""
    receipt = os.environ.get('MST2_EXECUTION_RECEIPT')
    if receipt is None:
        run, attempt = os.environ.get('GITHUB_RUN_ID', ''), os.environ.get('GITHUB_RUN_ATTEMPT', '')
        require(all(re.fullmatch(r'[1-9][0-9]{0,19}', item) for item in (run, attempt)),
                'job_ownership_identifiers_missing')
        return {'execution_provider': 'github-actions', 'run_id': run, 'attempt': attempt}
    require(sys.platform == 'linux' and hasattr(os, 'geteuid') and hasattr(os, 'getegid'),
            'unsupported_direct_execution_host')
    require(not any(key.startswith('GITHUB_') for key in os.environ)
            and 'RUNNER_ENVIRONMENT' not in os.environ, 'mixed_execution_identity')
    value, digest = read_receipt(Path(receipt))
    require((os.geteuid(), os.getegid()) == (value['run_uid'], value['run_gid']), 'execution_user_mismatch')
    require(pwd is not None, 'unsupported_direct_execution_host')
    account = pwd.getpwnam('benchmark')
    require((account.pw_uid, account.pw_gid, account.pw_dir) ==
            (value['run_uid'], value['run_gid'], str(DATA_ROOT / 'test-home')), 'execution_account_mismatch')
    expected_env = {'HOME': 'test-home', 'CARGO_HOME': 'cargo', 'RUSTUP_HOME': 'rustup', 'TMPDIR': 'tmp'}
    require(all(os.environ.get(key) == str(DATA_ROOT / name) for key, name in expected_env.items())
            and 'CARGO_TARGET_DIR' not in os.environ, 'execution_storage_environment_mismatch')
    require(utc(value['session_started_utc']) <= datetime.now(timezone.utc) < utc(value['hard_release_utc']),
            'execution_window_expired')
    require(all(os.environ.get(env) is not None and utc(os.environ[env]) == utc(value[key])
                for env, key in (('STARTED_INPUT', 'session_started_utc'),
                                 ('DEADLINE_INPUT', 'session_deadline_utc'))), 'execution_window_mismatch')
    if 'MST2_WORK_CLEANUP_DEADLINE_MONOTONIC' in os.environ:
        anchor = float(os.environ['MST2_WORK_CLEANUP_DEADLINE_MONOTONIC'])
        upper = time.monotonic() + utc(value['session_deadline_utc']).timestamp() - time.time() - 15 * 60
        require(math.isfinite(anchor) and 0 < anchor <= upper + 1, 'execution_monotonic_window_mismatch')
    check_storage(value)
    require(_instance_for_receipt(digest) == value['instance_id'], 'execution_instance_mismatch')
    return {**value, 'run_id': run_id_for_campaign(value['campaign_id']), 'execution_receipt_sha256': digest}


def owned_root(root):
    context = identity()
    run, attempt = context['run_id'], context['attempt']
    if context['execution_provider'] == 'github-actions':
        require(sys.platform == 'linux' and os.environ.get('GITHUB_ACTIONS') == 'true'
                and os.environ.get('RUNNER_ENVIRONMENT') in ('github-hosted', 'self-hosted'),
                'execution_requires_linux_actions_or_attested_aliyun')
        expected = Path(os.environ['RUNNER_TEMP']).resolve(strict=True) / f'mst2-real-{run}-{attempt}'
    else:
        expected = Path(context['owned_root'])
    root = Path(root)
    require(root.absolute() == expected and not root.is_symlink(), 'execution_owned_root_mismatch')
    return expected, f'm2perf-{run}-{attempt}'


def metadata_fields(context):
    if context['execution_provider'] == 'github-actions':
        return {}
    return {key: context[key] for key in DIRECT_FIELDS}


def bind_options(options):
    if 'MST2_EXECUTION_RECEIPT' in os.environ:
        context = identity()
        require(all(utc(getattr(options, key)) == utc(context[key]) for key in
                    ('session_started_utc', 'session_deadline_utc')), 'execution_window_mismatch')


def validate_metadata(value):
    context = {'revision': 1, **{key: value[key] for key in CONTEXT_FIELDS - {'revision'}}}
    validate_context(context)
    require(value['run_id'] == run_id_for_campaign(value['campaign_id']), 'execution_run_id_mismatch')
    require(type(value['execution_receipt_sha256']) is str
            and re.fullmatch(r'[0-9a-f]{64}', value['execution_receipt_sha256']), 'invalid_execution_receipt_digest')
