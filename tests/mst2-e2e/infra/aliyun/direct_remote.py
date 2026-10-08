"""Run one pinned native campaign directly on its disposable Aliyun instance."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
try:
    import pwd
except ImportError:
    pwd = None
import shutil
import subprocess
import sys
import time
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

DATA = Path('/srv/scorpiofs-benchmark')
CONTROL = Path('/var/lib/scorpiofs-benchmark')
IMDS = 'http://100.100.100.200/latest/'


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError('METADATA_REDIRECT_REJECTED')


def metadata(path, *, token=None, method='GET'):
    headers = ({'X-aliyun-ecs-metadata-token-ttl-seconds': '300'} if method == 'PUT'
               else {'X-aliyun-ecs-metadata-token': token})
    with build_opener(ProxyHandler({}), NoRedirect()).open(Request(IMDS + path, headers=headers, method=method), timeout=5) as stream:
        raw = stream.read(32769)
    if len(raw) > 32768:
        raise ValueError('METADATA_TOO_LARGE')
    return raw.decode().strip()


def bucket(config):
    import oss2
    token = metadata('api/token', method='PUT')
    if metadata('meta-data/instance-id', token=token) != config['instance_id']:
        raise ValueError('ACTUAL_INSTANCE_MISMATCH')
    credentials = json.loads(metadata('meta-data/ram/security-credentials/' + config['ram_role_name'], token=token))
    if credentials.get('Code') != 'Success':
        raise ValueError('INSTANCE_ROLE_UNAVAILABLE')
    auth = oss2.StsAuth(credentials['AccessKeyId'], credentials['AccessKeySecret'], credentials['SecurityToken'])
    return oss2.Bucket(auth, config['evidence_internal_endpoint'], config['evidence_bucket'], connect_timeout=15)


def save(path, value):
    temporary = path.with_suffix('.new')
    temporary.write_text(json.dumps(value, sort_keys=True) + '\n', encoding='utf-8')
    temporary.replace(path)


def install(config):
    if os.getuid() != 0:
        raise ValueError('ROOT_INSTALL_REQUIRED')
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    import workspace_update_execution as execution
    ready = json.loads(Path(config['bootstrap_ready_path']).read_bytes())
    if (ready['revision'] != 2 or ready['status'] != 'ready' or ready['campaign_id'] != config['campaign_id']
            or ready['test_tier'] != config['profile'] or ready['execution_mode'] != 'cloud-assistant'):
        raise ValueError('BOOTSTRAP_IDENTITY_MISMATCH')
    for key in ('session_started_utc', 'session_deadline_utc', 'hard_release_utc'):
        if ready[key] != config[key]:
            raise ValueError('ORIGINAL_BOOTSTRAP_WINDOW_MISMATCH')
    token = metadata('api/token', method='PUT')
    if metadata('meta-data/instance-id', token=token) != config['instance_id']:
        raise ValueError('ACTUAL_INSTANCE_MISMATCH')
    user = pwd.getpwnam('benchmark')
    if (user.pw_uid, user.pw_gid) != (ready['test_uid'], ready['test_gid']):
        raise ValueError('BENCHMARK_USER_MISMATCH')
    receipt = {'revision': 1, 'execution_provider': 'aliyun-direct', 'campaign_id': config['campaign_id'],
        'instance_id': config['instance_id'], 'attempt': '1', 'run_uid': user.pw_uid, 'run_gid': user.pw_gid,
        'data_root': str(DATA), 'data_device': ready['disk_device'], 'data_uuid': ready['disk_uuid'],
        'owned_root': str(DATA / 'work' / ('mst2-direct-' + config['campaign_id'] + '-1')),
        **{key: config[key] for key in ('session_started_utc', 'session_deadline_utc', 'hard_release_utc')}}
    execution.validate_context(receipt)
    receipt_path = CONTROL / ('direct-execution-' + config['campaign_id'] + '-1.json')
    if receipt_path.exists():
        raise ValueError('DIRECT_EXECUTION_CANNOT_RESTART')
    save(receipt_path, receipt)
    receipt_path.chmod(0o644)
    # Source objects were restored as root; give only this campaign's work tree
    # and evidence directory to its dedicated unprivileged benchmark account.
    workspace = Path(config['workspace'])
    if workspace != DATA / 'work' / config['campaign_id'] or workspace.is_symlink():
        raise ValueError('OWNED_SOURCE_WORKSPACE_REQUIRED')
    for path in (workspace, DATA / 'evidence' / config['campaign_id']):
        path.mkdir(parents=True, exist_ok=True)
        shutil.chown(path, user=user.pw_uid, group=user.pw_gid)
    for directory, dirs, files in os.walk(workspace, followlinks=False):
        os.chown(directory, user.pw_uid, user.pw_gid)
        for name in dirs + files:
            os.chown(Path(directory) / name, user.pw_uid, user.pw_gid, follow_symlinks=False)
    config['execution_receipt'] = str(receipt_path)
    config['status_path'] = str(DATA / 'evidence' / config['campaign_id'] / 'status.json')
    configuration = CONTROL / ('direct-config-' + config['campaign_id'] + '.json')
    save(configuration, config)
    configuration.chmod(0o644)
    remaining = int((datetime.fromisoformat(config['session_deadline_utc'].replace('Z', '+00:00'))
        - datetime.now(timezone.utc)).total_seconds())
    if remaining <= 0:
        raise TimeoutError('ORIGINAL_WINDOW_EXPIRED')
    environment = {'HOME': str(DATA / 'test-home'), 'CARGO_HOME': str(DATA / 'cargo'),
        'RUSTUP_HOME': str(DATA / 'rustup'), 'TMPDIR': str(DATA / 'tmp'),
        'MST2_EXECUTION_RECEIPT': str(receipt_path), 'PATH': str(DATA / 'cargo/bin') + ':/usr/local/bin:/usr/bin:/bin',
        'STARTED_INPUT': config['session_started_utc'], 'DEADLINE_INPUT': config['session_deadline_utc'],
        'CARGO_BUILD_JOBS': '2', 'CARGO_INCREMENTAL': '0', 'CARGO_PROFILE_RELEASE_DEBUG': '0',
        'NO_PROXY': 'localhost,127.0.0.1', 'LANG': 'C.UTF-8'}
    unit = 'scorpiofs-benchmark-' + config['campaign_id']
    subprocess.run(['systemd-run', '--unit=' + unit, '--collect', '--service-type=exec',
        '--property=User=benchmark', '--property=Group=benchmark', '--property=WorkingDirectory=' + str(workspace),
        '--property=RuntimeMaxSec=' + str(remaining), '--property=KillMode=control-group', '--property=TimeoutStopSec=30',
        *['--setenv=' + key + '=' + value for key, value in environment.items()],
        str(DATA / 'sdk/bin/python'), str(Path(__file__).resolve()), 'execute', '--config', str(configuration)],
        check=True, timeout=30, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return {'status': 'DIRECT_STARTED', 'campaign_id': config['campaign_id'], 'instance_id': config['instance_id'],
        'unit': unit, 'execution_receipt_sha256': hashlib.sha256(receipt_path.read_bytes()).hexdigest(),
        'status_path': config['status_path']}


def execute(config):
    harness = Path(config['workspace']) / 'scorpiofs'
    scripts = harness / 'tests/mst2-e2e'
    sys.path.insert(0, str(scripts))
    import commit_update_budget as budgets
    import commit_update_projection as projection
    import workspace_update_execution as execution
    import workspace_update_campaign_export as exporter
    import workspace_update_size as size
    os.environ.update(STARTED_INPUT=config['session_started_utc'], DEADLINE_INPUT=config['session_deadline_utc'])
    context = execution.identity()
    status_path = Path(config['status_path'])
    evidence = status_path.parent
    workspace = Path(config['workspace'])
    os.environ.update(STARTED_INPUT=config['session_started_utc'], DEADLINE_INPUT=config['session_deadline_utc'],
        MST2_SESSION_STARTED=config['session_started_utc'], MST2_SESSION_DEADLINE=config['session_deadline_utc'],
        SCORPIO_SHA=config['harness_sha'], MEGA_SHA=config['mega_sha'], BASELINE_SHA=config['baseline_sha'],
        CANDIDATE_SHA=config['candidate_sha'], PROFILE=config['profile'], ROUNDS='3', COMPARISON='isolated',
        BOOTSTRAP_COMMIT_TIME='1700000000', MST2_OWNED_ROOT=context['owned_root'])
    anchor = projection.window_anchor(config['session_started_utc'], config['session_deadline_utc'], admission=True)
    os.environ['MST2_WORK_CLEANUP_DEADLINE_MONOTONIC'] = repr(anchor)
    budget = budgets.IsolatedCampaignBudget(config['session_deadline_utc'], 3, anchor)
    receipts = {label: evidence / (label + '-build.json') for label in ('server', 'a', 'b')}
    base = {'revision': 1, 'campaign_id': config['campaign_id'], 'instance_id': config['instance_id'],
        'harness_sha': config['harness_sha'], 'profile': config['profile']}
    def status(state, stage, **fields):
        save(status_path, {**base, 'status': state, 'stage': stage, **fields})
    def run(args, deadline, *, build=False, fences=False):
        env = dict(os.environ)
        if fences:
            # Tests construct their own execution identities. Production build,
            # measurement and cleanup children retain the real cloud receipt.
            env = {key: value for key, value in env.items() if not key.startswith('MST2_')
                and key not in ('STARTED_INPUT', 'DEADLINE_INPUT')}
        if build:
            env['MST2_BUILD_DEADLINE_MONOTONIC'] = repr(deadline)
        code, _, _ = budgets.run_process(args, deadline, env=env, capture=False)
        if code:
            raise RuntimeError('NATIVE_STAGE_FAILED')
    python = sys.executable
    success, cleanup_ok, export_ok = False, False, False
    try:
        size.admit_backend(config['profile'], True)
        size.admit_campaign_disk(config['profile'], context['owned_root'])
        for stage, label, folder, sha in (('server-build', 'server', 'mega2', config['mega_sha']),
                ('client-a-build', 'a', 'client-a', config['baseline_sha']),
                ('client-b-build', 'b', 'client-b', config['candidate_sha'])):
            status('RUNNING', stage)
            run([python, '-B', str(scripts / 'workspace_update_build.py'), '--label', label,
                '--source', str(workspace / folder), '--source-sha', sha, '--receipt', str(receipts[label])],
                budget.stage_deadline(stage), build=True)
        status('RUNNING', 'correctness-fences')
        deadline = budget.stage_deadline('fences')
        for pattern in ('test_commit_update_*.py', 'test_workspace_update_*.py'):
            run([python, '-B', '-m', 'unittest', 'discover', '-s', str(scripts), '-p', pattern], deadline, fences=True)
        status('RUNNING', 'native-commit-measurements')
        args = [python, '-B', str(scripts / 'commit_update_ci.py'), '--execute', '--projection-traces',
            '--run-root', context['owned_root'], '--mega-source', str(workspace / 'mega2'), '--mega-sha', config['mega_sha'],
            '--mega-binary', str(workspace / 'mega2/target/release/mega2'), '--paired', '--build-a', str(receipts['a']),
            '--build-b', str(receipts['b']), '--baseline-sha', config['baseline_sha'], '--candidate-sha', config['candidate_sha'],
            '--isolated-backends', '--harness-sha', config['harness_sha'], '--bootstrap-commit-time', '1700000000',
            '--server-build-receipt', str(receipts['server']), '--profile', config['profile'], '--rounds', '3',
            '--session-started-utc', config['session_started_utc'], '--session-deadline-utc', config['session_deadline_utc']]
        run(args, anchor)
        success = True
    except BaseException as error:
        # The controller must leave the instance alive until cleanup and the
        # allowlisted partial export have finished.
        status('COLLECTING', 'native-execution-failed', error_type=type(error).__name__)
    finally:
        status('COLLECTING', 'cleanup', native_completed=success)
        try:
            run([python, '-B', str(scripts / 'commit_update_ci.py'), '--cleanup', '--paired', '--isolated-backends',
                '--run-root', context['owned_root'], '--session-started-utc', config['session_started_utc'],
                '--session-deadline-utc', config['session_deadline_utc'], '--rounds', '3'], anchor)
            cleanup_ok = True
        except BaseException:
            pass
        try:
            safe = evidence / 'safe-export'
            present = [(label, path) for label, path in receipts.items() if path.is_file()]
            exporter.export(Path(context['owned_root']), safe, config['session_deadline_utc'], present,
                run_metadata=exporter.run_metadata_from_env())
            import tarfile
            archive = evidence / 'safe-evidence.tar.gz'
            with tarfile.open(archive, 'w:gz') as stream:
                for path in sorted(safe.rglob('*')):
                    if path.is_file():
                        stream.add(path, arcname=path.relative_to(safe).as_posix(), recursive=False)
            if archive.stat().st_size > 512 * 1024 * 1024:
                raise ValueError('SAFE_ARCHIVE_TOO_LARGE')
            budgets.require_external_time(config['session_deadline_utc'], 60)
            bucket(config).put_object_from_file(config['evidence_prefix'] + 'safe-evidence.tar.gz', str(archive),
                headers={'x-oss-server-side-encryption': 'AES256', 'x-oss-object-acl': 'private', 'x-oss-forbid-overwrite': 'true'})
            export_ok = True
            manifest = json.loads((safe / 'safe-export.json').read_bytes())
            complete = success and cleanup_ok and manifest['complete_campaign'] is True
            status('COMPLETE_VERIFIED' if complete else 'FAILED', 'finished', native_completed=success,
                cleanup_verified=cleanup_ok, evidence_uploaded=True, evidence_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),
                evidence_bytes=archive.stat().st_size)
        except BaseException as error:
            status('FAILED', 'safe-export', native_completed=success, cleanup_verified=cleanup_ok,
                evidence_uploaded=False, error_type=type(error).__name__)
    return 0 if success and cleanup_ok and export_ok else 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=('install', 'execute'))
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    configuration = json.loads(Path(args.config).read_bytes())
    if args.action == 'install':
        print(json.dumps(install(configuration)))
    else:
        try:
            result = execute(configuration)
        except BaseException as error:
            save(Path(configuration['status_path']), {'revision': 1, 'status': 'FAILED', 'stage': 'execution-admission',
                'campaign_id': configuration['campaign_id'], 'instance_id': configuration['instance_id'],
                'harness_sha': configuration['harness_sha'], 'profile': configuration['profile'],
                'error_type': type(error).__name__, 'evidence_uploaded': False})
            result = 1
        raise SystemExit(result)
