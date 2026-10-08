"""Run one pinned native campaign directly on its disposable Aliyun instance."""

import argparse
from contextlib import contextmanager
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
import stat
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


def error_fields(error):
    line, trace = None, error.__traceback__
    while trace is not None:
        if Path(trace.tb_frame.f_code.co_filename).resolve() == Path(__file__).resolve():
            line = trace.tb_lineno
        trace = trace.tb_next
    return {'error_type': type(error).__name__,
        'error_errno': error.errno if isinstance(error, OSError) and type(error.errno) is int else None,
        'remote_source_line': line}


class ActiveExecution(Exception):
    pass


@contextmanager
def execution_claim(config):
    # The immutable root-owned receipt is the common inode for every entry.
    # A contender must never overwrite the owner's status or clean its work.
    import fcntl
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    import workspace_update_execution as execution
    context = execution.identity()
    path = Path(os.environ['MST2_EXECUTION_RECEIPT'])
    if (str(path) != config['execution_receipt'] or context['campaign_id'] != config['campaign_id']
            or context['instance_id'] != config['instance_id']):
        raise ValueError('EXECUTION_CLAIM_IDENTITY_MISMATCH')
    before = path.lstat()
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        opened = os.fstat(descriptor)
        fields = lambda value: (value.st_dev, value.st_ino, value.st_mode, value.st_uid,
                                value.st_gid, value.st_nlink, value.st_size, value.st_mtime_ns, value.st_ctime_ns)
        if (fields(before) != fields(opened) or not stat.S_ISREG(opened.st_mode)
                or opened.st_uid != 0 or opened.st_gid != 0 or opened.st_nlink != 1
                or opened.st_mode & 0o022):
            raise ValueError('EXECUTION_CLAIM_RECEIPT_CHANGED')
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ActiveExecution() from None
        raw = os.read(descriptor, 32769)
        if (not 0 < len(raw) <= 32768 or hashlib.sha256(raw).hexdigest() != context['execution_receipt_sha256']
                or fields(os.fstat(descriptor)) != fields(opened) or fields(path.lstat()) != fields(opened)):
            raise ValueError('EXECUTION_CLAIM_RECEIPT_CHANGED')
        yield context
    finally:
        os.close(descriptor)


def prepare_dependencies(config, workspace, user):
    """Resolve the actual locked clients from verified local Git mirrors first."""
    import direct_sources
    import tomllib
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    import commit_update_budget as budgets
    receipt_path = workspace / direct_sources.DEPENDENCY_RECEIPT_FILE
    if receipt_path.is_symlink() or not receipt_path.is_file() or receipt_path.stat().st_size > 65536:
        raise ValueError('INVALID_DEPENDENCY_RECEIPT')
    direct_sources.validate_dependency_receipt(json.loads(receipt_path.read_bytes()), workspace,
                                               config['source']['dependencies'])
    cargo = DATA / 'cargo'
    path = cargo / 'config.toml'
    base = {'source': {'crates-io': {'replace-with': 'rsproxy-sparse'},
                      'rsproxy-sparse': {'registry': 'sparse+https://rsproxy.cn/index/'}},
            'http': {'multiplexing': False}}
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 65536:
        raise ValueError('INVALID_CARGO_CONFIGURATION')
    raw = path.read_text(encoding='utf-8')
    if tomllib.loads(raw) != base:
        raise ValueError('INVALID_CARGO_CONFIGURATION')
    for label, identity in sorted(direct_sources.DEPENDENCIES.items()):
        original, mirror = 'pinned-' + label, 'local-' + label
        uri = direct_sources.dependency_repository(workspace, label).as_uri()
        raw += ('\n[source.' + json.dumps(original) + ']\ngit = ' + json.dumps(identity['url'])
            + '\nrev = ' + json.dumps(identity['sha']) + '\nreplace-with = ' + json.dumps(mirror)
            + '\n\n[source.' + json.dumps(mirror) + ']\ngit = ' + json.dumps(uri)
            + '\nrev = ' + json.dumps(identity['sha']) + '\n')
    tomllib.loads(raw)
    temporary = path.with_suffix('.new')
    with temporary.open('x', encoding='utf-8') as stream:
        stream.write(raw)
    os.chown(temporary, user.pw_uid, user.pw_gid)
    temporary.chmod(0o640)
    temporary.replace(path)
    env = {'HOME': str(DATA / 'test-home'), 'CARGO_HOME': str(cargo), 'RUSTUP_HOME': str(DATA / 'rustup'),
           'TMPDIR': str(DATA / 'tmp'), 'PATH': str(cargo / 'bin') + ':/usr/local/bin:/usr/bin:/bin'}
    rows = {}
    for label, sha in (('a', config['baseline_sha']), ('b', config['candidate_sha'])):
        source = workspace / ('client-' + label)
        lock = source / 'Cargo.lock'
        if lock.is_symlink() or not lock.is_file() or lock.stat().st_size > 4 * 1024 * 1024:
            raise ValueError('INVALID_CLIENT_LOCK')
        before = hashlib.sha256(lock.read_bytes()).hexdigest()
        seconds = (datetime.fromisoformat(config['preflight_deadline_utc'].replace('Z', '+00:00'))
                   - datetime.now(timezone.utc)).total_seconds()
        if seconds <= 0:
            raise TimeoutError('ORIGINAL_PREFLIGHT_EXPIRED')
        started = time.perf_counter_ns()
        deadline = time.monotonic() + min(300, seconds)
        code, _, _ = budgets.run_process(['runuser', '-u', 'benchmark', '--', 'env', '-i',
            *[key + '=' + value for key, value in env.items()], 'cargo', 'fetch', '--locked',
            '--target', 'x86_64-unknown-linux-gnu', '--manifest-path', str(source / 'Cargo.toml')],
            deadline, capture=True)
        if code:
            raise RuntimeError('LOCKED_DEPENDENCY_FETCH_FAILED')
        if hashlib.sha256(lock.read_bytes()).hexdigest() != before:
            raise ValueError('CLIENT_LOCK_CHANGED_DURING_PREPARATION')
        rows[label] = {'source_sha': sha, 'cargo_lock_sha256': before,
            'wall_ms': (time.perf_counter_ns() - started) / 1_000_000, 'locked': True}
    return {'revision': 1, 'target': 'x86_64-unknown-linux-gnu', 'clients': rows}


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
    dependency_preparation = prepare_dependencies(config, workspace, user)
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
        'status_path': config['status_path'], 'dependency_preparation': dependency_preparation}


def execute(config, context, progress):
    harness = Path(config['workspace']) / 'scorpiofs'
    scripts = harness / 'tests/mst2-e2e'
    sys.path.insert(0, str(scripts))
    import commit_update_budget as budgets
    import commit_update_projection as projection
    import workspace_update_execution as execution
    import workspace_update_campaign_export as exporter
    import workspace_update_size as size
    import workspace_update_git_performance as git_performance
    os.environ.update(STARTED_INPUT=config['session_started_utc'], DEADLINE_INPUT=config['session_deadline_utc'])
    status_path = Path(config['status_path'])
    evidence = status_path.parent
    performance_path = evidence / 'git-performance.jsonl'
    os.environ.pop('MST2_GIT_PERFORMANCE_PATH', None)
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
    def failure(error):
        if progress['failure'] is None:
            progress['failure'] = {'failed_stage': progress['stage'], **error_fields(error)}
    def status(state, stage, **fields):
        progress['stage'] = stage
        metric_stage = (stage if stage in ('server-build', 'client-a-build', 'client-b-build', 'cleanup')
                        else 'fences' if stage == 'correctness-fences'
                        else 'setup' if stage == 'native-commit-measurements' else 'report')
        metric_phase = ('build' if metric_stage.endswith('build') else
                        'cleanup' if metric_stage == 'cleanup' else 'setup')
        os.environ['MST2_GIT_PERFORMANCE_CONTEXT'] = json.dumps(
            dict(stage=metric_stage, phase=metric_phase, round=None, client=None, version=None))
        value = {**base, 'status': state, 'stage': stage, 'primary_failure': progress['failure'], **fields}
        try:
            save(status_path, value)
            return True
        except BaseException as error:
            failure(error)
            print(json.dumps({'status': 'STATUS_WRITE_FAILED', **error_fields(error)}), file=sys.stderr, flush=True)
            return False
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
    success, cleanup_ok, export_ok, metrics_created, complete = False, False, False, False, False
    cleanup_error = metrics_error = None
    try:
        with performance_path.open('xb'):
            pass
        metrics_created = True
        performance_path.chmod(0o600)
        os.environ['MST2_GIT_PERFORMANCE_PATH'] = str(performance_path)
        size.admit_backend(config['profile'], True)
        size.admit_campaign_disk(config['profile'], context['owned_root'])
        for stage, label, folder, sha in (('server-build', 'server', 'mega2', config['mega_sha']),
                ('client-a-build', 'a', 'client-a', config['baseline_sha']),
                ('client-b-build', 'b', 'client-b', config['candidate_sha'])):
            if not status('RUNNING', stage):
                raise RuntimeError('STATUS_WRITE_FAILED')
            run([python, '-B', str(scripts / 'workspace_update_build.py'), '--label', label,
                '--source', str(workspace / folder), '--source-sha', sha, '--receipt', str(receipts[label])],
                budget.stage_deadline(stage), build=True)
        if not status('RUNNING', 'correctness-fences'):
            raise RuntimeError('STATUS_WRITE_FAILED')
        deadline = budget.stage_deadline('fences')
        for pattern in ('test_commit_update_*.py', 'test_workspace_update_*.py'):
            run([python, '-B', '-m', 'unittest', 'discover', '-s', str(scripts), '-p', pattern], deadline, fences=True)
        run([python, '-B', '-m', 'unittest', 'discover', '-s', str(scripts / 'infra/aliyun'),
            '-p', 'test_*.py'], deadline, fences=True)
        if not status('RUNNING', 'native-commit-measurements'):
            raise RuntimeError('STATUS_WRITE_FAILED')
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
        failure(error)
        status('COLLECTING', 'native-execution-failed')
    finally:
        status('COLLECTING', 'cleanup', native_completed=success)
        try:
            run([python, '-B', str(scripts / 'commit_update_ci.py'), '--cleanup', '--paired', '--isolated-backends',
                '--run-root', context['owned_root'], '--session-started-utc', config['session_started_utc'],
                '--session-deadline-utc', config['session_deadline_utc'], '--rounds', '3'], anchor)
            cleanup_ok = True
        except BaseException as error:
            cleanup_error = error_fields(error)
        try:
            safe = evidence / 'safe-export'
            present = [(label, path) for label, path in receipts.items() if path.is_file()]
            metrics_closed = False
            if cleanup_ok and metrics_created:
                try:
                    git_performance.finalize(performance_path)
                    metrics_closed = True
                except (OSError, ValueError, TimeoutError) as error:
                    metrics_error = error_fields(error)
            exporter.export(Path(context['owned_root']), safe, config['session_deadline_utc'], present,
                run_metadata=exporter.run_metadata_from_env(),
                git_performance_path=performance_path if metrics_created else None,
                complete_allowed=success and cleanup_ok and metrics_created and metrics_closed and progress['failure'] is None)
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
            complete = (success and cleanup_ok and metrics_created and metrics_closed
                        and progress['failure'] is None and manifest['complete_campaign'] is True)
            status('COMPLETE_VERIFIED' if complete else 'FAILED', 'finished', native_completed=success,
                cleanup_verified=cleanup_ok, git_metrics_closed=metrics_closed,
                cleanup_failure=cleanup_error, git_metrics_failure=metrics_error,
                evidence_uploaded=True, evidence_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),
                evidence_bytes=archive.stat().st_size)
        except BaseException as error:
            status('FAILED', 'safe-export', native_completed=success, cleanup_verified=cleanup_ok,
                evidence_uploaded=False, export_failure=error_fields(error),
                cleanup_failure=cleanup_error, git_metrics_failure=metrics_error)
    return 0 if complete and export_ok and progress['failure'] is None else 1


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=('install', 'execute'))
    parser.add_argument('--config', required=True)
    args = parser.parse_args(argv)
    configuration = json.loads(Path(args.config).read_bytes())
    if args.action == 'install':
        print(json.dumps(install(configuration)))
        return 0
    else:
        progress = {'stage': 'execution-admission', 'failure': None}
        try:
            with execution_claim(configuration) as context:
                try:
                    return execute(configuration, context, progress)
                except BaseException as error:
                    value = {'revision': 1, 'status': 'FAILED', 'stage': progress['stage'],
                        'campaign_id': configuration['campaign_id'], 'instance_id': configuration['instance_id'],
                        'harness_sha': configuration['harness_sha'], 'profile': configuration['profile'],
                        'primary_failure': progress['failure'] or {'failed_stage': progress['stage'], **error_fields(error)},
                        'outer_failure': error_fields(error), 'evidence_uploaded': False}
                    try:
                        save(Path(configuration['status_path']), value)
                    except BaseException as save_error:
                        print(json.dumps({'status': 'STATUS_WRITE_FAILED', **error_fields(save_error)}), file=sys.stderr, flush=True)
                    return 1
        except ActiveExecution:
            print(json.dumps({'status': 'ACTIVE_EXECUTION_ALREADY_OWNS_CAMPAIGN'}), file=sys.stderr, flush=True)
            return 1
        except BaseException as error:
            # Failure before the claim grants no right to touch canonical state.
            print(json.dumps({'status': 'EXECUTION_CLAIM_FAILED', **error_fields(error)}), file=sys.stderr, flush=True)
            return 1


if __name__ == '__main__':
    raise SystemExit(main())
