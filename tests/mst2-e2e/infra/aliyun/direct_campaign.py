"""Operate a disposable native ECS campaign through Terraform and Cloud Assistant.

Preparation is local. Each start uses one immutable four-hour window and always
attempts cleanup; source packages, state and safe evidence stay outside Git.
"""

import argparse
import base64
from datetime import datetime, timedelta, timezone
import hashlib
import ipaddress
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys
import tarfile
import time
import uuid

import direct_sources

HERE = Path(__file__).resolve().parent
SERVER = '75a1d081e465c531f396a0d3b22d18a45f942f9a'
BASELINE = 'd265e31169fb2f8b137ce9922784ebd0238c6397'
CANDIDATE = 'f18b99645d7bbbc5b4ea3374165e57dc4ff9f922'
MAX_EVIDENCE = 512 * 1024 * 1024
MAX_EXPANDED_EVIDENCE = 2 * 1024 * 1024 * 1024
CONFIG_FIELDS = {'region', 'zone', 'image_id', 'instance_type', 'vpc_cidr', 'vswitch_cidr',
                 'harness_sha', 'profile', 'scorpiofs_source', 'mega2_source', 'dependency_sources'}


def require(ok, code):
    if not ok:
        raise ValueError(code)


def utc(value):
    require(type(value) is str and re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', value), 'INVALID_UTC')
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def stamp(value):
    return value.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def now():
    return datetime.now(timezone.utc)


def schedule(start):
    return {'session_started_utc': stamp(start), 'preflight_deadline_utc': stamp(start + timedelta(minutes=15)),
            'work_cleanup_deadline_utc': stamp(start + timedelta(minutes=220)),
            'collection_deadline_utc': stamp(start + timedelta(minutes=233)),
            'session_deadline_utc': stamp(start + timedelta(minutes=235)),
            'hard_release_utc': stamp(start + timedelta(minutes=240))}


def config(value):
    require(type(value) is dict and set(value) in (CONFIG_FIELDS, CONFIG_FIELDS | {'request_endpoint_diagnostic'}), 'INVALID_CONFIG_FIELDS')
    require(type(value.get('request_endpoint_diagnostic', False)) is bool, 'INVALID_DIAGNOSTIC_OPT_IN')
    for key in ('region', 'zone', 'image_id', 'instance_type'):
        require(type(value[key]) is str and re.fullmatch(r'[A-Za-z0-9_.-]{2,160}', value[key])
                and not value[key].startswith('-'), 'INVALID_CONFIG_VALUE')
    require(value['zone'].startswith(value['region'] + '-'), 'ZONE_REGION_MISMATCH')
    require(value['image_id'].startswith('ubuntu_24_04_x64_'), 'UBUNTU_24_04_AMD64_REQUIRED')
    require(value['instance_type'] == 'ecs.u1-c1m4.2xlarge', 'REVIEWED_NON_BURSTABLE_SKU_REQUIRED')
    require(type(value['harness_sha']) is str and re.fullmatch(r'[0-9a-f]{40}', value['harness_sha']), 'IMMUTABLE_HARNESS_REQUIRED')
    require(value['profile'] in ('smoke', 'medium', 'history-large'), 'INVALID_PROFILE')
    require(ipaddress.IPv4Network(value['vswitch_cidr']).subnet_of(ipaddress.IPv4Network(value['vpc_cidr'])), 'SUBNET_OUTSIDE_VPC')
    for key in ('scorpiofs_source', 'mega2_source'):
        require(type(value[key]) is str and Path(value[key]).is_absolute(), 'ABSOLUTE_SOURCE_PATH_REQUIRED')
    direct_sources.dependency_sources(value['dependency_sources'])
    return dict(value)


def save(path, value):
    temporary = path.with_suffix('.new')
    temporary.write_text(json.dumps(value, indent=2) + '\n', encoding='utf-8')
    temporary.replace(path)


def plan(value, directory):
    value = config(value)
    directory = Path(directory).resolve()
    require(not directory.is_relative_to(HERE.parents[3]), 'STATE_MUST_BE_OUTSIDE_REPOSITORY')
    require(not directory.exists(), 'FRESH_STATE_DIRECTORY_REQUIRED')
    directory.mkdir(parents=True, mode=0o700)
    state = {'revision': 2, 'execution_provider': 'aliyun-direct', 'campaign_id': 'v3-' + uuid.uuid4().hex[:20],
             'config': value, 'status': 'PREPARED_LOCAL_ONLY'}
    save(directory / 'campaign.json', state)
    return state


class Tools:
    def __init__(self, terraform='terraform', aliyun='aliyun'):
        self.terraform, self.aliyun = terraform, aliyun

    def call(self, arguments, *, timeout=60, cwd=None):
        try:
            result = subprocess.run(arguments, capture_output=True, timeout=max(.1, timeout), cwd=cwd)
        except (OSError, subprocess.TimeoutExpired):
            raise RuntimeError('TOOL_FAILED_OR_TIMED_OUT') from None
        if result.returncode:
            # Never relay arbitrary tool output, credentials or private logs.
            raise RuntimeError('TOOL_NONZERO_EXIT')
        return result.stdout


def launcher(configuration, source):
    """A small cloud command downloads the exact reviewed code from private OSS."""
    # The controller retains local pack timings; they are not remote inputs.
    value = dict(configuration, source={key: item for key, item in source.items()
                                       if key != 'source_pack_performance'})
    payload = base64.b64encode(json.dumps(value).encode()).decode()
    sdk_program = r'''
import base64, hashlib, json, runpy, subprocess, sys, tarfile, time
from pathlib import Path
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from datetime import datetime
import oss2
c=json.loads(base64.b64decode(sys.argv[1]))
limit=datetime.fromisoformat(c['preflight_deadline_utc'].replace('Z','+00:00')).timestamp()
def remaining():
    seconds=limit-time.time()
    if seconds<=0: raise TimeoutError('ORIGINAL_PREFLIGHT_EXPIRED')
    return seconds
class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*a,**kw): raise ValueError('METADATA_REDIRECT_REJECTED')
def meta(path,token=None,method='GET'):
    h={'X-aliyun-ecs-metadata-token-ttl-seconds':'300'} if method=='PUT' else {'X-aliyun-ecs-metadata-token':token}
    with build_opener(ProxyHandler({}),NoRedirect()).open(Request('http://100.100.100.200/latest/'+path,headers=h,method=method),timeout=min(5,remaining())) as r: raw=r.read(32769)
    if len(raw)>32768: raise ValueError('METADATA_TOO_LARGE')
    return raw.decode().strip()
token=meta('api/token',method='PUT')
if meta('meta-data/instance-id',token)!=c['instance_id']: raise ValueError('ACTUAL_INSTANCE_MISMATCH')
creds=json.loads(meta('meta-data/ram/security-credentials/'+c['ram_role_name'],token))
if creds.get('Code')!='Success': raise ValueError('INSTANCE_ROLE_UNAVAILABLE')
b=oss2.Bucket(oss2.StsAuth(creds['AccessKeyId'],creds['AccessKeySecret'],creds['SecurityToken']),c['evidence_internal_endpoint'],c['evidence_bucket'],connect_timeout=min(15,remaining()))
archive=Path('/srv/scorpiofs-benchmark')/('source-'+c['campaign_id']+'.tar.gz')
size=0; digest=hashlib.sha256()
stream=b.get_object(c['source']['key'])
with archive.open('xb') as out:
    while True:
        remaining(); chunk=stream.read(1024*1024)
        if not chunk: break
        size+=len(chunk)
        if size>c['source']['bytes'] or size>512*1024*1024: raise ValueError('SOURCE_TOO_LARGE')
        digest.update(chunk); out.write(chunk)
if size!=c['source']['bytes'] or digest.hexdigest()!=c['source']['sha256']: raise ValueError('SOURCE_DIGEST_MISMATCH')
with tarfile.open(archive,'r:gz') as t:
    members=t.getmembers()
    names={'scorpiofs.pack','mega2.pack','client-a.pack','client-b.pack','rk8s.pack','mst2-codec.pack','manifest.json','bootstrap.py'}
    if len(members)!=len(names) or {m.name for m in members}!=names or any(not m.isfile() or not 0<=m.size<=512*1024*1024 for m in members) or sum(m.size for m in members)>2*1024**3: raise ValueError('INVALID_SOURCE_MEMBERS')
    raw=t.extractfile('bootstrap.py').read(1024*1024+1)
if len(raw)>1024*1024 or hashlib.sha256(raw).hexdigest()!=c['source']['bootstrap_sha256']: raise ValueError('BOOTSTRAP_DIGEST_MISMATCH')
helper=Path('/var/lib/scorpiofs-benchmark')/('restore-'+c['campaign_id']+'.py')
helper.write_bytes(raw); helper.chmod(0o644)
pins={'scorpiofs':c['harness_sha'],'mega2':c['mega_sha'],'client-a':c['baseline_sha'],'client-b':c['candidate_sha']}
subprocess.run([sys.executable,str(helper),'--bundle',str(archive),'--output',c['workspace'],'--pins',json.dumps(pins)],check=True,timeout=remaining(),stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
performance_path=Path(c['workspace'])/'source-git-performance.json'
if performance_path.is_symlink() or not performance_path.is_file() or performance_path.stat().st_size>65536: raise ValueError('INVALID_SOURCE_PERFORMANCE')
source_performance=runpy.run_path(str(helper))['validate_performance'](json.loads(performance_path.read_bytes()),phase='restore',require_success=True)
target=Path('/var/lib/scorpiofs-benchmark')/('launch-'+c['campaign_id']+'.json')
target.write_text(json.dumps(c)); target.chmod(0o644)
remote=Path(c['workspace'])/'scorpiofs/tests/mst2-e2e/infra/aliyun/direct_remote.py'
result=subprocess.run([sys.executable,str(remote),'install','--config',str(target)],capture_output=True,check=True,timeout=remaining())
response=json.loads(result.stdout)
if type(response) is not dict or 'source_restore_performance' in response: raise ValueError('INVALID_DIRECT_INSTALL_RESPONSE')
response['source_restore_performance']=source_performance
remaining(); print(json.dumps(response,separators=(',',':')))
'''
    wait_program = "\n".join([
        'import base64,json,os,subprocess,time', 'from pathlib import Path', 'from datetime import datetime',
        'c=json.loads(base64.b64decode(' + repr(payload) + '))',
        "limit=datetime.fromisoformat(c['preflight_deadline_utc'].replace('Z','+00:00')).timestamp()",
        "while not Path(c['bootstrap_ready_path']).is_file():",
        "    if time.time()>=limit: raise TimeoutError('ORIGINAL_PREFLIGHT_EXPIRED')",
        '    time.sleep(2)',
        "seconds=limit-time.time()", "if seconds<=0: raise TimeoutError('ORIGINAL_PREFLIGHT_EXPIRED')",
        'subprocess.run([\'/srv/scorpiofs-benchmark/sdk/bin/python\',\'-c\',' + repr(sdk_program) + ',' + repr(payload) + '],check=True,timeout=seconds)',
    ])
    script = "#!/bin/bash\nset -euo pipefail\npython3 - <<'SCORPIO_DIRECT_PY'\n" + wait_program + '\nSCORPIO_DIRECT_PY\n'
    require(len(base64.b64encode(script.encode())) <= 24 * 1024, 'CLOUD_COMMAND_TOO_LARGE')
    return script


class Campaign:
    def __init__(self, directory, tools=None):
        self.directory = Path(directory).resolve(strict=True)
        self.path = self.directory / 'campaign.json'
        self.state = json.loads(self.path.read_bytes())
        require(self.state['revision'] == 2 and self.state['execution_provider'] == 'aliyun-direct', 'DIRECT_STATE_REQUIRED')
        require(re.fullmatch(r'v3-[0-9a-f]{20}', self.state['campaign_id']), 'INVALID_CAMPAIGN')
        self.cfg, self.tools = config(self.state['config']), tools or Tools()
        self.operation_deadline = None
        if 'session_started_utc' in self.state:
            require(all(self.state.get(k) == v for k, v in schedule(utc(self.state['session_started_utc'])).items()), 'IMMUTABLE_WINDOW_MISMATCH')

    def record(self, **values):
        self.state.update(values)
        save(self.path, self.state)

    def remaining(self, field, cap):
        seconds = (utc(self.state[field]) - now()).total_seconds()
        require(seconds > 0, 'ORIGINAL_DEADLINE_EXPIRED')
        return min(cap, seconds)

    def timeout(self, cap):
        if self.operation_deadline is None:
            return cap
        seconds = self.operation_deadline - time.monotonic()
        require(seconds > 0, 'OPERATION_DEADLINE_EXPIRED')
        return min(cap, seconds)

    def api(self, product, action, *arguments, cap=30):
        return json.loads(self.tools.call([self.tools.aliyun, product, action, *arguments], timeout=self.timeout(cap)))

    def tf(self, *arguments, cap=300):
        return self.tools.call([self.tools.terraform, '-chdir=' + str(self.directory / 'terraform'), *arguments], timeout=self.timeout(cap))

    def admit_cloud_shape(self):
        rows = self.api('ecs', 'DescribeInstanceTypes', '--region', self.cfg['region'], '--InstanceTypes.1', self.cfg['instance_type'])['InstanceTypes']['InstanceType']
        require(len(rows) == 1 and rows[0]['InstanceTypeId'] == self.cfg['instance_type']
                and rows[0]['CpuCoreCount'] == 8 and rows[0]['MemorySize'] >= 32, 'REVIEWED_SKU_SHAPE_REQUIRED')
        rows = self.api('ecs', 'DescribeImages', '--RegionId', self.cfg['region'], '--ImageId', self.cfg['image_id'])['Images']['Image']
        require(len(rows) == 1 and rows[0]['ImageId'] == self.cfg['image_id'] and rows[0]['Architecture'] == 'x86_64'
                and rows[0]['Platform'] == 'Ubuntu' and rows[0]['Status'] == 'Available', 'REVIEWED_UBUNTU_IMAGE_REQUIRED')

    def bind_outputs(self, value):
        identity, region = self.state['campaign_id'], self.cfg['region']
        expected = {'run_id': identity, 'test_tier': self.cfg['profile'], 'region': region, 'zone': self.cfg['zone'],
            'session_started_utc': self.state['session_started_utc'], 'expires_at': self.state['hard_release_utc'],
            'measurement_deadline': self.state['work_cleanup_deadline_utc'], 'cleanup_deadline': self.state['session_deadline_utc'],
            'evidence_bucket': 'scorpiofs-bench-' + identity, 'source_prefix': 'sources/' + identity + '/',
            'evidence_prefix': 'runs/' + identity + '/', 'evidence_endpoint': 'https://oss-' + region + '.aliyuncs.com',
            'evidence_internal_endpoint': 'https://oss-' + region + '-internal.aliyuncs.com',
            'ram_role_name': 'scorpiofs-' + identity, 'ram_policy_name': 'scorpiofs-' + identity + '-transfer',
            'execution_mode': 'cloud-assistant', 'native_storage_backend': 'local',
            'bootstrap_ready_path': '/var/lib/scorpiofs-benchmark/bootstrap-ready.json',
            'bootstrap_mount_root': '/srv/scorpiofs-benchmark', 'test_user': 'benchmark',
            'test_home': '/srv/scorpiofs-benchmark/test-home', 'work_root': '/srv/scorpiofs-benchmark/work',
            'cargo_home': '/srv/scorpiofs-benchmark/cargo', 'rustup_home': '/srv/scorpiofs-benchmark/rustup',
            'temporary_root': '/srv/scorpiofs-benchmark/tmp'}
        require(all(value.get(k) == v for k, v in expected.items()), 'RESOURCE_BINDING_MISMATCH')
        require(type(value.get('instance_id')) is str and re.fullmatch(r'i-[a-z0-9]+', value['instance_id']), 'INSTANCE_ID_REQUIRED')
        require(ipaddress.IPv4Address(value['public_ip']).is_global, 'PUBLIC_ECS_IP_REQUIRED')

    def ready(self):
        instance = self.state['resources']['instance_id']
        rows = self.inventory('ecs', 'DescribeInstances', 'Instances', 'Instance', ['--InstanceIds', json.dumps([instance])])
        require(len(rows) == 1 and rows[0]['InstanceId'] == instance, 'EXACT_INSTANCE_MISSING')
        agent = self.api('ecs', 'DescribeCloudAssistantStatus', '--RegionId', self.cfg['region'], '--InstanceId.1', instance)
        rows_agent = agent['InstanceCloudAssistantStatusSet']['InstanceCloudAssistantStatus']
        rows_agent = [row for row in rows_agent if row['InstanceId'] == instance]
        require(len(rows_agent) <= 1, 'AMBIGUOUS_AGENT_STATUS')
        return rows[0]['Status'] == 'Running' and len(rows_agent) == 1 and rows_agent[0]['CloudAssistantStatus'] == 'true'

    def command(self, script, seconds, phase):
        instance = self.state['resources']['instance_id']
        require(0 < seconds <= 900 and len(base64.b64encode(script.encode())) <= 24 * 1024, 'INVALID_CLOUD_COMMAND_BOUND')
        invoke = self.api('ecs', 'RunCommand', '--RegionId', self.cfg['region'], '--InstanceId.1', instance,
            '--Type', 'RunShellScript', '--CommandContent', base64.b64encode(script.encode()).decode(),
            '--ContentEncoding', 'Base64', '--Timeout', str(max(1, int(seconds))), '--WorkingDir', '/root',
            '--ClientToken', self.state['campaign_id'] + '-' + uuid.uuid4().hex[:16], '--Username', 'root',
            '--KeepCommand', 'false', '--RepeatMode', 'Once')
        invoke_id = invoke['InvokeId']
        self.record(last_invoke_id=invoke_id, last_command_phase=phase)
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            value = self.api('ecs', 'DescribeInvocationResults', '--RegionId', self.cfg['region'], '--InvokeId', invoke_id,
                '--InstanceId', instance, '--ContentEncoding', 'PlainText', '--MaxResults', '50')
            container = value['Invocation']
            require(not container.get('NextToken'), 'EXACT_INVOCATION_PAGINATED')
            rows = container['InvocationResults']['InvocationResult']
            require(len(rows) <= 1, 'AMBIGUOUS_INVOCATION')
            if rows:
                row = rows[0]
                require(row['InstanceId'] == instance and row['InvokeId'] == invoke_id, 'INVOCATION_IDENTITY_MISMATCH')
                state = row['InvocationStatus']
                overall = row['InvokeRecordStatus']
                require(state not in ('Invalid', 'Aborted', 'Failed', 'Error', 'Timeout', 'Cancelled', 'Terminated')
                        and overall not in ('Failed', 'PartialFailed', 'Stopped'), 'CLOUD_COMMAND_FAILED')
                if state == 'Success' and overall == 'Finished':
                    require(type(row['ExitCode']) is int and row['ExitCode'] == 0 and row.get('Dropped') == 0
                            and not row.get('ErrorCode'), 'CLOUD_COMMAND_FAILED_OR_TRUNCATED')
                    raw = row.get('Output', '')
                    require(type(raw) is str and len(raw.encode()) <= 32768, 'CLOUD_OUTPUT_TOO_LARGE')
                    return json.loads(raw)
            time.sleep(min(3, max(0, end - time.monotonic())))
        raise TimeoutError('ORIGINAL_CLOUD_COMMAND_DEADLINE_EXPIRED')

    def remote_config(self):
        resources = self.state['resources']
        return {**{key: self.state[key] for key in ('campaign_id', 'session_started_utc', 'session_deadline_utc', 'hard_release_utc', 'preflight_deadline_utc')},
            **{key: resources[key] for key in ('instance_id', 'ram_role_name', 'evidence_bucket', 'evidence_internal_endpoint', 'evidence_prefix', 'bootstrap_ready_path')},
            'workspace': '/srv/scorpiofs-benchmark/work/' + self.state['campaign_id'],
            'harness_sha': self.cfg['harness_sha'], 'mega_sha': SERVER, 'baseline_sha': BASELINE,
            'candidate_sha': CANDIDATE, 'profile': self.cfg['profile'],
            **({'request_endpoint_diagnostic': True} if self.cfg.get('request_endpoint_diagnostic', False) else {})}

    def run(self):
        require(self.state['status'] == 'PREPARED_LOCAL_ONLY', 'CAMPAIGN_CANNOT_BE_RESTARTED')
        sources = {'scorpiofs': {'path': self.cfg['scorpiofs_source'], 'sha': self.cfg['harness_sha']},
            'mega2': {'path': self.cfg['mega2_source'], 'sha': SERVER},
            'client-a': {'path': self.cfg['scorpiofs_source'], 'sha': BASELINE},
            'client-b': {'path': self.cfg['scorpiofs_source'], 'sha': CANDIDATE}}
        source = direct_sources.create(sources, self.directory / 'sources.tar.gz', self.cfg['dependency_sources'])
        source['key'] = 'sources/' + self.state['campaign_id'] + '/sources.tar.gz'
        self.record(source=source, evidence_object='runs/' + self.state['campaign_id'] + '/safe-evidence.tar.gz')
        self.admit_cloud_shape()
        work = self.directory / 'terraform'
        shutil.copytree(HERE / 'terraform', work, ignore=shutil.ignore_patterns('.terraform', '*.tfstate*', '*.tfplan', '*.tfvars', '*.tfvars.json'))
        self.tf('init', '-input=false', '-lockfile=readonly', cap=120)
        self.tf('validate', cap=30)
        self.record(**schedule(now()), status='PROVISIONING')
        self.operation_deadline = time.monotonic() + self.remaining('preflight_deadline_utc', 900)
        variables = {key: self.cfg[key] for key in ('region', 'zone', 'image_id', 'instance_type', 'vpc_cidr', 'vswitch_cidr')}
        variables.update(run_id=self.state['campaign_id'], test_tier=self.cfg['profile'],
            session_started_utc=self.state['session_started_utc'], expires_at=self.state['hard_release_utc'])
        save(work / 'campaign.auto.tfvars.json', variables)
        try:
            self.tf('plan', '-input=false', '-out=campaign.tfplan', cap=120)
            self.tf('apply', '-input=false', 'campaign.tfplan', cap=600)
            resources = json.loads(self.tf('output', '-json', 'campaign', cap=15))
            self.bind_outputs(resources)
            self.record(resources=resources)
            disks = self.inventory('ecs', 'DescribeDisks', 'Disks', 'Disk', ['--InstanceId', resources['instance_id'],
                '--DiskName', 'scorpiofs-' + self.state['campaign_id'] + '-work'])
            require(len(disks) == 1 and disks[0]['DeleteWithInstance'] is True, 'OWNED_DATA_DISK_REQUIRED')
            self.record(data_disk_ids=[disks[0]['DiskId']])
            self.tools.call([self.tools.aliyun, 'ossutil', 'api', 'put-object', '--bucket', resources['evidence_bucket'],
                '--key', source['key'], '--body', 'file://' + str(self.directory / 'sources.tar.gz'), '--forbid-overwrite',
                '--object-acl', 'private', '--server-side-encryption', 'AES256', '--endpoint', resources['evidence_endpoint'],
                '--retry-times', '0'], timeout=self.timeout(180))
            while not self.ready():
                self.remaining('preflight_deadline_utc', 1)
                time.sleep(5)
            self.record(status='DIRECT_START_INTENT')
            receipt = self.command(launcher(self.remote_config(), source), self.remaining('preflight_deadline_utc', 900), 'install')
            require(receipt['status'] == 'DIRECT_STARTED' and receipt['campaign_id'] == self.state['campaign_id']
                    and receipt['instance_id'] == resources['instance_id'], 'DIRECT_START_BINDING_MISMATCH')
            require(re.fullmatch(r'[0-9a-f]{64}', receipt['execution_receipt_sha256']), 'EXECUTION_RECEIPT_REQUIRED')
            require(receipt['status_path'] == '/srv/scorpiofs-benchmark/evidence/' + self.state['campaign_id'] + '/status.json', 'OWNED_STATUS_PATH_REQUIRED')
            self.record(status='RUNNING', direct_start_receipt=receipt)
            self.monitor()
        except BaseException as error:
            self.record(campaign_result='FAILED_OR_INTERRUPTED', failure_type=type(error).__name__,
                        failure_code=str(error) if re.fullmatch(r'[A-Z_]+', str(error)) else 'UNSPECIFIED')
            raise
        finally:
            self.cleanup(automatic=True)

    def monitor(self):
        cutoff = utc(self.state['session_started_utc']) + timedelta(minutes=228)
        self.operation_deadline = time.monotonic() + max(0, (cutoff - now()).total_seconds())
        path = self.state['direct_start_receipt']['status_path']
        program = "import json;from pathlib import Path;p=Path(" + repr(path) + ");print(p.read_text() if p.is_file() else json.dumps({'status':'WAITING'}))"
        script = "#!/bin/bash\npython3 - <<'SCORPIO_STATUS_PY'\n" + program + '\nSCORPIO_STATUS_PY\n'
        while now() < cutoff:
            value = self.command(script, min(30, self.timeout(30)), 'status')
            if value.get('status') != 'WAITING':
                for key in ('campaign_id', 'instance_id', 'harness_sha', 'profile'):
                    expected = self.state['resources']['instance_id'] if key == 'instance_id' else self.cfg[key] if key in ('harness_sha', 'profile') else self.state[key]
                    require(value.get(key) == expected, 'REMOTE_STATUS_BINDING_MISMATCH')
                self.record(remote_status=value)
                if value['status'] in ('COMPLETE_VERIFIED', 'FAILED'):
                    if value.get('evidence_uploaded'):
                        self.collect(value)
                    require(value['status'] == 'COMPLETE_VERIFIED', 'NATIVE_CAMPAIGN_FAILED')
                    self.record(campaign_result='COMPLETE_VERIFIED')
                    return
            time.sleep(min(30, max(0, (cutoff - now()).total_seconds())))
        raise TimeoutError('CAMPAIGN_COLLECTION_RESERVE_REACHED')

    def collect(self, status):
        self.operation_deadline = time.monotonic() + self.remaining('collection_deadline_utc', 300)
        require(type(status.get('evidence_bytes')) is int and 0 < status['evidence_bytes'] <= MAX_EVIDENCE
                and re.fullmatch(r'[0-9a-f]{64}', status.get('evidence_sha256', '')), 'INVALID_SAFE_ARCHIVE_RECEIPT')
        resources = self.state['resources']
        archive = self.directory / 'safe-evidence.tar.gz'
        self.tools.call([self.tools.aliyun, 'ossutil', 'cp', 'oss://' + resources['evidence_bucket'] + '/' + self.state['evidence_object'],
            str(archive), '--endpoint', resources['evidence_endpoint'], '--retry-times', '0'], timeout=self.timeout(120))
        require(archive.stat().st_size == status['evidence_bytes'] and direct_sources.digest(archive) == status['evidence_sha256'], 'EVIDENCE_DOWNLOAD_MISMATCH')
        root = self.directory / 'safe-evidence'
        extract_evidence(archive, root)
        validate_evidence(root, self.state, require_complete=status['status'] == 'COMPLETE_VERIFIED')
        self.record(evidence_local_copy=str(archive), evidence_sha256=status['evidence_sha256'], evidence_bytes=status['evidence_bytes'], evidence_replay_verified=True)

    def inventory(self, product, action, outer, inner, filters):
        items = []
        for page in range(1, 101):
            value = self.api(product, action, '--RegionId', self.cfg['region'], *filters,
                '--PageNumber', str(page), '--PageSize', '50', cap=15)
            rows = value[outer][inner]
            require(type(rows) is list and type(value['TotalCount']) is int, 'INVALID_INVENTORY')
            items.extend(rows)
            if len(items) >= value['TotalCount']:
                return items
            require(bool(rows), 'INVENTORY_INCOMPLETE')
        raise ValueError('INVENTORY_PAGE_LIMIT')

    def owned_instances(self):
        return self.inventory('ecs', 'DescribeInstances', 'Instances', 'Instance', [
            '--Tag.1.Key', 'run_id', '--Tag.1.Value', self.state['campaign_id']])

    def cleanup(self, automatic=False):
        seconds = max(0, (utc(self.state['hard_release_utc']) - now()).total_seconds()) if automatic else 600
        self.operation_deadline = time.monotonic() + seconds
        self.record(status='CLEANUP_STARTED')
        errors = []
        stopped = False
        try:
            for row in self.owned_instances():
                require(row['InstanceName'] == 'scorpiofs-' + self.state['campaign_id'], 'CLEANUP_INSTANCE_BINDING_MISMATCH')
                if self.state.get('resources'):
                    require(row['InstanceId'] == self.state['resources']['instance_id'], 'CLEANUP_INSTANCE_ID_MISMATCH')
                self.api('ecs', 'DeleteInstance', '--region', self.cfg['region'], '--InstanceId', row['InstanceId'], '--Force', 'true')
            end = time.monotonic() + self.timeout(60)
            while time.monotonic() < end:
                if not self.owned_instances():
                    stopped = True
                    break
                time.sleep(3)
        except Exception:
            pass
        # Stop writers before removing exact owned objects. Destroy is attempted
        # even if inventory/transfer failed, and retried after the writers retire.
        for attempt in range(2):
            if stopped:
                try:
                    identity = self.state['campaign_id']
                    for key in ('sources/' + identity + '/sources.tar.gz', 'runs/' + identity + '/safe-evidence.tar.gz'):
                        self.tools.call([self.tools.aliyun, 'ossutil', 'api', 'delete-object', '--bucket', 'scorpiofs-bench-' + identity,
                            '--key', key, '--endpoint', 'https://oss-' + self.cfg['region'] + '.aliyuncs.com', '--retry-times', '0'], timeout=self.timeout(20))
                except Exception:
                    # A partial apply may never have created the bucket; final
                    # inventories establish absence without masking a 403.
                    pass
            try:
                self.tf('destroy', '-auto-approve', '-input=false', cap=240)
                break
            except Exception:
                if attempt:
                    errors.append('TERRAFORM_DESTROY_FAILED')
            try:
                stopped = not self.owned_instances()
            except Exception:
                stopped = False
        try:
            audit = self.audit()
            save(self.directory / 'residual-audit.json', audit)
            require(not any(audit.values()), 'RESIDUAL_RESOURCES')
        except Exception:
            errors.append('RESIDUAL_AUDIT_FAILED')
        self.record(status='CLEANUP_FAILED' if errors else 'CLEANED', cleanup_errors=errors)
        require(not errors, 'CLOUD_CLEANUP_REQUIRES_ATTENTION')

    def ram_inventory(self, action, outer, inner, name_field, expected):
        rows, marker = [], None
        for _ in range(100):
            args = ['--MaxItems', '1000']
            if action == 'ListPolicies':
                args += ['--PolicyType', 'Custom']
            if marker:
                args += ['--Marker', marker]
            value = self.api('ram', action, *args, cap=15)
            rows.extend(row for row in value[outer][inner] if row[name_field] == expected)
            require(type(value['IsTruncated']) is bool, 'INVALID_RAM_PAGINATION')
            if not value['IsTruncated']:
                return rows
            require(value.get('Marker') and value['Marker'] != marker, 'INVALID_RAM_MARKER')
            marker = value['Marker']
        raise ValueError('RAM_INVENTORY_PAGE_LIMIT')

    def audit(self):
        identity = self.state['campaign_id']
        specs = (('ecs', 'DescribeInstances', 'Instances', 'Instance'), ('ecs', 'DescribeDisks', 'Disks', 'Disk'),
                 ('ecs', 'DescribeSecurityGroups', 'SecurityGroups', 'SecurityGroup'), ('vpc', 'DescribeVpcs', 'Vpcs', 'Vpc'),
                 ('vpc', 'DescribeVSwitches', 'VSwitches', 'VSwitch'))
        result = {action: self.inventory(product, action, outer, inner, ['--Tag.1.Key', 'run_id', '--Tag.1.Value', identity])
                  for product, action, outer, inner in specs}
        result['NamedWorkDisks'] = self.inventory('ecs', 'DescribeDisks', 'Disks', 'Disk', ['--DiskName', 'scorpiofs-' + identity + '-work'])
        if self.state.get('data_disk_ids'):
            result['RecordedWorkDisks'] = self.inventory('ecs', 'DescribeDisks', 'Disks', 'Disk', ['--DiskIds', json.dumps(self.state['data_disk_ids'])])
        bucket = 'scorpiofs-bench-' + identity
        value = self.api('ossutil', 'api', 'list-buckets', '--prefix', bucket, '--max-keys', '100', '--output-format', 'json', '--retry-times', '0', cap=15)
        result['OSS'] = oss_buckets(value, bucket)
        result['RAMRoles'] = self.ram_inventory('ListRoles', 'Roles', 'Role', 'RoleName', 'scorpiofs-' + identity)
        result['RAMPolicies'] = self.ram_inventory('ListPolicies', 'Policies', 'Policy', 'PolicyName', 'scorpiofs-' + identity + '-transfer')
        result['TerraformState'] = self.tf('state', 'list', cap=15).decode().splitlines()
        return result


def oss_buckets(value, expected):
    require(type(value) is dict, 'INVALID_OSS_INVENTORY')
    # ListBuckets omits false/empty XML fields for an empty successful result.
    # Accept that observed shape only with its owner envelope and no cursor.
    empty = ('IsTruncated' not in value and value.get('Buckets') is None
             and type(value.get('Owner')) is dict and set(value['Owner']) == {'ID', 'DisplayName'}
             and all(type(item) is str and item for item in value['Owner'].values())
             and not value.get('NextMarker'))
    require(value.get('IsTruncated') is False or value.get('IsTruncated') == 'false' or empty, 'OSS_INVENTORY_TRUNCATED')
    container = value.get('Buckets')
    require(container is None or type(container) is dict, 'INVALID_OSS_INVENTORY')
    rows = None if container is None else container.get('Bucket')
    rows = [] if rows is None else [rows] if type(rows) is dict else rows
    require(type(rows) is list and all(type(row) is dict and type(row.get('Name')) is str for row in rows), 'INVALID_OSS_INVENTORY')
    return [row for row in rows if row['Name'] == expected]


def extract_evidence(archive, root):
    require(not root.exists(), 'FRESH_EVIDENCE_DIRECTORY_REQUIRED')
    with tarfile.open(archive, 'r:gz') as stream:
        members = stream.getmembers()
        require(0 < len(members) <= 257 and len({m.name for m in members}) == len(members), 'INVALID_EVIDENCE_MEMBERS')
        require(sum(m.size for m in members) <= MAX_EXPANDED_EVIDENCE, 'EXPANDED_EVIDENCE_TOO_LARGE')
        for member in members:
            path = PurePosixPath(member.name)
            require(member.isfile() and not path.is_absolute() and '..' not in path.parts
                    and path.as_posix() == member.name and '\\' not in member.name and ':' not in member.name, 'UNSAFE_EVIDENCE_PATH')
        root.mkdir(mode=0o700)
        for member in members:
            path = root / member.name
            path.parent.mkdir(parents=True, exist_ok=True)
            with stream.extractfile(member) as source, path.open('xb') as target:
                shutil.copyfileobj(source, target)


def validate_evidence(root, state, *, require_complete):
    sys.path.insert(0, str(HERE.parents[1]))
    import workspace_update_campaign_export as exporter
    import workspace_update_execution as execution
    manifest = json.loads((root / 'safe-export.json').read_bytes())
    require(set(manifest) == {'revision', 'files_sha256', 'complete_campaign', 'private_logs_exported'}
            and manifest['revision'] == 1 and manifest['private_logs_exported'] is False, 'INVALID_SAFE_EXPORT')
    if state['config'].get('request_endpoint_diagnostic', False):
        require(not require_complete and manifest['complete_campaign'] is False, 'DIAGNOSTIC_IS_NOT_FORMAL_EVIDENCE')
    files = manifest['files_sha256']
    require(type(files) is dict and len(files) <= 256, 'INVALID_EXPORT_FILES')
    require('request-diagnostic-mode.json' not in files or state['config'].get('request_endpoint_diagnostic', False),
            'UNREQUESTED_ENDPOINT_DIAGNOSTIC')
    actual, total = set(), 0
    for path in root.rglob('*'):
        require(not path.is_symlink(), 'SYMLINK_IN_EVIDENCE')
        if not path.is_file():
            continue
        relative = path.relative_to(root).as_posix()
        actual.add(relative)
        total += path.stat().st_size
        require(total <= MAX_EXPANDED_EVIDENCE, 'EVIDENCE_TOO_LARGE')
        if relative == 'safe-export.json':
            continue
        require(relative in files and (exporter.allowed(Path(relative)) or relative in
                ('run.json', 'server-build.json', 'client-a-build.json', 'client-b-build.json')
                or relative in exporter.GIT_PERFORMANCE_FILES), 'UNEXPECTED_EXPORT_FILE')
        require(direct_sources.digest(path) == files[relative], 'EVIDENCE_HASH_MISMATCH')
    require(actual == set(files) | {'safe-export.json'}, 'MISSING_EXPORT_FILE')
    git_files = set(files) & exporter.GIT_PERFORMANCE_FILES
    require(not git_files or git_files == exporter.GIT_PERFORMANCE_FILES, 'INCOMPLETE_GIT_PERFORMANCE_EXPORT')
    require(not (require_complete or manifest['complete_campaign']) or bool(git_files), 'MISSING_GIT_PERFORMANCE_EXPORT')
    if git_files:
        exporter.validate_git_performance_export(root, require_complete=require_complete or manifest['complete_campaign'],
                                                deadline_utc=state['collection_deadline_utc'])
    metadata = exporter.validate_run_metadata(json.loads((root / 'run.json').read_bytes()))
    expected = {'execution_provider': 'aliyun-direct', 'campaign_id': state['campaign_id'],
        'instance_id': state['resources']['instance_id'], 'execution_receipt_sha256': state['direct_start_receipt']['execution_receipt_sha256'],
        'run_id': execution.run_id_for_campaign(state['campaign_id']), 'attempt': '1', 'rounds': 3, 'comparison': 'isolated',
        'bootstrap_commit_time': 1700000000, 'harness_sha': state['config']['harness_sha'], 'mega_sha': SERVER,
        'baseline_sha': BASELINE, 'candidate_sha': CANDIDATE, 'profile': state['config']['profile'],
        **{key: state[key] for key in ('session_started_utc', 'session_deadline_utc', 'hard_release_utc')}}
    require(all(metadata.get(key) == value for key, value in expected.items()), 'EVIDENCE_RUN_BINDING_MISMATCH')
    exporter.validate_request_diagnostics(root, run_metadata=metadata)
    if require_complete or manifest['complete_campaign']:
        require(manifest['complete_campaign'] is True, 'SUCCESS_REQUIRES_COMPLETE_CAMPAIGN')
        exporter.validate_complete(root, run_metadata=metadata, deadline_utc=state['collection_deadline_utc'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('plan', 'start', 'cleanup', 'audit'))
    parser.add_argument('--state-dir', required=True)
    parser.add_argument('--config')
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--terraform', default='terraform')
    parser.add_argument('--aliyun', default='aliyun')
    args = parser.parse_args()
    if args.action == 'plan':
        require(args.config and not args.execute, 'LOCAL_PLAN_REQUIRES_CONFIG')
        result = plan(json.loads(Path(args.config).read_bytes()), args.state_dir)
    else:
        require(args.execute or args.action == 'audit', 'EXPLICIT_EXECUTION_REQUIRED')
        campaign = Campaign(args.state_dir, Tools(args.terraform, args.aliyun))
        if args.action == 'start':
            campaign.run()
        elif args.action == 'cleanup':
            campaign.cleanup()
        else:
            campaign.operation_deadline = time.monotonic() + 120
            save(campaign.directory / 'residual-audit.json', campaign.audit())
        result = campaign.state
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
