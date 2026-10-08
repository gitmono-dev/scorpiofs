"""Prepare and operate one disposable Aliyun Actions campaign, never on import.

`plan` is local only. `start --execute` is the explicit cloud execution entry.
State, Terraform working files and evidence must live outside the repository.
The existing workflow owns all native process/mount correctness checks.
"""

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import ipaddress
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import time
import uuid


HERE = Path(__file__).resolve().parent
REPO = "gitmono-dev/scorpiofs"
WORKFLOW = "mst2-real-update.yml"
RUNNER_URL = "https://github.com/actions/runner/releases/download/v2.338.0/actions-runner-linux-x64-2.338.0.tar.gz"
RUNNER_SHA256 = "af4b794c1bc41d73d40535e3fe092a39f9679cd8d965954c2aca25a05ca41d32"
SERVER = "75a1d081e465c531f396a0d3b22d18a45f942f9a"
BASELINE = "d265e31169fb2f8b137ce9922784ebd0238c6397"
CANDIDATE = "f18b99645d7bbbc5b4ea3374165e57dc4ff9f922"
MAX_EVIDENCE_BYTES = 512 * 1024 * 1024
CONFIG_FIELDS = {"region", "zone", "image_id", "instance_type", "vpc_cidr", "vswitch_cidr",
                 "ssh_operator_cidrs", "ssh_public_key", "ssh_identity_file", "harness_ref",
                 "harness_sha", "profile"}


def require(ok, code):
    if not ok:
        raise ValueError(code)


def utc(value):
    require(type(value) is str and re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", value), "INVALID_UTC")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def stamp(value):
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now():
    return datetime.now(timezone.utc)


def config(value):
    require(type(value) is dict and set(value) == CONFIG_FIELDS, "INVALID_CONFIG_FIELDS")
    for key in ("region", "zone", "image_id", "instance_type", "harness_ref"):
        require(type(value[key]) is str and re.fullmatch(r"[A-Za-z0-9_./-]{2,160}", value[key])
                and not value[key].startswith("-"), "INVALID_CONFIG_VALUE")
    require(value["zone"].startswith(value["region"] + "-"), "ZONE_REGION_MISMATCH")
    require(value["instance_type"] == "ecs.u1-c1m4.2xlarge", "REVIEWED_NON_BURSTABLE_SKU_REQUIRED")
    require(re.fullmatch(r"[0-9a-f]{40}", value["harness_sha"] or ""), "IMMUTABLE_HARNESS_REQUIRED")
    require(value["profile"] in ("smoke", "medium", "large"), "INVALID_PROFILE")
    parent = ipaddress.IPv4Network(value["vpc_cidr"])
    child = ipaddress.IPv4Network(value["vswitch_cidr"])
    require(child.subnet_of(parent), "SUBNET_OUTSIDE_VPC")
    require(type(value["ssh_operator_cidrs"]) is list and len(value["ssh_operator_cidrs"]) == 1,
            "ONE_OPERATOR_IP_REQUIRED")
    operator = ipaddress.IPv4Network(value["ssh_operator_cidrs"][0])
    require(operator.prefixlen == 32 and operator.network_address.is_global, "PUBLIC_OPERATOR_32_REQUIRED")
    require(type(value["ssh_public_key"]) is str and
            re.fullmatch(r"(?:ssh-ed25519|ssh-rsa) [A-Za-z0-9+/]+={0,3}(?: [^\r\n]+)?", value["ssh_public_key"]),
            "PUBLIC_KEY_REQUIRED")
    require(type(value["ssh_identity_file"]) is str and Path(value["ssh_identity_file"]).is_absolute(),
            "ABSOLUTE_IDENTITY_PATH_REQUIRED")
    return dict(value)


def schedule(start):
    return {"session_started_utc": stamp(start), "preflight_deadline_utc": stamp(start + timedelta(minutes=15)),
            "work_cleanup_deadline_utc": stamp(start + timedelta(minutes=220)),
            "collection_deadline_utc": stamp(start + timedelta(minutes=233)),
            "session_deadline_utc": stamp(start + timedelta(minutes=235)),
            "hard_release_utc": stamp(start + timedelta(minutes=240))}


def save(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def external_directory(path):
    path = Path(path).resolve()
    repository = HERE.parents[3]
    require(not path.is_relative_to(repository), "STATE_MUST_BE_OUTSIDE_REPOSITORY")
    require(not path.exists(), "FRESH_STATE_DIRECTORY_REQUIRED")
    return path


def plan(value, state_dir):
    value = config(value)
    state_dir = external_directory(state_dir)
    state_dir.mkdir(parents=True, mode=0o700)
    # Preparation does not start the clock, register a runner or call any API.
    identity = "v3-" + uuid.uuid4().hex[:20]
    state = {"revision": 1, "campaign_id": identity, "runner_label": "scorpiofs-" + identity,
             "repository": REPO, "config": value, "status": "PREPARED_LOCAL_ONLY"}
    save(state_dir / "campaign.json", state)
    return state


class Tools:
    def __init__(self, terraform="terraform", gh="gh", aliyun="aliyun", ssh="ssh"):
        self.terraform, self.gh, self.aliyun, self.ssh = terraform, gh, aliyun, ssh

    def call(self, arguments, *, data=None, timeout=60, cwd=None):
        try:
            result = subprocess.run(arguments, input=data, capture_output=True, timeout=max(.1, timeout), cwd=cwd)
        except (OSError, subprocess.TimeoutExpired):
            raise RuntimeError("TOOL_FAILED_OR_TIMED_OUT") from None
        # Do not copy arbitrary tool stderr, environment or registration secrets to logs.
        if result.returncode:
            raise RuntimeError("TOOL_NONZERO_EXIT")
        return result.stdout

    def api(self, path, *, data=None, timeout=60):
        args = [self.gh, "api", path]
        if data is not None:
            args += ["--method", "POST", "--input", "-"]
        raw = self.call(args, data=None if data is None else json.dumps(data).encode(), timeout=timeout)
        return json.loads(raw) if raw.strip() else None


class Campaign:
    def __init__(self, directory, tools=None):
        self.directory = Path(directory).resolve(strict=True)
        self.path = self.directory / "campaign.json"
        self.state = json.loads(self.path.read_bytes())
        require(self.state["revision"] == 1 and self.state["repository"] == REPO, "INVALID_STATE")
        require(re.fullmatch(r"v3-[0-9a-f]{20}", self.state["campaign_id"]), "INVALID_CAMPAIGN")
        require(self.state["runner_label"] == "scorpiofs-" + self.state["campaign_id"], "INVALID_RUNNER_LABEL")
        self.cfg = config(self.state["config"])
        self.tools = tools or Tools()
        self.operation_deadline = None
        if "session_started_utc" in self.state:
            expected = schedule(utc(self.state["session_started_utc"]))
            require(all(self.state.get(k) == v for k, v in expected.items()), "IMMUTABLE_WINDOW_MISMATCH")

    def record(self, **values):
        self.state.update(values)
        save(self.path, self.state)

    def remaining(self, field, cap):
        seconds = (utc(self.state[field]) - now()).total_seconds()
        require(seconds > 0, "ORIGINAL_DEADLINE_EXPIRED")
        return min(cap, seconds)

    def operation_timeout(self, cap):
        if self.operation_deadline is None:
            return cap
        seconds = self.operation_deadline - time.monotonic()
        require(seconds > 0, "OPERATION_DEADLINE_EXPIRED")
        return min(cap, seconds)

    def tf(self, *arguments, cap=300):
        return self.tools.call([self.tools.terraform, "-chdir=" + str(self.directory / "terraform"), *arguments],
                               timeout=self.remaining("hard_release_utc", cap))

    def workflow_inputs(self):
        return {"runner_label": self.state["runner_label"], "mega_sha": SERVER, "comparison": "isolated",
                "baseline_sha": BASELINE, "candidate_sha": CANDIDATE, "profile": self.cfg["profile"],
                "rounds": "3", "session_started_utc": self.state["session_started_utc"],
                "session_deadline_utc": self.state["session_deadline_utc"], "recover_original_window": "false",
                "workspace_read_profile": "false", "bootstrap_commit_time": "1700000000"}

    def title(self):
        return "MST2 v3 " + self.state["runner_label"] + " " + self.state["session_started_utc"]

    def run(self):
        require(self.state["status"] == "PREPARED_LOCAL_ONLY", "CAMPAIGN_CANNOT_BE_RESTARTED")
        # Check repository permissions and the frozen harness before creating chargeable resources.
        resolved = self.tools.api(f"repos/{REPO}/commits/{self.cfg['harness_ref']}")
        require(resolved["sha"] == self.cfg["harness_sha"], "HARNESS_REF_MOVED")
        self.admit_cloud_shape()
        self.tools.api(f"repos/{REPO}/actions/runners?per_page=1")
        token = self.tools.api(f"repos/{REPO}/actions/runners/registration-token", data={})["token"]
        work = self.directory / "terraform"
        shutil.copytree(HERE / "terraform", work, ignore=shutil.ignore_patterns(".terraform", "*.tfstate*", "*.tfplan", "*.tfvars", "*.tfvars.json"))
        self.record(**schedule(now()), status="PROVISIONING")
        variables = {k: v for k, v in self.cfg.items() if k not in ("ssh_identity_file", "harness_ref", "harness_sha", "profile")}
        variables.update(run_id=self.state["campaign_id"], session_started_utc=self.state["session_started_utc"],
                         expires_at=self.state["hard_release_utc"])
        save(work / "campaign.auto.tfvars.json", variables)
        try:
            self.tf("init", "-input=false", "-lockfile=readonly", cap=120)
            self.tf("validate", cap=30)
            self.tf("plan", "-input=false", "-out=campaign.tfplan", cap=self.remaining("preflight_deadline_utc", 120))
            self.tf("apply", "-input=false", "campaign.tfplan", cap=self.remaining("preflight_deadline_utc", 600))
            outputs = json.loads(self.tf("output", "-json", "campaign", cap=10))
            self.bind_outputs(outputs)
            self.record(resources=outputs)
            disks = self.inventory("ecs", "DescribeDisks", "Disks", "Disk", [
                "--InstanceId", outputs["instance_id"], "--DiskName", "scorpiofs-" + self.state["campaign_id"] + "-work"])
            require(len(disks) == 1 and disks[0]["DeleteWithInstance"] is True, "OWNED_DATA_DISK_REQUIRED")
            self.record(data_disk_ids=[disks[0]["DiskId"]])
            payload = {"revision": 1, "campaign_id": self.state["campaign_id"], "repository": REPO,
                       "runner_label": self.state["runner_label"], "register_token": token,
                       "runner_url": RUNNER_URL, "runner_sha256": RUNNER_SHA256,
                       "session_started_utc": self.state["session_started_utc"],
                       "session_deadline_utc": self.state["session_deadline_utc"],
                       "hard_release_utc": self.state["hard_release_utc"]}
            self.install(payload)
            token = None
            self.remaining("preflight_deadline_utc", 1)
            resolved = self.tools.api(f"repos/{REPO}/commits/{self.cfg['harness_ref']}")
            require(resolved["sha"] == self.cfg["harness_sha"], "HARNESS_REF_MOVED")
            self.record(status="DISPATCH_INTENT")
            self.tools.api(f"repos/{REPO}/actions/workflows/{WORKFLOW}/dispatches", data={
                "ref": self.cfg["harness_ref"], "inputs": self.workflow_inputs()})
            self.record(status="RUNNING")
            self.monitor()
            self.record(campaign_result="COMPLETE_VERIFIED")
        except BaseException:
            self.record(campaign_result="FAILED_OR_INTERRUPTED")
            raise
        finally:
            token = None
            self.cleanup(automatic=True)

    def admit_cloud_shape(self):
        raw = self.tools.call([self.tools.aliyun, "ecs", "DescribeInstanceTypes", "--region", self.cfg["region"],
                               "--InstanceTypes.1", self.cfg["instance_type"]], timeout=30)
        types = json.loads(raw)["InstanceTypes"]["InstanceType"]
        require(len(types) == 1 and types[0]["InstanceTypeId"] == self.cfg["instance_type"]
                and types[0]["CpuCoreCount"] == 8 and types[0]["MemorySize"] >= 32, "REVIEWED_SKU_SHAPE_REQUIRED")
        raw = self.tools.call([self.tools.aliyun, "ecs", "DescribeImages", "--RegionId", self.cfg["region"],
                               "--ImageId", self.cfg["image_id"]], timeout=30)
        images = json.loads(raw)["Images"]["Image"]
        require(len(images) == 1 and images[0]["ImageId"] == self.cfg["image_id"]
                and images[0]["Architecture"] == "x86_64" and images[0]["Platform"] == "Ubuntu"
                and images[0]["Status"] == "Available", "REVIEWED_UBUNTU_IMAGE_REQUIRED")

    def bind_outputs(self, value):
        expected = {"run_id": self.state["campaign_id"], "region": self.cfg["region"],
                    "session_started_utc": self.state["session_started_utc"], "expires_at": self.state["hard_release_utc"],
                    "evidence_bucket": "scorpiofs-bench-" + self.state["campaign_id"],
                    "evidence_prefix": "runs/" + self.state["campaign_id"] + "/",
                    "evidence_endpoint": "https://oss-" + self.cfg["region"] + ".aliyuncs.com",
                    "native_storage_backend": "local"}
        require(all(value.get(k) == v for k, v in expected.items()), "RESOURCE_BINDING_MISMATCH")
        require(ipaddress.IPv4Address(value["public_ip"]).is_global, "PUBLIC_ECS_IP_REQUIRED")

    def install(self, payload):
        script = (HERE / "install_runner.py").read_bytes()
        # The script and its private payload travel only through SSH stdin. No token in argv/files.
        import base64
        envelope = base64.b64encode(json.dumps(payload).encode()).decode("ascii")
        # The installer main reads the remainder of stdin; avoid python - reading the whole stream.
        program = "import base64,io,sys;sys.stdin=io.TextIOWrapper(io.BytesIO(base64.b64decode('" + envelope + "')));exec(" + repr(script) + ")"
        host = self.state["resources"]["public_ip"]
        args = [self.tools.ssh, "-i", self.cfg["ssh_identity_file"], "-o", "BatchMode=yes",
                "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=accept-new", "-o",
                "UserKnownHostsFile=" + str(self.directory / "known_hosts"), "-o", "ConnectTimeout=10",
                "root@" + host]
        # Readiness/SSH probes cannot consume or re-register the one-use runner token.
        while True:
            try:
                self.tools.call(args + ["test -f /var/lib/scorpiofs-benchmark/bootstrap-ready.json"],
                                timeout=self.remaining("preflight_deadline_utc", 12))
                break
            except RuntimeError:
                self.remaining("preflight_deadline_utc", 1)
                time.sleep(2)
        # Installer may succeed before a transport failure. Never retry this invocation.
        self.tools.call(args + ["python3 -"], data=program.encode(),
                        timeout=self.remaining("preflight_deadline_utc", 180))

    def discover_run(self):
        matches = []
        for page in range(1, 11):
            result = self.tools.api(f"repos/{REPO}/actions/workflows/{WORKFLOW}/runs?event=workflow_dispatch&head_sha={self.cfg['harness_sha']}&per_page=100&page={page}",
                                    timeout=self.operation_timeout(30))
            rows = result["workflow_runs"]
            matches += [row for row in rows if row["display_title"] == self.title() and row["head_sha"] == self.cfg["harness_sha"]]
            if len(rows) < 100:
                break
        else:
            raise ValueError("RUN_SEARCH_PAGINATION_EXHAUSTED")
        require(len(matches) <= 1, "AMBIGUOUS_DISPATCH")
        if matches:
            self.record(github_run_id=str(matches[0]["id"]))
            return matches[0]
        return None

    def monitor(self):
        cutoff = utc(self.state["session_started_utc"]) + timedelta(minutes=228)
        self.operation_deadline = time.monotonic() + max(0, (cutoff - now()).total_seconds())
        while now() < cutoff:
            row = self.discover_run()
            if row and row["status"] == "completed":
                self.record(github_conclusion=row["conclusion"], github_attempt=str(row["run_attempt"]))
                self.collect()
                require(row["conclusion"] == "success", "BENCHMARK_WORKFLOW_FAILED")
                return
            # Short sleeps keep cancellation responsive; all deadlines retain the first-create anchor.
            time.sleep(min(20, max(0, (cutoff - now()).total_seconds())))
        raise TimeoutError("CAMPAIGN_COLLECTION_RESERVE_REACHED")

    def collect(self):
        destination = self.directory / "safe-evidence"
        artifact = f"mst2-v3-evidence-{self.state['github_run_id']}-{self.state['github_attempt']}"
        self.tools.call([self.tools.gh, "run", "download", self.state["github_run_id"], "-R", REPO,
                         "-n", artifact, "-D", str(destination)],
                        timeout=self.remaining("collection_deadline_utc", 120))
        validate_evidence(destination, self.state)
        archive = self.directory / "safe-evidence.tar.gz"
        with tarfile.open(archive, "w:gz") as stream:
            for path in sorted(destination.rglob("*")):
                if path.is_file():
                    stream.add(path, arcname=path.relative_to(destination).as_posix(), recursive=False)
        require(archive.stat().st_size <= MAX_EVIDENCE_BYTES, "ARCHIVE_TOO_LARGE")
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        resources = self.state["resources"]
        key = resources["evidence_prefix"] + "safe-evidence.tar.gz"
        # Persist exact object ownership before PutObject, including uncertain network outcomes.
        self.record(evidence_object=key, evidence_sha256=digest, evidence_bytes=archive.stat().st_size)
        args = [self.tools.aliyun, "ossutil", "api", "put-object", "--bucket", resources["evidence_bucket"],
                "--key", key, "--body", "file://" + str(archive), "--forbid-overwrite", "--object-acl", "private",
                "--server-side-encryption", "AES256", "--endpoint", resources["evidence_endpoint"], "--retry-times", "0"]
        self.tools.call(args, timeout=self.remaining("collection_deadline_utc", 60))
        downloaded = self.directory / "oss-roundtrip.tar.gz"
        self.tools.call([self.tools.aliyun, "ossutil", "cp", "oss://" + resources["evidence_bucket"] + "/" + key,
                         str(downloaded), "--endpoint", resources["evidence_endpoint"], "--retry-times", "0"],
                        timeout=self.remaining("collection_deadline_utc", 60))
        require(hashlib.sha256(downloaded.read_bytes()).hexdigest() == digest, "OSS_ROUNDTRIP_MISMATCH")
        self.record(oss_roundtrip_verified=True, evidence_local_copy=str(archive))

    def cleanup(self, automatic=False):
        # Automatic cleanup shares S+240. An explicit later cleanup is a bounded
        # recovery operation and never grants a new campaign or compute window.
        seconds = (utc(self.state["hard_release_utc"]) - now()).total_seconds() if automatic else 10 * 60
        self.operation_deadline = time.monotonic() + max(0, seconds)
        cleanup_limit = self.operation_deadline
        # Inventory/cancel/OSS errors get at most 30 seconds before destroy.
        # They must not consume the five-minute compute-destruction allowance.
        self.operation_deadline = min(cleanup_limit, time.monotonic() + 30)
        self.record(status="CLEANUP_STARTED")
        errors = []
        if not self.state.get("github_run_id") and self.state.get("session_started_utc"):
            try:
                self.discover_run()
            except Exception:
                errors.append("RUN_DISCOVERY_FAILED")
        if self.state.get("github_run_id"):
            try:
                run = self.tools.api(f"repos/{REPO}/actions/runs/{self.state['github_run_id']}", timeout=self.operation_timeout(15))
                require(run["head_sha"] == self.cfg["harness_sha"] and run["display_title"] == self.title(), "RUN_BINDING_MISMATCH")
                if run["status"] != "completed":
                    self.tools.api(f"repos/{REPO}/actions/runs/{self.state['github_run_id']}/cancel", data={}, timeout=self.operation_timeout(15))
            except Exception:
                errors.append("RUN_CANCEL_FAILED")
        key = self.state.get("evidence_object")
        resources = self.state.get("resources")
        if key and resources:
            try:
                self.bind_outputs(resources)
                require(key == resources["evidence_prefix"] + "safe-evidence.tar.gz", "OBJECT_BINDING_MISMATCH")
                # Local bytes and GitHub artifact survive cloud cleanup even if OSS upload failed.
                archive = self.directory / "safe-evidence.tar.gz"
                require(hashlib.sha256(archive.read_bytes()).hexdigest() == self.state["evidence_sha256"], "LOCAL_EVIDENCE_CHANGED")
                self.tools.call([self.tools.aliyun, "ossutil", "api", "delete-object", "--bucket", resources["evidence_bucket"],
                                 "--key", key, "--endpoint", resources["evidence_endpoint"], "--retry-times", "0"], timeout=self.operation_timeout(20))
            except Exception:
                errors.append("EXACT_OBJECT_CLEANUP_FAILED")
        # Always attempt compute cleanup even if evidence/run operations failed.
        self.operation_deadline = cleanup_limit
        try:
            self.tools.call([self.tools.terraform, "-chdir=" + str(self.directory / "terraform"),
                             "destroy", "-auto-approve", "-input=false"], timeout=self.operation_timeout(300))
        except Exception:
            errors.append("TERRAFORM_DESTROY_FAILED")
        # Stop the VM/config process before removing runner registrations: a
        # failed SSH connection may have left an in-flight registration behind.
        try:
            self.remove_runner()
        except Exception:
            errors.append("RUNNER_REGISTRATION_CLEANUP_FAILED")
        try:
            audit = self.audit()
            save(self.directory / "residual-audit.json", audit)
            require(not any(audit.values()), "RESIDUAL_RESOURCES")
        except Exception:
            errors.append("RESIDUAL_AUDIT_FAILED")
        self.record(status="CLEANUP_FAILED" if errors else "CLEANED", cleanup_errors=errors)
        if errors:
            raise RuntimeError("CLOUD_CLEANUP_REQUIRES_ATTENTION")

    def owned_runners(self):
        matches = []
        for page in range(1, 101):
            value = self.tools.api(f"repos/{REPO}/actions/runners?per_page=100&page={page}", timeout=self.operation_timeout(15))
            for runner in value["runners"]:
                labels = {item["name"] for item in runner["labels"]}
                if runner["name"] == self.state["runner_label"] and self.state["runner_label"] in labels:
                    matches.append(runner["id"])
            if page * 100 >= value["total_count"]:
                break
        else:
            raise ValueError("RUNNER_INVENTORY_PAGE_LIMIT")
        require(len(matches) <= 1, "AMBIGUOUS_RUNNER_IDENTITY")
        return matches

    def remove_runner(self):
        for runner_id in self.owned_runners():
            self.tools.call([self.tools.gh, "api", "--method", "DELETE", f"repos/{REPO}/actions/runners/{runner_id}"], timeout=self.operation_timeout(15))

    def inventory(self, product, action, outer, inner, filters):
        items = []
        for page in range(1, 101):
            raw = self.tools.call([self.tools.aliyun, product, action, "--RegionId", self.cfg["region"],
                *filters, "--PageNumber", str(page), "--PageSize", "50"], timeout=self.operation_timeout(15))
            value = json.loads(raw)
            rows = value[outer][inner]
            items.extend(rows)
            require(type(value["TotalCount"]) is int, "INVENTORY_TOTAL_REQUIRED")
            if len(items) >= value["TotalCount"]:
                return items
            require(bool(rows), "INVENTORY_INCOMPLETE")
        raise ValueError("INVENTORY_PAGE_LIMIT")

    def audit(self):
        # These read-only inventories include all pages; empty Terraform state alone is insufficient.
        specs = (("ecs", "DescribeInstances", "Instances", "Instance"),
                 ("ecs", "DescribeDisks", "Disks", "Disk"),
                 ("ecs", "DescribeSecurityGroups", "SecurityGroups", "SecurityGroup"),
                 ("vpc", "DescribeVpcs", "Vpcs", "Vpc"),
                 ("vpc", "DescribeVSwitches", "VSwitches", "VSwitch"),
                 ("ecs", "DescribeKeyPairs", "KeyPairs", "KeyPair"))
        result = {}
        for product, action, outer, inner in specs:
            result[action] = self.inventory(product, action, outer, inner, [
                "--Tag.1.Key", "run_id", "--Tag.1.Value", self.state["campaign_id"]])
        # Inline ECS data disks do not inherit system volume_tags. Check their exact
        # run-owned name even when apply failed before outputs/disk IDs were saved.
        result["NamedWorkDisks"] = self.inventory("ecs", "DescribeDisks", "Disks", "Disk", [
            "--DiskName", "scorpiofs-" + self.state["campaign_id"] + "-work"])
        result["RecordedWorkDisks"] = (self.inventory("ecs", "DescribeDisks", "Disks", "Disk", [
            "--DiskIds", json.dumps(self.state["data_disk_ids"])]) if self.state.get("data_disk_ids") else [])
        # OSS prefix must match the entire unique bucket name, never delete by prefix.
        bucket = "scorpiofs-bench-" + self.state["campaign_id"]
        raw = self.tools.call([self.tools.aliyun, "ossutil", "api", "list-buckets", "--prefix", bucket,
                               "--max-keys", "100", "--output-format", "json", "--retry-times", "0"], timeout=self.operation_timeout(15))
        result["OSS"] = oss_buckets(json.loads(raw), bucket)
        result["TerraformState"] = self.tools.call([self.tools.terraform, "-chdir=" + str(self.directory / "terraform"),
                                                    "state", "list"], timeout=self.operation_timeout(15)).decode().splitlines()
        result["GitHubRunners"] = self.owned_runners()
        return result


def oss_buckets(value, expected):
    # ossutil's XML-to-JSON formatter preserves XML booleans as strings and
    # folds a single <Bucket> into an object; verified against the real CLI.
    require(type(value) is dict, "INVALID_OSS_INVENTORY")
    flag = value.get("IsTruncated")
    require(flag is False or type(flag) is str and flag == "false", "OSS_INVENTORY_TRUNCATED")
    container = value.get("Buckets")
    require(container is None or type(container) is dict, "INVALID_OSS_INVENTORY")
    rows = None if container is None else container.get("Bucket")
    rows = [] if rows is None else [rows] if type(rows) is dict else rows
    require(type(rows) is list and all(type(v) is dict and type(v.get("Name")) is str for v in rows), "INVALID_OSS_INVENTORY")
    return [v for v in rows if v["Name"] == expected]


def validate_evidence(root, state):
    sys.path.insert(0, str(HERE.parents[1]))
    import workspace_update_campaign_export as exporter
    manifest = json.loads((root / "safe-export.json").read_bytes())
    require(set(manifest) == {"revision", "files_sha256", "complete_campaign", "private_logs_exported"}
            and manifest["revision"] == 1 and manifest["private_logs_exported"] is False, "INVALID_SAFE_EXPORT")
    files = manifest["files_sha256"]
    require(type(files) is dict and len(files) <= 256, "INVALID_EXPORT_FILES")
    actual, total = set(), 0
    for path in root.rglob("*"):
        require(not path.is_symlink(), "SYMLINK_IN_EVIDENCE")
        if not path.is_file():
            continue
        relative = path.relative_to(root).as_posix()
        actual.add(relative)
        total += path.stat().st_size
        require(total <= MAX_EVIDENCE_BYTES, "EVIDENCE_TOO_LARGE")
        if relative == "safe-export.json":
            continue
        require(relative in files and (exporter.allowed(Path(relative)) or relative in
                ("run.json", "server-build.json", "client-a-build.json", "client-b-build.json")), "UNEXPECTED_EXPORT_FILE")
        require(hashlib.sha256(path.read_bytes()).hexdigest() == files[relative], "EVIDENCE_HASH_MISMATCH")
    require(actual == set(files) | {"safe-export.json"}, "MISSING_EXPORT_FILE")
    run = exporter.validate_run_metadata(json.loads((root / "run.json").read_bytes()))
    expected = {"run_id": state["github_run_id"], "attempt": state["github_attempt"],
                "harness_sha": state["config"]["harness_sha"], "mega_sha": SERVER,
                "baseline_sha": BASELINE, "candidate_sha": CANDIDATE, "profile": state["config"]["profile"],
                "session_started_utc": state["session_started_utc"], "session_deadline_utc": state["session_deadline_utc"]}
    require(all(run[k] == v for k, v in expected.items()), "EVIDENCE_RUN_BINDING_MISMATCH")
    if state.get("github_conclusion") == "success":
        require(manifest["complete_campaign"] is True, "SUCCESS_REQUIRES_COMPLETE_CAMPAIGN")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("plan", "start", "cleanup", "audit"))
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--config")
    parser.add_argument("--execute", action="store_true", help="Explicitly authorize this invocation's cloud changes")
    parser.add_argument("--terraform", default="terraform")
    parser.add_argument("--gh", default="gh")
    parser.add_argument("--aliyun", default="aliyun")
    parser.add_argument("--ssh", default="ssh")
    options = parser.parse_args()
    if options.action == "plan":
        require(options.config is not None and not options.execute, "LOCAL_PLAN_REQUIRES_CONFIG")
        result = plan(json.loads(Path(options.config).read_bytes()), options.state_dir)
    else:
        require(options.execute or options.action == "audit", "EXPLICIT_EXECUTION_REQUIRED")
        campaign = Campaign(options.state_dir, Tools(options.terraform, options.gh, options.aliyun, options.ssh))
        if options.action == "start":
            campaign.run()
        elif options.action == "cleanup":
            require(campaign.state.get("session_started_utc"), "UNSTARTED_CAMPAIGN")
            campaign.cleanup()
        else:
            print(json.dumps(campaign.audit(), indent=2))
            return
        result = campaign.state
    print(json.dumps({k: v for k, v in result.items() if k != "config"}, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, RuntimeError, TimeoutError, KeyError, TypeError):
        raise SystemExit("CAMPAIGN_FAILED: inspect local campaign status; no tool output or secrets exported") from None
