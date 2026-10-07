"""Owned backend foundation; not yet wired into the benchmark or workflow.

Each backend owns a complete disposable dependency stack and native writer.
The caller must publish one common canonical seed through ordinary Git before
capturing measurements. No READY certificate or measured projection is seeded.
"""

from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import stat
import subprocess
import time
from types import SimpleNamespace
from urllib.request import urlopen
import uuid

import commit_update_bench as common
import commit_update_budget as budget_module
import commit_update_ci as ci
import commit_update_projection as projection

SHA = re.compile(r"[0-9a-f]{40}")
CONTAINER = re.compile(r"[0-9a-f]{64}")
SERVICES = frozenset({"postgres", "redis", "rustfs", "rustfs-init"})


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _check(deadline):
    if (type(deadline) not in (int, float) or not math.isfinite(deadline)
            or time.monotonic() >= deadline):
        raise TimeoutError("backend original deadline is exhausted or invalid")


def _digest(path):
    if path.is_symlink() or not path.is_file():
        raise AssertionError("owned source or state is not a real regular file")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_text(path, value, *, new=False):
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise AssertionError("owned state is not a real regular file")
    previous = None if new else path.lstat()
    if previous is not None and previous.st_nlink != 1:
        raise AssertionError("owned state cannot be a borrowed hard link")
    flags = os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0)
    if new:
        flags |= os.O_CREAT | os.O_EXCL
    fd = os.open(path, flags, 0o600)
    try:
        actual = os.fstat(fd)
        if (not stat.S_ISREG(actual.st_mode) or actual.st_nlink != 1
                or (previous is not None and (actual.st_dev, actual.st_ino) !=
                    (previous.st_dev, previous.st_ino))):
            raise AssertionError("owned state file was replaced before write")
        os.ftruncate(fd, 0)
        stream = os.fdopen(fd, "w", encoding="utf-8", newline="\n")
    except BaseException:
        os.close(fd)
        raise
    with stream:
        stream.write(value)
        stream.flush()
        os.fsync(stream.fileno())


def _write(path, value, *, new=False):
    _write_text(path, _canonical(value) + "\n", new=new)


def _real_path(path):
    path = Path(path).absolute()
    for component in [*reversed(path.parents), path]:
        if component.is_symlink():
            raise AssertionError("owned source or state contains a symlink")
    return path.resolve(strict=True)


def _process_environment(pid):
    result = {}
    try:
        for item in Path(f"/proc/{pid}/environ").read_bytes().split(b"\0"):
            if item:
                name, value = item.decode().split("=", 1)
                if name in result:
                    raise ValueError("duplicate environment key")
                result[name] = value
    except (OSError, UnicodeError, ValueError):
        raise AssertionError("owned process environment is unavailable or malformed") from None
    return result


@dataclass(frozen=True)
class RuntimeBinding:
    revision: int
    phase: str
    round: int
    client: str
    project: str
    database: str
    instance_id: str
    base_url: str
    git_url: str
    server_source_sha: str
    server_source_tree: str
    server_binary_sha256: str
    server_cargo_lock_sha256: str
    service_pid: int
    service_starttime: str
    config_sha256: str
    compose_sha256: str
    cache_prefix: str
    base_dir: str
    cache_dir: str
    pack_cache_dir: str
    projection_sink_instance: str
    projection_sink_root: str
    projection_sink_device: int
    projection_sink_inode: int
    dependency_container_ids: tuple
    identity_rows_json: str
    identity_json: str
    native_json: str

    @property
    def identity_rows(self):
        return json.loads(self.identity_rows_json)

    @property
    def identity(self):
        return json.loads(self.identity_json)

    @property
    def native(self):
        return json.loads(self.native_json)


class BackendGroup:
    """Register every owner before its first startup side effect.

    There is deliberately no CLI/dispatch flag in this foundation. Existing
    legacy single-backend execution is unchanged until the orchestrator lands.
    """

    def __init__(self, options, budget):
        if not isinstance(budget, budget_module.SessionBudget) or not budget.paired:
            raise ValueError("backend group needs the admitted paired session budget")
        root, project = ci.hosted_root(options.run_root)
        if root.exists() or root.is_symlink():
            raise ValueError("backend campaign root must be new")
        if type(options.mega_sha) is not str or not SHA.fullmatch(options.mega_sha):
            raise ValueError("backend server source must be an immutable full SHA")
        _check(budget.cleanup_deadline)
        self.root, self.project = root, project
        self.options, self.budget = options, budget
        self.deadline_utc = budget.deadline_utc
        self.cleanup_deadline = budget.cleanup_deadline
        self.measurement_deadline = budget.measurement_deadline
        self.backends = []
        self.closed = False
        root.mkdir(mode=0o700)
        (root / "backends").mkdir(mode=0o700)
        self.directory_bindings = {p: (p.stat().st_dev, p.stat().st_ino)
                                   for p in (root, root / "backends")}
        self.state_path = root / "backend-owners.json"
        self._persist(new=True)

    def _state(self):
        return {"revision": 1, "project": self.project,
                "session_deadline_utc": self.deadline_utc,
                "cleanup_deadline_monotonic": self.cleanup_deadline,
                "measurement_deadline_monotonic": self.measurement_deadline,
                "backends": [b.safe_owner() for b in self.backends], "closed": self.closed}

    def _persist(self, *, new=False):
        _check(self.cleanup_deadline)
        if not new:
            self._check_owner()
        _write(self.state_path, self._state(), new=new)
        self.state_digest = _digest(self.state_path)
        _check(self.cleanup_deadline)

    def _check_owner(self):
        _check(self.cleanup_deadline)
        root, project = ci.hosted_root(self.root)
        if (root != self.root or project != self.project or self.root.is_symlink()
                or self.state_digest != _digest(self.state_path)
                or self.cleanup_deadline != self.budget.cleanup_deadline
                or self.deadline_utc != self.budget.deadline_utc
                or self.measurement_deadline != self.budget.measurement_deadline):
            raise AssertionError("backend group ownership or original window changed")
        for path, identity in self.directory_bindings.items():
            actual = _real_path(path).stat()
            if (actual.st_dev, actual.st_ino) != identity:
                raise AssertionError("backend group directory was replaced")

    def admit(self, phase, number, client, deadline):
        self._check_owner()
        _check(deadline)
        if self.closed or deadline > self.measurement_deadline:
            raise ValueError("backend phase cannot exceed the original measurement deadline")
        if (type(number) is not int or phase not in ("fair", "diagnostic")
                or client not in ("a", "b")
                or (phase == "fair" and not 1 <= number <= 3)
                or (phase == "diagnostic" and (number != 1 or client != "b"))):
            raise ValueError("invalid backend phase/round/client")
        key = (phase, number, client)
        if any(b.key == key for b in self.backends):
            raise ValueError("backend identities cannot be reused")
        if any(b.state != "retired" and (b.phase, b.number) != (phase, number)
               for b in self.backends):
            raise ValueError("prior backend phase/round must retire before admitting another")
        backend = OwnedBackend(self, phase, number, client, deadline)
        self.backends.append(backend)
        self._persist()
        _check(deadline)
        return backend

    def start_pair(self, number, deadline):
        owners = []
        try:
            for label in ("a", "b"):
                backend = self.admit("fair", number, label, deadline)
                owners.append(backend)
                backend.start(deadline)
            assert_isolated(*(b.verify_runtime(deadline, ready=False) for b in owners))
            return tuple(owners)
        except BaseException as primary:
            # A failed second startup still retires both admitted owners.
            try:
                self.close()
            except BaseException:
                primary.add_note("owned backend cleanup remains incomplete; original cleanup deadline applies")
            raise

    def close(self):
        self._check_owner()
        if self.closed:
            return
        errors = []
        for backend in reversed(self.backends):
            try:
                backend.stop(self.cleanup_deadline)
            except BaseException as error:
                errors.append(error)
        if not errors:
            self.closed = True
            try:
                self._persist()
            except BaseException:
                self.closed = False
                raise
        if errors:
            raise BaseExceptionGroup("owned backend cleanup incomplete", errors)

    def __enter__(self):
        return self

    def __exit__(self, kind, error, traceback):
        try:
            self.close()
        except BaseException:
            if error is None:
                raise
            # Preserve the primary error, retain closed=False and retry owners.
        return False


class OwnedBackend:
    def __init__(self, group, phase, number, client, deadline):
        self.group = group
        self.phase, self.number, self.client = phase, number, client
        self.key = (phase, number, client)
        self._operation_deadline = deadline
        name = f"{phase}-r{number:02}-{client}"
        self.root = group.root / "backends" / name
        self.project = group.project + "-" + name
        self.database = "mst2_bench_" + uuid.uuid4().hex
        self.instance_id = str(uuid.uuid4())
        self.process = self.log = None
        self.started = None
        self.state = "admitted"
        self.ports = None
        self.pg_env = self.service_env = self.git_env = None
        self.config_digest = self.compose_digest = None
        self.source_tree = self.lock_digest = self.binary_digest = None
        self.container_ids = None
        self.owned_digest = None
        self.projection_collector = None
        self.root_identity = None
        self.startup_abort_pending = False

    @property
    def operation_deadline(self):
        return self._operation_deadline

    def safe_owner(self):
        return {"phase": self.phase, "round": self.number, "client": self.client,
                "project": self.project, "database": self.database, "instance_id": self.instance_id,
                "root": str(self.root), "state": self.state,
                "operation_deadline_monotonic": self.operation_deadline,
                "service_pid": self.process.pid if self.process is not None else None,
                "service_starttime": self.started}

    def _transition(self, state):
        previous = self.state
        self.state = state
        try:
            self.group._persist()
        except BaseException:
            self.state = previous
            raise

    def _operation(self, deadline):
        self.group._check_owner()
        _check(deadline)
        if deadline > self.operation_deadline:
            raise ValueError("backend operation cannot extend its admitted phase deadline")

    def _verify_owned(self):
        if self.root_identity is not None:
            actual = _real_path(self.root).stat()
            if (actual.st_dev, actual.st_ino) != self.root_identity:
                raise AssertionError("backend directory was replaced")
        if self.owned_digest is not None and _digest(self.root / "owned.json") != self.owned_digest:
            raise AssertionError("backend ownership manifest was replaced")
        for name, expected in (("service.toml", self.config_digest), ("dependencies.json", self.compose_digest)):
            if expected is not None and _digest(self.root / name) != expected:
                raise AssertionError("backend owned configuration changed")

    def _source(self, deadline):
        self._operation(deadline)
        options = self.group.options
        source = _real_path(options.mega_source)
        binary = _real_path(options.mega_binary)
        if (not binary.is_relative_to(source / "target")
                or common.git(source, deadline, "rev-parse", "HEAD").decode().strip() != options.mega_sha
                or common.git(source, deadline, "status", "--porcelain").strip()):
            raise AssertionError("backend source differs from fixed clean server build")
        tree = common.git(source, deadline, "rev-parse", "HEAD^{tree}").decode().strip()
        if SHA.fullmatch(tree) is None:
            raise AssertionError("backend fixed server tree is malformed")
        lock, digest = _digest(source / "Cargo.lock"), _digest(binary)
        if self.binary_digest is not None and (tree, lock, digest) != (
                self.source_tree, self.lock_digest, self.binary_digest):
            raise AssertionError("server source/lock/binary changed during backend lifetime")
        self.source_tree, self.lock_digest, self.binary_digest = tree, lock, digest
        self.source, self.binary = source, binary
        _check(deadline)

    def _probe_dependencies(self, deadline):
        self._operation(deadline)
        ids = common.command(["docker", "ps", "-aq", "--no-trunc", "--filter",
                              "label=com.docker.compose.project=" + self.project], deadline).decode().split()
        if len(ids) != 4 or len(set(ids)) != 4 or any(CONTAINER.fullmatch(x) is None for x in ids):
            raise AssertionError("backend dependency inventory is incomplete")
        inspected = json.loads(common.command(["docker", "inspect", *ids], deadline))
        if type(inspected) is not list or len(inspected) != 4 or any(type(x) is not dict for x in inspected):
            raise AssertionError("dependency inspection is malformed")
        services, actual_ids = set(), []
        for item in inspected:
            if (type(item.get("Config")) is not dict or type(item["Config"].get("Labels")) is not dict
                    or type(item.get("State")) is not dict
                    or type(item.get("NetworkSettings")) is not dict
                    or type(item["NetworkSettings"].get("Ports")) is not dict):
                raise AssertionError("dependency inspection shape is malformed")
            labels = item["Config"]["Labels"]
            service = labels.get("com.docker.compose.service")
            if (item.get("Id") not in ids or labels.get("com.docker.compose.project") != self.project
                    or service not in SERVICES or service in services):
                raise AssertionError("dependency container owner changed")
            state = item["State"]
            if service == "rustfs-init":
                if (state.get("Running") is not False or type(state.get("ExitCode")) is not int
                        or state["ExitCode"] != 0):
                    raise AssertionError("owned storage initialization is incomplete")
            elif state.get("Running") is not True:
                raise AssertionError("owned dependency stopped")
            if service in ("postgres", "redis", "rustfs"):
                target = {"postgres": "5432/tcp", "redis": "6379/tcp", "rustfs": "9000/tcp"}[service]
                exposed = item["NetworkSettings"]["Ports"].get(target)
                if exposed != [{"HostIp": "127.0.0.1", "HostPort": str(self.ports[service])}]:
                    raise AssertionError("dependency loopback endpoint changed")
            services.add(service)
            actual_ids.append(item["Id"])
        if services != SERVICES or set(actual_ids) != set(ids):
            raise AssertionError("dependency services differ from owned configuration")
        result = tuple(sorted(actual_ids))
        if self.container_ids is not None and result != self.container_ids:
            raise AssertionError("dependency containers were replaced")
        self.container_ids = result
        return result

    def start(self, deadline):
        self._operation(deadline)
        if deadline > self.group.measurement_deadline or self.state != "admitted":
            raise ValueError("backend start must use its original admitted lifetime")
        self._source(deadline)
        if self.root.exists() or self.root.is_symlink():
            raise ValueError("backend directory must be fresh")
        self.ports = {name: ci.free_port() for name in ("postgres", "redis", "rustfs", "http")}
        if len(set(self.ports.values())) != 4:
            raise AssertionError("backend ports collided")
        compose = ci.dependencies(self.source, self.project, self.ports, deadline)
        existing = common.command(["docker", "ps", "-aq", "--filter",
                                   "label=com.docker.compose.project=" + self.project], deadline)
        network = common.command(["docker", "network", "ls", "-q", "--filter",
                                  "name=^" + self.project + "-network$"], deadline)
        if existing.strip() or network.strip():
            raise AssertionError("refusing to reuse unrelated dependency resources")
        self.root.mkdir(mode=0o700)
        root_info = self.root.stat()
        self.root_identity = (root_info.st_dev, root_info.st_ino)
        compose_path = self.root / "dependencies.json"
        _write(compose_path, compose, new=True)
        self.compose_digest = _digest(compose_path)
        # Persist ownership before the first Docker side effect, including
        # failed `up` that may already have created some resources.
        owned = {"project": self.project, "binary": str(self.binary),
                 "mega_source_sha": self.group.options.mega_sha,
                 "compose_sha256": self.compose_digest, "workspace_layout": "single"}
        _write(self.root / "owned.json", owned, new=True)
        self.owned_digest = _digest(self.root / "owned.json")
        self._transition("starting")
        common.command(["docker", "compose", "-p", self.project, "-f", str(compose_path),
                        "up", "-d", "--wait", "--wait-timeout", "180"],
                       min(deadline, time.monotonic() + 240))
        self._probe_dependencies(deadline)
        env = common.clean_env({"PGHOST": "127.0.0.1", "PGPORT": str(self.ports["postgres"]),
                                "PGUSER": "mega2", "PGPASSWORD": "mega2_test_password", "PGDATABASE": "mega2"})
        common.command(["psql", "-X", "-v", "ON_ERROR_STOP=1", "-c", "CREATE DATABASE " + self.database],
                       deadline, env=env)
        env["PGDATABASE"] = self.database
        git_token, token = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
        # Mask before any child can output a token; tokens remain private files.
        for name, secret in (("git-token", git_token), ("mst2-token", token)):
            print("::add-mask::" + secret, flush=True)
            _write_text(self.root / name, secret, new=True)
        config = common.tomllib.loads((self.source / "config/config-storage-only.toml").read_text())
        config["base_dir"] = str(self.root / "service-data")
        config["log"].update(print_std=False, with_ansi=False)
        config["database"].update(db_url=f"postgres://mega2:mega2_test_password@127.0.0.1:{self.ports['postgres']}/{self.database}",
                                  max_connection=8, min_connection=1, acquire_timeout=60, connect_timeout=30)
        config["redis"]["url"] = f"redis://127.0.0.1:{self.ports['redis']}"
        config["monorepo"].update(root_dirs=["third-party", "project"], object_format="sha1", push_policy="trunk")
        config["pack"].update(pack_decode_mem_size="512M", pack_decode_cache_path=str(self.root / "pack-cache"))
        config["object_storage"]["s3"].update(endpoint_url=f"http://127.0.0.1:{self.ports['rustfs']}", bucket="mega2")
        config["git"].update(push_auth="token", ssh_receive_pack=False,
                             push_tokens=[{"name": "owned-benchmark", "token": "${file:" + str(self.root / "git-token") + "}",
                                           "paths": ["/project"]}])
        config["mst2"] = {"enabled": True, "instance_uuid": self.instance_id, "publication_enabled": True,
                          "auth_token": "${file:" + str(self.root / "mst2-token") + "}",
                          "projection_observation_enabled": True}
        for name in ("oci", "agent_capture", "storage_events"):
            config[name] = {"enabled": False}
        config_path = self.root / "service.toml"
        _write_text(config_path, ci.toml(config), new=True)
        self.config_digest = _digest(config_path)
        self.service_env = common.clean_env({"MEGA_BASE_DIR": config["base_dir"],
                                            "MEGA_CACHE_DIR": str(self.root / "cache"),
                                            "MEGA_GIT_OBJECT_CACHE_PREFIX": self.project})
        prefix = [str(self.binary), "--config", str(config_path)]
        common.command(prefix + ["config", "validate"], deadline, env=self.service_env)
        common.command(prefix + ["service", "init", "--yes"], deadline, env=self.service_env)
        # Existing maintenance bootstrap installs only INITIALIZING, never a
        # READY certificate. Common semantic seed publication is normal Git.
        ci.initialize_owned_native(self.database, self.instance_id, env, deadline)
        self.pg_env = dict(env, M2_TOKEN=token, M2_GIT_TOKEN=git_token)
        self.git_env = common.clean_env({"GIT_CONFIG_COUNT": "3", "GIT_CONFIG_KEY_0": "http.extraHeader",
                                        "GIT_CONFIG_VALUE_0": "Authorization: Bearer " + git_token,
                                        "GIT_CONFIG_KEY_1": "http.followRedirects", "GIT_CONFIG_VALUE_1": "false",
                                        "GIT_CONFIG_KEY_2": "credential.helper", "GIT_CONFIG_VALUE_2": ""})
        self.log = (self.root / "service-private.log").open("wb")
        self.process = budget_module.PinnedProcess(prefix + ["service", "http", "--host", "127.0.0.1", "-p", str(self.ports["http"])],
                                                  stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log,
                                                  env=self.service_env, start_new_session=True)
        self.startup_abort_pending = True
        try:
            started = budget_module.process_start(self.process.pid)
            if type(started) is not str or re.fullmatch(r"[1-9][0-9]*", started) is None:
                raise AssertionError("backend process start identity is unavailable")
            self.started = started
            owned["service"] = {"pid": self.process.pid, "pgid": self.process.pid,
                                "sid": self.process.pid, "starttime": self.started}
            _write(self.root / "owned.json", owned)
            self.owned_digest = _digest(self.root / "owned.json")
            self._transition("running")
            self.startup_abort_pending = False
        except BaseException as primary:
            try:
                self._abort_startup(self.group.cleanup_deadline)
            except BaseException:
                primary.add_note("direct owned startup child still requires cleanup under the original deadline")
            raise
        self.base_url = f"http://127.0.0.1:{self.ports['http']}"
        self.git_url = self.base_url + "/project"
        ready_until = min(deadline, time.monotonic() + 180)
        while True:
            _check(ready_until)
            if ci.owned_service_exit(self.process) is not None:
                raise RuntimeError("owned backend failed readiness")
            try:
                with urlopen(self.base_url + "/api/v2/snapshots/capabilities",
                             timeout=min(2, max(.001, ready_until - time.monotonic()))) as response:
                    if response.status == 200:
                        break
            except OSError:
                pass
            time.sleep(min(.2, max(0, ready_until - time.monotonic())))
        # Only materialize the seed path; never resolve a measured target here.
        self.initial_commit = self.tip(deadline)
        self.verify_runtime(deadline, ready=False)
        return self

    def tip(self, deadline):
        self._operation(deadline)
        rows = common.command(["git", "ls-remote", self.git_url, "refs/heads/main"],
                              deadline, env=self.git_env).decode().splitlines()
        if len(rows) != 1 or rows[0].split()[1:] != ["refs/heads/main"]:
            raise AssertionError("owned backend main ref is ambiguous")
        commit = rows[0].split()[0]
        if SHA.fullmatch(commit) is None:
            raise AssertionError("owned backend ref is not a canonical commit")
        return commit

    def publish_seed(self, fixture, parent, commit, tree, deadline):
        """Publish the exact shared seed only if both semantic parents match."""
        self._operation(deadline)
        if self.state != "running" or any(type(x) is not str or SHA.fullmatch(x) is None
                                          for x in (parent, commit, tree)):
            raise ValueError("canonical native seed needs a live owned backend")
        fixture = _real_path(fixture)
        if self.tip(deadline) != parent:
            raise AssertionError("common seed parent differs from backend path ancestry")
        actual_tree = common.git(fixture, deadline, "rev-parse", commit + "^{tree}").decode().strip()
        parents = common.git(fixture, deadline, "rev-list", "--parents", "-n", "1", commit).decode().split()
        if actual_tree != tree or parents != [commit, parent]:
            raise AssertionError("common seed Git objects differ from fixed parent/commit/tree")
        common.git(fixture, deadline, "push", "--no-thin", self.git_url,
                   f"{commit}:refs/heads/main", env=self.git_env)
        # Ordinary push may return while the native publication is still
        # INITIALIZING. Poll only that known transient under the admitted bound.
        while True:
            try:
                binding = self.verify_runtime(deadline)
                break
            except NativePublicationPending:
                _check(deadline)
                time.sleep(min(.1, max(0, deadline - time.monotonic())))
        if binding.identity["project_commit"] != commit or binding.identity["project_tree"] != tree:
            raise AssertionError("normal native seed publication did not preserve fixed objects")
        self._transition("ready")
        _check(deadline)
        return binding

    def verify_runtime(self, deadline, *, ready=True):
        self._operation(deadline)
        if self.state not in ("running", "ready") or self.process is None:
            raise AssertionError("backend is not a live owned writer")
        if deadline > self.group.measurement_deadline:
            raise ValueError("runtime proof cannot use a later deadline")
        self._source(deadline)
        self._verify_owned()
        if (_digest(self.root / "service.toml") != self.config_digest
                or _digest(self.root / "dependencies.json") != self.compose_digest):
            raise AssertionError("backend config or dependency configuration changed")
        ids = self._probe_dependencies(deadline)
        if (self.process.returncode is not None or ci.owned_service_exit(self.process) is not None
                or budget_module.process_start(self.process.pid) != self.started
                or os.getpgid(self.process.pid) != self.process.pid
                or os.getsid(self.process.pid) != self.process.pid):
            raise AssertionError("backend service is no longer its live pinned process group")
        inherited = _process_environment(self.process.pid)
        if ({k: v for k, v in inherited.items() if k.startswith("MEGA_")} !=
                {k: v for k, v in self.service_env.items() if k.startswith("MEGA_")}):
            raise AssertionError("backend effective cache/base/endpoint overrides changed")
        options = SimpleNamespace(service_pid=self.process.pid, database=self.database,
                                  instance_id=self.instance_id, publication_mode="native",
                                  projection_traces=True, base_url=self.base_url, git_url=self.git_url)
        owner = common.service_binding(options, self.pg_env)
        if (owner["starttime_ticks"] != self.started or owner["exe"] != str(self.binary)
                or owner["config_sha256"] != self.config_digest
                or owner["projection_cache"] != str(self.root / "cache")):
            raise AssertionError("backend runtime process/endpoint owner changed")
        rows = common.query(common.IDENTITY_SQL, deadline, env=self.pg_env)
        if type(rows) is not list or any(type(row) is not dict for row in rows):
            raise AssertionError("backend raw Git identity is malformed")
        commit = self.tip(deadline)
        projects = [row for row in rows if row.get("path") == "/project"]
        if len(projects) != 1:
            raise AssertionError("backend path identity missing")
        try:
            identity = common.validate_identity(rows, commit, projects[0]["tree"], self.database)
        except (KeyError, TypeError, ValueError, IndexError):
            raise AssertionError("backend raw Git identity is malformed") from None
        native = common.query(common.NATIVE_SQL, deadline, env=self.pg_env)
        if type(native) is not dict:
            raise AssertionError("backend raw native identity is malformed")
        if (type(native.get("sequence")) is not int or type(native.get("writer_epoch")) is not int):
            raise AssertionError("backend native counters are malformed")
        if native.get("state") == "READY":
            counters = ("certificate_receipt_id", "certificate_sequence", "origin_sequence", "certificate_id",
                        "certificate_epoch", "receipt_id", "receipt_epoch", "request_digest_version",
                        "native_certificate_version", "outbox_id", "outbox_sequence")
            if any(type(native.get(name)) is not int or native[name] <= 0 for name in counters):
                raise AssertionError("backend native certificate counters are malformed")
        try:
            common.validate_native(native, identity, self.instance_id, False)
        except (KeyError, TypeError, ValueError):
            raise AssertionError("backend raw native identity is malformed") from None
        if ready and native["state"] == "INITIALIZING":
            raise NativePublicationPending("owned native head is still INITIALIZING")
        try:
            common.validate_native(native, identity, self.instance_id, ready)
        except (KeyError, TypeError, ValueError):
            raise AssertionError("backend raw native identity is malformed") from None
        if self.projection_collector is None:
            self.projection_collector = projection.ProjectionCollector(self.root / "cache")
        collector = self.projection_collector
        for path in collector.directory_bindings:
            collector._directory(path, private=path not in (collector.cache, collector.cache / "logs"))
        discovered = projection.ProjectionCollector(self.root / "cache")
        if (discovered.root != collector.root or discovered.instance != collector.instance
                or discovered.directory_bindings != collector.directory_bindings):
            raise AssertionError("backend projection sink was replaced")
        sink_device, sink_inode = collector.directory_bindings[collector.root]
        _check(deadline)
        return RuntimeBinding(1, self.phase, self.number, self.client, self.project, self.database,
                              self.instance_id, self.base_url, self.git_url, self.group.options.mega_sha,
                              self.source_tree, self.binary_digest, self.lock_digest, self.process.pid,
                              self.started, self.config_digest, self.compose_digest, self.project,
                              str(self.root / "service-data"), str(self.root / "cache"), str(self.root / "pack-cache"),
                              collector.instance, str(collector.root), sink_device, sink_inode,
                              ids, _canonical(rows), _canonical(identity), _canonical(native))

    def finalize_projection(self, deadline):
        self.group._check_owner()
        _check(deadline)
        if deadline > self.group.cleanup_deadline or self.state not in ("running", "ready"):
            raise ValueError("projection retirement requires the original owned deadline")
        self._verify_owned()
        ci.graceful_owned(self.root, self.project, deadline, self.process)
        _check(deadline)
        self.owned_digest = _digest(self.root / "owned.json")
        self._transition("drained")

    def _abort_startup(self, deadline):
        _check(deadline)
        if self.process.returncode is None:
            if self.started is None:
                budget_module.abort_startup(self.process, deadline)
            else:
                budget_module.stop_group(self.process.pid, self.started, deadline, self.process)
        self.startup_abort_pending = False
        _check(deadline)

    def stop(self, deadline):
        _check(deadline)
        if deadline != self.group.cleanup_deadline:
            raise ValueError("backend cleanup cannot move the original anchor")
        if self.state == "retired":
            return
        try:
            self.group._check_owner()
            if self.startup_abort_pending:
                # This unreaped direct child remains signal authority even
                # when persisting its on-disk PID binding failed at startup.
                self._abort_startup(deadline)
            self._verify_owned()
            ci.stop_owned(self.root, self.project, deadline, self.process)
            _check(deadline)
            if self.owned_digest is not None:
                self.owned_digest = _digest(self.root / "owned.json")
            self._transition("retired")
        finally:
            if self.log is not None:
                self.log.close()
                self.log = None


class NativePublicationPending(AssertionError):
    pass


def assert_isolated(a, b):
    if type(a) is not RuntimeBinding or type(b) is not RuntimeBinding:
        raise ValueError("actual independently probed runtime bindings are required")
    if (a.phase != "fair" or b.phase != "fair" or a.round != b.round
            or a.client != "a" or b.client != "b"
            or (a.server_source_sha, a.server_source_tree, a.server_binary_sha256,
                a.server_cargo_lock_sha256) !=
               (b.server_source_sha, b.server_source_tree, b.server_binary_sha256,
                b.server_cargo_lock_sha256)):
        raise AssertionError("paired source/phase identities differ")
    for name in ("project", "database", "instance_id", "base_url", "git_url", "service_pid",
                 "cache_prefix", "base_dir", "cache_dir", "pack_cache_dir"):
        if getattr(a, name) == getattr(b, name):
            raise AssertionError("paired backends share an owned resource")
    if set(a.dependency_container_ids).intersection(b.dependency_container_ids):
        raise AssertionError("paired dependency containers share cache/storage")
    if (a.projection_sink_instance == b.projection_sink_instance
            or a.projection_sink_root == b.projection_sink_root
            or (a.projection_sink_device, a.projection_sink_inode) ==
               (b.projection_sink_device, b.projection_sink_inode)):
        raise AssertionError("paired projection sinks share an owner")
    paths = [Path(p) for binding in (a, b) for p in
             (binding.base_dir, binding.cache_dir, binding.pack_cache_dir)]
    if any(x == y or x.is_relative_to(y) or y.is_relative_to(x)
           for i, x in enumerate(paths) for y in paths[i + 1:]):
        raise AssertionError("paired backend paths overlap")
