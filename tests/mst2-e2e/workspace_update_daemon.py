"""Own the shipped v3 daemon and accept its evidence only after real exit."""

import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time

import commit_update_budget as budget
from workspace_update_observation import WorkspaceObservationCollector


class ListenerPending(Exception):
    pass


def file_digest(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def actual_exit(process):
    if process.returncode is not None:
        raise AssertionError("owned daemon leader was reaped before group verification")
    observed = os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
    if observed is None:
        return None
    return observed.si_status if observed.si_code == os.CLD_EXITED else -observed.si_status


def mounts_under(root):
    root = str(root)
    result = []
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        fields = line.split()
        point = fields[4]
        for code, char in (("\\040", " "), ("\\011", "\t"), ("\\012", "\n"), ("\\134", "\\")):
            point = point.replace(code, char)
        if point == root or point.startswith(root + "/"):
            result.append(line)
    return result


class WorkspaceDaemon:
    def __init__(self, binary, binary_sha256, round_root, upstream, token, run_id,
                 env, deadline, *, read_profile=False):
        if type(read_profile) is not bool:
            raise ValueError("read profiling requires an explicit boolean opt-in")
        self.process = None
        self.log = None
        self.started = None
        self.root = Path(round_root).resolve(strict=True)
        self.store = self.root / "scorpio-store"
        self.workspace_root = self.store / "workspaces-v3"
        self.cache_root = self.store / "mst2-cache"
        self.sink = self.root / "workspace-observation.jsonl"
        self.binary = Path(binary).resolve(strict=True)
        self.binary_sha256 = binary_sha256
        self.uid = os.getuid()
        self.state_path = self.root / "owned-workspace-daemon.json"
        if file_digest(self.binary) != binary_sha256 or self.store.exists() or mounts_under(self.root):
            raise AssertionError("v3 startup artifact or private storage differs")
        self.binary_identity = self.binary.stat()
        config = self.root / "scorpio.toml"
        config.write_text("store_path = " + json.dumps(str(self.store)) + "\n"
                          + "mst2_base_url = " + json.dumps(upstream) + "\n"
                          + 'log_level = "info,scorpiofs::workspace::performance=debug"\n')
        config.chmod(0o600)
        self.config = config
        self.config_digest = file_digest(config)
        # Reserve a failure record before any child exists. Missing PID/start
        # or an interruption between spawn and binding cannot become PASS.
        with self.state_path.open("x", encoding="utf-8") as stream:
            os.fchmod(stream.fileno(), 0o600)
            json.dump({"pid": None, "starttime": None, "cleanup_complete": False}, stream)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            self.port = listener.getsockname()[1]
        self.url = f"http://127.0.0.1:{self.port}"
        self.argv = [str(self.binary), "--config-path", str(config), "--http-addr",
                     f"127.0.0.1:{self.port}", "serve", "--workspace-observation-jsonl",
                     str(self.sink), "--workspace-observation-run-id", run_id]
        if read_profile:
            self.argv.append("--workspace-read-profile")
        self.collector = WorkspaceObservationCollector(self.sink, self.cache_root, run_id, self.uid)
        child_env = dict(env, SCORPIO_MST2_AUTH_TOKEN=token)
        try:
            self.log = (self.root / "scorpio-private.log").open("xb")
            os.chmod(self.log.fileno(), 0o600)
            self.process = budget.PinnedProcess(self.argv, stdin=subprocess.DEVNULL,
                                               stdout=self.log, stderr=self.log,
                                               env=child_env, start_new_session=True)
            self.started = budget.process_start(self.process.pid)
            self.record_cleanup(False)
            until = min(deadline - 10, time.monotonic() + 60)
            while time.monotonic() < until:
                self.check_owner()
                try:
                    self.check_owner(socket_required=True)
                    return
                except ListenerPending:
                    time.sleep(min(.05, max(0, until - time.monotonic())))
            raise TimeoutError("owned v3 daemon failed readiness")
        except BaseException:
            self.abort(deadline)
            raise

    def check_owner(self, socket_required=False):
        if self.process is None or actual_exit(self.process) is not None:
            raise AssertionError("owned v3 daemon exited")
        proc = Path(f"/proc/{self.process.pid}")
        info = proc.stat()
        fields = proc.joinpath("stat").read_text().rsplit(") ", 1)[1].split()
        argv = [part.decode() for part in proc.joinpath("cmdline").read_bytes().split(b"\0") if part]
        executable = proc.joinpath("exe").stat()
        current = self.binary.stat()
        fixed = self.binary_identity
        if (fields[19] != self.started or info.st_uid != self.uid or argv != self.argv
                or os.getpgid(self.process.pid) != self.process.pid
                or os.getsid(self.process.pid) != self.process.pid
                or (executable.st_dev, executable.st_ino) != (fixed.st_dev, fixed.st_ino)
                or (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns, current.st_ctime_ns) !=
                   (fixed.st_dev, fixed.st_ino, fixed.st_size, fixed.st_mtime_ns, fixed.st_ctime_ns)
                or file_digest(self.config) != self.config_digest):
            raise AssertionError("owned v3 daemon process binding changed")
        if socket_required:
            owned = set()
            for entry in proc.joinpath("fd").iterdir():
                try:
                    owned.add(os.readlink(entry))
                except FileNotFoundError:
                    # Unrelated transient daemon FDs may close during the
                    # scan; the required listener must still be present.
                    continue
            matches = []
            for line in proc.joinpath("net/tcp").read_text().splitlines()[1:]:
                row = line.split()
                if row[1] == f"0100007F:{self.port:04X}" and row[3] == "0A":
                    matches.append(row)
            if not matches:
                raise ListenerPending()
            if (len(matches) != 1 or int(matches[0][7]) != self.uid
                    or "socket:[" + matches[0][9] + "]" not in owned):
                raise AssertionError("HTTP listener is not owned by the v3 daemon")

    def binding(self, actual_status, expected, deadline):
        self.check_owner(socket_required=True)
        self.collector.expect_binding(dict(actual_status, **expected))
        until = min(deadline, time.monotonic() + 5)
        while time.monotonic() < until:
            records = self.collector.live()
            selected = [record for record in records if record["workspace_id"] == actual_status["workspace_id"]]
            if len(selected) == 1:
                record = selected[0]
                if (record["generation"] != actual_status["generation"]
                        or record["snapshot_id"] != actual_status["snapshot_id"]
                        or any(type(record[key]) is not type(value) or record[key] != value
                               for key, value in expected.items())):
                    raise AssertionError("actual workspace resolve differs from publication")
                return record
            time.sleep(min(.01, max(0, until - time.monotonic())))
        raise TimeoutError("actual workspace resolve evidence is missing")

    def record_cleanup(self, complete):
        # A fallback cleanup may verify these facts, but the record never
        # authorizes signalling a PID no longer owned as a direct child.
        state = {"pid": self.process.pid, "starttime": self.started,
                 "cleanup_complete": complete}
        with self.state_path.open("w", encoding="utf-8") as stream:
            os.fchmod(stream.fileno(), 0o600)
            json.dump(state, stream)

    def finish(self, deadline):
        self.check_owner(socket_required=True)
        try:
            os.killpg(self.process.pid, signal.SIGINT)
            until = min(deadline - 5, time.monotonic() + 60)
            while actual_exit(self.process) is None and time.monotonic() < until:
                time.sleep(min(.01, max(0, until - time.monotonic())))
            code = actual_exit(self.process)
            if code != 0 or budget.group_members(self.process.pid, self.started) or mounts_under(self.store):
                raise AssertionError("owned v3 daemon failed complete native shutdown")
            records = self.collector.finalize(code)
            budget.reap_owned(self.process, deadline)
            self.record_cleanup(True)
            self.log.close()
            self.log = None
            return {"actual_exit_code": code, "bindings": len(records), "footer_complete": True,
                    "native_mounts_remaining": 0}
        except BaseException:
            self.abort(deadline)
            raise

    def abort(self, deadline):
        try:
            if self.process is not None and self.process.returncode is None:
                if self.started is None:
                    budget.abort_startup(self.process, deadline)
                else:
                    budget.stop_group(self.process.pid, self.started, deadline, self.process)
            if mounts_under(self.store):
                raise AssertionError("failed v3 daemon left owned native mounts")
            if self.process is not None:
                self.record_cleanup(True)
        finally:
            if self.log is not None:
                self.log.close()
                self.log = None
