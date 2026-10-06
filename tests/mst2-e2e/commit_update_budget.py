"""One immutable session budget for builds, owned setup, rounds and cleanup."""

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


EXTERNAL_RESERVE = 15 * 60
CLEANUP_RESERVE = 10 * 60
REPORT_RESERVE = 10 * 60
MARGIN = 10 * 60
ROUND_SECONDS = 25 * 60
STAGES = {"server-build": 35 * 60, "client-build": 20 * 60,
          "fences": 10 * 60, "setup": 10 * 60}


def utc(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0:
        raise ValueError("session deadline must be explicit UTC")
    return parsed


class SessionBudget:
    def __init__(self, deadline_utc, rounds, cleanup_deadline=None):
        absolute = utc(deadline_utc)
        if isinstance(rounds, bool) or not 3 <= rounds <= 10:
            raise ValueError("at least three complete rounds are required")
        # Workflow establishes this monotonic anchor once, before builds. A
        # standalone invocation also converts its absolute UTC deadline once.
        if cleanup_deadline is None:
            remaining = absolute.timestamp() - time.time()
            if not 0 < remaining <= 235 * 60:
                raise ValueError("session deadline exceeds the fixed window")
            cleanup_deadline = time.monotonic() + remaining - EXTERNAL_RESERVE
        if (not math.isfinite(cleanup_deadline)
                or cleanup_deadline - time.monotonic() > 220 * 60):
            raise ValueError("invalid work/cleanup deadline anchor")
        self.deadline_utc = absolute.isoformat()
        self.cleanup_deadline = cleanup_deadline
        self.measurement_deadline = cleanup_deadline - CLEANUP_RESERVE - REPORT_RESERVE
        self.rounds = rounds

    def require(self, seconds):
        if self.cleanup_deadline - time.monotonic() < seconds:
            raise TimeoutError("insufficient complete-stage session budget")

    def stage_deadline(self, stage):
        if stage not in STAGES:
            raise ValueError("unknown session stage")
        names = tuple(STAGES)
        later = sum(STAGES[name] for name in names[names.index(stage) + 1:])
        reserve = later + self.rounds * ROUND_SECONDS + REPORT_RESERVE + CLEANUP_RESERVE + MARGIN
        self.require(STAGES[stage] + reserve)
        return min(time.monotonic() + STAGES[stage], self.cleanup_deadline - reserve)

    def round_deadline(self, number):
        if not 1 <= number <= self.rounds:
            raise ValueError("invalid round number")
        reserve = ((self.rounds - number) * ROUND_SECONDS
                   + REPORT_RESERVE + CLEANUP_RESERVE + MARGIN)
        self.require(ROUND_SECONDS + reserve)
        return min(time.monotonic() + ROUND_SECONDS, self.cleanup_deadline - reserve)

    def report_deadline(self):
        self.require(REPORT_RESERVE + CLEANUP_RESERVE)
        return min(time.monotonic() + REPORT_RESERVE,
                   self.cleanup_deadline - CLEANUP_RESERVE)


def from_options(options):
    existing = getattr(options, "budget", None)
    if existing is not None:
        return existing
    return SessionBudget(options.session_deadline_utc, options.rounds,
                         getattr(options, "work_cleanup_deadline_monotonic", None))


def group_members(pgid, started):
    """Only this new-session group; zombies hold no executing work."""
    members = []
    for proc in Path("/proc").iterdir():
        if not proc.name.isdecimal():
            continue
        try:
            fields = (proc / "stat").read_text().rsplit(") ", 1)[1].split()
        except (FileNotFoundError, ProcessLookupError):
            continue
        if int(fields[2]) != pgid:
            continue
        if (int(fields[3]) != pgid
                or (started is not None and int(fields[19]) < int(started))):
            raise AssertionError("owned process group identity changed")
        if started is not None and int(proc.name) == pgid and fields[19] != str(started):
            raise AssertionError("owned group leader identity was replaced")
        if fields[0] not in ("Z", "X"):
            members.append(int(proc.name))
    return members


def reap_owned(process, deadline):
    release = getattr(process, "release_reap", None)
    if release is not None:
        release()
    process.wait(timeout=max(0, deadline - time.monotonic()))


def reap_exited(process):
    """No signals or added wait budget: only reap an already exited child."""
    if process is None or process.returncode is not None or sys.platform != "linux":
        return
    observed = os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
    if observed is not None:
        release = getattr(process, "release_reap", None)
        if release is not None:
            release()
        process.wait(timeout=0)


@contextmanager
def cleanup_reap(process):
    """The final reap runs after all owned-group signal paths have ended."""
    try:
        yield
    finally:
        failed = sys.exc_info()[0] is not None
        try:
            reap_exited(process)
        except BaseException:
            # Keep the original cleanup/timeout/interrupt failure. This final
            # nonblocking reap cannot convert an incomplete cleanup to PASS.
            if not failed:
                raise


def stop_group(pgid, started, deadline, process=None):
    """Reaped leaders do not prove that their owned descendants exited."""
    with cleanup_reap(process):
        if group_members(pgid, started):
            try:
                os.killpg(pgid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            term_until = min(deadline, time.monotonic() + 5)
            while group_members(pgid, started) and time.monotonic() < term_until:
                time.sleep(min(.05, max(0, term_until - time.monotonic())))
            if group_members(pgid, started):
                try:
                    os.killpg(pgid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                while group_members(pgid, started) and time.monotonic() < deadline:
                    time.sleep(min(.01, max(0, deadline - time.monotonic())))
            if group_members(pgid, started):
                raise TimeoutError("owned group remained active at its original deadline")
        if process is not None:
            reap_owned(process, deadline)
        if time.monotonic() >= deadline:
            raise TimeoutError("owned group verification exceeded original deadline")


def process_start(pid):
    if sys.platform == "linux":
        return Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()[19]
    return None


class PinnedProcess(subprocess.Popen):
    """Observe Linux exit without freeing the PID until group cleanup finishes.

    communicate() calls wait(), including when no pipes are captured. An
    ordinary wait would release the leader PID before descendant cleanup and
    permit an unrelated new session to reuse its process-group number.
    """
    def __init__(self, *args, **kwargs):
        self.reap_pinned = True
        super().__init__(*args, **kwargs)

    def release_reap(self):
        self.reap_pinned = False

    def _wait(self, timeout=None):
        # CPython communicate's KeyboardInterrupt handler calls _wait directly.
        if self.reap_pinned:
            return self.wait(timeout=timeout)
        return super()._wait(timeout=timeout)

    def wait(self, timeout=None):
        if not self.reap_pinned:
            return super().wait(timeout=timeout)
        if self.returncode is not None:
            raise AssertionError("owned command leader was reaped before group cleanup")
        until = None if timeout is None else time.monotonic() + timeout
        while True:
            observed = os.waitid(os.P_PID, self.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
            if observed is not None:
                if observed.si_code == os.CLD_EXITED:
                    return observed.si_status
                return -observed.si_status
            if until is not None and time.monotonic() >= until:
                raise subprocess.TimeoutExpired(self.args, timeout)
            pause = .01 if until is None else min(.01, max(0, until - time.monotonic()))
            time.sleep(pause)


def abort_startup(process, deadline):
    """A direct unreaped Popen child pins its PID while startup is rejected."""
    if process.returncode is not None:
        raise AssertionError("startup abort requires an unreaped owned child")
    try:
        pgid, sid = os.getpgid(process.pid), os.getsid(process.pid)
    except ProcessLookupError:
        # No leader is distinct from an unreadable/replaced leader. Do not
        # signal an unbound group; still reap this direct child and fail closed
        # if any active group remains.
        if group_members(process.pid, None):
            raise AssertionError("missing startup leader still has an unbound group") from None
        reap_owned(process, deadline)
        if time.monotonic() >= deadline:
            raise TimeoutError("startup reap exceeded original deadline")
        return False
    if pgid != process.pid or sid != process.pid:
        raise AssertionError("startup child is outside its owned new-session group")
    # No poll/communicate/wait has released this direct child PID. Group scans
    # and signals finish before stop_group reaps it, even if its leader exits.
    stop_group(process.pid, None, deadline, process)
    return True


def run_process(args, deadline, env=None, data=None, capture=True):
    """Terminate and reap the owned child group within this same deadline."""
    now = time.monotonic()
    remaining = deadline - now
    if remaining <= 0:
        raise TimeoutError("operation deadline reached before child startup")
    reserve = min(10.0, remaining / 4)
    run_until = deadline - reserve
    popen = PinnedProcess if sys.platform == "linux" else subprocess.Popen
    process = popen(args, stdin=subprocess.PIPE if data is not None else subprocess.DEVNULL,
                               stdout=subprocess.PIPE if capture else None,
                               stderr=subprocess.PIPE if capture else None,
                               env=env, start_new_session=True)
    started = None

    def signal_group(signum):
        if process.returncode is not None:
            raise AssertionError("refusing signal after owned command leader was reaped")
        # The unreaped direct child pins this number through scan and signal.
        # Check the recorded identity before every signal, including KILL.
        if not group_members(process.pid, started):
            return
        try:
            os.killpg(process.pid, signum)
        except ProcessLookupError:
            pass

    def terminate():
        if started is None:
            if process.returncode is None:
                abort_startup(process, deadline)
            return
        signal_group(signal.SIGTERM)
        killed = False
        term_until = min(deadline, time.monotonic() + reserve / 2)
        try:
            process.communicate(timeout=max(0, term_until - time.monotonic()))
        except subprocess.TimeoutExpired:
            signal_group(signal.SIGKILL)
            killed = True
            try:
                process.communicate(timeout=max(0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                # Do not grant a fresh wait timeout when a pipe remains open.
                for pipe in (process.stdin, process.stdout, process.stderr):
                    if pipe is not None:
                        pipe.close()
                raise TimeoutError("owned child did not reap within its deadline") from None
        # Descendants may ignore TERM after their leader already exited.
        if not killed:
            signal_group(signal.SIGKILL)
        if started is not None:
            stop_group(process.pid, started, deadline, process)

    with cleanup_reap(process):
        try:
            started = process_start(process.pid)
        except FileNotFoundError:
            if abort_startup(process, deadline):
                raise AssertionError("startup process identity could not be established") from None
        except BaseException:
            abort_startup(process, deadline)
            raise
        try:
            output, error = process.communicate(data, timeout=max(0, run_until - time.monotonic()))
        except subprocess.TimeoutExpired:
            terminate()
            raise TimeoutError("owned child exceeded its operation budget") from None
        except BaseException:
            terminate()
            raise
        # A short-lived command owns its new-session group even when descendants
        # close inherited pipes and its leader returns a successful exit status.
        if started is not None:
            stop_group(process.pid, started, deadline, process)
        if time.monotonic() >= deadline:
            raise TimeoutError("owned child verification exceeded original deadline")
        return process.returncode, output, error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-deadline-utc", default=os.environ.get("MST2_SESSION_DEADLINE"), required=False)
    parser.add_argument("--work-cleanup-deadline-monotonic", type=float,
                        default=os.environ.get("MST2_WORK_CLEANUP_DEADLINE_MONOTONIC"))
    parser.add_argument("--rounds", type=int, choices=range(3, 11), default=3)
    parser.add_argument("--stage", choices=tuple(STAGES), required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    options = parser.parse_args()
    if not options.session_deadline_utc:
        raise ValueError("an original absolute session deadline is required")
    args = options.command
    if args and args[0] == "--":
        args = args[1:]
    if not args:
        raise ValueError("a stage command is required")
    deadline = from_options(options).stage_deadline(options.stage)
    status, _, _ = run_process(args, deadline, capture=False)
    return status


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (Exception, KeyboardInterrupt) as error:
        print(json.dumps({"record": "session_budget_failure", "error_type": type(error).__name__}), flush=True)
        raise SystemExit(1)
