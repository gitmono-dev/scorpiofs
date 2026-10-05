"""One immutable session budget for builds, owned setup, rounds and cleanup."""

import argparse
from datetime import datetime, timezone
import json
import math
import os
import signal
import subprocess
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


def run_process(args, deadline, env=None, data=None, capture=True):
    """Terminate and reap the owned child group within this same deadline."""
    now = time.monotonic()
    remaining = deadline - now
    if remaining <= 0:
        raise TimeoutError("operation deadline reached before child startup")
    reserve = min(10.0, remaining / 4)
    run_until = deadline - reserve
    process = subprocess.Popen(args, stdin=subprocess.PIPE if data is not None else subprocess.DEVNULL,
                               stdout=subprocess.PIPE if capture else None,
                               stderr=subprocess.PIPE if capture else None,
                               env=env, start_new_session=True)

    def signal_group(signum):
        try:
            os.killpg(process.pid, signum)
        except ProcessLookupError:
            pass

    def terminate():
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

    try:
        output, error = process.communicate(data, timeout=max(0, run_until - time.monotonic()))
    except subprocess.TimeoutExpired:
        terminate()
        raise TimeoutError("owned child exceeded its operation budget") from None
    except BaseException:
        terminate()
        raise
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
