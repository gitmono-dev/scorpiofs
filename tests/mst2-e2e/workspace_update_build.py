"""Bind a measured client to its actual immutable release build, without logs."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
from types import SimpleNamespace

import commit_update_bench as common
import workspace_update_git_performance as git_performance

BUILD_ENV = {"CARGO_BUILD_JOBS": "2", "CARGO_INCREMENTAL": "0",
             "CARGO_PROFILE_RELEASE_DEBUG": "0"}
FIELDS = {"revision", "label", "source", "source_sha", "cargo_lock_sha256", "binary",
          "binary_sha256", "build_argv", "build_env", "rustc_version", "cargo_version"}


def sha256(path, deadline):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(65536), b""):
            remaining(deadline)
            digest.update(chunk)
    remaining(deadline)
    return digest.hexdigest()


def remaining(deadline):
    seconds = deadline - time.monotonic()
    if seconds <= 0:
        raise TimeoutError("client build binding exceeded its original deadline")
    return seconds


def output(argv, deadline, env=None):
    if not git_performance.is_git(argv):
        return subprocess.check_output(argv, env=env or common.clean_env(), timeout=remaining(deadline))
    status, output, errors = common.budget_module.run_process(argv, deadline, env=env or common.clean_env())
    if status:
        raise subprocess.CalledProcessError(status, argv, output=output, stderr=errors)
    return output


def fixed_source(source, source_sha, deadline):
    source = Path(source)
    if source.is_symlink() or not source.is_dir() or not re.fullmatch(r"[0-9a-f]{40}", source_sha):
        raise ValueError("client source must be an immutable real checkout")
    source = source.resolve(strict=True)
    head = output(["git", "-C", str(source), "rev-parse", "HEAD"], deadline).decode().strip()
    dirty = output(["git", "-C", str(source), "-c", "core.fsmonitor=false", "status", "--porcelain"], deadline)
    lock = source / "Cargo.lock"
    if head != source_sha or dirty.strip() or lock.is_symlink() or not lock.is_file():
        raise AssertionError("client source or lock differs from the immutable build")
    return source, sha256(lock, deadline)


def build_argv(source, binary_name="scorpio"):
    return ["cargo", "build", "--manifest-path", str(source / "Cargo.toml"),
            "--locked", "--release", "--bin", binary_name]


def binary_path(source, binary, binary_name="scorpio"):
    binary = Path(binary)
    expected = source / "target" / "release" / binary_name
    if binary.is_symlink() or not binary.is_file() or binary.resolve(strict=True) != expected:
        raise ValueError("client binary must be the release artifact of its pinned checkout")
    # Reject target/release symlinks as well as a substituted final file.
    if any(parent.is_symlink() for parent in (source / "target", source / "target/release")):
        raise ValueError("client build artifact directory changed")
    return binary.resolve(strict=True)


def build(source, source_sha, label, receipt, deadline):
    if label not in ("a", "b", "server"):
        raise ValueError("build label must be a, b or server")
    binary_name = "mega2" if label == "server" else "scorpio"
    source, lock_before = fixed_source(source, source_sha, deadline)
    receipt = Path(receipt)
    if receipt.exists() or receipt.is_symlink() or receipt.resolve().is_relative_to(source):
        raise ValueError("build receipt must be a new file outside the client checkout")
    env = common.clean_env(BUILD_ENV)
    # The enclosing SessionBudget command owns this entire process group.
    # These children inherit that group and its one stage deadline.
    rustc = output(["rustc", "--version"], deadline, env).decode().strip()
    cargo = output(["cargo", "--version"], deadline, env).decode().strip()
    argv = build_argv(source, binary_name)
    subprocess.run(argv, env=env, check=True, timeout=remaining(deadline))
    _, lock_after = fixed_source(source, source_sha, deadline)
    if lock_after != lock_before:
        raise AssertionError("client lock changed while building")
    binary = binary_path(source, source / "target/release" / binary_name, binary_name)
    record = {"revision": 1, "label": label, "source": str(source), "source_sha": source_sha,
              "cargo_lock_sha256": lock_after, "binary": str(binary), "binary_sha256": sha256(binary, deadline),
              "build_argv": argv, "build_env": BUILD_ENV, "rustc_version": rustc, "cargo_version": cargo}
    with receipt.open("x", encoding="utf-8") as stream:
        if os.name == "posix":
            os.fchmod(stream.fileno(), 0o600)
        stream.write(json.dumps(record, sort_keys=True) + "\n")
    remaining(deadline)
    return record


def load(receipt, label, deadline):
    receipt = Path(receipt)
    if receipt.is_symlink() or not receipt.is_file() or receipt.stat().st_size > 16384:
        raise ValueError("client build receipt changed")
    record = json.loads(receipt.read_text(encoding="utf-8"))
    if (type(record) is not dict or set(record) != FIELDS or type(record["revision"]) is not int
            or record["revision"] != 1 or record["label"] != label):
        raise ValueError("client build receipt shape or label differs")
    source, lock = fixed_source(record["source"], record["source_sha"], deadline)
    binary_name = "mega2" if label == "server" else "scorpio"
    binary = binary_path(source, record["binary"], binary_name)
    if (record["cargo_lock_sha256"] != lock or record["binary_sha256"] != sha256(binary, deadline)
            or record["build_argv"] != build_argv(source, binary_name) or record["build_env"] != BUILD_ENV
            or not isinstance(record["rustc_version"], str) or not record["rustc_version"].startswith("rustc ")
            or not isinstance(record["cargo_version"], str) or not record["cargo_version"].startswith("cargo ")):
        raise AssertionError("client release build binding differs")
    remaining(deadline)
    return SimpleNamespace(label=label, driver=binary, driver_sha256=record["binary_sha256"],
                           receipt=receipt, build=record)


def clients(options, deadline):
    if not getattr(options, "paired", False):
        if getattr(options, "build_a", None) or getattr(options, "build_b", None):
            raise ValueError("two-client build receipts require paired mode")
        common.driver_binding(options)
        return [SimpleNamespace(label="single", driver=options.driver,
                                driver_sha256=options.driver_sha256, receipt=None, build=None)]
    if options.rounds != 3 or not getattr(options, "build_a", None) or not getattr(options, "build_b", None):
        raise ValueError("paired mode requires two build receipts and three complete rounds")
    lanes = [load(options.build_a, "a", deadline), load(options.build_b, "b", deadline)]
    if hasattr(options, "baseline_sha") or hasattr(options, "candidate_sha"):
        requested = comparison_pair(getattr(options, "baseline_sha", None),
                                    getattr(options, "candidate_sha", None))
        if [lane.build["source_sha"] for lane in lanes] != requested:
            raise AssertionError("paired build receipts differ from requested immutable commits")
    if (lanes[0].driver == lanes[1].driver
            or lanes[0].build["source"] == lanes[1].build["source"]
            or lanes[0].build["source_sha"] == lanes[1].build["source_sha"]):
        raise ValueError("paired clients require separate checkouts and distinct immutable versions")
    if (lanes[0].build["rustc_version"] != lanes[1].build["rustc_version"]
            or lanes[0].build["cargo_version"] != lanes[1].build["cargo_version"]):
        raise ValueError("paired clients require the same build toolchain")
    driver, digest = getattr(options, "driver", None), getattr(options, "driver_sha256", None)
    if ((driver is not None and Path(driver).resolve(strict=True) != lanes[0].driver)
            or (digest is not None and digest != lanes[0].driver_sha256)):
        raise ValueError("explicit paired driver binding differs from client A's build receipt")
    return lanes


def validate(lane, deadline):
    if lane.receipt is None:
        common.driver_binding(lane)
    elif load(lane.receipt, lane.label, deadline).build != lane.build:
        raise AssertionError("client build receipt changed after admission")


def read_profile_mode(lane, requested, deadline):
    """Probe only an explicitly requested diagnostic, outside all side timers.

    A pinned older v3 binary remains runnable without the new optional flag;
    its profiling data is explicitly absent, never inferred to be zero.
    """
    if type(requested) is not bool:
        raise ValueError("read profiling requires an explicit boolean opt-in")
    if not requested:
        return "disabled"
    validate(lane, deadline)
    help_text = common.command([str(lane.driver), "serve", "--help"],
                               min(deadline, time.monotonic() + 30))
    if len(help_text) > 65536:
        raise ValueError("client diagnostic capability response is too large")
    validate(lane, deadline)
    # Match a clap option line, not prose mentioning some other capability.
    supported = re.search(rb"(?m)^\s+--workspace-read-profile(?:\s|$)", help_text) is not None
    return "enabled" if supported else "unsupported"


DEFAULT_BASELINE = "d265e31169fb2f8b137ce9922784ebd0238c6397"
DEFAULT_CANDIDATE = "f18b99645d7bbbc5b4ea3374165e57dc4ff9f922"


def comparison_pair(baseline, candidate):
    if (any(type(value) is not str or re.fullmatch(r"[0-9a-f]{40}", value) is None
            for value in (baseline, candidate)) or baseline == candidate):
        raise ValueError("comparison requires two distinct immutable full commit SHAs")
    return [baseline, candidate]


def add_arguments(parser):
    parser.add_argument("--paired", action="store_true")
    parser.add_argument("--build-a", type=Path)
    parser.add_argument("--build-b", type=Path)
    parser.add_argument("--baseline-sha", default=DEFAULT_BASELINE)
    parser.add_argument("--candidate-sha", default=DEFAULT_CANDIDATE)
    parser.add_argument("--workspace-read-profile", action="store_true",
                        help="Opt into read diagnostics; instrumented timings are diagnostic only")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--label", choices=("a", "b", "server"), required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    opts = parser.parse_args()
    build(opts.source, opts.source_sha, opts.label, opts.receipt,
          float(os.environ["MST2_BUILD_DEADLINE_MONOTONIC"]))


if __name__ == "__main__":
    main()
