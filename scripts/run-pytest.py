#!/usr/bin/env python3
"""Run the lightweight merge pytest profile under one hard deadline."""

from __future__ import annotations

import argparse
import math
import os
import shlex
import signal
import subprocess
import sys
import time
from collections.abc import Sequence
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MERGE_BUDGET_SECONDS = 300.0
TERMINATION_RESERVE_SECONDS = 5.0
TERMINATION_GRACE_SECONDS = 2.0
TIMEOUT_EXIT_CODE = 124
TERMINATION_FAILED_EXIT_CODE = 125
INTERRUPTED_EXIT_CODE = 130


def _positive_seconds(value: str) -> float:
    try:
        seconds = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "must be a number greater than zero"
        ) from error
    if not math.isfinite(seconds) or seconds <= 0:
        raise argparse.ArgumentTypeError("must be a finite number greater than zero")
    return seconds


def _pytest_command() -> tuple[str, ...]:
    return (
        sys.executable,
        "-m",
        "pytest",
        "-o",
        "addopts=",
        "--strict-markers",
        "-m",
        "not deep",
        "--numprocesses=auto",
        "--max-worker-restart=0",
        "--dist=loadgroup",
        "--durations=20",
        "--tb=short",
        "-ra",
        "--maxfail=1",
    )


def _environment() -> dict[str, str]:
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment.pop("PYTEST_ADDOPTS", None)
    environment.pop("H2HDB_TEST_MARIADB", None)
    environment.pop("H2HDB_INGEST_TEST_PRIVATE_CORPUS", None)
    return environment


def _start_process() -> subprocess.Popen[bytes]:
    command = _pytest_command()
    print("+", shlex.join(command), flush=True)
    if os.name == "nt":
        creation_flag = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        if not isinstance(creation_flag, int) or creation_flag == 0:
            raise RuntimeError("Windows requires CREATE_NEW_PROCESS_GROUP")
        return subprocess.Popen(
            command,
            cwd=REPOSITORY_ROOT,
            env=_environment(),
            creationflags=creation_flag,
        )
    return subprocess.Popen(
        command,
        cwd=REPOSITORY_ROOT,
        env=_environment(),
        start_new_session=True,
    )


def _remaining(deadline: float) -> float:
    return max(0.0, deadline - time.monotonic())


def _wait_for_posix_group_exit(
    process: subprocess.Popen[bytes],
    *,
    deadline: float,
) -> bool:
    while True:
        process.poll()
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            remaining = _remaining(deadline)
            try:
                process.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                return False
            return True
        except PermissionError:
            pass
        remaining = _remaining(deadline)
        if remaining == 0:
            return False
        time.sleep(min(0.05, remaining))


def _terminate_process_group(
    process: subprocess.Popen[bytes],
    *,
    deadline: float,
) -> bool:
    if os.name == "nt":
        remaining = _remaining(deadline)
        if remaining == 0:
            return False
        try:
            completed = subprocess.run(
                ("taskkill", "/PID", str(process.pid), "/T", "/F"),
                check=False,
                timeout=remaining,
            )
        except OSError, subprocess.TimeoutExpired:
            return False
        if completed.returncode != 0:
            return False
        try:
            process.wait(timeout=_remaining(deadline))
        except subprocess.TimeoutExpired:
            return False
        return True

    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        try:
            process.wait(timeout=_remaining(deadline))
        except subprocess.TimeoutExpired:
            return False
        return True
    graceful_deadline = min(deadline, time.monotonic() + TERMINATION_GRACE_SECONDS)
    if _wait_for_posix_group_exit(process, deadline=graceful_deadline):
        return True
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    return _wait_for_posix_group_exit(process, deadline=deadline)


def run_merge(*, budget_seconds: float = DEFAULT_MERGE_BUDGET_SECONDS) -> int:
    """Run pytest and verify its process group exits inside the aggregate budget."""

    started = time.monotonic()
    deadline = started + budget_seconds
    pytest_deadline = max(started, deadline - TERMINATION_RESERVE_SECONDS)
    process = _start_process()
    try:
        process.wait(timeout=_remaining(pytest_deadline))
    except subprocess.TimeoutExpired:
        print(
            f"pytest exceeded its {budget_seconds:.1f}s aggregate merge budget",
            file=sys.stderr,
            flush=True,
        )
        if not _terminate_process_group(process, deadline=deadline):
            return TERMINATION_FAILED_EXIT_CODE
        return TIMEOUT_EXIT_CODE
    except KeyboardInterrupt:
        cleanup_deadline = max(deadline, time.monotonic() + TERMINATION_RESERVE_SECONDS)
        if not _terminate_process_group(process, deadline=cleanup_deadline):
            return TERMINATION_FAILED_EXIT_CODE
        return INTERRUPTED_EXIT_CODE

    if os.name != "nt" and not _wait_for_posix_group_exit(process, deadline=deadline):
        _terminate_process_group(process, deadline=deadline)
        print("pytest left a surviving process group", file=sys.stderr, flush=True)
        return TERMINATION_FAILED_EXIT_CODE
    return int(process.returncode or 0)


def _arguments(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", choices=("merge",))
    parser.add_argument(
        "--budget-seconds",
        type=_positive_seconds,
        default=DEFAULT_MERGE_BUDGET_SECONDS,
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _arguments(argv)
    return run_merge(budget_seconds=arguments.budget_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
