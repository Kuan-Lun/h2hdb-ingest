#!/usr/bin/env python3
"""Run the lightweight merge pytest profile under one hard deadline."""

from __future__ import annotations

import argparse
import ctypes
import math
import ntpath
import os
import shlex
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import FrameType
from typing import Any, Final, cast

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MERGE_BUDGET_SECONDS = 300.0
TERMINATION_RESERVE_SECONDS = 5.0
TERMINATION_GRACE_SECONDS = 2.0
TREE_EXIT_POLL_SECONDS = 0.05
NORMAL_TREE_DRAIN_SECONDS = 0.25
TIMEOUT_EXIT_CODE = 124
TERMINATION_FAILED_EXIT_CODE = 125
INTERRUPTED_EXIT_CODE = 130
_WINDOWS_SUPERVISOR_MODE: Final = "--internal-windows-supervisor"
_WINDOWS_START_TOKEN: Final = b"\x01"


class _RunnerSignalInterrupt(BaseException):
    """A Windows console break converted into controlled runner shutdown."""

    def __init__(self, signal_number: int) -> None:
        super().__init__(signal_number)
        self.signal_number = signal_number


class _ProcessOwnershipError(RuntimeError):
    """The runner could not prove that its provisional process tree exited."""


@dataclass
class _WindowsSignalController:
    pending_signal: int | None = None
    defer_depth: int = 0
    interruption_started: bool = False

    def receive(self, signal_number: int) -> None:
        if self.interruption_started:
            return
        if self.defer_depth:
            if self.pending_signal is None:
                self.pending_signal = signal_number
            return
        self._raise(signal_number)

    @contextmanager
    def defer(self) -> Iterator[None]:
        self.defer_depth += 1
        body_failed = True
        try:
            yield
            body_failed = False
        finally:
            self.defer_depth -= 1
            if self.defer_depth == 0 and not body_failed:
                self.raise_pending()

    def raise_pending(self) -> None:
        if self.pending_signal is None:
            return
        pending = self.pending_signal
        self.pending_signal = None
        self._raise(pending)

    def _raise(self, signal_number: int) -> None:
        self.interruption_started = True
        raise _RunnerSignalInterrupt(signal_number)


@contextmanager
def _controlled_windows_signals() -> Iterator[_WindowsSignalController | None]:
    if os.name != "nt":
        yield None
        return
    break_signal = cast(int | None, getattr(signal, "SIGBREAK", None))
    if break_signal is None:
        raise RuntimeError("Windows requires SIGBREAK support")
    signal_numbers = (signal.SIGINT, break_signal)
    previous_handlers = [
        (signal_number, signal.getsignal(signal_number))
        for signal_number in signal_numbers
    ]
    controller = _WindowsSignalController()

    def interrupt(received: int, frame: FrameType | None) -> None:
        del frame
        controller.receive(received)

    installed: list[int] = []
    try:
        for signal_number, _previous_handler in previous_handlers:
            signal.signal(signal_number, interrupt)
            installed.append(signal_number)
        yield controller
    finally:
        previous_by_signal = dict(previous_handlers)
        for signal_number in reversed(installed):
            signal.signal(signal_number, previous_by_signal[signal_number])


@contextmanager
def _defer_windows_interrupts(
    controller: _WindowsSignalController | None,
) -> Iterator[None]:
    if controller is None:
        yield
        return
    with controller.defer():
        yield


class _WindowsJob:
    """A non-inheritable Windows Job with kill-on-owner-close semantics."""

    _KILL_ON_JOB_CLOSE: Final = 0x00002000
    _EXTENDED_LIMIT_INFORMATION: Final = 9
    _BASIC_ACCOUNTING_INFORMATION: Final = 1

    def __init__(
        self,
        kernel32: Any,
        handle: int,
        accounting_information_type: type[ctypes.Structure],
    ) -> None:
        self._kernel32 = kernel32
        self._handle: int | None = handle
        self._accounting_information_type = accounting_information_type

    @classmethod
    def create(cls) -> _WindowsJob:
        if os.name != "nt":
            raise RuntimeError("Windows Job Objects are unavailable")
        from ctypes import wintypes

        class _IoCounters(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_ulonglong),
                ("WriteOperationCount", ctypes.c_ulonglong),
                ("OtherOperationCount", ctypes.c_ulonglong),
                ("ReadTransferCount", ctypes.c_ulonglong),
                ("WriteTransferCount", ctypes.c_ulonglong),
                ("OtherTransferCount", ctypes.c_ulonglong),
            ]

        class _BasicLimitInformation(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_longlong),
                ("PerJobUserTimeLimit", ctypes.c_longlong),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class _ExtendedLimitInformation(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", _BasicLimitInformation),
                ("IoInfo", _IoCounters),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        class _BasicAccountingInformation(ctypes.Structure):
            _fields_ = [
                ("TotalUserTime", ctypes.c_longlong),
                ("TotalKernelTime", ctypes.c_longlong),
                ("ThisPeriodTotalUserTime", ctypes.c_longlong),
                ("ThisPeriodTotalKernelTime", ctypes.c_longlong),
                ("TotalPageFaultCount", wintypes.DWORD),
                ("TotalProcesses", wintypes.DWORD),
                ("ActiveProcesses", wintypes.DWORD),
                ("TotalTerminatedProcesses", wintypes.DWORD),
            ]

        win_dll = getattr(ctypes, "WinDLL", None)
        if not callable(win_dll):
            raise RuntimeError("Windows Job Object APIs are unavailable")
        kernel32 = win_dll("kernel32", use_last_error=True)
        kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.SetInformationJobObject.argtypes = [
            wintypes.HANDLE,
            ctypes.c_int,
            ctypes.c_void_p,
            wintypes.DWORD,
        ]
        kernel32.SetInformationJobObject.restype = wintypes.BOOL
        kernel32.AssignProcessToJobObject.argtypes = [
            wintypes.HANDLE,
            wintypes.HANDLE,
        ]
        kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
        kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
        kernel32.TerminateJobObject.restype = wintypes.BOOL
        kernel32.QueryInformationJobObject.argtypes = [
            wintypes.HANDLE,
            ctypes.c_int,
            ctypes.c_void_p,
            wintypes.DWORD,
            ctypes.c_void_p,
        ]
        kernel32.QueryInformationJobObject.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL

        raw_handle = kernel32.CreateJobObjectW(None, None)
        if not raw_handle:
            raise cls._last_error("CreateJobObjectW")
        handle = int(raw_handle)
        information = _ExtendedLimitInformation()
        information.BasicLimitInformation.LimitFlags = cls._KILL_ON_JOB_CLOSE
        if not kernel32.SetInformationJobObject(
            handle,
            cls._EXTENDED_LIMIT_INFORMATION,
            ctypes.byref(information),
            ctypes.sizeof(information),
        ):
            error = cls._last_error("SetInformationJobObject")
            kernel32.CloseHandle(handle)
            raise error
        return cls(kernel32, handle, _BasicAccountingInformation)

    @staticmethod
    def _last_error(operation: str) -> OSError:
        get_last_error = cast(Any, getattr(ctypes, "get_last_error", lambda: 0))
        code = int(get_last_error())
        return OSError(code, f"{operation} failed with Windows error {code}")

    def assign(self, process: subprocess.Popen[bytes]) -> None:
        process_handle = getattr(process, "_handle", None)
        if process_handle is None:
            raise RuntimeError("Python did not expose the child process handle")
        if self._handle is None:
            raise RuntimeError("Windows Job Object is already closed")
        if not self._kernel32.AssignProcessToJobObject(
            self._handle,
            int(process_handle),
        ):
            raise self._last_error("AssignProcessToJobObject")

    def terminate(self) -> None:
        if self._handle is None:
            return
        if not self._kernel32.TerminateJobObject(self._handle, 1):
            raise self._last_error("TerminateJobObject")

    def active_processes(self) -> int:
        if self._handle is None:
            return 0
        information = self._accounting_information_type()
        if not self._kernel32.QueryInformationJobObject(
            self._handle,
            self._BASIC_ACCOUNTING_INFORMATION,
            ctypes.byref(information),
            ctypes.sizeof(information),
            None,
        ):
            raise self._last_error("QueryInformationJobObject")
        return int(information.ActiveProcesses)

    def wait_empty(self, *, deadline: float) -> bool:
        while self.active_processes() != 0:
            remaining = _remaining(deadline)
            if remaining == 0:
                return False
            time.sleep(min(TREE_EXIT_POLL_SECONDS, remaining))
        return True

    def close(self) -> None:
        handle = self._handle
        if handle is None:
            return
        if not self._kernel32.CloseHandle(handle):
            raise self._last_error("CloseHandle")
        self._handle = None


@dataclass(frozen=True)
class _OwnedProcess:
    process: subprocess.Popen[bytes]
    windows_job: _WindowsJob | None = None


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


def _windows_supervisor_launch() -> tuple[str, dict[str, str]]:
    executable = sys.executable
    base_executable = getattr(sys, "_base_executable", None)
    if not isinstance(base_executable, str) or not base_executable:
        raise RuntimeError("Windows Python did not expose its base executable")
    environment = _environment()
    environment.pop("__PYVENV_LAUNCHER__", None)
    if ntpath.normcase(ntpath.normpath(base_executable)) != ntpath.normcase(
        ntpath.normpath(executable)
    ):
        if not Path(base_executable).is_file():
            raise RuntimeError("Windows Python base executable is unavailable")
        executable = base_executable
    return executable, environment


def _cleanup_unstarted_windows_supervisor(
    process: subprocess.Popen[bytes],
    job: _WindowsJob,
    *,
    assigned: bool,
    deadline: float,
) -> bool:
    cleanup_succeeded = True
    if process.stdin is not None:
        try:
            process.stdin.close()
        except OSError:
            cleanup_succeeded = False
    if assigned:
        try:
            job.terminate()
        except OSError:
            cleanup_succeeded = False
        try:
            cleanup_succeeded = job.wait_empty(deadline=deadline) and cleanup_succeeded
        except OSError:
            cleanup_succeeded = False
    elif process.poll() is None:
        try:
            process.kill()
        except OSError:
            cleanup_succeeded = False
    try:
        process.wait(timeout=_remaining(deadline))
    except subprocess.TimeoutExpired:
        cleanup_succeeded = False
    try:
        job.close()
    except OSError:
        cleanup_succeeded = False
    return cleanup_succeeded and process.poll() is not None


def _start_windows_process(
    *,
    deadline: float,
    signal_controller: _WindowsSignalController | None = None,
) -> _OwnedProcess:
    command = _pytest_command()
    print("+", shlex.join(command), flush=True)
    creation_flag = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    if not isinstance(creation_flag, int) or creation_flag == 0:
        raise RuntimeError("Windows requires CREATE_NEW_PROCESS_GROUP")
    job: _WindowsJob | None = None
    supervisor: subprocess.Popen[bytes] | None = None
    assigned = False
    cleanup_performed = False
    try:
        with _defer_windows_interrupts(signal_controller):
            try:
                job = _WindowsJob.create()
                executable, environment = _windows_supervisor_launch()
                supervisor = subprocess.Popen(
                    (
                        executable,
                        str(Path(__file__).resolve()),
                        _WINDOWS_SUPERVISOR_MODE,
                        *command,
                    ),
                    cwd=REPOSITORY_ROOT,
                    env=environment,
                    stdin=subprocess.PIPE,
                    creationflags=creation_flag,
                )
                job.assign(supervisor)
                assigned = True
                if signal_controller is not None:
                    signal_controller.raise_pending()
                if supervisor.stdin is None:
                    raise RuntimeError(
                        "Windows pytest supervisor start gate is unavailable"
                    )
                written = supervisor.stdin.write(_WINDOWS_START_TOKEN)
                if written != len(_WINDOWS_START_TOKEN):
                    raise BlockingIOError(
                        "Windows pytest supervisor start gate was partial"
                    )
                supervisor.stdin.close()
            except BaseException as startup_error:
                cleaned = True
                if job is None:
                    cleaned = supervisor is None
                elif supervisor is None:
                    try:
                        job.close()
                    except OSError:
                        cleaned = False
                else:
                    cleanup_performed = True
                    cleaned = _cleanup_unstarted_windows_supervisor(
                        supervisor,
                        job,
                        assigned=assigned,
                        deadline=deadline,
                    )
                if not cleaned:
                    raise _ProcessOwnershipError(
                        "Windows pytest supervisor startup failed without a "
                        "complete process-tree cleanup receipt"
                    ) from startup_error
                if signal_controller is not None:
                    signal_controller.raise_pending()
                raise
        return _OwnedProcess(supervisor, job)
    except _RunnerSignalInterrupt as startup_error:
        if job is not None and supervisor is not None and not cleanup_performed:
            cleaned = _cleanup_unstarted_windows_supervisor(
                supervisor,
                job,
                assigned=assigned,
                deadline=deadline,
            )
            if not cleaned:
                raise _ProcessOwnershipError(
                    "Windows pytest supervisor interruption did not produce a "
                    "complete process-tree cleanup receipt"
                ) from startup_error
        raise


def _start_process(
    *,
    deadline: float,
    signal_controller: _WindowsSignalController | None = None,
) -> _OwnedProcess:
    if os.name == "nt":
        return _start_windows_process(
            deadline=deadline,
            signal_controller=signal_controller,
        )
    command = _pytest_command()
    print("+", shlex.join(command), flush=True)
    return _OwnedProcess(
        subprocess.Popen(
            command,
            cwd=REPOSITORY_ROOT,
            env=_environment(),
            start_new_session=True,
        )
    )


def _remaining(deadline: float) -> float:
    return max(0.0, deadline - time.monotonic())


def _wait_for_process(process: subprocess.Popen[bytes], *, deadline: float) -> bool:
    try:
        process.wait(timeout=_remaining(deadline))
    except subprocess.TimeoutExpired:
        return False
    return True


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
            return _wait_for_process(process, deadline=deadline)
        except PermissionError:
            pass
        remaining = _remaining(deadline)
        if remaining == 0:
            return False
        time.sleep(min(TREE_EXIT_POLL_SECONDS, remaining))


def _wait_for_owned_tree_exit(owner: _OwnedProcess, *, deadline: float) -> bool:
    if owner.windows_job is not None:
        try:
            return owner.windows_job.wait_empty(
                deadline=deadline
            ) and _wait_for_process(
                owner.process,
                deadline=deadline,
            )
        except OSError:
            return False
    return _wait_for_posix_group_exit(owner.process, deadline=deadline)


def _taskkill_windows_tree(
    process: subprocess.Popen[bytes],
    *,
    deadline: float,
) -> bool:
    remaining = _remaining(deadline)
    if remaining == 0:
        return False
    try:
        completed = subprocess.run(
            ("taskkill", "/PID", str(process.pid), "/T", "/F"),
            check=False,
            capture_output=True,
            text=True,
            timeout=min(TERMINATION_GRACE_SECONDS, remaining),
        )
    except OSError, subprocess.TimeoutExpired:
        return False
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        print(
            f"taskkill failed with exit {completed.returncode}"
            + (f": {detail}" if detail else ""),
            file=sys.stderr,
            flush=True,
        )
        return False
    return True


def _terminate_windows_tree(owner: _OwnedProcess, *, deadline: float) -> bool:
    job = owner.windows_job
    if job is None or _remaining(deadline) == 0:
        return False
    try:
        job.terminate()
    except OSError as error:
        print(str(error), file=sys.stderr, flush=True)
        if not _taskkill_windows_tree(owner.process, deadline=deadline):
            return False
    return _wait_for_owned_tree_exit(owner, deadline=deadline)


def _terminate_posix_tree(owner: _OwnedProcess, *, deadline: float) -> bool:
    process = owner.process
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return _wait_for_process(process, deadline=deadline)
    graceful_deadline = min(deadline, time.monotonic() + TERMINATION_GRACE_SECONDS)
    if _wait_for_posix_group_exit(process, deadline=graceful_deadline):
        return True
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    return _wait_for_posix_group_exit(process, deadline=deadline)


def _terminate_owned_tree(owner: _OwnedProcess, *, deadline: float) -> bool:
    if owner.windows_job is not None:
        return _terminate_windows_tree(owner, deadline=deadline)
    return _terminate_posix_tree(owner, deadline=deadline)


def _close_owner(owner: _OwnedProcess) -> bool:
    job = owner.windows_job
    if job is None:
        return True
    try:
        job.close()
    except OSError as error:
        print(str(error), file=sys.stderr, flush=True)
        return False
    return True


def _run_deferred_windows_cleanup(
    signal_controller: _WindowsSignalController | None,
    operation: Callable[[], bool],
) -> tuple[bool, int | None]:
    completed = False
    try:
        with _defer_windows_interrupts(signal_controller):
            completed = operation()
    except _RunnerSignalInterrupt as interruption:
        return completed, interruption.signal_number
    return completed, None


def _terminate_for_result(
    owner: _OwnedProcess,
    *,
    deadline: float,
    result_code: int,
    signal_controller: _WindowsSignalController | None,
) -> tuple[int, bool]:
    cleaned, deferred_signal = _run_deferred_windows_cleanup(
        signal_controller,
        lambda: _terminate_owned_tree(owner, deadline=deadline),
    )
    if not cleaned:
        return TERMINATION_FAILED_EXIT_CODE, False
    if deferred_signal is not None:
        return 128 + deferred_signal, True
    return result_code, True


def run_merge(
    *,
    budget_seconds: float = DEFAULT_MERGE_BUDGET_SECONDS,
    signal_controller: _WindowsSignalController | None = None,
) -> int:
    """Run pytest and verify its owned process tree exits inside one deadline."""

    started = time.monotonic()
    deadline = started + budget_seconds
    pytest_deadline = max(started, deadline - TERMINATION_RESERVE_SECONDS)
    owner: _OwnedProcess | None = None
    return_code = TERMINATION_FAILED_EXIT_CODE
    tree_empty_proven = False
    try:
        owner = _start_process(
            deadline=deadline,
            signal_controller=signal_controller,
        )
        try:
            owner.process.wait(timeout=_remaining(pytest_deadline))
        except subprocess.TimeoutExpired:
            print(
                f"pytest exceeded its {budget_seconds:.1f}s aggregate merge budget",
                file=sys.stderr,
                flush=True,
            )
            return_code, tree_empty_proven = _terminate_for_result(
                owner,
                deadline=deadline,
                result_code=TIMEOUT_EXIT_CODE,
                signal_controller=signal_controller,
            )
        except KeyboardInterrupt:
            cleanup_deadline = min(
                deadline,
                time.monotonic() + TERMINATION_RESERVE_SECONDS,
            )
            return_code, tree_empty_proven = _terminate_for_result(
                owner,
                deadline=cleanup_deadline,
                result_code=INTERRUPTED_EXIT_CODE,
                signal_controller=signal_controller,
            )
        except _RunnerSignalInterrupt as interruption:
            cleanup_deadline = min(
                deadline,
                time.monotonic() + TERMINATION_RESERVE_SECONDS,
            )
            return_code, tree_empty_proven = _terminate_for_result(
                owner,
                deadline=cleanup_deadline,
                result_code=128 + interruption.signal_number,
                signal_controller=signal_controller,
            )
        else:
            tree_deadline = min(
                deadline,
                time.monotonic() + NORMAL_TREE_DRAIN_SECONDS,
            )
            if _wait_for_owned_tree_exit(owner, deadline=tree_deadline):
                return_code = int(owner.process.returncode or 0)
                tree_empty_proven = True
            else:
                return_code, tree_empty_proven = _terminate_for_result(
                    owner,
                    deadline=deadline,
                    result_code=TERMINATION_FAILED_EXIT_CODE,
                    signal_controller=signal_controller,
                )
                print(
                    "pytest left a surviving owned process tree",
                    file=sys.stderr,
                    flush=True,
                )
    except KeyboardInterrupt:
        if owner is None:
            return_code = INTERRUPTED_EXIT_CODE
        else:
            cleanup_deadline = min(
                deadline,
                time.monotonic() + TERMINATION_RESERVE_SECONDS,
            )
            return_code, tree_empty_proven = _terminate_for_result(
                owner,
                deadline=cleanup_deadline,
                result_code=INTERRUPTED_EXIT_CODE,
                signal_controller=signal_controller,
            )
    except _RunnerSignalInterrupt as interruption:
        if owner is None:
            return_code = 128 + interruption.signal_number
        else:
            cleanup_deadline = min(
                deadline,
                time.monotonic() + TERMINATION_RESERVE_SECONDS,
            )
            return_code, tree_empty_proven = _terminate_for_result(
                owner,
                deadline=cleanup_deadline,
                result_code=128 + interruption.signal_number,
                signal_controller=signal_controller,
            )
    except _ProcessOwnershipError:
        return_code = TERMINATION_FAILED_EXIT_CODE
    finally:
        if owner is not None:
            close_succeeded, deferred_signal = _run_deferred_windows_cleanup(
                signal_controller,
                lambda: _close_owner(owner),
            )
            if not close_succeeded:
                return_code = TERMINATION_FAILED_EXIT_CODE
            elif deferred_signal is not None:
                if tree_empty_proven:
                    return_code = 128 + deferred_signal
                else:
                    return_code = TERMINATION_FAILED_EXIT_CODE
    return return_code


def _windows_supervisor_main(command: Sequence[str]) -> int:
    """Wait until the parent establishes Job ownership, then run pytest."""

    if os.name != "nt" or not command:
        return TERMINATION_FAILED_EXIT_CODE
    try:
        token = sys.stdin.buffer.read(len(_WINDOWS_START_TOKEN))
    except OSError:
        return TERMINATION_FAILED_EXIT_CODE
    if token != _WINDOWS_START_TOKEN:
        return TERMINATION_FAILED_EXIT_CODE
    try:
        process = subprocess.Popen(tuple(command))
    except OSError as error:
        print(f"could not start pytest: {error}", file=sys.stderr, flush=True)
        return TERMINATION_FAILED_EXIT_CODE
    return process.wait()


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
    with _controlled_windows_signals() as signal_controller:
        return run_merge(
            budget_seconds=arguments.budget_seconds,
            signal_controller=signal_controller,
        )


if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1] == _WINDOWS_SUPERVISOR_MODE:
        raise SystemExit(_windows_supervisor_main(sys.argv[2:]))
    raise SystemExit(main())
