from __future__ import annotations

import ctypes
import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, cast
from unittest.mock import Mock, call

import pytest

ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts" / "run-pytest.py"
SUPERVISION_MODEL = ROOT / "verification" / "tla" / "PytestProcessSupervision.tla"
SUPERVISION_PROFILE = (
    ROOT / "verification" / "tla" / "PytestProcessSupervisionSmall.cfg"
)


def _load_module(name: str, path: Path) -> ModuleType:
    specification = importlib.util.spec_from_file_location(name, path)
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[name] = module
    specification.loader.exec_module(module)
    return module


runner = _load_module("h2hdb_ingest_pytest_budget_runner", RUNNER)


def test_merge_runner_has_one_aggregate_five_minute_budget() -> None:
    arguments = runner._arguments(["merge"])

    assert arguments.profile == "merge"
    assert arguments.budget_seconds == 300.0
    assert runner.DEFAULT_MERGE_BUDGET_SECONDS == 300.0


def test_small_tla_profile_wires_process_supervision_invariants() -> None:
    model = SUPERVISION_MODEL.read_text(encoding="utf-8")
    profile = SUPERVISION_PROFILE.read_text(encoding="utf-8")

    assert "OpenStartGate" in model
    assert "LeaderExitWithSurvivor" in model
    assert "TaskkillFailsAndJobCloses" in model
    assert "CleanupDeadlineExpires" in model
    for invariant in (
        "GateRequiresOwnership",
        "ReturnedTreeIsEmptyByProofOrJobClose",
        "SemanticReceiptRequiresEmptyProof",
        "SuccessfulReturnIsClean",
        "SurvivorCannotSucceed",
        "SecondPhaseRequiresEmptyReceipt",
        "DeadlineNeverExtends",
    ):
        assert f"INVARIANT {invariant}" in profile


def test_merge_command_is_parallel_bounded_and_excludes_deep() -> None:
    command = runner._pytest_command()

    assert command[:3] == (sys.executable, "-m", "pytest")
    assert command[3:5] == ("-o", "addopts=")
    assert "not deep" in command
    assert "--numprocesses=auto" in command
    assert "--max-worker-restart=0" in command
    assert "--maxfail=1" in command


def test_merge_environment_cannot_enable_private_or_live_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("H2HDB_TEST_MARIADB", "1")
    monkeypatch.setenv("H2HDB_INGEST_TEST_PRIVATE_CORPUS", "1")
    monkeypatch.setenv("PYTEST_ADDOPTS", "--collect-only")

    environment = runner._environment()

    assert "H2HDB_TEST_MARIADB" not in environment
    assert "H2HDB_INGEST_TEST_PRIVATE_CORPUS" not in environment
    assert "PYTEST_ADDOPTS" not in environment


@pytest.mark.skipif(os.name == "nt", reason="requires POSIX process groups")
def test_timeout_terminates_and_reaps_the_owned_process_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active: subprocess.Popen[bytes] | None = None

    def start_sleeping_process(
        *,
        deadline: float,
        signal_controller: object = None,
    ) -> object:
        nonlocal active
        assert deadline > 0
        assert signal_controller is None
        active = subprocess.Popen(
            (sys.executable, "-c", "import time; time.sleep(60)"),
            start_new_session=True,
        )
        return runner._OwnedProcess(active)

    monkeypatch.setattr(runner, "_start_process", start_sleeping_process)

    assert runner.run_merge(budget_seconds=0.5) == runner.TIMEOUT_EXIT_CODE
    assert active is not None
    assert active.poll() is not None
    with pytest.raises(ProcessLookupError):
        os.killpg(active.pid, 0)


def test_start_process_inherits_output_and_starts_posix_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = Mock()
    popen = Mock(return_value=process)
    monkeypatch.setattr(runner.os, "name", "posix")
    monkeypatch.setattr(runner.subprocess, "Popen", popen)

    owner = runner._start_process(deadline=100.0)

    assert owner.process is process
    assert owner.windows_job is None
    _, keywords = popen.call_args
    assert keywords["start_new_session"] is True
    assert "stdout" not in keywords
    assert "stderr" not in keywords


def test_windows_start_assigns_job_before_opening_start_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    stdin = Mock()
    process = Mock(stdin=stdin)
    job = Mock()
    job.assign.side_effect = lambda _process: events.append("assign")

    def open_start_gate(_token: bytes) -> int:
        events.append("start")
        return 1

    stdin.write.side_effect = open_start_gate

    def popen(*_args: object, **_kwargs: object) -> object:
        events.append("popen")
        return process

    def create_job() -> object:
        events.append("job")
        return job

    monkeypatch.setattr(
        runner.subprocess, "CREATE_NEW_PROCESS_GROUP", 512, raising=False
    )
    monkeypatch.setattr(runner.subprocess, "Popen", popen)
    monkeypatch.setattr(runner._WindowsJob, "create", create_job)
    monkeypatch.setattr(
        runner,
        "_windows_supervisor_launch",
        Mock(return_value=("python", {})),
    )

    owner = runner._start_windows_process(deadline=100.0)

    assert owner.process is process
    assert owner.windows_job is job
    assert events == ["job", "popen", "assign", "start"]
    stdin.close.assert_called_once_with()


def test_windows_job_create_builds_real_ctypes_structures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    kernel32 = Mock()
    kernel32.CreateJobObjectW.return_value = 77
    kernel32.SetInformationJobObject.return_value = True
    kernel32.CloseHandle.return_value = True
    win_dll = Mock(return_value=kernel32)
    monkeypatch.setattr(runner.os, "name", "nt")
    monkeypatch.setattr(runner.ctypes, "WinDLL", win_dll, raising=False)

    job = runner._WindowsJob.create()

    assert job._handle == 77
    win_dll.assert_called_once_with("kernel32", use_last_error=True)
    kernel32.SetInformationJobObject.assert_called_once()
    information_pointer = kernel32.SetInformationJobObject.call_args.args[2]
    assert information_pointer is not None


def test_windows_assignment_failure_never_opens_start_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stdin = Mock()
    process = Mock(stdin=stdin)
    process.poll.side_effect = (None, 0)
    process.wait.return_value = 1
    job = Mock()
    job.assign.side_effect = OSError("assignment failed")
    monotonic = Mock(return_value=90.0)
    monkeypatch.setattr(runner.time, "monotonic", monotonic)
    monkeypatch.setattr(
        runner.subprocess, "CREATE_NEW_PROCESS_GROUP", 512, raising=False
    )
    monkeypatch.setattr(runner.subprocess, "Popen", Mock(return_value=process))
    monkeypatch.setattr(runner._WindowsJob, "create", Mock(return_value=job))
    monkeypatch.setattr(
        runner,
        "_windows_supervisor_launch",
        Mock(return_value=("python", {})),
    )

    with pytest.raises(OSError, match="assignment failed"):
        runner._start_windows_process(deadline=100.0)

    stdin.write.assert_not_called()
    process.kill.assert_called_once_with()
    process.wait.assert_called_once_with(timeout=10.0)
    job.close.assert_called_once_with()


def test_windows_establishment_cleanup_failure_becomes_ownership_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stdin = Mock()
    process = Mock(stdin=stdin)
    process.poll.return_value = None
    process.kill.side_effect = OSError("kill failed")
    process.wait.side_effect = subprocess.TimeoutExpired("supervisor", 1.0)
    job = Mock()
    job.assign.side_effect = KeyboardInterrupt
    monkeypatch.setattr(runner.time, "monotonic", Mock(return_value=100.0))
    monkeypatch.setattr(
        runner.subprocess, "CREATE_NEW_PROCESS_GROUP", 512, raising=False
    )
    monkeypatch.setattr(runner.subprocess, "Popen", Mock(return_value=process))
    monkeypatch.setattr(runner._WindowsJob, "create", Mock(return_value=job))
    monkeypatch.setattr(
        runner,
        "_windows_supervisor_launch",
        Mock(return_value=("python", {})),
    )

    with pytest.raises(runner._ProcessOwnershipError):
        runner._start_windows_process(deadline=101.0)

    stdin.write.assert_not_called()
    job.close.assert_called_once_with()


def test_windows_signal_during_establishment_cleanup_waits_for_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = runner._WindowsSignalController()
    stdin = Mock()
    process = Mock(stdin=stdin)
    job = Mock()
    job.assign.side_effect = OSError("assignment failed")

    def cleanup(*_args: object, **_kwargs: object) -> bool:
        controller.receive(2)
        controller.receive(21)
        return True

    monkeypatch.setattr(
        runner.subprocess, "CREATE_NEW_PROCESS_GROUP", 512, raising=False
    )
    monkeypatch.setattr(runner.subprocess, "Popen", Mock(return_value=process))
    monkeypatch.setattr(runner._WindowsJob, "create", Mock(return_value=job))
    monkeypatch.setattr(
        runner,
        "_windows_supervisor_launch",
        Mock(return_value=("python", {})),
    )
    cleanup_mock = Mock(side_effect=cleanup)
    monkeypatch.setattr(
        runner,
        "_cleanup_unstarted_windows_supervisor",
        cleanup_mock,
    )

    with pytest.raises(runner._RunnerSignalInterrupt) as raised:
        runner._start_windows_process(
            deadline=100.0,
            signal_controller=controller,
        )

    assert raised.value.signal_number == 2
    cleanup_mock.assert_called_once()
    stdin.write.assert_not_called()


def test_windows_venv_launch_uses_base_interpreter(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    base = tmp_path / "python.exe"
    base.touch()
    venv = tmp_path / ".venv" / "Scripts" / "python.exe"
    monkeypatch.setattr(runner.sys, "executable", str(venv))
    monkeypatch.setattr(runner.sys, "_base_executable", str(base), raising=False)

    executable, environment = runner._windows_supervisor_launch()

    assert executable == str(base)
    assert "__PYVENV_LAUNCHER__" not in environment


@pytest.mark.parametrize(
    ("terminated", "expected"),
    ((True, runner.TIMEOUT_EXIT_CODE), (False, runner.TERMINATION_FAILED_EXIT_CODE)),
)
def test_timeout_requires_owned_tree_termination(
    monkeypatch: pytest.MonkeyPatch,
    terminated: bool,
    expected: int,
) -> None:
    process = Mock()
    process.wait.side_effect = subprocess.TimeoutExpired("pytest", 3.0)
    owner = runner._OwnedProcess(process)
    terminate = Mock(return_value=terminated)
    monotonic = Mock(side_effect=(100.0, 101.0))
    monkeypatch.setattr(runner, "_start_process", Mock(return_value=owner))
    monkeypatch.setattr(runner, "_terminate_owned_tree", terminate)
    monkeypatch.setattr(runner.time, "monotonic", monotonic)

    assert runner.run_merge(budget_seconds=10.0) == expected
    process.wait.assert_called_once_with(timeout=4.0)
    terminate.assert_called_once_with(owner, deadline=110.0)


def test_keyboard_interrupt_cleanup_never_extends_absolute_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = Mock()
    process.wait.side_effect = KeyboardInterrupt
    owner = runner._OwnedProcess(process)
    terminate = Mock(return_value=True)
    monotonic = Mock(side_effect=(100.0, 101.0, 102.0))
    monkeypatch.setattr(runner, "_start_process", Mock(return_value=owner))
    monkeypatch.setattr(runner, "_terminate_owned_tree", terminate)
    monkeypatch.setattr(runner.time, "monotonic", monotonic)

    assert runner.run_merge(budget_seconds=300.0) == runner.INTERRUPTED_EXIT_CODE
    terminate.assert_called_once_with(owner, deadline=107.0)


def test_windows_signal_during_timeout_cleanup_waits_for_empty_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = runner._WindowsSignalController()
    process = Mock()
    process.wait.side_effect = subprocess.TimeoutExpired("pytest", 3.0)
    owner = runner._OwnedProcess(process)

    def terminate(_owner: object, *, deadline: float) -> bool:
        assert deadline == 110.0
        controller.receive(2)
        controller.receive(21)
        return True

    monkeypatch.setattr(runner, "_start_process", Mock(return_value=owner))
    monkeypatch.setattr(runner, "_terminate_owned_tree", terminate)
    monkeypatch.setattr(
        runner.time,
        "monotonic",
        Mock(side_effect=(100.0, 101.0)),
    )

    assert (
        runner.run_merge(
            budget_seconds=10.0,
            signal_controller=controller,
        )
        == runner.INTERRUPTED_EXIT_CODE
    )


def test_windows_failed_cleanup_cannot_publish_deferred_interrupt_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = runner._WindowsSignalController()
    process = Mock()
    process.wait.side_effect = subprocess.TimeoutExpired("pytest", 3.0)
    owner = runner._OwnedProcess(process)

    def terminate(_owner: object, *, deadline: float) -> bool:
        assert deadline == 110.0
        controller.receive(2)
        return False

    monkeypatch.setattr(runner, "_start_process", Mock(return_value=owner))
    monkeypatch.setattr(runner, "_terminate_owned_tree", terminate)
    monkeypatch.setattr(
        runner.time,
        "monotonic",
        Mock(side_effect=(100.0, 101.0)),
    )

    assert (
        runner.run_merge(
            budget_seconds=10.0,
            signal_controller=controller,
        )
        == runner.TERMINATION_FAILED_EXIT_CODE
    )


def test_windows_break_cleanup_uses_signal_exit_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = Mock()
    process.wait.side_effect = runner._RunnerSignalInterrupt(21)
    owner = runner._OwnedProcess(process)
    terminate = Mock(return_value=True)
    monkeypatch.setattr(runner, "_start_process", Mock(return_value=owner))
    monkeypatch.setattr(runner, "_terminate_owned_tree", terminate)
    monkeypatch.setattr(
        runner.time,
        "monotonic",
        Mock(side_effect=(100.0, 101.0, 102.0)),
    )

    assert runner.run_merge(budget_seconds=300.0) == 149
    terminate.assert_called_once_with(owner, deadline=107.0)


def test_normal_leader_exit_with_survivor_cleans_and_fails_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = Mock(returncode=0)
    process.wait.return_value = 0
    owner = runner._OwnedProcess(process)
    wait_tree = Mock(return_value=False)
    terminate = Mock(return_value=True)
    monkeypatch.setattr(runner, "_start_process", Mock(return_value=owner))
    monkeypatch.setattr(runner, "_wait_for_owned_tree_exit", wait_tree)
    monkeypatch.setattr(runner, "_terminate_owned_tree", terminate)
    monkeypatch.setattr(
        runner.time,
        "monotonic",
        Mock(side_effect=(100.0, 101.0, 102.0)),
    )

    assert runner.run_merge(budget_seconds=300.0) == runner.TERMINATION_FAILED_EXIT_CODE
    terminate.assert_called_once_with(owner, deadline=400.0)


def test_clean_normal_exit_preserves_pytest_return_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = Mock(returncode=7)
    process.wait.return_value = 7
    owner = runner._OwnedProcess(process)
    monkeypatch.setattr(runner, "_start_process", Mock(return_value=owner))
    monkeypatch.setattr(runner, "_wait_for_owned_tree_exit", Mock(return_value=True))
    monkeypatch.setattr(
        runner.time,
        "monotonic",
        Mock(side_effect=(100.0, 101.0, 102.0)),
    )

    assert runner.run_merge(budget_seconds=300.0) == 7


def test_windows_job_termination_falls_back_to_taskkill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = Mock(pid=4312)
    job = Mock()
    job.terminate.side_effect = OSError("job termination failed")
    owner = runner._OwnedProcess(process, job)
    taskkill = Mock(return_value=True)
    wait_tree = Mock(return_value=True)
    monkeypatch.setattr(runner, "_taskkill_windows_tree", taskkill)
    monkeypatch.setattr(runner, "_wait_for_owned_tree_exit", wait_tree)
    monkeypatch.setattr(runner.time, "monotonic", Mock(return_value=100.0))

    assert runner._terminate_windows_tree(owner, deadline=110.0)
    taskkill.assert_called_once_with(process, deadline=110.0)
    wait_tree.assert_called_once_with(owner, deadline=110.0)


def test_windows_job_query_failure_cannot_issue_success_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = Mock(returncode=0)
    process.wait.return_value = 0
    job = Mock()
    job.wait_empty.side_effect = OSError("query failed")
    owner = runner._OwnedProcess(process, job)
    monkeypatch.setattr(runner, "_start_process", Mock(return_value=owner))
    monkeypatch.setattr(runner.time, "monotonic", Mock(return_value=100.0))

    assert runner.run_merge(budget_seconds=300.0) == runner.TERMINATION_FAILED_EXIT_CODE
    job.terminate.assert_called_once_with()
    job.close.assert_called_once_with()


def test_windows_termination_does_not_start_after_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = Mock()
    owner = runner._OwnedProcess(Mock(), job)
    monkeypatch.setattr(runner.time, "monotonic", Mock(return_value=110.0))

    assert not runner._terminate_windows_tree(owner, deadline=110.0)
    job.terminate.assert_not_called()


@pytest.mark.parametrize(
    "failure",
    (
        subprocess.CompletedProcess((), 1, "", "tree not found"),
        OSError("taskkill unavailable"),
        subprocess.TimeoutExpired("taskkill", 2.0),
    ),
)
def test_windows_taskkill_failure_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    failure: subprocess.CompletedProcess[str] | BaseException,
) -> None:
    run = (
        Mock(side_effect=failure)
        if isinstance(failure, BaseException)
        else Mock(return_value=failure)
    )
    monkeypatch.setattr(runner.subprocess, "run", run)
    monkeypatch.setattr(runner.time, "monotonic", Mock(return_value=100.0))

    assert not runner._taskkill_windows_tree(Mock(pid=4312), deadline=110.0)


def test_windows_taskkill_uses_only_bounded_remaining_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = Mock(return_value=subprocess.CompletedProcess((), 0, "", ""))
    monkeypatch.setattr(runner.subprocess, "run", run)
    monkeypatch.setattr(runner.time, "monotonic", Mock(return_value=109.5))

    assert runner._taskkill_windows_tree(Mock(pid=4312), deadline=110.0)
    assert run.call_args_list == [
        call(
            ("taskkill", "/PID", "4312", "/T", "/F"),
            check=False,
            capture_output=True,
            text=True,
            timeout=0.5,
        )
    ]


def test_windows_job_wait_uses_active_process_count_until_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    accounting_type = type(
        "Accounting",
        (ctypes.Structure,),
        {"_fields_": [("ActiveProcesses", ctypes.c_ulong)]},
    )
    kernel32 = Mock()
    active = iter((1, 0))

    def query(
        _handle: object,
        _kind: object,
        information_pointer: object,
        _size: object,
        _unused: object,
    ) -> bool:
        information: Any = ctypes.cast(
            cast(Any, information_pointer),
            ctypes.POINTER(accounting_type),
        ).contents
        information.ActiveProcesses = next(active)
        return True

    kernel32.QueryInformationJobObject.side_effect = query
    job = runner._WindowsJob(kernel32, 77, accounting_type)
    monotonic = Mock(side_effect=(100.0, 100.1))
    monkeypatch.setattr(runner.time, "monotonic", monotonic)
    monkeypatch.setattr(runner.time, "sleep", Mock())

    assert job.wait_empty(deadline=110.0)
    assert kernel32.QueryInformationJobObject.call_count == 2


def test_windows_break_controller_ignores_repeat_during_cleanup() -> None:
    controller = runner._WindowsSignalController()

    with pytest.raises(runner._RunnerSignalInterrupt):
        controller.receive(21)
    controller.receive(21)


def test_windows_signal_is_deferred_until_job_ownership_boundary() -> None:
    controller = runner._WindowsSignalController()

    with pytest.raises(runner._RunnerSignalInterrupt) as raised:
        with controller.defer():
            controller.receive(2)
            controller.receive(21)
            assert controller.pending_signal == 2
            assert not controller.interruption_started

    assert raised.value.signal_number == 2
    assert controller.pending_signal is None
    assert controller.interruption_started


def test_windows_signal_handlers_cover_sigint_and_sigbreak_and_restore(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sigint = 2
    sigbreak = 21
    old_sigint = object()
    old_sigbreak = object()
    getsignal = Mock(side_effect=(old_sigint, old_sigbreak))
    install = Mock()
    monkeypatch.setattr(runner.os, "name", "nt")
    monkeypatch.setattr(runner.signal, "SIGINT", sigint)
    monkeypatch.setattr(runner.signal, "SIGBREAK", sigbreak, raising=False)
    monkeypatch.setattr(runner.signal, "getsignal", getsignal)
    monkeypatch.setattr(runner.signal, "signal", install)

    with runner._controlled_windows_signals() as controller:
        assert controller is not None
        assert install.call_count == 2
        installed_handler = install.call_args_list[0].args[1]
        assert install.call_args_list[1].args[1] is installed_handler

    assert getsignal.call_args_list == [call(sigint), call(sigbreak)]
    assert install.call_args_list[2:] == [
        call(sigbreak, old_sigbreak),
        call(sigint, old_sigint),
    ]


def test_windows_establishment_failure_returns_infrastructure_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runner,
        "_start_process",
        Mock(side_effect=runner._ProcessOwnershipError("not proven empty")),
    )
    monkeypatch.setattr(runner.time, "monotonic", Mock(return_value=100.0))

    assert runner.run_merge(budget_seconds=300.0) == 125


def test_windows_job_close_failure_overrides_clean_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = Mock(returncode=0)
    process.wait.return_value = 0
    job = Mock()
    job.close.side_effect = OSError("close failed")
    owner = runner._OwnedProcess(process, job)
    monkeypatch.setattr(runner, "_start_process", Mock(return_value=owner))
    monkeypatch.setattr(runner, "_wait_for_owned_tree_exit", Mock(return_value=True))
    monkeypatch.setattr(
        runner.time,
        "monotonic",
        Mock(side_effect=(100.0, 101.0, 102.0)),
    )

    assert runner.run_merge(budget_seconds=300.0) == 125


def test_windows_signal_during_job_close_is_delivered_after_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = runner._WindowsSignalController()
    process = Mock(returncode=0)
    process.wait.return_value = 0
    job = Mock()
    job.close.side_effect = lambda: controller.receive(2)
    owner = runner._OwnedProcess(process, job)
    monkeypatch.setattr(runner, "_start_process", Mock(return_value=owner))
    monkeypatch.setattr(runner, "_wait_for_owned_tree_exit", Mock(return_value=True))
    monkeypatch.setattr(
        runner.time,
        "monotonic",
        Mock(side_effect=(100.0, 101.0, 102.0)),
    )

    assert (
        runner.run_merge(
            budget_seconds=300.0,
            signal_controller=controller,
        )
        == runner.INTERRUPTED_EXIT_CODE
    )
    job.close.assert_called_once_with()
