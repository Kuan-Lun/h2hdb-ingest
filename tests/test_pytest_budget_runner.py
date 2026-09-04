from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts" / "run-pytest.py"


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

    def start_sleeping_process() -> subprocess.Popen[bytes]:
        nonlocal active
        active = subprocess.Popen(
            (sys.executable, "-c", "import time; time.sleep(60)"),
            start_new_session=True,
        )
        return active

    monkeypatch.setattr(runner, "_start_process", start_sleeping_process)

    assert runner.run_merge(budget_seconds=0.5) == runner.TIMEOUT_EXIT_CODE
    assert active is not None
    assert active.poll() is not None
    with pytest.raises(ProcessLookupError):
        os.killpg(active.pid, 0)
