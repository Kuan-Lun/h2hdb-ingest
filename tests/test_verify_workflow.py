from __future__ import annotations

import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).parents[1]
_WORKFLOWS = Path(__file__).parents[1] / ".github" / "workflows"
_PURE_JOBS = (
    "windows-pytest-process-supervision",
    "incremental-reference-properties",
    "incremental-reference-properties-deep",
)


def _job_section(job: str) -> str:
    workflow = (_WORKFLOWS / "verify.yml").read_text(encoding="utf-8")
    lines = workflow.split(f"  {job}:\n", 1)[1].splitlines()
    job_lines: list[str] = []
    for line in lines:
        if line.startswith("  ") and not line.startswith("    "):
            break
        job_lines.append(line)
    return "\n".join(job_lines) + "\n"


def _pytest_arguments(job: str) -> list[str]:
    # These jobs deliberately use folded scalar commands. Fail if their shape
    # changes instead of silently executing a hand-maintained copy of the CLI.
    commands = re.findall(
        r"        run: >-\n((?:          [^\n]*\n)+)", _job_section(job)
    )
    assert len(commands) == 1
    arguments = shlex.split(" ".join(commands[0].splitlines()))
    assert arguments[:3] == ["python", "-m", "pytest"]
    return arguments[3:]


def _isolated_environment(tmp_path: Path) -> tuple[dict[str, str], Path]:
    receipt = tmp_path / "imports.log"
    (tmp_path / "sitecustomize.py").write_text(
        """
import importlib.abc
import os
from pathlib import Path
import sys

receipt = Path(os.environ["H2HDB_CI_IMPORT_RECEIPT"])
def record(event):
    with receipt.open("a", encoding="utf-8") as stream:
        stream.write(event + "\\n")
record("started")
class RejectDatabaseRuntime(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in {"h2hdb", "h2hdb_ingest"}:
            record("blocked:" + fullname)
            raise ModuleNotFoundError("standalone CI imported " + fullname)
sys.meta_path.insert(0, RejectDatabaseRuntime())
""",
        encoding="utf-8",
    )
    environment = dict(os.environ)
    for name in (
        "PYTEST_ADDOPTS",
        "PYTEST_PLUGINS",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD",
        "H2HDB_HYPOTHESIS_PROFILE",
    ):
        environment.pop(name, None)
    environment.update(
        PYTHONPATH=str(tmp_path),
        PYTHONDONTWRITEBYTECODE="1",
        PYTHONNOUSERSITE="1",
        H2HDB_CI_IMPORT_RECEIPT=str(receipt),
        HYPOTHESIS_STORAGE_DIRECTORY=str(tmp_path / "hypothesis"),
    )
    return environment, receipt


def _run(
    arguments: list[str], environment: dict[str, str]
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "pytest", *arguments],
        cwd=_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


def test_pure_jobs_install_only_their_manifest_requirements() -> None:
    workflow = (_WORKFLOWS / "verify.yml").read_text(encoding="utf-8")
    for job in _PURE_JOBS[1:]:
        assert "scripts/install-ci-dependencies.py pytest hypothesis\n" in _job_section(
            job
        )
    assert "pytest==" not in workflow
    assert "hypothesis==" not in workflow
    assert "python scripts/install-ci-dependencies.py pytest\n" in workflow


@pytest.mark.parametrize("job", _PURE_JOBS)
def test_pure_job_command_runs_without_database_runtime(
    job: str, tmp_path: Path
) -> None:
    arguments = _pytest_arguments(job)
    environment, receipt = _isolated_environment(tmp_path)
    expected = "3 passed"
    if job.endswith("-deep"):
        profile = re.findall(
            r"^          H2HDB_HYPOTHESIS_PROFILE: (\w+)$",
            _job_section(job),
            re.MULTILINE,
        )
        assert profile == ["nightly"]
        environment["H2HDB_HYPOTHESIS_PROFILE"] = profile[0]
        # Exercise nightly setup and its execution hook within the merge budget;
        # the complete 500-example / 80-step profile remains a manual/CI check.
        arguments += ["-k", "test_generated_invalid_incremental_context_is_rejected"]
        expected = "1 passed"
    elif job.startswith("windows-"):
        # Collect the actual Windows module, then exercise protocol/setup/call/
        # teardown with a portable item. Native Job Object cases keep their own
        # CI process-tree owner and deadline; do not nest them in this subprocess.
        sentinel = tmp_path / "test_bootstrap.py"
        sentinel.write_text(
            "def test_bootstrap():\n    assert True\n", encoding="utf-8"
        )
        arguments += [str(sentinel), "-k", "test_bootstrap"]
        expected = "1 passed, 5 deselected"
    result = _run(arguments, environment)
    assert result.returncode == 0, result.stdout + result.stderr
    assert expected in result.stdout, result.stdout
    assert receipt.read_text(encoding="utf-8").splitlines() == ["started"]


def test_isolation_regression_detects_loading_database_conftest(tmp_path: Path) -> None:
    arguments = _pytest_arguments(_PURE_JOBS[0])
    arguments.remove("--noconftest")
    environment, receipt = _isolated_environment(tmp_path)
    result = _run(arguments, environment)
    assert result.returncode != 0
    assert "standalone CI imported h2hdb" in result.stdout + result.stderr
    assert "blocked:h2hdb" in receipt.read_text(encoding="utf-8").splitlines()


def test_publish_tools_use_manifest_requirements() -> None:
    workflow = (_WORKFLOWS / "publish.yml").read_text(encoding="utf-8")
    assert "python scripts/install-ci-dependencies.py packaging build" in workflow
