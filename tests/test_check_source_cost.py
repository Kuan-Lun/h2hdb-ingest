"""Source acceptance rejects excessive or missing work evidence independently."""

from __future__ import annotations

import copy
import json
import os
import py_compile
import runpy
import signal
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from time import monotonic, sleep
from types import SimpleNamespace
from typing import Any

import pytest
from h2hdb import ArtifactSourceRole, FileContentReceipt

_SCRIPT = Path(__file__).parents[1] / "scripts" / "check-source-cost.py"
pytestmark = pytest.mark.skipif(
    os.name != "posix",
    reason="manual source acceptance and its supervisor are POSIX-only",
)


@pytest.fixture
def probe() -> dict[str, Any]:
    return runpy.run_path(str(_SCRIPT))


@pytest.fixture
def measured(probe: dict[str, Any], tmp_path: Path) -> dict[str, Any]:
    probe["_HELPERS"]["_fixture"](tmp_path, 1, 1, 16, "png")
    result: dict[str, Any] = probe["_case"](tmp_path, mode="metadata_only", workers=1)
    return result


def test_real_boundaries_repeat_cycles_and_retry_account_all_adapter_work(
    probe: dict[str, Any], tmp_path: Path
) -> None:
    report = probe["_run"](
        sizes=(127, 128, 129, 512),
        edge=16,
        codecs=("png",),
        repeats=2,
        workers=1,
        image_cases=False,
        workspace=tmp_path,
    )
    probe["_validate_report"](
        report, dimensions={(p, 16, "png") for p in (127, 128, 129, 512)}, repeats=2
    )
    assert report["status"] == "completed"
    assert len(report["cases"]) == 28
    assert report["provenance"]["acceptance_sha256"]
    for case in report["cases"]:
        pages = case["fixture"]["pages"]
        assert case["telemetry_comparison"]["matched"]
        assert case["process_cpu_seconds"] >= 0
        assert case["entries"]["scandir_rows"] >= case["entries"]["revalidation_rows"]
        assert (
            case["entries"]["entry_stat_calls"]
            >= case["entries"]["revalidation_stat_calls"]
        )
        if case["mode"] == "marker_only":
            assert case["decode_calls"] == 0
            assert case["acceptance"]["status"] == "satisfied"
        else:
            assert case["returned_rows"] == {
                "file_rows": pages + 1,
                "directory_rows": pages + 1,
                "tag_rows": 2,
            }
            assert case["decode_calls"] == (
                0 if case["mode"] == "metadata_only" else pages
            )
        if case["mode"] == "retry":
            assert case["prior_interrupted_attempt"]["interrupted"]
            assert (
                case["prior_interrupted_attempt"]["production_telemetry"]["status"]
                == "interrupted"
            )
        checks = case["acceptance"]["checks"]
        assert case["acceptance"]["status"] == (
            "satisfied" if all(c["met"] for c in checks.values()) else "violated"
        )


def test_actual_extra_adapter_read_is_rejected_without_telemetry_disagreement(
    probe: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    probe["_HELPERS"]["_fixture"](tmp_path, 1, 1, 16, "png")
    original = probe["_observe"]

    def repeated(adapter: Any, *, mode: str, oracle: dict[str, Any]) -> Any:
        result = original(adapter, mode=mode, oracle=oracle)
        observation = adapter.observe_gallery(("1000000",))
        adapter.list_file_observations(observation, after_name_bytes=None, limit=256)
        return result

    monkeypatch.setitem(probe["_case"].__globals__, "_observe", repeated)
    case = probe["_case"](tmp_path, mode="metadata_only", workers=1)
    page_bytes = probe["_HELPERS"]["_manifest"](tmp_path)["page_bytes"]
    costs = probe["_costs"](case, pages=1, page_bytes=page_bytes)
    assert case["telemetry_comparison"]["matched"]
    assert costs["status"] == "violated"
    assert costs["checks"]["source_page_read_bytes"]["observed"] == 2 * page_bytes


def test_entry_scan_budget_rejects_a_deliberately_degraded_real_path(
    probe: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    probe["_HELPERS"]["_fixture"](tmp_path, 1, 1, 16, "png")
    original = probe["_observe"]

    def rescanned(adapter: Any, *, mode: str, oracle: dict[str, Any]) -> Any:
        result = original(adapter, mode=mode, oracle=oracle)
        for _ in range(9):
            adapter.observe_gallery(("1000000",))
        return result

    monkeypatch.setitem(probe["_case"].__globals__, "_observe", rescanned)
    case = probe["_case"](tmp_path, mode="metadata_only", workers=1)
    costs = probe["_costs"](case, pages=1, page_bytes=10_000)
    assert costs["status"] == "violated"
    assert costs["checks"]["entry_stat_calls"]["met"] is False


@pytest.mark.parametrize(
    "missing",
    (
        "io",
        "source_files",
        "returned_rows",
        "content_oracle",
        "decode_calls",
        "production_telemetry",
        "entries",
    ),
)
def test_missing_evidence_is_incomplete_not_zero_cost(
    probe: dict[str, Any], measured: dict[str, Any], missing: str
) -> None:
    measured.pop(missing)
    assert (
        probe["_costs"](measured, pages=1, page_bytes=10_000)["status"] == "incomplete"
    )


@pytest.mark.parametrize("category", ("io", "source_files"))
def test_empty_io_measurement_is_not_a_pass(
    probe: dict[str, Any], measured: dict[str, Any], category: str
) -> None:
    measured[category] = {}
    assert (
        probe["_costs"](measured, pages=1, page_bytes=10_000)["status"] == "incomplete"
    )


def test_parent_recomputes_costs_and_rejects_omitted_matrix_cases(
    probe: dict[str, Any], tmp_path: Path
) -> None:
    report = probe["_run"](
        sizes=(1,),
        edge=16,
        codecs=("png",),
        repeats=2,
        workers=1,
        image_cases=False,
        workspace=tmp_path,
    )
    falsified = copy.deepcopy(report)
    first = falsified["cases"][0]
    first["entries"]["entry_stat_calls"] = 1_000_000
    first["acceptance"] = {"status": "satisfied"}
    falsified["acceptance"] = {"status": "satisfied"}
    probe["_validate_report"](falsified, dimensions={(1, 16, "png")}, repeats=2)
    assert falsified["acceptance"]["status"] == "violated"
    report["cases"].pop()
    with pytest.raises(ValueError, match="omitted"):
        probe["_validate_report"](report, dimensions={(1, 16, "png")}, repeats=2)


def test_cli_exit_code_reports_real_acceptance_outcome(tmp_path: Path) -> None:
    output = tmp_path / "report.json"
    result = subprocess.run(
        (
            sys.executable,
            str(_SCRIPT),
            "--sizes",
            "1",
            "--edge",
            "16",
            "--codec",
            "png",
            "--skip-image-cases",
            "--output",
            str(output),
        ),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    report = json.loads(output.read_text())
    assert report["status"] == "completed"
    assert (
        result.returncode
        == {"satisfied": 0, "violated": 1, "incomplete": 2}[
            report["acceptance"]["status"]
        ]
    )


def test_cli_timeout_is_incomplete_and_preserves_error_evidence(
    probe: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "report.json"

    def timeout(command: list[str], **_kwargs: object) -> None:
        raise subprocess.TimeoutExpired(command, 10)

    monkeypatch.setitem(probe["main"].__globals__, "_bounded_worker", timeout)
    monkeypatch.setattr(sys, "argv", [str(_SCRIPT), "--output", str(output)])
    assert probe["main"]() == 2
    report = json.loads(output.read_text())
    assert report["status"] == "error"
    assert report["acceptance"]["status"] == "incomplete"
    assert report["error_type"] == "TimeoutExpired"


def test_existing_output_is_never_overwritten(
    probe: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "report.json"
    output.write_bytes(b"previous evidence")
    monkeypatch.setattr(sys, "argv", [str(_SCRIPT), "--output", str(output)])
    with pytest.raises(SystemExit) as caught:
        probe["main"]()
    assert caught.value.code == 2
    assert output.read_bytes() == b"previous evidence"


@pytest.mark.parametrize(
    "mutation",
    ("failed_status", "missing_phases", "missing_zero_contract", "incomplete_rows"),
)
def test_attribution_and_complete_observation_are_required(
    probe: dict[str, Any], measured: dict[str, Any], mutation: str
) -> None:
    if mutation == "failed_status":
        measured["production_telemetry"]["status"] = "failed"
    elif mutation == "missing_phases":
        measured["production_telemetry"]["phases_ns_inclusive"] = {}
    elif mutation == "missing_zero_contract":
        measured["production_telemetry"].pop("absent_zero_counters")
    else:
        measured["returned_rows"]["file_rows"] = 0
    assert (
        probe["_costs"](measured, pages=1, page_bytes=10_000)["status"] == "incomplete"
    )


def test_installed_runtime_drift_is_rejected_before_measurement_claim(
    probe: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    original = probe["_HELPERS"]["_provenance"]

    def other_runtime() -> dict[str, Any]:
        result: dict[str, Any] = original()
        result["h2hdb_ingest"]["python_source_sha256"] = "0" * 64
        return result

    monkeypatch.setitem(probe["_HELPERS"], "_provenance", other_runtime)
    with pytest.raises(ValueError, match="imported ingest runtime"):
        probe["_provenance"]()


def test_worker_source_changes_during_measurement_cannot_be_reported_as_current(
    probe: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = probe["_provenance"]
    calls = 0

    def changing(*, include_git: bool = True) -> dict[str, Any]:
        nonlocal calls
        result: dict[str, Any] = original(include_git=include_git)
        calls += 1
        if calls > 1:
            result["acceptance_sha256"] = "0" * 64
        return result

    monkeypatch.setitem(probe["_run"].__globals__, "_provenance", changing)
    with pytest.raises(RuntimeError, match="changed during measurement"):
        probe["_run"](
            sizes=(1,),
            edge=16,
            codecs=("png",),
            repeats=2,
            workers=1,
            image_cases=False,
            workspace=tmp_path,
        )


def test_parent_rejects_source_change_between_launch_and_validation(
    probe: dict[str, Any], tmp_path: Path
) -> None:
    report = probe["_run"](
        sizes=(1,),
        edge=16,
        codecs=("png",),
        repeats=2,
        workers=1,
        image_cases=False,
        workspace=tmp_path,
    )
    before = probe["_provenance"](include_git=False)
    before["h2hdb"]["python_source_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="supervisor source changed"):
        probe["_validate_report"](
            report, dimensions={(1, 16, "png")}, repeats=2, initial_provenance=before
        )


def test_worker_provenance_never_spawns_nested_git_owners(
    probe: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbidden(*_arguments: object) -> None:
        raise AssertionError("worker must not create nested process groups for Git")

    monkeypatch.setitem(probe["_provenance"].__globals__, "_git_text", forbidden)
    result = probe["_provenance"](include_git=False)
    assert result["ingest_checkout_source_verified"]
    assert "checkout_commit" not in result


@pytest.mark.skipif(
    os.name != "posix", reason="manual acceptance supervisor is POSIX-only"
)
def test_timeout_reaps_actual_descendant_without_pipe_drain_hang(
    probe: dict[str, Any], tmp_path: Path
) -> None:
    child_pid = tmp_path / "child.pid"
    program = (
        "import pathlib,signal,subprocess,sys,time\n"
        "def stop(*args): raise KeyboardInterrupt\n"
        "signal.signal(signal.SIGTERM, stop)\n"
        "child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)'])\n"
        "pathlib.Path(sys.argv[1]).write_text(str(child.pid))\n"
        "try: time.sleep(30)\n"
        "finally: child.wait()\n"
    )
    started = monotonic()
    with pytest.raises(subprocess.TimeoutExpired):
        probe["_bounded_worker"](
            [sys.executable, "-c", program, str(child_pid)],
            timeout=3.0,
            workspace=tmp_path,
        )
    assert monotonic() - started < 5.0
    pid = int(child_pid.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


def test_parent_provenance_hanging_git_has_a_bounded_owned_tree(
    probe: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    child_pid = tmp_path / "git-child.pid"
    fake_git = tmp_path / "git"
    fake_git.write_text(
        f"#!{sys.executable}\n"
        "import pathlib,signal,subprocess,sys,time\n"
        "def stop(*args): raise KeyboardInterrupt\n"
        "signal.signal(signal.SIGTERM, stop)\n"
        "child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)'])\n"
        f"pathlib.Path({str(child_pid)!r}).write_text(str(child.pid))\n"
        "try: time.sleep(30)\n"
        "finally: child.wait()\n"
    )
    fake_git.chmod(0o700)
    monkeypatch.setenv("PATH", str(tmp_path) + os.pathsep + os.environ["PATH"])
    monkeypatch.setitem(probe["_git_text"].__globals__, "_GIT_TIMEOUT", 3.0)
    started = monotonic()
    with pytest.raises(subprocess.TimeoutExpired):
        probe["_provenance"]()
    assert monotonic() - started < 5.0
    with pytest.raises(ProcessLookupError):
        os.kill(int(child_pid.read_text()), 0)


def test_sigterm_supervisor_reaps_worker_and_descendant(tmp_path: Path) -> None:
    child_pid, worker_pid = tmp_path / "child.pid", tmp_path / "worker.pid"
    worker = tmp_path / "worker.py"
    worker.write_text(
        "import os,pathlib,signal,subprocess,sys,time\n"
        "def stop(*args): raise KeyboardInterrupt\n"
        "signal.signal(signal.SIGTERM, stop)\n"
        "child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)'])\n"
        f"pathlib.Path({str(child_pid)!r}).write_text(str(child.pid))\n"
        f"pathlib.Path({str(worker_pid)!r}).write_text(str(os.getpid()))\n"
        "try: time.sleep(30)\n"
        "finally: child.wait()\n"
    )
    parent = (
        "import pathlib,runpy,sys\n"
        f"module=runpy.run_path({str(_SCRIPT)!r})\n"
        "try:\n"
        f" module['_bounded_worker']([sys.executable,{str(worker)!r}], timeout=30, workspace=pathlib.Path({str(tmp_path)!r}))\n"
        "except RuntimeError as error:\n"
        " print(type(error).__name__, flush=True)\n"
        " raise SystemExit(2)\n"
    )
    with (tmp_path / "parent.stdout").open("w+b") as output:
        process = subprocess.Popen(
            [sys.executable, "-c", parent], stdout=output, stderr=output
        )
        try:
            deadline = monotonic() + 5.0
            while not worker_pid.exists() and monotonic() < deadline:
                sleep(0.01)
            assert worker_pid.exists()
            started = monotonic()
            process.send_signal(signal.SIGTERM)
            assert process.wait(timeout=5) == 2
            assert monotonic() - started < 5.0
            for path in (worker_pid, child_pid):
                with pytest.raises(ProcessLookupError):
                    os.kill(int(path.read_text()), 0)
            output.seek(0)
            assert b"_WorkerInterrupted" in output.read()
        finally:
            if worker_pid.exists():
                try:
                    os.killpg(int(worker_pid.read_text()), signal.SIGKILL)
                except ProcessLookupError:
                    pass
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)


@pytest.mark.parametrize("boundary", ("_bounded_worker", "_provenance"))
def test_cli_io_failure_is_incomplete_not_a_measured_violation(
    probe: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    output = tmp_path / "report.json"

    def unavailable(*_args: object, **_kwargs: object) -> None:
        raise FileNotFoundError("injected unavailable process or runtime source")

    monkeypatch.setitem(probe["main"].__globals__, boundary, unavailable)
    monkeypatch.setattr(sys, "argv", [str(_SCRIPT), "--output", str(output)])
    assert probe["main"]() == 2
    report = json.loads(output.read_text())
    assert report["status"] == "error"
    assert report["acceptance"]["status"] == "incomplete"
    assert report["error_type"] == "FileNotFoundError"


@pytest.mark.parametrize(
    ("method", "corruption"),
    (
        ("list_file_observations", "duplicate"),
        ("list_file_observations", "order"),
        ("list_file_observations", "digest"),
        ("list_file_observations", "role"),
        ("list_directory_observations", "duplicate"),
        ("list_directory_observations", "stat"),
        ("list_tag_observations", "duplicate"),
        ("list_tag_observations", "value"),
        ("list_tag_observations", "cursor"),
    ),
)
def test_independent_fixture_oracle_rejects_same_count_corrupted_pages(
    probe: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    corruption: str,
) -> None:
    probe["_HELPERS"]["_fixture"](tmp_path, 1, 2, 16, "png")
    adapter_type = probe["VNextFilesystemSourceAdapter"]
    original = getattr(adapter_type, method)

    def corrupt(*args: Any, **kwargs: Any) -> Any:
        page = original(*args, **kwargs)
        items = list(page.items)
        if corruption == "duplicate":
            items[1] = items[0]
        elif corruption == "order":
            items.reverse()
        elif corruption == "digest":
            items[0] = replace(
                items[0], content=FileContentReceipt.from_parts((b"wrong bytes",))
            )
        elif corruption == "role":
            items[0] = replace(items[0], artifact_role=ArtifactSourceRole.OTHER)
        elif corruption == "stat":
            items[0] = replace(items[0], size_bytes=items[0].size_bytes + 1)
        elif corruption == "value":
            items[0] = replace(items[0], value="wrong tag")
        return SimpleNamespace(
            items=tuple(items),
            terminal=page.terminal,
            next_after=1 if corruption == "cursor" else page.next_after,
        )

    monkeypatch.setattr(adapter_type, method, corrupt)
    with pytest.raises(ValueError, match="independent fixture oracle"):
        probe["_case"](tmp_path, mode="metadata_only", workers=1)


def test_independent_fixture_oracle_rejects_metadata_changes(
    probe: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probe["_HELPERS"]["_fixture"](tmp_path, 1, 1, 16, "png")
    adapter_type = probe["VNextFilesystemSourceAdapter"]
    original = adapter_type.observe_gallery

    def corrupt(*args: Any, **kwargs: Any) -> Any:
        observed = original(*args, **kwargs)
        return replace(
            observed, metadata=replace(observed.metadata, title="wrong title")
        )

    monkeypatch.setattr(adapter_type, "observe_gallery", corrupt)
    with pytest.raises(ValueError, match=r"metadata.*independent fixture oracle"):
        probe["_case"](tmp_path, mode="metadata_only", workers=1)


def test_fresh_worker_does_not_load_timestamp_valid_stale_bytecode(
    probe: dict[str, Any],
    tmp_path: Path,
) -> None:
    module = tmp_path / "stale_runtime.py"
    module.write_text("VALUE = 'old'\n")
    identity = module.stat()
    py_compile.compile(str(module), doraise=True)
    module.write_text("VALUE = 'new'\n")
    os.utime(module, ns=(identity.st_atime_ns, identity.st_mtime_ns))
    program = (
        f"import sys; sys.path.insert(0, {str(tmp_path)!r}); "
        "import stale_runtime; print(stale_runtime.VALUE)"
    )
    command = [sys.executable, "-c", program]
    baseline_env = dict(os.environ)
    baseline_env.pop("PYTHONPYCACHEPREFIX", None)
    stale = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
        env=baseline_env,
    )
    assert stale.stdout.strip() == "old"  # Real defect, same source mtime and length.
    owned = tmp_path / "owned"
    owned.mkdir()
    command, environment = probe["_fresh_python_environment"](command, owned)
    fresh = probe["_bounded_worker"](
        command, timeout=10, workspace=owned, env=environment
    )
    assert fresh.stdout.strip() == "new"


def test_forced_timeout_removes_real_discovery_index_with_owned_temporary_root(
    probe: dict[str, Any],
    tmp_path: Path,
) -> None:
    receipt = tmp_path / "discovery-path.txt"
    owned = tmp_path / "owned"
    owned.mkdir()
    source = owned / "source"
    source.mkdir()
    program = (
        "import pathlib, signal, time\n"
        "from h2hdb_ingest.filesystem import FilesystemSource\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        f"source = FilesystemSource(pathlib.Path({str(source)!r}))\n"
        "source.list_gallery_locators(after_locator=None, limit=128)\n"
        f"pathlib.Path({str(receipt)!r}).write_text(source._discovery_temporary.name)\n"
        "time.sleep(30)\n"
    )
    command, environment = probe["_fresh_python_environment"](
        [sys.executable, "-c", program], owned
    )
    # Match the real supervisor's outer temporary-directory ownership. The
    # worker ignores SIGTERM, so its FilesystemSource.__exit__ never runs.
    try:
        with pytest.raises(subprocess.TimeoutExpired):
            probe["_bounded_worker"](
                command, timeout=8, workspace=owned, env=environment
            )
        discovery = Path(receipt.read_text())
        assert discovery.is_relative_to(owned / "temporary")
        assert (discovery / "locators.sqlite3").is_file()
    finally:
        import shutil

        shutil.rmtree(owned)
    assert not discovery.exists()
    assert not owned.exists()


@pytest.mark.parametrize(("pages", "edge"), ((4, 1024), (1, 2048)))
def test_real_image_dimension_overlap_runs_each_fixture_only_once(
    probe: dict[str, Any],
    tmp_path: Path,
    pages: int,
    edge: int,
) -> None:
    report = probe["_run"](
        sizes=(pages,),
        edge=edge,
        codecs=("jpeg",),
        repeats=2,
        workers=1,
        image_cases=True,
        workspace=tmp_path,
    )
    expected = {(4, 1024, "jpeg"), (1, 2048, "jpeg")}
    probe["_validate_report"](report, dimensions=expected, repeats=2)
    assert len(report["fixture_setup"]) == 2
    assert len(report["cases"]) == 14
    assert report["acceptance"]["status"] in {"satisfied", "violated"}
