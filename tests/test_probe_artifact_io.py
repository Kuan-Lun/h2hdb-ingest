"""The offline artifact probe uses real runtime results and rejects missing work."""

from __future__ import annotations

import copy
import json
import os
import py_compile
import runpy
import shutil
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import h2hdb_ingest

_SCRIPT = Path(__file__).parents[1] / "scripts" / "probe-artifact-io.py"


@pytest.fixture(scope="module")
def artifact_probe_report(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    output = tmp_path_factory.mktemp("artifact-io-probe") / "report.json"
    subprocess.run(
        [
            sys.executable,
            str(_SCRIPT),
            "--galleries",
            "2",
            "--pages",
            "4",
            "--edge",
            "64",
            "--workers",
            "2",
            "--timeout",
            "120",
            "--output",
            str(output),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=130,
    )
    result = json.loads(output.read_text())
    assert isinstance(result, dict)
    return result


def test_real_runtime_probe_proves_publication_cleanup_claim_and_rasters(
    artifact_probe_report: dict[str, Any],
) -> None:
    report = artifact_probe_report
    assert report["status"] == "completed"
    assert report["format_version"] == 2
    assert report["fixture_removed"] and report["supervisor_scratch_removed"]
    assert report["oracle"]["galleries"] == 2
    assert report["oracle"]["raster_pages"] == 8
    assert report["oracle"]["full_ready_audit"]
    assert report["oracle"]["max_mean_pixel_error"] < 32
    assert report["provenance"]["h2hdb"]["source_sha256"]
    assert "fresh supervisor-owned cache" in report["execution_binding"]["bytecode"]
    assert (
        report["environment_helper_sha256"] == report["environment_helper_sha256_after"]
    )
    assert report["logical_amplification"]["source_reopen_calls"] >= 10
    assert all(value > 0 for value in report["timings_ns"].values())
    events = report["cycle_events"]
    publication = next(
        i
        for i, line in enumerate(events)
        if "event=publication_completed generation=1 " in line
    )
    library_done = next(
        i
        for i, line in enumerate(events)
        if "event=component_done component=library " in line
    )
    catalog_done = next(
        i
        for i, line in enumerate(events)
        if "event=component_done component=catalog " in line
    )
    next_claim = next(
        i
        for i, line in enumerate(events)
        if "event=ingest_claimed generation=2 " in line
    )
    assert publication < library_done < next_claim
    assert publication < catalog_done < next_claim
    assert "library_done_observed=true catalog_done_observed=true" in events[next_claim]


@pytest.mark.parametrize(
    ("field", "reason"),
    [
        ("operation.protect.calls", "both resources"),
        ("operation.stage_write.logical_bytes", "protected output"),
        ("operation.source_open.calls", "source members"),
    ],
)
def test_probe_negative_control_rejects_missing_measured_work(
    artifact_probe_report: dict[str, Any], field: str, reason: str
) -> None:
    probe = runpy.run_path(str(_SCRIPT))
    metrics = [dict(item) for item in artifact_probe_report["publication_metrics"]]
    adapter = next(item for item in metrics if item["scope"] == "adapter_io")
    adapter[field] = 0
    with pytest.raises(RuntimeError, match=reason):
        probe["_validate_metrics"](
            metrics, artifact_probe_report["oracle"], artifact_probe_report["fixture"]
        )


def test_fixture_repeats_exact_real_jpeg_bytes(tmp_path: Path) -> None:
    probe = runpy.run_path(str(_SCRIPT))
    first, second = tmp_path / "first", tmp_path / "second"
    first.mkdir()
    second.mkdir()
    probe["_fixture"](first, 2, 4, 16)
    probe["_fixture"](second, 2, 4, 16)
    for path in (first / "source").rglob("*"):
        if path.is_file():
            assert path.read_bytes() == (second / path.relative_to(first)).read_bytes()


@pytest.mark.parametrize(
    "arguments",
    [
        ("--galleries", "129"),
        ("--pages", "257"),
        ("--edge", "2049"),
        ("--galleries", "128", "--pages", "256", "--edge", "2048"),
    ],
)
def test_probe_rejects_unbounded_fixture_before_writing_output(
    tmp_path: Path, arguments: tuple[str, ...]
) -> None:
    output = tmp_path / "report.json"
    result = subprocess.run(
        [sys.executable, str(_SCRIPT), *arguments, "--output", str(output)],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 2
    assert not output.exists()


@pytest.mark.parametrize("kind", ["file", "directory"])
def test_controlled_sync_latency_is_attributed_to_selected_info_phase(
    tmp_path: Path, kind: str
) -> None:
    output = tmp_path / "latency.json"
    subprocess.run(
        [
            sys.executable,
            str(_SCRIPT),
            "--galleries",
            "1",
            "--pages",
            "1",
            "--edge",
            "32",
            "--workers",
            "1",
            "--fsync-delay-ms",
            "1",
            "--fsync-delay-kind",
            kind,
            "--output",
            str(output),
            "--timeout",
            "120",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=130,
    )
    report = json.loads(output.read_text())
    metric = next(
        item for item in report["publication_metrics"] if item["scope"] == "adapter_io"
    )
    count = metric[f"operation.{kind}_fsync.calls"]
    elapsed = metric[f"operation.{kind}_fsync.exclusive_ns"]
    assert count > 0
    # Injected delay supplies a lower bound, independent of disk throughput or
    # scheduler overhead. This is not a performance-speed threshold.
    assert elapsed >= count * 1_000_000
    assert report["oracle"]["full_ready_audit"]
    assert report["source_unchanged_during_experiment"]


@pytest.mark.parametrize("mutation", ("runtime", "probe", "helper", "missing"))
def test_parent_rejects_source_drift_without_discarding_partial_evidence(
    artifact_probe_report: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    probe = runpy.run_path(str(_SCRIPT))
    report = copy.deepcopy(artifact_probe_report)
    if mutation == "runtime":
        report["provenance_after"]["h2hdb_ingest"]["source_sha256"] = "0" * 64
    elif mutation == "probe":
        report["probe_sha256_after"] = "0" * 64
    elif mutation == "helper":
        report["environment_helper_sha256_after"] = "0" * 64
    else:
        del report["provenance"]
    # Deliberately keep the old claimed success flag: parent must check digests.
    assert report["status"] == "completed"
    assert report["source_unchanged_during_experiment"]
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(stdout=json.dumps(report)),
    )
    output = tmp_path / "report.json"
    monkeypatch.setattr(sys, "argv", [str(_SCRIPT), "--output", str(output)])
    assert probe["main"]() != 0
    result = json.loads(output.read_text())
    assert result["status"] == "error"
    assert result["acceptance"]["status"] == "incomplete"
    assert result["error_type"] == "SourceProvenanceError"
    assert not result["source_unchanged_during_experiment"]
    assert result["oracle"] == report["oracle"]
    assert result["publication_metrics"] == report["publication_metrics"]


def test_real_cleanup_latency_is_separate_and_source_drift_retains_oracles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    probe = runpy.run_path(str(_SCRIPT))
    provenance = probe["_provenance"]
    calls = 0

    def drifting() -> dict[str, Any]:
        nonlocal calls
        result: dict[str, Any] = provenance()
        calls += 1
        if calls > 1:
            result["h2hdb_ingest"]["source_sha256"] = "0" * 64
        return result

    monkeypatch.setitem(probe["_run"].__globals__, "_provenance", drifting)
    report = probe["_run"](
        SimpleNamespace(
            galleries=1,
            pages=1,
            edge=32,
            workers=1,
            isolated=False,
            fsync_delay_ms=0,
            fsync_delay_kind="all",
            cleanup_journal_delay_ms=2,
        ),
        tmp_path,
    )
    assert report["status"] == "error"
    assert report["acceptance"]["status"] == "incomplete"
    assert report["error_type"] == "SourceProvenanceError"
    assert not report["source_unchanged_during_experiment"]
    assert report["oracle"]["full_ready_audit"]
    assert report["oracle"]["raster_pages"] == 1
    assert any(
        "event=ingest_claimed generation=2 " in event
        and "library_done_observed=true catalog_done_observed=true" in event
        for event in report["cycle_events"]
    )
    cleanup = report["library_cleanup_adapter"]
    assert cleanup["status"] == "completed"
    assert cleanup["calls"] == cleanup["terminal_measurements"] > 0
    assert (
        cleanup["attributed_exclusive_ns"] + cleanup["unattributed_ns"]
        == cleanup["elapsed_ns"]
    )
    journal = cleanup["operations"]["journal_session"]
    assert cleanup["injected_journal_calls"] == journal["calls"] > 0
    assert journal["exclusive_ns"] >= journal["calls"] * 2_000_000
    publication = [
        item
        for item in report["all_metrics"]
        if item["scope"] == "adapter_io" and item["operation"] == "publication"
    ]
    assert publication
    assert all(item["counter.ingest_generation"] > 0 for item in publication)
    assert "directory_fsync" in cleanup["operations"]


def test_cleanup_observer_preserves_failure_and_reports_failed_phase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probe = runpy.run_path(str(_SCRIPT))
    library = probe["library_module"]
    adapter_type = library.ManagedFilesystemLibraryAdapter

    def failed(_adapter: object) -> None:
        with library.adapter_phase("journal_session"):
            raise OSError("injected cleanup read failure")

    monkeypatch.setattr(adapter_type, "maintain_cleanup", failed)
    meter = probe["_CleanupMeasurement"]()
    with meter.observe(), pytest.raises(OSError, match="cleanup read failure"):
        adapter_type.maintain_cleanup(object())
    report = meter.report()
    assert report["status"] == "incomplete"
    assert report["calls"] == report["terminal_measurements"] == 1
    assert report["statuses"]["failed"] == 1
    assert report["operations"]["journal_session"]["failed_calls"] == 1
    assert adapter_type.maintain_cleanup is failed


def test_artifact_worker_compiles_current_runtime_despite_valid_stale_pyc(
    tmp_path: Path,
) -> None:
    probe = runpy.run_path(str(_SCRIPT))
    copied = tmp_path / "runtime" / "h2hdb_ingest"
    shutil.copytree(
        Path(h2hdb_ingest.__file__).parent,
        copied,
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    runtime = copied / "core_source.py"
    original = runtime.read_text()
    runtime.write_text(original + "\nSOURCE_BINDING_SENTINEL = 'old'\n")
    identity = runtime.stat()
    py_compile.compile(str(runtime), doraise=True)
    runtime.write_text(original + "\nSOURCE_BINDING_SENTINEL = 'new'\n")
    os.utime(runtime, ns=(identity.st_atime_ns, identity.st_mtime_ns))
    environment = {**os.environ, "PYTHONPATH": str(copied.parent)}
    environment.pop("PYTHONPYCACHEPREFIX", None)
    baseline = subprocess.run(
        [
            sys.executable,
            "-c",
            "from h2hdb_ingest.core_source import SOURCE_BINDING_SENTINEL; print(SOURCE_BINDING_SENTINEL)",
        ],
        env=environment,
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    assert baseline.stdout.strip() == "old"
    workspace = tmp_path / "owned"
    workspace.mkdir()
    worker_arguments = [
        str(_SCRIPT),
        "--worker",
        "--workspace",
        str(workspace),
        "--galleries",
        "1",
        "--pages",
        "1",
        "--edge",
        "32",
        "--workers",
        "1",
        "--timeout",
        "60",
        "--output",
        str(tmp_path / "unused.json"),
    ]
    program = (
        "from h2hdb_ingest.core_source import SOURCE_BINDING_SENTINEL\n"
        "assert SOURCE_BINDING_SENTINEL == 'new'\n"
        "import runpy,sys\n"
        f"sys.argv={worker_arguments!r}\n"
        f"runpy.run_path({str(_SCRIPT)!r},run_name='__main__')\n"
    )
    command, fresh_environment = probe["_ENVIRONMENT"]["fresh_python_environment"](
        [sys.executable, "-c", program],
        workspace,
    )
    fresh_environment["PYTHONPATH"] = str(copied.parent)
    # Internal worker mode has no nested supervisor/process group. The test's
    # existing POSIX owner bounds and cleans this one actual worker tree.
    owner = runpy.run_path(str(_SCRIPT.with_name("check-source-cost.py")))
    completed = owner["_bounded_worker"](
        command,
        env=fresh_environment,
        timeout=65,
        workspace=workspace,
    )
    report = json.loads(completed.stdout)
    assert report["status"] == "completed"
    assert report["provenance"]["h2hdb_ingest"]["location"] == str(copied)
    assert report["oracle"]["full_ready_audit"]
    assert report["oracle"]["raster_pages"] == 1
    assert report["execution_binding"]["pycache_prefix"] == str(workspace / "pycache")
