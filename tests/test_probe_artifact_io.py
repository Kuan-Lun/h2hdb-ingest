"""The offline artifact probe uses real runtime results and rejects missing work."""

from __future__ import annotations

import json
import runpy
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

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
    assert report["fixture_removed"] and report["supervisor_scratch_removed"]
    assert report["oracle"]["galleries"] == 2
    assert report["oracle"]["raster_pages"] == 8
    assert report["oracle"]["full_ready_audit"]
    assert report["oracle"]["max_mean_pixel_error"] < 32
    assert report["provenance"]["h2hdb"]["source_sha256"]
    assert (
        report["logical_amplification"]["snapshot_revalidation_reads_per_source_byte"]
        >= 1
    )
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
        ("operation.snapshot_read.logical_bytes", "source pages"),
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
