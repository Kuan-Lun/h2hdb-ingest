"""Real published authority drives source I/O comparisons; timings are evidence."""

from __future__ import annotations

import json
import os
import runpy
import subprocess
import sys
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest
from PIL import Image

from h2hdb_ingest.filesystem import (
    FilesystemArtifactSourceRole,
    FilesystemFileObservation,
    FilesystemSourceChangedError,
    FilesystemStat,
)
from h2hdb_ingest.source_snapshot import SourceSnapshotStore

_SCRIPT = Path(__file__).parents[1] / "scripts" / "probe-source-io.py"


def _io(case: dict[str, Any], category: str, counter: str) -> int:
    result = case["io"].get(category, {}).get(counter, 0)
    assert type(result) is int
    return result


@pytest.mark.parametrize("workers", (1, 4))
@pytest.mark.parametrize("codec", ("png", "jpeg"))
def test_real_source_matrix_measures_reuse_and_independent_invalidation(
    tmp_path: Path, workers: int, codec: str
) -> None:
    report_path = tmp_path / "report.json"
    subprocess.run(
        (
            sys.executable,
            str(_SCRIPT),
            "--galleries",
            "2",
            "--pages",
            "2",
            "--edge",
            "256",
            "--codec",
            codec,
            "--workers",
            str(workers),
            "--timeout",
            "90",
            "--output",
            str(report_path),
        ),
        check=True,
        capture_output=True,
        text=True,
        timeout=100,
    )
    report = json.loads(report_path.read_text())
    assert report["status"] == "ok"
    assert report["format_version"] == 2
    assert report["fixture"]["workers"] == workers
    assert report["fixture"]["codec"] == codec
    assert report["provenance"]["h2hdb"]["python_source_sha256"]
    assert report["provenance"]["checkout_project_version"]
    cases = report["cases"]
    baseline = cases["baseline"]
    unchanged = cases["unchanged"]
    policy = cases["policy_changed"]
    changed = cases["source_changed"]
    assert baseline["source_manifest"] == unchanged["source_manifest"]
    assert baseline["source_manifest"] == policy["source_manifest"]
    assert baseline["source_manifest"]["sha256"] != changed["source_manifest"]["sha256"]
    page_bytes = baseline["source_manifest"]["page_bytes"]

    for case in cases.values():
        assert case["process_cpu_seconds"] >= 0
        assert case["elapsed_seconds"] >= 0
        assert case["galleries"] == 2
        assert case["waiting"] == case["deferred"] == 0
        # File and phase summaries are alternate views of the same raw reads.
        for counter in ("read_bytes", "read_calls"):
            assert sum(item[counter] for item in case["source_files"].values()) == sum(
                item[counter]
                for name, item in case["io"].items()
                if name.startswith("source.")
            )
    for case in (baseline, policy):
        assert case["decode_calls"] == 4
        assert case["qualified_galleries"] == case["accepted_galleries"] == 2
        assert case["captured_files"] == 6
        # Preserve correctness without requiring today's duplicate read cost.
        assert (
            sum(
                item["read_bytes"]
                for name, item in case["source_files"].items()
                if name.endswith(f".{codec}")
            )
            >= page_bytes
        )
    assert unchanged["decode_calls"] == unchanged["captured_files"] == 0
    assert tuple(unchanged["io"]) == ("source.observation.marker",)
    assert (
        _io(unchanged, "source.observation.marker", "read_bytes")
        >= baseline["source_manifest"]["marker_bytes"]
    )
    assert changed["decode_calls"] == 2
    assert changed["qualified_galleries"] == changed["accepted_galleries"] == 1
    assert changed["captured_files"] == 3
    read_pages = {
        name: item["read_bytes"]
        for name, item in changed["source_files"].items()
        if name.endswith(f".{codec}")
    }
    assert set(read_pages) == {f"1000000/000.{codec}", f"1000000/001.{codec}"}
    assert sum(read_pages.values()) >= sum(
        changed["source_manifest"]["page_encoded_bytes"][:2]
    )
    assert unchanged["captured_pages_verified"] == 0
    assert policy["captured_pages_verified"] == 4
    assert changed["captured_pages_verified"] == 2


def test_meter_calibrates_actual_reads_and_separate_buffer_boundary(
    tmp_path: Path,
) -> None:
    module = runpy.run_path(str(_SCRIPT))
    source = tmp_path / "page.png"
    source.write_bytes(b"abcdef")
    meter = module["_Meter"](tmp_path)
    descriptor = os.open(source, os.O_RDONLY)
    try:
        with meter.instrument():
            assert os.read(descriptor, 2) == b"ab"
            assert os.read(descriptor, 8) == b"cdef"
            assert os.read(descriptor, 8) == b""
            with module["_CountedStream"](BytesIO(), meter, "scratch") as stream:
                assert stream.write(b"abc") == 3
                stream.seek(0)
                assert stream.read(2) == b"ab"
                assert stream.read() == b"c"
    finally:
        os.close(descriptor)
    result = meter.report()
    assert result["io"]["source.observation.page"] == {
        "read_calls": 3,
        "read_bytes": 6,
        "write_calls": 0,
        "write_bytes": 0,
    }
    assert result["source_files"] == {"page.png": {"read_calls": 3, "read_bytes": 6}}
    assert result["io"]["scratch"] == {
        "read_calls": 2,
        "read_bytes": 3,
        "write_calls": 1,
        "write_bytes": 3,
    }


@pytest.mark.parametrize(
    ("flag", "value"),
    (
        ("--galleries", "0"),
        ("--pages", "9"),
        ("--edge", "2049"),
        ("--workers", "5"),
        ("--timeout", "301"),
    ),
)
def test_cli_rejects_out_of_bounds_work_without_writing_report(
    tmp_path: Path, flag: str, value: str
) -> None:
    output = tmp_path / "result.json"
    result = subprocess.run(
        (sys.executable, str(_SCRIPT), flag, value, "--output", str(output)),
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 2
    assert "must be in" in result.stderr
    assert not output.exists()


def test_cli_rejects_aggregate_pixels_before_launching_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = runpy.run_path(str(_SCRIPT))
    output = tmp_path / "result.json"

    def forbidden_child(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("oversized fixture must be rejected before work")

    monkeypatch.setattr(subprocess, "run", forbidden_child)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(_SCRIPT),
            "--galleries",
            "4",
            "--pages",
            "8",
            "--edge",
            "2048",
            "--output",
            str(output),
        ],
    )
    with pytest.raises(SystemExit) as caught:
        module["main"]()
    assert caught.value.code == 2
    assert not output.exists()


@pytest.mark.parametrize("codec", ("png", "jpeg"))
def test_fixture_is_deterministic_and_crosses_disk_spool_boundary(
    tmp_path: Path, codec: str
) -> None:
    module = runpy.run_path(str(_SCRIPT))
    first, second = tmp_path / "first", tmp_path / "second"
    for root in (first, second):
        module["_fixture"](root, 1, 1, 2048, codec)
    manifest = module["_manifest"](first)
    assert manifest == module["_manifest"](second)
    assert manifest["pages_above_spool_threshold"] == 1
    assert manifest["page_bytes"] < module["_MAX_FIXTURE_ENCODED_BYTES"]
    with Image.open(first / "1000000" / f"000.{codec}") as image:
        assert image.size == (2048, 2048)
        assert image.format == codec.upper()


def test_fixture_enforces_encoded_byte_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = runpy.run_path(str(_SCRIPT))
    monkeypatch.setitem(module["_fixture"].__globals__, "_MAX_FIXTURE_ENCODED_BYTES", 1)
    with pytest.raises(ValueError, match="aggregate fixture encoded bytes"):
        module["_fixture"](tmp_path, 1, 1, 16, "png")


def test_real_source_change_still_fails_closed_and_restores_instrumentation(
    tmp_path: Path,
) -> None:
    module = runpy.run_path(str(_SCRIPT))
    path = tmp_path / "001.png"
    path.write_bytes(b"before")
    observed = FilesystemFileObservation(
        folder=tmp_path,
        name_bytes=path.name.encode(),
        stat=FilesystemStat.from_os_stat(path.stat()),
        artifact_role=FilesystemArtifactSourceRole.PAGE,
    )
    meter = module["_Meter"](tmp_path)
    original_read = os.read
    with SourceSnapshotStore() as captured:
        path.write_bytes(b"changed during observation")
        with pytest.raises(FilesystemSourceChangedError), meter.instrument():
            captured.capture(("1000000",), observed)
        assert captured.open_source(("1000000",), b"001.png") is None
    assert os.read is original_read
    assert meter.captured_files == 0


def test_atomic_report_failure_preserves_previous_complete_document(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = runpy.run_path(str(_SCRIPT))
    output = tmp_path / "result.json"
    output.write_text('{"previous": true}\n')

    def failed_replace(_source: Path, _destination: Path) -> None:
        raise OSError("injected report replacement failure")

    monkeypatch.setattr(os, "replace", failed_replace)
    with pytest.raises(OSError, match="report replacement failure"):
        module["_atomic_report"](output, {"status": "ok"}, overwrite=True)
    assert json.loads(output.read_text()) == {"previous": True}
    assert tuple(tmp_path.iterdir()) == (output,)


@pytest.mark.parametrize(
    "failure", ("timeout", "child_exit", "malformed", "incomplete")
)
def test_cli_errors_are_complete_reports_not_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    module = runpy.run_path(str(_SCRIPT))
    output = tmp_path / "result.json"

    def failed_child(
        command: list[str], **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        match failure:
            case "timeout":
                raise subprocess.TimeoutExpired(command, 10)
            case "child_exit":
                raise subprocess.CalledProcessError(1, command, stderr="fixture failed")
            case _:
                return subprocess.CompletedProcess(
                    command, 0, stdout="{}" if failure == "incomplete" else "not JSON"
                )

    monkeypatch.setattr(subprocess, "run", failed_child)
    monkeypatch.setattr(sys, "argv", [str(_SCRIPT), "--output", str(output)])
    assert module["main"]() == 1
    report = json.loads(output.read_text())
    assert report["status"] == "error"
    assert report["fixture"]["workers"] == 1
    assert (
        report["error_type"]
        == {
            "timeout": "TimeoutExpired",
            "child_exit": "CalledProcessError",
            "malformed": "JSONDecodeError",
            "incomplete": "ValueError",
        }[failure]
    )
    if failure == "child_exit":
        assert report["worker_stderr"] == "fixture failed"


@pytest.mark.parametrize("symlink", (False, True))
def test_existing_output_is_rejected_without_launching_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, symlink: bool
) -> None:
    module = runpy.run_path(str(_SCRIPT))
    output = tmp_path / "result.json"
    if symlink:
        output.symlink_to(tmp_path / "absent-target")
    else:
        output.write_bytes(b"previous evidence")

    def forbidden_child(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("existing output must be rejected before work")

    monkeypatch.setattr(subprocess, "run", forbidden_child)
    monkeypatch.setattr(sys, "argv", [str(_SCRIPT), "--output", str(output)])
    with pytest.raises(SystemExit) as caught:
        module["main"]()
    assert caught.value.code == 2
    if symlink:
        assert output.is_symlink()
    else:
        assert output.read_bytes() == b"previous evidence"


def test_atomic_report_rejects_destination_created_after_preflight(
    tmp_path: Path,
) -> None:
    module = runpy.run_path(str(_SCRIPT))
    output = tmp_path / "result.json"
    output.write_bytes(b"concurrent evidence")
    with pytest.raises(FileExistsError):
        module["_atomic_report"](output, {"status": "ok"})
    assert output.read_bytes() == b"concurrent evidence"
    assert tuple(tmp_path.iterdir()) == (output,)
