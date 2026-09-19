"""The manual wall-time A/B harness verifies bytes and writes honest evidence."""

from __future__ import annotations

import json
import runpy
import subprocess
import sys
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).parents[1] / "scripts/probe-source-snapshot.py"


def test_small_real_ab_report_has_independent_oracles_and_cleanup(
    tmp_path: Path,
) -> None:
    output = tmp_path / "report.json"
    subprocess.run(
        [
            sys.executable,
            str(_SCRIPT),
            "--output",
            str(output),
            "--workspace",
            str(tmp_path),
            "--counts",
            "3",
            "--bytes",
            "4096",
            "--repetitions",
            "3",
            "--monitor-galleries",
            "2",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    report = json.loads(output.read_text())
    assert report["status"] == "completed"
    assert report["supervisor_scratch_removed"]
    assert not report["isolated"]  # A CI smoke is not a dedicated timing run.
    assert report["source_sha256"] and report["baseline_sha256"]
    assert report["observer_baseline_sha256"]
    assert not report["unexecuted"] and not report["errors"] and not report["skipped"]
    (case,) = report["cases"]
    assert case["files"] == 3 and len(case["samples"]) == 3
    assert {sample["order"][0] for sample in case["samples"]} == {
        "historical_per_file",
        "batch_only",
        "bounded_page",
    }
    for sample in case["samples"]:
        for run in sample["runs"]:
            assert run["elapsed_ns"] > 0 and run["process_cpu_ns"] > 0
            assert run["verified_files"] == 3
            assert run["source_metrics"]["elapsed_ns"] == run["elapsed_ns"]
            assert run["source_metrics"]["scope"] == "source_snapshot"
            expected = 3 if run["variant"] == "historical_per_file" else 1
            assert run["sqlite_statements"]["COMMIT"] == expected
    assert tuple(path.name for path in tmp_path.iterdir()) == ("report.json",)


def test_historical_fixture_tampering_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = runpy.run_path(str(_SCRIPT))
    fake = tmp_path / "baseline.py.txt"
    fake.write_text("class SourceSnapshotStore: pass\n")
    monkeypatch.setitem(module["_baseline_type"].__globals__, "_BASELINE", fake)
    with pytest.raises(
        RuntimeError, match="historical source snapshot fixture changed"
    ):
        module["_baseline_type"]()
