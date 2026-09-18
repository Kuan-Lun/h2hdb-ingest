"""Fixed backlog cost evidence and an independent whole-inventory read mutant."""

from __future__ import annotations

import json
import os
import runpy
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

_SCRIPT = Path(__file__).parents[1] / "scripts" / "probe-source-backlog.py"


def _read_pages(paths: list[Path]) -> None:
    for path in paths:
        descriptor = os.open(path, os.O_RDONLY)
        try:
            while os.read(descriptor, 4 * 1024 * 1024):
                pass
        finally:
            os.close(descriptor)


def test_independent_PAGE_budget_rejects_extra_whole_inventory_scan(
    tmp_path: Path,
) -> None:
    module = runpy.run_path(str(_SCRIPT))
    helpers = module["_HELPERS"]
    helpers["_fixture"](tmp_path, 129, 1, 16, "png")
    immutable_manifest = helpers["_manifest"](tmp_path)
    pages = sorted(tmp_path.glob("*/000.png"))
    selected = pages[:8]
    expected_bytes = sum(path.stat().st_size for path in selected)

    def observe(*, extra_scan: bool) -> dict[str, Any]:
        meter = helpers["_Meter"](tmp_path)
        with meter.instrument():
            _read_pages(selected)
            _read_pages(selected)
            if extra_scan:
                _read_pages(pages)
        measured = meter.report()
        measured["production_telemetry"] = {
            "counters": {"locator_rows": 129, "qualified_galleries": 0}
        }
        measured["independent_locators"] = {"rows": 129, "calls": 2}
        result = module["_costs"](
            measured,
            inventory=129,
            selected_page_bytes=expected_bytes,
            all_marker_bytes=0,
            selected_marker_bytes=0,
        )
        assert isinstance(result, dict)
        return result

    baseline = observe(extra_scan=False)
    degraded = observe(extra_scan=True)
    assert baseline["status"] == "satisfied"
    assert (
        baseline["checks"]["PAGE_reads_track_admission"]["observed"]
        == 2 * expected_bytes
    )
    assert degraded["status"] == "violated"
    assert not degraded["checks"]["PAGE_reads_track_admission"]["met"]
    assert (
        degraded["checks"]["PAGE_reads_track_admission"]["upper_bound"]
        == 2 * expected_bytes
    )
    # The mutant only reads; the fixed input and selected bytes are unchanged.
    assert len(pages) == 129
    assert sum(path.stat().st_size for path in selected) == expected_bytes
    assert helpers["_manifest"](tmp_path) == immutable_manifest


@pytest.mark.parametrize("missing", ("locator_rows", "qualified_galleries"))
def test_missing_required_telemetry_is_not_zero_cost(missing: str) -> None:
    module = runpy.run_path(str(_SCRIPT))
    counters = {"locator_rows": 129, "qualified_galleries": 8}
    del counters[missing]
    with pytest.raises(RuntimeError, match="required production counter is missing"):
        module["_costs"](
            {"production_telemetry": {"counters": counters}},
            inventory=129,
            selected_page_bytes=1,
            all_marker_bytes=1,
            selected_marker_bytes=1,
        )


def test_locator_counter_must_match_independent_results() -> None:
    module = runpy.run_path(str(_SCRIPT))
    with pytest.raises(RuntimeError, match="locator rows differ"):
        module["_costs"](
            {
                "production_telemetry": {
                    "counters": {"locator_rows": 0, "qualified_galleries": 8}
                },
                "independent_locators": {"rows": 129, "calls": 2},
            },
            inventory=129,
            selected_page_bytes=1,
            all_marker_bytes=1,
            selected_marker_bytes=1,
        )


@pytest.mark.deep
@pytest.mark.parametrize("inventory", (127, 128, 129, 255, 256, 257, 1024))
def test_real_fixed_backlog_three_publications_cleanup_and_next_claim(
    tmp_path: Path, inventory: int
) -> None:
    output = tmp_path / "report.json"
    subprocess.run(
        [
            sys.executable,
            str(_SCRIPT),
            "--inventory",
            str(inventory),
            "--timeout",
            "240",
            "--output",
            str(output),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=260,
    )
    report = json.loads(output.read_text())
    assert report["status"] == "completed"
    assert report["fixture"]["fixed_inventory"] == inventory
    assert report["fixture"]["max_new_per_batch"] == 8
    assert report["probe_sha256"]
    assert len(report["rounds"]) == 3
    for number, item in enumerate(report["rounds"], start=1):
        assert item["inventory"] == inventory
        assert item["new_admitted"] == 8
        assert (
            item["published"] == item["source_admitted_including_reuse"] == 8 * number
        )
        assert item["pending"] == inventory - 8 * number
        assert item["waiting"] == 0
        assert item["input_manifest_unchanged"]
        assert item["catalog_schema_state"] == "READY"
        assert item["next_claim_granted"]
        assert item["lifecycle_sequence"][0] == "publication_observed"
        assert item["lifecycle_sequence"][-2:] == [
            "next_claim_granted",
            "next_claim_released",
        ]
        assert (
            item["post_publication_cleanup"]["library"][-1]
            == item["post_publication_cleanup"]["catalog"][-1]
            == "DONE"
        )
        assert (
            item["cleanup"]["catalog"][-1] == item["cleanup"]["library"][-1] == "DONE"
        )
        assert item["prepare"]["telemetry_comparison"]["matched"]
        assert (
            item["prepare"]["independent_locators"]["rows"]
            == item["prepare"]["production_telemetry"]["counters"]["locator_rows"]
        )
        assert item["core_source_logs"]["SOURCE_sql_calls"] > 0
        assert item["cost_targets"]["status"] in {"satisfied", "violated"}
        # A measured violation remains a result, not a permanently red runtime
        # gate disguised as an already achieved optimization target.


@pytest.mark.parametrize("inventory", ("0", "23", "1025"))
def test_backlog_cli_bounds_are_checked_before_work(
    tmp_path: Path,
    inventory: str,
) -> None:
    output = tmp_path / "report.json"
    result = subprocess.run(
        [
            sys.executable,
            str(_SCRIPT),
            "--inventory",
            inventory,
            "--output",
            str(output),
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 2
    assert not output.exists()
