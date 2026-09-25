"""The local journal cost gate must distinguish indexed work from retained scans."""

from __future__ import annotations

import copy
import json
import runpy
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts" / "check-library-cleanup-cost.py"


@pytest.fixture(scope="module")
def probe() -> dict[str, Any]:
    return runpy.run_path(str(SCRIPT))


@pytest.fixture(scope="module")
def measured(
    probe: dict[str, Any], tmp_path_factory: pytest.TempPathFactory
) -> dict[str, Any]:
    result: dict[str, Any] = probe["_run"](
        sizes=probe["DEFAULT_SIZES"], workspace=tmp_path_factory.mktemp("library-cost")
    )
    return result


def test_real_production_queries_on_exact_journal_have_three_cycle_boundary_evidence(
    probe: dict[str, Any], measured: dict[str, Any]
) -> None:
    assert measured["status"] == "completed"
    assert measured["acceptance"]["status"] == "violated"
    assert measured["acceptance"]["controls"] == "passed"
    assert measured["evidence_profile"] == "seeded_isolated_sqlite_engine"
    assert measured["cleanup_limit"] == 8
    assert len(measured["cases"]) == len(probe["DEFAULT_SIZES"]) * 2 * 3
    for case in measured["cases"]:
        assert len(case["cycles"]) == 3
        assert all(
            len(c["pages"]) == (3 if case["eligible_tokens"] else 1)
            for c in case["cycles"]
        )
        if case["variant"] == "fixture_index":
            assert case["acceptance"]["status"] == "satisfied"
        elif case["retained_tokens"] >= 4096:
            assert case["acceptance"]["status"] == "violated"
        for cycle in case["cycles"]:
            for pair in cycle["pages"]:
                for kind, event in pair.items():
                    assert event["vm_instructions"] > 0
                    assert event["query_plan"]
                    assert event["elapsed_seconds"] >= 0
                    assert len(event["columns"]) == (10 if kind == "page" else 1)
    assert set(measured["provenance"]["imported_sources"]) == {
        "library.py",
        "_library_journal.py",
    }


def test_engine_counterfactual_keeps_full_results_but_rejects_actual_forced_scan(
    measured: dict[str, Any],
) -> None:
    cases = {
        c["variant"]: c
        for c in measured["cases"]
        if c["retained_tokens"] == 32768 and c["scenario"] == "sparse_eligible"
    }
    indexed = cases["fixture_index"]["cycles"][0]["pages"]
    scanned = cases["forced_scan"]["cycles"][0]["pages"]
    assert cases["fixture_index"]["acceptance"]["status"] == "satisfied"
    assert cases["forced_scan"]["acceptance"]["status"] == "violated"
    assert [len(p["page"]["rows"]) for p in indexed] == [8, 1, 0]
    assert [p["remaining"]["rows"] for p in indexed] == [[[1]], [[0]], [[0]]]
    for fast, slow in zip(indexed, scanned, strict=True):
        for kind in ("page", "remaining"):
            assert fast[kind]["rows"] == slow[kind]["rows"]
            assert "NOT INDEXED" in slow[kind]["query"]
            assert slow[kind]["vm_instructions"] > fast[kind]["vm_instructions"] * 100


@pytest.mark.parametrize(
    "missing",
    (
        "vm_instructions",
        "elapsed_seconds",
        "rows",
        "query",
        "query_plan",
        "columns",
        "parameters",
    ),
)
def test_missing_engine_evidence_is_incomplete_not_zero(
    probe: dict[str, Any], measured: dict[str, Any], missing: str
) -> None:
    case = copy.deepcopy(measured["cases"][0])
    case["cycles"][0]["pages"][0]["page"].pop(missing)
    assert (
        probe["_assess_case"](case, queries=measured["production_queries"], limit=8)[
            "status"
        ]
        == "incomplete"
    )


@pytest.mark.parametrize("count", (0, -1, True, 1.5, float("inf"), float("nan")))
def test_invalid_vm_count_fails_closed(
    probe: dict[str, Any], measured: dict[str, Any], count: Any
) -> None:
    case = copy.deepcopy(measured["cases"][0])
    case["cycles"][0]["pages"][0]["page"]["vm_instructions"] = count
    assert (
        probe["_assess_case"](case, queries=measured["production_queries"], limit=8)[
            "status"
        ]
        == "incomplete"
    )


def test_wrong_projected_value_is_not_accepted_just_because_count_matches(
    probe: dict[str, Any], measured: dict[str, Any]
) -> None:
    case = copy.deepcopy(
        next(c for c in measured["cases"] if c["scenario"] == "sparse_eligible")
    )
    case["cycles"][0]["pages"][0]["page"]["rows"][0][2] = "wrong/path.cbz"
    assert (
        probe["_assess_case"](case, queries=measured["production_queries"], limit=8)[
            "status"
        ]
        == "incomplete"
    )


@pytest.mark.parametrize(
    "mutation",
    (
        "missing_case",
        "missing_cycle",
        "missing_page",
        "missing_provenance",
        "wrong_provenance",
        "unknown_variant",
        "wrong_control",
        "wrong_query",
        "unknown_execution_status",
        "false_pass_status",
    ),
)
def test_aggregate_recomputes_complete_matrix_and_controls(
    probe: dict[str, Any], measured: dict[str, Any], mutation: str
) -> None:
    report = copy.deepcopy(measured)
    if mutation == "missing_case":
        report["cases"].pop()
    elif mutation == "missing_cycle":
        report["cases"][0]["cycles"].pop()
    elif mutation == "missing_page":
        report["cases"][0]["cycles"][0]["pages"].pop()
    elif mutation == "missing_provenance":
        report.pop("provenance")
    elif mutation == "wrong_provenance":
        report["provenance"]["imported_sources"]["library.py"]["sha256"] = "wrong"
    elif mutation == "unknown_variant":
        report["cases"][0]["variant"] = "unknown"
    elif mutation == "wrong_query":
        report["production_queries"]["remaining"] = "SELECT 0"
    elif mutation == "unknown_execution_status":
        report["status"] = "unknown"
    elif mutation == "wrong_control":
        case = next(c for c in report["cases"] if c["variant"] == "fixture_index")
        case["cycles"][0]["pages"][0]["page"]["vm_instructions"] = 10**9
    else:
        report["acceptance"] = {"status": "satisfied"}
        for case in report["cases"]:
            case["acceptance"] = {"status": "satisfied"}
    assessed = probe["_assess"](
        report,
        sizes=probe["DEFAULT_SIZES"],
        queries=measured["production_queries"],
        limit=8,
    )
    assert assessed["status"] == (
        "violated" if mutation == "false_pass_status" else "incomplete"
    )


def test_query_extraction_has_no_manually_copied_selection_text(
    probe: dict[str, Any],
) -> None:
    queries, limit = probe["_queries"]()
    assert limit == probe["library"]._MAX_CLEANUP_ITEMS
    code = probe["library"].ManagedFilesystemLibraryAdapter._maintain_cleanup.__code__
    assert all(sql in code.co_consts for sql in queries.values())


def test_foreign_imported_code_is_rejected(
    probe: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        probe["library"], "__file__", "/foreign/site-packages/h2hdb_ingest/library.py"
    )
    with pytest.raises(ValueError, match="not the measured checkout"):
        probe["_source"]()


@pytest.mark.parametrize(
    ("dimensions", "status", "acceptance", "exit_code"),
    (
        ("0,4096", "completed", "violated", 1),
        ("0", "completed", "incomplete", 2),
        ("invalid", "error", "incomplete", 2),
    ),
)
def test_cli_distinguishes_violated_from_missing_evidence(
    tmp_path: Path, dimensions: str, status: str, acceptance: str, exit_code: int
) -> None:
    output = tmp_path / "report.json"
    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--retained-tokens",
            dimensions,
            "--output",
            str(output),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == exit_code, completed.stderr
    report = json.loads(output.read_text())
    assert report["status"] == status
    assert report["acceptance"]["status"] == acceptance
    summary = json.loads(completed.stdout)
    assert summary["status"] == status
    assert summary["acceptance"] == acceptance
