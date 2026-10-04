"""Fixed backlog cost evidence and an independent whole-inventory read mutant."""

from __future__ import annotations

import json
import logging
import os
import runpy
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from h2hdb import (
    CoreConfig,
    VNextCurrentOnlyMaintenanceOutcome,
    VNextIngestCompletionReceipt,
    VNextIngestFacade,
    VNextIngestSession,
)

_SCRIPT = Path(__file__).parents[1] / "scripts" / "probe-source-backlog.py"


@pytest.mark.parametrize("schema", (1, 2))
def test_core_log_uses_exact_operation_totals_across_attribution_schemas(
    schema: int,
) -> None:
    module = runpy.run_path(str(_SCRIPT))
    handler = module["_CoreLog"]()
    attribution = (
        {"query_top": [{"calls": 2, "seconds": 0.1}]}
        if schema == 1
        else {
            "query_attribution": {
                "algorithm": "bounded-duration-upper-lower-v1",
                "top": [
                    {
                        "fingerprint": "execute:SELECT:fixture",
                        "observed_calls": 2,
                        "seconds_lower": 0.1,
                        "seconds_upper": 0.2,
                        "complete": False,
                    }
                ],
            }
        }
    )
    preparation = {
        "schema": schema,
        "event": "completed",
        "operation": "source_prepare",
        "sql_calls": 3,
        **attribution,
    }
    step = {
        "schema": schema,
        "event": "completed",
        "operation": "source_step",
        "labels": {"action": "issue", "step_phase": "SOURCE"},
        "sql_calls": 3,
        "sql_seconds": 0.25,
        "read_rows": 7,
        "elapsed_seconds": 0.5,
        **attribution,
    }
    for payload in (
        {"schema": schema, "event": "started", "operation": "source_prepare"},
        preparation,
        step,
        step,
    ):
        handler.emit(
            logging.LogRecord(
                "h2hdb",
                logging.DEBUG,
                __file__,
                0,
                "database_performance " + json.dumps(payload),
                (),
                None,
            )
        )
    report = handler.report()
    assert report["source_prepare"] == [preparation]
    assert report["SOURCE_sql_calls"] == 6
    assert report["SOURCE_sql_seconds"] == 0.5
    assert report["SOURCE_returned_rows"] == 14
    assert report["SOURCE_phase_totals"] == {
        "issue.SOURCE": {
            "calls": 2,
            "sql_calls": 6,
            "sql_seconds": 0.5,
            "read_rows": 14,
            "elapsed_seconds": 1.0,
        }
    }


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
            if extra_scan:
                _read_pages(pages)
        measured = meter.report()
        measured["production_telemetry"] = {
            "counters": {"locator_rows": 129, "qualified_galleries": 8}
        }
        # This unit isolates PAGE I/O; real qualification correspondence is
        # exercised by the complete-workflow cases below.
        measured["qualified_galleries"] = 8
        measured["decode_calls"] = 8
        measured["independent_locators"] = {"rows": 129, "calls": 2}
        result = module["_costs"](
            measured,
            inventory=129,
            admitted=8,
            pages=1,
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
        baseline["checks"]["PAGE_reads_track_admission"]["observed"] == expected_bytes
    )
    assert degraded["status"] == "violated"
    assert not degraded["checks"]["PAGE_reads_track_admission"]["met"]
    assert (
        degraded["checks"]["PAGE_reads_track_admission"]["upper_bound"]
        == expected_bytes
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
            admitted=8,
            pages=1,
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
            admitted=8,
            pages=1,
            selected_page_bytes=1,
            all_marker_bytes=1,
            selected_marker_bytes=1,
        )


def _assert_natural_claim_sequence(report: dict[str, Any]) -> None:
    assert report["format_version"] == 3
    all_rounds = report["warmups_excluded_from_measurements"] + report["rounds"]
    generations = [row["ingest_generation"] for row in all_rounds]
    assert generations == list(range(generations[0], generations[0] + len(generations)))
    for index, row in enumerate(all_rounds):
        assert row["catalog_schema_state"] == "READY"
        assert row["lifecycle_sequence"][:2] == [
            "real_claim_granted",
            "publication_observed",
        ]
        assert row["lifecycle_sequence"][-1] == "catalog_cleanup:DONE"
        proof = row["next_claim"]
        assert proof["generation"] == row["ingest_generation"] + 1
        assert "next_claim_granted" not in row
        assert "next_claim_generation" not in row
        if index + 1 < len(all_rounds):
            assert proof == {
                "proof": "next_real_round",
                "generation": all_rounds[index + 1]["ingest_generation"],
                "measured_in_round": all_rounds[index + 1]["round"],
                "state_changed_by_probe": False,
            }
        else:
            assert proof["proof"] == "terminal_postmeasurement_probe"
            assert proof["measured_in_round"] is None
            assert proof["state_changed_by_probe"]
    terminal = report["terminal_next_claim"]
    assert terminal["status"] == "passed"
    assert terminal["generation"] == generations[-1] + 1
    assert terminal["state_changed"]
    assert terminal["after_last_round_ready_audit"]
    assert terminal["no_subsequent_measured_round_or_ready_audit"]


def test_extra_empty_claim_between_real_rounds_is_rejected(
    tmp_path: Path, core_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = runpy.run_path(str(_SCRIPT))
    original_claim = VNextIngestFacade.try_claim_ingest
    original_complete = VNextIngestFacade.complete_ingest
    original_cleanup = VNextIngestFacade.drain_current_only_maintenance
    completed_real_round = False
    injected = False

    def complete(
        facade: VNextIngestFacade, session: VNextIngestSession
    ) -> VNextIngestCompletionReceipt:
        nonlocal completed_real_round
        result = original_complete(facade, session)
        completed_real_round = True
        return result

    def cleanup(
        facade: VNextIngestFacade, *args: Any, **kwargs: Any
    ) -> VNextCurrentOnlyMaintenanceOutcome:
        nonlocal injected
        result = original_cleanup(facade, *args, **kwargs)
        if (
            result is VNextCurrentOnlyMaintenanceOutcome.DONE
            and completed_real_round
            and not injected
        ):
            injected = True
            # Reproduce the removed tool behavior with actual public SQL work.
            # Bypass the observation wrapper so the durable generation gap,
            # rather than an observer's call counter, must detect the mutation.
            grant = original_claim(facade, True, 1_800_000_000)
            assert grant is not None
            original_complete(facade, grant)
        return result

    monkeypatch.setattr(VNextIngestFacade, "complete_ingest", complete)
    monkeypatch.setattr(VNextIngestFacade, "drain_current_only_maintenance", cleanup)
    with pytest.raises(RuntimeError, match="unexpected ingest generation"):
        module["_run"](
            3,
            tmp_path,
            batch=1,
            measured_rounds=2,
            warmup_rounds=0,
            pages=1,
            core=core_config,
        )
    assert injected


@pytest.mark.backend_external(
    reason="Probe child receives its fixture-owned database via stdin and returns the actual backend with its full lifecycle oracle"
)
@pytest.mark.deep
@pytest.mark.parametrize("inventory", (127, 128, 129, 255, 256, 257, 1024))
def test_real_fixed_backlog_three_publications_cleanup_and_next_claim(
    tmp_path: Path, inventory: int, core_config: CoreConfig, probe_database_input: str
) -> None:
    output = tmp_path / "report.json"
    subprocess.run(
        [
            sys.executable,
            str(_SCRIPT),
            "--database-config-stdin",
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
        input=probe_database_input,
        timeout=260,
    )
    report = json.loads(output.read_text())
    assert report["status"] == "completed"
    _assert_natural_claim_sequence(report)
    assert report["fixture"]["backend"] == core_config.database.sql_type
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
        assert (
            item["post_publication_cleanup"]["library"][-1]
            == item["post_publication_cleanup"]["catalog"][-1]
            == "DONE"
        )
        assert (
            item["cleanup"]["catalog"][-1] == item["cleanup"]["library"][-1] == "DONE"
        )
        assert item["source_synchronization"]["telemetry_comparison"]["matched"]
        assert (
            item["source_synchronization"]["independent_locators"]["rows"]
            == item["source_synchronization"]["production_telemetry"]["counters"][
                "locator_rows"
            ]
        )
        assert item["core_source_logs"]["SOURCE_sql_calls"] > 0
        assert item["cost_targets"]["status"] in {"satisfied", "violated"}
        # A measured violation remains a result, not a permanently red runtime
        # gate disguised as an already achieved optimization target.


@pytest.mark.parametrize("inventory", ("0", "15", "1025"))
def test_backlog_cli_bounds_are_checked_before_work(
    tmp_path: Path,
    inventory: str,
) -> None:
    output = tmp_path / "report.json"
    result = subprocess.run(
        [
            sys.executable,
            str(_SCRIPT),
            "--database-config-stdin",
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


def test_source_meter_includes_lazy_steps_without_rehashing_the_fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = runpy.run_path(str(_SCRIPT))
    helpers = module["_HELPERS"]
    helpers["_fixture"](tmp_path, 2, 1, 16, "png")
    files = sorted(tmp_path.glob("*/000.png"))

    def unexpected_manifest(_root: Path) -> None:
        pytest.fail("complete fixture oracle must stay outside the cycle timer")

    monkeypatch.setitem(
        helpers["_measure"].__globals__, "_manifest", unexpected_manifest
    )

    def synchronize() -> SimpleNamespace:
        def prepare_lazy_observations() -> Iterator[Path]:
            for path in files:
                _read_pages([path])
                yield path

        handle = prepare_lazy_observations()
        # Preparing this generator reads no PAGE bytes. The source workflow
        # must keep its meter active while subsequent steps consume it.
        assert list(handle) == files
        return SimpleNamespace(
            receipt=SimpleNamespace(staged_galleries=2),
            waiting_gallery_count=0,
            deferred_gallery_count=0,
            inventory_scan_pending=False,
        )

    _result, measured = module["_measure_synchronization"](tmp_path, synchronize)
    assert sum(row["read_bytes"] for row in measured["source_files"].values()) == sum(
        path.stat().st_size for path in files
    )
    assert measured["galleries"] == 2
    assert "source_manifest" not in measured


def test_PAGE_groups_distinguish_retained_new_and_pending(tmp_path: Path) -> None:
    module = runpy.run_path(str(_SCRIPT))
    folders = [tmp_path / str(index) for index in range(3)]
    measured = {
        "source_files": {
            "0/000.png": {"read_bytes": 2},
            "1/000.png": {"read_bytes": 3},
            "2/000.png": {"read_bytes": 5},
            "0/galleryinfo.txt": {"read_bytes": 99},
        },
        "io": {"source.test.page": {"read_bytes": 10}},
    }
    assert module["_page_read_groups"](measured, folders, {"0"}, {"0", "1"}) == {
        "retained": 2,
        "new": 3,
        "pending": 5,
    }
    # Public admission order can differ from sorted path order. A retained
    # final path must not be misclassified as pending by positional slicing.
    assert module["_page_read_groups"](measured, folders, {"2"}, {"0", "2"}) == {
        "retained": 5,
        "new": 2,
        "pending": 3,
    }
    measured["io"]["source.test.page"]["read_bytes"] += 1
    with pytest.raises(RuntimeError, match="phase totals"):
        module["_page_read_groups"](measured, folders, {"0"}, {"0", "1"})


@pytest.mark.parametrize(
    "dimensions",
    [
        (1, 1, 1, 0, 1),
        (5, 2, 2, 1, 3),
        (1024, 32, 6, 4, 128),
    ],
)
def test_dimensions_allow_partial_last_batch_and_independent_axes(
    dimensions: tuple[int, ...],
) -> None:
    module = runpy.run_path(str(_SCRIPT))
    module["_validate_dimensions"](*dimensions)


@pytest.mark.parametrize(
    "arguments",
    [
        ["--batch", "0"],
        ["--batch", "33"],
        ["--rounds", "0"],
        ["--rounds", "7"],
        ["--warmup-rounds", "5"],
        ["--pages", "0"],
        ["--pages", "130"],
        ["--inventory", "1024", "--pages", "129"],
        ["--inventory", "8", "--batch", "8", "--warmup-rounds", "1", "--rounds", "1"],
        ["--inventory", "17", "--batch", "8", "--warmup-rounds", "1", "--rounds", "3"],
    ],
)
def test_new_dimension_bounds_fail_before_fixture_creation(
    tmp_path: Path, arguments: list[str]
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


@pytest.mark.backend_external(
    reason="Probe child receives its fixture-owned database via stdin and returns the actual backend with its full lifecycle oracle"
)
@pytest.mark.deep
def test_real_warmup_partial_final_batch_and_standalone_ledger(
    tmp_path: Path, core_config: CoreConfig, probe_database_input: str
) -> None:
    output, ledger = tmp_path / "report.json", tmp_path / "ledger.json"
    subprocess.run(
        [
            sys.executable,
            str(_SCRIPT),
            "--database-config-stdin",
            "--inventory",
            "5",
            "--batch",
            "2",
            "--warmup-rounds",
            "1",
            "--rounds",
            "2",
            "--pages",
            "2",
            "--output",
            str(output),
            "--ledger-output",
            str(ledger),
        ],
        capture_output=True,
        text=True,
        check=True,
        input=probe_database_input,
        timeout=260,
    )
    report = json.loads(output.read_text())
    model_input = json.loads(ledger.read_text())
    assert report["status"] == "completed"
    _assert_natural_claim_sequence(report)
    assert report["fixture"]["backend"] == core_config.database.sql_type
    assert report["ledger"] == model_input
    assert len(report["warmups_excluded_from_measurements"]) == 1
    assert model_input["dimensions"] == {
        "inventory": 5,
        "batch": 2,
        "initial_retained": 2,
        "pages_per_gallery": 2,
    }
    assert [row["new_admitted"] for row in model_input["rounds"]] == [2, 1]
    assert [row["retained_before"] for row in model_input["rounds"]] == [2, 4]
    assert [row["decode_calls"] for row in model_input["rounds"]] == [4, 2]
    for index, row in enumerate(model_input["rounds"]):
        evidence = report["rounds"][index]
        assert row["history_depth"] is None
        assert row["inventory_rows"] == 5
        assert row["retained_page_read_bytes"] == 0
        assert row["page_read_bytes"] >= row["new_page_bytes"] > 0
        assert row["source_sql_calls"] > 0
        assert evidence["source_PAGE_read_groups"]["pending"] == 0
        assert evidence["source_synchronization"]["telemetry_comparison"]["matched"]
        assert evidence["lifecycle_sequence"][-1] == "catalog_cleanup:DONE"
    assert report["rounds"][-1]["pending"] == 0
