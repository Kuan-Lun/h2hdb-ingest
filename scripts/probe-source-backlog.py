"""Measure independent inventory, admission, retained-set and PAGE dimensions.

This synthetic fixture accepts only private SQLite or pytest-owned local MariaDB.
Cost targets are declared before execution; a violated target remains evidence,
not an execution failure or a claim that production is already optimized.
"""

from __future__ import annotations

import argparse
import json
import logging
import runpy
import subprocess
import sys
import tempfile
from collections import defaultdict
from contextlib import ExitStack
from functools import partial
from hashlib import sha256
from pathlib import Path
from time import perf_counter
from typing import Any
from unittest.mock import patch

from h2hdb import CoreConfig, LoggerConfig, VNextIngestFacade, VNextIngestSession

from h2hdb_ingest import IngestConfig, IngestPathsConfig, ResidentConfig, service
from h2hdb_ingest.filesystem import FilesystemSource
from h2hdb_ingest.library import ManagedFilesystemLibraryAdapter
from h2hdb_ingest.metrics import IngestMetric, TextIngestMetricSink
from h2hdb_ingest.runtime import build_runtime

_HELPERS = runpy.run_path(str(Path(__file__).with_name("probe-source-io.py")))
_DATABASE = runpy.run_path(str(Path(__file__).with_name("_probe_database.py")))
_MAX_DRIVES = 128
_MODEL = {
    "dimensions": "fixed inventory N; new admission B; retained R from real warm-up publications; P 16px PNG pages per gallery; measured and warm-up rounds are separate",
    "units": "actual os.read calls including EOF; logical source bytes including rereads; adapter locator rows; qualified galleries; completed connector-method calls (not server statements)",
    "targets": {
        "one_locator_pass": "locator_rows <= N per preparation",
        "qualification_tracks_admission": "qualified_galleries = actual newly admitted galleries",
        "decode_tracks_new_pages": "decode_calls = actual newly admitted galleries * P",
        "PAGE_reads_track_admission": "PAGE read bytes <= encoded bytes of newly admitted PAGE files; one-pass engineering target, not an optimum proof",
        "retained_and_pending_PAGE_reads": "unchanged retained and not-yet-admitted PAGE bytes read = 0",
        "marker_reads_bounded_by_inventory_and_admission": "marker bytes <= 2 * all inventory marker bytes + 8 * newly admitted marker bytes",
    },
    "sql_interpretation": "record source_prepare and SOURCE action SQL counts separately; no unsupported SQL-count bound or NAS latency target is asserted",
    "counterexample": "one additional read of every PAGE in the fixed inventory must violate the PAGE bound",
    "limits": "N<=1024 is local evidence, not NAS N=130000; inclusive phase times overlap; no wall-time gate; source counts include lazy source steps; bytes are logical reads, not scratch retention or device traffic",
}


def _validate_dimensions(
    inventory: int, batch: int, rounds: int, warmups: int, pages: int
) -> None:
    for name, value, low, high in (
        ("inventory", inventory, 1, 1024),
        ("batch", batch, 1, 32),
        ("rounds", rounds, 1, 6),
        ("warmup-rounds", warmups, 0, 4),
        ("pages", pages, 1, 129),
    ):
        if type(value) is not int or not low <= value <= high:
            raise ValueError(f"{name} must be {low}..{high}")
    if (
        warmups * batch >= inventory
        or warmups + rounds > (inventory + batch - 1) // batch
    ):
        raise ValueError("requested rounds exceed remaining catch-up batches")
    _HELPERS["_require_fixture_budget"](inventory, pages, 16)


def _measure_synchronization(
    source: Path, operation: Any
) -> tuple[Any, dict[str, Any]]:
    """Observe the full lazy source workflow, not only creation of its handle."""
    original = FilesystemSource.list_gallery_locators
    locators = {"rows": 0, "calls": 0}

    def list_locators(filesystem: FilesystemSource, *args: Any, **kwargs: Any) -> Any:
        page = original(filesystem, *args, **kwargs)
        locators["rows"] += len(page.items)
        locators["calls"] += 1
        return page

    with patch.object(FilesystemSource, "list_gallery_locators", list_locators):
        # The caller already checks the complete fixture outside its outer cycle
        # timer. Rehashing it here would charge the oracle's O(N*P) work to ingest.
        result, measured = _HELPERS["_measure"](
            source, operation, include_source_manifest=False
        )
    measured["independent_locators"] = locators
    return result, measured


def _page_read_groups(
    measured: dict[str, Any],
    folders: list[Path],
    retained: set[str],
    selected: set[str],
) -> dict[str, int]:
    known = {path.name for path in folders}
    if not retained <= selected <= known:
        raise RuntimeError("published source membership contradicts the fixture")
    classes = {
        path.name: "retained"
        if path.name in retained
        else "new"
        if path.name in selected
        else "pending"
        for path in folders
    }
    totals = dict.fromkeys(("retained", "new", "pending"), 0)
    for filename, counters in measured["source_files"].items():
        relative = Path(filename)
        if relative.name == "galleryinfo.txt":
            continue
        if len(relative.parts) != 2 or relative.parts[0] not in classes:
            raise RuntimeError("meter returned an unknown source PAGE")
        count = counters["read_bytes"]
        if type(count) is not int or count < 0:
            raise RuntimeError("meter returned invalid source PAGE bytes")
        totals[classes[relative.parts[0]]] += count
    phase_bytes = sum(
        value["read_bytes"]
        for name, value in measured["io"].items()
        if name.startswith("source.") and name.endswith(".page")
    )
    if sum(totals.values()) != phase_bytes:
        raise RuntimeError("per-file PAGE bytes differ from source phase totals")
    return totals


def _published_names(catalog: Any, revision: Any, inventory: int) -> set[str]:
    """Read the actual public selection; locator order is not path-name order."""
    names: set[str] = set()
    cursor = None
    for _ in range((inventory + 127) // 128 + 1):
        page = catalog.discover_publications(revision=revision, limit=128, after=cursor)
        for item in page.publications:
            name = str(item.gid)
            if name in names:
                raise RuntimeError("public catalog repeated a synthetic gallery")
            names.add(name)
        cursor = page.next_cursor
        if cursor is None:
            return names
    raise RuntimeError("public catalog exceeded the inventory oracle bound")


class _CoreLog(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.reset()

    def reset(self) -> None:
        self.steps: dict[str, dict[str, float | int]] = defaultdict(
            lambda: {
                "calls": 0,
                "sql_calls": 0,
                "sql_seconds": 0.0,
                "read_rows": 0,
                "elapsed_seconds": 0.0,
            }
        )
        self.preparations: list[dict[str, Any]] = []
        self.stage_summaries: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        message = record.getMessage()
        if message.startswith("database_performance "):
            item = json.loads(message.removeprefix("database_performance "))
            if item["event"] != "completed":
                return
            match item["operation"]:
                case "source_prepare":
                    if len(self.preparations) < 2:
                        self.preparations.append(item)
                case "source_step":
                    labels = item["labels"]
                    key = labels["action"] + "." + labels["step_phase"]
                    if key not in self.steps and len(self.steps) >= 128:
                        raise RuntimeError("source action diagnostic budget exceeded")
                    target = self.steps[key]
                    target["calls"] += 1
                    for name in (
                        "sql_calls",
                        "sql_seconds",
                        "read_rows",
                        "elapsed_seconds",
                    ):
                        target[name] += item[name]
        elif (
            message.startswith("ingest_db_performance event=stage_terminal ")
            and "pipeline=source operation=SOURCE " in message
            and len(self.stage_summaries) < 2
        ):
            self.stage_summaries.append(message)

    def report(self) -> dict[str, Any]:
        return {
            "source_prepare": self.preparations,
            "SOURCE_phase_totals": dict(sorted(self.steps.items())),
            "SOURCE_sql_calls": sum(item["sql_calls"] for item in self.steps.values()),
            "SOURCE_sql_seconds": sum(
                item["sql_seconds"] for item in self.steps.values()
            ),
            "SOURCE_returned_rows": sum(
                item["read_rows"] for item in self.steps.values()
            ),
            "SOURCE_stage_summaries": self.stage_summaries,
            "views_are_not_additive": "stage summary and source_step aggregate describe the same SOURCE calls",
        }


def _costs(
    measured: dict[str, Any],
    *,
    inventory: int,
    admitted: int,
    pages: int,
    selected_page_bytes: int,
    all_marker_bytes: int,
    selected_marker_bytes: int,
) -> dict[str, Any]:
    counters = measured["production_telemetry"]["counters"]
    for name in ("locator_rows", "qualified_galleries"):
        if name not in counters or type(counters[name]) is not int:
            raise RuntimeError(f"required production counter is missing: {name}")
    if counters["locator_rows"] != measured["independent_locators"]["rows"]:
        raise RuntimeError(
            "production locator rows differ from independent page results"
        )
    if counters["qualified_galleries"] != measured["qualified_galleries"]:
        raise RuntimeError(
            "production qualification count differs from independent calls"
        )
    page_bytes = sum(
        v["read_bytes"]
        for k, v in measured["io"].items()
        if k.startswith("source.") and k.endswith(".page")
    )
    marker_bytes = sum(
        v["read_bytes"]
        for k, v in measured["io"].items()
        if k.startswith("source.") and k.endswith(".marker")
    )
    observed = {
        "one_locator_pass": (measured["independent_locators"]["rows"], inventory),
        "qualification_tracks_admission": (measured["qualified_galleries"], admitted),
        "decode_tracks_new_pages": (measured["decode_calls"], admitted * pages),
        "PAGE_reads_track_admission": (page_bytes, selected_page_bytes),
        "marker_reads_bounded_by_inventory_and_admission": (
            marker_bytes,
            2 * all_marker_bytes + 8 * selected_marker_bytes,
        ),
    }
    checks = {
        key: {
            "observed": value,
            "upper_bound": bound,
            "met": value == bound
            if key in {"qualification_tracks_admission", "decode_tracks_new_pages"}
            else value <= bound,
        }
        for key, (value, bound) in observed.items()
    }
    return {
        "status": "satisfied"
        if all(check["met"] for check in checks.values())
        else "violated",
        "checks": checks,
    }


def _require_next_generation(generation: int, previous: int | None) -> None:
    if generation < 1 or (previous is not None and generation != previous + 1):
        raise RuntimeError("unexpected ingest generation: an extra claim changed state")


def _run(
    inventory: int,
    workspace: Path,
    *,
    batch: int = 8,
    measured_rounds: int = 3,
    warmup_rounds: int = 0,
    pages: int = 1,
    core: CoreConfig | None = None,
) -> dict[str, Any]:
    _validate_dimensions(inventory, batch, measured_rounds, warmup_rounds, pages)
    with tempfile.TemporaryDirectory(
        prefix="fixed-backlog-", dir=workspace
    ) as temporary:
        root = Path(temporary)
        source, library, scratch = root / "source", root / "library", root / "scratch"
        scratch.mkdir()
        _HELPERS["_fixture"](source, inventory, pages, 16, "png")
        immutable_manifest = _HELPERS["_manifest"](source)
        folders = sorted(path for path in source.iterdir() if path.is_dir())
        assert len(folders) == inventory
        marker_sizes = [(path / "galleryinfo.txt").stat().st_size for path in folders]
        page_sizes = [
            sum(page.stat().st_size for page in path.glob("*.png")) for path in folders
        ]
        if len(set(marker_sizes)) != 1 or len(set(page_sizes)) != 1:
            raise RuntimeError(
                "backlog fixture must use equal encoded PAGE and marker sizes"
            )
        for child in ("current/acquisitions", "current/artwork", ".h2hdb-coordination"):
            (library / child).mkdir(parents=True, exist_ok=True)
        config = IngestConfig(
            core=(core or _DATABASE["default_config"](root)).model_copy(
                update={"logger": LoggerConfig.model_validate({"level": "DEBUG"})}
            ),
            paths=IngestPathsConfig(
                download_path=source, library_path=library, page_render_workers=1
            ),
            resident=ResidentConfig(
                publication_batch_galleries=batch,
                lease_seconds=1800,
                heartbeat_seconds=30,
            ),
        )
        source_metrics: list[IngestMetric] = []
        preparations: list[dict[str, Any]] = []
        maintenance: dict[str, list[str]] = {"catalog": [], "library": []}
        last_maintenance: dict[str, Any] = {}
        claim_generations: list[int] = []
        original_claim = VNextIngestFacade.try_claim_ingest
        original_metric = TextIngestMetricSink.__call__
        original_synchronize = service.synchronize_source
        original_catalog_cleanup = VNextIngestFacade.drain_current_only_maintenance
        original_library_cleanup = ManagedFilesystemLibraryAdapter.maintain_cleanup

        def claim(
            facade: VNextIngestFacade, periodic: bool, lease_duration_microseconds: int
        ) -> VNextIngestSession | None:
            result = original_claim(facade, periodic, lease_duration_microseconds)
            if result is not None:
                claim_generations.append(result.ingest_generation)
            return result

        def metric(sink: TextIngestMetricSink, value: IngestMetric) -> None:
            if value.scope == "source" and len(source_metrics) < 2:
                source_metrics.append(value)
            original_metric(sink, value)

        def synchronize(*args: Any, **kwargs: Any) -> Any:
            if kwargs.get("max_new_galleries") != batch:
                raise RuntimeError("backlog probe lost its fixed admission limit")
            result, measured = _measure_synchronization(
                source,
                partial(original_synchronize, *args, **kwargs),
            )
            if len(preparations) >= 2:
                raise RuntimeError("multiple source preparations exceeded probe budget")
            preparations.append(measured)
            return result

        def cleanup(facade: VNextIngestFacade, *args: Any, **kwargs: Any) -> Any:
            result = original_catalog_cleanup(facade, *args, **kwargs)
            maintenance["catalog"].append(result.value)
            last_maintenance["catalog"] = partial(cleanup, facade, *args, **kwargs)
            return result

        def library_cleanup(
            adapter: ManagedFilesystemLibraryAdapter, *args: Any, **kwargs: Any
        ) -> Any:
            result = original_library_cleanup(adapter, *args, **kwargs)
            maintenance["library"].append(result.value)
            last_maintenance["library"] = partial(
                library_cleanup, adapter, *args, **kwargs
            )
            return result

        logs = _CoreLog()
        rounds: list[dict[str, Any]] = []
        warmups: list[dict[str, Any]] = []
        ledger_rows: list[dict[str, Any]] = []
        retained_names: set[str] = set()
        previous_row: dict[str, Any] | None = None
        previous_generation: int | None = None
        with ExitStack() as stack:
            stack.enter_context(patch.object(tempfile, "tempdir", str(scratch)))
            stack.enter_context(
                patch.object(VNextIngestFacade, "try_claim_ingest", claim)
            )
            stack.enter_context(patch.object(TextIngestMetricSink, "__call__", metric))
            stack.enter_context(
                patch.object(service, "synchronize_source", synchronize)
            )
            stack.enter_context(
                patch.object(
                    VNextIngestFacade, "drain_current_only_maintenance", cleanup
                )
            )
            stack.enter_context(
                patch.object(
                    ManagedFilesystemLibraryAdapter, "maintain_cleanup", library_cleanup
                )
            )
            for name in ("h2hdb.database_performance", "h2hdb.ingest_performance"):
                logger = logging.getLogger(name)
                stack.enter_context(patch.object(logger, "handlers", [logs]))
                stack.enter_context(patch.object(logger, "level", logging.DEBUG))
                stack.enter_context(patch.object(logger, "propagate", False))
            runtime = stack.enter_context(
                build_runtime(config, event_logger=lambda _message: None)
            )
            runtime.database_admin.initialize()
            runtime.resident.initialize()
            for number in range(1, warmup_rounds + measured_rounds + 1):
                retained = (number - 1) * batch
                expected = min(number * batch, inventory)
                admitted = expected - retained
                logs.reset()
                source_metrics.clear()
                preparations.clear()
                for values in maintenance.values():
                    values.clear()
                cycle_meter = _HELPERS["_Meter"](source)
                claims_before = len(claim_generations)
                started = perf_counter()
                drives = 0
                with cycle_meter.instrument():
                    for attempt in range(1, _MAX_DRIVES + 1):
                        drives = attempt
                        runtime.resident.process_available(periodic_scan=True)
                        revision = runtime.catalog.get_catalog_revision()
                        if revision.publication_count >= expected:
                            break
                    if revision.publication_count != expected:
                        raise RuntimeError(
                            "publication failed the new-gallery admission contract"
                        )
                    sequence = ["real_claim_granted", "publication_observed"]
                    post_publication_cleanup: dict[str, list[str]] = {
                        "library": [],
                        "catalog": [],
                    }
                    for kind in ("library", "catalog"):
                        for _attempt in range(_MAX_DRIVES):
                            result = last_maintenance[kind]()
                            post_publication_cleanup[kind].append(result.value)
                            sequence.append(kind + "_cleanup:" + result.value)
                            if result.value == "DONE":
                                break
                        else:
                            raise RuntimeError(f"{kind} cleanup did not reach DONE")
                elapsed = perf_counter() - started
                if len(claim_generations) != claims_before + 1:
                    raise RuntimeError(
                        "one real publication must claim exactly one generation"
                    )
                generation = claim_generations[-1]
                _require_next_generation(generation, previous_generation)
                if previous_row is not None:
                    previous_row["next_claim"] = {
                        "proof": "next_real_round",
                        "generation": generation,
                        "measured_in_round": number,
                        "state_changed_by_probe": False,
                    }
                previous_generation = generation
                if len(preparations) != 1 or len(source_metrics) != 1:
                    raise RuntimeError(
                        "one publication must produce exactly one source observation"
                    )
                measured = preparations[0]
                _HELPERS["_attach_production_metric"](
                    measured,
                    source_metrics[0],
                    scope="production source synchronization including prepare and source issue/commit",
                )
                outcome = runtime.resident.last_synchronization_result
                if (
                    outcome is None
                    or outcome.deferred_gallery_count != inventory - expected
                ):
                    raise RuntimeError(
                        "backlog did not shrink by exactly the admitted galleries"
                    )
                if measured["galleries"] != expected or measured["waiting"] != 0:
                    raise RuntimeError(
                        "admitted source set is inconsistent with retained publication"
                    )
                if (
                    _HELPERS["_manifest"](source) != immutable_manifest
                    or len(tuple(source.iterdir())) != inventory
                ):
                    raise RuntimeError(
                        "fixed input changed during the backlog experiment"
                    )
                audit_started = perf_counter()
                if runtime.database_admin.check().state != "READY":
                    raise RuntimeError(
                        "published round failed the complete READY audit"
                    )
                audit_seconds = perf_counter() - audit_started
                evidence = logs.report()
                if (
                    len(evidence["source_prepare"]) != 1
                    or not evidence["SOURCE_phase_totals"]
                    or len(evidence["SOURCE_stage_summaries"]) != 1
                ):
                    raise RuntimeError("Core source diagnostic evidence is incomplete")
                selected_names = _published_names(runtime.catalog, revision, inventory)
                if len(retained_names) != retained or len(selected_names) != expected:
                    raise RuntimeError(
                        "catalog membership differs from admission counts"
                    )
                page_groups = _page_read_groups(
                    measured, folders, retained_names, selected_names
                )
                new_names = selected_names - retained_names
                new_page_bytes = sum(
                    size
                    for folder, size in zip(folders, page_sizes, strict=True)
                    if folder.name in new_names
                )
                new_marker_bytes = sum(
                    size
                    for folder, size in zip(folders, marker_sizes, strict=True)
                    if folder.name in new_names
                )
                costs = _costs(
                    measured,
                    inventory=inventory,
                    admitted=admitted,
                    pages=pages,
                    selected_page_bytes=new_page_bytes,
                    all_marker_bytes=sum(marker_sizes),
                    selected_marker_bytes=new_marker_bytes,
                )
                for kind in ("retained", "pending"):
                    costs["checks"][kind + "_PAGE_reads"] = {
                        "observed": page_groups[kind],
                        "upper_bound": 0,
                        "met": page_groups[kind] == 0,
                    }
                if not all(check["met"] for check in costs["checks"].values()):
                    costs["status"] = "violated"
                if number > warmup_rounds:
                    ledger_rows.append(
                        {
                            "new_admitted": admitted,
                            "retained_before": retained,
                            "history_depth": None,
                            "new_page_bytes": new_page_bytes,
                            "inventory_rows": measured["independent_locators"]["rows"],
                            "page_read_bytes": sum(page_groups.values()),
                            "retained_page_read_bytes": page_groups["retained"],
                            "decode_calls": measured["decode_calls"],
                            "source_sql_calls": evidence["SOURCE_sql_calls"]
                            + sum(
                                entry["sql_calls"]
                                for entry in evidence["source_prepare"]
                            ),
                        }
                    )
                row = {
                    "round": number,
                    "inventory": inventory,
                    "new_admitted": admitted,
                    "retained_before": retained,
                    "published": expected,
                    "source_admitted_including_reuse": measured["galleries"],
                    "pending": outcome.deferred_gallery_count,
                    "waiting": outcome.waiting_gallery_count,
                    "catalog_schema_state": "READY",
                    "catalog_revision": revision.revision,
                    "resident_drives": drives,
                    "cycle_elapsed_seconds": elapsed,
                    "validation_audit_seconds_excluded": audit_seconds,
                    "cleanup": {key: list(value) for key, value in maintenance.items()},
                    "post_publication_cleanup": post_publication_cleanup,
                    "lifecycle_sequence": sequence,
                    "ingest_generation": generation,
                    "next_claim": None,
                    "input_manifest_unchanged": True,
                    "source_synchronization": measured,
                    "whole_cycle_io_alternate_view": cycle_meter.report(),
                    "core_source_logs": evidence,
                    "source_PAGE_read_groups": page_groups,
                    "selected_fixture_galleries": sorted(selected_names),
                    "cost_targets": costs,
                }
                (warmups if number <= warmup_rounds else rounds).append(row)
                previous_row = row
                retained_names = selected_names
            # Audit above describes the measured state. This one final claim
            # changes generation and is never followed by another measured round.
            terminal_started = perf_counter()
            grant = runtime.facade.try_claim_ingest(True, 1_800_000_000)
            if grant is None:
                raise RuntimeError(
                    "cleanup DONE did not permit the terminal next claim"
                )
            _require_next_generation(grant.ingest_generation, previous_generation)
            if len(claim_generations) != warmup_rounds + measured_rounds + 1:
                raise RuntimeError(
                    "claim count exceeds real rounds plus terminal proof"
                )
            if previous_row is None:
                raise RuntimeError("terminal claim has no measured predecessor")
            previous_row["next_claim"] = {
                "proof": "terminal_postmeasurement_probe",
                "generation": grant.ingest_generation,
                "measured_in_round": None,
                "state_changed_by_probe": True,
            }
            runtime.facade.complete_ingest(grant)
            terminal = {
                "status": "passed",
                "generation": grant.ingest_generation,
                "state_changed": True,
                "after_last_round_ready_audit": True,
                "no_subsequent_measured_round_or_ready_audit": True,
                "elapsed_seconds_excluded": perf_counter() - terminal_started,
            }
        return {
            "status": "completed",
            "format_version": 3,
            "terminal_next_claim": terminal,
            "probe_sha256": sha256(Path(__file__).read_bytes()).hexdigest(),
            "model": _MODEL,
            "fixture": {
                "fixed_inventory": inventory,
                "max_new_per_batch": batch,
                "rounds": measured_rounds,
                "warmup_rounds": warmup_rounds,
                "initial_retained": warmup_rounds * batch,
                "pages_per_gallery": pages,
                "edge": 16,
                "equal_encoded_sizes_enforced": True,
                "backend": config.core.database.sql_type,
                "manifest": immutable_manifest,
            },
            "provenance": _HELPERS["_provenance"](),
            "rounds": rounds,
            "warmups_excluded_from_measurements": warmups,
            "ledger": {
                "schema_version": 1,
                "input_manifest_sha256": immutable_manifest["sha256"],
                "dimensions": {
                    "inventory": inventory,
                    "batch": batch,
                    "initial_retained": warmup_rounds * batch,
                    "pages_per_gallery": pages,
                },
                "rounds": ledger_rows,
            },
            "performance_targets_met": all(
                item["cost_targets"]["status"] == "satisfied" for item in rounds
            ),
            "scope": "whole-cycle and source-synchronization I/O are alternate views, never add them; full READY audits and immutable-input hash checks run outside the measured cycle; next real round proves the previous next-claim without an intervening empty session; one terminal postmeasurement claim follows the final READY audit and explicitly changes state",
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--inventory", type=_HELPERS["_bounded_integer"](1, 1024), default=129
    )
    parser.add_argument("--batch", type=_HELPERS["_bounded_integer"](1, 32), default=8)
    parser.add_argument("--rounds", type=_HELPERS["_bounded_integer"](1, 6), default=3)
    parser.add_argument(
        "--warmup-rounds", type=_HELPERS["_bounded_integer"](0, 4), default=0
    )
    parser.add_argument("--pages", type=_HELPERS["_bounded_integer"](1, 129), default=1)
    parser.add_argument(
        "--timeout", type=_HELPERS["_bounded_integer"](30, 600), default=240
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--ledger-output", type=Path)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--workspace", type=Path, help=argparse.SUPPRESS)
    parser.add_argument(
        "--database-config-stdin",
        action="store_true",
        help="read one pytest-owned local database configuration from stdin",
    )
    arguments = parser.parse_args()

    try:
        _validate_dimensions(
            arguments.inventory,
            arguments.batch,
            arguments.rounds,
            arguments.warmup_rounds,
            arguments.pages,
        )
    except ValueError as error:
        parser.error(str(error))
    database_input = sys.stdin.read() if arguments.database_config_stdin else None
    cores = (
        _DATABASE["parse_configs"](database_input, count=1)
        if database_input is not None
        else ()
    )
    if arguments.worker:
        if arguments.workspace is None:
            parser.error("worker requires supervisor-owned workspace")
        print(
            json.dumps(
                _run(
                    arguments.inventory,
                    arguments.workspace,
                    batch=arguments.batch,
                    measured_rounds=arguments.rounds,
                    warmup_rounds=arguments.warmup_rounds,
                    pages=arguments.pages,
                    core=cores[0] if cores else None,
                )
            )
        )
        return 0
    if (
        arguments.output is None
        or arguments.output.exists()
        or arguments.output.is_symlink()
    ):
        parser.error("a new --output path is required")
    if arguments.ledger_output is not None and (
        arguments.ledger_output.exists()
        or arguments.ledger_output.is_symlink()
        or arguments.ledger_output.resolve() == arguments.output.resolve()
    ):
        parser.error("--ledger-output must be a separate new file")
    try:
        with tempfile.TemporaryDirectory(prefix="h2hdb-backlog-probe-") as workspace:
            result = subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--worker",
                    "--workspace",
                    workspace,
                    "--inventory",
                    str(arguments.inventory),
                    "--batch",
                    str(arguments.batch),
                    "--rounds",
                    str(arguments.rounds),
                    "--warmup-rounds",
                    str(arguments.warmup_rounds),
                    "--pages",
                    str(arguments.pages),
                    *(["--database-config-stdin"] if cores else []),
                ],
                capture_output=True,
                input=database_input,
                text=True,
                check=True,
                timeout=arguments.timeout,
            )
        report = json.loads(result.stdout)
        if (
            report.get("status") != "completed"
            or len(report.get("rounds", ())) != arguments.rounds
            or len(report.get("warmups_excluded_from_measurements", ()))
            != arguments.warmup_rounds
        ):
            raise ValueError("worker returned incomplete backlog evidence")
    except (subprocess.SubprocessError, ValueError) as error:
        report = {
            "status": "error",
            "format_version": 3,
            "model": _MODEL,
            "error_type": type(error).__name__,
            "error": str(error),
        }
        if isinstance(error, subprocess.CalledProcessError):
            report["worker_stderr"] = error.stderr[-16000:]
    _HELPERS["_atomic_report"](arguments.output, report)
    if arguments.ledger_output is not None:
        _HELPERS["_atomic_report"](
            arguments.ledger_output,
            report["ledger"]
            if report["status"] == "completed"
            else {
                "schema_version": 1,
                "status": "incomplete",
                "reason": "backlog probe did not complete; no measured ledger",
            },
        )
    print(json.dumps({"status": report["status"], "output": str(arguments.output)}))
    return 0 if report["status"] == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
