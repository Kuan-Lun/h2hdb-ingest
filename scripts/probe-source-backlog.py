"""Fixed-inventory source cost probe: three real eight-gallery publications.

This opt-in SQLite fixture never accepts a database URL or existing corpus.
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

from h2hdb import CoreConfig, DatabaseConfig, LoggerConfig, VNextIngestFacade

from h2hdb_ingest import IngestConfig, IngestPathsConfig, ResidentConfig
from h2hdb_ingest.filesystem import FilesystemSource
from h2hdb_ingest.library import ManagedFilesystemLibraryAdapter
from h2hdb_ingest.metrics import IngestMetric, TextIngestMetricSink
from h2hdb_ingest.runtime import build_runtime

_HELPERS = runpy.run_path(str(Path(__file__).with_name("probe-source-io.py")))
_BATCH = 8
_ROUNDS = 3
_MAX_DRIVES = 128
_MODEL = {
    "dimensions": "fixed inventory N; new admission B=8; prior publication R=0,8,16; one 16px PNG per gallery; three rounds",
    "units": "actual os.read calls including EOF; logical source bytes including rereads; adapter locator rows; qualified galleries; completed connector-method calls (not server statements)",
    "targets": {
        "one_locator_pass": "locator_rows <= N per preparation",
        "qualification_tracks_admission": "decode_calls and qualified_galleries <= B",
        "PAGE_reads_track_admission": "PAGE read bytes <= 2 * encoded bytes of the B newly admitted PAGE files",
        "marker_reads_bounded_by_inventory_and_admission": "marker bytes <= 2 * all inventory marker bytes + 8 * newly admitted marker bytes",
        "snapshot_tracks_admission": "snapshot_files <= 2 * B (one PAGE and one marker)",
    },
    "sql_interpretation": "record source_prepare and SOURCE action SQL counts separately; no unsupported SQL-count bound or NAS latency target is asserted",
    "counterexample": "one additional read of every PAGE in the fixed inventory must violate the PAGE bound",
    "limits": "N<=1024 is local evidence, not NAS N=130000; inclusive phase times overlap; no wall-time gate",
}


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
        "qualification_tracks_admission": (
            max(measured["decode_calls"], measured["qualified_galleries"]),
            _BATCH,
        ),
        "PAGE_reads_track_admission": (page_bytes, 2 * selected_page_bytes),
        "marker_reads_bounded_by_inventory_and_admission": (
            marker_bytes,
            2 * all_marker_bytes + 8 * selected_marker_bytes,
        ),
        "snapshot_tracks_admission": (measured["captured_files"], 2 * _BATCH),
    }
    checks = {
        key: {"observed": value, "upper_bound": bound, "met": value <= bound}
        for key, (value, bound) in observed.items()
    }
    return {
        "status": "satisfied"
        if all(check["met"] for check in checks.values())
        else "violated",
        "checks": checks,
    }


def _run(inventory: int, workspace: Path) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(
        prefix="fixed-backlog-", dir=workspace
    ) as temporary:
        root = Path(temporary)
        source, library, scratch = root / "source", root / "library", root / "scratch"
        scratch.mkdir()
        _HELPERS["_fixture"](source, inventory, 1, 16, "png")
        immutable_manifest = _HELPERS["_manifest"](source)
        folders = sorted(path for path in source.iterdir() if path.is_dir())
        assert len(folders) == inventory
        marker_sizes = [(path / "galleryinfo.txt").stat().st_size for path in folders]
        page_sizes = [(path / "000.png").stat().st_size for path in folders]
        if len(set(marker_sizes)) != 1 or len(set(page_sizes)) != 1:
            raise RuntimeError(
                "backlog fixture must use equal encoded PAGE and marker sizes"
            )
        for child in ("current/acquisitions", "current/artwork", ".h2hdb-coordination"):
            (library / child).mkdir(parents=True, exist_ok=True)
        config = IngestConfig(
            core=CoreConfig(
                database=DatabaseConfig(
                    sql_type="sqlite", database=str(root / "catalog.sqlite3")
                ),
                logger=LoggerConfig(level="DEBUG"),
            ),
            paths=IngestPathsConfig(
                download_path=source, library_path=library, page_render_workers=1
            ),
            resident=ResidentConfig(
                publication_batch_galleries=_BATCH,
                lease_seconds=1800,
                heartbeat_seconds=30,
            ),
        )
        source_metrics: list[IngestMetric] = []
        preparations: list[dict[str, Any]] = []
        maintenance: dict[str, list[str]] = {"catalog": [], "library": []}
        last_maintenance: dict[str, Any] = {}
        original_metric = TextIngestMetricSink.__call__
        original_prepare = VNextIngestFacade.prepare_source
        original_locators = FilesystemSource.list_gallery_locators
        original_catalog_cleanup = VNextIngestFacade.drain_current_only_maintenance
        original_library_cleanup = ManagedFilesystemLibraryAdapter.maintain_cleanup

        def metric(sink: TextIngestMetricSink, value: IngestMetric) -> None:
            if value.scope == "source" and len(source_metrics) < 2:
                source_metrics.append(value)
            original_metric(sink, value)

        def prepare(facade: VNextIngestFacade, *args: Any, **kwargs: Any) -> Any:
            if kwargs.get("max_new_galleries") != _BATCH:
                raise RuntimeError("backlog probe lost its fixed admission limit")
            locators = {"rows": 0, "calls": 0}

            def list_locators(
                filesystem: FilesystemSource, *args: Any, **kwargs: Any
            ) -> Any:
                page = original_locators(filesystem, *args, **kwargs)
                locators["rows"] += len(page.items)
                locators["calls"] += 1
                return page

            with patch.object(FilesystemSource, "list_gallery_locators", list_locators):
                prepared, measured = _HELPERS["_measure"](
                    source, partial(original_prepare, facade, *args, **kwargs)
                )
            measured["independent_locators"] = locators
            if len(preparations) >= 2:
                prepared.close()
                raise RuntimeError("multiple source preparations exceeded probe budget")
            preparations.append(measured)
            return prepared

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
        with ExitStack() as stack:
            stack.enter_context(patch.object(tempfile, "tempdir", str(scratch)))
            stack.enter_context(patch.object(TextIngestMetricSink, "__call__", metric))
            stack.enter_context(
                patch.object(VNextIngestFacade, "prepare_source", prepare)
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
            for number in range(1, _ROUNDS + 1):
                expected = number * _BATCH
                logs.reset()
                source_metrics.clear()
                preparations.clear()
                for values in maintenance.values():
                    values.clear()
                cycle_meter = _HELPERS["_Meter"](source)
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
                            "publication failed the eight-new-gallery contract"
                        )
                    sequence = ["publication_observed"]
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
                    grant = runtime.facade.try_claim_ingest(True, 1_800_000_000)
                    if grant is None:
                        raise RuntimeError("cleanup DONE did not permit the next claim")
                    sequence.append("next_claim_granted")
                    runtime.facade.complete_ingest(grant)
                    sequence.append("next_claim_released")
                elapsed = perf_counter() - started
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
                        "backlog did not shrink by exactly eight galleries"
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
                rounds.append(
                    {
                        "round": number,
                        "inventory": inventory,
                        "new_admitted": _BATCH,
                        "published": expected,
                        "source_admitted_including_reuse": measured["galleries"],
                        "pending": outcome.deferred_gallery_count,
                        "waiting": outcome.waiting_gallery_count,
                        "catalog_schema_state": "READY",
                        "catalog_revision": revision.revision,
                        "resident_drives": drives,
                        "cycle_elapsed_seconds": elapsed,
                        "validation_audit_seconds_excluded": audit_seconds,
                        "cleanup": {
                            key: list(value) for key, value in maintenance.items()
                        },
                        "post_publication_cleanup": post_publication_cleanup,
                        "lifecycle_sequence": sequence,
                        "next_claim_granted": True,
                        "next_claim_generation": grant.ingest_generation,
                        "input_manifest_unchanged": True,
                        "prepare": measured,
                        "whole_cycle_io_alternate_view": cycle_meter.report(),
                        "core_source_logs": evidence,
                        "cost_targets": _costs(
                            measured,
                            inventory=inventory,
                            selected_page_bytes=sum(
                                page_sizes[expected - _BATCH : expected]
                            ),
                            all_marker_bytes=sum(marker_sizes),
                            selected_marker_bytes=sum(
                                marker_sizes[expected - _BATCH : expected]
                            ),
                        ),
                    }
                )
        return {
            "status": "completed",
            "format_version": 1,
            "probe_sha256": sha256(Path(__file__).read_bytes()).hexdigest(),
            "model": _MODEL,
            "fixture": {
                "fixed_inventory": inventory,
                "max_new_per_batch": _BATCH,
                "rounds": _ROUNDS,
                "pages_per_gallery": 1,
                "edge": 16,
                "equal_encoded_sizes_enforced": True,
                "backend": "sqlite",
                "manifest": immutable_manifest,
            },
            "provenance": _HELPERS["_provenance"](),
            "rounds": rounds,
            "performance_targets_met": all(
                item["cost_targets"]["status"] == "satisfied" for item in rounds
            ),
            "scope": "whole-cycle and preparation I/O are alternate views, never add them; full READY audits and immutable-input hash checks run outside the measured cycle; next-claim proof releases its empty session before the next publication",
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--inventory", type=_HELPERS["_bounded_integer"](24, 1024), default=129
    )
    parser.add_argument(
        "--timeout", type=_HELPERS["_bounded_integer"](30, 600), default=240
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--workspace", type=Path, help=argparse.SUPPRESS)
    arguments = parser.parse_args()
    if arguments.worker:
        if arguments.workspace is None:
            parser.error("worker requires supervisor-owned workspace")
        print(json.dumps(_run(arguments.inventory, arguments.workspace)))
        return 0
    if (
        arguments.output is None
        or arguments.output.exists()
        or arguments.output.is_symlink()
    ):
        parser.error("a new --output path is required")
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
                ],
                capture_output=True,
                text=True,
                check=True,
                timeout=arguments.timeout,
            )
        report = json.loads(result.stdout)
        if (
            report.get("status") != "completed"
            or len(report.get("rounds", ())) != _ROUNDS
        ):
            raise ValueError("worker returned incomplete backlog evidence")
    except (subprocess.SubprocessError, ValueError) as error:
        report = {
            "status": "error",
            "format_version": 1,
            "model": _MODEL,
            "error_type": type(error).__name__,
            "error": str(error),
        }
        if isinstance(error, subprocess.CalledProcessError):
            report["worker_stderr"] = error.stderr[-16000:]
    _HELPERS["_atomic_report"](arguments.output, report)
    print(json.dumps({"status": report["status"], "output": str(arguments.output)}))
    return 0 if report["status"] == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
