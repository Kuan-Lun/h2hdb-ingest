"""Offline real-JPEG publication and cleanup experiment using public runtime.

Fixtures, source observations, canonical CBZ, thumbnails and the catalog are real.
Times exclude fixture creation and the independent catalog/archive/raster oracle.
Logical bytes include rereads; this is not a physical-device or NAS benchmark.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import platform
import random
import resource
import runpy
import stat
import subprocess
import sys
import tempfile
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime
from hashlib import file_digest, sha256
from io import BytesIO
from itertools import pairwise
from pathlib import Path
from time import perf_counter_ns, process_time_ns, sleep
from types import SimpleNamespace
from unittest.mock import patch
from zipfile import ZIP_DEFLATED, ZIP_STORED, ZipFile

import h2hdb
from h2hdb import CoreConfig, DatabaseConfig, LoggerConfig
from PIL import Image

import h2hdb_ingest
from h2hdb_ingest import IngestConfig, IngestPathsConfig, ResidentConfig
from h2hdb_ingest import _adapter_performance as adapter_performance
from h2hdb_ingest import library as library_module
from h2hdb_ingest._maintenance_performance import LibraryMaintenancePerformance
from h2hdb_ingest.runtime import build_runtime, configure_logging
from h2hdb_ingest.scratch import DiskScratch

_ENVIRONMENT = runpy.run_path(str(Path(__file__).with_name("_probe_environment.py")))
_SEED = 20260919


def _fixture(root: Path, galleries: int, pages: int, edge: int) -> dict[str, int]:
    source = root / "source"
    source.mkdir()
    randomizer = random.Random(_SEED)
    page_bytes = marker_bytes = 0
    for gallery in range(galleries):
        gid = 1000000 + gallery
        folder = source / str(gid)
        folder.mkdir()
        for page in range(pages):
            destination = folder / f"{page:04d}.jpg"
            with Image.frombytes(
                "RGB", (edge, edge), randomizer.randbytes(edge * edge * 3)
            ) as image:
                image.save(destination, "JPEG", quality=92, optimize=False)
            page_bytes += destination.stat().st_size
        marker = folder / "galleryinfo.txt"
        marker.write_text(
            "\n".join(
                (
                    f"Title: Artifact I/O fixture {gid}",
                    "Upload Time: 2024-01-02 03:04",
                    "Uploaded By: probe",
                    "Downloaded: 2024-02-03 04:05",
                    f"Tags: artist:probe-{gid}, language:english",
                    "Uploader's Comments",
                    "Deterministic offline runtime experiment",
                    "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3",
                )
            ),
            encoding="utf-8",
        )
        marker_bytes += marker.stat().st_size
    library = root / "library"
    library.mkdir()
    for relative in (
        "current",
        "current/acquisitions",
        "current/artwork",
        ".h2hdb-coordination",
    ):
        (library / relative).mkdir()
    return {"page_bytes": page_bytes, "marker_bytes": marker_bytes}


def _digest(path: Path) -> str:
    with path.open("rb") as stream:
        return file_digest(stream, "sha256").hexdigest()


def _oracle(runtime, root: Path, galleries: int, pages: int, edge: int):
    revision = runtime.catalog.get_catalog_revision()
    found = runtime.catalog.discover_publications(revision=revision, limit=128)
    if found.total != galleries or len(found.publications) != galleries:
        raise RuntimeError("catalog fixture membership differs")
    if {item.gid for item in found.publications} != set(
        range(1000000, 1000000 + galleries)
    ):
        raise RuntimeError("catalog fixture GIDs differ")
    archive_bytes = thumbnail_bytes = raster_pages = 0
    worst_pixel_error = 0.0
    for publication in found.publications:
        if publication.page_count != pages or len(publication.artifacts) != 1:
            raise RuntimeError("catalog page/artifact count differs")
        artifact = publication.artifacts[0].storage_object
        path = root / "library" / "current" / Path(*artifact.key.segments)
        if (
            _digest(path) != artifact.sha256
            or path.stat().st_size != artifact.size_bytes
        ):
            raise RuntimeError("published archive differs from catalog authority")
        archive_bytes += artifact.size_bytes
        source = root / "source" / str(publication.gid)
        with ZipFile(path) as archive:
            expected = [
                "galleryinfo.txt",
                *(f"pages/{i:04d}.jpg" for i in range(pages)),
            ]
            if archive.namelist() != expected or archive.testzip() is not None:
                raise RuntimeError("archive member/CRC oracle failed")
            if (
                archive.read("galleryinfo.txt")
                != (source / "galleryinfo.txt").read_bytes()
            ):
                raise RuntimeError("published metadata differs from fixture")
            if archive.getinfo("galleryinfo.txt").compress_type != ZIP_DEFLATED:
                raise RuntimeError("metadata compression differs")
            for page in range(pages):
                name = f"pages/{page:04d}.jpg"
                if archive.getinfo(name).compress_type != ZIP_STORED:
                    raise RuntimeError("page packaging differs")
                encoded = archive.read(name)
                with (
                    Image.open(BytesIO(encoded)) as output,
                    Image.open(source / f"{page:04d}.jpg") as original,
                ):
                    output.load()
                    original.load()
                    if output.format != "JPEG" or output.size != (edge, edge):
                        raise RuntimeError("published page raster shape differs")
                    # A deliberately generous 32-level mean error allows a second
                    # lossy JPEG encode; unrelated fixed-seed noise is about 60.
                    # This detects swapped pages without using the renderer as oracle.
                    error = sum(
                        abs(a - b)
                        for sample in range(128)
                        for a, b in zip(
                            original.getpixel(
                                ((sample * 97) % edge, (sample * 193) % edge)
                            ),
                            output.getpixel(
                                ((sample * 97) % edge, (sample * 193) % edge)
                            ),
                            strict=True,
                        )
                    ) / (128 * 3)
                    worst_pixel_error = max(worst_pixel_error, error)
                    if error > 32:
                        raise RuntimeError(
                            "published raster disagrees with source page"
                        )
                    raster_pages += 1
                if page == 0:
                    cover = publication.cover
                    if cover is None or sha256(encoded).hexdigest() != cover.sha256:
                        raise RuntimeError("published cover digest differs")
                    with path.open("rb") as stream:
                        stream.seek(cover.extent.offset)
                        if stream.read(cover.extent.length) != encoded:
                            raise RuntimeError("published cover extent differs")
        thumbnail = publication.thumbnail
        if thumbnail is None:
            raise RuntimeError("published thumbnail missing")
        thumb = (
            root / "library" / "current" / Path(*thumbnail.storage_object.key.segments)
        )
        if _digest(thumb) != thumbnail.storage_object.sha256:
            raise RuntimeError("published thumbnail digest differs")
        with Image.open(thumb) as image:
            image.load()
            if image.format != "JPEG" or max(image.size) > 320:
                raise RuntimeError("published thumbnail raster differs")
        thumbnail_bytes += thumb.stat().st_size
    if (root / "library/.h2hdb-coordination/ACTIVATING").exists():
        raise RuntimeError("publication left ACTIVATING fence")
    if runtime.database_admin.check().state != "READY":
        raise RuntimeError("complete READY audit failed")
    return {
        "revision": revision.revision,
        "galleries": galleries,
        "raster_pages": raster_pages,
        "archive_bytes": archive_bytes,
        "thumbnail_bytes": thumbnail_bytes,
        "max_mean_pixel_error": worst_pixel_error,
        "full_ready_audit": True,
    }


def _metrics(text: str):
    records = []
    for line in text.splitlines():
        if "[INFO] ingest_metric " not in line:
            continue
        fields = line.split("[INFO] ingest_metric ", 1)[1].split()
        record = {}
        for field in fields:
            key, value = field.split("=", 1)
            record[key] = int(value) if value.isdecimal() else value
        records.append(record)
    return records


def _validate_metrics(metrics, oracle, fixture):
    adapter = next(
        value
        for value in metrics
        if value["scope"] == "adapter_io" and value["operation"] == "publication"
    )
    source = next(value for value in metrics if value["scope"] == "source")
    renderer = next(value for value in metrics if value["scope"] == "artifact_totals")
    if adapter["operation.protect.calls"] != 2 * oracle["galleries"]:
        raise RuntimeError("protection cost does not account for both resources")
    if renderer["operation.render_archive.completed_calls"] != oracle["galleries"]:
        raise RuntimeError("renderer cost does not account for all galleries")
    output_bytes = oracle["archive_bytes"] + oracle["thumbnail_bytes"]
    if adapter["operation.stage_write.logical_bytes"] < output_bytes:
        raise RuntimeError("stage byte accounting omitted protected output")
    if (
        adapter["operation.source_open.calls"]
        < oracle["raster_pages"] + oracle["galleries"]
    ):
        raise RuntimeError("source reopen accounting omitted source members")
    source_bytes = fixture["page_bytes"] + fixture["marker_bytes"]
    return {
        "source_observation_reads_per_source_byte": source["counter.logical_bytes_read"]
        / source_bytes,
        "source_reopen_calls": adapter["operation.source_open.calls"],
        "stage_reads_per_output_byte": adapter["operation.stage_read.logical_bytes"]
        / output_bytes,
        "stage_writes_per_output_byte": adapter["operation.stage_write.logical_bytes"]
        / output_bytes,
        "stage_read_bytes": adapter["operation.stage_read.logical_bytes"],
    }


def _provenance():
    result = {}
    for package in (h2hdb, h2hdb_ingest):
        root = Path(package.__file__).parent
        digest = sha256()
        for path in sorted(root.rglob("*.py")):
            digest.update(path.relative_to(root).as_posix().encode())
            digest.update(path.read_bytes())
        result[package.__name__] = {
            "location": str(root),
            "source_sha256": digest.hexdigest(),
        }
    return result


class _CleanupMeasurement:
    """Fixed operation totals from actual maintenance, with no retained paths."""

    def __init__(self, journal_delay_ms: int = 0) -> None:
        self.journal_delay_ms = journal_delay_ms
        self.calls = self.received = self.checkpoints = self.clock_failures = 0
        self.elapsed_ns = self.injected_calls = 0
        self.statuses = {"completed": 0, "failed": 0, "interrupted": 0}
        self.operations = {}
        self.sink_errors = []

    def _sink(self, metric) -> None:
        # Runtime telemetry deliberately suppresses sink exceptions. Retain a
        # bounded error flag and reject incomplete evidence after the runtime.
        try:
            if metric.scope != "adapter_io":
                raise ValueError("unexpected cleanup metric scope")
            if metric.operation == "checkpoint":
                self.checkpoints += 1
                return  # Cumulative snapshots must not be added to final totals.
            if metric.operation != "library_cleanup":
                raise ValueError("missing final cleanup adapter measurement")
            self.received += 1
            self.elapsed_ns += metric.elapsed_ns
            self.statuses[metric.status] += 1
            counters = {item.name: item.value for item in metric.counters}
            self.clock_failures += counters["clock_failures"]
            for operation in metric.operations:
                if operation.operation not in self.operations:
                    if len(self.operations) >= 32:
                        raise ValueError("cleanup operation vocabulary grew unbounded")
                    self.operations[operation.operation] = {}
                total = self.operations[operation.operation]
                for item in operation.phases_ns:
                    total[item.name + "_ns"] = (
                        total.get(item.name + "_ns", 0) + item.value
                    )
                for item in operation.counters:
                    total[item.name] = total.get(item.name, 0) + item.value
        except Exception as error:
            if not self.sink_errors:
                self.sink_errors.append(str(error))

    @contextmanager
    def observe(self):
        original = library_module.ManagedFilesystemLibraryAdapter.maintain_cleanup
        original_phase = library_module.adapter_phase
        original_record = LibraryMaintenancePerformance._record

        def record(observer, metric):
            original_record(observer, metric)
            self._sink(metric)

        @contextmanager
        def delayed_phase(operation):
            with original_phase(operation):
                if operation == "journal_session" and self.journal_delay_ms:
                    self.injected_calls += 1
                    sleep(self.journal_delay_ms / 1000)
                yield

        def observed(adapter):
            self.calls += 1
            # Use the production maintenance observer; an inner private scope
            # would steal attribution from the INFO report we are validating.
            with patch.object(library_module, "adapter_phase", delayed_phase):
                return original(adapter)

        with (
            patch.object(
                library_module.ManagedFilesystemLibraryAdapter,
                "maintain_cleanup",
                observed,
            ),
            patch.object(LibraryMaintenancePerformance, "_record", record),
        ):
            yield

    def report(self):
        complete = (
            self.calls > 0
            and self.received == self.calls
            and not self.clock_failures
            and not self.sink_errors
            and self.statuses["completed"] == self.calls
            and bool(self.operations.get("journal_session", {}).get("calls"))
        )
        attributed_ns = sum(
            operation.get("exclusive_ns", 0) for operation in self.operations.values()
        )
        return {
            "status": "completed" if complete else "incomplete",
            "scope": "actual_library_maintenance_calls",
            "calls": self.calls,
            "terminal_measurements": self.received,
            "ignored_cumulative_checkpoints": self.checkpoints,
            "elapsed_ns": self.elapsed_ns,
            "attributed_exclusive_ns": attributed_ns,
            "unattributed_ns": max(0, self.elapsed_ns - attributed_ns),
            "clock_failures": self.clock_failures,
            "statuses": self.statuses,
            "operations": self.operations,
            "sink_errors": self.sink_errors,
            "injected_journal_delay_ms": self.journal_delay_ms,
            "injected_journal_calls": self.injected_calls,
            "limits": [
                "Actual production maintenance observer, including scratch cleanup; all safety checks still execute",
                "Includes startup, pre-claim and post-session maintenance, overlapping enclosing process_available timings",
                "Exclusive same-thread phases add; inclusive phases overlap; bytes are logical instrumented transfers",
                "No source, render or publication metrics are included; uninstrumented cleanup work remains residual",
                "Controlled journal delay tests attribution and is not a NAS latency model",
            ],
        }


def _validate_cleanup_info(metrics, cleanup):
    records = [value for value in metrics if value["scope"] == "library_cleanup_io"]
    if not records:
        raise ValueError("production INFO omitted cleanup measurements")
    identities = {
        (r["counter.process_id"], r["counter.observer_started_ns"]) for r in records
    }
    if len(identities) != 1:
        raise ValueError("cleanup INFO mixed independent observer totals")
    expected_sequence = list(range(1, len(records) + 1))
    if [r["counter.snapshot_sequence"] for r in records] != expected_sequence:
        raise ValueError("cleanup INFO snapshot sequence is incomplete")
    if any(r["counter.cumulative"] != 1 for r in records):
        raise ValueError("cleanup INFO lacks cumulative semantics")
    final = records[-1]
    expected = {
        "counter.calls": cleanup["terminal_measurements"],
        "counter.failed_calls": cleanup["statuses"]["failed"],
        "counter.interrupted_calls": cleanup["statuses"]["interrupted"],
        "counter.clock_failures": cleanup["clock_failures"],
        "counter.collection_errors": 0,
        "elapsed_ns": cleanup["elapsed_ns"],
        "phase.attributed_exclusive_ns": cleanup["attributed_exclusive_ns"],
        "phase.unattributed_ns": cleanup["unattributed_ns"],
    }
    for operation, fields in cleanup["operations"].items():
        expected.update(
            {f"operation.{operation}.{name}": value for name, value in fields.items()}
        )
    if any(final[key] != value for key, value in expected.items()):
        raise ValueError("production INFO differs from actual cleanup attempts")
    # Differencing the snapshots must cover every completed attempt exactly once.
    calls = [0, *(r["counter.calls"] for r in records)]
    if any(after <= before for before, after in pairwise(calls)):
        raise ValueError("cleanup INFO repeated or decreased cumulative attempt counts")
    return {
        "status": "matched",
        "snapshots": len(records),
        "completed_attempts": calls[-1],
    }


def _validate_source_evidence(report):
    before = report.get("provenance")
    after = report.get("provenance_after")
    complete = (
        isinstance(before, dict)
        and set(before) == {"h2hdb", "h2hdb_ingest"}
        and before == after
        and all(
            isinstance(value, dict)
            and isinstance(value.get("source_sha256"), str)
            and len(value["source_sha256"]) == 64
            for value in before.values()
        )
        and report.get("source_unchanged_during_experiment") is True
        and isinstance(report.get("probe_sha256"), str)
        and len(report["probe_sha256"]) == 64
        and report["probe_sha256"] == report.get("probe_sha256_after")
        and isinstance(report.get("environment_helper_sha256"), str)
        and len(report["environment_helper_sha256"]) == 64
        and report["environment_helper_sha256"]
        == report.get("environment_helper_sha256_after")
    )
    report["source_unchanged_during_experiment"] = complete
    if not complete:
        # Preserve oracle, measured timings and both digests for diagnosis.
        report["status"] = "error"
        report["acceptance"] = {"status": "incomplete"}
        report["error_type"] = "SourceProvenanceError"
        report["error"] = "runtime or probe source changed, or provenance is incomplete"
    return report


def _run(args, root: Path):
    provenance_before = _provenance()
    probe_sha256 = sha256(Path(__file__).read_bytes()).hexdigest()
    environment_helper = Path(__file__).with_name("_probe_environment.py")
    environment_sha256 = sha256(environment_helper.read_bytes()).hexdigest()
    fixture_started = perf_counter_ns()
    fixture = _fixture(root, args.galleries, args.pages, args.edge)
    fixture_ns = perf_counter_ns() - fixture_started
    log_file = root / "ingest.log"
    config = IngestConfig(
        core=CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite", database=str(root / "catalog.sqlite3")
            ),
            logger=LoggerConfig.model_validate({"file": log_file, "level": "info"}),
        ),
        paths=IngestPathsConfig(
            download_path=root / "source",
            library_path=root / "library",
            page_render_workers=args.workers,
        ),
        resident=ResidentConfig(
            publication_batch_galleries=args.galleries,
            lease_seconds=60,
            heartbeat_seconds=5,
        ),
    )
    configure_logging(config)
    load_before = os.getloadavg()
    cleanup_measurement = _CleanupMeasurement(args.cleanup_journal_delay_ms)
    with ExitStack() as resources:
        resources.enter_context(cleanup_measurement.observe())
        if args.fsync_delay_ms:
            # Only this adapter helper's os reference is replaced. SQLite native
            # sync, Core transactions and unrelated Python os users remain real.
            fsync = os.fsync

            def delayed_fsync(descriptor: int) -> None:
                is_directory = stat.S_ISDIR(os.fstat(descriptor).st_mode)
                if (
                    args.fsync_delay_kind == "all"
                    or (args.fsync_delay_kind == "directory") == is_directory
                ):
                    sleep(args.fsync_delay_ms / 1000)
                fsync(descriptor)

            resources.enter_context(
                patch.object(
                    adapter_performance, "os", SimpleNamespace(fsync=delayed_fsync)
                )
            )
        scratch = resources.enter_context(DiskScratch(root / "library"))
        with build_runtime(
            config,
            temporary_cleanup=scratch.cleanup_page,
            owned_resources=resources.pop_all(),
        ) as runtime:
            setup_started = perf_counter_ns()
            runtime.database_admin.initialize()
            runtime.resident.initialize()
            setup_ns = perf_counter_ns() - setup_started
            started = perf_counter_ns()
            cpu_started = process_time_ns()
            if not runtime.resident.process_available(periodic_scan=True):
                raise RuntimeError("fresh publication did not progress")
            publication_ns = perf_counter_ns() - started
            cpu_ns = process_time_ns() - cpu_started
            publication_metrics = _metrics(log_file.read_text())
            published_revision = runtime.catalog.get_catalog_revision().revision
            # The public resident drains component maintenance before the next
            # claim; do not confuse its first publication with cleanup DONE.
            cleanup_started = perf_counter_ns()
            for _ in range(4096):
                runtime.resident.process_available(periodic_scan=True)
                text = log_file.read_text()
                claims = [
                    line
                    for line in text.splitlines()
                    if "event=ingest_claimed " in line
                ]
                if len(claims) >= 2:
                    if (
                        "library_done_observed=true" not in claims[1]
                        or "catalog_done_observed=true" not in claims[1]
                    ):
                        raise RuntimeError(
                            "next claim lacks both component DONE observations"
                        )
                    break
            else:
                raise RuntimeError("cleanup did not permit a subsequent claim")
            cleanup_and_replay_ns = perf_counter_ns() - cleanup_started
            if runtime.catalog.get_catalog_revision().revision != published_revision:
                raise RuntimeError("unchanged-source replay changed catalog revision")
            oracle_started = perf_counter_ns()
            oracle = _oracle(runtime, root, args.galleries, args.pages, args.edge)
            oracle_ns = perf_counter_ns() - oracle_started
            amplification = _validate_metrics(publication_metrics, oracle, fixture)
    text = log_file.read_text()
    logging.shutdown()
    provenance_after = _provenance()
    report = {
        "status": "completed",
        "format_version": 2,
        "fixture": {
            "galleries": args.galleries,
            "pages_per_gallery": args.pages,
            "edge": args.edge,
            "workers": args.workers,
            "seed": _SEED,
            **fixture,
        },
        "timings_ns": {
            "fixture": fixture_ns,
            "setup_and_startup_audit": setup_ns,
            "first_process_available": publication_ns,
            "process_cpu": cpu_ns,
            "independent_oracle": oracle_ns,
            "cleanup_and_unchanged_replay": cleanup_and_replay_ns,
        },
        "oracle": oracle,
        "logical_amplification": amplification,
        "publication_metrics": publication_metrics,
        "library_cleanup_adapter": cleanup_measurement.report(),
        "all_metrics": _metrics(text),
        "cycle_events": [
            line.split("[INFO] ", 1)[1]
            for line in text.splitlines()
            if "[INFO] ingest_cycle_performance " in line
        ],
        "core_local_work": [
            line
            for line in text.splitlines()
            if "local_work" in line or "local work (" in line
        ],
        "core_stage_events": [
            line
            for line in text.splitlines()
            if "[INFO] Ingest " in line and " stage " in line
        ],
        "provenance": provenance_before,
        "provenance_after": provenance_after,
        "source_unchanged_during_experiment": provenance_before == provenance_after,
        "probe_sha256": probe_sha256,
        "environment_helper_sha256": environment_sha256,
        "environment_helper_sha256_after": sha256(
            environment_helper.read_bytes()
        ).hexdigest(),
        "probe_sha256_after": sha256(Path(__file__).read_bytes()).hexdigest(),
        "injected_adapter_fsync_delay": {
            "milliseconds": args.fsync_delay_ms,
            "kind": args.fsync_delay_kind,
            "interpretation": "Controlled diagnostic attribution test; not a NAS latency model",
        },
        "load_before": load_before,
        "load_after": os.getloadavg(),
        "peak_rss_native_units": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "isolated": args.isolated,
        "platform": platform.platform(),
        "python": sys.version,
        "finished_at": datetime.now(UTC).isoformat(),
        "limits": [
            "Local filesystem and warm cache; not a physical-device or NAS measurement",
            "first_process_available includes publication and its immediate bounded maintenance; cycle events separate completion",
            "cleanup_and_unchanged_replay includes the next unchanged-source ingest cycle",
            "Logical amplification counts instrumented boundaries, not total physical device reads",
            "Independent oracle runs outside timing and verifies all pages; no public adapter safety checks are bypassed",
        ],
    }

    try:
        report["cleanup_info_reconciliation"] = _validate_cleanup_info(
            report["all_metrics"], report["library_cleanup_adapter"]
        )
        if report["library_cleanup_adapter"]["status"] != "completed":
            raise ValueError("cleanup adapter measurements are incomplete")
    except (KeyError, TypeError, ValueError) as error:
        report.update(
            status="error",
            acceptance={"status": "incomplete"},
            error_type="CleanupMeasurementError",
            error=str(error),
        )
    return _validate_source_evidence(report)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--galleries", type=int, default=2)
    parser.add_argument("--pages", type=int, default=4)
    parser.add_argument("--edge", type=int, default=512)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--fsync-delay-ms", type=int, default=0)
    parser.add_argument("--cleanup-journal-delay-ms", type=int, default=0)
    parser.add_argument(
        "--fsync-delay-kind", choices=("file", "directory", "all"), default="all"
    )
    parser.add_argument("--workspace", type=Path, default=Path(tempfile.gettempdir()))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--isolated", action="store_true")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not (
        1 <= args.galleries <= 128
        and 1 <= args.pages <= 256
        and 16 <= args.edge <= 2048
        and 1 <= args.workers <= 4
        and 10 <= args.timeout <= 3600
        and 0 <= args.fsync_delay_ms <= 5
        and 0 <= args.cleanup_journal_delay_ms <= 5
    ):
        parser.error("fixture/worker/deadline exceeds bounded probe limits")
    if args.galleries * args.pages * args.edge**2 > 2_147_483_648:
        parser.error("aggregate fixture exceeds two billion pixels")
    if args.worker:
        binding = _ENVIRONMENT["worker_binding"](args.workspace)
        with tempfile.TemporaryDirectory(
            prefix="artifact-io-fixture-", dir=args.workspace
        ) as folder:
            report = _run(args, Path(folder))
        report["fixture_removed"] = not Path(folder).exists()
        report["execution_binding"] = binding
        print(json.dumps(report))
        return 0
    if args.output.exists() or args.output.is_symlink():
        parser.error("output already exists")
    with tempfile.TemporaryDirectory(
        prefix="artifact-io-owner-", dir=args.workspace
    ) as workspace:
        try:
            command, environment = _ENVIRONMENT["fresh_python_environment"](
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    *sys.argv[1:],
                    "--worker",
                    "--workspace",
                    workspace,
                ],
                Path(workspace),
            )
            completed = subprocess.run(
                command,
                env=environment,
                capture_output=True,
                text=True,
                check=True,
                timeout=args.timeout,
            )
            report = json.loads(completed.stdout)
            if not isinstance(report, dict):
                raise ValueError("worker omitted report object")
            _validate_source_evidence(report)
            if report["status"] == "completed" and report.get("execution_binding") != {
                "bytecode": "fresh supervisor-owned cache; compile source without existing .pyc",
                "pycache_prefix": str(Path(workspace) / "pycache"),
                "temporary_root": str(Path(workspace) / "temporary"),
            }:
                report.update(
                    status="error",
                    acceptance={"status": "incomplete"},
                    error_type="WorkerBindingError",
                    error="worker runtime binding differs from supervisor ownership",
                )
            if report.get("status") not in {"completed", "error"}:
                raise ValueError("worker omitted completed or incomplete evidence")
        except (subprocess.SubprocessError, ValueError, OSError) as error:
            report = {
                "status": "failed",
                "error_type": type(error).__name__,
                "error": str(error),
                "stderr": str(getattr(error, "stderr", ""))[-16000:],
            }
    report["command"] = sys.argv
    report["supervisor_scratch_removed"] = not Path(workspace).exists()
    with args.output.open("x") as stream:
        json.dump(report, stream, indent=2, sort_keys=True)
        stream.write("\n")
    print(json.dumps({"status": report["status"], "output": str(args.output)}))
    return 0 if report["status"] == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
