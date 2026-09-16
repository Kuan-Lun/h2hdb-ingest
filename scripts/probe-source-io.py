"""Measure real source preparation I/O against a bounded published SQLite baseline.

Only synthetic files are accepted. A child process bounds the complete matrix;
the report is replaced atomically only after a complete success/error document.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import random
import subprocess
import sys
import tempfile
import tomllib
from collections import defaultdict
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
from functools import partial
from hashlib import sha256
from importlib.metadata import version
from pathlib import Path
from threading import Lock
from time import perf_counter, process_time
from typing import Any, BinaryIO
from unittest.mock import patch

import h2hdb
from h2hdb import CoreConfig, DatabaseConfig, VNextIngestFacade
from PIL import Image

import h2hdb_ingest
import h2hdb_ingest.image_qualification as qualification
from h2hdb_ingest import IngestConfig, IngestPathsConfig, ResidentConfig
from h2hdb_ingest.config import ArtifactRenderPolicyConfig
from h2hdb_ingest.core_source import VNextFilesystemSourceAdapter
from h2hdb_ingest.filesystem import FilesystemFileObservation, FilesystemSource
from h2hdb_ingest.image_qualification import ImageGalleryQualifier
from h2hdb_ingest.policy import build_ingest_policy
from h2hdb_ingest.runtime import build_runtime
from h2hdb_ingest.source_snapshot import SourceSnapshotStore

_PHASE: ContextVar[str] = ContextVar("source_io_probe_phase", default="observation")
_COUNTERS = ("read_calls", "read_bytes", "write_calls", "write_bytes")
_MAX_FIXTURE_PIXELS = 32 * 1024 * 1024
_MAX_FIXTURE_ENCODED_BYTES = 128 * 1024 * 1024
_SPOOL_THRESHOLD_BYTES = 4 * 1024 * 1024


class _CountedStream:
    """Count one stream boundary; never also instrument its underlying stream."""

    def __init__(self, stream: BinaryIO, meter: _Meter, category: str) -> None:
        self.stream = stream
        self.meter = meter
        self.category = category

    def __getattr__(self, name: str) -> Any:
        return getattr(self.stream, name)

    def __enter__(self) -> _CountedStream:
        self.stream.__enter__()
        return self

    def __exit__(self, *exc: object) -> None:
        self.stream.__exit__(*exc)

    def read(self, size: int = -1) -> bytes:
        result = self.stream.read(size)
        self.meter.add(self.category, "read_calls", 1)
        self.meter.add(self.category, "read_bytes", len(result))
        return result

    def write(self, value: bytes) -> int:
        written = self.stream.write(value)
        self.meter.add(self.category, "write_calls", 1)
        self.meter.add(self.category, "write_bytes", written)
        return written


class _Meter:
    def __init__(self, root: Path) -> None:
        self.source_files = {
            (value.st_dev, value.st_ino): (
                "marker" if path.name == "galleryinfo.txt" else "page",
                path.relative_to(root).as_posix(),
            )
            for path in root.rglob("*")
            if path.is_file()
            for value in (path.stat(),)
        }
        self.counts: dict[str, dict[str, int]] = defaultdict(
            lambda: dict.fromkeys(_COUNTERS, 0)
        )
        self.decode_calls = 0
        self.qualified_galleries = 0
        self.accepted_galleries = 0
        self.captured_files = 0
        self.qualification_disk_spools = 0
        self.operation_seconds: dict[str, float] = defaultdict(float)
        self.file_reads: dict[str, dict[str, int]] = defaultdict(
            lambda: {"read_calls": 0, "read_bytes": 0}
        )
        self.lock = Lock()

    def add(self, category: str, counter: str, amount: int) -> None:
        with self.lock:
            self.counts[category][counter] += amount

    @contextmanager
    def timed(self, operation: str):
        started = perf_counter()
        try:
            yield
        finally:
            with self.lock:
                self.operation_seconds[operation] += perf_counter() - started

    @contextmanager
    def instrument(self):
        original_read = os.read
        original_spool = qualification._spool
        original_temporary = qualification.SpooledTemporaryFile
        original_decode = qualification.load_source_page_image
        original_qualify = ImageGalleryQualifier.__call__
        original_capture = SourceSnapshotStore.capture
        original_open = Path.open

        def read(descriptor: int, size: int) -> bytes:
            value = os.fstat(descriptor)
            matched = self.source_files.get((value.st_dev, value.st_ino))
            result = original_read(descriptor, size)
            if matched is not None:
                kind, filename = matched
                category = f"source.{_PHASE.get()}.{kind}"
                self.add(category, "read_calls", 1)
                self.add(category, "read_bytes", len(result))
                with self.lock:
                    self.file_reads[filename]["read_calls"] += 1
                    self.file_reads[filename]["read_bytes"] += len(result)
            return result

        def spool(member: FilesystemFileObservation, position: int) -> BinaryIO:
            token = _PHASE.set("qualification")
            try:
                with self.timed("qualification_spool"):
                    result = original_spool(member, position)
                if result._rolled:
                    self.qualification_disk_spools += 1
                return result
            finally:
                _PHASE.reset(token)

        def temporary(*args: Any, **kwargs: Any) -> _CountedStream:
            return _CountedStream(
                original_temporary(*args, **kwargs), self, "qualification_buffer"
            )

        def decode(*args: Any, **kwargs: Any) -> Image.Image:
            with self.lock:
                self.decode_calls += 1
            with self.timed("qualification_decode"):
                return original_decode(*args, **kwargs)

        def qualify(*args: Any, **kwargs: Any) -> Any:
            with self.timed("qualification_total"):
                result = original_qualify(*args, **kwargs)
            self.qualified_galleries += 1
            self.accepted_galleries += int(result.accepted)
            return result

        def capture(store: SourceSnapshotStore, *args: Any, **kwargs: Any) -> Any:
            token = _PHASE.set("snapshot_capture")
            try:
                with self.timed("snapshot_capture"):
                    result = original_capture(store, *args, **kwargs)
                self.captured_files += 1
                return result
            finally:
                _PHASE.reset(token)

        def opened(path: Path, *args: Any, **kwargs: Any) -> Any:
            stream = original_open(path, *args, **kwargs)
            if _PHASE.get() == "snapshot_capture" and path.name.isdecimal():
                return _CountedStream(stream, self, "snapshot_buffer")
            return stream

        with ExitStack() as stack:
            for owner, name, replacement in (
                (os, "read", read),
                (qualification, "_spool", spool),
                (qualification, "SpooledTemporaryFile", temporary),
                (qualification, "load_source_page_image", decode),
                (ImageGalleryQualifier, "__call__", qualify),
                (SourceSnapshotStore, "capture", capture),
                (Path, "open", opened),
            ):
                stack.enter_context(patch.object(owner, name, replacement))
            yield

    def report(self) -> dict[str, object]:
        return {
            "io": dict(sorted(self.counts.items())),
            "source_files": dict(sorted(self.file_reads.items())),
            "decode_calls": self.decode_calls,
            "qualified_galleries": self.qualified_galleries,
            "accepted_galleries": self.accepted_galleries,
            "captured_files": self.captured_files,
            "qualification_disk_spools": self.qualification_disk_spools,
            "operation_seconds_nonadditive": dict(self.operation_seconds),
        }


def _metadata(gid: int, *, changed: bool = False) -> bytes:
    return (
        f"Title: Source I/O probe {gid}{' changed' if changed else ''}\n"
        "Upload Time: 2024-01-02 03:04\nUploaded By: synthetic\n"
        "Downloaded: 2024-02-03 04:05\n"
        f"Tags: artist:synthetic-{gid}, language:english\n"
        "Uploader's Comments\nSynthetic offline fixture\n"
        "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3\n"
    ).encode()


def _require_fixture_budget(galleries: int, pages: int, edge: int) -> None:
    if galleries * pages * edge * edge > _MAX_FIXTURE_PIXELS:
        raise ValueError(
            f"aggregate fixture pixels must not exceed {_MAX_FIXTURE_PIXELS}"
        )


def _write_page(path: Path, edge: int, random_bytes: random.Random, codec: str) -> int:
    with Image.frombytes(
        "RGB", (edge, edge), random_bytes.randbytes(edge * edge * 3)
    ) as image:
        if codec == "png":
            image.save(path, format="PNG", compress_level=0)
        else:
            image.save(path, format="JPEG", quality=95, subsampling=0, optimize=False)
    return path.stat().st_size


def _fixture(root: Path, galleries: int, pages: int, edge: int, codec: str) -> None:
    _require_fixture_budget(galleries, pages, edge)
    random_bytes = random.Random(47029)
    encoded_bytes = 0
    for gid in range(1_000_000, 1_000_000 + galleries):
        folder = root / str(gid)
        folder.mkdir(parents=True)
        for page in range(pages):
            encoded_bytes += _write_page(
                folder / f"{page:03}.{codec}", edge, random_bytes, codec
            )
            if encoded_bytes > _MAX_FIXTURE_ENCODED_BYTES:
                raise ValueError(
                    "aggregate fixture encoded bytes exceed "
                    f"{_MAX_FIXTURE_ENCODED_BYTES}"
                )
        # Producer completion evidence must be later than every PAGE write.
        (folder / "galleryinfo.txt").write_bytes(_metadata(gid))


def _manifest(root: Path) -> dict[str, object]:
    digest = sha256()
    pages = 0
    markers = 0
    page_sizes: list[int] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        content = path.read_bytes()
        name = path.relative_to(root).as_posix().encode()
        digest.update(len(name).to_bytes(8, "big") + name)
        digest.update(len(content).to_bytes(8, "big") + sha256(content).digest())
        if path.suffix in {".png", ".jpeg"}:
            pages += len(content)
            page_sizes.append(len(content))
        else:
            markers += len(content)
    return {
        "sha256": digest.hexdigest(),
        "page_bytes": pages,
        "marker_bytes": markers,
        "page_encoded_bytes": page_sizes,
        "pages_above_spool_threshold": sum(
            size > _SPOOL_THRESHOLD_BYTES for size in page_sizes
        ),
    }


def _measure(root: Path, operation: Any) -> tuple[Any, dict[str, object]]:
    meter = _Meter(root)
    started = perf_counter()
    cpu_started = process_time()
    with meter.instrument():
        prepared = operation()
    elapsed = perf_counter() - started
    cpu_elapsed = process_time() - cpu_started
    return prepared, {
        **meter.report(),
        "elapsed_seconds": elapsed,
        "process_cpu_seconds": cpu_elapsed,
        "galleries": prepared.gallery_count,
        "waiting": prepared.waiting_gallery_count,
        "deferred": prepared.deferred_gallery_count,
        "source_manifest": _manifest(root),
    }


def _run_matrix(
    galleries: int,
    pages: int,
    edge: int,
    workers: int,
    *,
    codec: str,
    workspace: Path,
) -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix="matrix-", dir=workspace) as temporary:
        root = Path(temporary)
        source = root / "source"
        library = root / "library"
        scratch = root / "scratch"
        scratch.mkdir()
        _fixture(source, galleries, pages, edge, codec)
        for child in ("current/acquisitions", "current/artwork", ".h2hdb-coordination"):
            (library / child).mkdir(parents=True, exist_ok=True)
        config = IngestConfig(
            core=CoreConfig(
                database=DatabaseConfig(
                    sql_type="sqlite", database=str(root / "catalog.sqlite3")
                )
            ),
            paths=IngestPathsConfig(
                download_path=source,
                library_path=library,
                page_render_workers=workers,
            ),
            resident=ResidentConfig(lease_seconds=1800, heartbeat_seconds=30),
        )
        cases: dict[str, dict[str, object]] = {}
        # An explicit disposable scratch scope is part of this synthetic probe.
        # It never changes deployment scratch policy or consumes a private corpus.
        with (
            patch.object(tempfile, "tempdir", str(scratch)),
            build_runtime(config, event_logger=lambda _message: None) as runtime,
        ):
            runtime.database_admin.initialize()
            runtime.resident.initialize()
            original_prepare = VNextIngestFacade.prepare_source

            def measured_prepare(facade: VNextIngestFacade, *args: Any, **kwargs: Any):
                prepared, measured = _measure(
                    source, lambda: original_prepare(facade, *args, **kwargs)
                )
                cases["baseline"] = measured
                return prepared

            started = perf_counter()
            with patch.object(VNextIngestFacade, "prepare_source", measured_prepare):
                if not runtime.resident.process_available(periodic_scan=True):
                    raise RuntimeError("synthetic baseline did not publish")
            seed_seconds = perf_counter() - started
            revision = runtime.catalog.get_catalog_revision()
            if revision.publication_count != galleries:
                raise RuntimeError("published baseline lost synthetic galleries")
            entries = runtime.catalog.discover_publications().publications
            if any(
                entry.page_count != pages or not entry.artifacts for entry in entries
            ):
                raise RuntimeError(
                    "published baseline lost pages or acquisition descriptors"
                )
            if runtime.database_admin.check().state != "READY":
                raise RuntimeError("published baseline failed full database audit")

            session = runtime.facade.try_claim_ingest(True, 1_800_000_000)
            if session is None:
                raise RuntimeError("synthetic baseline did not release its ingest turn")
            try:
                changed = config.model_copy(
                    update={
                        "paths": config.paths.model_copy(
                            update={
                                "render_policy": ArtifactRenderPolicyConfig(
                                    page_jpeg_quality=89
                                )
                            }
                        )
                    }
                )
                for name, selected in (
                    ("unchanged", config),
                    ("policy_changed", changed),
                    ("source_changed", config),
                ):
                    if name == "source_changed":
                        folder = source / "1000000"
                        _write_page(
                            folder / f"000.{codec}", edge, random.Random(87029), codec
                        )
                        (folder / "galleryinfo.txt").write_bytes(
                            _metadata(1_000_000, changed=True)
                        )
                    policy = runtime.facade.ensure_policy(
                        session, build_ingest_policy(selected)
                    )
                    with (
                        SourceSnapshotStore() as captured,
                        FilesystemSource(source) as fs,
                    ):
                        adapter = VNextFilesystemSourceAdapter(
                            fs,
                            qualify_gallery=ImageGalleryQualifier(
                                selected.paths.artifact_render_policy(), workers=workers
                            ),
                            snapshot=captured,
                        )
                        prepared, result = _measure(
                            source,
                            partial(
                                runtime.facade.prepare_source, adapter, policy=policy
                            ),
                        )
                        try:
                            result["captured_pages_verified"] = _verify_captures(
                                source, captured
                            )
                            cases[name] = result
                        finally:
                            prepared.close()
            finally:
                runtime.facade.complete_ingest(session)
            if runtime.catalog.get_catalog_revision() != revision:
                raise RuntimeError("prepare-only comparison changed the published head")
            if runtime.database_admin.check().state != "READY":
                raise RuntimeError("probe left invalid database authority")
        return {
            "status": "ok",
            "format_version": 2,
            "provenance": _provenance(),
            "fixture": {
                "galleries": galleries,
                "pages_per_gallery": pages,
                "dimensions": [edge, edge],
                "codec": codec,
                "encoding": (
                    "PNG, compression level 0"
                    if codec == "png"
                    else "JPEG, quality 95, subsampling 0, optimize false"
                ),
                "seed": 47029,
                "source_change_seed": 87029,
                "workers": workers,
                "backend": "sqlite",
                "aggregate_pixels": galleries * pages * edge * edge,
                "max_fixture_pixels": _MAX_FIXTURE_PIXELS,
                "max_fixture_encoded_bytes": _MAX_FIXTURE_ENCODED_BYTES,
                "qualification_spool_threshold_bytes": _SPOOL_THRESHOLD_BYTES,
            },
            "scope": {
                "measured": "prepare_source only; one real published baseline",
                "comparisons": "all prepare-only cases use the same published baseline",
                "source_changed": "one PAGE plus its producer completion marker",
                "policy_changed": "page_jpeg_quality 90 -> 89; source unchanged",
                "source_io": "actual os.read bytes/calls, including EOF calls, by inode and exclusive phase",
                "source_files": "a second view of the same raw reads; do not add it to phase totals",
                "buffer_io": "logical stream reads/writes, not physical disk I/O; memory qualification spools included",
                "qualification_disk_spools": "count of rolled spools when _spool returns, before decoding; not all native/scratch disk I/O",
                "native_io_limit": "only observed Python stream boundaries; native reads bypassing these wrappers are not counted",
                "excluded": "I/O counters omit SQLite and discovery/core spools; preparation wall includes their work; later rendering, snapshot verification and prepared-resource teardown are outside measured preparation",
                "timing": "observational; instrumentation overhead included; no wall-time pass threshold",
                "cache_state": "freshly generated local fixtures followed by baseline publication; no cache flush or NAS throughput claim",
                "operation_timing": "nested totals and concurrent decode sums overlap; never sum as wall time",
                "process_cpu": "process_time covers all process threads including native decoders; it is not concurrent decode elapsed sums",
            },
            "seed_publication_seconds": seed_seconds,
            "cases": cases,
        }


def _verify_captures(root: Path, captured: SourceSnapshotStore) -> int:
    checked = 0
    for path in sorted(root.rglob("*")):
        if path.suffix not in {".png", ".jpeg"}:
            continue
        stream = captured.open_source((path.parent.name,), path.name.encode())
        if stream is not None:
            with stream:
                if stream.read() != path.read_bytes():
                    raise RuntimeError(
                        "captured snapshot differs from exact source bytes"
                    )
            checked += 1
    return checked


def _provenance() -> dict[str, object]:
    repository = Path(__file__).resolve().parents[1]
    manifest = tomllib.loads((repository / "pyproject.toml").read_text())
    return {
        "python": sys.version,
        "platform": {
            "system": sys.platform,
            "release": platform.release(),
            "machine": platform.machine(),
        },
        "probe_sha256": sha256(Path(__file__).read_bytes()).hexdigest(),
        "checkout_project_version": manifest["project"]["version"],
        "h2hdb": {
            "distribution_version": version("h2hdb"),
            "module": h2hdb.__file__,
            "python_source_sha256": _package_digest(Path(h2hdb.__file__).parent),
        },
        "h2hdb_ingest": {
            "distribution_version": version("h2hdb-ingest"),
            "module": h2hdb_ingest.__file__,
            "python_source_sha256": _package_digest(Path(h2hdb_ingest.__file__).parent),
        },
        "image_packages": {
            name: version(name) for name in ("pillow", "pyvips", "pyvips-binary")
        },
    }


def _package_digest(root: Path) -> str:
    digest = sha256()
    for path in sorted(root.rglob("*.py")):
        name = path.relative_to(root).as_posix().encode()
        digest.update(len(name).to_bytes(8, "big") + name)
        digest.update(sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def _atomic_report(
    path: Path, report: dict[str, object], *, overwrite: bool = False
) -> None:
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            json.dump(report, stream, indent=2, sort_keys=True, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        if overwrite:
            os.replace(temporary, path)
        else:
            # Atomic no-replace publication also rejects a concurrently created
            # file or dangling symlink after the CLI's initial preflight.
            os.link(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _bounded_integer(minimum: int, maximum: int):
    def parse(value: str) -> int:
        result = int(value)
        if not minimum <= result <= maximum:
            raise argparse.ArgumentTypeError(f"must be in {minimum}..{maximum}")
        return result

    return parse


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--galleries", type=_bounded_integer(1, 4), default=2)
    parser.add_argument("--pages", type=_bounded_integer(1, 8), default=2)
    parser.add_argument("--edge", type=_bounded_integer(16, 2048), default=256)
    parser.add_argument("--codec", choices=("png", "jpeg"), default="png")
    parser.add_argument("--workers", type=_bounded_integer(1, 4), default=1)
    parser.add_argument("--timeout", type=_bounded_integer(10, 300), default=120)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--workspace", type=Path, help=argparse.SUPPRESS)
    arguments = parser.parse_args()
    try:
        _require_fixture_budget(arguments.galleries, arguments.pages, arguments.edge)
    except ValueError as error:
        parser.error(str(error))
    if arguments.worker:
        if arguments.workspace is None:
            parser.error("internal worker requires its supervisor-owned workspace")
        print(
            json.dumps(
                _run_matrix(
                    arguments.galleries,
                    arguments.pages,
                    arguments.edge,
                    arguments.workers,
                    codec=arguments.codec,
                    workspace=arguments.workspace,
                )
            )
        )
        return 0
    if arguments.output is None:
        parser.error("--output is required")
    if os.path.lexists(arguments.output):
        parser.error("--output already exists; choose a new report path")
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--galleries",
        str(arguments.galleries),
        "--pages",
        str(arguments.pages),
        "--edge",
        str(arguments.edge),
        "--workers",
        str(arguments.workers),
        "--codec",
        arguments.codec,
    ]
    try:
        # Parent ownership ensures cleanup after timeout kills the worker before
        # its own TemporaryDirectory.__exit__ can run.
        with tempfile.TemporaryDirectory(prefix="h2hdb-source-io-probe-") as workspace:
            result = subprocess.run(
                [*command, "--workspace", workspace],
                capture_output=True,
                text=True,
                timeout=arguments.timeout,
                check=True,
            )
        report = json.loads(result.stdout)
        if (
            not isinstance(report, dict)
            or report.get("status") != "ok"
            or report.get("format_version") != 2
            or not isinstance(report.get("cases"), dict)
            or set(report["cases"])
            != {"baseline", "unchanged", "policy_changed", "source_changed"}
        ):
            raise ValueError("worker returned an incomplete source I/O report")
    except (subprocess.SubprocessError, ValueError) as error:
        report = {
            "status": "error",
            "format_version": 2,
            "error_type": type(error).__name__,
            "error": str(error),
            "provenance": _provenance(),
            "fixture": {
                "galleries": arguments.galleries,
                "pages_per_gallery": arguments.pages,
                "edge": arguments.edge,
                "workers": arguments.workers,
                "codec": arguments.codec,
                "timeout_seconds": arguments.timeout,
            },
        }
        if isinstance(error, subprocess.CalledProcessError):
            report["worker_stderr"] = error.stderr[-16000:]
    _atomic_report(arguments.output, report)
    print(json.dumps({"status": report["status"], "output": str(arguments.output)}))
    return 0 if report["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
