"""Benchmark fresh synthetic source-to-CBZ overhead and source scan counts."""

from __future__ import annotations

import argparse
import json
import os
import resource
import sys
import tempfile
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from threading import Event, Thread
from time import perf_counter
from typing import Protocol, cast
from unittest.mock import patch

from h2hdb import CoreConfig, DatabaseConfig
from PIL import Image

from h2hdb_ingest.config import (
    ArtifactRenderPolicyConfig,
    ArtifactRenderPreset,
    IngestConfig,
    IngestPathsConfig,
    ResidentConfig,
)
from h2hdb_ingest.core_source import VNextFilesystemSourceAdapter
from h2hdb_ingest.filesystem import FilesystemSource
from h2hdb_ingest.runtime import build_runtime

_FIRST_GID = 1_000_000


class _Digest(Protocol):
    def update(self, value: bytes, /) -> None: ...


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--galleries",
        type=int,
        required=True,
        help="number of one-page synthetic galleries (1..10000)",
    )
    parser.add_argument(
        "--mode",
        choices=("pipeline", "source-only"),
        default="pipeline",
        help="run the fresh full pipeline or only freeze source observations",
    )
    parser.add_argument(
        "--progress-seconds",
        type=float,
        default=600.0,
        help="full-pipeline resource report interval (default: 600 seconds)",
    )
    return parser.parse_args()


def _metadata(gid: int) -> bytes:
    return (
        "\n".join(
            (
                f"Title: synthetic-{gid}",
                "Upload Time: 2024-01-02 03:04",
                "Uploaded By: synthetic",
                "Downloaded: 2024-02-03 04:05",
                f"Tags: artist:synthetic-{gid}, language:english",
                "Uploader's Comments",
                "Synthetic fixed-overhead benchmark",
                "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3",
            )
        )
        + "\n"
    ).encode()


def _build_source(root: Path, gallery_count: int) -> None:
    encoded = BytesIO()
    Image.new("RGB", (2, 2), (17, 91, 203)).save(encoded, format="JPEG")
    base_page = encoded.getvalue()
    root.mkdir()
    for offset in range(gallery_count):
        gid = _FIRST_GID + offset
        gallery = root / str(gid)
        gallery.mkdir()
        (gallery / "galleryinfo.txt").write_bytes(_metadata(gid))
        # A trailing decoder-safe discriminator prevents global duplicate-hash
        # policy from classifying all synthetic pages as one repeated source.
        (gallery / "001.jpg").write_bytes(base_page + gid.to_bytes(8, "big"))


def _provision_library(root: Path) -> None:
    for path in (
        root / "current" / "acquisitions",
        root / "current" / "artwork",
        root / ".h2hdb-coordination",
    ):
        path.mkdir(parents=True)


def _output_manifest(root: Path) -> tuple[str, int, int, int, int]:
    digest = sha256(b"h2hdb-ingest-synthetic-output-v1\0")
    file_count = 0
    byte_count = 0
    acquisition_count = 0
    artwork_count = 0
    for path in sorted(
        (item for item in root.rglob("*") if item.is_file()),
        key=lambda item: item.relative_to(root).as_posix(),
    ):
        relative = path.relative_to(root).as_posix().encode()
        content_digest = sha256()
        size = 0
        with path.open("rb") as stream:
            while block := stream.read(1024 * 1024):
                content_digest.update(block)
                size += len(block)
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(size.to_bytes(8, "big"))
        digest.update(content_digest.digest())
        file_count += 1
        byte_count += size
        if path.suffix == ".cbz":
            acquisition_count += 1
        elif path.name == "thumbnail-320.jpg":
            artwork_count += 1
    return (
        digest.hexdigest(),
        file_count,
        byte_count,
        acquisition_count,
        artwork_count,
    )


def _frame(digest: _Digest, value: bytes) -> None:
    digest.update(len(value).to_bytes(8, "big"))
    digest.update(value)


def _source_semantic_manifest(source_root: Path) -> tuple[str, int, int]:
    digest = sha256(b"h2hdb-ingest-synthetic-source-semantics-v1\0")
    gallery_count = 0
    file_count = 0
    with FilesystemSource(source_root) as source:
        adapter = VNextFilesystemSourceAdapter(source)
        after_locator: tuple[str, ...] | None = None
        while True:
            locator_page = adapter.list_gallery_locators(
                after_locator=after_locator,
                limit=128,
            )
            for locator in locator_page.items:
                gallery_count += 1
                for component in locator:
                    _frame(digest, component.encode())
                observation = adapter.observe_gallery(locator)
                metadata = observation.metadata
                for numeric in (
                    metadata.gid,
                    metadata.upload_time,
                    metadata.download_time,
                    metadata.scan_observation_version,
                    metadata.source_file_count,
                    metadata.page_count,
                ):
                    digest.update(numeric.to_bytes(16, "big", signed=True))
                for text in (
                    metadata.title,
                    metadata.comment,
                    metadata.upload_account,
                ):
                    _frame(digest, text.encode())

                after_name: bytes | None = None
                while True:
                    file_page = adapter.list_file_observations(
                        observation,
                        after_name_bytes=after_name,
                        limit=256,
                    )
                    for item in file_page.items:
                        file_count += 1
                        _frame(digest, item.name_bytes)
                        digest.update(item.content.file_sha256)
                        digest.update(item.content.size_bytes.to_bytes(8, "big"))
                        _frame(digest, item.artifact_role.value.encode())
                    if file_page.terminal:
                        break
                    if not isinstance(file_page.next_after, bytes):
                        raise RuntimeError("file page returned a non-byte cursor")
                    after_name = file_page.next_after

                after_name = None
                while True:
                    directory_page = adapter.list_directory_observations(
                        observation,
                        after_name_bytes=after_name,
                        limit=192,
                    )
                    for item in directory_page.items:
                        _frame(digest, item.name_bytes)
                        digest.update(int(item.file_type).to_bytes(1, "big"))
                    if directory_page.terminal:
                        break
                    if not isinstance(directory_page.next_after, bytes):
                        raise RuntimeError("directory page returned a non-byte cursor")
                    after_name = directory_page.next_after

                after_ordinal: int | None = None
                while True:
                    tag_page = adapter.list_tag_observations(
                        observation,
                        after_ordinal=after_ordinal,
                        limit=256,
                    )
                    for item in tag_page.items:
                        _frame(digest, item.namespace.encode())
                        _frame(digest, item.value.encode())
                    if tag_page.terminal:
                        break
                    if not isinstance(tag_page.next_after, int):
                        raise RuntimeError("tag page returned a non-integer cursor")
                    after_ordinal = tag_page.next_after
            if locator_page.terminal:
                break
            if not isinstance(locator_page.next_after, tuple):
                raise RuntimeError("locator page returned a non-tuple cursor")
            after_locator = locator_page.next_after
    return digest.hexdigest(), gallery_count, file_count


def _rss_bytes() -> int:
    raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return raw if sys.platform == "darwin" else raw * 1024


def _tree_usage(root: Path) -> tuple[int, int]:
    count = 0
    size = 0
    for path in root.rglob("*"):
        if path.is_file():
            count += 1
            size += path.stat().st_size
    return count, size


def _monitor_resources(
    *,
    stop: Event,
    safety_stop: Event,
    interval_seconds: float,
    benchmark_root: Path,
    database_path: Path,
) -> None:
    while not stop.wait(interval_seconds):
        file_count, tree_bytes = _tree_usage(benchmark_root)
        rss_bytes = _rss_bytes()
        database_bytes = database_path.stat().st_size if database_path.exists() else 0
        print(
            json.dumps(
                {
                    "event": "progress",
                    "database_size_bytes": database_bytes,
                    "scratch_file_count": file_count,
                    "scratch_tree_bytes": tree_bytes,
                    "ru_maxrss_bytes": rss_bytes,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )
        if tree_bytes > 20 * 1024**3 or rss_bytes > 4 * 1024**3:
            safety_stop.set()
            return


def _run_source_only(gallery_count: int) -> dict[str, object]:
    started = perf_counter()
    with tempfile.TemporaryDirectory(
        prefix="h2hdb-ingest-synthetic-source-"
    ) as temporary:
        source_root = Path(temporary) / "source"
        create_started = perf_counter()
        _build_source(source_root, gallery_count)
        create_seconds = perf_counter() - create_started
        gallery_entry_scans = 0
        original_scandir = os.scandir
        resolved_source_root = source_root.resolve()

        def counted_scandir(
            path: int | os.PathLike[str] | str,
        ) -> os.ScandirIterator[str]:
            nonlocal gallery_entry_scans
            if not isinstance(path, int):
                candidate = Path(path)
                if (
                    candidate.parent == resolved_source_root
                    and candidate.name.isdecimal()
                ):
                    gallery_entry_scans += 1
            return cast("os.ScandirIterator[str]", original_scandir(path))

        source_started = perf_counter()
        with patch.object(os, "scandir", counted_scandir):
            manifest, observed_galleries, observed_files = _source_semantic_manifest(
                source_root
            )
        source_seconds = perf_counter() - source_started
        if observed_galleries != gallery_count or observed_files != gallery_count * 2:
            raise RuntimeError("source-only benchmark observed an incomplete corpus")
        return {
            "gallery_count": gallery_count,
            "workload": "synthetic-source-only-two-files",
            "create_seconds": create_seconds,
            "source_seconds": source_seconds,
            "total_seconds": perf_counter() - started,
            "gallery_scans_including_discovery": gallery_entry_scans,
            "observation_gallery_scans": gallery_entry_scans - gallery_count,
            "source_file_count": observed_files,
            "source_semantic_manifest_sha256": manifest,
            "ru_maxrss_raw": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        }


def _run_pipeline(
    gallery_count: int,
    *,
    progress_seconds: float,
) -> dict[str, object]:
    started = perf_counter()
    with tempfile.TemporaryDirectory(
        prefix="h2hdb-ingest-synthetic-index-"
    ) as temporary:
        benchmark_root = Path(temporary)
        source_root = benchmark_root / "source"
        library_root = benchmark_root / "library"
        database_path = benchmark_root / "catalog.sqlite3"
        create_started = perf_counter()
        _build_source(source_root, gallery_count)
        _provision_library(library_root)
        create_seconds = perf_counter() - create_started
        config = IngestConfig(
            core=CoreConfig(
                database=DatabaseConfig(
                    sql_type="sqlite",
                    database=str(database_path),
                )
            ),
            paths=IngestPathsConfig(
                download_path=source_root,
                library_path=library_root,
                max_image_short_side=1,
                render_policy=ArtifactRenderPolicyConfig(
                    preset=ArtifactRenderPreset.BENCHMARK_LOW_COST,
                ),
                page_render_workers=1,
            ),
            resident=ResidentConfig(
                lease_seconds=1_800,
                heartbeat_seconds=30,
                max_rows=128,
            ),
        )
        runtime = build_runtime(config, event_logger=lambda _message: None)
        try:
            schema_started = perf_counter()
            runtime.database_admin.initialize()
            schema_seconds = perf_counter() - schema_started
            startup_started = perf_counter()
            runtime.resident.initialize()
            startup_seconds = perf_counter() - startup_started
            gallery_entry_scans = 0
            original_scandir = os.scandir
            resolved_source_root = source_root.resolve()

            def counted_scandir(
                path: int | os.PathLike[str] | str,
            ) -> os.ScandirIterator[str]:
                nonlocal gallery_entry_scans
                if not isinstance(path, int):
                    candidate = Path(path)
                    if (
                        candidate.parent == resolved_source_root
                        and candidate.name.isdecimal()
                    ):
                        gallery_entry_scans += 1
                return cast("os.ScandirIterator[str]", original_scandir(path))

            monitor_stop = Event()
            safety_stop = Event()
            monitor = Thread(
                target=_monitor_resources,
                kwargs={
                    "stop": monitor_stop,
                    "safety_stop": safety_stop,
                    "interval_seconds": progress_seconds,
                    "benchmark_root": benchmark_root,
                    "database_path": database_path,
                },
                name="synthetic-benchmark-resource-monitor",
                daemon=True,
            )
            print(
                json.dumps(
                    {
                        "event": "pipeline-start",
                        "gallery_count": gallery_count,
                        "scratch_root": str(benchmark_root),
                        "database_size_bytes": database_path.stat().st_size,
                        "ru_maxrss_bytes": _rss_bytes(),
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
                flush=True,
            )
            process_started = perf_counter()
            monitor.start()
            try:
                with patch.object(os, "scandir", counted_scandir):
                    progressed = runtime.resident.process_available(
                        periodic_scan=True,
                        should_stop=safety_stop.is_set,
                    )
            finally:
                monitor_stop.set()
                monitor.join()
            process_seconds = perf_counter() - process_started
            if safety_stop.is_set():
                raise RuntimeError(
                    "synthetic benchmark crossed its 20 GiB scratch or 4 GiB RSS "
                    "safety ceiling"
                )
            audit_started = perf_counter()
            report = runtime.database_admin.check()
            audit_seconds = perf_counter() - audit_started
            revision = runtime.catalog.get_catalog_revision()
            (
                manifest,
                output_file_count,
                output_byte_count,
                acquisition_count,
                artwork_count,
            ) = _output_manifest(library_root / "current")
            if not progressed:
                raise RuntimeError("fresh synthetic ingest reported no progress")
            if report.state != "READY":
                raise RuntimeError("fresh synthetic database is not READY")
            if revision.publication_count != gallery_count:
                raise RuntimeError(
                    "synthetic publication count differs from requested galleries"
                )
            if output_file_count != gallery_count * 2:
                raise RuntimeError(
                    "synthetic output must contain one CBZ and thumbnail per gallery"
                )
            if acquisition_count != gallery_count or artwork_count != gallery_count:
                raise RuntimeError(
                    "synthetic output kinds differ from one CBZ and thumbnail per gallery"
                )
            return {
                "gallery_count": gallery_count,
                "workload": "synthetic-one-2x2-page-low-cost",
                "create_seconds": create_seconds,
                "schema_seconds": schema_seconds,
                "startup_full_audit_seconds": startup_seconds,
                "process_seconds": process_seconds,
                "final_full_audit_seconds": audit_seconds,
                "total_seconds": perf_counter() - started,
                "gallery_scans_including_discovery": gallery_entry_scans,
                "observation_gallery_scans": gallery_entry_scans - gallery_count,
                "output_file_count": output_file_count,
                "output_byte_count": output_byte_count,
                "acquisition_count": acquisition_count,
                "artwork_count": artwork_count,
                "output_manifest_sha256": manifest,
                "database_size_bytes": database_path.stat().st_size,
                "ru_maxrss_raw": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "ru_maxrss_bytes": _rss_bytes(),
            }
        finally:
            runtime.close()


def main() -> None:
    arguments = _arguments()
    gallery_count = arguments.galleries
    if isinstance(gallery_count, bool) or not 1 <= gallery_count <= 10_000:
        raise SystemExit("--galleries must be in 1..10000")
    if arguments.progress_seconds <= 0:
        raise SystemExit("--progress-seconds must be positive")
    if arguments.mode == "source-only":
        result = _run_source_only(gallery_count)
    else:
        result = _run_pipeline(
            gallery_count,
            progress_seconds=arguments.progress_seconds,
        )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
