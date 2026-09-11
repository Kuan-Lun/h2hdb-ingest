"""Publication and restart evidence for independently changing galleries."""

from __future__ import annotations

import os
from pathlib import Path
from typing import BinaryIO

import pytest
from h2hdb import (
    ArtifactArchiveRenderEvidence,
    ArtifactSourceMember,
    CoreConfig,
    DatabaseConfig,
)
from PIL import Image

from h2hdb_ingest import IngestConfig, IngestPathsConfig, ResidentConfig
from h2hdb_ingest.library import ManagedFilesystemLibraryAdapter
from h2hdb_ingest.runtime import IngestRuntime, build_runtime


def _marker(folder: Path, *, stamp: int) -> None:
    path = folder / "galleryinfo.txt"
    path.write_text(
        f"Title: Gallery {folder.name}\n"
        "Upload Time: 2024-01-02 03:04\n"
        "Uploaded By: uploader\n"
        "Downloaded: 2024-02-03 04:05\n"
        "Tags: artist:shared\n"
        "Uploader's Comments\n"
        "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3\n",
        encoding="utf-8",
    )
    os.utime(path, ns=(stamp, stamp))


def _page(folder: Path, color: str, *, stamp: int) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / "001.jpg"
    with Image.new("RGB", (12, 16), color) as image:
        image.save(path)
    os.utime(path, ns=(stamp, stamp))


def _config(tmp_path: Path, *, batch: int = 100) -> IngestConfig:
    source = tmp_path / "download"
    source.mkdir(exist_ok=True)
    library = tmp_path / "library"
    for child in ("current/acquisitions", "current/artwork", ".h2hdb-coordination"):
        (library / child).mkdir(parents=True, exist_ok=True)
    return IngestConfig(
        core=CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite", database=str(tmp_path / "catalog.sqlite3")
            )
        ),
        paths=IngestPathsConfig(
            download_path=source, library_path=library, page_render_workers=1
        ),
        resident=ResidentConfig(
            lease_seconds=30, heartbeat_seconds=5, publication_batch_galleries=batch
        ),
    )


def _archive(tmp_path: Path, gid: int) -> bytes:
    matches = tuple((tmp_path / "library/current/acquisitions").rglob(f"h2h-{gid}.cbz"))
    assert len(matches) == 1
    return matches[0].read_bytes()


def _synchronize(runtime: IngestRuntime) -> None:
    """Drive maintenance steps until this call actually publishes a source turn."""

    previous = runtime.resident.last_synchronization_result
    for _ in range(128):
        assert runtime.resident.process_available(periodic_scan=True)
        result = runtime.resident.last_synchronization_result
        if result is not None and result is not previous:
            return
    pytest.fail("small gallery fixture did not finish its maintenance and publication")


def test_markerless_new_gallery_does_not_consume_quota_or_force_retries(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, batch=1)
    source = config.paths.download_path
    _page(source / "1000", "green", stamp=10)
    _page(source / "1001", "red", stamp=10)
    _marker(source / "1001", stamp=10)
    _page(source / "1002", "blue", stamp=10)
    _marker(source / "1002", stamp=10)
    events: list[str] = []
    with build_runtime(config, event_logger=events.append) as runtime:
        runtime.database_admin.initialize()
        runtime.resident.initialize()
        _synchronize(runtime)
        assert runtime.catalog.get_catalog_revision().publication_count == 1
        assert runtime.resident.last_synchronization_result is not None
        assert runtime.resident.last_synchronization_result.waiting_gallery_count == 0
        _synchronize(runtime)
        assert runtime.catalog.get_catalog_revision().publication_count == 2
        assert all("waiting_galleries=1" not in message for message in events)
        _marker(source / "1000", stamp=10)
        _synchronize(runtime)
        assert runtime.catalog.get_catalog_revision().publication_count == 3
        runtime.database_admin.check()


def test_numeric_collection_does_not_create_a_permanent_waiting_gallery(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    gallery = config.paths.download_path / "2024" / "1001"
    _page(gallery, "red", stamp=10)
    _marker(gallery, stamp=10)
    with build_runtime(config) as runtime:
        runtime.database_admin.initialize()
        runtime.resident.initialize()
        _synchronize(runtime)
        assert runtime.catalog.get_catalog_revision().publication_count == 1
        result = runtime.resident.last_synchronization_result
        assert result is not None
        assert result.waiting_gallery_count == 0
        assert result.deferred_gallery_count == 0
        runtime.database_admin.check()


def test_missing_marker_retains_published_gallery_across_restart(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    source = config.paths.download_path
    first = source / "1001"
    _page(first, "red", stamp=10)
    _marker(first, stamp=10)
    with build_runtime(config) as runtime:
        runtime.database_admin.initialize()
        runtime.resident.initialize()
        _synchronize(runtime)
        original = _archive(tmp_path, 1001)
        (first / "galleryinfo.txt").unlink()
        _page(first, "blue", stamp=20)
        _page(source / "1002", "green", stamp=20)
        _marker(source / "1002", stamp=20)
        _synchronize(runtime)
        assert runtime.catalog.get_catalog_revision().publication_count == 2
        assert _archive(tmp_path, 1001) == original
    with build_runtime(config) as restarted:
        restarted.resident.initialize()
        _synchronize(restarted)
        assert _archive(tmp_path, 1001) == original
        _marker(first, stamp=20)
        _synchronize(restarted)
        assert restarted.catalog.get_catalog_revision().publication_count == 2
        assert _archive(tmp_path, 1001) != original
        restarted.database_admin.check()


def test_source_update_during_render_uses_snapshot_then_converges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    folder = config.paths.download_path / "1001"
    _page(folder, "red", stamp=10)
    _marker(folder, stamp=10)
    render = ManagedFilesystemLibraryAdapter.render_archive
    changed = False

    def update_source(
        adapter: ManagedFilesystemLibraryAdapter,
        members: tuple[ArtifactSourceMember, ...],
        destination: BinaryIO,
        *,
        gid: int,
    ) -> ArtifactArchiveRenderEvidence:
        nonlocal changed
        if not changed:
            changed = True
            _page(folder, "blue", stamp=20)
            _marker(folder, stamp=20)
        return render(adapter, members, destination, gid=gid)

    monkeypatch.setattr(
        ManagedFilesystemLibraryAdapter, "render_archive", update_source
    )
    with build_runtime(config) as runtime:
        runtime.database_admin.initialize()
        runtime.resident.initialize()
        _synchronize(runtime)
        original = _archive(tmp_path, 1001)
        assert changed
        _synchronize(runtime)
        assert _archive(tmp_path, 1001) != original
        revision = runtime.catalog.get_catalog_revision()
        _synchronize(runtime)
        assert runtime.catalog.get_catalog_revision() == revision
        runtime.database_admin.check()
