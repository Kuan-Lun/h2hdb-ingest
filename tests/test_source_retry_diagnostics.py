from __future__ import annotations

import logging
from hashlib import sha256
from pathlib import Path

import pytest
from h2hdb import CoreConfig, DatabaseConfig
from PIL import Image

from h2hdb_ingest import IngestConfig, IngestPathsConfig, ResidentConfig
from h2hdb_ingest.runtime import build_runtime


def _source_snapshot(
    root: Path,
) -> dict[Path, tuple[tuple[int, ...], bytes | None]]:
    snapshot: dict[Path, tuple[tuple[int, ...], bytes | None]] = {}
    for path in (root, *sorted(root.rglob("*"))):
        stat = path.stat()
        snapshot[path] = (
            (
                stat.st_mode,
                stat.st_dev,
                stat.st_ino,
                stat.st_nlink,
                stat.st_size,
                stat.st_mtime_ns,
                stat.st_ctime_ns,
            ),
            sha256(path.read_bytes()).digest() if path.is_file() else None,
        )
    return snapshot


def test_incomplete_gallery_waits_with_original_diagnostic_and_releases_its_lease(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    source = tmp_path / "download"
    gallery = source / "1001"
    gallery.mkdir(parents=True)
    Image.new("RGB", (8, 12), "red").save(gallery / "001.jpg")
    (gallery / "galleryinfo.txt").write_text(
        "\n".join(
            (
                "Title: Stable invalid metadata",
                "Upload Time: 2024-01-02 03:04",
                "Uploaded By: uploader",
                "Downloaded: 2024-02-03 04:05",
                "Uploader's Comments",
                "The required Tags field is deliberately missing.",
                "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3",
            )
        ),
        encoding="utf-8",
    )
    original_source = _source_snapshot(source)
    library = tmp_path / "library"
    for relative in (
        "current/acquisitions",
        "current/artwork",
        ".h2hdb-coordination",
    ):
        (library / relative).mkdir(parents=True)
    config = IngestConfig(
        core=CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite", database=str(tmp_path / "catalog.sqlite3")
            )
        ),
        paths=IngestPathsConfig(
            download_path=source,
            library_path=library,
            page_render_workers=1,
        ),
        resident=ResidentConfig(lease_seconds=30, heartbeat_seconds=5),
    )
    events: list[str] = []
    caplog.set_level(logging.DEBUG, logger="h2hdb_ingest")
    with build_runtime(config, event_logger=events.append) as runtime:
        runtime.database_admin.initialize()
        runtime.resident.initialize()
        for _attempt in range(2):
            caplog.clear()
            events.clear()
            previous = runtime.resident.last_synchronization_result
            for _maintenance_step in range(128):
                assert runtime.resident.process_available(periodic_scan=True)
                if runtime.resident.last_synchronization_result is not previous:
                    break
            else:
                pytest.fail("gallery retry did not finish bounded maintenance")
            result = runtime.resident.last_synchronization_result
            assert result is not None and result.waiting_gallery_count == 1
            warnings = [
                record.getMessage()
                for record in caplog.records
                if record.levelno == logging.WARNING
            ]
            assert warnings == []
            diagnostics = [
                record.getMessage()
                for record in caplog.records
                if record.levelno == logging.DEBUG
                and "Gallery source deferred" in record.getMessage()
            ]
            assert len(diagnostics) == 1
            diagnostic = diagnostics[0]
            for expected in (
                "missing required fields",
                str(gallery),
                "observe_gallery",
            ):
                assert expected in diagnostic
            assert not any("Ingest work will be retried" in event for event in events)
            assert any(
                "galleries waiting for source completion 1" in event for event in events
            )
            assert runtime.database_admin.check().state == "READY"
            assert runtime.catalog.get_catalog_revision().publication_count == 0
            assert _source_snapshot(source) == original_source
        # The deferred observation releases its real lease on every retry, so
        # another claimant need not wait for its 30 second expiration.
        session = runtime.facade.try_claim_ingest(True, 30_000_000)
        assert session is not None
        runtime.facade.complete_ingest(session)
    assert not tuple((library / "current").rglob("*.cbz"))
