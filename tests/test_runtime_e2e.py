from __future__ import annotations

import sqlite3
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import cast
from zipfile import ZipFile

import pytest
from h2hdb import (
    CatalogRevisionNotFoundError,
    CoreConfig,
    DatabaseConfig,
    VNextCurrentOnlyMaintenanceOutcome,
)
from PIL import Image

from h2hdb_ingest import IngestConfig, IngestPathsConfig, ResidentConfig
from h2hdb_ingest.runtime import build_runtime


@pytest.fixture(params=("sqlite", "mariadb"))
def runtime_core_config(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> CoreConfig:
    if request.param == "sqlite":
        return CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite",
                database=str(tmp_path / "catalog.sqlite3"),
            )
        )
    return cast(CoreConfig, request.getfixturevalue("mariadb_config"))


def _gallery(
    root: Path,
    gid: int,
    artist: str,
    *,
    page_bytes: bytes = b"first page",
) -> None:
    folder = root / str(gid)
    folder.mkdir(parents=True)
    (folder / "galleryinfo.txt").write_text(
        "\n".join(
            (
                f"Title: Runtime integration {gid}",
                "Upload Time: 2024-01-02 03:04",
                "Uploaded By: uploader",
                "Downloaded: 2024-02-03 04:05",
                f"Tags: artist:{artist}, language:english",
                "Uploader's Comments",
                "A comment",
                "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3",
            )
        ),
        encoding="utf-8",
    )
    (folder / "001.jpg").write_bytes(page_bytes)


def test_fresh_epoch_runs_source_analysis_and_publication(
    tmp_path: Path,
    runtime_core_config: CoreConfig,
) -> None:
    source = tmp_path / "download"
    _gallery(source, 1001, "first")
    config = IngestConfig(
        core=runtime_core_config,
        paths=IngestPathsConfig(download_path=source),
        resident=ResidentConfig(
            lease_seconds=30,
            heartbeat_seconds=5,
        ),
    )
    runtime = build_runtime(config)

    initialized = runtime.database_admin.initialize()
    checked = runtime.resident.initialize()
    processed = runtime.resident.process_available(periodic_scan=True)
    revision = runtime.catalog.get_catalog_revision()

    assert initialized.epoch == checked.epoch
    assert processed
    assert revision.revision == 1
    assert revision.publication_count == 1

    # A periodic scan after process restart must replay the exact SEALED source
    # snapshot without attempting to reopen its discovery checkpoint.
    restarted = build_runtime(config)
    restarted.resident.initialize()
    assert restarted.resident.process_available(periodic_scan=True)
    replayed_revision = restarted.catalog.get_catalog_revision()
    assert replayed_revision == revision

    # The same content in three galleries with three distinct artists reaches
    # the registered spam threshold.  This public result covers both derived
    # source projections: per-observation hash occurrences and artist tags.
    _gallery(source, 1002, "second")
    _gallery(source, 1003, "third")

    assert restarted.resident.process_available(periodic_scan=True)
    excluded_revision = restarted.catalog.get_catalog_revision()
    assert excluded_revision.revision == 2
    assert excluded_revision.publication_count == 0

    # Replay the incremental build after its own publication has advanced the
    # channel head.  Its analysis must retain the baseline that was persisted
    # when the run began instead of deriving a new baseline from revision 2.
    restarted_incremental = build_runtime(config)
    restarted_incremental.resident.initialize()
    assert restarted_incremental.resident.process_available(periodic_scan=True)
    assert restarted_incremental.catalog.get_catalog_revision() == excluded_revision


def test_same_locator_content_a_b_a_creates_three_revisions_then_replays(
    tmp_path: Path,
    runtime_core_config: CoreConfig,
) -> None:
    source = tmp_path / "download"
    _gallery(source, 1001, "artist", page_bytes=b"content-A")
    _gallery(source, 1002, "other", page_bytes=b"stable-content")
    config = IngestConfig(
        core=runtime_core_config,
        paths=IngestPathsConfig(download_path=source),
        resident=ResidentConfig(
            lease_seconds=30,
            heartbeat_seconds=5,
        ),
    )
    runtime = build_runtime(config)
    runtime.database_admin.initialize()

    def current_content() -> str:
        page = runtime.catalog.list_publications()
        assert page.total == len(page.publications) == 2
        # These deterministic fixture GIDs occupy different cleanup shards, so
        # their two child-first cycles require more than 32 advances.
        publication = next(item for item in page.publications if item.gid == 1001)
        content_sha256 = publication.content_sha256
        assert content_sha256 is not None
        return content_sha256

    assert runtime.resident.process_available(periodic_scan=True)
    first_revision = runtime.catalog.get_catalog_revision()
    first_content = current_content()
    assert first_revision.revision == 1

    (source / "1001" / "001.jpg").write_bytes(b"content-B")
    assert runtime.resident.process_available(periodic_scan=True)
    second_revision = runtime.catalog.get_catalog_revision()
    second_content = current_content()
    assert second_revision.revision == 2
    assert second_content != first_content
    # The post-session attempt is capped at 16 committed cleanup advances.
    # More work remains, so the next resident cycle reports maintenance
    # progress immediately without claiming a third ingest generation.
    assert runtime.resident.process_available(periodic_scan=False)
    assert runtime.catalog.get_catalog_revision() == second_revision
    outcome = runtime.facade.drain_current_only_maintenance(30_000_000)
    for _attempt in range(8):
        if outcome is VNextCurrentOnlyMaintenanceOutcome.DONE:
            break
        assert outcome is VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
        outcome = runtime.facade.drain_current_only_maintenance(30_000_000)
    assert outcome is VNextCurrentOnlyMaintenanceOutcome.DONE

    # Session completion releases the SHARED ingest gate before the resident
    # claims EXCLUSIVE maintenance.  The finished sweep keeps revision 2 fully
    # readable, rejects the stale revision-1 pin, and leaves the FK-on epoch
    # READY after removing every gallery-linear revision-1 catalog row.
    assert runtime.database_admin.check().state == "READY"
    assert runtime.catalog.list_publications(revision=second_revision).total == 2
    with pytest.raises(CatalogRevisionNotFoundError):
        runtime.catalog.list_publications(revision=first_revision)
    (source / "1001" / "001.jpg").write_bytes(b"content-A")
    assert runtime.resident.process_available(periodic_scan=True)
    third_revision = runtime.catalog.get_catalog_revision()
    third_content = current_content()
    assert third_revision.revision == 3
    assert third_content == first_content

    restarted = build_runtime(config)
    restarted.resident.initialize()
    assert restarted.resident.process_available(periodic_scan=True)
    assert restarted.catalog.get_catalog_revision() == third_revision
    replayed = restarted.catalog.list_publications()
    assert replayed.total == len(replayed.publications) == 2
    assert next(
        item for item in replayed.publications if item.gid == 1001
    ).content_sha256 == (first_content)


def test_fresh_artifact_runtime_publishes_immutable_and_current_cbz(
    tmp_path: Path,
) -> None:
    source = tmp_path / "download"
    _gallery(source, 2001, "artist")
    Image.new("RGB", (8, 12), "red").save(source / "2001" / "001.jpg")
    artifact_root = tmp_path / "artifacts"
    current_root = tmp_path / "current"
    config = IngestConfig(
        core=CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite",
                database=str(tmp_path / "catalog.sqlite3"),
            )
        ),
        paths=IngestPathsConfig(
            download_path=source,
            artifact_store_path=artifact_root,
            cbz_path=current_root,
        ),
        resident=ResidentConfig(
            lease_seconds=30,
            heartbeat_seconds=5,
        ),
    )
    runtime = build_runtime(config)

    runtime.database_admin.initialize()
    assert runtime.resident.process_available(periodic_scan=True)
    page = runtime.catalog.list_publications(require_artifact=True)
    immutable = tuple((artifact_root / "sha256").glob("*/*.cbz"))
    current = tuple(current_root.rglob("*.cbz"))

    assert page.total == 1
    assert len(page.publications) == 1
    assert len(page.publications[0].artifacts) == 1
    assert len(immutable) == len(current) == 1
    assert artifact_root / page.publications[0].artifacts[0].location == immutable[0]
    first_immutable = immutable[0]
    first_digest = page.publications[0].artifacts[0].sha256
    assert immutable[0].read_bytes() == current[0].read_bytes()
    assert sha256(immutable[0].read_bytes()).hexdigest() == (
        page.publications[0].artifacts[0].sha256
    )
    with ZipFile(immutable[0]) as archive:
        assert archive.namelist() == [
            "0000000000000000__content.jpg",
            "0000000000000001__metadata.txt",
        ]
        assert (
            archive.read("0000000000000001__metadata.txt")
            == (source / "2001" / "galleryinfo.txt").read_bytes()
        )
        with Image.open(
            BytesIO(archive.read("0000000000000000__content.jpg"))
        ) as image:
            assert image.format == "JPEG"
            assert image.size == (8, 12)

    Image.new("RGB", (8, 12), "blue").save(source / "2001" / "001.jpg")
    assert runtime.resident.process_available(periodic_scan=True)
    second_page = runtime.catalog.list_publications(require_artifact=True)
    second_immutable = tuple((artifact_root / "sha256").glob("*/*.cbz"))
    second_current = tuple(current_root.rglob("*.cbz"))

    assert second_page.revision.revision == 2
    assert second_page.total == len(second_page.publications) == 1
    assert len(second_page.publications[0].artifacts) == 1
    second_artifact = second_page.publications[0].artifacts[0]
    assert second_artifact.sha256 != first_digest
    outcome = runtime.facade.drain_current_only_maintenance(30_000_000)
    for _attempt in range(8):
        if outcome is VNextCurrentOnlyMaintenanceOutcome.DONE:
            break
        assert outcome is VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
        outcome = runtime.facade.drain_current_only_maintenance(30_000_000)
    assert outcome is VNextCurrentOnlyMaintenanceOutcome.DONE
    assert not first_immutable.exists()
    assert second_immutable == (artifact_root / second_artifact.location,)
    assert len(second_current) == 1
    assert second_immutable[0].read_bytes() == second_current[0].read_bytes()
    assert runtime.database_admin.check().state == "READY"
    with sqlite3.connect(
        artifact_root / ".h2hdb-vnext-artifacts.sqlite3"
    ) as artifact_state:
        assert artifact_state.execute("SELECT COUNT(*) FROM artifacts").fetchone() == (
            1,
        )
        assert artifact_state.execute(
            "SELECT state, COUNT(*) FROM protection_tokens GROUP BY state"
        ).fetchall() == [("RELEASED", 2)]


def test_resident_drains_more_than_one_bounded_artifact_cleanup_page(
    tmp_path: Path,
) -> None:
    source = tmp_path / "download"
    artifact_root = tmp_path / "artifacts"
    current_root = tmp_path / "current"
    gallery_count = 10
    for offset in range(gallery_count):
        gid = 3001 + offset
        _gallery(source, gid, f"artist-{gid}")
        Image.new("RGB", (8, 12), (offset * 11, 0, 255)).save(
            source / str(gid) / "001.jpg"
        )
    config = IngestConfig(
        core=CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite",
                database=str(tmp_path / "catalog.sqlite3"),
            )
        ),
        paths=IngestPathsConfig(
            download_path=source,
            artifact_store_path=artifact_root,
            cbz_path=current_root,
        ),
        resident=ResidentConfig(
            lease_seconds=30,
            heartbeat_seconds=5,
        ),
    )
    runtime = build_runtime(config)
    runtime.database_admin.initialize()

    projection_database = artifact_root / ".h2hdb-vnext-current-projection.sqlite3"
    artifact_database = artifact_root / ".h2hdb-vnext-artifacts.sqlite3"

    def pending_cleanup_count() -> int:
        with sqlite3.connect(projection_database) as projection_state:
            projection_row = projection_state.execute(
                "SELECT COUNT(*) FROM artifact_cleanup_candidates"
            ).fetchone()
            assert projection_row is not None
            projection_pending = int(projection_row[0])
        with sqlite3.connect(artifact_database) as artifact_state:
            artifact_row = artifact_state.execute(
                "SELECT COUNT(*) FROM artifact_cleanup_candidates"
            ).fetchone()
            assert artifact_row is not None
            artifact_pending = int(artifact_row[0])
        return projection_pending + artifact_pending

    assert runtime.resident.process_available(periodic_scan=True)
    first = runtime.catalog.list_publications(require_artifact=True)
    assert first.total == len(first.publications) == gallery_count
    old_paths = {
        artifact_root / publication.artifacts[0].location
        for publication in first.publications
    }
    assert len(old_paths) == gallery_count
    while pending_cleanup_count() > 0:
        runtime.resident.process_available(periodic_scan=False)

    for offset in range(gallery_count):
        gid = 3001 + offset
        Image.new("RGB", (8, 12), (offset * 11, 255, 0)).save(
            source / str(gid) / "001.jpg"
        )
    for _attempt in range(8):
        runtime.resident.process_available(periodic_scan=True)
        if runtime.catalog.get_catalog_revision().revision == 2:
            break
    assert runtime.catalog.get_catalog_revision().revision == 2

    # Reconciliation and the post-session action are each capped at eight, so
    # this production backlog cannot be consumed by either one-shot call.
    assert pending_cleanup_count() > 0
    attempts = 0
    while pending_cleanup_count() > 0:
        assert attempts < 8
        runtime.resident.process_available(periodic_scan=False)
        attempts += 1
    assert attempts > 0

    second = runtime.catalog.list_publications(require_artifact=True)
    current_paths = {
        artifact_root / publication.artifacts[0].location
        for publication in second.publications
    }
    assert second.revision.revision == 2
    assert second.total == len(second.publications) == gallery_count
    assert old_paths.isdisjoint(current_paths)
    assert all(not path.exists() for path in old_paths)
    assert all(path.is_file() for path in current_paths)
    assert set((artifact_root / "sha256").glob("*/*.cbz")) == current_paths
    projected_paths = tuple(current_root.rglob("*.cbz"))
    assert len(projected_paths) == gallery_count
    assert {sha256(path.read_bytes()).hexdigest() for path in projected_paths} == {
        path.stem for path in current_paths
    }
