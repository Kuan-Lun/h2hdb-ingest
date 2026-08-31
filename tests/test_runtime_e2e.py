from __future__ import annotations

from collections.abc import Callable
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
    GalleryStagingCapacityError,
    VNextCurrentOnlyMaintenanceOutcome,
)
from PIL import Image

from h2hdb_ingest import (
    IngestConfig,
    IngestPathsConfig,
    IngestSessionController,
    LibraryMaintenanceOutcome,
    ResidentConfig,
    ResidentIngestor,
)
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


def _provision_library_root(root: Path) -> None:
    root.mkdir(mode=0o777)
    root.chmod(0o777)
    current = root / "current"
    for path in (
        current,
        current / "acquisitions",
        current / "artwork",
        root / ".h2hdb-coordination",
    ):
        path.mkdir(mode=0o777)
        path.chmod(0o777)


class _CapacityThenSuccessService:
    def __init__(self) -> None:
        self.calls = 0

    def synchronize_once(
        self,
        session: IngestSessionController,
        *,
        should_stop: Callable[[], bool] | None = None,
    ) -> object:
        del session, should_stop
        self.calls += 1
        if self.calls == 1:
            raise GalleryStagingCapacityError(1_500_000)
        return "recovered"


class _DoneLibraryMaintenance:
    def maintain_cleanup(self) -> LibraryMaintenanceOutcome:
        return LibraryMaintenanceOutcome.DONE


def test_capacity_backpressure_releases_real_core_session_for_retry(
    tmp_path: Path,
    runtime_core_config: CoreConfig,
) -> None:
    source = tmp_path / "download"
    source.mkdir()
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
    service = _CapacityThenSuccessService()
    resident = ResidentIngestor(
        service=service,
        facade=runtime.facade,
        database_admin=runtime.database_admin,
        library_maintenance=_DoneLibraryMaintenance(),
        config=config.resident,
        database_type=config.core.database.sql_type,
    )

    assert not resident.process_available(periodic_scan=True)
    assert resident.process_available(periodic_scan=True)
    assert service.calls == 2
    assert runtime.database_admin.check().state == "READY"


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
        page = runtime.catalog.discover_publications(limit=128)
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
    assert (
        runtime.catalog.discover_publications(
            revision=second_revision,
            limit=128,
        ).total
        == 2
    )
    with pytest.raises(CatalogRevisionNotFoundError):
        runtime.catalog.discover_publications(
            revision=first_revision,
            limit=128,
        )
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
    replayed = restarted.catalog.discover_publications(limit=128)
    assert replayed.total == len(replayed.publications) == 2
    assert next(
        item for item in replayed.publications if item.gid == 1001
    ).content_sha256 == (first_content)


def test_fresh_artifact_runtime_publishes_one_current_cbz(
    tmp_path: Path,
) -> None:
    source = tmp_path / "download"
    _gallery(source, 2001, "artist")
    Image.new("RGB", (8, 12), "red").save(source / "2001" / "001.jpg")
    library_root = tmp_path / "library"
    _provision_library_root(library_root)
    current_root = library_root / "current"
    config = IngestConfig(
        core=CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite",
                database=str(tmp_path / "catalog.sqlite3"),
            )
        ),
        paths=IngestPathsConfig(
            download_path=source,
            library_path=library_root,
        ),
        resident=ResidentConfig(
            lease_seconds=30,
            heartbeat_seconds=5,
        ),
    )
    runtime = build_runtime(config)

    runtime.database_admin.initialize()
    assert runtime.resident.process_available(periodic_scan=True)
    page = runtime.catalog.discover_publications()
    current = tuple(current_root.rglob("*.cbz"))

    assert page.total == 1
    assert len(page.publications) == 1
    publication = page.publications[0]
    assert len(publication.artifacts) == 1
    assert publication.page_count == 1
    assert publication.cover is not None
    assert publication.thumbnail is not None
    assert len(current) == 1
    first_path = current_root.joinpath(
        *publication.artifacts[0].storage_object.key.segments
    )
    thumbnail_path = current_root.joinpath(
        *publication.thumbnail.storage_object.key.segments
    )
    first_digest = publication.artifacts[0].storage_object.sha256
    assert current == (first_path,)
    assert thumbnail_path.is_file()
    assert sha256(first_path.read_bytes()).hexdigest() == (
        publication.artifacts[0].storage_object.sha256
    )
    assert sha256(thumbnail_path.read_bytes()).hexdigest() == (
        publication.thumbnail.storage_object.sha256
    )
    with ZipFile(first_path) as archive:
        assert archive.namelist() == [
            "galleryinfo.txt",
            "pages/0000.jpg",
        ]
        assert (
            archive.read("galleryinfo.txt")
            == (source / "2001" / "galleryinfo.txt").read_bytes()
        )
        with Image.open(BytesIO(archive.read("pages/0000.jpg"))) as image:
            assert image.format == "JPEG"
            assert image.size == (8, 12)
        archive_bytes = first_path.read_bytes()
        cover = publication.cover
        assert cover.storage_object == publication.artifacts[0].storage_object
        assert (
            sha256(
                archive_bytes[
                    cover.extent.offset : cover.extent.offset + cover.extent.length
                ]
            ).hexdigest()
            == cover.sha256
        )
    with Image.open(thumbnail_path) as thumbnail:
        assert thumbnail.format == "JPEG"
        assert max(thumbnail.size) <= 320

    Image.new("RGB", (8, 12), "blue").save(source / "2001" / "001.jpg")
    assert runtime.resident.process_available(periodic_scan=True)
    second_page = runtime.catalog.discover_publications()
    second_current = tuple(current_root.rglob("*.cbz"))

    assert second_page.revision.revision == 2
    assert second_page.total == len(second_page.publications) == 1
    assert len(second_page.publications[0].artifacts) == 1
    second_artifact = second_page.publications[0].artifacts[0]
    assert second_artifact.storage_object.sha256 != first_digest
    second_path = current_root.joinpath(*second_artifact.storage_object.key.segments)
    assert second_path == first_path
    assert len(second_current) == 1
    assert second_current == (second_path,)
    assert (
        sha256(second_path.read_bytes()).hexdigest()
        == second_artifact.storage_object.sha256
    )
    assert runtime.database_admin.check().state == "READY"
    state = library_root / ".h2hdb-state"
    assert not list((state / "staging").glob("*.cbz"))
    assert not list((state / "quarantine").glob("*.cbz"))
    assert not (library_root / ".h2hdb-coordination" / "ACTIVATING").exists()


def test_many_replacements_keep_one_stable_current_file_per_gid(
    tmp_path: Path,
) -> None:
    source = tmp_path / "download"
    library_root = tmp_path / "library"
    _provision_library_root(library_root)
    current_root = library_root / "current"
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
            library_path=library_root,
        ),
        resident=ResidentConfig(
            lease_seconds=30,
            heartbeat_seconds=5,
        ),
    )
    runtime = build_runtime(config)
    runtime.database_admin.initialize()

    assert runtime.resident.process_available(periodic_scan=True)
    first = runtime.catalog.discover_publications()
    assert first.total == len(first.publications) == gallery_count
    old_paths = {
        current_root.joinpath(*publication.artifacts[0].storage_object.key.segments)
        for publication in first.publications
    }
    old_digests = {
        publication.gid: publication.artifacts[0].storage_object.sha256
        for publication in first.publications
    }
    assert len(old_paths) == gallery_count

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

    second = runtime.catalog.discover_publications()
    current_paths = {
        current_root.joinpath(*publication.artifacts[0].storage_object.key.segments)
        for publication in second.publications
    }
    assert second.revision.revision == 2
    assert second.total == len(second.publications) == gallery_count
    assert old_paths == current_paths
    assert all(path.is_file() for path in current_paths)
    projected_paths = tuple(current_root.rglob("*.cbz"))
    assert len(projected_paths) == gallery_count
    assert {
        publication.gid: publication.artifacts[0].storage_object.sha256
        for publication in second.publications
    } != old_digests
    state = library_root / ".h2hdb-state"
    assert not list((state / "staging").glob("*.cbz"))
    assert not list((state / "quarantine").glob("*.cbz"))
    assert not (library_root / ".h2hdb-coordination" / "ACTIVATING").exists()
