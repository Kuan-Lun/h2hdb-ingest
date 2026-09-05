from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from shutil import rmtree
from time import time_ns
from typing import cast
from unittest.mock import patch
from zipfile import ZipFile

import pytest
from h2hdb import (
    CatalogDiscoveryQuery,
    CatalogPageCountRange,
    CatalogRevisionNotFoundError,
    CatalogSubjectFilter,
    CatalogTimestampRange,
    CoreConfig,
    DatabaseConfig,
    GalleryStagingCapacityError,
    VNextCurrentOnlyMaintenanceOutcome,
    VNextIngestAdvanceResult,
    VNextIngestFacade,
    VNextIngestSession,
    VNextIssuedPublicationStep,
    VNextPreparedPublicationStep,
    VNextResolvedIngestPolicy,
)
from PIL import Image

import h2hdb_ingest.runtime as runtime_module
from h2hdb_ingest import (
    ArtifactRenderPolicyConfig,
    IngestConfig,
    IngestPathsConfig,
    IngestSessionController,
    LibraryMaintenanceOutcome,
    ResidentConfig,
    ResidentIngestor,
    VNextIngestService,
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
        library_storage_identity=None,
        library_maintenance=_DoneLibraryMaintenance(),
        config=config.resident,
        database_type=config.core.database.sql_type,
        artifact_release_adapters={},
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
    assert initialized.schema_version == checked.schema_version == 3
    assert processed
    assert revision.revision == 1
    assert revision.publication_count == 1
    searchable = runtime.catalog.discover_publications(
        revision=revision,
        query=CatalogDiscoveryQuery(
            title="Runtime integration",
            gid=1001,
            subjects=(
                CatalogSubjectFilter(namespace="artist", value="first"),
                CatalogSubjectFilter(namespace="language", value="english"),
            ),
            uploaded=CatalogTimestampRange(
                start=datetime(2024, 1, 2, tzinfo=UTC),
                end=datetime(2024, 1, 3, tzinfo=UTC),
            ),
            downloaded=CatalogTimestampRange(
                start=datetime(2024, 2, 3, tzinfo=UTC),
                end=datetime(2024, 2, 4, tzinfo=UTC),
            ),
        ),
    )
    assert [publication.gid for publication in searchable.publications] == [1001]
    assert (
        runtime.catalog.discover_publications(
            revision=revision,
            query=CatalogDiscoveryQuery(title="uploader"),
        ).publications
        == ()
    )

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
    runtime.resident.initialize()
    assert runtime.resident.process_available(periodic_scan=True)
    page = runtime.catalog.discover_publications()
    current = tuple(current_root.rglob("*.cbz"))

    assert page.total == 1
    assert len(page.publications) == 1
    publication = page.publications[0]
    assert len(publication.artifacts) == 1
    assert publication.page_count == 1
    assert runtime.catalog.discover_publications(
        query=CatalogDiscoveryQuery(
            gid=2001, pages=CatalogPageCountRange(minimum=1, maximum=1)
        )
    ).publications == (publication,)
    assert (
        runtime.catalog.discover_publications(
            query=CatalogDiscoveryQuery(
                gid=2001, pages=CatalogPageCountRange(maximum=0)
            )
        ).publications
        == ()
    )
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


def test_restart_recovers_durable_publication_before_applying_new_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "download"
    _gallery(source, 2101, "artist")
    image = Image.new("RGB", (24, 24))
    image.putdata(
        [
            ((index * 17) % 256, (index * 29) % 256, (index * 43) % 256)
            for index in range(24 * 24)
        ]
    )
    image.save(source / "2101" / "001.png")
    (source / "2101" / "001.jpg").unlink()
    library_root = tmp_path / "library"
    _provision_library_root(library_root)
    database = str(tmp_path / "catalog.sqlite3")

    def config(*, page_jpeg_quality: int) -> IngestConfig:
        return IngestConfig(
            core=CoreConfig(
                database=DatabaseConfig(sql_type="sqlite", database=database)
            ),
            paths=IngestPathsConfig(
                download_path=source,
                library_path=library_root,
                page_render_workers=1,
                render_policy=ArtifactRenderPolicyConfig(
                    page_jpeg_quality=page_jpeg_quality,
                ),
            ),
            resident=ResidentConfig(
                lease_seconds=30,
                heartbeat_seconds=5,
            ),
        )

    first = build_runtime(config(page_jpeg_quality=90))
    first.database_admin.initialize()
    first.resident.initialize()
    claimed = first.facade.try_claim_ingest(True, 30_000_000)
    assert claimed is not None
    session = IngestSessionController(
        first.facade,
        claimed,
        lease_duration_microseconds=30_000_000,
        database_type="sqlite",
    )
    service = cast(VNextIngestService, first.resident._service)
    activation = service._library_activation
    original_begin = activation.begin
    crashed = False

    def fail_first_activation(
        revision: int,
        receipt_id: bytes,
    ) -> object:
        nonlocal crashed
        if not crashed:
            crashed = True
            raise OSError("injected crash after durable database commit")
        return original_begin(revision, receipt_id)

    monkeypatch.setattr(activation, "begin", fail_first_activation)
    with pytest.raises(OSError, match="after durable database commit"):
        service.synchronize_once(session)
    assert crashed
    staged_old_archives = tuple(
        (library_root / ".h2hdb-state" / "staging").glob("*.cbz")
    )
    assert len(staged_old_archives) == 1
    with ZipFile(staged_old_archives[0]) as archive:
        old_policy_page = archive.read("pages/0000.jpg")
    session.complete()
    first.close()

    # The old snapshot stays unchanged.  Changing only the byte-affecting
    # policy must still produce a successor after the pending old-policy
    # commit has been activated and finalized in this same synchronization.
    requested_config = config(page_jpeg_quality=55)
    restarted = build_runtime(requested_config)
    restarted.resident.initialize()
    assert restarted.resident.process_available(periodic_scan=True)

    revision = restarted.catalog.get_catalog_revision()
    page = restarted.catalog.discover_publications()
    assert revision.revision == 2
    assert page.revision == revision
    assert page.total == len(page.publications) == 1
    publication = page.publications[0]
    assert publication.gid == 2101
    assert len(publication.artifacts) == 1
    current_path = library_root.joinpath(
        "current",
        *publication.artifacts[0].storage_object.key.segments,
    )
    current_bytes = current_path.read_bytes()
    assert sha256(current_bytes).hexdigest() == (
        publication.artifacts[0].storage_object.sha256
    )
    with ZipFile(BytesIO(current_bytes)) as archive:
        current_page = archive.read("pages/0000.jpg")
    expected_page = BytesIO()
    with Image.open(source / "2101" / "001.png") as source_page:
        source_page.save(
            expected_page,
            format="JPEG",
            quality=55,
            optimize=True,
            progressive=False,
        )
    assert current_page == expected_page.getvalue()
    assert current_page != old_policy_page
    assert not (library_root / ".h2hdb-coordination" / "ACTIVATING").exists()
    restarted.close()


class _SimulatedProcessLoss(RuntimeError):
    pass


def test_policy_takeover_releases_only_abandoned_staging_and_keeps_current(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "download"
    _gallery(source, 1901, "artist")
    Image.new("RGB", (8, 12), "red").save(source / "1901" / "001.jpg")
    library_root = tmp_path / "library"
    _provision_library_root(library_root)
    current_root = library_root / "current"
    staging = library_root / ".h2hdb-state" / "staging"
    core = CoreConfig(
        database=DatabaseConfig(
            sql_type="sqlite",
            database=str(tmp_path / "catalog.sqlite3"),
        )
    )
    clock_offset = [0]

    def build_facade(config: CoreConfig) -> VNextIngestFacade:
        return VNextIngestFacade(
            config,
            clock=lambda: time_ns() // 1_000 + clock_offset[0],
        )

    monkeypatch.setattr(runtime_module, "VNextIngestFacade", build_facade)
    base_config = IngestConfig(
        core=core,
        paths=IngestPathsConfig(
            download_path=source,
            library_path=library_root,
        ),
        resident=ResidentConfig(lease_seconds=30, heartbeat_seconds=5),
    )

    def current_files() -> dict[Path, bytes]:
        return {
            path.relative_to(current_root): path.read_bytes()
            for path in current_root.rglob("*")
            if path.is_file()
        }

    initial = build_runtime(base_config)
    try:
        initial.database_admin.initialize()
        initial.resident.initialize()
        for _attempt in range(16):
            assert initial.resident.process_available(periodic_scan=True)
            try:
                initial_revision = initial.catalog.get_catalog_revision()
            except CatalogRevisionNotFoundError:
                continue
            if initial_revision.publication_count == 1:
                break
        else:
            raise AssertionError("initial current publication did not converge")
        initial_current = current_files()
        assert initial_current
    finally:
        initial.close()

    Image.new("RGB", (8, 12), "blue").save(source / "1901" / "001.jpg")
    abandoned_config = base_config.model_copy(
        update={
            "paths": base_config.paths.model_copy(
                update={
                    "render_policy": ArtifactRenderPolicyConfig(page_jpeg_quality=85)
                }
            )
        }
    )
    abandoned = build_runtime(abandoned_config)
    abandoned.resident.initialize()
    original_issue = VNextIngestFacade.issue_publication_step
    original_commit = VNextIngestFacade.commit_publication_step
    issued_operation = [""]
    protection_committed = [False]

    def record_issued_operation(
        facade: VNextIngestFacade,
        session: VNextIngestSession,
        policy: VNextResolvedIngestPolicy,
    ) -> VNextIssuedPublicationStep:
        if protection_committed[0]:
            raise _SimulatedProcessLoss
        issued = original_issue(facade, session, policy)
        issued_operation[0] = issued.operation
        return issued

    def record_protection_commit(
        facade: VNextIngestFacade,
        session: VNextIngestSession,
        prepared: VNextPreparedPublicationStep,
    ) -> VNextIngestAdvanceResult:
        result = original_commit(facade, session, prepared)
        if issued_operation[0] == "PREPARE_ARTIFACT" and any(
            path.is_file() for path in staging.rglob("*")
        ):
            protection_committed[0] = True
        return result

    try:
        with (
            patch.object(
                VNextIngestFacade,
                "issue_publication_step",
                record_issued_operation,
            ),
            patch.object(
                VNextIngestFacade,
                "commit_publication_step",
                record_protection_commit,
            ),
            pytest.raises(_SimulatedProcessLoss),
        ):
            for _attempt in range(16):
                abandoned.resident.process_available(periodic_scan=True)
    finally:
        abandoned.close()
    assert any(path.is_file() for path in staging.rglob("*"))
    assert current_files() == initial_current

    clock_offset[0] = 100_000_000
    successor_config = abandoned_config.model_copy(
        update={
            "paths": abandoned_config.paths.model_copy(
                update={
                    "render_policy": ArtifactRenderPolicyConfig(page_jpeg_quality=75)
                }
            )
        }
    )
    restarted = build_runtime(successor_config)
    try:
        restarted.resident.initialize()

        # Bounded preliminary maintenance may need several polls before the
        # successor encounters the predecessor's slot and releases it under
        # EXCLUSIVE maintenance.  Until that release completes, the already
        # published current must remain byte-exact.
        for _attempt in range(32):
            assert restarted.resident.process_available(periodic_scan=True)
            assert restarted.catalog.get_catalog_revision() == initial_revision
            assert current_files() == initial_current
            if not any(path.is_file() for path in staging.rglob("*")):
                break
        else:
            raise AssertionError("abandoned artifact staging was not released")

        for _attempt in range(32):
            assert restarted.resident.process_available(periodic_scan=True)
            revision = restarted.catalog.get_catalog_revision()
            if revision.revision > initial_revision.revision:
                break
        else:
            raise AssertionError("policy takeover did not publish after orphan cleanup")
        current_before_cleanup = current_files()
        assert current_before_cleanup
        assert current_before_cleanup != initial_current

        for _attempt in range(32):
            if not restarted.resident.process_available(periodic_scan=False):
                break
        else:
            raise AssertionError("resident maintenance did not converge")

        assert not any(path.is_file() for path in staging.rglob("*"))
        assert current_files() == current_before_cleanup
        assert restarted.catalog.get_catalog_revision().publication_count == 1
        assert restarted.database_admin.check().state == "READY"
    finally:
        restarted.close()


def test_deleted_gallery_reconciles_catalog_library_and_historical_cleanup(
    tmp_path: Path,
    runtime_core_config: CoreConfig,
) -> None:
    source = tmp_path / "download"
    _gallery(source, 2501, "artist")
    Image.new("RGB", (8, 12), "red").save(source / "2501" / "001.jpg")
    library_root = tmp_path / "library"
    _provision_library_root(library_root)
    current_root = library_root / "current"
    config = IngestConfig(
        core=runtime_core_config,
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
    runtime.resident.initialize()

    assert runtime.resident.process_available(periodic_scan=True)
    first_revision = runtime.catalog.get_catalog_revision()
    first_page = runtime.catalog.discover_publications()
    assert first_page.total == len(first_page.publications) == 1
    publication = first_page.publications[0]
    archive_path = current_root.joinpath(
        *publication.artifacts[0].storage_object.key.segments
    )
    assert publication.thumbnail is not None
    thumbnail_path = current_root.joinpath(
        *publication.thumbnail.storage_object.key.segments
    )
    assert archive_path.is_file()
    assert thumbnail_path.is_file()

    rmtree(source / "2501")
    assert runtime.resident.process_available(periodic_scan=True)
    empty_revision = runtime.catalog.get_catalog_revision()
    empty_page = runtime.catalog.discover_publications()
    assert empty_revision.revision == first_revision.revision + 1
    assert empty_revision.publication_count == 0
    assert empty_page.total == len(empty_page.publications) == 0
    assert not archive_path.exists()
    assert not thumbnail_path.exists()
    assert not tuple(current_root.rglob("*.cbz"))

    outcome = runtime.facade.drain_current_only_maintenance(30_000_000)
    for _attempt in range(16):
        if outcome is VNextCurrentOnlyMaintenanceOutcome.DONE:
            break
        assert outcome is VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
        outcome = runtime.facade.drain_current_only_maintenance(30_000_000)
    assert outcome is VNextCurrentOnlyMaintenanceOutcome.DONE
    assert runtime.database_admin.check().state == "READY"
    with pytest.raises(CatalogRevisionNotFoundError):
        runtime.catalog.discover_publications(
            revision=first_revision,
            limit=128,
        )


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
    runtime.resident.initialize()

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
