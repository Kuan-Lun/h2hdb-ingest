from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import pytest
from h2hdb import (
    H2HDB,
    CatalogBuild,
    CatalogBuildCoordinator,
    CatalogBuildPhase,
    CatalogBuildProjection,
    CatalogProjectionPublicationState,
    CatalogProjectionSelectionCursor,
    CoreConfig,
    DatabaseConfig,
    GalleryIngestTurn,
)
from PIL import Image

from h2hdb_ingest.cbz import CBZReconciler, CBZSourceChangedError
from h2hdb_ingest.models import (
    CBZArtifact,
    CBZPreparationSummary,
    CBZStreamingPreparationRequest,
)
from h2hdb_ingest.naming import gallery_name_to_cbz_file_name
from h2hdb_ingest.scanner import FilesystemScanner, GalleryScanError
from h2hdb_ingest.staged_deduplication import StagedDeduplicationPlanner
from h2hdb_ingest.staged_service import (
    CBZPublicationCoordinator,
    DatabaseGate,
    StagedCatalogCoordinator,
    StagedIngestService,
)
from h2hdb_ingest.staging import (
    CatalogScopeMismatchError,
    CoreFileHashCache,
    FilesystemSourceStager,
)


class _SimulatedProcessCrash(BaseException):
    pass


def _digest(value: str) -> str:
    return sha256(value.encode()).hexdigest()


def _write_gallery(root: Path, name: str, *, title: str, payload: bytes) -> None:
    folder = root / name
    folder.mkdir(parents=True)
    (folder / "galleryinfo.txt").write_text(
        "\n".join(
            (
                f"Title: {title}",
                "Upload Time: 2024-01-02 03:04",
                "Uploaded By: uploader",
                "Downloaded: 2024-01-03 04:05",
                "Tags: artist:tester, language:english",
                "Uploader's Comments:",
                "Summary",
                "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3",
            )
        ),
        encoding="utf-8",
    )
    (folder / "001.bin").write_bytes(payload)


class _RecordingGate:
    def __init__(self, database: H2HDB, events: list[str]) -> None:
        self._database = database
        self._events = events

    @contextmanager
    def database_gate(
        self,
        *,
        timeout_seconds: int | None = None,
    ) -> Iterator[None]:
        self._events.append("database-enter")
        with self._database.database_gate(timeout_seconds=timeout_seconds):
            yield
        self._events.append("database-exit")


class _RecordingCBZ:
    def __init__(
        self,
        artifact_root: Path,
        events: list[str],
        *,
        crash_during_finalize_once: bool = False,
        source_change_on_prepare_call: int | None = None,
    ) -> None:
        self._artifact_root = artifact_root
        self._events = events
        self._crash_during_finalize_once = crash_during_finalize_once
        self._source_change_on_prepare_call = source_change_on_prepare_call
        self._prepare_calls = 0
        self.protected: set[str] = set()
        self.released: list[str] = []
        self.finalized: list[tuple[CBZArtifact, ...]] = []

    @contextmanager
    def publication_guard(self) -> Iterator[None]:
        self._events.append("artifact-enter")
        try:
            yield
        finally:
            self._events.append("artifact-exit")

    def prepare_paged_stream(
        self,
        requests: Iterable[CBZStreamingPreparationRequest],
        *,
        result_sink: Callable[[CBZArtifact], None] | None = None,
        total: int | None = None,
    ) -> CBZPreparationSummary:
        self._artifact_root.mkdir(parents=True, exist_ok=True)
        self._prepare_calls += 1
        source_change = self._prepare_calls == self._source_change_on_prepare_call
        prepared = created = 0
        for request in requests:
            for source_file in request.open_files():
                assert source_file.file.path.is_file()
            if source_change:
                self._source_change_on_prepare_call = None
                raise CBZSourceChangedError(
                    "staged source changed during CBZ preparation"
                )
            metadata = request.metadata
            digest = _digest(
                f"{metadata.gallery.gid}:{metadata.source_digest}:"
                f"{metadata.content_digest}"
            )
            path = self._artifact_root / f"{digest}.cbz"
            was_created = not path.exists()
            if was_created:
                path.write_bytes(digest.encode())
                created += 1
            artifact = CBZArtifact(
                gallery=metadata.gallery,
                path=path,
                size_bytes=path.stat().st_size,
                sha256=digest,
                modified_at=metadata.gallery.upload_time,
                created=was_created,
                rebuilt=False,
            )
            if result_sink is not None:
                result_sink(artifact)
            prepared += 1
        assert total is None or prepared == total
        return CBZPreparationSummary(prepared, created, 0)

    def protect_for_publish(
        self,
        artifacts: Iterable[CBZArtifact],
        *,
        protection_id: str,
    ) -> None:
        assert tuple(artifacts)
        self.protected.add(protection_id)

    def release_publish_protection(self, protection_id: str) -> None:
        self._events.append("release")
        self.released.append(protection_id)
        self.protected.discard(protection_id)

    def finalize_published(
        self,
        artifacts: Iterable[CBZArtifact],
        *,
        revision: int | None = None,
        protection_id: str,
    ) -> None:
        materialized = tuple(artifacts)
        assert revision is not None
        assert not materialized or protection_id in self.protected
        self._events.append("finalize")
        if self._crash_during_finalize_once:
            self._crash_during_finalize_once = False
            raise _SimulatedProcessCrash
        self.finalized.append(materialized)
        self.protected.discard(protection_id)


class _CrashAfterCatalogCall:
    def __init__(
        self,
        database: H2HDB,
        method_name: str,
        *,
        error_type: type[BaseException] = _SimulatedProcessCrash,
    ) -> None:
        self._database = database
        self._method_name = method_name
        self._error_type = error_type
        self.crashed = False

    def __getattr__(self, name: str) -> Any:
        target = getattr(self._database, name)
        if name != self._method_name:
            return target

        def crash_after(*args: object, **kwargs: object) -> object:
            result = target(*args, **kwargs)
            if not self.crashed:
                self.crashed = True
                raise self._error_type(f"crash after {name}")
            return result

        return crash_after


class _DeletionRaceAfterOperationalPreparation:
    def __init__(self, database: H2HDB) -> None:
        self._database = database
        self.races = 0

    def __getattr__(self, name: str) -> Any:
        target = getattr(self._database, name)
        if name != "prepare_catalog_build_operations":
            return target

        def race_after(*args: object, **kwargs: object) -> object:
            result = target(*args, **kwargs)
            if self.races == 0 and result.complete:
                self.races += 1
                self._database.request_gallery_deletion(999_999)
            return result

        return race_after


class _MutateAfterOperationalPreparation:
    def __init__(self, database: H2HDB, source_file: Path) -> None:
        self._database = database
        self._source_file = source_file
        self.mutated = False

    def __getattr__(self, name: str) -> Any:
        target = getattr(self._database, name)
        if name != "prepare_catalog_build_operations":
            return target

        def mutate_after(*args: object, **kwargs: object) -> object:
            result = target(*args, **kwargs)
            if not self.mutated and result.complete:
                self.mutated = True
                self._source_file.write_bytes(b"changed during operational prepare")
            return result

        return mutate_after


class _RecordingCleanupCatalog:
    def __init__(self, database: H2HDB) -> None:
        self._database = database
        self.max_rows: list[int] = []

    def __getattr__(self, name: str) -> Any:
        target = getattr(self._database, name)
        if name not in {
            "prune_catalog_build_projection",
            "prune_catalog_build",
        }:
            return target

        def record(*args: object, **kwargs: object) -> object:
            self.max_rows.append(cast(int, kwargs["max_rows"]))
            return target(*args, **kwargs)

        return record


def _database(tmp_path: Path) -> H2HDB:
    database = H2HDB(
        CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite",
                database=str(tmp_path / "catalog.sqlite"),
            )
        )
    )
    database.migrate()
    return database


def _claim(database: H2HDB) -> GalleryIngestTurn:
    turn = database.claim_gallery_ingest(lease_seconds=120, periodic_scan=True)
    assert turn is not None
    return turn


def _service(
    *,
    database: H2HDB,
    source_root: Path,
    cbz: CBZPublicationCoordinator | None,
    events: list[str],
    catalog: object | None = None,
    scope_key: str | None = None,
    cleanup_page_size: int = 1_000,
) -> StagedIngestService:
    core = catalog or database
    build_coordinator = cast(CatalogBuildCoordinator, core)
    hash_cache = CoreFileHashCache(build_coordinator, write_batch_size=1)
    source_stager = FilesystemSourceStager(
        scanner=FilesystemScanner(
            source_root,
            hash_workers=1,
            hash_cache=hash_cache,
            max_galleries=1,
            max_files=1,
        ),
        coordinator=build_coordinator,
        hash_cache=hash_cache,
    )
    return StagedIngestService(
        source_stager=source_stager,
        planner=StagedDeduplicationPlanner(page_size=1, write_batch_size=1),
        catalog=cast(StagedCatalogCoordinator, core),
        database_admin=cast(DatabaseGate, _RecordingGate(database, events)),
        catalog_reader=database,
        source_root=source_root,
        scope_key=scope_key or f"filesystem:{source_root.resolve()}",
        cbz=cbz,
        projection_gallery_page_size=1,
        projection_file_page_size=1,
        projection_selection_batch_size=1,
        published_artifact_page_size=1,
        cleanup_page_size=cleanup_page_size,
    )


def test_sqlite_vertical_staged_publication_holds_artifact_guard_before_gate(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "galleries"
    _write_gallery(source_root, "First Gallery [101]", title="First", payload=b"1")
    _write_gallery(source_root, "Second Gallery [102]", title="Second", payload=b"2")
    database = _database(tmp_path)
    turn = _claim(database)
    events: list[str] = []
    cbz = _RecordingCBZ(tmp_path / "artifacts", events)

    outcome = _service(
        database=database,
        source_root=source_root,
        cbz=cbz,
        events=events,
    ).synchronize_once(turn)

    assert (outcome.scanned, outcome.published, outcome.new) == (2, 2, 2)
    assert outcome.cbz_created == 2
    assert not outcome.needs_immediate_rescan
    receipt = database.get_catalog_projection_publication_receipt()
    assert receipt is not None
    assert receipt.state is CatalogProjectionPublicationState.projection_finalized
    assert len(cbz.finalized) == 1
    assert [artifact.gallery.gallery_name for artifact in cbz.finalized[0]] == [
        "First Gallery [101]",
        "Second Gallery [102]",
    ]
    assert events[-7:] == [
        "artifact-enter",
        "database-enter",
        "database-exit",
        "finalize",
        "database-enter",
        "database-exit",
        "artifact-exit",
    ]


def test_sqlite_staged_publication_without_cbz(tmp_path: Path) -> None:
    source_root = tmp_path / "galleries"
    _write_gallery(source_root, "Gallery [201]", title="No CBZ", payload=b"data")
    database = _database(tmp_path)
    turn = _claim(database)

    outcome = _service(
        database=database,
        source_root=source_root,
        cbz=None,
        events=[],
    ).synchronize_once(turn)

    assert (outcome.scanned, outcome.published, outcome.cbz_created) == (1, 1, 0)
    publication = database.list_publications(limit=1).publications[0]
    assert publication.artifacts == ()
    receipt = database.get_catalog_projection_publication_receipt()
    assert receipt is not None
    assert receipt.state is CatalogProjectionPublicationState.projection_finalized


@pytest.mark.parametrize(
    "method_name",
    [
        "complete_catalog_discovery",
        "stage_catalog_file_chunks",
        "complete_catalog_source_staging",
        "complete_catalog_analysis_phase",
        "complete_catalog_analysis",
        "record_catalog_prepared_artifacts",
        "advance_catalog_artifact_checkpoint",
        "complete_catalog_artifact_preparation",
        "stage_catalog_projection_selections",
        "complete_catalog_projection_staging",
        "prepare_catalog_build_operations",
        "seal_catalog_build_projection",
        "seal_catalog_build",
        "publish_catalog_build_with_projection",
    ],
)
def test_sqlite_process_crash_resumes_every_durable_stage(
    tmp_path: Path,
    method_name: str,
) -> None:
    source_root = tmp_path / "galleries"
    _write_gallery(source_root, "Gallery [301]", title="Resume", payload=b"data")
    database = _database(tmp_path)
    first_turn = _claim(database)
    events: list[str] = []
    cbz = _RecordingCBZ(tmp_path / "artifacts", events)
    crashing_catalog = _CrashAfterCatalogCall(database, method_name)

    with pytest.raises(_SimulatedProcessCrash):
        _service(
            database=database,
            source_root=source_root,
            cbz=cbz,
            events=events,
            catalog=crashing_catalog,
        ).synchronize_once(first_turn)
    assert crashing_catalog.crashed

    assert database.complete_gallery_ingest(first_turn)
    second_turn = _claim(database)
    outcome = _service(
        database=database,
        source_root=source_root,
        cbz=cbz,
        events=events,
    ).synchronize_once(second_turn)

    assert outcome.revision == 1
    assert outcome.published == 1
    assert database.get_catalog_source_revision().revision == 1
    receipt = database.get_catalog_projection_publication_receipt()
    assert receipt is not None
    assert receipt.state is CatalogProjectionPublicationState.projection_finalized
    assert not cbz.protected
    assert cbz.released == []


def test_cross_turn_recovery_retries_cbz_finalize_from_published_pages(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "galleries"
    gallery_name = "Original Gallery Name [401]"
    _write_gallery(source_root, gallery_name, title="Recover", payload=b"data")
    database = _database(tmp_path)
    first_turn = _claim(database)
    events: list[str] = []
    cbz = _RecordingCBZ(
        tmp_path / "artifacts",
        events,
        crash_during_finalize_once=True,
    )

    with pytest.raises(_SimulatedProcessCrash):
        _service(
            database=database,
            source_root=source_root,
            cbz=cbz,
            events=events,
        ).synchronize_once(first_turn)
    pending = database.get_catalog_projection_publication_receipt(pending_only=True)
    assert pending is not None
    assert pending.build_id in cbz.protected

    assert database.complete_gallery_ingest(first_turn)
    second_turn = _claim(database)
    outcome = _service(
        database=database,
        source_root=source_root,
        cbz=cbz,
        events=events,
    ).synchronize_once(second_turn)

    assert outcome.revision == pending.catalog_revision.revision
    assert outcome.needs_immediate_rescan
    assert len(cbz.finalized) == 1
    recovered = cbz.finalized[0][0]
    assert recovered.gallery.gallery_name == gallery_name
    assert recovered.gallery.gid == 401
    assert recovered.gallery.upload_time.year == 2024
    receipt = database.get_catalog_projection_publication_receipt(pending_only=True)
    assert receipt is None
    assert not cbz.protected


def test_pending_required_cbz_projection_refuses_recovery_without_cbz(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "galleries"
    _write_gallery(source_root, "Gallery [451]", title="Required", payload=b"data")
    database = _database(tmp_path)
    first_turn = _claim(database)
    events: list[str] = []
    cbz = _RecordingCBZ(tmp_path / "artifacts", events)
    crashing_catalog = _CrashAfterCatalogCall(
        database,
        "publish_catalog_build_with_projection",
    )

    with pytest.raises(_SimulatedProcessCrash):
        _service(
            database=database,
            source_root=source_root,
            cbz=cbz,
            events=events,
            catalog=crashing_catalog,
        ).synchronize_once(first_turn)
    pending = database.get_catalog_projection_publication_receipt(pending_only=True)
    assert pending is not None
    assert pending.build_id in cbz.protected

    assert database.complete_gallery_ingest(first_turn)
    second_turn = _claim(database)
    with pytest.raises(RuntimeError, match="requires CBZ reconciliation"):
        _service(
            database=database,
            source_root=source_root,
            cbz=None,
            events=events,
        ).synchronize_once(second_turn)

    assert database.get_catalog_projection_publication_receipt(pending_only=True)
    assert pending.build_id in cbz.protected


def test_pending_no_cbz_projection_ignores_new_cbz_configuration(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "galleries"
    _write_gallery(source_root, "Gallery [452]", title="No CBZ", payload=b"data")
    database = _database(tmp_path)
    first_turn = _claim(database)
    crashing_catalog = _CrashAfterCatalogCall(
        database,
        "publish_catalog_build_with_projection",
    )

    with pytest.raises(_SimulatedProcessCrash):
        _service(
            database=database,
            source_root=source_root,
            cbz=None,
            events=[],
            catalog=crashing_catalog,
        ).synchronize_once(first_turn)
    assert database.get_catalog_projection_publication_receipt(pending_only=True)

    assert database.complete_gallery_ingest(first_turn)
    second_turn = _claim(database)
    cbz = _RecordingCBZ(tmp_path / "new-artifacts", [])
    outcome = _service(
        database=database,
        source_root=source_root,
        cbz=cbz,
        events=[],
    ).synchronize_once(second_turn)

    assert outcome.needs_immediate_rescan
    assert cbz.finalized == []
    assert (
        database.get_catalog_projection_publication_receipt(pending_only=True) is None
    )


def test_pending_projection_scope_change_fails_closed_before_cbz_finalize(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "galleries"
    _write_gallery(source_root, "Gallery [453]", title="Scoped", payload=b"data")
    database = _database(tmp_path)
    first_turn = _claim(database)
    events: list[str] = []
    cbz = _RecordingCBZ(tmp_path / "artifacts", events)
    crashing_catalog = _CrashAfterCatalogCall(
        database,
        "publish_catalog_build_with_projection",
    )

    with pytest.raises(_SimulatedProcessCrash):
        _service(
            database=database,
            source_root=source_root,
            cbz=cbz,
            events=events,
            catalog=crashing_catalog,
        ).synchronize_once(first_turn)
    pending = database.get_catalog_projection_publication_receipt(pending_only=True)
    assert pending is not None

    assert database.complete_gallery_ingest(first_turn)
    second_turn = _claim(database)
    with pytest.raises(CatalogScopeMismatchError, match="committed catalog projection"):
        _service(
            database=database,
            source_root=source_root,
            cbz=cbz,
            events=events,
            scope_key="different-scope",
        ).synchronize_once(second_turn)

    assert database.get_catalog_projection_publication_receipt(pending_only=True)
    assert cbz.finalized == []
    assert pending.build_id in cbz.protected


def test_confirmed_commit_ambiguity_continues_from_receipt(tmp_path: Path) -> None:
    source_root = tmp_path / "galleries"
    _write_gallery(source_root, "Gallery [501]", title="Ambiguous", payload=b"data")
    database = _database(tmp_path)
    turn = _claim(database)
    events: list[str] = []
    cbz = _RecordingCBZ(tmp_path / "artifacts", events)
    catalog = _CrashAfterCatalogCall(
        database,
        "publish_catalog_build_with_projection",
        error_type=RuntimeError,
    )

    outcome = _service(
        database=database,
        source_root=source_root,
        cbz=cbz,
        events=events,
        catalog=catalog,
    ).synchronize_once(turn)

    assert outcome.revision == 1
    assert catalog.crashed
    assert cbz.released == []
    receipt = database.get_catalog_projection_publication_receipt()
    assert receipt is not None
    assert receipt.state is CatalogProjectionPublicationState.projection_finalized


def test_deletion_generation_race_refreshes_operations_and_publishes(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "galleries"
    _write_gallery(source_root, "Gallery [550]", title="Refresh", payload=b"data")
    database = _database(tmp_path)
    turn = _claim(database)
    racing_catalog = _DeletionRaceAfterOperationalPreparation(database)

    outcome = _service(
        database=database,
        source_root=source_root,
        cbz=None,
        events=[],
        catalog=racing_catalog,
    ).synchronize_once(turn)

    assert racing_catalog.races == 1
    assert outcome.revision == 1
    assert outcome.published == 1


def test_prepublication_response_loss_retains_and_resumes_the_durable_build(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "galleries"
    _write_gallery(source_root, "Gallery [601]", title="Failure", payload=b"data")
    database = _database(tmp_path)
    turn = _claim(database)
    events: list[str] = []
    cbz = _RecordingCBZ(tmp_path / "artifacts", events)
    catalog = _CrashAfterCatalogCall(
        database,
        "advance_catalog_artifact_checkpoint",
        error_type=RuntimeError,
    )

    with pytest.raises(RuntimeError, match="crash after"):
        _service(
            database=database,
            source_root=source_root,
            cbz=cbz,
            events=events,
            catalog=catalog,
        ).synchronize_once(turn)

    working = database.get_working_catalog_build()
    assert working is not None
    assert working.build_id in cbz.protected
    assert cbz.released == []

    outcome = _service(
        database=database,
        source_root=source_root,
        cbz=cbz,
        events=events,
    ).synchronize_once(turn)

    assert outcome.revision == 1
    assert database.get_working_catalog_build() is None
    assert not cbz.protected
    assert cbz.released == []


def test_deterministic_final_validation_failure_abandons_before_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_root = tmp_path / "galleries"
    _write_gallery(source_root, "Gallery [602]", title="Changed", payload=b"old")
    source_file = source_root / "Gallery [602]" / "001.bin"
    database = _database(tmp_path)
    turn = _claim(database)
    events: list[str] = []
    cbz = _RecordingCBZ(tmp_path / "artifacts", events)
    complete_projection = database.complete_catalog_projection_staging

    def complete_then_mutate(
        build: CatalogBuild,
        *,
        expected_after: CatalogProjectionSelectionCursor | None,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildProjection:
        result = complete_projection(
            build,
            expected_after=expected_after,
            ingest_turn=ingest_turn,
        )
        source_file.write_bytes(b"changed after projection staging")
        return result

    monkeypatch.setattr(
        database,
        "complete_catalog_projection_staging",
        complete_then_mutate,
    )

    with pytest.raises(RuntimeError, match="changed after source staging"):
        _service(
            database=database,
            source_root=source_root,
            cbz=cbz,
            events=events,
        ).synchronize_once(turn)

    assert database.get_working_catalog_build() is None
    assert database.get_catalog_projection_publication_receipt() is None
    assert cbz.released
    assert events[-1] == "release"
    assert not cbz.protected


def test_cbz_source_change_abandons_and_rescans_across_turns(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "galleries"
    _write_gallery(source_root, "First Gallery [610]", title="First", payload=b"1")
    _write_gallery(source_root, "Second Gallery [611]", title="Second", payload=b"2")
    database = _database(tmp_path)
    first_turn = _claim(database)
    events: list[str] = []
    cbz = _RecordingCBZ(
        tmp_path / "artifacts",
        events,
        source_change_on_prepare_call=2,
    )

    with pytest.raises(CBZSourceChangedError, match="staged source changed"):
        _service(
            database=database,
            source_root=source_root,
            cbz=cbz,
            events=events,
        ).synchronize_once(first_turn)

    assert len(cbz.released) == 1
    failed_build_id = cbz.released[0]
    assert events[-1] == "release"
    assert failed_build_id not in cbz.protected
    assert database.get_working_catalog_build() is None
    assert database.get_catalog_build(failed_build_id) is None

    assert database.complete_gallery_ingest(first_turn)
    second_turn = _claim(database)
    outcome = _service(
        database=database,
        source_root=source_root,
        cbz=cbz,
        events=events,
    ).synchronize_once(second_turn)

    receipt = database.get_catalog_projection_publication_receipt()
    assert receipt is not None
    assert receipt.build_id != failed_build_id
    assert outcome.revision == receipt.catalog_revision.revision
    assert outcome.published == 2
    assert database.get_working_catalog_build() is None
    assert not cbz.protected
    assert cbz.released == [failed_build_id]


def test_final_validation_detects_mutation_during_operational_preparation(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "galleries"
    _write_gallery(source_root, "Gallery [603]", title="Changed", payload=b"old")
    source_file = source_root / "Gallery [603]" / "001.bin"
    database = _database(tmp_path)
    turn = _claim(database)
    catalog = _MutateAfterOperationalPreparation(database, source_file)

    with pytest.raises(GalleryScanError, match="changed after source staging"):
        _service(
            database=database,
            source_root=source_root,
            cbz=None,
            events=[],
            catalog=catalog,
        ).synchronize_once(turn)

    assert catalog.mutated
    assert database.get_working_catalog_build() is None
    assert database.get_catalog_projection_publication_receipt() is None


def test_next_publication_boundedly_prunes_the_inactive_source_build(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "galleries"
    _write_gallery(source_root, "Gallery [604]", title="Cleanup", payload=b"data")
    database = _database(tmp_path)
    first_turn = _claim(database)
    first = _service(
        database=database,
        source_root=source_root,
        cbz=None,
        events=[],
    ).synchronize_once(first_turn)
    first_receipt = database.get_catalog_projection_publication_receipt()
    assert first_receipt is not None
    first_build_id = first_receipt.build_id
    assert database.complete_gallery_ingest(first_turn)

    second_turn = _claim(database)
    recording = _RecordingCleanupCatalog(database)
    second = _service(
        database=database,
        source_root=source_root,
        cbz=None,
        events=[],
        catalog=recording,
        cleanup_page_size=1,
    ).synchronize_once(second_turn)

    assert second.revision == first.revision
    assert recording.max_rows
    assert set(recording.max_rows) == {1}
    assert database.get_catalog_build(first_build_id) is None
    historical = database.list_publications(
        revision=first_receipt.catalog_revision,
    )
    assert historical.publications[0].gid == 604


def test_startup_releases_abandoned_cbz_protection_before_pruning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_root = tmp_path / "galleries"
    _write_gallery(source_root, "Gallery [605]", title="Abandon", payload=b"old")
    source_file = source_root / "Gallery [605]" / "001.bin"
    database = _database(tmp_path)
    first_turn = _claim(database)
    events: list[str] = []
    cbz = _RecordingCBZ(tmp_path / "artifacts", events)
    complete_projection = database.complete_catalog_projection_staging
    mutated = False

    def complete_then_mutate(
        build: CatalogBuild,
        *,
        expected_after: CatalogProjectionSelectionCursor | None,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildProjection:
        nonlocal mutated
        result = complete_projection(
            build,
            expected_after=expected_after,
            ingest_turn=ingest_turn,
        )
        if not mutated:
            mutated = True
            source_file.write_bytes(b"changed before final validation")
        return result

    monkeypatch.setattr(
        database,
        "complete_catalog_projection_staging",
        complete_then_mutate,
    )
    crashing_catalog = _CrashAfterCatalogCall(database, "abandon_catalog_build")

    with pytest.raises(_SimulatedProcessCrash):
        _service(
            database=database,
            source_root=source_root,
            cbz=cbz,
            events=events,
            catalog=crashing_catalog,
        ).synchronize_once(first_turn)

    assert len(cbz.protected) == 1
    abandoned_build_id = next(iter(cbz.protected))
    abandoned = database.get_catalog_build(abandoned_build_id)
    assert abandoned is not None
    assert abandoned.phase is CatalogBuildPhase.abandoned
    assert database.complete_gallery_ingest(first_turn)

    second_turn = _claim(database)
    outcome = _service(
        database=database,
        source_root=source_root,
        cbz=cbz,
        events=events,
    ).synchronize_once(second_turn)

    assert outcome.published == 1
    assert abandoned_build_id in cbz.released
    assert database.get_catalog_build(abandoned_build_id) is None
    assert abandoned_build_id not in cbz.protected


def test_scope_mismatch_does_not_release_or_prune_abandoned_cbz_build(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_root = tmp_path / "galleries"
    _write_gallery(source_root, "Gallery [606]", title="Scoped", payload=b"old")
    source_file = source_root / "Gallery [606]" / "001.bin"
    database = _database(tmp_path)
    first_turn = _claim(database)
    cbz = _RecordingCBZ(tmp_path / "artifacts", [])
    complete_projection = database.complete_catalog_projection_staging
    mutated = False

    def complete_then_mutate(
        build: CatalogBuild,
        *,
        expected_after: CatalogProjectionSelectionCursor | None,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuildProjection:
        nonlocal mutated
        result = complete_projection(
            build,
            expected_after=expected_after,
            ingest_turn=ingest_turn,
        )
        if not mutated:
            mutated = True
            source_file.write_bytes(b"changed before final validation")
        return result

    monkeypatch.setattr(
        database,
        "complete_catalog_projection_staging",
        complete_then_mutate,
    )
    crashing_catalog = _CrashAfterCatalogCall(database, "abandon_catalog_build")

    with pytest.raises(_SimulatedProcessCrash):
        _service(
            database=database,
            source_root=source_root,
            cbz=cbz,
            events=[],
            catalog=crashing_catalog,
        ).synchronize_once(first_turn)
    abandoned_build_id = next(iter(cbz.protected))
    assert database.complete_gallery_ingest(first_turn)

    second_turn = _claim(database)
    mismatched = _service(
        database=database,
        source_root=source_root,
        cbz=cbz,
        events=[],
        scope_key="different-scope",
    )
    mismatched._cleanup_obsolete_builds(second_turn)

    retained = database.get_catalog_build(abandoned_build_id)
    assert retained is not None
    assert retained.phase is CatalogBuildPhase.abandoned
    assert abandoned_build_id in cbz.protected
    assert cbz.released == []


def test_cbz_enabled_republication_prunes_old_build_but_keeps_historical_artifact(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "galleries"
    gallery_name = "Gallery [607]"
    _write_gallery(source_root, gallery_name, title="Artifacts", payload=b"unused")
    gallery = source_root / gallery_name
    (gallery / "001.bin").unlink()
    image_path = gallery / "001.png"
    Image.new("RGB", (8, 8), (255, 0, 0)).save(image_path)
    database = _database(tmp_path)
    artifact_root = tmp_path / "artifacts"
    current_root = tmp_path / "current"
    cbz = CBZReconciler(
        artifact_store_path=artifact_root,
        cbz_path=current_root,
        max_image_short_side=8,
        workers=1,
    )

    first_turn = _claim(database)
    first = _service(
        database=database,
        source_root=source_root,
        cbz=cbz,
        events=[],
    ).synchronize_once(first_turn)
    first_receipt = database.get_catalog_projection_publication_receipt()
    assert first_receipt is not None
    first_build_id = first_receipt.build_id
    first_revision = database.get_catalog_revision(first.revision)
    first_publication = database.list_publications(
        revision=first_revision,
    ).publications[0]
    old_artifact = first_publication.artifacts[0].location
    assert old_artifact.is_file()
    assert database.complete_gallery_ingest(first_turn)

    Image.new("RGB", (8, 8), (0, 0, 255)).save(image_path)
    second_turn = _claim(database)
    second = _service(
        database=database,
        source_root=source_root,
        cbz=cbz,
        events=[],
    ).synchronize_once(second_turn)

    assert second.revision == first.revision + 1
    current_publication = database.list_publications().publications[0]
    new_artifact = current_publication.artifacts[0].location
    assert new_artifact != old_artifact
    assert new_artifact.is_file()
    assert old_artifact.is_file()
    historical = database.list_publications(
        revision=first_revision,
    ).publications[0]
    assert historical.artifacts[0].location == old_artifact
    current_path = current_root / gallery_name_to_cbz_file_name(gallery_name)
    assert current_path.is_file()
    assert (
        sha256(current_path.read_bytes()).hexdigest()
        == sha256(new_artifact.read_bytes()).hexdigest()
    )
    assert database.get_catalog_build(first_build_id) is None
