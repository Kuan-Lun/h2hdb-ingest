from pathlib import Path
from typing import Any, cast

import pytest
from h2hdb import (
    H2HDB,
    CatalogBuildCoordinator,
    CatalogBuildPhase,
    CatalogBuildStateError,
    CoreConfig,
    DatabaseConfig,
)

from h2hdb_ingest import scanner as scanner_module
from h2hdb_ingest.scanner import FilesystemScanner, GalleryScanError
from h2hdb_ingest.staging import (
    CatalogScopeMismatchError,
    CoreFileHashCache,
    FilesystemSourceStager,
)


def _write_gallery(root: Path, gid: int, *, file_count: int) -> None:
    folder = root / f"Gallery [{gid}]"
    folder.mkdir(parents=True)
    (folder / "galleryinfo.txt").write_text(
        "\n".join(
            (
                f"Title: Gallery {gid}",
                "Upload Time: 2024-01-02 03:04",
                "Uploaded By: uploader",
                "Downloaded: 2024-01-03 04:05",
                "Tags: artist:Example, language:english",
                "Uploader's Comments:",
                "Summary",
                "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3",
            )
        ),
        encoding="utf-8",
    )
    for index in range(file_count):
        (folder / f"{index:03d}.bin").write_bytes(
            f"gallery-{gid}-file-{index}".encode()
        )


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


def _stager(database: H2HDB, root: Path) -> FilesystemSourceStager:
    cache = CoreFileHashCache(database)
    return FilesystemSourceStager(
        scanner=FilesystemScanner(
            root,
            hash_workers=2,
            hash_cache=cache,
            max_galleries=2,
            max_files=3,
        ),
        coordinator=database,
        hash_cache=cache,
    )


class _RecordingCoordinator:
    def __init__(self, database: H2HDB) -> None:
        self._database = database
        self.discovery_batch_sizes: list[int] = []
        self.file_batch_sizes: list[int] = []

    def __getattr__(self, name: str) -> Any:
        target = getattr(self._database, name)
        if name == "discover_catalog_sources":

            def record_discovery(*args: object, **kwargs: object) -> object:
                discoveries = cast(tuple[object, ...], args[1])
                self.discovery_batch_sizes.append(len(discoveries))
                return target(*args, **kwargs)

            return record_discovery
        if name == "stage_catalog_file_chunks":

            def record_files(*args: object, **kwargs: object) -> object:
                chunks = cast(tuple[Any, ...], args[1])
                self.file_batch_sizes.append(sum(len(chunk.files) for chunk in chunks))
                return target(*args, **kwargs)

            return record_files
        return target


def test_source_staging_is_bounded_durable_and_reuses_hashes_after_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "galleries"
    _write_gallery(root, 1, file_count=4)
    _write_gallery(root, 2, file_count=2)
    database = _database(tmp_path)
    turn = database.claim_gallery_ingest(lease_seconds=60, periodic_scan=True)
    assert turn is not None

    first_stager = _stager(database, root)
    first = first_stager.begin_or_resume(
        scope_key="test-scope-v1",
        ingest_turn=turn,
    )
    first = first_stager.stage(first, ingest_turn=turn)

    assert first.phase is CatalogBuildPhase.analyzing
    assert first.discovered_gallery_count == 2
    assert first.staged_gallery_count == 2
    assert first.staged_file_count == 8
    page = database.list_catalog_build_sources(first.build_id, limit=2)
    assert page.total == 2
    assert {gallery.source_file_count for gallery in page.galleries} == {3, 5}
    first_files = database.list_catalog_build_files(
        first.build_id,
        "Gallery [1]",
        limit=3,
    )
    assert len(first_files.files) == 3
    assert first_files.has_more

    database.abandon_catalog_build(first, ingest_turn=turn)

    def unexpected_hash(_path: Path) -> scanner_module._CachedHash:
        raise AssertionError("a restarted scanner must reuse the durable hash cache")

    monkeypatch.setattr(scanner_module, "_hash_file", unexpected_hash)
    restarted_stager = _stager(database, root)
    restarted = restarted_stager.begin_or_resume(
        scope_key="test-scope-v1",
        ingest_turn=turn,
    )
    restarted = restarted_stager.stage(restarted, ingest_turn=turn)

    assert restarted.build_id != first.build_id
    assert restarted.phase is CatalogBuildPhase.analyzing
    assert restarted.staged_file_count == 8


def test_scope_change_fails_closed_without_abandoning_the_working_build(
    tmp_path: Path,
) -> None:
    root = tmp_path / "galleries"
    _write_gallery(root, 1, file_count=1)
    database = _database(tmp_path)
    turn = database.claim_gallery_ingest(lease_seconds=60, periodic_scan=True)
    assert turn is not None
    stager = _stager(database, root)
    previous = stager.begin_or_resume(
        scope_key="old-scope",
        ingest_turn=turn,
    )

    with pytest.raises(CatalogScopeMismatchError, match="different ingest scope"):
        stager.begin_or_resume(
            scope_key="new-scope",
            ingest_turn=turn,
        )

    retained = database.get_catalog_build(previous.build_id)
    assert retained is not None
    assert retained.phase is CatalogBuildPhase.discovering
    assert database.get_working_catalog_build() == retained


def test_scanner_gallery_limit_above_core_page_cap_is_safely_clamped(
    tmp_path: Path,
) -> None:
    root = tmp_path / "galleries"
    _write_gallery(root, 1, file_count=1)
    database = _database(tmp_path)
    turn = database.claim_gallery_ingest(lease_seconds=60, periodic_scan=True)
    assert turn is not None
    cache = CoreFileHashCache(database)
    stager = FilesystemSourceStager(
        scanner=FilesystemScanner(
            root,
            hash_workers=1,
            hash_cache=cache,
            max_galleries=10_000,
        ),
        coordinator=database,
        hash_cache=cache,
    )

    build = stager.begin_or_resume(
        scope_key="large-configured-page",
        ingest_turn=turn,
    )
    build = stager.stage(build, ingest_turn=turn)

    assert build.phase is CatalogBuildPhase.analyzing


def test_stager_enforces_discovery_transaction_cap_independent_of_scanner(
    tmp_path: Path,
) -> None:
    root = tmp_path / "galleries"
    for gid in range(1, 202):
        _write_gallery(root, gid, file_count=0)
    database = _database(tmp_path)
    turn = database.claim_gallery_ingest(lease_seconds=60, periodic_scan=True)
    assert turn is not None
    recording = _RecordingCoordinator(database)
    coordinator = cast(CatalogBuildCoordinator, recording)
    cache = CoreFileHashCache(coordinator)
    stager = FilesystemSourceStager(
        scanner=FilesystemScanner(
            root,
            hash_workers=2,
            hash_cache=cache,
            max_galleries=10_000,
            max_files=100_000,
        ),
        coordinator=coordinator,
        hash_cache=cache,
    )

    build = stager.begin_or_resume(
        scope_key="discovery-hard-cap",
        ingest_turn=turn,
    )
    build = stager.stage(build, ingest_turn=turn)

    assert build.phase is CatalogBuildPhase.analyzing
    assert recording.discovery_batch_sizes == [200, 1]


def test_stager_enforces_file_transaction_cap_independent_of_scanner(
    tmp_path: Path,
) -> None:
    root = tmp_path / "galleries"
    _write_gallery(root, 1, file_count=2_050)
    database = _database(tmp_path)
    turn = database.claim_gallery_ingest(lease_seconds=60, periodic_scan=True)
    assert turn is not None
    recording = _RecordingCoordinator(database)
    coordinator = cast(CatalogBuildCoordinator, recording)
    cache = CoreFileHashCache(coordinator)
    stager = FilesystemSourceStager(
        scanner=FilesystemScanner(
            root,
            hash_workers=4,
            hash_cache=cache,
            max_galleries=10_000,
            max_files=100_000,
        ),
        coordinator=coordinator,
        hash_cache=cache,
    )

    build = stager.begin_or_resume(
        scope_key="file-hard-cap",
        ingest_turn=turn,
    )
    build = stager.stage(build, ingest_turn=turn)

    assert build.phase is CatalogBuildPhase.analyzing
    assert sum(recording.file_batch_sizes) == 2_051
    assert max(recording.file_batch_sizes) <= 2_048


def test_final_validation_detects_gallery_file_mutation(
    tmp_path: Path,
) -> None:
    root = tmp_path / "galleries"
    _write_gallery(root, 1, file_count=2)
    database = _database(tmp_path)
    turn = database.claim_gallery_ingest(lease_seconds=60, periodic_scan=True)
    assert turn is not None
    stager = _stager(database, root)
    build = stager.begin_or_resume(
        scope_key="validation-file-mutation",
        ingest_turn=turn,
    )
    build = stager.stage(build, ingest_turn=turn)

    stager.validate(build)
    (root / "Gallery [1]" / "000.bin").write_bytes(b"changed after staging")

    with pytest.raises(GalleryScanError, match="changed after source staging"):
        stager.validate(build)


def test_final_validation_detects_added_gallery(
    tmp_path: Path,
) -> None:
    root = tmp_path / "galleries"
    _write_gallery(root, 1, file_count=1)
    database = _database(tmp_path)
    turn = database.claim_gallery_ingest(lease_seconds=60, periodic_scan=True)
    assert turn is not None
    stager = _stager(database, root)
    build = stager.begin_or_resume(
        scope_key="validation-tree-change",
        ingest_turn=turn,
    )
    build = stager.stage(build, ingest_turn=turn)

    _write_gallery(root, 2, file_count=1)

    with pytest.raises(GalleryScanError, match="gallery tree changed"):
        stager.validate(build)


def test_source_change_after_partial_batch_abandons_the_working_build(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "galleries"
    _write_gallery(root, 1, file_count=4)
    database = _database(tmp_path)
    turn = database.claim_gallery_ingest(lease_seconds=60, periodic_scan=True)
    assert turn is not None
    stager = _stager(database, root)
    build = stager.begin_or_resume(
        scope_key="partial-source-change",
        ingest_turn=turn,
    )
    original = database.stage_catalog_file_chunks
    first_batch = True

    def stage_then_remove_source(*args: Any, **kwargs: Any) -> object:
        nonlocal first_batch
        result = original(*args, **kwargs)
        if first_batch:
            first_batch = False
            (root / "Gallery [1]" / "003.bin").unlink()
        return result

    monkeypatch.setattr(database, "stage_catalog_file_chunks", stage_then_remove_source)

    with pytest.raises(GalleryScanError):
        stager.stage(build, ingest_turn=turn)

    abandoned = database.get_catalog_build(build.build_id)
    assert abandoned is not None
    assert abandoned.phase is CatalogBuildPhase.abandoned


def test_discovery_conflict_abandons_the_working_build(tmp_path: Path) -> None:
    root = tmp_path / "galleries"
    _write_gallery(root / "first", 1, file_count=1)
    _write_gallery(root / "second", 1, file_count=1)
    database = _database(tmp_path)
    turn = database.claim_gallery_ingest(lease_seconds=60, periodic_scan=True)
    assert turn is not None
    stager = _stager(database, root)
    build = stager.begin_or_resume(
        scope_key="conflicting-discovery",
        ingest_turn=turn,
    )

    with pytest.raises(CatalogBuildStateError):
        stager.stage(build, ingest_turn=turn)

    abandoned = database.get_catalog_build(build.build_id)
    assert abandoned is not None
    assert abandoned.phase is CatalogBuildPhase.abandoned


def test_hash_cache_writes_are_batched_across_small_galleries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "galleries"
    for gid in range(1, 21):
        _write_gallery(root, gid, file_count=1)
    database = _database(tmp_path)
    turn = database.claim_gallery_ingest(lease_seconds=60, periodic_scan=True)
    assert turn is not None
    cache = CoreFileHashCache(database, write_batch_size=1_000)
    stager = FilesystemSourceStager(
        scanner=FilesystemScanner(
            root,
            hash_workers=2,
            hash_cache=cache,
            max_galleries=128,
            max_files=2_048,
        ),
        coordinator=database,
        hash_cache=cache,
    )
    original = database.cache_catalog_file_hashes
    cache_batch_sizes: list[int] = []

    def record_cache_batch(
        build_arg: Any,
        entries: Any,
        **kwargs: Any,
    ) -> object:
        cache_batch_sizes.append(len(entries))
        return original(build_arg, entries, **kwargs)

    monkeypatch.setattr(database, "cache_catalog_file_hashes", record_cache_batch)
    build = stager.begin_or_resume(
        scope_key="batched-hash-cache",
        ingest_turn=turn,
    )

    staged = stager.stage(build, ingest_turn=turn)

    assert staged.phase is CatalogBuildPhase.analyzing
    assert cache_batch_sizes == [40]
