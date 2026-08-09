import json
from collections.abc import Iterable
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from threading import Lock
from time import sleep
from zipfile import ZipFile

import pytest
from h2hdb import (
    H2HDB,
    CatalogPublisher,
    CatalogPublishResult,
    CatalogReader,
    CatalogSnapshot,
    CoreConfig,
    DatabaseConfig,
    GalleryIngestTurn,
)
from PIL import Image

from h2hdb_ingest import (
    CBZGrouping,
    CBZReconciler,
    DeduplicationPolicy,
    FilesystemScanner,
    LegacyIngestService,
    SyncOutcome,
    gallery_name_to_cbz_file_name,
)
from h2hdb_ingest import cbz as cbz_module
from h2hdb_ingest.models import CBZArtifact


def _write_gallery(
    root: Path,
    gid: int,
    *,
    color: tuple[int, int, int],
    folder_name: str | None = None,
    title: str | None = None,
    tags: str = "artist:Example, language:english",
    download_time: str = "2024-01-03 04:05",
) -> Path:
    folder = root / (folder_name or str(gid))
    folder.mkdir(parents=True)
    gallery_title = f"Gallery {gid}" if title is None else title
    (folder / "galleryinfo.txt").write_text(
        "\n".join(
            (
                f"Title: {gallery_title}",
                "Upload Time: 2024-01-02 03:04",
                "Uploaded By: uploader",
                f"Downloaded: {download_time}",
                f"Tags: {tags}",
                "Uploader's Comments:",
                "Summary",
                "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3",
            )
        ),
        encoding="utf-8",
    )
    Image.new("RGB", (32, 24), color).save(folder / "001.png")
    return folder


def _service(
    database: H2HDB,
    galleries: Path,
    cbz_path: Path | None,
    *,
    catalog_reader: CatalogReader | None = None,
    catalog_publisher: CatalogPublisher | None = None,
    max_image_short_side: int = 16,
    cbz_reconciler: CBZReconciler | None = None,
    sort_mode: str = "no",
) -> LegacyIngestService:
    cbz = cbz_reconciler
    if cbz is None and cbz_path is not None:
        cbz = CBZReconciler(
            artifact_store_path=cbz_path.with_name(f"{cbz_path.name}-artifacts"),
            cbz_path=cbz_path,
            max_image_short_side=max_image_short_side,
        )
    return LegacyIngestService(
        scanner=FilesystemScanner(galleries, hash_workers=2),
        deduplication=DeduplicationPolicy(),
        cbz=cbz,
        catalog_reader=catalog_reader or database,
        catalog_publisher=catalog_publisher or database,
        database_admin=database,
        sort_mode=sort_mode,
    )


def _claim_ingest(database: H2HDB) -> GalleryIngestTurn:
    turn = database.claim_gallery_ingest(lease_seconds=30, periodic_scan=True)
    assert turn is not None
    return turn


def _synchronize(database: H2HDB, service: LegacyIngestService) -> SyncOutcome:
    turn = _claim_ingest(database)
    outcome = service.synchronize_once(turn)
    assert database.complete_gallery_ingest(turn)
    return outcome


def test_scanner_hashes_and_parses_gallery(tmp_path: Path) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(galleries, 1, color=(255, 0, 0))

    (gallery,) = FilesystemScanner(galleries, hash_workers=2).scan()

    assert gallery.gid == 1
    assert gallery.title == "Gallery 1"
    assert gallery.language == "english"
    assert gallery.summary == "Summary"
    assert [file.name for file in gallery.files] == ["001.png", "galleryinfo.txt"]
    assert all(len(file.sha256) == 64 for file in gallery.files)
    assert gallery.pages == 1
    assert len(gallery.source_digest) == 64
    assert gallery.content_digest is not None
    assert len(gallery.content_digest) == 64
    image_digest = next(
        bytes.fromhex(file.sha256) for file in gallery.files if file.name == "001.png"
    )
    assert gallery.content_digest == sha256(image_digest).hexdigest()


def test_scanner_reuses_hashes_until_a_source_stat_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    galleries = tmp_path / "galleries"
    folder = _write_gallery(galleries, 1, color=(255, 0, 0))
    scanner = FilesystemScanner(galleries, hash_workers=2)
    from h2hdb_ingest import scanner as scanner_module
    from h2hdb_ingest.scanner import _CachedHash

    original_hash_file = scanner_module._hash_file
    count_lock = Lock()
    hashed_paths: list[Path] = []

    def recording_hash_file(path: Path) -> _CachedHash:
        with count_lock:
            hashed_paths.append(path)
        return original_hash_file(path)

    monkeypatch.setattr(scanner_module, "_hash_file", recording_hash_file)

    scanner.scan()
    assert len(hashed_paths) == 2
    hashed_paths.clear()
    scanner.scan()
    assert hashed_paths == []

    Image.new("RGB", (32, 24), (0, 255, 0)).save(folder / "001.png")
    scanner.scan()
    assert hashed_paths == [folder / "001.png"]


def test_scanner_emits_progress_while_long_hashes_are_still_running(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(galleries, 1, color=(255, 0, 0))
    from h2hdb_ingest import scanner as scanner_module
    from h2hdb_ingest.scanner import _CachedHash

    original_hash_file = scanner_module._hash_file
    messages: list[str] = []

    def slow_hash_file(path: Path) -> _CachedHash:
        sleep(0.04)
        return original_hash_file(path)

    monkeypatch.setattr(scanner_module, "_hash_file", slow_hash_file)
    scanner = FilesystemScanner(
        galleries,
        hash_workers=1,
        event_logger=messages.append,
        progress_interval_seconds=0.01,
    )

    scanner.scan()

    assert any(
        message.startswith("Filesystem hashing in progress:") for message in messages
    )
    assert messages[-1].startswith("Filesystem scan completed:")


def test_friendly_cbz_name_preserves_historical_utf8_truncation() -> None:
    gallery_name = f"prefix-{'漫' * 100}"

    file_name = gallery_name_to_cbz_file_name(gallery_name)

    assert file_name.endswith(".cbz")
    assert len(file_name.encode("utf-8")) <= 255
    assert gallery_name.endswith(file_name.removesuffix(".cbz"))


def test_publication_selection_contains_no_canonical_metadata(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(
        galleries,
        6,
        color=(255, 0, 0),
        folder_name="Friendly Empty Title [6]",
        title="",
    )

    (gallery,) = FilesystemScanner(galleries, hash_workers=1).scan()
    selection = LegacyIngestService._to_publication_selection(gallery, None)

    assert gallery.title == ""
    assert selection.source_gallery_name == "Friendly Empty Title [6]"
    assert selection.artifacts == ()
    assert not hasattr(selection, "title")


def test_deduplication_preserves_live_incumbent(tmp_path: Path) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(galleries, 1, color=(255, 0, 0))
    _write_gallery(galleries, 2, color=(255, 0, 0))
    scanned = FilesystemScanner(galleries, hash_workers=1).scan()
    content_digest = scanned[0].content_digest
    assert content_digest is not None

    plan = DeduplicationPolicy().select(
        scanned,
        incumbent_gallery_name_by_content_sha256={content_digest: "1"},
    )

    assert [gallery.gid for gallery in plan.winners] == [1]
    assert [gallery.gid for gallery in plan.losers] == [2]
    assert plan.duplicate_of_by_gallery_name == (("2", "1"),)


def test_deduplication_preserves_priority_and_duplicate_target(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(
        galleries,
        30,
        color=(255, 0, 0),
        title="A very long but already-uploaded title",
        tags="misc:already uploaded",
        download_time="2025-01-01 00:00",
    )
    _write_gallery(
        galleries,
        31,
        color=(255, 0, 0),
        title="short",
        download_time="2023-01-01 00:00",
    )
    _write_gallery(
        galleries,
        32,
        color=(0, 255, 0),
        title="same",
        download_time="2024-01-01 00:00",
    )
    _write_gallery(
        galleries,
        33,
        color=(0, 255, 0),
        title="same but longer",
        download_time="2023-01-01 00:00",
    )
    scanned = FilesystemScanner(galleries, hash_workers=1).scan()

    plan = DeduplicationPolicy().select(scanned)

    assert [gallery.gid for gallery in plan.winners] == [31, 33]
    assert plan.duplicate_of_by_gallery_name == (("30", "31"), ("32", "33"))


def test_same_gid_different_content_is_canonical_but_not_a_content_duplicate(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(
        galleries,
        34,
        color=(255, 0, 0),
        folder_name="Older Source [34]",
        download_time="2023-01-01 00:00",
    )
    _write_gallery(
        galleries,
        34,
        color=(0, 255, 0),
        folder_name="Newer Source [34]",
        download_time="2025-01-01 00:00",
    )
    scanned = FilesystemScanner(galleries, hash_workers=1).scan()

    plan = DeduplicationPolicy().select(scanned)

    assert len(plan.canonical_galleries) == 2
    assert len(plan.winners) == 1
    assert plan.winners[0].gallery_name == "Newer Source [34]"
    assert len(plan.losers) == 1
    assert plan.duplicate_of_by_gallery_name == ()


def test_same_gid_priority_tie_preserves_exact_source_incumbent(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(
        galleries,
        36,
        color=(255, 0, 0),
        folder_name="A Source [36]",
    )
    _write_gallery(
        galleries,
        36,
        color=(0, 255, 0),
        folder_name="B Source [36]",
    )

    plan = DeduplicationPolicy().select(
        FilesystemScanner(galleries, hash_workers=1).scan(),
        incumbent_gallery_name_by_gid={36: "A Source [36]"},
    )

    assert [gallery.gallery_name for gallery in plan.winners] == ["A Source [36]"]


def test_catalog_source_identity_preserves_same_gid_incumbent_across_scans(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(
        galleries,
        37,
        color=(255, 0, 0),
        folder_name="A Source [37]",
    )
    database = H2HDB(
        CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite",
                database=str(tmp_path / "catalog.sqlite"),
            )
        )
    )
    database.migrate()
    service = _service(database, galleries, None)
    _synchronize(database, service)
    first = database.get_publication("urn:h2h:gallery:37")
    assert first is not None
    assert first.source_gallery_name == "A Source [37]"

    _write_gallery(
        galleries,
        37,
        color=(0, 255, 0),
        folder_name="B Source [37]",
    )
    _synchronize(database, service)

    retained = database.get_publication("urn:h2h:gallery:37")
    assert retained is not None
    assert retained.source_gallery_name == "A Source [37]"


def test_same_gid_tie_is_deterministic_across_service_restart(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(
        galleries,
        38,
        color=(255, 0, 0),
        folder_name="A Source [38]",
    )
    _write_gallery(
        galleries,
        38,
        color=(0, 255, 0),
        folder_name="B Source [38]",
    )
    database = H2HDB(
        CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite",
                database=str(tmp_path / "catalog.sqlite"),
            )
        )
    )
    database.migrate()

    _synchronize(database, _service(database, galleries, None))
    selected = database.get_publication("urn:h2h:gallery:38")
    assert selected is not None
    assert selected.source_gallery_name == "B Source [38]"

    _synchronize(database, _service(database, galleries, None))
    restarted = database.get_publication("urn:h2h:gallery:38")
    assert restarted is not None
    assert restarted.source_gallery_name == "B Source [38]"


def test_same_gid_uses_full_priority_before_download_time(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(
        galleries,
        35,
        color=(255, 0, 0),
        folder_name="Normal Source [35]",
        title="short",
        download_time="2023-01-01 00:00",
    )
    _write_gallery(
        galleries,
        35,
        color=(0, 255, 0),
        folder_name="Uploaded Source [35]",
        title="a substantially longer and newer title",
        tags="misc:already uploaded",
        download_time="2025-01-01 00:00",
    )

    plan = DeduplicationPolicy().select(
        FilesystemScanner(galleries, hash_workers=1).scan()
    )

    assert [gallery.gallery_name for gallery in plan.winners] == ["Normal Source [35]"]
    assert plan.duplicate_of_by_gallery_name == ()


def test_cbz_state_cannot_follow_a_symlink_outside_artifact_store(
    tmp_path: Path,
) -> None:
    output = tmp_path / "cbz"
    output.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    victim = outside / "victim.cbz"
    victim.write_bytes(b"must survive")
    try:
        (output / "escape").symlink_to(outside, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"symlinks are unavailable: {error}")
    (output / ".h2hdb-cbz-state.json").write_text(
        json.dumps(
            {
                "version": cbz_module.STATE_VERSION,
                "current": {},
                "currentRevision": None,
                "owned": ["escape/victim.cbz"],
                "pending": {},
                "pendingRevision": None,
                "published": [],
                "protected": [],
            }
        ),
        encoding="utf-8",
    )
    reconciler = CBZReconciler(
        artifact_store_path=output,
        cbz_path=tmp_path / "komga",
        max_image_short_side=16,
    )

    with pytest.raises(RuntimeError, match="outside artifact store"):
        reconciler.finalize_published(())

    assert victim.read_bytes() == b"must survive"


def test_metadata_only_galleries_remain_independently_eligible(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    for gid in (10, 11):
        folder = _write_gallery(galleries, gid, color=(255, 0, 0))
        (folder / "001.png").unlink()
    scanned = FilesystemScanner(galleries, hash_workers=1).scan()

    plan = DeduplicationPolicy().select(scanned)

    assert [gallery.gid for gallery in plan.winners] == [10, 11]
    assert all(gallery.content_digest is None for gallery in scanned)


def test_cross_artist_spam_file_is_excluded_from_effective_content_and_cbz(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    folders = [
        _write_gallery(
            galleries,
            gid,
            color=(128, 64, 32),
            tags=f"artist:{artist}",
        )
        for gid, artist in (
            (70, "artist-a"),
            (71, "artist-b"),
            (72, "artist-c"),
        )
    ]
    scanner = FilesystemScanner(galleries, hash_workers=1)
    scanned = scanner.scan()
    shared_digest = next(
        source_file.sha256
        for source_file in scanned[0].files
        if source_file.name == "001.png"
    )

    excluded_plan = DeduplicationPolicy().select(scanned)

    assert excluded_plan.excluded_file_sha256s == frozenset({shared_digest})
    assert len(excluded_plan.canonical_galleries) == 3
    assert len(excluded_plan.winners) == 3
    assert excluded_plan.duplicate_of_by_gallery_name == ()
    assert all(
        gallery.content_digest is None for gallery in excluded_plan.canonical_galleries
    )

    reconciler = CBZReconciler(
        artifact_store_path=tmp_path / "artifacts",
        cbz_path=tmp_path / "cbz",
        max_image_short_side=16,
    )
    excluded_artifacts = reconciler.prepare(excluded_plan)
    reconciler.finalize_published(excluded_artifacts)
    for artifact in excluded_artifacts:
        with ZipFile(artifact.path) as archive:
            assert archive.namelist() == ["galleryinfo.txt"]

    galleryinfo = folders[-1] / "galleryinfo.txt"
    galleryinfo.write_text(
        galleryinfo.read_text(encoding="utf-8").replace(
            "artist:artist-c",
            "artist:artist-a",
        ),
        encoding="utf-8",
    )
    recovered_plan = DeduplicationPolicy().select(scanner.scan())

    assert recovered_plan.excluded_file_sha256s == frozenset()
    assert len(recovered_plan.winners) == 1
    assert len(recovered_plan.losers) == 2
    assert len(recovered_plan.duplicate_of_by_gallery_name) == 2
    (recovered_artifact,) = reconciler.prepare(recovered_plan)
    with ZipFile(recovered_artifact.path) as archive:
        assert set(archive.namelist()) == {"001.jpg", "galleryinfo.txt"}


def test_cbz_reconciliation_creates_preserves_and_rebuilds(tmp_path: Path) -> None:
    galleries = tmp_path / "galleries"
    folder = _write_gallery(galleries, 1, color=(255, 0, 0))
    (folder / "notes.bin").write_bytes(b"canonical source attachment")
    scanner = FilesystemScanner(galleries, hash_workers=1)
    policy = DeduplicationPolicy()
    reconciler = CBZReconciler(
        artifact_store_path=tmp_path / "artifacts",
        cbz_path=tmp_path / "cbz",
        max_image_short_side=16,
    )

    first = reconciler.prepare(policy.select(scanner.scan()))
    reconciler.finalize_published(first)
    second = reconciler.prepare(policy.select(scanner.scan()))
    reconciler.finalize_published(second)
    Image.new("RGB", (32, 24), (0, 255, 0)).save(folder / "001.png")
    third = reconciler.prepare(policy.select(scanner.scan()))

    assert first[0].created and not first[0].rebuilt
    assert not second[0].created and not second[0].rebuilt
    assert third[0].rebuilt and not third[0].created
    assert third[0].path != first[0].path
    assert first[0].path.is_file()
    reconciler.finalize_published(third)
    assert first[0].path.exists()
    with ZipFile(third[0].path) as archive:
        assert archive.comment
        assert set(archive.namelist()) == {
            "001.jpg",
            "galleryinfo.txt",
            "notes.bin",
        }
        assert archive.read("notes.bin") == b"canonical source attachment"

    staged = CBZReconciler(
        artifact_store_path=tmp_path / "artifacts",
        cbz_path=tmp_path / "cbz",
        max_image_short_side=8,
    ).prepare(policy.select(scanner.scan()))
    fourth_reconciler = CBZReconciler(
        artifact_store_path=tmp_path / "artifacts",
        cbz_path=tmp_path / "cbz",
        max_image_short_side=4,
    )
    fourth = fourth_reconciler.prepare(policy.select(scanner.scan()))
    fourth_reconciler.finalize_published(fourth)

    assert not staged[0].path.exists()
    assert first[0].path.exists()
    assert third[0].path.exists()
    assert fourth[0].path.exists()


@pytest.mark.parametrize(
    ("source_size", "expected_size"),
    [
        ((100, 400), (50, 200)),
        ((400, 100), (200, 50)),
        ((40, 100), (40, 100)),
    ],
)
def test_cbz_webtoon_short_side_resize_preserves_long_strips_without_upscale(
    tmp_path: Path,
    source_size: tuple[int, int],
    expected_size: tuple[int, int],
) -> None:
    galleries = tmp_path / "galleries"
    folder = _write_gallery(galleries, 71, color=(255, 0, 0))
    Image.new("RGB", source_size, (10, 20, 30)).save(folder / "001.png")
    scanner = FilesystemScanner(galleries, hash_workers=1)
    reconciler = CBZReconciler(
        artifact_store_path=tmp_path / "artifacts",
        cbz_path=tmp_path / "cbz",
        max_image_short_side=50,
        workers=1,
    )

    (artifact,) = reconciler.prepare(DeduplicationPolicy().select(scanner.scan()))

    with ZipFile(artifact.path) as archive:
        manifest = json.loads(archive.comment.decode("utf-8"))
        with Image.open(BytesIO(archive.read("001.jpg"))) as image:
            assert image.size == expected_size
    assert manifest["version"] == cbz_module.CBZ_MANIFEST_VERSION
    assert manifest["resizePolicy"] == "webtoon-short-side-no-upscale-v1"
    assert manifest["maxImageShortSide"] == 50
    assert "files" not in manifest


def test_cbz_rejects_a_source_file_changed_after_staging(tmp_path: Path) -> None:
    galleries = tmp_path / "galleries"
    folder = _write_gallery(galleries, 72, color=(255, 0, 0))
    scanner = FilesystemScanner(galleries, hash_workers=1)
    plan = DeduplicationPolicy().select(scanner.scan())
    (folder / "001.png").write_bytes(b"changed-after-staging")
    reconciler = CBZReconciler(
        artifact_store_path=tmp_path / "artifacts",
        cbz_path=tmp_path / "cbz",
        max_image_short_side=50,
        workers=1,
    )

    with pytest.raises(RuntimeError, match="changed before CBZ read"):
        reconciler.prepare(plan)


def test_cbz_normalized_member_names_are_unique_and_bytes_are_reproducible(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    folder = _write_gallery(galleries, 2, color=(255, 0, 0))
    Image.new("RGB", (32, 24), (0, 255, 0)).save(folder / "001.jpg")
    scanner = FilesystemScanner(galleries, hash_workers=1)
    policy = DeduplicationPolicy()
    reconciler = CBZReconciler(
        artifact_store_path=tmp_path / "artifacts",
        cbz_path=tmp_path / "cbz",
        max_image_short_side=16,
    )

    first = reconciler.prepare(policy.select(scanner.scan()))
    reconciler.finalize_published(first)
    (tmp_path / "artifacts" / cbz_module.STATE_DATABASE_FILE_NAME).unlink()
    (tmp_path / "artifacts" / cbz_module.STATE_DATABASE_MARKER_FILE_NAME).unlink()
    second = CBZReconciler(
        artifact_store_path=tmp_path / "artifacts",
        cbz_path=tmp_path / "cbz",
        max_image_short_side=16,
    ).prepare(policy.select(scanner.scan()))

    assert first[0].sha256 == second[0].sha256
    assert first[0].path == second[0].path
    with ZipFile(second[0].path) as archive:
        image_members = [
            name for name in archive.namelist() if name != "galleryinfo.txt"
        ]
        assert len(image_members) == 2
        assert len({name.casefold() for name in image_members}) == 2


def test_cbz_grouping_changes_storage_location_but_not_friendly_name(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(
        galleries,
        12,
        color=(255, 0, 0),
        folder_name="Friendly Gallery [12]",
    )
    (gallery,) = FilesystemScanner(galleries, hash_workers=1).scan()
    plan = DeduplicationPolicy().select((gallery,))
    reconciler = CBZReconciler(
        artifact_store_path=tmp_path / "artifacts",
        cbz_path=tmp_path / "cbz",
        max_image_short_side=16,
        grouping=CBZGrouping.date_yyyy_mm_dd,
    )

    (artifact,) = reconciler.prepare(plan)
    reconciler.protect_for_publish((artifact,))
    reconciler.finalize_published((artifact,))
    selection = LegacyIngestService._to_publication_selection(gallery, artifact)

    assert artifact.path.parent.relative_to(tmp_path / "artifacts").parts == (
        "2024",
        "01",
        "02",
    )
    assert selection.artifacts[0].name == "Friendly Gallery [12].cbz"
    assert selection.artifacts[0].location == artifact.path
    friendly = tmp_path / "cbz" / "2024" / "01" / "02" / "Friendly Gallery [12].cbz"
    assert friendly.is_file()
    assert friendly.read_bytes() == artifact.path.read_bytes()


def test_full_sqlite_vertical_slice_publishes_only_after_cbz_success(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    folder = _write_gallery(
        galleries,
        1,
        color=(255, 0, 0),
        folder_name="Friendly Gallery [1]",
    )
    database = H2HDB(
        CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite",
                database=str(tmp_path / "catalog.sqlite"),
            )
        )
    )
    database.migrate()
    service = _service(database, galleries, tmp_path / "cbz")

    first = _synchronize(database, service)
    unchanged = _synchronize(database, service)

    assert first.revision == 1
    assert first.new == 1
    assert first.cbz_created == 1
    assert unchanged.revision == 1
    assert unchanged.changed == 0
    publication = database.get_publication("urn:h2h:gallery:1")
    assert publication is not None
    artifact = publication.artifacts[0]
    assert artifact.name == "Friendly Gallery [1].cbz"
    assert artifact.location.is_file()
    assert artifact.location.name != artifact.name
    assert artifact.location.name.startswith(f"1-{artifact.sha256}")

    Image.new("RGB", (32, 24), (0, 255, 0)).save(folder / "001.png")
    changed = _synchronize(database, service)
    assert changed.revision == 2
    assert changed.changed == 1
    assert changed.cbz_rebuilt == 1

    (folder / "001.png").write_bytes(b"not an image")
    turn = _claim_ingest(database)
    with pytest.raises(Exception):
        service.synchronize_once(turn)
    assert database.get_catalog_revision().revision == 2


def test_cbz_disabled_still_publishes_metadata_without_artifact(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(galleries, 2, color=(0, 0, 255))
    database = H2HDB(
        CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite",
                database=str(tmp_path / "catalog.sqlite"),
            )
        )
    )
    database.migrate()

    outcome = _synchronize(database, _service(database, galleries, None))

    publication = database.get_publication("urn:h2h:gallery:2")
    assert outcome.cbz_created == 0
    assert outcome.cbz_rebuilt == 0
    assert publication is not None
    assert publication.artifacts == ()
    assert not (tmp_path / "cbz").exists()


class _FailingCatalogPublisher:
    def publish_snapshot(
        self,
        snapshot: CatalogSnapshot,
        *,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogPublishResult:
        del snapshot, ingest_turn
        raise RuntimeError("injected publish failure")


class _RecordingCatalogPublisher:
    def __init__(self, delegate: H2HDB) -> None:
        self._delegate = delegate
        self.publication_orders: list[list[int]] = []
        self.snapshots: list[CatalogSnapshot] = []

    def publish_snapshot(
        self,
        snapshot: CatalogSnapshot,
        *,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogPublishResult:
        self.snapshots.append(snapshot)
        gallery_by_name = {
            gallery.gallery_name: gallery for gallery in snapshot.galleries
        }
        self.publication_orders.append(
            [
                gallery_by_name[selection.source_gallery_name].gid
                for selection in snapshot.selections
            ]
        )
        return self._delegate.publish_snapshot(
            snapshot,
            ingest_turn=ingest_turn,
        )


class _CrashAfterPublishCBZ(CBZReconciler):
    def finalize_published(
        self,
        artifacts: Iterable[CBZArtifact],
        *,
        revision: int | None = None,
        protection_id: str = cbz_module.LEGACY_PROTECTION_ID,
    ) -> None:
        del artifacts, revision, protection_id
        raise RuntimeError("injected post-commit crash")


def test_upload_time_sort_controls_processing_and_publication_order(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    for gid, upload_time in (
        (20, "2024-01-01 00:00"),
        (21, "2024-06-01 00:00"),
        (22, "2024-03-01 00:00"),
    ):
        folder = _write_gallery(galleries, gid, color=(gid, 0, 0))
        galleryinfo = folder / "galleryinfo.txt"
        galleryinfo.write_text(
            galleryinfo.read_text(encoding="utf-8").replace(
                "2024-01-02 03:04",
                upload_time,
            ),
            encoding="utf-8",
        )
    database = H2HDB(
        CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite",
                database=str(tmp_path / "catalog.sqlite"),
            )
        )
    )
    database.migrate()
    publisher = _RecordingCatalogPublisher(database)
    service = _service(
        database,
        galleries,
        None,
        catalog_publisher=publisher,
        sort_mode="upload_time",
    )

    _synchronize(database, service)

    assert publisher.publication_orders == [[21, 22, 20]]


@pytest.mark.parametrize(
    ("sort_mode", "expected_order"),
    [
        ("gid", [22, 21, 20]),
        ("title", [22, 20, 21]),
    ],
)
def test_gid_and_title_sort_modes_are_descending_and_stable(
    tmp_path: Path,
    sort_mode: str,
    expected_order: list[int],
) -> None:
    galleries = tmp_path / "galleries"
    for gid, title in ((20, "Bravo"), (21, "Alpha"), (22, "Charlie")):
        _write_gallery(
            galleries,
            gid,
            color=(gid, 0, 0),
            title=title,
        )
    database = H2HDB(
        CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite",
                database=str(tmp_path / "catalog.sqlite"),
            )
        )
    )
    database.migrate()
    publisher = _RecordingCatalogPublisher(database)

    _synchronize(
        database,
        _service(
            database,
            galleries,
            None,
            catalog_publisher=publisher,
            sort_mode=sort_mode,
        ),
    )

    assert publisher.publication_orders == [expected_order]


def test_publish_snapshot_preserves_canonical_source_metadata(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    folder = _write_gallery(
        galleries,
        23,
        color=(23, 0, 0),
        folder_name="Canonical Source [23]",
        title="",
        tags="artist:Example, misc:",
    )
    (folder / "source.bin").write_bytes(b"source attachment")
    database = H2HDB(
        CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite",
                database=str(tmp_path / "catalog.sqlite"),
            )
        )
    )
    database.migrate()
    publisher = _RecordingCatalogPublisher(database)

    outcome = _synchronize(
        database,
        _service(
            database,
            galleries,
            None,
            catalog_publisher=publisher,
        ),
    )

    assert outcome.new == 1
    assert len(publisher.snapshots) == 1
    (source,) = publisher.snapshots[0].galleries
    assert source.gallery_name == "Canonical Source [23]"
    assert source.gid == 23
    assert source.title == ""
    assert source.comment == "Summary"
    assert source.upload_account == "uploader"
    assert [(tag.name, tag.value) for tag in source.tags] == [
        ("artist", "Example"),
        ("misc", ""),
    ]
    assert [file.name for file in source.files] == [
        "001.png",
        "galleryinfo.txt",
        "source.bin",
    ]
    assert source.content_sha256 is not None
    assert source.source_manifest_sha256
    (selection,) = publisher.snapshots[0].selections
    assert selection.source_gallery_name == "Canonical Source [23]"
    publication = database.get_publication("urn:h2h:gallery:23")
    assert publication is not None
    assert publication.title == "Canonical Source [23]"
    assert publication.source_title == ""
    assert [(subject.code, subject.name) for subject in publication.subjects] == [
        ("artist", "Example"),
        ("misc", ""),
    ]


def test_failed_publish_preserves_active_cbz_when_build_config_changes(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(galleries, 3, color=(255, 0, 0))
    cbz_path = tmp_path / "cbz"
    database = H2HDB(
        CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite",
                database=str(tmp_path / "catalog.sqlite"),
            )
        )
    )
    database.migrate()
    _synchronize(database, _service(database, galleries, cbz_path))
    active = database.get_publication("urn:h2h:gallery:3")
    assert active is not None
    old_path = active.artifacts[0].location
    old_bytes = old_path.read_bytes()

    failing = _service(
        database,
        galleries,
        cbz_path,
        catalog_publisher=_FailingCatalogPublisher(),
        max_image_short_side=8,
    )
    turn = _claim_ingest(database)
    with pytest.raises(RuntimeError, match="injected publish failure"):
        failing.synchronize_once(turn)

    assert database.get_catalog_revision().revision == 1
    assert old_path.is_file()
    assert old_path.read_bytes() == old_bytes
    still_active = database.get_publication("urn:h2h:gallery:3")
    assert still_active is not None
    assert still_active.artifacts[0].location == old_path

    _service(
        database,
        galleries,
        cbz_path,
        max_image_short_side=8,
    ).synchronize_once(turn)
    assert database.complete_gallery_ingest(turn)
    assert old_path.exists()


def test_post_commit_crash_cannot_prune_a_historical_artifact(tmp_path: Path) -> None:
    galleries = tmp_path / "galleries"
    folder = _write_gallery(galleries, 4, color=(255, 0, 0))
    cbz_path = tmp_path / "cbz"
    database = H2HDB(
        CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite",
                database=str(tmp_path / "catalog.sqlite"),
            )
        )
    )
    database.migrate()
    crashing = _service(
        database,
        galleries,
        cbz_path,
        cbz_reconciler=_CrashAfterPublishCBZ(
            artifact_store_path=cbz_path.with_name(f"{cbz_path.name}-artifacts"),
            cbz_path=cbz_path,
            max_image_short_side=16,
        ),
    )
    turn = _claim_ingest(database)

    with pytest.raises(RuntimeError, match="post-commit crash"):
        crashing.synchronize_once(turn)

    historical_revision = database.get_catalog_revision()
    historical = database.get_publication(
        "urn:h2h:gallery:4",
        revision=historical_revision,
    )
    assert historical is not None
    historical_path = historical.artifacts[0].location
    assert historical_path.is_file()
    assert database.complete_gallery_ingest(turn)

    for child in folder.iterdir():
        child.unlink()
    folder.rmdir()
    recovered = _synchronize(
        database,
        _service(database, galleries, cbz_path),
    )

    assert recovered.revision == historical_revision.revision + 1
    assert database.get_catalog_revision().publication_count == 0
    assert historical_path.is_file()
    assert (
        database.get_publication(
            "urn:h2h:gallery:4",
            revision=historical_revision,
        )
        == historical
    )


def test_removed_gallery_is_queued_after_revision_publish(tmp_path: Path) -> None:
    galleries = tmp_path / "galleries"
    folder = _write_gallery(galleries, 5, color=(0, 0, 255))
    database = H2HDB(
        CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite",
                database=str(tmp_path / "catalog.sqlite"),
            )
        )
    )
    database.migrate()
    service = _service(database, galleries, tmp_path / "cbz")
    _synchronize(database, service)
    (folder / "001.png").unlink()
    (folder / "galleryinfo.txt").unlink()
    folder.rmdir()

    removed = _synchronize(database, service)

    assert removed.removed == 1
    assert database.get_catalog_revision().publication_count == 0
    assert database.get_download_request(5) is not None


def test_projection_loser_that_remains_in_source_is_not_queued_for_download(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(
        galleries,
        40,
        color=(255, 0, 0),
        title="short",
    )
    database = H2HDB(
        CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite",
                database=str(tmp_path / "catalog.sqlite"),
            )
        )
    )
    database.migrate()
    service = _service(database, galleries, None)
    _synchronize(database, service)
    _write_gallery(
        galleries,
        41,
        color=(255, 0, 0),
        title="a substantially longer title",
    )

    outcome = _synchronize(database, service)

    assert outcome.new == 1
    assert outcome.removed == 0
    assert database.get_publication("urn:h2h:gallery:40") is None
    assert database.get_publication("urn:h2h:gallery:41") is not None
    assert database.get_download_request(40) is None


def test_requested_deletion_keeps_source_until_folder_is_removed(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    folder = _write_gallery(galleries, 50, color=(0, 0, 255))
    database = H2HDB(
        CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite",
                database=str(tmp_path / "catalog.sqlite"),
            )
        )
    )
    database.migrate()
    service = _service(database, galleries, None)
    _synchronize(database, service)
    database.request_gallery_deletion(50)

    still_present = _synchronize(database, service)

    assert still_present.removed == 0
    assert database.get_publication("urn:h2h:gallery:50") is not None

    for child in folder.iterdir():
        child.unlink()
    folder.rmdir()
    removed = _synchronize(database, service)

    assert removed.removed == 1
    assert database.get_publication("urn:h2h:gallery:50") is None
    assert database.get_download_request(50) is None
