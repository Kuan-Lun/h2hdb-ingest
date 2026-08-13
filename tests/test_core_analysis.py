from dataclasses import replace
from pathlib import Path

import pytest
from h2hdb import (
    H2HDB,
    CatalogBuildPhase,
    CoreConfig,
    DatabaseConfig,
    IngestTurnLostError,
)

from h2hdb_ingest.core_analysis import CoreStagedDeduplicationAdapter
from h2hdb_ingest.deduplication import DeduplicationPolicy
from h2hdb_ingest.scanner import FilesystemScanner
from h2hdb_ingest.staged_deduplication import (
    GalleryAnalysisCursor,
    GalleryAnalysisDecision,
    GalleryFileHashCursor,
    GalleryFileHashRow,
    GallerySourceManifest,
    GidCandidateCursor,
    GidCandidateRow,
    StagedDeduplicationPlanner,
    StagedDeduplicationSummary,
)
from h2hdb_ingest.staging import CoreFileHashCache, FilesystemSourceStager


def _write_gallery(
    root: Path,
    folder_name: str,
    *,
    title: str,
    artist: str,
    files: tuple[tuple[str, bytes], ...],
    already_uploaded: bool = False,
) -> None:
    folder = root / folder_name
    folder.mkdir(parents=True)
    tags = [f"artist:{artist}", "language:english"]
    if already_uploaded:
        # The ingest policy intentionally applies Python casefold to the exact
        # tag value and does not depend on the database collation or tag name.
        tags.append("unrelated:AlReAdY UpLoAdEd")
    (folder / "galleryinfo.txt").write_text(
        "\n".join(
            (
                f"Title: {title}",
                "Upload Time: 2024-01-02 03:04",
                "Uploaded By: uploader",
                "Downloaded: 2024-01-03 04:05",
                f"Tags: {', '.join(tags)}",
                "Uploader's Comments:",
                "Summary",
                "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3",
            )
        ),
        encoding="utf-8",
    )
    for name, payload in files:
        (folder / name).write_bytes(payload)


def _write_fixture(root: Path) -> None:
    spam = b"shared cross-artist scanner artifact"
    _write_gallery(
        root,
        "Duplicate Alpha [101]",
        title="same",
        artist="artist-a",
        files=(
            ("001.bin", b"first"),
            ("002.bin", b"second"),
            ("003.bin", b"first"),
            ("spam-a.bin", spam),
        ),
    )
    _write_gallery(
        root,
        "Duplicate Beta [102]",
        title="same",
        artist="artist-b",
        already_uploaded=True,
        files=(
            ("001.bin", b"first"),
            ("002.bin", b"first"),
            ("003.bin", b"second"),
            ("spam-b.bin", spam),
        ),
    )
    _write_gallery(
        root,
        "Empty After Spam [700]",
        title="empty",
        artist="artist-c",
        files=(("spam-c.bin", spam),),
    )
    _write_gallery(
        root,
        "GID Alpha [500]",
        title="same gid",
        artist="artist-d",
        files=(("001.bin", b"gid-a"),),
    )
    _write_gallery(
        root,
        "GID Beta [500]",
        title="same gid",
        artist="artist-e",
        files=(("001.bin", b"gid-b"),),
    )
    _write_gallery(
        root,
        "Only Metadata [800]",
        title="metadata only",
        artist="artist-f",
        files=(),
    )


def _database_config(tmp_path: Path) -> CoreConfig:
    return CoreConfig(
        database=DatabaseConfig(
            sql_type="sqlite",
            database=str(tmp_path / "catalog.sqlite"),
        )
    )


def test_core_adapter_runs_bounded_deduplication_with_durable_restart(
    tmp_path: Path,
) -> None:
    root = tmp_path / "galleries"
    _write_fixture(root)

    # This is the existing policy's materialized result, used only as the
    # small-fixture oracle for the new database-backed path.
    legacy_galleries = FilesystemScanner(
        root,
        hash_workers=2,
        max_galleries=2,
        max_files=2,
    ).scan()
    legacy = DeduplicationPolicy().select(legacy_galleries)
    legacy_by_name = {
        gallery.gallery_name: gallery for gallery in legacy.canonical_galleries
    }

    config = _database_config(tmp_path)
    database = H2HDB(config)
    database.migrate()
    turn = database.claim_gallery_ingest(lease_seconds=120, periodic_scan=True)
    assert turn is not None
    cache = CoreFileHashCache(database)
    stager = FilesystemSourceStager(
        scanner=FilesystemScanner(
            root,
            hash_workers=2,
            hash_cache=cache,
            max_galleries=2,
            max_files=2,
        ),
        coordinator=database,
        hash_cache=cache,
    )
    build = stager.begin_or_resume(
        scope_key="core-analysis-integration-v1",
        ingest_turn=turn,
    )
    build = stager.stage(build, ingest_turn=turn)
    assert build.phase is CatalogBuildPhase.analyzing

    alpha = legacy_by_name["Duplicate Alpha [101]"]
    wrong_turn = replace(turn, owner_token="wrong-owner-token")
    wrong_adapter = CoreStagedDeduplicationAdapter(database, build, wrong_turn)
    with pytest.raises(IngestTurnLostError):
        wrong_adapter.stage_gallery_source_manifests(
            build.build_id,
            (GallerySourceManifest(alpha.gallery_name, alpha.source_digest),),
            batch_id="wrong-turn-must-not-write",
        )
    stale_turn = replace(turn, generation=turn.generation - 1)
    stale_adapter = CoreStagedDeduplicationAdapter(database, build, stale_turn)
    with pytest.raises(IngestTurnLostError):
        stale_adapter.stage_gallery_source_manifests(
            build.build_id,
            (GallerySourceManifest(alpha.gallery_name, alpha.source_digest),),
            batch_id="stale-turn-must-not-write",
        )

    adapter = CoreStagedDeduplicationAdapter(database, build, turn)
    planner = StagedDeduplicationPlanner(page_size=1, write_batch_size=2)
    summary = planner.run(adapter, build_id=build.build_id)
    # Canonical manifests are now sealed with each gallery completion.  The
    # planner observes the durable SOURCE_MANIFESTS checkpoint and does not
    # reread every staged source-file row to derive the same values again.
    assert summary.gallery_source_manifests == 0
    assert summary.gallery_content_digests == len(legacy_galleries)
    assert summary.gallery_analyses == len(legacy_galleries)

    source_page = database.list_catalog_build_sources(build.build_id, limit=20)
    source_by_name = {source.gallery_name: source for source in source_page.galleries}
    assert source_page.total == len(legacy_galleries)
    assert {
        name: (source.source_manifest_sha256, source.source_manifest_version)
        for name, source in source_by_name.items()
    } == {name: (gallery.source_digest, 1) for name, gallery in legacy_by_name.items()}

    spam_digest = next(
        source_file.sha256
        for source_file in legacy_by_name["Duplicate Alpha [101]"].files
        if source_file.name == "spam-a.bin"
    )
    spam_rows: list[GalleryFileHashRow] = []
    after_hash: GalleryFileHashCursor | None = None
    while True:
        hash_page = adapter.page_gallery_file_hashes(
            build.build_id,
            after=after_hash,
            limit=1,
        )
        if not hash_page.items:
            break
        spam_rows.extend(
            row for row in hash_page.items if row.file_sha256 == spam_digest
        )
        after_hash = hash_page.items[-1].cursor
    assert len(spam_rows) == 3
    assert all(row.excluded_as_spam for row in spam_rows)
    assert legacy.excluded_file_sha256s == frozenset({spam_digest})

    candidates = adapter.page_content_candidates(
        build.build_id,
        after=None,
        limit=20,
    ).items
    candidate_by_name = {
        row.candidate.gallery_name: row.candidate for row in candidates
    }
    assert candidate_by_name["Duplicate Beta [102]"].already_uploaded is True
    assert candidate_by_name["Duplicate Alpha [101]"].already_uploaded is False

    # GID candidates are the durably selected content owners.  Therefore the
    # content duplicate loser must already be absent at this boundary.
    gid_candidates: list[GidCandidateRow] = []
    after_gid: GidCandidateCursor | None = None
    while True:
        gid_page = adapter.page_gid_candidates(
            build.build_id,
            after=after_gid,
            limit=1,
        )
        if not gid_page.items:
            break
        gid_candidates.extend(gid_page.items)
        after_gid = gid_page.items[-1].cursor
    gid_candidate_names = {row.candidate.gallery_name for row in gid_candidates}
    assert "Duplicate Alpha [101]" in gid_candidate_names
    assert "Duplicate Beta [102]" not in gid_candidate_names

    final_rows: list[GalleryAnalysisDecision] = []
    after_final: GalleryAnalysisCursor | None = None
    while True:
        final_page = adapter.page_final_gallery_analyses(
            build.build_id,
            after=after_final,
            limit=1,
        )
        if not final_page.items:
            break
        final_rows.extend(final_page.items)
        after_final = final_page.items[-1].cursor
    final_by_name = {row.gallery_name: row for row in final_rows}
    assert {name: row.content_sha256 for name, row in final_by_name.items()} == {
        name: gallery.content_digest for name, gallery in legacy_by_name.items()
    }
    assert (
        {
            name: row.duplicate_of_gallery_name
            for name, row in final_by_name.items()
            if row.duplicate_of_gallery_name is not None
        }
        == dict(legacy.duplicate_of_by_gallery_name)
        == {"Duplicate Beta [102]": "Duplicate Alpha [101]"}
    )
    assert {name for name, row in final_by_name.items() if row.selected} == {
        gallery.gallery_name for gallery in legacy.winners
    }
    assert final_by_name["Empty After Spam [700]"].content_sha256 is None
    assert final_by_name["Only Metadata [800]"].content_sha256 is None
    assert final_by_name["GID Alpha [500]"].selected is False
    assert final_by_name["GID Beta [500]"].selected is True

    # A process restart consults the durable phase checkpoints before reading
    # or writing any reducer page, so a fully analysed build is a no-op.
    restarted_database = H2HDB(config)
    restarted_adapter = CoreStagedDeduplicationAdapter(
        restarted_database,
        build,
        turn,
    )
    assert planner.run(restarted_adapter, build_id=build.build_id) == (
        StagedDeduplicationSummary(0, 0, 0, 0, 0, 0)
    )
    completed = restarted_database.complete_catalog_analysis(
        build,
        ingest_turn=turn,
    )
    assert completed.phase is CatalogBuildPhase.artifacts
