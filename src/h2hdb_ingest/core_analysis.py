"""Adapt the core's durable analysis store to the ingest deduplication policy."""

__all__ = ["CoreStagedDeduplicationAdapter"]

from collections.abc import Sequence

from h2hdb import (
    CatalogAnalysisPhase,
    CatalogBuild,
    CatalogBuildAnalyzer,
    CatalogContentCandidateCursor,
    CatalogContentDigest,
    CatalogContentOwner,
    CatalogDeduplicationCandidate,
    CatalogFinalAnalysisCursor,
    CatalogGalleryFileHashCursor,
    CatalogGidCandidateCursor,
    CatalogGidWinner,
    CatalogSourceGalleryAnalysis,
    CatalogSourceManifest,
    CatalogSourceManifestCursor,
    GalleryIngestTurn,
)
from h2hdb import (
    CatalogFileHashAggregate as CoreFileHashAggregate,
)
from h2hdb import (
    CatalogFileHashAggregatePage as CoreFileHashAggregatePage,
)

from .deduplication import ALREADY_UPLOADED_TAG_VALUE, DeduplicationCandidate
from .staged_deduplication import (
    ContentCandidateCursor,
    ContentCandidatePage,
    ContentCandidateRow,
    ContentOwnershipDecision,
    FileHashAggregate,
    FileHashAggregatePage,
    GalleryAnalysisCursor,
    GalleryAnalysisDecision,
    GalleryAnalysisPage,
    GalleryContentDigest,
    GalleryFileHashCursor,
    GalleryFileHashPage,
    GalleryFileHashRow,
    GallerySourceFileCursor,
    GallerySourceFilePage,
    GallerySourceFileRow,
    GallerySourceManifest,
    GidCandidateCursor,
    GidCandidatePage,
    GidCandidateRow,
    GidWinnerDecision,
    StagedDeduplicationPhase,
)


def _core_phase(phase: StagedDeduplicationPhase) -> CatalogAnalysisPhase:
    """Map by durable value so the two public enums cannot silently drift."""

    return CatalogAnalysisPhase(phase.value)


def _require_gallery_key(value: str | None) -> str:
    if value is None:
        raise RuntimeError("Core returned an analysis row without its gallery key")
    return value


class CoreStagedDeduplicationAdapter:
    """Turn-fenced bridge between bounded ingest reducers and core storage.

    The build and ingest turn are deliberately bound at construction time.
    Every mutation therefore uses the same durable build identity and fencing
    token, while the planner's ``build_id`` argument is checked on every call.
    """

    def __init__(
        self,
        analyzer: CatalogBuildAnalyzer,
        build: CatalogBuild,
        ingest_turn: GalleryIngestTurn,
    ) -> None:
        self._analyzer = analyzer
        self._build = build
        self._ingest_turn = ingest_turn

    def _require_build(self, build_id: str) -> None:
        if build_id != self._build.build_id:
            raise ValueError(
                "The staged deduplication adapter is bound to a different build"
            )

    def is_deduplication_phase_complete(
        self,
        build_id: str,
        phase: StagedDeduplicationPhase,
    ) -> bool:
        self._require_build(build_id)
        return self._analyzer.is_catalog_analysis_phase_complete(
            build_id,
            _core_phase(phase),
        )

    def page_gallery_source_files(
        self,
        build_id: str,
        *,
        after: GallerySourceFileCursor | None,
        limit: int,
    ) -> GallerySourceFilePage:
        self._require_build(build_id)
        core_after = (
            CatalogSourceManifestCursor(
                after.gallery_key,
                after.file_sort_key,
                after.file_name,
                after.file_key,
            )
            if after is not None
            else None
        )
        page = self._analyzer.list_catalog_source_manifest_rows(
            build_id,
            after=core_after,
            limit=limit,
        )
        return GallerySourceFilePage(
            tuple(
                GallerySourceFileRow(
                    gallery_name=row.gallery_name,
                    gallery_key=row.gallery_key,
                    file_sort_key=row.file_sort_key,
                    file_name=row.file_name,
                    file_key=row.file_key,
                    size_bytes=row.size_bytes,
                    file_sha256=row.file_sha256,
                    empty_gallery_metadata_sha256=(row.empty_gallery_metadata_sha256),
                )
                for row in page.items
            )
        )

    def stage_gallery_source_manifests(
        self,
        build_id: str,
        manifests: Sequence[GallerySourceManifest],
        *,
        batch_id: str,
    ) -> None:
        self._require_build(build_id)
        self._analyzer.stage_catalog_source_manifests(
            self._build,
            tuple(
                CatalogSourceManifest(
                    gallery_name=value.gallery_name,
                    source_manifest_sha256=value.source_manifest_sha256,
                    source_manifest_version=value.source_manifest_version,
                )
                for value in manifests
            ),
            batch_id=batch_id,
            ingest_turn=self._ingest_turn,
        )

    def get_file_spam_page(
        self,
        build_id: str,
        *,
        minimum_occurrences: int,
        limit: int,
    ) -> FileHashAggregatePage:
        self._require_build(build_id)
        page = self._analyzer.get_catalog_file_spam_page(
            self._build,
            minimum_occurrences=minimum_occurrences,
            limit=limit,
            ingest_turn=self._ingest_turn,
        )
        return FileHashAggregatePage(
            items=tuple(
                FileHashAggregate(
                    file_sha256=row.file_sha256,
                    occurrence_count=row.occurrence_count,
                    distinct_artist_count=row.distinct_artist_count,
                    maximum_gallery_artist_count=row.maximum_gallery_artist_count,
                    minimum_occurrences=row.minimum_occurrences,
                )
                for row in page.items
            ),
            minimum_occurrences=page.minimum_occurrences,
            checkpoint_generation=page.checkpoint_generation,
            start_cursor_sha256=page.start_cursor_sha256,
            next_cursor_sha256=page.next_cursor_sha256,
            input_sha256=page.input_sha256,
            page_limit=page.limit,
        )

    def apply_file_spam_page(
        self,
        build_id: str,
        page: FileHashAggregatePage,
        hashes: Sequence[str],
    ) -> None:
        self._require_build(build_id)
        core_page = CoreFileHashAggregatePage(
            items=tuple(
                CoreFileHashAggregate(
                    file_sha256=item.file_sha256,
                    occurrence_count=item.occurrence_count,
                    distinct_artist_count=item.distinct_artist_count,
                    maximum_gallery_artist_count=item.maximum_gallery_artist_count,
                    minimum_occurrences=item.minimum_occurrences,
                )
                for item in page.items
            ),
            limit=page.page_limit,
            minimum_occurrences=page.minimum_occurrences,
            checkpoint_generation=page.checkpoint_generation,
            start_cursor_sha256=page.start_cursor_sha256,
            next_cursor_sha256=page.next_cursor_sha256,
            input_sha256=page.input_sha256,
        )
        self._analyzer.apply_catalog_file_spam_page(
            self._build,
            core_page,
            tuple(hashes),
            ingest_turn=self._ingest_turn,
        )

    def page_gallery_file_hashes(
        self,
        build_id: str,
        *,
        after: GalleryFileHashCursor | None,
        limit: int,
    ) -> GalleryFileHashPage:
        self._require_build(build_id)
        core_after = (
            CatalogGalleryFileHashCursor(
                after.gallery_key,
                after.file_sha256,
                after.file_key,
            )
            if after is not None
            else None
        )
        page = self._analyzer.list_catalog_gallery_file_hashes(
            build_id,
            after=core_after,
            limit=limit,
        )
        return GalleryFileHashPage(
            tuple(
                GalleryFileHashRow(
                    gallery_name=row.gallery_name,
                    gallery_key=row.gallery_key,
                    file_key=row.file_key,
                    file_sha256=row.file_sha256,
                    metadata_file=row.metadata_file,
                    excluded_as_spam=row.excluded_as_spam,
                )
                for row in page.items
            )
        )

    def stage_gallery_content_digests(
        self,
        build_id: str,
        digests: Sequence[GalleryContentDigest],
        *,
        batch_id: str,
    ) -> None:
        self._require_build(build_id)
        self._analyzer.stage_catalog_content_digests(
            self._build,
            tuple(
                CatalogContentDigest(
                    gallery_name=value.gallery_name,
                    content_sha256=value.content_sha256,
                    duplicate_hash_deletion_candidate=(
                        value.duplicate_hash_deletion_candidate
                    ),
                )
                for value in digests
            ),
            batch_id=batch_id,
            ingest_turn=self._ingest_turn,
        )

    def page_content_candidates(
        self,
        build_id: str,
        *,
        after: ContentCandidateCursor | None,
        limit: int,
    ) -> ContentCandidatePage:
        self._require_build(build_id)
        core_after = (
            CatalogContentCandidateCursor(
                after.content_sha256,
                after.gallery_key,
            )
            if after is not None
            else None
        )
        page = self._analyzer.list_catalog_content_candidates(
            build_id,
            after=core_after,
            limit=limit,
        )
        return ContentCandidatePage(
            tuple(
                ContentCandidateRow(
                    candidate=self._candidate(row.candidate),
                    incumbent_gallery_name=row.incumbent_gallery_name,
                    gallery_key=row.gallery_key,
                )
                for row in page.items
            )
        )

    def stage_content_owners(
        self,
        build_id: str,
        decisions: Sequence[ContentOwnershipDecision],
        *,
        batch_id: str,
    ) -> None:
        self._require_build(build_id)
        self._analyzer.stage_catalog_content_owners(
            self._build,
            tuple(
                CatalogContentOwner(
                    content_sha256=value.content_sha256,
                    owner_gallery_name=value.owner_gallery_name,
                )
                for value in decisions
            ),
            batch_id=batch_id,
            ingest_turn=self._ingest_turn,
        )

    def page_gid_candidates(
        self,
        build_id: str,
        *,
        after: GidCandidateCursor | None,
        limit: int,
    ) -> GidCandidatePage:
        self._require_build(build_id)
        core_after = (
            CatalogGidCandidateCursor(after.gid, after.gallery_key)
            if after is not None
            else None
        )
        page = self._analyzer.list_catalog_gid_candidates(
            build_id,
            after=core_after,
            limit=limit,
        )
        return GidCandidatePage(
            tuple(
                GidCandidateRow(
                    candidate=self._candidate(row.candidate),
                    incumbent_gallery_name=row.incumbent_gallery_name,
                    gallery_key=row.gallery_key,
                )
                for row in page.items
            )
        )

    def stage_gid_winners(
        self,
        build_id: str,
        decisions: Sequence[GidWinnerDecision],
        *,
        batch_id: str,
    ) -> None:
        self._require_build(build_id)
        self._analyzer.stage_catalog_gid_winners(
            self._build,
            tuple(
                CatalogGidWinner(
                    gid=value.gid,
                    winner_gallery_name=value.winner_gallery_name,
                )
                for value in decisions
            ),
            batch_id=batch_id,
            ingest_turn=self._ingest_turn,
        )

    def page_final_gallery_analyses(
        self,
        build_id: str,
        *,
        after: GalleryAnalysisCursor | None,
        limit: int,
    ) -> GalleryAnalysisPage:
        self._require_build(build_id)
        core_after = (
            CatalogFinalAnalysisCursor(after.gallery_key) if after is not None else None
        )
        page = self._analyzer.list_catalog_final_analyses(
            build_id,
            after=core_after,
            limit=limit,
        )
        return GalleryAnalysisPage(
            tuple(
                GalleryAnalysisDecision(
                    gallery_name=row.gallery_name,
                    gallery_key=_require_gallery_key(row.gallery_key),
                    content_sha256=row.content_sha256,
                    selected=row.selected,
                    duplicate_of_gallery_name=row.duplicate_of_gallery_name,
                )
                for row in page.items
            )
        )

    def stage_final_gallery_analyses(
        self,
        build_id: str,
        decisions: Sequence[GalleryAnalysisDecision],
        *,
        batch_id: str,
    ) -> None:
        self._require_build(build_id)
        self._analyzer.stage_catalog_final_analyses(
            self._build,
            tuple(
                CatalogSourceGalleryAnalysis(
                    gallery_name=value.gallery_name,
                    gallery_key=value.gallery_key,
                    content_sha256=value.content_sha256,
                    selected=value.selected,
                    duplicate_of_gallery_name=value.duplicate_of_gallery_name,
                )
                for value in decisions
            ),
            batch_id=batch_id,
            ingest_turn=self._ingest_turn,
        )

    def complete_deduplication_phase(
        self,
        build_id: str,
        phase: StagedDeduplicationPhase,
    ) -> None:
        self._require_build(build_id)
        self._analyzer.complete_catalog_analysis_phase(
            self._build,
            _core_phase(phase),
            ingest_turn=self._ingest_turn,
        )

    @staticmethod
    def _candidate(
        candidate: CatalogDeduplicationCandidate,
    ) -> DeduplicationCandidate:
        return DeduplicationCandidate(
            gallery_name=candidate.gallery_name,
            gid=candidate.gid,
            title=candidate.title,
            download_time=candidate.download_time,
            content_digest=candidate.content_sha256,
            already_uploaded=any(
                tag.value.casefold() == ALREADY_UPLOADED_TAG_VALUE
                for tag in candidate.tags
            ),
        )
