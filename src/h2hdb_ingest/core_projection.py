"""Bind staged projection orchestration to the public H2HDB build API."""

from __future__ import annotations

__all__ = ["CoreStagedProjectionAdapter"]

from collections.abc import Sequence
from pathlib import Path, PurePosixPath

from h2hdb import (
    CatalogBuild,
    CatalogBuildProjection,
    CatalogBuildProjectionCoordinator,
    CatalogBuildProjectionPhase,
    CatalogPreparedArtifact,
    CatalogProjectionSelectedFileCursor,
    CatalogProjectionSelectedGalleryCursor,
    CatalogProjectionSelection,
    CatalogProjectionSelectionCursor,
    GalleryIngestTurn,
)

from .models import FileStatSignature
from .staged_projection import (
    PreparedProjectionArtifact,
    ProjectionCheckpoint,
    ProjectionSelectionCursor,
    ProjectionSelectionPage,
    SelectedGalleryCursor,
    SelectedGalleryFile,
    SelectedGalleryFileCursor,
    SelectedGalleryFilePage,
    SelectedGalleryPage,
    SelectedGallerySource,
    StagedProjectionPhase,
    StagedProjectionSelection,
)

CORE_MAX_PROJECTION_PAGE_SIZE = 200


class CoreStagedProjectionAdapter:
    """Turn-fenced, thread-safe bridge for one durable catalog build.

    Read methods delegate to the facade, whose implementation opens an
    independent read transaction for every call.  That matters because CBZ
    workers page different galleries concurrently.  Every write is bound to
    the exact build and ingest turn supplied at construction time.
    """

    def __init__(
        self,
        coordinator: CatalogBuildProjectionCoordinator,
        build: CatalogBuild,
        ingest_turn: GalleryIngestTurn,
        *,
        source_root: Path,
    ) -> None:
        self._coordinator = coordinator
        self._build = build
        self._ingest_turn = ingest_turn
        self._source_root = source_root

    @property
    def build(self) -> CatalogBuild:
        return self._build

    def begin_or_resume(self, *, artifacts_required: bool) -> CatalogBuildProjection:
        return self._coordinator.begin_catalog_build_projection(
            self._build,
            artifacts_required=artifacts_required,
            ingest_turn=self._ingest_turn,
        )

    def get_projection_checkpoint(self, build_id: str) -> ProjectionCheckpoint:
        self._require_build(build_id)
        checkpoint = self._coordinator.get_catalog_projection_checkpoint(build_id)
        phase = self._phase(checkpoint.phase)
        return ProjectionCheckpoint(
            phase=phase,
            artifact_after=(
                None
                if checkpoint.artifact_after_gallery_key is None
                else SelectedGalleryCursor(checkpoint.artifact_after_gallery_key)
            ),
            selection_after=(
                None
                if checkpoint.selection_after_gallery_key is None
                else ProjectionSelectionCursor(checkpoint.selection_after_gallery_key)
            ),
        )

    def page_selected_galleries(
        self,
        build_id: str,
        *,
        after: SelectedGalleryCursor | None,
        limit: int,
    ) -> SelectedGalleryPage:
        self._require_build(build_id)
        page = self._coordinator.list_catalog_projection_selected_galleries(
            build_id,
            after=(
                None
                if after is None
                else CatalogProjectionSelectedGalleryCursor(after.gallery_key)
            ),
            limit=self._bounded_limit(limit),
        )
        items = tuple(
            SelectedGallerySource(
                gallery_key=value.gallery_key,
                folder=self._source_path(value.source_locator),
                gallery_name=value.gallery_name,
                gid=value.gid,
                title=value.title,
                summary=value.comment,
                upload_account=value.upload_account,
                upload_time=value.upload_time,
                download_time=value.download_time,
                modified_time=value.modified_time,
                pages=value.page_count,
                tags=tuple((tag.name, tag.value) for tag in value.tags),
                metadata_sha256=value.metadata_sha256,
                source_sha256=value.source_manifest_sha256,
                content_sha256=value.content_sha256,
            )
            for value in page.items
        )
        return SelectedGalleryPage(
            items,
            (
                None
                if page.next_cursor is None
                else SelectedGalleryCursor(page.next_cursor.gallery_key)
            ),
        )

    def page_selected_gallery_files(
        self,
        build_id: str,
        gallery_key: str,
        *,
        after: SelectedGalleryFileCursor | None,
        limit: int,
    ) -> SelectedGalleryFilePage:
        self._require_build(build_id)
        page = self._coordinator.list_catalog_projection_selected_files(
            build_id,
            gallery_key,
            after=(
                None
                if after is None
                else CatalogProjectionSelectedFileCursor(
                    after.file_sort_key,
                    after.file_name,
                    after.file_key,
                )
            ),
            limit=self._bounded_limit(limit),
        )
        items = tuple(
            SelectedGalleryFile(
                gallery_key=value.gallery_key,
                file_key=value.file_key,
                file_sort_key=value.file_sort_key,
                path=self._source_path(value.relative_locator),
                name=value.file_name,
                size_bytes=value.size_bytes,
                sha256=value.sha256,
                signature=FileStatSignature(
                    device=value.device,
                    inode=value.inode,
                    size_bytes=value.size_bytes,
                    modified_ns=value.modified_ns,
                    changed_ns=value.changed_ns,
                ),
                excluded=value.excluded,
            )
            for value in page.items
        )
        return SelectedGalleryFilePage(
            items,
            (
                None
                if page.next_cursor is None
                else SelectedGalleryFileCursor(
                    page.next_cursor.file_sort_key,
                    page.next_cursor.file_name,
                    page.next_cursor.file_key,
                )
            ),
        )

    def record_prepared_artifacts(
        self,
        build_id: str,
        prepared: Sequence[PreparedProjectionArtifact],
        *,
        batch_id: str,
    ) -> None:
        self._require_build(build_id)
        self._coordinator.record_catalog_prepared_artifacts(
            self._build,
            tuple(
                CatalogPreparedArtifact(value.gallery_key, value.artifact)
                for value in prepared
            ),
            batch_id=batch_id,
            ingest_turn=self._ingest_turn,
        )

    def advance_artifact_checkpoint(
        self,
        build_id: str,
        *,
        expected_after: SelectedGalleryCursor | None,
        after: SelectedGalleryCursor,
        batch_id: str,
    ) -> None:
        self._require_build(build_id)
        self._coordinator.advance_catalog_artifact_checkpoint(
            self._build,
            expected_after=(
                None
                if expected_after is None
                else CatalogProjectionSelectedGalleryCursor(expected_after.gallery_key)
            ),
            after=CatalogProjectionSelectedGalleryCursor(after.gallery_key),
            batch_id=batch_id,
            ingest_turn=self._ingest_turn,
        )

    def complete_artifact_preparation(
        self,
        build_id: str,
        *,
        expected_after: SelectedGalleryCursor | None,
    ) -> None:
        self._require_build(build_id)
        self._coordinator.complete_catalog_artifact_preparation(
            self._build,
            expected_after=(
                None
                if expected_after is None
                else CatalogProjectionSelectedGalleryCursor(expected_after.gallery_key)
            ),
            ingest_turn=self._ingest_turn,
        )

    def page_projection_selections(
        self,
        build_id: str,
        *,
        after: ProjectionSelectionCursor | None,
        limit: int,
    ) -> ProjectionSelectionPage:
        self._require_build(build_id)
        page = self._coordinator.list_catalog_projection_selections(
            build_id,
            after=(
                None
                if after is None
                else CatalogProjectionSelectionCursor(after.gallery_key)
            ),
            limit=self._bounded_limit(limit),
        )
        items = tuple(
            StagedProjectionSelection(
                gallery_key=value.gallery_key,
                artifact=value.artifact,
                redownload_required=value.redownload_required,
            )
            for value in page.items
        )
        return ProjectionSelectionPage(
            items,
            (
                None
                if page.next_cursor is None
                else ProjectionSelectionCursor(page.next_cursor.gallery_key)
            ),
        )

    def stage_projection_selections(
        self,
        build_id: str,
        selections: Sequence[StagedProjectionSelection],
        *,
        expected_after: ProjectionSelectionCursor | None,
        after: ProjectionSelectionCursor,
        batch_id: str,
    ) -> None:
        self._require_build(build_id)
        self._coordinator.stage_catalog_projection_selections(
            self._build,
            tuple(
                CatalogProjectionSelection(
                    gallery_key=value.gallery_key,
                    artifact=value.artifact,
                    redownload_required=value.redownload_required,
                )
                for value in selections
            ),
            expected_after=(
                None
                if expected_after is None
                else CatalogProjectionSelectionCursor(expected_after.gallery_key)
            ),
            after=CatalogProjectionSelectionCursor(after.gallery_key),
            batch_id=batch_id,
            ingest_turn=self._ingest_turn,
        )

    def complete_projection_staging(
        self,
        build_id: str,
        *,
        expected_after: ProjectionSelectionCursor | None,
    ) -> None:
        self._require_build(build_id)
        self._coordinator.complete_catalog_projection_staging(
            self._build,
            expected_after=(
                None
                if expected_after is None
                else CatalogProjectionSelectionCursor(expected_after.gallery_key)
            ),
            ingest_turn=self._ingest_turn,
        )

    def _require_build(self, build_id: str) -> None:
        if build_id != self._build.build_id:
            raise ValueError("The staged projection adapter is bound to another build")

    @staticmethod
    def _phase(value: CatalogBuildProjectionPhase) -> StagedProjectionPhase:
        if value in {
            CatalogBuildProjectionPhase.complete,
            CatalogBuildProjectionPhase.sealed,
            CatalogBuildProjectionPhase.published,
        }:
            return StagedProjectionPhase.complete
        return StagedProjectionPhase(value.value)

    @staticmethod
    def _bounded_limit(limit: int) -> int:
        if limit <= 0:
            raise ValueError("Projection page limit must be positive")
        return min(limit, CORE_MAX_PROJECTION_PAGE_SIZE)

    def _source_path(self, relative_locator: str) -> Path:
        locator = PurePosixPath(relative_locator)
        if not relative_locator or locator.is_absolute() or ".." in locator.parts:
            raise RuntimeError(
                f"Core returned an unsafe source locator: {relative_locator!r}"
            )
        return self._source_root.joinpath(*locator.parts)
