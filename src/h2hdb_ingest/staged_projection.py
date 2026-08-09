"""Bounded CBZ preparation and catalog-projection staging orchestration.

The core database remains the metadata authority.  This module only hydrates
one bounded window of selected source galleries for CBZ preparation, durably
records each resulting artifact, and forwards lightweight gallery selections
back to core.  It never constructs a ``CatalogPublication``.

Artifact preparation is checkpointed at page boundaries.  A crash can leave a
partially prepared page behind; restarting intentionally replays that page.
Content-addressed CBZ files and idempotent adapter writes make that replay safe.
Projection selection batches advance their checkpoint in the same storage
    operation that stages the batch.

    ``page_selected_gallery_files`` is called from CBZ worker threads and may
    be called concurrently for different galleries.  Database implementations
    must open a fresh read transaction/connection for every page call; they must
    never hand a cursor owned by the orchestration thread to a worker.
"""

from __future__ import annotations

__all__ = [
    "CBZStreamPreparer",
    "PreparedProjectionArtifact",
    "ProjectionCheckpoint",
    "ProjectionSelectionCursor",
    "ProjectionSelectionPage",
    "SelectedGalleryCursor",
    "SelectedGalleryFile",
    "SelectedGalleryFileCursor",
    "SelectedGalleryFilePage",
    "SelectedGalleryPage",
    "SelectedGallerySource",
    "StagedProjectionAdapter",
    "StagedProjectionOrchestrator",
    "StagedProjectionPhase",
    "StagedProjectionSelection",
    "StagedProjectionSummary",
]

import json
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime
from enum import Enum, StrEnum
from functools import partial
from hashlib import sha256
from pathlib import Path
from typing import Protocol

from h2hdb import CatalogArtifact

from .models import (
    CBZArtifact,
    CBZGalleryDescriptor,
    CBZPreparationFile,
    CBZPreparationMetadata,
    CBZPreparationSummary,
    CBZStreamingPreparationRequest,
    FileStatSignature,
    ScannedFile,
)
from .naming import gallery_name_to_cbz_file_name

CBZ_MEDIA_TYPE = "application/vnd.comicbook+zip"


def _validate_sha256(value: str, *, label: str) -> None:
    if len(value) != 64:
        raise ValueError(f"{label} must contain 64 hexadecimal characters")
    try:
        bytes.fromhex(value)
    except ValueError as error:
        raise ValueError(f"{label} is not hexadecimal") from error


def _validate_gallery_key(value: str) -> None:
    if not value:
        raise ValueError("gallery_key must not be blank")


def _canonical_batch_value(value: object) -> object:
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _canonical_batch_value(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, tuple | list):
        return [_canonical_batch_value(item) for item in value]
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise TypeError("Canonical batch dictionaries require string keys")
        return {
            key: _canonical_batch_value(item) for key, item in sorted(value.items())
        }
    raise TypeError(f"Unsupported canonical batch payload: {type(value).__name__}")


class StagedProjectionPhase(StrEnum):
    preparing_artifacts = "PREPARING_ARTIFACTS"
    staging_selections = "STAGING_SELECTIONS"
    complete = "COMPLETE"


@dataclass(frozen=True, order=True, slots=True)
class SelectedGalleryCursor:
    gallery_key: str

    def __post_init__(self) -> None:
        _validate_gallery_key(self.gallery_key)


@dataclass(frozen=True, order=True, slots=True)
class SelectedGalleryFileCursor:
    file_sort_key: str
    file_name: str
    file_key: str

    def __post_init__(self) -> None:
        if not self.file_name or not self.file_key:
            raise ValueError("file name and key must not be blank")


@dataclass(frozen=True, order=True, slots=True)
class ProjectionSelectionCursor:
    gallery_key: str

    def __post_init__(self) -> None:
        _validate_gallery_key(self.gallery_key)


@dataclass(frozen=True, slots=True)
class ProjectionCheckpoint:
    phase: StagedProjectionPhase
    artifact_after: SelectedGalleryCursor | None = None
    selection_after: ProjectionSelectionCursor | None = None

    def __post_init__(self) -> None:
        if (
            self.phase is StagedProjectionPhase.preparing_artifacts
            and self.selection_after is not None
        ):
            raise ValueError(
                "An artifact-preparation checkpoint cannot have a selection cursor"
            )


@dataclass(frozen=True, slots=True)
class SelectedGallerySource:
    """Core-owned metadata needed only to prepare one selected CBZ."""

    gallery_key: str
    folder: Path
    gallery_name: str
    gid: int
    title: str
    summary: str
    upload_account: str
    upload_time: datetime
    download_time: datetime
    modified_time: datetime
    pages: int
    tags: tuple[tuple[str, str], ...]
    metadata_sha256: str
    source_sha256: str
    content_sha256: str | None

    def __post_init__(self) -> None:
        _validate_gallery_key(self.gallery_key)
        object.__setattr__(self, "tags", tuple(self.tags))
        if not self.gallery_name:
            raise ValueError("gallery_name must not be blank")
        if self.gid <= 0:
            raise ValueError("gid must be positive")
        if self.pages < 0:
            raise ValueError("pages must not be negative")
        _validate_sha256(self.metadata_sha256, label="Metadata SHA-256")
        _validate_sha256(self.source_sha256, label="Source SHA-256")
        if self.content_sha256 is not None:
            _validate_sha256(self.content_sha256, label="Content SHA-256")

    @property
    def cursor(self) -> SelectedGalleryCursor:
        return SelectedGalleryCursor(self.gallery_key)


@dataclass(frozen=True, slots=True)
class SelectedGalleryPage:
    items: tuple[SelectedGallerySource, ...]
    next_cursor: SelectedGalleryCursor | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "items", tuple(self.items))
        cursors = tuple(item.cursor for item in self.items)
        if tuple(sorted(cursors)) != cursors or len(set(cursors)) != len(cursors):
            raise ValueError("Selected galleries must be strictly keyset ordered")
        if self.next_cursor is not None and (
            not cursors or self.next_cursor != cursors[-1]
        ):
            raise ValueError("next_cursor must identify the final selected gallery")


@dataclass(frozen=True, slots=True)
class SelectedGalleryFile:
    gallery_key: str
    file_key: str
    file_sort_key: str
    path: Path
    name: str
    size_bytes: int
    sha256: str
    signature: FileStatSignature
    excluded: bool = False

    def __post_init__(self) -> None:
        _validate_gallery_key(self.gallery_key)
        if not self.file_key:
            raise ValueError("file_key must not be blank")
        if not self.name:
            raise ValueError("file name must not be blank")
        if self.size_bytes < 0:
            raise ValueError("file size must not be negative")
        if self.signature.size_bytes != self.size_bytes:
            raise ValueError("file stat signature size must match the staged size")
        _validate_sha256(self.sha256, label="File SHA-256")

    @property
    def cursor(self) -> SelectedGalleryFileCursor:
        return SelectedGalleryFileCursor(
            self.file_sort_key,
            self.name,
            self.file_key,
        )


@dataclass(frozen=True, slots=True)
class SelectedGalleryFilePage:
    items: tuple[SelectedGalleryFile, ...]
    next_cursor: SelectedGalleryFileCursor | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "items", tuple(self.items))
        cursors = tuple(item.cursor for item in self.items)
        if tuple(sorted(cursors)) != cursors or len(set(cursors)) != len(cursors):
            raise ValueError("Selected gallery files must be strictly keyset ordered")
        if self.next_cursor is not None and (
            not cursors or self.next_cursor != cursors[-1]
        ):
            raise ValueError("next_cursor must identify the final selected file")


@dataclass(frozen=True, slots=True)
class PreparedProjectionArtifact:
    gallery_key: str
    artifact: CatalogArtifact

    def __post_init__(self) -> None:
        _validate_gallery_key(self.gallery_key)


@dataclass(frozen=True, slots=True)
class StagedProjectionSelection:
    """A projection selection without duplicated publication metadata."""

    gallery_key: str
    artifact: CatalogArtifact | None = None
    redownload_required: bool = False

    def __post_init__(self) -> None:
        _validate_gallery_key(self.gallery_key)

    @property
    def cursor(self) -> ProjectionSelectionCursor:
        return ProjectionSelectionCursor(self.gallery_key)


@dataclass(frozen=True, slots=True)
class ProjectionSelectionPage:
    items: tuple[StagedProjectionSelection, ...]
    next_cursor: ProjectionSelectionCursor | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "items", tuple(self.items))
        cursors = tuple(item.cursor for item in self.items)
        if tuple(sorted(cursors)) != cursors or len(set(cursors)) != len(cursors):
            raise ValueError("Projection selections must be strictly keyset ordered")
        if self.next_cursor is not None and (
            not cursors or self.next_cursor != cursors[-1]
        ):
            raise ValueError("next_cursor must identify the final selection")


@dataclass(frozen=True, slots=True)
class StagedProjectionSummary:
    artifacts_prepared: int
    artifacts_created: int
    artifacts_rebuilt: int
    selections_staged: int

    def __post_init__(self) -> None:
        if (
            min(
                self.artifacts_prepared,
                self.artifacts_created,
                self.artifacts_rebuilt,
                self.selections_staged,
            )
            < 0
        ):
            raise ValueError("Staged projection counts must not be negative")


class CBZStreamPreparer(Protocol):
    def publication_guard(self) -> AbstractContextManager[None]: ...

    def prepare_paged_stream(
        self,
        requests: Iterable[CBZStreamingPreparationRequest],
        *,
        result_sink: Callable[[CBZArtifact], None] | None = None,
        total: int | None = None,
    ) -> CBZPreparationSummary: ...

    def protect_for_publish(
        self,
        artifacts: Iterable[CBZArtifact],
        *,
        protection_id: str,
    ) -> None: ...


class StagedProjectionAdapter(Protocol):
    """Storage boundary required by :class:`StagedProjectionOrchestrator`.

    Every mutation must be idempotent.  Checkpoint advances must compare the
    supplied ``expected_after`` value and atomically reject a stale runner.
    Artifacts recorded by ``record_prepared_artifacts`` are provisional and
    must not be returned by ``page_projection_selections`` until its enclosing
    page checkpoint advances after filesystem protection succeeds.
    ``stage_projection_selections`` must persist its rows and advance the
    selection checkpoint in one transaction.
    """

    def get_projection_checkpoint(self, build_id: str) -> ProjectionCheckpoint: ...

    def page_selected_galleries(
        self,
        build_id: str,
        *,
        after: SelectedGalleryCursor | None,
        limit: int,
    ) -> SelectedGalleryPage: ...

    def page_selected_gallery_files(
        self,
        build_id: str,
        gallery_key: str,
        *,
        after: SelectedGalleryFileCursor | None,
        limit: int,
    ) -> SelectedGalleryFilePage: ...

    def record_prepared_artifacts(
        self,
        build_id: str,
        prepared: Sequence[PreparedProjectionArtifact],
        *,
        batch_id: str,
    ) -> None: ...

    def advance_artifact_checkpoint(
        self,
        build_id: str,
        *,
        expected_after: SelectedGalleryCursor | None,
        after: SelectedGalleryCursor,
        batch_id: str,
    ) -> None: ...

    def complete_artifact_preparation(
        self,
        build_id: str,
        *,
        expected_after: SelectedGalleryCursor | None,
    ) -> None: ...

    def page_projection_selections(
        self,
        build_id: str,
        *,
        after: ProjectionSelectionCursor | None,
        limit: int,
    ) -> ProjectionSelectionPage: ...

    def stage_projection_selections(
        self,
        build_id: str,
        selections: Sequence[StagedProjectionSelection],
        *,
        expected_after: ProjectionSelectionCursor | None,
        after: ProjectionSelectionCursor,
        batch_id: str,
    ) -> None: ...

    def complete_projection_staging(
        self,
        build_id: str,
        *,
        expected_after: ProjectionSelectionCursor | None,
    ) -> None: ...


class StagedProjectionOrchestrator:
    """Prepare selected CBZs and stage their projection in bounded pages."""

    def __init__(
        self,
        *,
        adapter: StagedProjectionAdapter,
        cbz: CBZStreamPreparer | None,
        gallery_page_size: int = 64,
        file_page_size: int = 512,
        selection_batch_size: int = 128,
    ) -> None:
        for label, value in (
            ("gallery_page_size", gallery_page_size),
            ("file_page_size", file_page_size),
            ("selection_batch_size", selection_batch_size),
        ):
            if value <= 0:
                raise ValueError(f"{label} must be positive")
        self._adapter = adapter
        self._cbz = cbz
        self._gallery_page_size = gallery_page_size
        self._file_page_size = file_page_size
        self._selection_batch_size = selection_batch_size

    def run(self, build_id: str) -> StagedProjectionSummary:
        if not build_id:
            raise ValueError("build_id must not be blank")
        prepared = created = rebuilt = staged = 0
        checkpoint = self._adapter.get_projection_checkpoint(build_id)

        if checkpoint.phase is StagedProjectionPhase.preparing_artifacts:
            if self._cbz is None:
                self._adapter.complete_artifact_preparation(
                    build_id,
                    expected_after=checkpoint.artifact_after,
                )
            else:
                artifact_summary = self._prepare_artifacts(
                    build_id,
                    checkpoint.artifact_after,
                )
                prepared += artifact_summary.prepared
                created += artifact_summary.created
                rebuilt += artifact_summary.rebuilt
            checkpoint = self._adapter.get_projection_checkpoint(build_id)

        if checkpoint.phase is StagedProjectionPhase.staging_selections:
            staged = self._stage_selections(build_id, checkpoint.selection_after)
            checkpoint = self._adapter.get_projection_checkpoint(build_id)

        if checkpoint.phase is not StagedProjectionPhase.complete:
            raise RuntimeError(
                "Projection adapter did not reach COMPLETE after orchestration: "
                f"phase={checkpoint.phase.value}"
            )
        return StagedProjectionSummary(
            artifacts_prepared=prepared,
            artifacts_created=created,
            artifacts_rebuilt=rebuilt,
            selections_staged=staged,
        )

    def _prepare_artifacts(
        self,
        build_id: str,
        after: SelectedGalleryCursor | None,
    ) -> CBZPreparationSummary:
        assert self._cbz is not None
        prepared = created = rebuilt = 0
        cursor = after
        while True:
            page = self._adapter.page_selected_galleries(
                build_id,
                after=cursor,
                limit=self._gallery_page_size,
            )
            if len(page.items) > self._gallery_page_size:
                raise RuntimeError("Selected gallery page exceeded its requested limit")
            if not page.items:
                self._adapter.complete_artifact_preparation(
                    build_id,
                    expected_after=cursor,
                )
                break
            if page.next_cursor is None:
                raise RuntimeError("A non-empty selected gallery page has no cursor")
            if cursor is not None and page.items[0].cursor <= cursor:
                raise RuntimeError("Selected gallery keyset cursor did not advance")

            key_by_identity = {
                (gallery.gallery_name, gallery.gid): gallery.gallery_key
                for gallery in page.items
            }
            if len(key_by_identity) != len(page.items):
                raise RuntimeError(
                    "Selected gallery page contains duplicate name/GID identities"
                )
            page_artifacts: list[CBZArtifact] = []
            prepared_by_gallery: dict[str, PreparedProjectionArtifact] = {}
            sunk_gallery_keys: set[str] = set()

            def durable_sink(cbz_artifact: CBZArtifact) -> None:
                identity = (
                    cbz_artifact.gallery.gallery_name,
                    cbz_artifact.gallery.gid,
                )
                gallery_key = key_by_identity.get(identity)
                if gallery_key is None:
                    raise RuntimeError(
                        "CBZ preparer returned an artifact outside the selected page: "
                        f"gallery={identity[0]!r} gid={identity[1]}"
                    )
                if gallery_key in sunk_gallery_keys:
                    raise RuntimeError(
                        "CBZ preparer returned more than one artifact for gallery "
                        f"key {gallery_key!r}"
                    )
                artifact = self._catalog_artifact(cbz_artifact)
                prepared_artifact = PreparedProjectionArtifact(gallery_key, artifact)
                sunk_gallery_keys.add(gallery_key)
                prepared_by_gallery[gallery_key] = prepared_artifact
                page_artifacts.append(cbz_artifact)

            # Hold the shared artifact-store guard across prepare, protection,
            # and the durable DB checkpoint.  The reconciler's nested state
            # locks are re-entrant; this closes the owned-but-unprotected gap in
            # which another finalizer could otherwise prune a fresh artifact.
            with self._cbz.publication_guard():
                summary = self._cbz.prepare_paged_stream(
                    self._preparation_requests(build_id, page.items),
                    result_sink=durable_sink,
                    total=len(page.items),
                )
                if sunk_gallery_keys != set(key_by_identity.values()):
                    raise RuntimeError(
                        "CBZ preparation completed without one artifact per selected "
                        "gallery"
                    )
                prepared_page = tuple(
                    prepared_by_gallery[gallery.gallery_key] for gallery in page.items
                )
                self._adapter.record_prepared_artifacts(
                    build_id,
                    prepared_page,
                    batch_id=self._batch_id(
                        build_id,
                        "PREPARED_ARTIFACTS",
                        prepared_page,
                    ),
                )
                self._cbz.protect_for_publish(
                    page_artifacts,
                    protection_id=build_id,
                )
                self._adapter.advance_artifact_checkpoint(
                    build_id,
                    expected_after=cursor,
                    after=page.next_cursor,
                    batch_id=self._batch_id(
                        build_id,
                        "ARTIFACT_PAGE",
                        cursor,
                        page.next_cursor,
                        prepared_page,
                    ),
                )
            cursor = page.next_cursor
            prepared += summary.prepared
            created += summary.created
            rebuilt += summary.rebuilt
        return CBZPreparationSummary(prepared, created, rebuilt)

    def _preparation_requests(
        self,
        build_id: str,
        galleries: Sequence[SelectedGallerySource],
    ) -> Iterator[CBZStreamingPreparationRequest]:
        for gallery in galleries:
            yield CBZStreamingPreparationRequest(
                metadata=CBZPreparationMetadata(
                    gallery=CBZGalleryDescriptor(
                        gallery_name=gallery.gallery_name,
                        gid=gallery.gid,
                        upload_time=gallery.upload_time,
                    ),
                    source_digest=gallery.source_sha256,
                    content_digest=gallery.content_sha256,
                ),
                open_files=partial(
                    self._gallery_files,
                    build_id,
                    gallery.gallery_key,
                ),
            )

    def _gallery_files(
        self,
        build_id: str,
        gallery_key: str,
    ) -> Iterator[CBZPreparationFile]:
        cursor: SelectedGalleryFileCursor | None = None
        while True:
            page = self._adapter.page_selected_gallery_files(
                build_id,
                gallery_key,
                after=cursor,
                limit=self._file_page_size,
            )
            if len(page.items) > self._file_page_size:
                raise RuntimeError("Selected file page exceeded its requested limit")
            if cursor is not None and page.items and page.items[0].cursor <= cursor:
                raise RuntimeError("Selected file keyset cursor did not advance")
            for source_file in page.items:
                if source_file.gallery_key != gallery_key:
                    raise RuntimeError(
                        "Selected file page crossed gallery boundaries: "
                        f"expected={gallery_key!r} actual={source_file.gallery_key!r}"
                    )
                yield CBZPreparationFile(
                    file_key=source_file.file_key,
                    file=ScannedFile(
                        path=source_file.path,
                        name=source_file.name,
                        size_bytes=source_file.size_bytes,
                        sha256=source_file.sha256,
                        signature=source_file.signature,
                    ),
                    excluded=source_file.excluded,
                )
            if page.next_cursor is None:
                break
            if not page.items or page.next_cursor == cursor:
                raise RuntimeError("Selected file keyset cursor did not advance")
            cursor = page.next_cursor

    def _stage_selections(
        self,
        build_id: str,
        after: ProjectionSelectionCursor | None,
    ) -> int:
        staged = 0
        cursor = after
        while True:
            page = self._adapter.page_projection_selections(
                build_id,
                after=cursor,
                limit=self._selection_batch_size,
            )
            if len(page.items) > self._selection_batch_size:
                raise RuntimeError("Projection selection page exceeded its limit")
            if not page.items:
                self._adapter.complete_projection_staging(
                    build_id,
                    expected_after=cursor,
                )
                break
            if page.next_cursor is None:
                raise RuntimeError(
                    "A non-empty projection selection page has no cursor"
                )
            if cursor is not None and page.items[0].cursor <= cursor:
                raise RuntimeError("Projection selection keyset cursor did not advance")
            self._adapter.stage_projection_selections(
                build_id,
                page.items,
                expected_after=cursor,
                after=page.next_cursor,
                batch_id=self._batch_id(
                    build_id,
                    "PROJECTION_SELECTIONS",
                    cursor,
                    page.next_cursor,
                    page.items,
                ),
            )
            staged += len(page.items)
            cursor = page.next_cursor
        return staged

    @staticmethod
    def _catalog_artifact(artifact: CBZArtifact) -> CatalogArtifact:
        _validate_sha256(artifact.sha256, label="CBZ SHA-256")
        return CatalogArtifact(
            artifact_id=(
                "urn:h2h:artifact:cbz:"
                f"{artifact.gallery.gid}:sha256:{artifact.sha256}"
            ),
            name=gallery_name_to_cbz_file_name(artifact.gallery.gallery_name),
            location=artifact.path,
            media_type=CBZ_MEDIA_TYPE,
            size_bytes=artifact.size_bytes,
            sha256=artifact.sha256,
            modified_at=artifact.modified_at,
        )

    @staticmethod
    def _batch_id(build_id: str, kind: str, *parts: object) -> str:
        payload = json.dumps(
            {
                "buildId": build_id,
                "kind": kind,
                "payload": _canonical_batch_value(parts),
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return f"{kind.casefold()}-{sha256(payload).hexdigest()}"
