"""Thin public-domain adapter from filesystem observations to h2hdb vNext."""

from __future__ import annotations

__all__ = ["VNextFilesystemSourceAdapter"]

import logging
from collections.abc import Callable
from functools import wraps
from itertools import batched
from typing import Concatenate, cast

from h2hdb import (
    ArtifactSourceRole,
    DirectoryObservation,
    FileContentReceipt,
    FileObservation,
    GalleryObservationDirectoryFileType,
    GalleryObservationMetadata,
    TagObservation,
    VNextIngestGalleryObservation,
    VNextIngestPage,
    VNextSourceCompletionMarker,
    VNextSourceDeferredError,
    VNextSourceQualification,
)

from .filesystem import (
    FILESYSTEM_OBSERVATION_VERSION,
    FilesystemArtifactSourceRole,
    FilesystemDirectoryObservation,
    FilesystemFileObservation,
    FilesystemGalleryMetadata,
    FilesystemGalleryObservation,
    FilesystemSource,
    FilesystemSourceChangedError,
)
from .source_performance import SourcePerformance
from .source_snapshot import SNAPSHOT_CAPTURE_PAGE_SIZE, SourceSnapshotStore

logger = logging.getLogger(__name__)

SourceGalleryQualifier = Callable[
    [FilesystemSource, tuple[str, ...], FilesystemGalleryObservation],
    VNextSourceQualification,
]


def _defer_source_changes[**Parameters, Result](
    operation: Callable[Concatenate[VNextFilesystemSourceAdapter, Parameters], Result],
) -> Callable[Concatenate[VNextFilesystemSourceAdapter, Parameters], Result]:
    @wraps(operation)
    def call(
        adapter: VNextFilesystemSourceAdapter,
        /,
        *args: Parameters.args,
        **kwargs: Parameters.kwargs,
    ) -> Result:
        try:
            return operation(adapter, *args, **kwargs)
        except FilesystemSourceChangedError as error:
            # All wrapped methods take a locator or its immutable observation.
            target = (
                args[0]
                if args
                else kwargs.get("locator_components", kwargs.get("observation"))
            )
            adapter._discard_snapshot(
                cast("tuple[str, ...] | VNextIngestGalleryObservation", target)
            )
            logger.debug(
                "Gallery source deferred: operation=%s reason=%s",
                operation.__name__,
                error,
            )
            raise VNextSourceDeferredError(str(error)) from error

    return call


class VNextFilesystemSourceAdapter:
    """Implement the public replayable source protocol without database access."""

    def __init__(
        self,
        source: FilesystemSource,
        *,
        qualify_gallery: SourceGalleryQualifier | None = None,
        snapshot: SourceSnapshotStore | None = None,
        performance: SourcePerformance | None = None,
    ) -> None:
        self._source = source
        self._qualify_gallery = qualify_gallery
        self._snapshot = snapshot
        self._source_performance = (
            performance if performance is not None else SourcePerformance()
        )

    def _discard_snapshot(
        self, target: tuple[str, ...] | VNextIngestGalleryObservation
    ) -> None:
        """Drop this turn's bytes when any stage defers the gallery."""
        if self._snapshot is not None:
            self._snapshot.discard_gallery(
                target.locator_components
                if isinstance(target, VNextIngestGalleryObservation)
                else target
            )

    def discard_gallery_observation(self, locator_components: tuple[str, ...]) -> None:
        """Release rejected attempt bytes even when the core detects the change."""
        if self._snapshot is not None:
            self._snapshot.discard_gallery(locator_components)

    @property
    def source_root_components(self) -> tuple[str, ...]:
        return self._source.source_root_components

    def list_gallery_locators(
        self,
        *,
        after_locator: tuple[str, ...] | None,
        limit: int,
    ) -> VNextIngestPage[tuple[str, ...]]:
        page = self._source.list_gallery_locators(
            after_locator=after_locator,
            limit=limit,
        )
        self._source_performance.add("locator_rows", len(page.items))
        return VNextIngestPage(
            page.items,
            None if page.terminal else page.items[-1],
            page.terminal,
        )

    @_defer_source_changes
    def gallery_exists(self, locator_components: tuple[str, ...]) -> bool:
        return self._source.gallery_exists(locator_components)

    @_defer_source_changes
    def observe_gallery(
        self,
        locator_components: tuple[str, ...],
    ) -> VNextIngestGalleryObservation:
        observed = self._source.observe_gallery(locator_components)
        qualification = VNextSourceQualification()
        if self._qualify_gallery is not None:
            with self._source_performance.phase("qualification"):
                qualification = self._qualify_gallery(
                    self._source, locator_components, observed
                )
            self._source_performance.add("qualified_galleries")
        return VNextIngestGalleryObservation(
            locator_components=locator_components,
            metadata=_metadata(observed.metadata),
            qualification=qualification,
        )

    @_defer_source_changes
    def observe_completion_marker(
        self,
        locator_components: tuple[str, ...],
    ) -> VNextSourceCompletionMarker:
        observed = self._source.observe_completion_marker(locator_components)
        self._source.revalidate_observed_gallery(locator_components)
        self._source_performance.add("completion_markers")
        return VNextSourceCompletionMarker(
            file=_file(observed),
            observation_version=FILESYSTEM_OBSERVATION_VERSION,
        )

    @_defer_source_changes
    def list_file_observations(
        self,
        observation: VNextIngestGalleryObservation,
        *,
        after_name_bytes: bytes | None,
        limit: int,
    ) -> VNextIngestPage[FileObservation]:
        observed, page = self._source.list_files(
            observation.locator_components,
            after_name=after_name_bytes,
            limit=limit,
        )
        self._require_metadata(observation, observed)
        captured: dict[bytes, FileContentReceipt] = {}
        if self._snapshot is not None:
            members = tuple(
                item
                for item in page.items
                if item.artifact_role is not FilesystemArtifactSourceRole.OTHER
            )
            with self._source_performance.phase("snapshot"):
                # Core FILE pages hold 256 rows. The disposable snapshot index
                # commits at most 128 members without changing that outer page.
                for batch in batched(members, SNAPSHOT_CAPTURE_PAGE_SIZE, strict=False):
                    contents = self._snapshot.capture_many(
                        observation.locator_components,
                        batch,
                        performance=self._source_performance,
                    )
                    captured.update(
                        (item.name_bytes, content)
                        for item, content in zip(batch, contents, strict=True)
                    )
                    self._source_performance.add(
                        "snapshot_bytes", sum(item.size_bytes for item in contents)
                    )
                    self._source_performance.add("snapshot_files", len(contents))
        items = tuple(
            _file(item, content=captured.get(item.name_bytes)) for item in page.items
        )
        self._source_performance.add("file_rows", len(items))
        return VNextIngestPage(
            items,
            None if page.terminal else items[-1].name_bytes,
            page.terminal,
        )

    @_defer_source_changes
    def list_directory_observations(
        self,
        observation: VNextIngestGalleryObservation,
        *,
        after_name_bytes: bytes | None,
        limit: int,
    ) -> VNextIngestPage[DirectoryObservation]:
        observed, page = self._source.list_directories(
            observation.locator_components,
            after_name=after_name_bytes,
            limit=limit,
        )
        self._require_metadata(observation, observed)
        items = tuple(_directory(item) for item in page.items)
        self._source_performance.add("directory_rows", len(items))
        return VNextIngestPage(
            items,
            None if page.terminal else items[-1].name_bytes,
            page.terminal,
        )

    @_defer_source_changes
    def list_tag_observations(
        self,
        observation: VNextIngestGalleryObservation,
        *,
        after_ordinal: int | None,
        limit: int,
    ) -> VNextIngestPage[TagObservation]:
        start = 0 if after_ordinal is None else after_ordinal + 1
        observed, page = self._source.list_tags(
            observation.locator_components,
            after_position=start,
            limit=limit,
        )
        self._require_metadata(observation, observed)
        items = tuple(
            TagObservation(namespace, value) for namespace, value in page.items
        )
        self._source_performance.add("tag_rows", len(items))
        return VNextIngestPage(
            items,
            None if page.terminal else start + len(items) - 1,
            page.terminal,
        )

    @staticmethod
    def _require_metadata(
        observation: VNextIngestGalleryObservation,
        reopened: FilesystemGalleryObservation,
    ) -> None:
        if not isinstance(observation, VNextIngestGalleryObservation):
            raise TypeError("observation must be VNextIngestGalleryObservation")
        if _metadata(reopened.metadata) != observation.metadata:
            raise FilesystemSourceChangedError(
                "gallery metadata changed between bounded source pages"
            )


def _metadata(value: FilesystemGalleryMetadata) -> GalleryObservationMetadata:
    return GalleryObservationMetadata(
        gid=value.gid,
        title=value.title,
        comment=value.comment,
        upload_account=value.upload_account,
        upload_time=value.upload_time,
        download_time=value.download_time,
        modified_time=value.modified_time,
        scan_observation_version=value.scan_observation_version,
        source_file_count=value.source_file_count,
        page_count=value.page_count,
    )


def _file(
    value: FilesystemFileObservation, *, content: FileContentReceipt | None = None
) -> FileObservation:
    if content is None:
        content = FileContentReceipt.from_parts(value.content_parts())
    source_stat = value.stat
    return FileObservation(
        name_bytes=value.name_bytes,
        content=content,
        artifact_role=ArtifactSourceRole(value.artifact_role.value),
        device=source_stat.device,
        inode=source_stat.inode,
        modified_ns=source_stat.modified_ns,
        changed_ns=source_stat.changed_ns,
    )


def _directory(value: FilesystemDirectoryObservation) -> DirectoryObservation:
    source_stat = value.stat
    return DirectoryObservation(
        name_bytes=value.name_bytes,
        size_bytes=source_stat.size_bytes,
        device=source_stat.device,
        inode=source_stat.inode,
        modified_ns=source_stat.modified_ns,
        changed_ns=source_stat.changed_ns,
        file_type=GalleryObservationDirectoryFileType(int(value.file_type)),
    )
