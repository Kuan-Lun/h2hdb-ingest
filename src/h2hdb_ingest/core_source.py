"""Thin public-domain adapter from filesystem observations to h2hdb vNext."""

from __future__ import annotations

__all__ = ["VNextFilesystemSourceAdapter"]

from h2hdb import (
    DirectoryObservation,
    FileContentReceipt,
    FileObservation,
    GalleryObservationDirectoryFileType,
    GalleryObservationMetadata,
    TagObservation,
    VNextIngestGalleryObservation,
    VNextIngestPage,
)

from .filesystem import (
    FilesystemDirectoryObservation,
    FilesystemFileObservation,
    FilesystemGalleryMetadata,
    FilesystemGalleryObservation,
    FilesystemObservationError,
    FilesystemSource,
)


class VNextFilesystemSourceAdapter:
    """Implement the public replayable source protocol without database access."""

    def __init__(self, source: FilesystemSource) -> None:
        self._source = source

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
        return VNextIngestPage(
            page.items,
            None if page.terminal else page.items[-1],
            page.terminal,
        )

    def observe_gallery(
        self,
        locator_components: tuple[str, ...],
    ) -> VNextIngestGalleryObservation:
        observed = self._source.observe_gallery(locator_components)
        return VNextIngestGalleryObservation(
            locator_components=locator_components,
            metadata=_metadata(observed.metadata),
        )

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
        items = tuple(_file(item) for item in page.items)
        return VNextIngestPage(
            items,
            None if page.terminal else items[-1].name_bytes,
            page.terminal,
        )

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
        return VNextIngestPage(
            items,
            None if page.terminal else items[-1].name_bytes,
            page.terminal,
        )

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
            raise FilesystemObservationError(
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


def _file(value: FilesystemFileObservation) -> FileObservation:
    content = FileContentReceipt.from_parts(value.content_parts())
    source_stat = value.stat
    return FileObservation(
        name_bytes=value.name_bytes,
        content=content,
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
