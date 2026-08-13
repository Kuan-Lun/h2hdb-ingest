"""Bounded filesystem-to-core source staging.

This module stops at the durable source/analysis boundary.  Activating the
source build is deliberately left to the joint catalog-projection publisher;
calling the core's source-only publish method here would expose a new source
snapshot beside an old user-facing catalog revision.
"""

__all__ = [
    "CatalogScopeMismatchError",
    "CoreFileHashCache",
    "FilesystemSourceStager",
]

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, is_dataclass
from datetime import datetime
from enum import Enum
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from h2hdb import (
    CatalogBuild,
    CatalogBuildBatchConflictError,
    CatalogBuildCoordinator,
    CatalogBuildPhase,
    CatalogBuildStateError,
    CatalogSourceDiscoveryCompletion,
    CatalogSourceFileChunk,
    CatalogSourceGalleryCompletion,
    CatalogSourceGalleryDiscovery,
    CatalogSourceGalleryHeader,
    GalleryIngestTurn,
    GallerySourceFile,
    GalleryTag,
)
from h2hdb import (
    FileHashCacheEntry as CoreFileHashCacheEntry,
)
from h2hdb import (
    FileHashCacheKey as CoreFileHashCacheKey,
)

from .models import (
    FileHashCacheEntry,
    FileHashCacheKey,
    FileStatSignature,
    FilesystemScanBatch,
    ScannedFile,
    ScannedGalleryChunk,
)
from .scanner import FileHashCache, FilesystemScanner, GalleryScanError

CORE_MAX_SOURCE_PAGE_SIZE = 200
STAGING_MAX_FILES_PER_TRANSACTION = 2_048
DEFAULT_HASH_CACHE_WRITE_BATCH_SIZE = 1_000


class CatalogScopeMismatchError(RuntimeError):
    """A working build belongs to different filesystem/policy semantics."""


def _jsonable(value: object) -> object:
    if is_dataclass(value) and not isinstance(value, type):
        return _jsonable(asdict(value))
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise TypeError(f"Unsupported batch payload value: {type(value).__name__}")


def _batch_id(kind: str, payload: object) -> str:
    digest = sha256(
        json.dumps(
            _jsonable(payload),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return f"{kind}:{digest}"


def _fingerprint(signature: FileStatSignature) -> str:
    return json.dumps(
        asdict(signature),
        sort_keys=True,
        separators=(",", ":"),
    )


def _core_cache_key(key: FileHashCacheKey) -> CoreFileHashCacheKey:
    return CoreFileHashCacheKey(
        source_key=f"{key.root_locator}\0{key.relative_locator}",
        fingerprint=_fingerprint(key.signature),
    )


class CoreFileHashCache(FileHashCache):
    """Adapt scanner cache batches to the turn-fenced core build API."""

    def __init__(
        self,
        coordinator: CatalogBuildCoordinator,
        *,
        write_batch_size: int = DEFAULT_HASH_CACHE_WRITE_BATCH_SIZE,
    ) -> None:
        if write_batch_size <= 0:
            raise ValueError("write_batch_size must be positive")
        self._coordinator = coordinator
        self._write_batch_size = write_batch_size
        self._build: CatalogBuild | None = None
        self._turn: GalleryIngestTurn | None = None
        self._pending: dict[CoreFileHashCacheKey, CoreFileHashCacheEntry] = {}

    def bind(self, build: CatalogBuild, turn: GalleryIngestTurn) -> None:
        self._build = build
        self._turn = turn

    def lookup(
        self,
        keys: Sequence[FileHashCacheKey],
        /,
    ) -> Iterable[FileHashCacheEntry]:
        core_by_source = {_core_cache_key(key): key for key in keys}
        found = self._coordinator.get_catalog_file_hashes(tuple(core_by_source))
        return tuple(
            FileHashCacheEntry(core_by_source[core_key], digest)
            for core_key, digest in found.items()
            if core_key in core_by_source
        )

    def remember(self, entries: Sequence[FileHashCacheEntry], /) -> None:
        for entry in entries:
            core_entry = CoreFileHashCacheEntry(
                _core_cache_key(entry.key),
                entry.sha256,
            )
            previous = self._pending.get(core_entry.key)
            if previous is not None and previous.sha256 != core_entry.sha256:
                raise GalleryScanError(
                    "A pending hash cache key has conflicting digests: "
                    f"{entry.key.relative_locator}"
                )
            self._pending[core_entry.key] = core_entry
        while len(self._pending) >= self._write_batch_size:
            self._flush_one(self._write_batch_size)

    def flush(self) -> None:
        """Persist every buffered digest in fixed-size fenced transactions."""

        while self._pending:
            self._flush_one(self._write_batch_size)

    def discard_pending(self) -> None:
        """Drop uncommitted cache hints after a failed scan attempt."""

        self._pending.clear()

    def _flush_one(self, limit: int) -> None:
        build = self._build
        turn = self._turn
        if build is None or turn is None:
            raise RuntimeError("The durable file hash cache is not bound to a build")
        keys = tuple(self._pending)[:limit]
        core_entries = tuple(self._pending[key] for key in keys)
        self._coordinator.cache_catalog_file_hashes(
            build,
            core_entries,
            batch_id=_batch_id("hash-cache", core_entries),
            ingest_turn=turn,
        )
        for key in keys:
            self._pending.pop(key, None)


class FilesystemSourceStager:
    """Advance one catalog build through bounded discovery and source staging."""

    def __init__(
        self,
        *,
        scanner: FilesystemScanner,
        coordinator: CatalogBuildCoordinator,
        hash_cache: CoreFileHashCache,
    ) -> None:
        self._scanner = scanner
        self._coordinator = coordinator
        self._hash_cache = hash_cache

    def begin_or_resume(
        self,
        *,
        scope_key: str,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuild:
        working = self._coordinator.get_working_catalog_build()
        if working is not None:
            adopted = self._coordinator.resume_catalog_build(
                scope_key=working.scope_key,
                ingest_turn=ingest_turn,
            )
            if adopted is None:
                raise RuntimeError(
                    "The working catalog build disappeared during resume"
                )
            if adopted.scope_key == scope_key:
                return adopted
            raise CatalogScopeMismatchError(
                "The unfinished catalog build belongs to a different ingest "
                "scope. Restore the previous filesystem and policy configuration "
                "to resume it, or explicitly abandon and clean up that build "
                f"before starting a new scope: build_id={adopted.build_id}"
            )
        resumed = self._coordinator.resume_catalog_build(
            scope_key=scope_key,
            ingest_turn=ingest_turn,
        )
        if resumed is not None:
            return resumed
        return self._coordinator.begin_catalog_build(
            scope_key=scope_key,
            ingest_turn=ingest_turn,
        )

    def stage(
        self,
        build: CatalogBuild,
        *,
        ingest_turn: GalleryIngestTurn,
    ) -> CatalogBuild:
        self._hash_cache.bind(build, ingest_turn)
        try:
            if build.phase is CatalogBuildPhase.discovering:
                build = self._discover(build, ingest_turn)
            if build.phase is CatalogBuildPhase.staging:
                build = self._stage_pending(build, ingest_turn)
        except (
            CatalogBuildBatchConflictError,
            CatalogBuildStateError,
            GalleryScanError,
        ) as error:
            self._hash_cache.discard_pending()
            try:
                self._coordinator.abandon_catalog_build(
                    build,
                    ingest_turn=ingest_turn,
                )
            except Exception as abandon_error:
                error.add_note(
                    "The failed catalog build could not be abandoned: "
                    f"{abandon_error!r}"
                )
            raise
        return build

    def validate(self, build: CatalogBuild) -> None:
        """Run a bounded, read-only clean pass over a completed source build.

        The first pass detects added, removed, renamed, or metadata-mutated
        galleries.  The second pass re-observes every direct gallery child and
        detects file membership or stat changes without reading file contents.
        Callers must run this after artifact preparation and immediately before
        sealing the build.  A mismatch deliberately fails the build instead of
        inferring removals from a partial observation.
        """

        if build.discovery_tree_sha256 is None:
            raise RuntimeError("A catalog build has no completed discovery tree")
        if build.expected_gallery_count is None:
            raise RuntimeError("A catalog build has no expected gallery count")

        discovery = self._scanner.discover(
            max_galleries=min(
                self._scanner.max_galleries,
                CORE_MAX_SOURCE_PAGE_SIZE,
            )
        )
        for _batch in discovery.iter_batches():
            pass
        summary = discovery.finish()
        if (
            summary.gallery_count != build.expected_gallery_count
            or summary.tree_observation_sha256 != build.discovery_tree_sha256
        ):
            raise GalleryScanError(
                "Filesystem gallery tree changed after source staging: "
                f"expected_count={build.expected_gallery_count} "
                f"actual_count={summary.gallery_count}"
            )

        offset = 0
        observed = 0
        while True:
            page = self._coordinator.list_catalog_build_sources(
                build.build_id,
                offset=offset,
                limit=CORE_MAX_SOURCE_PAGE_SIZE,
            )
            if not page.galleries:
                break
            for gallery in page.galleries:
                expected_count = gallery.directory_entry_count
                expected_digest = gallery.directory_observation_sha256
                if expected_count is None or expected_digest is None:
                    raise RuntimeError(
                        "A staged gallery has no durable directory observation: "
                        f"{gallery.gallery_name!r}"
                    )
                actual = self._scanner.observe_gallery(gallery.source_locator)
                if (
                    actual.directory_entry_count != expected_count
                    or actual.directory_observation_sha256 != expected_digest
                ):
                    raise GalleryScanError(
                        "Gallery changed after source staging: "
                        f"gallery={gallery.gallery_name!r}"
                    )
                observed += 1
            offset += len(page.galleries)

        if observed != build.expected_gallery_count:
            raise GalleryScanError(
                "Staged gallery count changed during final validation: "
                f"expected={build.expected_gallery_count} actual={observed}"
            )

    def _discover(
        self,
        build: CatalogBuild,
        turn: GalleryIngestTurn,
    ) -> CatalogBuild:
        if build.discovery_epoch is None:
            raise RuntimeError("A discovering catalog build has no discovery epoch")
        session = self._scanner.discover(
            max_galleries=min(
                self._scanner.max_galleries,
                CORE_MAX_SOURCE_PAGE_SIZE,
            ),
            scan_attempt=build.discovery_epoch,
        )
        for batch in session.iter_batches():
            discoveries = tuple(
                CatalogSourceGalleryDiscovery(
                    gallery_name=gallery.gallery_name,
                    source_locator=gallery.relative_folder,
                    metadata_fingerprint=_fingerprint(gallery.metadata_signature),
                )
                for gallery in batch.galleries
            )
            self._coordinator.discover_catalog_sources(
                build,
                discoveries,
                batch_id=_batch_id("discovery", discoveries),
                ingest_turn=turn,
            )
        summary = session.finish()
        return self._coordinator.complete_catalog_discovery(
            build,
            completion=CatalogSourceDiscoveryCompletion(
                scan_attempt=summary.scan_attempt,
                gallery_count=summary.gallery_count,
                tree_observation_sha256=summary.tree_observation_sha256,
            ),
            ingest_turn=turn,
        )

    def _stage_pending(
        self,
        build: CatalogBuild,
        turn: GalleryIngestTurn,
    ) -> CatalogBuild:
        after: str | None = None
        while True:
            page = self._coordinator.list_pending_catalog_galleries(
                build.build_id,
                after_gallery_name=after,
                limit=min(
                    self._scanner.max_galleries,
                    CORE_MAX_SOURCE_PAGE_SIZE,
                ),
            )
            if not page.galleries:
                break
            relative_folders = tuple(
                gallery.source_locator for gallery in page.galleries
            )
            scan_attempt = f"{build.build_id}:{uuid4().hex}"
            for batch in self._scanner.iter_batches(
                max_galleries=min(
                    self._scanner.max_galleries,
                    CORE_MAX_SOURCE_PAGE_SIZE,
                ),
                max_files=min(
                    self._scanner.max_files,
                    STAGING_MAX_FILES_PER_TRANSACTION,
                ),
                scan_attempt=scan_attempt,
                relative_folders=relative_folders,
            ):
                self._hash_cache.flush()
                self._stage_scan_batch(build, batch, turn)
            after = page.galleries[-1].gallery_name
        return self._coordinator.complete_catalog_source_staging(
            build,
            ingest_turn=turn,
        )

    def _stage_scan_batch(
        self,
        build: CatalogBuild,
        batch: FilesystemScanBatch,
        turn: GalleryIngestTurn,
    ) -> None:
        chunks_by_gallery: dict[str, list[ScannedGalleryChunk]] = {}
        for chunk in batch.chunks:
            chunks_by_gallery.setdefault(chunk.manifest.gallery_name, []).append(chunk)
        headers = tuple(
            self._header(chunks[0]) for chunks in chunks_by_gallery.values()
        )
        file_chunks = tuple(
            CatalogSourceFileChunk(
                gallery_name=chunk.manifest.gallery_name,
                files=tuple(
                    self._source_file(source_file) for source_file in chunk.files
                ),
            )
            for chunk in batch.chunks
            if chunk.files
        )
        completions = tuple(
            self._completion(chunk)
            for chunk in batch.chunks
            if chunk.completion is not None
        )
        if headers:
            self._coordinator.stage_catalog_gallery_headers(
                build,
                headers,
                batch_id=_batch_id("headers", headers),
                ingest_turn=turn,
            )
        if file_chunks:
            self._coordinator.stage_catalog_file_chunks(
                build,
                file_chunks,
                batch_id=_batch_id("files", file_chunks),
                ingest_turn=turn,
            )
        if completions:
            self._coordinator.complete_catalog_galleries(
                build,
                completions,
                batch_id=_batch_id("completions", completions),
                ingest_turn=turn,
            )

    @staticmethod
    def _header(chunk: ScannedGalleryChunk) -> CatalogSourceGalleryHeader:
        manifest = chunk.manifest
        return CatalogSourceGalleryHeader(
            gallery_name=manifest.gallery_name,
            gid=manifest.gid,
            title=manifest.title,
            comment=manifest.summary,
            upload_account=manifest.upload_account,
            upload_time=manifest.upload_time,
            download_time=manifest.download_time,
            modified_time=manifest.modified_time,
            tags=tuple(
                GalleryTag(name, value) for name, value in dict.fromkeys(manifest.tags)
            ),
        )

    @staticmethod
    def _source_file(source_file: ScannedFile) -> GallerySourceFile:
        signature = source_file.signature
        if source_file.relative_locator is None or signature is None:
            raise GalleryScanError(
                f"Scanned file is missing its durable observation: {source_file.path}"
            )
        return GallerySourceFile(
            name=source_file.name,
            size_bytes=source_file.size_bytes,
            sha256=source_file.sha256,
            relative_locator=source_file.relative_locator,
            device=signature.device,
            inode=signature.inode,
            modified_ns=signature.modified_ns,
            changed_ns=signature.changed_ns,
        )

    @staticmethod
    def _completion(chunk: ScannedGalleryChunk) -> CatalogSourceGalleryCompletion:
        completion = chunk.completion
        if completion is None:
            raise ValueError("Only a final gallery chunk can be completed")
        return CatalogSourceGalleryCompletion(
            gallery_name=chunk.manifest.gallery_name,
            expected_file_count=completion.source_file_count,
            scan_observation_sha256=completion.scan_observation_sha256,
            scan_observation_version=completion.scan_observation_version,
            canonical_source_manifest_sha256=(
                completion.canonical_source_manifest_sha256
            ),
            canonical_source_manifest_version=(
                completion.canonical_source_manifest_version
            ),
            raw_content_sha256=completion.raw_content_sha256,
            metadata_sha256=completion.metadata_sha256,
            page_count=completion.pages,
            directory_entry_count=completion.directory_entry_count,
            directory_observation_sha256=(completion.directory_observation_sha256),
        )
