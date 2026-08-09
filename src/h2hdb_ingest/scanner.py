from __future__ import annotations

__all__ = [
    "FileHashCache",
    "FilesystemDiscoverySession",
    "FilesystemScanner",
    "GalleryScanError",
]

import json
import logging
import os
from collections import OrderedDict
from collections.abc import Callable, Iterable, Iterator, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from hashlib import sha256
from itertools import islice
from pathlib import Path, PurePosixPath
from stat import S_IFMT, S_ISREG
from time import monotonic
from typing import Protocol
from uuid import uuid4

from h2h_galleryinfo_parser import GalleryInfoParser
from h2h_galleryinfo_parser import parse_galleryinfo as parse_galleryinfo

from .models import (
    FileHashCacheEntry,
    FileHashCacheKey,
    FileStatSignature,
    FilesystemDiscoveryBatch,
    FilesystemDiscoverySummary,
    FilesystemGalleryDiscovery,
    FilesystemGalleryObservation,
    FilesystemScanBatch,
    ScannedFile,
    ScannedGallery,
    ScannedGalleryChunk,
    ScannedGalleryCompletion,
    ScannedGalleryManifest,
)

GALLERY_INFO_NAME = "galleryinfo.txt"
DEFAULT_PROGRESS_INTERVAL_SECONDS = 60.0
DEFAULT_MAX_GALLERIES = 128
DEFAULT_MAX_FILES = 2_048
SOURCE_MANIFEST_VERSION = 2
_DIGEST_MODULUS = 1 << 256
logger = logging.getLogger(__name__)


class GalleryScanError(RuntimeError):
    pass


class FileHashCache(Protocol):
    """Bulk cache boundary implemented by durable catalog-build storage."""

    def lookup(
        self,
        keys: Sequence[FileHashCacheKey],
        /,
    ) -> Iterable[FileHashCacheEntry]: ...

    def remember(self, entries: Sequence[FileHashCacheEntry], /) -> None: ...


class _MemoryFileHashCache:
    """Compatibility cache for callers that have not supplied durable storage."""

    def __init__(self, *, max_entries: int) -> None:
        self._max_entries = max_entries
        self._entries: OrderedDict[tuple[str, str], FileHashCacheEntry] = OrderedDict()

    def lookup(
        self,
        keys: Sequence[FileHashCacheKey],
        /,
    ) -> Iterable[FileHashCacheEntry]:
        result: list[FileHashCacheEntry] = []
        for key in keys:
            locator = (key.root_locator, key.relative_locator)
            entry = self._entries.get(locator)
            if entry is not None and entry.key == key:
                self._entries.move_to_end(locator)
                result.append(entry)
        return tuple(result)

    def remember(self, entries: Sequence[FileHashCacheEntry], /) -> None:
        for entry in entries:
            key = entry.key
            locator = (key.root_locator, key.relative_locator)
            self._entries[locator] = entry
            self._entries.move_to_end(locator)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)


@dataclass(frozen=True, slots=True)
class _CachedHash:
    signature: FileStatSignature
    scanned_file: ScannedFile


@dataclass(frozen=True, slots=True)
class _FileCandidate:
    path: Path
    key: FileHashCacheKey


@dataclass(slots=True)
class _ScanProgress:
    started_at: float
    galleries_completed: int = 0
    files_discovered: int = 0
    hashes_reused: int = 0
    hashes_read: int = 0


@dataclass(frozen=True, slots=True)
class _DirectoryObservationSnapshot:
    entry_count: int
    digest: str


class _DirectoryObservationAccumulator:
    """Constant-space, order-independent observation of every direct child."""

    def __init__(self) -> None:
        self._entry_count = 0
        self._sum = 0
        self._xor = 0

    def add(
        self,
        *,
        name: str,
        signature: FileStatSignature,
        file_type: int,
    ) -> None:
        record = json.dumps(
            {
                "changed_ns": signature.changed_ns,
                "device": signature.device,
                "file_type": file_type,
                "inode": signature.inode,
                "modified_ns": signature.modified_ns,
                "name": name,
                "size_bytes": signature.size_bytes,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        leaf = sha256(b"h2h-directory-entry-v1\0" + record).digest()
        value = int.from_bytes(leaf)
        self._sum = (self._sum + value) % _DIGEST_MODULUS
        self._xor ^= value
        self._entry_count += 1

    def snapshot(self) -> _DirectoryObservationSnapshot:
        digest = sha256(b"h2h-directory-observation-v1\0")
        digest.update(self._entry_count.to_bytes(16, "big"))
        digest.update(self._sum.to_bytes(32, "big"))
        digest.update(self._xor.to_bytes(32, "big"))
        return _DirectoryObservationSnapshot(self._entry_count, digest.hexdigest())


class _DiscoveryObservationAccumulator:
    """Constant-space, order-independent observation of discovered galleries."""

    def __init__(self) -> None:
        self._gallery_count = 0
        self._sum = 0
        self._xor = 0

    def add(self, discovery: FilesystemGalleryDiscovery) -> None:
        signature = discovery.metadata_signature
        record = json.dumps(
            {
                "changed_ns": signature.changed_ns,
                "device": signature.device,
                "gallery_name": discovery.gallery_name,
                "inode": signature.inode,
                "modified_ns": signature.modified_ns,
                "relative_folder": discovery.relative_folder,
                "size_bytes": signature.size_bytes,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        value = int.from_bytes(sha256(b"h2h-gallery-discovery-v1\0" + record).digest())
        self._sum = (self._sum + value) % _DIGEST_MODULUS
        self._xor ^= value
        self._gallery_count += 1

    def finish(self, scan_attempt: str) -> FilesystemDiscoverySummary:
        digest = sha256(b"h2h-gallery-tree-observation-v1\0")
        digest.update(self._gallery_count.to_bytes(16, "big"))
        digest.update(self._sum.to_bytes(32, "big"))
        digest.update(self._xor.to_bytes(32, "big"))
        return FilesystemDiscoverySummary(
            scan_attempt=scan_attempt,
            gallery_count=self._gallery_count,
            tree_observation_sha256=digest.hexdigest(),
        )


class _ManifestDigestAccumulator:
    """Order-independent, constant-space source manifest digest version 2."""

    def __init__(self) -> None:
        self._source_count = 0
        self._source_sum = 0
        self._source_xor = 0

    @staticmethod
    def _add_digest(total: int, exclusive_or: int, digest: bytes) -> tuple[int, int]:
        value = int.from_bytes(digest)
        return (total + value) % _DIGEST_MODULUS, exclusive_or ^ value

    def add(self, source_file: ScannedFile) -> None:
        source_record = json.dumps(
            {
                "name": source_file.name,
                "sha256": source_file.sha256,
                "size": source_file.size_bytes,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        source_leaf = sha256(b"h2h-source-file-v2\0" + source_record).digest()
        self._source_sum, self._source_xor = self._add_digest(
            self._source_sum,
            self._source_xor,
            source_leaf,
        )
        self._source_count += 1

    @staticmethod
    def _finish(
        domain: bytes,
        count: int,
        total: int,
        exclusive_or: int,
        *context: bytes,
    ) -> str:
        digest = sha256(domain)
        for value in context:
            digest.update(value)
        digest.update(count.to_bytes(16, "big"))
        digest.update(total.to_bytes(32, "big"))
        digest.update(exclusive_or.to_bytes(32, "big"))
        return digest.hexdigest()

    def source_digest(self, metadata_sha256: str) -> str:
        return self._finish(
            b"h2h-source-manifest-v2\0",
            self._source_count,
            self._source_sum,
            self._source_xor,
            bytes.fromhex(metadata_sha256),
        )

    @property
    def source_file_count(self) -> int:
        return self._source_count


def _signature_from_stat(stat: os.stat_result) -> FileStatSignature:
    return FileStatSignature(
        device=stat.st_dev,
        inode=stat.st_ino,
        size_bytes=stat.st_size,
        modified_ns=stat.st_mtime_ns,
        changed_ns=stat.st_ctime_ns,
    )


def _file_signature(path: Path) -> FileStatSignature:
    return _signature_from_stat(path.stat())


def _hash_file(path: Path) -> _CachedHash:
    before = _file_signature(path)
    digest = sha256()
    size = 0
    with path.open("rb") as source:
        while chunk := source.read(4 * 1024 * 1024):
            size += len(chunk)
            digest.update(chunk)
    after = _file_signature(path)
    if after != before or size != after.size_bytes:
        raise GalleryScanError(f"Source file changed while it was hashed: {path}")
    return _CachedHash(
        signature=after,
        scanned_file=ScannedFile(path, path.name, size, digest.hexdigest()),
    )


class FilesystemDiscoverySession:
    """Single-use lazy discovery pass with a completion descriptor."""

    def __init__(
        self,
        scanner: FilesystemScanner,
        *,
        scan_attempt: str,
        max_galleries: int,
    ) -> None:
        self._scanner = scanner
        self._scan_attempt = scan_attempt
        self._max_galleries = max_galleries
        self._started = False
        self._summary: FilesystemDiscoverySummary | None = None

    def iter_batches(self) -> Iterator[FilesystemDiscoveryBatch]:
        if self._started:
            raise RuntimeError("a filesystem discovery session is single-use")
        self._started = True
        started_at = monotonic()
        observation = _DiscoveryObservationAccumulator()
        batch: list[FilesystemGalleryDiscovery] = []
        self._scanner._event_logger(
            f"Filesystem discovery started: root={self._scanner._root}"
        )
        for gallery_info_path in self._scanner._iter_gallery_info_paths(
            self._scanner._root
        ):
            folder = gallery_info_path.parent
            discovery = FilesystemGalleryDiscovery(
                relative_folder=folder.relative_to(self._scanner._root).as_posix()
                or ".",
                gallery_name=folder.name,
                metadata_signature=_file_signature(gallery_info_path),
            )
            observation.add(discovery)
            batch.append(discovery)
            if len(batch) == self._max_galleries:
                yield FilesystemDiscoveryBatch(tuple(batch))
                batch.clear()
        if batch:
            yield FilesystemDiscoveryBatch(tuple(batch))
        self._summary = observation.finish(self._scan_attempt)
        self._scanner._event_logger(
            "Filesystem discovery completed: "
            f"galleries={self._summary.gallery_count} "
            f"elapsed_s={monotonic() - started_at:.3f}"
        )

    def finish(self) -> FilesystemDiscoverySummary:
        if self._summary is None:
            raise RuntimeError(
                "filesystem discovery must be fully consumed before it can finish"
            )
        return self._summary


class FilesystemScanner:
    def __init__(
        self,
        root: Path,
        *,
        hash_workers: int,
        hash_cache: FileHashCache | None = None,
        max_galleries: int = DEFAULT_MAX_GALLERIES,
        max_files: int = DEFAULT_MAX_FILES,
        event_logger: Callable[[str], None] | None = None,
        progress_interval_seconds: float = DEFAULT_PROGRESS_INTERVAL_SECONDS,
    ) -> None:
        if hash_workers <= 0:
            raise ValueError("hash_workers must be positive")
        if max_galleries <= 0:
            raise ValueError("max_galleries must be positive")
        if max_files <= 0:
            raise ValueError("max_files must be positive")
        if progress_interval_seconds <= 0:
            raise ValueError("progress_interval_seconds must be positive")
        self._root = root
        self._root_locator = root.resolve(strict=False).as_posix()
        self._hash_workers = hash_workers
        self._hash_cache = (
            hash_cache
            if hash_cache is not None
            else _MemoryFileHashCache(max_entries=max(4_096, max_files * 4))
        )
        self._max_galleries = max_galleries
        self._max_files = max_files
        self._event_logger = event_logger or logger.info
        self._progress_interval_seconds = progress_interval_seconds

    @property
    def root_locator(self) -> str:
        return self._root_locator

    @property
    def max_galleries(self) -> int:
        return self._max_galleries

    @property
    def max_files(self) -> int:
        return self._max_files

    def iter_discovery_batches(
        self,
        *,
        max_galleries: int | None = None,
    ) -> Iterator[FilesystemDiscoveryBatch]:
        """Discover gallery folders without parsing metadata or reading source files.

        Durable build storage requires discovery to finish before it can decide
        which galleries remain pending.  Keeping this as a separate lazy pass
        avoids retaining every gallery path merely to satisfy that barrier.
        """

        gallery_limit = self._max_galleries if max_galleries is None else max_galleries
        if gallery_limit <= 0:
            raise ValueError("max_galleries must be positive")

        yield from self.discover(max_galleries=gallery_limit).iter_batches()

    def discover(
        self,
        *,
        max_galleries: int | None = None,
        scan_attempt: str | None = None,
    ) -> FilesystemDiscoverySession:
        gallery_limit = self._max_galleries if max_galleries is None else max_galleries
        if gallery_limit <= 0:
            raise ValueError("max_galleries must be positive")
        return FilesystemDiscoverySession(
            self,
            scan_attempt=scan_attempt or uuid4().hex,
            max_galleries=gallery_limit,
        )

    def observe_gallery(self, relative_folder: str) -> FilesystemGalleryObservation:
        """Re-stat one gallery for the clean validation pass before publication."""

        folder = self._gallery_folder(relative_folder)
        observation = self._observe_directory(folder)
        return FilesystemGalleryObservation(
            relative_folder=relative_folder,
            directory_entry_count=observation.entry_count,
            directory_observation_sha256=observation.digest,
        )

    def iter_batches(
        self,
        *,
        max_galleries: int | None = None,
        max_files: int | None = None,
        scan_attempt: str | None = None,
        relative_folders: Iterable[str] | None = None,
    ) -> Iterator[FilesystemScanBatch]:
        """Yield bounded chunks suitable for durable staging writes.

        ``scan_attempt`` should be the durable build identifier when a caller has
        one.  Otherwise a process-local unique identifier is generated.  A huge
        gallery is split across chunks, and only its final chunk carries the
        digests that allow storage to seal it.
        """

        gallery_limit = self._max_galleries if max_galleries is None else max_galleries
        file_limit = self._max_files if max_files is None else max_files
        if gallery_limit <= 0:
            raise ValueError("max_galleries must be positive")
        if file_limit <= 0:
            raise ValueError("max_files must be positive")

        attempt = scan_attempt or uuid4().hex
        progress = _ScanProgress(started_at=monotonic())
        self._event_logger(f"Filesystem scan started: root={self._root}")
        batch_chunks: list[ScannedGalleryChunk] = []
        batch_attempts: set[str] = set()
        batch_file_count = 0

        with ThreadPoolExecutor(max_workers=self._hash_workers) as executor:
            gallery_info_paths = (
                self._iter_gallery_info_paths(self._root)
                if relative_folders is None
                else self._gallery_info_paths(relative_folders)
            )
            chunks = self._iter_gallery_chunks(
                executor,
                gallery_info_paths=gallery_info_paths,
                scan_attempt=attempt,
                max_files=file_limit,
                progress=progress,
            )
            for chunk in chunks:
                gallery_attempt = chunk.manifest.gallery_attempt
                new_gallery = gallery_attempt not in batch_attempts
                exceeds_gallery_limit = (
                    new_gallery and len(batch_attempts) >= gallery_limit
                )
                exceeds_file_limit = batch_file_count + len(chunk.files) > file_limit
                if batch_chunks and (exceeds_gallery_limit or exceeds_file_limit):
                    yield FilesystemScanBatch(tuple(batch_chunks))
                    batch_chunks.clear()
                    batch_attempts.clear()
                    batch_file_count = 0
                batch_chunks.append(chunk)
                batch_attempts.add(gallery_attempt)
                batch_file_count += len(chunk.files)

                if (
                    len(batch_attempts) >= gallery_limit
                    or batch_file_count >= file_limit
                ):
                    yield FilesystemScanBatch(tuple(batch_chunks))
                    batch_chunks.clear()
                    batch_attempts.clear()
                    batch_file_count = 0

            if batch_chunks:
                yield FilesystemScanBatch(tuple(batch_chunks))

        self._event_logger(
            "Filesystem scan completed: "
            f"galleries={progress.galleries_completed} "
            f"files={progress.files_discovered} "
            f"elapsed_s={monotonic() - progress.started_at:.3f} "
            f"hashes_reused={progress.hashes_reused} "
            f"hashes_read={progress.hashes_read}"
        )

    def scan(self) -> tuple[ScannedGallery, ...]:
        """Compatibility API that deliberately materializes the streamed scan."""

        result: list[ScannedGallery] = []
        manifest: ScannedGalleryManifest | None = None
        files: list[ScannedFile] = []
        for batch in self.iter_batches():
            for chunk in batch.chunks:
                if manifest is None:
                    manifest = chunk.manifest
                elif manifest.gallery_attempt != chunk.manifest.gallery_attempt:
                    raise GalleryScanError(
                        f"Gallery chunks ended without a completion record: "
                        f"{manifest.folder}"
                    )
                files.extend(chunk.files)
                if chunk.complete:
                    assert chunk.completion is not None
                    result.append(
                        self._build_legacy_gallery(manifest, files, chunk.completion)
                    )
                    manifest = None
                    files = []
        if manifest is not None:
            raise GalleryScanError(
                f"Gallery chunks ended without a completion record: {manifest.folder}"
            )
        result.sort(key=lambda gallery: gallery.folder.as_posix())
        return tuple(result)

    def _iter_gallery_chunks(
        self,
        executor: ThreadPoolExecutor,
        *,
        gallery_info_paths: Iterable[Path],
        scan_attempt: str,
        max_files: int,
        progress: _ScanProgress,
    ) -> Iterator[ScannedGalleryChunk]:
        for gallery_info_path in gallery_info_paths:
            folder = gallery_info_path.parent
            try:
                metadata_before_parse = _file_signature(gallery_info_path)
                gallery = parse_galleryinfo(folder)
                metadata_after_parse = _file_signature(gallery_info_path)
            except Exception as error:
                raise GalleryScanError(
                    f"Unable to parse {gallery_info_path}: {error}"
                ) from error
            if metadata_before_parse != metadata_after_parse:
                raise GalleryScanError(
                    f"Gallery metadata changed while it was parsed: {gallery_info_path}"
                )
            relative_folder = folder.relative_to(self._root).as_posix() or "."
            gallery_attempt = sha256(
                f"{scan_attempt}\0{relative_folder}".encode()
            ).hexdigest()
            manifest = self._to_manifest(
                gallery,
                gallery_attempt=gallery_attempt,
                relative_folder=relative_folder,
            )
            digest = _ManifestDigestAccumulator()
            initial_observation = _DirectoryObservationAccumulator()
            metadata_sha256: str | None = None
            metadata_seen = False
            had_chunk = False
            candidate_chunks = self._iter_candidate_chunks(
                folder,
                max_files,
                progress,
                initial_observation,
            )
            for chunk_index, (candidates, is_last) in enumerate(candidate_chunks):
                had_chunk = True
                scanned_files = self._scan_candidate_chunk(
                    executor,
                    candidates,
                    progress,
                )
                for candidate, source_file in zip(
                    candidates, scanned_files, strict=True
                ):
                    digest.add(source_file)
                    if candidate.path == gallery_info_path:
                        if candidate.key.signature != metadata_after_parse:
                            raise GalleryScanError(
                                "Gallery metadata changed between parsing and hashing: "
                                f"{gallery_info_path}"
                            )
                        metadata_seen = True
                        metadata_sha256 = source_file.sha256

                if is_last:
                    if not metadata_seen or metadata_sha256 is None:
                        raise GalleryScanError(
                            f"Gallery metadata disappeared during scan: {folder}"
                        )
                    final_observation = self._observe_directory(folder)
                    if final_observation != initial_observation.snapshot():
                        raise GalleryScanError(
                            f"Gallery directory changed during scan: {folder}"
                        )
                    observation = final_observation
                    yield ScannedGalleryChunk(
                        manifest=manifest,
                        chunk_index=chunk_index,
                        files=scanned_files,
                        completion=ScannedGalleryCompletion(
                            source_manifest_version=SOURCE_MANIFEST_VERSION,
                            metadata_sha256=metadata_sha256,
                            scan_observation_sha256=digest.source_digest(
                                metadata_sha256
                            ),
                            source_file_count=digest.source_file_count,
                            pages=max(0, digest.source_file_count - 1),
                            directory_entry_count=observation.entry_count,
                            directory_observation_sha256=observation.digest,
                        ),
                    )
                    progress.galleries_completed += 1
                else:
                    yield ScannedGalleryChunk(
                        manifest=manifest,
                        chunk_index=chunk_index,
                        files=scanned_files,
                    )
            if not had_chunk:
                raise GalleryScanError(
                    f"Gallery metadata disappeared during scan: {folder}"
                )

    def _gallery_info_paths(self, relative_folders: Iterable[str]) -> Iterator[Path]:
        for relative_folder in relative_folders:
            yield self._gallery_folder(relative_folder) / GALLERY_INFO_NAME

    def _gallery_folder(self, relative_folder: str) -> Path:
        locator = PurePosixPath(relative_folder)
        if not relative_folder or locator.is_absolute() or ".." in locator.parts:
            raise GalleryScanError(
                f"Invalid gallery relative folder: {relative_folder!r}"
            )
        return self._root.joinpath(*locator.parts)

    def _iter_gallery_info_paths(self, directory: Path) -> Iterator[Path]:
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            yield from self._iter_gallery_info_paths(Path(entry.path))
                        elif entry.name == GALLERY_INFO_NAME and entry.is_file():
                            yield Path(entry.path)
                    except OSError as error:
                        raise GalleryScanError(
                            f"Unable to inspect filesystem entry {entry.path}: {error}"
                        ) from error
        except GalleryScanError:
            raise
        except OSError as error:
            raise GalleryScanError(
                f"Unable to discover galleries below {directory}: {error}"
            ) from error

    def _iter_candidate_chunks(
        self,
        folder: Path,
        max_files: int,
        progress: _ScanProgress,
        observation: _DirectoryObservationAccumulator,
    ) -> Iterator[tuple[tuple[_FileCandidate, ...], bool]]:
        candidates = iter(self._iter_file_candidates(folder, progress, observation))
        current = list(islice(candidates, max_files))
        while current:
            try:
                next_candidate = next(candidates)
            except StopIteration:
                yield tuple(current), True
                return
            yield tuple(current), False
            current = [next_candidate]
            current.extend(islice(candidates, max_files - 1))

    def _iter_file_candidates(
        self,
        folder: Path,
        progress: _ScanProgress,
        observation: _DirectoryObservationAccumulator,
    ) -> Iterator[_FileCandidate]:
        try:
            with os.scandir(folder) as entries:
                for entry in entries:
                    try:
                        path = Path(entry.path)
                        entry_stat = entry.stat()
                        signature = _signature_from_stat(entry_stat)
                    except OSError as error:
                        raise GalleryScanError(
                            f"Unable to inspect source file {entry.path}: {error}"
                        ) from error
                    observation.add(
                        name=entry.name,
                        signature=signature,
                        file_type=S_IFMT(entry_stat.st_mode),
                    )
                    if not S_ISREG(entry_stat.st_mode):
                        continue
                    relative_locator = path.relative_to(self._root).as_posix()
                    progress.files_discovered += 1
                    yield _FileCandidate(
                        path=path,
                        key=FileHashCacheKey(
                            root_locator=self._root_locator,
                            relative_locator=relative_locator,
                            signature=signature,
                        ),
                    )
        except GalleryScanError:
            raise
        except OSError as error:
            raise GalleryScanError(
                f"Unable to enumerate gallery folder {folder}: {error}"
            ) from error

    def _observe_directory(self, folder: Path) -> _DirectoryObservationSnapshot:
        observation = _DirectoryObservationAccumulator()
        try:
            with os.scandir(folder) as entries:
                for entry in entries:
                    try:
                        entry_stat = entry.stat()
                    except OSError as error:
                        raise GalleryScanError(
                            f"Unable to re-inspect source entry {entry.path}: {error}"
                        ) from error
                    observation.add(
                        name=entry.name,
                        signature=_signature_from_stat(entry_stat),
                        file_type=S_IFMT(entry_stat.st_mode),
                    )
        except GalleryScanError:
            raise
        except OSError as error:
            raise GalleryScanError(
                f"Unable to re-enumerate gallery folder {folder}: {error}"
            ) from error
        return observation.snapshot()

    def _scan_candidate_chunk(
        self,
        executor: ThreadPoolExecutor,
        candidates: tuple[_FileCandidate, ...],
        progress: _ScanProgress,
    ) -> tuple[ScannedFile, ...]:
        requested_keys = tuple(candidate.key for candidate in candidates)
        requested = set(requested_keys)
        cached_by_key: dict[FileHashCacheKey, str] = {}
        for entry in self._hash_cache.lookup(requested_keys):
            if entry.key not in requested:
                raise GalleryScanError(
                    "Hash cache returned an entry that was not requested: "
                    f"{entry.key.relative_locator}"
                )
            if entry.key in cached_by_key:
                raise GalleryScanError(
                    f"Hash cache returned duplicate entries: "
                    f"{entry.key.relative_locator}"
                )
            self._validate_sha256(entry.sha256, entry.key.relative_locator)
            cached_by_key[entry.key] = entry.sha256.lower()

        misses = tuple(
            candidate for candidate in candidates if candidate.key not in cached_by_key
        )
        hashed = self._hash_misses(executor, misses, progress)
        remembered: list[FileHashCacheEntry] = []
        scanned: list[ScannedFile] = []
        for candidate in candidates:
            cached_sha256 = cached_by_key.get(candidate.key)
            if cached_sha256 is not None:
                signature = candidate.key.signature
                scanned.append(
                    ScannedFile(
                        candidate.path,
                        candidate.path.name,
                        signature.size_bytes,
                        cached_sha256,
                        candidate.key.relative_locator,
                        signature,
                    )
                )
                progress.hashes_reused += 1
                continue
            cached_hash = hashed[candidate]
            actual_key = FileHashCacheKey(
                root_locator=self._root_locator,
                relative_locator=candidate.key.relative_locator,
                signature=cached_hash.signature,
            )
            remembered.append(
                FileHashCacheEntry(
                    key=actual_key,
                    sha256=cached_hash.scanned_file.sha256,
                )
            )
            scanned.append(
                ScannedFile(
                    candidate.path,
                    candidate.path.name,
                    cached_hash.scanned_file.size_bytes,
                    cached_hash.scanned_file.sha256,
                    candidate.key.relative_locator,
                    cached_hash.signature,
                )
            )
            progress.hashes_read += 1
        if remembered:
            self._hash_cache.remember(tuple(remembered))
        return tuple(scanned)

    def _hash_misses(
        self,
        executor: ThreadPoolExecutor,
        candidates: tuple[_FileCandidate, ...],
        progress: _ScanProgress,
    ) -> dict[_FileCandidate, _CachedHash]:
        maximum_pending = self._hash_workers * 2
        candidate_iterator = iter(candidates)
        pending: dict[Future[_CachedHash], _FileCandidate] = {}
        result: dict[_FileCandidate, _CachedHash] = {}
        next_progress_at = monotonic() + self._progress_interval_seconds

        def fill_pending() -> None:
            while len(pending) < maximum_pending:
                try:
                    candidate = next(candidate_iterator)
                except StopIteration:
                    return
                pending[executor.submit(_hash_file, candidate.path)] = candidate

        fill_pending()
        while pending:
            timeout = max(0.0, next_progress_at - monotonic())
            done, _not_done = wait(
                pending,
                timeout=timeout,
                return_when=FIRST_COMPLETED,
            )
            for future in done:
                candidate = pending.pop(future)
                try:
                    result[candidate] = future.result()
                except GalleryScanError:
                    raise
                except OSError as error:
                    raise GalleryScanError(
                        f"Unable to hash source file {candidate.path}: {error}"
                    ) from error
            fill_pending()
            if monotonic() >= next_progress_at:
                self._event_logger(
                    "Filesystem hashing in progress: "
                    f"hashes_completed={progress.hashes_read + len(result)} "
                    f"hashes_in_flight={len(pending)} "
                    f"files_discovered={progress.files_discovered} "
                    f"elapsed_s={monotonic() - progress.started_at:.3f}"
                )
                next_progress_at = monotonic() + self._progress_interval_seconds
        return result

    @staticmethod
    def _validate_sha256(value: str, locator: str) -> None:
        try:
            valid = len(value) == 64 and len(bytes.fromhex(value)) == 32
        except ValueError:
            valid = False
        if not valid:
            raise GalleryScanError(f"Hash cache returned an invalid SHA-256: {locator}")

    @staticmethod
    def _to_manifest(
        gallery: GalleryInfoParser,
        *,
        gallery_attempt: str,
        relative_folder: str,
    ) -> ScannedGalleryManifest:
        return ScannedGalleryManifest(
            gallery_attempt=gallery_attempt,
            folder=gallery.gallery_folder,
            relative_folder=relative_folder,
            gallery_name=gallery.gallery_name,
            gid=gallery.gid,
            title=gallery.title,
            summary=gallery.galleries_comments,
            upload_account=gallery.upload_account,
            upload_time=gallery.upload_time,
            download_time=gallery.download_time,
            modified_time=gallery.modified_time,
            tags=tuple(gallery.tags),
        )

    @staticmethod
    def _build_legacy_gallery(
        manifest: ScannedGalleryManifest,
        source_files: Sequence[ScannedFile],
        completion: ScannedGalleryCompletion,
    ) -> ScannedGallery:
        files = tuple(
            sorted(source_files, key=lambda file: (file.name.casefold(), file.name))
        )
        try:
            metadata_digest = next(
                file.sha256 for file in files if file.name == GALLERY_INFO_NAME
            )
        except StopIteration as error:
            raise GalleryScanError(
                f"Gallery metadata disappeared during scan: {manifest.folder}"
            ) from error
        source_payload = {
            "version": 1,
            "metadata": metadata_digest,
            "files": [
                {"name": file.name, "size": file.size_bytes, "sha256": file.sha256}
                for file in files
            ],
        }
        content_hashes = sorted(
            bytes.fromhex(file.sha256)
            for file in files
            if file.name != GALLERY_INFO_NAME
        )
        source_digest = sha256(
            json.dumps(source_payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        content_digest = (
            sha256(b"".join(content_hashes)).hexdigest() if content_hashes else None
        )
        return ScannedGallery(
            folder=manifest.folder,
            gallery_name=manifest.gallery_name,
            gid=manifest.gid,
            title=manifest.title,
            summary=manifest.summary,
            upload_account=manifest.upload_account,
            upload_time=manifest.upload_time,
            download_time=manifest.download_time,
            modified_time=manifest.modified_time,
            pages=completion.pages,
            tags=manifest.tags,
            files=files,
            metadata_sha256=metadata_digest,
            source_digest=source_digest,
            content_digest=content_digest,
        )
