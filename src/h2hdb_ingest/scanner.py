__all__ = ["FilesystemScanner", "GalleryScanError"]

import json
import logging
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from time import monotonic

from h2h_galleryinfo_parser import GalleryInfoParser, parse_galleryinfo

from .models import ScannedFile, ScannedGallery

GALLERY_INFO_NAME = "galleryinfo.txt"
DEFAULT_PROGRESS_INTERVAL_SECONDS = 60.0
logger = logging.getLogger(__name__)


class GalleryScanError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class _FileSignature:
    device: int
    inode: int
    size_bytes: int
    modified_ns: int
    changed_ns: int


@dataclass(frozen=True, slots=True)
class _CachedHash:
    signature: _FileSignature
    scanned_file: ScannedFile


def _file_signature(path: Path) -> _FileSignature:
    stat = path.stat()
    return _FileSignature(
        device=stat.st_dev,
        inode=stat.st_ino,
        size_bytes=stat.st_size,
        modified_ns=stat.st_mtime_ns,
        changed_ns=stat.st_ctime_ns,
    )


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


class FilesystemScanner:
    def __init__(
        self,
        root: Path,
        *,
        hash_workers: int,
        event_logger: Callable[[str], None] | None = None,
        progress_interval_seconds: float = DEFAULT_PROGRESS_INTERVAL_SECONDS,
    ) -> None:
        if progress_interval_seconds <= 0:
            raise ValueError("progress_interval_seconds must be positive")
        self._root = root
        self._hash_workers = hash_workers
        self._hash_cache: dict[Path, _CachedHash] = {}
        self._event_logger = event_logger or logger.info
        self._progress_interval_seconds = progress_interval_seconds

    def scan(self) -> tuple[ScannedGallery, ...]:
        started_at = monotonic()
        self._event_logger(f"Filesystem scan started: root={self._root}")
        gallery_info_paths = sorted(
            self._root.rglob(GALLERY_INFO_NAME),
            key=lambda path: path.parent.as_posix(),
        )
        self._event_logger(
            "Filesystem discovery completed: "
            f"galleries={len(gallery_info_paths)} "
            f"elapsed_s={monotonic() - started_at:.3f}"
        )
        parsed: list[tuple[GalleryInfoParser, Path, tuple[Path, ...]]] = []
        all_paths: list[Path] = []
        next_progress_at = monotonic() + self._progress_interval_seconds
        for gallery_index, gallery_info_path in enumerate(gallery_info_paths, start=1):
            folder = gallery_info_path.parent
            try:
                gallery = parse_galleryinfo(folder)
            except Exception as error:
                raise GalleryScanError(
                    f"Unable to parse {gallery_info_path}: {error}"
                ) from error
            source_paths = tuple(
                sorted(
                    (path for path in folder.iterdir() if path.is_file()),
                    key=lambda path: (path.name.casefold(), path.name),
                )
            )
            if gallery_info_path not in source_paths:
                raise GalleryScanError(
                    f"Gallery metadata disappeared during scan: {folder}"
                )
            parsed.append((gallery, gallery_info_path, source_paths))
            all_paths.extend(source_paths)
            if monotonic() >= next_progress_at:
                self._event_logger(
                    "Filesystem parsing in progress: "
                    f"galleries_parsed={gallery_index} "
                    f"galleries_total={len(gallery_info_paths)} "
                    f"files_discovered={len(all_paths)} "
                    f"elapsed_s={monotonic() - started_at:.3f}"
                )
                next_progress_at = monotonic() + self._progress_interval_seconds

        digests: dict[Path, ScannedFile] = {}
        paths_to_hash: list[Path] = []
        for path in all_paths:
            try:
                signature = _file_signature(path)
            except OSError as error:
                raise GalleryScanError(f"Unable to inspect source file {path}: {error}")
            cached = self._hash_cache.get(path)
            if cached is not None and cached.signature == signature:
                digests[path] = cached.scanned_file
            else:
                paths_to_hash.append(path)

        self._hash_paths(paths_to_hash, digests, started_at=started_at)

        live_paths = set(all_paths)
        self._hash_cache = {
            path: cached
            for path, cached in self._hash_cache.items()
            if path in live_paths
        }

        result = tuple(
            self._build_gallery(gallery, metadata_path, source_paths, digests)
            for gallery, metadata_path, source_paths in parsed
        )
        self._event_logger(
            "Filesystem scan completed: "
            f"galleries={len(result)} files={len(all_paths)} "
            f"elapsed_s={monotonic() - started_at:.3f} "
            f"hashes_reused={len(all_paths) - len(paths_to_hash)} "
            f"hashes_read={len(paths_to_hash)}"
        )
        return result

    def _hash_paths(
        self,
        paths: list[Path],
        digests: dict[Path, ScannedFile],
        *,
        started_at: float,
    ) -> None:
        if not paths:
            return
        maximum_pending = max(1, self._hash_workers * 2)
        path_iterator = iter(paths)
        pending: dict[Future[_CachedHash], Path] = {}
        completed = 0
        next_progress_at = monotonic() + self._progress_interval_seconds

        with ThreadPoolExecutor(max_workers=self._hash_workers) as executor:

            def fill_pending() -> None:
                while len(pending) < maximum_pending:
                    try:
                        path = next(path_iterator)
                    except StopIteration:
                        return
                    pending[executor.submit(_hash_file, path)] = path

            fill_pending()
            while pending:
                timeout = max(0.0, next_progress_at - monotonic())
                done, _not_done = wait(
                    pending,
                    timeout=timeout,
                    return_when=FIRST_COMPLETED,
                )
                for future in done:
                    path = pending.pop(future)
                    cached = future.result()
                    self._hash_cache[path] = cached
                    digests[path] = cached.scanned_file
                    completed += 1
                fill_pending()
                if monotonic() >= next_progress_at:
                    self._event_logger(
                        "Filesystem hashing in progress: "
                        f"hashes_completed={completed} hashes_total={len(paths)} "
                        f"hashes_in_flight={len(pending)} "
                        f"elapsed_s={monotonic() - started_at:.3f}"
                    )
                    next_progress_at = monotonic() + self._progress_interval_seconds

    @staticmethod
    def _build_gallery(
        gallery: GalleryInfoParser,
        metadata_path: Path,
        source_paths: tuple[Path, ...],
        digests: dict[Path, ScannedFile],
    ) -> ScannedGallery:
        files = tuple(digests[path] for path in source_paths)
        metadata_digest = digests[metadata_path].sha256
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
            folder=gallery.gallery_folder,
            gallery_name=gallery.gallery_name,
            gid=gallery.gid,
            title=gallery.title,
            summary=gallery.galleries_comments,
            upload_account=gallery.upload_account,
            upload_time=gallery.upload_time,
            download_time=gallery.download_time,
            modified_time=gallery.modified_time,
            pages=gallery.pages,
            tags=tuple(gallery.tags),
            files=files,
            metadata_sha256=metadata_digest,
            source_digest=source_digest,
            content_digest=content_digest,
        )
