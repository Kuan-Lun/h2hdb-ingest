__all__ = ["CBZReconciler", "CBZSourceChangedError"]

import fcntl
import json
import logging
import os
import re
import shutil
import sqlite3
import stat
import struct
import tempfile
import zlib
from collections.abc import Callable, Collection, Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import partial
from hashlib import sha256
from pathlib import Path, PurePosixPath
from threading import RLock
from time import monotonic, time_ns
from typing import BinaryIO
from uuid import uuid4
from zipfile import ZIP_DEFLATED, BadZipFile, ZipFile, ZipInfo

from PIL import Image, ImageFile, ImageOps

from .config import (
    DEFAULT_CBZ_WORKERS,
    DEFAULT_STALE_TEMP_AGE_SECONDS,
    CBZGrouping,
)
from .models import (
    CBZArtifact,
    CBZGalleryDescriptor,
    CBZPreparationFile,
    CBZPreparationMetadata,
    CBZPreparationRequest,
    CBZPreparationSummary,
    CBZStreamingPreparationRequest,
    DeduplicationPlan,
    FileStatSignature,
    ScannedFile,
    ScannedGallery,
)
from .naming import gallery_name_to_cbz_file_name

CBZ_MANIFEST_VERSION = 2
STATE_FILE_NAME = ".h2hdb-cbz-state.json"
STATE_LOCK_FILE_NAME = ".h2hdb-cbz-state.lock"
STATE_VERSION = 2
STATE_DATABASE_FILE_NAME = ".h2hdb-cbz-state.sqlite3"
STATE_DATABASE_MARKER_FILE_NAME = ".h2hdb-cbz-state.sqlite3.ready"
STATE_DATABASE_VERSION = 1
_STATE_DATABASE_TABLE_COLUMNS = {
    "state_meta": ("key", "value"),
    "artifacts": ("name", "gid", "published"),
    "protections": ("protection_id", "artifact_name"),
    "current_projection": (
        "path_name",
        "artifact_name",
        "device",
        "inode",
        "size_bytes",
        "modified_ns",
        "changed_ns",
    ),
    "pending_projection": ("path_name", "artifact_name"),
    "projection_revision": (
        "singleton",
        "current_revision",
        "pending_revision",
    ),
}
_STATE_DATABASE_INDEX_COLUMNS = {
    "artifacts_gid_name_idx": ("gid", "name"),
    "artifacts_prune_idx": ("published", "name"),
    "protections_artifact_idx": ("artifact_name", "protection_id"),
    "current_projection_artifact_idx": ("artifact_name", "path_name"),
    "pending_projection_artifact_idx": ("artifact_name", "path_name"),
}
_STATE_DATABASE_PRIMARY_KEYS = {
    "state_meta": ("key",),
    "artifacts": ("name",),
    "protections": ("protection_id", "artifact_name"),
    "current_projection": ("path_name",),
    "pending_projection": ("path_name",),
    "projection_revision": ("singleton",),
}
LEGACY_PROTECTION_ID = "__legacy__"
IMAGE_SPOOL_MEMORY_LIMIT = 16 * 1024 * 1024
CBZ_PROGRESS_INTERVAL_SECONDS = 60.0
PROJECTION_RECONCILIATION_PAGE_SIZE = 256
PROJECTION_RECONCILIATION_TEMP_CACHE_KIB = 2 * 1024
ARTIFACT_TEMP_PREFIX = ".h2hdb-ingest-artifact-"
PROJECTION_TEMP_PREFIX = ".h2hdb-ingest-projection-"
STATE_TEMP_PREFIX = ".h2hdb-ingest-state-"
STATE_DATABASE_TEMP_PREFIX = ".h2hdb-ingest-state-db-"
_OWNED_TEMP_PATTERN_BY_PREFIX = {
    prefix: re.compile(rf"{re.escape(prefix)}[0-9a-f]{{32}}\.tmp")
    for prefix in (
        ARTIFACT_TEMP_PREFIX,
        PROJECTION_TEMP_PREFIX,
        STATE_TEMP_PREFIX,
        STATE_DATABASE_TEMP_PREFIX,
    )
}
NORMALIZED_IMAGE_SUFFIXES = frozenset(
    {".avif", ".bmp", ".jpeg", ".jpg", ".png", ".webp"}
)

Image.MAX_IMAGE_PIXELS = None
ImageFile.LOAD_TRUNCATED_IMAGES = True
logger = logging.getLogger(__name__)


class CBZSourceChangedError(RuntimeError):
    """A staged CBZ source no longer matches its durable observation."""


@dataclass(slots=True)
class _ReconciliationState:
    owned: set[str]
    published: set[str]
    protections: dict[str, set[str]]
    current: dict[str, _CurrentProjection]
    current_revision: int | None = None
    pending: dict[str, str] = field(default_factory=dict)
    pending_revision: int | None = None


@dataclass(frozen=True, slots=True)
class _ProjectionJournalState:
    current: dict[str, _CurrentProjection]
    current_revision: int | None
    pending: dict[str, str]
    pending_revision: int | None


@dataclass(frozen=True, slots=True)
class _ProjectionSignature:
    device: int
    inode: int
    size_bytes: int
    modified_ns: int
    changed_ns: int


@dataclass(frozen=True, slots=True)
class _CurrentProjection:
    artifact_name: str
    signature: _ProjectionSignature


@dataclass(frozen=True, slots=True)
class _PreparationWork:
    gallery: CBZGalleryDescriptor
    ensure: Callable[[Collection[str]], CBZArtifact]


_ZIP_LOCAL_HEADER = struct.Struct("<IHHHHHIIIHH")
_ZIP_CENTRAL_HEADER = struct.Struct("<IHHHHHHIIIHHHHHII")
_ZIP_END_RECORD = struct.Struct("<IHHHHIIH")
_ZIP64_END_RECORD = struct.Struct("<IQHHIIQQQQ")
_ZIP64_END_LOCATOR = struct.Struct("<IIQI")
_ZIP32_MAX = (1 << 32) - 1
_ZIP16_MAX = (1 << 16) - 1
_ZIP_UTF8_FLAG = 1 << 11
_ZIP_DOS_DATE_1980_01_01 = (1 << 5) | 1


class _StreamingZipMember:
    """One raw-deflate member whose central record is spooled to disk."""

    def __init__(
        self,
        archive: _BoundedZipWriter,
        *,
        name: bytes,
        local_offset: int,
    ) -> None:
        self._archive = archive
        self._name = name
        self._local_offset = local_offset
        self._compressor = zlib.compressobj(9, zlib.DEFLATED, -15)
        self._crc32 = 0
        self._uncompressed_size = 0
        self._compressed_size = 0
        self._closed = False

    def __enter__(self) -> _StreamingZipMember:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        del exc_type, exc_value, traceback
        self.close()

    def write(self, data: bytes) -> int:
        if self._closed:
            raise ValueError("I/O operation on closed ZIP member")
        payload = bytes(data)
        self._crc32 = zlib.crc32(payload, self._crc32) & _ZIP32_MAX
        self._uncompressed_size += len(payload)
        compressed = self._compressor.compress(payload)
        if compressed:
            self._archive._output.write(compressed)
            self._compressed_size += len(compressed)
        return len(payload)

    def close(self) -> None:
        if self._closed:
            return
        final = self._compressor.flush(zlib.Z_FINISH)
        if final:
            self._archive._output.write(final)
            self._compressed_size += len(final)
        self._closed = True
        self._archive._finish_member(
            name=self._name,
            local_offset=self._local_offset,
            crc32=self._crc32,
            compressed_size=self._compressed_size,
            uncompressed_size=self._uncompressed_size,
        )


class _BoundedZipWriter:
    """Seekable ZIP writer with its central directory held in a temp file.

    ``zipfile.ZipFile`` retains one ``ZipInfo`` object per member until close.
    That makes a pathological giant gallery another unbounded in-memory
    collection.  This writer patches each local header after streaming the
    member and immediately spools its central-directory record to disk.
    """

    def __init__(self, output: BinaryIO) -> None:
        self._output = output
        self._central = tempfile.TemporaryFile(mode="w+b")
        self._member_open = False
        self._member_count = 0
        self._closed = False
        self.comment = b""

    def __enter__(self) -> _BoundedZipWriter:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        del exc_value, traceback
        if exc_type is None:
            self.close()
        else:
            self._central.close()
            self._closed = True

    def open(self, info: ZipInfo, mode: str) -> _StreamingZipMember:
        if self._closed:
            raise ValueError("I/O operation on closed ZIP archive")
        if self._member_open:
            raise RuntimeError("A ZIP member is already open")
        if mode != "w":
            raise ValueError("The bounded ZIP writer only supports write mode")
        name = info.filename.encode("utf-8")
        if not name or len(name) > _ZIP16_MAX:
            raise ValueError("ZIP member name has an invalid encoded length")
        local_offset = self._output.tell()
        self._output.write(
            _ZIP_LOCAL_HEADER.pack(
                0x04034B50,
                20,
                _ZIP_UTF8_FLAG,
                ZIP_DEFLATED,
                0,
                _ZIP_DOS_DATE_1980_01_01,
                0,
                0,
                0,
                len(name),
                0,
            )
        )
        self._output.write(name)
        self._member_open = True
        return _StreamingZipMember(
            self,
            name=name,
            local_offset=local_offset,
        )

    def _finish_member(
        self,
        *,
        name: bytes,
        local_offset: int,
        crc32: int,
        compressed_size: int,
        uncompressed_size: int,
    ) -> None:
        if compressed_size >= _ZIP32_MAX or uncompressed_size >= _ZIP32_MAX:
            raise RuntimeError(
                "A single CBZ source member exceeds the supported 4 GiB limit"
            )
        end_offset = self._output.tell()
        self._output.seek(local_offset)
        self._output.write(
            _ZIP_LOCAL_HEADER.pack(
                0x04034B50,
                20,
                _ZIP_UTF8_FLAG,
                ZIP_DEFLATED,
                0,
                _ZIP_DOS_DATE_1980_01_01,
                crc32,
                compressed_size,
                uncompressed_size,
                len(name),
                0,
            )
        )
        self._output.seek(end_offset)

        zip64_offset = local_offset >= _ZIP32_MAX
        version_needed = 45 if zip64_offset else 20
        extra = struct.pack("<HHQ", 0x0001, 8, local_offset) if zip64_offset else b""
        self._central.write(
            _ZIP_CENTRAL_HEADER.pack(
                0x02014B50,
                (3 << 8) | version_needed,
                version_needed,
                _ZIP_UTF8_FLAG,
                ZIP_DEFLATED,
                0,
                _ZIP_DOS_DATE_1980_01_01,
                crc32,
                compressed_size,
                uncompressed_size,
                len(name),
                len(extra),
                0,
                0,
                0,
                0o100644 << 16,
                _ZIP32_MAX if zip64_offset else local_offset,
            )
        )
        self._central.write(name)
        self._central.write(extra)
        self._member_count += 1
        self._member_open = False

    def close(self) -> None:
        if self._closed:
            return
        if self._member_open:
            raise RuntimeError("Cannot close a ZIP archive with an open member")
        if not isinstance(self.comment, bytes) or len(self.comment) > _ZIP16_MAX:
            raise ValueError("ZIP archive comment has an invalid encoded length")
        central_offset = self._output.tell()
        self._central.seek(0)
        shutil.copyfileobj(self._central, self._output, length=4 * 1024 * 1024)
        central_size = self._output.tell() - central_offset
        needs_zip64 = (
            self._member_count >= _ZIP16_MAX
            or central_size >= _ZIP32_MAX
            or central_offset >= _ZIP32_MAX
        )
        if needs_zip64:
            zip64_offset = self._output.tell()
            self._output.write(
                _ZIP64_END_RECORD.pack(
                    0x06064B50,
                    44,
                    (3 << 8) | 45,
                    45,
                    0,
                    0,
                    self._member_count,
                    self._member_count,
                    central_size,
                    central_offset,
                )
            )
            self._output.write(_ZIP64_END_LOCATOR.pack(0x07064B50, 0, zip64_offset, 1))
        self._output.write(
            _ZIP_END_RECORD.pack(
                0x06054B50,
                0,
                0,
                min(self._member_count, _ZIP16_MAX),
                min(self._member_count, _ZIP16_MAX),
                min(central_size, _ZIP32_MAX),
                min(central_offset, _ZIP32_MAX),
                len(self.comment),
            )
        )
        self._output.write(self.comment)
        self._central.close()
        self._closed = True


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as source:
        while chunk := source.read(4 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _content_addressed_name(gid: int, digest: str) -> str:
    return f"{gid}-{digest}.cbz"


def _zip_info(name: str) -> ZipInfo:
    info = ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = ZIP_DEFLATED
    info.create_system = 3
    info.external_attr = 0o100644 << 16
    return info


class CBZReconciler:
    def __init__(
        self,
        *,
        artifact_store_path: Path,
        cbz_path: Path,
        max_image_short_side: int,
        grouping: CBZGrouping = CBZGrouping.flat,
        workers: int = DEFAULT_CBZ_WORKERS,
        stale_temp_age_seconds: float = DEFAULT_STALE_TEMP_AGE_SECONDS,
        event_logger: Callable[[str], None] | None = None,
        progress_interval_seconds: float = CBZ_PROGRESS_INTERVAL_SECONDS,
    ) -> None:
        self._artifact_store_path = artifact_store_path.resolve()
        self._cbz_path = cbz_path.resolve()
        if self._artifact_store_path == self._cbz_path:
            raise ValueError("artifact_store_path and cbz_path must be different")
        if self._artifact_store_path.is_relative_to(
            self._cbz_path
        ) or self._cbz_path.is_relative_to(self._artifact_store_path):
            raise ValueError(
                "artifact_store_path and cbz_path must not contain one another"
            )
        if max_image_short_side < 1:
            raise ValueError("max_image_short_side must be positive")
        if not 1 <= workers <= 32:
            raise ValueError("workers must be between 1 and 32")
        if stale_temp_age_seconds <= 0:
            raise ValueError("stale_temp_age_seconds must be positive")
        if progress_interval_seconds <= 0:
            raise ValueError("progress_interval_seconds must be positive")
        self._max_image_short_side = max_image_short_side
        self._grouping = grouping
        self._workers = workers
        self._stale_temp_age_ns = int(stale_temp_age_seconds * 1_000_000_000)
        self._event_logger = event_logger or logger.info
        self._progress_interval_seconds = progress_interval_seconds
        self._state_path = self._artifact_store_path / STATE_FILE_NAME
        self._state_database_path = self._artifact_store_path / STATE_DATABASE_FILE_NAME
        self._state_database_marker_path = (
            self._artifact_store_path / STATE_DATABASE_MARKER_FILE_NAME
        )
        self._state_lock_path = self._artifact_store_path / STATE_LOCK_FILE_NAME
        self._process_lock = RLock()
        self._state_lock_depth = 0
        self._state_database_ready = False

    @contextmanager
    def publication_guard(self) -> Iterator[None]:
        """Serialize catalog publication through projection finalization.

        Every ingest instance sharing an artifact store participates in this
        filesystem lock.  A newer owner therefore cannot commit the next catalog
        revision between an older owner's revision check and atomic projection
        swaps.
        """

        with self._locked_state():
            yield

    def prepare(self, plan: DeduplicationPlan) -> tuple[CBZArtifact, ...]:
        winners = plan.winners
        if len({gallery.gid for gallery in winners}) != len(winners):
            raise ValueError("CBZ preparation requires one winner per GID")

        index_by_gid = {gallery.gid: index for index, gallery in enumerate(winners)}
        results: list[CBZArtifact | None] = [None] * len(winners)

        def collect(artifact: CBZArtifact) -> None:
            results[index_by_gid[artifact.gallery.gid]] = artifact

        requests = (
            CBZPreparationRequest(
                gallery=gallery,
                excluded_file_sha256s=frozenset(
                    source_file.sha256
                    for source_file in gallery.files
                    if source_file.sha256 in plan.excluded_file_sha256s
                ),
            )
            for gallery in winners
        )
        self.prepare_stream(
            requests,
            result_sink=collect,
            total=len(winners),
        )
        artifacts = tuple(artifact for artifact in results if artifact is not None)
        if len(artifacts) != len(winners):
            raise RuntimeError("CBZ preparation completed without every result")
        return artifacts

    def prepare_stream(
        self,
        requests: Iterable[CBZPreparationRequest],
        *,
        result_sink: Callable[[CBZArtifact], None] | None = None,
        total: int | None = None,
    ) -> CBZPreparationSummary:
        """Prepare an iterable of galleries with bounded in-flight work.

        Every successful artifact is recorded in reconciliation state before it
        is passed to ``result_sink``.  The method does not retain successful
        artifacts, so a durable sink can consume arbitrarily many requests while
        this process holds at most twice ``workers`` requests in flight.  The
        legacy :meth:`prepare` wrapper only accumulates the lightweight artifact
        descriptors needed by its tuple return value.

        ``total`` is an optional exact count used for progress reporting.  An
        iterable can omit it when determining the count would require
        materialization.
        """

        def works() -> Iterator[_PreparationWork]:
            for request in requests:
                gallery = request.gallery
                excluded = request.excluded_file_sha256s
                yield _PreparationWork(
                    gallery=CBZGalleryDescriptor.from_scanned_gallery(gallery),
                    ensure=partial(
                        self._ensure_cbz,
                        gallery,
                        excluded_file_sha256s=excluded,
                    ),
                )

        return self._prepare_work_stream(
            works(),
            result_sink=result_sink,
            total=total,
        )

    def prepare_paged_stream(
        self,
        requests: Iterable[CBZStreamingPreparationRequest],
        *,
        result_sink: Callable[[CBZArtifact], None] | None = None,
        total: int | None = None,
    ) -> CBZPreparationSummary:
        """Prepare page-backed galleries without hydrating any gallery's files.

        At most twice the worker count of lightweight requests are pending.  A
        worker opens its own file iterator only when a rebuild is necessary;
        that iterator may hold one bounded database page and the worker holds at
        most one transformed-image spool while writing the archive.
        """

        def works() -> Iterator[_PreparationWork]:
            for request in requests:
                metadata = request.metadata
                open_files = request.open_files
                yield _PreparationWork(
                    gallery=metadata.gallery,
                    ensure=partial(
                        self._ensure_streaming_cbz,
                        metadata,
                        open_files,
                    ),
                )

        return self._prepare_work_stream(
            works(),
            result_sink=result_sink,
            total=total,
        )

    def _prepare_work_stream(
        self,
        works: Iterable[_PreparationWork],
        *,
        result_sink: Callable[[CBZArtifact], None] | None,
        total: int | None,
    ) -> CBZPreparationSummary:
        if total is not None and total < 0:
            raise ValueError("CBZ preparation total must not be negative")
        with self._locked_state():
            self._cbz_path.mkdir(parents=True, exist_ok=True)
            self._cleanup_stale_temporary_files()
            self._ensure_state_database()
            total_label = str(total) if total is not None else "unknown"
            self._event_logger(
                "CBZ preparation started: "
                f"galleries={total_label} workers={self._workers}"
            )

            started_at = monotonic()
            maximum_pending = max(1, self._workers * 2)
            request_iterator = iter(enumerate(works))
            pending: dict[
                Future[CBZArtifact],
                tuple[int, _PreparationWork, float],
            ] = {}
            seen_gids: set[int] = set()
            first_error: BaseException | None = None
            submitted = 0
            completed = 0
            created = 0
            rebuilt = 0
            next_progress_at = monotonic() + self._progress_interval_seconds

            with ThreadPoolExecutor(
                max_workers=self._workers,
                thread_name_prefix="h2hdb-cbz",
            ) as executor:

                def fill_pending() -> None:
                    nonlocal first_error, submitted
                    while first_error is None and len(pending) < maximum_pending:
                        try:
                            index, work = next(request_iterator)
                        except StopIteration:
                            return
                        except BaseException as error:
                            first_error = error
                            return
                        gallery = work.gallery
                        if gallery.gid in seen_gids:
                            first_error = ValueError(
                                "CBZ preparation requires one winner per GID"
                            )
                            return
                        seen_gids.add(gallery.gid)
                        future = executor.submit(
                            work.ensure,
                            self._owned_names_for_gid(gallery.gid),
                        )
                        pending[future] = (index, work, monotonic())
                        submitted += 1

                fill_pending()
                while pending:
                    timeout = max(0.0, next_progress_at - monotonic())
                    done, _not_done = wait(
                        pending,
                        timeout=timeout,
                        return_when=FIRST_COMPLETED,
                    )
                    for future in sorted(done, key=lambda item: pending[item][0]):
                        index, work, gallery_started_at = pending.pop(future)
                        gallery = work.gallery
                        try:
                            artifact = future.result()
                            state_name = self._state_name(artifact.path)
                            self._record_owned_artifact(
                                state_name,
                                gid=artifact.gallery.gid,
                            )
                            completed += 1
                            created += int(artifact.created)
                            rebuilt += int(artifact.rebuilt)
                            if result_sink is not None and first_error is None:
                                result_sink(artifact)
                            outcome = (
                                "created"
                                if artifact.created
                                else "rebuilt" if artifact.rebuilt else "reused"
                            )
                            self._event_logger(
                                "CBZ book prepared: "
                                f"index={index + 1} galleries={total_label} "
                                f"gallery={gallery.gallery_name!r} gid={gallery.gid} "
                                f"outcome={outcome} "
                                f"elapsed_s={monotonic() - gallery_started_at:.3f}"
                            )
                        except BaseException as error:
                            if first_error is None:
                                first_error = error
                    fill_pending()
                    if monotonic() >= next_progress_at:
                        self._event_logger(
                            "CBZ preparation in progress: "
                            f"completed={completed} galleries={total_label} "
                            f"in_flight={len(pending)} "
                            f"elapsed_s={monotonic() - started_at:.3f}"
                        )
                        next_progress_at = monotonic() + self._progress_interval_seconds

            if first_error is not None:
                first_error.add_note(
                    "CBZ preparation stopped after draining all submitted workers; "
                    f"completed={completed} submitted={submitted} "
                    f"total={total_label}"
                )
                raise first_error
            if total is not None and submitted != total:
                raise ValueError(
                    "CBZ preparation iterable length does not match total: "
                    f"expected={total} actual={submitted}"
                )
            summary = CBZPreparationSummary(
                prepared=completed,
                created=created,
                rebuilt=rebuilt,
            )
            self._event_logger(
                "CBZ preparation completed: "
                f"galleries={completed} created={created} rebuilt={rebuilt} "
                f"reused={summary.reused} "
                f"elapsed_s={monotonic() - started_at:.3f}"
            )
            return summary

    def protect_for_publish(
        self,
        artifacts: Iterable[CBZArtifact],
        *,
        protection_id: str = LEGACY_PROTECTION_ID,
    ) -> None:
        self._validate_protection_id(protection_id)
        with self._locked_state():
            names = tuple(
                dict.fromkeys(self._state_name(artifact.path) for artifact in artifacts)
            )
            statuses = self._artifact_statuses(names)
            missing = set(names) - set(statuses)
            unavailable: set[str] = set()
            selected: list[str] = []
            for name, published in statuses.items():
                if not self._path_from_state_name(name).is_file():
                    unavailable.add(name)
                    continue
                if not published:
                    selected.append(name)
            if missing:
                raise RuntimeError(
                    "CBZ artifacts selected for publication are missing from ingest "
                    f"state: {sorted(missing)!r}"
                )
            if unavailable:
                raise RuntimeError(
                    "CBZ artifacts selected for publication are unavailable: "
                    f"{sorted(unavailable)!r}"
                )
            if selected:
                self._add_protections(protection_id, selected)

    def release_publish_protection(self, protection_id: str) -> None:
        """Release one build's protection without disturbing other builds."""

        self._validate_protection_id(protection_id)
        with self._locked_state():
            self._remove_protection(protection_id)
            self._prune_unprotected_artifacts()

    def finalize_published(
        self,
        artifacts: Iterable[CBZArtifact],
        *,
        revision: int | None = None,
        protection_id: str = LEGACY_PROTECTION_ID,
    ) -> None:
        if revision is not None and revision < 0:
            raise ValueError("Catalog revision must not be negative")
        self._validate_protection_id(protection_id)
        with self._locked_state():
            self._cleanup_stale_temporary_files()
            self._ensure_state_database()
            with self._state_connection() as connection:
                self._initialize_projection_reconciliation(connection)
                current_revision, pending_revision = self._projection_revisions(
                    connection
                )
                revision_floor = max(
                    candidate
                    for candidate in (current_revision, pending_revision, -1)
                    if candidate is not None
                )
                if revision is not None and revision < revision_floor:
                    raise RuntimeError(
                        "Refusing to overwrite a newer Komga projection: "
                        f"catalog revision {revision} is older than {revision_floor}"
                    )

                self._index_previously_managed_projection(connection)
                self._plan_current_view_in_database(connection, artifacts)

                # Persist the complete projection intent before touching the Komga
                # tree.  The temporary planning index keeps memory bounded; the
                # durable pending rows remain the crash-recovery source of truth.
                self._replace_pending_projection_from_plan(
                    connection,
                    revision=revision,
                )
                self._materialize_current_view_from_plan(connection)
                self._remove_stale_current_paths_from_plan(connection)
                self._commit_projection_state_from_plan(
                    connection,
                    revision=revision,
                    protection_id=protection_id,
                )
            self._prune_unprotected_artifacts()

    @staticmethod
    def _initialize_projection_reconciliation(
        connection: sqlite3.Connection,
    ) -> None:
        connection.execute("PRAGMA temp_store = FILE")
        if connection.execute("PRAGMA temp_store").fetchone() != (1,):
            raise RuntimeError(
                "Unable to enable file-backed CBZ projection reconciliation"
            )
        connection.execute(
            "PRAGMA temp.cache_size = " f"{-PROJECTION_RECONCILIATION_TEMP_CACHE_KIB}"
        )
        connection.executescript("""
            CREATE TEMP TABLE reconciliation_managed (
                path_name TEXT PRIMARY KEY
            );
            CREATE TEMP TABLE reconciliation_plan (
                sequence INTEGER PRIMARY KEY,
                path_name TEXT NOT NULL UNIQUE,
                casefold_key TEXT NOT NULL UNIQUE,
                artifact_name TEXT NOT NULL
            );
            CREATE INDEX reconciliation_plan_artifact_idx
                ON reconciliation_plan(artifact_name);
            CREATE TEMP TABLE reconciliation_current (
                path_name TEXT PRIMARY KEY,
                artifact_name TEXT NOT NULL,
                device INTEGER NOT NULL CHECK (device >= 0),
                inode INTEGER NOT NULL CHECK (inode >= 0),
                size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
                modified_ns INTEGER NOT NULL,
                changed_ns INTEGER NOT NULL
            );
            """)

    def _projection_revisions(
        self,
        connection: sqlite3.Connection,
    ) -> tuple[int | None, int | None]:
        row = connection.execute("""
            SELECT current_revision, pending_revision
            FROM projection_revision
            WHERE singleton = 1
            """).fetchone()
        if row is None:
            raise RuntimeError(f"Invalid CBZ SQLite state: {self._state_database_path}")
        return row[0], row[1]

    def _index_previously_managed_projection(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        connection.execute("""
            INSERT INTO reconciliation_managed(path_name)
            SELECT path_name FROM current_projection
            """)
        after: str | None = None
        while True:
            if after is None:
                cursor = connection.execute(
                    """
                    SELECT path_name, artifact_name
                    FROM pending_projection
                    ORDER BY path_name
                    LIMIT ?
                    """,
                    (PROJECTION_RECONCILIATION_PAGE_SIZE,),
                )
            else:
                cursor = connection.execute(
                    """
                    SELECT path_name, artifact_name
                    FROM pending_projection
                    WHERE path_name > ?
                    ORDER BY path_name
                    LIMIT ?
                    """,
                    (after, PROJECTION_RECONCILIATION_PAGE_SIZE),
                )
            rows = cursor.fetchmany(PROJECTION_RECONCILIATION_PAGE_SIZE)
            if not rows:
                break
            for name, artifact_name in rows:
                if self._pending_projection_is_recoverable(
                    connection,
                    name=name,
                    artifact_name=artifact_name,
                ):
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO reconciliation_managed(path_name)
                        VALUES (?)
                        """,
                        (name,),
                    )
            after = rows[-1][0]
        connection.commit()

    def _pending_projection_is_recoverable(
        self,
        connection: sqlite3.Connection,
        *,
        name: str,
        artifact_name: str,
    ) -> bool:
        if (
            connection.execute(
                "SELECT 1 FROM current_projection WHERE path_name = ?",
                (name,),
            ).fetchone()
            is not None
        ):
            return True
        target = self._current_path_from_state_name(name)
        if not target.exists() and not target.is_symlink():
            return True
        if target.is_symlink():
            return False
        source = self._path_from_state_name(artifact_name)
        try:
            return (
                source.is_file()
                and target.is_file()
                and source.stat().st_size == target.stat().st_size
                and _sha256_file(source) == _sha256_file(target)
            )
        except OSError:
            return False

    def _plan_current_view_in_database(
        self,
        connection: sqlite3.Connection,
        artifacts: Iterable[CBZArtifact],
    ) -> None:
        for artifact in artifacts:
            artifact_name = self._state_name(artifact.path)
            if (
                connection.execute(
                    "SELECT 1 FROM artifacts WHERE name = ?",
                    (artifact_name,),
                ).fetchone()
                is None
            ):
                raise RuntimeError(
                    "Published CBZ artifacts are missing from ingest state: "
                    f"{[artifact_name]!r}"
                )
            attempt = 0
            while True:
                leaf = self._current_leaf(artifact, attempt)
                target = self._current_directory(artifact.gallery) / leaf
                name = self._current_state_name(target)
                key = name.casefold()
                already_planned = connection.execute(
                    """
                    SELECT 1 FROM reconciliation_plan
                    WHERE casefold_key = ?
                    """,
                    (key,),
                ).fetchone()
                managed = connection.execute(
                    """
                    SELECT 1 FROM reconciliation_managed
                    WHERE path_name = ?
                    """,
                    (name,),
                ).fetchone()
                target_exists = target.exists() or target.is_symlink()
                if already_planned is None and (not target_exists or managed):
                    connection.execute(
                        """
                        INSERT INTO reconciliation_plan(
                            path_name,
                            casefold_key,
                            artifact_name
                        ) VALUES (?, ?, ?)
                        """,
                        (name, key, artifact_name),
                    )
                    break
                attempt += 1
                if attempt > 10_000:
                    raise RuntimeError(
                        "Unable to choose a unique managed CBZ name for "
                        f"{artifact.gallery.gallery_name!r}"
                    )
        connection.commit()

    @staticmethod
    def _replace_pending_projection_from_plan(
        connection: sqlite3.Connection,
        *,
        revision: int | None,
    ) -> None:
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute("DELETE FROM pending_projection")
            connection.execute("""
                INSERT INTO pending_projection(path_name, artifact_name)
                SELECT path_name, artifact_name
                FROM reconciliation_plan
                ORDER BY path_name
                """)
            connection.execute(
                """
                UPDATE projection_revision
                SET pending_revision = ?
                WHERE singleton = 1
                """,
                (revision,),
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    def _materialize_current_view_from_plan(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        after = 0
        while True:
            cursor = connection.execute(
                """
                SELECT sequence, path_name, artifact_name
                FROM reconciliation_plan
                WHERE sequence > ?
                ORDER BY sequence
                LIMIT ?
                """,
                (after, PROJECTION_RECONCILIATION_PAGE_SIZE),
            )
            rows = cursor.fetchmany(PROJECTION_RECONCILIATION_PAGE_SIZE)
            if not rows:
                break
            for sequence, name, artifact_name in rows:
                source = self._path_from_state_name(artifact_name)
                if not source.is_file():
                    raise RuntimeError(
                        f"Published CBZ artifact is unavailable: {source}"
                    )
                target = self._current_path_from_state_name(name)
                signature = self._projection_signature(target)
                previous = self._current_projection(connection, name=name)
                if (
                    previous is not None
                    and previous.artifact_name == artifact_name
                    and previous.signature == signature
                ):
                    materialized = previous
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target = self._current_path_from_state_name(name)
                    managed = connection.execute(
                        """
                        SELECT 1 FROM reconciliation_managed
                        WHERE path_name = ?
                        """,
                        (name,),
                    ).fetchone()
                    if (target.exists() or target.is_symlink()) and managed is None:
                        raise RuntimeError(
                            "Refusing to replace an unmanaged Komga library path: "
                            f"{target}"
                        )
                    self._atomic_copy(
                        source,
                        target,
                        replace_managed=managed is not None,
                    )
                    signature = self._projection_signature(target)
                    if signature is None:
                        raise RuntimeError(
                            "Komga projection is not a regular file after copy: "
                            f"{target}"
                        )
                    materialized = _CurrentProjection(
                        artifact_name=artifact_name,
                        signature=signature,
                    )
                self._record_materialized_projection(
                    connection,
                    name=name,
                    projection=materialized,
                )
                after = sequence
            connection.commit()

    @staticmethod
    def _current_projection(
        connection: sqlite3.Connection,
        *,
        name: str,
    ) -> _CurrentProjection | None:
        row = connection.execute(
            """
            SELECT
                artifact_name,
                device,
                inode,
                size_bytes,
                modified_ns,
                changed_ns
            FROM current_projection
            WHERE path_name = ?
            """,
            (name,),
        ).fetchone()
        if row is None:
            return None
        return _CurrentProjection(
            artifact_name=row[0],
            signature=_ProjectionSignature(
                device=row[1],
                inode=row[2],
                size_bytes=row[3],
                modified_ns=row[4],
                changed_ns=row[5],
            ),
        )

    @staticmethod
    def _record_materialized_projection(
        connection: sqlite3.Connection,
        *,
        name: str,
        projection: _CurrentProjection,
    ) -> None:
        signature = projection.signature
        connection.execute(
            """
            INSERT INTO reconciliation_current(
                path_name,
                artifact_name,
                device,
                inode,
                size_bytes,
                modified_ns,
                changed_ns
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                name,
                projection.artifact_name,
                signature.device,
                signature.inode,
                signature.size_bytes,
                signature.modified_ns,
                signature.changed_ns,
            ),
        )

    def _remove_stale_current_paths_from_plan(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        after: str | None = None
        while True:
            if after is None:
                cursor = connection.execute(
                    """
                    SELECT managed.path_name
                    FROM reconciliation_managed AS managed
                    LEFT JOIN reconciliation_plan AS planned
                      ON planned.path_name = managed.path_name
                    WHERE planned.path_name IS NULL
                    ORDER BY managed.path_name
                    LIMIT ?
                    """,
                    (PROJECTION_RECONCILIATION_PAGE_SIZE,),
                )
            else:
                cursor = connection.execute(
                    """
                    SELECT managed.path_name
                    FROM reconciliation_managed AS managed
                    LEFT JOIN reconciliation_plan AS planned
                      ON planned.path_name = managed.path_name
                    WHERE planned.path_name IS NULL
                      AND managed.path_name > ?
                    ORDER BY managed.path_name
                    LIMIT ?
                    """,
                    (after, PROJECTION_RECONCILIATION_PAGE_SIZE),
                )
            rows = cursor.fetchmany(PROJECTION_RECONCILIATION_PAGE_SIZE)
            if not rows:
                break
            for (name,) in rows:
                candidate = self._current_path_from_state_name(name)
                try:
                    candidate.unlink()
                except FileNotFoundError:
                    pass
                else:
                    self._fsync_directory(candidate.parent)
                self._remove_empty_current_parents(candidate.parent)
            after = rows[-1][0]

    def _commit_projection_state_from_plan(
        self,
        connection: sqlite3.Connection,
        *,
        revision: int | None,
        protection_id: str,
    ) -> None:
        planned_count = connection.execute(
            "SELECT COUNT(*) FROM reconciliation_plan"
        ).fetchone()
        materialized_count = connection.execute(
            "SELECT COUNT(*) FROM reconciliation_current"
        ).fetchone()
        if planned_count is None or planned_count != materialized_count:
            raise RuntimeError(
                "CBZ projection materialization did not produce every planned path"
            )
        connection.commit()
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute("""
                UPDATE artifacts
                SET published = 1
                WHERE EXISTS (
                    SELECT 1 FROM reconciliation_plan AS planned
                    WHERE planned.artifact_name = artifacts.name
                )
                """)
            if protection_id == LEGACY_PROTECTION_ID:
                connection.execute(
                    """
                    DELETE FROM protections
                    WHERE protection_id = ?
                      AND EXISTS (
                          SELECT 1 FROM reconciliation_plan AS planned
                          WHERE planned.artifact_name = protections.artifact_name
                      )
                    """,
                    (LEGACY_PROTECTION_ID,),
                )
            else:
                connection.execute(
                    "DELETE FROM protections WHERE protection_id = ?",
                    (protection_id,),
                )
            connection.execute("DELETE FROM current_projection")
            connection.execute("""
                INSERT INTO current_projection(
                    path_name,
                    artifact_name,
                    device,
                    inode,
                    size_bytes,
                    modified_ns,
                    changed_ns
                )
                SELECT
                    path_name,
                    artifact_name,
                    device,
                    inode,
                    size_bytes,
                    modified_ns,
                    changed_ns
                FROM reconciliation_current
                ORDER BY path_name
                """)
            connection.execute("DELETE FROM pending_projection")
            if revision is None:
                connection.execute("""
                    UPDATE projection_revision
                    SET pending_revision = NULL
                    WHERE singleton = 1
                    """)
            else:
                connection.execute(
                    """
                    UPDATE projection_revision
                    SET current_revision = ?, pending_revision = NULL
                    WHERE singleton = 1
                    """,
                    (revision,),
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    @staticmethod
    def _validate_protection_id(protection_id: str) -> None:
        if not isinstance(protection_id, str) or not protection_id:
            raise ValueError("CBZ publication protection ID must not be blank")

    @staticmethod
    def _protected_names(state: _ReconciliationState) -> set[str]:
        return set().union(*state.protections.values()) if state.protections else set()

    def _delete_unprotected_artifact_files(self, names: Collection[str]) -> None:
        for name in sorted(names):
            candidate = self._path_from_state_name(name)
            try:
                candidate.unlink()
            except FileNotFoundError:
                pass
            else:
                self._fsync_directory(candidate.parent)
            self._remove_empty_parents(candidate.parent)

    def _ensure_cbz(
        self,
        gallery: ScannedGallery,
        owned: Collection[str],
        excluded_file_sha256s: frozenset[str],
    ) -> CBZArtifact:
        prior_variant = any(
            PurePosixPath(name).name.startswith(f"{gallery.gid}-") for name in owned
        )
        reusable = self._find_reusable_cbz(
            gallery,
            owned,
            excluded_file_sha256s,
        )
        write_required = reusable is None
        if reusable is None:
            target, digest = self._build_cbz(gallery, excluded_file_sha256s)
        else:
            target, digest = reusable
        stat = target.stat()
        return CBZArtifact(
            gallery=CBZGalleryDescriptor.from_scanned_gallery(gallery),
            path=target.resolve(),
            size_bytes=stat.st_size,
            sha256=digest,
            modified_at=datetime.fromtimestamp(stat.st_mtime, tz=UTC),
            created=write_required and not prior_variant,
            rebuilt=write_required and prior_variant,
        )

    def _ensure_streaming_cbz(
        self,
        metadata: CBZPreparationMetadata,
        open_files: Callable[[], Iterator[CBZPreparationFile]],
        owned: Collection[str],
    ) -> CBZArtifact:
        gallery = metadata.gallery
        prior_variant = any(
            PurePosixPath(name).name.startswith(f"{gallery.gid}-") for name in owned
        )
        reusable = self._find_reusable_streaming_cbz(metadata, owned)
        write_required = reusable is None
        if reusable is None:
            target, digest = self._build_streaming_cbz(metadata, open_files)
        else:
            target, digest = reusable
        observed = target.stat()
        return CBZArtifact(
            gallery=gallery,
            path=target.resolve(),
            size_bytes=observed.st_size,
            sha256=digest,
            modified_at=datetime.fromtimestamp(observed.st_mtime, tz=UTC),
            created=write_required and not prior_variant,
            rebuilt=write_required and prior_variant,
        )

    def _find_reusable_streaming_cbz(
        self,
        metadata: CBZPreparationMetadata,
        owned: Collection[str],
    ) -> tuple[Path, str] | None:
        gallery = metadata.gallery
        for name in sorted(owned):
            if not PurePosixPath(name).name.startswith(
                f"{gallery.gid}-"
            ) or not name.endswith(".cbz"):
                continue
            candidate = self._path_from_state_name(name)
            if candidate.parent != self._storage_directory(gallery):
                continue
            if digest := self._matching_streaming_manifest_digest(
                candidate,
                metadata,
            ):
                return candidate, digest
        return None

    def _matching_streaming_manifest_digest(
        self,
        path: Path,
        metadata: CBZPreparationMetadata,
    ) -> str | None:
        if not path.is_file():
            return None
        try:
            with ZipFile(path) as archive:
                manifest = json.loads(archive.comment.decode("utf-8"))
        except BadZipFile, OSError, UnicodeDecodeError, json.JSONDecodeError:
            return None
        if not isinstance(manifest, dict):
            return None
        digest = _sha256_file(path)
        if (
            manifest.get("version") == CBZ_MANIFEST_VERSION
            and manifest.get("sourceDigest") == metadata.source_digest
            and manifest.get("contentDigest") == metadata.content_digest
            and manifest.get("exclusionPolicy") == "per-file-row-v1"
            and manifest.get("memberNaming") == "ordinal-source-name-v1"
            and manifest.get("resizePolicy") == "webtoon-short-side-no-upscale-v1"
            and manifest.get("maxImageShortSide") == self._max_image_short_side
            and path.name.startswith(f"{metadata.gallery.gid}-{digest}")
        ):
            return digest
        return None

    def _build_streaming_cbz(
        self,
        metadata: CBZPreparationMetadata,
        open_files: Callable[[], Iterator[CBZPreparationFile]],
    ) -> tuple[Path, str]:
        gallery = metadata.gallery
        output_directory = self._storage_directory(gallery)
        output_directory.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = self._create_owned_temporary_file(
            output_directory,
            prefix=ARTIFACT_TEMP_PREFIX,
        )
        os.close(descriptor)
        try:
            with temporary.open("w+b") as completed:
                with _BoundedZipWriter(completed) as archive:
                    included_index = 0
                    for preparation_file in open_files():
                        source_file = preparation_file.file
                        self._verify_staged_file_signature(
                            source_file,
                            stage="before CBZ selection",
                            required=True,
                        )
                        if preparation_file.excluded:
                            continue
                        member_name = self._streaming_member_name(
                            source_file,
                            included_index,
                        )
                        included_index += 1
                        self._write_member(archive, member_name, source_file)
                    archive.comment = json.dumps(
                        {
                            "version": CBZ_MANIFEST_VERSION,
                            "sourceDigest": metadata.source_digest,
                            "contentDigest": metadata.content_digest,
                            "exclusionPolicy": "per-file-row-v1",
                            "memberNaming": "ordinal-source-name-v1",
                            "resizePolicy": "webtoon-short-side-no-upscale-v1",
                            "maxImageShortSide": self._max_image_short_side,
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                completed.flush()
                os.fsync(completed.fileno())
            digest = _sha256_file(temporary)
            target = output_directory / _content_addressed_name(gallery.gid, digest)
            try:
                os.link(temporary, target)
            except FileExistsError:
                if target.is_symlink():
                    raise RuntimeError(
                        f"Refusing content-addressed artifact symlink: {target}"
                    )
                if _sha256_file(target) != digest:
                    target = output_directory / (
                        f"{gallery.gid}-{digest}-{uuid4().hex}.cbz"
                    )
                    os.link(temporary, target)
            temporary.unlink()
            self._fsync_directory(output_directory)
            return target, digest
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise

    def _find_reusable_cbz(
        self,
        gallery: ScannedGallery,
        owned: Collection[str],
        excluded_file_sha256s: frozenset[str],
    ) -> tuple[Path, str] | None:
        for name in sorted(owned):
            if not PurePosixPath(name).name.startswith(
                f"{gallery.gid}-"
            ) or not name.endswith(".cbz"):
                continue
            candidate = self._path_from_state_name(name)
            if candidate.parent != self._storage_directory(gallery):
                continue
            if digest := self._matching_manifest_digest(
                candidate,
                gallery,
                excluded_file_sha256s,
            ):
                return candidate, digest
        return None

    def _matching_manifest_digest(
        self,
        path: Path,
        gallery: ScannedGallery,
        excluded_file_sha256s: frozenset[str],
    ) -> str | None:
        if not path.is_file():
            return None
        try:
            with ZipFile(path) as archive:
                manifest = json.loads(archive.comment.decode("utf-8"))
        except BadZipFile, OSError, UnicodeDecodeError, json.JSONDecodeError:
            return None
        if not isinstance(manifest, dict):
            return None
        digest = _sha256_file(path)
        if (
            manifest.get("version") == CBZ_MANIFEST_VERSION
            and manifest.get("sourceDigest") == gallery.source_digest
            and manifest.get("contentDigest") == gallery.content_digest
            and manifest.get("excludedFileSha256s") == sorted(excluded_file_sha256s)
            and manifest.get("resizePolicy") == "webtoon-short-side-no-upscale-v1"
            and manifest.get("maxImageShortSide") == self._max_image_short_side
            and path.name.startswith(f"{gallery.gid}-{digest}")
        ):
            return digest
        return None

    def _build_cbz(
        self,
        gallery: ScannedGallery,
        excluded_file_sha256s: frozenset[str],
    ) -> tuple[Path, str]:
        output_directory = self._storage_directory(gallery)
        output_directory.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = self._create_owned_temporary_file(
            output_directory,
            prefix=ARTIFACT_TEMP_PREFIX,
        )
        os.close(descriptor)
        try:
            with ZipFile(
                temporary,
                mode="w",
                compression=ZIP_DEFLATED,
                compresslevel=9,
            ) as archive:
                member_names: set[str] = set()
                for file in gallery.files:
                    if file.sha256 in excluded_file_sha256s:
                        continue
                    member_name = self._member_name(file)
                    member_name = self._unique_member_name(
                        member_name,
                        file,
                        member_names,
                    )
                    member_names.add(member_name)
                    self._write_member(archive, member_name, file)
                archive.comment = json.dumps(
                    {
                        "version": CBZ_MANIFEST_VERSION,
                        "sourceDigest": gallery.source_digest,
                        "contentDigest": gallery.content_digest,
                        "excludedFileSha256s": sorted(excluded_file_sha256s),
                        "resizePolicy": "webtoon-short-side-no-upscale-v1",
                        "maxImageShortSide": self._max_image_short_side,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            with temporary.open("rb") as completed:
                os.fsync(completed.fileno())
            digest = _sha256_file(temporary)
            target = output_directory / _content_addressed_name(gallery.gid, digest)
            try:
                os.link(temporary, target)
            except FileExistsError:
                if target.is_symlink():
                    raise RuntimeError(
                        f"Refusing content-addressed artifact symlink: {target}"
                    )
                if _sha256_file(target) != digest:
                    target = output_directory / (
                        f"{gallery.gid}-{digest}-{uuid4().hex}.cbz"
                    )
                    os.link(temporary, target)
            temporary.unlink()
            self._fsync_directory(output_directory)
            return target, digest
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise

    @staticmethod
    def _member_name(file: ScannedFile) -> str:
        suffix = file.path.suffix.casefold()
        if suffix not in NORMALIZED_IMAGE_SUFFIXES and suffix != ".gif":
            return file.name
        if suffix == ".gif":
            return file.name
        return f"{file.path.stem}.jpg"

    def _streaming_member_name(self, file: ScannedFile, index: int) -> str:
        desired = PurePosixPath(self._member_name(file)).name
        return f"{index:012d}-{desired}"

    def _write_member(
        self,
        archive: ZipFile | _BoundedZipWriter,
        member_name: str,
        file: ScannedFile,
    ) -> None:
        self._verify_staged_file_signature(file, stage="before CBZ read")
        try:
            suffix = file.path.suffix.casefold()
            if suffix not in NORMALIZED_IMAGE_SUFFIXES and suffix != ".gif":
                with (
                    file.path.open("rb") as source,
                    archive.open(_zip_info(member_name), "w") as output,
                ):
                    shutil.copyfileobj(source, output, length=4 * 1024 * 1024)
            else:
                # Keep at most one transformed member in memory.  Large images
                # spill to a temporary file instead of retaining the complete CBZ
                # payload in RAM.
                with tempfile.SpooledTemporaryFile(
                    max_size=IMAGE_SPOOL_MEMORY_LIMIT,
                    mode="w+b",
                ) as transformed:
                    self._write_normalized_image(file, transformed)
                    transformed.seek(0)
                    with archive.open(_zip_info(member_name), "w") as output:
                        shutil.copyfileobj(
                            transformed,
                            output,
                            length=4 * 1024 * 1024,
                        )
        except Exception as error:
            try:
                self._verify_staged_file_signature(file, stage="during CBZ read")
            except CBZSourceChangedError as source_error:
                raise source_error from error
            raise
        self._verify_staged_file_signature(file, stage="after CBZ read")

    @staticmethod
    def _verify_staged_file_signature(
        file: ScannedFile,
        *,
        stage: str,
        required: bool = False,
    ) -> None:
        expected = file.signature
        if expected is None:
            if required:
                raise RuntimeError(
                    "Streamed CBZ source file is missing its staged stat signature: "
                    f"{file.path}"
                )
            return
        try:
            observed = file.path.stat()
        except OSError as error:
            raise CBZSourceChangedError(
                f"Unable to verify staged source file {stage}: {file.path}: {error}"
            ) from error
        actual = FileStatSignature(
            device=observed.st_dev,
            inode=observed.st_ino,
            size_bytes=observed.st_size,
            modified_ns=observed.st_mtime_ns,
            changed_ns=observed.st_ctime_ns,
        )
        if actual != expected:
            raise CBZSourceChangedError(
                f"Staged source file changed {stage}: {file.path}"
            )

    def _write_normalized_image(
        self,
        file: ScannedFile,
        output: tempfile.SpooledTemporaryFile[bytes],
    ) -> None:
        suffix = file.path.suffix.casefold()
        with Image.open(file.path) as source:
            image = ImageOps.exif_transpose(source)
            # Bound the short side, not the long side.  This retains readable
            # long-strip/webtoon pages while
            # Pillow's thumbnail() guarantee prevents upscaling small images.
            if image.height >= image.width:
                scale = self._max_image_short_side / image.width
                bounds = (
                    self._max_image_short_side,
                    int(image.height * scale),
                )
            else:
                scale = self._max_image_short_side / image.height
                bounds = (
                    int(image.width * scale),
                    self._max_image_short_side,
                )
            image.thumbnail(bounds, Image.Resampling.LANCZOS)
            if suffix == ".gif":
                image.save(output, format="GIF")
                return
            if image.has_transparency_data:
                foreground = image.convert("RGBA")
                background = Image.new("RGBA", foreground.size, "white")
                image = Image.alpha_composite(background, foreground).convert("RGB")
            if image.mode not in {"RGB", "L"}:
                image = image.convert("RGB")
            image.save(output, format="JPEG", quality=90, optimize=True)

    @staticmethod
    def _unique_member_name(
        desired: str,
        source_file: ScannedFile,
        members: set[str],
    ) -> str:
        used = {name.casefold() for name in members}
        if desired.casefold() not in used:
            return desired
        desired_path = PurePosixPath(desired)
        source_key = sha256(source_file.name.encode("utf-8")).hexdigest()[:12]
        candidate = f"{desired_path.stem}-{source_key}{desired_path.suffix}"
        suffix = 2
        while candidate.casefold() in used:
            candidate = (
                f"{desired_path.stem}-{source_key}-{suffix}{desired_path.suffix}"
            )
            suffix += 1
        return candidate

    def _storage_directory(
        self,
        gallery: ScannedGallery | CBZGalleryDescriptor,
    ) -> Path:
        return self._safe_grouped_directory(
            self._artifact_store_path,
            gallery,
            label="artifact_store_path",
        )

    def _current_directory(self, gallery: CBZGalleryDescriptor) -> Path:
        return self._safe_grouped_directory(
            self._cbz_path,
            gallery,
            label="cbz_path",
        )

    def _safe_grouped_directory(
        self,
        root: Path,
        gallery: ScannedGallery | CBZGalleryDescriptor,
        *,
        label: str,
    ) -> Path:
        directory = self._grouped_directory(root, gallery).resolve(strict=False)
        if not directory.is_relative_to(root):
            raise RuntimeError(f"Unsafe CBZ grouping path outside {label}: {directory}")
        return directory

    def _grouped_directory(
        self,
        root: Path,
        gallery: ScannedGallery | CBZGalleryDescriptor,
    ) -> Path:
        upload_date = gallery.upload_time.date()
        match self._grouping:
            case CBZGrouping.flat:
                return root
            case CBZGrouping.date_yyyy:
                return root / f"{upload_date.year:04d}"
            case CBZGrouping.date_yyyy_mm:
                return root / f"{upload_date.year:04d}" / f"{upload_date.month:02d}"
            case CBZGrouping.date_yyyy_mm_dd:
                return (
                    root
                    / f"{upload_date.year:04d}"
                    / f"{upload_date.month:02d}"
                    / f"{upload_date.day:02d}"
                )
        raise ValueError(f"Unsupported CBZ grouping: {self._grouping}")

    def _plan_current_view(
        self,
        artifacts: Iterable[CBZArtifact],
        managed_current: set[str],
    ) -> dict[str, CBZArtifact]:
        planned: dict[str, CBZArtifact] = {}
        planned_keys: set[str] = set()
        for artifact in artifacts:
            attempt = 0
            while True:
                leaf = self._current_leaf(artifact, attempt)
                target = self._current_directory(artifact.gallery) / leaf
                name = self._current_state_name(target)
                key = name.casefold()
                target_exists = target.exists() or target.is_symlink()
                if key not in planned_keys and (
                    not target_exists or name in managed_current
                ):
                    planned[name] = artifact
                    planned_keys.add(key)
                    break
                attempt += 1
                if attempt > 10_000:
                    raise RuntimeError(
                        "Unable to choose a unique managed CBZ name for "
                        f"{artifact.gallery.gallery_name!r}"
                    )
        return planned

    @staticmethod
    def _current_leaf(artifact: CBZArtifact, attempt: int) -> str:
        gallery = artifact.gallery
        if attempt == 0:
            source_name = gallery.gallery_name
        elif attempt == 1:
            source_name = f"{gallery.gallery_name} [{gallery.gid}]"
        else:
            source_name = f"{gallery.gallery_name} [{gallery.gid}-{attempt}]"
        return gallery_name_to_cbz_file_name(source_name)

    def _materialize_current_view(
        self,
        planned: dict[str, CBZArtifact],
        *,
        previously_managed: set[str],
        current: dict[str, _CurrentProjection],
    ) -> dict[str, _CurrentProjection]:
        materialized: dict[str, _CurrentProjection] = {}
        for name, artifact in planned.items():
            artifact_name = self._state_name(artifact.path)
            source = self._path_from_state_name(artifact_name)
            if not source.is_file():
                raise RuntimeError(f"Published CBZ artifact is unavailable: {source}")
            target = self._current_path_from_state_name(name)
            signature = self._projection_signature(target)
            previous = current.get(name)
            if (
                previous is not None
                and previous.artifact_name == artifact_name
                and previous.signature == signature
            ):
                materialized[name] = previous
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target = self._current_path_from_state_name(name)
            if (
                target.exists() or target.is_symlink()
            ) and name not in previously_managed:
                raise RuntimeError(
                    "Refusing to replace an unmanaged Komga library path: " f"{target}"
                )
            self._atomic_copy(
                source,
                target,
                replace_managed=name in previously_managed,
            )
            signature = self._projection_signature(target)
            if signature is None:
                raise RuntimeError(
                    f"Komga projection is not a regular file after copy: {target}"
                )
            materialized[name] = _CurrentProjection(
                artifact_name=artifact_name,
                signature=signature,
            )
        return materialized

    @staticmethod
    def _projection_signature(path: Path) -> _ProjectionSignature | None:
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            return None
        if not stat.S_ISREG(metadata.st_mode):
            return None
        return _ProjectionSignature(
            device=metadata.st_dev,
            inode=metadata.st_ino,
            size_bytes=metadata.st_size,
            modified_ns=metadata.st_mtime_ns,
            changed_ns=metadata.st_ctime_ns,
        )

    def _atomic_copy(
        self,
        source: Path,
        target: Path,
        *,
        replace_managed: bool,
    ) -> None:
        descriptor, temporary = self._create_owned_temporary_file(
            target.parent,
            prefix=PROJECTION_TEMP_PREFIX,
        )
        try:
            with (
                os.fdopen(descriptor, "wb") as output,
                source.open("rb") as source_file,
            ):
                shutil.copyfileobj(source_file, output, length=4 * 1024 * 1024)
                output.flush()
                os.fsync(output.fileno())
            if replace_managed:
                temporary.replace(target)
            else:
                # Linking the fully copied temporary into the same directory gives
                # us a portable no-replace operation.  Its inode has never belonged
                # to the immutable artifact, so the Komga view remains independent.
                os.link(temporary, target)
                temporary.unlink()
            self._fsync_directory(target.parent)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise

    def _recoverable_pending_names(
        self,
        state: _ProjectionJournalState,
    ) -> set[str]:
        recovered: set[str] = set()
        for name, artifact_name in state.pending.items():
            if name in state.current:
                recovered.add(name)
                continue
            target = self._current_path_from_state_name(name)
            if not target.exists() and not target.is_symlink():
                recovered.add(name)
                continue
            if target.is_symlink():
                continue
            source = self._path_from_state_name(artifact_name)
            try:
                if (
                    source.is_file()
                    and target.is_file()
                    and source.stat().st_size == target.stat().st_size
                    and _sha256_file(source) == _sha256_file(target)
                ):
                    recovered.add(name)
            except OSError:
                continue
        return recovered

    def _remove_stale_current_paths(self, stale_names: set[str]) -> None:
        for name in sorted(stale_names):
            candidate = self._current_path_from_state_name(name)
            try:
                candidate.unlink()
            except FileNotFoundError:
                pass
            else:
                self._fsync_directory(candidate.parent)
            self._remove_empty_current_parents(candidate.parent)

    def _remove_empty_current_parents(self, directory: Path) -> None:
        current = directory
        while current != self._cbz_path:
            try:
                current.rmdir()
            except OSError:
                return
            current = current.parent

    def _current_state_name(self, path: Path) -> str:
        try:
            parent = path.parent.resolve(strict=False)
            if not parent.is_relative_to(self._cbz_path):
                raise ValueError
            relative = (parent / path.name).relative_to(self._cbz_path)
        except ValueError as error:
            raise RuntimeError(
                f"Komga CBZ projection is outside cbz_path: {path}"
            ) from error
        if relative == Path() or ".." in relative.parts:
            raise RuntimeError(f"Unsafe Komga CBZ projection path: {path}")
        return relative.as_posix()

    def _current_path_from_state_name(self, name: str) -> Path:
        relative = PurePosixPath(name)
        if (
            relative.is_absolute()
            or not relative.parts
            or any(part in {"", ".", ".."} for part in relative.parts)
        ):
            raise RuntimeError(f"Unsafe Komga CBZ projection target: {name!r}")
        candidate = self._cbz_path.joinpath(*relative.parts)
        resolved_parent = candidate.parent.resolve(strict=False)
        if not resolved_parent.is_relative_to(self._cbz_path):
            raise RuntimeError(
                "Unsafe Komga CBZ projection target outside cbz_path: " f"{candidate}"
            )
        return resolved_parent / candidate.name

    def _state_name(self, path: Path) -> str:
        try:
            relative = path.resolve().relative_to(self._artifact_store_path)
        except ValueError as error:
            raise RuntimeError(
                f"CBZ artifact is outside artifact store: {path}"
            ) from error
        if relative == Path() or ".." in relative.parts:
            raise RuntimeError(f"Unsafe CBZ artifact path: {path}")
        return relative.as_posix()

    def _path_from_state_name(self, name: str) -> Path:
        relative = PurePosixPath(name)
        if (
            relative.is_absolute()
            or not relative.parts
            or any(part in {"", ".", ".."} for part in relative.parts)
        ):
            raise RuntimeError(f"Unsafe CBZ reconciliation target: {name!r}")
        candidate = self._artifact_store_path.joinpath(*relative.parts)
        resolved_parent = candidate.parent.resolve(strict=False)
        if not resolved_parent.is_relative_to(self._artifact_store_path):
            raise RuntimeError(
                "Unsafe CBZ reconciliation target outside artifact store: "
                f"{candidate}"
            )
        resolved_candidate = resolved_parent / candidate.name
        if resolved_candidate.is_symlink():
            raise RuntimeError(
                f"Unsafe symlink in CBZ artifact state: {resolved_candidate}"
            )
        return resolved_candidate

    def _remove_empty_parents(self, directory: Path) -> None:
        current = directory
        while current != self._artifact_store_path:
            try:
                current.rmdir()
            except OSError:
                return
            current = current.parent

    @staticmethod
    def _create_owned_temporary_file(
        directory: Path,
        *,
        prefix: str,
    ) -> tuple[int, Path]:
        if prefix not in _OWNED_TEMP_PATTERN_BY_PREFIX:
            raise ValueError(f"Unsupported ingest temporary-file prefix: {prefix}")
        for _attempt in range(100):
            candidate = directory / f"{prefix}{uuid4().hex}.tmp"
            try:
                descriptor = os.open(
                    candidate,
                    os.O_CREAT | os.O_EXCL | os.O_RDWR,
                    0o600,
                )
            except FileExistsError:
                continue
            return descriptor, candidate
        raise RuntimeError(
            f"Unable to allocate an ingest temporary file in {directory}"
        )

    def _cleanup_stale_temporary_files(self) -> None:
        """Remove only old files in ingest's reserved temporary namespaces.

        The caller holds the artifact-store flock, so no cooperating publisher
        can own a live temporary file.  The age floor also protects recent files
        from a process using a misconfigured coordination domain.
        """

        removed_artifacts = self._cleanup_owned_temporary_files(
            self._artifact_store_path,
            prefix=ARTIFACT_TEMP_PREFIX,
            recursive=True,
        )
        removed_projections = self._cleanup_owned_temporary_files(
            self._cbz_path,
            prefix=PROJECTION_TEMP_PREFIX,
            recursive=True,
        )
        removed_states = self._cleanup_owned_temporary_files(
            self._artifact_store_path,
            prefix=STATE_TEMP_PREFIX,
            recursive=False,
        )
        removed_state_databases = self._cleanup_owned_temporary_files(
            self._artifact_store_path,
            prefix=STATE_DATABASE_TEMP_PREFIX,
            recursive=False,
        )
        if (
            removed_artifacts
            or removed_projections
            or removed_states
            or removed_state_databases
        ):
            self._event_logger(
                "Stale ingest temporary files removed: "
                f"artifact_builds={removed_artifacts} "
                f"projections={removed_projections} states={removed_states} "
                f"state_databases={removed_state_databases}"
            )

    def _cleanup_owned_temporary_files(
        self,
        root: Path,
        *,
        prefix: str,
        recursive: bool,
    ) -> int:
        pattern = _OWNED_TEMP_PATTERN_BY_PREFIX[prefix]
        if not root.is_dir():
            return 0
        if recursive:
            candidates: list[Path] = []
            for directory, child_directories, file_names in os.walk(
                root,
                topdown=True,
                followlinks=False,
            ):
                directory_path = Path(directory)
                child_directories[:] = [
                    name
                    for name in child_directories
                    if not (directory_path / name).is_symlink()
                ]
                try:
                    resolved_directory = directory_path.resolve(strict=True)
                except OSError:
                    continue
                if not resolved_directory.is_relative_to(root):
                    child_directories.clear()
                    continue
                candidates.extend(
                    resolved_directory / name
                    for name in file_names
                    if pattern.fullmatch(name)
                )
        else:
            try:
                candidates = [
                    candidate
                    for candidate in root.iterdir()
                    if pattern.fullmatch(candidate.name)
                ]
            except OSError as error:
                self._event_logger(
                    "Unable to inspect ingest temporary files: "
                    f"root={root} error={error!r}"
                )
                return 0

        now_ns = time_ns()
        removed = 0
        for candidate in sorted(candidates):
            try:
                metadata = candidate.lstat()
                if not stat.S_ISREG(metadata.st_mode):
                    continue
                if now_ns - metadata.st_mtime_ns < self._stale_temp_age_ns:
                    continue
                descriptor = os.open(
                    candidate,
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                )
                try:
                    opened = os.fstat(descriptor)
                    current = candidate.lstat()
                    if (
                        not stat.S_ISREG(opened.st_mode)
                        or (opened.st_dev, opened.st_ino)
                        != (current.st_dev, current.st_ino)
                        or now_ns - opened.st_mtime_ns < self._stale_temp_age_ns
                    ):
                        continue
                    candidate.unlink()
                finally:
                    os.close(descriptor)
                self._fsync_directory(candidate.parent)
                removed += 1
            except FileNotFoundError:
                continue
            except OSError as error:
                self._event_logger(
                    "Unable to remove stale ingest temporary file: "
                    f"path={candidate} error={error!r}"
                )
        return removed

    @contextmanager
    def _locked_state(self) -> Iterator[None]:
        self._artifact_store_path.mkdir(parents=True, exist_ok=True)
        with self._process_lock:
            if self._state_lock_depth:
                self._state_lock_depth += 1
                try:
                    yield
                finally:
                    self._state_lock_depth -= 1
                return
            with self._state_lock_path.open("a+b") as lock_file:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                self._state_lock_depth = 1
                try:
                    yield
                finally:
                    self._state_lock_depth = 0
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    @staticmethod
    def _fsync_directory(directory: Path) -> None:
        descriptor = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _ensure_state_database(self) -> None:
        if self._state_database_ready:
            return
        if self._state_database_path.exists() or self._state_database_path.is_symlink():
            self._validate_state_database()
            self._write_state_database_marker_if_missing()
            self._state_database_ready = True
            return
        if (
            self._state_database_marker_path.exists()
            or self._state_database_marker_path.is_symlink()
        ):
            raise RuntimeError(
                "CBZ SQLite state is missing after migration; restore "
                f"{self._state_database_path} from backup instead of replaying the "
                "possibly stale legacy JSON state"
            )

        # Parse and validate the complete legacy document before creating any
        # authoritative database.  Unknown versions and unsafe paths therefore
        # remain fail-closed and a failed migration can simply be retried.
        legacy_state = self._read_legacy_state()
        descriptor, temporary = self._create_owned_temporary_file(
            self._artifact_store_path,
            prefix=STATE_DATABASE_TEMP_PREFIX,
        )
        os.close(descriptor)
        try:
            connection = sqlite3.connect(temporary)
            try:
                # This database is not visible until its final atomic rename.
                # Avoid a migration-temp sidecar, then restore durable rollback
                # journaling before the completed file becomes authoritative.
                if connection.execute("PRAGMA journal_mode = OFF").fetchone() != (
                    "off",
                ):
                    raise RuntimeError("Unable to disable migration-temp journaling")
                connection.execute("PRAGMA synchronous = FULL")
                self._create_state_schema(connection)
                self._replace_state_rows(connection, legacy_state)
                connection.commit()
                if connection.execute("PRAGMA journal_mode = DELETE").fetchone() != (
                    "delete",
                ):
                    raise RuntimeError(
                        "Unable to enable durable CBZ SQLite rollback journaling"
                    )
                result = connection.execute("PRAGMA integrity_check").fetchone()
                if result != ("ok",):
                    raise RuntimeError(
                        f"Unable to create valid CBZ SQLite state: {result!r}"
                    )
            finally:
                connection.close()
            os.chmod(temporary, 0o600)
            with temporary.open("rb") as state_file:
                os.fsync(state_file.fileno())
            temporary.replace(self._state_database_path)
            self._fsync_directory(self._artifact_store_path)
            self._write_state_database_marker_if_missing()
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        self._state_database_ready = True

    def _validate_state_database(self) -> None:
        try:
            metadata = self._state_database_path.lstat()
        except OSError as error:
            raise RuntimeError(
                f"Unable to inspect CBZ SQLite state {self._state_database_path}: "
                f"{error}"
            ) from error
        if not stat.S_ISREG(metadata.st_mode) or self._state_database_path.is_symlink():
            raise RuntimeError(
                f"CBZ SQLite state is not a regular file: {self._state_database_path}"
            )
        os.chmod(self._state_database_path, 0o600)
        try:
            with self._state_connection() as connection:
                version = connection.execute(
                    "SELECT value FROM state_meta WHERE key = 'schema_version'"
                ).fetchone()
                if version != (str(STATE_DATABASE_VERSION),):
                    raise RuntimeError(
                        "Unsupported CBZ SQLite state version: "
                        f"{self._state_database_path}"
                    )
                for (
                    table_name,
                    expected_columns,
                ) in _STATE_DATABASE_TABLE_COLUMNS.items():
                    table_info = tuple(
                        connection.execute(f"PRAGMA table_info({table_name})")
                    )
                    actual_columns = tuple(row[1] for row in table_info)
                    if actual_columns != expected_columns:
                        raise RuntimeError(
                            "Invalid CBZ SQLite state table: "
                            f"path={self._state_database_path} table={table_name}"
                        )
                    actual_primary_key = tuple(
                        row[1]
                        for row in sorted(table_info, key=lambda item: item[5])
                        if row[5]
                    )
                    if actual_primary_key != _STATE_DATABASE_PRIMARY_KEYS[table_name]:
                        raise RuntimeError(
                            "Invalid CBZ SQLite state primary key: "
                            f"path={self._state_database_path} table={table_name}"
                        )
                for (
                    index_name,
                    expected_columns,
                ) in _STATE_DATABASE_INDEX_COLUMNS.items():
                    actual_columns = tuple(
                        row[2]
                        for row in connection.execute(
                            f"PRAGMA index_info({index_name})"
                        )
                    )
                    if actual_columns != expected_columns:
                        raise RuntimeError(
                            "Invalid CBZ SQLite state index: "
                            f"path={self._state_database_path} index={index_name}"
                        )
                integrity = connection.execute("PRAGMA quick_check").fetchone()
                if integrity != ("ok",):
                    raise RuntimeError(
                        "Invalid CBZ SQLite state: "
                        f"{self._state_database_path}: {integrity!r}"
                    )
                if (
                    connection.execute("PRAGMA foreign_key_check").fetchone()
                    is not None
                ):
                    raise RuntimeError(
                        f"Invalid CBZ SQLite state: {self._state_database_path}"
                    )
                for (name,) in connection.execute("SELECT name FROM artifacts"):
                    self._path_from_state_name(name)
                for name, artifact_name in connection.execute(
                    "SELECT path_name, artifact_name FROM current_projection"
                ):
                    self._current_path_from_state_name(name)
                    self._path_from_state_name(artifact_name)
                for name, artifact_name in connection.execute(
                    "SELECT path_name, artifact_name FROM pending_projection"
                ):
                    self._current_path_from_state_name(name)
                    self._path_from_state_name(artifact_name)
        except sqlite3.Error as error:
            raise RuntimeError(
                f"Unable to read CBZ SQLite state {self._state_database_path}: {error}"
            ) from error

    @contextmanager
    def _state_connection(self) -> Iterator[sqlite3.Connection]:
        try:
            metadata = self._state_database_path.lstat()
        except OSError as error:
            raise RuntimeError(
                f"Unable to inspect CBZ SQLite state {self._state_database_path}: "
                f"{error}"
            ) from error
        if not stat.S_ISREG(metadata.st_mode) or self._state_database_path.is_symlink():
            raise RuntimeError(
                f"CBZ SQLite state is not a regular file: {self._state_database_path}"
            )
        os.chmod(self._state_database_path, 0o600)
        try:
            connection = sqlite3.connect(self._state_database_path, timeout=30.0)
            connection.execute("PRAGMA foreign_keys = ON")
            journal_mode = connection.execute("PRAGMA journal_mode = DELETE").fetchone()
            if journal_mode != ("delete",):
                raise RuntimeError(
                    "Unable to enable durable CBZ SQLite rollback journaling: "
                    f"{self._state_database_path}: {journal_mode!r}"
                )
            connection.execute("PRAGMA synchronous = FULL")
            if hasattr(os, "F_FULLFSYNC"):
                connection.execute("PRAGMA fullfsync = ON")
            yield connection
        finally:
            if "connection" in locals():
                connection.close()

    @staticmethod
    def _create_state_schema(connection: sqlite3.Connection) -> None:
        connection.executescript("""
            CREATE TABLE state_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE artifacts (
                name TEXT PRIMARY KEY,
                gid INTEGER,
                published INTEGER NOT NULL DEFAULT 0
                    CHECK (published IN (0, 1)),
                CHECK (gid IS NULL OR gid > 0)
            );
            CREATE INDEX artifacts_gid_name_idx ON artifacts(gid, name);
            CREATE INDEX artifacts_prune_idx ON artifacts(published, name);
            CREATE TABLE protections (
                protection_id TEXT NOT NULL,
                artifact_name TEXT NOT NULL
                    REFERENCES artifacts(name) ON DELETE CASCADE,
                PRIMARY KEY (protection_id, artifact_name)
            );
            CREATE INDEX protections_artifact_idx
                ON protections(artifact_name, protection_id);
            CREATE TABLE current_projection (
                path_name TEXT PRIMARY KEY,
                artifact_name TEXT NOT NULL
                    REFERENCES artifacts(name) ON DELETE RESTRICT,
                device INTEGER NOT NULL CHECK (device >= 0),
                inode INTEGER NOT NULL CHECK (inode >= 0),
                size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
                modified_ns INTEGER NOT NULL,
                changed_ns INTEGER NOT NULL
            );
            CREATE INDEX current_projection_artifact_idx
                ON current_projection(artifact_name, path_name);
            CREATE TABLE pending_projection (
                path_name TEXT PRIMARY KEY,
                artifact_name TEXT NOT NULL
                    REFERENCES artifacts(name) ON DELETE RESTRICT
            );
            CREATE INDEX pending_projection_artifact_idx
                ON pending_projection(artifact_name, path_name);
            CREATE TABLE projection_revision (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                current_revision INTEGER CHECK (
                    current_revision IS NULL OR current_revision >= 0
                ),
                pending_revision INTEGER CHECK (
                    pending_revision IS NULL OR pending_revision >= 0
                )
            );
            INSERT INTO state_meta(key, value)
            VALUES ('schema_version', '1');
            INSERT INTO projection_revision(
                singleton,
                current_revision,
                pending_revision
            ) VALUES (1, NULL, NULL);
            """)

    def _write_state_database_marker_if_missing(self) -> None:
        if (
            self._state_database_marker_path.exists()
            or self._state_database_marker_path.is_symlink()
        ):
            try:
                metadata = self._state_database_marker_path.lstat()
            except OSError as error:
                raise RuntimeError(
                    "Unable to read CBZ SQLite state marker: "
                    f"{self._state_database_marker_path}: {error}"
                ) from error
            if self._state_database_marker_path.is_symlink() or not stat.S_ISREG(
                metadata.st_mode
            ):
                raise RuntimeError(
                    "Invalid CBZ SQLite state marker: "
                    f"{self._state_database_marker_path}"
                )
            try:
                marker_version = self._state_database_marker_path.read_text(
                    encoding="ascii"
                )
            except (OSError, UnicodeDecodeError) as error:
                raise RuntimeError(
                    "Unable to read CBZ SQLite state marker: "
                    f"{self._state_database_marker_path}: {error}"
                ) from error
            if marker_version != f"{STATE_DATABASE_VERSION}\n":
                raise RuntimeError(
                    "Invalid CBZ SQLite state marker: "
                    f"{self._state_database_marker_path}"
                )
            os.chmod(self._state_database_marker_path, 0o600)
            return
        descriptor, temporary = self._create_owned_temporary_file(
            self._artifact_store_path,
            prefix=STATE_TEMP_PREFIX,
        )
        try:
            with os.fdopen(descriptor, "w", encoding="ascii") as marker:
                marker.write(f"{STATE_DATABASE_VERSION}\n")
                marker.flush()
                os.fsync(marker.fileno())
            temporary.replace(self._state_database_marker_path)
            os.chmod(self._state_database_marker_path, 0o600)
            self._fsync_directory(self._artifact_store_path)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise

    @staticmethod
    def _gid_from_artifact_name(name: str) -> int | None:
        gid_text, separator, _suffix = PurePosixPath(name).name.partition("-")
        if separator and gid_text.isdecimal() and int(gid_text) > 0:
            return int(gid_text)
        return None

    def _owned_names_for_gid(self, gid: int) -> tuple[str, ...]:
        self._ensure_state_database()
        with self._state_connection() as connection:
            return tuple(
                name
                for (name,) in connection.execute(
                    "SELECT name FROM artifacts WHERE gid = ? ORDER BY name",
                    (gid,),
                )
            )

    def _record_owned_artifact(self, name: str, *, gid: int) -> None:
        self._ensure_state_database()
        with self._state_connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                existing = connection.execute(
                    "SELECT gid FROM artifacts WHERE name = ?",
                    (name,),
                ).fetchone()
                if existing is not None and existing[0] not in {None, gid}:
                    raise RuntimeError(
                        "CBZ artifact state has a conflicting GID: "
                        f"artifact={name!r} existing={existing[0]} incoming={gid}"
                    )
                connection.execute(
                    """
                    INSERT INTO artifacts(name, gid, published)
                    VALUES (?, ?, 0)
                    ON CONFLICT(name) DO UPDATE SET gid = COALESCE(gid, excluded.gid)
                    """,
                    (name, gid),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _artifact_statuses(self, names: Collection[str]) -> dict[str, bool]:
        self._ensure_state_database()
        with self._state_connection() as connection:
            return {
                name: bool(row[0])
                for name in names
                if (
                    row := connection.execute(
                        "SELECT published FROM artifacts WHERE name = ?",
                        (name,),
                    ).fetchone()
                )
                is not None
            }

    def _add_protections(self, protection_id: str, names: Collection[str]) -> None:
        self._ensure_state_database()
        with self._state_connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.executemany(
                    """
                    INSERT OR IGNORE INTO protections(protection_id, artifact_name)
                    VALUES (?, ?)
                    """,
                    ((protection_id, name) for name in names),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _remove_protection(self, protection_id: str) -> None:
        self._ensure_state_database()
        with self._state_connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    "DELETE FROM protections WHERE protection_id = ?",
                    (protection_id,),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _read_projection_journal(self) -> _ProjectionJournalState:
        self._ensure_state_database()
        with self._state_connection() as connection:
            current = {
                name: _CurrentProjection(
                    artifact_name=artifact_name,
                    signature=_ProjectionSignature(
                        device=device,
                        inode=inode,
                        size_bytes=size_bytes,
                        modified_ns=modified_ns,
                        changed_ns=changed_ns,
                    ),
                )
                for (
                    name,
                    artifact_name,
                    device,
                    inode,
                    size_bytes,
                    modified_ns,
                    changed_ns,
                ) in connection.execute(
                    """
                    SELECT
                        path_name,
                        artifact_name,
                        device,
                        inode,
                        size_bytes,
                        modified_ns,
                        changed_ns
                    FROM current_projection
                    """
                )
            }
            pending = dict(
                connection.execute(
                    "SELECT path_name, artifact_name FROM pending_projection"
                )
            )
            revision_row = connection.execute("""
                SELECT current_revision, pending_revision
                FROM projection_revision
                WHERE singleton = 1
                """).fetchone()
        if revision_row is None:
            raise RuntimeError(f"Invalid CBZ SQLite state: {self._state_database_path}")
        return _ProjectionJournalState(
            current=current,
            current_revision=revision_row[0],
            pending=pending,
            pending_revision=revision_row[1],
        )

    def _replace_pending_projection(
        self,
        pending: dict[str, str],
        *,
        revision: int | None,
    ) -> None:
        self._ensure_state_database()
        with self._state_connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute("DELETE FROM pending_projection")
                connection.executemany(
                    """
                    INSERT INTO pending_projection(path_name, artifact_name)
                    VALUES (?, ?)
                    """,
                    sorted(pending.items()),
                )
                connection.execute(
                    """
                    UPDATE projection_revision
                    SET pending_revision = ?
                    WHERE singleton = 1
                    """,
                    (revision,),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _commit_projection_state(
        self,
        current: dict[str, _CurrentProjection],
        *,
        published_names: Collection[str],
        revision: int | None,
        protection_id: str,
    ) -> None:
        self._ensure_state_database()
        with self._state_connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.executemany(
                    "UPDATE artifacts SET published = 1 WHERE name = ?",
                    ((name,) for name in published_names),
                )
                if protection_id == LEGACY_PROTECTION_ID:
                    connection.executemany(
                        """
                        DELETE FROM protections
                        WHERE protection_id = ? AND artifact_name = ?
                        """,
                        ((LEGACY_PROTECTION_ID, name) for name in published_names),
                    )
                else:
                    connection.execute(
                        "DELETE FROM protections WHERE protection_id = ?",
                        (protection_id,),
                    )
                connection.execute("DELETE FROM current_projection")
                connection.executemany(
                    """
                    INSERT INTO current_projection(
                        path_name,
                        artifact_name,
                        device,
                        inode,
                        size_bytes,
                        modified_ns,
                        changed_ns
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        (
                            name,
                            projection.artifact_name,
                            projection.signature.device,
                            projection.signature.inode,
                            projection.signature.size_bytes,
                            projection.signature.modified_ns,
                            projection.signature.changed_ns,
                        )
                        for name, projection in sorted(current.items())
                    ),
                )
                connection.execute("DELETE FROM pending_projection")
                if revision is None:
                    connection.execute("""
                        UPDATE projection_revision
                        SET pending_revision = NULL
                        WHERE singleton = 1
                        """)
                else:
                    connection.execute(
                        """
                        UPDATE projection_revision
                        SET current_revision = ?, pending_revision = NULL
                        WHERE singleton = 1
                        """,
                        (revision,),
                    )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _prune_unprotected_artifacts(self) -> None:
        self._ensure_state_database()
        while True:
            with self._state_connection() as connection:
                cursor = connection.execute(
                    """
                    SELECT artifact.name
                    FROM artifacts AS artifact
                    WHERE artifact.published = 0
                      AND NOT EXISTS (
                          SELECT 1 FROM protections AS protection
                          WHERE protection.artifact_name = artifact.name
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM current_projection AS current_item
                          WHERE current_item.artifact_name = artifact.name
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM pending_projection AS pending_item
                          WHERE pending_item.artifact_name = artifact.name
                      )
                    ORDER BY artifact.name
                    LIMIT ?
                    """,
                    (PROJECTION_RECONCILIATION_PAGE_SIZE,),
                )
                candidates = tuple(
                    name
                    for (name,) in cursor.fetchmany(PROJECTION_RECONCILIATION_PAGE_SIZE)
                )
            if not candidates:
                return
            self._delete_unprotected_artifact_files(candidates)
            with self._state_connection() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.executemany(
                        """
                        DELETE FROM artifacts
                        WHERE name = ?
                          AND published = 0
                          AND NOT EXISTS (
                              SELECT 1 FROM protections
                              WHERE protections.artifact_name = artifacts.name
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM current_projection
                              WHERE current_projection.artifact_name = artifacts.name
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM pending_projection
                              WHERE pending_projection.artifact_name = artifacts.name
                          )
                        """,
                        ((name,) for name in candidates),
                    )
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise

    def _replace_state_rows(
        self,
        connection: sqlite3.Connection,
        state: _ReconciliationState,
    ) -> None:
        connection.execute("DELETE FROM protections")
        connection.execute("DELETE FROM current_projection")
        connection.execute("DELETE FROM pending_projection")
        connection.execute("DELETE FROM artifacts")
        connection.executemany(
            "INSERT INTO artifacts(name, gid, published) VALUES (?, ?, ?)",
            (
                (
                    name,
                    self._gid_from_artifact_name(name),
                    int(name in state.published),
                )
                for name in sorted(state.owned)
            ),
        )
        connection.executemany(
            """
            INSERT INTO protections(protection_id, artifact_name)
            VALUES (?, ?)
            """,
            (
                (protection_id, name)
                for protection_id, names in sorted(state.protections.items())
                for name in sorted(names)
            ),
        )
        connection.executemany(
            """
            INSERT INTO current_projection(
                path_name,
                artifact_name,
                device,
                inode,
                size_bytes,
                modified_ns,
                changed_ns
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    name,
                    projection.artifact_name,
                    projection.signature.device,
                    projection.signature.inode,
                    projection.signature.size_bytes,
                    projection.signature.modified_ns,
                    projection.signature.changed_ns,
                )
                for name, projection in sorted(state.current.items())
            ),
        )
        connection.executemany(
            """
            INSERT INTO pending_projection(path_name, artifact_name)
            VALUES (?, ?)
            """,
            sorted(state.pending.items()),
        )
        connection.execute(
            """
            UPDATE projection_revision
            SET current_revision = ?, pending_revision = ?
            WHERE singleton = 1
            """,
            (state.current_revision, state.pending_revision),
        )

    def _read_legacy_state(self) -> _ReconciliationState:
        try:
            metadata = self._state_path.lstat()
        except FileNotFoundError:
            return _ReconciliationState(set(), set(), {}, {})
        except OSError as error:
            raise RuntimeError(
                f"Unable to inspect CBZ state {self._state_path}: {error}"
            ) from error
        if not stat.S_ISREG(metadata.st_mode) or self._state_path.is_symlink():
            raise RuntimeError(
                f"CBZ legacy state is not a regular file: {self._state_path}"
            )
        try:
            raw = json.loads(self._state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise RuntimeError(
                f"Unable to read CBZ state {self._state_path}: {error}"
            ) from error
        version_value = raw.get("version") if isinstance(raw, dict) else None
        if (
            not isinstance(version_value, int)
            or isinstance(version_value, bool)
            or version_value not in {1, STATE_VERSION}
        ):
            raise RuntimeError(f"Unsupported CBZ state version: {self._state_path}")
        version = version_value
        names = raw.get("owned")
        if not isinstance(names, list) or not all(
            isinstance(name, str) for name in names
        ):
            raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
        owned = set(names)
        for name in owned:
            self._path_from_state_name(name)
        published_names = raw.get("published")
        if not isinstance(published_names, list) or not all(
            isinstance(name, str) for name in published_names
        ):
            raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
        published = set(published_names)
        for name in published:
            self._path_from_state_name(name)
        if not published <= owned:
            raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
        protections: dict[str, set[str]] = {}
        protections_raw = raw.get("protections")
        if version == 1 or protections_raw is None:
            protected_names = raw.get("protected")
            if not isinstance(protected_names, list) or not all(
                isinstance(name, str) for name in protected_names
            ):
                raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
            if protected_names:
                protections[LEGACY_PROTECTION_ID] = set(protected_names)
        else:
            if not isinstance(protections_raw, dict):
                raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
            for protection_id, protected_names in protections_raw.items():
                if (
                    not isinstance(protection_id, str)
                    or not protection_id
                    or not isinstance(protected_names, list)
                    or not all(isinstance(name, str) for name in protected_names)
                ):
                    raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
                protections[protection_id] = set(protected_names)
        protected = self._protected_names(
            _ReconciliationState(owned, published, protections, {})
        )
        for name in protected:
            self._path_from_state_name(name)
        if not protected <= owned:
            raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
        current = self._read_current_projections(raw.get("current"), owned)

        current_revision = self._optional_revision(
            raw.get("currentRevision"),
            label="currentRevision",
        )
        pending_revision = self._optional_revision(
            raw.get("pendingRevision"),
            label="pendingRevision",
        )
        pending_raw = raw.get("pending")
        if not isinstance(pending_raw, dict) or not all(
            isinstance(name, str) and isinstance(artifact_name, str)
            for name, artifact_name in pending_raw.items()
        ):
            raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
        pending = dict(pending_raw)
        for name, artifact_name in pending.items():
            self._current_path_from_state_name(name)
            self._path_from_state_name(artifact_name)
        if not set(pending.values()) <= owned:
            raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
        return _ReconciliationState(
            owned=owned,
            published=published,
            protections=protections,
            current=current,
            current_revision=current_revision,
            pending=pending,
            pending_revision=pending_revision,
        )

    def _read_current_projections(
        self,
        raw: object,
        owned: set[str],
    ) -> dict[str, _CurrentProjection]:
        if not isinstance(raw, dict):
            raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
        current: dict[str, _CurrentProjection] = {}
        for name, projection_raw in raw.items():
            if not isinstance(name, str) or not isinstance(projection_raw, dict):
                raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
            artifact_name = projection_raw.get("artifact")
            signature_raw = projection_raw.get("signature")
            if not isinstance(artifact_name, str) or signature_raw is None:
                raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
            self._current_path_from_state_name(name)
            self._path_from_state_name(artifact_name)
            if artifact_name not in owned:
                raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
            current[name] = _CurrentProjection(
                artifact_name=artifact_name,
                signature=self._read_projection_signature(signature_raw),
            )
        return current

    def _read_projection_signature(self, raw: object) -> _ProjectionSignature:
        if not isinstance(raw, dict):
            raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")

        def integer(name: str, *, nonnegative: bool = False) -> int:
            value = raw.get(name)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or (nonnegative and value < 0)
            ):
                raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
            return value

        return _ProjectionSignature(
            device=integer("device", nonnegative=True),
            inode=integer("inode", nonnegative=True),
            size_bytes=integer("sizeBytes", nonnegative=True),
            modified_ns=integer("modifiedNs"),
            changed_ns=integer("changedNs"),
        )

    def _optional_revision(self, value: object, *, label: str) -> int | None:
        if value is None:
            return None
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise RuntimeError(f"Invalid {label} in CBZ state file: {self._state_path}")
        return value
