"""Replayable, bounded-memory filesystem observations for vNext ingest."""

from __future__ import annotations

__all__ = [
    "FILESYSTEM_OBSERVATION_VERSION",
    "FilesystemArtifactSourceRole",
    "FilesystemEntryType",
    "FilesystemFileObservation",
    "FilesystemGalleryMetadata",
    "FilesystemGalleryObservation",
    "FilesystemObservationError",
    "FilesystemPage",
    "FilesystemSource",
    "FilesystemStat",
]

import os
import sqlite3
import stat
import tempfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import IntEnum, StrEnum
from hashlib import sha256
from pathlib import Path

from h2h_galleryinfo_parser import parse_galleryinfo

FILESYSTEM_OBSERVATION_VERSION = 2
GALLERY_INFO_NAME = "galleryinfo.txt"
_READ_BYTES = 4 * 1024 * 1024
_ENTRY_AUDIT_PREFIX = b"h2hdb-ingest-filesystem-entry-audit-v2\0"
_GALLERY_AUDIT_PREFIX = b"h2hdb-ingest-filesystem-gallery-audit-v2\0"
_PAGE_SUFFIXES = frozenset(
    {b".avif", b".bmp", b".gif", b".jpeg", b".jpg", b".png", b".webp"}
)


def _noop_checkpoint() -> None:
    pass


class FilesystemObservationError(RuntimeError):
    """The source tree is unsafe, malformed, unavailable, or changed."""


class FilesystemEntryType(IntEnum):
    REGULAR = 0
    DIRECTORY = 1
    SYMLINK = 2
    OTHER = 3


class FilesystemArtifactSourceRole(StrEnum):
    """Adapter-owned interpretation of a regular source file."""

    METADATA = "metadata"
    PAGE = "page"
    OTHER = "other"


@dataclass(frozen=True, slots=True)
class FilesystemStat:
    device: int
    inode: int
    size_bytes: int
    modified_ns: int
    changed_ns: int

    def __post_init__(self) -> None:
        for label, value in (("device", self.device), ("inode", self.inode)):
            if not 0 <= value < 1 << 64:
                raise FilesystemObservationError(f"{label} is outside uint64")
        if not 0 <= self.size_bytes < 1 << 63:
            raise FilesystemObservationError("size_bytes is outside int63")
        for label, value in (
            ("modified_ns", self.modified_ns),
            ("changed_ns", self.changed_ns),
        ):
            if not -(1 << 63) <= value < 1 << 63:
                raise FilesystemObservationError(f"{label} is outside int64")

    @classmethod
    def from_os_stat(cls, value: os.stat_result) -> FilesystemStat:
        return cls(
            device=value.st_dev,
            inode=value.st_ino,
            size_bytes=value.st_size,
            modified_ns=value.st_mtime_ns,
            changed_ns=value.st_ctime_ns,
        )


@dataclass(frozen=True, slots=True)
class FilesystemFileObservation:
    folder: Path
    name_bytes: bytes
    stat: FilesystemStat
    artifact_role: FilesystemArtifactSourceRole
    expected_sha256: bytes | None = None
    _checkpoint: Callable[[], None] = field(
        default=_noop_checkpoint,
        repr=False,
        compare=False,
        kw_only=True,
    )

    def content_parts(self) -> Iterator[bytes]:
        """Yield exact file bytes after a no-follow open and stat check."""

        self._checkpoint()
        directory_descriptor = os.open(
            self.folder,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        descriptor: int | None = None
        try:
            descriptor = os.open(
                self.name_bytes,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=directory_descriptor,
            )
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode):
                raise FilesystemObservationError(
                    f"source entry is no longer regular: {self.path}"
                )
            self._require_stat(opened, stage="before read")
            digest = sha256()
            while True:
                self._checkpoint()
                part = os.read(descriptor, _READ_BYTES)
                if not part:
                    break
                digest.update(part)
                yield part
            self._checkpoint()
            self._require_stat(os.fstat(descriptor), stage="after read")
            if self.expected_sha256 is not None and digest.digest() != (
                self.expected_sha256
            ):
                raise FilesystemObservationError(
                    f"source metadata bytes changed after parsing: {self.path}"
                )
        except OSError as error:
            raise FilesystemObservationError(
                f"unable to read source file {self.path}: {error}"
            ) from error
        finally:
            if descriptor is not None:
                os.close(descriptor)
            os.close(directory_descriptor)

    @property
    def path(self) -> Path:
        return self.folder / os.fsdecode(self.name_bytes)

    def _require_stat(self, value: os.stat_result, *, stage: str) -> None:
        if FilesystemStat.from_os_stat(value) != self.stat:
            raise FilesystemObservationError(
                f"source file changed {stage}: {self.path}"
            )


@dataclass(frozen=True, slots=True)
class FilesystemDirectoryObservation:
    name_bytes: bytes
    stat: FilesystemStat
    file_type: FilesystemEntryType


@dataclass(frozen=True, slots=True)
class FilesystemGalleryMetadata:
    gid: int
    title: str
    comment: str
    upload_account: str
    upload_time: int
    download_time: int
    modified_time: int
    scan_observation_version: int
    source_file_count: int
    page_count: int


@dataclass(frozen=True, slots=True)
class FilesystemGalleryObservation:
    metadata: FilesystemGalleryMetadata


@dataclass(frozen=True, slots=True)
class _FilesystemGalleryIndex:
    folder: Path
    observation: FilesystemGalleryObservation
    directory_stat: FilesystemStat
    entry_audit_sha256: bytes
    metadata_sha256: bytes
    metadata_stat: FilesystemStat
    entry_count: int


@dataclass(frozen=True, slots=True)
class FilesystemPage[PageItemT]:
    items: tuple[PageItemT, ...]
    terminal: bool

    def __post_init__(self) -> None:
        if type(self.items) is not tuple:
            raise TypeError("filesystem page items must be an exact tuple")
        if type(self.terminal) is not bool:
            raise TypeError("filesystem page terminal must be bool")


class FilesystemSource:
    """Discover nested galleries and reproduce exact direct-child snapshots."""

    def __init__(
        self,
        root: Path,
        *,
        checkpoint: Callable[[], None] | None = None,
    ) -> None:
        if checkpoint is not None and not callable(checkpoint):
            raise TypeError("checkpoint must be callable or None")
        self._checkpoint = checkpoint if checkpoint is not None else _noop_checkpoint
        self._checkpoint()
        try:
            resolved = root.resolve(strict=True)
        except OSError as error:
            raise FilesystemObservationError(
                f"unable to resolve source root {root}: {error}"
            ) from error
        if not resolved.is_dir():
            raise FilesystemObservationError(
                f"source root is not a directory: {resolved}"
            )
        if not resolved.is_absolute():  # pragma: no cover - Path.resolve is absolute
            raise FilesystemObservationError("source root must be absolute")
        self._root = resolved
        self.source_root_components = tuple(resolved.parts[1:])
        self._discovery_temporary: tempfile.TemporaryDirectory[str] | None = None
        self._discovery_connection: sqlite3.Connection | None = None
        self._discovery_root_stat: FilesystemStat | None = None
        self._closed = False

    @property
    def root(self) -> Path:
        return self._root

    def __enter__(self) -> FilesystemSource:
        if self._closed:
            raise RuntimeError("filesystem source is closed")
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        """Release the session-scoped spill index and make this source terminal."""

        if self._closed:
            return
        self._closed = True
        if self._discovery_connection is not None:
            self._discovery_connection.close()
            self._discovery_connection = None
        if self._discovery_temporary is not None:
            self._discovery_temporary.cleanup()
            self._discovery_temporary = None
        self._discovery_root_stat = None

    def list_gallery_locators(
        self,
        *,
        after_locator: tuple[str, ...] | None,
        limit: int,
    ) -> FilesystemPage[tuple[str, ...]]:
        """Return one keyset page from this session's disk-backed snapshot."""

        self._checkpoint()
        bound = _page_limit(limit)
        after = None if after_locator is None else _encode_locator(after_locator)
        connection = self._discovery_index()
        expected_root = self._discovery_root_stat
        if expected_root is None:  # pragma: no cover - established with the index
            raise RuntimeError("filesystem discovery index lacks its root audit")
        if self._directory_stat(self._root) != expected_root:
            raise FilesystemObservationError(
                f"source root changed after discovery snapshot: {self._root}"
            )
        columns = "payload, device, inode, size_bytes, modified_ns, changed_ns"
        if after is None:
            rows = connection.execute(
                f"SELECT {columns} FROM locators ORDER BY payload LIMIT ?",
                (bound + 1,),
            ).fetchall()
        else:
            rows = connection.execute(
                f"SELECT {columns} FROM locators WHERE payload > ? "
                "ORDER BY payload LIMIT ?",
                (after, bound + 1),
            ).fetchall()
        items: list[tuple[str, ...]] = []
        for row in rows[:bound]:
            self._checkpoint()
            locator = _decode_locator(bytes(row[0]))
            current = self._directory_stat(self._gallery_path(locator))
            if current != _stat_from_row(row[1:]):
                raise FilesystemObservationError(
                    f"gallery locator changed after discovery snapshot: {locator!r}"
                )
            items.append(locator)
        return FilesystemPage(tuple(items), len(rows) <= bound)

    def observe_gallery(
        self,
        locator_components: tuple[str, ...],
    ) -> FilesystemGalleryObservation:
        self._checkpoint()
        index, created = self._gallery_index(locator_components)
        if not created:
            self._require_gallery_unchanged(index)
        return index.observation

    def list_files(
        self,
        locator_components: tuple[str, ...],
        *,
        after_name: bytes | None,
        limit: int,
    ) -> tuple[
        FilesystemGalleryObservation,
        FilesystemPage[FilesystemFileObservation],
    ]:
        self._checkpoint()
        index, _created = self._gallery_index(locator_components)
        bound = _page_limit(limit)
        after = _after_name(after_name)
        columns = "name_bytes, device, inode, size_bytes, modified_ns, changed_ns"
        connection = self._discovery_index()
        if after is None:
            rows = connection.execute(
                f"SELECT {columns} FROM gallery_entries "
                "WHERE file_type = ? "
                "ORDER BY name_bytes LIMIT ?",
                (
                    int(FilesystemEntryType.REGULAR),
                    bound + 1,
                ),
            ).fetchall()
        else:
            rows = connection.execute(
                f"SELECT {columns} FROM gallery_entries "
                "WHERE file_type = ? AND name_bytes > ? "
                "ORDER BY name_bytes LIMIT ?",
                (
                    int(FilesystemEntryType.REGULAR),
                    after,
                    bound + 1,
                ),
            ).fetchall()
        items = tuple(
            FilesystemFileObservation(
                folder=index.folder,
                name_bytes=bytes(row[0]),
                stat=_stat_from_row(row[1:]),
                artifact_role=_artifact_source_role(bytes(row[0])),
                expected_sha256=(
                    index.metadata_sha256
                    if bytes(row[0]) == GALLERY_INFO_NAME.encode("ascii")
                    else None
                ),
                _checkpoint=self._checkpoint,
            )
            for row in rows[:bound]
        )
        self._require_gallery_unchanged(index)
        return index.observation, FilesystemPage(items, len(rows) <= bound)

    def list_directories(
        self,
        locator_components: tuple[str, ...],
        *,
        after_name: bytes | None,
        limit: int,
    ) -> tuple[
        FilesystemGalleryObservation,
        FilesystemPage[FilesystemDirectoryObservation],
    ]:
        self._checkpoint()
        index, _created = self._gallery_index(locator_components)
        bound = _page_limit(limit)
        after = _after_name(after_name)
        columns = (
            "name_bytes, device, inode, size_bytes, modified_ns, changed_ns, file_type"
        )
        connection = self._discovery_index()
        if after is None:
            rows = connection.execute(
                f"SELECT {columns} FROM gallery_entries ORDER BY name_bytes LIMIT ?",
                (bound + 1,),
            ).fetchall()
        else:
            rows = connection.execute(
                f"SELECT {columns} FROM gallery_entries "
                "WHERE name_bytes > ? "
                "ORDER BY name_bytes LIMIT ?",
                (after, bound + 1),
            ).fetchall()
        items = tuple(
            FilesystemDirectoryObservation(
                name_bytes=bytes(row[0]),
                stat=_stat_from_row(row[1:6]),
                file_type=FilesystemEntryType(int(row[6])),
            )
            for row in rows[:bound]
        )
        self._require_gallery_unchanged(index)
        return index.observation, FilesystemPage(items, len(rows) <= bound)

    def list_tags(
        self,
        locator_components: tuple[str, ...],
        *,
        after_position: int,
        limit: int,
    ) -> tuple[FilesystemGalleryObservation, FilesystemPage[tuple[str, str]]]:
        self._checkpoint()
        index, _created = self._gallery_index(locator_components)
        bound = _page_limit(limit)
        if isinstance(after_position, bool) or not isinstance(after_position, int):
            raise TypeError("after_position must be int")
        if after_position < 0:
            raise ValueError("after_position must be nonnegative")
        rows = (
            self._discovery_index()
            .execute(
                "SELECT namespace, value FROM gallery_tags "
                "WHERE ordinal >= ? "
                "ORDER BY ordinal LIMIT ?",
                (after_position, bound + 1),
            )
            .fetchall()
        )
        selected = tuple((_exact_text(row[0]), _exact_text(row[1])) for row in rows)
        self._require_gallery_unchanged(index)
        return index.observation, FilesystemPage(
            selected[:bound], len(selected) <= bound
        )

    def _discovery_index(self) -> sqlite3.Connection:
        self._require_open()
        self._checkpoint()
        if self._discovery_connection is not None:
            return self._discovery_connection
        temporary = tempfile.TemporaryDirectory(prefix="h2hdb-ingest-discovery-")
        connection = sqlite3.connect(Path(temporary.name) / "locators.sqlite3")
        try:
            connection.executescript("""
                PRAGMA foreign_keys = ON;
                PRAGMA temp_store = FILE;
                CREATE TABLE locators (
                    payload BLOB PRIMARY KEY,
                    device BLOB NOT NULL CHECK (length(device) = 8),
                    inode BLOB NOT NULL CHECK (length(inode) = 8),
                    size_bytes INTEGER NOT NULL,
                    modified_ns INTEGER NOT NULL,
                    changed_ns INTEGER NOT NULL
                );
                CREATE TABLE gallery_audits (
                    payload BLOB PRIMARY KEY REFERENCES locators(payload),
                    metadata_audit_sha256 BLOB NOT NULL
                        CHECK (length(metadata_audit_sha256) = 32),
                    entry_audit_sha256 BLOB NOT NULL
                        CHECK (length(entry_audit_sha256) = 32),
                    metadata_sha256 BLOB NOT NULL
                        CHECK (length(metadata_sha256) = 32),
                    directory_device BLOB NOT NULL
                        CHECK (length(directory_device) = 8),
                    directory_inode BLOB NOT NULL
                        CHECK (length(directory_inode) = 8),
                    directory_size_bytes INTEGER NOT NULL,
                    directory_modified_ns INTEGER NOT NULL,
                    directory_changed_ns INTEGER NOT NULL,
                    entry_count INTEGER NOT NULL CHECK (entry_count >= 1)
                );
                CREATE TABLE active_gallery_snapshot (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    payload BLOB NOT NULL UNIQUE REFERENCES locators(payload),
                    metadata_audit_sha256 BLOB NOT NULL
                        CHECK (length(metadata_audit_sha256) = 32),
                    entry_audit_sha256 BLOB NOT NULL
                        CHECK (length(entry_audit_sha256) = 32),
                    metadata_sha256 BLOB NOT NULL
                        CHECK (length(metadata_sha256) = 32),
                    directory_device BLOB NOT NULL
                        CHECK (length(directory_device) = 8),
                    directory_inode BLOB NOT NULL
                        CHECK (length(directory_inode) = 8),
                    directory_size_bytes INTEGER NOT NULL,
                    directory_modified_ns INTEGER NOT NULL,
                    directory_changed_ns INTEGER NOT NULL,
                    entry_count INTEGER NOT NULL CHECK (entry_count >= 1),
                    gid INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    comment TEXT NOT NULL,
                    upload_account TEXT NOT NULL,
                    upload_time INTEGER NOT NULL,
                    download_time INTEGER NOT NULL,
                    modified_time INTEGER NOT NULL,
                    scan_observation_version INTEGER NOT NULL,
                    source_file_count INTEGER NOT NULL
                        CHECK (source_file_count >= 1),
                    page_count INTEGER NOT NULL CHECK (page_count >= 0)
                );
                CREATE TABLE gallery_entries (
                    name_bytes BLOB PRIMARY KEY,
                    device BLOB NOT NULL CHECK (length(device) = 8),
                    inode BLOB NOT NULL CHECK (length(inode) = 8),
                    size_bytes INTEGER NOT NULL,
                    modified_ns INTEGER NOT NULL,
                    changed_ns INTEGER NOT NULL,
                    file_type INTEGER NOT NULL CHECK (file_type BETWEEN 0 AND 3)
                );
                CREATE TABLE gallery_tags (
                    ordinal INTEGER PRIMARY KEY CHECK (ordinal >= 0),
                    namespace TEXT NOT NULL,
                    value TEXT NOT NULL
                );
                CREATE TABLE gallery_audit_entries (
                    name_bytes BLOB PRIMARY KEY,
                    device BLOB NOT NULL CHECK (length(device) = 8),
                    inode BLOB NOT NULL CHECK (length(inode) = 8),
                    size_bytes INTEGER NOT NULL,
                    modified_ns INTEGER NOT NULL,
                    changed_ns INTEGER NOT NULL,
                    file_type INTEGER NOT NULL CHECK (file_type BETWEEN 0 AND 3)
                );
                """)
            expected_root = self._directory_stat(self._root)
            for locator, observed in self._discover_directory(self._root):
                self._checkpoint()
                try:
                    connection.execute(
                        "INSERT INTO locators VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            _encode_locator(locator),
                            observed.device.to_bytes(8, "big"),
                            observed.inode.to_bytes(8, "big"),
                            observed.size_bytes,
                            observed.modified_ns,
                            observed.changed_ns,
                        ),
                    )
                except sqlite3.IntegrityError as error:
                    raise FilesystemObservationError(
                        f"duplicate gallery locator: {locator!r}"
                    ) from error
            if self._directory_stat(self._root) != expected_root:
                raise FilesystemObservationError(
                    f"source root changed during discovery snapshot: {self._root}"
                )
            connection.commit()
            self._checkpoint()
        except BaseException:
            connection.close()
            temporary.cleanup()
            raise
        self._discovery_temporary = temporary
        self._discovery_connection = connection
        self._discovery_root_stat = expected_root
        return connection

    def _gallery_index(
        self,
        locator_components: tuple[str, ...],
    ) -> tuple[_FilesystemGalleryIndex, bool]:
        self._checkpoint()
        connection = self._discovery_index()
        payload = _encode_locator(locator_components)
        folder = self._gallery_path(locator_components)
        directory_stat = self._directory_stat(folder)
        discovered = connection.execute(
            "SELECT device, inode, size_bytes, modified_ns, changed_ns "
            "FROM locators WHERE payload = ?",
            (payload,),
        ).fetchone()
        if discovered is None:
            raise FilesystemObservationError(
                f"gallery is outside the discovery snapshot: {locator_components!r}"
            )
        if _stat_from_row(discovered) != directory_stat:
            raise FilesystemObservationError(
                f"gallery changed after discovery snapshot: {locator_components!r}"
            )
        persisted = connection.execute(
            "SELECT metadata_audit_sha256, entry_audit_sha256, metadata_sha256, "
            "directory_device, directory_inode, directory_size_bytes, "
            "directory_modified_ns, directory_changed_ns, entry_count, gid, title, "
            "comment, upload_account, upload_time, download_time, modified_time, "
            "scan_observation_version, source_file_count, page_count "
            "FROM active_gallery_snapshot WHERE payload = ?",
            (payload,),
        ).fetchone()
        if persisted is None:
            return (
                self._build_gallery_index(
                    connection,
                    payload=payload,
                    folder=folder,
                    directory_stat=directory_stat,
                ),
                True,
            )
        return (
            self._gallery_index_from_row(
                connection,
                folder=folder,
                row=persisted,
            ),
            False,
        )

    def _build_gallery_index(
        self,
        connection: sqlite3.Connection,
        *,
        payload: bytes,
        folder: Path,
        directory_stat: FilesystemStat,
    ) -> _FilesystemGalleryIndex:
        self._checkpoint()
        metadata_path = folder / GALLERY_INFO_NAME
        metadata_stat = self._regular_file_stat(metadata_path)
        metadata_sha256 = self._hash_path(metadata_path, metadata_stat)
        self._checkpoint()
        try:
            parsed = parse_galleryinfo(folder)
        except Exception as error:
            raise FilesystemObservationError(
                f"unable to parse {metadata_path}: {error}"
            ) from error
        self._checkpoint()
        if self._regular_file_stat(metadata_path) != metadata_stat:
            raise FilesystemObservationError(
                f"gallery metadata changed while parsing: {metadata_path}"
            )
        if self._hash_path(metadata_path, metadata_stat) != metadata_sha256:
            raise FilesystemObservationError(
                f"gallery metadata bytes changed while parsing: {metadata_path}"
            )
        tags: list[tuple[str, str]] = []
        for namespace, tag_text in parsed.tags:
            self._checkpoint()
            tags.append((str(namespace), str(tag_text)))
        try:
            with connection:
                self._checkpoint()
                connection.execute("DELETE FROM gallery_tags")
                connection.execute("DELETE FROM gallery_entries")
                connection.execute("DELETE FROM active_gallery_snapshot")
                before = self._directory_stat(folder)
                if before != directory_stat:
                    raise FilesystemObservationError(
                        f"gallery directory changed before observation: {folder}"
                    )
                try:
                    entry_batch: list[tuple[object, ...]] = []
                    with os.scandir(folder) as entries:
                        for entry in entries:
                            self._checkpoint()
                            name = os.fsencode(entry.name)
                            _strict_name_bytes(name)
                            value = entry.stat(follow_symlinks=False)
                            observed = FilesystemStat.from_os_stat(value)
                            entry_batch.append(
                                (
                                    name,
                                    observed.device.to_bytes(8, "big"),
                                    observed.inode.to_bytes(8, "big"),
                                    observed.size_bytes,
                                    observed.modified_ns,
                                    observed.changed_ns,
                                    int(_entry_type(value.st_mode)),
                                )
                            )
                            if len(entry_batch) == 128:
                                _insert_gallery_entry_batch(connection, entry_batch)
                                entry_batch.clear()
                                self._checkpoint()
                    if entry_batch:
                        _insert_gallery_entry_batch(connection, entry_batch)
                        self._checkpoint()
                except (OSError, sqlite3.DatabaseError) as error:
                    raise FilesystemObservationError(
                        f"unable to snapshot gallery directory {folder}: {error}"
                    ) from error
                if self._directory_stat(folder) != directory_stat:
                    raise FilesystemObservationError(
                        f"gallery directory changed during observation: {folder}"
                    )
                digest = sha256(_ENTRY_AUDIT_PREFIX)
                entry_count = 0
                regular_count = 0
                page_count = 0
                rows = connection.execute(
                    "SELECT name_bytes, device, inode, size_bytes, modified_ns, "
                    "changed_ns, file_type FROM gallery_entries ORDER BY name_bytes"
                )
                while True:
                    self._checkpoint()
                    page = rows.fetchmany(128)
                    if not page:
                        break
                    for row in page:
                        name = bytes(row[0])
                        digest.update(len(name).to_bytes(4, "big"))
                        digest.update(name)
                        digest.update(bytes(row[1]))
                        digest.update(bytes(row[2]))
                        digest.update(int(row[3]).to_bytes(8, "big"))
                        digest.update(int(row[4]).to_bytes(8, "big", signed=True))
                        digest.update(int(row[5]).to_bytes(8, "big", signed=True))
                        file_type = int(row[6])
                        digest.update(file_type.to_bytes(1, "big"))
                        entry_count += 1
                        regular_count += int(file_type == FilesystemEntryType.REGULAR)
                        page_count += int(
                            file_type == FilesystemEntryType.REGULAR
                            and _artifact_source_role(name)
                            is FilesystemArtifactSourceRole.PAGE
                        )
                if regular_count < 1:
                    raise FilesystemObservationError(
                        f"gallery contains no regular metadata file: {folder}"
                    )
                metadata_row = connection.execute(
                    "SELECT device, inode, size_bytes, modified_ns, changed_ns, "
                    "file_type FROM gallery_entries WHERE name_bytes = ?",
                    (GALLERY_INFO_NAME.encode("ascii"),),
                ).fetchone()
                if (
                    metadata_row is None
                    or int(metadata_row[5]) != FilesystemEntryType.REGULAR
                    or _stat_from_row(metadata_row[:5]) != metadata_stat
                ):
                    raise FilesystemObservationError(
                        f"gallery metadata changed while indexing: {metadata_path}"
                    )
                metadata = FilesystemGalleryMetadata(
                    gid=parsed.gid,
                    title=parsed.title,
                    comment=parsed.galleries_comments,
                    upload_account=parsed.upload_account,
                    upload_time=_wall_time_microseconds(parsed.upload_time),
                    download_time=_wall_time_microseconds(parsed.download_time),
                    modified_time=metadata_stat.modified_ns // 1_000,
                    scan_observation_version=FILESYSTEM_OBSERVATION_VERSION,
                    source_file_count=regular_count,
                    page_count=page_count,
                )
                for ordinal, (namespace, tag_value) in enumerate(tags):
                    self._checkpoint()
                    connection.execute(
                        "INSERT INTO gallery_tags VALUES (?, ?, ?)",
                        (ordinal, namespace, tag_value),
                    )
                current_audit = (
                    _gallery_metadata_audit(metadata),
                    digest.digest(),
                    metadata_sha256,
                    directory_stat.device.to_bytes(8, "big"),
                    directory_stat.inode.to_bytes(8, "big"),
                    directory_stat.size_bytes,
                    directory_stat.modified_ns,
                    directory_stat.changed_ns,
                    entry_count,
                )
                persisted_audit = connection.execute(
                    "SELECT metadata_audit_sha256, entry_audit_sha256, "
                    "metadata_sha256, directory_device, directory_inode, "
                    "directory_size_bytes, directory_modified_ns, "
                    "directory_changed_ns, entry_count FROM gallery_audits "
                    "WHERE payload = ?",
                    (payload,),
                ).fetchone()
                if persisted_audit is None:
                    connection.execute(
                        "INSERT INTO gallery_audits VALUES "
                        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (payload, *current_audit),
                    )
                elif _normalize_gallery_audit(persisted_audit) != current_audit:
                    raise _gallery_changed(folder)
                connection.execute(
                    "INSERT INTO active_gallery_snapshot VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        1,
                        payload,
                        _gallery_metadata_audit(metadata),
                        digest.digest(),
                        metadata_sha256,
                        directory_stat.device.to_bytes(8, "big"),
                        directory_stat.inode.to_bytes(8, "big"),
                        directory_stat.size_bytes,
                        directory_stat.modified_ns,
                        directory_stat.changed_ns,
                        entry_count,
                        metadata.gid,
                        metadata.title,
                        metadata.comment,
                        metadata.upload_account,
                        metadata.upload_time,
                        metadata.download_time,
                        metadata.modified_time,
                        metadata.scan_observation_version,
                        metadata.source_file_count,
                        metadata.page_count,
                    ),
                )
                self._checkpoint()
        except sqlite3.DatabaseError as error:
            raise FilesystemObservationError(
                f"unable to index gallery directory {folder}: {error}"
            ) from error
        persisted = connection.execute(
            "SELECT metadata_audit_sha256, entry_audit_sha256, metadata_sha256, "
            "directory_device, directory_inode, directory_size_bytes, "
            "directory_modified_ns, directory_changed_ns, entry_count, gid, title, "
            "comment, upload_account, upload_time, download_time, modified_time, "
            "scan_observation_version, source_file_count, page_count "
            "FROM active_gallery_snapshot WHERE payload = ?",
            (payload,),
        ).fetchone()
        if persisted is None:  # pragma: no cover - inserted in the transaction above
            raise RuntimeError("filesystem gallery index lacks its snapshot")
        return self._gallery_index_from_row(
            connection,
            folder=folder,
            row=persisted,
        )

    @staticmethod
    def _gallery_index_from_row(
        connection: sqlite3.Connection,
        *,
        folder: Path,
        row: tuple[object, ...],
    ) -> _FilesystemGalleryIndex:
        if len(row) != 19:
            raise RuntimeError("filesystem gallery snapshot has an invalid shape")
        metadata = FilesystemGalleryMetadata(
            gid=_exact_int(row[9]),
            title=_exact_text(row[10]),
            comment=_exact_text(row[11]),
            upload_account=_exact_text(row[12]),
            upload_time=_exact_int(row[13]),
            download_time=_exact_int(row[14]),
            modified_time=_exact_int(row[15]),
            scan_observation_version=_exact_int(row[16]),
            source_file_count=_exact_int(row[17]),
            page_count=_exact_int(row[18]),
        )
        metadata_audit = _exact_digest(row[0], label="metadata audit")
        if _gallery_metadata_audit(metadata) != metadata_audit:
            raise RuntimeError("filesystem gallery metadata index is corrupt")
        metadata_row = connection.execute(
            "SELECT device, inode, size_bytes, modified_ns, changed_ns, file_type "
            "FROM gallery_entries WHERE name_bytes = ?",
            (GALLERY_INFO_NAME.encode("ascii"),),
        ).fetchone()
        if metadata_row is None or int(metadata_row[5]) != FilesystemEntryType.REGULAR:
            raise RuntimeError("filesystem gallery metadata entry is missing")
        entry_count = _exact_int(row[8])
        if entry_count < 1:
            raise RuntimeError("filesystem gallery entry count is invalid")
        return _FilesystemGalleryIndex(
            folder=folder,
            observation=FilesystemGalleryObservation(metadata),
            directory_stat=_stat_from_row(row[3:8]),
            entry_audit_sha256=_exact_digest(row[1], label="entry audit"),
            metadata_sha256=_exact_digest(row[2], label="metadata digest"),
            metadata_stat=_stat_from_row(metadata_row[:5]),
            entry_count=entry_count,
        )

    def _require_gallery_unchanged(self, index: _FilesystemGalleryIndex) -> None:
        self._checkpoint()
        connection = self._discovery_index()
        before = self._directory_stat(index.folder)
        if before != index.directory_stat:
            raise _gallery_changed(index.folder)
        entry_count = 0
        batch: list[tuple[object, ...]] = []
        try:
            with connection:
                connection.execute("DELETE FROM gallery_audit_entries")
                with os.scandir(index.folder) as entries:
                    for entry in entries:
                        self._checkpoint()
                        name = os.fsencode(entry.name)
                        _strict_name_bytes(name)
                        value = entry.stat(follow_symlinks=False)
                        observed = FilesystemStat.from_os_stat(value)
                        batch.append(
                            (
                                name,
                                observed.device.to_bytes(8, "big"),
                                observed.inode.to_bytes(8, "big"),
                                observed.size_bytes,
                                observed.modified_ns,
                                observed.changed_ns,
                                int(_entry_type(value.st_mode)),
                            )
                        )
                        entry_count += 1
                        if len(batch) == 128:
                            _insert_audit_batch(connection, batch)
                            batch.clear()
                            self._checkpoint()
                if batch:
                    _insert_audit_batch(connection, batch)
                    batch.clear()
                    self._checkpoint()
                current_audit = _entry_audit(
                    connection,
                    checkpoint=self._checkpoint,
                )
                connection.execute("DELETE FROM gallery_audit_entries")
                self._checkpoint()
        except FilesystemObservationError:
            raise
        except (OSError, sqlite3.DatabaseError) as error:
            raise FilesystemObservationError(
                f"unable to revalidate gallery directory {index.folder}: {error}"
            ) from error
        if (
            entry_count != index.entry_count
            or self._directory_stat(index.folder) != index.directory_stat
            or current_audit != index.entry_audit_sha256
        ):
            raise _gallery_changed(index.folder)
        try:
            metadata_digest = self._hash_path(
                index.folder / GALLERY_INFO_NAME,
                index.metadata_stat,
            )
        except FilesystemObservationError as error:
            raise _gallery_changed(index.folder) from error
        if metadata_digest != index.metadata_sha256:
            raise _gallery_changed(index.folder)

    def _require_open(self) -> None:
        if self._closed:
            raise RuntimeError("filesystem source is closed")

    def _discover_directory(
        self,
        directory: Path,
    ) -> Iterator[tuple[tuple[str, ...], FilesystemStat]]:
        self._checkpoint()
        expected_directory = self._directory_stat(directory)
        has_metadata = False
        children: list[Path] = []
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    self._checkpoint()
                    if entry.name == GALLERY_INFO_NAME:
                        value = entry.stat(follow_symlinks=False)
                        if stat.S_ISREG(value.st_mode):
                            has_metadata = True
                        elif stat.S_ISLNK(value.st_mode):
                            raise FilesystemObservationError(
                                f"gallery metadata must not be a symlink: {entry.path}"
                            )
                    if entry.is_dir(follow_symlinks=False):
                        children.append(Path(entry.path))
        except FilesystemObservationError:
            raise
        except OSError as error:
            raise FilesystemObservationError(
                f"unable to discover galleries below {directory}: {error}"
            ) from error
        relative = directory.relative_to(self._root)
        if has_metadata:
            if not relative.parts:
                raise FilesystemObservationError(
                    "source root itself cannot be a gallery locator"
                )
            yield (
                tuple(_strict_component(part) for part in relative.parts),
                expected_directory,
            )
        for child in children:
            self._checkpoint()
            yield from self._discover_directory(child)
        self._checkpoint()
        if self._directory_stat(directory) != expected_directory:
            raise FilesystemObservationError(
                f"directory changed during gallery discovery: {directory}"
            )

    def _gallery_path(self, locator_components: tuple[str, ...]) -> Path:
        if type(locator_components) is not tuple or not locator_components:
            raise FilesystemObservationError(
                "gallery locator must be a nonempty exact tuple"
            )
        current = self._root
        for component in locator_components:
            self._checkpoint()
            current /= _strict_component(component)
            try:
                value = current.lstat()
            except OSError as error:
                raise FilesystemObservationError(
                    f"gallery locator is unavailable: {current}: {error}"
                ) from error
            if not stat.S_ISDIR(value.st_mode):
                raise FilesystemObservationError(
                    f"gallery locator crosses a non-directory: {current}"
                )
        return current

    @staticmethod
    def _directory_stat(path: Path) -> FilesystemStat:
        try:
            value = path.lstat()
        except OSError as error:
            raise FilesystemObservationError(
                f"unable to inspect gallery directory {path}: {error}"
            ) from error
        if not stat.S_ISDIR(value.st_mode):
            raise FilesystemObservationError(f"gallery path is not a directory: {path}")
        return FilesystemStat.from_os_stat(value)

    @staticmethod
    def _regular_file_stat(path: Path) -> FilesystemStat:
        try:
            value = path.lstat()
        except OSError as error:
            raise FilesystemObservationError(
                f"unable to inspect gallery metadata {path}: {error}"
            ) from error
        if not stat.S_ISREG(value.st_mode):
            raise FilesystemObservationError(
                f"gallery metadata is not a regular file: {path}"
            )
        return FilesystemStat.from_os_stat(value)

    def _hash_path(self, path: Path, expected: FilesystemStat) -> bytes:
        self._checkpoint()
        digest = sha256()
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError as error:
            raise FilesystemObservationError(
                f"unable to open gallery metadata {path}: {error}"
            ) from error
        try:
            if FilesystemStat.from_os_stat(os.fstat(descriptor)) != expected:
                raise FilesystemObservationError(
                    f"gallery metadata changed before hashing: {path}"
                )
            while True:
                self._checkpoint()
                part = os.read(descriptor, _READ_BYTES)
                if not part:
                    break
                digest.update(part)
            self._checkpoint()
            if FilesystemStat.from_os_stat(os.fstat(descriptor)) != expected:
                raise FilesystemObservationError(
                    f"gallery metadata changed after hashing: {path}"
                )
        finally:
            os.close(descriptor)
        return digest.digest()


def _insert_audit_batch(
    connection: sqlite3.Connection,
    batch: list[tuple[object, ...]],
) -> None:
    connection.executemany(
        "INSERT INTO gallery_audit_entries VALUES (?, ?, ?, ?, ?, ?, ?)",
        batch,
    )


def _insert_gallery_entry_batch(
    connection: sqlite3.Connection,
    batch: list[tuple[object, ...]],
) -> None:
    connection.executemany(
        "INSERT INTO gallery_entries VALUES (?, ?, ?, ?, ?, ?, ?)",
        batch,
    )


def _entry_audit(
    connection: sqlite3.Connection,
    *,
    checkpoint: Callable[[], None] = _noop_checkpoint,
) -> bytes:
    digest = sha256(_ENTRY_AUDIT_PREFIX)
    rows = connection.execute(
        "SELECT name_bytes, device, inode, size_bytes, modified_ns, changed_ns, "
        "file_type FROM gallery_audit_entries ORDER BY name_bytes"
    )
    while True:
        checkpoint()
        page = rows.fetchmany(128)
        if not page:
            break
        for row in page:
            name = bytes(row[0])
            digest.update(len(name).to_bytes(4, "big"))
            digest.update(name)
            digest.update(bytes(row[1]))
            digest.update(bytes(row[2]))
            digest.update(int(row[3]).to_bytes(8, "big"))
            digest.update(int(row[4]).to_bytes(8, "big", signed=True))
            digest.update(int(row[5]).to_bytes(8, "big", signed=True))
            digest.update(int(row[6]).to_bytes(1, "big"))
    return digest.digest()


def _normalize_gallery_audit(row: tuple[object, ...]) -> tuple[object, ...]:
    if len(row) != 9:
        raise RuntimeError("filesystem gallery audit has an invalid shape")
    return (
        _exact_digest(row[0], label="metadata audit"),
        _exact_digest(row[1], label="entry audit"),
        _exact_digest(row[2], label="metadata digest"),
        _exact_u64_bytes(row[3], label="directory device"),
        _exact_u64_bytes(row[4], label="directory inode"),
        _exact_int(row[5]),
        _exact_int(row[6]),
        _exact_int(row[7]),
        _exact_int(row[8]),
    )


def _exact_digest(value: object, *, label: str) -> bytes:
    if type(value) is not bytes or len(value) != 32:
        raise RuntimeError(f"filesystem {label} is invalid")
    return value


def _exact_u64_bytes(value: object, *, label: str) -> bytes:
    if type(value) is not bytes or len(value) != 8:
        raise RuntimeError(f"filesystem {label} is invalid")
    return value


def _exact_int(value: object) -> int:
    if type(value) is not int:
        raise RuntimeError("filesystem index integer is invalid")
    return value


def _exact_text(value: object) -> str:
    if type(value) is not str:
        raise RuntimeError("filesystem index text is invalid")
    return value


def _gallery_changed(folder: Path) -> FilesystemObservationError:
    return FilesystemObservationError(
        f"gallery changed between bounded pages: {folder}"
    )


def _encode_locator(components: tuple[str, ...]) -> bytes:
    if type(components) is not tuple or not components:
        raise FilesystemObservationError(
            "gallery locator must be a nonempty exact tuple"
        )
    encoded = bytearray((0, 0, 0, 1))
    encoded.extend(len(components).to_bytes(4, "big"))
    for component in components:
        value = _strict_component(component).encode("utf-8", errors="strict")
        encoded.extend(len(value).to_bytes(4, "big"))
        encoded.extend(value)
    return bytes(encoded)


def _decode_locator(payload: bytes) -> tuple[str, ...]:
    if len(payload) < 8 or payload[:4] != b"\x00\x00\x00\x01":
        raise RuntimeError("filesystem locator index is corrupt")
    count = int.from_bytes(payload[4:8], "big")
    offset = 8
    components: list[str] = []
    for _index in range(count):
        if len(payload) - offset < 4:
            raise RuntimeError("filesystem locator index is truncated")
        size = int.from_bytes(payload[offset : offset + 4], "big")
        offset += 4
        value = payload[offset : offset + size]
        offset += size
        try:
            component = value.decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:  # pragma: no cover - locally encoded
            raise RuntimeError("filesystem locator index is not UTF-8") from error
        components.append(_strict_component(component))
    if offset != len(payload) or not components:
        raise RuntimeError("filesystem locator index has trailing bytes")
    return tuple(components)


def _page_limit(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("page limit must be int")
    if not 1 <= value <= 256:
        raise ValueError("page limit must be in 1..256")
    return value


def _after_name(value: bytes | None) -> bytes | None:
    if value is None:
        return None
    result = bytes(value)
    _strict_name_bytes(result)
    return result


def _stat_from_row(row: tuple[object, ...]) -> FilesystemStat:
    if len(row) != 5:
        raise RuntimeError("filesystem stat row has an invalid shape")
    device, inode, size_bytes, modified_ns, changed_ns = row
    if type(device) is not bytes or type(inode) is not bytes:
        raise RuntimeError("filesystem stat row has invalid byte fields")
    if (
        type(size_bytes) is not int
        or type(modified_ns) is not int
        or type(changed_ns) is not int
    ):
        raise RuntimeError("filesystem stat row has invalid integer fields")
    return FilesystemStat(
        int.from_bytes(device, "big"),
        int.from_bytes(inode, "big"),
        size_bytes,
        modified_ns,
        changed_ns,
    )


def _gallery_metadata_audit(value: FilesystemGalleryMetadata) -> bytes:
    digest = sha256(_GALLERY_AUDIT_PREFIX)
    for numeric_value in (
        value.gid,
        value.upload_time,
        value.download_time,
        value.modified_time,
        value.scan_observation_version,
        value.source_file_count,
        value.page_count,
    ):
        digest.update(numeric_value.to_bytes(16, "big", signed=True))
    for text_value in (value.title, value.comment, value.upload_account):
        encoded = text_value.encode("utf-8", errors="strict")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.digest()


def _entry_type(mode: int) -> FilesystemEntryType:
    if stat.S_ISREG(mode):
        return FilesystemEntryType.REGULAR
    if stat.S_ISDIR(mode):
        return FilesystemEntryType.DIRECTORY
    if stat.S_ISLNK(mode):
        return FilesystemEntryType.SYMLINK
    return FilesystemEntryType.OTHER


def _artifact_source_role(name_bytes: bytes) -> FilesystemArtifactSourceRole:
    if name_bytes == GALLERY_INFO_NAME.encode("ascii"):
        return FilesystemArtifactSourceRole.METADATA
    _stem, separator, suffix = name_bytes.rpartition(b".")
    if separator and b"." + suffix.lower() in _PAGE_SUFFIXES:
        return FilesystemArtifactSourceRole.PAGE
    return FilesystemArtifactSourceRole.OTHER


def _strict_component(value: str) -> str:
    if not isinstance(value, str):
        raise FilesystemObservationError("filesystem path component must be str")
    try:
        encoded = value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as error:
        raise FilesystemObservationError(
            f"filesystem path is not strict UTF-8: {value!r}"
        ) from error
    if not encoded or len(encoded) > 255 or value in {".", ".."} or "/" in value:
        raise FilesystemObservationError(
            f"filesystem path component is outside the vNext domain: {value!r}"
        )
    return value


def _strict_name_bytes(value: bytes) -> None:
    if not value or len(value) > 255 or value in {b".", b".."} or b"/" in value:
        raise FilesystemObservationError(
            f"filesystem entry name is outside the vNext domain: {value!r}"
        )


def _wall_time_microseconds(value: datetime) -> int:
    aware = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    utc = aware.astimezone(UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = utc - epoch
    result = (
        delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds
    )
    if result < 0:
        raise FilesystemObservationError("gallery timestamp predates the Unix epoch")
    return result
