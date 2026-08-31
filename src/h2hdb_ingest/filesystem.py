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
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
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

    def content_parts(self) -> Iterator[bytes]:
        """Yield exact file bytes after a no-follow open and stat check."""

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
            while part := os.read(descriptor, _READ_BYTES):
                digest.update(part)
                yield part
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
class _FilesystemGallerySnapshot:
    observation: FilesystemGalleryObservation
    files: _ReplayableFiles
    directories: _ReplayableDirectories
    tags: tuple[tuple[str, str], ...]


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

    def __init__(self, root: Path) -> None:
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
        return self._observe_gallery_snapshot(locator_components).observation

    def _observe_gallery_snapshot(
        self,
        locator_components: tuple[str, ...],
    ) -> _FilesystemGallerySnapshot:
        self._require_open()
        folder = self._gallery_path(locator_components)
        expected_directory = self._directory_stat(folder)
        metadata_path = folder / GALLERY_INFO_NAME
        metadata_before = self._regular_file_stat(metadata_path)
        metadata_sha256 = self._hash_path(metadata_path, metadata_before)
        try:
            parsed = parse_galleryinfo(folder)
        except Exception as error:
            raise FilesystemObservationError(
                f"unable to parse {metadata_path}: {error}"
            ) from error
        metadata_after = self._regular_file_stat(metadata_path)
        if metadata_after != metadata_before:
            raise FilesystemObservationError(
                f"gallery metadata changed while parsing: {metadata_path}"
            )
        if self._hash_path(metadata_path, metadata_after) != metadata_sha256:
            raise FilesystemObservationError(
                f"gallery metadata bytes changed while parsing: {metadata_path}"
            )
        audit, regular_count, page_count = self._directory_audit(
            folder, expected_directory
        )
        if regular_count < 1:
            raise FilesystemObservationError(
                f"gallery contains no regular metadata file: {folder}"
            )
        tags = tuple((str(namespace), str(value)) for namespace, value in parsed.tags)
        observation = FilesystemGalleryObservation(
            metadata=FilesystemGalleryMetadata(
                gid=parsed.gid,
                title=parsed.title,
                comment=parsed.galleries_comments,
                upload_account=parsed.upload_account,
                upload_time=_wall_time_microseconds(parsed.upload_time),
                download_time=_wall_time_microseconds(parsed.download_time),
                modified_time=metadata_before.modified_ns // 1_000,
                scan_observation_version=FILESYSTEM_OBSERVATION_VERSION,
                source_file_count=regular_count,
                page_count=page_count,
            )
        )
        self._require_or_record_gallery_audit(
            locator_components,
            observation=observation,
            directory_stat=expected_directory,
            entry_audit_sha256=audit,
            metadata_sha256=metadata_sha256,
        )
        return _FilesystemGallerySnapshot(
            observation=observation,
            files=_ReplayableFiles(
                folder,
                expected_directory=expected_directory,
                expected_audit=audit,
                metadata_sha256=metadata_sha256,
            ),
            directories=_ReplayableDirectories(
                folder,
                expected_directory=expected_directory,
                expected_audit=audit,
            ),
            tags=tags,
        )

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
        snapshot = self._observe_gallery_snapshot(locator_components)
        return snapshot.observation, snapshot.files.page(
            after_name=after_name,
            limit=limit,
        )

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
        snapshot = self._observe_gallery_snapshot(locator_components)
        return snapshot.observation, snapshot.directories.page(
            after_name=after_name,
            limit=limit,
        )

    def list_tags(
        self,
        locator_components: tuple[str, ...],
        *,
        after_position: int,
        limit: int,
    ) -> tuple[FilesystemGalleryObservation, FilesystemPage[tuple[str, str]]]:
        snapshot = self._observe_gallery_snapshot(locator_components)
        bound = _page_limit(limit)
        if isinstance(after_position, bool) or not isinstance(after_position, int):
            raise TypeError("after_position must be int")
        if after_position < 0:
            raise ValueError("after_position must be nonnegative")
        tags = snapshot.tags
        selected = tags[after_position : after_position + bound + 1]
        return snapshot.observation, FilesystemPage(
            selected[:bound],
            len(selected) <= bound,
        )

    def _discovery_index(self) -> sqlite3.Connection:
        self._require_open()
        if self._discovery_connection is not None:
            return self._discovery_connection
        temporary = tempfile.TemporaryDirectory(prefix="h2hdb-ingest-discovery-")
        connection = sqlite3.connect(Path(temporary.name) / "locators.sqlite3")
        try:
            connection.executescript("""
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
                    directory_changed_ns INTEGER NOT NULL
                );
                """)
            expected_root = self._directory_stat(self._root)
            for locator, observed in self._discover_directory(self._root):
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
        except BaseException:
            connection.close()
            temporary.cleanup()
            raise
        self._discovery_temporary = temporary
        self._discovery_connection = connection
        self._discovery_root_stat = expected_root
        return connection

    def _require_or_record_gallery_audit(
        self,
        locator_components: tuple[str, ...],
        *,
        observation: FilesystemGalleryObservation,
        directory_stat: FilesystemStat,
        entry_audit_sha256: bytes,
        metadata_sha256: bytes,
    ) -> None:
        connection = self._discovery_index()
        payload = _encode_locator(locator_components)
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
        current = (
            _gallery_metadata_audit(observation.metadata),
            entry_audit_sha256,
            metadata_sha256,
            directory_stat.device.to_bytes(8, "big"),
            directory_stat.inode.to_bytes(8, "big"),
            directory_stat.size_bytes,
            directory_stat.modified_ns,
            directory_stat.changed_ns,
        )
        persisted = connection.execute(
            "SELECT metadata_audit_sha256, entry_audit_sha256, metadata_sha256, "
            "directory_device, directory_inode, directory_size_bytes, "
            "directory_modified_ns, directory_changed_ns "
            "FROM gallery_audits WHERE payload = ?",
            (payload,),
        ).fetchone()
        if persisted is None:
            connection.execute(
                "INSERT INTO gallery_audits VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (payload, *current),
            )
            connection.commit()
            return
        normalized = (
            bytes(persisted[0]),
            bytes(persisted[1]),
            bytes(persisted[2]),
            bytes(persisted[3]),
            bytes(persisted[4]),
            int(persisted[5]),
            int(persisted[6]),
            int(persisted[7]),
        )
        if normalized != current:
            raise FilesystemObservationError(
                f"gallery changed between bounded pages: {locator_components!r}"
            )

    def _require_open(self) -> None:
        if self._closed:
            raise RuntimeError("filesystem source is closed")

    def _discover_directory(
        self,
        directory: Path,
    ) -> Iterator[tuple[tuple[str, ...], FilesystemStat]]:
        expected_directory = self._directory_stat(directory)
        has_metadata = False
        children: list[Path] = []
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
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
            yield from self._discover_directory(child)
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

    @staticmethod
    def _hash_path(path: Path, expected: FilesystemStat) -> bytes:
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
            while part := os.read(descriptor, _READ_BYTES):
                digest.update(part)
            if FilesystemStat.from_os_stat(os.fstat(descriptor)) != expected:
                raise FilesystemObservationError(
                    f"gallery metadata changed after hashing: {path}"
                )
        finally:
            os.close(descriptor)
        return digest.digest()

    @staticmethod
    def _directory_audit(
        folder: Path,
        expected_directory: FilesystemStat,
    ) -> tuple[bytes, int, int]:
        with _entry_index(folder, expected_directory) as index:
            row = index.execute(
                "SELECT audit_sha256, regular_count, page_count FROM snapshot"
            ).fetchone()
            if row is None:
                raise RuntimeError("filesystem entry index lacks its snapshot")
            return bytes(row[0]), int(row[1]), int(row[2])


class _ReplayableFiles:
    def __init__(
        self,
        folder: Path,
        *,
        expected_directory: FilesystemStat,
        expected_audit: bytes,
        metadata_sha256: bytes,
    ) -> None:
        self._folder = folder
        self._expected_directory = expected_directory
        self._expected_audit = expected_audit
        self._metadata_sha256 = metadata_sha256

    def page(
        self,
        *,
        after_name: bytes | None,
        limit: int,
    ) -> FilesystemPage[FilesystemFileObservation]:
        bound = _page_limit(limit)
        after = _after_name(after_name)
        with _entry_index(self._folder, self._expected_directory) as index:
            _require_audit(index, self._expected_audit)
            if after is None:
                rows = index.execute(
                    "SELECT name_bytes, device, inode, size_bytes, modified_ns, "
                    "changed_ns FROM entries WHERE file_type = ? "
                    "ORDER BY name_bytes LIMIT ?",
                    (int(FilesystemEntryType.REGULAR), bound + 1),
                ).fetchall()
            else:
                rows = index.execute(
                    "SELECT name_bytes, device, inode, size_bytes, modified_ns, "
                    "changed_ns FROM entries WHERE file_type = ? AND name_bytes > ? "
                    "ORDER BY name_bytes LIMIT ?",
                    (int(FilesystemEntryType.REGULAR), after, bound + 1),
                ).fetchall()
            items = tuple(
                FilesystemFileObservation(
                    folder=self._folder,
                    name_bytes=bytes(row[0]),
                    stat=_stat_from_row(row[1:]),
                    artifact_role=_artifact_source_role(bytes(row[0])),
                    expected_sha256=(
                        self._metadata_sha256
                        if bytes(row[0]) == GALLERY_INFO_NAME.encode("ascii")
                        else None
                    ),
                )
                for row in rows[:bound]
            )
        _require_directory_unchanged(
            self._folder,
            self._expected_directory,
            self._expected_audit,
        )
        return FilesystemPage(items, len(rows) <= bound)


class _ReplayableDirectories:
    def __init__(
        self,
        folder: Path,
        *,
        expected_directory: FilesystemStat,
        expected_audit: bytes,
    ) -> None:
        self._folder = folder
        self._expected_directory = expected_directory
        self._expected_audit = expected_audit

    def page(
        self,
        *,
        after_name: bytes | None,
        limit: int,
    ) -> FilesystemPage[FilesystemDirectoryObservation]:
        bound = _page_limit(limit)
        after = _after_name(after_name)
        with _entry_index(self._folder, self._expected_directory) as index:
            _require_audit(index, self._expected_audit)
            if after is None:
                rows = index.execute(
                    "SELECT name_bytes, device, inode, size_bytes, modified_ns, "
                    "changed_ns, file_type FROM entries "
                    "ORDER BY name_bytes LIMIT ?",
                    (bound + 1,),
                ).fetchall()
            else:
                rows = index.execute(
                    "SELECT name_bytes, device, inode, size_bytes, modified_ns, "
                    "changed_ns, file_type FROM entries WHERE name_bytes > ? "
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
        _require_directory_unchanged(
            self._folder,
            self._expected_directory,
            self._expected_audit,
        )
        return FilesystemPage(items, len(rows) <= bound)


@contextmanager
def _entry_index(
    folder: Path,
    expected_directory: FilesystemStat,
) -> Iterator[sqlite3.Connection]:
    temporary = tempfile.TemporaryDirectory(prefix="h2hdb-ingest-source-")
    connection = sqlite3.connect(Path(temporary.name) / "entries.sqlite3")
    try:
        connection.executescript("""
            PRAGMA temp_store = FILE;
            CREATE TABLE entries (
                name_bytes BLOB PRIMARY KEY,
                device BLOB NOT NULL CHECK (length(device) = 8),
                inode BLOB NOT NULL CHECK (length(inode) = 8),
                size_bytes INTEGER NOT NULL,
                modified_ns INTEGER NOT NULL,
                changed_ns INTEGER NOT NULL,
                file_type INTEGER NOT NULL
            );
            CREATE TABLE snapshot (
                audit_sha256 BLOB NOT NULL,
                regular_count INTEGER NOT NULL,
                page_count INTEGER NOT NULL
            );
            """)
        before = FilesystemSource._directory_stat(folder)
        if before != expected_directory:
            raise FilesystemObservationError(
                f"gallery directory changed before observation: {folder}"
            )
        try:
            with os.scandir(folder) as entries:
                for entry in entries:
                    name = os.fsencode(entry.name)
                    _strict_name_bytes(name)
                    value = entry.stat(follow_symlinks=False)
                    observed = FilesystemStat.from_os_stat(value)
                    connection.execute(
                        "INSERT INTO entries VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            name,
                            observed.device.to_bytes(8, "big"),
                            observed.inode.to_bytes(8, "big"),
                            observed.size_bytes,
                            observed.modified_ns,
                            observed.changed_ns,
                            int(_entry_type(value.st_mode)),
                        ),
                    )
        except (OSError, sqlite3.DatabaseError) as error:
            raise FilesystemObservationError(
                f"unable to snapshot gallery directory {folder}: {error}"
            ) from error
        after = FilesystemSource._directory_stat(folder)
        if after != expected_directory:
            raise FilesystemObservationError(
                f"gallery directory changed during observation: {folder}"
            )
        digest = sha256(_ENTRY_AUDIT_PREFIX)
        regular_count = 0
        page_count = 0
        rows = connection.execute(
            "SELECT name_bytes, device, inode, size_bytes, modified_ns, "
            "changed_ns, file_type FROM entries ORDER BY name_bytes"
        )
        while page := rows.fetchmany(128):
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
                regular_count += int(file_type == FilesystemEntryType.REGULAR)
                page_count += int(
                    file_type == FilesystemEntryType.REGULAR
                    and _artifact_source_role(name) is FilesystemArtifactSourceRole.PAGE
                )
        connection.execute(
            "INSERT INTO snapshot VALUES (?, ?, ?)",
            (digest.digest(), regular_count, page_count),
        )
        connection.commit()
        yield connection
    finally:
        connection.close()
        temporary.cleanup()


def _require_audit(connection: sqlite3.Connection, expected: bytes) -> None:
    row = connection.execute("SELECT audit_sha256 FROM snapshot").fetchone()
    if row != (expected,):
        raise FilesystemObservationError("gallery directory facts changed")


def _require_directory_unchanged(
    folder: Path,
    expected_directory: FilesystemStat,
    expected_audit: bytes,
) -> None:
    with _entry_index(folder, expected_directory) as index:
        _require_audit(index, expected_audit)


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
