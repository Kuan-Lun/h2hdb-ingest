"""Crash-safe current CBZ projection for Komga."""

from __future__ import annotations

__all__ = [
    "CurrentProjectionAdapter",
    "CurrentProjectionCheckpoint",
    "CurrentProjectionItem",
    "CurrentProjectionStatus",
]

import fcntl
import os
import re
import sqlite3
import stat
import tempfile
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path, PurePosixPath
from threading import RLock

from h2hdb import (
    CurrentProjectionCheckpoint,
    CurrentProjectionStatus,
)
from h2hdb import (
    VNextCurrentProjectionItem as CurrentProjectionItem,
)

from .config import CBZGrouping

_COPY_BUFFER_BYTES = 4 * 1024 * 1024
_DATABASE_NAME = ".h2hdb-vnext-current-projection.sqlite3"
_LOCK_NAME = ".h2hdb-vnext-publication.lock"
_ARTIFACT_LEAF = re.compile(r"[0-9a-f]{64}\.cbz")
_MAX_PAGE_ITEMS = 128
_MAX_FILE_NAME_BYTES = 255


class CurrentProjectionAdapter:
    """Spool one pinned revision, then reconcile only managed friendly paths."""

    def __init__(
        self,
        *,
        artifact_store_path: Path,
        cbz_path: Path,
        grouping: CBZGrouping,
    ) -> None:
        artifact_root = artifact_store_path.resolve(strict=False)
        current_root = cbz_path.resolve(strict=False)
        if artifact_root == current_root:
            raise ValueError("artifact_store_path and cbz_path must be different")
        if artifact_root.is_relative_to(current_root) or current_root.is_relative_to(
            artifact_root
        ):
            raise ValueError("artifact and current-view roots must not be nested")
        if not isinstance(grouping, CBZGrouping):
            raise TypeError("grouping must be CBZGrouping")
        self._artifact_root = artifact_root
        self._current_root = current_root
        self._grouping = grouping
        self._database_path = artifact_root / _DATABASE_NAME
        self._lock_path = artifact_root / _LOCK_NAME
        self._process_lock = RLock()
        self._guard_depth = 0

    @contextmanager
    def publication_guard(self) -> Iterator[None]:
        """Serialize publication, intent creation, projection, and finalization."""

        with self._process_lock:
            self._artifact_root.mkdir(mode=0o700, parents=True, exist_ok=True)
            self._current_root.mkdir(mode=0o755, parents=True, exist_ok=True)
            _require_directory(self._artifact_root, label="artifact store")
            _require_directory(self._current_root, label="current projection")
            descriptor = os.open(
                self._lock_path,
                os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                self._guard_depth += 1
                with self._connection():
                    pass
                yield
            finally:
                self._guard_depth -= 1
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                finally:
                    os.close(descriptor)

    def begin(
        self,
        revision: int,
        receipt_id: bytes,
    ) -> CurrentProjectionCheckpoint:
        """Start a new spool or identify a durable recovery state."""

        target = _revision(revision)
        receipt = _receipt_id(receipt_id)
        self._require_guard()
        with self._connection() as connection:
            state = _state(connection)
            (
                current_revision,
                current_receipt,
                pending_revision,
                pending_receipt,
                phase,
                last_cursor,
            ) = state
            if current_revision is not None and target < current_revision:
                raise RuntimeError("refusing to project an older catalog revision")
            if pending_revision is not None and pending_revision != target:
                raise RuntimeError(
                    "another catalog revision has an unfinished current projection"
                )
            if pending_revision == target:
                if pending_receipt != receipt:
                    raise RuntimeError(
                        "pending projection belongs to another publication receipt"
                    )
                status = (
                    CurrentProjectionStatus.SPOOL
                    if phase == "OPEN"
                    else CurrentProjectionStatus.RECONCILE
                )
                return CurrentProjectionCheckpoint(
                    target,
                    receipt,
                    status,
                    (last_cursor if status is CurrentProjectionStatus.SPOOL else None),
                )
            if pending_revision is None and current_revision == target:
                if current_receipt != receipt:
                    raise RuntimeError(
                        "installed projection belongs to another publication receipt"
                    )
                return CurrentProjectionCheckpoint(
                    target,
                    receipt,
                    CurrentProjectionStatus.COMPLETE,
                    None,
                )
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute("DELETE FROM pending_projection")
                connection.execute(
                    "UPDATE projection_state SET pending_revision = ?, "
                    "pending_receipt_id = ?, phase = 'OPEN', last_cursor = NULL "
                    "WHERE singleton = 1",
                    (target, receipt),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return CurrentProjectionCheckpoint(
            target,
            receipt,
            CurrentProjectionStatus.SPOOL,
            None,
        )

    def append_page(
        self,
        revision: int,
        items: Sequence[CurrentProjectionItem],
    ) -> None:
        """Durably append one strictly ordered, bounded page to the intent."""

        target = _revision(revision)
        page = tuple(items)
        if len(page) > _MAX_PAGE_ITEMS:
            raise ValueError("current projection page exceeds 128 items")
        if any(not isinstance(item, CurrentProjectionItem) for item in page):
            raise TypeError("projection page contains a foreign item")
        if any(
            left.publication_key >= right.publication_key
            for left, right in zip(page, page[1:])
        ):
            raise ValueError("projection page keys must be strictly increasing")
        self._require_guard()
        with self._connection() as connection:
            (
                _current_revision,
                _current_receipt,
                pending_revision,
                _pending_receipt,
                phase,
                last_cursor,
            ) = _state(connection)
            if pending_revision != target or phase != "OPEN":
                raise RuntimeError("projection intent is not open for this revision")
            if (
                page
                and last_cursor is not None
                and page[0].publication_key <= last_cursor
            ):
                raise ValueError("projection page does not advance the keyset cursor")
            connection.execute("BEGIN IMMEDIATE")
            try:
                for item in page:
                    path_name = self._friendly_path(item)
                    locator = "/".join(item.artifact_locator_components)
                    connection.execute(
                        "INSERT INTO pending_projection "
                        "(publication_key, path_name, gid, source_name, upload_time, "
                        "artifact_locator, artifact_sha256, size_bytes, authorized, "
                        "device, inode, modified_ns, changed_ns) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL, NULL, NULL)",
                        (
                            item.publication_key,
                            path_name,
                            item.gid,
                            item.source_gallery_name,
                            item.upload_time,
                            locator,
                            item.artifact_sha256,
                            item.size_bytes,
                        ),
                    )
                if page:
                    connection.execute(
                        "UPDATE projection_state SET last_cursor = ? "
                        "WHERE singleton = 1",
                        (page[-1].publication_key,),
                    )
                connection.commit()
            except sqlite3.IntegrityError as error:
                connection.rollback()
                raise RuntimeError(
                    "projection contains a duplicate key or friendly path"
                ) from error
            except BaseException:
                connection.rollback()
                raise

    def seal(self, revision: int) -> None:
        target = _revision(revision)
        self._require_guard()
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                affected = connection.execute(
                    "UPDATE projection_state SET phase = 'SEALED' "
                    "WHERE singleton = 1 AND pending_revision = ? "
                    "AND phase = 'OPEN'",
                    (target,),
                ).rowcount
                if affected != 1:
                    raise RuntimeError("projection intent cannot be sealed")
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def reconcile(self, revision: int) -> None:
        """Materialize the sealed intent and atomically install its state."""

        target = _revision(revision)
        self._require_guard()
        with self._connection() as connection:
            (
                _current_revision,
                _current_receipt,
                pending_revision,
                pending_receipt,
                phase,
                _last_cursor,
            ) = _state(connection)
            if pending_revision != target or phase not in {"SEALED", "APPLYING"}:
                raise RuntimeError("projection intent is not ready to reconcile")
            self._preflight(connection, applying=phase == "APPLYING")
            if phase == "SEALED":
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.execute("UPDATE pending_projection SET authorized = 1")
                    connection.execute(
                        "UPDATE projection_state SET phase = 'APPLYING' "
                        "WHERE singleton = 1 AND pending_revision = ? "
                        "AND phase = 'SEALED'",
                        (target,),
                    )
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
            self._materialize(connection)
            self._remove_stale(connection)
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute("DELETE FROM current_projection")
                connection.execute(
                    "INSERT INTO current_projection "
                    "(path_name, artifact_sha256, device, inode, size_bytes, "
                    "modified_ns, changed_ns) "
                    "SELECT path_name, artifact_sha256, device, inode, size_bytes, "
                    "modified_ns, changed_ns FROM pending_projection"
                )
                connection.execute("DELETE FROM pending_projection")
                affected = connection.execute(
                    "UPDATE projection_state SET current_revision = ?, "
                    "current_receipt_id = ?, pending_revision = NULL, "
                    "pending_receipt_id = NULL, phase = 'IDLE', last_cursor = NULL "
                    "WHERE singleton = 1 AND pending_revision = ? "
                    "AND phase = 'APPLYING'",
                    (target, pending_receipt, target),
                ).rowcount
                if affected != 1:
                    raise RuntimeError("projection state changed before commit")
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _preflight(self, connection: sqlite3.Connection, *, applying: bool) -> None:
        rows = connection.execute(
            "SELECT p.path_name, p.artifact_locator, p.artifact_sha256, "
            "p.size_bytes, p.authorized, c.path_name "
            "FROM pending_projection AS p LEFT JOIN current_projection AS c "
            "ON c.path_name = p.path_name ORDER BY p.publication_key"
        )
        while page := rows.fetchmany(_MAX_PAGE_ITEMS):
            for row in page:
                path_name = str(row[0])
                artifact = self._artifact_path(str(row[1]), bytes(row[2]))
                _verify_regular_file(
                    artifact,
                    expected_sha256=bytes(row[2]),
                    expected_size=int(row[3]),
                    label="immutable artifact",
                )
                target = self._current_path(path_name)
                if not target.exists() and not target.is_symlink():
                    continue
                managed = row[5] is not None
                authorized = bool(row[4]) and applying
                if managed:
                    if target.is_dir() and not target.is_symlink():
                        raise RuntimeError(
                            f"managed projection path became a directory: {target}"
                        )
                    continue
                if authorized:
                    _verify_regular_file(
                        target,
                        expected_sha256=bytes(row[2]),
                        expected_size=int(row[3]),
                        label="recoverable projected artifact",
                    )
                    continue
                raise RuntimeError(
                    f"refusing to replace unknown current-view path: {target}"
                )
        stale = connection.execute(
            "SELECT c.path_name, c.device, c.inode, c.size_bytes, c.modified_ns, "
            "c.changed_ns FROM current_projection AS c "
            "LEFT JOIN pending_projection AS p ON p.path_name = c.path_name "
            "WHERE p.path_name IS NULL ORDER BY c.path_name"
        )
        while page := stale.fetchmany(_MAX_PAGE_ITEMS):
            for row in page:
                target = self._current_path(str(row[0]))
                if not target.exists() and not target.is_symlink():
                    continue
                if _lstat_signature(target) != _signature_from_row(row[1:]):
                    raise RuntimeError(
                        f"refusing to delete externally changed managed path: {target}"
                    )

    def _materialize(self, connection: sqlite3.Connection) -> None:
        rows = connection.execute(
            "SELECT path_name, artifact_locator, artifact_sha256, size_bytes "
            "FROM pending_projection ORDER BY publication_key"
        )
        while page := rows.fetchmany(_MAX_PAGE_ITEMS):
            for row in page:
                path_name = str(row[0])
                digest = bytes(row[2])
                size_bytes = int(row[3])
                source = self._artifact_path(str(row[1]), digest)
                target = self._current_path(path_name)
                current = connection.execute(
                    "SELECT artifact_sha256, device, inode, size_bytes, "
                    "modified_ns, changed_ns FROM current_projection "
                    "WHERE path_name = ?",
                    (path_name,),
                ).fetchone()
                if current is None and (target.exists() or target.is_symlink()):
                    _verify_regular_file(
                        target,
                        expected_sha256=digest,
                        expected_size=size_bytes,
                        label="recoverable projected artifact",
                    )
                    signature = _lstat_signature(target)
                elif (
                    current is not None
                    and bytes(current[0]) == digest
                    and target.exists()
                    and not target.is_symlink()
                    and _lstat_signature(target) == _signature_from_row(current[1:])
                ):
                    signature = _signature_from_row(current[1:])
                else:
                    signature = self._atomic_copy(
                        source,
                        target,
                        expected_sha256=digest,
                        expected_size=size_bytes,
                        replace_managed=current is not None,
                    )
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.execute(
                        "UPDATE pending_projection SET device = ?, inode = ?, "
                        "size_bytes = ?, modified_ns = ?, changed_ns = ? "
                        "WHERE path_name = ?",
                        (*signature, path_name),
                    )
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise

    def _remove_stale(self, connection: sqlite3.Connection) -> None:
        rows = connection.execute(
            "SELECT c.path_name, c.device, c.inode, c.size_bytes, c.modified_ns, "
            "c.changed_ns FROM current_projection AS c "
            "LEFT JOIN pending_projection AS p ON p.path_name = c.path_name "
            "WHERE p.path_name IS NULL ORDER BY c.path_name"
        )
        while page := rows.fetchmany(_MAX_PAGE_ITEMS):
            for row in page:
                target = self._current_path(str(row[0]))
                if not target.exists() and not target.is_symlink():
                    continue
                if _lstat_signature(target) != _signature_from_row(row[1:]):
                    raise RuntimeError(
                        f"refusing to delete externally changed managed path: {target}"
                    )
                target.unlink()
                _fsync_directory(target.parent)

    def _atomic_copy(
        self,
        source: Path,
        target: Path,
        *,
        expected_sha256: bytes,
        expected_size: int,
        replace_managed: bool,
    ) -> tuple[bytes, bytes, int, int, int]:
        self._ensure_current_parent(target.parent)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".h2hdb-current-",
            suffix=".tmp",
            dir=target.parent,
        )
        temporary = Path(temporary_name)
        digest = sha256()
        size_bytes = 0
        try:
            os.fchmod(descriptor, 0o644)
            flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
            with (
                os.fdopen(os.open(source, flags), "rb", closefd=True) as reader,
                os.fdopen(descriptor, "wb", closefd=True) as writer,
            ):
                while part := reader.read(_COPY_BUFFER_BYTES):
                    digest.update(part)
                    size_bytes += len(part)
                    writer.write(part)
                writer.flush()
                os.fsync(writer.fileno())
            if digest.digest() != expected_sha256 or size_bytes != expected_size:
                raise RuntimeError("immutable artifact changed during projection copy")
            if replace_managed:
                os.replace(temporary, target)
            else:
                try:
                    os.link(temporary, target, follow_symlinks=False)
                except FileExistsError as error:
                    raise RuntimeError(
                        f"refusing to replace unknown current-view path: {target}"
                    ) from error
                # Removing the temporary hard link changes the inode ctime, so
                # do it before capturing the durable target signature.
                temporary.unlink()
            _fsync_directory(target.parent)
            return _lstat_signature(target)
        finally:
            temporary.unlink(missing_ok=True)

    def _friendly_path(self, item: CurrentProjectionItem) -> str:
        leaf = _friendly_leaf(item.gid, item.source_gallery_name)
        if self._grouping is CBZGrouping.flat:
            return leaf
        uploaded = datetime.fromtimestamp(item.upload_time / 1_000_000, tz=UTC)
        components = [f"{uploaded.year:04}"]
        if self._grouping in {CBZGrouping.date_yyyy_mm, CBZGrouping.date_yyyy_mm_dd}:
            components.append(f"{uploaded.month:02}")
        if self._grouping is CBZGrouping.date_yyyy_mm_dd:
            components.append(f"{uploaded.day:02}")
        return PurePosixPath(*components, leaf).as_posix()

    def _artifact_path(self, locator: str, expected_sha256: bytes) -> Path:
        components = tuple(PurePosixPath(locator).parts)
        _validate_artifact_locator(components, expected_sha256)
        return self._artifact_root.joinpath(*components)

    def _current_path(self, path_name: str) -> Path:
        pure = PurePosixPath(path_name)
        if (
            pure.is_absolute()
            or not pure.parts
            or any(part in {"", ".", ".."} for part in pure.parts)
        ):
            raise RuntimeError("projection state contains an unsafe path")
        return self._current_root.joinpath(*pure.parts)

    def _ensure_current_parent(self, directory: Path) -> None:
        directory.mkdir(mode=0o755, parents=True, exist_ok=True)
        current = directory
        while current != self._current_root:
            _require_directory(current, label="current projection parent")
            current = current.parent
        _require_directory(self._current_root, label="current projection root")

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        if self._database_path.exists() or self._database_path.is_symlink():
            value = self._database_path.lstat()
            if stat.S_ISLNK(value.st_mode) or not stat.S_ISREG(value.st_mode):
                raise RuntimeError("current projection database path is unsafe")
        connection = sqlite3.connect(self._database_path)
        try:
            connection.execute("PRAGMA journal_mode = DELETE")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS projection_state (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    format_version INTEGER NOT NULL CHECK (format_version = 2),
                    current_revision INTEGER NULL,
                    current_receipt_id BLOB NULL
                        CHECK (current_receipt_id IS NULL
                               OR length(current_receipt_id) = 16),
                    pending_revision INTEGER NULL,
                    pending_receipt_id BLOB NULL
                        CHECK (pending_receipt_id IS NULL
                               OR length(pending_receipt_id) = 16),
                    phase TEXT NOT NULL
                        CHECK (phase IN ('IDLE', 'OPEN', 'SEALED', 'APPLYING')),
                    last_cursor BLOB NULL
                );
                INSERT OR IGNORE INTO projection_state
                    (singleton, format_version, current_revision,
                     current_receipt_id, pending_revision, pending_receipt_id,
                     phase, last_cursor)
                VALUES (1, 2, NULL, NULL, NULL, NULL, 'IDLE', NULL);
                CREATE TABLE IF NOT EXISTS current_projection (
                    path_name TEXT PRIMARY KEY,
                    artifact_sha256 BLOB NOT NULL
                        CHECK (length(artifact_sha256) = 32),
                    device BLOB NOT NULL CHECK (length(device) = 8),
                    inode BLOB NOT NULL CHECK (length(inode) = 8),
                    size_bytes INTEGER NOT NULL,
                    modified_ns INTEGER NOT NULL,
                    changed_ns INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS pending_projection (
                    publication_key BLOB PRIMARY KEY,
                    path_name TEXT NOT NULL UNIQUE,
                    gid INTEGER NOT NULL,
                    source_name TEXT NOT NULL,
                    upload_time INTEGER NOT NULL,
                    artifact_locator TEXT NOT NULL,
                    artifact_sha256 BLOB NOT NULL
                        CHECK (length(artifact_sha256) = 32),
                    size_bytes INTEGER NOT NULL,
                    authorized INTEGER NOT NULL CHECK (authorized IN (0, 1)),
                    device BLOB NULL,
                    inode BLOB NULL,
                    modified_ns INTEGER NULL,
                    changed_ns INTEGER NULL
                );
                """)
            if connection.execute(
                "SELECT format_version FROM projection_state WHERE singleton = 1"
            ).fetchone() != (2,):
                raise RuntimeError("unsupported current projection state format")
            connection.commit()
            yield connection
        finally:
            connection.close()

    def _require_guard(self) -> None:
        if self._guard_depth != 1:
            raise RuntimeError("current projection requires publication_guard")


def _state(
    connection: sqlite3.Connection,
) -> tuple[
    int | None,
    bytes | None,
    int | None,
    bytes | None,
    str,
    bytes | None,
]:
    row = connection.execute(
        "SELECT current_revision, current_receipt_id, pending_revision, "
        "pending_receipt_id, phase, last_cursor "
        "FROM projection_state WHERE singleton = 1"
    ).fetchone()
    if row is None or len(row) != 6:
        raise RuntimeError("current projection state is corrupt")
    state = (
        int(row[0]) if row[0] is not None else None,
        bytes(row[1]) if row[1] is not None else None,
        int(row[2]) if row[2] is not None else None,
        bytes(row[3]) if row[3] is not None else None,
        str(row[4]),
        bytes(row[5]) if row[5] is not None else None,
    )
    (
        current_revision,
        current_receipt,
        pending_revision,
        pending_receipt,
        phase,
        cursor,
    ) = state
    if (current_revision is None) != (current_receipt is None):
        raise RuntimeError("current projection receipt pair is corrupt")
    if (pending_revision is None) != (pending_receipt is None):
        raise RuntimeError("pending projection receipt pair is corrupt")
    if (phase == "IDLE") != (pending_revision is None):
        raise RuntimeError("pending projection phase is corrupt")
    if cursor is not None and len(cursor) != 32:
        raise RuntimeError("current projection cursor is corrupt")
    return state


def _validate_artifact_locator(
    components: tuple[str, ...],
    expected_sha256: bytes,
) -> None:
    digest = expected_sha256.hex()
    if (
        type(components) is not tuple
        or len(components) != 3
        or components[0] != "sha256"
        or components[1] != digest[:2]
        or components[2] != f"{digest}.cbz"
        or _ARTIFACT_LEAF.fullmatch(components[2]) is None
    ):
        raise ValueError("artifact locator is not canonical managed-filesystem v1")


def _friendly_leaf(gid: int, source_name: str) -> str:
    sanitized = source_name.replace("/", "_").replace("\\", "_").replace("\0", "_")
    prefix = f"{gid} - "
    suffix = ".cbz"
    maximum = _MAX_FILE_NAME_BYTES - len(prefix.encode()) - len(suffix.encode())
    while len(sanitized.encode("utf-8")) > maximum:
        sanitized = sanitized[:-1]
    if not sanitized:
        sanitized = "gallery"
    return f"{prefix}{sanitized}{suffix}"


def _revision(value: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 1 <= value < 1 << 63
    ):
        raise ValueError("catalog revision must be a positive signed int63")
    return value


def _receipt_id(value: bytes) -> bytes:
    if type(value) is not bytes or len(value) != 16:
        raise ValueError("publication receipt_id must contain exactly 16 bytes")
    return value


def _require_directory(path: Path, *, label: str) -> None:
    value = path.lstat()
    if stat.S_ISLNK(value.st_mode) or not stat.S_ISDIR(value.st_mode):
        raise RuntimeError(f"{label} is not a safe directory: {path}")


def _verify_regular_file(
    path: Path,
    *,
    expected_sha256: bytes,
    expected_size: int,
    label: str,
) -> None:
    try:
        value = path.lstat()
    except OSError as error:
        raise RuntimeError(f"{label} is unavailable: {path}") from error
    if not stat.S_ISREG(value.st_mode) or value.st_size != expected_size:
        raise RuntimeError(f"{label} has an unexpected type or size: {path}")
    digest = sha256()
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    with os.fdopen(os.open(path, flags), "rb", closefd=True) as source:
        while part := source.read(_COPY_BUFFER_BYTES):
            digest.update(part)
    if digest.digest() != expected_sha256:
        raise RuntimeError(f"{label} has an unexpected digest: {path}")


def _lstat_signature(path: Path) -> tuple[bytes, bytes, int, int, int]:
    value = path.lstat()
    if not stat.S_ISREG(value.st_mode):
        raise RuntimeError(f"projection target is not a regular file: {path}")
    return (
        value.st_dev.to_bytes(8, "big"),
        value.st_ino.to_bytes(8, "big"),
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _signature_from_row(row: Sequence[object]) -> tuple[bytes, bytes, int, int, int]:
    if len(row) != 5:
        raise RuntimeError("projection stat signature has an invalid shape")
    device, inode, size_bytes, modified_ns, changed_ns = row
    if type(device) is not bytes or type(inode) is not bytes:
        raise RuntimeError("projection stat signature has invalid byte fields")
    if (
        type(size_bytes) is not int
        or type(modified_ns) is not int
        or type(changed_ns) is not int
    ):
        raise RuntimeError("projection stat signature has invalid integer fields")
    return (
        device,
        inode,
        size_bytes,
        modified_ns,
        changed_ns,
    )


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
