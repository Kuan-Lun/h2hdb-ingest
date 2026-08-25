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
import secrets
import sqlite3
import stat
from collections.abc import Iterator, Sequence
from contextlib import ExitStack, contextmanager
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

from .artifact import (
    _QUARANTINE_PAYLOAD_NAME,
    ManagedFilesystemArtifactAdapter,
    _lstat_at,
    _open_directory_chain,
    _open_private_quarantine,
    _PrivateQuarantine,
    _rename_noreplace,
    _require_directory_chain_identity,
    _verify_regular_at,
)
from .config import CBZGrouping
from .maintenance import CurrentProjectionMaintenanceOutcome

_COPY_BUFFER_BYTES = 4 * 1024 * 1024
_DATABASE_NAME = ".h2hdb-vnext-current-projection.sqlite3"
_LOCK_NAME = ".h2hdb-vnext-publication.lock"
_CURRENT_QUARANTINE_NAME = ".h2hdb-vnext-current-quarantine"
_ARTIFACT_LEAF = re.compile(r"[0-9a-f]{64}\.cbz")
_MAX_PAGE_ITEMS = 128
_MAX_CLEANUP_ARTIFACTS_PER_ATTEMPT = 8
_MAX_FILE_NAME_BYTES = 255


class CurrentProjectionAdapter:
    """Spool one pinned revision, then reconcile only managed friendly paths."""

    def __init__(
        self,
        *,
        artifact_store_path: Path,
        cbz_path: Path,
        grouping: CBZGrouping,
        artifact_adapter: ManagedFilesystemArtifactAdapter,
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
        if not isinstance(artifact_adapter, ManagedFilesystemArtifactAdapter):
            raise TypeError("artifact_adapter must be ManagedFilesystemArtifactAdapter")
        if artifact_adapter._root != artifact_root:
            raise ValueError(
                "artifact_adapter and current projection must share one artifact root"
            )
        self._artifact_root = artifact_root
        self._current_root = current_root
        self._grouping = grouping
        self._artifact_adapter = artifact_adapter
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
                self._prune_artifacts(connection)
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

    def maintain_cleanup(self) -> CurrentProjectionMaintenanceOutcome:
        """Run one publication-serialized, bounded cleanup action."""

        with self.publication_guard():
            with self._connection() as connection:
                return self._prune_artifacts(connection)

    def _prune_artifacts(
        self,
        connection: sqlite3.Connection,
    ) -> CurrentProjectionMaintenanceOutcome:
        """Forward one outbox page and attempt one fixed artifact page."""

        rows = connection.execute(
            "SELECT artifact_sha256 FROM artifact_cleanup_candidates "
            "ORDER BY artifact_sha256 LIMIT ?",
            (_MAX_CLEANUP_ARTIFACTS_PER_ATTEMPT,),
        ).fetchall()
        candidates = tuple(bytes(row[0]) for row in rows)
        if any(len(digest) != 32 for digest in candidates):
            raise RuntimeError("artifact cleanup candidate state is corrupt")
        if candidates:
            # The projection database is a durable outbox.  A crash after the
            # idempotent artifact-state enqueue but before this acknowledgement
            # simply forwards the same digests again on replay.
            self._artifact_adapter._enqueue_cleanup_candidates(candidates)
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.executemany(
                    "DELETE FROM artifact_cleanup_candidates "
                    "WHERE artifact_sha256 = ?",
                    ((digest,) for digest in candidates),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

        acknowledged = self._artifact_adapter._prune_cleanup_candidates(
            is_retained=lambda digest: self._projection_retains(connection, digest),
            limit=_MAX_CLEANUP_ARTIFACTS_PER_ATTEMPT,
        )
        projection_pending = connection.execute(
            "SELECT EXISTS (SELECT 1 FROM artifact_cleanup_candidates)"
        ).fetchone()
        if projection_pending is None or projection_pending[0] not in {0, 1}:
            raise RuntimeError("projection cleanup queue state is corrupt")
        remaining = (
            bool(projection_pending[0])
            or self._artifact_adapter._has_cleanup_candidates()
        )
        if not remaining:
            return CurrentProjectionMaintenanceOutcome.DONE
        if candidates or acknowledged:
            return CurrentProjectionMaintenanceOutcome.PROGRESSED
        return CurrentProjectionMaintenanceOutcome.BLOCKED

    @staticmethod
    def _projection_retains(connection: sqlite3.Connection, digest: bytes) -> bool:
        row = connection.execute(
            "SELECT (EXISTS (SELECT 1 FROM current_projection "
            "WHERE artifact_sha256 = ?) OR EXISTS ("
            "SELECT 1 FROM pending_projection WHERE artifact_sha256 = ?))",
            (digest, digest),
        ).fetchone()
        if row is None or row[0] not in {0, 1}:
            raise RuntimeError("current projection retention state is corrupt")
        return bool(row[0])

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
                connection.execute(
                    "INSERT OR IGNORE INTO artifact_cleanup_candidates "
                    "(artifact_sha256) "
                    "SELECT DISTINCT current.artifact_sha256 "
                    "FROM current_projection AS current "
                    "WHERE NOT EXISTS ("
                    "SELECT 1 FROM pending_projection AS pending "
                    "WHERE pending.artifact_sha256 = current.artifact_sha256)"
                )
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
            self._prune_artifacts(connection)

    def _preflight(self, connection: sqlite3.Connection, *, applying: bool) -> None:
        rows = connection.execute(
            "SELECT p.path_name, p.artifact_locator, p.artifact_sha256, "
            "p.size_bytes, p.authorized, c.path_name, c.artifact_sha256, "
            "c.device, c.inode, c.size_bytes, c.modified_ns, c.changed_ns "
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
                signature = self._safe_current_signature(path_name)
                if signature is None:
                    continue
                managed = row[5] is not None
                authorized = bool(row[4]) and applying
                if managed:
                    expected_signature = _signature_from_row(row[7:])
                    if signature == expected_signature:
                        continue
                    if authorized:
                        self._verify_current_file(
                            path_name,
                            expected_sha256=bytes(row[2]),
                            expected_size=int(row[3]),
                            label="recoverable projected artifact",
                        )
                        continue
                    raise RuntimeError(
                        "refusing to replace externally changed managed path: "
                        f"{target}"
                    )
                if authorized:
                    self._verify_current_file(
                        path_name,
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
                path_name = str(row[0])
                target = self._current_path(path_name)
                signature = self._safe_current_signature(path_name)
                if signature is None:
                    continue
                if signature != _signature_from_row(row[1:]):
                    raise RuntimeError(
                        f"refusing to delete externally changed managed path: {target}"
                    )

    def _materialize(self, connection: sqlite3.Connection) -> None:
        rows = connection.execute(
            "SELECT p.path_name, p.artifact_locator, p.artifact_sha256, "
            "p.size_bytes, c.path_name, c.artifact_sha256, c.device, c.inode, "
            "c.size_bytes, c.modified_ns, c.changed_ns "
            "FROM pending_projection AS p LEFT JOIN current_projection AS c "
            "ON c.path_name = p.path_name ORDER BY p.publication_key"
        )
        while page := rows.fetchmany(_MAX_PAGE_ITEMS):
            for row in page:
                path_name = str(row[0])
                digest = bytes(row[2])
                size_bytes = int(row[3])
                source = self._artifact_path(str(row[1]), digest)
                managed = row[4] is not None
                existing_signature = self._safe_current_signature(path_name)
                if not managed and existing_signature is not None:
                    signature = self._verify_current_file(
                        path_name,
                        expected_sha256=digest,
                        expected_size=size_bytes,
                        label="recoverable projected artifact",
                    )
                elif managed:
                    current_digest = bytes(row[5])
                    current_signature = _signature_from_row(row[6:])
                    if (
                        existing_signature == current_signature
                        and current_digest == digest
                    ):
                        signature = current_signature
                    else:
                        signature = self._atomic_copy(
                            source,
                            path_name,
                            expected_sha256=digest,
                            expected_size=size_bytes,
                            replace_managed_digest=current_digest,
                            replace_managed_signature=current_signature,
                        )
                else:
                    signature = self._atomic_copy(
                        source,
                        path_name,
                        expected_sha256=digest,
                        expected_size=size_bytes,
                        replace_managed_digest=None,
                        replace_managed_signature=None,
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
            "SELECT c.path_name, c.artifact_sha256, c.device, c.inode, "
            "c.size_bytes, c.modified_ns, c.changed_ns "
            "FROM current_projection AS c "
            "LEFT JOIN pending_projection AS p ON p.path_name = c.path_name "
            "WHERE p.path_name IS NULL ORDER BY c.path_name"
        )
        while page := rows.fetchmany(_MAX_PAGE_ITEMS):
            for row in page:
                self._safe_unlink_current(
                    str(row[0]),
                    expected_sha256=bytes(row[1]),
                    expected_signature=_signature_from_row(row[2:]),
                )

    def _safe_current_signature(
        self,
        path_name: str,
    ) -> tuple[bytes, bytes, int, int, int] | None:
        """Read a managed leaf signature through no-follow directory fds."""

        components = self._current_components(path_name)
        target = self._current_root.joinpath(*components)
        with _open_directory_chain(
            self._current_root,
            components[:-1],
            label=f"current projection parent is unsafe: {target.parent}",
            create_mode=0o755,
        ) as parent_descriptor:
            value = _lstat_at(parent_descriptor, components[-1])
            if value is None:
                return None
            if not stat.S_ISREG(value.st_mode):
                raise RuntimeError(f"projection target is not a regular file: {target}")
            return _signature_from_stat(value)

    def _verify_current_file(
        self,
        path_name: str,
        *,
        expected_sha256: bytes,
        expected_size: int,
        label: str,
    ) -> tuple[bytes, bytes, int, int, int]:
        components = self._current_components(path_name)
        target = self._current_root.joinpath(*components)
        with _open_directory_chain(
            self._current_root,
            components[:-1],
            label=f"current projection parent is unsafe: {target.parent}",
        ) as parent_descriptor:
            try:
                value = _verify_regular_at(
                    parent_descriptor,
                    components[-1],
                    expected_sha256=expected_sha256,
                    expected_size=expected_size,
                )
            except RuntimeError as error:
                raise RuntimeError(f"{label} failed verification: {target}") from error
            return _signature_from_stat(value)

    def _safe_unlink_current(
        self,
        path_name: str,
        *,
        expected_sha256: bytes,
        expected_signature: tuple[bytes, bytes, int, int, int],
    ) -> None:
        """Quarantine, verify, and unlink one stale managed leaf safely."""

        components = self._current_components(path_name)
        target = self._current_root.joinpath(*components)
        with _open_directory_chain(
            self._current_root,
            components[:-1],
            label=f"current projection parent is unsafe: {target.parent}",
        ) as parent_descriptor:
            with _open_private_quarantine(
                self._current_root,
                namespace_name=_CURRENT_QUARANTINE_NAME,
                object_name=_quarantine_object_name(
                    "stale",
                    path_name,
                    expected_sha256,
                ),
                label=f"stale projection quarantine is unsafe: {target}",
            ) as quarantine:
                leaf = components[-1]
                leaf_value = _lstat_at(parent_descriptor, leaf)
                payload_value = _lstat_at(
                    quarantine.object_descriptor,
                    _QUARANTINE_PAYLOAD_NAME,
                )
                if payload_value is not None:
                    if leaf_value is not None:
                        raise RuntimeError(
                            f"refusing competing stale cleanup state: {target}"
                        )
                else:
                    if leaf_value is None:
                        quarantine.remove_empty()
                        return
                    if (
                        not stat.S_ISREG(leaf_value.st_mode)
                        or _signature_from_stat(leaf_value) != expected_signature
                    ):
                        raise RuntimeError(
                            "refusing to delete externally changed managed path: "
                            f"{target}"
                        )
                    try:
                        _rename_noreplace(
                            leaf,
                            _QUARANTINE_PAYLOAD_NAME,
                            source_descriptor=parent_descriptor,
                            destination_descriptor=quarantine.object_descriptor,
                        )
                    except FileExistsError as error:
                        raise RuntimeError(
                            "stale projection quarantine destination changed: "
                            f"{target}"
                        ) from error
                    os.fsync(parent_descriptor)
                    os.fsync(quarantine.object_descriptor)
                quarantine.validate({_QUARANTINE_PAYLOAD_NAME})
                _require_directory_chain_identity(
                    self._current_root,
                    components[:-1],
                    parent_descriptor,
                    label=f"current projection parent changed: {target.parent}",
                )
                verified = self._verify_quarantined_current(
                    quarantine,
                    target=target,
                    expected_sha256=expected_sha256,
                    expected_signature=expected_signature,
                    operation="stale projection",
                )
                if _lstat_at(parent_descriptor, leaf) is not None:
                    raise RuntimeError(
                        f"refusing competing stale cleanup state: {target}"
                    )
                quarantine.validate({_QUARANTINE_PAYLOAD_NAME})
                verified_again = self._verify_quarantined_current(
                    quarantine,
                    target=target,
                    expected_sha256=expected_sha256,
                    expected_signature=expected_signature,
                    operation="stale projection",
                )
                if (verified.st_dev, verified.st_ino) != (
                    verified_again.st_dev,
                    verified_again.st_ino,
                ):
                    raise RuntimeError(
                        f"stale projection quarantine changed identity: {target}"
                    )
                # The publication lock and private 0700 object directory exclude
                # every authorized writer after this second verification.
                os.unlink(
                    _QUARANTINE_PAYLOAD_NAME,
                    dir_fd=quarantine.object_descriptor,
                )
                os.fsync(quarantine.object_descriptor)
                quarantine.remove_empty()

    @staticmethod
    def _verify_quarantined_current(
        quarantine: _PrivateQuarantine,
        *,
        target: Path,
        expected_sha256: bytes,
        expected_signature: tuple[bytes, bytes, int, int, int],
        operation: str,
    ) -> os.stat_result:
        try:
            verified = _verify_regular_at(
                quarantine.object_descriptor,
                _QUARANTINE_PAYLOAD_NAME,
                expected_sha256=expected_sha256,
                expected_size=expected_signature[2],
            )
        except RuntimeError as error:
            raise RuntimeError(
                f"{operation} quarantine failed verification: {target}"
            ) from error
        if (
            verified.st_dev.to_bytes(8, "big") != expected_signature[0]
            or verified.st_ino.to_bytes(8, "big") != expected_signature[1]
        ):
            raise RuntimeError(f"{operation} quarantine changed identity: {target}")
        return verified

    def _atomic_copy(
        self,
        source: Path,
        path_name: str,
        *,
        expected_sha256: bytes,
        expected_size: int,
        replace_managed_digest: bytes | None,
        replace_managed_signature: tuple[bytes, bytes, int, int, int] | None,
    ) -> tuple[bytes, bytes, int, int, int]:
        if (replace_managed_digest is None) != (replace_managed_signature is None):
            raise RuntimeError("managed replacement evidence is incomplete")
        components = self._current_components(path_name)
        target = self._current_root.joinpath(*components)
        with _open_directory_chain(
            self._current_root,
            components[:-1],
            label=f"current projection parent is unsafe: {target.parent}",
            create_mode=0o755,
        ) as parent_descriptor:
            leaf = components[-1]
            temporary = f".h2hdb-current-{secrets.token_hex(16)}.tmp"
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(
                temporary,
                flags,
                0o600,
                dir_fd=parent_descriptor,
            )
            digest = sha256()
            size_bytes = 0
            temporary_inode: tuple[int, int] | None = None
            try:
                os.fchmod(descriptor, 0o644)
                source_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
                with (
                    os.fdopen(
                        os.open(source, source_flags),
                        "rb",
                        closefd=True,
                    ) as reader,
                    os.fdopen(descriptor, "wb", closefd=False) as writer,
                ):
                    while part := reader.read(_COPY_BUFFER_BYTES):
                        digest.update(part)
                        size_bytes += len(part)
                        writer.write(part)
                    writer.flush()
                    os.fsync(writer.fileno())
                temporary_value = os.fstat(descriptor)
                temporary_inode = (temporary_value.st_dev, temporary_value.st_ino)
            finally:
                os.close(descriptor)
            try:
                if digest.digest() != expected_sha256 or size_bytes != expected_size:
                    raise RuntimeError(
                        "immutable artifact changed during projection copy"
                    )
                with ExitStack() as quarantine_stack:
                    quarantine = (
                        quarantine_stack.enter_context(
                            _open_private_quarantine(
                                self._current_root,
                                namespace_name=_CURRENT_QUARANTINE_NAME,
                                object_name=_quarantine_object_name(
                                    "replace",
                                    path_name,
                                    replace_managed_digest,
                                ),
                                label=(
                                    "managed replacement quarantine is unsafe: "
                                    f"{target}"
                                ),
                            )
                        )
                        if replace_managed_digest is not None
                        else None
                    )
                    quarantine_owned = False
                    if quarantine is not None:
                        assert replace_managed_digest is not None
                        assert replace_managed_signature is not None
                        quarantine_owned = self._prepare_managed_replacement(
                            parent_descriptor,
                            leaf=leaf,
                            quarantine=quarantine,
                            target=target,
                            old_sha256=replace_managed_digest,
                            old_signature=replace_managed_signature,
                            new_sha256=expected_sha256,
                            new_size=expected_size,
                        )
                        _require_directory_chain_identity(
                            self._current_root,
                            components[:-1],
                            parent_descriptor,
                            label=(
                                "current projection parent changed: " f"{target.parent}"
                            ),
                        )
                    if _lstat_at(parent_descriptor, leaf) is None:
                        try:
                            os.link(
                                temporary,
                                leaf,
                                src_dir_fd=parent_descriptor,
                                dst_dir_fd=parent_descriptor,
                                follow_symlinks=False,
                            )
                        except FileExistsError as error:
                            raise RuntimeError(
                                "refusing to replace competing current-view path: "
                                f"{target}"
                            ) from error
                    current_temporary = _lstat_at(parent_descriptor, temporary)
                    if (
                        temporary_inode is None
                        or current_temporary is None
                        or (
                            current_temporary.st_dev,
                            current_temporary.st_ino,
                        )
                        != temporary_inode
                    ):
                        raise RuntimeError(
                            f"projection temporary changed before install: {target}"
                        )
                    os.unlink(temporary, dir_fd=parent_descriptor)
                    temporary_inode = None
                    os.fsync(parent_descriptor)
                    installed = _verify_regular_at(
                        parent_descriptor,
                        leaf,
                        expected_sha256=expected_sha256,
                        expected_size=expected_size,
                    )
                    if quarantine is not None:
                        if quarantine_owned:
                            assert replace_managed_digest is not None
                            assert replace_managed_signature is not None
                            verified_old = self._verify_quarantined_current(
                                quarantine,
                                target=target,
                                expected_sha256=replace_managed_digest,
                                expected_signature=replace_managed_signature,
                                operation="managed replacement",
                            )
                            quarantine.validate({_QUARANTINE_PAYLOAD_NAME})
                            verified_old_again = self._verify_quarantined_current(
                                quarantine,
                                target=target,
                                expected_sha256=replace_managed_digest,
                                expected_signature=replace_managed_signature,
                                operation="managed replacement",
                            )
                            if (verified_old.st_dev, verified_old.st_ino) != (
                                verified_old_again.st_dev,
                                verified_old_again.st_ino,
                            ):
                                raise RuntimeError(
                                    "managed replacement quarantine changed "
                                    f"identity: {target}"
                                )
                            _require_directory_chain_identity(
                                self._current_root,
                                components[:-1],
                                parent_descriptor,
                                label=(
                                    "current projection parent changed: "
                                    f"{target.parent}"
                                ),
                            )
                            # The publication lock and private 0700 object
                            # directory exclude every authorized writer after
                            # the second verification above.
                            os.unlink(
                                _QUARANTINE_PAYLOAD_NAME,
                                dir_fd=quarantine.object_descriptor,
                            )
                            os.fsync(quarantine.object_descriptor)
                        quarantine.remove_empty()
                    os.fsync(parent_descriptor)
                    return _signature_from_stat(installed)
            finally:
                if temporary_inode is not None:
                    current_temporary = _lstat_at(parent_descriptor, temporary)
                    if (
                        current_temporary is not None
                        and (
                            current_temporary.st_dev,
                            current_temporary.st_ino,
                        )
                        == temporary_inode
                    ):
                        os.unlink(temporary, dir_fd=parent_descriptor)
                        os.fsync(parent_descriptor)

    @staticmethod
    def _prepare_managed_replacement(
        parent_descriptor: int,
        *,
        leaf: str,
        quarantine: _PrivateQuarantine,
        target: Path,
        old_sha256: bytes,
        old_signature: tuple[bytes, bytes, int, int, int],
        new_sha256: bytes,
        new_size: int,
    ) -> bool:
        leaf_value = _lstat_at(parent_descriptor, leaf)
        payload_value = _lstat_at(
            quarantine.object_descriptor,
            _QUARANTINE_PAYLOAD_NAME,
        )
        if payload_value is not None:
            if leaf_value is not None:
                try:
                    _verify_regular_at(
                        parent_descriptor,
                        leaf,
                        expected_sha256=new_sha256,
                        expected_size=new_size,
                    )
                except RuntimeError as error:
                    raise RuntimeError(
                        f"refusing competing managed replacement: {target}"
                    ) from error
        else:
            if leaf_value is None:
                return False
            if _signature_from_stat(leaf_value) == old_signature:
                try:
                    _rename_noreplace(
                        leaf,
                        _QUARANTINE_PAYLOAD_NAME,
                        source_descriptor=parent_descriptor,
                        destination_descriptor=quarantine.object_descriptor,
                    )
                except FileExistsError as error:
                    raise RuntimeError(
                        "managed replacement quarantine destination changed: "
                        f"{target}"
                    ) from error
                os.fsync(parent_descriptor)
                os.fsync(quarantine.object_descriptor)
                leaf_value = None
            else:
                try:
                    _verify_regular_at(
                        parent_descriptor,
                        leaf,
                        expected_sha256=new_sha256,
                        expected_size=new_size,
                    )
                except RuntimeError as error:
                    raise RuntimeError(
                        "refusing to replace externally changed managed path: "
                        f"{target}"
                    ) from error
                return False
        quarantine.validate({_QUARANTINE_PAYLOAD_NAME})
        CurrentProjectionAdapter._verify_quarantined_current(
            quarantine,
            target=target,
            expected_sha256=old_sha256,
            expected_signature=old_signature,
            operation="managed replacement",
        )
        if leaf_value is not None:
            _verify_regular_at(
                parent_descriptor,
                leaf,
                expected_sha256=new_sha256,
                expected_size=new_size,
            )
        return True

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
        return self._current_root.joinpath(*self._current_components(path_name))

    @staticmethod
    def _current_components(path_name: str) -> tuple[str, ...]:
        pure = PurePosixPath(path_name)
        if (
            pure.is_absolute()
            or not pure.parts
            or any(part in {"", ".", ".."} for part in pure.parts)
        ):
            raise RuntimeError("projection state contains an unsafe path")
        return pure.parts

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
                CREATE INDEX IF NOT EXISTS current_projection_artifact_sha256_idx
                    ON current_projection(artifact_sha256);
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
                CREATE INDEX IF NOT EXISTS pending_projection_artifact_sha256_idx
                    ON pending_projection(artifact_sha256);
                CREATE TABLE IF NOT EXISTS artifact_cleanup_candidates (
                    artifact_sha256 BLOB PRIMARY KEY
                        CHECK (length(artifact_sha256) = 32)
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


def _signature_from_stat(value: os.stat_result) -> tuple[bytes, bytes, int, int, int]:
    return (
        value.st_dev.to_bytes(8, "big"),
        value.st_ino.to_bytes(8, "big"),
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _quarantine_object_name(kind: str, path_name: str, digest: bytes) -> str:
    if not re.fullmatch(r"[a-z]+", kind) or type(digest) is not bytes:
        raise RuntimeError("invalid current projection quarantine identity")
    identity = sha256()
    identity.update(kind.encode("ascii"))
    identity.update(b"\0")
    identity.update(path_name.encode("utf-8"))
    identity.update(b"\0")
    identity.update(digest)
    return f"{kind}-{identity.hexdigest()}"


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
