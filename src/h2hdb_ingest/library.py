"""Crash-safe single-copy CBZ library storage and activation."""

from __future__ import annotations

__all__ = ["ManagedFilesystemLibraryAdapter"]

import fcntl
import json
import os
import re
import secrets
import sqlite3
import stat
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from itertools import pairwise
from pathlib import Path
from threading import RLock
from typing import BinaryIO, cast

from h2hdb import (
    ArtifactReleaseStorageEvidence,
    ArtifactStorageEvidence,
    ArtifactStorageKey,
    ArtifactTransformKind,
    LibraryActivationCheckpoint,
    LibraryActivationStatus,
    VNextLibraryActivationItem,
    artifact_storage_key,
)

from .artifact import ARTIFACT_ADAPTER_ID, ArtifactProducerIdentity
from .maintenance import LibraryMaintenanceOutcome

_COPY_BUFFER_BYTES = 4 * 1024 * 1024
_STATE_DIRECTORY_NAME = ".h2hdb-state"
_CURRENT_DIRECTORY_NAME = "current"
_STAGING_DIRECTORY_NAME = "staging"
_QUARANTINE_DIRECTORY_NAME = "quarantine"
_JOURNAL_DIRECTORY_NAME = "journal"
_LOCKS_DIRECTORY_NAME = "locks"
_COORDINATION_DIRECTORY_NAME = "coordination"
_DATABASE_NAME = "library-activation.sqlite3"
_STATE_LOCK_NAME = "state.lock"
_PUBLICATION_LOCK_NAME = "publication.lock"
_ACTIVATING_MARKER_NAME = "ACTIVATING"
_MARKER_FORMAT = "h2hdb-library-activation-v1"
_MAX_PAGE_ITEMS = 128
_MAX_CLEANUP_ITEMS = 8
_MAX_JOURNAL_CLEANUP_ITEMS = 128
_PRIVATE_MODE = 0o700
_PUBLIC_DIRECTORY_MODE = 0o755
_PUBLIC_FILE_MODE = 0o644
_STAGE_LEAF = re.compile(r"[0-9a-f]{64}\.cbz")
_TEMPORARY_LEAF = re.compile(r"\.[0-9a-f]{64}\.tmp")


@dataclass(frozen=True, slots=True)
class _Signature:
    device: bytes
    inode: bytes
    size_bytes: int
    modified_ns: int
    changed_ns: int

    @classmethod
    def from_stat(cls, value: os.stat_result) -> _Signature:
        return cls(
            value.st_dev.to_bytes(8, "big"),
            value.st_ino.to_bytes(8, "big"),
            value.st_size,
            value.st_mtime_ns,
            value.st_ctime_ns,
        )

    @classmethod
    def from_row(cls, row: Sequence[object]) -> _Signature:
        if len(row) != 5:
            raise RuntimeError("library journal stat signature has an invalid shape")
        device, inode, size_bytes, modified_ns, changed_ns = row
        if type(device) is not bytes or type(inode) is not bytes:
            raise RuntimeError("library journal stat identity is corrupt")
        if any(
            type(value) is not int for value in (size_bytes, modified_ns, changed_ns)
        ):
            raise RuntimeError("library journal stat scalars are corrupt")
        return cls(
            device,
            inode,
            cast(int, size_bytes),
            cast(int, modified_ns),
            cast(int, changed_ns),
        )

    def sql(self) -> tuple[bytes, bytes, int, int, int]:
        return (
            self.device,
            self.inode,
            self.size_bytes,
            self.modified_ns,
            self.changed_ns,
        )


@dataclass(frozen=True, slots=True)
class _JournalState:
    current_revision: int | None
    current_receipt: bytes | None
    pending_revision: int | None
    pending_receipt: bytes | None
    phase: str
    last_cursor: bytes | None


class ManagedFilesystemLibraryAdapter:
    """Render, stage, activate, and release one authoritative CBZ tree.

    Only ``current`` is public.  Candidate bytes and the durable activation
    journal remain below ``.h2hdb-state`` on the same filesystem.  The adapter
    never derives a filesystem location itself: it accepts only the core-owned
    stable GID storage key and validates it against the public resolver.
    """

    adapter_id = ARTIFACT_ADAPTER_ID

    def __init__(
        self,
        library_path: Path,
        *,
        max_image_short_side: int,
        producer: ArtifactProducerIdentity | None = None,
    ) -> None:
        if not isinstance(library_path, Path):
            raise TypeError("library_path must be Path")
        if type(max_image_short_side) is not int or max_image_short_side < 1:
            raise ValueError("max_image_short_side must be positive")
        self._root = library_path.resolve(strict=False)
        self._current = self._root / _CURRENT_DIRECTORY_NAME
        self._state = self._root / _STATE_DIRECTORY_NAME
        self._staging = self._state / _STAGING_DIRECTORY_NAME
        self._quarantine = self._state / _QUARANTINE_DIRECTORY_NAME
        self._journal = self._state / _JOURNAL_DIRECTORY_NAME
        self._locks = self._state / _LOCKS_DIRECTORY_NAME
        self._coordination = self._state / _COORDINATION_DIRECTORY_NAME
        self._database_path = self._journal / _DATABASE_NAME
        self._state_lock_path = self._locks / _STATE_LOCK_NAME
        self._publication_lock_path = self._coordination / _PUBLICATION_LOCK_NAME
        self._marker_path = self._coordination / _ACTIVATING_MARKER_NAME
        self._max_image_short_side = max_image_short_side
        self._producer = producer or ArtifactProducerIdentity.current()
        self.producer_fingerprint_sha256 = self._producer.fingerprint_sha256
        self._process_lock = RLock()
        self._guard_depth = 0
        self._publication_descriptor: int | None = None

    @property
    def producer(self) -> ArtifactProducerIdentity:
        return self._producer

    @property
    def current_path(self) -> Path:
        """The sole read-only mount exposed to Komga and OPDS."""

        return self._current

    @contextmanager
    def publication_guard(self) -> Iterator[None]:
        """Keep one local publication turn and its activation lifecycle paired."""

        with self._process_lock:
            if self._guard_depth != 0:
                raise RuntimeError("library publication guard is not reentrant")
            self._guard_depth = 1
            try:
                self._ensure_layout()
                yield
            finally:
                self._release_publication_lock()
                self._guard_depth = 0

    def render_member(
        self,
        source: BinaryIO,
        transform_kind: ArtifactTransformKind,
        destination: BinaryIO,
    ) -> None:
        ArtifactProducerIdentity.render_member(
            source,
            transform_kind,
            destination,
            max_image_short_side=self._max_image_short_side,
        )

    def protect(
        self,
        archive: BinaryIO,
        storage_key: ArtifactStorageKey,
        expected_artifact_sha256: bytes,
        expected_size_bytes: int,
        protection_token: bytes,
    ) -> ArtifactStorageEvidence:
        """Durably stage exact bytes; no public CBZ is created here."""

        key = _storage_key(storage_key)
        digest = _digest(expected_artifact_sha256)
        size_bytes = _size(expected_size_bytes)
        token = _token(protection_token)
        token_name = sha256(token).hexdigest()
        stage_leaf = f"{token_name}.cbz"
        temporary_leaf = f".{token_name}.tmp"
        self._ensure_layout()
        with self._exclusive_state() as connection:
            existing = connection.execute(
                "SELECT storage_codec, storage_path, artifact_sha256, size_bytes, "
                "state, staging_leaf, device, inode, modified_ns, changed_ns "
                "FROM protection_tokens WHERE token = ?",
                (token,),
            ).fetchone()
            expected_facts = (key.codec, _key_text(key), digest, size_bytes)
            if existing is not None:
                if tuple(existing[:4]) != expected_facts:
                    raise RuntimeError(
                        "artifact protection token was reused for another artifact"
                    )
                state = str(existing[4])
                if state == "RELEASED":
                    return ArtifactStorageEvidence(False)
                if state == "INSTALLED":
                    self._verify_current(
                        key,
                        expected_sha256=digest,
                        expected_size=size_bytes,
                        label="installed library artifact",
                    )
                    return ArtifactStorageEvidence(True)
                if state == "STAGED":
                    self._verify_stage_row(existing[5:], digest=digest, size=size_bytes)
                    return ArtifactStorageEvidence(True)
                if state != "WRITING":
                    raise RuntimeError("artifact protection journal state is corrupt")
            else:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.execute(
                        "INSERT INTO protection_tokens "
                        "(token, storage_codec, storage_path, artifact_sha256, "
                        "size_bytes, state, staging_leaf) "
                        "VALUES (?, ?, ?, ?, ?, 'WRITING', ?)",
                        (token, *expected_facts, stage_leaf),
                    )
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise

            final_path = self._staging / stage_leaf
            if final_path.exists() or final_path.is_symlink():
                signature = _verify_regular_file(
                    final_path,
                    expected_sha256=digest,
                    expected_size=size_bytes,
                    label="recoverable staged artifact",
                )
            else:
                temporary_path = self._staging / temporary_leaf
                if temporary_path.exists() or temporary_path.is_symlink():
                    temporary_value = temporary_path.lstat()
                    if not stat.S_ISREG(temporary_value.st_mode):
                        raise RuntimeError("managed staging temporary changed type")
                    temporary_path.unlink()
                    _fsync_directory(self._staging)
                signature = self._write_stage(
                    archive,
                    temporary_path=temporary_path,
                    final_path=final_path,
                    expected_sha256=digest,
                    expected_size=size_bytes,
                )
            connection.execute("BEGIN IMMEDIATE")
            try:
                affected = connection.execute(
                    "UPDATE protection_tokens SET state = 'STAGED', "
                    "device = ?, inode = ?, modified_ns = ?, changed_ns = ? "
                    "WHERE token = ? AND state = 'WRITING'",
                    (
                        signature.device,
                        signature.inode,
                        signature.modified_ns,
                        signature.changed_ns,
                        token,
                    ),
                ).rowcount
                if affected != 1:
                    raise RuntimeError("artifact protection journal changed")
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        archive.seek(0)
        return ArtifactStorageEvidence(True)

    def release(
        self,
        storage_key: ArtifactStorageKey,
        expected_artifact_sha256: bytes,
        expected_size_bytes: int,
        protection_token: bytes,
    ) -> ArtifactReleaseStorageEvidence:
        """Persist a terminal tombstone and remove only exact private staging."""

        key = _storage_key(storage_key)
        digest = _digest(expected_artifact_sha256)
        size_bytes = _size(expected_size_bytes)
        token = _token(protection_token)
        self._ensure_layout()
        with self._exclusive_state() as connection:
            row = connection.execute(
                "SELECT storage_codec, storage_path, artifact_sha256, size_bytes, "
                "state, staging_leaf, device, inode, modified_ns, changed_ns "
                "FROM protection_tokens WHERE token = ?",
                (token,),
            ).fetchone()
            facts = (key.codec, _key_text(key), digest, size_bytes)
            if row is None:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.execute(
                        "INSERT INTO protection_tokens "
                        "(token, storage_codec, storage_path, artifact_sha256, "
                        "size_bytes, state, staging_leaf) "
                        "VALUES (?, ?, ?, ?, ?, 'RELEASED', NULL)",
                        (token, *facts),
                    )
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
                return ArtifactReleaseStorageEvidence(True)
            if tuple(row[:4]) != facts:
                raise RuntimeError(
                    "artifact release token refers to another exact artifact"
                )
            if str(row[4]) == "RELEASED":
                return ArtifactReleaseStorageEvidence(True)
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    "UPDATE protection_tokens SET state = 'RELEASED' WHERE token = ?",
                    (token,),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
            self._remove_stage_from_row(row[5:], digest=digest, size=size_bytes)
            temporary_path = self._staging / f".{sha256(token).hexdigest()}.tmp"
            if temporary_path.exists() or temporary_path.is_symlink():
                value = temporary_path.lstat()
                if not stat.S_ISREG(value.st_mode):
                    raise RuntimeError("released staging temporary changed type")
                temporary_path.unlink()
                _fsync_directory(self._staging)
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    "UPDATE protection_tokens SET staging_leaf = NULL, "
                    "device = NULL, inode = NULL, modified_ns = NULL, "
                    "changed_ns = NULL WHERE token = ? AND state = 'RELEASED'",
                    (token,),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return ArtifactReleaseStorageEvidence(True)

    def begin(
        self,
        revision: int,
        receipt_id: bytes,
    ) -> LibraryActivationCheckpoint:
        """Acquire the cross-service gate and resume one exact activation."""

        target = _revision(revision)
        receipt = _receipt(receipt_id)
        self._require_guard()
        self._acquire_publication_lock()
        with self._exclusive_state() as connection:
            state = _journal_state(connection)
            if state.current_revision is not None and target < state.current_revision:
                raise RuntimeError("refusing to activate an older catalog revision")
            if state.pending_revision is not None:
                if state.pending_revision != target:
                    raise RuntimeError("another library activation is unfinished")
                if state.pending_receipt != receipt:
                    raise RuntimeError(
                        "pending activation belongs to another publication receipt"
                    )
                if state.phase in {"SEALED", "ACTIVATING", "READY"}:
                    if self._marker_path.exists() or self._marker_path.is_symlink():
                        self._verify_marker(target, receipt)
                return _checkpoint(state)
            if state.current_revision == target:
                if state.current_receipt != receipt:
                    raise RuntimeError(
                        "installed library belongs to another publication receipt"
                    )
                if self._marker_path.exists() or self._marker_path.is_symlink():
                    self._verify_marker(target, receipt)
                    self._remove_marker()
                return LibraryActivationCheckpoint(
                    target,
                    receipt,
                    LibraryActivationStatus.COMPLETE,
                    None,
                )
            if self._marker_path.exists() or self._marker_path.is_symlink():
                raise RuntimeError("orphaned library ACTIVATING marker")
            connection.execute("BEGIN IMMEDIATE")
            try:
                affected = connection.execute(
                    "UPDATE library_state SET pending_revision = ?, "
                    "pending_receipt_id = ?, phase = 'OPEN', last_cursor = NULL "
                    "WHERE singleton = 1 AND pending_revision IS NULL",
                    (target, receipt),
                ).rowcount
                if affected != 1:
                    raise RuntimeError("library activation state changed")
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return LibraryActivationCheckpoint(
            target,
            receipt,
            LibraryActivationStatus.SPOOL,
            None,
        )

    def activate_page(
        self,
        revision: int,
        items: Sequence[VNextLibraryActivationItem],
    ) -> None:
        """Durably append one bounded page of core-owned stable storage keys."""

        target = _revision(revision)
        page = tuple(items)
        if len(page) > _MAX_PAGE_ITEMS:
            raise ValueError("library activation page exceeds 128 items")
        if any(not isinstance(item, VNextLibraryActivationItem) for item in page):
            raise TypeError("library activation page contains a foreign item")
        if any(
            left.publication_key >= right.publication_key
            for left, right in pairwise(page)
        ):
            raise ValueError("library activation keys must be strictly increasing")
        self._require_publication_lock()
        with self._exclusive_state() as connection:
            state = _journal_state(connection)
            if state.pending_revision != target or state.phase != "OPEN":
                raise RuntimeError("library activation spool is not open")
            if (
                page
                and state.last_cursor is not None
                and page[0].publication_key <= state.last_cursor
            ):
                raise ValueError("library activation page does not advance cursor")
            connection.execute("BEGIN IMMEDIATE")
            try:
                for item in page:
                    item.__post_init__()
                    key = _storage_key(item.storage_key)
                    connection.execute(
                        "INSERT INTO pending_entries "
                        "(activation_revision, publication_key, gid, storage_codec, "
                        "storage_path, "
                        "artifact_sha256, size_bytes, operation_started, activated) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0)",
                        (
                            target,
                            item.publication_key,
                            item.gid,
                            key.codec,
                            _key_text(key),
                            item.artifact_sha256,
                            item.size_bytes,
                        ),
                    )
                if page:
                    connection.execute(
                        "UPDATE library_state SET last_cursor = ? WHERE singleton = 1",
                        (page[-1].publication_key,),
                    )
                connection.commit()
            except sqlite3.IntegrityError as error:
                connection.rollback()
                raise RuntimeError(
                    "library activation contains a duplicate GID or storage key"
                ) from error
            except BaseException:
                connection.rollback()
                raise

    def seal(self, revision: int) -> None:
        target = _revision(revision)
        self._require_publication_lock()
        with self._exclusive_state() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                affected = connection.execute(
                    "UPDATE library_state SET phase = 'SEALED', last_cursor = NULL "
                    "WHERE singleton = 1 AND pending_revision = ? "
                    "AND phase = 'OPEN'",
                    (target,),
                ).rowcount
                if affected != 1:
                    raise RuntimeError("library activation spool cannot be sealed")
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def reconcile_page(
        self,
        revision: int,
        receipt_id: bytes,
        *,
        limit: int,
    ) -> LibraryActivationCheckpoint:
        """Advance at most ``limit`` crash-safe current-path operations."""

        target = _revision(revision)
        receipt = _receipt(receipt_id)
        if type(limit) is not int or not 1 <= limit <= _MAX_PAGE_ITEMS:
            raise ValueError("library reconcile limit must be from 1 through 128")
        self._require_publication_lock()
        with self._exclusive_state() as connection:
            state = _journal_state(connection)
            if (
                state.pending_revision != target
                or state.pending_receipt != receipt
                or state.phase not in {"SEALED", "ACTIVATING", "READY"}
            ):
                raise RuntimeError("library activation is not ready to reconcile")
            if state.phase == "READY":
                self._verify_marker(target, receipt)
                return LibraryActivationCheckpoint(
                    target,
                    receipt,
                    LibraryActivationStatus.READY,
                    None,
                )
            if state.phase == "SEALED":
                self._write_marker(target, receipt)
                connection.execute("BEGIN IMMEDIATE")
                try:
                    affected = connection.execute(
                        "UPDATE library_state SET phase = 'ACTIVATING', "
                        "last_cursor = NULL "
                        "WHERE singleton = 1 AND pending_revision = ? "
                        "AND phase = 'SEALED'",
                        (target,),
                    ).rowcount
                    if affected != 1:
                        raise RuntimeError("library activation state changed")
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
            else:
                self._verify_marker(target, receipt)

            install_cursor = self._activate_pending(
                connection,
                revision=target,
                limit=limit,
            )
            if install_cursor is not None:
                return self._record_reconcile_cursor(
                    connection,
                    revision=target,
                    receipt=receipt,
                    cursor=install_cursor,
                )

            removal_rows = connection.execute(
                "SELECT EXISTS(SELECT 1 FROM pending_removals "
                "WHERE activation_revision = ?)",
                (target,),
            ).fetchone()
            if removal_rows not in {(0,), (1,)}:
                raise RuntimeError("library removal journal is corrupt")
            if removal_rows == (0,):
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.execute(
                        "INSERT INTO pending_removals "
                        "(activation_revision, storage_path, artifact_sha256, "
                        "size_bytes, device, inode, modified_ns, changed_ns, "
                        "operation_started) "
                        "SELECT ?, c.storage_path, c.artifact_sha256, c.size_bytes, "
                        "c.device, c.inode, c.modified_ns, c.changed_ns, 0 "
                        "FROM current_entries AS c WHERE NOT EXISTS ("
                        "SELECT 1 FROM pending_entries AS p "
                        "WHERE p.activation_revision = ? "
                        "AND p.storage_path = c.storage_path) "
                        "ORDER BY c.storage_path LIMIT ?",
                        (target, target, limit),
                    )
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
            removal_cursor = self._remove_stale(
                connection,
                revision=target,
                limit=limit,
            )
            if removal_cursor is not None:
                return self._record_reconcile_cursor(
                    connection,
                    revision=target,
                    receipt=receipt,
                    cursor=removal_cursor,
                )

            remaining = connection.execute(
                "SELECT EXISTS(SELECT 1 FROM pending_entries "
                "WHERE activation_revision = ? AND activated = 0), "
                "EXISTS(SELECT 1 FROM pending_removals "
                "WHERE activation_revision = ?), "
                "EXISTS(SELECT 1 FROM current_entries AS c WHERE NOT EXISTS ("
                "SELECT 1 FROM pending_entries AS p "
                "WHERE p.activation_revision = ? "
                "AND p.storage_path = c.storage_path))",
                (target, target, target),
            ).fetchone()
            if remaining != (0, 0, 0):
                raise RuntimeError("library activation bounded page made no progress")
            connection.execute("BEGIN IMMEDIATE")
            try:
                affected = connection.execute(
                    "UPDATE library_state SET phase = 'READY', last_cursor = NULL "
                    "WHERE singleton = 1 AND pending_revision = ? "
                    "AND phase = 'ACTIVATING'",
                    (target,),
                ).rowcount
                if affected != 1:
                    raise RuntimeError("library activation state changed before READY")
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
            return LibraryActivationCheckpoint(
                target,
                receipt,
                LibraryActivationStatus.READY,
                None,
            )

    @staticmethod
    def _record_reconcile_cursor(
        connection: sqlite3.Connection,
        *,
        revision: int,
        receipt: bytes,
        cursor: bytes,
    ) -> LibraryActivationCheckpoint:
        connection.execute("BEGIN IMMEDIATE")
        try:
            affected = connection.execute(
                "UPDATE library_state SET last_cursor = ? WHERE singleton = 1 "
                "AND pending_revision = ? AND pending_receipt_id = ? "
                "AND phase = 'ACTIVATING'",
                (cursor, revision, receipt),
            ).rowcount
            if affected != 1:
                raise RuntimeError("library reconcile cursor lost its activation")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return LibraryActivationCheckpoint(
            revision,
            receipt,
            LibraryActivationStatus.RECONCILE,
            cursor,
        )

    def complete(self, revision: int, receipt_id: bytes) -> None:
        """Acknowledge the reader-head CAS, then durably remove maintenance."""

        target = _revision(revision)
        receipt = _receipt(receipt_id)
        self._require_publication_lock()
        with self._exclusive_state() as connection:
            state = _journal_state(connection)
            if state.pending_revision is None and state.current_revision == target:
                if state.current_receipt != receipt:
                    raise RuntimeError("completed library receipt differs")
                if self._marker_path.exists() or self._marker_path.is_symlink():
                    self._verify_marker(target, receipt)
                    self._remove_marker()
                return
            if (
                state.pending_revision != target
                or state.pending_receipt != receipt
                or state.phase != "READY"
            ):
                raise RuntimeError("library activation is not READY for completion")
            self._verify_marker(target, receipt)
            connection.execute("BEGIN IMMEDIATE")
            try:
                affected = connection.execute(
                    "UPDATE library_state SET current_revision = ?, "
                    "current_receipt_id = ?, pending_revision = NULL, "
                    "pending_receipt_id = NULL, phase = 'IDLE', last_cursor = NULL "
                    "WHERE singleton = 1 AND pending_revision = ? "
                    "AND pending_receipt_id = ? AND phase = 'READY'",
                    (target, receipt, target, receipt),
                ).rowcount
                if affected != 1:
                    raise RuntimeError("library activation changed before completion")
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
            self._remove_marker()

    def maintain_cleanup(self) -> LibraryMaintenanceOutcome:
        """Remove one bounded page of terminal private staging bytes."""

        self._ensure_layout()
        with self._exclusive_state() as connection:
            state = _journal_state(connection)
            if state.pending_revision is not None:
                return LibraryMaintenanceOutcome.BLOCKED
            rows = connection.execute(
                "SELECT token, artifact_sha256, size_bytes, staging_leaf, device, "
                "inode, modified_ns, changed_ns FROM protection_tokens "
                "WHERE state = 'RELEASED' AND staging_leaf IS NOT NULL "
                "ORDER BY token LIMIT ?",
                (_MAX_CLEANUP_ITEMS,),
            ).fetchall()
            for row in rows:
                self._remove_stage_from_row(
                    row[3:],
                    digest=bytes(row[1]),
                    size=int(row[2]),
                )
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.execute(
                        "UPDATE protection_tokens SET staging_leaf = NULL, "
                        "device = NULL, inode = NULL, modified_ns = NULL, "
                        "changed_ns = NULL WHERE token = ? AND state = 'RELEASED'",
                        (bytes(row[0]),),
                    )
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
            remaining = connection.execute(
                "SELECT EXISTS(SELECT 1 FROM protection_tokens "
                "WHERE state = 'RELEASED' AND staging_leaf IS NOT NULL)"
            ).fetchone()
            if remaining not in {(0,), (1,)}:
                raise RuntimeError("library cleanup state is corrupt")
            if rows:
                return LibraryMaintenanceOutcome.PROGRESSED
            if remaining == (1,):
                return LibraryMaintenanceOutcome.BLOCKED
            completed_rows = connection.execute(
                "SELECT activation_revision, publication_key "
                "FROM pending_entries ORDER BY activation_revision, publication_key "
                "LIMIT ?",
                (_MAX_JOURNAL_CLEANUP_ITEMS,),
            ).fetchall()
            if completed_rows:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.executemany(
                        "DELETE FROM pending_entries "
                        "WHERE activation_revision = ? AND publication_key = ?",
                        completed_rows,
                    )
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
                return LibraryMaintenanceOutcome.PROGRESSED
            return LibraryMaintenanceOutcome.DONE

    def _activate_pending(
        self,
        connection: sqlite3.Connection,
        *,
        revision: int,
        limit: int,
    ) -> bytes | None:
        rows = connection.execute(
            "SELECT p.storage_codec, p.storage_path, p.artifact_sha256, "
            "p.size_bytes, p.operation_started, p.activated, p.device, p.inode, "
            "p.modified_ns, p.changed_ns, c.storage_path, c.artifact_sha256, "
            "c.size_bytes, c.device, c.inode, c.modified_ns, c.changed_ns "
            "FROM pending_entries AS p LEFT JOIN current_entries AS c "
            "ON c.storage_path = p.storage_path "
            "WHERE p.activation_revision = ? AND p.activated = 0 "
            "ORDER BY p.publication_key LIMIT ?",
            (revision, limit),
        ).fetchall()
        last_cursor: bytes | None = None
        for row in rows:
            key = _key_from_row(str(row[0]), str(row[1]))
            digest = bytes(row[2])
            size_bytes = int(row[3])
            target = self._target(key)
            if bool(row[5]):
                signature = self._verify_current(
                    key,
                    expected_sha256=digest,
                    expected_size=size_bytes,
                    label="activated library artifact",
                )
                if signature != _storage_signature(row[6:10], size=size_bytes):
                    raise RuntimeError(f"activated library path changed: {target}")
                continue
            current_exists = row[10] is not None
            current_digest = bytes(row[11]) if current_exists else None
            current_size = int(row[12]) if current_exists else None
            current_signature = (
                _storage_signature(row[13:17], size=int(row[12]))
                if current_exists
                else None
            )
            target_value = self._current_lstat(key)
            if (
                current_exists
                and current_digest == digest
                and current_size == size_bytes
            ):
                if (
                    target_value is None
                    or _Signature.from_stat(target_value) != current_signature
                ):
                    raise RuntimeError(f"unchanged library path changed: {target}")
                assert current_signature is not None
                signature = current_signature
            else:
                started = bool(row[4])
                if started and target_value is not None:
                    try:
                        signature = self._verify_current(
                            key,
                            expected_sha256=digest,
                            expected_size=size_bytes,
                            label="recoverable activated artifact",
                        )
                    except RuntimeError:
                        signature = self._install_staged(
                            connection,
                            key=key,
                            digest=digest,
                            size=size_bytes,
                            current_signature=current_signature,
                        )
                else:
                    if current_exists:
                        if (
                            target_value is None
                            or _Signature.from_stat(target_value) != current_signature
                        ):
                            raise RuntimeError(
                                f"managed library path changed before replace: {target}"
                            )
                    elif target_value is not None:
                        raise RuntimeError(f"unknown library path appeared: {target}")
                    connection.execute("BEGIN IMMEDIATE")
                    try:
                        connection.execute(
                            "UPDATE pending_entries SET operation_started = 1 "
                            "WHERE activation_revision = ? AND storage_path = ? "
                            "AND operation_started = 0",
                            (revision, _key_text(key)),
                        )
                        connection.commit()
                    except BaseException:
                        connection.rollback()
                        raise
                    signature = self._install_staged(
                        connection,
                        key=key,
                        digest=digest,
                        size=size_bytes,
                        current_signature=current_signature,
                    )
            self._retire_staged_candidates(
                connection,
                key=key,
                digest=digest,
                size=size_bytes,
            )
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    "INSERT INTO current_entries "
                    "(storage_path, gid, artifact_sha256, size_bytes, device, inode, "
                    "modified_ns, changed_ns) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(storage_path) DO UPDATE SET gid = excluded.gid, "
                    "artifact_sha256 = excluded.artifact_sha256, "
                    "size_bytes = excluded.size_bytes, device = excluded.device, "
                    "inode = excluded.inode, modified_ns = excluded.modified_ns, "
                    "changed_ns = excluded.changed_ns",
                    (
                        _key_text(key),
                        _gid_from_key(key),
                        digest,
                        size_bytes,
                        signature.device,
                        signature.inode,
                        signature.modified_ns,
                        signature.changed_ns,
                    ),
                )
                connection.execute(
                    "UPDATE pending_entries SET activated = 1, device = ?, "
                    "inode = ?, modified_ns = ?, changed_ns = ? "
                    "WHERE activation_revision = ? AND storage_path = ? "
                    "AND activated = 0",
                    (
                        signature.device,
                        signature.inode,
                        signature.modified_ns,
                        signature.changed_ns,
                        revision,
                        _key_text(key),
                    ),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
            last_cursor = _reconcile_cursor(
                b"install",
                str(row[1]).encode("ascii"),
            )
        return last_cursor

    def _install_staged(
        self,
        connection: sqlite3.Connection,
        *,
        key: ArtifactStorageKey,
        digest: bytes,
        size: int,
        current_signature: _Signature | None,
    ) -> _Signature:
        target = self._target(key)
        current_value = self._current_lstat(key)
        if current_value is not None:
            if (
                current_signature is None
                or _Signature.from_stat(current_value) != current_signature
            ):
                raise RuntimeError(
                    f"library target changed during activation: {target}"
                )
        elif current_signature is not None:
            raise RuntimeError(f"managed library target disappeared: {target}")
        stage, expected_stage_signature = self._require_staged_candidate(
            connection,
            key=key,
            digest=digest,
            size=size,
        )
        with (
            _open_directory_chain(
                self._current,
                key.segments[:-1],
                create=True,
            ) as (current_root_descriptor, parent_descriptor),
            _open_directory(self._staging) as staging_descriptor,
        ):
            visible_current = _lstat_at(parent_descriptor, key.segments[-1])
            if current_signature is None:
                if visible_current is not None:
                    raise RuntimeError(
                        f"unknown library target appeared: {self._target(key)}"
                    )
            elif (
                visible_current is None
                or _Signature.from_stat(visible_current) != current_signature
            ):
                raise RuntimeError(
                    f"managed library target changed: {self._target(key)}"
                )
            stage_signature = _verify_regular_at(
                staging_descriptor,
                stage.name,
                expected_sha256=digest,
                expected_size=size,
                label="staged library artifact",
            )
            if stage_signature != expected_stage_signature:
                raise RuntimeError("staged library path changed before activation")
            os.replace(
                stage.name,
                key.segments[-1],
                src_dir_fd=staging_descriptor,
                dst_dir_fd=parent_descriptor,
            )
            os.fsync(staging_descriptor)
            os.fsync(parent_descriptor)
            installed = _verify_regular_at(
                parent_descriptor,
                key.segments[-1],
                expected_sha256=digest,
                expected_size=size,
                label="newly activated library artifact",
            )
            _require_chain_identity(
                self._current,
                key.segments[:-1],
                current_root_descriptor,
                parent_descriptor,
            )
            return installed

    def _retire_staged_candidates(
        self,
        connection: sqlite3.Connection,
        *,
        key: ArtifactStorageKey,
        digest: bytes,
        size: int,
    ) -> None:
        row = connection.execute(
            "SELECT token, staging_leaf, device, inode, modified_ns, changed_ns "
            "FROM protection_tokens WHERE storage_codec = ? AND storage_path = ? "
            "AND artifact_sha256 = ? AND size_bytes = ? "
            "AND state = 'STAGED' LIMIT 1",
            (key.codec, _key_text(key), digest, size),
        ).fetchone()
        if row is None:
            return
        if row[1] is not None:
            stage = self._staging / str(row[1])
            if stage.exists() or stage.is_symlink():
                signature = _verify_regular_file(
                    stage,
                    expected_sha256=digest,
                    expected_size=size,
                    label="redundant managed staging artifact",
                )
                if signature != _storage_signature(row[2:], size=size):
                    raise RuntimeError("managed staging artifact changed identity")
                stage.unlink()
                _fsync_directory(self._staging)
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute(
                "UPDATE protection_tokens SET state = 'INSTALLED', "
                "staging_leaf = NULL, device = NULL, inode = NULL, "
                "modified_ns = NULL, changed_ns = NULL WHERE token = ? "
                "AND state = 'STAGED'",
                (bytes(row[0]),),
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    def _remove_stale(
        self,
        connection: sqlite3.Connection,
        *,
        revision: int,
        limit: int,
    ) -> bytes | None:
        rows = connection.execute(
            "SELECT storage_path, artifact_sha256, size_bytes, device, inode, "
            "modified_ns, changed_ns, operation_started FROM pending_removals "
            "WHERE activation_revision = ? ORDER BY storage_path LIMIT ?",
            (revision, limit),
        ).fetchall()
        last_cursor: bytes | None = None
        for row in rows:
            key = _key_from_path(str(row[0]))
            digest = bytes(row[1])
            size_bytes = int(row[2])
            expected_signature = _Signature.from_row(
                (row[3], row[4], row[2], row[5], row[6])
            )
            target = self._target(key)
            quarantine = self._quarantine / _quarantine_leaf(str(row[0]), digest)
            target_value = self._current_lstat(key)
            quarantine_value = _safe_lstat(quarantine)
            if not bool(row[7]):
                if (
                    target_value is None
                    or _Signature.from_stat(target_value) != expected_signature
                    or quarantine_value is not None
                ):
                    raise RuntimeError(f"stale library path changed: {target}")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.execute(
                        "UPDATE pending_removals SET operation_started = 1 "
                        "WHERE activation_revision = ? AND storage_path = ? "
                        "AND operation_started = 0",
                        (revision, str(row[0])),
                    )
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
            if target_value is not None:
                if _Signature.from_stat(target_value) != expected_signature:
                    raise RuntimeError(f"stale library path changed: {target}")
                if quarantine_value is not None:
                    raise RuntimeError("stale quarantine destination is occupied")
                self._quarantine_current(
                    key,
                    quarantine.name,
                    expected_signature=expected_signature,
                )
            quarantine_value = _safe_lstat(quarantine)
            if quarantine_value is not None:
                verified = _verify_regular_file(
                    quarantine,
                    expected_sha256=digest,
                    expected_size=size_bytes,
                    label="quarantined stale library artifact",
                )
                # Moving the authorized inode into quarantine changes ctime on
                # POSIX filesystems.  Device, inode, size, mtime, and the
                # independently verified digest still bind this name to the
                # exact managed artifact across a lost rename response.
                if not _same_content_identity(verified, expected_signature):
                    raise RuntimeError("stale quarantine changed artifact identity")
                quarantine.unlink()
                _fsync_directory(self._quarantine)
            elif not bool(row[7]):
                raise RuntimeError("stale library path vanished before authorization")
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    "DELETE FROM pending_removals WHERE activation_revision = ? "
                    "AND storage_path = ?",
                    (revision, str(row[0])),
                )
                connection.execute(
                    "DELETE FROM current_entries WHERE storage_path = ?",
                    (str(row[0]),),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
            last_cursor = _reconcile_cursor(
                b"remove",
                str(row[0]).encode("ascii"),
            )
        return last_cursor

    def _require_staged_candidate(
        self,
        connection: sqlite3.Connection,
        *,
        key: ArtifactStorageKey,
        digest: bytes,
        size: int,
    ) -> tuple[Path, _Signature]:
        row = connection.execute(
            "SELECT staging_leaf, device, inode, modified_ns, changed_ns "
            "FROM protection_tokens WHERE storage_codec = ? AND storage_path = ? "
            "AND artifact_sha256 = ? AND size_bytes = ? AND state = 'STAGED' "
            "ORDER BY token LIMIT 1",
            (key.codec, _key_text(key), digest, size),
        ).fetchone()
        if row is None:
            raise RuntimeError(
                f"library activation lacks staged bytes for {_key_text(key)}"
            )
        leaf = str(row[0])
        if _STAGE_LEAF.fullmatch(leaf) is None:
            raise RuntimeError("library staging journal contains an unsafe path")
        path = self._staging / leaf
        signature = _verify_regular_file(
            path,
            expected_sha256=digest,
            expected_size=size,
            label="staged library artifact",
        )
        expected_signature = _storage_signature(row[1:], size=size)
        if signature != expected_signature:
            raise RuntimeError("staged library artifact changed identity")
        return path, expected_signature

    def _verify_stage_row(
        self,
        row: Sequence[object],
        *,
        digest: bytes,
        size: int,
    ) -> None:
        leaf = str(row[0])
        if _STAGE_LEAF.fullmatch(leaf) is None:
            raise RuntimeError("artifact staging journal contains an unsafe path")
        signature = _verify_regular_file(
            self._staging / leaf,
            expected_sha256=digest,
            expected_size=size,
            label="staged artifact",
        )
        if signature != _storage_signature(row[1:], size=size):
            raise RuntimeError("staged artifact changed identity")

    def _remove_stage_from_row(
        self,
        row: Sequence[object],
        *,
        digest: bytes,
        size: int,
    ) -> None:
        if row[0] is None:
            return
        leaf = str(row[0])
        if _STAGE_LEAF.fullmatch(leaf) is None:
            raise RuntimeError("artifact release journal contains an unsafe path")
        path = self._staging / leaf
        if not path.exists() and not path.is_symlink():
            return
        signature = _verify_regular_file(
            path,
            expected_sha256=digest,
            expected_size=size,
            label="released staged artifact",
        )
        signature_values = tuple(row[1:])
        if all(value is None for value in signature_values):
            expected_signature = None
        elif any(value is None for value in signature_values):
            raise RuntimeError("staged artifact journal signature is incomplete")
        else:
            expected_signature = _storage_signature(signature_values, size=size)
        if expected_signature is not None and signature != expected_signature:
            raise RuntimeError("refusing to delete changed staged artifact")
        path.unlink()
        _fsync_directory(self._staging)

    def _write_stage(
        self,
        archive: BinaryIO,
        *,
        temporary_path: Path,
        final_path: Path,
        expected_sha256: bytes,
        expected_size: int,
    ) -> _Signature:
        if _TEMPORARY_LEAF.fullmatch(temporary_path.name) is None:
            raise RuntimeError("unsafe staging temporary name")
        descriptor = os.open(
            temporary_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            _PUBLIC_FILE_MODE,
        )
        os.fchmod(descriptor, _PUBLIC_FILE_MODE)
        digest = sha256()
        size_bytes = 0
        archive.seek(0)
        try:
            with os.fdopen(descriptor, "wb", closefd=True) as destination:
                while part := archive.read(_COPY_BUFFER_BYTES):
                    if not isinstance(part, bytes):
                        raise TypeError("artifact archive must yield bytes")
                    digest.update(part)
                    size_bytes += len(part)
                    destination.write(part)
                destination.flush()
                os.fsync(destination.fileno())
            if digest.digest() != expected_sha256 or size_bytes != expected_size:
                raise RuntimeError("rendered artifact differs from core preparation")
            os.replace(temporary_path, final_path)
            _fsync_directory(self._staging)
            return _verify_regular_file(
                final_path,
                expected_sha256=expected_sha256,
                expected_size=expected_size,
                label="new staged artifact",
            )
        finally:
            archive.seek(0)
            if temporary_path.exists() or temporary_path.is_symlink():
                value = temporary_path.lstat()
                if not stat.S_ISREG(value.st_mode):
                    raise RuntimeError("staging temporary changed type")
                temporary_path.unlink()
                _fsync_directory(self._staging)

    def _target(self, key: ArtifactStorageKey) -> Path:
        return self._current.joinpath(*key.segments)

    def _current_lstat(self, key: ArtifactStorageKey) -> os.stat_result | None:
        try:
            with _open_directory_chain(
                self._current,
                key.segments[:-1],
                create=False,
            ) as (root_descriptor, parent_descriptor):
                value = _lstat_at(parent_descriptor, key.segments[-1])
                _require_chain_identity(
                    self._current,
                    key.segments[:-1],
                    root_descriptor,
                    parent_descriptor,
                )
                return value
        except FileNotFoundError:
            return None

    def _verify_current(
        self,
        key: ArtifactStorageKey,
        *,
        expected_sha256: bytes,
        expected_size: int,
        label: str,
    ) -> _Signature:
        try:
            with _open_directory_chain(
                self._current,
                key.segments[:-1],
                create=False,
            ) as (root_descriptor, parent_descriptor):
                signature = _verify_regular_at(
                    parent_descriptor,
                    key.segments[-1],
                    expected_sha256=expected_sha256,
                    expected_size=expected_size,
                    label=label,
                )
                _require_chain_identity(
                    self._current,
                    key.segments[:-1],
                    root_descriptor,
                    parent_descriptor,
                )
                return signature
        except FileNotFoundError as error:
            raise RuntimeError(
                f"{label} is unavailable: {self._target(key)}"
            ) from error

    def _quarantine_current(
        self,
        key: ArtifactStorageKey,
        quarantine_leaf: str,
        *,
        expected_signature: _Signature,
    ) -> None:
        with (
            _open_directory_chain(
                self._current,
                key.segments[:-1],
                create=False,
            ) as (root_descriptor, parent_descriptor),
            _open_directory(self._quarantine) as quarantine_descriptor,
        ):
            current = _lstat_at(parent_descriptor, key.segments[-1])
            if current is None or _Signature.from_stat(current) != expected_signature:
                raise RuntimeError(f"stale library path changed: {self._target(key)}")
            if _lstat_at(quarantine_descriptor, quarantine_leaf) is not None:
                raise RuntimeError("stale quarantine destination is occupied")
            os.rename(
                key.segments[-1],
                quarantine_leaf,
                src_dir_fd=parent_descriptor,
                dst_dir_fd=quarantine_descriptor,
            )
            os.fsync(parent_descriptor)
            os.fsync(quarantine_descriptor)
            _require_chain_identity(
                self._current,
                key.segments[:-1],
                root_descriptor,
                parent_descriptor,
            )

    def _write_marker(self, revision: int, receipt: bytes) -> None:
        payload = _marker_payload(revision, receipt)
        if self._marker_path.exists() or self._marker_path.is_symlink():
            self._verify_marker(revision, receipt)
            return
        temporary = (
            self._coordination / f".{_ACTIVATING_MARKER_NAME}-{secrets.token_hex(16)}"
        )
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            _PUBLIC_FILE_MODE,
        )
        try:
            os.fchmod(descriptor, _PUBLIC_FILE_MODE)
            with os.fdopen(descriptor, "wb", closefd=True) as destination:
                destination.write(payload)
                destination.flush()
                os.fsync(destination.fileno())
            os.replace(temporary, self._marker_path)
            _fsync_directory(self._coordination)
        finally:
            if temporary.exists() or temporary.is_symlink():
                temporary.unlink()
                _fsync_directory(self._coordination)

    def _verify_marker(self, revision: int, receipt: bytes) -> None:
        value = self._marker_path.lstat()
        if not stat.S_ISREG(value.st_mode):
            raise RuntimeError("library ACTIVATING marker is not a regular file")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        with os.fdopen(os.open(self._marker_path, flags), "rb", closefd=True) as source:
            payload = source.read(1024)
            if source.read(1):
                raise RuntimeError("library ACTIVATING marker is oversized")
        if payload != _marker_payload(revision, receipt):
            raise RuntimeError("library ACTIVATING marker has foreign contents")

    def _remove_marker(self) -> None:
        value = self._marker_path.lstat()
        if not stat.S_ISREG(value.st_mode):
            raise RuntimeError("library ACTIVATING marker changed type")
        self._marker_path.unlink()
        _fsync_directory(self._coordination)

    def _ensure_layout(self) -> None:
        self._root.mkdir(mode=_PUBLIC_DIRECTORY_MODE, parents=True, exist_ok=True)
        _require_directory(self._root, label="library root", private=False)
        _ensure_managed_directory(self._current, _PUBLIC_DIRECTORY_MODE)
        _ensure_managed_directory(self._state, _PRIVATE_MODE)
        for path in (self._staging, self._quarantine, self._journal, self._locks):
            _ensure_managed_directory(path, _PRIVATE_MODE)
        _ensure_managed_directory(self._coordination, _PUBLIC_DIRECTORY_MODE)
        _require_directory(self._current, label="current library", private=False)
        _require_directory(self._state, label="library state", private=True)
        for path in (self._staging, self._quarantine, self._journal, self._locks):
            _require_directory(path, label=f"private library {path.name}", private=True)
        _require_directory(
            self._coordination,
            label="library coordination",
            private=False,
        )
        devices = {
            self._current.stat().st_dev,
            self._staging.stat().st_dev,
            self._quarantine.stat().st_dev,
        }
        if len(devices) != 1:
            raise RuntimeError(
                "current, staging, and quarantine must share a filesystem"
            )
        descriptor = os.open(
            self._publication_lock_path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
            _PUBLIC_FILE_MODE,
        )
        try:
            os.fchmod(descriptor, _PUBLIC_FILE_MODE)
        finally:
            os.close(descriptor)
        value = self._publication_lock_path.lstat()
        if not stat.S_ISREG(value.st_mode):
            raise RuntimeError("publication lock is not a regular file")
        with self._connection():
            pass

    @contextmanager
    def _exclusive_state(self) -> Iterator[sqlite3.Connection]:
        with self._process_lock:
            descriptor = os.open(
                self._state_lock_path,
                os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                with self._connection() as connection:
                    yield connection
            finally:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                finally:
                    os.close(descriptor)

    def _acquire_publication_lock(self) -> None:
        if self._publication_descriptor is not None:
            return
        descriptor = os.open(
            self._publication_lock_path,
            os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
        except BaseException:
            os.close(descriptor)
            raise
        self._publication_descriptor = descriptor

    def _release_publication_lock(self) -> None:
        descriptor = self._publication_descriptor
        if descriptor is None:
            return
        self._publication_descriptor = None
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    def _require_guard(self) -> None:
        if self._guard_depth != 1:
            raise RuntimeError("library activation requires publication_guard")

    def _require_publication_lock(self) -> None:
        self._require_guard()
        if self._publication_descriptor is None:
            raise RuntimeError("library activation lacks its exclusive lock")

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        if self._database_path.exists() or self._database_path.is_symlink():
            value = self._database_path.lstat()
            if not stat.S_ISREG(value.st_mode):
                raise RuntimeError("library activation database path is unsafe")
        connection = sqlite3.connect(self._database_path)
        try:
            journal_mode = connection.execute("PRAGMA journal_mode = DELETE").fetchone()
            if (
                journal_mode is None
                or len(journal_mode) != 1
                or str(journal_mode[0]).casefold() != "delete"
            ):
                raise RuntimeError(
                    "library activation journal could not enforce DELETE mode"
                )
            connection.execute("PRAGMA synchronous = FULL")
            if connection.execute("PRAGMA synchronous").fetchone() != (2,):
                raise RuntimeError(
                    "library activation journal could not enforce FULL sync"
                )
            connection.executescript(_SCHEMA)
            if connection.execute(
                "SELECT format_version FROM library_state WHERE singleton = 1"
            ).fetchone() != (1,):
                raise RuntimeError("unsupported library activation journal format")
            connection.commit()
            yield connection
        finally:
            connection.close()


def _storage_key(value: ArtifactStorageKey) -> ArtifactStorageKey:
    if type(value) is not ArtifactStorageKey:
        raise TypeError("storage_key must be ArtifactStorageKey")
    value.__post_init__()
    if value.codec != "gid-sha256-12-v1" or len(value.segments) != 4:
        raise ValueError("storage key is not the canonical GID hash-v1 codec")
    leaf = value.segments[-1]
    if not leaf.startswith("h2h-") or not leaf.endswith(".cbz"):
        raise ValueError("storage key has an invalid CBZ leaf")
    try:
        gid = int(leaf[4:-4])
    except ValueError as error:
        raise ValueError("storage key has an invalid GID leaf") from error
    if artifact_storage_key(gid) != value:
        raise ValueError("storage key disagrees with the core GID resolver")
    return value


def _key_text(key: ArtifactStorageKey) -> str:
    return "/".join(key.segments)


def _gid_from_key(key: ArtifactStorageKey) -> int:
    return int(key.segments[-1][4:-4])


def _key_from_row(codec: str, path: str) -> ArtifactStorageKey:
    return _storage_key(ArtifactStorageKey(codec, tuple(path.split("/"))))


def _key_from_path(path: str) -> ArtifactStorageKey:
    return _key_from_row("gid-sha256-12-v1", path)


def _digest(value: bytes) -> bytes:
    if type(value) is not bytes or len(value) != 32:
        raise ValueError("artifact SHA-256 must contain exactly 32 bytes")
    return value


def _size(value: int) -> int:
    if type(value) is not int or not 0 <= value < 1 << 63:
        raise ValueError("artifact size must be a non-negative signed int63")
    return value


def _token(value: bytes) -> bytes:
    if type(value) is not bytes or len(value) != 184:
        raise ValueError("artifact protection token must contain exactly 184 bytes")
    return value


def _revision(value: int) -> int:
    if type(value) is not int or not 1 <= value < 1 << 63:
        raise ValueError("catalog revision must be a positive signed int63")
    return value


def _receipt(value: bytes) -> bytes:
    if type(value) is not bytes or len(value) != 16:
        raise ValueError("publication receipt must contain exactly 16 bytes")
    return value


def _checkpoint(state: _JournalState) -> LibraryActivationCheckpoint:
    if state.pending_revision is None or state.pending_receipt is None:
        raise RuntimeError("pending library activation state is incomplete")
    statuses = {
        "OPEN": LibraryActivationStatus.SPOOL,
        "SEALED": LibraryActivationStatus.RECONCILE,
        "ACTIVATING": LibraryActivationStatus.RECONCILE,
        "READY": LibraryActivationStatus.READY,
    }
    try:
        status = statuses[state.phase]
    except KeyError as error:
        raise RuntimeError("pending library activation phase is corrupt") from error
    cursor = (
        state.last_cursor
        if status
        in {
            LibraryActivationStatus.SPOOL,
            LibraryActivationStatus.RECONCILE,
        }
        else None
    )
    return LibraryActivationCheckpoint(
        state.pending_revision,
        state.pending_receipt,
        status,
        cursor,
    )


def _journal_state(connection: sqlite3.Connection) -> _JournalState:
    row = connection.execute(
        "SELECT current_revision, current_receipt_id, pending_revision, "
        "pending_receipt_id, phase, last_cursor FROM library_state "
        "WHERE singleton = 1"
    ).fetchone()
    if row is None or len(row) != 6:
        raise RuntimeError("library activation state is corrupt")
    result = _JournalState(
        int(row[0]) if row[0] is not None else None,
        bytes(row[1]) if row[1] is not None else None,
        int(row[2]) if row[2] is not None else None,
        bytes(row[3]) if row[3] is not None else None,
        str(row[4]),
        bytes(row[5]) if row[5] is not None else None,
    )
    if (result.current_revision is None) != (result.current_receipt is None):
        raise RuntimeError("current library receipt pair is corrupt")
    if (result.pending_revision is None) != (result.pending_receipt is None):
        raise RuntimeError("pending library receipt pair is corrupt")
    if (result.phase == "IDLE") != (result.pending_revision is None):
        raise RuntimeError("pending library phase is corrupt")
    if result.last_cursor is not None and len(result.last_cursor) != 32:
        raise RuntimeError("library activation cursor is corrupt")
    return result


def _storage_signature(row: Sequence[object], *, size: int) -> _Signature:
    """Build a signature where size is stored in the artifact fact column."""

    if len(row) != 4:
        raise RuntimeError("library journal storage signature has an invalid shape")
    return _Signature.from_row((row[0], row[1], size, row[2], row[3]))


def _same_content_identity(left: _Signature, right: _Signature) -> bool:
    return (
        left.device,
        left.inode,
        left.size_bytes,
        left.modified_ns,
    ) == (
        right.device,
        right.inode,
        right.size_bytes,
        right.modified_ns,
    )


def _marker_payload(revision: int, receipt: bytes) -> bytes:
    return (
        json.dumps(
            {
                "format": _MARKER_FORMAT,
                "receipt_id": receipt.hex(),
                "revision": revision,
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
        + b"\n"
    )


def _reconcile_cursor(kind: bytes, key: bytes) -> bytes:
    digest = sha256()
    digest.update(b"h2hdb-library-reconcile-cursor-v1\0")
    digest.update(kind)
    digest.update(b"\0")
    digest.update(key)
    return digest.digest()


def _require_directory(path: Path, *, label: str, private: bool) -> None:
    value = path.lstat()
    if not stat.S_ISDIR(value.st_mode) or stat.S_ISLNK(value.st_mode):
        raise RuntimeError(f"{label} is not a safe directory: {path}")
    permissions = stat.S_IMODE(value.st_mode)
    if private and permissions & 0o077:
        raise RuntimeError(f"{label} must not grant group/world access: {path}")
    if not private and permissions & 0o022:
        raise RuntimeError(f"{label} must not grant group/world write: {path}")


def _ensure_managed_directory(path: Path, mode: int) -> None:
    path.mkdir(mode=mode, exist_ok=True)
    with _open_directory(path) as descriptor:
        os.fchmod(descriptor, mode)


@contextmanager
def _open_directory(path: Path) -> Iterator[int]:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        visible = path.lstat()
        if (
            not stat.S_ISDIR(opened.st_mode)
            or stat.S_ISLNK(visible.st_mode)
            or (opened.st_dev, opened.st_ino) != (visible.st_dev, visible.st_ino)
        ):
            raise RuntimeError(f"directory identity is unsafe: {path}")
        yield descriptor
    finally:
        os.close(descriptor)


@contextmanager
def _open_directory_chain(
    root: Path,
    components: Sequence[str],
    *,
    create: bool,
) -> Iterator[tuple[int, int]]:
    descriptors: list[int] = []
    try:
        flags = (
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        )
        root_descriptor = os.open(root, flags)
        descriptors.append(root_descriptor)
        current_descriptor = root_descriptor
        for component in components:
            if component in {"", ".", ".."} or "/" in component:
                raise RuntimeError("library storage key contains an unsafe segment")
            if create:
                try:
                    os.mkdir(
                        component,
                        _PUBLIC_DIRECTORY_MODE,
                        dir_fd=current_descriptor,
                    )
                    os.fsync(current_descriptor)
                except FileExistsError:
                    pass
            try:
                next_descriptor = os.open(
                    component,
                    flags,
                    dir_fd=current_descriptor,
                )
            except FileNotFoundError:
                raise
            except OSError as error:
                raise RuntimeError(
                    f"library shard is not a safe directory: {component}"
                ) from error
            opened = os.fstat(next_descriptor)
            if not stat.S_ISDIR(opened.st_mode):
                os.close(next_descriptor)
                raise RuntimeError(f"library shard is not a directory: {component}")
            if create:
                os.fchmod(next_descriptor, _PUBLIC_DIRECTORY_MODE)
            descriptors.append(next_descriptor)
            current_descriptor = next_descriptor
        yield root_descriptor, current_descriptor
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _require_chain_identity(
    root: Path,
    components: Sequence[str],
    expected_root_descriptor: int,
    expected_parent_descriptor: int,
) -> None:
    expected_root = os.fstat(expected_root_descriptor)
    expected_parent = os.fstat(expected_parent_descriptor)
    with _open_directory_chain(root, components, create=False) as (
        visible_root_descriptor,
        visible_parent_descriptor,
    ):
        visible_root = os.fstat(visible_root_descriptor)
        visible_parent = os.fstat(visible_parent_descriptor)
        if (expected_root.st_dev, expected_root.st_ino) != (
            visible_root.st_dev,
            visible_root.st_ino,
        ) or (expected_parent.st_dev, expected_parent.st_ino) != (
            visible_parent.st_dev,
            visible_parent.st_ino,
        ):
            raise RuntimeError("library shard chain changed identity")


def _lstat_at(descriptor: int, leaf: str) -> os.stat_result | None:
    try:
        value = os.stat(leaf, dir_fd=descriptor, follow_symlinks=False)
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(value.st_mode) or stat.S_ISLNK(value.st_mode):
        raise RuntimeError(f"library leaf is not a regular file: {leaf}")
    return value


def _verify_regular_at(
    descriptor: int,
    leaf: str,
    *,
    expected_sha256: bytes,
    expected_size: int,
    label: str,
) -> _Signature:
    before = _lstat_at(descriptor, leaf)
    if before is None or before.st_size != expected_size:
        raise RuntimeError(f"{label} has an unexpected type or size: {leaf}")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    digest = sha256()
    with os.fdopen(
        os.open(leaf, flags, dir_fd=descriptor), "rb", closefd=True
    ) as source:
        opened = os.fstat(source.fileno())
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise RuntimeError(f"{label} changed while opening: {leaf}")
        while part := source.read(_COPY_BUFFER_BYTES):
            digest.update(part)
        after = os.fstat(source.fileno())
    if _Signature.from_stat(opened) != _Signature.from_stat(after):
        raise RuntimeError(f"{label} changed while hashing: {leaf}")
    if digest.digest() != expected_sha256:
        raise RuntimeError(f"{label} has an unexpected digest: {leaf}")
    return _Signature.from_stat(after)


def _safe_lstat(path: Path) -> os.stat_result | None:
    try:
        value = path.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(value.st_mode) or stat.S_ISLNK(value.st_mode):
        raise RuntimeError(f"library path is not a regular file: {path}")
    return value


def _verify_regular_file(
    path: Path,
    *,
    expected_sha256: bytes,
    expected_size: int,
    label: str,
) -> _Signature:
    before = _safe_lstat(path)
    if before is None or before.st_size != expected_size:
        raise RuntimeError(f"{label} has an unexpected type or size: {path}")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    digest = sha256()
    with os.fdopen(os.open(path, flags), "rb", closefd=True) as source:
        opened = os.fstat(source.fileno())
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise RuntimeError(f"{label} changed while opening: {path}")
        while part := source.read(_COPY_BUFFER_BYTES):
            digest.update(part)
        after = os.fstat(source.fileno())
    if _Signature.from_stat(opened) != _Signature.from_stat(after):
        raise RuntimeError(f"{label} changed while hashing: {path}")
    if digest.digest() != expected_sha256:
        raise RuntimeError(f"{label} has an unexpected digest: {path}")
    return _Signature.from_stat(after)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _quarantine_leaf(storage_path: str, digest: bytes) -> str:
    framed = sha256()
    framed.update(b"h2hdb-library-quarantine-v1\0")
    framed.update(storage_path.encode("ascii"))
    framed.update(b"\0")
    framed.update(digest)
    return f"{framed.hexdigest()}.cbz"


_SCHEMA = """
CREATE TABLE IF NOT EXISTS library_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    format_version INTEGER NOT NULL CHECK (format_version = 1),
    current_revision INTEGER NULL,
    current_receipt_id BLOB NULL,
    pending_revision INTEGER NULL,
    pending_receipt_id BLOB NULL,
    phase TEXT NOT NULL CHECK (
        phase IN ('IDLE', 'OPEN', 'SEALED', 'ACTIVATING', 'READY')
    ),
    last_cursor BLOB NULL
);
INSERT OR IGNORE INTO library_state
    (singleton, format_version, phase) VALUES (1, 1, 'IDLE');
CREATE TABLE IF NOT EXISTS protection_tokens (
    token BLOB PRIMARY KEY CHECK (length(token) = 184),
    storage_codec TEXT NOT NULL,
    storage_path TEXT NOT NULL,
    artifact_sha256 BLOB NOT NULL CHECK (length(artifact_sha256) = 32),
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    state TEXT NOT NULL CHECK (
        state IN ('WRITING', 'STAGED', 'INSTALLED', 'RELEASED')
    ),
    staging_leaf TEXT NULL,
    device BLOB NULL,
    inode BLOB NULL,
    modified_ns INTEGER NULL,
    changed_ns INTEGER NULL
);
CREATE INDEX IF NOT EXISTS protection_artifact_idx ON protection_tokens (
    storage_codec, storage_path, artifact_sha256, size_bytes, state
);
CREATE UNIQUE INDEX IF NOT EXISTS protection_one_active_stage_idx
    ON protection_tokens(storage_path)
    WHERE state IN ('WRITING', 'STAGED');
CREATE TABLE IF NOT EXISTS current_entries (
    storage_path TEXT PRIMARY KEY,
    gid INTEGER NOT NULL UNIQUE,
    artifact_sha256 BLOB NOT NULL CHECK (length(artifact_sha256) = 32),
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    device BLOB NOT NULL CHECK (length(device) = 8),
    inode BLOB NOT NULL CHECK (length(inode) = 8),
    modified_ns INTEGER NOT NULL,
    changed_ns INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS pending_entries (
    activation_revision INTEGER NOT NULL,
    publication_key BLOB NOT NULL CHECK (length(publication_key) = 32),
    gid INTEGER NOT NULL,
    storage_codec TEXT NOT NULL,
    storage_path TEXT NOT NULL,
    artifact_sha256 BLOB NOT NULL CHECK (length(artifact_sha256) = 32),
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    operation_started INTEGER NOT NULL CHECK (operation_started IN (0, 1)),
    activated INTEGER NOT NULL CHECK (activated IN (0, 1)),
    device BLOB NULL,
    inode BLOB NULL,
    modified_ns INTEGER NULL,
    changed_ns INTEGER NULL,
    PRIMARY KEY (activation_revision, publication_key),
    UNIQUE (activation_revision, gid),
    UNIQUE (activation_revision, storage_path)
);
CREATE INDEX IF NOT EXISTS pending_entries_activation_idx
    ON pending_entries(activation_revision, activated, publication_key);
CREATE TABLE IF NOT EXISTS pending_removals (
    activation_revision INTEGER NOT NULL,
    storage_path TEXT NOT NULL,
    artifact_sha256 BLOB NOT NULL CHECK (length(artifact_sha256) = 32),
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    device BLOB NOT NULL CHECK (length(device) = 8),
    inode BLOB NOT NULL CHECK (length(inode) = 8),
    modified_ns INTEGER NOT NULL,
    changed_ns INTEGER NOT NULL,
    operation_started INTEGER NOT NULL CHECK (operation_started IN (0, 1)),
    PRIMARY KEY (activation_revision, storage_path)
);
"""
