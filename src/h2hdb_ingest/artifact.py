"""Managed-filesystem adapter for vNext canonical CBZ artifacts."""

from __future__ import annotations

__all__ = [
    "ARTIFACT_ADAPTER_ID",
    "ArtifactProducerIdentity",
    "ManagedFilesystemArtifactAdapter",
]

import ctypes
import errno
import fcntl
import os
import re
import sqlite3
import stat
import sys
import tempfile
import zlib
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from threading import RLock
from typing import BinaryIO

from h2hdb import (
    ArtifactReleaseStorageEvidence,
    ArtifactStorageEvidence,
    ArtifactTransformKind,
    artifact_producer_fingerprint_sha256,
)
from PIL import Image, ImageFile, ImageOps, features
from PIL import __version__ as PILLOW_VERSION

ARTIFACT_ADAPTER_ID = b"managed-filesystem"
ARTIFACT_WRITER_ID = b"h2hdb-ingest-canonical-cbz-v1"
_COPY_BUFFER_BYTES = 4 * 1024 * 1024
_STATE_DATABASE_NAME = ".h2hdb-vnext-artifacts.sqlite3"
_STATE_LOCK_NAME = ".h2hdb-vnext-artifacts.lock"
_ARTIFACT_QUARANTINE_NAME = ".h2hdb-vnext-artifact-quarantine"
_QUARANTINE_PAYLOAD_NAME = "payload"
_PRIVATE_DIRECTORY_MODE = 0o700
_LOCATOR_LEAF = re.compile(r"[0-9a-f]{64}\.cbz")

Image.MAX_IMAGE_PIXELS = None
ImageFile.LOAD_TRUNCATED_IMAGES = True


@dataclass(frozen=True, slots=True)
class ArtifactProducerIdentity:
    """Exact producer fields shared by policy registration and the adapter."""

    writer_id: bytes
    python_abi: bytes
    pillow_build: bytes
    libjpeg_build: bytes
    zlib_build: bytes

    @classmethod
    def current(cls) -> ArtifactProducerIdentity:
        cache_tag = sys.implementation.cache_tag or (
            f"cpython-{sys.version_info.major}.{sys.version_info.minor}"
        )
        jpeg = features.version_codec("jpg") or "unknown"
        return cls(
            writer_id=ARTIFACT_WRITER_ID,
            python_abi=cache_tag.encode("ascii", errors="strict"),
            pillow_build=PILLOW_VERSION.encode("ascii", errors="strict"),
            libjpeg_build=jpeg.encode("ascii", errors="strict"),
            zlib_build=zlib.ZLIB_RUNTIME_VERSION.encode("ascii", errors="strict"),
        )

    @property
    def fingerprint_sha256(self) -> bytes:
        return artifact_producer_fingerprint_sha256(
            self.writer_id,
            self.python_abi,
            self.pillow_build,
            self.libjpeg_build,
            self.zlib_build,
        )


class ManagedFilesystemArtifactAdapter:
    """Protect immutable artifacts and prune released non-current state.

    The SQLite file is ingest-owned coordination state, not part of the h2hdb
    schema.  A process-wide lock and an advisory filesystem lock serialize the
    token decision with artifact materialization across every publisher that
    shares the same store.
    """

    adapter_id = ARTIFACT_ADAPTER_ID

    def __init__(
        self,
        artifact_store_path: Path,
        *,
        max_image_short_side: int,
        producer: ArtifactProducerIdentity | None = None,
    ) -> None:
        if max_image_short_side < 1:
            raise ValueError("max_image_short_side must be positive")
        self._root = artifact_store_path.resolve(strict=False)
        self._max_image_short_side = max_image_short_side
        self._producer = producer or ArtifactProducerIdentity.current()
        self.producer_fingerprint_sha256 = self._producer.fingerprint_sha256
        self._state_path = self._root / _STATE_DATABASE_NAME
        self._lock_path = self._root / _STATE_LOCK_NAME
        self._process_lock = RLock()

    @property
    def producer(self) -> ArtifactProducerIdentity:
        return self._producer

    def render_member(
        self,
        source: BinaryIO,
        transform_kind: ArtifactTransformKind,
        destination: BinaryIO,
    ) -> None:
        """Render the two registered normalized-image transformations."""

        if transform_kind is ArtifactTransformKind.RAW_COPY:
            raise ValueError("core owns RAW_COPY artifact members")
        if transform_kind not in {
            ArtifactTransformKind.GIF_NORMALIZE,
            ArtifactTransformKind.JPEG_NORMALIZE,
        }:
            raise ValueError(f"unsupported artifact transform: {transform_kind!r}")
        with Image.open(source) as opened:
            image = ImageOps.exif_transpose(opened)
            if image.height >= image.width:
                scale = self._max_image_short_side / image.width
                bounds = (
                    self._max_image_short_side,
                    max(1, int(image.height * scale)),
                )
            else:
                scale = self._max_image_short_side / image.height
                bounds = (
                    max(1, int(image.width * scale)),
                    self._max_image_short_side,
                )
            image.thumbnail(bounds, Image.Resampling.LANCZOS)
            if transform_kind is ArtifactTransformKind.GIF_NORMALIZE:
                image.save(destination, format="GIF")
                return
            if image.has_transparency_data:
                foreground = image.convert("RGBA")
                background = Image.new("RGBA", foreground.size, "white")
                image = Image.alpha_composite(background, foreground).convert("RGB")
            elif image.mode not in {"RGB", "L"}:
                image = image.convert("RGB")
            image.save(destination, format="JPEG", quality=90, optimize=True)

    def protect(
        self,
        archive: BinaryIO,
        locator_components: tuple[str, ...],
        protection_token: bytes,
    ) -> ArtifactStorageEvidence:
        """Idempotently materialize bytes unless release already won."""

        token = self._validate_token(protection_token)
        target, locator = self._artifact_path(locator_components)
        expected_digest = bytes.fromhex(target.stem)
        with self._exclusive_state() as connection:
            current = connection.execute(
                "SELECT locator, state FROM protection_tokens WHERE token = ?",
                (token,),
            ).fetchone()
            if current is not None:
                if str(current[0]) != locator:
                    raise RuntimeError(
                        "artifact protection token was reused for another locator"
                    )
                if str(current[1]) == "RELEASED":
                    return ArtifactStorageEvidence(False)
            size_bytes = self._materialize_archive(
                archive,
                target=target,
                expected_digest=expected_digest,
            )
            connection.execute("BEGIN IMMEDIATE")
            try:
                current = connection.execute(
                    "SELECT locator, state FROM protection_tokens WHERE token = ?",
                    (token,),
                ).fetchone()
                if current is not None and str(current[0]) != locator:
                    raise RuntimeError(
                        "artifact protection token was reused for another locator"
                    )
                if current is not None and str(current[1]) == "RELEASED":
                    connection.rollback()
                    return ArtifactStorageEvidence(False)
                connection.execute(
                    "INSERT OR IGNORE INTO artifacts(locator, sha256, size_bytes) "
                    "VALUES (?, ?, ?)",
                    (locator, expected_digest, size_bytes),
                )
                stored = connection.execute(
                    "SELECT sha256, size_bytes FROM artifacts WHERE locator = ?",
                    (locator,),
                ).fetchone()
                if stored != (expected_digest, size_bytes):
                    raise RuntimeError(
                        "artifact state disagrees with content-addressed bytes"
                    )
                connection.execute(
                    "INSERT OR IGNORE INTO protection_tokens(token, locator, state) "
                    "VALUES (?, ?, 'PROTECTED')",
                    (token, locator),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return ArtifactStorageEvidence(True)

    def release(
        self,
        locator_components: tuple[str, ...],
        protection_token: bytes,
    ) -> ArtifactReleaseStorageEvidence:
        """Persist a terminal tombstone; immutable artifact bytes are retained."""

        token = self._validate_token(protection_token)
        target, locator = self._artifact_path(locator_components)
        artifact_sha256 = bytes.fromhex(target.stem)
        with self._exclusive_state() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                current = connection.execute(
                    "SELECT locator FROM protection_tokens WHERE token = ?",
                    (token,),
                ).fetchone()
                if current is not None and str(current[0]) != locator:
                    raise RuntimeError(
                        "artifact release token was reused for another locator"
                    )
                connection.execute(
                    "INSERT INTO protection_tokens(token, locator, state) "
                    "VALUES (?, ?, 'RELEASED') "
                    "ON CONFLICT(token) DO UPDATE SET state = 'RELEASED'",
                    (token, locator),
                )
                connection.execute(
                    "INSERT OR IGNORE INTO artifact_cleanup_candidates "
                    "(artifact_sha256) VALUES (?)",
                    (artifact_sha256,),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return ArtifactReleaseStorageEvidence(True)

    def _prune_released_artifact(self, artifact_sha256: bytes) -> bool:
        """Prune one exact released artifact while retaining token tombstones.

        The artifact database owns the durable cleanup queue, while callers hold
        the publication lock. Hashing, unlinking, and directory fsync happen
        before the short artifact-state transaction.
        Returning ``False`` means that a live protection token still fences the
        bytes and the queue candidate must be retried later.
        """

        with self._exclusive_state() as connection:
            return self._prune_released_artifact_in_state(
                connection,
                artifact_sha256,
            )

    def _enqueue_cleanup_candidates(
        self,
        artifact_sha256s: Sequence[bytes],
    ) -> None:
        """Durably receive one bounded projection cleanup outbox page."""

        candidates = tuple(artifact_sha256s)
        if len(candidates) > 128:
            raise ValueError("artifact cleanup enqueue page exceeds 128 items")
        for digest in candidates:
            self._validate_cleanup_digest(digest)
        if not candidates:
            return
        with self._exclusive_state() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.executemany(
                    "INSERT OR IGNORE INTO artifact_cleanup_candidates "
                    "(artifact_sha256) VALUES (?)",
                    ((digest,) for digest in candidates),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _prune_cleanup_candidates(
        self,
        *,
        is_retained: Callable[[bytes], bool],
        limit: int,
    ) -> int:
        """Attempt one circular, durable, fixed-size cleanup page."""

        if not callable(is_retained):
            raise TypeError("artifact cleanup retention predicate must be callable")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 128
        ):
            raise ValueError("artifact cleanup limit must be in 1..128")

        with self._exclusive_state() as connection:
            state = connection.execute(
                "SELECT after_sha256 FROM artifact_cleanup_state " "WHERE singleton = 1"
            ).fetchone()
            if state is None or len(state) != 1:
                raise RuntimeError("artifact cleanup cursor state is corrupt")
            cursor = bytes(state[0]) if state[0] is not None else None
            if cursor is not None:
                self._validate_cleanup_digest(cursor)

            rows = connection.execute(
                "SELECT artifact_sha256 FROM artifact_cleanup_candidates "
                "WHERE (? IS NULL OR artifact_sha256 > ?) "
                "ORDER BY artifact_sha256 LIMIT ?",
                (cursor, cursor, limit),
            ).fetchall()
            if cursor is not None and len(rows) < limit:
                rows.extend(
                    connection.execute(
                        "SELECT artifact_sha256 FROM artifact_cleanup_candidates "
                        "WHERE artifact_sha256 <= ? "
                        "ORDER BY artifact_sha256 LIMIT ?",
                        (cursor, limit - len(rows)),
                    ).fetchall()
                )

            candidates = tuple(bytes(row[0]) for row in rows)
            for digest in candidates:
                self._validate_cleanup_digest(digest)
            acknowledged: list[bytes] = []
            for digest in candidates:
                retained = is_retained(digest)
                if type(retained) is not bool:
                    raise TypeError(
                        "artifact cleanup retention predicate must return bool"
                    )
                if retained or self._prune_released_artifact_in_state(
                    connection,
                    digest,
                ):
                    acknowledged.append(digest)

            next_cursor = candidates[-1] if len(candidates) == limit else None
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.executemany(
                    "DELETE FROM artifact_cleanup_candidates "
                    "WHERE artifact_sha256 = ?",
                    ((digest,) for digest in acknowledged),
                )
                affected = connection.execute(
                    "UPDATE artifact_cleanup_state SET after_sha256 = ? "
                    "WHERE singleton = 1",
                    (next_cursor,),
                ).rowcount
                if affected != 1:
                    raise RuntimeError("artifact cleanup cursor state changed")
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
            return len(acknowledged)

    def _has_cleanup_candidates(self) -> bool:
        """Return whether the durable artifact cleanup queue is nonempty."""

        with self._exclusive_state() as connection:
            row = connection.execute(
                "SELECT EXISTS (SELECT 1 FROM artifact_cleanup_candidates)"
            ).fetchone()
            if row is None or row[0] not in {0, 1}:
                raise RuntimeError("artifact cleanup candidate state is corrupt")
            return bool(row[0])

    def _prune_released_artifact_in_state(
        self,
        connection: sqlite3.Connection,
        artifact_sha256: bytes,
    ) -> bool:
        digest = self._validate_cleanup_digest(artifact_sha256)
        digest_hex = digest.hex()
        target, locator = self._artifact_path(
            ("sha256", digest_hex[:2], f"{digest_hex}.cbz")
        )
        stored = connection.execute(
            "SELECT sha256, size_bytes FROM artifacts WHERE locator = ?",
            (locator,),
        ).fetchone()
        token_states = self._token_states(connection, locator)
        if "PROTECTED" in token_states:
            return False
        if stored is None:
            # The artifact may already have been removed before a crash.
            # Unknown filesystem bytes are never inferred to be managed.
            return True
        if not token_states:
            raise RuntimeError(
                "artifact cleanup found state without a protection token"
            )

        stored_digest = bytes(stored[0])
        stored_size = int(stored[1])
        if stored_digest != digest:
            raise RuntimeError("artifact cleanup state has a noncanonical locator")
        self._unlink_verified_artifact(
            target,
            expected_digest=digest,
            expected_size=stored_size,
        )

        connection.execute("BEGIN IMMEDIATE")
        try:
            current = connection.execute(
                "SELECT sha256, size_bytes FROM artifacts WHERE locator = ?",
                (locator,),
            ).fetchone()
            if current is None:
                connection.commit()
                return True
            if current != (digest, stored_size):
                raise RuntimeError("artifact cleanup state changed")
            current_token_states = self._token_states(connection, locator)
            if "PROTECTED" in current_token_states:
                connection.rollback()
                return False
            if not current_token_states:
                raise RuntimeError(
                    "artifact cleanup found state without a protection token"
                )
            affected = connection.execute(
                "DELETE FROM artifacts "
                "WHERE locator = ? AND sha256 = ? AND size_bytes = ?",
                (locator, digest, stored_size),
            ).rowcount
            if affected != 1:
                raise RuntimeError("artifact cleanup state changed")
            # RELEASED rows are permanent terminal tombstones: deleting one
            # would let a delayed protect with the same token resurrect.
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return True

    @staticmethod
    def _validate_cleanup_digest(value: bytes) -> bytes:
        if type(value) is not bytes or len(value) != 32:
            raise ValueError("artifact cleanup digest must contain exactly 32 bytes")
        return value

    @staticmethod
    def _token_states(
        connection: sqlite3.Connection,
        locator: str,
    ) -> tuple[str, ...]:
        states = tuple(
            str(row[0])
            for row in connection.execute(
                "SELECT state FROM protection_tokens WHERE locator = ?",
                (locator,),
            )
        )
        if any(state not in {"PROTECTED", "RELEASED"} for state in states):
            raise RuntimeError("artifact cleanup found an invalid token state")
        return states

    @contextmanager
    def _exclusive_state(self) -> Iterator[sqlite3.Connection]:
        with self._process_lock:
            self._root.mkdir(mode=0o700, parents=True, exist_ok=True)
            if self._root.is_symlink() or not self._root.is_dir():
                raise RuntimeError(f"artifact store is not a directory: {self._root}")
            descriptor = os.open(
                self._lock_path,
                os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                if self._state_path.exists():
                    value = self._state_path.lstat()
                    if stat.S_ISLNK(value.st_mode) or not stat.S_ISREG(value.st_mode):
                        raise RuntimeError(
                            f"artifact state is not a regular file: {self._state_path}"
                        )
                connection = sqlite3.connect(self._state_path)
                try:
                    self._initialize_state(connection)
                    yield connection
                finally:
                    connection.close()
            finally:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                finally:
                    os.close(descriptor)

    @staticmethod
    def _initialize_state(connection: sqlite3.Connection) -> None:
        connection.execute("PRAGMA journal_mode = DELETE")
        connection.execute("PRAGMA synchronous = FULL")
        connection.executescript("""
            CREATE TABLE IF NOT EXISTS state_meta (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                format_version INTEGER NOT NULL CHECK (format_version = 1)
            );
            INSERT OR IGNORE INTO state_meta(singleton, format_version)
            VALUES (1, 1);
            CREATE TABLE IF NOT EXISTS artifacts (
                locator TEXT PRIMARY KEY,
                sha256 BLOB NOT NULL CHECK (length(sha256) = 32),
                size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0)
            );
            CREATE TABLE IF NOT EXISTS protection_tokens (
                token BLOB PRIMARY KEY CHECK (length(token) = 184),
                locator TEXT NOT NULL,
                state TEXT NOT NULL CHECK (state IN ('PROTECTED', 'RELEASED'))
            );
            CREATE INDEX IF NOT EXISTS protection_tokens_locator_idx
                ON protection_tokens(locator);
            CREATE TABLE IF NOT EXISTS artifact_cleanup_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                after_sha256 BLOB NULL
                    CHECK (after_sha256 IS NULL OR length(after_sha256) = 32)
            );
            INSERT OR IGNORE INTO artifact_cleanup_state
                (singleton, after_sha256) VALUES (1, NULL);
            CREATE TABLE IF NOT EXISTS artifact_cleanup_candidates (
                artifact_sha256 BLOB PRIMARY KEY
                    CHECK (length(artifact_sha256) = 32)
            );
            """)
        if connection.execute(
            "SELECT format_version FROM state_meta WHERE singleton = 1"
        ).fetchone() != (1,):
            raise RuntimeError("unsupported managed artifact state format")
        connection.commit()

    def _artifact_path(
        self,
        locator_components: tuple[str, ...],
    ) -> tuple[Path, str]:
        if (
            type(locator_components) is not tuple
            or len(locator_components) != 3
            or locator_components[0] != "sha256"
            or not re.fullmatch(r"[0-9a-f]{2}", locator_components[1])
            or _LOCATOR_LEAF.fullmatch(locator_components[2]) is None
            or not locator_components[2].startswith(locator_components[1])
        ):
            raise ValueError("artifact locator is not canonical managed-filesystem v1")
        directory = self._root / locator_components[0] / locator_components[1]
        target = directory / locator_components[2]
        locator = "/".join(locator_components)
        return target, locator

    def _materialize_archive(
        self,
        archive: BinaryIO,
        *,
        target: Path,
        expected_digest: bytes,
    ) -> int:
        self._ensure_artifact_directory(target.parent)
        archive.seek(0)
        temporary_descriptor, temporary_name = tempfile.mkstemp(
            prefix=".h2hdb-artifact-",
            suffix=".tmp",
            dir=target.parent,
        )
        temporary = Path(temporary_name)
        digest = sha256()
        size_bytes = 0
        try:
            os.fchmod(temporary_descriptor, 0o600)
            with os.fdopen(temporary_descriptor, "wb", closefd=True) as output:
                while part := archive.read(_COPY_BUFFER_BYTES):
                    digest.update(part)
                    size_bytes += len(part)
                    output.write(part)
                output.flush()
                os.fsync(output.fileno())
            if digest.digest() != expected_digest:
                raise RuntimeError("artifact archive does not match its locator")
            if target.exists() or target.is_symlink():
                existing_size = self._verify_artifact(target, expected_digest)
                if existing_size != size_bytes:
                    raise RuntimeError(
                        "artifact archive size disagrees with existing immutable bytes"
                    )
                return existing_size
            try:
                os.link(temporary, target, follow_symlinks=False)
            except FileExistsError:
                size_bytes = self._verify_artifact(target, expected_digest)
            self._fsync_directory(target.parent)
            return size_bytes
        finally:
            temporary.unlink(missing_ok=True)
            archive.seek(0)

    @staticmethod
    def _ensure_artifact_directory(directory: Path) -> None:
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        current = directory
        for _index in range(2):
            value = current.lstat()
            if stat.S_ISLNK(value.st_mode) or not stat.S_ISDIR(value.st_mode):
                raise RuntimeError(f"artifact locator parent is unsafe: {current}")
            current = current.parent

    @staticmethod
    def _verify_artifact(target: Path, expected_digest: bytes) -> int:
        value = target.lstat()
        if not stat.S_ISREG(value.st_mode):
            raise RuntimeError(f"artifact target is not a regular file: {target}")
        digest = sha256()
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        with os.fdopen(os.open(target, flags), "rb", closefd=True) as source:
            while part := source.read(_COPY_BUFFER_BYTES):
                digest.update(part)
        if digest.digest() != expected_digest:
            raise RuntimeError(f"content-addressed artifact is corrupt: {target}")
        return value.st_size

    def _unlink_verified_artifact(
        self,
        target: Path,
        *,
        expected_digest: bytes,
        expected_size: int,
    ) -> None:
        """Verify and unlink a managed leaf through one no-follow parent fd.

        Resolving the leaf and unlinking it relative to the same directory
        descriptor prevents an intermediate locator directory from being
        replaced with a symlink between validation and deletion.  The leaf is
        also opened without following symlinks and its exact inode signature is
        rechecked after hashing and immediately before ``unlinkat``.
        """

        try:
            relative = target.relative_to(self._root)
        except ValueError as error:
            raise RuntimeError(
                f"artifact target escapes its store: {target}"
            ) from error
        components = relative.parts
        if len(components) != 3:
            raise RuntimeError(f"artifact target has an unsafe locator: {target}")
        with _open_directory_chain(
            self._root,
            components[:-1],
            label=f"artifact locator parent is unsafe: {target.parent}",
        ) as parent_descriptor:
            with _open_private_quarantine(
                self._root,
                namespace_name=_ARTIFACT_QUARANTINE_NAME,
                object_name=f"artifact-{expected_digest.hex()}",
                label=f"artifact quarantine is unsafe: {target}",
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
                            "artifact cleanup found competing filesystem state: "
                            f"{target}"
                        )
                else:
                    if leaf_value is None:
                        # Crash replay after unlink but before the state transaction.
                        quarantine.remove_empty()
                        return
                    if not stat.S_ISREG(leaf_value.st_mode):
                        raise RuntimeError(
                            f"artifact target is not a regular file: {target}"
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
                            "artifact quarantine destination changed during capture: "
                            f"{target}"
                        ) from error
                    os.fsync(parent_descriptor)
                    os.fsync(quarantine.object_descriptor)
                quarantine.validate({_QUARANTINE_PAYLOAD_NAME})
                _require_directory_chain_identity(
                    self._root,
                    components[:-1],
                    parent_descriptor,
                    label=f"artifact locator parent changed: {target.parent}",
                )
                try:
                    _verify_regular_at(
                        quarantine.object_descriptor,
                        _QUARANTINE_PAYLOAD_NAME,
                        expected_sha256=expected_digest,
                        expected_size=expected_size,
                    )
                except RuntimeError as error:
                    # The quarantined object is intentionally retained for operator
                    # inspection; acknowledging it could discard unknown bytes.
                    raise RuntimeError(
                        f"artifact quarantine failed verification: {target}"
                    ) from error
                if _lstat_at(parent_descriptor, leaf) is not None:
                    raise RuntimeError(
                        "artifact cleanup found competing filesystem state: "
                        f"{target}"
                    )
                quarantine.validate({_QUARANTINE_PAYLOAD_NAME})
                try:
                    _verify_regular_at(
                        quarantine.object_descriptor,
                        _QUARANTINE_PAYLOAD_NAME,
                        expected_sha256=expected_digest,
                        expected_size=expected_size,
                    )
                except RuntimeError as error:
                    raise RuntimeError(
                        f"artifact quarantine changed before unlink: {target}"
                    ) from error
                # The second verification is the last mutable-state boundary.
                # This 0700 object directory is an adapter-only capability and
                # the artifact lock excludes every authorized concurrent writer.
                os.unlink(
                    _QUARANTINE_PAYLOAD_NAME,
                    dir_fd=quarantine.object_descriptor,
                )
                os.fsync(quarantine.object_descriptor)
                quarantine.remove_empty()

    @staticmethod
    def _fsync_directory(directory: Path) -> None:
        descriptor = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    @staticmethod
    def _validate_token(value: bytes) -> bytes:
        token = bytes(value)
        if len(token) != 184:
            raise ValueError("artifact protection token must contain 184 bytes")
        return token


@dataclass(slots=True)
class _PrivateQuarantine:
    root_descriptor: int
    namespace_descriptor: int
    object_descriptor: int
    namespace_name: str
    object_name: str
    label: str
    removed: bool = False

    def validate(self, expected_entries: set[str] | None = None) -> None:
        if self.removed:
            raise RuntimeError(f"{self.label}: quarantine object was removed")
        _validate_private_directory(
            self.root_descriptor,
            self.namespace_name,
            self.namespace_descriptor,
            label=self.label,
        )
        _validate_private_directory(
            self.namespace_descriptor,
            self.object_name,
            self.object_descriptor,
            label=self.label,
        )
        entries = set(os.listdir(self.object_descriptor))
        if expected_entries is not None and entries != expected_entries:
            raise RuntimeError(f"{self.label}: quarantine entries changed")
        if not entries.issubset({_QUARANTINE_PAYLOAD_NAME}):
            raise RuntimeError(f"{self.label}: quarantine contains unknown entries")

    def remove_empty(self) -> None:
        self.validate(set())
        os.rmdir(self.object_name, dir_fd=self.namespace_descriptor)
        os.fsync(self.namespace_descriptor)
        self.removed = True


@contextmanager
def _open_private_quarantine(
    root: Path,
    *,
    namespace_name: str,
    object_name: str,
    label: str,
) -> Iterator[_PrivateQuarantine]:
    """Open one adapter-private, crash-replayable quarantine directory.

    The 0700 namespace is an adapter-owned capability protected by the same
    artifact/publication lock as its caller.  No other writer is authorized to
    create or mutate entries within it; any metadata or entry discrepancy is a
    fail-closed state error.
    """

    if (
        not namespace_name.startswith(".h2hdb-vnext-")
        or "/" in namespace_name
        or not re.fullmatch(r"[a-z]+-[0-9a-f]{64}", object_name)
    ):
        raise RuntimeError(f"{label}: invalid private quarantine name")
    with _open_directory_chain(root, (), label=label) as root_descriptor:
        namespace_descriptor = _open_private_directory(
            root_descriptor,
            namespace_name,
            label=label,
        )
        try:
            object_descriptor = _open_private_directory(
                namespace_descriptor,
                object_name,
                label=label,
            )
            try:
                quarantine = _PrivateQuarantine(
                    root_descriptor,
                    namespace_descriptor,
                    object_descriptor,
                    namespace_name,
                    object_name,
                    label,
                )
                quarantine.validate()
                yield quarantine
            finally:
                os.close(object_descriptor)
        finally:
            os.close(namespace_descriptor)


def _open_private_directory(
    parent_descriptor: int,
    name: str,
    *,
    label: str,
) -> int:
    value = _lstat_at(parent_descriptor, name)
    if value is None:
        try:
            os.mkdir(name, mode=_PRIVATE_DIRECTORY_MODE, dir_fd=parent_descriptor)
        except FileExistsError as error:
            raise RuntimeError(f"{label}: private directory creation raced") from error
        os.fsync(parent_descriptor)
        value = _lstat_at(parent_descriptor, name)
    if value is None:
        raise RuntimeError(f"{label}: private directory is unavailable")
    _validate_private_metadata(value, label=label)
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(name, flags, dir_fd=parent_descriptor)
    except OSError as error:
        raise RuntimeError(f"{label}: private directory is unavailable") from error
    try:
        _validate_private_directory(
            parent_descriptor,
            name,
            descriptor,
            label=label,
        )
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _validate_private_directory(
    parent_descriptor: int,
    name: str,
    opened_descriptor: int,
    *,
    label: str,
) -> None:
    named = _lstat_at(parent_descriptor, name)
    if named is None:
        raise RuntimeError(f"{label}: private directory disappeared")
    opened = os.fstat(opened_descriptor)
    _validate_private_metadata(named, label=label)
    _validate_private_metadata(opened, label=label)
    if (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino):
        raise RuntimeError(f"{label}: private directory identity changed")


def _validate_private_metadata(value: os.stat_result, *, label: str) -> None:
    if (
        not stat.S_ISDIR(value.st_mode)
        or stat.S_IMODE(value.st_mode) != _PRIVATE_DIRECTORY_MODE
        or value.st_uid != os.geteuid()
    ):
        raise RuntimeError(f"{label}: private directory metadata is unsafe")


def _rename_noreplace(
    source: str,
    destination: str,
    *,
    source_descriptor: int,
    destination_descriptor: int,
) -> None:
    """Atomically rename one leaf without replacing an existing destination."""

    if (
        not source
        or not destination
        or "/" in source
        or "/" in destination
        or source in {".", ".."}
        or destination in {".", ".."}
    ):
        raise ValueError("no-replace rename requires safe leaf names")
    library = ctypes.CDLL(None, use_errno=True)
    source_bytes = os.fsencode(source)
    destination_bytes = os.fsencode(destination)
    if sys.platform == "darwin":
        function = getattr(library, "renameatx_np", None)
        flag = 0x00000004  # RENAME_EXCL
    elif sys.platform.startswith("linux"):
        function = getattr(library, "renameat2", None)
        flag = 0x00000001  # RENAME_NOREPLACE
    else:
        function = None
        flag = 0
    if function is None:
        raise RuntimeError("atomic no-replace rename is unavailable")
    function.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    function.restype = ctypes.c_int
    ctypes.set_errno(0)
    result = function(
        source_descriptor,
        source_bytes,
        destination_descriptor,
        destination_bytes,
        flag,
    )
    if result == 0:
        return
    error_number = ctypes.get_errno()
    if error_number == errno.EEXIST:
        raise FileExistsError(
            error_number,
            os.strerror(error_number),
            destination,
        )
    raise OSError(error_number, os.strerror(error_number), source)


@contextmanager
def _open_directory_chain(
    root: Path,
    components: Sequence[str],
    *,
    label: str,
    create_mode: int | None = None,
) -> Iterator[int]:
    """Open one directory chain without ever following a symlink."""

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptors: list[int] = []
    try:
        descriptors.append(os.open(root, flags))
        for component in components:
            created = False
            try:
                descriptor = os.open(component, flags, dir_fd=descriptors[-1])
            except FileNotFoundError:
                if create_mode is None:
                    raise
                try:
                    os.mkdir(component, mode=create_mode, dir_fd=descriptors[-1])
                except FileExistsError:
                    pass
                descriptor = os.open(component, flags, dir_fd=descriptors[-1])
                created = True
            descriptors.append(descriptor)
            if created:
                os.fsync(descriptors[-2])
    except OSError as error:
        for descriptor in reversed(descriptors):
            os.close(descriptor)
        raise RuntimeError(label) from error
    try:
        yield descriptors[-1]
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _stat_identity(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        value.st_mode,
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _require_directory_chain_identity(
    root: Path,
    components: Sequence[str],
    opened_descriptor: int,
    *,
    label: str,
) -> None:
    """Require the root namespace to still name the opened parent directory."""

    opened = os.fstat(opened_descriptor)
    with _open_directory_chain(root, components, label=label) as current_descriptor:
        current = os.fstat(current_descriptor)
    if (
        not stat.S_ISDIR(opened.st_mode)
        or not stat.S_ISDIR(current.st_mode)
        or (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino)
    ):
        raise RuntimeError(label)


def _lstat_at(parent_descriptor: int, name: str) -> os.stat_result | None:
    try:
        return os.stat(
            name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        return None


def _verify_regular_at(
    parent_descriptor: int,
    name: str,
    *,
    expected_sha256: bytes,
    expected_size: int,
) -> os.stat_result:
    initial = _lstat_at(parent_descriptor, name)
    if initial is None or not stat.S_ISREG(initial.st_mode):
        raise RuntimeError("quarantined artifact is not a regular file")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(name, flags, dir_fd=parent_descriptor)
    except OSError as error:
        raise RuntimeError("quarantined artifact is unavailable") from error
    digest = sha256()
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or _stat_identity(opened) != _stat_identity(
            initial
        ):
            raise RuntimeError("quarantined artifact changed before verification")
        with os.fdopen(descriptor, "rb", closefd=False) as source:
            while part := source.read(_COPY_BUFFER_BYTES):
                digest.update(part)
        hashed = os.fstat(descriptor)
        if _stat_identity(hashed) != _stat_identity(opened):
            raise RuntimeError("quarantined artifact changed during verification")
    finally:
        os.close(descriptor)
    current = _lstat_at(parent_descriptor, name)
    if current is None or _stat_identity(current) != _stat_identity(hashed):
        raise RuntimeError("quarantined artifact changed after verification")
    if current.st_size != expected_size or digest.digest() != expected_sha256:
        raise RuntimeError("quarantined artifact content is unexpected")
    return current
