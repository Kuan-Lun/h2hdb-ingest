"""Managed-filesystem adapter for vNext canonical CBZ artifacts."""

from __future__ import annotations

__all__ = [
    "ARTIFACT_ADAPTER_ID",
    "ArtifactProducerIdentity",
    "ManagedFilesystemArtifactAdapter",
]

import fcntl
import os
import re
import sqlite3
import stat
import sys
import tempfile
import zlib
from collections.abc import Iterator
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
    """Protect immutable artifacts and retain monotone release tombstones.

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
        _target, locator = self._artifact_path(locator_components)
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
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return ArtifactReleaseStorageEvidence(True)

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
