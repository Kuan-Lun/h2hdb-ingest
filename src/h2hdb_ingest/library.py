"""Crash-safe acquisition and artwork storage with atomic activation."""

from __future__ import annotations

__all__ = ["ManagedFilesystemLibraryAdapter"]

import ctypes
import errno
import fcntl
import json
import os
import re
import sqlite3
import stat
import sys
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from itertools import pairwise
from pathlib import Path
from threading import RLock, get_ident
from typing import BinaryIO, cast

from h2hdb import (
    ArtifactArchiveRenderEvidence,
    ArtifactPresentationRenderEvidence,
    ArtifactReleaseStorageEvidence,
    ArtifactRenderedPage,
    ArtifactSourceMember,
    ArtifactStorageEvidence,
    CatalogResourceKind,
    LibraryActivationCheckpoint,
    LibraryActivationStatus,
    StorageObjectDescriptor,
    StorageObjectKey,
    VNextLibraryActivationCursor,
    VNextLibraryActivationItem,
)

from ._library_layout import (
    COORDINATION_DIRECTORY_NAME as _COORDINATION_DIRECTORY_NAME,
)
from ._library_layout import CURRENT_DIRECTORY_NAME as _CURRENT_DIRECTORY_NAME
from ._library_layout import STATE_DIRECTORY_NAME as _STATE_DIRECTORY_NAME
from ._library_layout import validate_precreated_library_layout
from .artifact import (
    ARTIFACT_ADAPTER_ID,
    MAX_PAGE_RENDER_WORKERS,
    ArtifactRenderPolicy,
    artifact_policy_fingerprint_sha256,
    render_archive,
    render_presentation,
)
from .maintenance import LibraryMaintenanceOutcome
from .metrics import IngestMetricSink
from .storage import (
    STORAGE_OBJECT_CODEC,
    storage_key_gid,
    storage_key_resource_kind,
    validate_storage_key,
)
from .storage import acquisition_storage_key as _acquisition_storage_key
from .storage import thumbnail_storage_key as _thumbnail_storage_key

_COPY_BUFFER_BYTES = 4 * 1024 * 1024
_STAGING_DIRECTORY_NAME = "staging"
_QUARANTINE_DIRECTORY_NAME = "quarantine"
_JOURNAL_DIRECTORY_NAME = "journal"
_LOCKS_DIRECTORY_NAME = "locks"
_DATABASE_NAME = "library-activation.sqlite3"
_STATE_LOCK_NAME = "state.lock"
_PUBLICATION_LOCK_NAME = "publication.lock"
_PROTECTION_LOCK_PREFIX = "protection-"
_ACTIVATING_MARKER_NAME = "ACTIVATING"
_MARKER_FORMAT = "h2hdb-library-activation-v2"
_MAX_PAGE_ITEMS = 128
_MAX_CLEANUP_ITEMS = 8
_MAX_JOURNAL_CLEANUP_ITEMS = 128
_PRIVATE_DIRECTORY_CREATION_MODE = 0o700
_PUBLIC_DIRECTORY_CREATION_MODE = 0o755
_PRIVATE_FILE_CREATION_MODE = 0o600
_PUBLIC_FILE_CREATION_MODE = 0o644
_STAGE_LEAF = re.compile(r"[0-9a-f]{64}\.(?:cbz|jpg)")
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


@dataclass(frozen=True, slots=True)
class _StagedAuthority:
    token: bytes
    leaf: str
    signature: _Signature


@dataclass(frozen=True, slots=True)
class _PendingInstall:
    revision: int
    receipt: bytes
    publication_key: bytes
    resource_kind: str
    key: StorageObjectKey
    digest: bytes
    size_bytes: int
    modified_text: str
    fresh_authorization: bool
    staged: _StagedAuthority | None
    current_digest: bytes | None
    current_size: int | None
    current_signature: _Signature | None


class ManagedFilesystemLibraryAdapter:
    """Render, stage, activate, and release presentation-v2 resources.

    Only ``current`` and ``.h2hdb-coordination`` are reader-visible. Candidate
    bytes and the durable activation journal remain below ``.h2hdb-state`` on
    the same filesystem. The adapter alone derives the concrete filesystem
    location and validates every returned opaque key against its v2 codec.
    """

    adapter_id = ARTIFACT_ADAPTER_ID

    def __init__(
        self,
        library_path: Path,
        *,
        source_root: Path,
        render_policy: ArtifactRenderPolicy,
        page_render_workers: int = 1,
        metrics_sink: IngestMetricSink | None = None,
    ) -> None:
        if not isinstance(library_path, Path):
            raise TypeError("library_path must be Path")
        if not isinstance(render_policy, ArtifactRenderPolicy):
            raise TypeError("render_policy must be ArtifactRenderPolicy")
        render_policy.__post_init__()
        if type(page_render_workers) is not int:
            raise TypeError("page_render_workers must be int")
        if not 1 <= page_render_workers <= MAX_PAGE_RENDER_WORKERS:
            raise ValueError(
                f"page_render_workers must be from 1 through {MAX_PAGE_RENDER_WORKERS}"
            )
        if metrics_sink is not None and not callable(metrics_sink):
            raise TypeError("metrics_sink must be callable")
        if not isinstance(source_root, Path):
            raise TypeError("source_root must be Path")
        try:
            resolved_source = source_root.resolve(strict=True)
        except OSError as error:
            raise RuntimeError("source_root cannot be resolved") from error
        if not resolved_source.is_dir():
            raise RuntimeError("source_root must be a directory")
        source_stat = resolved_source.lstat()
        if not stat.S_ISDIR(source_stat.st_mode) or stat.S_ISLNK(source_stat.st_mode):
            raise RuntimeError("source_root must resolve to a real directory")
        self._source_root = resolved_source
        self._source_root_components = tuple(resolved_source.parts[1:])
        self._source_root_identity = (source_stat.st_dev, source_stat.st_ino)
        self._root = library_path.absolute()
        self._current = self._root / _CURRENT_DIRECTORY_NAME
        self._state = self._root / _STATE_DIRECTORY_NAME
        self._staging = self._state / _STAGING_DIRECTORY_NAME
        self._quarantine = self._state / _QUARANTINE_DIRECTORY_NAME
        self._journal = self._state / _JOURNAL_DIRECTORY_NAME
        self._locks = self._state / _LOCKS_DIRECTORY_NAME
        self._coordination = self._root / _COORDINATION_DIRECTORY_NAME
        self._database_path = self._journal / _DATABASE_NAME
        self._state_lock_path = self._locks / _STATE_LOCK_NAME
        self._publication_lock_path = self._coordination / _PUBLICATION_LOCK_NAME
        self._marker_path = self._coordination / _ACTIVATING_MARKER_NAME
        self._render_policy = render_policy
        self._page_render_workers = page_render_workers
        self._metrics_sink = metrics_sink
        self.policy_fingerprint_sha256 = artifact_policy_fingerprint_sha256(
            render_policy
        )
        self._process_lock = RLock()
        self._state_process_lock = RLock()
        self._guard_owner: int | None = None
        self._publication_descriptor: int | None = None

    @property
    def current_path(self) -> Path:
        """The read-only root exposed to OPDS."""

        return self._current

    @property
    def acquisitions_path(self) -> Path:
        """The only current subtree that a Komga deployment must mount."""

        return self._current / "acquisitions"

    @staticmethod
    def storage_key(
        gid: int,
        resource_kind: CatalogResourceKind,
    ) -> StorageObjectKey:
        """Issue the adapter-owned opaque key for one logical resource."""

        if type(resource_kind) is not CatalogResourceKind:
            raise TypeError("resource_kind must be CatalogResourceKind")
        if resource_kind is CatalogResourceKind.ACQUISITION:
            return _acquisition_storage_key(gid)
        if resource_kind is CatalogResourceKind.THUMBNAIL:
            return _thumbnail_storage_key(gid)
        raise ValueError("unsupported catalog resource kind")

    @contextmanager
    def publication_guard(self) -> Iterator[None]:
        """Keep one local publication turn and its activation lifecycle paired."""

        owner = get_ident()
        with self._process_lock:
            if self._guard_owner is not None:
                raise RuntimeError("library publication guard is not reentrant")
            self._guard_owner = owner
        try:
            self._ensure_layout()
            yield
        finally:
            with self._process_lock:
                if self._guard_owner != owner:
                    raise RuntimeError("library publication guard owner changed")
                self._release_publication_lock()
                self._guard_owner = None

    def open_source(
        self,
        *,
        source_root_components: tuple[str, ...],
        gallery_locator_components: tuple[str, ...],
        source_name: bytes,
    ) -> BinaryIO:
        """Open one exact observed source below the configured no-follow root."""

        if source_root_components != self._source_root_components:
            raise RuntimeError("source request names another configured root")
        if type(gallery_locator_components) is not tuple:
            raise TypeError("gallery locator components must be an exact tuple")
        _require_source_leaf(source_name)
        root_flags = (
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        )
        descriptors: list[int] = []
        leaf_descriptor: int | None = None
        try:
            root_descriptor = os.open(self._source_root, root_flags)
            descriptors.append(root_descriptor)
            self._require_source_root_identity(root_descriptor)
            current_descriptor = root_descriptor
            for component in gallery_locator_components:
                _require_source_component(component)
                try:
                    child_descriptor = os.open(
                        component,
                        root_flags,
                        dir_fd=current_descriptor,
                    )
                except OSError as error:
                    raise RuntimeError(
                        "gallery locator is not a safe directory chain"
                    ) from error
                opened = os.fstat(child_descriptor)
                visible = os.stat(
                    component,
                    dir_fd=current_descriptor,
                    follow_symlinks=False,
                )
                if (
                    not stat.S_ISDIR(opened.st_mode)
                    or stat.S_ISLNK(visible.st_mode)
                    or (opened.st_dev, opened.st_ino)
                    != (visible.st_dev, visible.st_ino)
                ):
                    os.close(child_descriptor)
                    raise RuntimeError("gallery locator changed directory identity")
                descriptors.append(child_descriptor)
                current_descriptor = child_descriptor
            try:
                leaf_descriptor = os.open(
                    source_name,
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=current_descriptor,
                )
            except OSError as error:
                raise RuntimeError("source leaf is not safely openable") from error
            opened_leaf = os.fstat(leaf_descriptor)
            visible_leaf = os.stat(
                source_name,
                dir_fd=current_descriptor,
                follow_symlinks=False,
            )
            if (
                not stat.S_ISREG(opened_leaf.st_mode)
                or stat.S_ISLNK(visible_leaf.st_mode)
                or (opened_leaf.st_dev, opened_leaf.st_ino)
                != (visible_leaf.st_dev, visible_leaf.st_ino)
            ):
                raise RuntimeError("source leaf changed identity")
            self._require_source_root_identity(root_descriptor)
            result = os.fdopen(leaf_descriptor, "rb", closefd=True)
            leaf_descriptor = None
            return cast(BinaryIO, result)
        finally:
            if leaf_descriptor is not None:
                os.close(leaf_descriptor)
            for descriptor in reversed(descriptors):
                os.close(descriptor)

    def _require_source_root_identity(self, descriptor: int) -> None:
        opened = os.fstat(descriptor)
        try:
            visible = self._source_root.lstat()
        except OSError as error:
            raise RuntimeError("configured source root became unavailable") from error
        if (
            not stat.S_ISDIR(opened.st_mode)
            or stat.S_ISLNK(visible.st_mode)
            or (opened.st_dev, opened.st_ino) != self._source_root_identity
            or (visible.st_dev, visible.st_ino) != self._source_root_identity
        ):
            raise RuntimeError("configured source root changed identity")

    def render_archive(
        self,
        members: tuple[ArtifactSourceMember, ...],
        destination: BinaryIO,
        *,
        gid: int,
    ) -> ArtifactArchiveRenderEvidence:
        """Own the complete canonical CBZ serialization contract."""

        return render_archive(
            members,
            destination,
            gid=gid,
            policy=self._render_policy,
            page_render_workers=self._page_render_workers,
            metrics_sink=self._metrics_sink,
        )

    def protect(
        self,
        content: BinaryIO,
        storage_key: StorageObjectKey,
        expected_sha256: bytes,
        expected_size_bytes: int,
        modified_at: datetime,
        protection_token: bytes,
    ) -> ArtifactStorageEvidence:
        """Durably stage one exact immutable acquisition or thumbnail object."""

        self._ensure_layout()
        with self._publication_io_guard(exclusive=False):
            with self._protection_io_guard(_token(protection_token)):
                return self._protect(
                    content,
                    storage_key,
                    expected_sha256,
                    expected_size_bytes,
                    modified_at,
                    protection_token,
                )

    def _protect(
        self,
        content: BinaryIO,
        storage_key: StorageObjectKey,
        expected_sha256: bytes,
        expected_size_bytes: int,
        modified_at: datetime,
        protection_token: bytes,
    ) -> ArtifactStorageEvidence:
        """Run one protected staging attempt below the shared publication gate."""

        key = _storage_key(storage_key)
        digest = _digest(expected_sha256)
        size_bytes = _size(expected_size_bytes)
        modified = _modified_at(modified_at)
        modified_text = modified.isoformat(timespec="microseconds")
        storage_object = StorageObjectDescriptor(
            key=key,
            size_bytes=size_bytes,
            sha256=digest.hex(),
            modified_at=modified,
        )
        token = _token(protection_token)
        token_name = sha256(token).hexdigest()
        stage_leaf = f"{token_name}{_storage_suffix(key)}"
        temporary_leaf = f".{token_name}.tmp"
        expected_facts = (
            key.codec,
            _key_text(key),
            digest,
            size_bytes,
            modified_text,
        )
        installed_signature: _Signature | None = None
        with self._exclusive_state() as connection:
            existing = connection.execute(
                "SELECT storage_codec, storage_path, object_sha256, size_bytes, "
                "published_modified_at, state, staging_leaf, device, inode, "
                "modified_ns, changed_ns "
                "FROM protection_tokens WHERE token = ?",
                (token,),
            ).fetchone()
            if existing is not None:
                if tuple(existing[:4]) != expected_facts[:4]:
                    raise RuntimeError(
                        "resource protection token was reused for another object"
                    )
                state = str(existing[5])
                if state == "RELEASED":
                    if existing[4] not in {None, modified_text}:
                        raise RuntimeError(
                            "released resource token has another modified_at"
                        )
                    return ArtifactStorageEvidence(False)
                if tuple(existing[:5]) != expected_facts:
                    raise RuntimeError(
                        "resource protection token was reused with another modified_at"
                    )
                if state == "STAGED":
                    if existing[6] != stage_leaf:
                        raise RuntimeError(
                            "staged resource journal leaf disagrees with its token"
                        )
                elif state not in {"WRITING", "INSTALLED"}:
                    raise RuntimeError("resource protection journal state is corrupt")
                if state == "WRITING" and (
                    existing[6] != stage_leaf
                    or any(value is not None for value in existing[7:])
                ):
                    raise RuntimeError(
                        "writing resource journal authority is inconsistent"
                    )
                if state == "INSTALLED" and (
                    existing[6] is not None
                    or any(value is not None for value in existing[7:])
                ):
                    raise RuntimeError(
                        "installed resource journal authority is inconsistent"
                    )
                if state == "INSTALLED":
                    installed = connection.execute(
                        "SELECT device, inode, modified_ns, changed_ns "
                        "FROM current_entries WHERE storage_codec = ? "
                        "AND storage_path = ? AND object_sha256 = ? "
                        "AND size_bytes = ? AND published_modified_at = ?",
                        expected_facts,
                    ).fetchone()
                    if installed is None:
                        raise RuntimeError(
                            "installed resource lacks current journal authority"
                        )
                    installed_signature = _storage_signature(
                        installed,
                        size=size_bytes,
                    )
            else:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.execute(
                        "INSERT INTO protection_tokens "
                        "(token, storage_codec, storage_path, object_sha256, "
                        "size_bytes, published_modified_at, state, staging_leaf) "
                        "VALUES (?, ?, ?, ?, ?, ?, 'WRITING', ?)",
                        (token, *expected_facts, stage_leaf),
                    )
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise

        if existing is not None and state == "INSTALLED":
            if installed_signature is None:
                raise RuntimeError("installed resource authority is incomplete")
            self._require_current_authority(
                key,
                expected_size=size_bytes,
                expected_signature=installed_signature,
                label="installed library resource",
            )
            return ArtifactStorageEvidence(True, storage_object)
        if existing is not None and state == "STAGED":
            self._verify_stage_row(
                existing[6:],
                token=token,
                key=key,
                digest=digest,
                size=size_bytes,
            )
            return ArtifactStorageEvidence(True, storage_object)

        final_path = self._staging / stage_leaf
        temporary_path = self._staging / temporary_leaf
        try:
            if final_path.exists() or final_path.is_symlink():
                if temporary_path.exists() or temporary_path.is_symlink():
                    with _open_directory(self._staging) as descriptor:
                        signature = _heal_same_inode_rename_duplicate(
                            source_descriptor=descriptor,
                            source_leaf=temporary_leaf,
                            destination_descriptor=descriptor,
                            destination_leaf=stage_leaf,
                            expected_sha256=digest,
                            expected_size=size_bytes,
                            expected_identity=None,
                            label="recoverable staged publish",
                        )
                else:
                    with _open_directory(self._staging) as descriptor:
                        signature = _verify_and_fsync_published_at(
                            descriptor,
                            stage_leaf,
                            expected_sha256=digest,
                            expected_size=size_bytes,
                            label="recoverable staged artifact",
                        )
            else:
                signature = self._write_stage(
                    content,
                    temporary_path=temporary_path,
                    final_path=final_path,
                    expected_sha256=digest,
                    expected_size=size_bytes,
                )
        except BaseException:
            replay = self._protection_replay_result(
                token=token,
                key=key,
                digest=digest,
                size=size_bytes,
                modified_text=modified_text,
                stage_leaf=stage_leaf,
                storage_object=storage_object,
            )
            if replay is not None:
                content.seek(0)
                return replay
            raise

        released = False
        terminal_signature = signature
        with self._exclusive_state() as connection:
            current = connection.execute(
                "SELECT storage_codec, storage_path, object_sha256, size_bytes, "
                "published_modified_at, state, staging_leaf, device, inode, "
                "modified_ns, changed_ns FROM protection_tokens WHERE token = ?",
                (token,),
            ).fetchone()
            if current is None or tuple(current[:5]) != expected_facts:
                raise RuntimeError("resource protection journal changed")
            state = str(current[5])
            if state == "RELEASED":
                released = True
            elif state == "STAGED":
                if current[6] != stage_leaf:
                    raise RuntimeError(
                        "staged resource journal leaf disagrees with its token"
                    )
                journal_signature = _storage_signature(current[7:], size=size_bytes)
                if not _same_content_identity(journal_signature, signature):
                    raise RuntimeError("resource protection journal changed")
                terminal_signature = journal_signature
            elif (
                state != "WRITING"
                or current[6] != stage_leaf
                or any(value is not None for value in current[7:])
            ):
                raise RuntimeError("resource protection journal changed")
            else:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    affected = connection.execute(
                        "UPDATE protection_tokens SET state = 'STAGED', "
                        "device = ?, inode = ?, modified_ns = ?, changed_ns = ? "
                        "WHERE token = ? AND state = 'WRITING' "
                        "AND storage_codec = ? AND storage_path = ? "
                        "AND object_sha256 = ? AND size_bytes = ? "
                        "AND published_modified_at = ? AND staging_leaf = ? "
                        "AND device IS NULL AND inode IS NULL "
                        "AND modified_ns IS NULL AND changed_ns IS NULL",
                        (
                            signature.device,
                            signature.inode,
                            signature.modified_ns,
                            signature.changed_ns,
                            token,
                            *expected_facts,
                            stage_leaf,
                        ),
                    ).rowcount
                    if affected != 1:
                        raise RuntimeError("resource protection journal changed")
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise

        if released:
            self._discard_uncommitted_stage(
                token=token,
                key=key,
                digest=digest,
                size=size_bytes,
                stage_leaf=stage_leaf,
                signature=terminal_signature,
            )
            content.seek(0)
            return ArtifactStorageEvidence(False)
        content.seek(0)
        return ArtifactStorageEvidence(True, storage_object)

    def _protection_replay_result(
        self,
        *,
        token: bytes,
        key: StorageObjectKey,
        digest: bytes,
        size: int,
        modified_text: str,
        stage_leaf: str,
        storage_object: StorageObjectDescriptor,
    ) -> ArtifactStorageEvidence | None:
        """Resolve a concurrent or response-lost terminal state after local I/O."""

        with self._exclusive_state() as connection:
            row = connection.execute(
                "SELECT storage_codec, storage_path, object_sha256, size_bytes, "
                "published_modified_at, state, staging_leaf, device, inode, "
                "modified_ns, changed_ns FROM protection_tokens WHERE token = ?",
                (token,),
            ).fetchone()
        expected = (key.codec, _key_text(key), digest, size, modified_text)
        if row is None or tuple(row[:5]) != expected:
            return None
        state = str(row[5])
        if state == "RELEASED":
            return ArtifactStorageEvidence(False)
        if state == "INSTALLED":
            self._verify_current(
                key,
                expected_sha256=digest,
                expected_size=size,
                label="installed library resource",
            )
            return ArtifactStorageEvidence(True, storage_object)
        if state != "STAGED" or row[6] != stage_leaf:
            return None
        self._verify_stage_row(
            row[6:],
            token=token,
            key=key,
            digest=digest,
            size=size,
        )
        return ArtifactStorageEvidence(True, storage_object)

    def _discard_uncommitted_stage(
        self,
        *,
        token: bytes,
        key: StorageObjectKey,
        digest: bytes,
        size: int,
        stage_leaf: str,
        signature: _Signature,
    ) -> None:
        """Remove only bytes this stale writer proved before observing RELEASED."""

        self._remove_stage_from_row(
            (
                stage_leaf,
                signature.device,
                signature.inode,
                signature.modified_ns,
                signature.changed_ns,
            ),
            token=token,
            key=key,
            digest=digest,
            size=size,
        )
        self._remove_stage_temporary(
            token,
            digest=digest,
            size=size,
            allow_partial=True,
        )

    def render_presentation(
        self,
        archive: BinaryIO,
        thumbnail_destination: BinaryIO,
        *,
        rendered_pages: tuple[ArtifactRenderedPage, ...],
    ) -> ArtifactPresentationRenderEvidence:
        """Write thumbnail bytes and return untrusted neutral presentation facts."""

        return render_presentation(
            archive,
            thumbnail_destination,
            rendered_pages=rendered_pages,
            policy=self._render_policy,
            metrics_sink=self._metrics_sink,
        )

    def release(
        self,
        storage_key: StorageObjectKey,
        expected_sha256: bytes,
        expected_size_bytes: int,
        protection_token: bytes,
    ) -> ArtifactReleaseStorageEvidence:
        """Persist a terminal tombstone and remove only exact private staging."""

        self._ensure_layout()
        with self._publication_io_guard(exclusive=True):
            return self._release(
                storage_key,
                expected_sha256,
                expected_size_bytes,
                protection_token,
            )

    def _release(
        self,
        storage_key: StorageObjectKey,
        expected_sha256: bytes,
        expected_size_bytes: int,
        protection_token: bytes,
    ) -> ArtifactReleaseStorageEvidence:
        """Run one terminal cleanup below the exclusive publication gate."""

        key = _storage_key(storage_key)
        digest = _digest(expected_sha256)
        size_bytes = _size(expected_size_bytes)
        token = _token(protection_token)
        facts = (key.codec, _key_text(key), digest, size_bytes)
        with self._exclusive_state() as connection:
            journal = _journal_state(connection)
            row = connection.execute(
                "SELECT storage_codec, storage_path, object_sha256, size_bytes, "
                "state, staging_leaf, device, inode, modified_ns, changed_ns "
                "FROM protection_tokens WHERE token = ?",
                (token,),
            ).fetchone()
            if row is not None and tuple(row[:4]) != facts:
                raise RuntimeError(
                    "artifact release token refers to another exact artifact"
                )
            if journal.pending_revision is not None and row is not None:
                referenced = connection.execute(
                    "SELECT EXISTS(SELECT 1 FROM pending_entries "
                    "WHERE activation_revision = ? AND storage_codec = ? "
                    "AND storage_path = ? AND object_sha256 = ? "
                    "AND size_bytes = ? AND activated = 0)",
                    (journal.pending_revision, *facts),
                ).fetchone()
                if referenced not in {(0,), (1,)}:
                    raise RuntimeError("library activation reference is corrupt")
                if referenced == (1,):
                    raise RuntimeError(
                        "artifact release is blocked by unfinished library activation"
                    )
        if row is None:
            with _open_directory(self._staging) as descriptor:
                unexpected_stage = _lstat_at(
                    descriptor,
                    _stage_leaf(token, key),
                )
            if unexpected_stage is not None:
                raise RuntimeError(
                    "unowned staged resource exists without journal authority"
                )
            unexpected_temporary = self._verify_stage_temporary(
                token,
                digest=digest,
                size=size_bytes,
            )
            if unexpected_temporary is not None:
                raise RuntimeError(
                    "unowned staging temporary exists without journal authority"
                )
            with self._exclusive_state() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.execute(
                        "INSERT INTO protection_tokens "
                        "(token, storage_codec, storage_path, object_sha256, "
                        "size_bytes, state, staging_leaf) "
                        "VALUES (?, ?, ?, ?, ?, 'RELEASED', NULL)",
                        (token, *facts),
                    )
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
            return ArtifactReleaseStorageEvidence(True)

        with self._exclusive_state() as connection:
            current = connection.execute(
                "SELECT storage_codec, storage_path, object_sha256, size_bytes, "
                "state, staging_leaf, device, inode, modified_ns, changed_ns "
                "FROM protection_tokens WHERE token = ?",
                (token,),
            ).fetchone()
            if current is None:
                raise RuntimeError("artifact release journal disappeared")
            row = current
            if tuple(row[:4]) != facts:
                raise RuntimeError(
                    "artifact release token refers to another exact artifact"
                )
            state = str(row[4])
            partial_authorized = _stage_row_authorizes_partial_temporary(
                row[5:],
                token=token,
                key=key,
            )
            if state not in {"WRITING", "STAGED", "INSTALLED", "RELEASED"}:
                raise RuntimeError("artifact release journal state is corrupt")
            if (state == "WRITING") != partial_authorized and state != "RELEASED":
                raise RuntimeError("artifact release journal state is inconsistent")
            newly_released = state != "RELEASED"
            if newly_released:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    affected = connection.execute(
                        "UPDATE protection_tokens SET state = 'RELEASED' "
                        "WHERE token = ? AND storage_codec = ? "
                        "AND storage_path = ? AND object_sha256 = ? "
                        "AND size_bytes = ? AND state = ? "
                        "AND staging_leaf IS ? AND device IS ? AND inode IS ? "
                        "AND modified_ns IS ? AND changed_ns IS ?",
                        (token, *facts, state, *row[5:]),
                    ).rowcount
                    if affected != 1:
                        raise RuntimeError("artifact release authorization changed")
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise

        self._heal_released_stage_duplicate(
            row[5:],
            token=token,
            key=key,
            digest=digest,
            size=size_bytes,
        )
        self._remove_stage_from_row(
            row[5:],
            token=token,
            key=key,
            digest=digest,
            size=size_bytes,
        )
        self._remove_stage_temporary(
            token,
            digest=digest,
            size=size_bytes,
            allow_partial=partial_authorized,
        )
        if newly_released and row[5] is not None:
            with self._exclusive_state() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    affected = connection.execute(
                        "UPDATE protection_tokens SET staging_leaf = NULL, "
                        "device = NULL, inode = NULL, modified_ns = NULL, "
                        "changed_ns = NULL WHERE token = ? AND state = 'RELEASED' "
                        "AND storage_codec = ? AND storage_path = ? "
                        "AND object_sha256 = ? AND size_bytes = ? "
                        "AND staging_leaf IS ? AND device IS ? AND inode IS ? "
                        "AND modified_ns IS ? AND changed_ns IS ?",
                        (token, *facts, *row[5:]),
                    ).rowcount
                    if affected != 1:
                        raise RuntimeError("artifact release cleanup authority changed")
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
                    self._remove_marker(target, receipt)
                else:
                    self._replay_removed_marker()
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
        """Durably append one bounded page of acquisition/thumbnail resources."""

        target = _revision(revision)
        page = tuple(items)
        if len(page) > _MAX_PAGE_ITEMS:
            raise ValueError("library activation page exceeds 128 items")
        if any(not isinstance(item, VNextLibraryActivationItem) for item in page):
            raise TypeError("library activation page contains a foreign item")
        if any(
            _activation_key(left) >= _activation_key(right)
            for left, right in pairwise(page)
        ):
            raise ValueError("library activation resource keys must be increasing")
        self._require_publication_lock()
        with self._exclusive_state() as connection:
            state = _journal_state(connection)
            if state.pending_revision != target or state.phase != "OPEN":
                raise RuntimeError("library activation spool is not open")
            if (
                page
                and state.last_cursor is not None
                and _activation_key(page[0]) <= state.last_cursor
            ):
                raise ValueError("library activation page does not advance cursor")
            connection.execute("BEGIN IMMEDIATE")
            try:
                for item in page:
                    item.__post_init__()
                    storage_object = item.storage_object
                    storage_object.__post_init__()
                    key = _storage_key(storage_object.key)
                    if _gid_from_key(key) != item.gid:
                        raise ValueError(
                            "library activation storage key belongs to another GID"
                        )
                    if _resource_kind(key) is not item.resource_kind:
                        raise ValueError(
                            "library activation storage key has another resource kind"
                        )
                    connection.execute(
                        "INSERT INTO pending_entries "
                        "(activation_revision, publication_key, gid, resource_kind, "
                        "storage_codec, storage_path, object_sha256, size_bytes, "
                        "published_modified_at, operation_started, activated) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0)",
                        (
                            target,
                            item.publication_key,
                            item.gid,
                            item.resource_kind.value,
                            key.codec,
                            _key_text(key),
                            bytes.fromhex(storage_object.sha256),
                            storage_object.size_bytes,
                            _modified_at(storage_object.modified_at).isoformat(
                                timespec="microseconds"
                            ),
                        ),
                    )
                if page:
                    connection.execute(
                        "UPDATE library_state SET last_cursor = ? WHERE singleton = 1",
                        (_activation_key(page[-1]),),
                    )
                connection.commit()
            except sqlite3.IntegrityError as error:
                connection.rollback()
                raise RuntimeError(
                    "library activation contains a duplicate resource or storage key"
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
            revision=target,
            receipt=receipt,
            limit=limit,
        )
        if install_cursor is not None:
            with self._exclusive_state() as connection:
                return self._record_reconcile_cursor(
                    connection,
                    revision=target,
                    receipt=receipt,
                    cursor=install_cursor,
                )

        with self._exclusive_state() as connection:
            state = _journal_state(connection)
            if (
                state.pending_revision != target
                or state.pending_receipt != receipt
                or state.phase != "ACTIVATING"
            ):
                raise RuntimeError("library activation changed after installation")
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
                        "(activation_revision, publication_key, resource_kind, "
                        "storage_codec, storage_path, object_sha256, size_bytes, "
                        "device, inode, modified_ns, changed_ns, operation_started) "
                        "SELECT ?, c.publication_key, c.resource_kind, "
                        "c.storage_codec, c.storage_path, c.object_sha256, "
                        "c.size_bytes, c.device, c.inode, c.modified_ns, "
                        "c.changed_ns, 0 "
                        "FROM current_entries AS c WHERE NOT EXISTS ("
                        "SELECT 1 FROM pending_entries AS p "
                        "WHERE p.activation_revision = ? "
                        "AND p.publication_key = c.publication_key "
                        "AND p.resource_kind = c.resource_kind) "
                        "ORDER BY c.publication_key, c.resource_kind LIMIT ?",
                        (target, target, limit),
                    )
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise

        removal_cursor = self._remove_stale(
            revision=target,
            receipt=receipt,
            limit=limit,
        )
        if removal_cursor is not None:
            with self._exclusive_state() as connection:
                return self._record_reconcile_cursor(
                    connection,
                    revision=target,
                    receipt=receipt,
                    cursor=removal_cursor,
                )

        with self._exclusive_state() as connection:
            state = _journal_state(connection)
            if (
                state.pending_revision != target
                or state.pending_receipt != receipt
                or state.phase != "ACTIVATING"
            ):
                raise RuntimeError("library activation changed after removal")
            remaining = connection.execute(
                "SELECT EXISTS(SELECT 1 FROM pending_entries "
                "WHERE activation_revision = ? AND activated = 0), "
                "EXISTS(SELECT 1 FROM pending_removals "
                "WHERE activation_revision = ?), "
                "EXISTS(SELECT 1 FROM current_entries AS c WHERE NOT EXISTS ("
                "SELECT 1 FROM pending_entries AS p "
                "WHERE p.activation_revision = ? "
                "AND p.publication_key = c.publication_key "
                "AND p.resource_kind = c.resource_kind))",
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
        cursor: VNextLibraryActivationCursor,
    ) -> LibraryActivationCheckpoint:
        encoded_cursor = cursor.to_bytes()
        connection.execute("BEGIN IMMEDIATE")
        try:
            affected = connection.execute(
                "UPDATE library_state SET last_cursor = ? WHERE singleton = 1 "
                "AND pending_revision = ? AND pending_receipt_id = ? "
                "AND phase = 'ACTIVATING'",
                (encoded_cursor, revision, receipt),
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
                    self._remove_marker(target, receipt)
                else:
                    self._replay_removed_marker()
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
            self._remove_marker(target, receipt)

    def maintain_cleanup(self) -> LibraryMaintenanceOutcome:
        """Remove one bounded page of terminal private staging bytes."""

        self._ensure_layout()
        with self._publication_io_guard(exclusive=True):
            return self._maintain_cleanup()

    def _maintain_cleanup(self) -> LibraryMaintenanceOutcome:
        """Run bounded cleanup while fenced from publication filesystem work."""

        with self._exclusive_state() as connection:
            state = _journal_state(connection)
            if state.pending_revision is not None:
                return LibraryMaintenanceOutcome.BLOCKED
            rows = connection.execute(
                "SELECT token, storage_codec, storage_path, object_sha256, "
                "size_bytes, staging_leaf, device, inode, modified_ns, changed_ns "
                "FROM protection_tokens "
                "WHERE state = 'RELEASED' AND staging_leaf IS NOT NULL "
                "ORDER BY token LIMIT ?",
                (_MAX_CLEANUP_ITEMS,),
            ).fetchall()
        for row in rows:
            token = bytes(row[0])
            key = _key_from_row(str(row[1]), str(row[2]))
            digest = bytes(row[3])
            size = int(row[4])
            partial_authorized = _stage_row_authorizes_partial_temporary(
                row[5:],
                token=token,
                key=key,
            )
            self._heal_released_stage_duplicate(
                row[5:],
                token=token,
                key=key,
                digest=digest,
                size=size,
            )
            self._remove_stage_from_row(
                row[5:],
                token=token,
                key=key,
                digest=digest,
                size=size,
            )
            self._remove_stage_temporary(
                token,
                digest=digest,
                size=size,
                allow_partial=partial_authorized,
            )
            with self._exclusive_state() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    affected = connection.execute(
                        "UPDATE protection_tokens SET staging_leaf = NULL, "
                        "device = NULL, inode = NULL, modified_ns = NULL, "
                        "changed_ns = NULL WHERE token = ? AND state = 'RELEASED' "
                        "AND storage_codec = ? AND storage_path = ? "
                        "AND object_sha256 = ? AND size_bytes = ? "
                        "AND staging_leaf IS ? AND device IS ? AND inode IS ? "
                        "AND modified_ns IS ? AND changed_ns IS ?",
                        (token, *row[1:5], *row[5:]),
                    ).rowcount
                    if affected != 1:
                        raise RuntimeError("library cleanup authority changed")
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise

        with self._exclusive_state() as connection:
            state = _journal_state(connection)
            if state.pending_revision is not None:
                return LibraryMaintenanceOutcome.BLOCKED
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
                "SELECT activation_revision, publication_key, resource_kind "
                "FROM pending_entries ORDER BY activation_revision, publication_key, "
                "resource_kind LIMIT ?",
                (_MAX_JOURNAL_CLEANUP_ITEMS,),
            ).fetchall()
            if completed_rows:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.executemany(
                        "DELETE FROM pending_entries "
                        "WHERE activation_revision = ? AND publication_key = ? "
                        "AND resource_kind = ?",
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
        *,
        revision: int,
        receipt: bytes,
        limit: int,
    ) -> VNextLibraryActivationCursor | None:
        with self._exclusive_state() as connection:
            state = _journal_state(connection)
            if (
                state.pending_revision != revision
                or state.pending_receipt != receipt
                or state.phase != "ACTIVATING"
            ):
                raise RuntimeError("library activation changed before installation")
            rows = connection.execute(
                "SELECT publication_key, resource_kind FROM pending_entries "
                "WHERE activation_revision = ? AND activated = 0 "
                "ORDER BY publication_key, resource_kind LIMIT ?",
                (revision, limit),
            ).fetchall()

        last_cursor: VNextLibraryActivationCursor | None = None
        for publication_key, resource_kind in rows:
            plan = self._claim_pending_install(
                revision=revision,
                receipt=receipt,
                publication_key=bytes(publication_key),
                resource_kind=str(resource_kind),
            )
            signature, terminal_token = self._perform_pending_install(plan)
            self._commit_pending_install(
                plan,
                signature=signature,
                terminal_token=terminal_token,
            )
            last_cursor = _activation_cursor_from_fields(
                plan.publication_key,
                plan.resource_kind,
            )
        return last_cursor

    def _claim_pending_install(
        self,
        *,
        revision: int,
        receipt: bytes,
        publication_key: bytes,
        resource_kind: str,
    ) -> _PendingInstall:
        """Durably reserve one exact pending row in a short state transaction."""

        with self._exclusive_state() as connection:
            state = _journal_state(connection)
            if (
                state.pending_revision != revision
                or state.pending_receipt != receipt
                or state.phase != "ACTIVATING"
            ):
                raise RuntimeError("library activation changed before reservation")
            row = connection.execute(
                "SELECT p.storage_codec, p.storage_path, p.object_sha256, "
                "p.size_bytes, p.operation_started, p.activated, p.device, p.inode, "
                "p.modified_ns, p.changed_ns, c.storage_path, c.object_sha256, "
                "c.size_bytes, c.device, c.inode, c.modified_ns, c.changed_ns, "
                "p.published_modified_at "
                "FROM pending_entries AS p LEFT JOIN current_entries AS c "
                "ON c.publication_key = p.publication_key "
                "AND c.resource_kind = p.resource_kind "
                "WHERE p.activation_revision = ? AND p.publication_key = ? "
                "AND p.resource_kind = ?",
                (revision, publication_key, resource_kind),
            ).fetchone()
            if row is None or bool(row[5]):
                raise RuntimeError("pending library activation changed")
            if any(value is not None for value in row[6:10]):
                raise RuntimeError("pending library activation authority is corrupt")
            key = _key_from_row(str(row[0]), str(row[1]))
            digest = bytes(row[2])
            size_bytes = int(row[3])
            modified_text = str(row[17])
            current_exists = row[10] is not None
            current_digest = bytes(row[11]) if current_exists else None
            current_size = int(row[12]) if current_exists else None
            current_signature = (
                _storage_signature(row[13:17], size=int(row[12]))
                if current_exists
                else None
            )
            staged = self._staged_candidate_authority(
                connection,
                key=key,
                digest=digest,
                size=size_bytes,
                modified_text=modified_text,
            )
            fresh_authorization = not bool(row[4])
            if fresh_authorization:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    affected = connection.execute(
                        "UPDATE pending_entries SET operation_started = 1 "
                        "WHERE activation_revision = ? AND publication_key = ? "
                        "AND resource_kind = ? AND storage_codec = ? "
                        "AND storage_path = ? AND object_sha256 = ? "
                        "AND size_bytes = ? AND published_modified_at = ? "
                        "AND operation_started = 0 AND activated = 0 "
                        "AND device IS NULL AND inode IS NULL "
                        "AND modified_ns IS NULL AND changed_ns IS NULL",
                        (
                            revision,
                            publication_key,
                            resource_kind,
                            key.codec,
                            _key_text(key),
                            digest,
                            size_bytes,
                            modified_text,
                        ),
                    ).rowcount
                    if affected != 1:
                        raise RuntimeError("library activation authorization changed")
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise

        return _PendingInstall(
            revision=revision,
            receipt=receipt,
            publication_key=publication_key,
            resource_kind=resource_kind,
            key=key,
            digest=digest,
            size_bytes=size_bytes,
            modified_text=modified_text,
            fresh_authorization=fresh_authorization,
            staged=staged,
            current_digest=current_digest,
            current_size=current_size,
            current_signature=current_signature,
        )

    def _perform_pending_install(
        self,
        plan: _PendingInstall,
    ) -> tuple[_Signature, bytes | None]:
        """Perform reserved filesystem work without holding state.lock."""

        current_content_unchanged = (
            plan.current_signature is not None
            and plan.current_digest == plan.digest
            and plan.current_size == plan.size_bytes
        )
        if plan.staged is None:
            if not current_content_unchanged or plan.current_signature is None:
                raise RuntimeError(
                    f"library activation lacks staged authority for "
                    f"{self._target(plan.key)}"
                )
            quarantine = self._quarantine / _quarantine_leaf(
                _key_text(plan.key),
                plan.digest,
            )
            if _safe_lstat(quarantine) is not None:
                raise RuntimeError("unchanged library path has replacement quarantine")
            signature = self._require_current_authority(
                plan.key,
                expected_size=plan.size_bytes,
                expected_signature=plan.current_signature,
                label="unchanged library artifact",
            )
            terminal_token = None
        else:
            signature = self._install_staged(
                key=plan.key,
                digest=plan.digest,
                size=plan.size_bytes,
                staged=plan.staged,
                current_digest=plan.current_digest,
                current_size=plan.current_size,
                current_signature=plan.current_signature,
                fresh_authorization=plan.fresh_authorization,
            )
            terminal_token = plan.staged.token
        if (
            plan.current_signature is not None
            and plan.current_digest is not None
            and plan.current_size is not None
            and plan.staged is not None
        ):
            self._retire_replaced_current(
                plan.key,
                digest=plan.current_digest,
                size=plan.current_size,
                expected_signature=plan.current_signature,
                reuse_verified_digest=plan.fresh_authorization,
            )
        durable = self._require_current_authority(
            plan.key,
            expected_size=plan.size_bytes,
            expected_signature=signature,
            label="activation-commit library artifact",
        )
        if plan.staged is not None and not _same_content_identity(
            durable,
            plan.staged.signature,
        ):
            raise RuntimeError("activation current lost its staged inode authority")
        return durable, terminal_token

    def _commit_pending_install(
        self,
        plan: _PendingInstall,
        *,
        signature: _Signature,
        terminal_token: bytes | None,
    ) -> None:
        """Fence one filesystem outcome into the exact reserved journal row."""

        with self._exclusive_state() as connection:
            state = _journal_state(connection)
            if (
                state.pending_revision != plan.revision
                or state.pending_receipt != plan.receipt
                or state.phase != "ACTIVATING"
            ):
                raise RuntimeError("library activation changed before terminalization")
            pending = connection.execute(
                "SELECT storage_codec, storage_path, object_sha256, size_bytes, "
                "published_modified_at, operation_started, activated, device, inode, "
                "modified_ns, changed_ns FROM pending_entries "
                "WHERE activation_revision = ? AND publication_key = ? "
                "AND resource_kind = ?",
                (plan.revision, plan.publication_key, plan.resource_kind),
            ).fetchone()
            expected_pending = (
                plan.key.codec,
                _key_text(plan.key),
                plan.digest,
                plan.size_bytes,
                plan.modified_text,
                1,
                0,
                None,
                None,
                None,
                None,
            )
            if pending is None or tuple(pending) != expected_pending:
                raise RuntimeError("pending library activation changed")
            exact_signature = self._require_current_authority(
                plan.key,
                expected_size=plan.size_bytes,
                expected_signature=signature,
                label="activation terminal library artifact",
            )
            connection.execute("BEGIN IMMEDIATE")
            try:
                if terminal_token is not None:
                    if plan.staged is None:
                        raise RuntimeError("activation terminal token lacks authority")
                    self._terminalize_stage_in_transaction(
                        connection,
                        terminal_token,
                        key=plan.key,
                        digest=plan.digest,
                        size=plan.size_bytes,
                        modified_text=plan.modified_text,
                        staged=plan.staged,
                    )
                connection.execute(
                    "INSERT INTO current_entries "
                    "(publication_key, resource_kind, storage_path, storage_codec, "
                    "gid, object_sha256, size_bytes, published_modified_at, device, "
                    "inode, modified_ns, changed_ns) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(publication_key, resource_kind) DO UPDATE SET "
                    "storage_path = excluded.storage_path, "
                    "storage_codec = excluded.storage_codec, gid = excluded.gid, "
                    "object_sha256 = excluded.object_sha256, "
                    "size_bytes = excluded.size_bytes, "
                    "published_modified_at = excluded.published_modified_at, "
                    "device = excluded.device, inode = excluded.inode, "
                    "modified_ns = excluded.modified_ns, "
                    "changed_ns = excluded.changed_ns",
                    (
                        plan.publication_key,
                        plan.resource_kind,
                        _key_text(plan.key),
                        plan.key.codec,
                        _gid_from_key(plan.key),
                        plan.digest,
                        plan.size_bytes,
                        plan.modified_text,
                        exact_signature.device,
                        exact_signature.inode,
                        exact_signature.modified_ns,
                        exact_signature.changed_ns,
                    ),
                )
                affected = connection.execute(
                    "UPDATE pending_entries SET activated = 1, device = ?, "
                    "inode = ?, modified_ns = ?, changed_ns = ? "
                    "WHERE activation_revision = ? AND publication_key = ? "
                    "AND resource_kind = ? AND storage_codec = ? "
                    "AND storage_path = ? AND object_sha256 = ? "
                    "AND size_bytes = ? AND published_modified_at = ? "
                    "AND operation_started = 1 AND activated = 0 "
                    "AND device IS NULL AND inode IS NULL "
                    "AND modified_ns IS NULL AND changed_ns IS NULL",
                    (
                        exact_signature.device,
                        exact_signature.inode,
                        exact_signature.modified_ns,
                        exact_signature.changed_ns,
                        plan.revision,
                        plan.publication_key,
                        plan.resource_kind,
                        plan.key.codec,
                        _key_text(plan.key),
                        plan.digest,
                        plan.size_bytes,
                        plan.modified_text,
                    ),
                ).rowcount
                if affected != 1:
                    raise RuntimeError("pending library activation changed")
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _install_staged(
        self,
        *,
        key: StorageObjectKey,
        digest: bytes,
        size: int,
        staged: _StagedAuthority,
        current_digest: bytes | None,
        current_size: int | None,
        current_signature: _Signature | None,
        fresh_authorization: bool,
    ) -> _Signature:
        if fresh_authorization:
            return self._install_fresh_staged(
                key=key,
                size=size,
                staged=staged,
                current_digest=current_digest,
                current_size=current_size,
                current_signature=current_signature,
            )
        target = self._target(key)
        if (current_signature is None) != (current_digest is None) or (
            current_signature is None
        ) != (current_size is None):
            raise RuntimeError("current library replacement authority is incomplete")
        if staged.leaf != _stage_leaf(staged.token, key):
            raise RuntimeError("staged authority token disagrees with its leaf")

        current_value = self._current_lstat(key)
        with _open_directory(self._staging) as staging_descriptor:
            stage_value = _lstat_at(staging_descriptor, staged.leaf)
        if current_value is not None and stage_value is not None:
            current_observation = _Signature.from_stat(current_value)
            # A normal replacement starts with the authorized old current and
            # the new staged inode both visible.  Only interpret the pair as a
            # replayed stage-to-current rename when the current name no longer
            # has the durable old-current identity (or no old current exists).
            # The healing helper then requires both names to be the exact same
            # staged inode; byte-identical foreign inodes remain ambiguous.
            if current_signature is None or not _same_content_identity(
                current_observation,
                current_signature,
            ):
                return self._heal_recovered_install_duplicate(
                    key,
                    digest=digest,
                    size=size,
                    staged=staged,
                )
        if current_value is not None and stage_value is None:
            try:
                recovered = self._verify_current(
                    key,
                    expected_sha256=digest,
                    expected_size=size,
                    label="recoverable renamed library artifact",
                )
            except RuntimeError:
                pass
            else:
                if _same_content_identity(recovered, staged.signature):
                    if (
                        current_signature is not None
                        and current_digest is not None
                        and current_size is not None
                    ):
                        quarantine_leaf = _quarantine_leaf(
                            _key_text(key),
                            current_digest,
                        )
                        with _open_directory(self._quarantine) as descriptor:
                            quarantined = _lstat_at(descriptor, quarantine_leaf)
                        if quarantined is not None:
                            self._verify_quarantined(
                                quarantine_leaf,
                                digest=current_digest,
                                size=current_size,
                                expected_signature=current_signature,
                                label="recoverable replaced library artifact",
                            )
                    self._fsync_recovered_install(
                        key,
                        digest=digest,
                        size=size,
                        staged=staged,
                    )
                    durable = self._verify_current(
                        key,
                        expected_sha256=digest,
                        expected_size=size,
                        label="durable recovered library artifact",
                    )
                    if not _same_content_identity(durable, staged.signature):
                        raise RuntimeError(
                            "recovered current changed staged inode identity"
                        )
                    return durable

        if stage_value is None:
            raise RuntimeError(
                f"staged and current library authority disagree: {target}"
            )
        with _open_directory(self._staging) as staging_descriptor:
            verified_stage = _verify_regular_at(
                staging_descriptor,
                staged.leaf,
                expected_sha256=digest,
                expected_size=size,
                label="staged library artifact",
            )
        if verified_stage != staged.signature:
            raise RuntimeError("staged library artifact changed identity")

        if (
            current_signature is not None
            and current_digest is not None
            and current_size is not None
        ):
            self._capture_replaced_current(
                key,
                digest=current_digest,
                size=current_size,
                expected_signature=current_signature,
            )
        elif current_value is not None:
            raise RuntimeError(f"unknown library target appeared: {target}")

        with (
            _open_directory_chain(
                self._current,
                key.segments[:-1],
                create=True,
            ) as (current_root_descriptor, parent_descriptor),
            _open_directory(self._staging) as staging_descriptor,
        ):
            visible_current = _lstat_at(parent_descriptor, key.segments[-1])
            if visible_current is not None:
                raise RuntimeError(
                    f"unknown library target appeared: {self._target(key)}"
                )
            stage_signature = _verify_regular_at(
                staging_descriptor,
                staged.leaf,
                expected_sha256=digest,
                expected_size=size,
                label="staged library artifact",
            )
            if stage_signature != staged.signature:
                raise RuntimeError("staged library path changed before activation")
            try:
                _rename_noreplace(
                    staged.leaf,
                    key.segments[-1],
                    source_descriptor=staging_descriptor,
                    destination_descriptor=parent_descriptor,
                )
            except FileExistsError as error:
                raise RuntimeError(
                    f"unknown library target appeared: {self._target(key)}"
                ) from error
            os.fsync(parent_descriptor)
            os.fsync(staging_descriptor)
            installed = _verify_regular_at(
                parent_descriptor,
                key.segments[-1],
                expected_sha256=digest,
                expected_size=size,
                label="newly activated library artifact",
            )
            if not _same_content_identity(installed, staged.signature):
                raise RuntimeError(
                    "installed artifact is not the authorized staged inode"
                )
            if _lstat_at(staging_descriptor, staged.leaf) is not None:
                raise RuntimeError("renamed staging artifact retained a source path")
            _require_chain_identity(
                self._current,
                key.segments[:-1],
                current_root_descriptor,
                parent_descriptor,
            )
            return installed

    def _install_fresh_staged(
        self,
        *,
        key: StorageObjectKey,
        size: int,
        staged: _StagedAuthority,
        current_digest: bytes | None,
        current_size: int | None,
        current_signature: _Signature | None,
    ) -> _Signature:
        """Install continuously owned verified bytes without rehashing the inode."""

        target = self._target(key)
        if (current_signature is None) != (current_digest is None) or (
            current_signature is None
        ) != (current_size is None):
            raise RuntimeError("current library replacement authority is incomplete")
        if staged.leaf != _stage_leaf(staged.token, key):
            raise RuntimeError("staged authority token disagrees with its leaf")
        _require_regular_path_authority(
            self._staging / staged.leaf,
            expected_size=size,
            expected_signature=staged.signature,
            label="staged library artifact",
        )
        current_value = self._current_lstat(key)
        if current_signature is not None:
            if current_value is None or _Signature.from_stat(current_value) != (
                current_signature
            ):
                raise RuntimeError(
                    f"managed library path changed before replace: {target}"
                )
            if current_digest is None or current_size is None:
                raise RuntimeError(
                    "current library replacement authority is incomplete"
                )
            self._capture_replaced_current(
                key,
                digest=current_digest,
                size=current_size,
                expected_signature=current_signature,
                reuse_verified_digest=True,
            )
        elif current_value is not None:
            raise RuntimeError(f"unknown library path appeared: {target}")

        with (
            _open_directory_chain(
                self._current,
                key.segments[:-1],
                create=True,
            ) as (current_root_descriptor, parent_descriptor),
            _open_directory(self._staging) as staging_descriptor,
        ):
            if _lstat_at(parent_descriptor, key.segments[-1]) is not None:
                raise RuntimeError(f"unknown library target appeared: {target}")
            stage_signature = _require_regular_authority_at(
                staging_descriptor,
                staged.leaf,
                expected_size=size,
                expected_signature=staged.signature,
                allow_renamed_identity=False,
                label="staged library artifact",
            )
            try:
                _rename_noreplace(
                    staged.leaf,
                    key.segments[-1],
                    source_descriptor=staging_descriptor,
                    destination_descriptor=parent_descriptor,
                )
            except FileExistsError as error:
                raise RuntimeError(
                    f"unknown library target appeared: {target}"
                ) from error
            os.fsync(parent_descriptor)
            os.fsync(staging_descriptor)
            installed = _require_regular_authority_at(
                parent_descriptor,
                key.segments[-1],
                expected_size=size,
                expected_signature=stage_signature,
                allow_renamed_identity=True,
                label="newly activated library artifact",
            )
            if _lstat_at(staging_descriptor, staged.leaf) is not None:
                raise RuntimeError("renamed staging artifact retained a source path")
            _require_chain_identity(
                self._current,
                key.segments[:-1],
                current_root_descriptor,
                parent_descriptor,
            )
            return installed

    def _heal_recovered_install_duplicate(
        self,
        key: StorageObjectKey,
        *,
        digest: bytes,
        size: int,
        staged: _StagedAuthority,
    ) -> _Signature:
        with (
            _open_directory_chain(
                self._current,
                key.segments[:-1],
                create=False,
            ) as (root_descriptor, parent_descriptor),
            _open_directory(self._staging) as staging_descriptor,
        ):
            survivor = _heal_same_inode_rename_duplicate(
                source_descriptor=staging_descriptor,
                source_leaf=staged.leaf,
                destination_descriptor=parent_descriptor,
                destination_leaf=key.segments[-1],
                expected_sha256=digest,
                expected_size=size,
                expected_identity=staged.signature,
                label="recoverable activated artifact rename",
            )
            _require_chain_identity(
                self._current,
                key.segments[:-1],
                root_descriptor,
                parent_descriptor,
            )
            return survivor

    def _fsync_recovered_install(
        self,
        key: StorageObjectKey,
        *,
        digest: bytes,
        size: int,
        staged: _StagedAuthority,
    ) -> None:
        with (
            _open_directory_chain(
                self._current,
                key.segments[:-1],
                create=False,
            ) as (root_descriptor, parent_descriptor),
            _open_directory(self._staging) as staging_descriptor,
        ):
            if _lstat_at(staging_descriptor, staged.leaf) is not None:
                raise RuntimeError("recovered install retained its staging path")
            verified = _verify_and_fsync_file_at(
                parent_descriptor,
                key.segments[-1],
                expected_sha256=digest,
                expected_size=size,
                label="recoverable renamed library artifact",
            )
            if not _same_content_identity(verified, staged.signature):
                raise RuntimeError("recovered current changed staged inode identity")
            os.fsync(parent_descriptor)
            os.fsync(staging_descriptor)
            durable = _verify_regular_at(
                parent_descriptor,
                key.segments[-1],
                expected_sha256=digest,
                expected_size=size,
                label="durable recoverable renamed library artifact",
            )
            if durable != verified:
                raise RuntimeError("recovered current changed across directory sync")
            _require_chain_identity(
                self._current,
                key.segments[:-1],
                root_descriptor,
                parent_descriptor,
            )

    def _capture_replaced_current(
        self,
        key: StorageObjectKey,
        *,
        digest: bytes,
        size: int,
        expected_signature: _Signature,
        reuse_verified_digest: bool = False,
    ) -> None:
        """Capture an old current leaf without ever overwriting quarantine."""

        quarantine_leaf = _quarantine_leaf(_key_text(key), digest)
        with _open_directory(self._quarantine) as quarantine_descriptor:
            quarantined = _lstat_at(quarantine_descriptor, quarantine_leaf)
        current = self._current_lstat(key)
        if quarantined is not None:
            if current is not None:
                self._heal_recovered_capture_duplicate(
                    key,
                    quarantine_leaf,
                    digest=digest,
                    size=size,
                    expected_signature=expected_signature,
                    label="recoverable replaced library artifact capture",
                )
                return
            self._fsync_recovered_capture(
                key,
                quarantine_leaf,
                digest=digest,
                size=size,
                expected_signature=expected_signature,
                quarantine_present=True,
                label="recoverable replaced library artifact",
            )
            return
        if current is None:
            raise RuntimeError(
                f"managed library target and quarantine both disappeared: "
                f"{self._target(key)}"
            )
        if _Signature.from_stat(current) != expected_signature:
            raise RuntimeError(
                f"managed library target changed before capture: {self._target(key)}"
            )
        self._quarantine_current(
            key,
            quarantine_leaf,
            expected_sha256=digest,
            expected_size=size,
            expected_signature=expected_signature,
            reuse_verified_digest=reuse_verified_digest,
        )

    def _retire_replaced_current(
        self,
        key: StorageObjectKey,
        *,
        digest: bytes,
        size: int,
        expected_signature: _Signature,
        reuse_verified_digest: bool,
    ) -> None:
        """Delete only the exact old leaf after the renamed current is durable."""

        quarantine_leaf = _quarantine_leaf(_key_text(key), digest)
        self._unlink_quarantined(
            quarantine_leaf,
            digest=digest,
            size=size,
            expected_signature=expected_signature,
            label="replaced library artifact",
            reuse_verified_digest=reuse_verified_digest,
        )

    def _terminalize_stage_in_transaction(
        self,
        connection: sqlite3.Connection,
        token: bytes,
        *,
        key: StorageObjectKey,
        digest: bytes,
        size: int,
        modified_text: str,
        staged: _StagedAuthority,
    ) -> None:
        affected = connection.execute(
            "UPDATE protection_tokens SET state = 'INSTALLED', "
            "staging_leaf = NULL, device = NULL, inode = NULL, "
            "modified_ns = NULL, changed_ns = NULL WHERE token = ? "
            "AND state = 'STAGED' AND storage_codec = ? AND storage_path = ? "
            "AND object_sha256 = ? AND size_bytes = ? "
            "AND published_modified_at = ? AND staging_leaf = ? "
            "AND device = ? AND inode = ? AND modified_ns = ? AND changed_ns = ?",
            (
                token,
                key.codec,
                _key_text(key),
                digest,
                size,
                modified_text,
                staged.leaf,
                staged.signature.device,
                staged.signature.inode,
                staged.signature.modified_ns,
                staged.signature.changed_ns,
            ),
        ).rowcount
        if affected != 1:
            raise RuntimeError("staged authority changed before activation commit")

    def _remove_stale(
        self,
        *,
        revision: int,
        receipt: bytes,
        limit: int,
    ) -> VNextLibraryActivationCursor | None:
        query = (
            "SELECT publication_key, resource_kind, storage_codec, storage_path, "
            "object_sha256, size_bytes, device, inode, modified_ns, changed_ns, "
            "operation_started FROM pending_removals "
        )
        with self._exclusive_state() as connection:
            state = _journal_state(connection)
            if (
                state.pending_revision != revision
                or state.pending_receipt != receipt
                or state.phase != "ACTIVATING"
            ):
                raise RuntimeError("library activation changed before removal")
            rows = connection.execute(
                query + "WHERE activation_revision = ? "
                "ORDER BY publication_key, resource_kind LIMIT ?",
                (revision, limit),
            ).fetchall()

        last_cursor: VNextLibraryActivationCursor | None = None
        for row in rows:
            with self._exclusive_state() as connection:
                state = _journal_state(connection)
                if (
                    state.pending_revision != revision
                    or state.pending_receipt != receipt
                    or state.phase != "ACTIVATING"
                ):
                    raise RuntimeError(
                        "library activation changed before removal claim"
                    )
                current_row = connection.execute(
                    query + "WHERE activation_revision = ? AND publication_key = ? "
                    "AND resource_kind = ?",
                    (revision, bytes(row[0]), str(row[1])),
                ).fetchone()
                if current_row is None or tuple(current_row) != tuple(row):
                    raise RuntimeError("pending library removal changed")
                if not bool(row[10]):
                    connection.execute("BEGIN IMMEDIATE")
                    try:
                        affected = connection.execute(
                            "UPDATE pending_removals SET operation_started = 1 "
                            "WHERE activation_revision = ? AND publication_key = ? "
                            "AND resource_kind = ? AND storage_codec = ? "
                            "AND storage_path = ? AND object_sha256 = ? "
                            "AND size_bytes = ? AND device = ? AND inode = ? "
                            "AND modified_ns = ? AND changed_ns = ? "
                            "AND operation_started = 0",
                            (revision, *row[:10]),
                        ).rowcount
                        if affected != 1:
                            raise RuntimeError("library removal authorization changed")
                        connection.commit()
                    except BaseException:
                        connection.rollback()
                        raise

            key = _key_from_row(str(row[2]), str(row[3]))
            digest = bytes(row[4])
            size_bytes = int(row[5])
            expected_signature = _Signature.from_row(
                (row[6], row[7], row[5], row[8], row[9])
            )
            target = self._target(key)
            quarantine = self._quarantine / _quarantine_leaf(str(row[3]), digest)
            target_value = self._current_lstat(key)
            quarantine_value = _safe_lstat(quarantine)
            if not bool(row[10]):
                if (
                    target_value is None
                    or _Signature.from_stat(target_value) != expected_signature
                    or quarantine_value is not None
                ):
                    raise RuntimeError(f"stale library path changed: {target}")
            if target_value is not None:
                if quarantine_value is not None:
                    self._heal_recovered_capture_duplicate(
                        key,
                        quarantine.name,
                        digest=digest,
                        size=size_bytes,
                        expected_signature=expected_signature,
                        label="recoverable stale library artifact capture",
                    )
                    target_value = None
                else:
                    if _Signature.from_stat(target_value) != expected_signature:
                        raise RuntimeError(f"stale library path changed: {target}")
                    self._quarantine_current(
                        key,
                        quarantine.name,
                        expected_sha256=digest,
                        expected_size=size_bytes,
                        expected_signature=expected_signature,
                        reuse_verified_digest=not bool(row[10]),
                    )
            quarantine_value = _safe_lstat(quarantine)
            if quarantine_value is None and not bool(row[10]):
                raise RuntimeError("stale library path vanished before authorization")
            if target_value is None:
                self._fsync_recovered_capture(
                    key,
                    quarantine.name,
                    digest=digest,
                    size=size_bytes,
                    expected_signature=expected_signature,
                    quarantine_present=quarantine_value is not None,
                    label="recoverable quarantined stale library artifact",
                )
            self._unlink_quarantined(
                quarantine.name,
                digest=digest,
                size=size_bytes,
                expected_signature=expected_signature,
                label="quarantined stale library artifact",
                reuse_verified_digest=not bool(row[10]),
            )
            with self._exclusive_state() as connection:
                state = _journal_state(connection)
                if (
                    state.pending_revision != revision
                    or state.pending_receipt != receipt
                    or state.phase != "ACTIVATING"
                ):
                    raise RuntimeError(
                        "library activation changed before removal terminalization"
                    )
                pending = connection.execute(
                    query + "WHERE activation_revision = ? AND publication_key = ? "
                    "AND resource_kind = ?",
                    (revision, bytes(row[0]), str(row[1])),
                ).fetchone()
                expected_pending = (*row[:10], 1)
                if pending is None or tuple(pending) != expected_pending:
                    raise RuntimeError("pending library removal changed")
                current = connection.execute(
                    "SELECT storage_codec, storage_path, object_sha256, size_bytes, "
                    "device, inode, modified_ns, changed_ns FROM current_entries "
                    "WHERE publication_key = ? AND resource_kind = ?",
                    (bytes(row[0]), str(row[1])),
                ).fetchone()
                if current is None or tuple(current) != tuple(row[2:10]):
                    raise RuntimeError("current library removal authority changed")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    removed_pending = connection.execute(
                        "DELETE FROM pending_removals WHERE activation_revision = ? "
                        "AND publication_key = ? AND resource_kind = ? "
                        "AND storage_codec = ? AND storage_path = ? "
                        "AND object_sha256 = ? AND size_bytes = ? "
                        "AND device = ? AND inode = ? AND modified_ns = ? "
                        "AND changed_ns = ? AND operation_started = 1",
                        (revision, *row[:10]),
                    ).rowcount
                    removed_current = connection.execute(
                        "DELETE FROM current_entries WHERE publication_key = ? "
                        "AND resource_kind = ? AND storage_codec = ? "
                        "AND storage_path = ? AND object_sha256 = ? "
                        "AND size_bytes = ? AND device = ? AND inode = ? "
                        "AND modified_ns = ? AND changed_ns = ?",
                        tuple(row[:10]),
                    ).rowcount
                    if removed_pending != 1 or removed_current != 1:
                        raise RuntimeError("library removal terminalization changed")
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
            last_cursor = _activation_cursor_from_fields(
                bytes(row[0]),
                str(row[1]),
            )
        return last_cursor

    def _staged_candidate_authority(
        self,
        connection: sqlite3.Connection,
        *,
        key: StorageObjectKey,
        digest: bytes,
        size: int,
        modified_text: str,
    ) -> _StagedAuthority | None:
        row = connection.execute(
            "SELECT token, staging_leaf, device, inode, modified_ns, changed_ns "
            "FROM protection_tokens WHERE storage_codec = ? AND storage_path = ? "
            "AND object_sha256 = ? AND size_bytes = ? "
            "AND published_modified_at = ? AND state = 'STAGED' "
            "ORDER BY token LIMIT 1",
            (key.codec, _key_text(key), digest, size, modified_text),
        ).fetchone()
        if row is None:
            return None
        token = _token(bytes(row[0]))
        leaf = str(row[1])
        if _STAGE_LEAF.fullmatch(leaf) is None or leaf != _stage_leaf(token, key):
            raise RuntimeError("library staging journal contains an unsafe path")
        return _StagedAuthority(
            token,
            leaf,
            _storage_signature(row[2:], size=size),
        )

    def _verify_stage_row(
        self,
        row: Sequence[object],
        *,
        token: bytes,
        key: StorageObjectKey,
        digest: bytes,
        size: int,
    ) -> None:
        leaf = str(row[0])
        if _STAGE_LEAF.fullmatch(leaf) is None or leaf != _stage_leaf(token, key):
            raise RuntimeError("artifact staging journal contains an unsafe path")
        expected_signature = _storage_signature(row[1:], size=size)
        signature = _require_regular_path_authority(
            self._staging / leaf,
            expected_size=size,
            expected_signature=expected_signature,
            label="staged artifact",
        )
        if signature != expected_signature:
            raise RuntimeError("staged artifact changed identity")

    def _remove_stage_from_row(
        self,
        row: Sequence[object],
        *,
        token: bytes,
        key: StorageObjectKey,
        digest: bytes,
        size: int,
    ) -> None:
        if row[0] is None:
            return
        leaf = str(row[0])
        if _STAGE_LEAF.fullmatch(leaf) is None or leaf != _stage_leaf(token, key):
            raise RuntimeError("artifact release journal contains an unsafe path")
        signature_values = tuple(row[1:])
        if all(value is None for value in signature_values):
            expected_signature = None
        elif any(value is None for value in signature_values):
            raise RuntimeError("staged artifact journal signature is incomplete")
        else:
            expected_signature = _storage_signature(signature_values, size=size)
        with _open_directory(self._staging) as descriptor:
            if _lstat_at(descriptor, leaf) is None:
                _fsync_absent_at(
                    descriptor,
                    leaf,
                    label="released staged artifact",
                )
                return
            if expected_signature is not None:
                _unlink_preserved_authority_at(
                    descriptor,
                    leaf,
                    expected_size=size,
                    expected_signature=expected_signature,
                    allow_renamed_identity=False,
                    label="released staged artifact",
                )
                return
            signature = _verify_regular_at(
                descriptor,
                leaf,
                expected_sha256=digest,
                expected_size=size,
                label="released staged artifact",
            )
            _unlink_verified_at(
                descriptor,
                leaf,
                expected_sha256=digest,
                expected_size=size,
                expected_signature=signature,
                label="released staged artifact",
            )

    def _heal_released_stage_duplicate(
        self,
        row: Sequence[object],
        *,
        token: bytes,
        key: StorageObjectKey,
        digest: bytes,
        size: int,
    ) -> None:
        """Collapse only one exact response-lost temp/final publish pair."""

        if len(row) != 5:
            raise RuntimeError("staging cleanup authority has an invalid shape")
        if row[0] is None:
            return
        final_leaf = str(row[0])
        if _STAGE_LEAF.fullmatch(final_leaf) is None or final_leaf != _stage_leaf(
            token, key
        ):
            raise RuntimeError("artifact release journal contains an unsafe path")
        signature_values = tuple(row[1:])
        if all(value is None for value in signature_values):
            expected_signature = None
        elif any(value is None for value in signature_values):
            raise RuntimeError("staged artifact journal signature is incomplete")
        else:
            expected_signature = _storage_signature(signature_values, size=size)
        temporary_leaf = f".{sha256(token).hexdigest()}.tmp"
        with _open_directory(self._staging) as descriptor:
            if (
                _lstat_at(descriptor, final_leaf) is None
                or _lstat_at(descriptor, temporary_leaf) is None
            ):
                return
            _heal_same_inode_rename_duplicate(
                source_descriptor=descriptor,
                source_leaf=final_leaf,
                destination_descriptor=descriptor,
                destination_leaf=temporary_leaf,
                expected_sha256=digest,
                expected_size=size,
                expected_identity=expected_signature,
                label="released staged publish",
            )

    def _remove_stage_temporary(
        self,
        token: bytes,
        *,
        digest: bytes,
        size: int,
        allow_partial: bool,
    ) -> None:
        leaf = f".{sha256(token).hexdigest()}.tmp"
        with _open_directory(self._staging) as descriptor:
            value = _lstat_at(descriptor, leaf)
            if value is None:
                _fsync_absent_at(
                    descriptor,
                    leaf,
                    label="released staging temporary",
                )
                return
            if allow_partial:
                observed_digest, signature = _capture_regular_at(
                    descriptor,
                    leaf,
                    maximum_size=size,
                    label="released partial staging temporary",
                )
                observed_size = signature.size_bytes
            else:
                observed_digest = digest
                observed_size = size
                signature = _verify_regular_at(
                    descriptor,
                    leaf,
                    expected_sha256=digest,
                    expected_size=size,
                    label="released staging temporary",
                )
            _require_managed_file_metadata(
                value,
                expected_signature=signature,
                label="released staging temporary",
            )
            _unlink_verified_at(
                descriptor,
                leaf,
                expected_sha256=observed_digest,
                expected_size=observed_size,
                expected_signature=signature,
                label="released staging temporary",
            )

    def _verify_stage_temporary(
        self,
        token: bytes,
        *,
        digest: bytes,
        size: int,
    ) -> _Signature | None:
        leaf = f".{sha256(token).hexdigest()}.tmp"
        with _open_directory(self._staging) as descriptor:
            value = _lstat_at(descriptor, leaf)
            if value is None:
                return None
            if value.st_size != size:
                raise RuntimeError(
                    "released staging temporary lacks exact content authority"
                )
            signature = _verify_regular_at(
                descriptor,
                leaf,
                expected_sha256=digest,
                expected_size=size,
                label="released staging temporary",
            )
            _require_managed_file_metadata(
                value,
                expected_signature=signature,
                label="released staging temporary",
            )
            return signature

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
        if _STAGE_LEAF.fullmatch(final_path.name) is None:
            raise RuntimeError("unsafe final staging name")
        return _publish_resumable_file(
            archive,
            directory=self._staging,
            temporary_leaf=temporary_path.name,
            final_leaf=final_path.name,
            expected_sha256=expected_sha256,
            expected_size=expected_size,
            label="staged artifact",
        )

    def _target(self, key: StorageObjectKey) -> Path:
        return self._current.joinpath(*key.segments)

    def _current_lstat(self, key: StorageObjectKey) -> os.stat_result | None:
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
        key: StorageObjectKey,
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
                named = _lstat_at(parent_descriptor, key.segments[-1])
                if named is None:
                    raise RuntimeError(f"{label} disappeared after verification")
                _require_managed_file_metadata(
                    named,
                    expected_signature=signature,
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

    def _require_current_authority(
        self,
        key: StorageObjectKey,
        *,
        expected_size: int,
        expected_signature: _Signature,
        label: str,
    ) -> _Signature:
        """Reuse a verified digest while the exact current inode is unchanged."""

        try:
            with _open_directory_chain(
                self._current,
                key.segments[:-1],
                create=False,
            ) as (root_descriptor, parent_descriptor):
                signature = _require_regular_authority_at(
                    parent_descriptor,
                    key.segments[-1],
                    expected_size=expected_size,
                    expected_signature=expected_signature,
                    allow_renamed_identity=False,
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
        key: StorageObjectKey,
        quarantine_leaf: str,
        *,
        expected_sha256: bytes,
        expected_size: int,
        expected_signature: _Signature,
        reuse_verified_digest: bool = False,
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
            try:
                _rename_noreplace(
                    key.segments[-1],
                    quarantine_leaf,
                    source_descriptor=parent_descriptor,
                    destination_descriptor=quarantine_descriptor,
                )
            except FileExistsError as error:
                raise RuntimeError(
                    "stale quarantine destination is occupied"
                ) from error
            os.fsync(quarantine_descriptor)
            os.fsync(parent_descriptor)
            try:
                if reuse_verified_digest:
                    captured = _require_regular_authority_at(
                        quarantine_descriptor,
                        quarantine_leaf,
                        expected_size=expected_size,
                        expected_signature=expected_signature,
                        allow_renamed_identity=True,
                        label="captured library artifact",
                    )
                else:
                    captured = _verify_regular_at(
                        quarantine_descriptor,
                        quarantine_leaf,
                        expected_sha256=expected_sha256,
                        expected_size=expected_size,
                        label="captured library artifact",
                    )
                captured_value = _lstat_at(
                    quarantine_descriptor,
                    quarantine_leaf,
                )
                if captured_value is None:
                    raise RuntimeError("captured library artifact disappeared")
                _require_managed_file_metadata(
                    captured_value,
                    expected_signature=captured,
                    label="captured library artifact",
                )
                if not _same_content_identity(captured, expected_signature):
                    raise RuntimeError(
                        "captured library artifact changed content identity"
                    )
            except BaseException as error:
                try:
                    _rename_noreplace(
                        quarantine_leaf,
                        key.segments[-1],
                        source_descriptor=quarantine_descriptor,
                        destination_descriptor=parent_descriptor,
                    )
                except FileExistsError:
                    # Both names are retained for operator inspection.  Never
                    # overwrite the entry that raced with the capture.
                    pass
                else:
                    os.fsync(parent_descriptor)
                    os.fsync(quarantine_descriptor)
                raise RuntimeError(
                    f"captured a foreign library path: {self._target(key)}"
                ) from error
            _require_chain_identity(
                self._current,
                key.segments[:-1],
                root_descriptor,
                parent_descriptor,
            )

    def _heal_recovered_capture_duplicate(
        self,
        key: StorageObjectKey,
        quarantine_leaf: str,
        *,
        digest: bytes,
        size: int,
        expected_signature: _Signature,
        label: str,
    ) -> _Signature:
        with (
            _open_directory_chain(
                self._current,
                key.segments[:-1],
                create=False,
            ) as (root_descriptor, parent_descriptor),
            _open_directory(self._quarantine) as quarantine_descriptor,
        ):
            survivor = _heal_same_inode_rename_duplicate(
                source_descriptor=parent_descriptor,
                source_leaf=key.segments[-1],
                destination_descriptor=quarantine_descriptor,
                destination_leaf=quarantine_leaf,
                expected_sha256=digest,
                expected_size=size,
                expected_identity=expected_signature,
                label=label,
            )
            _require_chain_identity(
                self._current,
                key.segments[:-1],
                root_descriptor,
                parent_descriptor,
            )
            return survivor

    def _fsync_recovered_capture(
        self,
        key: StorageObjectKey,
        quarantine_leaf: str,
        *,
        digest: bytes,
        size: int,
        expected_signature: _Signature,
        quarantine_present: bool,
        label: str,
    ) -> None:
        """Replay both sides of a response-lost current-to-quarantine rename."""

        with (
            _open_directory_chain(
                self._current,
                key.segments[:-1],
                create=False,
            ) as (root_descriptor, parent_descriptor),
            _open_directory(self._quarantine) as quarantine_descriptor,
        ):
            if _lstat_at(parent_descriptor, key.segments[-1]) is not None:
                raise RuntimeError(f"{label} retained its current path")
            quarantined = _lstat_at(quarantine_descriptor, quarantine_leaf)
            if (quarantined is not None) != quarantine_present:
                raise RuntimeError(f"{label} changed before directory sync")
            verified: _Signature | None = None
            if quarantined is not None:
                verified = _verify_and_fsync_file_at(
                    quarantine_descriptor,
                    quarantine_leaf,
                    expected_sha256=digest,
                    expected_size=size,
                    label=label,
                )
                if not _same_content_identity(verified, expected_signature):
                    raise RuntimeError(f"{label} changed content identity")
                named = _lstat_at(quarantine_descriptor, quarantine_leaf)
                if named is None:
                    raise RuntimeError(f"{label} disappeared before directory sync")
                _require_managed_file_metadata(
                    named,
                    expected_signature=verified,
                    label=label,
                )

            os.fsync(quarantine_descriptor)
            os.fsync(parent_descriptor)

            if _lstat_at(parent_descriptor, key.segments[-1]) is not None:
                raise RuntimeError(
                    f"{label} current path reappeared after directory sync"
                )
            durable = _lstat_at(quarantine_descriptor, quarantine_leaf)
            if (durable is not None) != quarantine_present:
                raise RuntimeError(f"{label} changed after directory sync")
            if durable is not None:
                durable_signature = _verify_regular_at(
                    quarantine_descriptor,
                    quarantine_leaf,
                    expected_sha256=digest,
                    expected_size=size,
                    label=f"durable {label}",
                )
                if durable_signature != verified:
                    raise RuntimeError(f"{label} changed across directory sync")
                named = _lstat_at(quarantine_descriptor, quarantine_leaf)
                if named is None:
                    raise RuntimeError(f"{label} disappeared after verification")
                _require_managed_file_metadata(
                    named,
                    expected_signature=durable_signature,
                    label=f"durable {label}",
                )
            _require_chain_identity(
                self._current,
                key.segments[:-1],
                root_descriptor,
                parent_descriptor,
            )

    def _verify_quarantined(
        self,
        leaf: str,
        *,
        digest: bytes,
        size: int,
        expected_signature: _Signature,
        label: str,
    ) -> _Signature:
        with _open_directory(self._quarantine) as descriptor:
            verified = _verify_regular_at(
                descriptor,
                leaf,
                expected_sha256=digest,
                expected_size=size,
                label=label,
            )
            if not _same_content_identity(verified, expected_signature):
                raise RuntimeError(f"{label} changed content identity")
            named = _lstat_at(descriptor, leaf)
            if named is None or _Signature.from_stat(named) != verified:
                raise RuntimeError(f"{label} changed after verification")
            _require_managed_file_metadata(
                named,
                expected_signature=verified,
                label=label,
            )
            return verified

    def _unlink_quarantined(
        self,
        leaf: str,
        *,
        digest: bytes,
        size: int,
        expected_signature: _Signature,
        label: str,
        reuse_verified_digest: bool = False,
    ) -> None:
        with _open_directory(self._quarantine) as descriptor:
            if _lstat_at(descriptor, leaf) is None:
                _fsync_absent_at(descriptor, leaf, label=label)
                return
            if reuse_verified_digest:
                _unlink_preserved_authority_at(
                    descriptor,
                    leaf,
                    expected_size=size,
                    expected_signature=expected_signature,
                    allow_renamed_identity=True,
                    label=label,
                )
                return
            verified = _verify_regular_at(
                descriptor,
                leaf,
                expected_sha256=digest,
                expected_size=size,
                label=label,
            )
            if not _same_content_identity(verified, expected_signature):
                raise RuntimeError(f"{label} changed content identity")
            named = _lstat_at(descriptor, leaf)
            if named is None:
                raise RuntimeError(f"{label} disappeared before unlink")
            _require_managed_file_metadata(
                named,
                expected_signature=verified,
                label=label,
            )
            _unlink_verified_at(
                descriptor,
                leaf,
                expected_sha256=digest,
                expected_size=size,
                expected_signature=verified,
                label=label,
            )

    def _write_marker(self, revision: int, receipt: bytes) -> None:
        payload = _marker_payload(revision, receipt)
        temporary = self._coordination / (
            f".{_ACTIVATING_MARKER_NAME}-{sha256(payload).hexdigest()}.tmp"
        )
        if self._marker_path.exists() or self._marker_path.is_symlink():
            self._verify_marker(revision, receipt)
            return
        with BytesIO(payload) as source:
            _publish_resumable_file(
                source,
                directory=self._coordination,
                temporary_leaf=temporary.name,
                final_leaf=self._marker_path.name,
                expected_sha256=sha256(payload).digest(),
                expected_size=len(payload),
                label="library ACTIVATING marker",
            )

    def _verify_marker(self, revision: int, receipt: bytes) -> _Signature:
        payload = _marker_payload(revision, receipt)
        temporary_leaf = f".{_ACTIVATING_MARKER_NAME}-{sha256(payload).hexdigest()}.tmp"
        with _open_directory(self._coordination) as descriptor:
            try:
                if _lstat_at(descriptor, temporary_leaf) is not None:
                    return _heal_same_inode_rename_duplicate(
                        source_descriptor=descriptor,
                        source_leaf=temporary_leaf,
                        destination_descriptor=descriptor,
                        destination_leaf=self._marker_path.name,
                        expected_sha256=sha256(payload).digest(),
                        expected_size=len(payload),
                        expected_identity=None,
                        label="recoverable library ACTIVATING marker publish",
                    )
                return _verify_and_fsync_published_at(
                    descriptor,
                    self._marker_path.name,
                    expected_sha256=sha256(payload).digest(),
                    expected_size=len(payload),
                    label="recoverable library ACTIVATING marker",
                )
            except RuntimeError as error:
                raise RuntimeError(
                    "library ACTIVATING marker has foreign contents or metadata"
                ) from error

    def _remove_marker(self, revision: int, receipt: bytes) -> None:
        payload = _marker_payload(revision, receipt)
        expected_signature = self._verify_marker(revision, receipt)
        with _open_directory(self._coordination) as descriptor:
            _unlink_verified_at(
                descriptor,
                self._marker_path.name,
                expected_sha256=sha256(payload).digest(),
                expected_size=len(payload),
                expected_signature=expected_signature,
                label="library ACTIVATING marker",
            )

    def _replay_removed_marker(self) -> None:
        with _open_directory(self._coordination) as descriptor:
            _fsync_absent_at(
                descriptor,
                self._marker_path.name,
                label="removed library ACTIVATING marker",
            )

    def _ensure_layout(self) -> None:
        validate_precreated_library_layout(self._root, durable=True)
        _ensure_managed_directory(
            self._state,
            _PRIVATE_DIRECTORY_CREATION_MODE,
            label="library state",
        )
        for path in (self._staging, self._quarantine, self._journal, self._locks):
            _ensure_managed_directory(
                path,
                _PRIVATE_DIRECTORY_CREATION_MODE,
                label=f"private library {path.name}",
            )
        validate_precreated_library_layout(self._root, durable=True)
        _require_directory(
            self._state,
            label="library state",
        )
        for path in (self._staging, self._quarantine, self._journal, self._locks):
            _require_directory(
                path,
                label=f"private library {path.name}",
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
        _ensure_managed_file(
            self._state_lock_path,
            _PRIVATE_FILE_CREATION_MODE,
            label="library state lock",
        )
        _ensure_managed_file(
            self._publication_lock_path,
            _PUBLIC_FILE_CREATION_MODE,
            label="library publication lock",
        )
        _ensure_managed_file(
            self._database_path,
            _PRIVATE_FILE_CREATION_MODE,
            label="library activation database",
        )
        with self._connection():
            pass

    @contextmanager
    def _exclusive_state(self) -> Iterator[sqlite3.Connection]:
        with self._state_process_lock:
            descriptor = os.open(
                self._state_lock_path,
                os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
            )
            try:
                _require_managed_file_descriptor(
                    self._state_lock_path,
                    descriptor,
                    label="library state lock",
                )
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                with self._connection() as connection:
                    yield connection
            finally:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                finally:
                    os.close(descriptor)

    def _acquire_publication_lock(self) -> None:
        with self._process_lock:
            if self._guard_owner != get_ident():
                raise RuntimeError("library activation requires publication_guard")
            if self._publication_descriptor is not None:
                return
        descriptor = os.open(
            self._publication_lock_path,
            os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            _require_managed_file_descriptor(
                self._publication_lock_path,
                descriptor,
                label="library publication lock",
            )
            fcntl.flock(descriptor, fcntl.LOCK_EX)
        except BaseException:
            os.close(descriptor)
            raise
        with self._process_lock:
            if self._guard_owner != get_ident():
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                finally:
                    os.close(descriptor)
                raise RuntimeError("library publication guard owner changed")
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

    @contextmanager
    def _publication_io_guard(self, *, exclusive: bool) -> Iterator[None]:
        """Fence storage mutation against activation without holding state.lock."""

        descriptor: int | None = None
        uses_existing_exclusive = False
        with self._process_lock:
            if (
                self._publication_descriptor is not None
                and self._guard_owner == get_ident()
            ):
                uses_existing_exclusive = True
            else:
                descriptor = os.open(
                    self._publication_lock_path,
                    os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
                )
                try:
                    _require_managed_file_descriptor(
                        self._publication_lock_path,
                        descriptor,
                        label="library publication lock",
                    )
                except BaseException:
                    os.close(descriptor)
                    raise
        if uses_existing_exclusive:
            yield
            return
        if descriptor is None:
            raise RuntimeError("library publication I/O guard lacks its descriptor")
        try:
            operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
            fcntl.flock(descriptor, operation)
            yield
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)

    @contextmanager
    def _protection_io_guard(self, token: bytes) -> Iterator[None]:
        """Serialize one bounded token stripe without blocking unrelated protects."""

        stripe = sha256(token).hexdigest()[:2]
        lock_path = self._locks / f"{_PROTECTION_LOCK_PREFIX}{stripe}.lock"
        _ensure_managed_file(
            lock_path,
            _PRIVATE_FILE_CREATION_MODE,
            label="library protection stripe lock",
        )
        descriptor = os.open(
            lock_path,
            os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            _require_managed_file_descriptor(
                lock_path,
                descriptor,
                label="library protection stripe lock",
            )
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)

    def _require_guard(self) -> None:
        with self._process_lock:
            if self._guard_owner != get_ident():
                raise RuntimeError("library activation requires publication_guard")

    def _require_publication_lock(self) -> None:
        with self._process_lock:
            if self._guard_owner != get_ident():
                raise RuntimeError("library activation requires publication_guard")
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
            existing_objects = connection.execute(
                "SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' "
                "ORDER BY name"
            ).fetchall()
            if existing_objects:
                if ("library_state",) not in existing_objects:
                    raise RuntimeError(
                        "unsupported library activation journal shape; a fresh "
                        "library root is required"
                    )
                try:
                    existing_version = connection.execute(
                        "SELECT format_version FROM library_state WHERE singleton = 1"
                    ).fetchone()
                except sqlite3.DatabaseError as error:
                    raise RuntimeError(
                        "unsupported library activation journal; a fresh library "
                        "root is required"
                    ) from error
                if existing_version != (2,):
                    raise RuntimeError(
                        "unsupported library activation journal format; a fresh "
                        "library root is required"
                    )
                _require_exact_journal_schema(connection)
            else:
                connection.executescript(_SCHEMA)
                _require_exact_journal_schema(connection)
            if connection.execute(
                "SELECT format_version FROM library_state WHERE singleton = 1"
            ).fetchone() != (2,):
                raise RuntimeError("unsupported library activation journal format")
            connection.commit()
            _ensure_managed_file(
                self._database_path,
                _PRIVATE_FILE_CREATION_MODE,
                label="library activation database",
                create=False,
            )
            yield connection
        finally:
            connection.close()


def _storage_key(value: StorageObjectKey) -> StorageObjectKey:
    return validate_storage_key(value)


def _require_exact_journal_schema(connection: sqlite3.Connection) -> None:
    """Reject every journal table/index shape outside the v2 closed world."""

    def signature(database: sqlite3.Connection) -> tuple[tuple[object, ...], ...]:
        return tuple(
            database.execute(
                "SELECT type, name, tbl_name, sql FROM sqlite_master "
                "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
            ).fetchall()
        )

    reference = sqlite3.connect(":memory:")
    try:
        reference.executescript(_SCHEMA)
        expected = signature(reference)
    finally:
        reference.close()
    if signature(connection) != expected:
        raise RuntimeError(
            "unsupported library activation journal shape; a fresh library "
            "root is required"
        )


def _require_source_component(value: object) -> str:
    if type(value) is not str:
        raise TypeError("source path component must be str")
    if value in {"", ".", ".."} or "/" in value or "\\" in value or "\0" in value:
        raise ValueError("source path component is unsafe")
    if len(value.encode("utf-8", errors="strict")) > 255:
        raise ValueError("source path component exceeds 255 UTF-8 bytes")
    return value


def _require_source_leaf(value: object) -> bytes:
    if type(value) is not bytes:
        raise TypeError("source_name must be bytes")
    if (
        value in {b"", b".", b".."}
        or b"/" in value
        or b"\\" in value
        or b"\0" in value
        or len(value) > 255
    ):
        raise ValueError("source_name is an unsafe leaf")
    return value


def _resource_kind(key: StorageObjectKey) -> CatalogResourceKind:
    return storage_key_resource_kind(key)


def _key_text(key: StorageObjectKey) -> str:
    return "/".join(key.segments)


def _gid_from_key(key: StorageObjectKey) -> int:
    return storage_key_gid(_storage_key(key))


def _key_from_row(codec: str, path: str) -> StorageObjectKey:
    return _storage_key(StorageObjectKey(codec, tuple(path.split("/"))))


def _key_from_path(path: str) -> StorageObjectKey:
    return _key_from_row(STORAGE_OBJECT_CODEC, path)


def _storage_suffix(key: StorageObjectKey) -> str:
    return ".cbz" if _resource_kind(key) is CatalogResourceKind.ACQUISITION else ".jpg"


def _stage_leaf(token: bytes, key: StorageObjectKey) -> str:
    return f"{sha256(token).hexdigest()}{_storage_suffix(key)}"


def _activation_key(item: VNextLibraryActivationItem) -> bytes:
    return VNextLibraryActivationCursor(
        item.publication_key,
        item.resource_kind,
    ).to_bytes()


def _activation_cursor_from_fields(
    publication_key: bytes,
    resource_kind: str,
) -> VNextLibraryActivationCursor:
    try:
        kind = CatalogResourceKind(resource_kind)
    except ValueError as error:
        raise RuntimeError("library journal resource kind is corrupt") from error
    return VNextLibraryActivationCursor(publication_key, kind)


def _digest(value: bytes) -> bytes:
    if type(value) is not bytes or len(value) != 32:
        raise ValueError("storage object SHA-256 must contain exactly 32 bytes")
    return value


def _size(value: int) -> int:
    if type(value) is not int or not 1 <= value < 1 << 63:
        raise ValueError("storage object size must be a positive signed int63")
    return value


def _modified_at(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("storage object modified_at must be timezone-aware datetime")
    if value.utcoffset() is None:
        raise ValueError("storage object modified_at has an invalid timezone")
    return value.astimezone(UTC)


def _token(value: bytes) -> bytes:
    if type(value) is not bytes or len(value) != 32:
        raise ValueError("artifact protection token must contain exactly 32 bytes")
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
        VNextLibraryActivationCursor.from_bytes(state.last_cursor)
        if status
        in {
            LibraryActivationStatus.SPOOL,
            LibraryActivationStatus.RECONCILE,
        }
        and state.last_cursor is not None
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
    if result.last_cursor is not None:
        if result.phase not in {"OPEN", "ACTIVATING"}:
            raise RuntimeError("library activation cursor is corrupt")
        try:
            VNextLibraryActivationCursor.from_bytes(result.last_cursor)
        except (TypeError, ValueError) as error:
            raise RuntimeError("library activation cursor is corrupt") from error
    if result.phase == "OPEN":
        maximum = connection.execute(
            "SELECT publication_key, resource_kind FROM pending_entries "
            "WHERE activation_revision = ? "
            "ORDER BY publication_key DESC, resource_kind DESC LIMIT 1",
            (result.pending_revision,),
        ).fetchone()
        expected_cursor = (
            None
            if maximum is None
            else _activation_cursor_from_fields(
                bytes(maximum[0]),
                str(maximum[1]),
            ).to_bytes()
        )
        if result.last_cursor != expected_cursor:
            raise RuntimeError(
                "library activation cursor lacks exact pending membership"
            )
    return result


def _storage_signature(row: Sequence[object], *, size: int) -> _Signature:
    """Build a signature where size is stored in the artifact fact column."""

    if len(row) != 4:
        raise RuntimeError("library journal storage signature has an invalid shape")
    return _Signature.from_row((row[0], row[1], size, row[2], row[3]))


def _stage_row_authorizes_partial_temporary(
    row: Sequence[object],
    *,
    token: bytes,
    key: StorageObjectKey,
) -> bool:
    """Recognize the durable WRITING shape retained across RELEASED cleanup."""

    if len(row) != 5:
        raise RuntimeError("staging cleanup authority has an invalid shape")
    leaf = row[0]
    signature = tuple(row[1:])
    if leaf is None:
        if any(value is not None for value in signature):
            raise RuntimeError("staging cleanup authority is incomplete")
        return False
    if str(leaf) != _stage_leaf(token, key):
        raise RuntimeError("staging cleanup authority contains an unsafe path")
    if all(value is None for value in signature):
        return True
    if any(value is None for value in signature):
        raise RuntimeError("staging cleanup authority is incomplete")
    return False


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


def _require_directory(path: Path, *, label: str) -> None:
    value = path.lstat()
    if not stat.S_ISDIR(value.st_mode) or stat.S_ISLNK(value.st_mode):
        raise RuntimeError(f"{label} is not a safe directory: {path}")


def _ensure_managed_directory(
    path: Path,
    creation_mode: int,
    *,
    label: str,
) -> None:
    leaf = path.name
    if not leaf or leaf in {".", ".."} or "/" in leaf:
        raise RuntimeError(f"managed directory has an unsafe name: {path}")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    with _open_directory(path.parent) as parent_descriptor:
        try:
            os.mkdir(leaf, creation_mode, dir_fd=parent_descriptor)
        except FileExistsError:
            pass
        try:
            child_descriptor = os.open(leaf, flags, dir_fd=parent_descriptor)
        except OSError as error:
            raise RuntimeError(
                f"managed directory is not safely openable: {path}"
            ) from error
        try:
            opened = os.fstat(child_descriptor)
            visible = os.stat(leaf, dir_fd=parent_descriptor, follow_symlinks=False)
            if (
                not stat.S_ISDIR(opened.st_mode)
                or stat.S_ISLNK(visible.st_mode)
                or (opened.st_dev, opened.st_ino) != (visible.st_dev, visible.st_ino)
            ):
                raise RuntimeError(f"managed directory identity is unsafe: {path}")
            os.fsync(child_descriptor)
            os.fsync(parent_descriptor)
            durable = os.fstat(child_descriptor)
            visible = os.stat(leaf, dir_fd=parent_descriptor, follow_symlinks=False)
            if (
                not stat.S_ISDIR(durable.st_mode)
                or stat.S_ISLNK(visible.st_mode)
                or (durable.st_dev, durable.st_ino) != (visible.st_dev, visible.st_ino)
            ):
                raise RuntimeError(f"managed directory changed identity: {path}")
        finally:
            os.close(child_descriptor)


def _ensure_managed_file(
    path: Path,
    creation_mode: int,
    *,
    label: str,
    create: bool = True,
) -> None:
    leaf = path.name
    if not leaf or leaf in {".", ".."} or "/" in leaf:
        raise RuntimeError(f"{label} has an unsafe name: {path}")
    flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
    with _open_directory(path.parent) as parent_descriptor:
        try:
            if not create:
                descriptor = os.open(leaf, flags, dir_fd=parent_descriptor)
            else:
                try:
                    descriptor = os.open(
                        leaf,
                        flags | os.O_CREAT | os.O_EXCL,
                        creation_mode,
                        dir_fd=parent_descriptor,
                    )
                except FileExistsError:
                    descriptor = os.open(leaf, flags, dir_fd=parent_descriptor)
        except OSError as error:
            raise RuntimeError(f"{label} is not safely openable: {path}") from error
        try:
            _require_managed_file_descriptor(
                path,
                descriptor,
                label=label,
                parent_descriptor=parent_descriptor,
            )
            os.fsync(descriptor)
            os.fsync(parent_descriptor)
            _require_managed_file_descriptor(
                path,
                descriptor,
                label=label,
                parent_descriptor=parent_descriptor,
            )
        finally:
            os.close(descriptor)


def _require_managed_file_descriptor(
    path: Path,
    descriptor: int,
    *,
    label: str,
    parent_descriptor: int | None = None,
) -> None:
    opened = os.fstat(descriptor)
    if parent_descriptor is None:
        visible = path.lstat()
    else:
        visible = os.stat(
            path.name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
    if (
        not stat.S_ISREG(opened.st_mode)
        or stat.S_ISLNK(visible.st_mode)
        or (opened.st_dev, opened.st_ino) != (visible.st_dev, visible.st_ino)
        or opened.st_nlink != 1
    ):
        raise RuntimeError(f"{label} changed durable identity: {path}")


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
                        _PUBLIC_DIRECTORY_CREATION_MODE,
                        dir_fd=current_descriptor,
                    )
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
            visible = os.stat(
                component,
                dir_fd=current_descriptor,
                follow_symlinks=False,
            )
            if (
                not stat.S_ISDIR(opened.st_mode)
                or stat.S_ISLNK(visible.st_mode)
                or (opened.st_dev, opened.st_ino) != (visible.st_dev, visible.st_ino)
            ):
                os.close(next_descriptor)
                raise RuntimeError(f"library shard identity is unsafe: {component}")
            if create:
                os.fsync(next_descriptor)
                os.fsync(current_descriptor)
                durable = os.fstat(next_descriptor)
                visible = os.stat(
                    component,
                    dir_fd=current_descriptor,
                    follow_symlinks=False,
                )
                if (durable.st_dev, durable.st_ino) != (visible.st_dev, visible.st_ino):
                    os.close(next_descriptor)
                    raise RuntimeError(f"library shard changed identity: {component}")
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


def _require_managed_file_metadata(
    value: os.stat_result,
    *,
    expected_signature: _Signature,
    label: str,
) -> None:
    if (
        _Signature.from_stat(value) != expected_signature
        or not stat.S_ISREG(value.st_mode)
        or value.st_nlink != 1
    ):
        raise RuntimeError(f"{label} changed exact metadata authority")


def _require_regular_authority_at(
    descriptor: int,
    leaf: str,
    *,
    expected_size: int,
    expected_signature: _Signature,
    allow_renamed_identity: bool,
    label: str,
) -> _Signature:
    """Reuse a verified digest only while its private inode authority is intact."""

    value = _lstat_at(descriptor, leaf)
    if value is None or value.st_size != expected_size or value.st_nlink != 1:
        raise RuntimeError(f"{label} changed durable inode authority")
    observed = _Signature.from_stat(value)
    matches = (
        _same_content_identity(observed, expected_signature)
        if allow_renamed_identity
        else observed == expected_signature
    )
    if not matches:
        raise RuntimeError(f"{label} changed durable inode authority")
    return observed


def _require_regular_path_authority(
    path: Path,
    *,
    expected_size: int,
    expected_signature: _Signature,
    label: str,
) -> _Signature:
    with _open_directory(path.parent) as descriptor:
        return _require_regular_authority_at(
            descriptor,
            path.name,
            expected_size=expected_size,
            expected_signature=expected_signature,
            allow_renamed_identity=False,
            label=label,
        )


def _publish_resumable_file(
    source: BinaryIO,
    *,
    directory: Path,
    temporary_leaf: str,
    final_leaf: str,
    expected_sha256: bytes,
    expected_size: int,
    label: str,
) -> _Signature:
    """Resume one private exact prefix and publish it without overwriting a leaf."""

    source.seek(0)
    file_descriptor: int | None = None
    try:
        with _open_directory(directory) as directory_descriptor:
            flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
            written_signature: _Signature
            try:
                file_descriptor = os.open(
                    temporary_leaf,
                    flags | os.O_CREAT | os.O_EXCL,
                    _PUBLIC_FILE_CREATION_MODE,
                    dir_fd=directory_descriptor,
                )
            except FileExistsError:
                file_descriptor = os.open(
                    temporary_leaf,
                    flags,
                    dir_fd=directory_descriptor,
                )
            if file_descriptor is not None:
                fcntl.flock(file_descriptor, fcntl.LOCK_EX)
                opened = os.fstat(file_descriptor)
                opened_signature = _Signature.from_stat(opened)
                _require_managed_file_metadata(
                    opened,
                    expected_signature=opened_signature,
                    label=f"{label} temporary",
                )
                named = _lstat_at(directory_descriptor, temporary_leaf)
                if named is None:
                    raise RuntimeError(f"{label} temporary disappeared")
                _require_managed_file_metadata(
                    named,
                    expected_signature=opened_signature,
                    label=f"{label} temporary",
                )
                if opened.st_size > expected_size:
                    raise RuntimeError(f"{label} temporary exceeds expected size")

                observed_digest = sha256()
                with os.fdopen(file_descriptor, "r+b", closefd=False) as destination:
                    destination.seek(0)
                    remaining = opened.st_size
                    while remaining:
                        part = destination.read(min(_COPY_BUFFER_BYTES, remaining))
                        if not isinstance(part, bytes) or not part:
                            raise RuntimeError(f"{label} temporary could not be read")
                        expected_part = source.read(len(part))
                        if not isinstance(expected_part, bytes):
                            raise TypeError(f"{label} source must yield bytes")
                        if expected_part != part:
                            raise RuntimeError(
                                f"{label} temporary is not an exact source prefix"
                            )
                        observed_digest.update(part)
                        remaining -= len(part)

                    size_bytes = opened.st_size
                    destination.seek(size_bytes)
                    while part := source.read(_COPY_BUFFER_BYTES):
                        if not isinstance(part, bytes):
                            raise TypeError(f"{label} source must yield bytes")
                        if size_bytes + len(part) > expected_size:
                            raise RuntimeError(f"{label} source exceeds expected size")
                        observed_digest.update(part)
                        _write_all(destination, part, label=label)
                        size_bytes += len(part)
                    if size_bytes != expected_size:
                        raise RuntimeError(f"{label} source has an unexpected size")
                    if observed_digest.digest() != expected_sha256:
                        raise RuntimeError(f"{label} source has an unexpected digest")
                    destination.flush()
                    os.fsync(destination.fileno())
                after = os.fstat(file_descriptor)
                if (after.st_dev, after.st_ino) != (opened.st_dev, opened.st_ino):
                    raise RuntimeError(f"{label} temporary changed open identity")
                written_signature = _Signature.from_stat(after)
                _require_managed_file_metadata(
                    after,
                    expected_signature=written_signature,
                    label=f"{label} temporary",
                )
            else:  # pragma: no cover - both os.open branches assign or raise
                raise RuntimeError(f"{label} temporary could not be opened")
            os.fsync(directory_descriptor)
            try:
                temporary_signature = _require_regular_authority_at(
                    directory_descriptor,
                    temporary_leaf,
                    expected_size=expected_size,
                    expected_signature=written_signature,
                    allow_renamed_identity=False,
                    label=f"{label} temporary",
                )
            except RuntimeError as error:
                raise RuntimeError(
                    f"{label} temporary changed durable identity before publish"
                ) from error
            if temporary_signature != written_signature:
                raise RuntimeError(
                    f"{label} temporary changed durable identity before publish"
                )
            named = _lstat_at(directory_descriptor, temporary_leaf)
            if named is None:
                raise RuntimeError(f"{label} temporary disappeared after verification")
            _require_managed_file_metadata(
                named,
                expected_signature=temporary_signature,
                label=f"{label} temporary",
            )
            try:
                _rename_noreplace(
                    temporary_leaf,
                    final_leaf,
                    source_descriptor=directory_descriptor,
                    destination_descriptor=directory_descriptor,
                )
            except FileExistsError as error:
                raise RuntimeError(f"{label} destination appeared") from error
            os.fsync(directory_descriptor)
            try:
                installed = _require_regular_authority_at(
                    directory_descriptor,
                    final_leaf,
                    expected_size=expected_size,
                    expected_signature=temporary_signature,
                    allow_renamed_identity=True,
                    label=f"published {label}",
                )
                installed_value = _lstat_at(directory_descriptor, final_leaf)
                if installed_value is None:
                    raise RuntimeError(f"published {label} disappeared")
                _require_managed_file_metadata(
                    installed_value,
                    expected_signature=installed,
                    label=f"published {label}",
                )
                if not _same_content_identity(installed, temporary_signature):
                    raise RuntimeError(f"published {label} changed content identity")
            except BaseException as error:
                try:
                    _rename_noreplace(
                        final_leaf,
                        temporary_leaf,
                        source_descriptor=directory_descriptor,
                        destination_descriptor=directory_descriptor,
                    )
                except FileExistsError, FileNotFoundError, RuntimeError, OSError:
                    pass
                else:
                    os.fsync(directory_descriptor)
                raise RuntimeError(f"published {label} is foreign") from error
            return installed
    finally:
        if file_descriptor is not None:
            try:
                fcntl.flock(file_descriptor, fcntl.LOCK_UN)
            finally:
                os.close(file_descriptor)
        source.seek(0)


def _write_all(destination: BinaryIO, content: bytes, *, label: str) -> None:
    view = memoryview(content)
    offset = 0
    while offset < len(view):
        written = destination.write(view[offset:])
        if type(written) is not int or written <= 0 or written > len(view) - offset:
            raise RuntimeError(f"{label} destination write made no progress")
        offset += written


def _capture_regular_at(
    descriptor: int,
    leaf: str,
    *,
    maximum_size: int,
    label: str,
) -> tuple[bytes, _Signature]:
    before = _lstat_at(descriptor, leaf)
    if before is None or before.st_size > maximum_size:
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
    return digest.digest(), _Signature.from_stat(after)


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


def _verify_and_fsync_published_at(
    descriptor: int,
    leaf: str,
    *,
    expected_sha256: bytes,
    expected_size: int,
    label: str,
) -> _Signature:
    """Replay directory durability before accepting a response-lost publish."""

    verified = _verify_and_fsync_file_at(
        descriptor,
        leaf,
        expected_sha256=expected_sha256,
        expected_size=expected_size,
        label=label,
    )
    named = _lstat_at(descriptor, leaf)
    if named is None:
        raise RuntimeError(f"{label} disappeared before directory sync")
    _require_managed_file_metadata(
        named,
        expected_signature=verified,
        label=label,
    )
    os.fsync(descriptor)
    durable = _verify_regular_at(
        descriptor,
        leaf,
        expected_sha256=expected_sha256,
        expected_size=expected_size,
        label=f"durable {label}",
    )
    named = _lstat_at(descriptor, leaf)
    if named is None:
        raise RuntimeError(f"{label} disappeared after directory sync")
    _require_managed_file_metadata(
        named,
        expected_signature=durable,
        label=f"durable {label}",
    )
    if durable != verified:
        raise RuntimeError(f"{label} changed across directory sync")
    return durable


def _verify_and_fsync_file_at(
    descriptor: int,
    leaf: str,
    *,
    expected_sha256: bytes,
    expected_size: int,
    label: str,
) -> _Signature:
    """Fsync one exact managed inode through its descriptor-relative name."""

    verified = _verify_regular_at(
        descriptor,
        leaf,
        expected_sha256=expected_sha256,
        expected_size=expected_size,
        label=label,
    )
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    file_descriptor = os.open(leaf, flags, dir_fd=descriptor)
    try:
        opened = os.fstat(file_descriptor)
        named = _lstat_at(descriptor, leaf)
        if named is None or _Signature.from_stat(opened) != verified:
            raise RuntimeError(f"{label} changed before inode sync")
        _require_managed_file_metadata(
            opened,
            expected_signature=verified,
            label=label,
        )
        _require_managed_file_metadata(
            named,
            expected_signature=verified,
            label=label,
        )
        os.fsync(file_descriptor)
        durable = os.fstat(file_descriptor)
        named = _lstat_at(descriptor, leaf)
        if (
            named is None
            or _Signature.from_stat(durable) != verified
            or _Signature.from_stat(named) != verified
        ):
            raise RuntimeError(f"{label} changed across inode sync")
        _require_managed_file_metadata(
            durable,
            expected_signature=verified,
            label=f"durable {label}",
        )
        return verified
    finally:
        os.close(file_descriptor)


def _heal_same_inode_rename_duplicate(
    *,
    source_descriptor: int,
    source_leaf: str,
    destination_descriptor: int,
    destination_leaf: str,
    expected_sha256: bytes,
    expected_size: int,
    expected_identity: _Signature | None,
    label: str,
) -> _Signature:
    """Collapse a crash-replayed rename only when both names are one exact inode."""

    duplicate = _verify_same_inode_rename_names(
        source_descriptor=source_descriptor,
        source_leaf=source_leaf,
        destination_descriptor=destination_descriptor,
        destination_leaf=destination_leaf,
        expected_sha256=expected_sha256,
        expected_size=expected_size,
        expected_identity=expected_identity,
        label=label,
    )
    os.fsync(destination_descriptor)
    if source_descriptor != destination_descriptor:
        os.fsync(source_descriptor)
    durable_duplicate = _verify_same_inode_rename_names(
        source_descriptor=source_descriptor,
        source_leaf=source_leaf,
        destination_descriptor=destination_descriptor,
        destination_leaf=destination_leaf,
        expected_sha256=expected_sha256,
        expected_size=expected_size,
        expected_identity=duplicate,
        label=f"durable {label}",
    )
    if durable_duplicate != duplicate:
        raise RuntimeError(f"{label} changed across directory sync")
    os.unlink(source_leaf, dir_fd=source_descriptor)
    survivor = _verify_and_fsync_file_at(
        destination_descriptor,
        destination_leaf,
        expected_sha256=expected_sha256,
        expected_size=expected_size,
        label=f"surviving {label}",
    )
    if not _same_content_identity(survivor, durable_duplicate):
        raise RuntimeError(f"{label} survivor changed content identity")
    os.fsync(source_descriptor)
    if source_descriptor != destination_descriptor:
        os.fsync(destination_descriptor)
    if _lstat_at(source_descriptor, source_leaf) is not None:
        raise RuntimeError(f"{label} retained its duplicate source name")
    durable_survivor = _verify_regular_at(
        destination_descriptor,
        destination_leaf,
        expected_sha256=expected_sha256,
        expected_size=expected_size,
        label=f"surviving {label}",
    )
    if durable_survivor != survivor:
        raise RuntimeError(f"{label} survivor changed across directory sync")
    named = _lstat_at(destination_descriptor, destination_leaf)
    if named is None:
        raise RuntimeError(f"{label} survivor disappeared")
    _require_managed_file_metadata(
        named,
        expected_signature=durable_survivor,
        label=f"surviving {label}",
    )
    return durable_survivor


def _verify_same_inode_rename_names(
    *,
    source_descriptor: int,
    source_leaf: str,
    destination_descriptor: int,
    destination_leaf: str,
    expected_sha256: bytes,
    expected_size: int,
    expected_identity: _Signature | None,
    label: str,
) -> _Signature:
    source = _verify_regular_at(
        source_descriptor,
        source_leaf,
        expected_sha256=expected_sha256,
        expected_size=expected_size,
        label=f"{label} source",
    )
    destination = _verify_regular_at(
        destination_descriptor,
        destination_leaf,
        expected_sha256=expected_sha256,
        expected_size=expected_size,
        label=f"{label} destination",
    )
    if source != destination:
        raise RuntimeError(f"{label} names do not share exact inode authority")
    if expected_identity is not None and not _same_content_identity(
        source,
        expected_identity,
    ):
        raise RuntimeError(f"{label} differs from durable inode authority")
    for descriptor, leaf in (
        (destination_descriptor, destination_leaf),
        (source_descriptor, source_leaf),
    ):
        named = _lstat_at(descriptor, leaf)
        if (
            named is None
            or _Signature.from_stat(named) != source
            or named.st_nlink != 2
        ):
            raise RuntimeError(f"{label} changed shared-link authority")
    return source


def _fsync_absent_at(descriptor: int, leaf: str, *, label: str) -> None:
    """Make a response-lost unlink durable before retiring its authority."""

    if _lstat_at(descriptor, leaf) is not None:
        raise RuntimeError(f"{label} reappeared before absence sync")
    os.fsync(descriptor)
    if _lstat_at(descriptor, leaf) is not None:
        raise RuntimeError(f"{label} reappeared after absence sync")


def _unlink_verified_at(
    descriptor: int,
    leaf: str,
    *,
    expected_sha256: bytes,
    expected_size: int,
    expected_signature: _Signature,
    label: str,
) -> None:
    """Unlink one private leaf only while its exact verified name is stable."""

    verified = _verify_regular_at(
        descriptor,
        leaf,
        expected_sha256=expected_sha256,
        expected_size=expected_size,
        label=label,
    )
    if verified != expected_signature:
        raise RuntimeError(f"{label} changed before unlink")
    named = _lstat_at(descriptor, leaf)
    if named is None or _Signature.from_stat(named) != verified:
        raise RuntimeError(f"{label} changed immediately before unlink")
    os.unlink(leaf, dir_fd=descriptor)
    os.fsync(descriptor)


def _unlink_preserved_authority_at(
    descriptor: int,
    leaf: str,
    *,
    expected_size: int,
    expected_signature: _Signature,
    allow_renamed_identity: bool,
    label: str,
) -> None:
    """Unlink private bytes whose previously verified inode stayed authoritative."""

    observed = _require_regular_authority_at(
        descriptor,
        leaf,
        expected_size=expected_size,
        expected_signature=expected_signature,
        allow_renamed_identity=allow_renamed_identity,
        label=label,
    )
    named = _lstat_at(descriptor, leaf)
    if named is None or _Signature.from_stat(named) != observed:
        raise RuntimeError(f"{label} changed immediately before unlink")
    os.unlink(leaf, dir_fd=descriptor)
    os.fsync(descriptor)


def _rename_noreplace(
    source: str,
    destination: str,
    *,
    source_descriptor: int,
    destination_descriptor: int,
) -> None:
    """Atomically rename one safe leaf and fail if destination already exists."""

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
    syscall_number: int | None = None
    if sys.platform == "darwin":
        function = getattr(library, "renameatx_np", None)
        flag = 0x00000004  # RENAME_EXCL
    elif sys.platform.startswith("linux"):
        function = getattr(library, "renameat2", None)
        flag = 0x00000001  # RENAME_NOREPLACE
        if function is None:
            machine = os.uname().machine.casefold()
            syscall_number = {
                "aarch64": 276,
                "arm64": 276,
                "amd64": 316,
                "x86_64": 316,
            }.get(machine)
            function = getattr(library, "syscall", None)
    else:
        function = None
        flag = 0
    if function is None or (
        sys.platform.startswith("linux")
        and getattr(library, "renameat2", None) is None
        and syscall_number is None
    ):
        raise RuntimeError("atomic no-replace rename is unavailable")
    if syscall_number is None:
        function.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        function.restype = ctypes.c_int
        arguments: tuple[object, ...] = (
            source_descriptor,
            os.fsencode(source),
            destination_descriptor,
            os.fsencode(destination),
            flag,
        )
    else:
        function.argtypes = [
            ctypes.c_long,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        function.restype = ctypes.c_long
        arguments = (
            syscall_number,
            source_descriptor,
            os.fsencode(source),
            destination_descriptor,
            os.fsencode(destination),
            flag,
        )
    ctypes.set_errno(0)
    result = function(*arguments)
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
    key = _key_from_path(storage_path)
    framed = sha256()
    framed.update(b"h2hdb-library-quarantine-v2\0")
    framed.update(storage_path.encode("ascii"))
    framed.update(b"\0")
    framed.update(digest)
    return f"{framed.hexdigest()}{_storage_suffix(key)}"


_SCHEMA = """
CREATE TABLE IF NOT EXISTS library_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    format_version INTEGER NOT NULL CHECK (format_version = 2),
    current_revision INTEGER NULL,
    current_receipt_id BLOB NULL,
    pending_revision INTEGER NULL,
    pending_receipt_id BLOB NULL,
    phase TEXT NOT NULL CHECK (
        phase IN ('IDLE', 'OPEN', 'SEALED', 'ACTIVATING', 'READY')
    ),
    last_cursor BLOB NULL CHECK (
        last_cursor IS NULL OR length(last_cursor) = 33
    )
);
INSERT OR IGNORE INTO library_state
    (singleton, format_version, phase) VALUES (1, 2, 'IDLE');
CREATE TABLE IF NOT EXISTS protection_tokens (
    token BLOB PRIMARY KEY CHECK (length(token) = 32),
    storage_codec TEXT NOT NULL,
    storage_path TEXT NOT NULL,
    object_sha256 BLOB NOT NULL CHECK (length(object_sha256) = 32),
    size_bytes INTEGER NOT NULL CHECK (size_bytes > 0),
    published_modified_at TEXT NULL CHECK (
        published_modified_at IS NULL OR length(published_modified_at) > 0
    ),
    state TEXT NOT NULL CHECK (
        state IN ('WRITING', 'STAGED', 'INSTALLED', 'RELEASED')
    ),
    staging_leaf TEXT NULL,
    device BLOB NULL,
    inode BLOB NULL,
    modified_ns INTEGER NULL,
    changed_ns INTEGER NULL
);
CREATE INDEX IF NOT EXISTS protection_object_idx ON protection_tokens (
    storage_codec, storage_path, object_sha256, size_bytes,
    published_modified_at, state
);
CREATE UNIQUE INDEX IF NOT EXISTS protection_one_active_stage_idx
    ON protection_tokens(storage_path)
    WHERE state IN ('WRITING', 'STAGED');
CREATE TABLE IF NOT EXISTS current_entries (
    publication_key BLOB NOT NULL CHECK (length(publication_key) = 32),
    resource_kind TEXT NOT NULL CHECK (
        resource_kind IN ('acquisition', 'thumbnail')
    ),
    storage_path TEXT NOT NULL UNIQUE,
    storage_codec TEXT NOT NULL,
    gid INTEGER NOT NULL,
    object_sha256 BLOB NOT NULL CHECK (length(object_sha256) = 32),
    size_bytes INTEGER NOT NULL CHECK (size_bytes > 0),
    published_modified_at TEXT NOT NULL CHECK (length(published_modified_at) > 0),
    device BLOB NOT NULL CHECK (length(device) = 8),
    inode BLOB NOT NULL CHECK (length(inode) = 8),
    modified_ns INTEGER NOT NULL,
    changed_ns INTEGER NOT NULL,
    PRIMARY KEY (publication_key, resource_kind),
    UNIQUE (gid, resource_kind)
);
CREATE TABLE IF NOT EXISTS pending_entries (
    activation_revision INTEGER NOT NULL,
    publication_key BLOB NOT NULL CHECK (length(publication_key) = 32),
    gid INTEGER NOT NULL,
    resource_kind TEXT NOT NULL CHECK (
        resource_kind IN ('acquisition', 'thumbnail')
    ),
    storage_codec TEXT NOT NULL,
    storage_path TEXT NOT NULL,
    object_sha256 BLOB NOT NULL CHECK (length(object_sha256) = 32),
    size_bytes INTEGER NOT NULL CHECK (size_bytes > 0),
    published_modified_at TEXT NOT NULL CHECK (length(published_modified_at) > 0),
    operation_started INTEGER NOT NULL CHECK (operation_started IN (0, 1)),
    activated INTEGER NOT NULL CHECK (activated IN (0, 1)),
    device BLOB NULL,
    inode BLOB NULL,
    modified_ns INTEGER NULL,
    changed_ns INTEGER NULL,
    PRIMARY KEY (activation_revision, publication_key, resource_kind),
    UNIQUE (activation_revision, gid, resource_kind),
    UNIQUE (activation_revision, storage_path)
);
CREATE INDEX IF NOT EXISTS pending_entries_activation_idx
    ON pending_entries(
        activation_revision, activated, publication_key, resource_kind
    );
CREATE TABLE IF NOT EXISTS pending_removals (
    activation_revision INTEGER NOT NULL,
    publication_key BLOB NOT NULL CHECK (length(publication_key) = 32),
    resource_kind TEXT NOT NULL CHECK (
        resource_kind IN ('acquisition', 'thumbnail')
    ),
    storage_codec TEXT NOT NULL,
    storage_path TEXT NOT NULL,
    object_sha256 BLOB NOT NULL CHECK (length(object_sha256) = 32),
    size_bytes INTEGER NOT NULL CHECK (size_bytes > 0),
    device BLOB NOT NULL CHECK (length(device) = 8),
    inode BLOB NOT NULL CHECK (length(inode) = 8),
    modified_ns INTEGER NOT NULL,
    changed_ns INTEGER NOT NULL,
    operation_started INTEGER NOT NULL CHECK (operation_started IN (0, 1)),
    PRIMARY KEY (activation_revision, publication_key, resource_kind),
    UNIQUE (activation_revision, storage_path)
);
"""
