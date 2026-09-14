"""Shared real-filesystem library fixtures for activation safety tests."""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from pathlib import Path

from h2hdb import (
    CatalogResourceKind,
    LibraryActivationStatus,
    StorageObjectDescriptor,
    StorageObjectKey,
    VNextLibraryActivationItem,
)

from h2hdb_ingest.artifact import ArtifactRenderPolicy
from h2hdb_ingest.library import ManagedFilesystemLibraryAdapter
from h2hdb_ingest.storage import acquisition_storage_key, thumbnail_storage_key

_MODIFIED_AT = datetime(2026, 1, 1, tzinfo=UTC)

_RENDER_POLICY = ArtifactRenderPolicy()


class _ActivationItem(VNextLibraryActivationItem):
    @property
    def storage_key(self) -> StorageObjectKey:
        return self.storage_object.key

    @property
    def artifact_sha256(self) -> bytes:
        return bytes.fromhex(self.storage_object.sha256)

    @property
    def size_bytes(self) -> int:
        return self.storage_object.size_bytes


def _publication_key(gid: int) -> bytes:
    digest = sha256(b"h2hdb-vnext-publication-key\0")
    digest.update((1).to_bytes(4, "big"))
    digest.update(gid.to_bytes(8, "big"))
    return digest.digest()


def _item(gid: int, payload: bytes) -> _ActivationItem:
    return _item_kind(gid, payload, CatalogResourceKind.ACQUISITION)


def _item_kind(
    gid: int,
    payload: bytes,
    resource_kind: CatalogResourceKind,
) -> _ActivationItem:
    storage_key = (
        acquisition_storage_key(gid)
        if resource_kind is CatalogResourceKind.ACQUISITION
        else thumbnail_storage_key(gid)
    )
    return _ActivationItem(
        publication_key=_publication_key(gid),
        gid=gid,
        resource_kind=resource_kind,
        storage_object=StorageObjectDescriptor(
            key=storage_key,
            size_bytes=len(payload),
            sha256=sha256(payload).hexdigest(),
            modified_at=_MODIFIED_AT,
        ),
    )


def _adapter(root: Path) -> ManagedFilesystemLibraryAdapter:
    if not root.exists():
        _provision_library_root(root)
    return ManagedFilesystemLibraryAdapter(
        root,
        source_root=_source_root(root),
        render_policy=_RENDER_POLICY,
    )


def _source_root(root: Path) -> Path:
    source = root.parent / "download-source"
    source.mkdir(exist_ok=True)
    return source


def _provision_library_root(root: Path) -> None:
    root.mkdir(mode=0o777, exist_ok=True)
    root.chmod(0o777)
    current = root / "current"
    for path in (
        current,
        current / "acquisitions",
        current / "artwork",
        root / ".h2hdb-coordination",
    ):
        path.mkdir(mode=0o777, exist_ok=True)
        path.chmod(0o777)


def _protect(
    adapter: ManagedFilesystemLibraryAdapter,
    item: _ActivationItem,
    payload: bytes,
    token_byte: int,
) -> bytes:
    token = bytes((token_byte,)) * 32
    evidence = adapter.protect(
        BytesIO(payload),
        item.storage_key,
        item.artifact_sha256,
        item.size_bytes,
        item.storage_object.modified_at,
        token,
    )
    assert evidence.stored
    return token


def _activate(
    adapter: ManagedFilesystemLibraryAdapter,
    revision: int,
    receipt: bytes,
    items: tuple[VNextLibraryActivationItem, ...],
) -> None:
    with adapter.publication_guard():
        checkpoint = adapter.begin(revision, receipt)
        assert checkpoint.status is LibraryActivationStatus.SPOOL
        adapter.activate_page(revision, items)
        adapter.seal(revision)
        while True:
            checkpoint = adapter.reconcile_page(revision, receipt, limit=128)
            if checkpoint.status is LibraryActivationStatus.READY:
                break
        adapter.complete(revision, receipt)
