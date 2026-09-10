"""Ingest-owned presentation-v2 storage-key codec."""

from __future__ import annotations

__all__ = [
    "STORAGE_OBJECT_CODEC",
    "acquisition_storage_key",
    "artifact_name",
    "storage_key_gid",
    "storage_key_resource_kind",
    "thumbnail_storage_key",
    "validate_storage_key",
]

from h2hdb import CatalogResourceKind, StorageObjectKey

from ._storage_paths import (
    STORAGE_OBJECT_CODEC,
    artifact_name,
    path_gid,
    path_kind,
    storage_path,
    validate_storage_path,
)


def acquisition_storage_key(gid: int) -> StorageObjectKey:
    """Return the sole acquisition path for a positive signed-int63 GID."""

    return StorageObjectKey(
        STORAGE_OBJECT_CODEC,
        tuple(storage_path(gid, "acquisition").split("/")),
    )


def thumbnail_storage_key(gid: int) -> StorageObjectKey:
    """Return the sole thumbnail-320 path for a positive signed-int63 GID."""

    return StorageObjectKey(
        STORAGE_OBJECT_CODEC,
        tuple(storage_path(gid, "thumbnail").split("/")),
    )


def validate_storage_key(value: StorageObjectKey) -> StorageObjectKey:
    """Fail closed unless ``value`` is exactly reproducible by this codec."""

    if type(value) is not StorageObjectKey:
        raise TypeError("storage_key must be StorageObjectKey")
    value.__post_init__()
    validate_storage_path(value.codec, "/".join(value.segments))
    return value


def storage_key_resource_kind(value: StorageObjectKey) -> CatalogResourceKind:
    return CatalogResourceKind(path_kind(value.codec, value.segments))


def storage_key_gid(value: StorageObjectKey) -> int:
    return path_gid(value.codec, value.segments)
