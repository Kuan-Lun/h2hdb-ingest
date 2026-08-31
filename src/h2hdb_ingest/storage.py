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

from hashlib import sha256

from h2hdb import CatalogResourceKind, StorageObjectKey

STORAGE_OBJECT_CODEC = "managed-filesystem-v2"
_SHARD_DOMAIN = b"h2hdb-storage-object-shard-v2\0"


def acquisition_storage_key(gid: int) -> StorageObjectKey:
    """Return the sole acquisition path for a positive signed-int63 GID."""

    exact_gid, shard = _gid_and_shard(gid)
    return StorageObjectKey(
        STORAGE_OBJECT_CODEC,
        (
            "acquisitions",
            "hash-v2",
            shard[:2],
            shard[2],
            artifact_name(exact_gid),
        ),
    )


def thumbnail_storage_key(gid: int) -> StorageObjectKey:
    """Return the sole thumbnail-320 path for a positive signed-int63 GID."""

    exact_gid, shard = _gid_and_shard(gid)
    return StorageObjectKey(
        STORAGE_OBJECT_CODEC,
        (
            "artwork",
            "hash-v2",
            shard[:2],
            shard[2],
            artifact_name(exact_gid).removesuffix(".cbz"),
            "thumbnail-320.jpg",
        ),
    )


def artifact_name(gid: int) -> str:
    """Return the sole user-visible acquisition download name."""

    return f"h2h-{_require_gid(gid)}.cbz"


def validate_storage_key(value: StorageObjectKey) -> StorageObjectKey:
    """Fail closed unless ``value`` is exactly reproducible by this codec."""

    if type(value) is not StorageObjectKey:
        raise TypeError("storage_key must be StorageObjectKey")
    value.__post_init__()
    kind = storage_key_resource_kind(value)
    gid = storage_key_gid(value)
    expected = (
        acquisition_storage_key(gid)
        if kind is CatalogResourceKind.ACQUISITION
        else thumbnail_storage_key(gid)
    )
    if value != expected:
        raise ValueError("storage key disagrees with the ingest v2 codec")
    return value


def storage_key_resource_kind(value: StorageObjectKey) -> CatalogResourceKind:
    if value.codec != STORAGE_OBJECT_CODEC:
        raise ValueError("storage key is not the ingest presentation-v2 codec")
    if len(value.segments) == 5 and value.segments[:2] == (
        "acquisitions",
        "hash-v2",
    ):
        return CatalogResourceKind.ACQUISITION
    if (
        len(value.segments) == 6
        and value.segments[:2] == ("artwork", "hash-v2")
        and value.segments[-1] == "thumbnail-320.jpg"
    ):
        return CatalogResourceKind.THUMBNAIL
    raise ValueError("storage key has an unsupported presentation-v2 shape")


def storage_key_gid(value: StorageObjectKey) -> int:
    kind = storage_key_resource_kind(value)
    leaf = (
        value.segments[-1]
        if kind is CatalogResourceKind.ACQUISITION
        else value.segments[-2]
    )
    suffix = ".cbz" if kind is CatalogResourceKind.ACQUISITION else ""
    if not leaf.startswith("h2h-") or not leaf.endswith(suffix):
        raise ValueError("storage key has an invalid GID leaf")
    encoded = leaf[4 : len(leaf) - len(suffix) if suffix else None]
    try:
        gid = int(encoded)
    except ValueError as error:
        raise ValueError("storage key has an invalid GID leaf") from error
    _require_gid(gid)
    if encoded != str(gid):
        raise ValueError("storage key GID is not canonical decimal")
    return gid


def _gid_and_shard(gid: int) -> tuple[int, str]:
    exact_gid = _require_gid(gid)
    shard = sha256(_SHARD_DOMAIN + exact_gid.to_bytes(8, "big")).hexdigest()
    return exact_gid, shard


def _require_gid(value: int) -> int:
    if type(value) is not int or not 1 <= value < 1 << 63:
        raise ValueError("storage object GID must be a positive signed int63")
    return value
