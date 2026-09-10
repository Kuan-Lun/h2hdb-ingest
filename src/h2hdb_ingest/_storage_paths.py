"""Canonical storage paths shared by runtime and the relocation command."""

from __future__ import annotations

from hashlib import sha256

STORAGE_OBJECT_CODEC = "managed-filesystem-v2"
_SHARD_DOMAIN = b"h2hdb-storage-object-shard-v2\0"


def require_gid(value: int) -> int:
    if type(value) is not int or not 1 <= value < 1 << 63:
        raise ValueError("storage object GID must be a positive signed int63")
    return value


def artifact_name(gid: int) -> str:
    return f"h2h-{require_gid(gid)}.cbz"


def storage_path(gid: int, kind: str) -> str:
    exact_gid = require_gid(gid)
    shard = sha256(_SHARD_DOMAIN + exact_gid.to_bytes(8, "big")).hexdigest()
    if kind == "acquisition":
        return f"acquisitions/hash-v2/{shard[:2]}/{shard[2]}/{artifact_name(gid)}"
    if kind == "thumbnail":
        return (
            f"artwork/hash-v2/{shard[:2]}/{shard[2]}/h2h-{exact_gid}/thumbnail-320.jpg"
        )
    raise ValueError("storage key has an unsupported presentation-v2 shape")


def path_kind(codec: str, segments: tuple[str, ...]) -> str:
    if codec != STORAGE_OBJECT_CODEC:
        raise ValueError("storage key is not the ingest presentation-v2 codec")
    if len(segments) == 5 and segments[:2] == ("acquisitions", "hash-v2"):
        return "acquisition"
    if (
        len(segments) == 6
        and segments[:2] == ("artwork", "hash-v2")
        and segments[-1] == "thumbnail-320.jpg"
    ):
        return "thumbnail"
    raise ValueError("storage key has an unsupported presentation-v2 shape")


def path_gid(codec: str, segments: tuple[str, ...]) -> int:
    kind = path_kind(codec, segments)
    leaf = segments[-1] if kind == "acquisition" else segments[-2]
    suffix = ".cbz" if kind == "acquisition" else ""
    if not leaf.startswith("h2h-") or not leaf.endswith(suffix):
        raise ValueError("storage key has an invalid GID leaf")
    encoded = leaf[4 : len(leaf) - len(suffix) if suffix else None]
    try:
        gid = int(encoded)
    except ValueError as error:
        raise ValueError("storage key has an invalid GID leaf") from error
    require_gid(gid)
    if encoded != str(gid):
        raise ValueError("storage key GID is not canonical decimal")
    return gid


def validate_storage_path(codec: str, path: str) -> tuple[int, str]:
    if type(path) is not str:
        raise TypeError("storage path must be text")
    segments = tuple(path.split("/"))
    kind = path_kind(codec, segments)
    gid = path_gid(codec, segments)
    if path != storage_path(gid, kind):
        raise ValueError("storage key disagrees with the ingest v2 codec")
    return gid, kind
