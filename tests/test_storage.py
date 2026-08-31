from __future__ import annotations

import pytest
from h2hdb import CatalogResourceKind, StorageObjectKey

from h2hdb_ingest.storage import (
    STORAGE_OBJECT_CODEC,
    acquisition_storage_key,
    artifact_name,
    storage_key_gid,
    storage_key_resource_kind,
    thumbnail_storage_key,
    validate_storage_key,
)


def test_v2_storage_keys_are_disjoint_reproducible_resources() -> None:
    acquisition = acquisition_storage_key(42)
    thumbnail = thumbnail_storage_key(42)

    assert acquisition.codec == STORAGE_OBJECT_CODEC
    assert acquisition.segments[:2] == ("acquisitions", "hash-v2")
    assert acquisition.segments[2:4] == ("84", "0")
    assert acquisition.segments[-1] == "h2h-42.cbz"
    assert artifact_name(42) == "h2h-42.cbz"
    assert thumbnail.codec == STORAGE_OBJECT_CODEC
    assert thumbnail.segments[:2] == ("artwork", "hash-v2")
    assert thumbnail.segments[-2:] == ("h2h-42", "thumbnail-320.jpg")
    assert acquisition.segments[2:4] == thumbnail.segments[2:4]
    assert validate_storage_key(acquisition) is acquisition
    assert validate_storage_key(thumbnail) is thumbnail
    assert storage_key_gid(acquisition) == storage_key_gid(thumbnail) == 42
    assert storage_key_resource_kind(acquisition) is CatalogResourceKind.ACQUISITION
    assert storage_key_resource_kind(thumbnail) is CatalogResourceKind.THUMBNAIL


@pytest.mark.parametrize("gid", (1, (1 << 63) - 1))
def test_storage_key_builder_accepts_signed_int63_boundaries(gid: int) -> None:
    assert storage_key_gid(validate_storage_key(acquisition_storage_key(gid))) == gid
    assert storage_key_gid(validate_storage_key(thumbnail_storage_key(gid))) == gid


@pytest.mark.parametrize("gid", (True, 0, -1, 1 << 63))
def test_storage_key_builder_rejects_non_positive_int63(gid: int) -> None:
    with pytest.raises(ValueError, match="positive signed int63"):
        acquisition_storage_key(gid)


def test_storage_key_validation_rejects_caller_path_variants() -> None:
    canonical = acquisition_storage_key(42)
    fullwidth_gid = "\N{FULLWIDTH DIGIT FOUR}\N{FULLWIDTH DIGIT TWO}"

    with pytest.raises(ValueError, match="canonical decimal"):
        validate_storage_key(
            StorageObjectKey(
                canonical.codec,
                (*canonical.segments[:-1], "h2h-042.cbz"),
            )
        )
    with pytest.raises(ValueError, match="canonical decimal"):
        validate_storage_key(
            StorageObjectKey(
                canonical.codec,
                (*canonical.segments[:-1], f"h2h-{fullwidth_gid}.cbz"),
            )
        )
    with pytest.raises(ValueError, match="disagrees with the ingest v2 codec"):
        validate_storage_key(
            StorageObjectKey(
                canonical.codec,
                (*canonical.segments[:2], "ff", "f", canonical.segments[-1]),
            )
        )
    with pytest.raises(ValueError, match="unsupported presentation-v2 shape"):
        validate_storage_key(
            StorageObjectKey(
                canonical.codec,
                (
                    "acquisitions",
                    "hash-v2",
                    "aa",
                    "b",
                    "unexpected",
                    "h2h-42.cbz",
                ),
            )
        )
    with pytest.raises(ValueError, match="presentation-v2 codec"):
        validate_storage_key(StorageObjectKey("foreign", canonical.segments))
