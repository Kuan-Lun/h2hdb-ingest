from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pytest
from h2hdb import (
    ArtifactReleaseAdapter,
    ArtifactStorageAdapter,
    ArtifactTransformKind,
    VNextArtifactProducer,
)
from PIL import Image

from h2hdb_ingest.artifact import ArtifactProducerIdentity
from h2hdb_ingest.library import ManagedFilesystemLibraryAdapter


def test_producer_identity_matches_public_core_domain() -> None:
    identity = ArtifactProducerIdentity.current()
    producer = VNextArtifactProducer(
        identity.writer_id,
        identity.python_abi,
        identity.pillow_build,
        identity.libjpeg_build,
        identity.zlib_build,
    )

    assert producer.fingerprint_sha256 == identity.fingerprint_sha256


def test_single_library_implements_storage_and_release_ports(tmp_path: Path) -> None:
    adapter = ManagedFilesystemLibraryAdapter(
        tmp_path / "library",
        max_image_short_side=20,
    )

    assert isinstance(adapter, ArtifactStorageAdapter)
    assert isinstance(adapter, ArtifactReleaseAdapter)


def test_render_member_uses_registered_bounded_image_transforms(
    tmp_path: Path,
) -> None:
    adapter = ManagedFilesystemLibraryAdapter(
        tmp_path / "library",
        max_image_short_side=20,
    )
    source = BytesIO()
    Image.new("RGBA", (80, 40), (255, 0, 0, 0)).save(source, format="PNG")
    source.seek(0)
    destination = BytesIO()

    adapter.render_member(
        source,
        ArtifactTransformKind.JPEG_NORMALIZE,
        destination,
    )
    destination.seek(0)
    with Image.open(destination) as rendered:
        assert rendered.format == "JPEG"
        assert rendered.mode == "RGB"
        assert min(rendered.size) == 20

    with pytest.raises(ValueError, match="core owns RAW_COPY"):
        adapter.render_member(
            BytesIO(b"raw"),
            ArtifactTransformKind.RAW_COPY,
            BytesIO(),
        )
