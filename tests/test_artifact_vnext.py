from __future__ import annotations

from hashlib import sha256
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

from h2hdb_ingest.artifact import (
    ArtifactProducerIdentity,
    ManagedFilesystemArtifactAdapter,
)


def _locator(payload: bytes) -> tuple[str, str, str]:
    digest = sha256(payload).hexdigest()
    return ("sha256", digest[:2], f"{digest}.cbz")


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


def test_protect_and_release_are_monotone_and_idempotent(tmp_path: Path) -> None:
    adapter = ManagedFilesystemArtifactAdapter(
        tmp_path / "artifacts",
        max_image_short_side=768,
    )
    assert isinstance(adapter, ArtifactStorageAdapter)
    assert isinstance(adapter, ArtifactReleaseAdapter)
    payload = b"an immutable archive"
    locator = _locator(payload)
    token = b"t" * 184

    first = adapter.protect(BytesIO(payload), locator, token)
    replay = adapter.protect(BytesIO(payload), locator, token)
    target = tmp_path / "artifacts" / Path(*locator)

    assert first.stored
    assert replay.stored
    assert target.read_bytes() == payload
    with pytest.raises(RuntimeError, match="does not match its locator"):
        adapter.protect(BytesIO(b"different"), locator, token)
    assert adapter.release(locator, token).released
    assert adapter.release(locator, token).released
    assert not adapter.protect(BytesIO(payload), locator, token).stored
    assert target.read_bytes() == payload


def test_protect_rejects_bytes_that_disagree_with_locator(tmp_path: Path) -> None:
    adapter = ManagedFilesystemArtifactAdapter(
        tmp_path / "artifacts",
        max_image_short_side=768,
    )

    with pytest.raises(RuntimeError, match="does not match its locator"):
        adapter.protect(BytesIO(b"different"), _locator(b"expected"), b"x" * 184)


def test_render_member_uses_registered_bounded_image_transforms(tmp_path: Path) -> None:
    adapter = ManagedFilesystemArtifactAdapter(
        tmp_path / "artifacts",
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
