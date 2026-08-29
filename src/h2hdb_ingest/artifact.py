"""Deterministic CBZ producer identity and member rendering."""

from __future__ import annotations

__all__ = ["ARTIFACT_ADAPTER_ID", "ArtifactProducerIdentity"]

import sys
import zlib
from dataclasses import dataclass
from typing import BinaryIO

from h2hdb import ArtifactTransformKind, artifact_producer_fingerprint_sha256
from PIL import Image, ImageFile, ImageOps, features
from PIL import __version__ as PILLOW_VERSION

ARTIFACT_ADAPTER_ID = b"managed-filesystem"
ARTIFACT_WRITER_ID = b"h2hdb-ingest-canonical-cbz-v1"

Image.MAX_IMAGE_PIXELS = None
ImageFile.LOAD_TRUNCATED_IMAGES = True


@dataclass(frozen=True, slots=True)
class ArtifactProducerIdentity:
    """Exact producer fields shared by policy registration and rendering."""

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

    @staticmethod
    def render_member(
        source: BinaryIO,
        transform_kind: ArtifactTransformKind,
        destination: BinaryIO,
        *,
        max_image_short_side: int,
    ) -> None:
        """Render one registered normalized-image transformation."""

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
                scale = max_image_short_side / image.width
                bounds = (max_image_short_side, max(1, int(image.height * scale)))
            else:
                scale = max_image_short_side / image.height
                bounds = (max(1, int(image.width * scale)), max_image_short_side)
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
