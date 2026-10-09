"""Immutable presentation policy, byte-format limits, and verified evidence values."""

from __future__ import annotations

import sys
import zlib
from dataclasses import dataclass
from enum import StrEnum
from hashlib import sha256

from PIL import Image, features
from PIL import __version__ as PILLOW_VERSION

from ..source_image import SOURCE_IMAGE_NATIVE_VERSIONS, SOURCE_IMAGE_PIPELINE_ID

ARTIFACT_ADAPTER_ID = b"managed-filesystem"
ARTIFACT_WRITER_ID = b"h2hdb-ingest-presentation-v2"

MAX_PAGE_COUNT = 4096
# Generated canonical JPEG limit; source images have no encoded-byte ceiling.
MAX_ENCODED_PAGE_BYTES = 32 * 1024 * 1024
# v2 forbids ZIP64. This deliberately matches the standard library's safe
# non-ZIP64 ceiling and is checked before a completed archive is exposed.
MAX_ARCHIVE_SIZE_BYTES = (1 << 31) - 1
MAX_DECODED_PIXELS = 40_000_000
MAX_IMAGE_LONG_SIDE = 8192
PAGE_JPEG_QUALITY = 90
THUMBNAIL_MAX_SIDE = 320
THUMBNAIL_JPEG_QUALITY = 85
MIN_SUPPORTED_JPEG_QUALITY = 0
MAX_SUPPORTED_JPEG_QUALITY = 95
PAGE_MEDIA_TYPE = "image/jpeg"
THUMBNAIL_VARIANT = "thumbnail-320"
ARCHIVE_MEDIA_TYPE = "application/vnd.comicbook+zip"


class PresentationImageError(ValueError):
    """Raised when a source cannot safely become a presentation-v2 image."""


class ArtifactImageResampler(StrEnum):
    """Closed set of Pillow resamplers accepted by the artifact policy."""

    NEAREST = "nearest"
    BOX = "box"
    BILINEAR = "bilinear"
    HAMMING = "hamming"
    BICUBIC = "bicubic"
    LANCZOS = "lanczos"


@dataclass(frozen=True, slots=True)
class ArtifactRenderPolicy:
    """Validated byte-affecting image render policy."""

    max_image_short_side: int = 768
    page_jpeg_quality: int = PAGE_JPEG_QUALITY
    thumbnail_jpeg_quality: int = THUMBNAIL_JPEG_QUALITY
    optimize: bool = True
    resampler: ArtifactImageResampler = ArtifactImageResampler.LANCZOS

    def __post_init__(self) -> None:
        if (
            type(self.max_image_short_side) is not int
            or not 1 <= self.max_image_short_side <= MAX_IMAGE_LONG_SIDE
        ):
            raise ValueError("max_image_short_side is outside presentation policy")
        _validate_jpeg_quality(self.page_jpeg_quality, label="page JPEG quality")
        _validate_jpeg_quality(
            self.thumbnail_jpeg_quality,
            label="thumbnail JPEG quality",
        )
        if type(self.optimize) is not bool:
            raise TypeError("artifact optimize must be bool")
        if type(self.resampler) is not ArtifactImageResampler:
            raise TypeError("artifact resampler must be ArtifactImageResampler")

    @property
    def pillow_resampler(self) -> Image.Resampling:
        """Resolve the validated neutral name to Pillow's exact enum."""

        return {
            ArtifactImageResampler.NEAREST: Image.Resampling.NEAREST,
            ArtifactImageResampler.BOX: Image.Resampling.BOX,
            ArtifactImageResampler.BILINEAR: Image.Resampling.BILINEAR,
            ArtifactImageResampler.HAMMING: Image.Resampling.HAMMING,
            ArtifactImageResampler.BICUBIC: Image.Resampling.BICUBIC,
            ArtifactImageResampler.LANCZOS: Image.Resampling.LANCZOS,
        }[self.resampler]


@dataclass(frozen=True, slots=True)
class CanonicalImageEvidence:
    """Facts recomputed from exact canonical JPEG bytes."""

    sha256: bytes
    size_bytes: int
    width: int
    height: int
    media_type: str = PAGE_MEDIA_TYPE

    def __post_init__(self) -> None:
        if type(self.sha256) is not bytes or len(self.sha256) != 32:
            raise ValueError("canonical image SHA-256 must contain 32 bytes")
        if type(self.size_bytes) is not int or not 1 <= self.size_bytes <= (
            MAX_ENCODED_PAGE_BYTES
        ):
            raise ValueError("canonical image encoded size is outside policy")
        _validate_dimensions(self.width, self.height, max_long_side=MAX_IMAGE_LONG_SIDE)
        if self.media_type != PAGE_MEDIA_TYPE:
            raise ValueError("presentation-v2 images must be image/jpeg")


@dataclass(frozen=True, slots=True)
class PreparedPageEvidence:
    """One verified stored JPEG extent inside the acquisition CBZ."""

    page_index: int
    member_name: str
    byte_offset: int
    image: CanonicalImageEvidence

    def __post_init__(self) -> None:
        if (
            type(self.page_index) is not int
            or not 0 <= self.page_index < MAX_PAGE_COUNT
        ):
            raise ValueError("page_index is outside presentation policy")
        if not isinstance(self.member_name, str) or not self.member_name.endswith(
            ".jpg"
        ):
            raise ValueError("presentation page member must use a .jpg name")
        if type(self.byte_offset) is not int or self.byte_offset < 0:
            raise ValueError("page byte_offset must be non-negative")
        if not isinstance(self.image, CanonicalImageEvidence):
            raise TypeError("page image evidence has a foreign type")
        self.image.__post_init__()


@dataclass(frozen=True, slots=True)
class PreparedPresentationEvidence:
    """Bounded page evidence recomputed from one completed acquisition."""

    archive_sha256: bytes
    archive_size_bytes: int
    pages: tuple[PreparedPageEvidence, ...]

    def __post_init__(self) -> None:
        if type(self.archive_sha256) is not bytes or len(self.archive_sha256) != 32:
            raise ValueError("archive SHA-256 must contain 32 bytes")
        if (
            type(self.archive_size_bytes) is not int
            or not 1 <= (self.archive_size_bytes) <= MAX_ARCHIVE_SIZE_BYTES
        ):
            raise ValueError("archive size is outside presentation policy")
        object.__setattr__(self, "pages", tuple(self.pages))
        if len(self.pages) > MAX_PAGE_COUNT:
            raise ValueError("presentation page count exceeds policy")
        for index, page in enumerate(self.pages):
            if not isinstance(page, PreparedPageEvidence):
                raise TypeError("presentation contains foreign page evidence")
            page.__post_init__()
            if page.page_index != index:
                raise ValueError(
                    "presentation page indices must be dense and zero-based"
                )
            end = page.byte_offset + page.image.size_bytes
            if end > self.archive_size_bytes:
                raise ValueError("presentation page extent exceeds the archive")

    @property
    def cover(self) -> PreparedPageEvidence | None:
        """The full-size cover is canonical page zero, without duplicate bytes."""

        return self.pages[0] if self.pages else None


def artifact_policy_fingerprint_sha256(policy: ArtifactRenderPolicy) -> bytes:
    """Bind policy identity to every byte-affecting implementation fact."""

    if not isinstance(policy, ArtifactRenderPolicy):
        raise TypeError("artifact policy must be ArtifactRenderPolicy")
    policy.__post_init__()
    cache_tag = sys.implementation.cache_tag or (
        f"cpython-{sys.version_info.major}.{sys.version_info.minor}"
    )
    jpeg = features.version_codec("jpg") or "unknown"
    fields = (
        ARTIFACT_WRITER_ID,
        SOURCE_IMAGE_PIPELINE_ID,
        *(value.encode("ascii") for value in SOURCE_IMAGE_NATIVE_VERSIONS),
        cache_tag.encode("ascii", errors="strict"),
        PILLOW_VERSION.encode("ascii", errors="strict"),
        jpeg.encode("ascii", errors="strict"),
        zlib.ZLIB_RUNTIME_VERSION.encode("ascii", errors="strict"),
        str(policy.max_image_short_side).encode("ascii"),
        str(policy.page_jpeg_quality).encode("ascii"),
        str(policy.thumbnail_jpeg_quality).encode("ascii"),
        str(policy.optimize).encode("ascii"),
        policy.resampler.value.encode("ascii"),
        str(THUMBNAIL_MAX_SIDE).encode("ascii"),
    )
    framed = sha256(b"h2hdb-ingest-artifact-policy-v6\0")
    for value in fields:
        framed.update(len(value).to_bytes(4, "big"))
        framed.update(value)
    return framed.digest()


def _validate_jpeg_quality(value: int, *, label: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{label} must be int")
    if not MIN_SUPPORTED_JPEG_QUALITY <= value <= MAX_SUPPORTED_JPEG_QUALITY:
        raise ValueError(
            f"{label} must be from {MIN_SUPPORTED_JPEG_QUALITY} through "
            f"{MAX_SUPPORTED_JPEG_QUALITY}"
        )


def _validate_dimensions(width: int, height: int, *, max_long_side: int) -> None:
    if type(width) is not int or type(height) is not int or width < 1 or height < 1:
        raise PresentationImageError("image dimensions must be positive integers")
    if width > max_long_side or height > max_long_side:
        raise PresentationImageError("image long side exceeds presentation policy")
    if width * height > MAX_DECODED_PIXELS:
        raise PresentationImageError("image exceeds the 40 MP decoded-pixel policy")
