"""Shared source-image rendering and canonical JPEG validation."""

from __future__ import annotations

import warnings
from contextlib import ExitStack
from hashlib import sha256
from io import BytesIO
from tempfile import SpooledTemporaryFile
from threading import Lock
from typing import BinaryIO

from PIL import Image, ImageFile, ImageOps, UnidentifiedImageError

from .._image_performance import image_phase
from .._resource_cleanup import owned_resource
from ..source_image import SourceImageDecodeError, load_source_image
from ._streams import _COPY_BUFFER_BYTES, _read_extent, _write_all
from .model import (
    MAX_DECODED_PIXELS,
    MAX_ENCODED_PAGE_BYTES,
    MAX_IMAGE_LONG_SIDE,
    THUMBNAIL_MAX_SIDE,
    ArtifactRenderPolicy,
    CanonicalImageEvidence,
    PreparedPageEvidence,
    PresentationImageError,
    _validate_dimensions,
    _validate_jpeg_quality,
)

_IMAGE_HEADER_WARNING_LOCK = Lock()

# Pillow only opens canonical output images. Source headers and pixels use
# the streaming decoder and are not subject to this output-only bound.
Image.MAX_IMAGE_PIXELS = MAX_DECODED_PIXELS
ImageFile.LOAD_TRUNCATED_IMAGES = False


def _render_page(
    source: BinaryIO,
    destination: BinaryIO,
    *,
    policy: ArtifactRenderPolicy,
) -> CanonicalImageEvidence:
    policy.__post_init__()
    image = load_source_page_image(source, policy=policy)
    try:
        with image_phase("color_convert"):
            image = _rgb_on_white(image)
        return _encode_jpeg(
            image,
            destination,
            quality=policy.page_jpeg_quality,
            optimize=policy.optimize,
            max_long_side=MAX_IMAGE_LONG_SIDE,
        )
    finally:
        image.close()


def load_source_page_image(
    source: BinaryIO, *, policy: ArtifactRenderPolicy
) -> Image.Image:
    """Fully decode one source into an owned canonical-sized image for preflight."""

    policy.__post_init__()
    try:
        image = load_source_image(
            source,
            max_short_side=policy.max_image_short_side,
            max_long_side=MAX_IMAGE_LONG_SIDE,
            max_pixels=MAX_DECODED_PIXELS,
            resampler=policy.pillow_resampler,
        )
    except SourceImageDecodeError as error:
        raise PresentationImageError(str(error)) from error
    try:
        _validate_dimensions(
            image.width, image.height, max_long_side=MAX_IMAGE_LONG_SIDE
        )
    except BaseException:
        image.close()
        raise
    return image


def _load_safe_image(source: BinaryIO) -> Image.Image:
    try:
        with ExitStack() as opened_context:
            # CPython's process-global warnings filter is not concurrency-safe
            # when context_aware_warnings is disabled.  Only the lazy header
            # open and initial size check need the Pillow warning conversion;
            # decoding and image conversion remain outside this short lock.
            with _IMAGE_HEADER_WARNING_LOCK:
                with warnings.catch_warnings():
                    warnings.simplefilter("error", Image.DecompressionBombWarning)
                    opened = opened_context.enter_context(Image.open(source))
                    opened.seek(0)
                    _validate_dimensions(
                        opened.width,
                        opened.height,
                        max_long_side=MAX_IMAGE_LONG_SIDE,
                    )
            opened.load()
            transposed = ImageOps.exif_transpose(opened)
            image = transposed.copy()
            if transposed is not opened:
                transposed.close()
    except (Image.DecompressionBombError, Image.DecompressionBombWarning) as error:
        raise PresentationImageError(
            "image exceeds the decoded pixel policy"
        ) from error
    except (OSError, SyntaxError, UnidentifiedImageError) as error:
        raise PresentationImageError("image is truncated or invalid") from error
    _validate_dimensions(image.width, image.height, max_long_side=MAX_IMAGE_LONG_SIDE)
    return image


def _rgb_on_white(image: Image.Image) -> Image.Image:
    if image.has_transparency_data:
        foreground = image.convert("RGBA")
        background = Image.new("RGBA", foreground.size, "white")
        composited = Image.alpha_composite(background, foreground).convert("RGB")
        foreground.close()
        background.close()
        image.close()
        return composited
    if image.mode != "RGB":
        converted = image.convert("RGB")
        image.close()
        return converted
    return image


def _encode_jpeg(
    image: Image.Image,
    destination: BinaryIO,
    *,
    quality: int,
    optimize: bool,
    max_long_side: int,
) -> CanonicalImageEvidence:
    _validate_jpeg_quality(quality, label="JPEG quality")
    if type(optimize) is not bool:
        raise TypeError("JPEG optimize must be bool")
    _validate_dimensions(image.width, image.height, max_long_side=max_long_side)
    with owned_resource(
        SpooledTemporaryFile(max_size=4 * 1024 * 1024, mode="w+b")
    ) as encoded:
        with image_phase("jpeg_encode"):
            image.save(
                encoded,
                format="JPEG",
                quality=quality,
                optimize=optimize,
                progressive=False,
            )
        size = encoded.tell()
        if not 1 <= size <= MAX_ENCODED_PAGE_BYTES:
            raise PresentationImageError("encoded JPEG exceeds the 32 MiB policy")
        encoded.seek(0)
        digest = sha256()
        remaining = size
        with image_phase("encoded_copy_hash"):
            while remaining:
                chunk = encoded.read(min(_COPY_BUFFER_BYTES, remaining))
                if not chunk:
                    raise PresentationImageError("encoded JPEG ended unexpectedly")
                _write_all(destination, chunk, label="encoded JPEG")
                digest.update(chunk)
                remaining -= len(chunk)
    return CanonicalImageEvidence(
        sha256=digest.digest(),
        size_bytes=size,
        width=image.width,
        height=image.height,
    )


def _verify_canonical_jpeg(content: bytes) -> CanonicalImageEvidence:
    if len(content) > MAX_ENCODED_PAGE_BYTES:
        raise PresentationImageError("canonical JPEG exceeds the encoded-size policy")
    if not content.startswith(b"\xff\xd8") or not content.endswith(b"\xff\xd9"):
        raise PresentationImageError("presentation page bytes are not JPEG")
    try:
        with ExitStack() as opened_context:
            with _IMAGE_HEADER_WARNING_LOCK:
                with warnings.catch_warnings():
                    warnings.simplefilter("error", Image.DecompressionBombWarning)
                    opened = opened_context.enter_context(Image.open(BytesIO(content)))
                    if opened.format != "JPEG":
                        raise PresentationImageError(
                            "presentation page bytes are not JPEG"
                        )
                    _validate_dimensions(
                        opened.width,
                        opened.height,
                        max_long_side=MAX_IMAGE_LONG_SIDE,
                    )
            # Verification must decode every pixel, but never transpose or copy
            # them: canonical rendering already applied orientation and removed
            # EXIF. Preserve the inspector's oriented-dimension semantics for
            # externally supplied JPEGs using metadata alone.
            opened.load()
            width, height = opened.size
            if opened.getexif().get(0x0112) in {5, 6, 7, 8}:
                width, height = height, width
    except (Image.DecompressionBombError, Image.DecompressionBombWarning) as error:
        raise PresentationImageError(
            "image exceeds the decoded pixel policy"
        ) from error
    except (OSError, SyntaxError, UnidentifiedImageError) as error:
        raise PresentationImageError("image is truncated or invalid") from error
    return CanonicalImageEvidence(
        sha256=sha256(content).digest(),
        size_bytes=len(content),
        width=width,
        height=height,
    )


def _render_thumbnail(
    archive: BinaryIO,
    cover: PreparedPageEvidence,
    destination: BinaryIO,
    *,
    policy: ArtifactRenderPolicy,
) -> CanonicalImageEvidence:
    content = _read_extent(
        archive,
        offset=cover.byte_offset,
        size=cover.image.size_bytes,
    )
    if sha256(content).digest() != cover.image.sha256:
        raise PresentationImageError("cover changed after archive inspection")
    image = _load_safe_image(BytesIO(content))
    try:
        image.thumbnail(
            (THUMBNAIL_MAX_SIDE, THUMBNAIL_MAX_SIDE),
            policy.pillow_resampler,
        )
        image = _rgb_on_white(image)
        evidence = _encode_jpeg(
            image,
            destination,
            quality=policy.thumbnail_jpeg_quality,
            optimize=policy.optimize,
            max_long_side=THUMBNAIL_MAX_SIDE,
        )
    finally:
        image.close()
    if max(evidence.width, evidence.height) > THUMBNAIL_MAX_SIDE:
        raise PresentationImageError("thumbnail dimensions exceed policy")
    return evidence
