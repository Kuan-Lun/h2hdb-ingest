"""Deterministic presentation-v2 image rendering and CBZ evidence."""

from __future__ import annotations

__all__ = [
    "ARTIFACT_ADAPTER_ID",
    "ARTIFACT_WRITER_ID",
    "MAX_ARCHIVE_SIZE_BYTES",
    "MAX_DECODED_PIXELS",
    "MAX_ENCODED_PAGE_BYTES",
    "MAX_IMAGE_LONG_SIDE",
    "MAX_METADATA_BYTES",
    "MAX_PAGE_COUNT",
    "MAX_PAGE_RENDER_WORKERS",
    "MAX_SUPPORTED_JPEG_QUALITY",
    "MIN_SUPPORTED_JPEG_QUALITY",
    "PAGE_JPEG_QUALITY",
    "THUMBNAIL_JPEG_QUALITY",
    "THUMBNAIL_MAX_SIDE",
    "ArtifactImageResampler",
    "ArtifactPreparationRenderer",
    "ArtifactRenderPolicy",
    "CanonicalImageEvidence",
    "PreparedPageEvidence",
    "PreparedPresentationEvidence",
    "PresentationImageError",
    "artifact_policy_fingerprint_sha256",
    "canonical_page_member_name",
    "inspect_presentation_archive",
    "load_source_page_image",
    "render_archive",
    "render_presentation",
]

from .._limits import MAX_METADATA_BYTES
from ..page_workers import MAX_PAGE_RENDER_WORKERS
from .archive import canonical_page_member_name, inspect_presentation_archive
from .images import load_source_page_image
from .model import (
    ARTIFACT_ADAPTER_ID,
    ARTIFACT_WRITER_ID,
    MAX_ARCHIVE_SIZE_BYTES,
    MAX_DECODED_PIXELS,
    MAX_ENCODED_PAGE_BYTES,
    MAX_IMAGE_LONG_SIDE,
    MAX_PAGE_COUNT,
    MAX_SUPPORTED_JPEG_QUALITY,
    MIN_SUPPORTED_JPEG_QUALITY,
    PAGE_JPEG_QUALITY,
    THUMBNAIL_JPEG_QUALITY,
    THUMBNAIL_MAX_SIDE,
    ArtifactImageResampler,
    ArtifactRenderPolicy,
    CanonicalImageEvidence,
    PreparedPageEvidence,
    PreparedPresentationEvidence,
    PresentationImageError,
    artifact_policy_fingerprint_sha256,
)
from .renderer import ArtifactPreparationRenderer, render_archive, render_presentation
