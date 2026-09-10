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

import struct
import sys
import warnings
import zlib
from concurrent.futures import Future, ThreadPoolExecutor, wait
from contextlib import ExitStack
from dataclasses import dataclass, replace
from enum import StrEnum
from hashlib import sha256
from io import BytesIO
from itertools import pairwise
from tempfile import SpooledTemporaryFile
from threading import Lock
from time import monotonic_ns
from typing import BinaryIO, cast
from zipfile import (
    ZIP_DEFLATED,
    ZIP_STORED,
    BadZipFile,
    LargeZipFile,
    ZipFile,
    ZipInfo,
)

from h2hdb import (
    ArtifactArchiveRenderEvidence,
    ArtifactPagePresentationEvidence,
    ArtifactPresentationRenderEvidence,
    ArtifactRenderedPage,
    ArtifactSourceMember,
    ArtifactSourceRole,
    ArtifactThumbnailPresentationEvidence,
    ByteExtent,
    VNextSourceChangedError,
)
from PIL import Image, ImageFile, ImageOps, UnidentifiedImageError, features
from PIL import __version__ as PILLOW_VERSION

from ._limits import MAX_METADATA_BYTES
from ._resource_cleanup import close_resources, owned_resource
from .artifact_errors import attach_page_failure_context
from .image_diagnostics import (
    SourceImageLogContext,
    current_image_log_context,
    image_log_scope,
)
from .metrics import (
    IngestMetric,
    IngestMetricSink,
    IngestMetricValue,
    emit_ingest_metric,
)
from .page_workers import MAX_PAGE_RENDER_WORKERS, resolve_page_render_workers
from .progress import IngestProgress, ProgressWork
from .source_image import (
    SOURCE_IMAGE_NATIVE_VERSIONS,
    SOURCE_IMAGE_PIPELINE_ID,
    SourceImageDecodeError,
    load_source_image,
)
from .storage import artifact_name

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

_COPY_BUFFER_BYTES = 1024 * 1024
_MAX_SOURCE_MEMBER_COUNT = 8192
_LOCAL_FILE_HEADER = struct.Struct("<IHHHHHIIIHH")
_LOCAL_FILE_HEADER_SIGNATURE = 0x04034B50
_CENTRAL_DIRECTORY_HEADER = struct.Struct("<I6H3I5H2I")
_CENTRAL_DIRECTORY_HEADER_SIGNATURE = 0x02014B50
_END_OF_CENTRAL_DIRECTORY = struct.Struct("<4sHHHHIIH")
_END_OF_CENTRAL_DIRECTORY_SIGNATURE = b"PK\x05\x06"
_CENTRAL_DIRECTORY_HEADER_BYTES = _CENTRAL_DIRECTORY_HEADER.size
_MAX_ZIP_COMMENT_BYTES = 65_535
_METADATA_MEMBER_NAME = "galleryinfo.txt"
_CANONICAL_ZIP_DATE_TIME = (1980, 1, 1, 0, 0, 0)
_CANONICAL_CREATE_SYSTEM = 3
_CANONICAL_ZIP_VERSION = 20
_CANONICAL_EXTERNAL_ATTR = 0o100644 << 16
_IMAGE_HEADER_WARNING_LOCK = Lock()

# Pillow only opens canonical output images. Source headers and pixels use
# the streaming decoder and are not subject to this output-only bound.
Image.MAX_IMAGE_PIXELS = MAX_DECODED_PIXELS
ImageFile.LOAD_TRUNCATED_IMAGES = False


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
class _RawCentralMember:
    name: bytes
    compression: int
    crc32: int
    compressed_size: int
    uncompressed_size: int
    local_offset: int


@dataclass(slots=True)
class _RenderedPageBuffer:
    image: CanonicalImageEvidence
    stream: BinaryIO

    def close(self) -> None:
        self.stream.close()


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


class ArtifactPreparationRenderer:
    """Own at most one archive inspection for adjacent preparation operations.

    Only the completed private writer stage can seed this optimization. No
    caller digest or durable receipt is accepted as inspection authority. The
    next presentation hashes the actual stream again before reusing facts; a
    restart, eviction, or changed byte stream requires full ZIP/JPEG validation.
    The slot contains immutable metadata for at most MAX_PAGE_COUNT pages and
    never retains pixels, open streams, futures, or archive bytes.
    """

    def __init__(
        self,
        *,
        policy: ArtifactRenderPolicy,
        page_render_workers: int | None = None,
        metrics_sink: IngestMetricSink | None = None,
        progress: IngestProgress | None = None,
    ) -> None:
        self._policy = policy
        self._workers = page_render_workers
        self._metrics_sink = metrics_sink
        self._progress = progress
        self._inspection: PreparedPresentationEvidence | None = None
        self._inspection_lock = Lock()

    def render_archive(
        self,
        members: tuple[ArtifactSourceMember, ...],
        destination: BinaryIO,
        *,
        gid: int,
    ) -> ArtifactArchiveRenderEvidence:
        """Fully inspect a private completed stage before remembering facts."""
        return _render_archive(
            members,
            destination,
            gid=gid,
            policy=self._policy,
            page_render_workers=self._workers,
            metrics_sink=self._metrics_sink,
            preparation=self,
            progress=None if self._progress is None else self._progress.current(),
        )

    def render_presentation(
        self,
        archive: BinaryIO,
        thumbnail_destination: BinaryIO,
        *,
        rendered_pages: tuple[ArtifactRenderedPage, ...],
    ) -> ArtifactPresentationRenderEvidence:
        """Rehash exact input bytes before reusing this process's inspection."""
        return _render_presentation(
            archive,
            thumbnail_destination,
            rendered_pages=rendered_pages,
            policy=self._policy,
            metrics_sink=self._metrics_sink,
            preparation=self,
            progress=None if self._progress is None else self._progress.current(),
        )

    def _remember(self, inspected: PreparedPresentationEvidence) -> None:
        with self._inspection_lock:
            self._inspection = inspected

    def _inspect_actual_bytes(
        self,
        archive: BinaryIO,
        names: tuple[str, ...],
    ) -> PreparedPresentationEvidence:
        # Take ownership of the single-use slot before I/O. Concurrent renders
        # may replace it and lose a speedup, but cannot bypass byte validation.
        with self._inspection_lock:
            inspected, self._inspection = self._inspection, None
        if inspected is not None:
            archive.seek(0, 2)
            size = archive.tell()
            if size == inspected.archive_size_bytes and names == tuple(
                page.member_name for page in inspected.pages
            ):
                archive.seek(0)
                digest = _stream_digest(archive, size)
                archive.seek(0)
                if digest == inspected.archive_sha256:
                    return inspected
        return inspect_presentation_archive(archive, names)


def render_archive(
    members: tuple[ArtifactSourceMember, ...],
    destination: BinaryIO,
    *,
    gid: int,
    policy: ArtifactRenderPolicy,
    page_render_workers: int | None = None,
    metrics_sink: IngestMetricSink | None = None,
    progress: ProgressWork | None = None,
) -> ArtifactArchiveRenderEvidence:
    """Render and fully inspect one canonical CBZ without retaining evidence."""

    return _render_archive(
        members,
        destination,
        gid=gid,
        policy=policy,
        page_render_workers=page_render_workers,
        metrics_sink=metrics_sink,
        preparation=None,
        progress=progress,
    )


class _ArchiveScratch:
    """Borrow one unpublished stream; discard partial bytes after any failure."""

    def __init__(self, stream: BinaryIO) -> None:
        self._stream = stream

    def __enter__(self) -> _ArchiveScratch:
        self._stream.seek(0)
        self._stream.truncate(0)
        return self

    def __exit__(self, _type: object, error: BaseException | None, _tb: object) -> None:
        if error is not None:
            try:
                self._stream.seek(0)
                self._stream.truncate(0)
            except BaseException as cleanup_error:
                error.add_note(
                    f"Discarding partial archive also failed: {cleanup_error!r}"
                )

    def read(self, size: int = -1) -> bytes:
        return self._stream.read(size)

    def write(self, content: bytes) -> int:
        _write_all(self._stream, content, label="canonical archive scratch")
        return len(content)

    def seek(self, offset: int, whence: int = 0) -> int:
        return self._stream.seek(offset, whence)

    def tell(self) -> int:
        return self._stream.tell()

    def flush(self) -> None:
        self._stream.flush()

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return True


def _render_archive(
    members: tuple[ArtifactSourceMember, ...],
    destination: BinaryIO,
    *,
    gid: int,
    policy: ArtifactRenderPolicy,
    page_render_workers: int | None = None,
    metrics_sink: IngestMetricSink | None = None,
    preparation: ArtifactPreparationRenderer | None,
    progress: ProgressWork | None = None,
) -> ArtifactArchiveRenderEvidence:
    """Render and verify one CBZ in caller-owned, unpublished scratch storage."""

    started_ns = monotonic_ns()
    if progress is not None:
        progress.operation("archive_preflight")
    download_name = artifact_name(gid)
    if type(members) is not tuple:
        raise TypeError("archive members must be an exact tuple")
    if not isinstance(policy, ArtifactRenderPolicy):
        raise TypeError("artifact policy must be ArtifactRenderPolicy")
    policy.__post_init__()
    workers = resolve_page_render_workers(page_render_workers)
    if not all(
        hasattr(destination, method) for method in ("read", "seek", "truncate", "write")
    ):
        raise TypeError(
            "destination must be a readable, seekable, writable scratch stream"
        )
    if not destination.readable():
        raise TypeError("archive scratch destination must be readable")

    metadata, pages = _preflight_archive_members(members)
    page_evidence: list[ArtifactRenderedPage] = []
    member_names: list[str] = []
    with _ArchiveScratch(destination) as staged:
        try:
            with owned_resource(
                ZipFile(
                    cast(BinaryIO, staged),
                    mode="w",
                    compression=ZIP_DEFLATED,
                    compresslevel=9,
                    allowZip64=False,
                    strict_timestamps=True,
                )
            ) as archive:
                _verify_source_stream(metadata)
                _require_projected_archive_size(
                    staged.tell(),
                    member_names,
                    _METADATA_MEMBER_NAME,
                    _deflate_worst_case(metadata.expected_size_bytes),
                )
                info = _canonical_zip_info(
                    _METADATA_MEMBER_NAME,
                    compression=ZIP_DEFLATED,
                    file_size=metadata.expected_size_bytes,
                )
                with archive.open(
                    info,
                    mode="w",
                    force_zip64=False,
                ) as target:
                    _copy_exact_source(metadata, cast(BinaryIO, target))
                _verify_source_stream(metadata)
                member_names.append(_METADATA_MEMBER_NAME)
                render_pages_started_ns = monotonic_ns()
                if progress is not None:
                    progress.operation("archive_render_pages")
                executor = (
                    ThreadPoolExecutor(
                        max_workers=workers,
                        thread_name_prefix="h2hdb-page-render",
                    )
                    if workers > 1
                    else None
                )
                try:
                    for batch_start in range(0, len(pages), workers):
                        if progress is not None:
                            progress.operation("archive_render_pages")
                        batch = pages[batch_start : batch_start + workers]
                        rendered_batch = _render_page_batch(
                            batch,
                            gid=gid,
                            policy=policy,
                            executor=executor,
                            progress=progress,
                        )
                        try:
                            if progress is not None:
                                progress.operation("archive_write_pages")
                            for offset, (member, rendered) in enumerate(
                                zip(batch, rendered_batch, strict=True)
                            ):
                                page_index = batch_start + offset
                                image = rendered.image
                                locator = canonical_page_member_name(page_index)
                                _require_projected_archive_size(
                                    staged.tell(),
                                    member_names,
                                    locator,
                                    image.size_bytes,
                                )
                                info = _canonical_zip_info(
                                    locator,
                                    compression=ZIP_STORED,
                                    file_size=image.size_bytes,
                                )
                                rendered.stream.seek(0)
                                with archive.open(
                                    info,
                                    mode="w",
                                    force_zip64=False,
                                ) as target:
                                    _copy_exact_bytes(
                                        rendered.stream,
                                        cast(BinaryIO, target),
                                        size=image.size_bytes,
                                        label="rendered JPEG page",
                                    )
                                member_names.append(locator)
                                page_evidence.append(
                                    ArtifactRenderedPage(
                                        page_index=page_index,
                                        source_position=member.position,
                                        locator=locator,
                                    )
                                )
                                if progress is not None:
                                    progress.advance("pages_written")
                        finally:
                            close_resources(rendered_batch, error=sys.exception())
                finally:
                    if executor is not None:
                        executor.shutdown(wait=True, cancel_futures=True)
                render_pages_ns = monotonic_ns() - render_pages_started_ns
        except LargeZipFile as error:
            raise PresentationImageError("presentation-v2 forbids ZIP64") from error

        size_bytes = staged.tell()
        if not 1 <= size_bytes <= MAX_ARCHIVE_SIZE_BYTES:
            raise PresentationImageError("rendered archive exceeds the v2 size cap")
        archive_inspect_started_ns = monotonic_ns()
        if progress is not None:
            progress.operation("archive_inspect")
        staged.seek(0)
        artifact_sha256 = _stream_digest(cast(BinaryIO, staged), size_bytes)
        staged.seek(0)
        inspected = inspect_presentation_archive(
            cast(BinaryIO, staged),
            tuple(page.locator for page in page_evidence),
        )
        if (
            inspected.archive_sha256 != artifact_sha256
            or inspected.archive_size_bytes != size_bytes
        ):
            raise PresentationImageError(
                "rendered archive inspection changed its byte authority"
            )
        archive_inspect_ns = monotonic_ns() - archive_inspect_started_ns
        archive_finalize_started_ns = monotonic_ns()
        if progress is not None:
            progress.operation("archive_finalize")
        staged.flush()
        staged.seek(0)
        archive_finalize_ns = monotonic_ns() - archive_finalize_started_ns
        if preparation is not None:
            preparation._remember(inspected)
    evidence = ArtifactArchiveRenderEvidence(
        artifact_sha256=artifact_sha256,
        size_bytes=size_bytes,
        media_type=ARCHIVE_MEDIA_TYPE,
        download_name=download_name,
        pages=tuple(page_evidence),
    )
    if progress is not None:
        progress.advance("archives_rendered")
    emit_ingest_metric(
        metrics_sink,
        IngestMetric(
            scope="artifact",
            operation="render_archive",
            elapsed_ns=monotonic_ns() - started_ns,
            phases_ns=(
                IngestMetricValue("render_pages", render_pages_ns),
                IngestMetricValue("archive_inspect", archive_inspect_ns),
                IngestMetricValue("archive_finalize", archive_finalize_ns),
            ),
            counters=(
                IngestMetricValue("source_members", len(members)),
                IngestMetricValue(
                    "source_bytes",
                    sum(member.expected_size_bytes for member in members),
                ),
                IngestMetricValue("pages", len(page_evidence)),
                IngestMetricValue("page_render_workers", workers),
                IngestMetricValue("archive_bytes", size_bytes),
            ),
        ),
    )
    return evidence


def _render_page_member(
    member: ArtifactSourceMember,
    *,
    policy: ArtifactRenderPolicy,
    progress: ProgressWork | None = None,
) -> _RenderedPageBuffer:
    stream = cast(
        BinaryIO,
        SpooledTemporaryFile(max_size=4 * 1024 * 1024, mode="w+b"),
    )
    try:
        _verify_source_stream(member)
        image = _render_page(member.source, stream, policy=policy)
        _verify_source_stream(member)
        stream.seek(0)
        # Record completion in the worker: an earlier slow future must not hide
        # another page's successful render. The captured work fences late workers.
        if progress is not None:
            progress.advance("pages_rendered")
        return _RenderedPageBuffer(image=image, stream=stream)
    except BaseException as error:
        attach_page_failure_context(
            error,
            source_position=member.position,
            source_name=member.source_name,
            expected_size_bytes=member.expected_size_bytes,
        )
        close_resources((stream,), error=error)
        raise


def _render_page_batch(
    members: tuple[ArtifactSourceMember, ...],
    *,
    policy: ArtifactRenderPolicy,
    executor: ThreadPoolExecutor | None,
    progress: ProgressWork | None = None,
    gid: int | None = None,
) -> tuple[_RenderedPageBuffer, ...]:
    context = replace(
        current_image_log_context() or SourceImageLogContext("archive_render"),
        operation="archive_render",
        gid=gid,
    )
    if executor is None:
        rendered_members: list[_RenderedPageBuffer] = []
        try:
            for member in members:
                rendered_members.append(
                    _render_contextual_page_member(
                        member, policy=policy, progress=progress, context=context
                    )
                )
        except BaseException as error:
            close_resources(rendered_members, error=error)
            raise
        return tuple(rendered_members)

    futures: tuple[Future[_RenderedPageBuffer], ...] = tuple(
        executor.submit(
            _render_contextual_page_member,
            member,
            policy=policy,
            progress=progress,
            context=context,
        )
        for member in members
    )
    try:
        return tuple(future.result() for future in futures)
    except BaseException as error:
        for future in futures:
            future.cancel()
        wait(futures)
        closed: set[int] = set()
        completed: list[_RenderedPageBuffer] = []
        for future in futures:
            if future.cancelled() or future.exception() is not None:
                continue
            rendered = future.result()
            identity = id(rendered)
            if identity not in closed:
                completed.append(rendered)
                closed.add(identity)
        close_resources(completed, error=error)
        raise


def _render_contextual_page_member(
    member: ArtifactSourceMember,
    *,
    policy: ArtifactRenderPolicy,
    progress: ProgressWork | None,
    context: SourceImageLogContext,
) -> _RenderedPageBuffer:
    with image_log_scope(
        replace(
            context,
            source_name=member.source_name,
            source_position=member.position,
            expected_size_bytes=member.expected_size_bytes,
            source_sha256=member.expected_sha256,
        )
    ):
        return _render_page_member(member, policy=policy, progress=progress)


def _preflight_archive_members(
    members: tuple[ArtifactSourceMember, ...],
) -> tuple[ArtifactSourceMember, tuple[ArtifactSourceMember, ...]]:
    if len(members) > _MAX_SOURCE_MEMBER_COUNT:
        raise PresentationImageError("artifact source member count exceeds its bound")
    metadata: ArtifactSourceMember | None = None
    pages: list[ArtifactSourceMember] = []
    previous_position: int | None = None
    for member in members:
        role = _validate_source_member(member)
        if previous_position is not None and member.position <= previous_position:
            raise PresentationImageError(
                "selected source positions must be strictly increasing"
            )
        previous_position = member.position
        if role is ArtifactSourceRole.OTHER:
            raise PresentationImageError(
                "OTHER sources must never cross the archive render boundary"
            )
        if role is ArtifactSourceRole.METADATA:
            if metadata is not None:
                raise PresentationImageError(
                    "artifact source has more than one metadata member"
                )
            if member.expected_size_bytes > MAX_METADATA_BYTES:
                raise PresentationImageError(
                    "artifact source exceeds its encoded-size bound"
                )
            metadata = member
            continue
        pages.append(member)
        if len(pages) > MAX_PAGE_COUNT:
            raise PresentationImageError("presentation exceeds 4096 pages")
    if metadata is None:
        raise PresentationImageError("artifact source lacks its unique metadata member")
    return metadata, tuple(pages)


def _validate_source_member(member: ArtifactSourceMember) -> ArtifactSourceRole:
    if not isinstance(member, ArtifactSourceMember):
        raise TypeError("archive render contains a foreign source member")
    member.__post_init__()
    if type(member.role) is not ArtifactSourceRole:
        raise PresentationImageError("artifact source role is unsupported")
    if type(member.source_name) is not bytes or not 1 <= len(member.source_name) <= 255:
        raise PresentationImageError("artifact source name is outside policy")
    if type(member.expected_sha256) is not bytes or len(member.expected_sha256) != 32:
        raise PresentationImageError("artifact source SHA-256 must contain 32 bytes")
    if type(member.expected_size_bytes) is not int or member.expected_size_bytes < 1:
        raise PresentationImageError("artifact source size must be positive")
    if not all(hasattr(member.source, method) for method in ("read", "seek")):
        raise PresentationImageError("artifact source must be a seekable binary stream")
    return member.role


def _verify_source_stream(member: ArtifactSourceMember) -> None:
    """Verify exact observed bytes in bounded reads, independent of input size."""
    member.source.seek(0)
    digest = sha256()
    remaining = member.expected_size_bytes
    while remaining:
        part = member.source.read(min(_COPY_BUFFER_BYTES, remaining))
        if type(part) is not bytes:
            raise PresentationImageError("artifact source did not yield bytes")
        if not part:
            raise VNextSourceChangedError("artifact source ended before its exact size")
        digest.update(part)
        remaining -= len(part)
    trailing = member.source.read(1)
    if type(trailing) is not bytes:
        raise PresentationImageError("artifact source did not yield bytes")
    if trailing:
        raise VNextSourceChangedError("artifact source exceeds its exact size")
    if digest.digest() != member.expected_sha256:
        raise VNextSourceChangedError("artifact source SHA-256 disagrees")
    member.source.seek(0)


def _copy_exact_source(
    member: ArtifactSourceMember,
    destination: BinaryIO,
) -> None:
    member.source.seek(0)
    _copy_exact_bytes(
        member.source,
        destination,
        size=member.expected_size_bytes,
        label="artifact source",
    )
    member.source.seek(0)


def _copy_exact_bytes(
    source: BinaryIO,
    destination: BinaryIO,
    *,
    size: int,
    label: str,
) -> None:
    remaining = size
    while remaining:
        part = source.read(min(_COPY_BUFFER_BYTES, remaining))
        if type(part) is not bytes or not part:
            raise PresentationImageError(f"{label} ended before its exact size")
        _write_all(destination, part, label=label)
        remaining -= len(part)
    trailing = source.read(1)
    if type(trailing) is not bytes:
        raise PresentationImageError(f"{label} did not yield bytes")
    if trailing:
        raise PresentationImageError(f"{label} exceeds its exact size")


def _write_all(destination: BinaryIO, content: bytes, *, label: str) -> None:
    view = memoryview(content)
    offset = 0
    while offset < len(view):
        written = destination.write(view[offset:])
        if type(written) is not int or written <= 0 or written > len(view) - offset:
            raise PresentationImageError(f"{label} destination write made no progress")
        offset += written


def _canonical_zip_info(
    name: str,
    *,
    compression: int,
    file_size: int,
) -> ZipInfo:
    info = ZipInfo(name, date_time=_CANONICAL_ZIP_DATE_TIME)
    info.compress_type = compression
    info.create_system = _CANONICAL_CREATE_SYSTEM
    info.create_version = _CANONICAL_ZIP_VERSION
    info.extract_version = _CANONICAL_ZIP_VERSION
    info.external_attr = _CANONICAL_EXTERNAL_ATTR
    info.internal_attr = 0
    info.flag_bits = 0
    info.file_size = file_size
    return info


def _deflate_worst_case(size: int) -> int:
    # zlib's public compressBound formula; raw DEFLATE cannot exceed this.
    return size + (size >> 12) + (size >> 14) + (size >> 25) + 13


def _require_projected_archive_size(
    current_size: int,
    existing_names: list[str],
    next_name: str,
    next_encoded_size: int,
) -> None:
    names = (*existing_names, next_name)
    projected = (
        current_size
        + _LOCAL_FILE_HEADER.size
        + len(next_name.encode("ascii", errors="strict"))
        + next_encoded_size
        + sum(
            _CENTRAL_DIRECTORY_HEADER_BYTES + len(name.encode("ascii", errors="strict"))
            for name in names
        )
        + _END_OF_CENTRAL_DIRECTORY.size
    )
    if projected > MAX_ARCHIVE_SIZE_BYTES:
        raise PresentationImageError(
            "presentation archive would require ZIP64 before member write"
        )


def inspect_presentation_archive(
    archive: BinaryIO,
    page_member_names: tuple[str, ...],
) -> PreparedPresentationEvidence:
    """Verify exact stored JPEG extents in one canonical acquisition.

    ``page_member_names`` contains opaque locators previously emitted by this
    adapter's archive renderer. ZIP parsing happens once during ingest preparation;
    OPDS receives byte extents and never parses or decompresses the archive at
    request time.
    """

    if not hasattr(archive, "seek") or not hasattr(archive, "read"):
        raise TypeError("archive must be a seekable binary stream")
    names = tuple(page_member_names)
    if len(names) > MAX_PAGE_COUNT:
        raise PresentationImageError("presentation exceeds 4096 pages")
    if any(
        name != canonical_page_member_name(index) for index, name in enumerate(names)
    ):
        raise PresentationImageError("presentation page names are not canonical")
    if len(set(names)) != len(names):
        raise PresentationImageError("presentation page names must be unique")

    archive.seek(0, 2)
    archive_size = archive.tell()
    if archive_size < 1:
        raise PresentationImageError("presentation archive is empty")
    if archive_size > MAX_ARCHIVE_SIZE_BYTES:
        raise PresentationImageError("presentation archive exceeds the v2 size cap")
    archive.seek(0)
    archive_digest = _stream_digest(archive, archive_size)
    archive.seek(0)
    member_count, central_offset = _bounded_zip_directory(
        archive,
        archive_size=archive_size,
        expected_member_names=(_METADATA_MEMBER_NAME, *names),
    )
    if member_count != len(names) + 1:
        raise PresentationImageError("archive member count is not presentation-closed")

    try:
        with ZipFile(archive, mode="r") as opened:
            infos = opened.infolist()
            if len(infos) != member_count:
                raise PresentationImageError("archive central-directory count changed")
            expected_names = (_METADATA_MEMBER_NAME, *names)
            observed_names = tuple(info.filename for info in infos)
            if observed_names != expected_names:
                raise PresentationImageError(
                    "archive member order or coverage is not canonical"
                )
            if opened.comment:
                raise PresentationImageError("presentation ZIP comment must be empty")
            _validate_local_member_layout(
                archive,
                infos,
                central_offset=central_offset,
                archive_size=archive_size,
            )
            metadata = infos[0]
            _validate_metadata_member(
                archive,
                opened,
                metadata,
                archive_size=archive_size,
            )
            infos_by_name = {info.filename: info for info in infos}
            pages = tuple(
                _inspect_page_extent(
                    archive,
                    infos_by_name.get(name),
                    member_name=name,
                    page_index=index,
                    archive_size=archive_size,
                )
                for index, name in enumerate(names)
            )
    except (BadZipFile, zlib.error) as error:
        raise PresentationImageError(
            "presentation archive is not a valid ZIP"
        ) from error

    ordered_extents = sorted(
        (page.byte_offset, page.byte_offset + page.image.size_bytes) for page in pages
    )
    if any(
        left_end > right_start
        for (_, left_end), (right_start, _) in pairwise(ordered_extents)
    ):
        raise PresentationImageError("presentation page extents overlap")

    archive.seek(0)
    return PreparedPresentationEvidence(
        archive_sha256=archive_digest,
        archive_size_bytes=archive_size,
        pages=pages,
    )


def render_presentation(
    archive: BinaryIO,
    thumbnail_destination: BinaryIO,
    *,
    rendered_pages: tuple[ArtifactRenderedPage, ...],
    policy: ArtifactRenderPolicy,
    metrics_sink: IngestMetricSink | None = None,
    progress: ProgressWork | None = None,
) -> ArtifactPresentationRenderEvidence:
    """Fully inspect acquisition bytes and render a standalone thumbnail."""

    return _render_presentation(
        archive,
        thumbnail_destination,
        rendered_pages=rendered_pages,
        policy=policy,
        metrics_sink=metrics_sink,
        preparation=None,
        progress=progress,
    )


def _render_presentation(
    archive: BinaryIO,
    thumbnail_destination: BinaryIO,
    *,
    rendered_pages: tuple[ArtifactRenderedPage, ...],
    policy: ArtifactRenderPolicy,
    metrics_sink: IngestMetricSink | None = None,
    preparation: ArtifactPreparationRenderer | None,
    progress: ProgressWork | None = None,
) -> ArtifactPresentationRenderEvidence:
    """Derive neutral page facts and write one standalone thumbnail.

    The destination belongs to core and is intentionally treated as write-only.
    Returned digests are evidence only; core rehashes every page extent and the
    complete thumbnail before it persists or protects either resource.
    """

    started_ns = monotonic_ns()
    if progress is not None:
        progress.operation("presentation_inspect")
    if type(rendered_pages) is not tuple:
        raise TypeError("rendered_pages must be an exact tuple")
    if not isinstance(policy, ArtifactRenderPolicy):
        raise TypeError("artifact policy must be ArtifactRenderPolicy")
    policy.__post_init__()
    if not hasattr(thumbnail_destination, "write"):
        raise TypeError("thumbnail_destination must be writable")
    for page_index, page in enumerate(rendered_pages):
        if not isinstance(page, ArtifactRenderedPage):
            raise TypeError("rendered_pages contains foreign evidence")
        page.__post_init__()
        if page.page_index != page_index:
            raise PresentationImageError("rendered page indices must be dense")
        if page.locator != canonical_page_member_name(page_index):
            raise PresentationImageError("rendered page locator is not canonical")
    if any(
        left.source_position >= right.source_position
        for left, right in pairwise(rendered_pages)
    ):
        raise PresentationImageError(
            "rendered page source positions must be strictly increasing"
        )

    archive_inspect_started_ns = monotonic_ns()
    names = tuple(page.locator for page in rendered_pages)
    inspected = (
        inspect_presentation_archive(archive, names)
        if preparation is None
        else preparation._inspect_actual_bytes(archive, names)
    )
    archive_inspect_ns = monotonic_ns() - archive_inspect_started_ns
    presentation_started_ns = monotonic_ns()
    pages = tuple(
        ArtifactPagePresentationEvidence(
            page_index=page.page_index,
            locator=page.member_name,
            extent=ByteExtent(page.byte_offset, page.image.size_bytes),
            media_type=page.image.media_type,
            sha256=page.image.sha256,
            width=page.image.width,
            height=page.image.height,
        )
        for page in inspected.pages
    )
    presentation_ns = monotonic_ns() - presentation_started_ns
    thumbnail: ArtifactThumbnailPresentationEvidence | None = None
    thumbnail_ns = 0
    if inspected.cover is not None:
        thumbnail_started_ns = monotonic_ns()
        if progress is not None:
            progress.operation("thumbnail_render")
        image = _render_thumbnail(
            archive,
            inspected.cover,
            thumbnail_destination,
            policy=policy,
        )
        thumbnail = ArtifactThumbnailPresentationEvidence(
            size_bytes=image.size_bytes,
            media_type=image.media_type,
            sha256=image.sha256,
            width=image.width,
            height=image.height,
        )
        thumbnail_ns = monotonic_ns() - thumbnail_started_ns
    archive.seek(0)
    evidence = ArtifactPresentationRenderEvidence(pages=pages, thumbnail=thumbnail)
    if progress is not None:
        progress.advance("presentations_rendered")
    emit_ingest_metric(
        metrics_sink,
        IngestMetric(
            scope="artifact",
            operation="render_presentation",
            elapsed_ns=monotonic_ns() - started_ns,
            phases_ns=(
                IngestMetricValue("archive_inspect", archive_inspect_ns),
                IngestMetricValue("presentation", presentation_ns),
                IngestMetricValue("thumbnail", thumbnail_ns),
            ),
            counters=(
                IngestMetricValue("pages", len(pages)),
                IngestMetricValue("archive_bytes", inspected.archive_size_bytes),
                IngestMetricValue(
                    "thumbnail_bytes",
                    0 if thumbnail is None else thumbnail.size_bytes,
                ),
            ),
        ),
    )
    return evidence


def canonical_page_member_name(page_index: int) -> str:
    """Return the only page-member spelling accepted by presentation-v2."""

    if type(page_index) is not int or not 0 <= page_index < MAX_PAGE_COUNT:
        raise ValueError("page_index is outside presentation policy")
    return f"pages/{page_index:04d}.jpg"


def _bounded_zip_directory(
    archive: BinaryIO,
    *,
    archive_size: int,
    expected_member_names: tuple[str, ...],
) -> tuple[int, int]:
    """Read EOCD before ZipFile can allocate an unbounded member list."""

    window_size = min(
        archive_size,
        _END_OF_CENTRAL_DIRECTORY.size + _MAX_ZIP_COMMENT_BYTES,
    )
    archive.seek(archive_size - window_size)
    tail = archive.read(window_size)
    marker = tail.rfind(_END_OF_CENTRAL_DIRECTORY_SIGNATURE)
    if marker < 0 or marker + _END_OF_CENTRAL_DIRECTORY.size > len(tail):
        raise PresentationImageError("archive lacks a bounded ZIP central directory")
    fields = _END_OF_CENTRAL_DIRECTORY.unpack_from(tail, marker)
    (
        _,
        disk_number,
        central_disk,
        disk_entries,
        total_entries,
        central_size,
        central_offset,
        comment_size,
    ) = fields
    if disk_number != 0 or central_disk != 0 or disk_entries != total_entries:
        raise PresentationImageError("multi-disk presentation ZIP is unsupported")
    if total_entries > MAX_PAGE_COUNT + 1:
        raise PresentationImageError("archive has too many members")
    if total_entries != len(expected_member_names):
        raise PresentationImageError("archive member count is not presentation-closed")
    if comment_size != 0:
        raise PresentationImageError("presentation ZIP comment must be empty")
    if marker + _END_OF_CENTRAL_DIRECTORY.size + comment_size != len(tail):
        raise PresentationImageError("archive EOCD or comment length is invalid")
    expected_central_size = sum(
        _CENTRAL_DIRECTORY_HEADER_BYTES + len(name.encode("ascii", errors="strict"))
        for name in expected_member_names
    )
    if central_size != expected_central_size:
        raise PresentationImageError("archive central directory size is not canonical")
    eocd_offset = archive_size - window_size + marker
    if central_offset + central_size != eocd_offset:
        raise PresentationImageError("archive central directory is not canonical")
    _validate_raw_central_directory(
        archive,
        central_offset=central_offset,
        central_size=central_size,
        expected_member_names=expected_member_names,
    )
    archive.seek(0)
    return total_entries, central_offset


def _validate_raw_central_directory(
    archive: BinaryIO,
    *,
    central_offset: int,
    central_size: int,
    expected_member_names: tuple[str, ...],
) -> None:
    """Reject central fields that ``zipfile`` may normalize while parsing."""

    archive.seek(central_offset)
    expected_local_offset = 0
    members: list[_RawCentralMember] = []
    for index, expected_name_text in enumerate(expected_member_names):
        header = archive.read(_CENTRAL_DIRECTORY_HEADER.size)
        if len(header) != _CENTRAL_DIRECTORY_HEADER.size:
            raise PresentationImageError("archive central directory is truncated")
        (
            signature,
            create_version,
            extract_version,
            flag_bits,
            compression,
            modified_time,
            modified_date,
            _crc32_value,
            compressed_size,
            uncompressed_size,
            name_size,
            extra_size,
            comment_size,
            disk_start,
            internal_attr,
            external_attr,
            local_offset,
        ) = _CENTRAL_DIRECTORY_HEADER.unpack(header)
        if signature != _CENTRAL_DIRECTORY_HEADER_SIGNATURE:
            raise PresentationImageError(
                "member central-directory signature is invalid"
            )
        if (
            create_version != ((_CANONICAL_CREATE_SYSTEM << 8) | _CANONICAL_ZIP_VERSION)
            or extract_version != _CANONICAL_ZIP_VERSION
        ):
            raise PresentationImageError(
                "member central-directory attributes disagree: version is not canonical"
            )
        if flag_bits != 0:
            raise PresentationImageError(
                "member central-directory flags are not canonical"
            )
        expected_compression = ZIP_DEFLATED if index == 0 else ZIP_STORED
        if compression != expected_compression:
            if index == 0:
                raise PresentationImageError(
                    "presentation metadata must use ZIP_DEFLATED"
                )
            raise PresentationImageError(
                "presentation page members must use ZIP_STORED"
            )
        if modified_time != 0 or modified_date != 33:
            raise PresentationImageError(
                "member central-directory timestamp is not canonical"
            )
        if index == 0:
            if not 1 <= uncompressed_size <= MAX_METADATA_BYTES:
                raise PresentationImageError(
                    "presentation metadata size is outside policy"
                )
            if not 1 <= compressed_size <= _deflate_worst_case(MAX_METADATA_BYTES):
                raise PresentationImageError(
                    "presentation metadata compressed size is outside policy"
                )
        elif (
            compressed_size != uncompressed_size
            or not 1 <= uncompressed_size <= MAX_ENCODED_PAGE_BYTES
        ):
            raise PresentationImageError(
                "presentation page central-directory sizes are not canonical"
            )
        try:
            expected_name = expected_name_text.encode("ascii", errors="strict")
        except UnicodeEncodeError as error:  # pragma: no cover - caller prevalidates
            raise PresentationImageError(
                "member filename is not canonical ASCII"
            ) from error
        if name_size != len(expected_name):
            raise PresentationImageError(
                "member central-directory filename length disagrees"
            )
        if extra_size != 0 or comment_size != 0:
            raise PresentationImageError(
                "member central-directory extra data is not canonical"
            )
        if disk_start != 0 or internal_attr != 0:
            raise PresentationImageError("member central-directory attributes disagree")
        if external_attr != _CANONICAL_EXTERNAL_ATTR:
            raise PresentationImageError("member central-directory attributes disagree")
        if local_offset != expected_local_offset:
            raise PresentationImageError(
                "member central-directory local offset is not canonical"
            )
        observed_name = archive.read(name_size)
        if observed_name != expected_name:
            raise PresentationImageError(
                "archive member order or coverage is not canonical: "
                "central-directory filename disagrees"
            )
        members.append(
            _RawCentralMember(
                expected_name,
                compression,
                _crc32_value,
                compressed_size,
                uncompressed_size,
                local_offset,
            )
        )
        expected_local_offset = (
            local_offset + _LOCAL_FILE_HEADER.size + name_size + compressed_size
        )
        if expected_local_offset > central_offset:
            raise PresentationImageError(
                "member central-directory local extent is outside the archive"
            )
    if archive.tell() != central_offset + central_size:
        raise PresentationImageError("archive central directory is not canonical")
    if expected_local_offset != central_offset:
        raise PresentationImageError(
            "member local data does not end at the central directory"
        )
    _validate_raw_local_members(
        archive,
        tuple(members),
        central_offset=central_offset,
    )


def _validate_raw_local_members(
    archive: BinaryIO,
    members: tuple[_RawCentralMember, ...],
    *,
    central_offset: int,
) -> None:
    """Compare every raw local field with the bounded central authority."""

    expected_header_offset = 0
    for member in members:
        if member.local_offset != expected_header_offset:
            raise PresentationImageError(
                "member central-directory local offset is not canonical"
            )
        archive.seek(member.local_offset)
        header = archive.read(_LOCAL_FILE_HEADER.size)
        if len(header) != _LOCAL_FILE_HEADER.size:
            raise PresentationImageError("member local header is truncated")
        (
            signature,
            extract_version,
            flag_bits,
            compression,
            modified_time,
            modified_date,
            crc32_value,
            compressed_size,
            uncompressed_size,
            name_size,
            extra_size,
        ) = _LOCAL_FILE_HEADER.unpack(header)
        if signature != _LOCAL_FILE_HEADER_SIGNATURE:
            raise PresentationImageError("member local header signature is invalid")
        if extract_version != _CANONICAL_ZIP_VERSION:
            raise PresentationImageError("member local extraction version disagrees")
        if flag_bits != 0:
            raise PresentationImageError("member ZIP flags are not canonical")
        if compression != member.compression:
            raise PresentationImageError("member local compression disagrees")
        if modified_time != 0 or modified_date != 33:
            raise PresentationImageError("member local timestamp disagrees")
        if crc32_value != member.crc32:
            raise PresentationImageError("member local CRC disagrees")
        if (
            compressed_size != member.compressed_size
            or uncompressed_size != member.uncompressed_size
        ):
            raise PresentationImageError("member local sizes disagree")
        if name_size != len(member.name):
            raise PresentationImageError("member local filename length disagrees")
        if extra_size != 0:
            raise PresentationImageError("member ZIP extra data is not canonical")
        if archive.read(name_size) != member.name:
            raise PresentationImageError("member local filename disagrees")
        expected_header_offset = (
            member.local_offset + _LOCAL_FILE_HEADER.size + name_size + compressed_size
        )
        if expected_header_offset > central_offset:
            raise PresentationImageError("member byte extent is outside the archive")
    if expected_header_offset != central_offset:
        raise PresentationImageError(
            "member local data does not end at the central directory"
        )


def _validate_local_member_layout(
    archive: BinaryIO,
    infos: list[ZipInfo],
    *,
    central_offset: int,
    archive_size: int,
) -> None:
    expected_header_offset = 0
    for info in infos:
        if info.header_offset != expected_header_offset:
            raise PresentationImageError(
                "member local headers are not contiguously canonical"
            )
        data_offset = _member_data_offset(
            archive,
            info,
            archive_size=archive_size,
        )
        expected_header_offset = data_offset + info.compress_size
    if expected_header_offset != central_offset:
        raise PresentationImageError(
            "member local data does not end at the central directory"
        )


def _validate_metadata_member(
    archive: BinaryIO,
    opened: ZipFile,
    info: ZipInfo,
    *,
    archive_size: int,
) -> None:
    if info.is_dir() or info.filename != _METADATA_MEMBER_NAME:
        raise PresentationImageError("presentation metadata member is invalid")
    if info.flag_bits & 0x1:
        raise PresentationImageError("encrypted presentation metadata is unsupported")
    if info.compress_type != ZIP_DEFLATED:
        raise PresentationImageError("presentation metadata must use ZIP_DEFLATED")
    if not 1 <= info.file_size <= MAX_METADATA_BYTES:
        raise PresentationImageError("presentation metadata size is outside policy")
    if not 1 <= info.compress_size <= _deflate_worst_case(MAX_METADATA_BYTES):
        raise PresentationImageError(
            "presentation metadata compressed size is outside policy"
        )
    _member_data_offset(archive, info, archive_size=archive_size)
    try:
        with opened.open(info, mode="r") as source:
            content = source.read(MAX_METADATA_BYTES + 1)
            if source.read(1):
                raise PresentationImageError("presentation metadata exceeds its bound")
    except (BadZipFile, zlib.error) as error:
        raise PresentationImageError(
            "presentation metadata CRC does not match"
        ) from error
    if len(content) != info.file_size:
        raise PresentationImageError("presentation metadata size does not match")


def _render_page(
    source: BinaryIO,
    destination: BinaryIO,
    *,
    policy: ArtifactRenderPolicy,
) -> CanonicalImageEvidence:
    policy.__post_init__()
    image = load_source_page_image(source, policy=policy)
    try:
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


def _inspect_page_extent(
    archive: BinaryIO,
    info: ZipInfo | None,
    *,
    member_name: str,
    page_index: int,
    archive_size: int,
) -> PreparedPageEvidence:
    if info is None:
        raise PresentationImageError(f"archive lacks page member: {member_name}")
    if info.is_dir() or info.compress_type != ZIP_STORED:
        raise PresentationImageError("presentation page members must use ZIP_STORED")
    if info.flag_bits & 0x1:
        raise PresentationImageError("encrypted presentation pages are unsupported")
    if info.file_size != info.compress_size:
        raise PresentationImageError("stored presentation page sizes disagree")
    if not 1 <= info.file_size <= MAX_ENCODED_PAGE_BYTES:
        raise PresentationImageError("presentation page encoded size is outside policy")

    offset = _member_data_offset(archive, info, archive_size=archive_size)
    content = _read_extent(archive, offset=offset, size=info.file_size)
    if zlib.crc32(content) & 0xFFFFFFFF != info.CRC:
        raise PresentationImageError("presentation page CRC does not match")
    image = _verify_canonical_jpeg(content)
    return PreparedPageEvidence(
        page_index=page_index,
        member_name=member_name,
        byte_offset=offset,
        image=image,
    )


def _member_data_offset(
    archive: BinaryIO,
    info: ZipInfo,
    *,
    archive_size: int,
) -> int:
    if (
        info.header_offset < 0
        or info.header_offset + _LOCAL_FILE_HEADER.size > archive_size
    ):
        raise PresentationImageError("member local header is outside the archive")
    archive.seek(info.header_offset)
    header = archive.read(_LOCAL_FILE_HEADER.size)
    if len(header) != _LOCAL_FILE_HEADER.size:
        raise PresentationImageError("member local header is truncated")
    unpacked = _LOCAL_FILE_HEADER.unpack(header)
    if unpacked[0] != _LOCAL_FILE_HEADER_SIGNATURE:
        raise PresentationImageError("member local header signature is invalid")
    (
        _,
        extract_version,
        flag_bits,
        compression,
        modified_time,
        modified_date,
        crc32_value,
        compressed_size,
        uncompressed_size,
        name_size,
        extra_size,
    ) = unpacked
    if (
        info.create_system != _CANONICAL_CREATE_SYSTEM
        or info.create_version != _CANONICAL_ZIP_VERSION
        or info.extract_version != _CANONICAL_ZIP_VERSION
        or info.external_attr != _CANONICAL_EXTERNAL_ATTR
        or info.internal_attr != 0
        or info.volume != 0
    ):
        raise PresentationImageError("member central-directory attributes disagree")
    if extract_version != info.extract_version:
        raise PresentationImageError("member local extraction version disagrees")
    if flag_bits != 0 or info.flag_bits != 0:
        raise PresentationImageError("member ZIP flags are not canonical")
    if compression != info.compress_type:
        raise PresentationImageError("member local compression disagrees")
    if crc32_value != info.CRC:
        raise PresentationImageError("member local CRC disagrees")
    if compressed_size != info.compress_size or uncompressed_size != info.file_size:
        raise PresentationImageError("member local sizes disagree")
    if info.date_time != _CANONICAL_ZIP_DATE_TIME:
        raise PresentationImageError("member timestamp is not canonical")
    if modified_time != 0 or modified_date != 33:
        raise PresentationImageError("member local timestamp disagrees")
    if info.extra or info.comment or extra_size != 0:
        raise PresentationImageError("member ZIP extra data is not canonical")
    try:
        expected_name = info.filename.encode("ascii", errors="strict")
    except UnicodeEncodeError as error:
        raise PresentationImageError(
            "member filename is not canonical ASCII"
        ) from error
    if name_size != len(expected_name):
        raise PresentationImageError("member local filename length disagrees")
    local_name = archive.read(name_size)
    if local_name != expected_name:
        raise PresentationImageError("member local filename disagrees")
    offset = int(info.header_offset + _LOCAL_FILE_HEADER.size + name_size + extra_size)
    end = offset + info.compress_size
    if offset < 0 or end < offset or end > archive_size:
        raise PresentationImageError("member byte extent is outside the archive")
    return offset


def _read_extent(archive: BinaryIO, *, offset: int, size: int) -> bytes:
    archive.seek(offset)
    content = archive.read(size)
    if len(content) != size:
        raise PresentationImageError("page byte extent ended unexpectedly")
    return content


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


def _stream_digest(source: BinaryIO, size: int) -> bytes:
    digest = sha256()
    remaining = size
    while remaining:
        chunk = source.read(min(_COPY_BUFFER_BYTES, remaining))
        if not chunk:
            raise PresentationImageError("archive ended before its observed size")
        digest.update(chunk)
        remaining -= len(chunk)
    if source.read(1):
        raise PresentationImageError("archive grew while it was inspected")
    return digest.digest()


def _validate_dimensions(width: int, height: int, *, max_long_side: int) -> None:
    if type(width) is not int or type(height) is not int or width < 1 or height < 1:
        raise PresentationImageError("image dimensions must be positive integers")
    if width > max_long_side or height > max_long_side:
        raise PresentationImageError("image long side exceeds presentation policy")
    if width * height > MAX_DECODED_PIXELS:
        raise PresentationImageError("image exceeds the 40 MP decoded-pixel policy")
