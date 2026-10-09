"""Prepare canonical acquisitions and presentation evidence from exact source bytes."""

from __future__ import annotations

import sys
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, replace
from hashlib import sha256
from itertools import pairwise
from tempfile import SpooledTemporaryFile
from threading import Lock
from time import monotonic_ns
from typing import BinaryIO, cast
from zipfile import ZIP_DEFLATED, ZIP_STORED, LargeZipFile, ZipFile

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

from .._image_performance import ImageWorkMeasurement, image_phase, measure_image_work
from .._limits import MAX_METADATA_BYTES
from .._resource_cleanup import close_resources, owned_resource
from ..artifact_errors import attach_page_failure_context
from ..image_diagnostics import (
    SourceImageLogContext,
    current_image_log_context,
    image_log_scope,
)
from ..metrics import (
    IngestMetric,
    IngestMetricSink,
    IngestMetricValue,
    emit_ingest_metric,
)
from ..page_workers import resolve_page_render_workers
from ..progress import IngestProgress, ProgressWork
from ..storage import artifact_name
from ._streams import _COPY_BUFFER_BYTES, _copy_exact_bytes, _stream_digest, _write_all
from .archive import (
    _METADATA_MEMBER_NAME,
    _canonical_zip_info,
    _deflate_worst_case,
    _require_projected_archive_size,
    canonical_page_member_name,
    inspect_presentation_archive,
)
from .images import _render_page, _render_thumbnail
from .model import (
    ARCHIVE_MEDIA_TYPE,
    MAX_ARCHIVE_SIZE_BYTES,
    MAX_PAGE_COUNT,
    ArtifactRenderPolicy,
    CanonicalImageEvidence,
    PreparedPresentationEvidence,
    PresentationImageError,
)

_MAX_SOURCE_MEMBER_COUNT = 8192


@dataclass(slots=True)
class _RenderedPageBuffer:
    image: CanonicalImageEvidence
    stream: BinaryIO
    measurement: ImageWorkMeasurement | None = None

    def close(self) -> None:
        self.stream.close()


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
    worker_phases: dict[str, int] = {}
    worker_elapsed_ns = 0
    worker_thread_cpu_ns = 0
    decoder_input_bytes = 0
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
                metadata_write_started_ns = monotonic_ns()
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
                metadata_write_ns = monotonic_ns() - metadata_write_started_ns
                _verify_source_stream(metadata)
                member_names.append(_METADATA_MEMBER_NAME)
                render_pages_started_ns = monotonic_ns()
                render_batches_ns = 0
                archive_page_write_ns = 0
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
                        render_batch_started_ns = monotonic_ns()
                        rendered_batch = _render_page_batch(
                            batch,
                            gid=gid,
                            policy=policy,
                            executor=executor,
                            progress=progress,
                        )
                        render_batches_ns += monotonic_ns() - render_batch_started_ns
                        try:
                            archive_page_write_started_ns = monotonic_ns()
                            if progress is not None:
                                progress.operation("archive_write_pages")
                            for offset, (member, rendered) in enumerate(
                                zip(batch, rendered_batch, strict=True)
                            ):
                                measured = rendered.measurement
                                if measured is not None:
                                    worker_elapsed_ns += measured.elapsed_ns
                                    worker_thread_cpu_ns += measured.thread_cpu_ns
                                    decoder_input_bytes += measured.decoder_input_bytes
                                    for name, elapsed in measured.phases_ns.items():
                                        worker_phases[name] = (
                                            worker_phases.get(name, 0) + elapsed
                                        )
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
                            archive_page_write_ns += (
                                monotonic_ns() - archive_page_write_started_ns
                            )
                        finally:
                            close_resources(rendered_batch, error=sys.exception())
                finally:
                    if executor is not None:
                        executor.shutdown(wait=True, cancel_futures=True)
                render_pages_ns = monotonic_ns() - render_pages_started_ns
                archive_close_started_ns = monotonic_ns()
            archive_close_ns = monotonic_ns() - archive_close_started_ns
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
                IngestMetricValue("render_batches", render_batches_ns),
                IngestMetricValue("archive_page_write", archive_page_write_ns),
                IngestMetricValue("archive_inspect", archive_inspect_ns),
                IngestMetricValue("archive_finalize", archive_finalize_ns),
                IngestMetricValue("archive_metadata_write", metadata_write_ns),
                IngestMetricValue("archive_zip_close", archive_close_ns),
                IngestMetricValue("worker_elapsed_sum", worker_elapsed_ns),
                IngestMetricValue("worker_thread_cpu_sum", worker_thread_cpu_ns),
                *(
                    IngestMetricValue("worker_" + name + "_sum", elapsed)
                    for name, elapsed in sorted(worker_phases.items())
                ),
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
                IngestMetricValue("decoder_input_logical_bytes", decoder_input_bytes),
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
        with measure_image_work() as measured:
            with image_phase("source_verify"):
                _verify_source_stream(member)
            image = _render_page(member.source, stream, policy=policy)
            with image_phase("source_verify"):
                _verify_source_stream(member)
        stream.seek(0)
        # Record completion in the worker: an earlier slow future must not hide
        # another page's successful render. The captured work fences late workers.
        if progress is not None:
            progress.advance("pages_rendered")
        return _RenderedPageBuffer(image=image, stream=stream, measurement=measured)
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
        match role:
            case ArtifactSourceRole.OTHER:
                raise PresentationImageError(
                    "OTHER sources must never cross the archive render boundary"
                )
            case ArtifactSourceRole.METADATA:
                if metadata is not None:
                    raise PresentationImageError(
                        "artifact source has more than one metadata member"
                    )
                if member.expected_size_bytes > MAX_METADATA_BYTES:
                    raise PresentationImageError(
                        "artifact source exceeds its encoded-size bound"
                    )
                metadata = member
            case _:
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
