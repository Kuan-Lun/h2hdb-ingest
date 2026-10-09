"""Qualify complete gallery image inputs before global publication selection."""

from __future__ import annotations

import logging
from collections import deque
from collections.abc import Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import nullcontext
from dataclasses import dataclass
from hashlib import sha256
from tempfile import SpooledTemporaryFile
from time import monotonic_ns, thread_time_ns
from typing import BinaryIO, Literal, cast

from h2hdb import ArtifactFailureContext, VNextSourceQualification

from ._image_performance import current_image_measurement, measure_image_work
from .artifact.images import load_source_page_image
from .artifact.model import ArtifactRenderPolicy
from .artifact_errors import (
    attach_page_failure_context,
    attach_qualification_failure_context,
    format_artifact_failure,
)
from .filesystem import (
    FilesystemArtifactSourceRole,
    FilesystemFileObservation,
    FilesystemGalleryObservation,
    FilesystemSource,
    FilesystemSourceChangedError,
)
from .image_diagnostics import SourceImageLogContext, image_log_scope
from .page_workers import MAX_PAGE_RENDER_WORKERS
from .progress import IngestProgress, ProgressWork
from .source_image import SourceImageDecodeError
from .source_performance import (
    SourcePerformance,
    current_qualification_performance,
    qualification_phase,
)

logger = logging.getLogger(__name__)
_SPOOL_MEMORY_BYTES = 4 * 1024 * 1024
_SOURCE_PAGE_SIZE = 128
_SPOOL_READ_BYTES = 1024 * 1024


@dataclass(frozen=True, slots=True)
class _PageFailure:
    member: FilesystemFileObservation
    error: Exception


def _is_decode_failure(error: BaseException) -> bool:
    current: BaseException | None = error
    seen: set[int] = set()
    for _ in range(32):
        if current is None or id(current) in seen:
            return False
        if isinstance(current, SourceImageDecodeError):
            return True
        seen.add(id(current))
        current = current.__cause__
    return False


def _pages(
    source: FilesystemSource,
    locator: tuple[str, ...],
    observed: FilesystemGalleryObservation,
) -> Iterator[tuple[int, FilesystemFileObservation]]:
    after: bytes | None = None
    position = 0
    while True:
        reopened, page = source.list_files(
            locator, after_name=after, limit=_SOURCE_PAGE_SIZE
        )
        if reopened != observed:
            raise FilesystemSourceChangedError(
                "gallery metadata changed during image qualification"
            )
        for member in page.items:
            if member.artifact_role is FilesystemArtifactSourceRole.PAGE:
                yield position, member
                position += 1
        if page.terminal:
            return
        after = page.items[-1].name_bytes


def _spool(member: FilesystemFileObservation, position: int) -> BinaryIO:
    stream = SpooledTemporaryFile(max_size=_SPOOL_MEMORY_BYTES, mode="w+b")
    performance = current_qualification_performance()
    expected_size = actual_size = 0
    try:
        # Exhausting content_parts verifies the original no-follow stat and hash
        # after the read. A source change is never a durable image rejection.
        expected_digest = sha256()
        for part in member.content_parts():
            with qualification_phase("owner_source_hash"):
                expected_digest.update(part)
            expected_size += len(part)
            if expected_size > member.stat.size_bytes:
                raise FilesystemSourceChangedError(
                    "qualification source grew beyond its observed size"
                )
            with qualification_phase("owner_spool_write"):
                written = stream.write(part)
            if written != len(part):
                raise OSError("qualification spool accepted a partial source write")
        if expected_size != member.stat.size_bytes:
            raise OSError("qualification source read did not match its observed size")
        stream.seek(0)
        actual_digest = sha256()
        while True:
            with qualification_phase("owner_spool_readback"):
                part = stream.read(_SPOOL_READ_BYTES)
            if not part:
                break
            with qualification_phase("owner_spool_hash"):
                actual_digest.update(part)
            actual_size += len(part)
        if (
            actual_size != expected_size
            or actual_digest.digest() != expected_digest.digest()
        ):
            raise OSError("qualification spool bytes differ from the exact source read")
        stream.seek(0)
        return cast(BinaryIO, stream)
    except BaseException as error:
        try:
            stream.close()
        except BaseException as close_error:
            error.add_note(f"Qualification spool also failed to close: {close_error!r}")
        attach_page_failure_context(
            error,
            source_position=position,
            source_name=member.name_bytes,
            expected_size_bytes=member.stat.size_bytes,
        )
        raise
    finally:
        if performance is not None:
            performance.qualification_count(
                "source_spooled_logical_bytes", expected_size
            )
            performance.qualification_count("spool_readback_logical_bytes", actual_size)
            performance.qualification_count("spool_attempts")
            performance.qualification_count(
                "disk_spool_attempts", int(bool(getattr(stream, "_rolled", False)))
            )


def _decode_page(
    stream: BinaryIO,
    member: FilesystemFileObservation,
    position: int,
    policy: ArtifactRenderPolicy,
    work: ProgressWork | None,
    context: SourceImageLogContext,
    performance: SourcePerformance | None = None,
) -> _PageFailure | None:
    if performance is None:
        return _decode_page_contents(stream, member, position, policy, work, context)
    # A dev probe may already own the worker measurement. Reuse that context,
    # while retaining this call's exact elapsed/CPU boundaries for production.
    existing = current_image_measurement()
    performance.qualification_count("worker_started")
    started, cpu_started = monotonic_ns(), thread_time_ns()
    outcome: Literal["accepted", "rejected", "failed", "interrupted"] = "failed"
    with (
        nullcontext(existing)
        if existing is not None
        else measure_image_work() as measured
    ):
        previous_detail = measured.qualification_details
        measured.qualification_details = True
        try:
            result = _decode_page_contents(
                stream, member, position, policy, work, context
            )
            outcome = "accepted" if result is None else "rejected"
            return result
        except BaseException as error:
            outcome = "failed" if isinstance(error, Exception) else "interrupted"
            raise
        finally:
            measured.qualification_details = previous_detail
            performance.record_qualification_worker(
                measured,
                elapsed_ns=monotonic_ns() - started,
                thread_cpu_ns=thread_time_ns() - cpu_started,
                encoded_bytes=member.stat.size_bytes,
                outcome=outcome,
            )


def _decode_page_contents(
    stream: BinaryIO,
    member: FilesystemFileObservation,
    position: int,
    policy: ArtifactRenderPolicy,
    work: ProgressWork | None,
    context: SourceImageLogContext,
) -> _PageFailure | None:
    with stream, image_log_scope(context):
        try:
            with load_source_page_image(stream, policy=policy):
                pass
        except Exception as error:
            attach_page_failure_context(
                error,
                source_position=position,
                source_name=member.name_bytes,
                expected_size_bytes=member.stat.size_bytes,
            )
            if not _is_decode_failure(error):
                raise
            return _PageFailure(member, error)
        finally:
            if work is not None:
                work.advance("source_images_checked")
    return None


class ImageGalleryQualifier:
    """Decode every PAGE using bounded concurrent spools and the actual renderer.

    The core binds this result to the exact observation and artifact policy. It
    can reuse unchanged completion markers without decoding those images again.
    Qualification does not certify later storage writes or source mutations.
    """

    def __init__(
        self,
        policy: ArtifactRenderPolicy,
        *,
        workers: int,
        progress: IngestProgress | None = None,
    ) -> None:
        if type(workers) is not int or not 1 <= workers <= MAX_PAGE_RENDER_WORKERS:
            raise ValueError(
                f"qualification workers must be from 1 through {MAX_PAGE_RENDER_WORKERS}"
            )
        self._policy = policy
        self._workers = workers
        self._progress = progress

    def __call__(
        self,
        source: FilesystemSource,
        locator: tuple[str, ...],
        observed: FilesystemGalleryObservation,
    ) -> VNextSourceQualification:
        work = None if self._progress is None else self._progress.current()
        activity = (
            nullcontext()
            if work is None
            else work.activity("source_image_qualification")
        )
        pending: deque[Future[_PageFailure | None]] = deque()
        performance = current_qualification_performance()
        failure: _PageFailure | None = None
        try:
            with (
                activity,
                ThreadPoolExecutor(
                    max_workers=self._workers, thread_name_prefix="image-qualification"
                ) as executor,
            ):
                for position, member in _pages(source, locator, observed):
                    with qualification_phase("owner_spool"):
                        stream = _spool(member, position)
                    try:
                        pending.append(
                            executor.submit(
                                _decode_page,
                                stream,
                                member,
                                position,
                                self._policy,
                                work,
                                SourceImageLogContext(
                                    operation="source_image_qualification",
                                    gid=observed.metadata.gid,
                                    source_root_components=source.source_root_components,
                                    gallery_locator_components=locator,
                                    source_name=member.name_bytes,
                                    source_position=position,
                                    expected_size_bytes=member.stat.size_bytes,
                                ),
                                performance,
                            )
                        )
                    except BaseException:
                        stream.close()
                        raise
                    if len(pending) >= self._workers:
                        with qualification_phase("owner_future_wait"):
                            failure = pending.popleft().result()
                        if failure is not None:
                            break
                # Consume all submitted results even after a corrupt page. Storage,
                # cancellation and programming errors must still propagate.
                for future in pending:
                    with qualification_phase("owner_future_wait"):
                        additional = future.result()
                    if failure is None:
                        failure = additional
        except Exception as error:
            attach_qualification_failure_context(
                error,
                ArtifactFailureContext(
                    gid=observed.metadata.gid,
                    source_root_components=source.source_root_components,
                    gallery_locator_components=locator,
                ),
            )
            # The resident owns fatal/capacity reporting and retry suppression.
            # Keep standalone diagnostics on the original exception and at DEBUG;
            # logging ERROR here would repeat it on every storage-pressure retry.
            logger.debug("%s", format_artifact_failure(error))
            raise
        if work is not None:
            work.advance("source_galleries_qualified")
        if failure is None:
            return VNextSourceQualification()
        if work is not None:
            work.advance("source_galleries_rejected")
        message = format_artifact_failure(
            failure.error,
            event="gallery_image_rejected",
            context=ArtifactFailureContext(
                gid=observed.metadata.gid,
                source_root_components=source.source_root_components,
                gallery_locator_components=locator,
            ),
        )
        logger.warning(
            "%s reason_code=invalid_image action=exclude_gallery_from_publication "
            "retry=source_marker_or_render_policy_change other_galleries=continue",
            message,
        )
        return VNextSourceQualification(
            accepted=False,
            reason_code="invalid_image",
            source_name=failure.member.name_bytes,
        )
