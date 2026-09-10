"""Attribute native image diagnostics without serializing concurrent decoding."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from itertools import islice
from threading import Lock

from ._log_fields import quote_log_field
from .page_workers import MAX_PAGE_RENDER_WORKERS

_MAXIMUM_FIELD_CHARACTERS = 1024
# Retain every page of a supported production worker batch. Independent callers
# may exceed that bound; report their omitted count without guessing a source.
_MAXIMUM_SOURCE_CANDIDATES = MAX_PAGE_RENDER_WORKERS


@dataclass(frozen=True, slots=True)
class SourceImageLogContext:
    """Diagnostic identity supplied by the worker owning the immutable source."""

    operation: str
    gid: int | None = None
    source_root_components: tuple[str, ...] | None = None
    gallery_locator_components: tuple[str, ...] | None = None
    source_name: bytes | None = None
    source_position: int | None = None
    expected_size_bytes: int | None = None
    source_sha256: bytes | None = None


_CURRENT_SOURCE: ContextVar[SourceImageLogContext | None] = ContextVar(
    "h2hdb_ingest_image_log_context", default=None
)
_ACTIVE_SOURCES: dict[object, SourceImageLogContext] = {}
_ACTIVE_SOURCES_LOCK = Lock()


def current_image_log_context() -> SourceImageLogContext | None:
    """Capture the caller's immutable context before submitting page workers."""

    return _CURRENT_SOURCE.get()


@contextmanager
def image_log_scope(context: SourceImageLogContext) -> Iterator[None]:
    """Bind one exact worker context; callers must pass it explicitly to workers."""

    token = _CURRENT_SOURCE.set(context)
    try:
        yield
    finally:
        _CURRENT_SOURCE.reset(token)


@contextmanager
def native_image_log_scope() -> Iterator[None]:
    """Retain bounded source candidates only while their native decode is active.

    GLib can log on a native worker that has no Python context. Such messages
    cannot be assigned to an exact source: even one active source is only a
    candidate, since unrelated users of libvips can log concurrently. Never
    retain thread IDs or a last-gallery hint after an operation completes.
    """

    context = _CURRENT_SOURCE.get()
    token = object()
    if context is not None:
        with _ACTIVE_SOURCES_LOCK:
            _ACTIVE_SOURCES[token] = context
    try:
        yield
    finally:
        if context is not None:
            with _ACTIVE_SOURCES_LOCK:
                del _ACTIVE_SOURCES[token]


def _quoted(value: str) -> str:
    if len(value) > _MAXIMUM_FIELD_CHARACTERS:
        value = value[:_MAXIMUM_FIELD_CHARACTERS] + "...[truncated]"
    return quote_log_field(value)


def _source_fields(context: SourceImageLogContext) -> str:
    fields = [f"operation={_quoted(context.operation)}"]
    if context.gid is not None:
        fields.append(f"gid={context.gid}")
    if context.source_root_components is not None:
        fields.append(
            "source_root=" + _quoted("/" + "/".join(context.source_root_components))
        )
        if context.gallery_locator_components is not None:
            fields.append(
                "gallery_folder="
                + _quoted(
                    "/"
                    + "/".join(
                        (
                            *context.source_root_components,
                            *context.gallery_locator_components,
                        )
                    )
                )
            )
    if context.source_name is not None:
        fields.append(
            "file="
            + _quoted(context.source_name.decode("utf-8", errors="backslashreplace"))
        )
    if context.source_position is not None:
        fields.append(f"source_position={context.source_position}")
    if context.expected_size_bytes is not None:
        fields.append(f"source_bytes={context.expected_size_bytes}")
    if context.source_sha256 is not None:
        fields.append("source_sha256=" + context.source_sha256.hex())
    return " ".join(fields)


class _NativeImageDiagnosticFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno < logging.WARNING:
            return True
        context = _CURRENT_SOURCE.get()
        if context is not None:
            attribution = "source_attribution=exact " + _source_fields(context)
        else:
            # Snapshot before formatting or logging: no handler executes while
            # this lock is held, including callbacks from native decoder threads.
            with _ACTIVE_SOURCES_LOCK:
                count = len(_ACTIVE_SOURCES)
                candidates = tuple(
                    islice(_ACTIVE_SOURCES.values(), _MAXIMUM_SOURCE_CANDIDATES)
                )
            if not candidates:
                return True
            attribution = (
                "source_attribution=active_candidates "
                f"active_source_count={count} "
                f"omitted_source_candidates={count - len(candidates)} "
                "active_source_candidates=["
                + "; ".join(
                    "{" + _source_fields(candidate) + "}" for candidate in candidates
                )
                + "]"
            )
        event = (
            "image_decoder_error"
            if record.levelno >= logging.ERROR
            else "image_decoder_warning"
        )
        record.msg = (
            f"event={event} {attribution} reason={_quoted(record.getMessage())}"
        )
        record.args = ()
        return True


# A logger filter runs before every handler and preserves the original logger
# and severity. It neither re-emits nor suppresses a native warning.
logging.getLogger("pyvips").addFilter(_NativeImageDiagnosticFilter())
