"""Immutable retry diagnostics captured before nested activity unwinds."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ._log_fields import diagnostic_text, quote_log_field

if TYPE_CHECKING:
    from .progress import ProgressSnapshot

_CONTEXT_ATTRIBUTE = "_h2hdb_ingest_failure_snapshot"
_MAX_ERROR_CHAIN = 32
_MAX_NOTES = 16
_MAX_DIAGNOSTIC_BYTES = 32 * 1024


@dataclass(frozen=True, slots=True)
class _FailureContext:
    snapshot: ProgressSnapshot


@dataclass(frozen=True, slots=True)
class RetryDiagnostic:
    """Keep text and measured progress, never exception-owned resources."""

    snapshot: ProgressSnapshot | None
    detail: str


def _error_chain(error: BaseException) -> Iterator[BaseException]:
    current: BaseException | None = error
    visited: set[int] = set()
    while current is not None and len(visited) < _MAX_ERROR_CHAIN:
        if id(current) in visited:
            break
        visited.add(id(current))
        yield current
        current = current.__cause__ or (
            None if current.__suppress_context__ else current.__context__
        )


def _failure_snapshot(
    error: BaseException, snapshot: ProgressSnapshot
) -> ProgressSnapshot | None:
    for cause in _error_chain(error):
        context = getattr(cause, _CONTEXT_ATTRIBUTE, None)
        if (
            isinstance(context, _FailureContext)
            and context.snapshot.generation == snapshot.generation
            and context.snapshot.work_identity is snapshot.work_identity
        ):
            return context.snapshot
    return None


def capture_failure_snapshot(error: BaseException, snapshot: ProgressSnapshot) -> None:
    """The innermost unwinding activity owns context for this work generation."""

    try:
        if _failure_snapshot(error, snapshot) is None:
            setattr(error, _CONTEXT_ATTRIBUTE, _FailureContext(snapshot))
    except Exception:
        # Diagnostics must preserve the original exception and cancellation.
        pass


def retry_diagnostic(
    error: BaseException, fallback: ProgressSnapshot | None
) -> RetryDiagnostic:
    try:
        return _retry_diagnostic(error, fallback)
    except Exception:
        # Optional exception metadata must never prevent lease completion.
        return RetryDiagnostic(
            fallback,
            "error_type="
            + quote_log_field(type(error).__name__)
            + " reason="
            + quote_log_field(diagnostic_text(error))
            + " diagnostic_context=unavailable",
        )


def _retry_diagnostic(
    error: BaseException, fallback: ProgressSnapshot | None
) -> RetryDiagnostic:
    snapshot = (
        None if fallback is None else _failure_snapshot(error, fallback) or fallback
    )
    fields = [
        "phase=" + quote_log_field("unknown" if snapshot is None else snapshot.phase),
        "operation="
        + quote_log_field(
            "unknown" if snapshot is None else snapshot.operation or snapshot.phase
        ),
    ]
    omitted = "diagnostics_truncated=true"
    remaining = _MAX_DIAGNOSTIC_BYTES - len(" ".join(fields).encode("utf-8"))
    remaining -= len(omitted) + 1
    for item in _exception_fields(error):
        size = len(item.encode("utf-8")) + 1
        if size > remaining:
            fields.append(omitted)
            break
        fields.append(item)
        remaining -= size
    return RetryDiagnostic(snapshot, " ".join(fields))


def _exception_fields(error: BaseException) -> Iterator[str]:
    for position, cause in enumerate(_error_chain(error)):
        prefix = "" if position == 0 else f"cause.{position}."
        yield prefix + "error_type=" + quote_log_field(type(cause).__name__)
        yield prefix + "reason=" + quote_log_field(diagnostic_text(cause))
        notes = getattr(cause, "__notes__", ())
        if isinstance(notes, (tuple, list)):
            yield from (
                prefix + f"note.{index}=" + quote_log_field(diagnostic_text(note))
                for index, note in enumerate(notes[:_MAX_NOTES])
            )
            if len(notes) > _MAX_NOTES:
                yield prefix + f"notes_omitted={len(notes) - _MAX_NOTES}"
