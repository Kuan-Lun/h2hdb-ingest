"""Bounded diagnostics for repeatedly failing, independently retried operations."""

from __future__ import annotations

from collections.abc import Callable
from hashlib import sha256
from logging import Logger
from threading import RLock
from time import monotonic


def _diagnostic_text(value: object) -> str:
    try:
        return str(value)
    except Exception:
        # Error reporting must survive an exception with a broken __str__.
        # KeyboardInterrupt/SystemExit still keep their cancellation semantics.
        return "<diagnostic text unavailable>"


def _failure_signature(error: BaseException) -> bytes:
    """Identify repeated diagnostics without retaining exception-owned resources."""

    digest = sha256()
    current: BaseException | None = error
    visited: set[int] = set()
    while current is not None and len(visited) < 32 and id(current) not in visited:
        visited.add(id(current))
        error_type = type(current)
        fields = (
            f"{error_type.__module__}.{error_type.__qualname__}",
            _diagnostic_text(current),
            *getattr(current, "__notes__", ()),
        )
        for field in fields:
            encoded = _diagnostic_text(field).encode("utf-8", errors="backslashreplace")
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)
        current = current.__cause__ or (
            None if current.__suppress_context__ else current.__context__
        )
    return digest.digest()


class RecoveryLog:
    """Coalesce one operation's repeated failures without changing its retry policy."""

    def __init__(
        self,
        logger: Logger,
        *,
        operation: str,
        interval_seconds: float,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._logger = logger
        self._operation = operation
        self._interval_seconds = interval_seconds
        self._clock = clock or monotonic
        self._lock = RLock()
        self._signature: bytes | None = None
        self._last_reported_at = 0.0
        self._suppressed_since_report = 0
        self._suppressed_total = 0

    def reset(self) -> None:
        """Forget an abandoned diagnostic owner without claiming it recovered."""
        with self._lock:
            self._signature = None
            self._last_reported_at = 0.0
            self._suppressed_since_report = 0
            self._suppressed_total = 0

    def log_failure(self, error: BaseException) -> None:
        try:
            signature = _failure_signature(error)
            with self._lock:
                now = self._clock()
                if (
                    signature == self._signature
                    and now - self._last_reported_at < self._interval_seconds
                ):
                    self._suppressed_since_report += 1
                    self._suppressed_total += 1
                    return
                suppressed = self._suppressed_since_report
                self._signature = signature
                self._last_reported_at = now
                self._suppressed_since_report = 0
                self._logger.error(
                    "Operation failed: operation=%s suppressed_repeats=%d; "
                    "further identical failures are summarized",
                    self._operation,
                    suppressed,
                    exc_info=error,
                )
        except Exception:
            # A formatter/handler failure cannot change the operation's retry
            # result. Reporting that failure recursively would create a log loop.
            pass

    def recovered(self) -> None:
        try:
            with self._lock:
                if self._signature is None:
                    return
                suppressed = self._suppressed_total
                self.reset()
                self._logger.info(
                    "Operation recovered: operation=%s suppressed_repeats=%d",
                    self._operation,
                    suppressed,
                )
        except Exception:
            # Diagnostic delivery is best effort; BaseException still propagates.
            pass
