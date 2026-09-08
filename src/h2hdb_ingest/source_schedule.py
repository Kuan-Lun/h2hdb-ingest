"""Process-local source debounce with generation-fenced scan completion."""

from __future__ import annotations

import math
from dataclasses import dataclass
from threading import Lock


@dataclass(frozen=True)
class SourceScanTicket:
    """Identify one scan and the last change known when it started."""

    sequence: int
    generation: int
    started_at: float


class SourceScanSchedule:
    """Keep monitoring changes while one scan runs, without losing newer work.

    Times use the caller's monotonic clock. A new instance always requests an
    immediate reconciliation; durable source reuse belongs to the core cache.
    The monitor and resident may call this object from different threads.
    """

    def __init__(
        self,
        *,
        quiet_seconds: float,
        max_wait_seconds: float,
        now: float,
    ) -> None:
        if (
            not math.isfinite(quiet_seconds)
            or not math.isfinite(max_wait_seconds)
            or not 0 < quiet_seconds <= max_wait_seconds
        ):
            raise ValueError("require finite 0 < quiet_seconds <= max_wait_seconds")
        self._validate_time(now)
        self._quiet_seconds = quiet_seconds
        self._max_wait_seconds = max_wait_seconds
        self._lock = Lock()
        self._generation = 1
        self._sequence = 0
        self._active: SourceScanTicket | None = None
        self._last_change_at: float | None = now
        self._hard_deadline: float | None = now

    def note_change(self, *, now: float) -> None:
        """Coalesce a probe's changes while preserving the first-change cap."""

        self._validate_time(now)
        with self._lock:
            self._generation += 1
            if self._last_change_at is None:
                self._hard_deadline = now + self._max_wait_seconds
                self._last_change_at = now
            else:
                self._last_change_at = max(self._last_change_at, now)

    def next_scan_at(self) -> float | None:
        """Return the next deadline, or None while clean or already scanning."""

        with self._lock:
            if self._active is not None or self._last_change_at is None:
                return None
            assert self._hard_deadline is not None
            return min(self._last_change_at + self._quiet_seconds, self._hard_deadline)

    def start_scan(self, *, now: float) -> SourceScanTicket:
        """Capture pending changes, also allowing an immediate downloader handoff."""

        self._validate_time(now)
        with self._lock:
            if self._active is not None:
                raise RuntimeError("a source scan is already active")
            self._sequence += 1
            ticket = SourceScanTicket(self._sequence, self._generation, now)
            self._active = ticket
            self._last_change_at = None
            self._hard_deadline = None
            return ticket

    def finish_scan(
        self,
        ticket: SourceScanTicket,
        *,
        now: float,
        succeeded: bool,
        pending_batch: bool = False,
    ) -> None:
        """Acknowledge only this scan; newer changes survive its completion.

        Changes observed during a scan get a maximum wait from its completion.
        Their quiet period can expire earlier, including before completion.
        Transient failure keeps a retry pending with a fresh quiet delay, so a
        continuously mutating source cannot cause an immediate retry loop.
        A published batch with deferred galleries continues immediately, even
        when the completion-marker monitor observed no further source changes.
        """

        self._validate_time(now)
        if pending_batch and not succeeded:
            raise ValueError(
                "only a successful publication can schedule its next batch"
            )
        with self._lock:
            if self._active is not ticket:
                raise RuntimeError("source scan completion has a stale ticket")
            if now < ticket.started_at:
                raise ValueError("source scan cannot finish before it started")
            if pending_batch:
                self._last_change_at = now
                self._hard_deadline = now
            elif not succeeded:
                self._last_change_at = now
            if not pending_batch and self._last_change_at is not None:
                self._hard_deadline = now + self._max_wait_seconds
            self._active = None

    @staticmethod
    def _validate_time(now: float) -> None:
        if not math.isfinite(now):
            raise ValueError("source schedule time must be finite")
