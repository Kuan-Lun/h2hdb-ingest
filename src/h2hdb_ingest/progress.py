"""Ephemeral, generation-fenced progress without database or filesystem reads."""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Event, Lock, Thread, current_thread
from time import monotonic

MAX_PROGRESS_COUNTERS = 64
MAX_PROGRESS_TOKEN_LENGTH = 128


@dataclass(frozen=True)
class ProgressSnapshot:
    """One immutable observation; counters describe only the current work."""

    generation: int
    phase: str
    operation: str | None
    elapsed_seconds: float
    phase_elapsed_seconds: float
    last_progress_age_seconds: float
    counters: tuple[tuple[str, int], ...]


@dataclass
class _ProgressState:
    work: ProgressWork
    phase: str
    announced: bool
    started_at: float
    phase_started_at: float
    last_progress_at: float
    next_report_at: float
    operation: str | None = None
    counters: dict[str, int] = field(default_factory=dict)

    def snapshot(self, now: float) -> ProgressSnapshot:
        return ProgressSnapshot(
            generation=self.work.generation,
            phase=self.phase,
            operation=self.operation,
            elapsed_seconds=max(0.0, now - self.started_at),
            phase_elapsed_seconds=max(0.0, now - self.phase_started_at),
            last_progress_age_seconds=max(0.0, now - self.last_progress_at),
            counters=tuple(sorted(self.counters.items())),
        )


class ProgressWork:
    """A single work generation; late worker updates are harmless no-ops.

    Phase changes preserve cumulative counters, the hourly deadline and the last
    counter-progress timestamp. A different phase clears the operation detail;
    repeating the same phase is a no-op. Only changed counter values represent
    measured progress. None of these observations are recovery authority.
    """

    def __init__(self, owner: IngestProgress, generation: int) -> None:
        self._owner = owner
        self._generation = generation

    @property
    def generation(self) -> int:
        return self._generation

    def phase(self, name: str, *, announce: bool = True) -> None:
        self._owner._phase(self, name, announce=announce)

    def operation(self, name: str) -> None:
        self._owner._operation(self, name)

    def advance(self, counter: str, amount: int = 1) -> None:
        self._owner._counter(self, counter, amount, additive=True)

    def set_counter(self, counter: str, value: int) -> None:
        self._owner._counter(self, counter, value, additive=False)

    def finish(self, status: str = "completed", *, announce: bool = True) -> None:
        self._owner._finish(self, status, announce=announce)


class IngestProgress:
    """Report active work independently of slow adapters and lease operations.

    The reporter waits on an event, snapshots in-memory state under a small lock,
    then emits outside that lock. Lifecycle messages use a separate emission lock
    to preserve ordering and ensure close returns with no emission still pending.
    Counter writers never acquire the emission lock. An emitter may inspect a
    snapshot but must not reenter lifecycle methods. Idle state emits nothing.
    """

    def __init__(
        self,
        emit: Callable[[str], None],
        *,
        interval_seconds: float = 3600,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        if not callable(emit) or not callable(clock):
            raise TypeError("progress emitter and clock must be callable")
        if not math.isfinite(interval_seconds) or interval_seconds <= 0:
            raise ValueError("progress interval must be finite and positive")
        self._emit = emit
        self._interval_seconds = interval_seconds
        self._clock = clock
        self._lock = Lock()
        self._emission_lock = Lock()
        self._wake = Event()
        self._thread: Thread | None = None
        self._closed = False
        self._generation = 0
        self._state: _ProgressState | None = None

    def start(self) -> None:
        with self._lock:
            if self._closed or self._thread is not None:
                return
            self._thread = Thread(
                target=self._run,
                name="h2hdb-ingest-progress",
                daemon=True,
            )
            self._thread.start()

    def close(self) -> None:
        with self._emission_lock:
            with self._lock:
                self._closed = True
                self._state = None
                thread = self._thread
                self._wake.set()
        if thread is not None and thread is not current_thread():
            thread.join()

    def begin(self, phase: str, *, announce: bool = True) -> ProgressWork:
        _require_name(phase)
        with self._emission_lock:
            with self._lock:
                if self._closed:
                    raise RuntimeError("progress reporter is closed")
                if self._state is not None:
                    raise RuntimeError("progress work is already active")
                now = self._clock()
                self._generation += 1
                work = ProgressWork(self, self._generation)
                self._state = _ProgressState(
                    work=work,
                    phase=phase,
                    announced=announce,
                    started_at=now,
                    phase_started_at=now,
                    last_progress_at=now,
                    next_report_at=now + self._interval_seconds,
                )
                snapshot = self._state.snapshot(now)
                self._wake.set()
            if announce:
                self._emit_snapshot("phase_started", snapshot)
            return work

    def current(self) -> ProgressWork | None:
        with self._lock:
            return None if self._state is None else self._state.work

    def snapshot(self) -> ProgressSnapshot | None:
        """Copy only this process's observations, without querying durable data."""
        with self._lock:
            return None if self._state is None else self._state.snapshot(self._clock())

    def _phase(self, work: ProgressWork, name: str, *, announce: bool) -> None:
        _require_name(name)
        with self._emission_lock:
            with self._lock:
                state = self._state
                if state is None or state.work is not work or state.phase == name:
                    return
                now = self._clock()
                previous = state.snapshot(now) if state.announced else None
                state.phase = name
                state.announced = announce
                state.phase_started_at = now
                state.operation = None
                snapshot = state.snapshot(now)
            if previous is not None:
                self._emit_snapshot("phase_ended", previous)
            if announce:
                self._emit_snapshot("phase_started", snapshot)

    def _operation(self, work: ProgressWork, name: str) -> None:
        _require_name(name)
        with self._lock:
            if self._state is not None and self._state.work is work:
                self._state.operation = name

    def _counter(
        self, work: ProgressWork, name: str, value: int, *, additive: bool
    ) -> None:
        _require_name(name)
        if type(value) is not int or value < 0:
            raise ValueError("progress counter value must be a non-negative int")
        with self._lock:
            state = self._state
            if state is None or state.work is not work:
                return
            if (
                name not in state.counters
                and len(state.counters) >= MAX_PROGRESS_COUNTERS
            ):
                raise ValueError("progress work exceeds the counter key limit")
            old = state.counters.get(name, 0)
            new = old + value if additive else value
            state.counters[name] = new
            if old != new:
                state.last_progress_at = self._clock()

    def _finish(self, work: ProgressWork, status: str, *, announce: bool) -> None:
        _require_name(status)
        with self._emission_lock:
            with self._lock:
                state = self._state
                if state is None or state.work is not work:
                    return
                snapshot = state.snapshot(self._clock())
                self._state = None
                self._wake.set()
            if announce:
                self._emit_snapshot("work_finished", snapshot, status=status)

    def _run(self) -> None:
        while True:
            # Clear before observing state so an intervening begin/close wakeup
            # cannot be lost between the state observation and the wait.
            self._wake.clear()
            with self._lock:
                if self._closed:
                    return
                delay = (
                    None
                    if self._state is None
                    else max(0.0, self._state.next_report_at - self._clock())
                )
            if delay is None or delay > 0:
                self._wake.wait(delay)
            else:
                self._report_due()

    def _report_due(self) -> None:
        with self._emission_lock:
            with self._lock:
                state = self._state
                if self._closed or state is None:
                    return
                now = self._clock()
                if now < state.next_report_at:
                    return
                snapshot = state.snapshot(now)
                # A delayed reporter emits one current summary, not an event
                # backlog for each elapsed hour.
                state.next_report_at = now + self._interval_seconds
            self._emit_snapshot("periodic", snapshot)

    def _emit_snapshot(
        self,
        event: str,
        snapshot: ProgressSnapshot,
        *,
        status: str | None = None,
    ) -> None:
        parts = [
            "ingest_progress",
            f"event={event}",
            f"generation={snapshot.generation}",
            f"phase={snapshot.phase}",
            f"elapsed_seconds={snapshot.elapsed_seconds:.1f}",
            f"phase_elapsed_seconds={snapshot.phase_elapsed_seconds:.1f}",
            f"last_progress_age_seconds={snapshot.last_progress_age_seconds:.1f}",
        ]
        if snapshot.operation is not None:
            parts.append(f"operation={snapshot.operation}")
        if status is not None:
            parts.append(f"status={status}")
        parts.extend(f"counter.{name}={value}" for name, value in snapshot.counters)
        try:
            self._emit(" ".join(parts))
        except Exception:
            # Observability is best effort: a failed logging sink must never
            # cancel ingest or kill future periodic reports.
            pass


def _require_name(value: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > MAX_PROGRESS_TOKEN_LENGTH
        or not value.isascii()
        or any(not (c.isalnum() or c in "_.-") for c in value)
    ):
        raise ValueError(
            "progress names must be non-empty tokens of at most 128 ASCII characters"
        )
