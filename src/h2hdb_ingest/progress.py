"""Ephemeral, generation-fenced progress without database or filesystem reads."""

from __future__ import annotations

import logging
import math
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from threading import Event, Lock, Thread, current_thread
from time import monotonic

from ._retry_diagnostics import RetryDiagnostic, capture_failure_snapshot
from .progress_format import format_diagnostics, format_progress

MAX_PROGRESS_COUNTERS = 64
MAX_PROGRESS_TOKEN_LENGTH = 128

_OPERATION_COUNTERS = {
    "source_discovery": ("galleries_discovered", "galleries"),
    "source_gallery_observation": ("gallery_indexes_built", "gallery indexes"),
    "source_file_read": ("file_observations_completed", "files"),
    "archive_render_pages": ("pages_rendered", "pages"),
    "archive_write_pages": ("pages_written", "pages"),
    "library_install": ("library_resources_reconciled", "library files"),
    "library_remove_stale": ("library_resources_removed", "library files"),
}


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
    operation_generation: int = 0
    operation_elapsed_seconds: float = 0
    operation_completed: int | None = None
    operation_total: int | None = None
    operation_unit: str | None = None
    work_identity: object | None = field(default=None, repr=False, compare=False)


@dataclass(frozen=True)
class _Operation:
    name: str
    generation: int
    started_at: float
    completed: int | None = None
    total: int | None = None
    unit: str | None = None
    counter_baseline: int = 0


@dataclass(frozen=True)
class _ActivityCheckpoint:
    phase_generation: int
    operation: _Operation | None


@dataclass
class _ProgressState:
    work: ProgressWork
    phase: str
    announced: bool
    started_at: float
    phase_started_at: float
    last_progress_at: float
    next_report_at: float
    phase_generation: int = 0
    operation_generation: int = 0
    operation: _Operation | None = None
    counters: dict[str, int] = field(default_factory=dict)

    def snapshot(self, now: float) -> ProgressSnapshot:
        operation = self.operation
        completed = None if operation is None else operation.completed
        unit = None if operation is None else operation.unit
        if operation is not None and completed is None:
            inferred = _OPERATION_COUNTERS.get(operation.name)
            if inferred is not None:
                counter, unit = inferred
                completed = max(
                    0, self.counters.get(counter, 0) - operation.counter_baseline
                )
        return ProgressSnapshot(
            generation=self.work.generation,
            phase=self.phase,
            operation=None if operation is None else operation.name,
            elapsed_seconds=max(0.0, now - self.started_at),
            phase_elapsed_seconds=max(0.0, now - self.phase_started_at),
            last_progress_age_seconds=max(0.0, now - self.last_progress_at),
            counters=tuple(sorted(self.counters.items())),
            operation_generation=0 if operation is None else operation.generation,
            operation_elapsed_seconds=(
                0 if operation is None else max(0.0, now - operation.started_at)
            ),
            operation_completed=completed,
            operation_total=None if operation is None else operation.total,
            operation_unit=unit,
            work_identity=self.work._identity,
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
        self._identity = object()

    @property
    def generation(self) -> int:
        return self._generation

    def phase(self, name: str, *, announce: bool = True) -> None:
        self._owner._phase(self, name, announce=announce)

    def operation(
        self,
        name: str,
        *,
        completed: int | None = None,
        total: int | None = None,
        unit: str | None = None,
    ) -> None:
        """Update the current activity without producing an INFO per item.

        Repeated updates of the same operation preserve its start time. An
        unknown total is represented by None, including when completed is known.
        Worker threads should update counters, leaving activity changes to their
        orchestrator. Use activity() around nested adapter calls.
        """
        self._owner._operation(self, name, completed=completed, total=total, unit=unit)

    @contextmanager
    def activity(
        self,
        name: str,
        *,
        completed: int | None = None,
        total: int | None = None,
        unit: str | None = None,
    ) -> Iterator[None]:
        """Restore the enclosing operation after nested work, including errors.

        Scopes belong to the orchestration thread, not concurrent workers.
        A phase transition or replacement work fences restoration of old state.
        Escaping errors retain their innermost measured activity for diagnostics.
        """
        checkpoint = self._owner._operation(
            self, name, completed=completed, total=total, unit=unit, scoped=True
        )
        try:
            yield
        except BaseException as error:
            try:
                self._owner._capture_failure(self, error)
            except Exception:
                # Diagnostics must not replace the activity's original error.
                pass
            raise
        finally:
            if checkpoint is not None:
                self._owner._restore_activity(self, checkpoint)

    def advance(self, counter: str, amount: int = 1) -> None:
        self._owner._counter(self, counter, amount, additive=True)

    def set_counter(self, counter: str, value: int) -> None:
        self._owner._counter(self, counter, value, additive=False)

    def finish(
        self,
        status: str = "completed",
        *,
        announce: bool = True,
        failure: RetryDiagnostic | None = None,
    ) -> None:
        self._owner._finish(self, status, announce=announce, failure=failure)


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
        emit_debug: Callable[[str], None] | None = None,
    ) -> None:
        if not callable(emit) or not callable(clock):
            raise TypeError("progress emitter and clock must be callable")
        if not math.isfinite(interval_seconds) or interval_seconds <= 0:
            raise ValueError("progress interval must be finite and positive")
        self._emit = emit
        self._emit_debug = emit_debug or logging.getLogger(__name__).debug
        self._last_emitted: ProgressSnapshot | None = None
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
                state.phase_generation += 1
                state.operation = None
                snapshot = state.snapshot(now)
            if previous is not None:
                self._emit_snapshot("phase_ended", previous)
            if announce:
                self._emit_snapshot("phase_started", snapshot)

    def _operation(
        self,
        work: ProgressWork,
        name: str,
        *,
        completed: int | None,
        total: int | None,
        unit: str | None,
        scoped: bool = False,
    ) -> _ActivityCheckpoint | None:
        _require_name(name)
        _require_measurement(completed, total, unit)
        with self._lock:
            state = self._state
            if state is None or state.work is not work:
                return None
            previous = state.operation
            checkpoint = _ActivityCheckpoint(state.phase_generation, previous)
            now = self._clock()
            same = not scoped and previous is not None and previous.name == name
            if not same:
                state.operation_generation += 1
            inferred = _OPERATION_COUNTERS.get(name)
            operation = _Operation(
                name=name,
                generation=(
                    previous.generation
                    if same and previous is not None
                    else state.operation_generation
                ),
                started_at=(
                    previous.started_at if same and previous is not None else now
                ),
                completed=completed,
                total=total,
                unit=unit,
                counter_baseline=(
                    previous.counter_baseline
                    if same and previous is not None
                    else 0
                    if inferred is None
                    else state.counters.get(inferred[0], 0)
                ),
            )
            if completed is not None and (
                (
                    same
                    and previous is not None
                    and completed != (previous.completed or 0)
                )
                or (not same and completed > 0)
            ):
                state.last_progress_at = now
            state.operation = operation
            return checkpoint

    def _restore_activity(
        self, work: ProgressWork, checkpoint: _ActivityCheckpoint
    ) -> None:
        with self._lock:
            state = self._state
            if (
                state is not None
                and state.work is work
                and state.phase_generation == checkpoint.phase_generation
            ):
                state.operation = checkpoint.operation

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

    def _capture_failure(self, work: ProgressWork, error: BaseException) -> None:
        with self._lock:
            state = self._state
            if state is None or state.work is not work:
                return
            snapshot = state.snapshot(self._clock())
        capture_failure_snapshot(error, snapshot)

    def _finish(
        self,
        work: ProgressWork,
        status: str,
        *,
        announce: bool,
        failure: RetryDiagnostic | None,
    ) -> None:
        _require_name(status)
        with self._emission_lock:
            with self._lock:
                state = self._state
                if state is None or state.work is not work:
                    return
                snapshot = state.snapshot(self._clock())
                if (
                    failure is not None
                    and failure.snapshot is not None
                    and failure.snapshot.generation == work.generation
                    and failure.snapshot.work_identity is work._identity
                ):
                    snapshot = failure.snapshot
                self._state = None
                self._wake.set()
            if announce:
                self._emit_snapshot(
                    "work_finished", snapshot, status=status, failure=failure
                )

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
        failure: RetryDiagnostic | None = None,
    ) -> None:
        message = format_progress(event, snapshot, self._last_emitted, status=status)
        if failure is not None:
            message += "; " + failure.detail
        self._last_emitted = snapshot
        emit_status = (
            logging.getLogger(__name__).warning if status == "retry" else self._emit
        )
        for emit, text in (
            (emit_status, message),
            (self._emit_debug, format_diagnostics(event, snapshot, status=status)),
        ):
            try:
                emit(text)
            except Exception:
                # Each sink is independent and best effort. A failed logger
                # must not cancel ingest or suppress the other log level.
                pass


def _require_measurement(
    completed: int | None, total: int | None, unit: str | None
) -> None:
    for value in (completed, total):
        if value is not None and (type(value) is not int or value < 0):
            raise ValueError("progress measurements must be non-negative ints")
    if completed is not None and total is not None and completed > total:
        raise ValueError("completed progress cannot exceed total")
    if unit is not None:
        _require_name(unit)


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
