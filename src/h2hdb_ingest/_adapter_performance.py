"""Bounded operation-local I/O attribution, independent of storage authority.

Inclusive durations include nested operations and must not be added. Exclusive
durations subtract measured children on the same thread; their sum is bounded
by operation wall time. Bytes are logical bytes, including repeated reads,
not physical disk traffic. Query rows count returned rows, never rows examined
by the database engine. No path, object key or protection token is retained.
"""

from __future__ import annotations

import os
from asyncio import current_task
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
from threading import get_ident
from time import monotonic_ns
from typing import BinaryIO, Literal

from .metrics import (
    IngestMetric,
    IngestMetricOperation,
    IngestMetricSink,
    IngestMetricValue,
    emit_ingest_metric,
)

AdapterOperation = Literal[
    "protect",
    "layout",
    "stage",
    "stage_read",
    "stage_prefix_read",
    "stage_write",
    "stage_hash",
    "stage_flush",
    "verify_read",
    "verify_hash",
    "source_open",
    "journal_session",
    "journal_commit",
    "journal_rollback",
    "journal_cleanup_select",
    "journal_cleanup_exists",
    "state_lock_wait",
    "publication_lock_wait",
    "protection_lock_wait",
    "stage_lock_wait",
    "file_fsync",
    "directory_fsync",
    "rename",
    "scratch_cleanup",
]


@dataclass
class _Total:
    inclusive: int = 0
    exclusive: int = 0
    calls: int = 0
    failed_calls: int = 0
    logical_bytes: int = 0
    rows_returned: int = 0


@dataclass
class _Frame:
    started: int
    children: int = 0


class AdapterPerformance:
    """Fixed operation totals; synchronous adapter calls only, no worker clocks."""

    def __init__(
        self,
        sink: IngestMetricSink | None,
        *,
        generation: int,
        clock: Callable[[], int] = monotonic_ns,
        interval_ns: int = 60_000_000_000,
    ) -> None:
        self.sink = sink
        self.owner = _execution_owner()
        self.active = True
        self.generation = generation
        self.clock = clock
        self.interval_ns = interval_ns
        self.clock_failures = 0
        self.last_clock = 0
        self.started = self._now()
        self.last_emitted = self.started
        self.sequence = 0
        self.totals: dict[AdapterOperation, _Total] = {}
        self.stack: list[_Frame] = []

    def _now(self) -> int:
        try:
            observed = self.clock()
            if type(observed) is not int or observed < self.last_clock:
                raise ValueError("invalid adapter telemetry clock")
        except Exception:
            self.clock_failures += 1
            return self.last_clock
        self.last_clock = observed
        return observed

    @contextmanager
    def phase(self, operation: AdapterOperation) -> Iterator[None]:
        frame = _Frame(self._now())
        self.stack.append(frame)
        total = self.totals.setdefault(operation, _Total())
        try:
            yield
        except BaseException:
            total.failed_calls += 1
            raise
        finally:
            elapsed = max(0, self._now() - frame.started)
            self.stack.pop()
            total.calls += 1
            total.inclusive += elapsed
            total.exclusive += max(0, elapsed - frame.children)
            if self.stack:
                self.stack[-1].children += elapsed
            elif self._now() - self.last_emitted >= self.interval_ns:
                self.emit("checkpoint", "completed")

    def emit(
        self,
        operation: str,
        status: Literal["completed", "failed", "interrupted"],
    ) -> None:
        now = self._now()
        self.last_emitted = now
        self.sequence += 1
        emit_ingest_metric(
            self.sink,
            IngestMetric(
                scope="adapter_io",
                operation=operation,
                elapsed_ns=max(0, now - self.started),
                status=status,
                counters=(
                    IngestMetricValue("ingest_generation", self.generation),
                    IngestMetricValue("snapshot_sequence", self.sequence),
                    IngestMetricValue("cumulative", 1),
                    IngestMetricValue("logical_bytes_only", 1),
                    IngestMetricValue("clock_failures", self.clock_failures),
                ),
                operations=tuple(
                    IngestMetricOperation(
                        operation=name,
                        phases_ns=(
                            IngestMetricValue("inclusive", total.inclusive),
                            IngestMetricValue("exclusive", total.exclusive),
                        ),
                        counters=(
                            IngestMetricValue("calls", total.calls),
                            IngestMetricValue("failed_calls", total.failed_calls),
                            IngestMetricValue("logical_bytes", total.logical_bytes),
                            IngestMetricValue("rows_returned", total.rows_returned),
                        ),
                    )
                    for name, total in sorted(self.totals.items())
                ),
            ),
        )


_current: ContextVar[AdapterPerformance | None] = ContextVar(
    "h2hdb_ingest_adapter_performance", default=None
)


def _execution_owner() -> tuple[int, int | None]:
    try:
        task = current_task()
    except RuntimeError:
        task = None
    return get_ident(), None if task is None else id(task)


def _owned_measurement() -> AdapterPerformance | None:
    measured = _current.get()
    if measured is None or not measured.active or measured.owner != _execution_owner():
        return None
    return measured


@contextmanager
def summarize_adapter_io(
    sink: IngestMetricSink | None,
    *,
    generation: int,
    operation: str = "publication",
    clock: Callable[[], int] = monotonic_ns,
    interval_ns: int = 60_000_000_000,
) -> Iterator[None]:
    """Emit cumulative INFO snapshots at safe operation boundaries and on exit.

    A checkpoint contains completed calls including failed work. There is no
    timer thread: one slow in-flight operation appears when it returns. The
    existing progress heartbeat continues to identify that active operation.
    """
    measured = AdapterPerformance(
        sink, generation=generation, clock=clock, interval_ns=interval_ns
    )
    token = _current.set(measured)
    status: Literal["completed", "failed", "interrupted"] = "completed"
    try:
        yield
    except Exception:
        status = "failed"
        raise
    except BaseException:
        status = "interrupted"
        raise
    finally:
        measured.active = False
        _current.reset(token)
        measured.emit(operation, status)


@contextmanager
def adapter_phase(operation: AdapterOperation) -> Iterator[None]:
    measured = _owned_measurement()
    if measured is None:
        yield
    else:
        with measured.phase(operation):
            yield


def adapter_operation[**P, R](
    operation: AdapterOperation,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def decorate(function: Callable[P, R]) -> Callable[P, R]:
        @wraps(function)
        def measured(*args: P.args, **kwargs: P.kwargs) -> R:
            with adapter_phase(operation):
                return function(*args, **kwargs)

        return measured

    return decorate


def adapter_bytes(operation: AdapterOperation, size: int) -> None:
    measured = _owned_measurement()
    if measured is not None:
        measured.totals.setdefault(operation, _Total()).logical_bytes += size


def adapter_rows(operation: AdapterOperation, count: int) -> None:
    """Count actual fetched query rows without claiming database scan work."""
    measured = _owned_measurement()
    if measured is not None:
        measured.totals.setdefault(operation, _Total()).rows_returned += count


def adapter_read(stream: BinaryIO, size: int, operation: AdapterOperation) -> bytes:
    with adapter_phase(operation):
        value = stream.read(size)
        if isinstance(value, bytes):
            adapter_bytes(operation, len(value))
        return value


def adapter_fsync(descriptor: int, *, directory: bool) -> None:
    with adapter_phase("directory_fsync" if directory else "file_fsync"):
        os.fsync(descriptor)
