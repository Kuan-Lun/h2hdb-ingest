"""Bounded source costs; inclusive phases and worker sums are nonadditive.

Read bytes describe logical I/O, never physical disk traffic. Minute progress
records are cumulative snapshots, not extra completed operations. No paths,
gallery names, source payloads or per-gallery dictionaries are retained.
Progress delivery uses one process-owned daemon and a single pending snapshot.
An already started sink call cannot be cancelled and may finish after the terminal
record; scope, work generation and sequence identify these cumulative snapshots.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterator
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from dataclasses import dataclass
from threading import Condition, Event, Lock, Thread
from time import monotonic_ns
from typing import Literal

from ._image_performance import ImageWorkMeasurement
from .metrics import (
    IngestMetric,
    IngestMetricOperation,
    IngestMetricSink,
    IngestMetricValue,
    emit_ingest_metric,
)

SourcePhase = Literal[
    "discovery",
    "gallery_index",
    "read",
    "hash",
    "metadata_parse",
    "qualification",
    "source_synchronize",
]
QualificationPhase = Literal[
    "owner_spool",
    "owner_source_hash",
    "owner_spool_write",
    "owner_spool_readback",
    "owner_spool_hash",
    "owner_future_wait",
]
_COUNTER_LIMIT = 64
_current_qualification: ContextVar[SourcePerformance | None] = ContextVar(
    "source_qualification_performance", default=None
)


@dataclass(slots=True)
class _ProgressToken:
    active: bool = True


@dataclass(frozen=True, slots=True)
class _ProgressDelivery:
    token: _ProgressToken
    sink: IngestMetricSink
    metric: IngestMetric


class _ProgressDispatcher:
    """A stalled observer owns at most one daemon and one pending snapshot.

    No observer code executes under the mailbox lock. Dequeueing an active token
    starts delivery; cancellation only removes work not yet started. The existing
    synchronous terminal sink contract is independent of this progress channel.
    """

    def __init__(self) -> None:
        self._condition = Condition()
        self._pending: _ProgressDelivery | None = None
        self._thread: Thread | None = None

    def submit(
        self, token: _ProgressToken, sink: IngestMetricSink, metric: IngestMetric
    ) -> bool:
        with self._condition:
            if not token.active or self._pending is not None:
                return False
            if self._thread is None:
                thread = Thread(
                    target=self._run, name="h2hdb-source-metric-delivery", daemon=True
                )
                try:
                    thread.start()
                except RuntimeError, OSError:
                    return False
                self._thread = thread
            self._pending = _ProgressDelivery(token, sink, metric)
            self._condition.notify()
            return True

    def cancel(self, token: _ProgressToken) -> bool:
        with self._condition:
            token.active = False
            if self._pending is not None and self._pending.token is token:
                self._pending = None
                return True
            return False

    def _run(self) -> None:
        while True:
            with self._condition:
                self._condition.wait_for(lambda: self._pending is not None)
                delivery = self._pending
                self._pending = None
            if delivery is not None:
                try:
                    emit_ingest_metric(delivery.sink, delivery.metric)
                except BaseException:
                    # This daemon has no business work to interrupt. Even a sink
                    # raising SystemExit must not disable or multiply dispatchers.
                    pass
                finally:
                    # Do not retain the last sink's runtime while idle.
                    delivery = None


_progress_dispatcher = _ProgressDispatcher()


def current_qualification_performance() -> SourcePerformance | None:
    return _current_qualification.get()


@contextmanager
def qualification_phase(name: QualificationPhase) -> Iterator[None]:
    performance = current_qualification_performance()
    with (
        nullcontext() if performance is None else performance.qualification_phase(name)
    ):
        yield


class SourcePerformance:
    """One source turn with bounded scalar state and a joined snapshot reporter."""

    def __init__(self, *, clock: Callable[[], int] = monotonic_ns) -> None:
        self._clock = clock
        self._lock = Lock()
        self._phases: dict[SourcePhase, int] = {}
        self._active: dict[SourcePhase, tuple[int, int]] = {}
        self._counters: dict[str, int] = {}
        self._qualification_phases: dict[str, int] = {}
        self._qualification_counters: dict[str, int] = {}
        self._progress_sequence = 0
        self._operation_active = False

    def _now(self) -> int | None:
        try:
            value = self._clock()
            return value if type(value) is int and value >= 0 else None
        except Exception:
            return None

    def add(self, name: str, value: int = 1) -> None:
        with self._lock:
            if name not in self._counters and len(self._counters) >= _COUNTER_LIMIT:
                return
            self._counters[name] = self._counters.get(name, 0) + value
            if name == "logical_bytes_read" and _current_qualification.get() is self:
                self._qualification_counters["source_logical_bytes_read"] = (
                    self._qualification_counters.get("source_logical_bytes_read", 0)
                    + value
                )

    @contextmanager
    def phase(self, name: SourcePhase) -> Iterator[None]:
        started = self._now()
        if started is not None:
            with self._lock:
                count, total = self._active.get(name, (0, 0))
                self._active[name] = count + 1, total + started
        try:
            yield
        finally:
            finished = self._now()
            with self._lock:
                if started is not None:
                    count, total = self._active[name]
                    if count == 1:
                        del self._active[name]
                    else:
                        self._active[name] = count - 1, total - started
                if started is not None and finished is not None:
                    self._phases[name] = self._phases.get(name, 0) + max(
                        0, finished - started
                    )
                    if name == "read" and _current_qualification.get() is self:
                        self._qualification_phases["owner_source_read"] = (
                            self._qualification_phases.get("owner_source_read", 0)
                            + max(0, finished - started)
                        )
                else:
                    self._counters["clock_failures"] = (
                        self._counters.get("clock_failures", 0) + 1
                    )
            self.add(name + "_calls")

    @contextmanager
    def qualifying(self) -> Iterator[None]:
        """Bind the owner; worker threads receive it explicitly."""
        token = _current_qualification.set(self)
        try:
            yield
        finally:
            _current_qualification.reset(token)

    @contextmanager
    def qualification_phase(self, name: QualificationPhase) -> Iterator[None]:
        started = self._now()
        try:
            yield
        finally:
            finished = self._now()
            with self._lock:
                if started is not None and finished is not None:
                    self._qualification_phases[name] = self._qualification_phases.get(
                        name, 0
                    ) + max(0, finished - started)
                else:
                    self._qualification_counters["clock_failures"] = (
                        self._qualification_counters.get("clock_failures", 0) + 1
                    )

    def qualification_count(self, name: str, amount: int = 1) -> None:
        with self._lock:
            if (
                name in self._qualification_counters
                or len(self._qualification_counters) < _COUNTER_LIMIT
            ):
                self._qualification_counters[name] = (
                    self._qualification_counters.get(name, 0) + amount
                )

    def record_qualification_worker(
        self,
        measured: ImageWorkMeasurement,
        *,
        elapsed_ns: int,
        thread_cpu_ns: int,
        encoded_bytes: int,
        outcome: Literal["accepted", "rejected", "failed", "interrupted"],
    ) -> None:
        """Include failed partial work; worker elapsed overlaps owner and workers.

        Thread CPU excludes native helper threads. Bins are fixed marginals,
        not a joint distribution. Decoder phases are fused elapsed spans.
        """
        phases = {"worker_elapsed_sum": elapsed_ns}
        phases.update(
            ("worker_" + name, value) for name, value in measured.phases_ns.items()
        )
        codec = next(
            (
                name
                for name in ("jpeg", "png", "gif", "webp", "avif", "heif", "bmp")
                if measured.source_kind.startswith(name + "load")
            ),
            "other",
        )
        encoded_bin = (
            "le_256k"
            if encoded_bytes <= 256 * 1024
            else "le_1m"
            if encoded_bytes <= 1024 * 1024
            else "le_4m"
            if encoded_bytes <= 4 * 1024 * 1024
            else "gt_4m"
        )
        pixels = measured.source_width * measured.source_height
        pixel_bin = (
            "unknown"
            if not pixels
            else "le_1m"
            if pixels <= 1_000_000
            else "le_8m"
            if pixels <= 8_000_000
            else "le_40m"
            if pixels <= 40_000_000
            else "gt_40m"
        )
        counters = {
            "worker_attempts": 1,
            "worker_" + outcome: 1,
            "worker_thread_cpu_ns": thread_cpu_ns,
            "decoder_input_logical_bytes": measured.decoder_input_bytes,
            "worker_source_encoded_bytes": encoded_bytes,
            "codec_" + codec: 1,
            "codec_" + codec + "_worker_elapsed_ns": elapsed_ns,
            "encoded_bytes_" + encoded_bin: 1,
            "encoded_bytes_" + encoded_bin + "_worker_elapsed_ns": elapsed_ns,
            "pixels_" + pixel_bin: 1,
            "pixels_" + pixel_bin + "_worker_elapsed_ns": elapsed_ns,
            "source_pixels": pixels,
            "exclusive_source_pages": int(measured.source_exclusive),
        }
        with self._lock:
            for name, value in phases.items():
                self._qualification_phases[name] = (
                    self._qualification_phases.get(name, 0) + value
                )
            for name, value in counters.items():
                self._qualification_counters[name] = (
                    self._qualification_counters.get(name, 0) + value
                )

    def metric(
        self,
        *,
        status: Literal["completed", "failed", "interrupted", "progress"],
        scope: str = "source",
        operation: str = "synchronize",
    ) -> IngestMetric:
        now = self._now() if status == "progress" else None
        with self._lock:
            phases = dict(self._phases)
            if now is not None:
                for name, (count, total) in self._active.items():
                    phases[name] = phases.get(name, 0) + max(0, count * now - total)
            counters = dict(self._counters)
            if status == "progress":
                self._progress_sequence += 1
                if now is None:
                    self._counters["progress_clock_failures"] = (
                        self._counters.get("progress_clock_failures", 0) + 1
                    )
                    counters["progress_clock_failures"] = self._counters[
                        "progress_clock_failures"
                    ]
            if self._progress_sequence:
                counters["progress_sequence"] = self._progress_sequence
            return IngestMetric(
                scope=scope,
                operation=operation,
                status=status,
                elapsed_ns=phases.get("source_synchronize", 0),
                phases_ns=tuple(
                    IngestMetricValue(name, value)
                    for name, value in sorted(phases.items())
                    if name != "source_synchronize"
                ),
                counters=tuple(
                    IngestMetricValue(name, value)
                    for name, value in sorted(counters.items())
                ),
                operations=(
                    (
                        IngestMetricOperation(
                            operation="qualification",
                            phases_ns=tuple(
                                IngestMetricValue(name, value)
                                for name, value in sorted(
                                    self._qualification_phases.items()
                                )
                            ),
                            counters=tuple(
                                IngestMetricValue(name, value)
                                for name, value in sorted(
                                    self._qualification_counters.items()
                                )
                            ),
                        ),
                    )
                    if self._qualification_phases or self._qualification_counters
                    else ()
                ),
            )

    @contextmanager
    def operation(
        self,
        sink: IngestMetricSink | None,
        *,
        interruptions: tuple[type[BaseException], ...] = (),
        scope: str = "source",
        operation: str = "synchronize",
        progress_interval_seconds: float = 60,
    ) -> Iterator[None]:
        if (
            not math.isfinite(progress_interval_seconds)
            or progress_interval_seconds <= 0
        ):
            raise ValueError("source progress interval must be finite and positive")
        with self._lock:
            if self._operation_active:
                raise RuntimeError("source performance operation is already active")
            self._operation_active = True
        stop = Event()
        token = _ProgressToken()
        reporter: Thread | None = None

        def report() -> None:
            while not stop.wait(progress_interval_seconds):
                try:
                    if sink is not None and not _progress_dispatcher.submit(
                        token,
                        sink,
                        self.metric(
                            status="progress",
                            scope=scope + "_progress",
                            operation=operation,
                        ),
                    ):
                        self.add("progress_dropped_snapshots")
                except Exception:
                    self.add("progress_reporter_failures")

        status: Literal["completed", "failed", "interrupted"] = "completed"
        try:
            with self.phase("source_synchronize"):
                try:
                    if sink is not None:
                        reporter = Thread(
                            target=report, name="h2hdb-source-metrics", daemon=True
                        )
                        try:
                            reporter.start()
                        except RuntimeError, OSError:
                            # A telemetry thread cannot make the source fail.
                            self.add("progress_reporter_failures")
                            reporter = None
                    yield
                finally:
                    stop.set()
                    if _progress_dispatcher.cancel(token):
                        self.add("progress_cancelled_snapshots")
                    if reporter is not None and reporter.ident is not None:
                        reporter.join()
        except Exception as error:
            status = "interrupted" if isinstance(error, interruptions) else "failed"
            raise
        except BaseException:
            status = "interrupted"
            raise
        finally:
            with self._lock:
                self._operation_active = False
            emit_ingest_metric(
                sink, self.metric(status=status, scope=scope, operation=operation)
            )
