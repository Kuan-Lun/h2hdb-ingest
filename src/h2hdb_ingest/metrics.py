"""Low-overhead immutable metrics shared by ingest orchestration and adapters."""

from __future__ import annotations

__all__ = [
    "IngestMetric",
    "IngestMetricOperation",
    "IngestMetricSink",
    "IngestMetricValue",
    "TextIngestMetricSink",
    "emit_ingest_metric",
]

import logging
from collections.abc import Callable
from dataclasses import dataclass
from threading import RLock
from typing import Protocol

from ._log_recovery import RecoveryLog

logger = logging.getLogger(__name__)


def _configure_metric_log_interval(interval_seconds: float) -> None:
    """Reset the one process-owned metric delivery diagnostic at configuration."""
    global _sink_failures
    _sink_failures = _MetricSinkDiagnostics(interval_seconds=interval_seconds)


@dataclass(frozen=True, slots=True)
class IngestMetricValue:
    """One non-negative monotonic duration or bounded-work counter."""

    name: str
    value: int

    def __post_init__(self) -> None:
        if type(self.name) is not str or not self.name:
            raise ValueError("metric value name must be a non-empty str")
        if type(self.value) is not int or self.value < 0:
            raise ValueError("metric value must be a non-negative int")


@dataclass(frozen=True, slots=True)
class IngestMetricOperation:
    """Aggregate timing and work for one durable publication operation."""

    operation: str
    phases_ns: tuple[IngestMetricValue, ...]
    counters: tuple[IngestMetricValue, ...]

    def __post_init__(self) -> None:
        if type(self.operation) is not str or not self.operation:
            raise ValueError("metric operation must be a non-empty str")
        object.__setattr__(self, "phases_ns", tuple(self.phases_ns))
        object.__setattr__(self, "counters", tuple(self.counters))
        _validate_metric_values(self.phases_ns, label="phase")
        _validate_metric_values(self.counters, label="counter")


@dataclass(frozen=True, slots=True)
class IngestMetric:
    """One immutable, terminally emitted ingest measurement."""

    scope: str
    operation: str
    elapsed_ns: int
    phases_ns: tuple[IngestMetricValue, ...] = ()
    counters: tuple[IngestMetricValue, ...] = ()
    operations: tuple[IngestMetricOperation, ...] = ()

    def __post_init__(self) -> None:
        if type(self.scope) is not str or not self.scope:
            raise ValueError("metric scope must be a non-empty str")
        if type(self.operation) is not str or not self.operation:
            raise ValueError("metric operation must be a non-empty str")
        if type(self.elapsed_ns) is not int or self.elapsed_ns < 0:
            raise ValueError("metric elapsed_ns must be a non-negative int")
        object.__setattr__(self, "phases_ns", tuple(self.phases_ns))
        object.__setattr__(self, "counters", tuple(self.counters))
        object.__setattr__(self, "operations", tuple(self.operations))
        _validate_metric_values(self.phases_ns, label="phase")
        _validate_metric_values(self.counters, label="counter")
        observed_operations: set[str] = set()
        for operation in self.operations:
            if not isinstance(operation, IngestMetricOperation):
                raise TypeError("metric contains a foreign operation")
            operation.__post_init__()
            if operation.operation in observed_operations:
                raise ValueError("metric operation names must be unique")
            observed_operations.add(operation.operation)


class IngestMetricSink(Protocol):
    """Consumer for an immutable metric; it is outside ingest protocols."""

    def __call__(self, metric: IngestMetric, /) -> None:
        """Consume one complete metric."""


class _MetricSinkDiagnostics:
    """Retain only the latest failed callable and its bounded diagnostic state.

    Only that same callable's successful delivery establishes recovery. A failure
    from a different sink replaces the owner and is reported immediately; there
    is no growing per-sink registry and no claim that the previous sink recovered.
    """

    def __init__(
        self, *, interval_seconds: float, clock: Callable[[], float] | None = None
    ) -> None:
        self._lock = RLock()
        self._owner: IngestMetricSink | None = None
        self._reporter = RecoveryLog(
            logger,
            operation="ingest metric sink",
            interval_seconds=interval_seconds,
            clock=clock,
        )

    def failed(
        self, sink: IngestMetricSink, error: BaseException, metric: IngestMetric
    ) -> None:
        with self._lock:
            if sink is not self._owner:
                self._reporter.reset()
                self._owner = sink
            self._reporter.log_failure(
                error,
                context={
                    "metric_scope": metric.scope,
                    "metric_operation": metric.operation,
                },
            )

    def succeeded(self, sink: IngestMetricSink) -> None:
        with self._lock:
            if sink is self._owner:
                self._owner = None
                self._reporter.recovered()


_sink_failures = _MetricSinkDiagnostics(interval_seconds=3600.0)


@dataclass(frozen=True, slots=True)
class TextIngestMetricSink:
    """Emit one compact log record for each complete metric."""

    emit: Callable[[str], None]

    def __post_init__(self) -> None:
        if not callable(self.emit):
            raise TypeError("metric text emitter must be callable")

    def __call__(self, metric: IngestMetric, /) -> None:
        if not isinstance(metric, IngestMetric):
            raise TypeError("metric sink received a foreign value")
        parts = [
            "ingest_metric",
            f"scope={metric.scope}",
            f"operation={metric.operation}",
            f"elapsed_ns={metric.elapsed_ns}",
        ]
        parts.extend(
            f"phase.{value.name}_ns={value.value}" for value in metric.phases_ns
        )
        parts.extend(f"counter.{value.name}={value.value}" for value in metric.counters)
        for operation in metric.operations:
            prefix = f"operation.{operation.operation}"
            parts.extend(
                f"{prefix}.{value.name}_ns={value.value}"
                for value in operation.phases_ns
            )
            parts.extend(
                f"{prefix}.{value.name}={value.value}" for value in operation.counters
            )
        self.emit(" ".join(parts))


def emit_ingest_metric(
    sink: IngestMetricSink | None,
    metric: IngestMetric,
) -> None:
    """Emit telemetry without letting an observer alter ingest completion."""

    if sink is None:
        return
    try:
        sink(metric)
    except Exception as error:
        _sink_failures.failed(sink, error, metric)
    else:
        _sink_failures.succeeded(sink)


def _validate_metric_values(
    values: tuple[IngestMetricValue, ...],
    *,
    label: str,
) -> None:
    observed: set[str] = set()
    for value in values:
        if not isinstance(value, IngestMetricValue):
            raise TypeError(f"metric contains a foreign {label}")
        value.__post_init__()
        if value.name in observed:
            raise ValueError(f"metric {label} names must be unique")
        observed.add(value.name)
