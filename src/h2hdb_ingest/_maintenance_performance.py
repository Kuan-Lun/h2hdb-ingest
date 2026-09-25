"""Fixed-size, process-local INFO totals for completed library cleanup attempts.

Elapsed time is the sum of active attempts, excluding time spent polling or in
other ingest stages. Snapshots are cumulative and must be differenced, not added.
None of these observations authorizes cleanup or proves that it is complete.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from time import monotonic_ns
from typing import Literal

from ._adapter_performance import summarize_adapter_io
from .maintenance import LibraryMaintenanceOutcome
from .metrics import (
    IngestMetric,
    IngestMetricOperation,
    IngestMetricSink,
    IngestMetricValue,
    TextIngestMetricSink,
    emit_ingest_metric,
)

logger = logging.getLogger(__name__)
_Status = Literal["completed", "failed", "interrupted"]


class LibraryMaintenancePerformance:
    """Aggregate bounded calls, emitting on outcome transitions and periodically."""

    def __init__(
        self,
        sink: IngestMetricSink | None = None,
        *,
        interval_ns: int = 60_000_000_000,
        clock: Callable[[], int] = monotonic_ns,
    ) -> None:
        self._sink = TextIngestMetricSink(logger.info) if sink is None else sink
        self._clock = clock
        self._interval_ns = interval_ns
        self._clock_failures = self._collection_errors = 0
        self._last_clock = 0
        self._observer_started = self._now()
        self._last_emitted = self._observer_started
        self._sequence = self._calls = self._elapsed = 0
        self._failed = self._interrupted = 0
        self._operations: dict[str, dict[str, int]] = {}
        self._outcomes = dict.fromkeys(LibraryMaintenanceOutcome, 0)
        self._previous_state: str | None = None
        self._last_status: _Status = "completed"
        self._dirty = False

    def _now(self) -> int:
        try:
            value = self._clock()
            if type(value) is not int or value < self._last_clock:
                raise ValueError("invalid cleanup telemetry clock")
        except Exception:
            self._clock_failures += 1
            return self._last_clock
        self._last_clock = value
        return value

    def run(
        self, cleanup: Callable[[], LibraryMaintenanceOutcome]
    ) -> LibraryMaintenanceOutcome:
        try:
            with summarize_adapter_io(
                self._record,
                generation=0,
                operation="library_cleanup",
                clock=self._clock,
                interval_ns=self._interval_ns,
            ):
                outcome = cleanup()
                if not isinstance(outcome, LibraryMaintenanceOutcome):
                    raise TypeError("library maintenance returned an invalid outcome")
        except BaseException as error:
            status: Literal["failed", "interrupted"] = (
                "failed" if isinstance(error, Exception) else "interrupted"
            )
            self._maybe_emit(status, status)
            raise
        self._outcomes[outcome] += 1
        self._maybe_emit(outcome, "completed")
        return outcome

    def _maybe_emit(self, state: str, status: _Status) -> None:
        self._last_status = status
        if (
            state != self._previous_state
            or self._now() - self._last_emitted >= self._interval_ns
        ):
            self.flush()
        self._previous_state = state

    def flush(self) -> None:
        """Publish pending totals once at a claim or process-lifecycle boundary."""
        if not self._dirty:
            return
        try:
            self._emit(self._last_status)
        except Exception:
            # Even a malformed diagnostic must not replace the cleanup outcome
            # or prevent the runtime from releasing its resources.
            self._collection_errors += 1
        else:
            self._dirty = False

    def _record(self, metric: IngestMetric) -> None:
        if metric.operation == "checkpoint":
            return
        self._dirty = True
        try:
            if metric.scope != "adapter_io" or metric.operation != "library_cleanup":
                raise ValueError("unexpected cleanup observation")
            self._calls += 1
            self._elapsed += metric.elapsed_ns
            self._failed += metric.status == "failed"
            self._interrupted += metric.status == "interrupted"
            counters = {value.name: value.value for value in metric.counters}
            self._clock_failures += counters["clock_failures"]
            for operation in metric.operations:
                if operation.operation not in self._operations:
                    if len(self._operations) >= 32:
                        raise ValueError("cleanup operation vocabulary exceeded limit")
                    self._operations[operation.operation] = {}
                total = self._operations[operation.operation]
                for value in (*operation.phases_ns, *operation.counters):
                    total[value.name] = total.get(value.name, 0) + value.value
        except Exception:
            self._collection_errors += 1

    def _emit(self, status: _Status) -> None:
        self._last_emitted = self._now()
        self._sequence += 1
        attributed = sum(
            value.get("exclusive", 0) for value in self._operations.values()
        )
        emit_ingest_metric(
            self._sink,
            IngestMetric(
                scope="library_cleanup_io",
                operation="maintenance",
                status=status,
                elapsed_ns=self._elapsed,
                phases_ns=(
                    IngestMetricValue("attributed_exclusive", attributed),
                    IngestMetricValue(
                        "unattributed", max(0, self._elapsed - attributed)
                    ),
                ),
                counters=tuple(
                    IngestMetricValue(name, value)
                    for name, value in (
                        ("process_id", os.getpid()),
                        ("observer_started_ns", self._observer_started),
                        ("snapshot_sequence", self._sequence),
                        ("cumulative", 1),
                        ("active_attempt_time_only", 1),
                        ("logical_bytes_only", 1),
                        ("calls", self._calls),
                        ("failed_calls", self._failed),
                        ("interrupted_calls", self._interrupted),
                        ("clock_failures", self._clock_failures),
                        ("collection_errors", self._collection_errors),
                        *(
                            (f"outcome_{key.lower()}", value)
                            for key, value in self._outcomes.items()
                        ),
                    )
                ),
                operations=tuple(
                    IngestMetricOperation(
                        operation=name,
                        phases_ns=tuple(
                            IngestMetricValue(key, values.get(key, 0))
                            for key in ("inclusive", "exclusive")
                        ),
                        counters=tuple(
                            IngestMetricValue(key, values.get(key, 0))
                            for key in (
                                "calls",
                                "failed_calls",
                                "logical_bytes",
                                "rows_returned",
                            )
                        ),
                    )
                    for name, values in sorted(self._operations.items())
                ),
            ),
        )
