"""Bounded adapter work measurements; never source or transaction authority.

Phase times are inclusive and may overlap (qualification includes source reads).
Read bytes count logical reads including rereads, never physical device traffic.
No paths, gallery names, source payloads or per-gallery dictionaries are retained.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from threading import Lock
from time import monotonic_ns
from typing import Literal

from .metrics import (
    IngestMetric,
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
_COUNTER_LIMIT = 64


class SourcePerformance:
    """One source turn with fixed phase vocabulary and bounded scalar counters."""

    def __init__(self, *, clock: Callable[[], int] = monotonic_ns) -> None:
        self._clock = clock
        self._lock = Lock()
        self._phases: dict[SourcePhase, int] = {}
        self._counters: dict[str, int] = {}

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

    @contextmanager
    def phase(self, name: SourcePhase) -> Iterator[None]:
        started = self._now()
        try:
            yield
        finally:
            finished = self._now()
            with self._lock:
                if started is not None and finished is not None:
                    self._phases[name] = self._phases.get(name, 0) + max(
                        0, finished - started
                    )
                else:
                    self._counters["clock_failures"] = (
                        self._counters.get("clock_failures", 0) + 1
                    )
            self.add(name + "_calls")

    def metric(
        self,
        *,
        status: Literal["completed", "failed", "interrupted"],
        scope: str = "source",
        operation: str = "synchronize",
    ) -> IngestMetric:
        with self._lock:
            return IngestMetric(
                scope=scope,
                operation=operation,
                status=status,
                elapsed_ns=self._phases.get("source_synchronize", 0),
                phases_ns=tuple(
                    IngestMetricValue(name, value)
                    for name, value in sorted(self._phases.items())
                    if name != "source_synchronize"
                ),
                counters=tuple(
                    IngestMetricValue(name, value)
                    for name, value in sorted(self._counters.items())
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
    ) -> Iterator[None]:
        status: Literal["completed", "failed", "interrupted"] = "completed"
        try:
            with self.phase("source_synchronize"):
                yield
        except Exception as error:
            status = "interrupted" if isinstance(error, interruptions) else "failed"
            raise
        except BaseException:
            status = "interrupted"
            raise
        finally:
            emit_ingest_metric(
                sink, self.metric(status=status, scope=scope, operation=operation)
            )
