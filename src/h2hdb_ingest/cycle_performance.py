"""Bounded, process-local publication/maintenance/claim diagnostics.

The observer consumes completed public results. It never authorizes a claim,
proves global cleanup, or reconstructs history after a process restart.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Callable
from dataclasses import dataclass
from time import monotonic
from typing import Literal

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class _Publication:
    generation: int
    at: float | None
    library_done: bool = False
    catalog_done: bool = False


class CyclePerformance:
    """Keep one publication and report each subsequent component DONE once."""

    def __init__(
        self,
        emit: Callable[[str], None] | None = None,
        *,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        self._emit = logger.info if emit is None else emit
        self._clock = clock
        self._publication: _Publication | None = None

    def reset(self) -> None:
        self._publication = None

    def published(self, generation: int) -> None:
        now = self._now()
        self._publication = _Publication(generation, now)
        self._record(
            f"event=publication_completed generation={generation} "
            f"monotonic_seconds={self._number(now)} history=process_local"
        )

    def maintenance_done(self, component: Literal["library", "catalog"]) -> None:
        publication = self._publication
        if publication is None:
            return
        match component:
            case "library":
                if publication.library_done:
                    return
                publication.library_done = True
            case "catalog":
                if publication.catalog_done:
                    return
                publication.catalog_done = True
        now = self._now()
        self._record(
            f"event=component_done component={component} "
            f"publication_generation={publication.generation} "
            f"monotonic_seconds={self._number(now)} "
            f"after_publication_seconds={self._elapsed(publication.at, now)} "
            "scope=component_only history=process_local"
        )

    def claimed(self, generation: int) -> None:
        publication = self._publication
        self._publication = None
        now = self._now()
        common = (
            f"event=ingest_claimed generation={generation} "
            f"monotonic_seconds={self._number(now)} history=process_local "
        )
        if publication is None:
            self._record(
                common + "previous_publication_generation=unknown "
                "after_publication_seconds=unknown "
                "reason=no_process_local_publication"
            )
            return
        self._record(
            common + f"previous_publication_generation={publication.generation} "
            f"after_publication_seconds={self._elapsed(publication.at, now)} "
            f"library_done_observed={str(publication.library_done).lower()} "
            f"catalog_done_observed={str(publication.catalog_done).lower()}"
        )

    def _now(self) -> float | None:
        try:
            value = self._clock()
            return (
                value
                if type(value) in (int, float) and math.isfinite(value) and value >= 0
                else None
            )
        except Exception:
            return None

    @staticmethod
    def _number(value: float | None) -> str:
        return "unknown" if value is None else f"{value:.6f}"

    @classmethod
    def _elapsed(cls, start: float | None, end: float | None) -> str:
        if start is None or end is None or end < start:
            return "unknown"
        return cls._number(end - start)

    def _record(self, fields: str) -> None:
        try:
            self._emit("ingest_cycle_performance " + fields)
        except Exception:
            # An observer must not turn durable publication, cleanup or claim
            # success into a failed ingest operation.
            pass
