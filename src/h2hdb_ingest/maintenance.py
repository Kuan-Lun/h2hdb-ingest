"""Public bounded maintenance contract for ingest-owned CBZ state."""

from __future__ import annotations

__all__ = [
    "CurrentProjectionMaintenanceAdapter",
    "CurrentProjectionMaintenanceOutcome",
]

from enum import StrEnum
from typing import Protocol, runtime_checkable


class CurrentProjectionMaintenanceOutcome(StrEnum):
    """Result of one bounded current-projection cleanup action."""

    DONE = "DONE"
    PROGRESSED = "PROGRESSED"
    BLOCKED = "BLOCKED"


@runtime_checkable
class CurrentProjectionMaintenanceAdapter(Protocol):
    """Ingest-owned cleanup port called by the normal resident poll loop."""

    def maintain_cleanup(self) -> CurrentProjectionMaintenanceOutcome:
        """Attempt one bounded cleanup unit and report durable queue state."""
