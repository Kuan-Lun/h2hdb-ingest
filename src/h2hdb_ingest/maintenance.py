"""Public bounded maintenance contract for ingest-owned CBZ state."""

from __future__ import annotations

__all__ = [
    "LibraryMaintenanceAdapter",
    "LibraryMaintenanceOutcome",
]

from enum import StrEnum
from typing import Protocol, runtime_checkable


class LibraryMaintenanceOutcome(StrEnum):
    """Result of one bounded private-library cleanup action."""

    DONE = "DONE"
    PROGRESSED = "PROGRESSED"
    BLOCKED = "BLOCKED"


@runtime_checkable
class LibraryMaintenanceAdapter(Protocol):
    """Ingest-owned cleanup port called by the normal resident poll loop."""

    def maintain_cleanup(self) -> LibraryMaintenanceOutcome:
        """Attempt one bounded cleanup unit and report durable queue state."""
