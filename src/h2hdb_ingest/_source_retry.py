"""Bounded reobservation hints; sealed source facts remain the authority."""

from __future__ import annotations

from dataclasses import dataclass, field

from h2hdb import get_artifact_failure_context

_MAXIMUM_LOCATORS = 128


@dataclass(slots=True)
class SourceReobservation:
    """Keep every failed locator until publication, avoiding alternating retries."""

    _locators: set[tuple[str, ...]] = field(default_factory=set)
    _full_refresh: bool = False

    @property
    def locators(self) -> tuple[tuple[str, ...], ...]:
        return tuple(sorted(self._locators))

    @property
    def reuse_sealed_observations(self) -> bool:
        return not self._full_refresh

    def record_failure(self, error: BaseException) -> None:
        try:
            context = get_artifact_failure_context(error)
            if context is not None:
                self.record(context.gallery_locator_components)
        except Exception:
            # Optional diagnostics cannot prevent releasing the ingest lease.
            # Refreshing all observations also prevents an unknown stale cache
            # entry from repeatedly failing after its producer reused a marker.
            self._locators.clear()
            self._full_refresh = True
            return

    def record(self, locator: tuple[str, ...]) -> None:
        if self._full_refresh or locator in self._locators:
            return
        if len(self._locators) == _MAXIMUM_LOCATORS:
            self._locators.clear()
            self._full_refresh = True
        else:
            self._locators.add(locator)

    def clear(self) -> None:
        self._locators.clear()
        self._full_refresh = False
