"""Resident polling loop for the public vNext ingest facade."""

from __future__ import annotations

__all__ = ["IngestSynchronizer", "ResidentIngestor"]

import logging
from collections.abc import Callable
from enum import StrEnum
from threading import Event
from time import monotonic
from typing import Protocol

from h2hdb import (
    GalleryStagingCapacityError,
    SchemaEpochReport,
    VNextCurrentOnlyMaintenanceOutcome,
    VNextDatabaseAdminFacade,
    VNextIngestFacade,
    VNextSourceManifestMismatchError,
)

from .config import ResidentConfig
from .maintenance import (
    LibraryMaintenanceAdapter,
    LibraryMaintenanceOutcome,
)
from .service import _IngestStopRequested
from .session import IngestLeaseHeartbeat, IngestSessionController

logger = logging.getLogger(__name__)


class _ResidentCycleOutcome(StrEnum):
    INGESTED = "INGESTED"
    MAINTENANCE_PROGRESSED = "MAINTENANCE_PROGRESSED"
    IDLE = "IDLE"


class IngestSynchronizer(Protocol):
    """Cross-phase service invoked under one renewable public session."""

    def synchronize_once(
        self,
        session: IngestSessionController,
        *,
        should_stop: Callable[[], bool] | None = None,
    ) -> object: ...


class ResidentIngestor:
    def __init__(
        self,
        *,
        service: IngestSynchronizer,
        facade: VNextIngestFacade,
        database_admin: VNextDatabaseAdminFacade,
        library_maintenance: LibraryMaintenanceAdapter,
        config: ResidentConfig,
        database_type: str,
        event_logger: Callable[[str], None] | None = None,
    ) -> None:
        self._service = service
        self._facade = facade
        self._database_admin = database_admin
        if not isinstance(
            library_maintenance,
            LibraryMaintenanceAdapter,
        ):
            raise TypeError(
                "library_maintenance must implement the bounded maintenance protocol"
            )
        self._library_maintenance = library_maintenance
        self._config = config
        self._database_type = database_type.casefold()
        self._event_logger = event_logger or logger.info

    def initialize(self) -> SchemaEpochReport:
        """Validate an existing READY epoch without creating or migrating it."""

        report = self._database_admin.check()
        self._try_library_maintenance()
        self._try_current_only_maintenance()
        return report

    def process_available(
        self,
        *,
        periodic_scan: bool,
        preflight: Callable[[], None] | None = None,
        should_stop: Callable[[], bool] | None = None,
    ) -> bool:
        """Process one ingest or maintenance progress unit if available."""

        return (
            self._process_cycle(
                periodic_scan=periodic_scan,
                preflight=preflight,
                should_stop=should_stop,
            )
            is not _ResidentCycleOutcome.IDLE
        )

    def _process_cycle(
        self,
        *,
        periodic_scan: bool,
        preflight: Callable[[], None] | None = None,
        should_stop: Callable[[], bool] | None = None,
    ) -> _ResidentCycleOutcome:
        if should_stop is not None and should_stop():
            return _ResidentCycleOutcome.IDLE
        lease_duration = self._config.lease_seconds * 1_000_000
        # A previous bounded sweep may have lost its response, contended on the
        # EXCLUSIVE gate, or remained blocked by a live predecessor.  Retrying
        # once before every claim also provides progress while ingest is idle.
        library_maintenance = self._try_library_maintenance()
        if should_stop is not None and should_stop():
            return _ResidentCycleOutcome.IDLE
        if library_maintenance is LibraryMaintenanceOutcome.PROGRESSED:
            return _ResidentCycleOutcome.MAINTENANCE_PROGRESSED
        database_maintenance = self._try_current_only_maintenance(lease_duration)
        if should_stop is not None and should_stop():
            return _ResidentCycleOutcome.IDLE
        if database_maintenance is VNextCurrentOnlyMaintenanceOutcome.PROGRESSED:
            return _ResidentCycleOutcome.MAINTENANCE_PROGRESSED
        claimed = self._facade.try_claim_ingest(periodic_scan, lease_duration)
        if claimed is None:
            return _ResidentCycleOutcome.IDLE
        session = IngestSessionController(
            self._facade,
            claimed,
            lease_duration_microseconds=lease_duration,
            database_type=self._database_type,
        )
        if preflight is not None:
            try:
                preflight()
            except BaseException as error:
                try:
                    session.complete()
                    self._try_library_maintenance()
                    self._try_current_only_maintenance(lease_duration)
                except BaseException as completion_error:
                    error.add_note(
                        "The ingest session could not be completed after preflight "
                        f"failed: {completion_error!r}"
                    )
                raise
        try:
            with IngestLeaseHeartbeat(
                session,
                interval_seconds=self._config.heartbeat_seconds,
            ) as heartbeat:
                if should_stop is None:
                    outcome = self._service.synchronize_once(session)
                else:
                    outcome = self._service.synchronize_once(
                        session,
                        should_stop=should_stop,
                    )
                heartbeat.raise_if_failed()
                self._event_logger(
                    f"vNext ingest synchronization completed: {outcome!r}"
                )
        except _IngestStopRequested:
            self._event_logger(
                "vNext ingest stopped at a durable bounded-step boundary"
            )
            return _ResidentCycleOutcome.IDLE
        except GalleryStagingCapacityError as error:
            # Capacity is bounded backpressure, not a failed resident process.
            # The rejected request committed no rows, while completing the
            # exact session releases its SHARED gate and makes stale terminal
            # staging eligible for bounded EXCLUSIVE maintenance.
            try:
                session.complete()
            except BaseException as completion_error:
                error.add_note(
                    "The ingest session could not be completed after gallery "
                    f"staging capacity was exhausted: {completion_error!r}"
                )
                raise error from completion_error
            library_maintenance = self._try_library_maintenance()
            database_maintenance = self._try_current_only_maintenance(lease_duration)
            if (
                library_maintenance is LibraryMaintenanceOutcome.PROGRESSED
                or database_maintenance is VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
            ):
                return _ResidentCycleOutcome.MAINTENANCE_PROGRESSED
            return _ResidentCycleOutcome.IDLE
        except VNextSourceManifestMismatchError as error:
            # The mismatch has already abandoned the exact build.  Completing
            # after heartbeat shutdown makes it immediately eligible for
            # bounded maintenance instead of waiting for lease expiry, but the
            # source-authority failure remains fatal to this resident turn.
            try:
                session.complete()
                self._try_library_maintenance()
                self._try_current_only_maintenance(lease_duration)
            except BaseException as completion_error:
                error.add_note(
                    "The ingest session could not be completed after source "
                    f"synchronization failed: {completion_error!r}"
                )
            raise
        completion = session.complete()
        self._try_library_maintenance()
        self._try_current_only_maintenance(lease_duration)
        self._event_logger(
            "vNext ingest session completed: "
            f"generation={completion.ingest_generation} "
            f"replayed={completion.replayed}"
        )
        return _ResidentCycleOutcome.INGESTED

    def _try_library_maintenance(
        self,
    ) -> LibraryMaintenanceOutcome | None:
        """Make one bounded ingest-owned CBZ cleanup attempt."""

        try:
            outcome = self._library_maintenance.maintain_cleanup()
            if not isinstance(outcome, LibraryMaintenanceOutcome):
                raise TypeError("library maintenance returned an invalid outcome")
            return outcome
        except Exception:
            logger.exception("library maintenance attempt failed")
            return None

    def _try_current_only_maintenance(
        self,
        lease_duration_microseconds: int | None = None,
    ) -> VNextCurrentOnlyMaintenanceOutcome | None:
        """Make one bounded current-only sweep attempt without blocking ingest."""

        duration = lease_duration_microseconds
        if duration is None:
            duration = self._config.lease_seconds * 1_000_000
        try:
            return self._facade.drain_current_only_maintenance(duration)
        except Exception:
            # The ingest receipt is already durable when this is called after
            # completion.  Maintenance is response-loss safe and the resident
            # retries on the next poll, so a transient failure must not make a
            # completed ingest appear to have rolled back.
            logger.exception("current-only maintenance attempt failed")
            return None

    def run_forever(self, *, stop: Event | None = None) -> None:
        stop_event = stop or Event()
        next_periodic = monotonic()
        while not stop_event.is_set():
            periodic = monotonic() >= next_periodic
            outcome = self._process_cycle(
                periodic_scan=periodic,
                should_stop=stop_event.is_set,
            )
            if outcome is _ResidentCycleOutcome.INGESTED:
                next_periodic = monotonic() + self._config.periodic_scan_seconds
                continue
            if outcome is _ResidentCycleOutcome.MAINTENANCE_PROGRESSED:
                continue
            remaining = max(0.0, next_periodic - monotonic())
            stop_event.wait(
                min(
                    self._config.poll_seconds,
                    remaining or self._config.poll_seconds,
                )
            )
