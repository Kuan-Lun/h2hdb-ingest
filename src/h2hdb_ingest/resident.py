"""Resident polling loop for the public vNext ingest facade."""

from __future__ import annotations

__all__ = ["IngestSynchronizer", "ResidentIngestor"]

import logging
from collections.abc import Callable
from threading import Event
from time import monotonic
from typing import Protocol

from h2hdb import SchemaEpochReport, VNextDatabaseAdminFacade, VNextIngestFacade

from .config import ResidentConfig
from .session import IngestLeaseHeartbeat, IngestSessionController

logger = logging.getLogger(__name__)


class IngestSynchronizer(Protocol):
    """Cross-phase service invoked under one renewable public session."""

    def synchronize_once(self, session: IngestSessionController) -> object: ...


class ResidentIngestor:
    def __init__(
        self,
        *,
        service: IngestSynchronizer,
        facade: VNextIngestFacade,
        database_admin: VNextDatabaseAdminFacade,
        config: ResidentConfig,
        database_type: str,
        event_logger: Callable[[str], None] | None = None,
    ) -> None:
        self._service = service
        self._facade = facade
        self._database_admin = database_admin
        self._config = config
        self._database_type = database_type.casefold()
        self._event_logger = event_logger or logger.info

    def initialize(self) -> SchemaEpochReport:
        """Validate an existing READY epoch without creating or migrating it."""

        return self._database_admin.check()

    def process_available(
        self,
        *,
        periodic_scan: bool,
        preflight: Callable[[], None] | None = None,
    ) -> bool:
        lease_duration = self._config.lease_seconds * 1_000_000
        claimed = self._facade.try_claim_ingest(periodic_scan, lease_duration)
        if claimed is None:
            return False
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
                except BaseException as completion_error:
                    error.add_note(
                        "The ingest session could not be completed after preflight "
                        f"failed: {completion_error!r}"
                    )
                raise
        with IngestLeaseHeartbeat(
            session,
            interval_seconds=self._config.heartbeat_seconds,
        ) as heartbeat:
            outcome = self._service.synchronize_once(session)
            heartbeat.raise_if_failed()
            self._event_logger(f"vNext ingest synchronization completed: {outcome!r}")
        completion = session.complete()
        self._event_logger(
            "vNext ingest session completed: "
            f"generation={completion.ingest_generation} "
            f"replayed={completion.replayed}"
        )
        return True

    def run_forever(self, *, stop: Event | None = None) -> None:
        stop_event = stop or Event()
        next_periodic = monotonic()
        while not stop_event.is_set():
            periodic = monotonic() >= next_periodic
            if self.process_available(periodic_scan=periodic):
                next_periodic = monotonic() + self._config.periodic_scan_seconds
                continue
            remaining = max(0.0, next_periodic - monotonic())
            stop_event.wait(
                min(
                    self._config.poll_seconds,
                    remaining or self._config.poll_seconds,
                )
            )
