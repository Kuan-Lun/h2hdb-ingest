__all__ = ["IngestLeaseHeartbeat", "ResidentIngestor"]

import logging
import sqlite3
from collections.abc import Callable
from threading import Event, Lock, Thread
from time import monotonic
from types import TracebackType

from h2hdb import (
    DatabaseAdmin,
    DownloadCoordinator,
    GalleryIngestTurn,
    SchemaCompatibility,
)

from .config import ResidentConfig
from .staged_service import IngestSynchronizer

SQLITE_RENEW_BUSY_TIMEOUT_SECONDS = 1.0
SQLITE_RENEW_RETRY_SECONDS = 0.1
logger = logging.getLogger(__name__)


def _is_sqlite_lock_error(error: BaseException) -> bool:
    current: BaseException | None = error
    visited: set[int] = set()
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        if isinstance(current, sqlite3.OperationalError):
            code = getattr(current, "sqlite_errorcode", None)
            if code is not None:
                return int(code) & 0xFF in {
                    sqlite3.SQLITE_BUSY,
                    sqlite3.SQLITE_LOCKED,
                }
            message = str(current).casefold()
            if "locked" in message or "busy" in message:
                return True
        current = current.__cause__ or current.__context__
    return False


class _HeartbeatStopped(Exception):
    pass


class IngestLeaseHeartbeat:
    def __init__(
        self,
        coordinator: DownloadCoordinator,
        turn: GalleryIngestTurn,
        *,
        lease_seconds: int,
        interval_seconds: float,
        database_type: str = "mariadb",
    ) -> None:
        self._coordinator = coordinator
        self._turn = turn
        self._lease_seconds = lease_seconds
        self._interval_seconds = interval_seconds
        self._database_type = database_type.casefold()
        self._retry_window_seconds = max(
            0.1,
            lease_seconds - interval_seconds - 0.5,
        )
        self._stop = Event()
        self._lock = Lock()
        self._failure: BaseException | None = None
        self._thread = Thread(
            target=self._run,
            name="h2hdb-ingest-heartbeat",
            daemon=True,
        )

    def __enter__(self) -> IngestLeaseHeartbeat:
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.stop()
        if exc_type is None:
            self.raise_if_failed()

    def stop(self) -> None:
        self._stop.set()
        if self._thread.ident is not None:
            self._thread.join()

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            try:
                self.renew_now()
            except _HeartbeatStopped:
                return
            except BaseException as error:
                with self._lock:
                    self._failure = error
                return

    def renew_now(self) -> int:
        with self._lock:
            if self._failure is not None:
                raise RuntimeError("Gallery ingest heartbeat failed") from self._failure
            return self._renew_with_bounded_retry()

    def _renew_with_bounded_retry(self) -> int:
        deadline = monotonic() + self._retry_window_seconds
        while True:
            remaining = max(0.0, deadline - monotonic())
            busy_timeout_ms = (
                max(
                    1,
                    int(min(remaining, SQLITE_RENEW_BUSY_TIMEOUT_SECONDS) * 1000),
                )
                if self._database_type == "sqlite"
                else None
            )
            try:
                renewed = self._coordinator.renew_gallery_ingest(
                    self._turn,
                    lease_seconds=self._lease_seconds,
                    sqlite_busy_timeout_ms=busy_timeout_ms,
                )
            except BaseException as error:
                if (
                    self._database_type != "sqlite"
                    or not _is_sqlite_lock_error(error)
                    or monotonic() >= deadline
                ):
                    raise
                if self._stop.wait(
                    min(SQLITE_RENEW_RETRY_SECONDS, max(0.0, deadline - monotonic()))
                ):
                    raise _HeartbeatStopped from None
                continue
            if renewed is None:
                raise RuntimeError("Gallery ingest lease ownership was lost")
            return renewed

    def raise_if_failed(self) -> None:
        with self._lock:
            if self._failure is not None:
                raise RuntimeError("Gallery ingest heartbeat failed") from self._failure


class ResidentIngestor:
    def __init__(
        self,
        *,
        service: IngestSynchronizer,
        coordinator: DownloadCoordinator,
        database_admin: DatabaseAdmin,
        config: ResidentConfig,
        database_type: str,
        event_logger: Callable[[str], None] | None = None,
    ) -> None:
        self._service = service
        self._coordinator = coordinator
        self._database_admin = database_admin
        self._config = config
        self._database_type = database_type.casefold()
        self._event_logger = event_logger or logger.info

    def initialize(self) -> SchemaCompatibility:
        return self._database_admin.check_compatibility()

    def process_available(
        self,
        *,
        periodic_scan: bool,
        preflight: Callable[[], None] | None = None,
    ) -> bool:
        turn = self._coordinator.claim_gallery_ingest(
            lease_seconds=self._config.lease_seconds,
            periodic_scan=periodic_scan,
        )
        if turn is None:
            return False
        if preflight is not None:
            try:
                preflight()
            except BaseException as error:
                if not self._coordinator.complete_gallery_ingest(turn):
                    error.add_note(
                        "The ingest lease could not be released after preflight "
                        "failed."
                    )
                raise
        with IngestLeaseHeartbeat(
            self._coordinator,
            turn,
            lease_seconds=self._config.lease_seconds,
            interval_seconds=self._config.heartbeat_seconds,
            database_type=self._database_type,
        ) as heartbeat:
            while True:
                outcome = self._service.synchronize_once(turn)
                self._event_logger(
                    "Ingest synchronization completed: "
                    f"revision={outcome.revision} scanned={outcome.scanned} "
                    f"published={outcome.published} new={outcome.new} "
                    f"changed={outcome.changed} removed={outcome.removed} "
                    f"duplicate_losers={outcome.duplicate_losers} "
                    f"cbz_created={outcome.cbz_created} "
                    f"cbz_rebuilt={outcome.cbz_rebuilt}"
                )
                heartbeat.raise_if_failed()
                if not outcome.needs_immediate_rescan:
                    break
            if self._database_type == "sqlite":
                heartbeat.renew_now()
                heartbeat.stop()
            try:
                maintenance = self._database_admin.run_scheduled_database_maintenance()
            except BaseException:
                self._event_logger(
                    "Scheduled database maintenance failed; the ingest turn "
                    "remains unacknowledged for recovery"
                )
                raise
            self._event_logger(
                "Scheduled database maintenance completed: "
                f"evaluated={maintenance.evaluated} "
                f"optimized_targets={len(maintenance.optimized_targets)} "
                f"accumulated_work={maintenance.accumulated_work}"
            )
            heartbeat.raise_if_failed()
        if not self._coordinator.complete_gallery_ingest(
            turn,
            allow_expired_sqlite_lease=self._database_type == "sqlite",
        ):
            raise RuntimeError(
                "Gallery ingest lease ownership was lost before completion"
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
            wait_seconds = min(
                self._config.poll_seconds, remaining or self._config.poll_seconds
            )
            stop_event.wait(wait_seconds)
