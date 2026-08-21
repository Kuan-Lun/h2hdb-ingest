"""Serialized vNext ingest-session access and lease renewal."""

from __future__ import annotations

__all__ = ["IngestLeaseHeartbeat", "IngestSessionController"]

import sqlite3
from collections.abc import Callable
from threading import Event, Lock, Thread
from time import monotonic
from types import TracebackType

from h2hdb import (
    VNextIngestCompletionReceipt,
    VNextIngestFacade,
    VNextIngestSession,
)

SQLITE_RENEW_RETRY_SECONDS = 0.1


class _HeartbeatStopped(Exception):
    pass


class IngestSessionController:
    """Keep the one exact, renewable session receipt current across threads."""

    def __init__(
        self,
        facade: VNextIngestFacade,
        session: VNextIngestSession,
        *,
        lease_duration_microseconds: int,
        database_type: str,
    ) -> None:
        if not isinstance(session, VNextIngestSession):
            raise TypeError("session must be VNextIngestSession")
        if lease_duration_microseconds <= 0:
            raise ValueError("lease_duration_microseconds must be positive")
        self._facade = facade
        self._session = session
        self._lease_duration_microseconds = lease_duration_microseconds
        self._database_type = database_type.casefold()
        self._lock = Lock()
        self._failure: BaseException | None = None

    def call[ResultT](
        self,
        operation: Callable[[VNextIngestFacade, VNextIngestSession], ResultT],
    ) -> ResultT:
        """Run one bounded facade call with the latest exact lease receipt."""

        if not callable(operation):
            raise TypeError("session operation must be callable")
        with self._lock:
            self._raise_if_failed_locked()
            return operation(self._facade, self._session)

    def outside_session[ResultT](
        self,
        operation: Callable[[VNextIngestFacade], ResultT],
    ) -> ResultT:
        """Run local/non-authoritative facade work without blocking renewal."""

        if not callable(operation):
            raise TypeError("outside-session operation must be callable")
        with self._lock:
            self._raise_if_failed_locked()
            facade = self._facade
        result = operation(facade)
        self.raise_if_failed()
        return result

    def renew(self, *, stop: Event | None = None) -> VNextIngestSession:
        """Renew atomically, with a bounded SQLite busy retry window."""

        with self._lock:
            self._raise_if_failed_locked()
            deadline = monotonic() + max(
                0.1,
                self._lease_duration_microseconds / 1_000_000 - 0.5,
            )
            while True:
                try:
                    renewed = self._facade.renew_ingest(
                        self._session,
                        self._lease_duration_microseconds,
                    )
                except BaseException as error:
                    if (
                        self._database_type != "sqlite"
                        or not _is_sqlite_lock_error(error)
                        or monotonic() >= deadline
                    ):
                        raise
                    if stop is not None and stop.wait(
                        min(
                            SQLITE_RENEW_RETRY_SECONDS,
                            max(0.0, deadline - monotonic()),
                        )
                    ):
                        raise _HeartbeatStopped from None
                    continue
                self._session = renewed
                return renewed

    def complete(self) -> VNextIngestCompletionReceipt:
        """Complete using the latest receipt after the heartbeat has stopped."""

        with self._lock:
            self._raise_if_failed_locked()
            return self._facade.complete_ingest(self._session)

    def fail(self, error: BaseException) -> None:
        with self._lock:
            if self._failure is None:
                self._failure = error

    def raise_if_failed(self) -> None:
        with self._lock:
            self._raise_if_failed_locked()

    def _raise_if_failed_locked(self) -> None:
        if self._failure is not None:
            raise RuntimeError("vNext ingest lease heartbeat failed") from self._failure


class IngestLeaseHeartbeat:
    """Renew between bounded facade calls without racing stale receipts."""

    def __init__(
        self,
        controller: IngestSessionController,
        *,
        interval_seconds: float,
    ) -> None:
        if not isinstance(controller, IngestSessionController):
            raise TypeError("controller must be IngestSessionController")
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        self._controller = controller
        self._interval_seconds = interval_seconds
        self._stop = Event()
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

    def renew_now(self) -> VNextIngestSession:
        return self._controller.renew(stop=self._stop)

    def raise_if_failed(self) -> None:
        self._controller.raise_if_failed()

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            try:
                self.renew_now()
            except _HeartbeatStopped:
                return
            except BaseException as error:
                self._controller.fail(error)
                return


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
