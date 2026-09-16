"""Serialized vNext ingest-session access and lease renewal."""

from __future__ import annotations

__all__ = ["IngestLeaseHeartbeat", "IngestSessionController"]

import logging
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from datetime import UTC, datetime
from threading import Event, Lock, Thread
from time import monotonic, time_ns
from types import TracebackType

from h2hdb import (
    VNextIngestCompletionReceipt,
    VNextIngestFacade,
    VNextIngestSession,
)

logger = logging.getLogger(__name__)

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
        self._last_renewed_at: int | None = None

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

    @contextmanager
    def prepare[ResultT](
        self,
        operation: Callable[[VNextIngestFacade], AbstractContextManager[ResultT]],
    ) -> Iterator[ResultT]:
        """Own prepared resources before propagating a concurrent lease failure."""

        if not callable(operation):
            raise TypeError("preparation operation must be callable")
        with self._lock:
            self._raise_if_failed_locked()
            facade = self._facade
        with operation(facade) as prepared:
            self.raise_if_failed()
            yield prepared

    def renew(self, *, stop: Event | None = None) -> VNextIngestSession:
        """Renew the exact receipt, fencing failures before another call can start."""

        requested_at = time_ns() // 1000
        requested = monotonic()
        diagnostic: str | None = None
        try:
            with self._lock:
                self._raise_if_failed_locked()
                started = monotonic()
                try:
                    renewed = self._renew_locked(stop=stop)
                except _HeartbeatStopped:
                    raise
                except BaseException as error:
                    diagnostic = self._fail_locked(error)
                    if diagnostic is not None:
                        diagnostic += (
                            f" Renewal requested at {_format_timestamp(requested_at)};"
                            f" waited {started - requested:.3f}s for the session lock;"
                            f" renewal attempt took {monotonic() - started:.3f}s."
                        )
                    raise
                self._last_renewed_at = time_ns() // 1000
                return renewed
        except BaseException as error:
            if diagnostic is not None:
                _log_failure(diagnostic, error)
            raise

    def _renew_locked(self, *, stop: Event | None) -> VNextIngestSession:
        remaining_seconds = (
            min(
                self._session.gate_lease_expires_at,
                self._session.ingest_lease_expires_at,
            )
            - time_ns() // 1000
        ) / 1_000_000
        deadline = monotonic() + max(
            0.0,
            min(self._lease_duration_microseconds / 1_000_000, remaining_seconds) - 0.5,
        )
        interrupt = stop if stop is not None else Event()
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
                if interrupt.wait(
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
            diagnostic = self._fail_locked(error)
        if diagnostic is not None:
            _log_failure(diagnostic, error)

    def _fail_locked(self, error: BaseException) -> str | None:
        if self._failure is not None:
            return None
        self._failure = error
        return (
            "Lease renewal failed; ingest will stop at the next safe checkpoint. "
            f"Last successful renewal: {_format_timestamp(self._last_renewed_at)}; "
            "current receipt: "
            f"gate lease valid until {_format_timestamp(self._session.gate_lease_expires_at)}, "
            f"ingest lease valid until {_format_timestamp(self._session.ingest_lease_expires_at)}. "
            f"Cause: {type(error).__name__}: {error}."
        )

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
        # The claim may have spent most of its remaining lease reaching us.
        # A failed renewal is fatal; an expired receipt is never re-claimed here.
        self.renew_now()
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


def _format_timestamp(microseconds: int | None) -> str:
    if microseconds is None:
        return "none"
    try:
        return datetime.fromtimestamp(microseconds / 1_000_000, tz=UTC).isoformat()
    except OverflowError, OSError, ValueError:
        return f"{microseconds} microseconds since Unix epoch"


def _log_failure(diagnostic: str, error: BaseException) -> None:
    # No receipt or owner token is logged. Logging runs outside the receipt lock,
    # after the failure is already authoritative for every caller.
    logger.error("%s", diagnostic, exc_info=(type(error), error, error.__traceback__))
