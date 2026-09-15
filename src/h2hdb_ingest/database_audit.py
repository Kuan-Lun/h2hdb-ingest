"""Own the ingest process audit lease without moving audit policy out of core."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from threading import Event, Lock, Thread
from time import monotonic

from h2hdb import (
    DatabaseAuditPolicy,
    DatabaseAuditReason,
    DatabaseAuditReport,
    DatabaseAuditSession,
    DatabaseAuditSessionUnavailableError,
    VNextDatabaseAdminFacade,
)

from .config import ResidentConfig
from .session import SQLITE_RENEW_RETRY_SECONDS, _is_sqlite_lock_error

_REASONS = {
    DatabaseAuditReason.FIRST_RUN: "this database has no completed scheduled audit",
    DatabaseAuditReason.PREVIOUS_INTERRUPTION: "the previous ingest process did not finish cleanly",
    DatabaseAuditReason.VALIDATOR_CHANGED: "the database validators have changed",
    DatabaseAuditReason.SCHEDULE_DUE: "the scheduled audit is due",
    DatabaseAuditReason.CLOCK_CHANGED: "the database clock moved behind the previous audit",
    DatabaseAuditReason.FORCED: "a full audit was requested",
    DatabaseAuditReason.RECENT_AUDIT: "the previous audit is still within its scheduled interval",
    DatabaseAuditReason.INITIAL_CATCHUP: "the initial gallery catch-up is still in progress",
}
_QUICK_REASONS = frozenset(
    (DatabaseAuditReason.RECENT_AUDIT, DatabaseAuditReason.INITIAL_CATCHUP)
)


class IngestStartupStopped(Exception):
    """Termination was requested before admission to the database audit session."""


class IngestDatabaseAudit:
    """Serialize renewal and audits; durable core state owns all due decisions."""

    def __init__(
        self,
        admin: VNextDatabaseAdminFacade,
        config: ResidentConfig,
        emit: Callable[[str], None],
        *,
        database_type: str,
    ) -> None:
        self._admin = admin
        self._database_type = database_type.casefold()
        self._policy = DatabaseAuditPolicy(
            minimum_interval_microseconds=config.database_audit_minimum_interval_seconds
            * 1_000_000,
            duration_multiplier=config.database_audit_duration_multiplier,
        )
        self._lease_duration = config.lease_seconds * 1_000_000
        self._heartbeat_interval = config.heartbeat_seconds
        self._event_logger = emit
        self._poll_seconds = config.poll_seconds
        self._lock = Lock()
        self._stop = Event()
        self._thread: Thread | None = None
        self._report: DatabaseAuditReport | None = None
        self._failure: BaseException | None = None
        self._failure_observed = False
        self._closed = False

    @property
    def session(self) -> DatabaseAuditSession | None:
        with self._lock:
            return None if self._report is None else self._report.session

    @property
    def failed(self) -> bool:
        with self._lock:
            return self._failure is not None

    def fail(self, error: BaseException) -> None:
        with self._lock:
            if self._failure is None:
                self._failure = error
                self._failure_observed = True

    def raise_if_failed(self) -> None:
        with self._lock:
            self._raise_if_failed()

    def _raise_if_failed(self) -> None:
        if self._failure is not None:
            self._failure_observed = True
            raise RuntimeError(
                "The ingest database audit session failed"
            ) from self._failure

    def start(
        self, *, should_stop: Callable[[], bool] | None = None
    ) -> DatabaseAuditReport:
        with self._lock:
            self._raise_if_failed()
            if self._closed:
                raise RuntimeError("The ingest database audit session is closed")
            if self._report is not None:
                raise RuntimeError(
                    "The ingest database audit session is already initialized"
                )
            self._emit("Checking the database audit schedule before ingest starts")
            try:
                waiting = False
                while True:
                    if self._stop.is_set() or (
                        should_stop is not None and should_stop()
                    ):
                        raise IngestStartupStopped
                    try:
                        report = self._admin.start_ingest_runtime(
                            policy=self._policy,
                            lease_duration_microseconds=self._lease_duration,
                            on_check=self._announce_decision,
                        )
                        break
                    except DatabaseAuditSessionUnavailableError:
                        if not waiting:
                            self._emit(
                                "Waiting for the previous ingest process to release its database audit session"
                            )
                            waiting = True
                        remaining = self._poll_seconds
                        while remaining > 0:
                            if self._stop.is_set() or (
                                should_stop is not None and should_stop()
                            ):
                                raise IngestStartupStopped from None
                            delay = min(remaining, 1.0)
                            self._stop.wait(delay)
                            remaining -= delay
                self._report = report
                self._describe(report, startup=True)
                self._thread = Thread(
                    target=self._run,
                    name="h2hdb-database-audit-heartbeat",
                    daemon=True,
                )
                self._thread.start()
                return report
            except IngestStartupStopped:
                raise
            except BaseException as error:
                self._failure = error
                self._failure_observed = True
                raise

    def check_between_sessions(self) -> None:
        with self._lock:
            self._raise_if_failed()
            if self._report is None:
                raise RuntimeError(
                    "Initialize the ingest resident before processing work"
                )
            report = self._admin.check_ingest_runtime_if_due(
                self._report.session, on_check=self._announce_periodic_decision
            )
            self._report = report
            if report.full_audit is not None:
                self._describe(report, startup=False)

    def initial_catchup_complete(self) -> None:
        with self._lock:
            self._raise_if_failed()
            if self._report is None or not self._report.initial_catchup_pending:
                return
            report = self._admin.mark_initial_catchup_complete(self._report.session)
            self._report = report
            self._emit(
                "Initial gallery catch-up completed; the next full database audit is scheduled for "
                + _timestamp(report.next_full_audit_at)
            )

    def renew(self) -> None:
        with self._lock:
            self._raise_if_failed()
            if self._report is None or self._stop.is_set():
                return
            deadline = monotonic() + max(0.1, self._lease_duration / 1_000_000 - 0.5)
            while True:
                try:
                    self._admin.renew_ingest_runtime(
                        self._report.session, self._lease_duration
                    )
                    return
                except BaseException as error:
                    if (
                        self._database_type != "sqlite"
                        or not _is_sqlite_lock_error(error)
                        or monotonic() >= deadline
                    ):
                        raise
                    if self._stop.wait(
                        min(
                            SQLITE_RENEW_RETRY_SECONDS, max(0.0, deadline - monotonic())
                        )
                    ):
                        return

    def close(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join()
        with self._lock:
            self._closed = True
            if not self._failure_observed:
                self._raise_if_failed()

    def _run(self) -> None:
        while not self._stop.wait(self._heartbeat_interval):
            try:
                self.renew()
            except BaseException as error:
                with self._lock:
                    if self._failure is None:
                        self._failure = error
                return

    def _emit(self, message: str) -> None:
        try:
            self._event_logger(message)
        except Exception:
            # Diagnostics never change the durable admission or audit result.
            pass

    def _announce_decision(self, reason: DatabaseAuditReason) -> None:
        mode = (
            "Quick database startup check"
            if reason in _QUICK_REASONS
            else "Full database audit"
        )
        self._emit(mode + " selected: " + _REASONS[reason])

    def _announce_periodic_decision(self, reason: DatabaseAuditReason) -> None:
        if reason not in _QUICK_REASONS:
            self._announce_decision(reason)

    def _describe(self, report: DatabaseAuditReport, *, startup: bool) -> None:
        reason = _REASONS[report.reason]
        schedule = (
            "the next periodic audit will be scheduled when initial gallery catch-up completes"
            if report.initial_catchup_pending
            else "next full audit at " + _timestamp(report.next_full_audit_at)
        )
        if report.full_audit is None:
            self._emit(
                "Quick database startup check completed: " + reason + "; " + schedule
            )
        else:
            seconds = report.last_full_audit_duration_microseconds / 1_000_000
            label = "Startup" if startup else "Scheduled"
            self._emit(
                f"{label} full database audit completed in {seconds:.3f}s: {reason}; "
                f"{schedule}"
            )


def _timestamp(microseconds: int) -> str:
    try:
        return datetime.fromtimestamp(microseconds / 1_000_000, tz=UTC).isoformat(
            timespec="seconds"
        )
    except OverflowError, OSError, ValueError:
        return f"Unix time {microseconds} microseconds"
