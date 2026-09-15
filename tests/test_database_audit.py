"""Audit-session ownership, diagnostics and process-resource acknowledgement."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import replace
from threading import Event
from typing import cast

import pytest
from h2hdb import (
    DatabaseAuditPolicy,
    DatabaseAuditReason,
    DatabaseAuditReport,
    DatabaseAuditSession,
    DatabaseAuditSessionUnavailableError,
    SchemaEpochReadiness,
    SchemaEpochReport,
    VNextCatalogFacade,
    VNextDatabaseAdminFacade,
    VNextIngestFacade,
)

from h2hdb_ingest import ResidentConfig
from h2hdb_ingest.database_audit import IngestDatabaseAudit, IngestStartupStopped
from h2hdb_ingest.resident import ResidentIngestor
from h2hdb_ingest.runtime import IngestRuntime


def audit_report(*, full: bool = True, pending: bool = True) -> DatabaseAuditReport:
    return DatabaseAuditReport(
        session=DatabaseAuditSession(1, b"r" * 16),
        readiness=SchemaEpochReadiness(3, 7, "READY", "a" * 64, 1, 2),
        full_audit=(
            SchemaEpochReport(3, 7, "READY", "a" * 64, (), (), False, False)
            if full
            else None
        ),
        reason=DatabaseAuditReason.FIRST_RUN
        if full
        else DatabaseAuditReason.RECENT_AUDIT,
        last_full_audit_at=1_700_000_000_000_000,
        last_full_audit_duration_microseconds=4_000_000,
        next_full_audit_at=1_700_604_800_000_000,
        initial_catchup_pending=pending,
    )


class _Admin:
    def __init__(self, report: DatabaseAuditReport) -> None:
        self.report = report
        self.policy: DatabaseAuditPolicy | None = None
        self.renewed = Event()
        self.renew_failure: BaseException | None = None
        self.start_failure: BaseException | None = None
        self.check_calls = 0
        self.catchup_calls = 0
        self.start_calls = 0

    def start_ingest_runtime(
        self,
        *,
        policy: DatabaseAuditPolicy,
        lease_duration_microseconds: int,
        on_check: Callable[[DatabaseAuditReason], None],
    ) -> DatabaseAuditReport:
        assert lease_duration_microseconds > 0
        self.policy = policy
        self.start_calls += 1
        if self.start_failure is not None:
            raise self.start_failure
        on_check(self.report.reason)
        return self.report

    def check_ingest_runtime_if_due(
        self,
        session: DatabaseAuditSession,
        *,
        on_check: Callable[[DatabaseAuditReason], None],
    ) -> DatabaseAuditReport:
        assert session == self.report.session
        self.check_calls += 1
        on_check(self.report.reason)
        return self.report

    def renew_ingest_runtime(
        self, session: DatabaseAuditSession, duration: int
    ) -> None:
        assert session == self.report.session and duration > 0
        self.renewed.set()
        if self.renew_failure is not None:
            raise self.renew_failure

    def mark_initial_catchup_complete(
        self, session: DatabaseAuditSession
    ) -> DatabaseAuditReport:
        assert session == self.report.session
        self.catchup_calls += 1
        self.report = replace(
            self.report,
            full_audit=None,
            reason=DatabaseAuditReason.INITIAL_CATCHUP,
            initial_catchup_pending=False,
            next_full_audit_at=self.report.next_full_audit_at + 123_000_000,
        )
        return self.report


@pytest.fixture
def audits() -> Iterator[list[IngestDatabaseAudit]]:
    values: list[IngestDatabaseAudit] = []
    yield values
    for audit in values:
        try:
            audit.close()
        except RuntimeError:
            if not audit.failed:
                raise


def _audit(
    audits: list[IngestDatabaseAudit],
    admin: _Admin,
    messages: list[str],
    *,
    heartbeat: float = 60,
) -> IngestDatabaseAudit:
    audit = IngestDatabaseAudit(
        cast(VNextDatabaseAdminFacade, admin),
        ResidentConfig(heartbeat_seconds=heartbeat),
        messages.append,
        database_type="sqlite",
    )
    audits.append(audit)
    return audit


@pytest.mark.parametrize("full", [True, False])
def test_startup_reports_actual_check_and_durable_schedule(
    audits: list[IngestDatabaseAudit], full: bool
) -> None:
    admin = _Admin(audit_report(full=full))
    messages: list[str] = []
    audit = _audit(audits, admin, messages)
    assert audit.start() is admin.report
    with pytest.raises(RuntimeError, match="already initialized"):
        audit.start()
    assert admin.start_calls == 1
    assert admin.policy == DatabaseAuditPolicy(604_800_000_000, 100)
    assert audit.session == admin.report.session
    assert any(
        "scheduled when initial gallery catch-up completes" in message
        for message in messages
    )
    assert any("4.000s" in message for message in messages) is full
    assert (
        any("Full database audit selected" in message for message in messages) is full
    )


@pytest.mark.parametrize(
    "reason", [DatabaseAuditReason.RECENT_AUDIT, DatabaseAuditReason.INITIAL_CATCHUP]
)
def test_periodic_schedule_uses_core_decision_and_does_not_spam_quick_checks(
    audits: list[IngestDatabaseAudit],
    reason: DatabaseAuditReason,
) -> None:
    admin = _Admin(replace(audit_report(full=False), reason=reason))
    messages: list[str] = []
    audit = _audit(audits, admin, messages)
    audit.start()
    messages.clear()
    for _ in range(3):
        audit.check_between_sessions()
    assert messages == []
    admin.report = replace(audit_report(), reason=DatabaseAuditReason.SCHEDULE_DUE)
    audit.check_between_sessions()
    assert admin.check_calls == 4
    assert "scheduled audit is due" in messages[0]
    assert "Scheduled full database audit completed" in messages[1]


def test_first_catchup_uses_returned_deadline_only_once(
    audits: list[IngestDatabaseAudit],
) -> None:
    admin = _Admin(audit_report())
    messages: list[str] = []
    audit = _audit(audits, admin, messages)
    audit.start()
    old_success = admin.report.last_full_audit_at
    audit.initial_catchup_complete()
    audit.initial_catchup_complete()
    assert admin.catchup_calls == 1
    assert admin.report.last_full_audit_at == old_success
    assert sum("Initial gallery catch-up completed" in value for value in messages) == 1


def test_background_renewal_failure_is_fatal_before_more_work(
    audits: list[IngestDatabaseAudit],
) -> None:
    admin = _Admin(audit_report())
    failure = RuntimeError("lost database session")
    admin.renew_failure = failure
    audit = _audit(audits, admin, [], heartbeat=0.001)
    audit.start()
    assert admin.renewed.wait(2)
    with pytest.raises(RuntimeError, match="audit session failed") as captured:
        audit.close()
    assert captured.value.__cause__ is failure
    with pytest.raises(RuntimeError, match="audit session failed"):
        audit.check_between_sessions()
    assert admin.check_calls == 0


def test_waiting_for_live_owner_can_stop_without_admitting_or_marking_clean(
    audits: list[IngestDatabaseAudit],
) -> None:
    admin = _Admin(audit_report())
    admin.start_failure = DatabaseAuditSessionUnavailableError("busy")
    audit = _audit(audits, admin, [])
    with pytest.raises(IngestStartupStopped):
        audit.start(should_stop=lambda: admin.start_calls == 1)
    assert audit.session is None
    assert not audit.failed


def test_startup_failure_is_retained_without_a_session(
    audits: list[IngestDatabaseAudit],
) -> None:
    admin = _Admin(audit_report())
    admin.start_failure = ValueError("corrupt database")
    audit = _audit(audits, admin, [])
    with pytest.raises(ValueError, match="corrupt database"):
        audit.start()
    assert audit.failed and audit.session is None


class _Resource:
    def __init__(self, events: list[str], name: str, *, fail: bool = False) -> None:
        self.events, self.name, self.fail = events, name, fail

    def close(self) -> None:
        self.events.append(self.name)
        if self.fail:
            raise RuntimeError("cleanup failed")


def _runtime(
    audit: IngestDatabaseAudit, events: list[str], scratch: _Resource
) -> IngestRuntime:
    return IngestRuntime(
        cast(VNextIngestFacade, _Resource(events, "ingest")),
        cast(VNextDatabaseAdminFacade, _Resource(events, "admin")),
        cast(VNextCatalogFacade, _Resource(events, "catalog")),
        cast(ResidentIngestor, object()),
        _audit=audit,
        _owned_resources=scratch,
        _finish_audit=lambda _session: events.append("clean"),
    )


def test_clean_ack_follows_all_pools_and_scratch_cleanup(
    audits: list[IngestDatabaseAudit],
) -> None:
    audit = _audit(audits, _Admin(audit_report()), [])
    audit.start()
    events: list[str] = []
    runtime = _runtime(audit, events, _Resource(events, "scratch"))
    runtime.close()
    runtime.close()
    assert events == ["ingest", "catalog", "scratch", "admin", "clean"]


def test_cleanup_failure_remains_dirty_even_when_resource_close_is_retried(
    audits: list[IngestDatabaseAudit],
) -> None:
    audit = _audit(audits, _Admin(audit_report()), [])
    audit.start()
    events: list[str] = []
    scratch = _Resource(events, "scratch", fail=True)
    runtime = _runtime(audit, events, scratch)
    with pytest.raises(RuntimeError, match="cleanup failed"):
        runtime.close()
    scratch.fail = False
    runtime.close()
    assert "clean" not in events


def test_escaped_work_exception_never_marks_clean(
    audits: list[IngestDatabaseAudit],
) -> None:
    audit = _audit(audits, _Admin(audit_report()), [])
    audit.start()
    events: list[str] = []
    with (
        pytest.raises(ValueError, match="work failed"),
        _runtime(audit, events, _Resource(events, "scratch")),
    ):
        raise ValueError("work failed")
    assert events == ["ingest", "catalog", "scratch", "admin"]


def test_sqlite_busy_renewal_retries_without_changing_the_session(
    audits: list[IngestDatabaseAudit],
) -> None:
    import sqlite3

    class BusyAdmin(_Admin):
        calls = 0

        def renew_ingest_runtime(
            self, session: DatabaseAuditSession, duration: int
        ) -> None:
            self.calls += 1
            if self.calls == 1:
                raise sqlite3.OperationalError("database is locked")
            super().renew_ingest_runtime(session, duration)

    admin = BusyAdmin(audit_report())
    audit = _audit(audits, admin, [])
    audit.start()
    session = audit.session
    audit.renew()
    assert admin.calls == 2 and admin.renewed.is_set()
    assert audit.session == session and not audit.failed


def test_sqlite_busy_retry_deadline_uses_monotonic_time(
    audits: list[IngestDatabaseAudit],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sqlite3

    import h2hdb_ingest.database_audit as audit_module

    now = [0.0]

    class BusyAdmin(_Admin):
        calls = 0

        def renew_ingest_runtime(
            self, session: DatabaseAuditSession, duration: int
        ) -> None:
            del session, duration
            self.calls += 1
            now[0] = 301.0
            raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(audit_module, "monotonic", lambda: now[0])
    admin = BusyAdmin(audit_report())
    audit = _audit(audits, admin, [])
    audit.start()
    with pytest.raises(sqlite3.OperationalError, match="locked"):
        audit.renew()
    assert admin.calls == 1


def test_already_reported_work_failure_closes_resources_without_clean_ack(
    audits: list[IngestDatabaseAudit],
) -> None:
    audit = _audit(audits, _Admin(audit_report()), [])
    audit.start()
    audit.fail(ValueError("work failed and was already reported"))
    events: list[str] = []
    runtime = _runtime(audit, events, _Resource(events, "scratch"))
    runtime.close()
    assert events == ["ingest", "catalog", "scratch", "admin"]


def test_audit_heartbeat_remains_live_while_scratch_drains(
    audits: list[IngestDatabaseAudit],
) -> None:
    admin = _Admin(audit_report())
    audit = _audit(audits, admin, [], heartbeat=0.01)
    audit.start()
    events: list[str] = []

    class Scratch(_Resource):
        def close(self) -> None:
            super().close()
            admin.renewed.clear()
            assert admin.renewed.wait(2), "scratch cleanup lost its lease heartbeat"

    _runtime(audit, events, Scratch(events, "scratch")).close()
    assert events[-1] == "clean"
