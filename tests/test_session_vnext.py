from __future__ import annotations

import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock
from time import sleep
from typing import cast

import pytest
from h2hdb import VNextIngestFacade, VNextIngestSession

import h2hdb_ingest.session as session_module
from h2hdb_ingest.session import IngestLeaseHeartbeat, IngestSessionController


def _session(expiry: int = 1_000_000) -> VNextIngestSession:
    return VNextIngestSession(
        gate_owner_token=b"g" * 16,
        gate_generation=1,
        gate_slot=0,
        gate_lease_expires_at=expiry,
        ingest_generation=1,
        ingest_owner_token=b"i" * 16,
        ingest_lease_expires_at=expiry,
        download_generation=None,
        handoff_owner_token=None,
        handoff_kind=None,
        consumed_at=None,
    )


class _Facade:
    def __init__(self, *, busy_attempts: int = 0) -> None:
        self.busy_attempts = busy_attempts
        self.calls = 0
        self.seen_expiries: list[int] = []
        self._active_lock = Lock()
        self.active = 0
        self.maximum_active = 0

    def renew_ingest(
        self,
        session: VNextIngestSession,
        lease_duration_microseconds: int,
    ) -> VNextIngestSession:
        del lease_duration_microseconds
        with self._active_lock:
            self.active += 1
            self.maximum_active = max(self.maximum_active, self.active)
        try:
            self.calls += 1
            if self.calls <= self.busy_attempts:
                raise sqlite3.OperationalError("database is locked")
            sleep(0.01)
            self.seen_expiries.append(session.ingest_lease_expires_at)
            return VNextIngestSession(
                session.gate_owner_token,
                session.gate_generation,
                session.gate_slot,
                session.gate_lease_expires_at + 1,
                session.ingest_generation,
                session.ingest_owner_token,
                session.ingest_lease_expires_at + 1,
                session.download_generation,
                session.handoff_owner_token,
                session.handoff_kind,
                session.consumed_at,
            )
        finally:
            with self._active_lock:
                self.active -= 1


def _controller(facade: _Facade, *, database_type: str) -> IngestSessionController:
    return IngestSessionController(
        cast(VNextIngestFacade, facade),
        _session(),
        lease_duration_microseconds=2_000_000,
        database_type=database_type,
    )


def test_renewal_retries_sqlite_busy_and_carries_forward_latest_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session_module, "time_ns", lambda: 0)
    facade = _Facade(busy_attempts=2)
    controller = _controller(facade, database_type="sqlite")

    controller.renew()
    controller.renew()

    assert facade.calls == 4
    assert facade.seen_expiries == [1_000_000, 1_000_001]


def test_facade_calls_and_renewals_are_serialized() -> None:
    facade = _Facade()
    controller = _controller(facade, database_type="mariadb")

    with ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(lambda _index: controller.renew(), range(2)))

    assert facade.maximum_active == 1
    assert facade.seen_expiries == [1_000_000, 1_000_001]


class _FailedFacade(_Facade):
    def renew_ingest(
        self,
        session: VNextIngestSession,
        lease_duration_microseconds: int,
    ) -> VNextIngestSession:
        del session, lease_duration_microseconds
        self.calls += 1
        raise RuntimeError("the gate lease is stale or expired")


def test_heartbeat_renews_before_entering_work_without_initial_interval() -> None:
    facade = _Facade()
    controller = _controller(facade, database_type="mariadb")
    with IngestLeaseHeartbeat(controller, interval_seconds=60):
        assert facade.calls == 1
        assert controller.call(lambda _facade, receipt: receipt) == _session(1_000_001)
    assert facade.calls == 1


def test_failed_renewal_immediately_fences_calls_and_reports_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    facade = _FailedFacade()
    controller = _controller(facade, database_type="mariadb")
    with pytest.raises(RuntimeError, match="stale or expired"):
        controller.renew()
    records = [
        record for record in caplog.records if record.name == session_module.__name__
    ]
    assert len(records) == 1
    assert records[0].levelno == logging.ERROR
    message = records[0].getMessage()
    assert "next safe checkpoint" in message
    assert "Last successful renewal: none" in message
    assert "gate lease" in message and "ingest lease" in message
    assert "1970-01-01T00:00:01" in message
    assert "RuntimeError" in message
    assert "gggg" not in message and "iiii" not in message
    with pytest.raises(RuntimeError, match="heartbeat failed"):
        controller.call(lambda _facade, _receipt: pytest.fail("stale receipt escaped"))
    with pytest.raises(RuntimeError, match="heartbeat failed"):
        controller.renew()
    assert facade.calls == 1
    assert len(caplog.records) == 1


def test_failed_renewal_is_fenced_before_error_log_is_emitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    facade = _FailedFacade()
    controller = _controller(facade, database_type="mariadb")
    logging_started = Event()
    finish_logging = Event()

    def blocked_log(*args: object, **kwargs: object) -> None:
        del args, kwargs
        logging_started.set()
        assert finish_logging.wait(5)

    monkeypatch.setattr(
        session_module,
        "logger",
        type("Logger", (), {"error": staticmethod(blocked_log)})(),
    )
    with ThreadPoolExecutor(max_workers=2) as executor:
        renewed = executor.submit(controller.renew)
        try:
            assert logging_started.wait(5)
            checked = executor.submit(controller.call, lambda _facade, _receipt: None)
            with pytest.raises(RuntimeError, match="heartbeat failed"):
                checked.result(timeout=5)
        finally:
            finish_logging.set()
        with pytest.raises(RuntimeError, match="stale or expired"):
            renewed.result(timeout=5)


def test_expired_heartbeat_is_not_retried_or_entered() -> None:
    facade = _FailedFacade()
    controller = _controller(facade, database_type="sqlite")
    with pytest.raises(RuntimeError, match="stale or expired"):
        with IngestLeaseHeartbeat(controller, interval_seconds=60):
            pytest.fail("work began after failed initial renewal")
    assert facade.calls == 1


@pytest.mark.parametrize("expiry", (0, 400_000))
def test_sqlite_busy_retry_cannot_spend_another_full_lease(
    monkeypatch: pytest.MonkeyPatch, expiry: int
) -> None:
    monkeypatch.setattr(session_module, "time_ns", lambda: 0)
    facade = _Facade(busy_attempts=2)
    controller = IngestSessionController(
        cast(VNextIngestFacade, facade),
        _session(expiry),
        lease_duration_microseconds=300_000_000,
        database_type="sqlite",
    )
    with pytest.raises(sqlite3.OperationalError, match="locked"):
        controller.renew()
    assert facade.calls == 1
    with pytest.raises(RuntimeError, match="heartbeat failed"):
        controller.renew()


def test_failure_diagnostic_uses_latest_successful_receipt_and_wall_time(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Wall-clock jumps do not affect the monotonic duration calculation.
    wall_times = iter(
        (1_000_000_000, 1_000_000_000, 2_000_000_000, 100_000_000_000, 100_000_000_000)
    )
    monotonic_times = iter((10.0, 10.25, 10.25, 20.0, 20.5, 20.5, 23.5))
    monkeypatch.setattr(session_module, "time_ns", lambda: next(wall_times))
    monkeypatch.setattr(session_module, "monotonic", lambda: next(monotonic_times))
    facade = _Facade()
    controller = _controller(facade, database_type="mariadb")
    controller.renew()
    facade.busy_attempts = 2
    with pytest.raises(sqlite3.OperationalError, match="locked"):
        controller.renew()
    message = caplog.records[-1].getMessage()
    assert "Last successful renewal: 1970-01-01T00:00:02+00:00" in message
    assert "Renewal requested at 1970-01-01T00:01:40+00:00" in message
    assert "waited 0.500s" in message
    assert "renewal attempt took 3.000s" in message
    assert "1970-01-01T00:00:01.000001+00:00" in message


def test_background_failure_is_logged_before_local_work_returns(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    reported = Event()
    original_log = session_module.logger.error

    def observed_log(message: str, *args: object, **kwargs: object) -> None:
        original_log(message, *args)
        reported.set()

    monkeypatch.setattr(session_module.logger, "error", observed_log)

    class _FailsAfterAdmission(_Facade):
        def renew_ingest(
            self, session: VNextIngestSession, lease_duration_microseconds: int
        ) -> VNextIngestSession:
            if self.calls:
                raise RuntimeError("renewal after admission failed")
            return super().renew_ingest(session, lease_duration_microseconds)

    facade = _FailsAfterAdmission()
    controller = _controller(facade, database_type="mariadb")
    with pytest.raises(RuntimeError, match="heartbeat failed"):
        with IngestLeaseHeartbeat(controller, interval_seconds=0.01):
            # This local operation has not returned to orchestration yet.
            assert reported.wait(5)
            assert len(caplog.records) == 1
            assert "renewal after admission failed" in caplog.records[0].getMessage()
            controller.raise_if_failed()
    assert len(caplog.records) == 1
