from __future__ import annotations

from threading import Event
from typing import cast

import pytest
from h2hdb import (
    SchemaEpochReport,
    VNextCurrentOnlyMaintenanceOutcome,
    VNextDatabaseAdminFacade,
    VNextIngestCompletionReceipt,
    VNextIngestFacade,
    VNextIngestSession,
    VNextSourceManifestMismatchError,
)

import h2hdb_ingest.resident as resident_module
from h2hdb_ingest import ResidentConfig
from h2hdb_ingest.maintenance import CurrentProjectionMaintenanceOutcome
from h2hdb_ingest.resident import ResidentIngestor
from h2hdb_ingest.session import IngestSessionController


def _session() -> VNextIngestSession:
    return VNextIngestSession(
        gate_owner_token=b"g" * 16,
        gate_generation=1,
        gate_slot=0,
        gate_lease_expires_at=10_000_000,
        ingest_generation=2,
        ingest_owner_token=b"i" * 16,
        ingest_lease_expires_at=10_000_000,
        download_generation=None,
        handoff_owner_token=None,
        handoff_kind=None,
        consumed_at=None,
    )


class _Facade:
    def __init__(
        self,
        events: list[object],
        *,
        available: bool = True,
        maintenance_results: tuple[VNextCurrentOnlyMaintenanceOutcome, ...] = (
            VNextCurrentOnlyMaintenanceOutcome.DONE,
        ),
    ) -> None:
        self._events = events
        self._available = available
        self._maintenance_results = iter(maintenance_results)

    def try_claim_ingest(
        self,
        periodic: bool,
        lease_duration_microseconds: int,
    ) -> VNextIngestSession | None:
        self._events.append(("claim", periodic, lease_duration_microseconds))
        return _session() if self._available else None

    def complete_ingest(
        self,
        session: VNextIngestSession,
    ) -> VNextIngestCompletionReceipt:
        self._events.append(("complete", session.ingest_generation))
        return VNextIngestCompletionReceipt(
            session.ingest_generation,
            session.ingest_owner_token,
            10,
            session.download_generation,
            False,
        )

    def drain_current_only_maintenance(
        self,
        lease_duration_microseconds: int,
    ) -> VNextCurrentOnlyMaintenanceOutcome:
        self._events.append(("current-only", lease_duration_microseconds))
        return next(
            self._maintenance_results,
            VNextCurrentOnlyMaintenanceOutcome.DONE,
        )


class _Admin:
    def __init__(self, events: list[object]) -> None:
        self._events = events

    def check(self) -> SchemaEpochReport:
        self._events.append("check")
        return cast(SchemaEpochReport, object())


class _ProjectionMaintenance:
    def __init__(
        self,
        results: tuple[CurrentProjectionMaintenanceOutcome, ...] = (
            CurrentProjectionMaintenanceOutcome.DONE,
        ),
    ) -> None:
        self._results = iter(results)
        self.calls = 0

    def maintain_cleanup(self) -> CurrentProjectionMaintenanceOutcome:
        self.calls += 1
        return next(self._results, CurrentProjectionMaintenanceOutcome.DONE)


class _Service:
    def __init__(self, events: list[object]) -> None:
        self._events = events

    def synchronize_once(self, session: IngestSessionController) -> object:
        self._events.append("synchronize")
        return session.call(lambda _facade, receipt: receipt.ingest_generation)


class _ManifestMismatchService:
    def __init__(self, events: list[object]) -> None:
        self._events = events

    def synchronize_once(self, session: IngestSessionController) -> object:
        del session
        self._events.append("synchronize")
        raise VNextSourceManifestMismatchError("source changed")


class _Heartbeat:
    def __init__(
        self,
        controller: IngestSessionController,
        *,
        interval_seconds: float,
    ) -> None:
        del controller, interval_seconds

    def __enter__(self) -> _Heartbeat:
        return self

    def __exit__(self, *args: object) -> None:
        del args

    def raise_if_failed(self) -> None:
        return None


def _resident(
    events: list[object],
    *,
    available: bool = True,
    maintenance_results: tuple[VNextCurrentOnlyMaintenanceOutcome, ...] = (
        VNextCurrentOnlyMaintenanceOutcome.DONE,
    ),
    projection_maintenance: _ProjectionMaintenance | None = None,
    service: _Service | _ManifestMismatchService | None = None,
) -> ResidentIngestor:
    facade = _Facade(
        events,
        available=available,
        maintenance_results=maintenance_results,
    )
    return ResidentIngestor(
        service=service or _Service(events),
        facade=cast(VNextIngestFacade, facade),
        database_admin=cast(VNextDatabaseAdminFacade, _Admin(events)),
        current_projection_maintenance=(
            projection_maintenance or _ProjectionMaintenance()
        ),
        config=ResidentConfig(
            periodic_scan_seconds=60,
            poll_seconds=1,
            lease_seconds=10,
            heartbeat_seconds=5,
        ),
        database_type="sqlite",
        event_logger=lambda message: events.append(("log", message)),
    )


def test_startup_only_checks_existing_epoch_and_processes_one_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []
    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _Heartbeat)
    resident = _resident(events)

    resident.initialize()
    assert resident.process_available(periodic_scan=True)

    assert events[:6] == [
        "check",
        ("current-only", 10_000_000),
        ("current-only", 10_000_000),
        ("claim", True, 10_000_000),
        "synchronize",
        ("log", "vNext ingest synchronization completed: 2"),
    ]
    assert events[6] == ("complete", 2)
    assert events[7] == ("current-only", 10_000_000)
    assert events[8] == (
        "log",
        "vNext ingest session completed: generation=2 replayed=False",
    )


def test_ordinary_claim_contention_is_not_an_error() -> None:
    events: list[object] = []
    resident = _resident(events, available=False)

    assert not resident.process_available(periodic_scan=False)
    assert events == [
        ("current-only", 10_000_000),
        ("claim", False, 10_000_000),
    ]


def test_maintenance_progress_skips_ingest_claim() -> None:
    events: list[object] = []
    resident = _resident(
        events,
        maintenance_results=(VNextCurrentOnlyMaintenanceOutcome.PROGRESSED,),
    )

    assert resident.process_available(periodic_scan=True)
    assert events == [("current-only", 10_000_000)]


def test_projection_maintenance_progress_skips_database_and_ingest() -> None:
    events: list[object] = []
    projection_maintenance = _ProjectionMaintenance(
        (CurrentProjectionMaintenanceOutcome.PROGRESSED,)
    )
    resident = _resident(
        events,
        projection_maintenance=projection_maintenance,
    )

    assert resident.process_available(periodic_scan=True)
    assert events == []


def test_blocked_projection_maintenance_uses_ordinary_claim_poll() -> None:
    events: list[object] = []
    projection_maintenance = _ProjectionMaintenance(
        (CurrentProjectionMaintenanceOutcome.BLOCKED,)
    )
    resident = _resident(
        events,
        available=False,
        projection_maintenance=projection_maintenance,
    )

    assert not resident.process_available(periodic_scan=False)
    assert events == [
        ("current-only", 10_000_000),
        ("claim", False, 10_000_000),
    ]


def test_failed_preflight_completes_claim_without_running_service() -> None:
    events: list[object] = []
    resident = _resident(events)

    with pytest.raises(RuntimeError, match="not fresh"):
        resident.process_available(
            periodic_scan=True,
            preflight=lambda: (_ for _ in ()).throw(RuntimeError("not fresh")),
        )

    assert events == [
        ("current-only", 10_000_000),
        ("claim", True, 10_000_000),
        ("complete", 2),
        ("current-only", 10_000_000),
    ]


def test_source_manifest_mismatch_completes_claim_after_heartbeat_stops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []

    class _OrderedHeartbeat(_Heartbeat):
        def __enter__(self) -> _OrderedHeartbeat:
            events.append("heartbeat-start")
            return self

        def __exit__(self, *args: object) -> None:
            del args
            events.append("heartbeat-stop")

    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _OrderedHeartbeat)
    resident = _resident(events, service=_ManifestMismatchService(events))

    with pytest.raises(VNextSourceManifestMismatchError, match="source changed"):
        resident.process_available(periodic_scan=True)

    assert events == [
        ("current-only", 10_000_000),
        ("claim", True, 10_000_000),
        "heartbeat-start",
        "synchronize",
        "heartbeat-stop",
        ("complete", 2),
        ("current-only", 10_000_000),
    ]


def test_startup_contention_is_retried_once_on_the_next_idle_poll() -> None:
    events: list[object] = []
    resident = _resident(
        events,
        available=False,
        maintenance_results=(
            VNextCurrentOnlyMaintenanceOutcome.CONTENDED,
            VNextCurrentOnlyMaintenanceOutcome.DONE,
        ),
    )

    resident.initialize()
    assert not resident.process_available(periodic_scan=False)

    assert events == [
        "check",
        ("current-only", 10_000_000),
        ("current-only", 10_000_000),
        ("claim", False, 10_000_000),
    ]


def test_maintenance_failure_does_not_undo_completed_ingest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []

    class _FailAfterCompletionFacade(_Facade):
        def __init__(self) -> None:
            super().__init__(events)
            self._attempt = 0

        def drain_current_only_maintenance(
            self, lease_duration_microseconds: int
        ) -> VNextCurrentOnlyMaintenanceOutcome:
            self._attempt += 1
            if self._attempt == 2:
                raise RuntimeError("maintenance unavailable")
            return super().drain_current_only_maintenance(lease_duration_microseconds)

    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _Heartbeat)
    resident = ResidentIngestor(
        service=_Service(events),
        facade=cast(VNextIngestFacade, _FailAfterCompletionFacade()),
        database_admin=cast(VNextDatabaseAdminFacade, _Admin(events)),
        current_projection_maintenance=_ProjectionMaintenance(),
        config=ResidentConfig(
            periodic_scan_seconds=60,
            poll_seconds=1,
            lease_seconds=10,
            heartbeat_seconds=5,
        ),
        database_type="sqlite",
        event_logger=lambda message: events.append(("log", message)),
    )

    assert resident.process_available(periodic_scan=True)
    assert ("complete", 2) in events
    assert events[-1] == (
        "log",
        "vNext ingest session completed: generation=2 replayed=False",
    )


def test_run_forever_retries_progress_immediately_without_resetting_periodic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []
    resident = _resident(
        events,
        available=False,
        maintenance_results=(
            VNextCurrentOnlyMaintenanceOutcome.PROGRESSED,
            VNextCurrentOnlyMaintenanceOutcome.DONE,
        ),
    )

    class _Stop:
        def __init__(self) -> None:
            self.waited = False

        def is_set(self) -> bool:
            return self.waited

        def wait(self, timeout: float) -> bool:
            events.append(("wait", timeout))
            self.waited = True
            return True

    monkeypatch.setattr(resident_module, "monotonic", lambda: 100.0)
    stop = _Stop()
    resident.run_forever(stop=cast(Event, stop))

    assert events == [
        ("current-only", 10_000_000),
        ("current-only", 10_000_000),
        ("claim", True, 10_000_000),
        ("wait", 1.0),
    ]


def test_run_forever_immediately_drains_projection_progress_then_waits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []
    projection_maintenance = _ProjectionMaintenance(
        (
            CurrentProjectionMaintenanceOutcome.PROGRESSED,
            CurrentProjectionMaintenanceOutcome.DONE,
        )
    )
    resident = _resident(
        events,
        available=False,
        projection_maintenance=projection_maintenance,
    )

    class _Stop:
        def __init__(self) -> None:
            self.waited = False

        def is_set(self) -> bool:
            return self.waited

        def wait(self, timeout: float) -> bool:
            events.append(("wait", timeout))
            self.waited = True
            return True

    monkeypatch.setattr(resident_module, "monotonic", lambda: 100.0)
    stop = _Stop()
    resident.run_forever(stop=cast(Event, stop))

    assert projection_maintenance.calls == 2
    assert events == [
        ("current-only", 10_000_000),
        ("claim", True, 10_000_000),
        ("wait", 1.0),
    ]
