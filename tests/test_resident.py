from __future__ import annotations

from typing import cast

import pytest
from h2hdb import (
    SchemaEpochReport,
    VNextDatabaseAdminFacade,
    VNextIngestCompletionReceipt,
    VNextIngestFacade,
    VNextIngestSession,
)

import h2hdb_ingest.resident as resident_module
from h2hdb_ingest import ResidentConfig
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
    def __init__(self, events: list[object], *, available: bool = True) -> None:
        self._events = events
        self._available = available

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


class _Admin:
    def __init__(self, events: list[object]) -> None:
        self._events = events

    def check(self) -> SchemaEpochReport:
        self._events.append("check")
        return cast(SchemaEpochReport, object())


class _Service:
    def __init__(self, events: list[object]) -> None:
        self._events = events

    def synchronize_once(self, session: IngestSessionController) -> object:
        self._events.append("synchronize")
        return session.call(lambda _facade, receipt: receipt.ingest_generation)


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
) -> ResidentIngestor:
    facade = _Facade(events, available=available)
    return ResidentIngestor(
        service=_Service(events),
        facade=cast(VNextIngestFacade, facade),
        database_admin=cast(VNextDatabaseAdminFacade, _Admin(events)),
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

    assert events[:4] == [
        "check",
        ("claim", True, 10_000_000),
        "synchronize",
        ("log", "vNext ingest synchronization completed: 2"),
    ]
    assert events[4] == ("complete", 2)
    assert events[5] == (
        "log",
        "vNext ingest session completed: generation=2 replayed=False",
    )


def test_ordinary_claim_contention_is_not_an_error() -> None:
    events: list[object] = []
    resident = _resident(events, available=False)

    assert not resident.process_available(periodic_scan=False)
    assert events == [("claim", False, 10_000_000)]


def test_failed_preflight_completes_claim_without_running_service() -> None:
    events: list[object] = []
    resident = _resident(events)

    with pytest.raises(RuntimeError, match="not fresh"):
        resident.process_available(
            periodic_scan=True,
            preflight=lambda: (_ for _ in ()).throw(RuntimeError("not fresh")),
        )

    assert events == [
        ("claim", True, 10_000_000),
        ("complete", 2),
    ]
