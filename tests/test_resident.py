import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from time import sleep
from typing import cast

import pytest
from h2hdb import (
    DatabaseAdmin,
    DatabaseMaintenanceResult,
    DownloadCoordinator,
    GalleryIngestPhase,
    GalleryIngestTurn,
    SchemaCompatibility,
)

from h2hdb_ingest import ResidentConfig, ResidentIngestor, SyncOutcome
from h2hdb_ingest.resident import IngestLeaseHeartbeat
from h2hdb_ingest.staged_service import IngestSynchronizer


def _outcome(*, new: int = 0, changed: int = 0, removed: int = 0) -> SyncOutcome:
    return SyncOutcome(
        revision=1,
        scanned=1,
        published=1,
        new=new,
        changed=changed,
        removed=removed,
        duplicate_losers=0,
        cbz_created=0,
        cbz_rebuilt=0,
    )


class _Service:
    def __init__(
        self,
        events: list[str],
        outcomes: list[SyncOutcome] | None = None,
    ) -> None:
        self._events = events
        self._outcomes = iter(outcomes or [_outcome()])

    def synchronize_once(self, turn: GalleryIngestTurn) -> SyncOutcome:
        del turn
        self._events.append("synchronize")
        return next(self._outcomes)


class _Coordinator:
    def __init__(self, events: list[str]) -> None:
        self._events = events
        self.turn = GalleryIngestTurn(
            generation=1,
            owner_token="owner",
            lease_expires_at=10_000,
            claimed_from_phase=GalleryIngestPhase.ingest_requested,
        )
        self.allow_expired_sqlite_lease = False

    def claim_gallery_ingest(
        self,
        *,
        lease_seconds: int,
        periodic_scan: bool,
    ) -> GalleryIngestTurn:
        del lease_seconds, periodic_scan
        self._events.append("claim")
        return self.turn

    def renew_gallery_ingest(
        self,
        turn: GalleryIngestTurn,
        *,
        lease_seconds: int,
        sqlite_busy_timeout_ms: int | None = None,
    ) -> int:
        del turn, lease_seconds, sqlite_busy_timeout_ms
        self._events.append("renew")
        return 10_000

    def complete_gallery_ingest(
        self,
        turn: GalleryIngestTurn,
        *,
        allow_expired_sqlite_lease: bool = False,
    ) -> bool:
        del turn
        self.allow_expired_sqlite_lease = allow_expired_sqlite_lease
        self._events.append(
            "complete-expired" if allow_expired_sqlite_lease else "complete"
        )
        return True


class _Admin:
    def __init__(self, events: list[str], *, maintenance_fails: bool = False) -> None:
        self._events = events
        self._maintenance_fails = maintenance_fails

    def check_compatibility(self) -> SchemaCompatibility:
        self._events.append("check")
        return SchemaCompatibility(1, 1, 1)

    def run_scheduled_database_maintenance(self) -> DatabaseMaintenanceResult:
        self._events.append("maintenance")
        if self._maintenance_fails:
            raise RuntimeError("injected maintenance failure")
        return DatabaseMaintenanceResult(False, (), 0)


def _resident(
    events: list[str],
    *,
    maintenance_fails: bool = False,
    database_type: str = "mariadb",
    outcomes: list[SyncOutcome] | None = None,
) -> ResidentIngestor:
    return ResidentIngestor(
        service=cast(IngestSynchronizer, _Service(events, outcomes)),
        coordinator=cast(DownloadCoordinator, _Coordinator(events)),
        database_admin=cast(
            DatabaseAdmin,
            _Admin(events, maintenance_fails=maintenance_fails),
        ),
        config=ResidentConfig(
            periodic_scan_seconds=60,
            poll_seconds=1,
            lease_seconds=10,
            heartbeat_seconds=5,
        ),
        database_type=database_type,
    )


def test_startup_checks_compatibility_and_maintenance_precedes_completion() -> None:
    events: list[str] = []
    resident = _resident(events)

    compatibility = resident.initialize()
    processed = resident.process_available(periodic_scan=True)

    assert compatibility == SchemaCompatibility(1, 1, 1)
    assert processed
    assert events == ["check", "claim", "synchronize", "maintenance", "complete"]


def test_failed_maintenance_does_not_acknowledge_ingest_turn() -> None:
    events: list[str] = []
    resident = _resident(events, maintenance_fails=True)

    with pytest.raises(RuntimeError, match="injected maintenance failure"):
        resident.process_available(periodic_scan=False)

    assert events == ["claim", "synchronize", "maintenance"]


def test_failed_preflight_releases_claim_before_synchronization() -> None:
    events: list[str] = []
    resident = _resident(events)

    def fail_preflight() -> None:
        events.append("preflight")
        raise RuntimeError("not fresh")

    with pytest.raises(RuntimeError, match="not fresh"):
        resident.process_available(
            periodic_scan=True,
            preflight=fail_preflight,
        )

    assert events == ["claim", "preflight", "complete"]


def test_same_lease_rescans_until_new_and_changed_work_converges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    class _Heartbeat:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs

        def __enter__(self) -> _Heartbeat:
            events.append("heartbeat-enter")
            return self

        def __exit__(self, *args: object) -> None:
            del args
            events.append("heartbeat-exit")

        def raise_if_failed(self) -> None:
            events.append("heartbeat-check")

    monkeypatch.setattr("h2hdb_ingest.resident.IngestLeaseHeartbeat", _Heartbeat)
    resident = _resident(
        events,
        outcomes=[
            _outcome(new=1),
            _outcome(changed=1),
            _outcome(removed=1),
        ],
    )

    assert resident.process_available(periodic_scan=False)

    assert events == [
        "claim",
        "heartbeat-enter",
        "synchronize",
        "heartbeat-check",
        "synchronize",
        "heartbeat-check",
        "synchronize",
        "heartbeat-check",
        "maintenance",
        "heartbeat-check",
        "heartbeat-exit",
        "complete",
    ]


def test_sqlite_stops_heartbeat_and_renews_before_exclusive_maintenance() -> None:
    events: list[str] = []
    resident = _resident(events, database_type="sqlite")

    assert resident.process_available(periodic_scan=True)

    assert events == [
        "claim",
        "synchronize",
        "renew",
        "maintenance",
        "complete-expired",
    ]


def test_sqlite_heartbeat_retries_transient_busy_until_renewed() -> None:
    events: list[str] = []

    class _BusyCoordinator(_Coordinator):
        def __init__(self) -> None:
            super().__init__(events)
            self.attempts = 0
            self.busy_timeouts: list[int | None] = []

        def renew_gallery_ingest(
            self,
            turn: GalleryIngestTurn,
            *,
            lease_seconds: int,
            sqlite_busy_timeout_ms: int | None = None,
        ) -> int:
            del turn, lease_seconds
            self.attempts += 1
            self.busy_timeouts.append(sqlite_busy_timeout_ms)
            if self.attempts <= 3:
                raise sqlite3.OperationalError("database is locked")
            return 10_000

    coordinator = _BusyCoordinator()
    heartbeat = IngestLeaseHeartbeat(
        cast(DownloadCoordinator, coordinator),
        coordinator.turn,
        lease_seconds=10,
        interval_seconds=2,
        database_type="sqlite",
    )

    assert heartbeat.renew_now() == 10_000
    assert coordinator.attempts == 4
    assert all(
        timeout is not None and timeout > 0 for timeout in coordinator.busy_timeouts
    )


def test_heartbeat_serializes_manual_and_background_renewals() -> None:
    events: list[str] = []

    class _ConcurrentCoordinator(_Coordinator):
        def __init__(self) -> None:
            super().__init__(events)
            self._active_lock = Lock()
            self.active = 0
            self.maximum_active = 0

        def renew_gallery_ingest(
            self,
            turn: GalleryIngestTurn,
            *,
            lease_seconds: int,
            sqlite_busy_timeout_ms: int | None = None,
        ) -> int:
            del turn, lease_seconds, sqlite_busy_timeout_ms
            with self._active_lock:
                self.active += 1
                self.maximum_active = max(self.maximum_active, self.active)
            sleep(0.02)
            with self._active_lock:
                self.active -= 1
            return 10_000

    coordinator = _ConcurrentCoordinator()
    heartbeat = IngestLeaseHeartbeat(
        cast(DownloadCoordinator, coordinator),
        coordinator.turn,
        lease_seconds=10,
        interval_seconds=2,
        database_type="sqlite",
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _index: heartbeat.renew_now(), range(2)))

    assert results == [10_000, 10_000]
    assert coordinator.maximum_active == 1
