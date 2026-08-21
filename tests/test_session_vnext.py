from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from time import sleep
from typing import cast

from h2hdb import VNextIngestFacade, VNextIngestSession

from h2hdb_ingest.session import IngestSessionController


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


def test_renewal_retries_sqlite_busy_and_carries_forward_latest_receipt() -> None:
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
