"""Retry hints retain all failed galleries without unbounded process state."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from database_audit_fixtures import IsolatedDatabaseAudit
from h2hdb import ArtifactFailureContext, VNextSourceChangedError
from test_resident import _Heartbeat, _resident, _synchronized

import h2hdb_ingest._source_retry as retry_module
import h2hdb_ingest.resident as resident_module
from h2hdb_ingest._source_retry import SourceReobservation
from h2hdb_ingest.service import VNextIngestSynchronizationResult
from h2hdb_ingest.session import IngestSessionController


@pytest.mark.parametrize("count", [127, 128, 129])
def test_repeated_failures_keep_every_target_or_request_full_refresh(
    count: int,
) -> None:
    hints = SourceReobservation()
    failed = tuple((f"gallery-{index}",) for index in range(count))
    for _ in range(3):
        for locator in failed:
            hints.record(locator)
        assert len(hints.locators) <= 128
        if count <= 128:
            assert hints.reuse_sealed_observations
            assert set(hints.locators) == set(failed)
        else:
            assert not hints.reuse_sealed_observations
            assert len(hints.locators) == 0
    hints.clear()
    assert len(hints.locators) == 0
    assert hints.reuse_sealed_observations
    hints.record(("next-publication",))
    assert set(hints.locators) == {("next-publication",)}


def test_resident_accumulates_failures_until_publication_then_clears(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[tuple[tuple[str, ...], ...], bool]] = []
    failures = iter(("first", "second", "first", None, None))
    context: ArtifactFailureContext | None = None

    class Service:
        def synchronize_once(
            self,
            session: IngestSessionController,
            *,
            should_stop: Callable[[], bool] | None = None,
            reobserve_gallery_locators: tuple[tuple[str, ...], ...] = (),
            reuse_sealed_observations: bool = True,
        ) -> VNextIngestSynchronizationResult:
            del session, should_stop
            nonlocal context
            calls.append((reobserve_gallery_locators, reuse_sealed_observations))
            failed = next(failures)
            if failed is not None:
                context = ArtifactFailureContext(1001, ("root",), (failed,))
                raise VNextSourceChangedError("sealed source changed")
            return _synchronized()

    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _Heartbeat)
    monkeypatch.setattr(resident_module, "IngestDatabaseAudit", IsolatedDatabaseAudit)
    monkeypatch.setattr(
        retry_module, "get_artifact_failure_context", lambda _error: context
    )
    resident = _resident([], service=Service())
    for expected in (False, False, False, True, True):
        assert resident.process_available(periodic_scan=True) is expected
    assert calls == [
        ((), True),
        ((("first",),), True),
        ((("first",), ("second",)), True),
        ((("first",), ("second",)), True),
        ((), True),
    ]
