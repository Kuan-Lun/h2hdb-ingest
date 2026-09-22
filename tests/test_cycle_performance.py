from __future__ import annotations

from collections.abc import Callable
from typing import cast

import pytest
from database_audit_fixtures import IsolatedDatabaseAudit
from h2hdb import VNextCurrentOnlyMaintenanceOutcome, VNextIngestSession
from test_resident import (
    _Facade,
    _Heartbeat,
    _LibraryMaintenance,
    _resident,
)

import h2hdb_ingest.resident as resident_module
from h2hdb_ingest.cycle_performance import CyclePerformance
from h2hdb_ingest.maintenance import LibraryMaintenanceOutcome
from h2hdb_ingest.service import VNextIngestSynchronizationResult
from h2hdb_ingest.session import IngestSessionController


class _Clock:
    value = 10.0

    def __call__(self) -> float:
        return self.value


def _fields(line: str) -> dict[str, str]:
    assert line.startswith("ingest_cycle_performance ")
    return dict(field.split("=", 1) for field in line.split()[1:])


def test_process_local_cycle_reports_distinct_milestones_without_double_counting() -> (
    None
):
    clock = _Clock()
    lines: list[str] = []
    observer = CyclePerformance(lines.append, clock=clock)
    observer.claimed(80)
    first = _fields(lines[-1])
    assert first["previous_publication_generation"] == "unknown"
    assert first["after_publication_seconds"] == "unknown"
    clock.value = 20
    observer.published(80)
    clock.value = 25
    observer.maintenance_done("library")
    for _ in range(100):
        observer.maintenance_done("library")
    clock.value = 30
    observer.maintenance_done("catalog")
    clock.value = 40
    observer.claimed(81)
    records = list(map(_fields, lines))
    assert [item["event"] for item in records] == [
        "ingest_claimed",
        "publication_completed",
        "component_done",
        "component_done",
        "ingest_claimed",
    ]
    assert records[2]["after_publication_seconds"] == "5.000000"
    assert records[2]["scope"] == "component_only"
    assert records[3]["after_publication_seconds"] == "10.000000"
    assert records[4]["after_publication_seconds"] == "20.000000"
    assert records[4]["previous_publication_generation"] == "80"
    assert records[4]["generation"] == "81"
    assert records[4]["library_done_observed"] == "true"
    assert records[4]["catalog_done_observed"] == "true"
    observer.maintenance_done("catalog")
    assert len(lines) == 5
    observer.claimed(82)
    assert _fields(lines[-1])["after_publication_seconds"] == "unknown"


def test_partial_component_done_never_claims_total_cleanup() -> None:
    lines: list[str] = []
    observer = CyclePerformance(lines.append, clock=_Clock())
    observer.published(7)
    observer.maintenance_done("library")
    observer.claimed(8)
    fields = _fields(lines[-1])
    assert fields["library_done_observed"] == "true"
    assert fields["catalog_done_observed"] == "false"
    assert "cleanup_complete" not in " ".join(lines)


def test_restart_or_explicit_reset_does_not_invent_previous_duration() -> None:
    lines: list[str] = []
    observer = CyclePerformance(lines.append, clock=_Clock())
    observer.published(7)
    observer.reset()
    observer.maintenance_done("catalog")
    observer.claimed(8)
    restarted = CyclePerformance(lines.append, clock=_Clock())
    restarted.claimed(9)
    for line in lines[-2:]:
        fields = _fields(line)
        assert fields["after_publication_seconds"] == "unknown"
        assert fields["reason"] == "no_process_local_publication"


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), -1.0, True])
def test_invalid_diagnostic_clock_is_unknown_not_zero(invalid: float) -> None:
    lines: list[str] = []
    observer = CyclePerformance(lines.append, clock=lambda: invalid)
    observer.published(7)
    observer.maintenance_done("library")
    observer.claimed(8)
    assert all(_fields(line)["monotonic_seconds"] == "unknown" for line in lines)
    assert _fields(lines[-1])["after_publication_seconds"] == "unknown"


def test_clock_and_sink_failures_do_not_change_work_or_retain_previous_cycle() -> None:
    def failure(*_args: object) -> float:
        raise RuntimeError("diagnostic failed")

    def sink_failure(_message: str) -> None:
        raise RuntimeError("diagnostic failed")

    observer = CyclePerformance(sink_failure, clock=failure)
    observer.published(7)
    observer.maintenance_done("library")
    observer.claimed(8)
    lines: list[str] = []
    observer._emit = lines.append
    observer.claimed(9)
    assert _fields(lines[-1])["previous_publication_generation"] == "unknown"


def test_backwards_clock_does_not_emit_zero_elapsed() -> None:
    clock = _Clock()
    lines: list[str] = []
    observer = CyclePerformance(lines.append, clock=clock)
    observer.published(7)
    clock.value = 9
    observer.claimed(8)
    assert _fields(lines[-1])["after_publication_seconds"] == "unknown"


@pytest.fixture
def resident_observer(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[CyclePerformance, list[str]]:
    lines: list[str] = []
    observer = CyclePerformance(lines.append, clock=_Clock())
    monkeypatch.setattr(resident_module, "CyclePerformance", lambda: observer)
    monkeypatch.setattr(resident_module, "IngestDatabaseAudit", IsolatedDatabaseAudit)
    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _Heartbeat)
    return observer, lines


def test_resident_logs_publication_before_postflight_and_next_actual_claim(
    resident_observer: tuple[CyclePerformance, list[str]],
) -> None:
    _observer, lines = resident_observer
    events: list[object] = []
    resident = _resident(events)

    def postflight() -> None:
        assert _fields(lines[-1])["event"] == "publication_completed"
        assert not any(_fields(line)["event"] == "component_done" for line in lines)

    assert resident.process_available(periodic_scan=True, postflight=postflight)
    assert [_fields(line)["event"] for line in lines] == [
        "ingest_claimed",
        "publication_completed",
        "component_done",
        "component_done",
    ]
    resident._claim_after_maintenance(periodic_scan=True, should_stop=None)
    assert _fields(lines[-1])["previous_publication_generation"] == "2"
    assert _fields(lines[-1])["library_done_observed"] == "true"
    assert _fields(lines[-1])["catalog_done_observed"] == "true"


def test_failed_postflight_still_reports_already_completed_publication(
    resident_observer: tuple[CyclePerformance, list[str]],
) -> None:
    _observer, lines = resident_observer
    resident = _resident([])

    def postflight() -> None:
        assert _fields(lines[-1])["event"] == "publication_completed"
        raise RuntimeError("postflight failure")

    with pytest.raises(RuntimeError, match="postflight failure"):
        resident.process_available(periodic_scan=True, postflight=postflight)
    assert [_fields(line)["event"] for line in lines] == [
        "ingest_claimed",
        "publication_completed",
        "component_done",
        "component_done",
    ]


class _FaultService:
    def synchronize_once(
        self,
        session: IngestSessionController,
        *,
        should_stop: Callable[[], bool] | None = None,
        reobserve_gallery_locators: tuple[tuple[str, ...], ...] = (),
        reuse_sealed_observations: bool = True,
    ) -> VNextIngestSynchronizationResult:
        del session, should_stop
        raise RuntimeError("not published")


def test_failed_service_or_unavailable_claim_does_not_forge_milestones(
    resident_observer: tuple[CyclePerformance, list[str]],
) -> None:
    _observer, lines = resident_observer
    idle = _resident([], available=False)
    for _ in range(10):
        assert not idle.process_available(periodic_scan=True)
    assert lines == []
    resident = _resident([], service=_FaultService())
    with pytest.raises(RuntimeError, match="not published"):
        resident.process_available(periodic_scan=True)
    assert [_fields(line)["event"] for line in lines] == ["ingest_claimed"]


def test_only_exact_done_responses_are_component_completion(
    resident_observer: tuple[CyclePerformance, list[str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observer, lines = resident_observer
    library = _LibraryMaintenance(
        results=(
            LibraryMaintenanceOutcome.PROGRESSED,
            LibraryMaintenanceOutcome.BLOCKED,
            LibraryMaintenanceOutcome.DONE,
            LibraryMaintenanceOutcome.DONE,
        )
    )
    facade = _Facade(
        [],
        maintenance_results=(
            VNextCurrentOnlyMaintenanceOutcome.PROGRESSED,
            VNextCurrentOnlyMaintenanceOutcome.BLOCKED,
            VNextCurrentOnlyMaintenanceOutcome.CONTENDED,
            VNextCurrentOnlyMaintenanceOutcome.DONE,
            VNextCurrentOnlyMaintenanceOutcome.DONE,
        ),
    )
    resident = _resident([], facade=facade, library_maintenance=library)
    observer.published(7)
    for _ in range(2):
        resident._try_library_maintenance()
    for _ in range(3):
        resident._try_current_only_maintenance()
    assert len(lines) == 1
    with monkeypatch.context() as patcher:

        def failed() -> LibraryMaintenanceOutcome:
            raise RuntimeError("transient")

        patcher.setattr(library, "maintain_cleanup", failed)
        assert resident._try_library_maintenance() is None
    assert len(lines) == 1
    for _ in range(2):
        resident._try_library_maintenance()
        resident._try_current_only_maintenance()
    assert [_fields(line).get("component") for line in lines] == [
        None,
        "library",
        "catalog",
    ]


def test_foreign_claim_keeps_original_contract_error_and_pending_history(
    resident_observer: tuple[CyclePerformance, list[str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observer, lines = resident_observer
    facade = _Facade([])

    def foreign_claim(_periodic: bool, _duration: int) -> VNextIngestSession:
        return cast(VNextIngestSession, object())

    monkeypatch.setattr(facade, "try_claim_ingest", foreign_claim)
    resident = _resident([], facade=facade)
    observer.published(7)
    with pytest.raises(TypeError, match="session must be VNextIngestSession"):
        resident.process_available(periodic_scan=True)
    assert not any(_fields(line)["event"] == "ingest_claimed" for line in lines)
    observer.claimed(8)
    assert _fields(lines[-1])["previous_publication_generation"] == "7"


def test_completed_publication_observation_survives_later_heartbeat_failure(
    resident_observer: tuple[CyclePerformance, list[str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _observer, lines = resident_observer

    class FailedHeartbeat(_Heartbeat):
        def raise_if_failed(self) -> None:
            raise RuntimeError("heartbeat failed after publication")

    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", FailedHeartbeat)
    resident = _resident([])
    with pytest.raises(RuntimeError, match="heartbeat failed after publication"):
        resident.process_available(periodic_scan=True)
    assert [_fields(line)["event"] for line in lines] == [
        "ingest_claimed",
        "publication_completed",
    ]
