from __future__ import annotations

import math
from dataclasses import replace

import pytest
from pydantic import ValidationError

from h2hdb_ingest.config import ResidentConfig
from h2hdb_ingest.source_schedule import SourceScanSchedule


def _schedule() -> SourceScanSchedule:
    return SourceScanSchedule(quiet_seconds=300, max_wait_seconds=1800, now=0)


def _clean_schedule() -> SourceScanSchedule:
    schedule = _schedule()
    ticket = schedule.start_scan(now=0)
    schedule.finish_scan(ticket, now=10, succeeded=True)
    assert schedule.next_scan_at() is None
    return schedule


def test_startup_and_restart_reconcile_immediately() -> None:
    schedule = _schedule()
    schedule.note_change(now=30)
    assert schedule.next_scan_at() == 0
    ticket = schedule.start_scan(now=30)
    assert schedule.next_scan_at() is None
    schedule.finish_scan(ticket, now=40, succeeded=True)
    assert schedule.next_scan_at() is None
    assert _schedule().next_scan_at() == 0


def test_quiet_period_moves_without_extending_first_change_cap() -> None:
    schedule = _clean_schedule()
    schedule.note_change(now=100)
    assert schedule.next_scan_at() == 400
    schedule.note_change(now=300)
    assert schedule.next_scan_at() == 600
    for now in range(500, 1900, 200):
        schedule.note_change(now=now)
    assert schedule.next_scan_at() == 1900
    ticket = schedule.start_scan(now=1900)
    schedule.finish_scan(ticket, now=1950, succeeded=True)
    assert schedule.next_scan_at() is None


@pytest.mark.parametrize(
    ("changed_at", "completed_at", "next_at"),
    [(100, 500, 400), (450, 500, 750), (100, 4000, 400)],
)
def test_changes_during_scan_keep_their_quiet_period(
    changed_at: int, completed_at: int, next_at: int
) -> None:
    schedule = _schedule()
    first = schedule.start_scan(now=0)
    schedule.note_change(now=changed_at)
    assert schedule.next_scan_at() is None
    schedule.finish_scan(first, now=completed_at, succeeded=True)
    assert schedule.next_scan_at() == next_at
    second = schedule.start_scan(now=max(completed_at, next_at))
    assert second.generation > first.generation
    schedule.finish_scan(second, now=max(completed_at, next_at), succeeded=True)
    assert schedule.next_scan_at() is None


def test_changes_during_scan_cap_wait_from_completion() -> None:
    schedule = _schedule()
    ticket = schedule.start_scan(now=0)
    schedule.note_change(now=100)
    schedule.finish_scan(ticket, now=1000, succeeded=True)
    for now in range(1100, 2800, 200):
        schedule.note_change(now=now)
    assert schedule.next_scan_at() == 2800


@pytest.mark.parametrize("changed_during_scan", [False, True])
def test_pending_publication_batch_bypasses_debounce_and_drains_to_clean(
    changed_during_scan: bool,
) -> None:
    schedule = _schedule()
    first = schedule.start_scan(now=0)
    if changed_during_scan:
        schedule.note_change(now=90)
    schedule.finish_scan(first, now=100, succeeded=True, pending_batch=True)
    assert schedule.next_scan_at() == 100
    schedule.note_change(now=101)
    assert schedule.next_scan_at() == 100
    second = schedule.start_scan(now=102)
    schedule.finish_scan(second, now=200, succeeded=True)
    assert schedule.next_scan_at() is None


def test_failed_publication_cannot_acknowledge_pending_batch() -> None:
    schedule = _schedule()
    ticket = schedule.start_scan(now=0)
    with pytest.raises(ValueError, match="successful publication"):
        schedule.finish_scan(ticket, now=100, succeeded=False, pending_batch=True)
    assert schedule.next_scan_at() is None
    schedule.finish_scan(ticket, now=100, succeeded=False)
    assert schedule.next_scan_at() == 400


def test_unstable_galleries_retry_without_marker_change_or_busy_loop() -> None:
    schedule = _schedule()
    first = schedule.start_scan(now=0)
    schedule.finish_scan(first, now=100, succeeded=True, waiting_galleries=True)
    assert schedule.next_scan_at() == 400
    second = schedule.start_scan(now=400)
    schedule.finish_scan(second, now=450, succeeded=True, waiting_galleries=True)
    assert schedule.next_scan_at() == 750
    third = schedule.start_scan(now=750)
    schedule.finish_scan(third, now=800, succeeded=True)
    assert schedule.next_scan_at() is None


def test_new_gallery_backlog_continues_before_unstable_gallery_retry() -> None:
    schedule = _schedule()
    first = schedule.start_scan(now=0)
    schedule.finish_scan(
        first, now=100, succeeded=True, pending_batch=True, waiting_galleries=True
    )
    assert schedule.next_scan_at() == 100
    second = schedule.start_scan(now=100)
    schedule.finish_scan(second, now=200, succeeded=True, waiting_galleries=True)
    assert schedule.next_scan_at() == 500


def test_downloader_handoff_can_start_a_clean_or_debouncing_scan() -> None:
    schedule = _clean_schedule()
    clean_ticket = schedule.start_scan(now=20)
    schedule.finish_scan(clean_ticket, now=30, succeeded=True)
    assert schedule.next_scan_at() is None
    schedule.note_change(now=40)
    early_ticket = schedule.start_scan(now=50)
    assert early_ticket.generation > clean_ticket.generation
    schedule.finish_scan(early_ticket, now=60, succeeded=True)
    assert schedule.next_scan_at() is None


@pytest.mark.parametrize("changed_during_scan", [False, True])
def test_transient_failure_retains_retry_without_busy_loop(
    changed_during_scan: bool,
) -> None:
    schedule = _schedule()
    ticket = schedule.start_scan(now=0)
    if changed_during_scan:
        schedule.note_change(now=10)
    schedule.finish_scan(ticket, now=4000, succeeded=False)
    assert schedule.next_scan_at() == 4300
    retry = schedule.start_scan(now=4300)
    schedule.finish_scan(retry, now=4500, succeeded=True)
    assert schedule.next_scan_at() is None


def test_old_or_copied_ticket_cannot_acknowledge_newer_scan() -> None:
    schedule = _schedule()
    first = schedule.start_scan(now=0)
    with pytest.raises(RuntimeError, match="already active"):
        schedule.start_scan(now=1)
    with pytest.raises(RuntimeError, match="stale ticket"):
        schedule.finish_scan(replace(first), now=10, succeeded=True)
    schedule.note_change(now=20)
    schedule.finish_scan(first, now=30, succeeded=True)
    assert schedule.next_scan_at() == 320
    second = schedule.start_scan(now=320)
    schedule.note_change(now=330)
    with pytest.raises(RuntimeError, match="stale ticket"):
        schedule.finish_scan(first, now=400, succeeded=True)
    schedule.finish_scan(second, now=500, succeeded=True)
    assert schedule.next_scan_at() == 630
    with pytest.raises(RuntimeError, match="stale ticket"):
        schedule.finish_scan(second, now=600, succeeded=True)
    assert schedule.next_scan_at() == 630


def test_invalid_completion_time_does_not_consume_ticket() -> None:
    schedule = _schedule()
    ticket = schedule.start_scan(now=10)
    with pytest.raises(ValueError, match="before it started"):
        schedule.finish_scan(ticket, now=9, succeeded=True)
    schedule.finish_scan(ticket, now=10, succeeded=True)
    assert schedule.next_scan_at() is None


@pytest.mark.parametrize(
    ("quiet", "maximum"),
    [(0, 1800), (-1, 1800), (300, 0), (301, 300), (math.nan, 1800), (300, math.inf)],
)
def test_schedule_rejects_invalid_intervals(quiet: float, maximum: float) -> None:
    with pytest.raises(ValueError, match="require finite"):
        SourceScanSchedule(quiet_seconds=quiet, max_wait_seconds=maximum, now=0)


def test_schedule_rejects_nonfinite_time_without_consuming_ticket() -> None:
    schedule = _schedule()
    with pytest.raises(ValueError, match="time must be finite"):
        schedule.note_change(now=math.nan)
    with pytest.raises(ValueError, match="time must be finite"):
        schedule.start_scan(now=math.inf)
    ticket = schedule.start_scan(now=0)
    with pytest.raises(ValueError, match="time must be finite"):
        schedule.finish_scan(ticket, now=math.inf, succeeded=True)
    schedule.finish_scan(ticket, now=1, succeeded=True)
    assert schedule.next_scan_at() is None


def test_resident_source_schedule_defaults_and_legacy_config_rejection() -> None:
    config = ResidentConfig()
    assert config.source_quiet_seconds == 300
    assert config.source_max_wait_seconds == 1800
    assert config.source_probe_interval_seconds == 30
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        ResidentConfig.model_validate({"periodic_scan_seconds": 1800})


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("source_quiet_seconds", 0),
        ("source_quiet_seconds", 1801),
        ("source_max_wait_seconds", 299),
        ("source_max_wait_seconds", math.inf),
        ("source_probe_interval_seconds", 0),
        ("source_probe_interval_seconds", math.nan),
    ],
)
def test_resident_rejects_invalid_source_schedule(name: str, value: float) -> None:
    with pytest.raises(ValidationError):
        ResidentConfig.model_validate({name: value})
