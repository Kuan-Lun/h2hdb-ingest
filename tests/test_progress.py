from __future__ import annotations

import math
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Condition, Event, Lock, Thread
from threading import enumerate as enumerate_threads

import pytest

import h2hdb_ingest.progress as progress_module
from h2hdb_ingest.progress import IngestProgress, ProgressSnapshot


class _Clock:
    def __init__(self) -> None:
        self._lock = Lock()
        self._now = 0.0

    def __call__(self) -> float:
        with self._lock:
            return self._now

    def advance(self, seconds: float) -> None:
        with self._lock:
            self._now += seconds


class _ObservedEvent:
    """Wake fake-clock sleepers and observe their next real wait deterministically."""

    def __init__(self) -> None:
        self._event = Event()
        self._condition = Condition()
        self._waits = 0

    def clear(self) -> None:
        self._event.clear()

    def set(self) -> None:
        self._event.set()

    def wait(self, timeout: float | None = None) -> bool:
        with self._condition:
            self._waits += 1
            self._condition.notify_all()
        return self._event.wait(timeout)

    @property
    def waits(self) -> int:
        with self._condition:
            return self._waits

    def await_wait(self, count: int) -> None:
        with self._condition:
            assert self._condition.wait_for(lambda: self._waits >= count, timeout=5)

    def advance(self, clock: _Clock, seconds: float) -> None:
        count = self.waits
        clock.advance(seconds)
        self.set()
        self.await_wait(count + 1)


@contextmanager
def _running(
    monkeypatch: pytest.MonkeyPatch, emit: Callable[[str], None]
) -> Iterator[tuple[IngestProgress, _Clock, _ObservedEvent]]:
    clock = _Clock()
    wake = _ObservedEvent()
    monkeypatch.setattr(progress_module, "Event", lambda: wake)
    progress = IngestProgress(lambda _: None, clock=clock, emit_debug=emit)
    progress.start()
    wake.await_wait(1)
    try:
        yield progress, clock, wake
    finally:
        progress.close()


def _snapshot(progress: IngestProgress) -> ProgressSnapshot:
    snapshot = progress.snapshot()
    assert snapshot is not None
    return snapshot


def test_announced_phases_emit_immediately_and_keep_work_counters() -> None:
    messages: list[str] = []
    clock = _Clock()
    progress = IngestProgress(lambda _: None, clock=clock, emit_debug=messages.append)
    work = progress.begin("source")
    assert progress.current() is work
    work.advance("galleries", 2)
    clock.advance(10)
    work.operation("observing")
    work.phase("analysis")
    work.phase("analysis")
    snapshot = _snapshot(progress)
    assert snapshot.phase == "analysis"
    assert snapshot.operation is None
    assert snapshot.elapsed_seconds == 10
    assert snapshot.phase_elapsed_seconds == 0
    assert snapshot.last_progress_age_seconds == 10
    assert snapshot.counters == (("galleries", 2),)
    assert len(messages) == 3
    assert "event=phase_started" in messages[0]
    assert "phase=source" in messages[0]
    assert "event=phase_ended" in messages[1]
    assert "operation=observing" in messages[1]
    assert "event=phase_started" in messages[2]
    assert "phase=analysis" in messages[2]
    clock.advance(5)
    work.finish()
    assert "event=work_finished" in messages[-1]
    assert "status=completed" in messages[-1]
    assert "elapsed_seconds=15.0" in messages[-1]
    assert progress.current() is None
    assert progress.snapshot() is None


def test_silent_candidate_and_failed_work_have_explicit_outcomes() -> None:
    messages: list[str] = []
    progress = IngestProgress(lambda _: None, emit_debug=messages.append)
    work = progress.begin("maintenance_probe", announce=False)
    work.finish(announce=False)
    assert not messages
    work = progress.begin("source")
    work.phase("quiet_wait", announce=False)
    assert len(messages) == 2
    assert "event=phase_ended" in messages[-1]
    work.finish("failed")
    assert "status=failed" in messages[-1]


def test_counter_progress_age_changes_only_when_values_change() -> None:
    clock = _Clock()
    progress = IngestProgress(lambda _: None, clock=clock)
    work = progress.begin("source")
    clock.advance(10)
    work.advance("pages", 0)
    work.set_counter("galleries", 0)
    work.operation("slow_read")
    work.phase("analysis")
    assert _snapshot(progress).last_progress_age_seconds == 10
    work.advance("pages")
    assert _snapshot(progress).last_progress_age_seconds == 0
    clock.advance(3)
    work.set_counter("pages", 1)
    assert _snapshot(progress).last_progress_age_seconds == 3
    work.set_counter("pages", 0)
    assert _snapshot(progress).last_progress_age_seconds == 0


def test_concurrent_worker_counters_are_exact_and_snapshots_are_immutable() -> None:
    progress = IngestProgress(lambda _: None)
    work = progress.begin("render")

    def render_pages() -> None:
        for _ in range(1000):
            work.advance("pages_rendered")

    with ThreadPoolExecutor(max_workers=16) as executor:
        for future in [executor.submit(render_pages) for _ in range(32)]:
            future.result()
    before = _snapshot(progress)
    assert before.counters == (("pages_rendered", 32000),)
    work.advance("pages_written")
    assert before.counters == (("pages_rendered", 32000),)
    assert _snapshot(progress).counters == (
        ("pages_rendered", 32000),
        ("pages_written", 1),
    )


def test_late_worker_is_fenced_after_finish_and_replacement() -> None:
    messages: list[str] = []
    progress = IngestProgress(lambda _: None, emit_debug=messages.append)
    old = progress.begin("old")
    with pytest.raises(RuntimeError, match="already active"):
        progress.begin("concurrent")
    old.finish()
    new = progress.begin("new")
    message_count = len(messages)
    old.advance("pages_rendered", 100)
    old.set_counter("galleries", 999)
    old.operation("stale_operation")
    old.phase("stale_phase")
    old.finish("failed")
    assert progress.current() is new
    assert new.generation > old.generation
    assert _snapshot(progress).phase == "new"
    assert not _snapshot(progress).counters
    assert len(messages) == message_count
    progress.close()
    new.advance("pages_rendered")
    new.phase("after_close")
    new.finish()
    progress.start()
    progress.close()
    assert progress.current() is None
    assert progress.snapshot() is None
    assert len(messages) == message_count
    with pytest.raises(RuntimeError, match="closed"):
        progress.begin("after_close")


def test_idle_reporter_is_silent_for_multiple_hours(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages: list[str] = []
    with _running(monkeypatch, messages.append) as (progress, clock, wake):
        wake.advance(clock, 8 * 3600)
        assert not messages
        wait_count = wake.waits
        work = progress.begin("maintenance_probe", announce=False)
        wake.await_wait(wait_count + 1)
        wake.advance(clock, 3599)
        assert not messages
        wait_count = wake.waits
        work.finish(announce=False)
        wake.await_wait(wait_count + 1)
        wake.advance(clock, 8 * 3600)
        assert not messages


def test_periodic_reports_continue_while_main_work_is_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages: list[str] = []
    with _running(monkeypatch, messages.append) as (progress, clock, wake):
        wait_count = wake.waits
        work = progress.begin("source")
        wake.await_wait(wait_count + 1)
        work.advance("galleries", 7)
        work.operation("reading_source")
        wake.advance(clock, 3599)
        assert len(messages) == 1
        # No worker API calls occur during either of these simulated hours.
        wake.advance(clock, 1)
        assert len(messages) == 2
        assert "event=periodic" in messages[-1]
        assert "counter.galleries=7" in messages[-1]
        assert "operation=reading_source" in messages[-1]
        assert "last_progress_age_seconds=3600.0" in messages[-1]
        wake.advance(clock, 3600)
        assert len(messages) == 3
        assert "last_progress_age_seconds=7200.0" in messages[-1]


def test_phase_and_counter_updates_do_not_postpone_hourly_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages: list[str] = []
    with _running(monkeypatch, messages.append) as (progress, clock, wake):
        wait_count = wake.waits
        work = progress.begin("source", announce=False)
        wake.await_wait(wait_count + 1)
        wake.advance(clock, 3599)
        work.phase("analysis", announce=False)
        work.advance("galleries", 2)
        wake.advance(clock, 1)
        assert len(messages) == 1
        assert "phase=analysis" in messages[0]
        assert "elapsed_seconds=3600.0" in messages[0]
        assert "phase_elapsed_seconds=1.0" in messages[0]
        assert "last_progress_age_seconds=1.0" in messages[0]
        wake.advance(clock, 5 * 3600)
        assert len(messages) == 2
        wake.advance(clock, 0)
        assert len(messages) == 2


def test_observer_failure_does_not_abort_work_or_stop_periodic_reporter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def failing_emitter(message: str) -> None:
        calls.append(message)
        raise OSError("log sink unavailable")

    with _running(monkeypatch, failing_emitter) as (progress, clock, wake):
        wait_count = wake.waits
        work = progress.begin("source")
        wake.await_wait(wait_count + 1)
        work.advance("galleries")
        work.phase("analysis")
        wake.advance(clock, 3600)
        wake.advance(clock, 3600)
        work.finish()
        assert sum("event=periodic" in message for message in calls) == 2
        assert "status=completed" in calls[-1]


def test_slow_observer_does_not_hold_counter_lock_and_close_waits_for_it() -> None:
    entered = Event()
    release = Event()
    counter_done = Event()
    close_done = Event()
    messages: list[str] = []

    def slow_emitter(message: str) -> None:
        assert progress.snapshot() is not None
        entered.set()
        assert release.wait(5)
        messages.append(message)

    progress = IngestProgress(slow_emitter)
    work = progress.begin("source", announce=False)
    announce = Thread(target=lambda: work.phase("analysis"))
    announce.start()
    assert entered.wait(5)

    def advance() -> None:
        work.advance("pages")
        counter_done.set()

    worker = Thread(target=advance)
    worker.start()
    assert counter_done.wait(5)

    def close() -> None:
        progress.close()
        close_done.set()

    closing = Thread(target=close)
    closing.start()
    assert not close_done.is_set()
    release.set()
    worker.join(5)
    announce.join(5)
    closing.join(5)
    assert not any(thread.is_alive() for thread in (worker, announce, closing))
    assert close_done.is_set()
    assert len(messages) == 1
    work.finish()
    assert len(messages) == 1


def test_reporter_lifecycle_is_idempotent_and_close_joins_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages: list[str] = []
    with _running(monkeypatch, messages.append) as (progress, _, _):
        threads = [
            thread
            for thread in enumerate_threads()
            if thread.name == "h2hdb-ingest-progress"
        ]
        assert len(threads) == 1
        progress.start()
        progress.close()
        progress.close()
        progress.start()
        assert not threads[0].is_alive()
        assert not messages


@pytest.mark.parametrize("interval", [0, -1, math.nan, math.inf])
def test_rejects_invalid_interval(interval: float) -> None:
    with pytest.raises(ValueError, match="finite and positive"):
        IngestProgress(lambda _: None, interval_seconds=interval)


@pytest.mark.parametrize("value", [-1, True, 1.5])
def test_rejects_invalid_counter_values(value: int) -> None:
    progress = IngestProgress(lambda _: None)
    work = progress.begin("source")
    with pytest.raises(ValueError, match="non-negative int"):
        work.advance("galleries", value)
    with pytest.raises(ValueError, match="non-negative int"):
        work.set_counter("galleries", value)


@pytest.mark.parametrize("name", ["", "with space", "with\nnewline", "x" * 129, "圖"])
def test_rejects_invalid_names(name: str) -> None:
    progress = IngestProgress(lambda _: None)
    with pytest.raises(ValueError, match="non-empty tokens"):
        progress.begin(name)


def test_counter_keys_are_bounded_without_limiting_existing_counters() -> None:
    progress = IngestProgress(lambda _: None)
    work = progress.begin("source")
    for position in range(64):
        work.advance(f"counter_{position}")
    work.advance("counter_0")
    with pytest.raises(ValueError, match="counter key limit"):
        work.advance("one_too_many")
    assert len(_snapshot(progress).counters) == 64
    assert dict(_snapshot(progress).counters)["counter_0"] == 2


def test_info_explains_current_work_and_debug_retains_internal_details() -> None:
    info: list[str] = []
    debug: list[str] = []
    clock = _Clock()
    progress = IngestProgress(info.append, clock=clock, emit_debug=debug.append)
    work = progress.begin("source", announce=False)
    work.set_counter("batch_new_gallery_limit", 10)
    work.set_counter("cbz_enabled", 1)
    work.advance("galleries_discovered", 131256)
    work.operation(
        "source_discovery_transfer", completed=4096, total=131256, unit="galleries"
    )
    clock.advance(3600)
    progress._report_due()
    assert info == [
        "Ingest progress: Copying the gallery inventory into the batch plan; "
        "4,096 / 131,256 galleries completed; "
        "since work started (1h 0m 0s): +131,256 gallery folders discovered; "
        "last measured advance 1h 0m 0s ago; "
        "current operation elapsed 1h 0m 0s; work elapsed 1h 0m 0s; "
        "batch limit 10 new galleries; CBZs rendered this work 0; "
        "catalog publication pending"
    ]
    assert len(debug) == 1
    assert "event=periodic generation=1 phase=source" in debug[0]
    assert "operation_completed=4096 operation_total=131256" in debug[0]
    assert "counter.galleries_discovered=131256" in debug[0]
    for internal in ("generation=", "counter.", "operation=", "phase=", "event="):
        assert internal not in info[0]
    clock.advance(1800)
    work.operation(
        "source_discovery_transfer", completed=8192, total=131256, unit="galleries"
    )
    clock.advance(1800)
    progress._report_due()
    assert "since previous report (1h 0m 0s): +4,096 galleries completed" in info[-1]
    assert "current operation elapsed 2h 0m 0s" in info[-1]
    assert "last measured advance 30m 0s ago" in info[-1]
    clock.advance(3600)
    progress._report_due()
    assert "since previous report (1h 0m 0s): +0 galleries completed" in info[-1]
    assert "last measured advance 1h 30m 0s ago" in info[-1]


def test_unknown_and_empty_totals_are_distinct_and_operation_timer_is_separate() -> (
    None
):
    info: list[str] = []
    clock = _Clock()
    progress = IngestProgress(info.append, clock=clock)
    work = progress.begin("source", announce=False)
    work.operation("source_discovery", completed=20, unit="galleries")
    clock.advance(3600)
    progress._report_due()
    assert "20 galleries completed (total unknown)" in info[-1]
    work.operation("source_batch_selection", completed=0, total=0, unit="galleries")
    clock.advance(3600)
    progress._report_due()
    assert "0 / 0 galleries completed" in info[-1]
    assert "current operation elapsed 1h 0m 0s; work elapsed 2h 0m 0s" in info[-1]
    assert "current operation started since previous report" in info[-1]
    work.operation("source_batch_order")
    clock.advance(3600)
    progress._report_due()
    assert "completion count unavailable for this operation" in info[-1]
    assert "no completed-item counts available" in info[-1]


def test_info_distinguishes_rendered_cbz_from_published_catalog_and_metadata_only() -> (
    None
):
    info: list[str] = []
    progress = IngestProgress(info.append)
    work = progress.begin("source")
    assert "CBZ" not in info[-1]
    work.set_counter("batch_selected_galleries", 1010)
    work.advance("archives_rendered", 10)
    work.phase("publication")
    assert "galleries in this batch 1,010 (existing and new)" in info[-1]
    assert "CBZs rendered this work 10" in info[-1]
    assert "catalog publication pending" in info[-1]
    work.advance("publication_batches_finalized")
    work.finish()
    assert info[-1].startswith("Ingest work completed:")
    assert "CBZs rendered this work 10; catalog batches published 1" in info[-1]
    assert "catalog publication pending" not in info[-1]


def test_info_reports_counter_deltas_when_operations_change() -> None:
    info: list[str] = []
    clock = _Clock()
    progress = IngestProgress(info.append, clock=clock)
    work = progress.begin("publication")
    work.advance("pages_rendered", 4)
    work.advance("pages_written", 2)
    work.operation("archive_inspect")
    clock.advance(3600)
    progress._report_due()
    assert (
        "since previous report (1h 0m 0s): +4 pages rendered, +2 pages written into CBZs"
        in info[-1]
    )
    work.advance("pages_written", 2)
    work.advance("archives_rendered")
    work.operation("archive_finalize")
    clock.advance(3600)
    progress._report_due()
    assert (
        "since previous report (1h 0m 0s): +2 pages written into CBZs, +1 CBZs rendered"
        in info[-1]
    )
    assert "+4 pages rendered" not in info[-1]


def test_activity_restores_parent_operation_counts_and_elapsed_after_nested_error() -> (
    None
):
    clock = _Clock()
    progress = IngestProgress(lambda _: None, clock=clock)
    work = progress.begin("source", announce=False)
    work.operation("source_prepare", completed=2, total=10, unit="galleries")
    original = _snapshot(progress).operation_generation
    clock.advance(30)
    with work.activity("source_discovery"):
        work.advance("galleries_discovered", 100)
        with pytest.raises(OSError, match="source disappeared"):
            with work.activity("source_discovery_transfer", completed=0, total=100):
                clock.advance(20)
                work.operation("source_discovery_order", completed=100, total=100)
                raise OSError("source disappeared")
        snapshot = _snapshot(progress)
        assert snapshot.operation == "source_discovery"
        assert snapshot.operation_completed == 100
        assert snapshot.operation_elapsed_seconds == 20
    snapshot = _snapshot(progress)
    assert snapshot.operation == "source_prepare"
    assert snapshot.operation_generation == original
    assert snapshot.operation_completed == 2
    assert snapshot.operation_total == 10
    assert snapshot.operation_elapsed_seconds == 50
    with work.activity("source_file_read"):
        pass
    assert _snapshot(progress).operation == "source_prepare"


def test_activity_restores_no_operation_without_leaving_a_stale_label() -> None:
    progress = IngestProgress(lambda _: None)
    work = progress.begin("source", announce=False)
    with work.activity("source_discovery"):
        work.operation("source_discovery_order")
    snapshot = _snapshot(progress)
    assert snapshot.operation is None
    assert snapshot.operation_completed is None
    assert snapshot.operation_elapsed_seconds == 0


def test_activity_restoration_is_fenced_by_phase_and_work_generations() -> None:
    progress = IngestProgress(lambda _: None)
    work = progress.begin("source", announce=False)
    work.operation("source_prepare")
    with work.activity("source_discovery"):
        work.phase("analysis", announce=False)
        work.operation("analysis_prepare")
    assert _snapshot(progress).operation == "analysis_prepare"
    with work.activity("analysis_prepare_step"):
        work.finish(announce=False)
        replacement = progress.begin("source", announce=False)
        replacement.operation("source_prepare")
    assert _snapshot(progress).operation == "source_prepare"
    assert progress.current() is replacement
    with work.activity("stale_scope", completed=999):
        pass
    assert _snapshot(progress).operation == "source_prepare"
    assert _snapshot(progress).operation_completed is None


def test_new_operation_scope_gets_new_timer_even_when_name_is_unchanged() -> None:
    clock = _Clock()
    progress = IngestProgress(lambda _: None, clock=clock)
    work = progress.begin("source", announce=False)
    work.operation("source_discovery", completed=2)
    clock.advance(30)
    with work.activity("source_discovery", completed=0):
        assert _snapshot(progress).operation_elapsed_seconds == 0
        assert _snapshot(progress).operation_completed == 0
    assert _snapshot(progress).operation_elapsed_seconds == 30
    assert _snapshot(progress).operation_completed == 2


@pytest.mark.parametrize(
    ("completed", "total", "unit", "error"),
    [
        (-1, None, None, "non-negative ints"),
        (True, None, None, "non-negative ints"),
        (0, -1, None, "non-negative ints"),
        (0, 2.5, None, "non-negative ints"),
        (2, 1, None, "cannot exceed total"),
        (None, None, "line\nbreak", "non-empty tokens"),
    ],
)
def test_invalid_measurements_fail_before_changing_work(
    completed: int | None, total: int | None, unit: str | None, error: str
) -> None:
    progress = IngestProgress(lambda _: None)
    work = progress.begin("source", announce=False)
    with pytest.raises(ValueError, match=error):
        work.operation("source_discovery", completed=completed, total=total, unit=unit)
    assert _snapshot(progress).operation is None


@pytest.mark.parametrize("failed_sink", ["info", "debug"])
def test_log_levels_have_independent_failure_handling(failed_sink: str) -> None:
    info: list[str] = []
    debug: list[str] = []

    def info_sink(message: str) -> None:
        info.append(message)
        if failed_sink == "info":
            raise OSError("INFO sink failed")

    def debug_sink(message: str) -> None:
        debug.append(message)
        if failed_sink == "debug":
            raise OSError("DEBUG sink failed")

    progress = IngestProgress(info_sink, emit_debug=debug_sink)
    work = progress.begin("source")
    work.finish("failed")
    assert len(info) == len(debug) == 2
    assert info[-1].startswith("Ingest work failed:")
    assert "status=failed" in debug[-1]


def test_default_diagnostics_use_debug_level(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("DEBUG", logger="h2hdb_ingest.progress")
    info: list[str] = []
    progress = IngestProgress(info.append)
    progress.begin("source")
    assert len(info) == 1
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "DEBUG"
    assert "event=phase_started" in caplog.records[0].message


def test_known_total_does_not_invent_a_completed_count() -> None:
    info: list[str] = []
    clock = _Clock()
    progress = IngestProgress(info.append, clock=clock)
    work = progress.begin("source", announce=False)
    work.operation("source_batch_order", total=10, unit="galleries")
    clock.advance(3600)
    progress._report_due()
    assert (
        "completion count unavailable for this operation (total 10 galleries)"
        in info[-1]
    )
    assert "0 / 10" not in info[-1]


def test_inferred_counts_only_include_work_done_during_the_current_operation() -> None:
    progress = IngestProgress(lambda _: None)
    work = progress.begin("publication", announce=False)
    work.advance("pages_rendered", 200)
    with work.activity("archive_render_pages"):
        assert _snapshot(progress).operation_completed == 0
        work.advance("pages_rendered", 2)
        work.operation("archive_render_pages")
        assert _snapshot(progress).operation_completed == 2
    with work.activity("archive_render_pages"):
        work.advance("pages_rendered", 3)
        assert _snapshot(progress).operation_completed == 3
    assert dict(_snapshot(progress).counters)["pages_rendered"] == 205


def test_unchanged_zero_completion_and_total_changes_are_not_measured_advance() -> None:
    clock = _Clock()
    progress = IngestProgress(lambda _: None, clock=clock)
    work = progress.begin("source", announce=False)
    work.operation("source_batch_selection")
    clock.advance(30)
    work.operation("source_batch_selection", completed=0, total=100)
    assert _snapshot(progress).last_progress_age_seconds == 30
    work.operation("source_batch_selection", completed=0, total=101)
    assert _snapshot(progress).last_progress_age_seconds == 30
    work.operation("source_batch_selection", completed=1, total=101)
    assert _snapshot(progress).last_progress_age_seconds == 0
