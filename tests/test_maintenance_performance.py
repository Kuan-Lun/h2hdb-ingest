from __future__ import annotations

from typing import cast

import pytest

from h2hdb_ingest._adapter_performance import adapter_bytes, adapter_phase, adapter_rows
from h2hdb_ingest._maintenance_performance import LibraryMaintenancePerformance
from h2hdb_ingest.maintenance import LibraryMaintenanceOutcome
from h2hdb_ingest.metrics import IngestMetric


def _counters(metric: IngestMetric) -> dict[str, int]:
    return {value.name: value.value for value in metric.counters}


def test_cleanup_totals_cover_repeated_attempts_and_exclude_polling_time() -> None:
    now = [1]
    records: list[IngestMetric] = []
    observer = LibraryMaintenancePerformance(
        records.append, interval_ns=1000, clock=lambda: now[0]
    )

    def cleanup() -> LibraryMaintenanceOutcome:
        with adapter_phase("journal_session"):
            now[0] += 20
            with adapter_phase("journal_commit"):
                now[0] += 10
        return LibraryMaintenanceOutcome.PROGRESSED

    for _ in range(129):
        observer.run(cleanup)
        now[0] += 100
    observer.run(lambda: LibraryMaintenanceOutcome.DONE)
    final = records[-1]
    assert final.scope == "library_cleanup_io"
    assert final.elapsed_ns == 129 * 30
    assert _counters(final)["calls"] == 130
    assert _counters(final)["outcome_progressed"] == 129
    assert _counters(final)["outcome_done"] == 1
    assert len(records) < 20  # INFO is bounded by time/outcome, not page count.
    assert len(final.operations) == 2
    totals = {
        item.operation: {v.name: v.value for v in (*item.phases_ns, *item.counters)}
        for item in final.operations
    }
    assert totals["journal_session"]["exclusive"] == 129 * 20
    assert totals["journal_commit"]["exclusive"] == 129 * 10
    assert {v.name: v.value for v in final.phases_ns}["unattributed"] == 0
    assert [r.elapsed_ns for r in records] == sorted(r.elapsed_ns for r in records)
    assert len({_counters(r)["observer_started_ns"] for r in records}) == 1


@pytest.mark.parametrize("failure", [RuntimeError, KeyboardInterrupt])
def test_failed_cleanup_is_reported_and_recovery_is_immediate(
    failure: type[BaseException],
) -> None:
    records: list[IngestMetric] = []
    observer = LibraryMaintenancePerformance(records.append)
    observer.run(lambda: LibraryMaintenanceOutcome.DONE)

    def broken() -> LibraryMaintenanceOutcome:
        with adapter_phase("verify_read"):
            adapter_bytes("verify_read", 17)
            raise failure("incomplete cleanup")

    with pytest.raises(failure, match="incomplete cleanup"):
        observer.run(broken)
    observer.run(lambda: LibraryMaintenanceOutcome.DONE)
    assert len(records) == 3
    assert records[1].status == ("failed" if failure is RuntimeError else "interrupted")
    assert records[2].status == "completed"
    assert _counters(records[2])["calls"] == 3
    assert _counters(records[2])["outcome_done"] == 2
    assert {v.name: v.value for v in records[2].operations[0].counters}[
        "logical_bytes"
    ] == 17


def test_idle_cleanup_logs_periodically_without_losing_attempts() -> None:
    now = [1]
    records: list[IngestMetric] = []
    observer = LibraryMaintenancePerformance(
        records.append, interval_ns=60, clock=lambda: now[0]
    )
    for tick in (1, 10, 30, 62):
        now[0] = tick
        observer.run(lambda: LibraryMaintenanceOutcome.DONE)
    assert len(records) == 2
    assert _counters(records[-1])["calls"] == 4
    assert records[-1].elapsed_ns == 0


def test_broken_sink_and_clock_do_not_change_cleanup_result() -> None:
    def fail_clock() -> int:
        raise OSError("clock unavailable")

    records: list[IngestMetric] = []

    def fail_sink(metric: IngestMetric) -> None:
        records.append(metric)
        raise OSError("log unavailable")

    observer = LibraryMaintenancePerformance(fail_sink, clock=fail_clock)
    assert (
        observer.run(lambda: LibraryMaintenanceOutcome.DONE)
        is LibraryMaintenanceOutcome.DONE
    )
    assert _counters(records[0])["clock_failures"] > 0


def test_invalid_cleanup_outcome_is_failed_before_reporting_done() -> None:
    records: list[IngestMetric] = []
    observer = LibraryMaintenancePerformance(records.append)

    # A malformed adapter must be rejected inside the measured failure scope.
    def malformed() -> LibraryMaintenanceOutcome:
        return cast(LibraryMaintenanceOutcome, None)

    with pytest.raises(TypeError, match="invalid outcome"):
        observer.run(malformed)
    assert records[0].status == "failed"
    assert _counters(records[0])["failed_calls"] == 1
    assert _counters(records[0])["outcome_done"] == 0


def test_flush_emits_pending_same_outcome_once_and_preserves_last_failure() -> None:
    records: list[IngestMetric] = []
    observer = LibraryMaintenancePerformance(records.append, clock=lambda: 1)
    observer.flush()
    assert not records
    observer.run(lambda: LibraryMaintenanceOutcome.DONE)
    observer.run(lambda: LibraryMaintenanceOutcome.DONE)
    assert _counters(records[-1])["calls"] == 1
    observer.flush()
    observer.flush()
    assert len(records) == 2
    assert _counters(records[-1])["calls"] == 2

    def failed() -> LibraryMaintenanceOutcome:
        raise OSError("same cleanup failure")

    for _ in range(20):
        with pytest.raises(OSError, match="same cleanup failure"):
            observer.run(failed)
    assert len(records) == 3  # Repeated failures are not emitted per attempt.
    assert _counters(records[-1])["failed_calls"] == 1
    observer.flush()
    observer.flush()
    assert len(records) == 4
    assert records[-1].status == "failed"
    assert _counters(records[-1])["calls"] == 22
    assert _counters(records[-1])["failed_calls"] == 20


def test_repeated_failure_reports_periodically_and_recovery_flushes_immediately() -> (
    None
):
    now = [1]
    records: list[IngestMetric] = []
    observer = LibraryMaintenancePerformance(
        records.append, interval_ns=60, clock=lambda: now[0]
    )

    def failed() -> LibraryMaintenanceOutcome:
        with adapter_phase("journal_commit"):
            raise OSError("persistent cleanup failure")

    for tick in (1, 10, 30, 62, 70):
        now[0] = tick
        with pytest.raises(OSError, match="persistent cleanup failure"):
            observer.run(failed)
    assert len(records) == 2
    assert _counters(records[-1])["failed_calls"] == 4
    observer.run(lambda: LibraryMaintenanceOutcome.DONE)
    assert len(records) == 3
    assert records[-1].status == "completed"
    assert _counters(records[-1])["failed_calls"] == 5
    assert _counters(records[-1])["outcome_done"] == 1
    observer.flush()
    assert len(records) == 3


def test_summary_construction_failure_cannot_replace_business_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observer = LibraryMaintenancePerformance(lambda _metric: None)

    def fail_emit(_status: object) -> None:
        raise ValueError("diagnostic construction unavailable")

    monkeypatch.setattr(observer, "_emit", fail_emit)
    assert (
        observer.run(lambda: LibraryMaintenanceOutcome.DONE)
        is LibraryMaintenanceOutcome.DONE
    )
    observer.flush()
    failure = OSError("original cleanup failure")

    def failed() -> LibraryMaintenanceOutcome:
        raise failure

    with pytest.raises(OSError) as raised:
        observer.run(failed)
    assert raised.value is failure
    observer.flush()


def test_cleanup_info_preserves_query_returned_rows_across_flushes() -> None:
    records: list[IngestMetric] = []
    observer = LibraryMaintenancePerformance(records.append, clock=lambda: 1)

    def cleanup() -> LibraryMaintenanceOutcome:
        with adapter_phase("journal_cleanup_select"):
            adapter_rows("journal_cleanup_select", 128)
        with adapter_phase("journal_cleanup_exists"):
            adapter_rows("journal_cleanup_exists", 1)
        return LibraryMaintenanceOutcome.PROGRESSED

    for _ in range(3):
        observer.run(cleanup)
    observer.flush()
    assert len(records) == 2
    totals = {
        operation.operation: {value.name: value.value for value in operation.counters}
        for operation in records[-1].operations
    }
    assert totals["journal_cleanup_select"]["calls"] == 3
    assert totals["journal_cleanup_select"]["rows_returned"] == 384
    assert totals["journal_cleanup_exists"]["rows_returned"] == 3
