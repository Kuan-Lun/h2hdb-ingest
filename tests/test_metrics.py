from __future__ import annotations

import errno
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from threading import Barrier

import pytest

import h2hdb_ingest.metrics as metrics_module
from h2hdb_ingest._log_recovery import RecoveryLog
from h2hdb_ingest.metrics import (
    IngestMetric,
    IngestMetricOperation,
    IngestMetricValue,
    TextIngestMetricSink,
    emit_ingest_metric,
)


def test_metric_is_frozen_and_rejects_negative_or_duplicate_values() -> None:
    metric = IngestMetric(
        "artifact",
        "render_archive",
        10,
        phases_ns=(IngestMetricValue("render_pages", 7),),
    )

    with pytest.raises(FrozenInstanceError):
        metric.__setattr__("elapsed_ns", 11)
    with pytest.raises(ValueError, match="non-negative"):
        IngestMetricValue("rows", -1)
    with pytest.raises(ValueError, match="unique"):
        IngestMetric(
            "artifact",
            "render_archive",
            10,
            counters=(
                IngestMetricValue("pages", 1),
                IngestMetricValue("pages", 2),
            ),
        )


def test_text_sink_emits_one_compact_record_for_nested_operations() -> None:
    messages: list[str] = []
    sink = TextIngestMetricSink(messages.append)

    sink(
        IngestMetric(
            "publication",
            "synchronize",
            40,
            counters=(IngestMetricValue("steps", 1),),
            operations=(
                IngestMetricOperation(
                    "PREPARE_ARTIFACT",
                    phases_ns=(IngestMetricValue("prepare", 30),),
                    counters=(IngestMetricValue("processed_rows", 1),),
                ),
            ),
        )
    )

    assert messages == [
        "ingest_metric scope=publication operation=synchronize elapsed_ns=40 "
        "counter.steps=1 operation.PREPARE_ARTIFACT.prepare_ns=30 "
        "operation.PREPARE_ARTIFACT.processed_rows=1"
    ]


def test_observer_failure_cannot_change_ingest_completion() -> None:
    def fail(_metric: IngestMetric) -> None:
        raise RuntimeError("observer unavailable")

    emit_ingest_metric(fail, IngestMetric("artifact", "render_archive", 1))


def test_repeated_metric_sink_failure_is_bounded_and_recovers(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    clock = [0.0]
    reporter = metrics_module._MetricSinkDiagnostics(
        interval_seconds=30, clock=lambda: clock[0]
    )
    monkeypatch.setattr(metrics_module, "_sink_failures", reporter)
    caplog.set_level(logging.INFO, logger=metrics_module.__name__)
    rendezvous = Barrier(8)
    metric = IngestMetric("artifact", "render_archive", 1)

    unavailable = True

    def deliver(_metric: IngestMetric) -> None:
        if unavailable:
            raise RuntimeError("observer unavailable")

    def emit() -> None:
        rendezvous.wait(timeout=5)
        emit_ingest_metric(deliver, metric)

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(emit) for _ in range(8)]
        for future in futures:
            future.result(timeout=5)
    assert len(caplog.records) == 1
    assert caplog.records[0].levelno == logging.ERROR
    assert caplog.records[0].exc_info is not None
    assert 'metric_scope="artifact"' in caplog.records[0].message
    assert 'metric_operation="render_archive"' in caplog.records[0].message
    assert 'error_type="RuntimeError"' in caplog.records[0].message
    assert 'reason="observer unavailable"' in caplog.records[0].message
    clock[0] = 30
    emit_ingest_metric(deliver, metric)
    assert len(caplog.records) == 2
    assert "suppressed_repeats=7" in caplog.records[-1].message
    unavailable = False
    emit_ingest_metric(deliver, metric)
    assert len(caplog.records) == 3
    assert caplog.records[-1].levelno == logging.INFO
    assert "Operation recovered" in caplog.records[-1].message
    emit_ingest_metric(deliver, metric)
    assert len(caplog.records) == 3
    unavailable = True
    emit_ingest_metric(deliver, metric)
    assert len(caplog.records) == 4


def test_successful_other_sink_does_not_reset_repeated_failures(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(
        metrics_module,
        "_sink_failures",
        metrics_module._MetricSinkDiagnostics(interval_seconds=3600),
    )
    caplog.set_level(logging.INFO, logger=metrics_module.__name__)
    unavailable = True
    metric = IngestMetric("artifact", "render_archive", 1)

    def failing_owner(_metric: IngestMetric) -> None:
        if unavailable:
            raise RuntimeError("same unavailable owner")

    def unrelated_owner(_metric: IngestMetric) -> None:
        pass

    for _ in range(3):
        emit_ingest_metric(failing_owner, metric)
        emit_ingest_metric(unrelated_owner, metric)
    assert len(caplog.records) == 1
    assert caplog.records[0].levelno == logging.ERROR
    unavailable = False
    emit_ingest_metric(failing_owner, metric)
    assert len(caplog.records) == 2
    assert caplog.records[-1].levelno == logging.INFO
    assert "suppressed_repeats=2" in caplog.records[-1].message


def test_different_metric_context_preserves_existing_failure_coalescing(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    now = [0.0]
    monkeypatch.setattr(
        metrics_module,
        "_sink_failures",
        metrics_module._MetricSinkDiagnostics(
            interval_seconds=30, clock=lambda: now[0]
        ),
    )

    def fail(_metric: IngestMetric) -> None:
        raise RuntimeError("sink unavailable")

    emit_ingest_metric(fail, IngestMetric("artifact", "render_archive", 1))
    emit_ingest_metric(fail, IngestMetric("publication", "prepare", 1))
    assert len(caplog.records) == 1
    assert 'metric_scope="artifact"' in caplog.records[0].message
    now[0] = 30
    emit_ingest_metric(fail, IngestMetric("publication", "prepare\n\u202e", 1))
    assert len(caplog.records) == 2
    message = caplog.records[1].getMessage()
    assert "suppressed_repeats=1" in message
    assert 'metric_scope="publication"' in message
    assert 'metric_operation="prepare\\n\\u202e"' in message
    assert "\n" not in message
    assert "\u202e" not in message


def test_recovery_failure_first_line_contains_original_cause_and_target(
    caplog: pytest.LogCaptureFixture,
) -> None:
    reporter = RecoveryLog(
        metrics_module.logger, operation="library_cleanup", interval_seconds=30
    )
    cause = OSError(errno.EIO, "damaged\nentry\u202e", "/library/damaged-entry")
    failure = RuntimeError("cleanup wrapper for /library/private/journal.sqlite3")
    failure.__cause__ = cause
    reporter.log_failure(failure)
    assert len(caplog.records) == 1
    record = caplog.records[0]
    message = record.getMessage()
    assert 'error_type="OSError"' in message
    assert 'outer_error_type="RuntimeError"' in message
    assert (
        'outer_reason="cleanup wrapper for /library/private/journal.sqlite3"' in message
    )
    assert "/library/damaged-entry" in message
    assert "damaged\\nentry\\u202e" in message
    assert "\n" not in message
    assert "\u202e" not in message
    assert record.exc_info is not None
    assert record.exc_info[1] is failure


def test_different_failing_sink_replaces_only_diagnostic_owner_without_false_recovery(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(
        metrics_module,
        "_sink_failures",
        metrics_module._MetricSinkDiagnostics(interval_seconds=3600),
    )
    caplog.set_level(logging.INFO, logger=metrics_module.__name__)
    first_unavailable = second_unavailable = True
    metric = IngestMetric("artifact", "render_archive", 1)

    def first_owner(_metric: IngestMetric) -> None:
        if first_unavailable:
            raise RuntimeError("identical diagnostics")

    def second_owner(_metric: IngestMetric) -> None:
        if second_unavailable:
            raise RuntimeError("identical diagnostics")

    emit_ingest_metric(first_owner, metric)
    emit_ingest_metric(second_owner, metric)
    first_unavailable = False
    emit_ingest_metric(first_owner, metric)
    emit_ingest_metric(second_owner, metric)
    assert [record.levelno for record in caplog.records] == [
        logging.ERROR,
        logging.ERROR,
    ]
    second_unavailable = False
    emit_ingest_metric(second_owner, metric)
    assert len(caplog.records) == 3
    assert caplog.records[-1].levelno == logging.INFO
    assert "suppressed_repeats=1" in caplog.records[-1].message


def test_broken_exception_text_remains_an_observer_failure_and_is_coalesced(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(
        metrics_module,
        "_sink_failures",
        metrics_module._MetricSinkDiagnostics(interval_seconds=3600),
    )

    class UnprintableError(Exception):
        def __str__(self) -> str:
            raise ValueError("error formatting itself failed")

    def fail(_metric: IngestMetric) -> None:
        raise UnprintableError()

    for _ in range(3):
        emit_ingest_metric(fail, IngestMetric("artifact", "render_archive", 1))
    assert len(caplog.records) == 1
    assert caplog.records[0].levelno == logging.ERROR
    assert caplog.records[0].exc_info is not None
    assert isinstance(caplog.records[0].exc_info[1], UnprintableError)
    assert "UnprintableError" in caplog.text


def test_throwing_log_handler_cannot_change_metric_delivery_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    levels: list[int] = []

    class ThrowingHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            levels.append(record.levelno)
            raise OSError("log destination unavailable")

    monkeypatch.setattr(metrics_module.logger, "handlers", [ThrowingHandler()])
    monkeypatch.setattr(metrics_module.logger, "level", logging.INFO)
    monkeypatch.setattr(
        metrics_module,
        "_sink_failures",
        metrics_module._MetricSinkDiagnostics(interval_seconds=3600),
    )
    unavailable = True

    def deliver(_metric: IngestMetric) -> None:
        if unavailable:
            raise RuntimeError("observer unavailable")

    metric = IngestMetric("artifact", "render_archive", 1)
    emit_ingest_metric(deliver, metric)
    emit_ingest_metric(deliver, metric)
    unavailable = False
    emit_ingest_metric(deliver, metric)
    assert levels == [logging.ERROR, logging.INFO]


@pytest.mark.parametrize("failure_type", (KeyboardInterrupt, SystemExit))
def test_metric_delivery_preserves_process_cancellation(
    failure_type: type[BaseException],
) -> None:
    failure = failure_type("stop now")

    def fail(_metric: IngestMetric) -> None:
        raise failure

    with pytest.raises(failure_type) as caught:
        emit_ingest_metric(fail, IngestMetric("artifact", "render_archive", 1))
    assert caught.value is failure


@pytest.mark.parametrize("failure_type", (KeyboardInterrupt, SystemExit))
@pytest.mark.parametrize("event", ("failure", "recovery"))
def test_diagnostic_handler_preserves_process_cancellation(
    failure_type: type[BaseException],
    event: str,
) -> None:
    failure = failure_type("stop diagnostic delivery")
    armed = False

    class InterruptingHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            del record
            if armed:
                raise failure

    diagnostic_logger = logging.Logger("isolated-metric-cancellation", logging.INFO)
    diagnostic_logger.addHandler(InterruptingHandler())
    reporter = RecoveryLog(
        diagnostic_logger, operation="metric sink", interval_seconds=3600
    )
    if event == "recovery":
        reporter.log_failure(RuntimeError("unavailable"))
    armed = True
    with pytest.raises(failure_type) as caught:
        if event == "failure":
            reporter.log_failure(RuntimeError("unavailable"))
        else:
            reporter.recovered()
    assert caught.value is failure


@pytest.mark.parametrize("failure_type", (KeyboardInterrupt, SystemExit))
def test_exception_formatting_preserves_process_cancellation(
    failure_type: type[BaseException],
) -> None:
    failure = failure_type("stop diagnostic formatting")

    class InterruptedError(Exception):
        def __str__(self) -> str:
            raise failure

    reporter = RecoveryLog(
        logging.Logger("isolated-metric-formatting"),
        operation="metric sink",
        interval_seconds=3600,
    )
    with pytest.raises(failure_type) as caught:
        reporter.log_failure(InterruptedError())
    assert caught.value is failure
