"""Cumulative source progress and bounded qualification attribution are truthful."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from threading import Event, Thread
from threading import enumerate as enumerate_threads
from typing import Any, BinaryIO

import pytest
from PIL import Image
from test_image_qualification import _gallery

import h2hdb_ingest.image_qualification as qualification_module
import h2hdb_ingest.source_performance as performance_module
from h2hdb_ingest._image_performance import (
    ImageWorkMeasurement,
    current_image_measurement,
)
from h2hdb_ingest.artifact import ArtifactRenderPolicy
from h2hdb_ingest.core_source import VNextFilesystemSourceAdapter
from h2hdb_ingest.filesystem import FilesystemSource
from h2hdb_ingest.image_qualification import ImageGalleryQualifier
from h2hdb_ingest.metrics import IngestMetric, TextIngestMetricSink
from h2hdb_ingest.source_performance import (
    SourcePerformance,
    current_qualification_performance,
)


@pytest.mark.parametrize("failure", [None, ValueError("original"), KeyboardInterrupt()])
@pytest.mark.parametrize("scope", ("source", "source_monitor"))
def test_progress_reports_while_owner_is_blocked_and_joins_snapshot_reporter(
    monkeypatch: pytest.MonkeyPatch, failure: BaseException | None, scope: str
) -> None:
    records: list[IngestMetric] = []
    reported = Event()
    owner_ready = Event()
    threads: list[Thread] = []
    now = [0]
    performance = SourcePerformance(clock=lambda: now[0])

    def thread_factory(*args: Any, **kwargs: Any) -> Thread:
        target = kwargs["target"]

        def run() -> None:
            assert owner_ready.wait(5)
            target()

        kwargs["target"] = run
        thread = Thread(*args, **kwargs)
        if thread.name == "h2hdb-source-metrics":
            threads.append(thread)
        return thread

    def emit(metric: IngestMetric) -> None:
        # This would deadlock if delivery happened under the measurement lock.
        performance.metric(status="completed")
        records.append(metric)
        if metric.status == "progress":
            reported.set()

    monkeypatch.setattr(performance_module, "Thread", thread_factory)

    def operation() -> None:
        with performance.operation(emit, scope=scope, progress_interval_seconds=0.01):
            with performance.phase("qualification"):
                performance.add("work_generation", 101)
                now[0] = 9_000_000_000
                owner_ready.set()
                assert reported.wait(5)
                if failure is not None:
                    raise failure

    if failure is None:
        operation()
    else:
        with pytest.raises(type(failure)) as caught:
            operation()
        assert caught.value is failure
    assert threads and all(not thread.is_alive() for thread in threads)
    snapshots, final = records[:-1], records[-1]
    assert snapshots and all(
        m.scope == scope + "_progress" and m.status == "progress" for m in snapshots
    )
    for sequence, snapshot in enumerate(snapshots, 1):
        assert snapshot.elapsed_ns == 9_000_000_000
        assert {v.name: v.value for v in snapshot.phases_ns}[
            "qualification"
        ] == 9_000_000_000
        counters = {v.name: v.value for v in snapshot.counters}
        assert counters["progress_sequence"] == sequence
        assert counters["work_generation"] == 101
    assert final.scope == scope
    assert final.status == (
        "completed"
        if failure is None
        else "failed"
        if isinstance(failure, Exception)
        else "interrupted"
    )
    assert final.elapsed_ns == 9_000_000_000
    assert {v.name: v.value for v in final.counters}["progress_sequence"] >= len(
        snapshots
    )
    assert len(records) == len(snapshots) + 1  # No completion duplicated as progress.


def test_reporter_start_failure_does_not_abort_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable(_thread: Thread) -> None:
        raise RuntimeError("thread budget exhausted")

    monkeypatch.setattr(Thread, "start", unavailable)
    records: list[IngestMetric] = []
    performance = SourcePerformance()
    with performance.operation(records.append):
        performance.add("file_rows", 3)
    assert len(records) == 1 and records[0].status == "completed"
    assert {v.name: v.value for v in records[0].counters}[
        "progress_reporter_failures"
    ] == 1


def test_progress_sink_failure_keeps_owner_result_and_terminal_delivery() -> None:
    reported = Event()
    records: list[IngestMetric] = []

    def sink(metric: IngestMetric) -> None:
        if metric.status == "progress":
            reported.set()
            raise OSError("log sink unavailable")
        records.append(metric)

    with SourcePerformance().operation(sink, progress_interval_seconds=0.01):
        assert reported.wait(5)
    assert len(records) == 1 and records[0].status == "completed"


@pytest.mark.parametrize("failure", [None, ValueError("original"), KeyboardInterrupt()])
def test_blocked_progress_sink_does_not_block_owner_or_accumulate_dispatchers(
    monkeypatch: pytest.MonkeyPatch,
    failure: BaseException | None,
) -> None:
    started, release, returned = Event(), Event(), Event()
    owner_finished = Event()
    records: list[IngestMetric] = []
    caught: list[BaseException] = []
    deliveries: list[IngestMetric] = []

    def sink(metric: IngestMetric) -> None:
        if metric.status == "progress":
            deliveries.append(metric)
            started.set()
            release.wait()
            returned.set()
        else:
            records.append(metric)

    def owner() -> None:
        try:
            with SourcePerformance().operation(sink, progress_interval_seconds=0.001):
                assert started.wait(5)
                if failure is not None:
                    raise failure
        except BaseException as error:
            caught.append(error)
        finally:
            owner_finished.set()

    thread = Thread(target=owner, daemon=True)
    thread.start()
    try:
        assert started.wait(5)
        assert owner_finished.wait(5), "blocked progress delivery held the owner"
        assert caught == ([] if failure is None else [failure])
        assert records[0].status == (
            "completed"
            if failure is None
            else "failed"
            if isinstance(failure, Exception)
            else "interrupted"
        )
        dispatcher = performance_module._progress_dispatcher
        original_dispatcher = dispatcher._thread
        assert original_dispatcher is not None and original_dispatcher.is_alive()
        for _ in range(4):
            performance = SourcePerformance()
            dropped = Event()
            original_add = performance.add

            def count(
                name: str,
                value: int = 1,
                original_add: Callable[[str, int], None] = original_add,
                dropped: Event = dropped,
            ) -> None:
                original_add(name, value)
                if name == "progress_dropped_snapshots":
                    dropped.set()

            # The first attempted snapshot occupies the one mailbox slot. A
            # later attempt proves the bound without relying on a sleep delay.
            monkeypatch.setattr(performance, "add", count)
            with performance.operation(sink, progress_interval_seconds=0.001):
                assert dropped.wait(5)
                with dispatcher._condition:
                    pending_snapshot = dispatcher._pending
                assert pending_snapshot is not None
            counters = {item.name: item.value for item in records[-1].counters}
            assert counters["progress_cancelled_snapshots"] == 1
            assert counters["progress_dropped_snapshots"] >= 1
            with dispatcher._condition:
                assert dispatcher._pending is None
            assert dispatcher._thread is original_dispatcher
        assert len(deliveries) == 1
        assert (
            len(
                [
                    item
                    for item in enumerate_threads()
                    if item.name == "h2hdb-source-metric-delivery"
                ]
            )
            == 1
        )
        assert not any(
            item.name == "h2hdb-source-metrics" for item in enumerate_threads()
        )
    finally:
        release.set()
        thread.join(5)
        assert returned.wait(5)
    # No cancelled snapshot is delivered when the blocked sink recovers.
    resumed = Event()

    def resumed_sink(metric: IngestMetric) -> None:
        if metric.status == "progress":
            resumed.set()

    with SourcePerformance().operation(resumed_sink, progress_interval_seconds=0.001):
        assert resumed.wait(5)
    assert len(deliveries) == 1


def test_dispatcher_start_failure_keeps_no_pending_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable(_thread: Thread) -> None:
        raise OSError("thread budget exhausted")

    monkeypatch.setattr(Thread, "start", unavailable)
    dispatcher = performance_module._ProgressDispatcher()
    token = performance_module._ProgressToken()
    metric = SourcePerformance().metric(status="progress", scope="source_progress")
    assert not dispatcher.submit(token, lambda _metric: None, metric)
    assert dispatcher._pending is None and dispatcher._thread is None
    assert not dispatcher.cancel(token)
    assert not dispatcher.submit(token, lambda _metric: None, metric)


@pytest.mark.parametrize("failure", [KeyboardInterrupt(), SystemExit()])
def test_progress_sink_baseexception_does_not_disable_shared_dispatcher(
    failure: BaseException,
) -> None:
    failed, recovered = Event(), Event()

    def sink(metric: IngestMetric) -> None:
        if metric.status == "progress":
            failed.set()
            raise failure

    with SourcePerformance().operation(sink, progress_interval_seconds=0.001):
        assert failed.wait(5)

    def healthy(metric: IngestMetric) -> None:
        if metric.status == "progress":
            recovered.set()

    with SourcePerformance().operation(healthy, progress_interval_seconds=0.001):
        assert recovered.wait(5)


def test_nested_qualification_context_and_operation_ownership() -> None:
    outer, inner = SourcePerformance(), SourcePerformance()
    assert current_qualification_performance() is None
    with outer.operation(None), outer.qualifying():
        assert current_qualification_performance() is outer
        with pytest.raises(RuntimeError, match="already active"), outer.operation(None):
            pytest.fail("nested operation must not run")
        with inner.qualifying():
            assert current_qualification_performance() is inner
        assert current_qualification_performance() is outer
    assert current_qualification_performance() is None


@pytest.mark.parametrize(
    "failure", [None, OSError("resource failure"), KeyboardInterrupt()]
)
def test_qualification_records_exact_work_and_partial_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: BaseException | None
) -> None:
    folder = _gallery(tmp_path)
    path = folder / "001.png"
    with Image.new("RGB", (32, 24), "red") as picture:
        picture.save(path)
    (folder / "galleryinfo.txt").touch()
    performance = SourcePerformance()
    metrics: list[IngestMetric] = []
    if failure is not None:

        def fail(_source: BinaryIO, *, policy: ArtifactRenderPolicy) -> Image.Image:
            del policy
            assert current_image_measurement() is not None
            raise failure

        monkeypatch.setattr(qualification_module, "load_source_page_image", fail)

    def observe() -> None:
        with (
            performance.operation(metrics.append),
            FilesystemSource(tmp_path, performance=performance) as source,
        ):
            adapter = VNextFilesystemSourceAdapter(
                source,
                performance=performance,
                qualify_gallery=ImageGalleryQualifier(
                    ArtifactRenderPolicy(), workers=2
                ),
            )
            observed = adapter.observe_gallery(("1234",))
            assert observed.qualification.accepted
            adapter.list_file_observations(observed, after_name_bytes=None, limit=128)
            adapter.observe_completion_marker(("1234",))

    if failure is None:
        observe()
    else:
        with pytest.raises(type(failure)) as caught:
            observe()
        assert caught.value is failure
    assert current_qualification_performance() is None
    assert len(metrics) == 1
    (qualification,) = metrics[0].operations
    phases = {v.name: v.value for v in qualification.phases_ns}
    counters = {v.name: v.value for v in qualification.counters}
    assert counters["source_spooled_logical_bytes"] == path.stat().st_size
    assert counters["spool_readback_logical_bytes"] == path.stat().st_size
    assert (
        counters["spool_attempts"]
        == counters["worker_started"]
        == counters["worker_attempts"]
        == 1
    )
    assert counters["worker_thread_cpu_ns"] > 0
    assert (
        phases["owner_spool"]
        >= phases["owner_spool_write"]
        + phases["owner_spool_hash"]
        + phases["owner_source_hash"]
        + phases["owner_spool_readback"]
    )
    assert phases["owner_future_wait"] >= 0
    assert phases["worker_elapsed_sum"] > 0
    if failure is None:
        assert (
            counters["worker_accepted"]
            == counters["codec_png"]
            == counters["pixels_le_1m"]
            == 1
        )
        assert phases["worker_decoder_pipeline"] <= phases["worker_elapsed_sum"]
        assert phases["worker_header"] > 0
        assert phases["worker_decode_and_shrink"] > 0
        assert phases["worker_scheduler_wait"] > 0
        assert counters["decoder_input_logical_bytes"] > 0
    else:
        key = (
            "worker_failed" if isinstance(failure, Exception) else "worker_interrupted"
        )
        assert counters[key] == counters["pixels_unknown"] == 1
    messages: list[str] = []
    TextIngestMetricSink(messages.append)(metrics[0])
    assert "operation.qualification.worker_elapsed_sum_ns=" in messages[0]
    assert str(folder) not in messages[0] and "001.png" not in messages[0]


def test_worker_bins_and_accumulation_do_not_retain_per_page_state() -> None:
    performance = SourcePerformance()
    measured = ImageWorkMeasurement(
        phases_ns={"decode_and_shrink": 19}, decoder_input_bytes=31
    )
    for index in range(10_000):
        performance.record_qualification_worker(
            replace(
                measured,
                source_kind=f"unknown-codec-{index}",
                source_width=index,
                source_height=2,
            ),
            elapsed_ns=23,
            thread_cpu_ns=17,
            encoded_bytes=index,
            outcome="accepted",
        )
    (operation,) = performance.metric(status="completed").operations
    counters = {v.name: v.value for v in operation.counters}
    phases = {v.name: v.value for v in operation.phases_ns}
    assert counters["codec_other"] == counters["worker_attempts"] == 10_000
    assert counters["decoder_input_logical_bytes"] == 310_000
    assert phases["worker_elapsed_sum"] == 230_000
    assert len(counters) < 16 and len(phases) == 2
    assert len(performance._qualification_counters) == len(counters)
    assert len(performance._qualification_phases) == len(phases)


def test_rejected_gallery_is_a_completed_qualification_with_rejected_worker(
    tmp_path: Path,
) -> None:
    folder = _gallery(tmp_path)
    (folder / "001.jpg").write_bytes(b"corrupt image input")
    (folder / "galleryinfo.txt").touch()
    performance = SourcePerformance()
    records: list[IngestMetric] = []
    with (
        performance.operation(records.append),
        FilesystemSource(tmp_path, performance=performance) as source,
    ):
        adapter = VNextFilesystemSourceAdapter(
            source,
            performance=performance,
            qualify_gallery=ImageGalleryQualifier(ArtifactRenderPolicy(), workers=1),
        )
        observed = adapter.observe_gallery(("1234",))
        assert not observed.qualification.accepted
        assert (
            len(
                adapter.list_file_observations(
                    observed, after_name_bytes=None, limit=128
                ).items
            )
            == 2
        )
        adapter.observe_completion_marker(("1234",))
    assert records[0].status == "completed"
    counters = {v.name: v.value for v in records[0].operations[0].counters}
    assert counters["worker_rejected"] == counters["worker_attempts"] == 1
    assert "worker_failed" not in counters


def test_progress_marks_broken_clock_without_failing_owner() -> None:
    reported = Event()
    records: list[IngestMetric] = []

    def broken_clock() -> int:
        raise OSError("clock unavailable")

    def sink(metric: IngestMetric) -> None:
        records.append(metric)
        if metric.status == "progress":
            reported.set()

    performance = SourcePerformance(clock=broken_clock)
    with performance.operation(sink, progress_interval_seconds=0.01):
        assert reported.wait(5)
    assert records[-1].status == "completed"
    for snapshot in records[:-1]:
        assert snapshot.status == "progress" and snapshot.elapsed_ns == 0
        assert {v.name: v.value for v in snapshot.counters}[
            "progress_clock_failures"
        ] > 0
    assert {v.name: v.value for v in records[-1].counters}["clock_failures"] == 1
