"""Adapter telemetry must explain actual work and cannot alter its outcome."""

from __future__ import annotations

import logging
import os
from contextlib import nullcontext
from pathlib import Path

import pytest
from test_source_preparation_progress import _config

from h2hdb_ingest.filesystem import (
    FilesystemArtifactSourceRole,
    FilesystemFileObservation,
    FilesystemStat,
)
from h2hdb_ingest.metrics import IngestMetric
from h2hdb_ingest.runtime import build_runtime
from h2hdb_ingest.source_performance import SourcePerformance


def test_file_read_timing_excludes_generator_consumer_and_counts_rereads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "private-name.bin"
    path.write_bytes(b"source payload")
    now = [0]
    performance = SourcePerformance(clock=lambda: now[0])
    original_read = os.read

    def read(fd: int, count: int) -> bytes:
        now[0] += 7
        return original_read(fd, count)

    monkeypatch.setattr(os, "read", read)
    member = FilesystemFileObservation(
        tmp_path,
        path.name.encode(),
        FilesystemStat.from_os_stat(path.stat()),
        FilesystemArtifactSourceRole.OTHER,
        _source_performance=performance,
    )
    records: list[IngestMetric] = []
    with performance.operation(records.append):
        for _ in range(3):
            parts = []
            for part in member.content_parts():
                now[0] += 1000  # The consumer owns this work, not os.read.
                parts.append(part)
            assert b"".join(parts) == b"source payload"
    metric = records[0]
    phases = {value.name: value.value for value in metric.phases_ns}
    counters = {value.name: value.value for value in metric.counters}
    assert phases["read"] == 42  # data + EOF, three passes
    assert counters["read_calls"] == 6
    assert counters["logical_bytes_read"] == 3 * len(b"source payload")
    assert metric.elapsed_ns == 3042
    assert metric.status == "completed"
    assert "private-name" not in repr(metric) and "source payload" not in repr(metric)


@pytest.mark.parametrize("error", [ValueError("original failure"), KeyboardInterrupt()])
def test_failure_and_interruption_keep_the_original_exception_and_partial_cost(
    error: BaseException,
) -> None:
    records: list[IngestMetric] = []
    ticks = iter(range(100))
    performance = SourcePerformance(clock=lambda: next(ticks))
    with pytest.raises(type(error)) as caught, performance.operation(records.append):
        with performance.phase("qualification"):
            raise error
    assert caught.value is error
    assert records[0].status == (
        "failed" if isinstance(error, Exception) else "interrupted"
    )
    assert {v.name for v in records[0].phases_ns} == {"qualification"}


def test_broken_clock_and_sink_do_not_change_source_outcome() -> None:
    def fail_clock() -> int:
        raise RuntimeError("clock unavailable")

    def fail_sink(metric: IngestMetric) -> None:
        del metric
        raise RuntimeError("sink unavailable")

    performance = SourcePerformance(clock=fail_clock)
    with performance.operation(fail_sink), performance.phase("read"):
        performance.add("logical_bytes_read", 17)
    counters = {
        v.name: v.value for v in performance.metric(status="completed").counters
    }
    assert counters["clock_failures"] == 2
    assert counters["logical_bytes_read"] == 17


def _require_read_scope(metric: IngestMetric) -> None:
    assert "read" in {v.name for v in metric.phases_ns}
    assert {v.name: v.value for v in metric.counters}["read_calls"] > 0


def test_oracle_rejects_removed_scope_even_when_bytes_are_correct(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "x.bin"
    path.write_bytes(b"correct")
    performance = SourcePerformance()
    original = performance.phase
    monkeypatch.setattr(
        performance,
        "phase",
        lambda name: nullcontext() if name == "read" else original(name),
    )
    member = FilesystemFileObservation(
        tmp_path,
        b"x.bin",
        FilesystemStat.from_os_stat(path.stat()),
        FilesystemArtifactSourceRole.OTHER,
        _source_performance=performance,
    )
    assert b"".join(member.content_parts()) == b"correct"
    with pytest.raises(AssertionError):
        _require_read_scope(performance.metric(status="completed"))


def test_real_source_summary_is_visible_at_info_and_separates_adapter_work(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    config = _config(tmp_path, galleries=2, artifacts=True)
    with caplog.at_level(logging.INFO, logger="h2hdb_ingest.metrics"):
        with build_runtime(config, event_logger=lambda _message: None) as runtime:
            runtime.database_admin.initialize()
            runtime.resident.initialize()
            assert runtime.resident.process_available(periodic_scan=True)
            assert runtime.catalog.get_catalog_revision().publication_count == 2
            assert runtime.database_admin.check().state == "READY"
    summaries = [
        r.getMessage()
        for r in caplog.records
        if r.name == "h2hdb_ingest.metrics" and "scope=source" in r.getMessage()
    ]
    assert len(summaries) == 1
    summary = summaries[0]
    assert "status=completed" in summary
    for phase in (
        "discovery",
        "gallery_index",
        "read",
        "hash",
        "metadata_parse",
        "qualification",
        "snapshot",
    ):
        assert f"phase.{phase}_ns=" in summary
        assert f"counter.{phase}_calls=" in summary
    for counter in (
        "logical_bytes_read",
        "snapshot_bytes",
        "snapshot_files",
        "tag_rows",
        "file_rows",
        "selected_galleries",
        "work_generation",
    ):
        assert f"counter.{counter}=" in summary
    assert "counter.selected_galleries=2" in summary
