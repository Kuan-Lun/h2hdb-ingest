from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

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
