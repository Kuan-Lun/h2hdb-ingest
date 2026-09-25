"""Adapter attribution counts actual I/O while preserving safety and replay."""

from __future__ import annotations

import asyncio
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import BinaryIO, cast

import pytest
from library_fixtures import _adapter, _item, _protect

from h2hdb_ingest._adapter_performance import (
    adapter_bytes,
    adapter_phase,
    adapter_read,
    adapter_rows,
    summarize_adapter_io,
)
from h2hdb_ingest.library import _publish_resumable_file
from h2hdb_ingest.metrics import IngestMetric


def _values(metric: IngestMetric, operation: str) -> dict[str, int]:
    found = next(item for item in metric.operations if item.operation == operation)
    return {value.name: value.value for value in (*found.phases_ns, *found.counters)}


def test_nested_wall_attribution_and_checkpoint_are_cumulative() -> None:
    now = [0]
    records: list[IngestMetric] = []
    with summarize_adapter_io(
        records.append, generation=91, clock=lambda: now[0], interval_ns=60
    ):
        with adapter_phase("protect"):
            now[0] = 10
            with adapter_phase("stage"):
                now[0] = 20
                with adapter_phase("stage_read"):
                    now[0] = 30
                    adapter_bytes("stage_read", 100)
                now[0] = 40
            now[0] = 70
        assert len(records) == 1
        with adapter_phase("protect"):
            now[0] = 80
    assert [item.operation for item in records] == ["checkpoint", "publication"]
    assert _values(records[0], "protect")["inclusive"] == 70
    terminal = records[-1]
    assert _values(terminal, "protect")["inclusive"] == 80
    assert _values(terminal, "protect")["exclusive"] == 50
    assert _values(terminal, "stage")["exclusive"] == 20
    assert _values(terminal, "stage_read")["exclusive"] == 10
    assert (
        sum(
            _values(terminal, item.operation)["exclusive"]
            for item in terminal.operations
        )
        == 80
    )
    assert _values(terminal, "stage_read")["logical_bytes"] == 100
    assert {item.name: item.value for item in terminal.counters}[
        "ingest_generation"
    ] == 91


def test_cleanup_query_rows_include_only_actual_results_in_the_owner_scope() -> None:
    records: list[IngestMetric] = []
    with sqlite3.connect(":memory:") as connection:
        connection.execute("CREATE TABLE candidates (position INTEGER PRIMARY KEY)")
        connection.executemany(
            "INSERT INTO candidates VALUES (?)", ((position,) for position in range(9))
        )
        with summarize_adapter_io(
            records.append, generation=0, operation="library_cleanup"
        ):
            with adapter_phase("journal_cleanup_select"):
                rows = connection.execute(
                    "SELECT position FROM candidates ORDER BY position LIMIT 8"
                ).fetchall()
                adapter_rows("journal_cleanup_select", len(rows))
            with adapter_phase("journal_cleanup_exists"):
                exists = connection.execute(
                    "SELECT EXISTS(SELECT 1 FROM candidates WHERE position > 7)"
                ).fetchone()
                adapter_rows("journal_cleanup_exists", int(exists is not None))
            copied = copy_context()
            with ThreadPoolExecutor(max_workers=1) as executor:
                executor.submit(
                    copied.run, adapter_rows, "journal_cleanup_select", 1000
                ).result()
        copied.run(adapter_rows, "journal_cleanup_select", 1000)
    assert len(records) == 1
    assert records[0].operation == "library_cleanup"
    assert _values(records[0], "journal_cleanup_select")["rows_returned"] == 8
    assert _values(records[0], "journal_cleanup_exists")["rows_returned"] == 1
    assert _values(records[0], "journal_cleanup_select")["calls"] == 1
    assert _values(records[0], "journal_cleanup_exists")["calls"] == 1


@pytest.mark.parametrize("failure", [RuntimeError, KeyboardInterrupt])
def test_failed_work_is_retained_and_new_publication_does_not_leak(
    failure: type[BaseException],
) -> None:
    records: list[IngestMetric] = []
    with pytest.raises(failure), summarize_adapter_io(records.append, generation=1):
        with adapter_phase("stage_read"):
            adapter_bytes("stage_read", 7)
            raise failure("partial I/O")
    with summarize_adapter_io(records.append, generation=2):
        pass
    assert records[0].status == ("failed" if failure is RuntimeError else "interrupted")
    assert _values(records[0], "stage_read")["failed_calls"] == 1
    assert _values(records[0], "stage_read")["logical_bytes"] == 7
    assert records[1].operations == ()


@pytest.mark.parametrize("prefix", [0, 1, 1023, 1024])
def test_real_resumable_stage_measures_read_write_hash_and_sync(
    tmp_path: Path, prefix: int
) -> None:
    payload = b"abcdefgh" * 128
    if prefix:
        (tmp_path / "partial").write_bytes(payload[:prefix])
    records: list[IngestMetric] = []
    with summarize_adapter_io(records.append, generation=4):
        _publish_resumable_file(
            BytesIO(payload),
            directory=tmp_path,
            temporary_leaf="partial",
            final_leaf="complete",
            expected_sha256=sha256(payload).digest(),
            expected_size=len(payload),
            label="test stage",
        )
    assert (tmp_path / "complete").read_bytes() == payload
    assert not (tmp_path / "partial").exists()
    terminal = records[-1]
    assert _values(terminal, "stage_read")["logical_bytes"] == len(payload)
    assert _values(terminal, "stage_hash")["logical_bytes"] == len(payload)
    if prefix:
        assert _values(terminal, "stage_prefix_read")["logical_bytes"] == prefix
    if prefix < len(payload):
        assert (
            _values(terminal, "stage_write")["logical_bytes"] == len(payload) - prefix
        )
    assert _values(terminal, "file_fsync")["calls"] == 1
    assert _values(terminal, "directory_fsync")["calls"] == 2
    assert _values(terminal, "rename")["calls"] == 1


def test_stage_failure_keeps_exact_prefix_rejection(tmp_path: Path) -> None:
    (tmp_path / "partial").write_bytes(b"bad")
    records: list[IngestMetric] = []
    with (
        pytest.raises(RuntimeError, match="exact source prefix"),
        summarize_adapter_io(records.append, generation=5),
    ):
        _publish_resumable_file(
            BytesIO(b"correct"),
            directory=tmp_path,
            temporary_leaf="partial",
            final_leaf="complete",
            expected_sha256=sha256(b"correct").digest(),
            expected_size=7,
            label="test stage",
        )
    assert records[-1].status == "failed"
    assert _values(records[-1], "stage_prefix_read")["logical_bytes"] == 3
    assert (tmp_path / "partial").read_bytes() == b"bad"
    assert not (tmp_path / "complete").exists()


def test_real_protect_replay_has_no_copy_and_records_journal_cost(
    tmp_path: Path,
) -> None:
    adapter = _adapter(tmp_path / "library")
    payload = b"artifact"
    item = _item(1001, payload)
    records: list[IngestMetric] = []
    with summarize_adapter_io(records.append, generation=6):
        _protect(adapter, item, payload, 1)
    with summarize_adapter_io(records.append, generation=7):
        _protect(adapter, item, payload, 1)
    first, replay = records
    assert _values(first, "protect")["calls"] == 1
    assert _values(first, "stage_read")["logical_bytes"] == len(payload)
    assert _values(first, "journal_commit")["calls"] >= 2
    assert _values(first, "journal_session")["calls"] == 2
    assert _values(first, "state_lock_wait")["calls"] == 4
    assert "stage_read" not in {item.operation for item in replay.operations}
    assert _values(replay, "layout")["calls"] == 1


def test_observer_failure_does_not_change_filesystem_result(tmp_path: Path) -> None:
    def broken_sink(_metric: IngestMetric) -> None:
        raise OSError("diagnostic destination unavailable")

    adapter = _adapter(tmp_path / "library")
    with summarize_adapter_io(broken_sink, generation=10):
        _protect(adapter, _item(1002, b"payload"), b"payload", 1)


def test_clock_failure_preserves_adapter_result_and_is_reported() -> None:
    def broken_clock() -> int:
        raise RuntimeError("clock failed")

    records: list[IngestMetric] = []
    with summarize_adapter_io(records.append, generation=11, clock=broken_clock):
        with adapter_phase("stage_read"):
            adapter_bytes("stage_read", 7)
    assert _values(records[0], "stage_read")["logical_bytes"] == 7
    assert {item.name: item.value for item in records[0].counters}["clock_failures"] > 0


@pytest.mark.parametrize("count", [127, 128, 129])
def test_repeated_pages_keep_fixed_totals_without_per_object_state(count: int) -> None:
    records: list[IngestMetric] = []
    with summarize_adapter_io(records.append, generation=12):
        for _ in range(3):
            for _ in range(count):
                with adapter_phase("stage_read"):
                    adapter_bytes("stage_read", 1024)
    assert len(records) == 1
    assert len(records[0].operations) == 1
    assert _values(records[0], "stage_read")["calls"] == 3 * count
    assert _values(records[0], "stage_read")["logical_bytes"] == 3 * count * 1024


def _record_one_read() -> None:
    with adapter_phase("stage_read"):
        adapter_bytes("stage_read", 7)


def test_copied_context_in_another_thread_cannot_mutate_owner_stack() -> None:
    records: list[IngestMetric] = []
    with summarize_adapter_io(records.append, generation=13):
        copied = copy_context()
        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(copied.run, _record_one_read).result()
        _record_one_read()
    assert _values(records[0], "stage_read")["calls"] == 1


def test_expired_copied_context_cannot_emit_or_mutate_previous_measurement() -> None:
    records: list[IngestMetric] = []
    with summarize_adapter_io(records.append, generation=14, interval_ns=0):
        copied = copy_context()
    copied.run(_record_one_read)
    assert len(records) == 1
    assert records[0].operations == ()


def test_async_child_context_cannot_mutate_owner_stack() -> None:
    records: list[IngestMetric] = []

    async def child() -> None:
        _record_one_read()

    async def parent() -> None:
        with summarize_adapter_io(records.append, generation=15):
            await asyncio.create_task(child())
            _record_one_read()

    asyncio.run(parent())
    assert _values(records[0], "stage_read")["calls"] == 1


@pytest.mark.parametrize("value", [None, 7, bytearray(b"unexpected mutable bytes")])
def test_read_observer_preserves_foreign_stream_result_for_caller_validation(
    value: object,
) -> None:
    class ForeignStream:
        def read(self, size: int) -> object:
            assert size == 1
            return value

    records: list[IngestMetric] = []
    with summarize_adapter_io(records.append, generation=16):
        assert adapter_read(cast(BinaryIO, ForeignStream()), 1, "stage_read") is value
    assert _values(records[0], "stage_read")["logical_bytes"] == 0
