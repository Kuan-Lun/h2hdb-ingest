"""Measure a disposable-index durability candidate without changing production.

Run with ``pytest -n 0 -s --basetemp=...`` to retain the printed JSON reports.
The 122-page case requires explicit ``-m deep``. Real local SQLite commits are
timed in AB/BA order; no synthetic latency or wall-time pass threshold is used.
These results do not establish NAS performance or a power-loss safety contract.
"""

from __future__ import annotations

import json
import platform
import random
import sqlite3
import tempfile
from dataclasses import asdict, dataclass
from hashlib import sha256
from pathlib import Path
from time import perf_counter_ns, process_time_ns
from typing import Literal

import pytest
import test_source_snapshot as snapshot_contract
import test_source_snapshot_batches as batch_contract
from h2hdb import FileContentReceipt

import h2hdb_ingest.source_snapshot as snapshot_module
from h2hdb_ingest.filesystem import FilesystemFileObservation
from h2hdb_ingest.source_performance import SourcePerformance
from h2hdb_ingest.source_snapshot import (
    SNAPSHOT_CAPTURE_PAGE_SIZE,
    SourceSnapshotStore,
)

_Variant = Literal["default", "off", "per_file_negative_control"]
_CYCLES = 3
_LOCATOR = ("gallery",)


class _MeasuredConnection(sqlite3.Connection):
    commit_ns: int = 0
    begins: int = 0
    commits: int = 0
    inserts: int = 0

    def trace(self, sql: str) -> None:
        match sql.split()[0]:
            case "BEGIN":
                self.begins += 1
            case "COMMIT":
                self.commits += 1
            case "INSERT":
                self.inserts += 1

    def reset_measurements(self) -> None:
        self.commit_ns = self.begins = self.commits = self.inserts = 0

    def commit(self) -> None:
        started = perf_counter_ns()
        try:
            super().commit()
        finally:
            self.commit_ns += perf_counter_ns() - started


@dataclass(frozen=True)
class _Cycle:
    cycle: int
    wall_ns: int
    process_cpu_ns: int
    sqlite_commit_ns: int
    metric_commit_ns: int
    commits: int
    begins: int
    inserts: int
    indexed_rows: int
    verified_bytes: int
    exact_output_sha256: str


@dataclass(frozen=True)
class _Run:
    variant: _Variant
    synchronous: int
    journal_mode: str
    page_size: int
    cycles: tuple[_Cycle, ...]


def _fixture(root: Path, count: int) -> tuple[FilesystemFileObservation, ...]:
    root.mkdir()
    payload = random.Random(20260919).randbytes(1024)
    result = []
    for position in range(count):
        path = root / f"{position:08d}.jpg"
        path.write_bytes(position.to_bytes(8, "big") + payload[8:])
        result.append(snapshot_contract._observation(path))
    return tuple(result)


def _verify_cycle(
    store: SourceSnapshotStore,
    connection: sqlite3.Connection,
    observations: tuple[FilesystemFileObservation, ...],
    receipts: list[FileContentReceipt],
    cycle: int,
) -> tuple[int, int, str]:
    rows = connection.execute(
        "SELECT ordinal, locator, name, size_bytes, sha256 FROM source ORDER BY name"
    ).fetchall()
    assert len(rows) == len(observations) == len(receipts)
    output = sha256()
    verified_bytes = 0
    for position, (observation, receipt, row) in enumerate(
        zip(observations, receipts, rows, strict=True)
    ):
        expected = observation.path.read_bytes()
        assert receipt == FileContentReceipt.from_parts((expected,))
        assert batch_contract._contents(store, observation.name_bytes) == expected
        assert row == (
            cycle * len(observations) + position,
            '["gallery"]',
            observation.name_bytes,
            len(expected),
            receipt.file_sha256,
        )
        output.update(observation.name_bytes)
        output.update(receipt.file_sha256)
        output.update(receipt.size_bytes.to_bytes(8, "big"))
        verified_bytes += len(expected)
    return len(rows), verified_bytes, output.hexdigest()


def _require_page_cost(run: _Run, count: int) -> None:
    pages = (count + SNAPSHOT_CAPTURE_PAGE_SIZE - 1) // SNAPSHOT_CAPTURE_PAGE_SIZE
    for cycle in run.cycles:
        assert cycle.commits == pages, "commit count exceeded bounded page model"
        assert cycle.begins == pages, "transaction count exceeded bounded page model"
        assert cycle.inserts == count


def _capture_run(
    observations: tuple[FilesystemFileObservation, ...],
    variant: _Variant,
    monkeypatch: pytest.MonkeyPatch,
) -> _Run:
    original_connect = sqlite3.connect
    connections: list[_MeasuredConnection] = []

    def connect(path: Path) -> _MeasuredConnection:
        connection = original_connect(path, factory=_MeasuredConnection)
        if variant == "off":
            connection.execute("PRAGMA synchronous = OFF")
        connection.set_trace_callback(connection.trace)
        connections.append(connection)
        return connection

    cycles: list[_Cycle] = []
    with monkeypatch.context() as patch:
        patch.setattr(sqlite3, "connect", connect)
        with SourceSnapshotStore() as store:
            (connection,) = connections
            synchronous = int(connection.execute("PRAGMA synchronous").fetchone()[0])
            journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0])
            page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
            if variant == "off":
                assert synchronous == 0
            bound = (
                1
                if variant == "per_file_negative_control"
                else SNAPSHOT_CAPTURE_PAGE_SIZE
            )
            for cycle in range(_CYCLES):
                performance = SourcePerformance()
                connection.reset_measurements()
                receipts: list[FileContentReceipt] = []
                started = perf_counter_ns()
                cpu_started = process_time_ns()
                for start in range(0, len(observations), bound):
                    receipts.extend(
                        store.capture_many(
                            _LOCATOR,
                            observations[start : start + bound],
                            performance=performance,
                        )
                    )
                cpu_ns = process_time_ns() - cpu_started
                wall_ns = perf_counter_ns() - started
                metric = performance.metric(status="completed")
                phases = {value.name: value.value for value in metric.phases_ns}
                counters = {value.name: value.value for value in metric.counters}
                assert counters["snapshot_index_commit_calls"] == connection.commits
                # Verification and source fixture creation are outside capture timing.
                indexed, verified_bytes, digest = _verify_cycle(
                    store, connection, observations, receipts, cycle
                )
                cycles.append(
                    _Cycle(
                        cycle,
                        wall_ns,
                        cpu_ns,
                        connection.commit_ns,
                        phases["snapshot_index_commit"],
                        connection.commits,
                        connection.begins,
                        connection.inserts,
                        indexed,
                        verified_bytes,
                        digest,
                    )
                )
    return _Run(variant, synchronous, journal_mode, page_size, tuple(cycles))


def _experiment(
    root: Path, count: int, monkeypatch: pytest.MonkeyPatch, *, negative_control: bool
) -> None:
    observations = _fixture(root / "sources", count)
    scratch = root / "scratch"
    scratch.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(scratch))
    order: tuple[_Variant, ...] = ("default", "off", "off", "default")
    runs = [_capture_run(observations, variant, monkeypatch) for variant in order]
    for run in runs:
        _require_page_cost(run, count)
    expected_output = {
        (cycle.indexed_rows, cycle.verified_bytes, cycle.exact_output_sha256)
        for run in runs
        for cycle in run.cycles
    }
    assert len(expected_output) == 1
    rejected = False
    if negative_control:
        degraded = _capture_run(observations, "per_file_negative_control", monkeypatch)
        with pytest.raises(AssertionError, match="bounded page model"):
            _require_page_cost(degraded, count)
        assert {
            (cycle.indexed_rows, cycle.verified_bytes, cycle.exact_output_sha256)
            for cycle in degraded.cycles
        } == expected_output
        runs.append(degraded)
        rejected = True
    assert tuple(scratch.iterdir()) == ()
    source_path = snapshot_module.__file__
    assert source_path is not None
    report = {
        "status": "completed",
        "scope": "local_disposable_snapshot_index",
        "members": count,
        "member_bytes": 1024,
        "cycles_per_run": _CYCLES,
        "order": order,
        "runs": [asdict(run) for run in runs],
        "negative_control_rejected": rejected,
        "scratch_removed": True,
        "source_sha256": sha256(Path(source_path).read_bytes()).hexdigest(),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "sqlite": sqlite3.sqlite_version,
        "workspace": str(root),
        "timing_scope": "capture only; fixtures, byte/index oracle and close excluded",
        "limitations": ["not isolated", "no NAS measurement", "no power-loss test"],
    }
    output = root / "durability-cost.json"
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"report": str(output), **report}, sort_keys=True))


@pytest.mark.parametrize("count", (127, 128, 129))
def test_durability_candidate_preserves_bounded_cost_and_exact_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, count: int
) -> None:
    _experiment(tmp_path, count, monkeypatch, negative_control=True)


@pytest.mark.deep
def test_durability_candidate_122_page_experiment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _experiment(
        tmp_path,
        122 * SNAPSHOT_CAPTURE_PAGE_SIZE,
        monkeypatch,
        negative_control=False,
    )


@pytest.mark.parametrize(
    "failure", ("spool", "write", "before_commit", "after_commit", "corrupt")
)
def test_off_candidate_preserves_existing_failure_contracts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    original_connect = sqlite3.connect

    def connect(
        path: Path, *, factory: type[sqlite3.Connection] = sqlite3.Connection
    ) -> sqlite3.Connection:
        connection = original_connect(path, factory=factory)
        connection.execute("PRAGMA synchronous = OFF")
        return connection

    monkeypatch.setattr(sqlite3, "connect", connect)
    match failure:
        case "spool":
            batch_contract.test_failed_spool_rolls_back_the_whole_page_and_preserves_previous_bytes(
                tmp_path
            )
        case "corrupt":
            snapshot_contract.test_private_capture_corruption_is_detected_before_render(
                tmp_path, monkeypatch
            )
        case _:
            batch_contract.test_partial_index_failure_and_commit_response_loss_keep_readable_authority(
                tmp_path, monkeypatch, failure
            )
