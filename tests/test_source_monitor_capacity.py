from __future__ import annotations

import errno
import json
import logging
import sqlite3
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path
from threading import Event, get_ident
from time import monotonic

import pytest

import h2hdb_ingest.source_monitor as monitor_module
from h2hdb_ingest.filesystem import FilesystemCompletionMarker, FilesystemStat
from h2hdb_ingest.source_monitor import (
    FilesystemCompletionMarkerProbe,
    SourceChangeMonitor,
    _MarkerIndex,
)
from h2hdb_ingest.source_schedule import SourceScanSchedule

type MarkerInventory = Iterator[tuple[tuple[str, ...], FilesystemCompletionMarker]]


def _capacity_error(kind: str) -> BaseException:
    if kind == "sqlite_full":
        error = sqlite3.OperationalError("database or disk is full")
        error.sqlite_errorcode = sqlite3.SQLITE_FULL
        return error
    return OSError(getattr(errno, kind), "injected storage capacity failure")


def _monitor(root: Path, *, interval_seconds: float = 0.001) -> SourceChangeMonitor:
    gallery = root / "1001"
    gallery.mkdir()
    (gallery / "galleryinfo.txt").write_bytes(b"completion marker")
    return SourceChangeMonitor(
        probe=FilesystemCompletionMarkerProbe(root),
        schedule=SourceScanSchedule(quiet_seconds=1, max_wait_seconds=10, now=0),
        interval_seconds=interval_seconds,
    )


def _assert_capacity_logs(
    caplog: pytest.LogCaptureFixture, *, recovered: bool, index_known: bool = True
) -> None:
    records = [
        record for record in caplog.records if record.name == monitor_module.__name__
    ]
    warnings = [record for record in records if record.levelno == logging.WARNING]
    assert len(warnings) == 1
    message = warnings[0].getMessage()
    prefix = "Storage capacity exhausted: "
    assert message.startswith(prefix)
    detail = json.loads(message.removeprefix(prefix))
    assert detail["operation"] == "source metadata probe"
    assert detail["scratch_directory"] == tempfile.gettempdir()
    assert detail["scratch_free_bytes"] is None or detail["scratch_free_bytes"] >= 0
    assert detail["reason"]
    assert detail["error_type"] in ("OSError", "OperationalError")
    assert detail["error_has_filename"] is False
    if index_known:
        index_path = Path(detail["index_path"])
        assert index_path.name == "markers.sqlite3"
        assert index_path.parent.name.startswith("h2hdb-source-monitor-")
        assert detail["working_directory"] == str(index_path.parent)
    else:
        assert "index_path" not in detail
        assert "working_directory" not in detail
    assert detail["action"] == (
        "preserve gallery eligibility and retry durable work when space is available"
    )
    resumed = [record for record in records if "probe recovered" in record.getMessage()]
    assert len(resumed) == int(recovered)
    assert all(record.levelno == logging.INFO for record in resumed)


@pytest.mark.parametrize("kind", ["ENOSPC", "EDQUOT", "sqlite_full"])
def test_consumer_capacity_retries_without_log_flood_and_recovers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    kind: str,
) -> None:
    caplog.set_level(logging.INFO, logger=monitor_module.__name__)
    monitor = _monitor(tmp_path)
    reconcile = _MarkerIndex.reconcile
    complete = Event()
    attempts = 0
    closed_threads: list[int] = []
    real_probe = FilesystemCompletionMarkerProbe(tmp_path)

    @contextmanager
    def tracked_probe(checkpoint: Callable[[], None]) -> Iterator[MarkerInventory]:
        try:
            with real_probe(checkpoint) as markers:
                yield markers
        finally:
            closed_threads.append(get_ident())

    def limited_consumer(
        index: _MarkerIndex,
        markers: MarkerInventory,
        *,
        changed: Callable[[], None],
        checkpoint: Callable[[], None],
    ) -> None:
        nonlocal attempts
        attempts += 1

        def may_fail() -> None:
            checkpoint()
            if attempts <= 2:
                raise _capacity_error(kind)

        reconcile(index, markers, changed=changed, checkpoint=may_fail)
        complete.set()
        assert monitor._stop.wait(2), "successful probe was not stopped"

    monitor._probe = tracked_probe
    monkeypatch.setattr(_MarkerIndex, "reconcile", limited_consumer)
    with monitor:
        assert complete.wait(2), "capacity failure prevented a later complete probe"
    monitor.raise_if_failed()
    assert attempts == 3
    assert len(closed_threads) == 3
    assert len(set(closed_threads)) == 1
    assert closed_threads[0] != get_ident()
    _assert_capacity_logs(caplog, recovered=True)


@pytest.mark.parametrize("stage", ["temporary_directory", "sqlite_schema"])
def test_initial_capacity_failure_rebuilds_index_and_then_probes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    stage: str,
) -> None:
    caplog.set_level(logging.INFO, logger=monitor_module.__name__)
    monitor = _monitor(tmp_path)
    complete = Event()
    reconcile = _MarkerIndex.reconcile
    temporary = tempfile.TemporaryDirectory
    connect = sqlite3.connect
    attempts = 0
    failed_connections: list[sqlite3.Connection] = []
    checked_closed_threads: list[int] = []

    def constrained_temporary(*, prefix: str) -> tempfile.TemporaryDirectory[str]:
        nonlocal attempts
        if prefix == "h2hdb-source-monitor-":
            attempts += 1
            if attempts == 1:
                raise OSError(errno.ENOSPC, "cannot create monitor scratch directory")
        return temporary(prefix=prefix)

    def constrained_connection(path: Path) -> sqlite3.Connection:
        nonlocal attempts
        connection = connect(path)
        if path.name == "markers.sqlite3":
            attempts += 1
            if attempts == 1:
                connection.execute("PRAGMA max_page_count = 1")
                failed_connections.append(connection)
            else:
                for failed in failed_connections:
                    with pytest.raises(
                        sqlite3.ProgrammingError, match="closed database"
                    ):
                        failed.execute("SELECT 1")
                    checked_closed_threads.append(get_ident())
        return connection

    def complete_probe(
        index: _MarkerIndex,
        markers: MarkerInventory,
        *,
        changed: Callable[[], None],
        checkpoint: Callable[[], None],
    ) -> None:
        reconcile(index, markers, changed=changed, checkpoint=checkpoint)
        complete.set()
        assert monitor._stop.wait(2), "rebuilt probe was not stopped"

    if stage == "temporary_directory":
        monkeypatch.setattr(tempfile, "TemporaryDirectory", constrained_temporary)
    else:
        monkeypatch.setattr(sqlite3, "connect", constrained_connection)
    monkeypatch.setattr(_MarkerIndex, "reconcile", complete_probe)
    with monitor:
        assert complete.wait(2), (
            "monitor initialization did not recover from capacity failure"
        )
    monitor.raise_if_failed()
    assert attempts == 2
    assert len(checked_closed_threads) == int(stage == "sqlite_schema")
    assert all(owner != get_ident() for owner in checked_closed_threads)
    _assert_capacity_logs(caplog, recovered=True, index_known=stage == "sqlite_schema")


def test_capacity_wait_is_interrupted_by_stop_without_another_probe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger=monitor_module.__name__)
    monitor = _monitor(tmp_path, interval_seconds=3600)
    attempted = Event()
    attempts = 0
    reconcile = _MarkerIndex.reconcile

    def full_consumer(
        index: _MarkerIndex,
        markers: MarkerInventory,
        *,
        changed: Callable[[], None],
        checkpoint: Callable[[], None],
    ) -> None:
        nonlocal attempts
        attempts += 1

        def fail() -> None:
            attempted.set()
            raise OSError(errno.EDQUOT, "scratch quota exhausted")

        reconcile(index, markers, changed=changed, checkpoint=fail)

    monkeypatch.setattr(_MarkerIndex, "reconcile", full_consumer)
    with monitor:
        assert attempted.wait(2), "capacity fault was not reached"
        before_stop = monotonic()
    assert monotonic() - before_stop < 2
    monitor.raise_if_failed()
    assert attempts == 1
    _assert_capacity_logs(caplog, recovered=False)


def test_retry_keeps_complete_inventory_after_partial_capacity_failure(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger=monitor_module.__name__)
    marker = FilesystemCompletionMarker(
        FilesystemStat(1, 2, 300, 400, 500), sha256(b"metadata").digest()
    )
    complete = Event()
    observations: list[tuple[int, int]] = []
    attempts = 0
    reconcile = _MarkerIndex.reconcile

    @contextmanager
    def probe(checkpoint: Callable[[], None]) -> Iterator[MarkerInventory]:
        checkpoint()
        yield iter([(("first",), marker), (("second",), marker)])

    monitor = SourceChangeMonitor(
        probe=probe,
        schedule=SourceScanSchedule(quiet_seconds=1, max_wait_seconds=10, now=0),
        interval_seconds=0.001,
    )

    def interrupted_consumer(
        index: _MarkerIndex,
        markers: MarkerInventory,
        *,
        changed: Callable[[], None],
        checkpoint: Callable[[], None],
    ) -> None:
        nonlocal attempts
        attempts += 1
        checkpoints = 0
        changes = 0

        def may_fail() -> None:
            nonlocal checkpoints
            checkpoint()
            checkpoints += 1
            if attempts == 2 and checkpoints == 2:
                raise OSError(errno.ENOSPC, "partial inventory could not be persisted")

        def record_change() -> None:
            nonlocal changes
            changes += 1
            changed()

        try:
            reconcile(index, markers, changed=record_change, checkpoint=may_fail)
        finally:
            observations.append((attempts, changes))
        if attempts == 3:
            complete.set()
            assert monitor._stop.wait(2), "recovered inventory was not stopped"

    monkeypatch.setattr(_MarkerIndex, "reconcile", interrupted_consumer)
    with monitor:
        assert complete.wait(2), "partial inventory failure was not retried"
    monitor.raise_if_failed()
    assert observations == [(1, 2), (2, 0), (3, 0)]
    _assert_capacity_logs(caplog, recovered=True)
