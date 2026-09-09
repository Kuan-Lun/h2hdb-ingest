from __future__ import annotations

import errno
import gc
import sqlite3
import sys
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field, replace
from hashlib import sha256
from pathlib import Path
from threading import Event, get_ident

import pytest

from h2hdb_ingest.filesystem import (
    FilesystemCompletionMarker,
    FilesystemSource,
    FilesystemStat,
)
from h2hdb_ingest.source_monitor import (
    FilesystemCompletionMarkerProbe,
    SourceChangeMonitor,
    _MarkerIndex,
)
from h2hdb_ingest.source_schedule import SourceScanSchedule

type MarkerInventory = Iterator[tuple[tuple[str, ...], FilesystemCompletionMarker]]


@dataclass
class _SourceLifetime:
    created_threads: list[int] = field(default_factory=list)
    closed_threads: list[int] = field(default_factory=list)
    temporary_paths: list[Path] = field(default_factory=list)
    closed: Event = field(default_factory=Event)

    def assert_closed_in_owner(self) -> None:
        assert self.created_threads
        assert self.closed_threads == self.created_threads
        assert all(owner != get_ident() for owner in self.created_threads)
        assert all(not path.exists() for path in self.temporary_paths)


@pytest.fixture
def source_lifetime(monkeypatch: pytest.MonkeyPatch) -> _SourceLifetime:
    lifetime = _SourceLifetime()
    build_index = FilesystemSource._build_discovery_index
    close_source = FilesystemSource.close

    def tracked_index(source: FilesystemSource) -> sqlite3.Connection:
        connection = build_index(source)
        lifetime.created_threads.append(get_ident())
        temporary = source._discovery_temporary
        assert temporary is not None
        lifetime.temporary_paths.append(Path(temporary.name))
        return connection

    def tracked_close(source: FilesystemSource) -> None:
        close_source(source)
        assert source._discovery_connection is None
        assert source._discovery_temporary is None
        lifetime.closed_threads.append(get_ident())
        lifetime.closed.set()

    monkeypatch.setattr(FilesystemSource, "_build_discovery_index", tracked_index)
    monkeypatch.setattr(FilesystemSource, "close", tracked_close)
    return lifetime


def _monitor(root: Path, *, interval_seconds: float = 30) -> SourceChangeMonitor:
    for number in (1001, 1002):
        gallery = root / str(number)
        gallery.mkdir()
        (gallery / "galleryinfo.txt").write_bytes(b"completed metadata")
    return SourceChangeMonitor(
        probe=FilesystemCompletionMarkerProbe(root),
        schedule=SourceScanSchedule(quiet_seconds=1, max_wait_seconds=10, now=0),
        interval_seconds=interval_seconds,
    )


def test_complete_filesystem_probe_closes_sqlite_in_its_worker(
    tmp_path: Path,
    source_lifetime: _SourceLifetime,
) -> None:
    with _monitor(tmp_path) as monitor:
        assert source_lifetime.closed.wait(2), "complete probe did not close its source"
    monitor.raise_if_failed()
    source_lifetime.assert_closed_in_owner()


def test_stop_after_yield_closes_filesystem_probe_before_worker_exits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_lifetime: _SourceLifetime,
) -> None:
    reached = Event()
    reconcile = _MarkerIndex.reconcile
    monitor = _monitor(tmp_path)

    def wait_at_consumer(
        index: _MarkerIndex,
        markers: MarkerInventory,
        *,
        changed: Callable[[], None],
        checkpoint: Callable[[], None],
    ) -> None:
        def stopping_checkpoint() -> None:
            reached.set()
            assert monitor._stop.wait(2), "monitor stop was not requested"
            checkpoint()

        reconcile(index, markers, changed=changed, checkpoint=stopping_checkpoint)

    monkeypatch.setattr(_MarkerIndex, "reconcile", wait_at_consumer)
    with monitor:
        assert reached.wait(2), "probe did not reach its post-yield consumer checkpoint"
    monitor.raise_if_failed()
    source_lifetime.assert_closed_in_owner()


def test_consumer_io_error_preserves_failure_and_closes_source_before_main_thread_gc(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_lifetime: _SourceLifetime,
) -> None:
    expected_failure = OSError(errno.EIO, "injected marker-index write failure")
    reconcile = _MarkerIndex.reconcile
    unraisable: list[object] = []
    monkeypatch.setattr(sys, "unraisablehook", unraisable.append)

    def fail_after_marker_yield(
        index: _MarkerIndex,
        markers: MarkerInventory,
        *,
        changed: Callable[[], None],
        checkpoint: Callable[[], None],
    ) -> None:
        def failed_checkpoint() -> None:
            raise expected_failure

        reconcile(index, markers, changed=changed, checkpoint=failed_checkpoint)

    with monkeypatch.context() as failure_patch:
        failure_patch.setattr(_MarkerIndex, "reconcile", fail_after_marker_yield)
        with _monitor(tmp_path) as monitor:
            assert source_lifetime.closed.wait(2), "failed consumer retained the source"
        with pytest.raises(OSError, match="marker-index write failure") as raised:
            monitor.raise_if_failed()
        assert raised.value is expected_failure
        source_lifetime.assert_closed_in_owner()

    expected_failure.__traceback__ = None
    del monitor, raised, fail_after_marker_yield
    gc.collect()
    assert not unraisable
    source_lifetime.assert_closed_in_owner()


def test_consumer_failure_rolls_back_partial_inventory_without_reporting_deletions(
    tmp_path: Path,
) -> None:
    marker = FilesystemCompletionMarker(
        FilesystemStat(1, 2, 300, 400, 500), sha256(b"metadata").digest()
    )
    baseline = [(("first",), marker), (("second",), marker)]
    index = _MarkerIndex(tmp_path / "markers.sqlite3")
    changes: list[bool] = []
    checkpoints = 0

    def checkpoint() -> None:
        nonlocal checkpoints
        checkpoints += 1
        if checkpoints == 2:
            raise OSError(errno.ENOSPC, "index write failed after first observation")

    def noop() -> None:
        pass

    try:
        index.reconcile(iter(baseline), changed=noop, checkpoint=noop)
        incomplete = [
            (("first",), replace(marker, observation_version=99)),
            (("new",), marker),
        ]
        with pytest.raises(OSError, match="after first observation"):
            index.reconcile(
                iter(incomplete),
                changed=lambda: changes.append(True),
                checkpoint=checkpoint,
            )
        assert changes == [True]
        changes.clear()
        index.reconcile(
            iter(baseline), changed=lambda: changes.append(True), checkpoint=noop
        )
        assert changes == []
    finally:
        index.close()
