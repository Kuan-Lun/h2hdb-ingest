from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from threading import Event

import pytest

from h2hdb_ingest.filesystem import (
    FilesystemCompletionMarker,
    FilesystemObservationError,
    FilesystemSourceChangedError,
    FilesystemStat,
)
from h2hdb_ingest.source_monitor import (
    FilesystemCompletionMarkerProbe,
    SourceChangeMonitor,
    _MarkerIndex,
)
from h2hdb_ingest.source_schedule import SourceScanSchedule


def _marker() -> FilesystemCompletionMarker:
    return FilesystemCompletionMarker(
        FilesystemStat(1, 2, 300, 400, 500), sha256(b"metadata").digest()
    )


def _checkpoint() -> None:
    pass


def test_index_detects_additions_deletions_and_rename_without_retaining_ram_inventory(
    tmp_path: Path,
) -> None:
    index = _MarkerIndex(tmp_path / "markers.sqlite3")
    changed: list[bool] = []

    def inventory() -> Iterator[tuple[tuple[str, ...], FilesystemCompletionMarker]]:
        for number in range(1000):
            yield (str(number),), _marker()

    try:
        index.reconcile(
            inventory(), changed=lambda: changed.append(True), checkpoint=_checkpoint
        )
        assert len(changed) == 1000
        changed.clear()
        index.reconcile(
            inventory(), changed=lambda: changed.append(True), checkpoint=_checkpoint
        )
        assert not changed
        index.reconcile(
            iter([(("renamed",), _marker())]),
            changed=lambda: changed.append(True),
            checkpoint=_checkpoint,
        )
        assert len(changed) == 2  # Addition and the completed removal sweep.
        changed.clear()
        index.reconcile(
            iter(()), changed=lambda: changed.append(True), checkpoint=_checkpoint
        )
        assert len(changed) == 1
        changed.clear()
        index.reconcile(
            iter(()), changed=lambda: changed.append(True), checkpoint=_checkpoint
        )
        assert not changed
    finally:
        index.close()


@pytest.mark.parametrize(
    "new_marker",
    [
        replace(_marker(), file_sha256=sha256(b"changed metadata").digest()),
        replace(_marker(), stat=replace(_marker().stat, modified_ns=401)),
        replace(_marker(), stat=replace(_marker().stat, changed_ns=501)),
        replace(_marker(), stat=replace(_marker().stat, inode=3)),
        replace(_marker(), stat=replace(_marker().stat, device=2)),
        replace(_marker(), stat=replace(_marker().stat, size_bytes=301)),
        replace(_marker(), observation_version=99),
    ],
)
def test_hash_and_stat_independently_detect_completion_marker_changes(
    tmp_path: Path, new_marker: FilesystemCompletionMarker
) -> None:
    index = _MarkerIndex(tmp_path / "markers.sqlite3")
    changed: list[bool] = []
    try:
        for marker in (_marker(), new_marker, new_marker):
            index.reconcile(
                iter([(("gallery",), marker)]),
                changed=lambda: changed.append(True),
                checkpoint=_checkpoint,
            )
        assert len(changed) == 2
    finally:
        index.close()


def test_failed_probe_preserves_prior_complete_inventory(tmp_path: Path) -> None:
    index = _MarkerIndex(tmp_path / "markers.sqlite3")
    changed: list[bool] = []
    baseline = [(("first",), _marker()), (("second",), _marker())]

    def interrupted() -> Iterator[tuple[tuple[str, ...], FilesystemCompletionMarker]]:
        yield ("first",), replace(_marker(), observation_version=99)
        raise FilesystemSourceChangedError("gallery disappeared")

    try:
        index.reconcile(
            iter(baseline), changed=lambda: changed.append(True), checkpoint=_checkpoint
        )
        changed.clear()
        with pytest.raises(FilesystemSourceChangedError, match="disappeared"):
            index.reconcile(
                interrupted(),
                changed=lambda: changed.append(True),
                checkpoint=_checkpoint,
            )
        assert len(changed) == 1
        changed.clear()
        index.reconcile(
            iter(baseline), changed=lambda: changed.append(True), checkpoint=_checkpoint
        )
        assert not changed
    finally:
        index.close()


def test_removal_sweep_checks_stop_between_bounded_pages(tmp_path: Path) -> None:
    index = _MarkerIndex(tmp_path / "markers.sqlite3")

    def baseline() -> Iterator[tuple[tuple[str, ...], FilesystemCompletionMarker]]:
        for number in range(300):
            yield (str(number),), _marker()

    checkpoints = 0

    def stopping() -> None:
        nonlocal checkpoints
        checkpoints += 1
        if checkpoints == 2:
            raise RuntimeError("stop removal sweep")

    try:
        index.reconcile(baseline(), changed=_checkpoint, checkpoint=_checkpoint)
        with pytest.raises(RuntimeError, match="stop removal sweep"):
            index.reconcile(iter(()), changed=_checkpoint, checkpoint=stopping)
        changes: list[bool] = []
        index.reconcile(
            baseline(), changed=lambda: changes.append(True), checkpoint=_checkpoint
        )
        assert not changes
    finally:
        index.close()


def test_monitor_keeps_changes_detected_during_active_startup_scan() -> None:
    observed = Event()
    schedule = SourceScanSchedule(quiet_seconds=300, max_wait_seconds=1800, now=0)
    ticket = schedule.start_scan(now=0)

    @contextmanager
    def probe(
        checkpoint: Callable[[], None],
    ) -> Iterator[Iterator[tuple[tuple[str, ...], FilesystemCompletionMarker]]]:
        checkpoint()
        yield iter([(("gallery",), _marker())])
        observed.set()

    with SourceChangeMonitor(
        probe=probe, schedule=schedule, interval_seconds=30, clock=lambda: 100
    ) as monitor:
        assert observed.wait(2), "metadata probe did not run alongside active scan"
        monitor.raise_if_failed()
        assert schedule.next_scan_at() is None
        schedule.finish_scan(ticket, now=200, succeeded=True)
        assert schedule.next_scan_at() == 400
    monitor.raise_if_failed()


def test_raw_empty_partial_and_complete_markers_extend_the_same_quiet_window(
    tmp_path: Path,
) -> None:
    root = tmp_path / "download"
    gallery = root / "1001"
    gallery.mkdir(parents=True)
    (gallery / "001.jpg").write_bytes(b"image bytes are already complete")
    marker = gallery / "galleryinfo.txt"
    probe = FilesystemCompletionMarkerProbe(root)
    index = _MarkerIndex(tmp_path / "markers.sqlite3")
    schedule = SourceScanSchedule(quiet_seconds=300, max_wait_seconds=1800, now=0)
    startup = schedule.start_scan(now=0)
    schedule.finish_scan(startup, now=1, succeeded=True)
    now = 100

    def changed() -> None:
        schedule.note_change(now=now)

    try:
        for observed_at, content, deadline in (
            (100, b"", 400),
            (200, b"Title: Still writing", 500),
            (
                250,
                b"Title: Complete\nUpload Time: 2024-01-02 03:04\n"
                b"Uploaded By: uploader\nDownloaded: 2024-02-03 04:05\n"
                b"Tags: artist:artist\nUploader's Comments\nDone\n"
                b"Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3",
                550,
            ),
        ):
            now = observed_at
            marker.write_bytes(content)
            with probe(_checkpoint) as markers:
                index.reconcile(markers, changed=changed, checkpoint=_checkpoint)
            assert schedule.next_scan_at() == deadline
        now = 300
        with probe(_checkpoint) as markers:
            index.reconcile(markers, changed=changed, checkpoint=_checkpoint)
        assert schedule.next_scan_at() == 550
    finally:
        index.close()


def test_new_download_schedules_a_scan_only_when_its_first_marker_appears(
    tmp_path: Path,
) -> None:
    root = tmp_path / "download"
    gallery = root / "2024" / "1001"
    gallery.mkdir(parents=True)
    image = gallery / "001.jpg"
    image.write_bytes(b"new download in progress")
    probe = FilesystemCompletionMarkerProbe(root)
    index = _MarkerIndex(tmp_path / "markers.sqlite3")
    schedule = SourceScanSchedule(quiet_seconds=300, max_wait_seconds=1800, now=0)
    startup = schedule.start_scan(now=0)
    schedule.finish_scan(startup, now=1, succeeded=True)
    now = 100

    def changed() -> None:
        schedule.note_change(now=now)

    try:
        for observed_at in (100, 200):
            now = observed_at
            image.write_bytes(f"still downloading at {now}".encode())
            with probe(_checkpoint) as markers:
                index.reconcile(markers, changed=changed, checkpoint=_checkpoint)
            assert schedule.next_scan_at() is None
        now = 300
        (gallery / "galleryinfo.txt").write_bytes(b"producer completion marker")
        with probe(_checkpoint) as markers:
            index.reconcile(markers, changed=changed, checkpoint=_checkpoint)
        assert schedule.next_scan_at() == 600
        now = 400
        with probe(_checkpoint) as markers:
            index.reconcile(markers, changed=changed, checkpoint=_checkpoint)
        assert schedule.next_scan_at() == 600
    finally:
        index.close()


def test_monitor_retries_transient_mutation_and_retains_dirty_generation(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger="h2hdb_ingest.source_monitor")
    retried = Event()
    attempts = 0
    schedule = SourceScanSchedule(quiet_seconds=300, max_wait_seconds=1800, now=0)
    ticket = schedule.start_scan(now=0)
    schedule.finish_scan(ticket, now=1, succeeded=True)

    @contextmanager
    def probe(
        checkpoint: Callable[[], None],
    ) -> Iterator[Iterator[tuple[tuple[str, ...], FilesystemCompletionMarker]]]:
        nonlocal attempts
        checkpoint()
        attempts += 1
        if attempts == 1:
            raise FilesystemSourceChangedError("marker changed")
        yield iter([(("gallery",), _marker())])
        retried.set()

    with SourceChangeMonitor(
        probe=probe, schedule=schedule, interval_seconds=0.001, clock=lambda: 100
    ) as monitor:
        assert retried.wait(2), "metadata monitor did not retry transient mutation"
        monitor.raise_if_failed()
        assert schedule.next_scan_at() == 400
    monitor.raise_if_failed()
    assert [(record.levelno, record.getMessage()) for record in caplog.records] == [
        (logging.DEBUG, "source changed during metadata probe; retrying")
    ]


def test_monitor_reports_unsafe_source_as_fatal() -> None:
    attempted = Event()
    schedule = SourceScanSchedule(quiet_seconds=300, max_wait_seconds=1800, now=0)

    @contextmanager
    def probe(
        checkpoint: Callable[[], None],
    ) -> Iterator[Iterator[tuple[tuple[str, ...], FilesystemCompletionMarker]]]:
        def markers() -> Iterator[tuple[tuple[str, ...], FilesystemCompletionMarker]]:
            checkpoint()
            attempted.set()
            raise FilesystemObservationError("unsafe source")

        yield markers()

    with SourceChangeMonitor(
        probe=probe, schedule=schedule, interval_seconds=30
    ) as monitor:
        assert attempted.wait(2), "metadata probe did not run"
    with pytest.raises(FilesystemObservationError, match="unsafe source"):
        monitor.raise_if_failed()


def test_monitor_shutdown_interrupts_stream_and_closes_probe() -> None:
    started = Event()
    closed = Event()
    schedule = SourceScanSchedule(quiet_seconds=300, max_wait_seconds=1800, now=0)

    @contextmanager
    def probe(
        checkpoint: Callable[[], None],
    ) -> Iterator[Iterator[tuple[tuple[str, ...], FilesystemCompletionMarker]]]:
        def markers() -> Iterator[tuple[tuple[str, ...], FilesystemCompletionMarker]]:
            started.set()
            number = 0
            while True:
                checkpoint()
                yield (str(number),), _marker()
                number += 1

        try:
            yield markers()
        finally:
            closed.set()

    with SourceChangeMonitor(
        probe=probe, schedule=schedule, interval_seconds=30
    ) as monitor:
        assert started.wait(2), "metadata probe did not start"
    assert closed.is_set()
    monitor.raise_if_failed()
