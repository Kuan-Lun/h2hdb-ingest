"""Metadata-only source probes with a disk-backed, process-local comparison index."""

from __future__ import annotations

import json
import logging
import sqlite3
import tempfile
from collections.abc import Callable, Iterator
from pathlib import Path
from threading import Event, Lock, Thread
from time import monotonic
from typing import Protocol, Self

from .filesystem import (
    FilesystemCompletionMarker,
    FilesystemSource,
    FilesystemSourceChangedError,
)
from .source_schedule import SourceScanSchedule

logger = logging.getLogger(__name__)


class CompletionMarkerProbe(Protocol):
    """Yield a complete marker inventory without opening source images."""

    def __call__(
        self, checkpoint: Callable[[], None]
    ) -> Iterator[tuple[tuple[str, ...], FilesystemCompletionMarker]]: ...


class FilesystemCompletionMarkerProbe:
    def __init__(self, source_root: Path) -> None:
        self._source_root = source_root

    def __call__(
        self, checkpoint: Callable[[], None]
    ) -> Iterator[tuple[tuple[str, ...], FilesystemCompletionMarker]]:
        with FilesystemSource(self._source_root, checkpoint=checkpoint) as source:
            yield from source.iter_completion_markers()


class _SourceProbeStopped(Exception):
    pass


class _MarkerIndex:
    """Own only disposable monitor state; never share an ingest DB connection."""

    def __init__(self, path: Path) -> None:
        self._connection = sqlite3.connect(path)
        self._connection.execute(
            "CREATE TABLE marker (locator TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, "
            "seen INTEGER NOT NULL) WITHOUT ROWID"
        )
        self._generation = 0

    def close(self) -> None:
        self._connection.close()

    def reconcile(
        self,
        markers: Iterator[tuple[tuple[str, ...], FilesystemCompletionMarker]],
        *,
        changed: Callable[[], None],
        checkpoint: Callable[[], None],
    ) -> None:
        self._generation += 1
        with self._connection:
            for locator, marker in markers:
                checkpoint()
                key = json.dumps(locator, ensure_ascii=True, separators=(",", ":"))
                observed_stat = marker.stat
                fingerprint = json.dumps(
                    (
                        marker.observation_version,
                        observed_stat.device,
                        observed_stat.inode,
                        observed_stat.size_bytes,
                        observed_stat.modified_ns,
                        observed_stat.changed_ns,
                        marker.file_sha256.hex(),
                    ),
                    separators=(",", ":"),
                )
                old = self._connection.execute(
                    "SELECT fingerprint FROM marker WHERE locator = ?", (key,)
                ).fetchone()
                if old is None or old[0] != fingerprint:
                    # This includes the initial inventory. If it finishes after
                    # startup ingest began, its generation must remain pending.
                    changed()
                self._connection.execute(
                    "INSERT INTO marker VALUES (?, ?, ?) "
                    "ON CONFLICT(locator) DO UPDATE SET "
                    "fingerprint = excluded.fingerprint, seen = excluded.seen",
                    (key, fingerprint, self._generation),
                )
            after = ""
            while True:
                checkpoint()
                missing = self._connection.execute(
                    "SELECT locator FROM marker WHERE seen != ? AND locator > ? "
                    "ORDER BY locator LIMIT 128",
                    (self._generation, after),
                ).fetchall()
                if not missing:
                    break
                if not after:
                    changed()
                self._connection.executemany(
                    "DELETE FROM marker WHERE locator = ?", missing
                )
                after = missing[-1][0]


class SourceChangeMonitor:
    """Continue bounded-memory marker probing while resident ingest is busy."""

    def __init__(
        self,
        *,
        probe: CompletionMarkerProbe,
        schedule: SourceScanSchedule,
        interval_seconds: float,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        self._probe = probe
        self._schedule = schedule
        self._interval_seconds = interval_seconds
        self._clock = clock
        self._stop = Event()
        self._failure_lock = Lock()
        self._failure: BaseException | None = None
        self._thread = Thread(target=self._run, name="h2hdb-source-monitor")

    def __enter__(self) -> Self:
        self._thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._stop.set()
        self._thread.join()

    def raise_if_failed(self) -> None:
        with self._failure_lock:
            failure = self._failure
        if failure is not None:
            raise failure

    def _checkpoint(self) -> None:
        if self._stop.is_set():
            raise _SourceProbeStopped

    def _changed(self) -> None:
        self._schedule.note_change(now=self._clock())

    def _run(self) -> None:
        try:
            with tempfile.TemporaryDirectory(prefix="h2hdb-source-monitor-") as folder:
                index = _MarkerIndex(Path(folder) / "markers.sqlite3")
                try:
                    while not self._stop.is_set():
                        try:
                            index.reconcile(
                                self._probe(self._checkpoint),
                                changed=self._changed,
                                checkpoint=self._checkpoint,
                            )
                        except FilesystemSourceChangedError:
                            # An incomplete pass cannot establish absence. Its
                            # comparison transaction rolls back, and both scan
                            # and probe will retry without restarting the process.
                            self._changed()
                            logger.info(
                                "source changed during metadata probe; retrying"
                            )
                        if self._stop.wait(self._interval_seconds):
                            break
                finally:
                    index.close()
        except _SourceProbeStopped:
            pass
        except BaseException as error:
            with self._failure_lock:
                self._failure = error
