"""Disposable exact source bytes shared by observation and artifact preparation."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
from collections.abc import Generator, Iterator
from contextlib import closing
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import BinaryIO, Final, Self, cast

from h2hdb import FileContentReceipt

from ._adapter_performance import (
    adapter_bytes,
    adapter_operation,
    adapter_phase,
    adapter_read,
)
from .filesystem import FilesystemFileObservation
from .source_performance import SourcePerformance

SNAPSHOT_CAPTURE_PAGE_SIZE: Final = 128


class SourceSnapshotStore:
    """Keep bounded-memory source copies only for the current publication turn.

    The CLI directs this temporary directory to its leased disk scratch. These
    copies are not restart checkpoints: a new process observes sources again.
    The core still checks every opened member against its sealed hash and size.
    """

    def __init__(self) -> None:
        self._temporary = TemporaryDirectory(prefix="h2hdb-ingest-source-bytes-")
        self._root = Path(self._temporary.name)
        self._closed = False
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(self._root / "sources.sqlite3")
            connection.execute(
                "CREATE TABLE source (ordinal INTEGER PRIMARY KEY, locator TEXT NOT NULL, "
                "name BLOB NOT NULL, size_bytes INTEGER NOT NULL, sha256 BLOB NOT NULL, "
                "UNIQUE(locator, name))"
            )
        except BaseException as error:
            if connection is not None:
                try:
                    connection.close()
                except BaseException as close_error:
                    error.add_note(
                        f"Source snapshot index also failed to close: {close_error!r}"
                    )
            self._temporary.cleanup()
            raise
        self._connection = connection
        self._next_ordinal = 0

    def __enter__(self) -> Self:
        self._require_open()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._connection.close()
        finally:
            self._temporary.cleanup()

    def capture(
        self,
        locator: tuple[str, ...],
        observed: FilesystemFileObservation,
        *,
        performance: SourcePerformance | None = None,
    ) -> FileContentReceipt:
        """Capture one member using the same bounded page operation."""
        return self.capture_many(locator, (observed,), performance=performance)[0]

    def capture_many(
        self,
        locator: tuple[str, ...],
        observations: tuple[FilesystemFileObservation, ...],
        *,
        performance: SourcePerformance | None = None,
    ) -> tuple[FileContentReceipt, ...]:
        """Spool at most 128 members, then publish their private index atomically.

        Bytes are copied outside the SQLite transaction. This index is disposable,
        not a restart checkpoint. Old copies remain until the complete index page
        commits; an ambiguous commit failure retains both copies until close.
        """
        self._require_open()
        if len(observations) > SNAPSHOT_CAPTURE_PAGE_SIZE:
            raise ValueError(
                f"source snapshot capture page exceeds {SNAPSHOT_CAPTURE_PAGE_SIZE} members"
            )
        if not observations:
            return ()
        if len({item.name_bytes for item in observations}) != len(observations):
            raise ValueError("source snapshot page contains duplicate names")
        measured = performance if performance is not None else SourcePerformance()
        key = _locator_key(locator)
        captured: list[tuple[int, FilesystemFileObservation, FileContentReceipt]] = []
        previous: list[int] = []
        index_write_started = False
        try:
            for observed in observations:
                ordinal = self._next_ordinal
                self._next_ordinal += 1
                content = self._spool(ordinal, observed, measured)
                captured.append((ordinal, observed, content))
            with measured.phase("snapshot_index"):
                for _, observed, _ in captured:
                    row = self._connection.execute(
                        "SELECT ordinal FROM source WHERE locator = ? AND name = ?",
                        (key, observed.name_bytes),
                    ).fetchone()
                    if row is not None:
                        previous.append(int(row[0]))
                index_write_started = True
                self._connection.executemany(
                    "INSERT INTO source VALUES (?, ?, ?, ?, ?) "
                    "ON CONFLICT(locator, name) DO UPDATE SET ordinal = excluded.ordinal, "
                    "size_bytes = excluded.size_bytes, sha256 = excluded.sha256",
                    (
                        (
                            ordinal,
                            key,
                            observed.name_bytes,
                            content.size_bytes,
                            content.file_sha256,
                        )
                        for ordinal, observed, content in captured
                    ),
                )
            with measured.phase("snapshot_index_commit"):
                self._connection.commit()
            for ordinal in previous:
                (self._root / str(ordinal)).unlink()
            measured.add("snapshot_index_pages")
            return tuple(content for _, _, content in captured)
        except BaseException as error:
            try:
                with measured.phase("snapshot_index_rollback"):
                    self._connection.rollback()
            except BaseException as rollback_error:
                error.add_note(
                    f"Source snapshot rollback also failed: {rollback_error!r}"
                )
            # Before any index write, all new complete copies are unreferenced.
            # Later failures can be response loss: keep both complete versions.
            if not index_write_started:
                for ordinal, _, _ in captured:
                    (self._root / str(ordinal)).unlink(missing_ok=True)
            raise

    def _spool(
        self,
        ordinal: int,
        observed: FilesystemFileObservation,
        measured: SourcePerformance,
    ) -> FileContentReceipt:
        path = self._root / str(ordinal)

        def owned_source() -> Generator[bytes]:
            # yield-from forwards close to the source iterator when supported,
            # preserving its Iterator API without depending on garbage collection.
            yield from observed.content_parts()

        try:
            with closing(owned_source()) as source, path.open("xb") as destination:

                def parts() -> Iterator[bytes]:
                    while True:
                        # Includes source open/stat/hash/checkpoint work; the
                        # separate source read/hash phases measure their primitives.
                        with measured.phase("snapshot_observe"):
                            part = next(source, None)
                        if part is None:
                            break
                        with measured.phase("snapshot_write"):
                            if destination.write(part) != len(part):
                                raise OSError(
                                    "source snapshot accepted a partial write"
                                )
                        # The receipt's consumer hashes while this yield is suspended.
                        with measured.phase("snapshot_receipt_hash"):
                            yield part

                content = FileContentReceipt.from_parts(parts())
                with measured.phase("snapshot_flush"):
                    destination.flush()
            if content.size_bytes != observed.stat.size_bytes:
                raise RuntimeError("source snapshot size differs from observation")
            return content
        except BaseException:
            path.unlink(missing_ok=True)
            raise

    @adapter_operation("snapshot_open")
    def open_source(self, locator: tuple[str, ...], name: bytes) -> BinaryIO | None:
        """Open captured bytes, or report that this turn reused a durable source."""

        self._require_open()
        row = self._connection.execute(
            "SELECT ordinal, size_bytes, sha256 FROM source WHERE locator = ? AND name = ?",
            (_locator_key(locator), name),
        ).fetchone()
        if row is None:
            return None
        descriptor = os.open(
            self._root / str(row[0]),
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0),
        )
        try:
            stream = cast(BinaryIO, os.fdopen(descriptor, "rb"))
        except BaseException:
            os.close(descriptor)
            raise
        try:
            opened = os.fstat(stream.fileno())
            if not stat.S_ISREG(opened.st_mode) or opened.st_size != row[1]:
                raise RuntimeError("source snapshot file differs from its index")
            digest = sha256()
            remaining = int(row[1])
            while remaining:
                part = adapter_read(
                    stream, min(remaining, 1024 * 1024), "snapshot_read"
                )
                if not part:
                    raise RuntimeError("source snapshot ended before its indexed size")
                remaining -= len(part)
                with adapter_phase("snapshot_hash"):
                    digest.update(part)
                    adapter_bytes("snapshot_hash", len(part))
            if adapter_read(stream, 1, "snapshot_read"):
                raise RuntimeError("source snapshot grew beyond its indexed size")
            if digest.digest() != bytes(row[2]):
                raise RuntimeError("source snapshot bytes differ from their index")
            stream.seek(0)
            return stream
        except BaseException:
            stream.close()
            raise

    def discard_gallery(self, locator: tuple[str, ...]) -> None:
        """Discard a deferred gallery's attempt, preserving captured siblings."""

        self._require_open()
        key = _locator_key(locator)
        while rows := self._connection.execute(
            "SELECT ordinal FROM source WHERE locator = ? ORDER BY ordinal LIMIT 128",
            (key,),
        ).fetchall():
            for (ordinal,) in rows:
                (self._root / str(ordinal)).unlink(missing_ok=True)
            with self._connection:
                self._connection.executemany(
                    "DELETE FROM source WHERE ordinal = ?", rows
                )

    def _require_open(self) -> None:
        if self._closed:
            raise RuntimeError("source snapshot store is closed")


def _locator_key(locator: tuple[str, ...]) -> str:
    return json.dumps(locator, ensure_ascii=True, separators=(",", ":"))
