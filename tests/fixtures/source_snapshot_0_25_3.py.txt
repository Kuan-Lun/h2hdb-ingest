"""Disposable exact source bytes shared by observation and artifact preparation."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
from collections.abc import Iterable, Iterator
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import BinaryIO, Self, cast

from h2hdb import FileContentReceipt

from .filesystem import FilesystemFileObservation


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
    ) -> FileContentReceipt:
        """Hash and spool the same bounded read consumed by source observation."""

        self._require_open()
        key = _locator_key(locator)
        ordinal = self._next_ordinal
        self._next_ordinal += 1
        path = self._root / str(ordinal)
        index_write_started = False
        try:
            with path.open("xb") as destination:

                def parts(source: Iterable[bytes]) -> Iterator[bytes]:
                    for part in source:
                        if destination.write(part) != len(part):
                            raise OSError("source snapshot accepted a partial write")
                        yield part

                content = FileContentReceipt.from_parts(parts(observed.content_parts()))
            if content.size_bytes != observed.stat.size_bytes:
                raise RuntimeError("source snapshot size differs from observation")
            previous = self._connection.execute(
                "SELECT ordinal FROM source WHERE locator = ? AND name = ?",
                (key, observed.name_bytes),
            ).fetchone()
            index_write_started = True
            with self._connection:
                self._connection.execute(
                    "INSERT INTO source VALUES (?, ?, ?, ?, ?) "
                    "ON CONFLICT(locator, name) DO UPDATE SET ordinal = excluded.ordinal, "
                    "size_bytes = excluded.size_bytes, sha256 = excluded.sha256",
                    (
                        ordinal,
                        key,
                        observed.name_bytes,
                        content.size_bytes,
                        content.file_sha256,
                    ),
                )
            if previous is not None:
                (self._root / str(previous[0])).unlink()
            return content
        except BaseException:
            # An index-write/cleanup failure may leave either mapping installed.
            # Keep both complete copies until this disposable attempt closes;
            # deleting the new copy here could create a dangling committed row.
            if not index_write_started:
                path.unlink(missing_ok=True)
            raise

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
                part = stream.read(min(remaining, 1024 * 1024))
                if not part:
                    raise RuntimeError("source snapshot ended before its indexed size")
                remaining -= len(part)
                digest.update(part)
            if stream.read(1):
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
