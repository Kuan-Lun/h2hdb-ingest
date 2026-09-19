"""Bounded snapshot index transactions preserve exact bytes under failures."""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Iterable
from pathlib import Path
from types import TracebackType
from typing import Any, BinaryIO, cast

import pytest
from h2hdb import FileContentReceipt
from test_source_snapshot import _observation

from h2hdb_ingest.filesystem import (
    FilesystemFileObservation,
    FilesystemSourceChangedError,
)
from h2hdb_ingest.source_performance import SourcePerformance
from h2hdb_ingest.source_snapshot import SourceSnapshotStore


def _members(root: Path, count: int) -> tuple[FilesystemFileObservation, ...]:
    result = []
    for position in range(count):
        path = root / f"{position:04d}.jpg"
        path.write_bytes(f"original {position}".encode())
        result.append(_observation(path))
    return tuple(result)


def _contents(store: SourceSnapshotStore, name: bytes) -> bytes | None:
    stream = store.open_source(("gallery",), name)
    if stream is None:
        return None
    with stream:
        return stream.read()


@pytest.mark.parametrize("count", (127, 128, 129, 512))
def test_page_transaction_count_and_bytes_repeat_across_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, count: int
) -> None:
    events: list[str] = []
    original_connect = sqlite3.connect

    def connect(path: Path) -> sqlite3.Connection:
        connection = original_connect(path)
        connection.set_trace_callback(events.append)
        return connection

    monkeypatch.setattr(sqlite3, "connect", connect)
    observations = _members(tmp_path, count)
    performance = SourcePerformance()
    with SourceSnapshotStore() as store:
        for cycle in range(3):
            events.clear()
            result: list[FileContentReceipt] = []
            for start in range(0, count, 128):
                result.extend(
                    store.capture_many(
                        ("gallery",),
                        observations[start : start + 128],
                        performance=performance,
                    )
                )
            assert sum(sql == "COMMIT" for sql in events) == (count + 127) // 128
            assert (
                sum(sql.startswith("BEGIN") for sql in events) == (count + 127) // 128
            )
            for observation, receipt in zip(observations, result, strict=True):
                expected = observation.path.read_bytes()
                assert _contents(store, observation.name_bytes) == expected
                assert receipt == FileContentReceipt.from_parts((expected,))
            counters = {
                v.name: v.value for v in performance.metric(status="completed").counters
            }
            assert counters["snapshot_index_commit_calls"] == (cycle + 1) * (
                (count + 127) // 128
            )


def test_empty_and_oversized_pages_do_not_read_or_write(tmp_path: Path) -> None:
    observations = _members(tmp_path, 129)
    with SourceSnapshotStore() as store:
        assert store.capture_many(("gallery",), ()) == ()
        with pytest.raises(ValueError, match="exceeds 128"):
            store.capture_many(("gallery",), observations)
        with pytest.raises(ValueError, match="duplicate names"):
            store.capture_many(("gallery",), (observations[0], observations[0]))
        assert _contents(store, observations[0].name_bytes) is None


def test_failed_spool_rolls_back_the_whole_page_and_preserves_previous_bytes(
    tmp_path: Path,
) -> None:
    observations = _members(tmp_path, 2)
    with SourceSnapshotStore() as store:
        store.capture(("gallery",), observations[0])
        observations[0].path.write_bytes(b"replacement")
        replacement = _observation(observations[0].path)
        observations[1].path.write_bytes(b"producer mutation after observation")
        with pytest.raises(FilesystemSourceChangedError):
            store.capture_many(("gallery",), (replacement, observations[1]))
        assert _contents(store, observations[0].name_bytes) == b"original 0"
        assert _contents(store, observations[1].name_bytes) is None
        # A subsequent independent capture is not poisoned by the failed page.
        store.capture(("gallery",), _observation(observations[1].path))
        assert (
            _contents(store, observations[1].name_bytes)
            == b"producer mutation after observation"
        )


@pytest.mark.parametrize("stage", ("write", "before_commit", "after_commit"))
def test_partial_index_failure_and_commit_response_loss_keep_readable_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    original_connect = sqlite3.connect
    armed = [False]

    class FaultConnection(sqlite3.Connection):
        def executemany(self, sql: str, parameters: Iterable[Any], /) -> sqlite3.Cursor:
            if armed[0] and stage == "write":
                iterator = iter(parameters)
                super().execute(sql, next(iterator))
                raise sqlite3.OperationalError("injected partial write")
            return super().executemany(sql, parameters)

        def commit(self) -> None:
            if armed[0] and stage == "before_commit":
                raise sqlite3.OperationalError("injected before commit")
            super().commit()
            if armed[0] and stage == "after_commit":
                raise sqlite3.OperationalError("injected commit response loss")

    def connect(path: Path) -> sqlite3.Connection:
        return original_connect(path, factory=FaultConnection)

    monkeypatch.setattr(sqlite3, "connect", connect)
    observations = _members(tmp_path, 2)
    with SourceSnapshotStore() as store:
        store.capture_many(("gallery",), observations)
        for observation in observations:
            observation.path.write_bytes(b"replacement")
        armed[0] = True
        with pytest.raises(sqlite3.OperationalError, match="injected"):
            store.capture_many(
                ("gallery",), tuple(_observation(item.path) for item in observations)
            )
        armed[0] = False
        for position, observation in enumerate(observations):
            expected = (
                b"replacement"
                if stage == "after_commit"
                else f"original {position}".encode()
            )
            assert _contents(store, observation.name_bytes) == expected
        store.discard_gallery(("gallery",))
        assert _contents(store, observations[0].name_bytes) is None


def test_same_output_per_file_regression_is_rejected_by_transaction_oracle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[str] = []
    original_connect = sqlite3.connect
    original_capture = SourceSnapshotStore.capture_many

    def connect(path: Path) -> sqlite3.Connection:
        connection = original_connect(path)
        connection.set_trace_callback(events.append)
        return connection

    def degraded_capture(
        store: SourceSnapshotStore,
        locator: tuple[str, ...],
        observations: tuple[FilesystemFileObservation, ...],
        *,
        performance: SourcePerformance | None = None,
    ) -> tuple[FileContentReceipt, ...]:
        return tuple(
            original_capture(store, locator, (item,), performance=performance)[0]
            for item in observations
        )

    monkeypatch.setattr(sqlite3, "connect", connect)
    monkeypatch.setattr(SourceSnapshotStore, "capture_many", degraded_capture)
    observations = _members(tmp_path, 128)
    with SourceSnapshotStore() as store:
        store.capture_many(("gallery",), observations)
        assert _contents(store, observations[-1].name_bytes) == b"original 127"
        with pytest.raises(AssertionError):
            assert sum(sql == "COMMIT" for sql in events) == 1


@pytest.mark.parametrize(
    "failure", (OSError("destination write failed"), KeyboardInterrupt())
)
def test_destination_failure_closes_source_even_when_exception_is_retained(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: BaseException
) -> None:
    path = tmp_path / "page.jpg"
    path.write_bytes(b"source descriptor must close before retry")
    observed = _observation(path)
    original_open = os.open
    original_path_open = Path.open
    source_descriptors: list[int] = []

    def open_descriptor(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        descriptor = original_open(path, flags, mode, dir_fd=dir_fd)
        if path == b"page.jpg":
            source_descriptors.append(descriptor)
        return descriptor

    class FailingWriter:
        def __init__(self, stream: BinaryIO) -> None:
            self._stream = stream

        def __enter__(self) -> FailingWriter:
            self._stream.__enter__()
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            traceback: TracebackType | None,
        ) -> None:
            self._stream.__exit__(exc_type, exc, traceback)

        def write(self, _part: bytes) -> int:
            raise failure

    def open_path(target: Path, mode: str = "r", *args: Any, **kwargs: Any) -> Any:
        stream = original_path_open(target, mode, *args, **kwargs)
        if target.name.isdecimal() and mode == "xb":
            return FailingWriter(cast(BinaryIO, stream))
        return stream

    with SourceSnapshotStore() as store:
        monkeypatch.setattr(os, "open", open_descriptor)
        monkeypatch.setattr(Path, "open", open_path)
        with pytest.raises(type(failure)) as retained:
            store.capture(("gallery",), observed)
        assert retained.value is failure
        assert retained.value.__traceback__ is not None
        assert len(source_descriptors) == 1
        with pytest.raises(OSError):
            os.fstat(source_descriptors[0])
        assert store.open_source(("gallery",), b"page.jpg") is None
