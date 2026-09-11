"""Source-byte capture isolates rendering and discards interrupted attempts."""

from __future__ import annotations

import os
import sqlite3
import tempfile
from hashlib import sha256
from pathlib import Path
from typing import BinaryIO, cast

import pytest
from h2hdb import VNextSourceDeferredError

from h2hdb_ingest.core_source import VNextFilesystemSourceAdapter
from h2hdb_ingest.filesystem import (
    FilesystemArtifactSourceRole,
    FilesystemFileObservation,
    FilesystemSource,
    FilesystemSourceChangedError,
    FilesystemStat,
)
from h2hdb_ingest.source_snapshot import SourceSnapshotStore


def _observation(path: Path) -> FilesystemFileObservation:
    return FilesystemFileObservation(
        folder=path.parent,
        name_bytes=path.name.encode(),
        stat=FilesystemStat.from_os_stat(path.stat()),
        artifact_role=FilesystemArtifactSourceRole.PAGE,
    )


def test_capture_keeps_exact_bytes_after_original_is_replaced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    path = tmp_path / "page.jpg"
    path.write_bytes(b"observed source")
    with SourceSnapshotStore() as captured:
        receipt = captured.capture(("1001",), _observation(path))
        path.unlink()
        path.write_bytes(b"new producer bytes")
        stream = captured.open_source(("1001",), b"page.jpg")
        assert stream is not None
        with stream:
            assert stream.read() == b"observed source"
        assert receipt.file_sha256 == sha256(b"observed source").digest()
        assert captured.open_source(("1002",), b"page.jpg") is None
    assert tuple(tmp_path.glob("h2hdb-ingest-source-bytes-*")) == ()
    # A new process/turn cannot treat old scratch as source authority.
    with SourceSnapshotStore() as restarted:
        assert restarted.open_source(("1001",), b"page.jpg") is None


def test_changed_source_cannot_leave_a_readable_partial_capture(tmp_path: Path) -> None:
    path = tmp_path / "page.jpg"
    path.write_bytes(b"before")
    observed = _observation(path)
    path.write_bytes(b"after and changed size")
    with SourceSnapshotStore() as captured:
        with pytest.raises(FilesystemSourceChangedError):
            captured.capture(("1001",), observed)
        assert captured.open_source(("1001",), b"page.jpg") is None


def test_private_capture_corruption_is_detected_before_render(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    path = tmp_path / "page.jpg"
    path.write_bytes(b"before")
    with SourceSnapshotStore() as captured:
        captured.capture(("1001",), _observation(path))
        (scratch,) = tmp_path.glob("h2hdb-ingest-source-bytes-*")
        (scratch / "0").write_bytes(b"broken")
        with pytest.raises(RuntimeError, match="bytes differ"):
            captured.open_source(("1001",), b"page.jpg")


def test_failed_index_initialization_closes_connection_before_removing_scratch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    original_connect = sqlite3.connect
    closed: list[bool] = []

    class BrokenConnection(sqlite3.Connection):
        def execute(self, _sql: str, _parameters: object = (), /) -> sqlite3.Cursor:
            raise sqlite3.OperationalError("injected index initialization failure")

        def close(self) -> None:
            closed.append(True)
            super().close()

    def broken_connect(path: Path) -> sqlite3.Connection:
        return original_connect(path, factory=BrokenConnection)

    monkeypatch.setattr(sqlite3, "connect", broken_connect)
    with pytest.raises(sqlite3.OperationalError, match="initialization failure"):
        SourceSnapshotStore()
    assert closed == [True]
    assert tuple(tmp_path.glob("h2hdb-ingest-source-bytes-*")) == ()


def test_failed_stream_wrapper_closes_its_owned_descriptor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "page.jpg"
    path.write_bytes(b"captured bytes")
    opened: list[int] = []

    def broken_fdopen(descriptor: int, _mode: str) -> None:
        opened.append(descriptor)
        raise OSError("injected file wrapper failure")

    with SourceSnapshotStore() as captured:
        captured.capture(("1001",), _observation(path))
        with monkeypatch.context() as patch:
            patch.setattr(os, "fdopen", broken_fdopen)
            with pytest.raises(OSError, match="wrapper failure"):
                captured.open_source(("1001",), b"page.jpg")
        assert len(opened) == 1
        with pytest.raises(OSError):
            os.fstat(opened[0])
        stream = captured.open_source(("1001",), b"page.jpg")
        assert stream is not None
        with stream:
            assert stream.read() == b"captured bytes"


def test_discard_gallery_cleans_multiple_bounded_pages_and_preserves_siblings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    with SourceSnapshotStore() as captured:
        for position in range(260):
            path = tmp_path / f"{position:04d}.jpg"
            path.write_bytes(f"page {position}".encode())
            captured.capture(("1001",), _observation(path))
        sibling = tmp_path / "0000.jpg"
        captured.capture(("1002",), _observation(sibling))
        (scratch,) = tmp_path.glob("h2hdb-ingest-source-bytes-*")
        assert sum(path.name.isdecimal() for path in scratch.iterdir()) == 261
        captured.discard_gallery(("1001",))
        captured.discard_gallery(("1001",))
        assert captured.open_source(("1001",), b"0000.jpg") is None
        assert captured.open_source(("1001",), b"0259.jpg") is None
        assert sum(path.name.isdecimal() for path in scratch.iterdir()) == 1
        stream = captured.open_source(("1002",), b"0000.jpg")
        assert stream is not None
        with stream:
            assert stream.read() == b"page 0"
    assert tuple(tmp_path.glob("h2hdb-ingest-source-bytes-*")) == ()


def test_final_gallery_change_discards_captured_bytes_and_allows_its_sibling(
    tmp_path: Path,
) -> None:
    for gid in (1001, 1002):
        folder = tmp_path / str(gid)
        folder.mkdir()
        (folder / "page.jpg").write_bytes(b"completed image bytes")
        (folder / "galleryinfo.txt").write_text(
            "Title: Capture fixture\nUpload Time: 2024-01-02 03:04\n"
            "Uploaded By: uploader\nDownloaded: 2024-02-03 04:05\n"
            "Tags: language:english\nUploader's Comments\n"
            "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3\n"
        )
    with SourceSnapshotStore() as captured, FilesystemSource(tmp_path) as source:
        adapter = VNextFilesystemSourceAdapter(source, snapshot=captured)
        observation = adapter.observe_gallery(("1001",))
        adapter.list_file_observations(observation, after_name_bytes=None, limit=128)
        (tmp_path / "1001" / "page.jpg").write_bytes(b"producer is still updating")
        with pytest.raises(VNextSourceDeferredError):
            adapter.observe_completion_marker(("1001",))
        assert captured.open_source(("1001",), b"page.jpg") is None
        assert captured.open_source(("1001",), b"galleryinfo.txt") is None
        sibling = adapter.observe_gallery(("1002",))
        adapter.list_file_observations(sibling, after_name_bytes=None, limit=128)
        adapter.observe_completion_marker(("1002",))
        stream = captured.open_source(("1002",), b"page.jpg")
        assert stream is not None
        with stream:
            assert stream.read() == b"completed image bytes"


def test_failed_old_copy_cleanup_preserves_the_committed_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    path = tmp_path / "page.jpg"
    path.write_bytes(b"previous bytes")
    original_unlink = Path.unlink
    with SourceSnapshotStore() as captured:
        captured.capture(("1001",), _observation(path))
        (scratch,) = tmp_path.glob("h2hdb-ingest-source-bytes-*")

        def denied_unlink(target: Path, *, missing_ok: bool = False) -> None:
            if target == scratch / "0":
                raise OSError("injected previous-copy cleanup failure")
            original_unlink(target, missing_ok=missing_ok)

        path.write_bytes(b"replacement bytes")
        with monkeypatch.context() as patch:
            patch.setattr(Path, "unlink", denied_unlink)
            with pytest.raises(OSError, match="previous-copy cleanup failure"):
                captured.capture(("1001",), _observation(path))
        stream = captured.open_source(("1001",), b"page.jpg")
        assert stream is not None
        with stream:
            assert stream.read() == b"replacement bytes"
    assert tuple(tmp_path.glob("h2hdb-ingest-source-bytes-*")) == ()


def test_snapshot_growth_is_bounded_by_indexed_size_plus_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "page.jpg"
    path.write_bytes(b"short")
    original_fdopen = os.fdopen
    reads: list[int] = []

    class GrowingStream:
        def __init__(self, stream: BinaryIO) -> None:
            self._stream = stream

        def fileno(self) -> int:
            return self._stream.fileno()

        def read(self, maximum: int) -> bytes:
            reads.append(maximum)
            return b"x" * maximum

        def close(self) -> None:
            self._stream.close()

    def growing_fdopen(descriptor: int, mode: str) -> GrowingStream:
        return GrowingStream(cast(BinaryIO, original_fdopen(descriptor, mode)))

    with SourceSnapshotStore() as captured:
        captured.capture(("1001",), _observation(path))
        monkeypatch.setattr(os, "fdopen", growing_fdopen)
        with pytest.raises(RuntimeError, match="grew beyond its indexed size"):
            captured.open_source(("1001",), b"page.jpg")
    assert reads == [5, 1]
