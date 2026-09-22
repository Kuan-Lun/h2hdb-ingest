"""Exact source facts remain bounded; original bytes are reread at render time."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from time import perf_counter

import pytest
from h2hdb import (
    ArtifactSourceRole,
    FileContentReceipt,
    FileObservation,
    VNextSourceChangedError,
    VNextSourceDeferredError,
)
from library_fixtures import _adapter

import h2hdb_ingest.core_source as source_adapter_module
from h2hdb_ingest.core_source import VNextFilesystemSourceAdapter
from h2hdb_ingest.filesystem import FilesystemFileObservation, FilesystemSource
from h2hdb_ingest.source_performance import SourcePerformance


def _gallery(root: Path, gid: int, pages: int, size: int = 32) -> dict[bytes, bytes]:
    folder = root / str(gid)
    folder.mkdir(parents=True)
    contents = {
        f"{position:04d}.png".encode(): position.to_bytes(8, "big") + b"x" * (size - 8)
        for position in range(pages)
    }
    contents[b"galleryinfo.txt"] = (
        b"Title: Reread fixture\nUpload Time: 2024-01-02 03:04\n"
        b"Uploaded By: uploader\nDownloaded: 2024-02-03 04:05\n"
        b"Tags: language:english\nUploader's Comments\n"
        b"Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3\n"
    )
    for name, payload in contents.items():
        (folder / name.decode()).write_bytes(payload)
    return contents


def _walk(
    adapter: VNextFilesystemSourceAdapter, gid: int
) -> tuple[FileObservation, ...]:
    observation = adapter.observe_gallery((str(gid),))
    after: bytes | None = None
    rows: list[FileObservation] = []
    while True:
        page = adapter.list_file_observations(
            observation, after_name_bytes=after, limit=256
        )
        rows.extend(page.items)
        if page.terminal:
            assert page.next_after is None
            break
        assert len(page.items) == 256
        assert isinstance(page.next_after, bytes)
        assert after is None or page.next_after > after
        after = page.next_after
    adapter.observe_completion_marker((str(gid),))
    return tuple(rows)


@pytest.mark.parametrize("members", (255, 256, 257, 512))
def test_real_source_pages_hash_every_member_without_retaining_source_copies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, members: int
) -> None:
    source_root = tmp_path / "source"
    expected = _gallery(source_root, 1001, members - 1)
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(scratch))
    with FilesystemSource(source_root) as source:
        adapter = VNextFilesystemSourceAdapter(source)
        first = _walk(adapter, 1001)
        for _ in range(3):
            rows = _walk(adapter, 1001)
            assert rows == first
            assert [row.name_bytes for row in rows] == sorted(expected)
            for row in rows:
                assert row.content == FileContentReceipt.from_parts(
                    (expected[row.name_bytes],)
                )
                assert row.artifact_role is (
                    ArtifactSourceRole.METADATA
                    if row.name_bytes == b"galleryinfo.txt"
                    else ArtifactSourceRole.PAGE
                )
            assert not tuple(scratch.glob("h2hdb-ingest-source-bytes-*"))
    assert not tuple(scratch.iterdir())


def test_other_files_remain_exact_facts_without_becoming_artifact_pages(
    tmp_path: Path,
) -> None:
    expected = _gallery(tmp_path, 1001, 2)
    expected[b"notes.bin"] = b"opaque non-page source bytes"
    (tmp_path / "1001" / "notes.bin").write_bytes(expected[b"notes.bin"])
    with FilesystemSource(tmp_path) as source:
        rows = _walk(VNextFilesystemSourceAdapter(source), 1001)
    assert len(rows) == len(expected)
    other = next(row for row in rows if row.name_bytes == b"notes.bin")
    assert other.artifact_role is ArtifactSourceRole.OTHER
    assert other.content == FileContentReceipt.from_parts((expected[b"notes.bin"],))


@pytest.mark.parametrize("change", ("replace", "delete", "symlink"))
def test_source_adapter_reopens_live_bytes_without_frozen_copy(
    tmp_path: Path, change: str
) -> None:
    library = _adapter(tmp_path / "library")
    source_root = tmp_path / "download-source"
    expected = _gallery(source_root, 1001, 1)
    with FilesystemSource(source_root) as source:
        rows = _walk(VNextFilesystemSourceAdapter(source), 1001)
    original = next(row for row in rows if row.name_bytes == b"0000.png")
    path = source_root / "1001" / "0000.png"
    path.unlink()
    if change == "replace":
        path.write_bytes(b"replacement bytes")
        with library.open_source(
            source_root_components=tuple(source_root.parts[1:]),
            gallery_locator_components=("1001",),
            source_name=b"0000.png",
        ) as stream:
            payload = stream.read()
        assert payload == b"replacement bytes"
        assert original.content == FileContentReceipt.from_parts(
            (expected[b"0000.png"],)
        )
        assert FileContentReceipt.from_parts((payload,)) != original.content
    else:
        if change == "symlink":
            path.symlink_to(source_root / "1001" / "galleryinfo.txt")
        with pytest.raises(
            VNextSourceChangedError if change == "delete" else RuntimeError
        ):
            library.open_source(
                source_root_components=tuple(source_root.parts[1:]),
                gallery_locator_components=("1001",),
                source_name=b"0000.png",
            )


def test_final_marker_still_defers_changed_gallery_and_accepts_sibling(
    tmp_path: Path,
) -> None:
    _gallery(tmp_path, 1001, 1)
    _gallery(tmp_path, 1002, 1)
    with FilesystemSource(tmp_path) as source:
        adapter = VNextFilesystemSourceAdapter(source)
        observation = adapter.observe_gallery(("1001",))
        adapter.list_file_observations(observation, after_name_bytes=None, limit=256)
        (tmp_path / "1001" / "0000.png").write_bytes(b"producer updated source")
        with pytest.raises(VNextSourceDeferredError):
            adapter.observe_completion_marker(("1001",))
        adapter.discard_gallery_observation(("1001",))
        assert len(_walk(adapter, 1002)) == 2


@contextmanager
def _count_reads(monkeypatch: pytest.MonkeyPatch, root: Path) -> Iterator[list[int]]:
    identities = {
        (s.st_dev, s.st_ino)
        for p in root.rglob("*")
        if p.is_file()
        for s in (p.stat(),)
    }
    original = os.read
    counted = [0, 0]

    def read(fd: int, count: int) -> bytes:
        info = os.fstat(fd)
        data = original(fd, count)
        if (info.st_dev, info.st_ino) in identities:
            counted[0] += 1
            counted[1] += len(data)
        return data

    with monkeypatch.context() as patch:
        patch.setattr(os, "read", read)
        yield counted


def _scratch_bytes(root: Path) -> int:
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


@pytest.mark.parametrize("pages", (255, 256, 257))
def test_observation_storage_does_not_scale_with_source_bytes_and_counts_real_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pages: int
) -> None:
    """3 cycles, before/at/after the FILE page cap; independently meter actual reads."""
    peaks = []
    for size in (4096, 65536):
        root = tmp_path / str(size)
        source_root = root / "source"
        expected = _gallery(source_root, 1001, pages, size)
        scratch = root / "scratch"
        scratch.mkdir()
        performance = SourcePerformance()
        with monkeypatch.context() as patch:
            patch.setattr(tempfile, "tempdir", str(scratch))
            with (
                _count_reads(patch, source_root) as actual,
                FilesystemSource(source_root, performance=performance) as source,
            ):
                adapter = VNextFilesystemSourceAdapter(source, performance=performance)
                peak = 0
                for _ in range(3):
                    assert len(_walk(adapter, 1001)) == pages + 1
                    peak = max(peak, _scratch_bytes(scratch))
                metrics = {
                    item.name: item.value
                    for item in performance.metric(status="completed").counters
                }
                assert metrics["read_calls"] == actual[0]
                assert metrics["logical_bytes_read"] == actual[1]
                assert actual[1] >= 3 * sum(map(len, expected.values()))
                assert actual[1] < 4 * sum(map(len, expected.values()))
                peaks.append(peak)
        assert not tuple(scratch.iterdir())
    # SQLite metadata occupies fixed pages; payload bytes are never retained.
    assert peaks[0] == peaks[1]
    assert peaks[1] < pages * 4096
    # Inject a correct-output implementation that retains every observed payload.
    # The same storage oracle must reject the actual degraded adapter execution.
    degraded = tmp_path / "degraded"
    degraded.mkdir()
    original = source_adapter_module._file

    def retaining_file(observation: FilesystemFileObservation) -> FileObservation:
        result = original(observation)
        (degraded / observation.name_bytes.decode()).write_bytes(
            observation.path.read_bytes()
        )
        return result

    with monkeypatch.context() as patch:
        patch.setattr(source_adapter_module, "_file", retaining_file)
        with FilesystemSource(tmp_path / "65536" / "source") as source:
            rows = _walk(VNextFilesystemSourceAdapter(source), 1001)
            assert len(rows) == pages + 1
            assert sum(row.content.size_bytes for row in rows) == _scratch_bytes(
                degraded
            )
            with pytest.raises(AssertionError):
                assert _scratch_bytes(degraded) < pages * 4096


@pytest.mark.deep
def test_large_source_observation_retains_metadata_not_corpus_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """512 MiB source bytes, 16,384 pages, three complete observation cycles."""
    galleries, pages, size, cycles = 128, 128, 32768, 3
    root = tmp_path / "source"
    marker_size = 0
    for gid in range(1000, 1000 + galleries):
        contents = _gallery(root, gid, pages, size)
        marker_size += len(contents[b"galleryinfo.txt"])
    source_bytes = galleries * pages * size + marker_size
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    performance = SourcePerformance()
    monkeypatch.setattr(tempfile, "tempdir", str(scratch))
    peak = 0
    started = perf_counter()
    with (
        _count_reads(monkeypatch, root) as reads,
        FilesystemSource(root, performance=performance) as source,
    ):
        adapter = VNextFilesystemSourceAdapter(source, performance=performance)
        for _cycle in range(cycles):
            for gid in range(1000, 1000 + galleries):
                rows = _walk(adapter, gid)
                assert len(rows) == pages + 1
                assert all(
                    row.content.size_bytes == size
                    for row in rows
                    if row.artifact_role is ArtifactSourceRole.PAGE
                )
                peak = max(peak, _scratch_bytes(scratch))
        counters = {
            item.name: item.value
            for item in performance.metric(status="completed").counters
        }
        assert counters["logical_bytes_read"] == reads[1]
        assert counters["read_calls"] == reads[0]
        assert cycles * source_bytes <= reads[1] < (cycles + 1) * source_bytes
        # The bound permits ample per-gallery metadata but rejects source copies.
        assert peak < 1024 * (galleries + pages)
    assert not tuple(scratch.iterdir())
    result = {
        "galleries": galleries,
        "pages_per_gallery": pages,
        "cycles": cycles,
        "source_bytes": source_bytes,
        "source_read_bytes": reads[1],
        "source_read_calls": reads[0],
        "peak_retained_observation_scratch_bytes": peak,
        "elapsed_seconds": perf_counter() - started,
        "scope": "real filesystem adapter; scratch logical sizes sampled after every gallery; excludes transient in-call journal/native work, decoder, artifact, SQL pipeline, and NAS timing claims",
    }
    (tmp_path / "reread-storage-cost.json").write_text(
        json.dumps(result, indent=2) + "\n"
    )
    print(json.dumps(result))
