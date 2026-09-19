"""Exercise the real 256-row FILE protocol over 128-member snapshot transactions."""

from __future__ import annotations

from pathlib import Path

import pytest
from h2hdb import (
    ArtifactSourceRole,
    FileContentReceipt,
    FileObservation,
    VNextIngestGalleryObservation,
    VNextSourceDeferredError,
)

from h2hdb_ingest.core_source import VNextFilesystemSourceAdapter
from h2hdb_ingest.filesystem import FilesystemFileObservation, FilesystemSource
from h2hdb_ingest.source_performance import SourcePerformance
from h2hdb_ingest.source_snapshot import SourceSnapshotStore


def _gallery(
    root: Path, gid: str, captured_count: int, *, page_prefix: str = ""
) -> dict[bytes, bytes]:
    """Count METADATA as one captured member; PAGE payloads need no decoder here."""
    folder = root / gid
    folder.mkdir()
    contents = {
        f"{page_prefix}{position:04d}.png".encode(): f"exact page {gid}/{position}".encode()
        for position in range(captured_count - 1)
    }
    contents[b"galleryinfo.txt"] = (
        b"Title: Adapter boundary fixture\nUpload Time: 2024-01-02 03:04\n"
        b"Uploaded By: uploader\nDownloaded: 2024-02-03 04:05\n"
        b"Tags: language:english\nUploader's Comments\n"
        b"Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3\n"
    )
    for name, payload in contents.items():
        (folder / name.decode()).write_bytes(payload)
    return contents


def _captured_bytes(
    snapshot: SourceSnapshotStore, locator: tuple[str, ...], name: bytes
) -> bytes | None:
    stream = snapshot.open_source(locator, name)
    if stream is None:
        return None
    with stream:
        return stream.read()


def _verify_walk(
    adapter: VNextFilesystemSourceAdapter,
    observation: VNextIngestGalleryObservation,
    snapshot: SourceSnapshotStore,
    expected: dict[bytes, bytes],
    *,
    other_names: frozenset[bytes] = frozenset(),
) -> tuple[list[FileObservation], list[int]]:
    names = sorted(expected)
    rows: list[FileObservation] = []
    captured_per_outer_page: list[int] = []
    after: bytes | None = None
    while True:
        page = adapter.list_file_observations(
            observation, after_name_bytes=after, limit=256
        )
        start = len(rows)
        assert [row.name_bytes for row in page.items] == names[start : start + 256]
        assert page.terminal is (start + 256 >= len(names))
        captured_per_outer_page.append(
            sum(row.artifact_role is not ArtifactSourceRole.OTHER for row in page.items)
        )
        for row in page.items:
            payload = expected[row.name_bytes]
            assert row.content == FileContentReceipt.from_parts((payload,))
            expected_role = (
                ArtifactSourceRole.OTHER
                if row.name_bytes in other_names
                else ArtifactSourceRole.METADATA
                if row.name_bytes == b"galleryinfo.txt"
                else ArtifactSourceRole.PAGE
            )
            assert row.artifact_role is expected_role
            assert _captured_bytes(
                snapshot, observation.locator_components, row.name_bytes
            ) == (None if row.name_bytes in other_names else payload)
        rows.extend(page.items)
        if page.terminal:
            assert page.next_after is None
            break
        # The outer Core page remains exactly 256, even when capture needs
        # multiple smaller transactions or omits OTHER files from byte copying.
        assert len(page.items) == 256
        assert isinstance(page.next_after, bytes)
        assert page.next_after == page.items[-1].name_bytes
        assert after is None or page.next_after > after
        after = page.next_after
    assert len(rows) == len(names) == len({row.name_bytes for row in rows})
    return rows, captured_per_outer_page


@pytest.mark.parametrize("captured_count", (127, 128, 129, 255, 256, 257, 512))
def test_real_adapter_pages_preserve_core_capacity_and_bound_snapshot_commits(
    tmp_path: Path, captured_count: int
) -> None:
    expected = _gallery(tmp_path, "1001", captured_count)
    statements: list[str] = []
    with SourceSnapshotStore() as snapshot, FilesystemSource(tmp_path) as source:
        snapshot._connection.set_trace_callback(statements.append)
        adapter = VNextFilesystemSourceAdapter(source, snapshot=snapshot)
        observation = adapter.observe_gallery(("1001",))
        _, captured_counts = _verify_walk(adapter, observation, snapshot, expected)
        expected_commits = sum((count + 127) // 128 for count in captured_counts)
        assert sum(sql == "COMMIT" for sql in statements) == expected_commits
        assert sum(sql.startswith("BEGIN") for sql in statements) == expected_commits
        assert (
            sum(path.name.isdecimal() for path in snapshot._root.iterdir())
            == captured_count
        )


@pytest.mark.parametrize("layout", ("interleaved", "other_first"))
def test_other_files_remain_exact_file_facts_without_snapshot_copies(
    tmp_path: Path, layout: str
) -> None:
    expected = _gallery(
        tmp_path, "1001", 129, page_prefix="p" if layout == "other_first" else ""
    )
    others = {
        f"{position:04d}.bin".encode(): f"other file {position}".encode()
        for position in range(256)
    }
    for name, payload in others.items():
        (tmp_path / "1001" / name.decode()).write_bytes(payload)
    expected.update(others)
    statements: list[str] = []
    with SourceSnapshotStore() as snapshot, FilesystemSource(tmp_path) as source:
        snapshot._connection.set_trace_callback(statements.append)
        adapter = VNextFilesystemSourceAdapter(source, snapshot=snapshot)
        observation = adapter.observe_gallery(("1001",))
        _, captured_counts = _verify_walk(
            adapter, observation, snapshot, expected, other_names=frozenset(others)
        )
        assert captured_counts == ([128, 1] if layout == "interleaved" else [0, 129])
        assert sum(sql == "COMMIT" for sql in statements) == 2
        assert sum(path.name.isdecimal() for path in snapshot._root.iterdir()) == 129


def test_same_nonterminal_core_page_can_be_replayed_without_leaking_old_copies(
    tmp_path: Path,
) -> None:
    expected = _gallery(tmp_path, "1001", 257)
    statements: list[str] = []
    with SourceSnapshotStore() as snapshot, FilesystemSource(tmp_path) as source:
        snapshot._connection.set_trace_callback(statements.append)
        adapter = VNextFilesystemSourceAdapter(source, snapshot=snapshot)
        observation = adapter.observe_gallery(("1001",))
        first_rows: tuple[FileObservation, ...] | None = None
        for cycle in range(3):
            page = adapter.list_file_observations(
                observation, after_name_bytes=None, limit=256
            )
            assert len(page.items) == 256
            assert not page.terminal
            assert page.next_after == b"0255.png"
            if first_rows is None:
                first_rows = page.items
            assert page.items == first_rows
            assert sum(sql == "COMMIT" for sql in statements) == (cycle + 1) * 2
            assert (
                sum(path.name.isdecimal() for path in snapshot._root.iterdir()) == 256
            )
            for row in page.items:
                assert (
                    _captured_bytes(snapshot, ("1001",), row.name_bytes)
                    == expected[row.name_bytes]
                )
        final = adapter.list_file_observations(
            observation, after_name_bytes=b"0255.png", limit=256
        )
        assert final.terminal and final.next_after is None
        assert [row.name_bytes for row in final.items] == [b"galleryinfo.txt"]
        assert final.items[0].content == FileContentReceipt.from_parts(
            (expected[b"galleryinfo.txt"],)
        )
        assert sum(sql == "COMMIT" for sql in statements) == 7
        assert sum(path.name.isdecimal() for path in snapshot._root.iterdir()) == 257


def test_second_capture_batch_source_change_discards_gallery_after_first_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    affected = _gallery(tmp_path, "1001", 256)
    sibling = _gallery(tmp_path, "1002", 2)
    original = SourceSnapshotStore.capture_many
    affected_batch_sizes: list[int] = []
    first_batch_committed = False
    statements: list[str] = []

    def capture_with_second_batch_mutation(
        store: SourceSnapshotStore,
        locator: tuple[str, ...],
        observations: tuple[FilesystemFileObservation, ...],
        *,
        performance: SourcePerformance | None = None,
    ) -> tuple[FileContentReceipt, ...]:
        nonlocal first_batch_committed
        if locator == ("1001",):
            affected_batch_sizes.append(len(observations))
            if len(affected_batch_sizes) == 2:
                assert first_batch_committed
                assert sum(sql == "COMMIT" for sql in statements) == 1
                assert (
                    _captured_bytes(store, locator, b"0000.png")
                    == affected[b"0000.png"]
                )
                observations[0].path.write_bytes(
                    b"producer changed the second subbatch"
                )
        result = original(store, locator, observations, performance=performance)
        if locator == ("1001",):
            first_batch_committed = True
        return result

    monkeypatch.setattr(
        SourceSnapshotStore, "capture_many", capture_with_second_batch_mutation
    )
    performance = SourcePerformance()
    with SourceSnapshotStore() as snapshot, FilesystemSource(tmp_path) as source:
        adapter = VNextFilesystemSourceAdapter(
            source, snapshot=snapshot, performance=performance
        )
        other = adapter.observe_gallery(("1002",))
        _verify_walk(adapter, other, snapshot, sibling)
        snapshot._connection.set_trace_callback(statements.append)
        observation = adapter.observe_gallery(("1001",))
        with pytest.raises(VNextSourceDeferredError):
            adapter.list_file_observations(
                observation, after_name_bytes=None, limit=256
            )
        assert affected_batch_sizes == [128, 128]
        assert first_batch_committed
        counters = {
            item.name: item.value
            for item in performance.metric(status="failed").counters
        }
        # Cleanup removes provisional bytes, but must not erase completed copy
        # work from performance evidence when the later subbatch is deferred.
        assert counters["snapshot_files"] == len(sibling) + 128
        assert counters["snapshot_bytes"] == sum(map(len, sibling.values())) + sum(
            len(affected[name]) for name in sorted(affected)[:128]
        )
        for name in affected:
            assert _captured_bytes(snapshot, ("1001",), name) is None
        for name, payload in sibling.items():
            assert _captured_bytes(snapshot, ("1002",), name) == payload
        assert sum(path.name.isdecimal() for path in snapshot._root.iterdir()) == len(
            sibling
        )
