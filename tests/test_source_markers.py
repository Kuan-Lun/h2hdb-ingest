from __future__ import annotations

import os
from collections.abc import Iterator
from hashlib import sha256
from pathlib import Path

import pytest

import h2hdb_ingest.filesystem as filesystem_module
from h2hdb_ingest._limits import MAX_METADATA_BYTES
from h2hdb_ingest.core_source import VNextFilesystemSourceAdapter
from h2hdb_ingest.filesystem import (
    FILESYSTEM_OBSERVATION_VERSION,
    FilesystemObservationError,
    FilesystemSource,
    FilesystemSourceChangedError,
    FilesystemStat,
)


def _gallery(root: Path, locator: str = "1001") -> Path:
    folder = root / locator
    folder.mkdir(parents=True)
    (folder / "galleryinfo.txt").write_text(
        "Title: First\nUpload Time: 2024-01-02 03:04\n"
        "Uploaded By: uploader\nDownloaded: 2024-02-03 04:05\n"
        "Tags: artist:first\nUploader's Comments\ncomment\n"
        "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3\n",
        encoding="utf-8",
    )
    (folder / "001.jpg").write_bytes(b"source page")
    return folder


def test_completion_discovery_preserves_collections_and_stops_at_gallery_leaves(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "source"
    first = _gallery(root, "collection/1001")
    second = _gallery(root, "collection/1002")
    _gallery(first, "2001")
    scanned: list[Path] = []
    original_scandir = os.scandir

    def scandir(path: int | str | os.PathLike[str]) -> Iterator[os.DirEntry[str]]:
        if isinstance(path, int):
            return original_scandir(path)
        target = Path(path)
        assert target not in {first, second}
        scanned.append(target)
        return original_scandir(path)

    monkeypatch.setattr(os, "scandir", scandir)
    with FilesystemSource(root) as source:
        page = source.list_gallery_locators(after_locator=None, limit=256)
        assert page.items == (("collection", "1001"), ("collection", "1002"))
        assert page.terminal
        markers = tuple(source.iter_completion_markers())
        assert tuple(locator for locator, _marker in markers) == page.items
        assert all(marker.observation_version == 3 for _locator, marker in markers)
    assert scanned == [root, root / "collection"]


def test_adapter_marker_owns_exact_metadata_bytes_without_a_deep_gallery_index(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "source"
    folder = _gallery(root)
    metadata = folder / "galleryinfo.txt"
    expected = metadata.read_bytes()

    def unexpected_scandir(
        _path: int | str | os.PathLike[str],
    ) -> Iterator[os.DirEntry[str]]:
        raise AssertionError("metadata-only observation must not enumerate entries")

    monkeypatch.setattr(os, "scandir", unexpected_scandir)
    with FilesystemSource(root) as source:
        marker = VNextFilesystemSourceAdapter(source).observe_completion_marker(
            ("1001",)
        )
        assert marker.file.name_bytes == b"galleryinfo.txt"
        assert marker.file.artifact_role.value == "metadata"
        assert marker.file.content.file_sha256 == sha256(expected).digest()
        assert marker.file.content.size_bytes == len(expected)
        assert marker.file.inode == metadata.stat().st_ino
        assert marker.observation_version == FILESYSTEM_OBSERVATION_VERSION
        assert source._discovery_connection is None


def test_every_marker_probe_hashes_bytes_even_when_stat_facts_compare_equal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "source"
    folder = _gallery(root)
    metadata = folder / "galleryinfo.txt"
    original_stat = FilesystemStat.from_os_stat(metadata.stat())
    original_from_stat = FilesystemStat.from_os_stat

    def same_metadata_stat(
        _cls: type[FilesystemStat], value: os.stat_result
    ) -> FilesystemStat:
        if value.st_ino == original_stat.inode:
            return original_stat
        return original_from_stat(value)

    monkeypatch.setattr(FilesystemStat, "from_os_stat", classmethod(same_metadata_stat))
    with FilesystemSource(root) as source:
        adapter = VNextFilesystemSourceAdapter(source)
        before = adapter.observe_completion_marker(("1001",))
        metadata.write_bytes(metadata.read_bytes().replace(b"First", b"Other"))
        after = adapter.observe_completion_marker(("1001",))
    assert before.file.modified_ns == after.file.modified_ns
    assert before.file.changed_ns == after.file.changed_ns
    assert before.file.content.file_sha256 != after.file.content.file_sha256
    assert before != after


def test_equal_metadata_bytes_with_replaced_inode_are_a_new_marker(
    tmp_path: Path,
) -> None:
    root = tmp_path / "source"
    folder = _gallery(root)
    metadata = folder / "galleryinfo.txt"
    with FilesystemSource(root) as source:
        before = source.observe_completion_marker(("1001",))
        replacement = folder / "replacement.txt"
        replacement.write_bytes(metadata.read_bytes())
        replacement.replace(metadata)
        after = source.observe_completion_marker(("1001",))
    assert before.expected_sha256 == after.expected_sha256
    assert before.stat.inode != after.stat.inode
    assert before != after


def test_missing_metadata_after_discovery_is_a_retryable_source_change(
    tmp_path: Path,
) -> None:
    root = tmp_path / "source"
    folder = _gallery(root)
    with FilesystemSource(root) as source:
        assert source.list_gallery_locators(after_locator=None, limit=1).items
        (folder / "galleryinfo.txt").unlink()
        with pytest.raises(FilesystemSourceChangedError):
            source.observe_completion_marker(("1001",))


def test_marker_tracks_truncate_partial_and_complete_without_parsing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "source"
    folder = _gallery(root)
    metadata = folder / "galleryinfo.txt"
    complete = metadata.read_bytes()

    def unexpected_parse(_folder: Path, _content: bytes, *, modified_ns: int) -> None:
        raise AssertionError("completion marker must not parse in-progress bytes")

    monkeypatch.setattr(
        filesystem_module, "_parse_galleryinfo_content", unexpected_parse
    )
    with FilesystemSource(root) as source:
        adapter = VNextFilesystemSourceAdapter(source)
        before = adapter.observe_completion_marker(("1001",))
        for content in (b"", b"Title: Fir", complete):
            metadata.write_bytes(content)
            marker = adapter.observe_completion_marker(("1001",))
            assert marker.file.content.size_bytes == len(content)
            assert marker.file.content.file_sha256 == sha256(content).digest()
            assert marker != before
            before = marker


@pytest.mark.parametrize("content", (b"", b"Title: Fir", b"not gallery metadata"))
def test_full_gallery_observation_retries_startup_incomplete_metadata(
    tmp_path: Path, content: bytes
) -> None:
    root = tmp_path / "source"
    folder = _gallery(root)
    (folder / "galleryinfo.txt").write_bytes(content)
    with FilesystemSource(root) as source:
        marker = source.observe_completion_marker(("1001",))
        assert marker.stat.size_bytes == len(content)
        with pytest.raises(FilesystemSourceChangedError):
            source.observe_gallery(("1001",))


@pytest.mark.parametrize("invalid", ("utf8", "date"))
def test_full_gallery_observation_rejects_semantically_invalid_metadata(
    tmp_path: Path, invalid: str
) -> None:
    root = tmp_path / "source"
    folder = _gallery(root)
    metadata = folder / "galleryinfo.txt"
    original = metadata.read_bytes()
    metadata.write_bytes(
        original + b"\xff"
        if invalid == "utf8"
        else original.replace(b"2024-01-02", b"2024-99-99")
    )
    with FilesystemSource(root) as source:
        with pytest.raises(FilesystemObservationError) as raised:
            source.observe_gallery(("1001",))
        assert not isinstance(raised.value, FilesystemSourceChangedError)


def test_parse_failure_rechecks_source_changes_before_classifying_invalid_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "source"
    folder = _gallery(root)
    metadata = folder / "galleryinfo.txt"

    def changed_while_parsing(
        _folder: Path, _content: bytes, *, modified_ns: int
    ) -> None:
        metadata.write_bytes(b"Title: source changed while parsing")
        raise ValueError("parser failure while source changed")

    monkeypatch.setattr(
        filesystem_module, "_parse_galleryinfo_content", changed_while_parsing
    )
    with (
        FilesystemSource(root) as source,
        pytest.raises(FilesystemSourceChangedError, match="changed while parsing"),
    ):
        source.observe_gallery(("1001",))


@pytest.mark.parametrize("invalid", ("symlink", "ancestor-symlink"))
def test_unsafe_metadata_is_not_a_retryable_source_change(
    tmp_path: Path,
    invalid: str,
) -> None:
    root = tmp_path / "source"
    folder = _gallery(root)
    metadata = folder / "galleryinfo.txt"
    if invalid == "symlink":
        target = tmp_path / "metadata.txt"
        metadata.replace(target)
        metadata.symlink_to(target)
    else:
        target_folder = tmp_path / "outside"
        folder.rename(target_folder)
        folder.symlink_to(target_folder, target_is_directory=True)
    with (
        FilesystemSource(root) as source,
        pytest.raises(FilesystemObservationError) as raised,
    ):
        source.observe_completion_marker(("1001",))
    assert not isinstance(raised.value, FilesystemSourceChangedError)


def test_marker_size_is_checked_before_reading(tmp_path: Path) -> None:
    root = tmp_path / "source"
    folder = _gallery(root)
    (folder / "galleryinfo.txt").write_bytes(b"x" * (MAX_METADATA_BYTES + 1))
    with (
        FilesystemSource(root) as source,
        pytest.raises(FilesystemObservationError, match="size is outside policy"),
    ):
        source.observe_completion_marker(("1001",))


def test_marker_accepts_the_exact_metadata_size_limit(tmp_path: Path) -> None:
    root = tmp_path / "source"
    folder = _gallery(root)
    metadata = folder / "galleryinfo.txt"
    content = metadata.read_bytes()
    metadata.write_bytes(
        content + b"\n" + b"x" * (MAX_METADATA_BYTES - len(content) - 1)
    )
    with FilesystemSource(root) as source:
        marker = VNextFilesystemSourceAdapter(source).observe_completion_marker(
            ("1001",)
        )
    assert marker.file.content.size_bytes == MAX_METADATA_BYTES


def test_marker_growth_during_read_is_bounded_and_retryable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "source"
    _gallery(root)
    read_bytes = 0

    def growing_read(_descriptor: int, maximum: int) -> bytes:
        nonlocal read_bytes
        read_bytes += maximum
        return b"x" * maximum

    monkeypatch.setattr(os, "read", growing_read)
    with (
        FilesystemSource(root) as source,
        pytest.raises(FilesystemSourceChangedError, match="size changed"),
    ):
        source.observe_completion_marker(("1001",))
    assert read_bytes == MAX_METADATA_BYTES + 1


@pytest.mark.parametrize("replacement", ("marker", "gallery"))
def test_marker_probe_revalidates_name_and_directory_identity_after_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replacement: str,
) -> None:
    root = tmp_path / "source"
    folder = _gallery(root)
    original_read = FilesystemSource._read_metadata_descriptor

    def replacing_read(
        source: FilesystemSource,
        descriptor: int,
        path: Path,
        expected: FilesystemStat,
    ) -> tuple[bytes, bytes]:
        content, digest = original_read(source, descriptor, path, expected)
        if replacement == "marker":
            new = folder / "replacement.txt"
            new.write_bytes(content)
            new.replace(folder / "galleryinfo.txt")
        else:
            moved = tmp_path / "moved"
            folder.rename(moved)
            folder.symlink_to(moved, target_is_directory=True)
        return content, digest

    monkeypatch.setattr(FilesystemSource, "_read_metadata_descriptor", replacing_read)
    with (
        FilesystemSource(root) as source,
        pytest.raises(FilesystemSourceChangedError),
    ):
        source.observe_completion_marker(("1001",))


def test_interrupted_marker_read_closes_every_open_descriptor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "source"
    _gallery(root, "collection/1001")
    opened: set[int] = set()
    closed: set[int] = set()
    original_open = os.open
    original_close = os.close

    def tracked_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        descriptor = original_open(path, flags, mode, dir_fd=dir_fd)
        opened.add(descriptor)
        return descriptor

    def tracked_close(descriptor: int) -> None:
        closed.add(descriptor)
        original_close(descriptor)

    def interrupted_read(_descriptor: int, _maximum: int) -> bytes:
        raise KeyboardInterrupt

    monkeypatch.setattr(os, "open", tracked_open)
    monkeypatch.setattr(os, "close", tracked_close)
    monkeypatch.setattr(os, "read", interrupted_read)
    with FilesystemSource(root) as source, pytest.raises(KeyboardInterrupt):
        source.observe_completion_marker(("collection", "1001"))
    assert len(opened) == 4
    assert closed == opened


def test_marker_iterator_pages_without_building_gallery_payloads(
    tmp_path: Path,
) -> None:
    root = tmp_path / "source"
    count = 260
    for offset in range(count):
        _gallery(root, str(1000 + offset))
    with FilesystemSource(root) as source:
        observed = sum(1 for _entry in source.iter_completion_markers())
        connection = source._discovery_index()
        assert observed == count
        assert connection.execute("SELECT count(*) FROM locators").fetchone() == (
            count,
        )
        assert connection.execute(
            "SELECT count(*) FROM gallery_entries"
        ).fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM gallery_audits").fetchone() == (
            0,
        )
