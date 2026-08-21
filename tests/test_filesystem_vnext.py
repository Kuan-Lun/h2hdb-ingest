from __future__ import annotations

import os
from collections.abc import Iterator
from hashlib import sha256
from pathlib import Path

import pytest
from h2hdb import VNextIngestSourceAdapter

from h2hdb_ingest.core_source import VNextFilesystemSourceAdapter
from h2hdb_ingest.filesystem import (
    FilesystemEntryType,
    FilesystemObservationError,
    FilesystemSource,
)


def _gallery(root: Path, name: str = "nested/1001") -> Path:
    folder = root.joinpath(*name.split("/"))
    folder.mkdir(parents=True)
    (folder / "galleryinfo.txt").write_text(
        "\n".join(
            (
                "Title: A title",
                "Upload Time: 2024-01-02 03:04",
                "Uploaded By: uploader",
                "Downloaded: 2024-02-03 04:05",
                "Tags: artist:first, language:english",
                "Uploader's Comments",
                "A comment",
                "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3",
            )
        ),
        encoding="utf-8",
    )
    (folder / "002.jpg").write_bytes(b"second")
    (folder / "001.jpg").write_bytes(b"first")
    return folder


def test_source_observation_is_sorted_bounded_and_replayable(tmp_path: Path) -> None:
    root = tmp_path / "download"
    folder = _gallery(root)
    (folder / "child").mkdir()
    (folder / "linked").symlink_to(folder / "001.jpg")

    source = FilesystemSource(root)

    assert source.source_root_components == tuple(root.resolve().parts[1:])
    assert source.list_gallery_locators(after_locator=None, limit=1).items == (
        ("nested", "1001"),
    )
    assert (
        source.list_gallery_locators(after_locator=("nested", "1001"), limit=1).items
        == ()
    )
    observation = source.observe_gallery(("nested", "1001"))
    assert observation.metadata.gid == 1001
    assert observation.metadata.title == "A title"
    assert observation.metadata.comment == "A comment"
    assert observation.metadata.source_file_count == 3
    assert observation.metadata.page_count == 2
    _reopened, first_file_page = source.list_files(
        ("nested", "1001"), after_name=None, limit=2
    )
    _reopened, final_file_page = source.list_files(
        ("nested", "1001"), after_name=b"002.jpg", limit=2
    )
    first = first_file_page.items + final_file_page.items
    _reopened, replay_page = source.list_files(
        ("nested", "1001"), after_name=None, limit=2
    )
    replay = replay_page.items
    assert [item.name_bytes for item in first] == [
        b"001.jpg",
        b"002.jpg",
        b"galleryinfo.txt",
    ]
    assert replay == first[:2]
    assert b"".join(first[0].content_parts()) == b"first"
    assert (
        sha256(b"first").digest()
        == sha256(b"".join(replay[0].content_parts())).digest()
    )

    _reopened, first_directory_page = source.list_directories(
        ("nested", "1001"), after_name=None, limit=2
    )
    _reopened, final_directory_page = source.list_directories(
        ("nested", "1001"),
        after_name=first_directory_page.items[-1].name_bytes,
        limit=256,
    )
    directories = first_directory_page.items + final_directory_page.items
    assert [item.name_bytes for item in directories] == sorted(
        item.name_bytes for item in directories
    )
    kinds = {item.name_bytes: item.file_type for item in directories}
    assert kinds[b"001.jpg"] is FilesystemEntryType.REGULAR
    assert kinds[b"child"] is FilesystemEntryType.DIRECTORY
    assert kinds[b"linked"] is FilesystemEntryType.SYMLINK

    assert [item.name_bytes for item in first_file_page.items] == [
        b"001.jpg",
        b"002.jpg",
    ]
    assert not first_file_page.terminal
    assert [item.name_bytes for item in final_file_page.items] == [b"galleryinfo.txt"]
    assert final_file_page.terminal

    assert len(first_directory_page.items) == 2
    assert not first_directory_page.terminal

    _reopened, tag_page = source.list_tags(
        ("nested", "1001"), after_position=1, limit=2
    )
    assert tag_page.items == (("language", "english"),)
    assert tag_page.terminal


def test_observation_replay_rejects_changed_source(tmp_path: Path) -> None:
    root = tmp_path / "download"
    folder = _gallery(root, "1002")
    source = FilesystemSource(root)
    source.list_files(("1002",), after_name=None, limit=1)

    (folder / "001.jpg").write_bytes(b"changed")

    with pytest.raises(
        FilesystemObservationError, match="changed between bounded pages"
    ):
        source.list_files(("1002",), after_name=b"001.jpg", limit=1)


def test_discovery_rejects_symlink_metadata(tmp_path: Path) -> None:
    root = tmp_path / "download"
    root.mkdir()
    metadata = tmp_path / "outside.txt"
    metadata.write_text("outside", encoding="utf-8")
    gallery = root / "1003"
    gallery.mkdir()
    (gallery / "galleryinfo.txt").symlink_to(metadata)

    source = FilesystemSource(root)

    with pytest.raises(FilesystemObservationError, match="must not be a symlink"):
        source.list_gallery_locators(after_locator=None, limit=1)


def test_discovery_snapshot_is_built_once_and_closed_explicitly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "download"
    _gallery(root, "nested/1001")
    _gallery(root, "nested/1002")
    scans = 0
    original_scandir = os.scandir

    def counted_scandir(path: os.PathLike[str] | str) -> Iterator[os.DirEntry[str]]:
        nonlocal scans
        scans += 1
        return original_scandir(path)

    monkeypatch.setattr(os, "scandir", counted_scandir)
    source = FilesystemSource(root)
    first = source.list_gallery_locators(after_locator=None, limit=1)
    first_scan_count = scans
    second = source.list_gallery_locators(
        after_locator=first.items[-1],
        limit=1,
    )

    assert first.items == (("nested", "1001"),)
    assert second.items == (("nested", "1002"),)
    assert scans == first_scan_count
    source.close()
    source.close()
    with pytest.raises(RuntimeError, match="closed"):
        source.list_gallery_locators(after_locator=None, limit=1)


def test_public_adapter_exposes_only_keyset_pages(tmp_path: Path) -> None:
    root = tmp_path / "download"
    _gallery(root, "nested/1001")
    _gallery(root, "nested/1002")
    adapter = VNextFilesystemSourceAdapter(FilesystemSource(root))

    assert isinstance(adapter, VNextIngestSourceAdapter)
    locator_page = adapter.list_gallery_locators(after_locator=None, limit=1)
    assert locator_page.items == (("nested", "1001"),)
    assert locator_page.next_after == ("nested", "1001")
    assert not locator_page.terminal
    final_locator_page = adapter.list_gallery_locators(
        after_locator=locator_page.next_after,
        limit=1,
    )
    assert final_locator_page.items == (("nested", "1002"),)
    assert final_locator_page.next_after is None
    assert final_locator_page.terminal

    observation = adapter.observe_gallery(("nested", "1001"))
    file_page = adapter.list_file_observations(
        observation,
        after_name_bytes=None,
        limit=2,
    )
    assert [item.name_bytes for item in file_page.items] == [b"001.jpg", b"002.jpg"]
    assert file_page.next_after == b"002.jpg"
    assert not file_page.terminal

    directory_page = adapter.list_directory_observations(
        observation,
        after_name_bytes=None,
        limit=2,
    )
    assert directory_page.next_after == b"002.jpg"
    assert not directory_page.terminal

    first_tag_page = adapter.list_tag_observations(
        observation,
        after_ordinal=None,
        limit=1,
    )
    assert [(item.namespace, item.value) for item in first_tag_page.items] == [
        ("artist", "first")
    ]
    assert first_tag_page.next_after == 0
    assert not first_tag_page.terminal
    final_tag_page = adapter.list_tag_observations(
        observation,
        after_ordinal=first_tag_page.next_after,
        limit=1,
    )
    assert [(item.namespace, item.value) for item in final_tag_page.items] == [
        ("language", "english")
    ]
    assert final_tag_page.next_after is None
    assert final_tag_page.terminal
