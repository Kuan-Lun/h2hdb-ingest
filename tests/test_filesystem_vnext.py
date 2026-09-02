from __future__ import annotations

import os
import stat
from collections.abc import Iterator
from hashlib import sha256
from pathlib import Path

import pytest
from h2hdb import VNextIngestSourceAdapter

from h2hdb_ingest.core_source import VNextFilesystemSourceAdapter
from h2hdb_ingest.filesystem import (
    FILESYSTEM_OBSERVATION_VERSION,
    FilesystemArtifactSourceRole,
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


def test_artifact_roles_and_page_count_are_adapter_owned(tmp_path: Path) -> None:
    root = tmp_path / "download"
    folder = _gallery(root, "1004")
    (folder / "003.GIF").write_bytes(b"gif")
    (folder / "notes.json").write_bytes(b"not a page")

    source = FilesystemSource(root)
    observation, page = source.list_files(("1004",), after_name=None, limit=256)

    assert FILESYSTEM_OBSERVATION_VERSION == 2
    assert observation.metadata.scan_observation_version == 2
    assert observation.metadata.source_file_count == 5
    assert observation.metadata.page_count == 3
    assert {item.name_bytes: item.artifact_role for item in page.items} == {
        b"001.jpg": FilesystemArtifactSourceRole.PAGE,
        b"002.jpg": FilesystemArtifactSourceRole.PAGE,
        b"003.GIF": FilesystemArtifactSourceRole.PAGE,
        b"galleryinfo.txt": FilesystemArtifactSourceRole.METADATA,
        b"notes.json": FilesystemArtifactSourceRole.OTHER,
    }


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


def test_gallery_index_is_reused_across_pages_with_exact_reference_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "download"
    folder = _gallery(root, "1005")
    for position in range(3, 261):
        (folder / f"{position:03d}.jpg").write_bytes(f"page-{position}".encode())
    for position in range(130):
        (folder / f"child-{position:03d}").mkdir()
    tag_values = tuple(f"tag-{position:03d}" for position in range(257))
    metadata = (folder / "galleryinfo.txt").read_text(encoding="utf-8")
    (folder / "galleryinfo.txt").write_text(
        metadata.replace(
            "Tags: artist:first, language:english",
            "Tags: " + ", ".join(f"misc:{value}" for value in tag_values),
        ),
        encoding="utf-8",
    )
    direct_entries = tuple(
        sorted(folder.iterdir(), key=lambda entry: os.fsencode(entry.name))
    )
    expected_file_facts: list[tuple[object, ...]] = []
    expected_directory_facts: list[tuple[object, ...]] = []
    for entry in direct_entries:
        value = entry.stat(follow_symlinks=False)
        name = os.fsencode(entry.name)
        if stat.S_ISREG(value.st_mode):
            expected_file_facts.append(
                (
                    name,
                    sha256(entry.read_bytes()).digest(),
                    value.st_size,
                    ("metadata" if entry.name == "galleryinfo.txt" else "page"),
                    value.st_dev,
                    value.st_ino,
                    value.st_mtime_ns,
                    value.st_ctime_ns,
                )
            )
        expected_directory_facts.append(
            (
                name,
                value.st_size,
                value.st_dev,
                value.st_ino,
                value.st_mtime_ns,
                value.st_ctime_ns,
                (
                    int(FilesystemEntryType.REGULAR)
                    if stat.S_ISREG(value.st_mode)
                    else int(FilesystemEntryType.DIRECTORY)
                ),
            )
        )
    expected_file_names = tuple(fact[0] for fact in expected_file_facts)
    expected_entry_names = tuple(fact[0] for fact in expected_directory_facts)

    source = FilesystemSource(root)
    source.list_gallery_locators(after_locator=None, limit=1)
    target_scans = 0
    original_scandir = os.scandir

    def counted_scandir(
        path: int | os.PathLike[str] | str,
    ) -> Iterator[os.DirEntry[str]]:
        nonlocal target_scans
        if not isinstance(path, int) and Path(path) == folder:
            target_scans += 1
        return original_scandir(path)

    monkeypatch.setattr(os, "scandir", counted_scandir)
    adapter = VNextFilesystemSourceAdapter(source)
    observation = adapter.observe_gallery(("1005",))

    file_names: list[bytes] = []
    file_facts: list[tuple[object, ...]] = []
    after_name: bytes | None = None
    file_page_count = 0
    while True:
        file_page = adapter.list_file_observations(
            observation,
            after_name_bytes=after_name,
            limit=256,
        )
        file_page_count += 1
        file_names.extend(item.name_bytes for item in file_page.items)
        file_facts.extend(
            (
                item.name_bytes,
                item.content.file_sha256,
                item.content.size_bytes,
                item.artifact_role.value,
                item.device,
                item.inode,
                item.modified_ns,
                item.changed_ns,
            )
            for item in file_page.items
        )
        if file_page.terminal:
            break
        assert isinstance(file_page.next_after, bytes)
        after_name = file_page.next_after

    entry_names: list[bytes] = []
    directory_facts: list[tuple[object, ...]] = []
    after_name = None
    directory_page_count = 0
    while True:
        directory_page = adapter.list_directory_observations(
            observation,
            after_name_bytes=after_name,
            limit=192,
        )
        directory_page_count += 1
        entry_names.extend(item.name_bytes for item in directory_page.items)
        directory_facts.extend(
            (
                item.name_bytes,
                item.size_bytes,
                item.device,
                item.inode,
                item.modified_ns,
                item.changed_ns,
                int(item.file_type),
            )
            for item in directory_page.items
        )
        if directory_page.terminal:
            break
        assert isinstance(directory_page.next_after, bytes)
        after_name = directory_page.next_after

    tags: list[tuple[str, str]] = []
    after_ordinal: int | None = None
    tag_page_count = 0
    while True:
        tag_page = adapter.list_tag_observations(
            observation,
            after_ordinal=after_ordinal,
            limit=256,
        )
        tag_page_count += 1
        tags.extend((item.namespace, item.value) for item in tag_page.items)
        if tag_page.terminal:
            break
        assert isinstance(tag_page.next_after, int)
        after_ordinal = tag_page.next_after

    assert tuple(file_names) == expected_file_names
    assert tuple(entry_names) == expected_entry_names
    assert tuple(tags) == tuple(("misc", value) for value in tag_values)
    assert tuple(file_facts) == tuple(expected_file_facts)
    assert tuple(directory_facts) == tuple(expected_directory_facts)
    assert observation.metadata.modified_time == (
        (folder / "galleryinfo.txt").stat().st_mtime_ns // 1_000
    )
    assert observation.metadata.source_file_count == len(expected_file_names)
    assert observation.metadata.page_count == 260
    assert (file_page_count, directory_page_count, tag_page_count) == (2, 3, 2)
    # The legacy implementation built and audited three entry indexes for every
    # named page, while tags rebuilt once.  The immutable active index is built
    # once and every returned page performs one fresh, exact directory audit.
    legacy_scan_count = (
        1 + 3 * file_page_count + 3 * directory_page_count + (tag_page_count)
    )
    assert legacy_scan_count == 18
    assert (
        target_scans
        == (1 + file_page_count + directory_page_count + tag_page_count)
        == 8
    )
    source.close()


def test_gallery_index_keeps_only_one_payload_and_rebuilds_against_fixed_audit(
    tmp_path: Path,
) -> None:
    root = tmp_path / "download"
    first = _gallery(root, "1006")
    second = _gallery(root, "1007")
    (second / "only-second.webp").write_bytes(b"second gallery only")
    source = FilesystemSource(root)
    source.list_gallery_locators(after_locator=None, limit=2)

    first_observation = source.observe_gallery(("1006",))
    second_observation = source.observe_gallery(("1007",))
    connection = source._discovery_index()

    assert connection.execute("SELECT count(*) FROM gallery_entries").fetchone() == (4,)
    assert connection.execute(
        "SELECT count(*) FROM active_gallery_snapshot"
    ).fetchone() == (1,)
    assert connection.execute("SELECT count(*) FROM gallery_audits").fetchone() == (2,)
    assert source.observe_gallery(("1006",)) == first_observation
    assert source.observe_gallery(("1007",)) == second_observation
    active_before = connection.execute(
        "SELECT * FROM active_gallery_snapshot"
    ).fetchone()
    entries_before = connection.execute(
        "SELECT * FROM gallery_entries ORDER BY name_bytes"
    ).fetchall()
    tags_before = connection.execute(
        "SELECT * FROM gallery_tags ORDER BY ordinal"
    ).fetchall()

    (first / "001.jpg").write_bytes(b"changed without a directory entry change")

    with pytest.raises(
        FilesystemObservationError,
        match="changed between bounded pages",
    ):
        source.observe_gallery(("1006",))

    assert connection.execute("SELECT * FROM active_gallery_snapshot").fetchone() == (
        active_before
    )
    assert (
        connection.execute(
            "SELECT * FROM gallery_entries ORDER BY name_bytes"
        ).fetchall()
        == entries_before
    )
    assert (
        connection.execute("SELECT * FROM gallery_tags ORDER BY ordinal").fetchall()
        == tags_before
    )
    assert source.observe_gallery(("1007",)) == second_observation
    _observed, second_files = source.list_files(("1007",), after_name=None, limit=256)
    assert tuple(item.name_bytes for item in second_files.items) == (
        b"001.jpg",
        b"002.jpg",
        b"galleryinfo.txt",
        b"only-second.webp",
    )


def test_gallery_page_boundary_rejects_mutation_during_fresh_audit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "download"
    folder = _gallery(root, "1008")
    source = FilesystemSource(root)
    source.list_gallery_locators(after_locator=None, limit=1)
    source.observe_gallery(("1008",))
    original_scandir = os.scandir
    mutate_on_next_gallery_scan = True

    def mutating_scandir(
        path: int | os.PathLike[str] | str,
    ) -> Iterator[os.DirEntry[str]]:
        nonlocal mutate_on_next_gallery_scan
        if (
            mutate_on_next_gallery_scan
            and not isinstance(path, int)
            and Path(path) == folder
        ):
            mutate_on_next_gallery_scan = False
            (folder / "002.jpg").write_bytes(b"changed during boundary audit")
        return original_scandir(path)

    monkeypatch.setattr(os, "scandir", mutating_scandir)

    with pytest.raises(
        FilesystemObservationError,
        match="changed between bounded pages",
    ):
        source.list_files(("1008",), after_name=None, limit=256)


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
