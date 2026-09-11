from __future__ import annotations

import os
import stat
import tempfile
from collections.abc import Iterator
from hashlib import sha256
from pathlib import Path

import pytest
from h2h_galleryinfo_parser import parse_galleryinfo
from h2hdb import VNextIngestSourceAdapter, VNextSourceDeferredError

import h2hdb_ingest.filesystem as filesystem_module
from h2hdb_ingest.artifact import MAX_METADATA_BYTES
from h2hdb_ingest.core_source import VNextFilesystemSourceAdapter
from h2hdb_ingest.filesystem import (
    FILESYSTEM_OBSERVATION_VERSION,
    FilesystemArtifactSourceRole,
    FilesystemEntryType,
    FilesystemObservationError,
    FilesystemSource,
    FilesystemSourceChangedError,
)
from h2hdb_ingest.progress import IngestProgress

type _OpenPath = str | bytes | os.PathLike[str] | os.PathLike[bytes]


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
    (folder / "galleryinfo.txt").touch()
    return folder


class _CheckpointInterrupted(Exception):
    pass


class _StopOnceAt:
    def __init__(self, position: int) -> None:
        self._position = position
        self.calls = 0
        self.armed = False
        self.interrupted = False

    def arm(self) -> None:
        self.calls = 0
        self.armed = True

    def __call__(self) -> None:
        if not self.armed:
            return
        self.calls += 1
        if not self.interrupted and self.calls == self._position:
            self.interrupted = True
            raise _CheckpointInterrupted


def test_source_observation_is_sorted_bounded_and_replayable(tmp_path: Path) -> None:
    root = tmp_path / "download"
    folder = _gallery(root)
    (folder / "child").mkdir()
    (folder / "linked").symlink_to(folder / "001.jpg")
    checkpoints = 0

    def checkpoint() -> None:
        nonlocal checkpoints
        checkpoints += 1

    source = FilesystemSource(root, checkpoint=checkpoint)

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
    assert checkpoints > 0


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
    (folder / "galleryinfo.txt").touch()

    source = FilesystemSource(root)
    observation, page = source.list_files(("1004",), after_name=None, limit=256)

    assert FILESYSTEM_OBSERVATION_VERSION == 3
    assert observation.metadata.scan_observation_version == 3
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


def test_discovery_checkpoint_interrupts_and_cleans_partial_spill_index(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "download"
    _gallery(root, "nested/1001")
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    stop = _StopOnceAt(4)
    source = FilesystemSource(root, checkpoint=stop)
    stop.arm()

    with pytest.raises(_CheckpointInterrupted):
        source.list_gallery_locators(after_locator=None, limit=1)

    assert stop.interrupted
    assert source._discovery_connection is None
    assert source._discovery_temporary is None
    assert tuple(tmp_path.glob("h2hdb-ingest-discovery-*")) == ()
    assert source.list_gallery_locators(after_locator=None, limit=1).items == (
        ("nested", "1001"),
    )
    source.close()


def test_gallery_metadata_size_is_bounded_before_parser(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "download"
    folder = _gallery(root, "1008")
    (folder / "galleryinfo.txt").write_bytes(b"x" * (MAX_METADATA_BYTES + 1))
    parser_called = False

    def unexpected_parser(
        _folder: Path,
        _content: bytes,
        *,
        modified_ns: int,
    ) -> object:
        del modified_ns
        nonlocal parser_called
        parser_called = True
        raise AssertionError("oversized metadata reached the parser")

    monkeypatch.setattr(
        filesystem_module,
        "_parse_galleryinfo_content",
        unexpected_parser,
    )
    source = FilesystemSource(root)

    with pytest.raises(FilesystemObservationError, match="size is outside policy"):
        source.observe_gallery(("1008",))

    assert not parser_called
    source.close()


def test_gallery_metadata_accepts_the_exact_size_limit(tmp_path: Path) -> None:
    root = tmp_path / "download"
    folder = _gallery(root, "1007")
    metadata_path = folder / "galleryinfo.txt"
    metadata = metadata_path.read_bytes()
    padding_size = MAX_METADATA_BYTES - len(metadata) - 1
    assert padding_size > 0
    metadata_path.write_bytes(metadata + b"\n" + (b"x" * padding_size))
    source = FilesystemSource(root)

    observation = source.observe_gallery(("1007",))

    assert observation.metadata.gid == 1007
    assert observation.metadata.title == "A title"
    assert metadata_path.stat().st_size == MAX_METADATA_BYTES
    source.close()


@pytest.mark.parametrize(
    "metadata",
    [
        (
            "\nTitle: A title\nUpload Time: 2024-01-02 03:04\n"
            "Uploaded By: uploader\nDownloaded: 2024-02-03 04:05\n"
            "Tags: artist:first, :second, plain\nUploader's Comments\n"
            " A comment \nDownloaded from E-Hentai Galleries by the "
            "Hentai@Home Downloader <3\n\n"
        ),
        (
            "Title: First\r\nTitle: Last\r\nUpload Time: 2024-01-02 03:04\r\n"
            "Uploaded By: uploader\r\nDownloaded: 2024-02-03 04:05\r\n"
            "Tags: language:english\r\nUploader's Comments\r\n"
            "line one\r\n\r\nline two\r\nDownloaded from E-Hentai Galleries by "
            "the Hentai@Home Downloader <3\r\nignored"
        ),
    ],
)
def test_snapshot_parser_matches_the_pinned_galleryinfo_parser(
    tmp_path: Path,
    metadata: str,
) -> None:
    root = tmp_path / "download"
    folder = _gallery(root, "1010")
    metadata_path = folder / "galleryinfo.txt"
    content = metadata.encode("utf-8")
    metadata_path.write_bytes(content)

    expected = parse_galleryinfo(folder)
    actual = filesystem_module._parse_galleryinfo_content(
        folder,
        content,
        modified_ns=metadata_path.stat().st_mtime_ns,
    )

    assert actual == expected


def test_file_stream_checkpoint_interrupts_between_chunks_and_closes_descriptors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "download"
    folder = _gallery(root, "1009")
    (folder / "001.jpg").write_bytes(b"x" * (5 * 1024 * 1024))
    (folder / "galleryinfo.txt").touch()
    stop = _StopOnceAt(3)
    source = FilesystemSource(root, checkpoint=stop)
    _observation, page = source.list_files(("1009",), after_name=None, limit=256)
    streamed = next(item for item in page.items if item.name_bytes == b"001.jpg")
    stop.arm()
    opened: set[int] = set()
    closed: set[int] = set()
    original_open = os.open
    original_close = os.close

    def tracking_open(
        path: _OpenPath,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        descriptor = original_open(path, flags, mode, dir_fd=dir_fd)
        opened.add(descriptor)
        return descriptor

    def tracking_close(descriptor: int) -> None:
        closed.add(descriptor)
        original_close(descriptor)

    monkeypatch.setattr(os, "open", tracking_open)
    monkeypatch.setattr(os, "close", tracking_close)
    parts = streamed.content_parts()

    assert len(next(parts)) == 4 * 1024 * 1024
    with pytest.raises(_CheckpointInterrupted):
        next(parts)

    assert stop.interrupted
    assert opened
    assert closed == opened
    source.close()


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
    (second / "galleryinfo.txt").touch()
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
        match="newer than its completion marker",
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


@pytest.mark.parametrize("image_offset_ns", (-1, 0, 1))
def test_completion_timestamp_accepts_equal_and_defers_only_newer_images(
    tmp_path: Path, image_offset_ns: int
) -> None:
    root = tmp_path / "download"
    folder = _gallery(root, "1001")
    marker_ns = 1_700_000_000_000_000_000
    for image in (folder / "001.jpg", folder / "002.jpg"):
        image_ns = marker_ns + image_offset_ns
        os.utime(image, ns=(image_ns, image_ns))
    os.utime(folder / "galleryinfo.txt", ns=(marker_ns, marker_ns))
    with FilesystemSource(root) as source:
        adapter = VNextFilesystemSourceAdapter(source)
        if image_offset_ns > 0:
            with pytest.raises(
                VNextSourceDeferredError, match="newer than its completion marker"
            ):
                adapter.observe_gallery(("1001",))
        else:
            observed = adapter.observe_gallery(("1001",))
            assert observed.metadata.page_count == 2
            assert observed.metadata.scan_observation_version == 3
            assert adapter.observe_completion_marker(("1001",)).file.modified_ns == (
                marker_ns
            )


def test_newer_non_image_does_not_block_completed_gallery(tmp_path: Path) -> None:
    root = tmp_path / "download"
    folder = _gallery(root, "1001")
    notes = folder / "notes.txt"
    notes.write_bytes(b"downloader bookkeeping")
    newer_ns = (folder / "galleryinfo.txt").stat().st_mtime_ns + 1
    os.utime(notes, ns=(newer_ns, newer_ns))
    with FilesystemSource(root) as source:
        assert source.observe_gallery(("1001",)).metadata.page_count == 2


@pytest.mark.parametrize("mutation", ("add", "remove", "replace", "mtime", "bytes"))
def test_final_marker_probe_revalidates_the_entire_observed_entry_set(
    tmp_path: Path, mutation: str
) -> None:
    root = tmp_path / "download"
    folder = _gallery(root, "1001")
    with FilesystemSource(root) as source:
        adapter = VNextFilesystemSourceAdapter(source)
        adapter.observe_gallery(("1001",))
        target = folder / "002.jpg"
        if mutation == "add":
            (folder / "003.jpg").write_bytes(b"new page")
        elif mutation == "remove":
            target.unlink()
        elif mutation == "replace":
            replacement = folder / "replacement"
            replacement.write_bytes(target.read_bytes())
            replacement.replace(target)
        elif mutation == "mtime":
            value = target.stat()
            os.utime(target, ns=(value.st_atime_ns, value.st_mtime_ns - 1))
        else:
            target.write_bytes(b"third!")
        with pytest.raises(VNextSourceDeferredError):
            adapter.observe_completion_marker(("1001",))


def test_discovery_allows_root_and_gallery_changes_between_pages(
    tmp_path: Path,
) -> None:
    root = tmp_path / "download"
    first = _gallery(root, "1001")
    _gallery(root, "1002")
    with FilesystemSource(root) as source:
        page = source.list_gallery_locators(after_locator=None, limit=1)
        assert page.items == (("1001",),)
        _gallery(root, "1003")
        (first / "003.jpg").write_bytes(b"in progress")
        second = source.list_gallery_locators(after_locator=page.items[-1], limit=1)
        assert second.items == (("1002",),)
        assert second.terminal
        assert source.observe_gallery(("1002",)).metadata.gid == 1002
        with pytest.raises(FilesystemSourceChangedError, match="newer"):
            source.observe_gallery(("1001",))
    with FilesystemSource(root) as restarted:
        assert restarted.list_gallery_locators(after_locator=None, limit=128).items == (
            ("1001",),
            ("1002",),
            ("1003",),
        )


def test_discovery_accepts_completed_gallery_changes_while_traversing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "download"
    folder = _gallery(root, "collection/1001")
    original_stat = FilesystemSource._directory_stat
    updated = False

    def updating_stat(path: Path) -> filesystem_module.FilesystemStat:
        nonlocal updated
        if path == folder and not updated:
            updated = True
            (folder / "003.jpg").write_bytes(b"new page")
            (folder / "galleryinfo.txt").touch()
            (root / "download-in-progress").mkdir()
        return original_stat(path)

    monkeypatch.setattr(
        FilesystemSource, "_directory_stat", staticmethod(updating_stat)
    )
    with FilesystemSource(root) as source:
        assert source.list_gallery_locators(after_locator=None, limit=128).items == (
            ("collection", "1001"),
        )
        assert source.observe_gallery(("collection", "1001")).metadata.page_count == 3
    assert updated


def test_restart_discards_the_private_gallery_snapshot_and_observes_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "download"
    folder = _gallery(root, "1001")
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    with FilesystemSource(root) as source:
        source.observe_gallery(("1001",))
        assert tuple(tmp_path.glob("h2hdb-ingest-discovery-*"))
        (folder / "001.jpg").write_bytes(b"changed during terminated attempt")
        with pytest.raises(FilesystemSourceChangedError):
            source.revalidate_observed_gallery(("1001",))
    assert tuple(tmp_path.glob("h2hdb-ingest-discovery-*")) == ()
    (folder / "galleryinfo.txt").touch()
    with FilesystemSource(root) as restarted:
        _observed, page = restarted.list_files(("1001",), after_name=None, limit=128)
        image = next(item for item in page.items if item.name_bytes == b"001.jpg")
        assert b"".join(image.content_parts()) == b"changed during terminated attempt"
    assert tuple(tmp_path.glob("h2hdb-ingest-discovery-*")) == ()


def test_disappearing_gallery_during_discovery_does_not_abort_its_sibling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "download"
    first = _gallery(root, "1001")
    _gallery(root, "1002")
    original_stat = FilesystemSource._directory_stat
    moved = False

    def moving_stat(path: Path) -> filesystem_module.FilesystemStat:
        nonlocal moved
        if path == first and not moved:
            moved = True
            first.rename(tmp_path / "removed-gallery")
        return original_stat(path)

    monkeypatch.setattr(FilesystemSource, "_directory_stat", staticmethod(moving_stat))
    progress = IngestProgress(lambda _message: None)
    work = progress.begin("source")
    with FilesystemSource(root, progress=work) as source:
        assert source.list_gallery_locators(after_locator=None, limit=128).items == (
            ("1002",),
        )
        assert source.observe_gallery(("1002",)).metadata.gid == 1002
    work.finish()
