from collections.abc import Iterable, Sequence
from hashlib import sha256
from pathlib import Path

import pytest

from h2hdb_ingest import scanner as scanner_module
from h2hdb_ingest.models import (
    FileHashCacheEntry,
    FileHashCacheKey,
    FileStatSignature,
)
from h2hdb_ingest.scanner import FilesystemScanner, GalleryScanError
from h2hdb_ingest.source_manifest import CanonicalManifestAccumulator


def _write_gallery(root: Path, gid: int, *, source_files: int) -> Path:
    folder = root / f"Gallery [{gid}]"
    folder.mkdir(parents=True)
    (folder / "galleryinfo.txt").write_text(
        "\n".join(
            (
                f"Title: Gallery {gid}",
                "Upload Time: 2024-01-02 03:04",
                "Uploaded By: uploader",
                "Downloaded: 2024-01-03 04:05",
                "Tags: language:english",
                "Uploader's Comments:",
                "Summary",
                "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3",
            )
        ),
        encoding="utf-8",
    )
    for index in range(source_files):
        (folder / f"{index:03}.bin").write_bytes(f"source-{gid}-{index}".encode())
    return folder


class _RecordingCache:
    def __init__(self) -> None:
        self.entries: dict[FileHashCacheKey, FileHashCacheEntry] = {}
        self.lookup_sizes: list[int] = []
        self.remember_sizes: list[int] = []

    def lookup(
        self,
        keys: Sequence[FileHashCacheKey],
        /,
    ) -> Iterable[FileHashCacheEntry]:
        self.lookup_sizes.append(len(keys))
        return tuple(self.entries[key] for key in keys if key in self.entries)

    def remember(self, entries: Sequence[FileHashCacheEntry], /) -> None:
        self.remember_sizes.append(len(entries))
        self.entries.update((entry.key, entry) for entry in entries)


def test_batch_iterator_enforces_both_limits_and_chunks_one_large_gallery(
    tmp_path: Path,
) -> None:
    root = tmp_path / "galleries"
    folders = [_write_gallery(root, gid, source_files=4) for gid in range(1, 4)]
    scanner = FilesystemScanner(
        root,
        hash_workers=2,
        max_galleries=2,
        max_files=3,
    )

    batches = tuple(scanner.iter_batches(scan_attempt="catalog-build-7"))

    assert batches
    assert all(batch.gallery_count <= 2 for batch in batches)
    assert all(batch.file_count <= 3 for batch in batches)
    chunks = tuple(chunk for batch in batches for chunk in batch.chunks)
    assert sum(len(chunk.files) for chunk in chunks) == 15
    for folder in folders:
        gallery_chunks = tuple(
            chunk for chunk in chunks if chunk.manifest.folder == folder
        )
        assert len(gallery_chunks) == 2
        assert [chunk.chunk_index for chunk in gallery_chunks] == [0, 1]
        assert len({chunk.manifest.gallery_attempt for chunk in gallery_chunks}) == 1
        assert [chunk.complete for chunk in gallery_chunks] == [False, True]
        assert gallery_chunks[0].completion is None
        completion = gallery_chunks[-1].completion
        assert completion is not None
        assert completion.scan_observation_sha256
        assert completion.metadata_sha256
        assert completion.scan_observation_version == 2
        assert completion.canonical_source_manifest_sha256
        assert completion.canonical_source_manifest_version == 1
        assert completion.raw_content_sha256
        assert completion.source_file_count == 5
        assert completion.pages == 4
        assert all(
            file.relative_locator for chunk in gallery_chunks for file in chunk.files
        )
        assert all(file.signature for chunk in gallery_chunks for file in chunk.files)


def test_completion_digests_match_legacy_scan_across_chunk_sizes_and_names(
    tmp_path: Path,
) -> None:
    root = tmp_path / "galleries"
    folder = _write_gallery(root, 1, source_files=0)
    (folder / "SS.bin").write_bytes(b"latin")
    (folder / "ß.bin").write_bytes(b"unicode-casefold")
    (folder / "漫 畫.bin ").write_bytes(b"trailing-space")

    completions = []
    for max_files in (1, 2, 99):
        scanner = FilesystemScanner(root, hash_workers=2, max_files=max_files)
        chunks = tuple(
            chunk
            for batch in scanner.iter_batches(scan_attempt=f"chunk-{max_files}")
            for chunk in batch.chunks
        )
        completion = next(
            chunk.completion for chunk in chunks if chunk.completion is not None
        )
        completions.append(completion)

        files = tuple(source_file for chunk in chunks for source_file in chunk.files)
        content_hashes = sorted(
            bytes.fromhex(source_file.sha256)
            for source_file in files
            if source_file.name != "galleryinfo.txt"
        )
        assert (
            completion.raw_content_sha256
            == sha256(b"".join(content_hashes)).hexdigest()
        )

        (legacy,) = scanner.scan()
        assert legacy.source_digest == completion.canonical_source_manifest_sha256
        assert legacy.content_digest == completion.raw_content_sha256

    assert len({completion.scan_observation_sha256 for completion in completions}) == 1
    assert (
        len({completion.canonical_source_manifest_sha256 for completion in completions})
        == 1
    )
    assert len({completion.raw_content_sha256 for completion in completions}) == 1


def test_metadata_only_completion_has_no_raw_content_digest(tmp_path: Path) -> None:
    root = tmp_path / "galleries"
    _write_gallery(root, 1, source_files=0)
    scanner = FilesystemScanner(root, hash_workers=1, max_files=1)

    completion = next(
        chunk.completion
        for batch in scanner.iter_batches(scan_attempt="metadata-only")
        for chunk in batch.chunks
        if chunk.completion is not None
    )

    assert completion.raw_content_sha256 is None
    (legacy,) = scanner.scan()
    assert legacy.content_digest is None
    assert legacy.source_digest == completion.canonical_source_manifest_sha256


def test_discovery_is_bounded_and_scan_can_target_pending_relative_folders(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "galleries"
    folders = [_write_gallery(root, gid, source_files=1) for gid in range(1, 6)]
    scanner = FilesystemScanner(root, hash_workers=1, max_galleries=2)

    def parsing_is_not_discovery(_folder: Path) -> object:
        raise AssertionError("discovery must not parse galleryinfo.txt")

    original_parse = scanner_module.parse_galleryinfo
    monkeypatch.setattr(scanner_module, "parse_galleryinfo", parsing_is_not_discovery)
    session = scanner.discover(scan_attempt="discovery-1")
    discovery_batches = tuple(session.iter_batches())
    summary = session.finish()
    monkeypatch.setattr(scanner_module, "parse_galleryinfo", original_parse)

    assert [len(batch.galleries) for batch in discovery_batches] == [2, 2, 1]
    discoveries = tuple(
        gallery for batch in discovery_batches for gallery in batch.galleries
    )
    assert {gallery.relative_folder for gallery in discoveries} == {
        folder.relative_to(root).as_posix() for folder in folders
    }
    assert all(gallery.metadata_signature.size_bytes > 0 for gallery in discoveries)
    assert summary.scan_attempt == "discovery-1"
    assert summary.gallery_count == len(folders)
    assert len(summary.tree_observation_sha256) == 64

    pending = discoveries[1:3]
    batches = tuple(
        scanner.iter_batches(
            scan_attempt="resume-1",
            relative_folders=(gallery.relative_folder for gallery in pending),
        )
    )
    chunks = tuple(chunk for batch in batches for chunk in batch.chunks)
    assert {chunk.manifest.relative_folder for chunk in chunks} == {
        gallery.relative_folder for gallery in pending
    }


def test_discovery_does_not_inspect_the_next_batch_before_yielding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "galleries"
    paths = tuple(
        _write_gallery(root, gid, source_files=0) / "galleryinfo.txt"
        for gid in range(1, 4)
    )
    inspected: list[Path] = []
    scanner = FilesystemScanner(root, hash_workers=1, max_galleries=2)

    def discoveries(_directory: Path) -> Iterable[Path]:
        for path in paths:
            inspected.append(path)
            yield path

    monkeypatch.setattr(scanner, "_iter_gallery_info_paths", discoveries)
    iterator = scanner.iter_discovery_batches()

    first = next(iterator)

    assert len(first.galleries) == 2
    assert inspected == list(paths[:2])


def test_scan_yields_full_file_batch_before_hashing_the_next_chunk(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "galleries"
    _write_gallery(root, 1, source_files=5)
    scanner = FilesystemScanner(root, hash_workers=1, max_files=2)
    original_hash_file = scanner_module._hash_file
    hashed: list[Path] = []

    def recording_hash(path: Path) -> scanner_module._CachedHash:
        hashed.append(path)
        return original_hash_file(path)

    monkeypatch.setattr(scanner_module, "_hash_file", recording_hash)
    iterator = scanner.iter_batches(scan_attempt="lazy-batches")

    first = next(iterator)

    assert first.file_count == 2
    assert len(hashed) == 2


def test_abandoned_scan_iterator_closes_manifest_spill(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "galleries"
    _write_gallery(root, 1, source_files=2)
    spill_root = tmp_path / "manifest-spill"
    spill_root.mkdir()

    def tiny_manifest_accumulator() -> CanonicalManifestAccumulator:
        return CanonicalManifestAccumulator(
            memory_limit_bytes=1,
            temporary_directory=spill_root,
        )

    monkeypatch.setattr(
        scanner_module,
        "CanonicalManifestAccumulator",
        tiny_manifest_accumulator,
    )
    scanner = FilesystemScanner(root, hash_workers=1, max_files=1)
    iterator = scanner.iter_batches(scan_attempt="abandoned")

    next(iterator)
    assert tuple(spill_root.iterdir())
    iterator.close()

    assert not tuple(spill_root.iterdir())


def test_metadata_change_after_parse_rejects_the_gallery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "galleries"
    folder = _write_gallery(root, 1, source_files=1)
    metadata_path = folder / "galleryinfo.txt"
    scanner = FilesystemScanner(root, hash_workers=1)
    original_parse = scanner_module.parse_galleryinfo

    def mutating_parse(target: Path) -> object:
        parsed = original_parse(target)
        metadata_path.write_text(
            metadata_path.read_text(encoding="utf-8") + "\nchanged",
            encoding="utf-8",
        )
        return parsed

    monkeypatch.setattr(scanner_module, "parse_galleryinfo", mutating_parse)

    with pytest.raises(GalleryScanError, match="changed while it was parsed"):
        tuple(scanner.iter_batches())


def test_gallery_observation_detects_a_change_after_source_staging(
    tmp_path: Path,
) -> None:
    root = tmp_path / "galleries"
    folder = _write_gallery(root, 1, source_files=1)
    scanner = FilesystemScanner(root, hash_workers=1)
    batches = tuple(scanner.iter_batches(scan_attempt="validation"))
    completion = tuple(
        chunk.completion
        for batch in batches
        for chunk in batch.chunks
        if chunk.completion is not None
    )[0]

    relative_folder = folder.relative_to(root).as_posix()
    before = scanner.observe_gallery(relative_folder)
    assert before.directory_entry_count == completion.directory_entry_count
    assert (
        before.directory_observation_sha256 == completion.directory_observation_sha256
    )

    (folder / "001-new.bin").write_bytes(b"new")

    after = scanner.observe_gallery(relative_folder)
    assert after.directory_entry_count != completion.directory_entry_count
    assert after.directory_observation_sha256 != completion.directory_observation_sha256


@pytest.mark.parametrize("relative_folder", ("", "../outside", "/absolute"))
def test_targeted_scan_rejects_invalid_relative_folder(
    tmp_path: Path,
    relative_folder: str,
) -> None:
    scanner = FilesystemScanner(tmp_path, hash_workers=1)

    with pytest.raises(GalleryScanError, match="Invalid gallery relative folder"):
        tuple(scanner.iter_batches(relative_folders=(relative_folder,)))


def test_bulk_cache_hit_uses_complete_locator_and_does_not_hash_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "galleries"
    _write_gallery(root, 1, source_files=3)
    cache = _RecordingCache()
    scanner = FilesystemScanner(
        root,
        hash_workers=1,
        hash_cache=cache,
        max_files=2,
    )
    tuple(scanner.iter_batches(scan_attempt="first"))
    assert cache.remember_sizes == [2, 2]
    assert all(size <= 2 for size in cache.lookup_sizes)
    assert all(
        entry.key.root_locator == root.resolve().as_posix()
        for entry in cache.entries.values()
    )
    assert all(entry.key.relative_locator for entry in cache.entries.values())
    assert all(
        entry.key.signature
        == FileStatSignature(
            device=entry.key.signature.device,
            inode=entry.key.signature.inode,
            size_bytes=entry.key.signature.size_bytes,
            modified_ns=entry.key.signature.modified_ns,
            changed_ns=entry.key.signature.changed_ns,
        )
        for entry in cache.entries.values()
    )

    def unexpected_hash(_path: Path) -> scanner_module._CachedHash:
        raise AssertionError("a complete cache hit must not read file bytes")

    monkeypatch.setattr(scanner_module, "_hash_file", unexpected_hash)
    tuple(scanner.iter_batches(scan_attempt="second"))


def test_cached_gallery_is_not_sealed_if_a_file_changes_after_lookup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "galleries"
    folder = _write_gallery(root, 1, source_files=1)
    spill_root = tmp_path / "manifest-spill"
    spill_root.mkdir()

    def tiny_manifest_accumulator() -> CanonicalManifestAccumulator:
        return CanonicalManifestAccumulator(
            memory_limit_bytes=1,
            temporary_directory=spill_root,
        )

    monkeypatch.setattr(
        scanner_module,
        "CanonicalManifestAccumulator",
        tiny_manifest_accumulator,
    )
    cache = _RecordingCache()
    scanner = FilesystemScanner(root, hash_workers=1, hash_cache=cache)
    tuple(scanner.iter_batches(scan_attempt="first"))
    source_path = folder / "000.bin"
    original_lookup = cache.lookup

    def mutating_lookup(
        keys: Sequence[FileHashCacheKey],
        /,
    ) -> Iterable[FileHashCacheEntry]:
        entries = original_lookup(keys)
        source_path.write_bytes(b"changed-after-cache-lookup")
        return entries

    cache.lookup = mutating_lookup  # type: ignore[method-assign]

    with pytest.raises(GalleryScanError, match="directory changed during scan"):
        tuple(scanner.iter_batches(scan_attempt="second"))

    assert not tuple(spill_root.iterdir())


def test_compatibility_memory_cache_is_lru_bounded() -> None:
    cache = scanner_module._MemoryFileHashCache(max_entries=2)
    signature = FileStatSignature(1, 2, 3, 4, 5)
    entries = tuple(
        FileHashCacheEntry(
            FileHashCacheKey("/root", f"gallery/{index}", signature),
            f"{index:064x}",
        )
        for index in range(3)
    )

    cache.remember(entries)

    assert tuple(cache.lookup(tuple(entry.key for entry in entries))) == entries[1:]
