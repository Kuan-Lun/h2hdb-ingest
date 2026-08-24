from __future__ import annotations

import os
from pathlib import Path

import pytest

from h2hdb_ingest.filesystem import FilesystemSource

_DOWNLOAD_PATH_ENVIRONMENT = "H2HDB_INGEST_TEST_DOWNLOAD_PATH"
_DEFAULT_DOWNLOAD_PATH = (
    Path(__file__).resolve().parents[1] / ".local-test-data" / "hath-download"
)


def _local_download_path() -> Path:
    configured = os.environ.get(_DOWNLOAD_PATH_ENVIRONMENT)
    if configured is None:
        path = _DEFAULT_DOWNLOAD_PATH
        if not path.exists():
            pytest.skip(
                f"place a private H@H corpus at {path} or set "
                f"{_DOWNLOAD_PATH_ENVIRONMENT}"
            )
    else:
        path = Path(configured)
    if not path.is_dir():
        pytest.fail(f"private H@H corpus path is not a directory: {path}")
    return path


def test_default_private_corpus_uses_the_ignored_repository_local_root() -> None:
    repository_root = Path(__file__).resolve().parents[1]
    ignored = (repository_root / ".gitignore").read_text(encoding="utf-8").splitlines()

    assert _DEFAULT_DOWNLOAD_PATH == (
        repository_root / ".local-test-data" / "hath-download"
    )
    assert "/.local-test-data/" in ignored
    assert "/download-1" not in ignored


def test_private_corpus_path_allows_an_environment_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(_DOWNLOAD_PATH_ENVIRONMENT, str(tmp_path))

    assert _local_download_path() == tmp_path


def test_opt_in_local_download_corpus_is_bounded_and_replayable() -> None:
    """Exercise discovery, parsing, paging, and exact reads on a private corpus."""

    gallery_count = 0
    file_count = 0
    directory_entry_count = 0
    tag_count = 0
    bytes_read = 0
    with FilesystemSource(_local_download_path()) as source:
        after_locator: tuple[str, ...] | None = None
        while True:
            galleries = source.list_gallery_locators(
                after_locator=after_locator,
                limit=2,
            )
            for locator in galleries.items:
                gallery_count += 1
                observation = source.observe_gallery(locator)
                assert observation.metadata.source_file_count >= 1
                assert (
                    observation.metadata.page_count
                    == observation.metadata.source_file_count - 1
                )

                after_name: bytes | None = None
                previous_name: bytes | None = None
                gallery_file_count = 0
                while True:
                    replayed, files = source.list_files(
                        locator,
                        after_name=after_name,
                        limit=64,
                    )
                    assert replayed == observation
                    for item in files.items:
                        if previous_name is not None:
                            assert item.name_bytes > previous_name
                        previous_name = item.name_bytes
                        file_count += 1
                        gallery_file_count += 1
                        for part in item.content_parts():
                            bytes_read += len(part)
                    if files.terminal:
                        break
                    assert files.items
                    after_name = files.items[-1].name_bytes
                assert gallery_file_count == observation.metadata.source_file_count

                after_name = None
                previous_name = None
                while True:
                    replayed, entries = source.list_directories(
                        locator,
                        after_name=after_name,
                        limit=64,
                    )
                    assert replayed == observation
                    for entry in entries.items:
                        if previous_name is not None:
                            assert entry.name_bytes > previous_name
                        previous_name = entry.name_bytes
                        directory_entry_count += 1
                    if entries.terminal:
                        break
                    assert entries.items
                    after_name = entries.items[-1].name_bytes

                after_position = 0
                while True:
                    replayed, tags = source.list_tags(
                        locator,
                        after_position=after_position,
                        limit=64,
                    )
                    assert replayed == observation
                    tag_count += len(tags.items)
                    if tags.terminal:
                        break
                    assert tags.items
                    after_position += len(tags.items)

            if galleries.terminal:
                break
            assert galleries.items
            after_locator = galleries.items[-1]

    assert gallery_count > 0
    assert file_count >= gallery_count
    assert directory_entry_count >= file_count
    assert tag_count > 0
    assert bytes_read > 0
