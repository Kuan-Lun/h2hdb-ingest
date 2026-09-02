from __future__ import annotations

import os
import stat
from hashlib import sha256
from pathlib import Path
from zipfile import ZipFile

import pytest
from h2hdb import (
    CatalogPublication,
    CatalogRevision,
    CoreConfig,
    VNextCatalogFacade,
)
from PIL import Image

from h2hdb_ingest import (
    ArtifactRenderPolicyConfig,
    ArtifactRenderPreset,
    IngestConfig,
    IngestPathsConfig,
    ResidentConfig,
)
from h2hdb_ingest.filesystem import FilesystemArtifactSourceRole, FilesystemSource
from h2hdb_ingest.runtime import build_runtime

_DOWNLOAD_PATH_ENVIRONMENT = "H2HDB_INGEST_TEST_DOWNLOAD_PATH"
_PRIVATE_CORPUS_OPT_IN_ENVIRONMENT = "H2HDB_INGEST_TEST_PRIVATE_CORPUS"
_DEFAULT_DOWNLOAD_PATH = (
    Path(__file__).resolve().parents[1] / ".local-test-data" / "hath-download"
)
_PRIVATE_CORPUS_OPT_IN = pytest.mark.skipif(
    os.environ.get(_PRIVATE_CORPUS_OPT_IN_ENVIRONMENT) != "1",
    reason=f"set {_PRIVATE_CORPUS_OPT_IN_ENVIRONMENT}=1 to read a private corpus",
)


def _provision_library_root(root: Path) -> None:
    for path in (
        root / "current" / "acquisitions",
        root / "current" / "artwork",
        root / ".h2hdb-coordination",
    ):
        path.mkdir(parents=True)


def _tree_authority(root: Path) -> tuple[tuple[str, int, int, int, int, int], ...]:
    """Capture source identity without reading or mutating private bytes."""

    authority: list[tuple[str, int, int, int, int, int]] = []
    for path in sorted(root.rglob("*"), key=lambda item: item.relative_to(root).parts):
        observed = path.lstat()
        authority.append(
            (
                path.relative_to(root).as_posix(),
                observed.st_dev,
                observed.st_ino,
                stat.S_IFMT(observed.st_mode),
                observed.st_size,
                observed.st_mtime_ns,
            )
        )
    return tuple(authority)


def _output_manifest(
    root: Path,
) -> tuple[str, tuple[tuple[str, int, str], ...]]:
    """Hash every current output's path, size, and complete byte digest."""

    digest = sha256(b"h2hdb-ingest-private-output-v1\0")
    entries: list[tuple[str, int, str]] = []
    for path in sorted(
        (item for item in root.rglob("*") if item.is_file()),
        key=lambda item: item.relative_to(root).as_posix(),
    ):
        relative = path.relative_to(root).as_posix()
        content_digest = sha256()
        size = 0
        with path.open("rb") as stream:
            while block := stream.read(1024 * 1024):
                content_digest.update(block)
                size += len(block)
        relative_bytes = relative.encode()
        digest.update(len(relative_bytes).to_bytes(4, "big"))
        digest.update(relative_bytes)
        digest.update(size.to_bytes(8, "big"))
        digest.update(content_digest.digest())
        entries.append((relative, size, content_digest.hexdigest()))
    return digest.hexdigest(), tuple(entries)


def _discover_all_publications(
    catalog: VNextCatalogFacade,
    revision: CatalogRevision,
) -> tuple[CatalogPublication, ...]:
    publications: list[CatalogPublication] = []
    after = None
    expected_total: int | None = None
    while True:
        page = catalog.discover_publications(
            revision=revision,
            after=after,
            limit=128,
        )
        assert page.revision == revision
        if expected_total is None:
            assert page.total is not None
            expected_total = page.total
        else:
            assert page.total == expected_total
        publications.extend(page.publications)
        if page.next_cursor is None:
            break
        after = page.next_cursor
    assert len(publications) == expected_total
    return tuple(publications)


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


def _opt_in_local_download_path() -> Path:
    if os.environ.get(_PRIVATE_CORPUS_OPT_IN_ENVIRONMENT) != "1":
        pytest.skip(
            f"set {_PRIVATE_CORPUS_OPT_IN_ENVIRONMENT}=1 to read a private corpus"
        )
    return _local_download_path()


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


def test_private_corpus_requires_an_explicit_opt_in(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(_DOWNLOAD_PATH_ENVIRONMENT, str(tmp_path))
    monkeypatch.delenv(_PRIVATE_CORPUS_OPT_IN_ENVIRONMENT, raising=False)

    with pytest.raises(
        pytest.skip.Exception,
        match=f"set {_PRIVATE_CORPUS_OPT_IN_ENVIRONMENT}=1",
    ):
        _opt_in_local_download_path()

    monkeypatch.setenv(_PRIVATE_CORPUS_OPT_IN_ENVIRONMENT, "1")
    assert _opt_in_local_download_path() == tmp_path


@_PRIVATE_CORPUS_OPT_IN
def test_opt_in_local_download_corpus_is_bounded_and_replayable() -> None:
    """Exercise discovery, parsing, paging, and exact reads on a private corpus."""

    gallery_count = 0
    file_count = 0
    directory_entry_count = 0
    tag_count = 0
    bytes_read = 0
    with FilesystemSource(_opt_in_local_download_path()) as source:
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

                after_name: bytes | None = None
                previous_name: bytes | None = None
                gallery_file_count = 0
                gallery_page_count = 0
                gallery_metadata_count = 0
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
                        gallery_page_count += int(
                            item.artifact_role is FilesystemArtifactSourceRole.PAGE
                        )
                        gallery_metadata_count += int(
                            item.artifact_role is FilesystemArtifactSourceRole.METADATA
                        )
                        for part in item.content_parts():
                            bytes_read += len(part)
                    if files.terminal:
                        break
                    assert files.items
                    after_name = files.items[-1].name_bytes
                assert gallery_file_count == observation.metadata.source_file_count
                assert gallery_page_count == observation.metadata.page_count
                assert gallery_metadata_count == 1

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


@_PRIVATE_CORPUS_OPT_IN
def test_private_corpus_completes_mariadb_resident_cycle_and_restart_replay(
    mariadb_config: CoreConfig,
    tmp_path: Path,
) -> None:
    """Build and replay the exact private corpus CBZ tree through MariaDB."""

    download_path = _opt_in_local_download_path()
    source_authority = _tree_authority(download_path)
    library_path = tmp_path / "library"
    _provision_library_root(library_path)
    config = IngestConfig(
        core=mariadb_config,
        paths=IngestPathsConfig(
            download_path=download_path,
            library_path=library_path,
            render_policy=ArtifactRenderPolicyConfig(
                preset=ArtifactRenderPreset.BENCHMARK_LOW_COST,
            ),
            page_render_workers=4,
        ),
        resident=ResidentConfig(
            lease_seconds=1_800,
            heartbeat_seconds=30,
            max_rows=128,
        ),
    )
    with build_runtime(config) as runtime:
        initialized = runtime.database_admin.initialize()
        checked = runtime.resident.initialize()

        assert initialized.epoch == checked.epoch
        assert runtime.resident.process_available(periodic_scan=True)
        first_revision = runtime.catalog.get_catalog_revision()
        first_publications = _discover_all_publications(
            runtime.catalog,
            first_revision,
        )
    with pytest.raises(ValueError, match="ingest facade is closed"):
        runtime.facade.try_claim_ingest(False, 1)

    assert first_revision.revision == 1
    assert first_revision.publication_count > 0
    assert len(first_publications) == first_revision.publication_count
    assert first_revision.artifact_count == first_revision.publication_count

    current_root = library_path / "current"
    first_manifest, first_entries = _output_manifest(current_root)
    entry_by_path = {
        relative: (size, digest) for relative, size, digest in first_entries
    }
    assert sum(size for _relative, size, _digest in first_entries) > 0

    expected_paths: set[str] = set()
    for publication in first_publications:
        assert len(publication.artifacts) == 1
        assert publication.page_count > 0
        assert publication.cover is not None
        assert publication.thumbnail is not None

        artifact = publication.artifacts[0].storage_object
        artifact_path = current_root.joinpath(*artifact.key.segments)
        artifact_relative = artifact_path.relative_to(current_root).as_posix()
        expected_paths.add(artifact_relative)
        assert entry_by_path[artifact_relative] == (
            artifact.size_bytes,
            artifact.sha256,
        )
        with ZipFile(artifact_path) as archive:
            assert archive.namelist() == [
                "galleryinfo.txt",
                *(
                    f"pages/{page_index:04d}.jpg"
                    for page_index in range(publication.page_count)
                ),
            ]

        thumbnail = publication.thumbnail.storage_object
        thumbnail_path = current_root.joinpath(*thumbnail.key.segments)
        thumbnail_relative = thumbnail_path.relative_to(current_root).as_posix()
        expected_paths.add(thumbnail_relative)
        assert entry_by_path[thumbnail_relative] == (
            thumbnail.size_bytes,
            thumbnail.sha256,
        )
        with Image.open(thumbnail_path) as image:
            assert image.format == "JPEG"
            assert max(image.size) <= 320
            image.verify()

    assert len(first_entries) == len(expected_paths) == 2 * len(first_publications)
    assert set(entry_by_path) == expected_paths
    assert not (library_path / ".h2hdb-coordination" / "ACTIVATING").exists()

    with build_runtime(config) as restarted:
        restarted.resident.initialize()
        assert restarted.resident.process_available(periodic_scan=True)
        replayed_revision = restarted.catalog.get_catalog_revision()
        replayed_publications = _discover_all_publications(
            restarted.catalog,
            replayed_revision,
        )
    with pytest.raises(ValueError, match="ingest facade is closed"):
        restarted.facade.try_claim_ingest(False, 1)

    replayed_manifest, replayed_entries = _output_manifest(current_root)
    assert replayed_revision == first_revision
    assert replayed_publications == first_publications
    assert replayed_manifest == first_manifest
    assert replayed_entries == first_entries
    assert _tree_authority(download_path) == source_authority
