from __future__ import annotations

from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import cast
from zipfile import ZipFile

import pytest
from h2hdb import CoreConfig, DatabaseConfig
from PIL import Image

from h2hdb_ingest import IngestConfig, IngestPathsConfig, ResidentConfig
from h2hdb_ingest.runtime import build_runtime


@pytest.fixture(params=("sqlite", "mariadb"))
def runtime_core_config(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> CoreConfig:
    if request.param == "sqlite":
        return CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite",
                database=str(tmp_path / "catalog.sqlite3"),
            )
        )
    return cast(CoreConfig, request.getfixturevalue("mariadb_config"))


def _gallery(
    root: Path,
    gid: int,
    artist: str,
    *,
    page_bytes: bytes = b"first page",
) -> None:
    folder = root / str(gid)
    folder.mkdir(parents=True)
    (folder / "galleryinfo.txt").write_text(
        "\n".join(
            (
                f"Title: Runtime integration {gid}",
                "Upload Time: 2024-01-02 03:04",
                "Uploaded By: uploader",
                "Downloaded: 2024-02-03 04:05",
                f"Tags: artist:{artist}, language:english",
                "Uploader's Comments",
                "A comment",
                "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3",
            )
        ),
        encoding="utf-8",
    )
    (folder / "001.jpg").write_bytes(page_bytes)


def test_fresh_epoch_runs_source_analysis_and_publication(
    tmp_path: Path,
    runtime_core_config: CoreConfig,
) -> None:
    source = tmp_path / "download"
    _gallery(source, 1001, "first")
    config = IngestConfig(
        core=runtime_core_config,
        paths=IngestPathsConfig(download_path=source),
        resident=ResidentConfig(
            lease_seconds=30,
            heartbeat_seconds=5,
        ),
    )
    runtime = build_runtime(config)

    initialized = runtime.database_admin.initialize()
    checked = runtime.resident.initialize()
    processed = runtime.resident.process_available(periodic_scan=True)
    revision = runtime.catalog.get_catalog_revision()

    assert initialized.epoch == checked.epoch
    assert processed
    assert revision.revision == 1
    assert revision.publication_count == 1

    # A periodic scan after process restart must replay the exact SEALED source
    # snapshot without attempting to reopen its discovery checkpoint.
    restarted = build_runtime(config)
    restarted.resident.initialize()
    assert restarted.resident.process_available(periodic_scan=True)
    replayed_revision = restarted.catalog.get_catalog_revision()
    assert replayed_revision == revision

    # The same content in three galleries with three distinct artists reaches
    # the registered spam threshold.  This public result covers both derived
    # source projections: per-observation hash occurrences and artist tags.
    _gallery(source, 1002, "second")
    _gallery(source, 1003, "third")

    assert restarted.resident.process_available(periodic_scan=True)
    excluded_revision = restarted.catalog.get_catalog_revision()
    assert excluded_revision.revision == 2
    assert excluded_revision.publication_count == 0

    # Replay the incremental build after its own publication has advanced the
    # channel head.  Its analysis must retain the baseline that was persisted
    # when the run began instead of deriving a new baseline from revision 2.
    restarted_incremental = build_runtime(config)
    restarted_incremental.resident.initialize()
    assert restarted_incremental.resident.process_available(periodic_scan=True)
    assert restarted_incremental.catalog.get_catalog_revision() == excluded_revision


def test_same_locator_content_a_b_a_creates_three_revisions_then_replays(
    tmp_path: Path,
    runtime_core_config: CoreConfig,
) -> None:
    source = tmp_path / "download"
    _gallery(source, 1001, "artist", page_bytes=b"content-A")
    config = IngestConfig(
        core=runtime_core_config,
        paths=IngestPathsConfig(download_path=source),
        resident=ResidentConfig(
            lease_seconds=30,
            heartbeat_seconds=5,
        ),
    )
    runtime = build_runtime(config)
    runtime.database_admin.initialize()

    def current_content() -> str:
        page = runtime.catalog.list_publications()
        assert page.total == len(page.publications) == 1
        content_sha256 = page.publications[0].content_sha256
        assert content_sha256 is not None
        return content_sha256

    assert runtime.resident.process_available(periodic_scan=True)
    first_revision = runtime.catalog.get_catalog_revision()
    first_content = current_content()
    assert first_revision.revision == 1

    (source / "1001" / "001.jpg").write_bytes(b"content-B")
    assert runtime.resident.process_available(periodic_scan=True)
    second_revision = runtime.catalog.get_catalog_revision()
    second_content = current_content()
    assert second_revision.revision == 2
    assert second_content != first_content

    (source / "1001" / "001.jpg").write_bytes(b"content-A")
    assert runtime.resident.process_available(periodic_scan=True)
    third_revision = runtime.catalog.get_catalog_revision()
    third_content = current_content()
    assert third_revision.revision == 3
    assert third_content == first_content

    restarted = build_runtime(config)
    restarted.resident.initialize()
    assert restarted.resident.process_available(periodic_scan=True)
    assert restarted.catalog.get_catalog_revision() == third_revision
    replayed = restarted.catalog.list_publications()
    assert replayed.total == len(replayed.publications) == 1
    assert replayed.publications[0].content_sha256 == first_content


def test_fresh_artifact_runtime_publishes_immutable_and_current_cbz(
    tmp_path: Path,
) -> None:
    source = tmp_path / "download"
    _gallery(source, 2001, "artist")
    Image.new("RGB", (8, 12), "red").save(source / "2001" / "001.jpg")
    artifact_root = tmp_path / "artifacts"
    current_root = tmp_path / "current"
    config = IngestConfig(
        core=CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite",
                database=str(tmp_path / "catalog.sqlite3"),
            )
        ),
        paths=IngestPathsConfig(
            download_path=source,
            artifact_store_path=artifact_root,
            cbz_path=current_root,
        ),
        resident=ResidentConfig(
            lease_seconds=30,
            heartbeat_seconds=5,
        ),
    )
    runtime = build_runtime(config)

    runtime.database_admin.initialize()
    assert runtime.resident.process_available(periodic_scan=True)
    page = runtime.catalog.list_publications(require_artifact=True)
    immutable = tuple((artifact_root / "sha256").glob("*/*.cbz"))
    current = tuple(current_root.rglob("*.cbz"))

    assert page.total == 1
    assert len(page.publications) == 1
    assert len(page.publications[0].artifacts) == 1
    assert len(immutable) == len(current) == 1
    assert artifact_root / page.publications[0].artifacts[0].location == immutable[0]
    assert immutable[0].read_bytes() == current[0].read_bytes()
    assert sha256(immutable[0].read_bytes()).hexdigest() == (
        page.publications[0].artifacts[0].sha256
    )
    with ZipFile(immutable[0]) as archive:
        assert archive.namelist() == [
            "0000000000000000__content.jpg",
            "0000000000000001__metadata.txt",
        ]
        assert (
            archive.read("0000000000000001__metadata.txt")
            == (source / "2001" / "galleryinfo.txt").read_bytes()
        )
        with Image.open(
            BytesIO(archive.read("0000000000000000__content.jpg"))
        ) as image:
            assert image.format == "JPEG"
            assert image.size == (8, 12)
