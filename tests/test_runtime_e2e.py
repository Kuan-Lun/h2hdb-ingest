from __future__ import annotations

from hashlib import sha256
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

from h2hdb import CoreConfig, DatabaseConfig
from PIL import Image

from h2hdb_ingest import IngestConfig, IngestPathsConfig, ResidentConfig
from h2hdb_ingest.runtime import build_runtime


def _gallery(root: Path, gid: int, artist: str) -> None:
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
    (folder / "001.jpg").write_bytes(b"first page")


def test_fresh_sqlite_epoch_runs_source_analysis_and_publication(
    tmp_path: Path,
) -> None:
    source = tmp_path / "download"
    _gallery(source, 1001, "first")
    config = IngestConfig(
        core=CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite",
                database=str(tmp_path / "catalog.sqlite3"),
            )
        ),
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

    # The same content in three galleries with three distinct artists reaches
    # the registered spam threshold.  This public result covers both derived
    # source projections: per-observation hash occurrences and artist tags.
    _gallery(source, 1002, "second")
    _gallery(source, 1003, "third")

    assert runtime.resident.process_available(periodic_scan=True)
    excluded_revision = runtime.catalog.get_catalog_revision()
    assert excluded_revision.revision == 2
    assert excluded_revision.publication_count == 0


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
