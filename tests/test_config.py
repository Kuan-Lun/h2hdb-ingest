import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from h2hdb_ingest import CBZGrouping, IngestConfig, IngestPathsConfig, load_config


def test_loader_resolves_nested_core_secret_and_path_environment_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    download_path = tmp_path / "download"
    monkeypatch.setenv("H2HDB_INGEST_DOWNLOAD_PATH", str(download_path))
    monkeypatch.setenv("H2HDB_INGEST_DATABASE_PASSWORD", "write-secret")
    config_path = tmp_path / "ingest.json"
    config_path.write_text(
        json.dumps(
            {
                "core": {
                    "database": {
                        "password": "${H2HDB_INGEST_DATABASE_PASSWORD}",
                    }
                },
                "paths": {
                    "download_path": "${H2HDB_INGEST_DOWNLOAD_PATH}",
                },
            }
        ),
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.core.database.password == "write-secret"
    assert config.paths.download_path == download_path


def test_cbz_can_be_disabled_without_creating_an_output_directory(
    tmp_path: Path,
) -> None:
    download_path = tmp_path / "download"
    download_path.mkdir()
    (download_path / "mounted-volume-marker").touch()
    config = IngestConfig(
        paths=IngestPathsConfig(
            download_path=download_path,
            cbz_path=None,
            artifact_store_path=None,
        )
    )

    config.ensure_paths()

    assert config.paths.cbz_path is None
    assert config.paths.artifact_store_path is None


@pytest.mark.parametrize(
    ("cbz_path", "artifact_store_path"),
    [
        ("komga", None),
        (None, "artifacts"),
        ("same", "same"),
        ("root", "root/artifacts"),
        ("root/komga", "root"),
    ],
)
def test_cbz_roots_must_be_distinct_non_nested_pairs(
    tmp_path: Path,
    cbz_path: str | None,
    artifact_store_path: str | None,
) -> None:
    with pytest.raises(ValidationError):
        IngestPathsConfig(
            download_path=tmp_path,
            cbz_path=tmp_path / cbz_path if cbz_path is not None else None,
            artifact_store_path=(
                tmp_path / artifact_store_path
                if artifact_store_path is not None
                else None
            ),
        )


def test_runtime_paths_create_both_cbz_roots(tmp_path: Path) -> None:
    download_path = tmp_path / "download"
    download_path.mkdir()
    (download_path / "gallery").mkdir()
    cbz_path = tmp_path / "komga"
    artifact_store_path = tmp_path / "artifacts"
    config = IngestConfig(
        paths=IngestPathsConfig(
            download_path=download_path,
            cbz_path=cbz_path,
            artifact_store_path=artifact_store_path,
        )
    )

    config.ensure_paths()

    assert cbz_path.is_dir()
    assert artifact_store_path.is_dir()


def test_runtime_paths_reject_empty_download_mount(tmp_path: Path) -> None:
    download_path = tmp_path / "empty-download"
    download_path.mkdir()
    config = IngestConfig(paths=IngestPathsConfig(download_path=download_path))

    with pytest.raises(ValueError, match="gallery volume is mounted"):
        config.ensure_paths()


@pytest.mark.parametrize(
    "value",
    [
        "no",
        "upload_time",
        "download_time",
        "gid",
        "title",
        "pages",
        "pages+1",
        "pages+200",
    ],
)
def test_supported_cbz_sort_modes_are_accepted(tmp_path: Path, value: str) -> None:
    paths = IngestPathsConfig(
        download_path=tmp_path,
        cbz_sort=value,
        cbz_grouping=CBZGrouping.date_yyyy_mm,
    )

    assert paths.cbz_sort == value


@pytest.mark.parametrize("value", ["pages+0", "pages+-1", "pages+word"])
def test_unsupported_cbz_sort_modes_are_rejected(tmp_path: Path, value: str) -> None:
    with pytest.raises(ValidationError):
        IngestPathsConfig(download_path=tmp_path, cbz_sort=value)
