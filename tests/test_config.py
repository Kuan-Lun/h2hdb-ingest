from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from h2hdb_ingest import IngestConfig, IngestPathsConfig, ResidentConfig, load_config


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


def test_artifacts_can_be_disabled_without_creating_output_directories(
    tmp_path: Path,
) -> None:
    download_path = tmp_path / "download"
    download_path.mkdir()
    (download_path / "mounted-volume-marker").touch()
    config = IngestConfig(paths=IngestPathsConfig(download_path=download_path))

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
def test_artifact_roots_must_be_distinct_non_nested_pairs(
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


def test_runtime_paths_create_both_output_roots(tmp_path: Path) -> None:
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


def test_bounded_runtime_defaults() -> None:
    resident = ResidentConfig()

    assert resident.max_rows == 128


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_rows", 0),
        ("max_rows", 129),
    ],
)
def test_bounded_runtime_limits_are_enforced(field: str, value: int) -> None:
    with pytest.raises(ValidationError):
        ResidentConfig.model_validate({field: value})


def test_heartbeat_must_be_shorter_than_lease() -> None:
    with pytest.raises(ValidationError, match="shorter than lease_seconds"):
        ResidentConfig(lease_seconds=10, heartbeat_seconds=10)
