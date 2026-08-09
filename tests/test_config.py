import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from h2hdb_ingest import CBZGrouping, IngestConfig, IngestPathsConfig, load_config
from h2hdb_ingest.scope import catalog_scope_key


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


def test_scan_batch_limits_have_bounded_defaults(tmp_path: Path) -> None:
    paths = IngestPathsConfig(download_path=tmp_path)

    assert paths.scan_batch_galleries == 128
    assert paths.scan_batch_files == 2_048


@pytest.mark.parametrize("field", ["scan_batch_galleries", "scan_batch_files"])
def test_scan_batch_limits_must_be_positive(tmp_path: Path, field: str) -> None:
    with pytest.raises(ValidationError):
        IngestPathsConfig.model_validate({"download_path": tmp_path, field: 0})


@pytest.mark.parametrize(
    ("field", "value"),
    [("scan_batch_galleries", 201), ("scan_batch_files", 2_049)],
)
def test_scan_batch_limits_reject_oversized_transactions(
    tmp_path: Path,
    field: str,
    value: int,
) -> None:
    with pytest.raises(ValidationError):
        IngestPathsConfig.model_validate({"download_path": tmp_path, field: value})


def test_catalog_scope_ignores_throughput_tuning_but_tracks_semantics(
    tmp_path: Path,
) -> None:
    base = IngestPathsConfig(
        download_path=tmp_path / "source",
        cbz_path=tmp_path / "current",
        artifact_store_path=tmp_path / "artifacts",
        hash_workers=1,
        cbz_workers=1,
        scan_batch_galleries=1,
        scan_batch_files=1,
    )
    tuned = base.model_copy(
        update={
            "hash_workers": 8,
            "cbz_workers": 8,
            "scan_batch_galleries": 200,
            "scan_batch_files": 2_048,
        }
    )

    base_key = catalog_scope_key(base, parser_version="0.5.0")

    assert catalog_scope_key(tuned, parser_version="0.5.0") == base_key
    assert base_key.startswith("filesystem-v1:")
    assert (
        catalog_scope_key(
            base.model_copy(update={"max_image_short_side": 1024}),
            parser_version="0.5.0",
        )
        != base_key
    )
    assert catalog_scope_key(base, parser_version="0.6.0") != base_key


def test_catalog_scope_ignores_cbz_policy_when_cbz_is_disabled(tmp_path: Path) -> None:
    base = IngestPathsConfig(download_path=tmp_path / "source")
    changed = base.model_copy(
        update={
            "max_image_short_side": 2_048,
            "cbz_grouping": CBZGrouping.date_yyyy_mm_dd,
            "cbz_sort": "gid",
        }
    )

    assert catalog_scope_key(base, parser_version="0.5.0") == catalog_scope_key(
        changed,
        parser_version="0.5.0",
    )


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
