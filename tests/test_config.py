from __future__ import annotations

import json
import stat
from pathlib import Path

import pytest
from pydantic import ValidationError

from h2hdb_ingest import IngestConfig, IngestPathsConfig, ResidentConfig, load_config


def _provision_library_root(root: Path) -> None:
    root.mkdir(mode=0o777)
    root.chmod(0o777)
    for path in (root / "current", root / ".h2hdb-coordination"):
        path.mkdir(mode=0o777)
        path.chmod(0o777)


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

    assert config.paths.library_path is None


@pytest.mark.parametrize("library_relative", [".", "nested", "../download"])
def test_source_and_library_roots_must_be_distinct_and_non_nested(
    tmp_path: Path,
    library_relative: str,
) -> None:
    download_path = tmp_path / "download"
    with pytest.raises(ValidationError):
        IngestPathsConfig(
            download_path=download_path,
            library_path=download_path / library_relative,
        )


def test_runtime_paths_require_three_preexisting_library_mount_roots(
    tmp_path: Path,
) -> None:
    download_path = tmp_path / "download"
    download_path.mkdir()
    (download_path / "gallery").mkdir()
    library_path = tmp_path / "library"
    _provision_library_root(library_path)
    config = IngestConfig(
        paths=IngestPathsConfig(
            download_path=download_path,
            library_path=library_path,
        )
    )

    config.ensure_paths()

    assert {path.name for path in library_path.iterdir()} == {
        ".h2hdb-coordination",
        "current",
    }


def test_runtime_paths_delegate_host_permission_policy_to_deployment(
    tmp_path: Path,
) -> None:
    download_path = tmp_path / "download"
    download_path.mkdir()
    (download_path / "gallery").mkdir()
    library_path = tmp_path / "library"
    _provision_library_root(library_path)
    config = IngestConfig(
        paths=IngestPathsConfig(
            download_path=download_path,
            library_path=library_path,
        )
    )

    config.ensure_paths()

    assert stat.S_IMODE(library_path.stat().st_mode) == 0o777
    assert all(
        stat.S_IMODE(path.stat().st_mode) == 0o777
        for path in (
            library_path / "current",
            library_path / ".h2hdb-coordination",
        )
    )


@pytest.mark.parametrize("missing_leaf", ("current", ".h2hdb-coordination"))
def test_runtime_paths_reject_missing_precreated_reader_root_without_mutation(
    tmp_path: Path,
    missing_leaf: str,
) -> None:
    download_path = tmp_path / "download"
    download_path.mkdir()
    (download_path / "gallery").mkdir()
    library_path = tmp_path / "library"
    _provision_library_root(library_path)
    (library_path / missing_leaf).rmdir()
    config = IngestConfig(
        paths=IngestPathsConfig(
            download_path=download_path,
            library_path=library_path,
        )
    )

    with pytest.raises(ValueError, match="pre-existing real directory"):
        config.ensure_paths()

    assert not (library_path / missing_leaf).exists()
    assert not (library_path / ".h2hdb-state").exists()


def test_runtime_paths_reject_missing_library_mount(tmp_path: Path) -> None:
    download_path = tmp_path / "download"
    download_path.mkdir()
    (download_path / "gallery").mkdir()
    config = IngestConfig(
        paths=IngestPathsConfig(
            download_path=download_path,
            library_path=tmp_path / "missing-library",
        )
    )

    with pytest.raises(ValueError, match="pre-existing real directory"):
        config.ensure_paths()


def test_runtime_paths_reject_empty_download_mount(tmp_path: Path) -> None:
    download_path = tmp_path / "empty-download"
    download_path.mkdir()
    config = IngestConfig(paths=IngestPathsConfig(download_path=download_path))

    with pytest.raises(ValueError, match="gallery volume is mounted"):
        config.ensure_paths()


def test_runtime_paths_reject_managed_directory_symlink_without_touching_target(
    tmp_path: Path,
) -> None:
    download_path = tmp_path / "download"
    download_path.mkdir()
    (download_path / "gallery").mkdir()
    library_path = tmp_path / "library"
    outside = tmp_path / "outside"
    outside.mkdir(mode=0o700)
    library_path.symlink_to(outside, target_is_directory=True)
    config = IngestConfig(
        paths=IngestPathsConfig(
            download_path=download_path,
            library_path=library_path,
        )
    )

    with pytest.raises(ValueError, match="not a real directory"):
        config.ensure_paths()

    assert stat.S_IMODE(outside.stat().st_mode) == 0o700


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
