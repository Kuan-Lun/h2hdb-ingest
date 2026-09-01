from __future__ import annotations

import json
import stat
from pathlib import Path

import pytest
from pydantic import ValidationError

from h2hdb_ingest import (
    ArtifactImageResampler,
    ArtifactRenderPolicyConfig,
    ArtifactRenderPreset,
    IngestConfig,
    IngestPathsConfig,
    ResidentConfig,
    load_config,
)


def _provision_library_root(root: Path) -> None:
    root.mkdir(mode=0o777)
    root.chmod(0o777)
    current = root / "current"
    for path in (
        current,
        current / "acquisitions",
        current / "artwork",
        root / ".h2hdb-coordination",
    ):
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


def test_loader_resolves_benchmark_preset_and_explicit_render_override(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "ingest.json"
    config_path.write_text(
        json.dumps(
            {
                "paths": {
                    "download_path": "/download",
                    "render_policy": {
                        "preset": "benchmark-low-cost",
                        "page_jpeg_quality": 77,
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.paths.render_policy.page_jpeg_quality == 77
    assert config.paths.render_policy.thumbnail_jpeg_quality == 70
    assert not config.paths.render_policy.optimize
    assert config.paths.render_policy.resampler is ArtifactImageResampler.BILINEAR


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
            library_path / "current" / "acquisitions",
            library_path / "current" / "artwork",
            library_path / ".h2hdb-coordination",
        )
    )


@pytest.mark.parametrize(
    "missing_leaf",
    (
        "current",
        "current/acquisitions",
        "current/artwork",
        ".h2hdb-coordination",
    ),
)
def test_runtime_paths_reject_missing_precreated_reader_root_without_mutation(
    tmp_path: Path,
    missing_leaf: str,
) -> None:
    download_path = tmp_path / "download"
    download_path.mkdir()
    (download_path / "gallery").mkdir()
    library_path = tmp_path / "library"
    _provision_library_root(library_path)
    missing = library_path / missing_leaf
    if missing_leaf == "current":
        (missing / "acquisitions").rmdir()
        (missing / "artwork").rmdir()
    missing.rmdir()
    config = IngestConfig(
        paths=IngestPathsConfig(
            download_path=download_path,
            library_path=library_path,
        )
    )

    with pytest.raises(ValueError, match="pre-existing real directory"):
        config.ensure_paths()

    assert not missing.exists()
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


@pytest.mark.parametrize("value", (0, 8193))
def test_image_short_side_limit_is_bounded(value: int) -> None:
    with pytest.raises(ValidationError):
        IngestPathsConfig(download_path=Path("/download"), max_image_short_side=value)


def test_default_render_policy_preserves_canonical_encoder_parameters() -> None:
    paths = IngestPathsConfig(download_path=Path("/download"))

    assert paths.render_policy == ArtifactRenderPolicyConfig(
        preset=ArtifactRenderPreset.CANONICAL,
        page_jpeg_quality=90,
        thumbnail_jpeg_quality=85,
        optimize=True,
        resampler=ArtifactImageResampler.LANCZOS,
    )
    assert paths.artifact_render_policy().max_image_short_side == 768
    assert paths.page_render_workers == 2


@pytest.mark.parametrize("value", (1, 2, 3, 4))
def test_page_render_workers_accept_every_bounded_value(value: int) -> None:
    paths = IngestPathsConfig(
        download_path=Path("/download"),
        page_render_workers=value,
    )

    assert paths.page_render_workers == value


@pytest.mark.parametrize("value", (0, 5, True, 1.0, "2", None))
def test_page_render_workers_reject_unbounded_or_coerced_values(value: object) -> None:
    with pytest.raises(ValidationError):
        IngestPathsConfig.model_validate(
            {"download_path": "/download", "page_render_workers": value}
        )


def test_benchmark_render_preset_is_explicit_and_can_be_overridden() -> None:
    benchmark = ArtifactRenderPolicyConfig(
        preset=ArtifactRenderPreset.BENCHMARK_LOW_COST,
    )
    overridden = ArtifactRenderPolicyConfig(
        preset=ArtifactRenderPreset.BENCHMARK_LOW_COST,
        page_jpeg_quality=91,
        optimize=True,
    )

    assert benchmark.page_jpeg_quality == 70
    assert benchmark.thumbnail_jpeg_quality == 70
    assert not benchmark.optimize
    assert benchmark.resampler is ArtifactImageResampler.BILINEAR
    assert overridden.page_jpeg_quality == 91
    assert overridden.thumbnail_jpeg_quality == 70
    assert overridden.optimize


@pytest.mark.parametrize("quality", range(96))
def test_every_supported_jpeg_quality_is_validated_without_coercion(
    quality: int,
) -> None:
    config = ArtifactRenderPolicyConfig(
        page_jpeg_quality=quality,
        thumbnail_jpeg_quality=95 - quality,
    )

    assert config.page_jpeg_quality == quality
    assert config.thumbnail_jpeg_quality == 95 - quality
    assert config.to_domain(max_image_short_side=1).page_jpeg_quality == quality


@pytest.mark.parametrize("optimize", (False, True))
@pytest.mark.parametrize("resampler", tuple(ArtifactImageResampler))
def test_every_optimize_resampler_combination_is_validated(
    optimize: bool,
    resampler: ArtifactImageResampler,
) -> None:
    config = ArtifactRenderPolicyConfig(
        optimize=optimize,
        resampler=resampler,
    )

    domain = config.to_domain(max_image_short_side=768)
    assert domain.optimize is optimize
    assert domain.resampler is resampler


@pytest.mark.parametrize("field", ("page_jpeg_quality", "thumbnail_jpeg_quality"))
@pytest.mark.parametrize("value", (-1, 96, True, 1.0, "90", None))
def test_render_policy_rejects_unbounded_or_non_integer_quality(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValidationError):
        ArtifactRenderPolicyConfig.model_validate({field: value})


@pytest.mark.parametrize("value", (0, 1, "true", None))
def test_render_policy_rejects_non_boolean_optimize(value: object) -> None:
    with pytest.raises(ValidationError):
        ArtifactRenderPolicyConfig.model_validate({"optimize": value})


@pytest.mark.parametrize("value", ("spline", "LANCZOS", 1, None))
def test_render_policy_rejects_unsupported_resampler(value: object) -> None:
    with pytest.raises(ValidationError):
        ArtifactRenderPolicyConfig.model_validate({"resampler": value})


def test_render_policy_rejects_unsupported_preset() -> None:
    with pytest.raises(ValidationError):
        ArtifactRenderPolicyConfig.model_validate({"preset": "fastest"})


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
