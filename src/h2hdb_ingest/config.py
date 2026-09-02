__all__ = [
    "ArtifactRenderPolicyConfig",
    "ArtifactRenderPreset",
    "IngestConfig",
    "IngestPathsConfig",
    "ResidentConfig",
    "load_config",
]

import json
from collections.abc import Mapping
from enum import StrEnum
from pathlib import Path
from typing import cast

from h2hdb import CoreConfig, resolve_environment_placeholders
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    model_validator,
)

from ._library_layout import (
    LibraryLayoutValidationError,
    validate_precreated_library_layout,
)
from .artifact import (
    MAX_IMAGE_LONG_SIDE,
    MAX_SUPPORTED_JPEG_QUALITY,
    MIN_SUPPORTED_JPEG_QUALITY,
    PAGE_JPEG_QUALITY,
    THUMBNAIL_JPEG_QUALITY,
    ArtifactImageResampler,
    ArtifactRenderPolicy,
)
from .page_workers import MAX_PAGE_RENDER_WORKERS, resolve_page_render_workers

DEFAULT_MAX_ROWS = 128


class ConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ArtifactRenderPreset(StrEnum):
    """Named effective render policies; presets never alter the default implicitly."""

    CANONICAL = "canonical"
    BENCHMARK_LOW_COST = "benchmark-low-cost"


_RENDER_PRESET_VALUES: dict[ArtifactRenderPreset, dict[str, object]] = {
    ArtifactRenderPreset.CANONICAL: {
        "page_jpeg_quality": PAGE_JPEG_QUALITY,
        "thumbnail_jpeg_quality": THUMBNAIL_JPEG_QUALITY,
        "optimize": True,
        "resampler": ArtifactImageResampler.LANCZOS,
    },
    ArtifactRenderPreset.BENCHMARK_LOW_COST: {
        "page_jpeg_quality": 70,
        "thumbnail_jpeg_quality": 70,
        "optimize": False,
        "resampler": ArtifactImageResampler.BILINEAR,
    },
}


class ArtifactRenderPolicyConfig(ConfigModel):
    """Frozen, bounded configuration for all byte-affecting image choices."""

    preset: ArtifactRenderPreset = ArtifactRenderPreset.CANONICAL
    page_jpeg_quality: StrictInt = Field(
        default=PAGE_JPEG_QUALITY,
        ge=MIN_SUPPORTED_JPEG_QUALITY,
        le=MAX_SUPPORTED_JPEG_QUALITY,
    )
    thumbnail_jpeg_quality: StrictInt = Field(
        default=THUMBNAIL_JPEG_QUALITY,
        ge=MIN_SUPPORTED_JPEG_QUALITY,
        le=MAX_SUPPORTED_JPEG_QUALITY,
    )
    optimize: StrictBool = True
    resampler: ArtifactImageResampler = ArtifactImageResampler.LANCZOS

    @model_validator(mode="before")
    @classmethod
    def apply_preset(cls, value: object) -> object:
        if not isinstance(value, Mapping):
            return value
        configured = dict(cast(Mapping[str, object], value))
        raw_preset = configured.get("preset", ArtifactRenderPreset.CANONICAL)
        if not isinstance(raw_preset, str):
            return configured
        try:
            preset = ArtifactRenderPreset(raw_preset)
        except TypeError, ValueError:
            return configured
        for field, default in _RENDER_PRESET_VALUES[preset].items():
            configured.setdefault(field, default)
        return configured

    def to_domain(self, *, max_image_short_side: int) -> ArtifactRenderPolicy:
        """Resolve this config into the immutable renderer boundary value."""

        return ArtifactRenderPolicy(
            max_image_short_side=max_image_short_side,
            page_jpeg_quality=self.page_jpeg_quality,
            thumbnail_jpeg_quality=self.thumbnail_jpeg_quality,
            optimize=self.optimize,
            resampler=self.resampler,
        )


class IngestPathsConfig(ConfigModel):
    download_path: Path
    library_path: Path | None = None
    max_image_short_side: StrictInt = Field(
        default=768,
        ge=1,
        le=MAX_IMAGE_LONG_SIDE,
        description=(
            "Maximum short-side pixels for canonical JPEG pages; aspect ratio is "
            "preserved, images are never enlarged, and the fixed long-side ceiling "
            "is 8192 pixels"
        ),
    )
    render_policy: ArtifactRenderPolicyConfig = Field(
        default_factory=ArtifactRenderPolicyConfig
    )
    page_render_workers: StrictInt | None = Field(
        default=None,
        ge=1,
        le=MAX_PAGE_RENDER_WORKERS,
        description=(
            "Explicit concurrent image-page worker count, or null/omitted for "
            "the process-cached bounded platform default; CBZ serialization "
            "always remains deterministic and in order"
        ),
    )

    def artifact_render_policy(self) -> ArtifactRenderPolicy:
        """Return the exact effective render policy used by policy and runtime."""

        return self.render_policy.to_domain(
            max_image_short_side=self.max_image_short_side
        )

    @property
    def effective_page_render_workers(self) -> int:
        """Resolve the optional override to one bounded runtime worker count."""

        return resolve_page_render_workers(self.page_render_workers)

    @model_validator(mode="after")
    def validate_library_root(self) -> IngestPathsConfig:
        if self.library_path is None:
            return self
        source_root = self.download_path.resolve(strict=False)
        library_root = self.library_path.resolve(strict=False)
        if source_root == library_root:
            raise ValueError("download_path and library_path must be different")
        if source_root.is_relative_to(library_root) or library_root.is_relative_to(
            source_root
        ):
            raise ValueError(
                "download_path and library_path must not contain one another"
            )
        return self


class ResidentConfig(ConfigModel):
    periodic_scan_seconds: float = Field(default=1800, gt=0)
    poll_seconds: float = Field(default=5, gt=0)
    lease_seconds: int = Field(default=300, ge=2)
    heartbeat_seconds: float = Field(default=60, gt=0)
    max_rows: int = Field(default=DEFAULT_MAX_ROWS, ge=1, le=128)

    @model_validator(mode="after")
    def validate_heartbeat(self) -> ResidentConfig:
        if self.heartbeat_seconds >= self.lease_seconds:
            raise ValueError("heartbeat_seconds must be shorter than lease_seconds")
        return self


class IngestConfig(ConfigModel):
    core: CoreConfig = Field(default_factory=CoreConfig)
    paths: IngestPathsConfig
    resident: ResidentConfig = Field(default_factory=ResidentConfig)

    def ensure_paths(self) -> None:
        if not self.paths.download_path.is_dir():
            raise ValueError(
                f"download_path is not a directory: {self.paths.download_path}"
            )
        try:
            is_empty = not any(self.paths.download_path.iterdir())
        except OSError as error:
            raise ValueError(
                f"Unable to inspect download_path {self.paths.download_path}: {error}"
            ) from error
        if is_empty:
            raise ValueError(
                f"download_path is empty: {self.paths.download_path}; "
                "check that the gallery volume is mounted"
            )
        library_path = self.paths.library_path
        if library_path is not None:
            try:
                validate_precreated_library_layout(library_path, durable=False)
            except LibraryLayoutValidationError as error:
                raise ValueError(str(error)) from error


def load_config(path: str | Path) -> IngestConfig:
    config_path = Path(path)
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(
            f"Unable to load ingest config from {config_path}: {error}"
        ) from error
    return IngestConfig.model_validate(resolve_environment_placeholders(raw))
