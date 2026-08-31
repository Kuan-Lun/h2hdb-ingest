__all__ = [
    "IngestConfig",
    "IngestPathsConfig",
    "ResidentConfig",
    "load_config",
]

import json
from pathlib import Path

from h2hdb import CoreConfig, resolve_environment_placeholders
from pydantic import BaseModel, ConfigDict, Field, model_validator

from ._library_layout import (
    LibraryLayoutValidationError,
    validate_precreated_library_layout,
)
from .artifact import MAX_IMAGE_LONG_SIDE

DEFAULT_MAX_ROWS = 128


class ConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class IngestPathsConfig(ConfigModel):
    download_path: Path
    library_path: Path | None = None
    max_image_short_side: int = Field(
        default=768,
        ge=1,
        le=MAX_IMAGE_LONG_SIDE,
        description=(
            "Maximum short-side pixels for canonical JPEG pages; aspect ratio is "
            "preserved, images are never enlarged, and the fixed long-side ceiling "
            "is 8192 pixels"
        ),
    )

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
