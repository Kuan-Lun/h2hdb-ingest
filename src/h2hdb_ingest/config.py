__all__ = [
    "IngestConfig",
    "IngestPathsConfig",
    "ResidentConfig",
    "load_config",
]

import json
from enum import StrEnum
from pathlib import Path

from h2hdb import CoreConfig, resolve_environment_placeholders
from pydantic import BaseModel, ConfigDict, Field, model_validator

DEFAULT_MAX_ROWS = 128


class CBZGrouping(StrEnum):
    flat = "flat"
    date_yyyy = "date-yyyy"
    date_yyyy_mm = "date-yyyy-mm"
    date_yyyy_mm_dd = "date-yyyy-mm-dd"


class ConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class IngestPathsConfig(ConfigModel):
    download_path: Path
    cbz_path: Path | None = None
    artifact_store_path: Path | None = None
    max_image_short_side: int = Field(
        default=768,
        ge=1,
        description=(
            "Maximum short-side pixels for CBZ images; aspect ratio is preserved "
            "and images are never enlarged"
        ),
    )
    cbz_grouping: CBZGrouping = CBZGrouping.flat

    @model_validator(mode="after")
    def validate_cbz_roots(self) -> IngestPathsConfig:
        if (self.cbz_path is None) != (self.artifact_store_path is None):
            raise ValueError(
                "cbz_path and artifact_store_path must either both be set or "
                "both be null"
            )
        if self.cbz_path is None or self.artifact_store_path is None:
            return self
        cbz_root = self.cbz_path.resolve(strict=False)
        artifact_root = self.artifact_store_path.resolve(strict=False)
        if cbz_root == artifact_root:
            raise ValueError("cbz_path and artifact_store_path must be different")
        if cbz_root.is_relative_to(artifact_root) or artifact_root.is_relative_to(
            cbz_root
        ):
            raise ValueError(
                "cbz_path and artifact_store_path must not contain one another"
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
        if self.paths.cbz_path is not None:
            assert self.paths.artifact_store_path is not None
            self.paths.cbz_path.mkdir(parents=True, exist_ok=True)
            self.paths.artifact_store_path.mkdir(parents=True, exist_ok=True)


def load_config(path: str | Path) -> IngestConfig:
    config_path = Path(path)
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(
            f"Unable to load ingest config from {config_path}: {error}"
        ) from error
    return IngestConfig.model_validate(resolve_environment_placeholders(raw))
