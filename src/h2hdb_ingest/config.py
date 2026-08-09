__all__ = [
    "IngestConfig",
    "IngestPathsConfig",
    "ResidentConfig",
    "load_config",
]

import json
import os
import re
from enum import StrEnum
from pathlib import Path

from h2hdb import CoreConfig, resolve_environment_placeholders
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

DEFAULT_HASH_WORKERS = min(4, os.process_cpu_count() or 1)
DEFAULT_CBZ_WORKERS = min(4, os.process_cpu_count() or 1)
DEFAULT_STALE_TEMP_AGE_SECONDS = 60
DEFAULT_SCAN_BATCH_GALLERIES = 128
DEFAULT_SCAN_BATCH_FILES = 2_048
_PAGES_SORT_PATTERN = re.compile(r"pages(?:\+([1-9]\d*))?")


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
    cbz_sort: str = "no"
    cbz_workers: int = Field(default=DEFAULT_CBZ_WORKERS, ge=1, le=32)
    stale_temp_age_seconds: int = Field(
        default=DEFAULT_STALE_TEMP_AGE_SECONDS,
        ge=60,
    )
    hash_workers: int = Field(default=DEFAULT_HASH_WORKERS, ge=1, le=32)
    scan_batch_galleries: int = Field(
        default=DEFAULT_SCAN_BATCH_GALLERIES,
        ge=1,
        le=200,
    )
    scan_batch_files: int = Field(
        default=DEFAULT_SCAN_BATCH_FILES,
        ge=1,
        le=2_048,
    )

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

    @field_validator("cbz_sort")
    @classmethod
    def validate_cbz_sort(cls, value: str) -> str:
        if value in {"no", "upload_time", "download_time", "gid", "title"}:
            return value
        if _PAGES_SORT_PATTERN.fullmatch(value):
            return value
        raise ValueError(
            "cbz_sort must be no, upload_time, download_time, gid, title, pages, "
            "or pages+[num]"
        )


class ResidentConfig(ConfigModel):
    periodic_scan_seconds: float = Field(default=1800, gt=0)
    poll_seconds: float = Field(default=5, gt=0)
    lease_seconds: int = Field(default=300, ge=2)
    heartbeat_seconds: float = Field(default=60, gt=0)

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
