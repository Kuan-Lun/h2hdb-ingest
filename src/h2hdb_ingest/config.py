__all__ = [
    "IngestConfig",
    "IngestPathsConfig",
    "ResidentConfig",
    "load_config",
]

import json
import os
import stat
from pathlib import Path

from h2hdb import CoreConfig, resolve_environment_placeholders
from pydantic import BaseModel, ConfigDict, Field, model_validator

DEFAULT_MAX_ROWS = 128


class ConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class IngestPathsConfig(ConfigModel):
    download_path: Path
    library_path: Path | None = None
    max_image_short_side: int = Field(
        default=768,
        ge=1,
        description=(
            "Maximum short-side pixels for CBZ images; aspect ratio is preserved "
            "and images are never enlarged"
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
            _require_existing_library_root(library_path)


def load_config(path: str | Path) -> IngestConfig:
    config_path = Path(path)
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(
            f"Unable to load ingest config from {config_path}: {error}"
        ) from error
    return IngestConfig.model_validate(resolve_environment_placeholders(raw))


def _require_existing_library_root(path: Path) -> None:
    try:
        value = path.lstat()
    except FileNotFoundError as error:
        raise ValueError(
            f"library_path must be a pre-existing bind mount directory: {path}"
        ) from error
    if stat.S_ISLNK(value.st_mode) or not stat.S_ISDIR(value.st_mode):
        raise ValueError(f"library_path is not a safe directory: {path}")
    if value.st_uid != os.geteuid() or stat.S_IMODE(value.st_mode) & 0o022:
        raise ValueError(
            f"library_path must be owned by the ingest UID without group/world write: "
            f"{path}"
        )
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ValueError(f"library_path is not safely openable: {path}") from error
    try:
        opened = os.fstat(descriptor)
        visible = path.lstat()
        if (opened.st_dev, opened.st_ino) != (visible.st_dev, visible.st_ino):
            raise ValueError(f"library_path changed identity: {path}")
    finally:
        os.close(descriptor)
