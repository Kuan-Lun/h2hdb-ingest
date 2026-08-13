"""Stable identity for resumable filesystem catalog builds."""

from __future__ import annotations

__all__ = ["catalog_scope_key"]

import json
from hashlib import sha256
from importlib.metadata import version
from pathlib import Path

from .cbz import CBZ_MANIFEST_VERSION
from .config import IngestPathsConfig
from .scanner import FILESYSTEM_OBSERVATION_VERSION
from .source_manifest import CANONICAL_SOURCE_MANIFEST_VERSION

_SCOPE_FORMAT_VERSION = 1
_STAGED_DEDUPLICATION_POLICY_VERSION = 1


def catalog_scope_key(
    paths: IngestPathsConfig,
    *,
    parser_version: str | None = None,
) -> str:
    """Return a canonical fingerprint of inputs that affect catalog semantics.

    Concurrency and batch-size settings are deliberately absent so operators can
    tune throughput without discarding durable scan progress. Paths are resolved
    because changing the mounted corpus or either CBZ root changes the meaning of
    a resumable build.
    """

    if parser_version is None:
        parser_version = version("h2h-galleryinfo-parser")
    cbz_enabled = paths.cbz_path is not None
    cbz_policy = (
        {
            "artifact_manifest_version": CBZ_MANIFEST_VERSION,
            "artifact_store_root": _resolved(paths.artifact_store_path),
            "current_projection_root": _resolved(paths.cbz_path),
            "enabled": True,
            "grouping": paths.cbz_grouping.value,
            "max_image_short_side": paths.max_image_short_side,
            "sort": paths.cbz_sort,
        }
        if cbz_enabled
        else {"enabled": False}
    )
    payload = {
        "cbz": cbz_policy,
        "deduplication_policy_version": _STAGED_DEDUPLICATION_POLICY_VERSION,
        "parser_version": parser_version,
        "scope_format_version": _SCOPE_FORMAT_VERSION,
        "source": {
            "canonical_manifest_version": CANONICAL_SOURCE_MANIFEST_VERSION,
            "filesystem_observation_version": FILESYSTEM_OBSERVATION_VERSION,
            "root": _resolved(paths.download_path),
        },
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return f"filesystem-v{_SCOPE_FORMAT_VERSION}:{sha256(encoded).hexdigest()}"


def _resolved(path: Path | None) -> str | None:
    if path is None:
        return None
    return str(path.resolve(strict=False))
