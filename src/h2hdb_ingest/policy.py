"""Natural, immutable vNext policy construction."""

from __future__ import annotations

__all__ = ["build_ingest_policy"]

from h2hdb import (
    VNextArtifactAdapterPolicy,
    VNextIngestPolicy,
)

from .artifact import ARTIFACT_ADAPTER_ID, artifact_policy_fingerprint_sha256
from .config import IngestConfig


def build_ingest_policy(config: IngestConfig) -> VNextIngestPolicy:
    """Derive all registry authority from consumer-owned natural facts."""

    if not isinstance(config, IngestConfig):
        raise TypeError("config must be IngestConfig")
    return VNextIngestPolicy(
        artifact=VNextArtifactAdapterPolicy(
            adapter_id=ARTIFACT_ADAPTER_ID,
            policy_fingerprint_sha256=artifact_policy_fingerprint_sha256(
                config.paths.max_image_short_side
            ),
        ),
        operational_max_batch_rows=config.resident.max_rows,
        artifacts_required=config.paths.library_path is not None,
    )
