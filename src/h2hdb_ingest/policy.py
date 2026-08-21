"""Natural, immutable vNext policy construction."""

from __future__ import annotations

__all__ = ["build_ingest_policy"]

from h2hdb import (
    VNextArtifactProducer,
    VNextArtifactStoragePolicy,
    VNextArtifactZipPolicy,
    VNextIngestPolicy,
)

from .artifact import ARTIFACT_ADAPTER_ID, ArtifactProducerIdentity
from .config import IngestConfig


def build_ingest_policy(config: IngestConfig) -> VNextIngestPolicy:
    """Derive all registry authority from consumer-owned natural facts."""

    if not isinstance(config, IngestConfig):
        raise TypeError("config must be IngestConfig")
    identity = ArtifactProducerIdentity.current()
    return VNextIngestPolicy(
        producer=VNextArtifactProducer(
            writer_id=identity.writer_id,
            python_abi=identity.python_abi,
            pillow_build=identity.pillow_build,
            libjpeg_build=identity.libjpeg_build,
            zlib_build=identity.zlib_build,
        ),
        storage=VNextArtifactStoragePolicy(adapter_id=ARTIFACT_ADAPTER_ID),
        max_image_short_side=config.paths.max_image_short_side,
        zip=VNextArtifactZipPolicy(),
        operational_max_batch_rows=config.resident.max_rows,
        artifacts_required=config.paths.artifact_store_path is not None,
    )
