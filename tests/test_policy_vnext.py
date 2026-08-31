from __future__ import annotations

from pathlib import Path

from h2hdb_ingest.artifact import artifact_policy_fingerprint_sha256
from h2hdb_ingest.config import IngestConfig, IngestPathsConfig, ResidentConfig
from h2hdb_ingest.policy import build_ingest_policy


def test_policy_is_derived_only_from_natural_consumer_facts(tmp_path: Path) -> None:
    config = IngestConfig(
        paths=IngestPathsConfig(
            download_path=tmp_path / "download",
            library_path=tmp_path / "library",
            max_image_short_side=1024,
        ),
        resident=ResidentConfig(max_rows=64),
    )

    policy = build_ingest_policy(config)

    assert policy.artifact.adapter_id == b"managed-filesystem"
    assert policy.artifact.policy_fingerprint_sha256 == (
        artifact_policy_fingerprint_sha256(1024)
    )
    assert policy.operational_max_batch_rows == 64
    assert policy.artifacts_required
