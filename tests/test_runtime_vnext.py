from __future__ import annotations

from pathlib import Path

from h2hdb import (
    CurrentProjectionStatus,
    VNextCatalogFacade,
    VNextDatabaseAdminFacade,
    VNextIngestFacade,
)

from h2hdb_ingest import IngestConfig, IngestPathsConfig
from h2hdb_ingest.artifact import ARTIFACT_ADAPTER_ID
from h2hdb_ingest.projection import CurrentProjectionAdapter
from h2hdb_ingest.runtime import build_runtime
from h2hdb_ingest.service import VNextIngestService


def _source_root(tmp_path: Path) -> Path:
    source = tmp_path / "download"
    source.mkdir()
    (source / "mounted-volume-marker").touch()
    return source


def test_runtime_composes_only_public_vnext_core_facades(tmp_path: Path) -> None:
    config = IngestConfig(paths=IngestPathsConfig(download_path=_source_root(tmp_path)))

    runtime = build_runtime(config)

    assert isinstance(runtime.facade, VNextIngestFacade)
    assert isinstance(runtime.database_admin, VNextDatabaseAdminFacade)
    assert isinstance(runtime.catalog, VNextCatalogFacade)
    assert isinstance(runtime.resident._service, VNextIngestService)
    assert runtime.resident._facade is runtime.facade
    assert runtime.resident._database_admin is runtime.database_admin


def test_artifact_disabled_runtime_uses_a_terminal_noop_projection(
    tmp_path: Path,
) -> None:
    config = IngestConfig(paths=IngestPathsConfig(download_path=_source_root(tmp_path)))

    service = build_runtime(config).resident._service

    assert isinstance(service, VNextIngestService)
    assert service._artifact_adapters == {}
    assert service._finalization_adapters == {}
    checkpoint = service._current_projection.begin(3, b"r" * 16)
    assert checkpoint.revision == 3
    assert checkpoint.receipt_id == b"r" * 16
    assert checkpoint.status is CurrentProjectionStatus.COMPLETE


def test_cbz_runtime_shares_one_adapter_for_protection_and_release(
    tmp_path: Path,
) -> None:
    source = _source_root(tmp_path)
    artifact_root = tmp_path / "artifacts"
    current_root = tmp_path / "komga"
    config = IngestConfig(
        paths=IngestPathsConfig(
            download_path=source,
            artifact_store_path=artifact_root,
            cbz_path=current_root,
        )
    )

    service = build_runtime(config).resident._service

    assert isinstance(service, VNextIngestService)
    storage = service._artifact_adapters[ARTIFACT_ADAPTER_ID]
    assert id(service._finalization_adapters[ARTIFACT_ADAPTER_ID]) == id(storage)
    assert isinstance(service._current_projection, CurrentProjectionAdapter)
    assert service._publication_guard == service._current_projection.publication_guard
