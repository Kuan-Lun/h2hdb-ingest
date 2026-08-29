from __future__ import annotations

from pathlib import Path

from h2hdb import (
    LibraryActivationStatus,
    VNextCatalogFacade,
    VNextDatabaseAdminFacade,
    VNextIngestFacade,
)

from h2hdb_ingest import IngestConfig, IngestPathsConfig
from h2hdb_ingest.artifact import ARTIFACT_ADAPTER_ID
from h2hdb_ingest.library import ManagedFilesystemLibraryAdapter
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


def test_artifact_disabled_runtime_uses_a_terminal_noop_library(
    tmp_path: Path,
) -> None:
    config = IngestConfig(paths=IngestPathsConfig(download_path=_source_root(tmp_path)))

    service = build_runtime(config).resident._service

    assert isinstance(service, VNextIngestService)
    assert service._artifact_adapters == {}
    assert service._finalization_adapters == {}
    checkpoint = service._library_activation.begin(3, b"r" * 16)
    assert checkpoint.revision == 3
    assert checkpoint.receipt_id == b"r" * 16
    assert checkpoint.status is LibraryActivationStatus.COMPLETE


def test_cbz_runtime_shares_one_adapter_for_protection_and_release(
    tmp_path: Path,
) -> None:
    source = _source_root(tmp_path)
    library_root = tmp_path / "library"
    config = IngestConfig(
        paths=IngestPathsConfig(
            download_path=source,
            library_path=library_root,
        )
    )

    service = build_runtime(config).resident._service

    assert isinstance(service, VNextIngestService)
    storage = service._artifact_adapters[ARTIFACT_ADAPTER_ID]
    assert id(service._finalization_adapters[ARTIFACT_ADAPTER_ID]) == id(storage)
    assert isinstance(service._library_activation, ManagedFilesystemLibraryAdapter)
    assert service._publication_guard == service._library_activation.publication_guard
