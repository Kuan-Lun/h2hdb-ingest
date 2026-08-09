"""Filesystem ingest and CBZ reconciliation for H2HDB."""

__all__ = [
    "CBZReconciler",
    "CBZSourceChangedError",
    "CBZGrouping",
    "DeduplicationPolicy",
    "FilesystemScanner",
    "FilesystemSourceStager",
    "GalleryScanError",
    "IngestConfig",
    "IngestSynchronizer",
    "IngestPathsConfig",
    "LegacyIngestService",
    "ResidentConfig",
    "ResidentIngestor",
    "StagedDeduplicationPlanner",
    "StagedIngestService",
    "SyncOutcome",
    "CoreFileHashCache",
    "CatalogScopeMismatchError",
    "catalog_scope_key",
    "gallery_name_to_cbz_file_name",
    "load_config",
]

from .cbz import CBZReconciler, CBZSourceChangedError
from .config import (
    CBZGrouping,
    IngestConfig,
    IngestPathsConfig,
    ResidentConfig,
    load_config,
)
from .deduplication import DeduplicationPolicy
from .models import SyncOutcome
from .naming import gallery_name_to_cbz_file_name
from .resident import ResidentIngestor
from .scanner import FilesystemScanner, GalleryScanError
from .scope import catalog_scope_key
from .service import IngestService as LegacyIngestService
from .staged_deduplication import StagedDeduplicationPlanner
from .staged_service import IngestSynchronizer, StagedIngestService
from .staging import (
    CatalogScopeMismatchError,
    CoreFileHashCache,
    FilesystemSourceStager,
)
