"""Composition root for the greenfield h2hdb-ingest process."""

from __future__ import annotations

__all__ = ["IngestRuntime", "build_runtime", "configure_logging"]

import logging
from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass

from h2hdb import (
    CurrentProjectionCheckpoint,
    CurrentProjectionStatus,
    VNextCatalogFacade,
    VNextCurrentProjectionAdapter,
    VNextCurrentProjectionItem,
    VNextDatabaseAdminFacade,
    VNextIngestFacade,
)

from .artifact import ManagedFilesystemArtifactAdapter
from .config import IngestConfig
from .maintenance import (
    CurrentProjectionMaintenanceAdapter,
    CurrentProjectionMaintenanceOutcome,
)
from .policy import build_ingest_policy
from .projection import CurrentProjectionAdapter
from .resident import ResidentIngestor
from .service import VNextIngestService

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class IngestRuntime:
    """Public core facades and the fully composed resident process."""

    facade: VNextIngestFacade
    database_admin: VNextDatabaseAdminFacade
    catalog: VNextCatalogFacade
    resident: ResidentIngestor


def build_runtime(
    config: IngestConfig,
    *,
    event_logger: Callable[[str], None] | None = None,
) -> IngestRuntime:
    """Build the sole supported source-to-publication runtime."""

    if not isinstance(config, IngestConfig):
        raise TypeError("config must be IngestConfig")
    facade = VNextIngestFacade(config.core)
    database_admin = VNextDatabaseAdminFacade(config.core)
    catalog = VNextCatalogFacade(config.core)

    artifact_adapters: dict[bytes, ManagedFilesystemArtifactAdapter] = {}
    finalization_adapters: dict[bytes, ManagedFilesystemArtifactAdapter] = {}
    current_projection: VNextCurrentProjectionAdapter
    current_projection_maintenance: CurrentProjectionMaintenanceAdapter
    publication_guard: Callable[[], AbstractContextManager[None]]
    if config.paths.artifact_store_path is None:
        disabled_projection = _DisabledCurrentProjectionAdapter()
        current_projection = disabled_projection
        current_projection_maintenance = disabled_projection
        publication_guard = _disabled_publication_guard
    else:
        cbz_path = config.paths.cbz_path
        if cbz_path is None:  # protected by IngestPathsConfig validation
            raise RuntimeError("artifact output lacks its current projection root")
        artifact = ManagedFilesystemArtifactAdapter(
            config.paths.artifact_store_path,
            max_image_short_side=config.paths.max_image_short_side,
        )
        artifact_adapters[artifact.adapter_id] = artifact
        finalization_adapters[artifact.adapter_id] = artifact
        projection = CurrentProjectionAdapter(
            artifact_store_path=config.paths.artifact_store_path,
            cbz_path=cbz_path,
            grouping=config.paths.cbz_grouping,
            artifact_adapter=artifact,
        )
        current_projection = projection
        current_projection_maintenance = projection
        publication_guard = projection.publication_guard

    service = VNextIngestService(
        source_root=config.paths.download_path,
        policy=build_ingest_policy(config),
        max_rows=config.resident.max_rows,
        artifact_adapters=artifact_adapters,
        finalization_adapters=finalization_adapters,
        current_projection=current_projection,
        publication_guard=publication_guard,
    )
    resident = ResidentIngestor(
        service=service,
        facade=facade,
        database_admin=database_admin,
        current_projection_maintenance=current_projection_maintenance,
        config=config.resident,
        database_type=config.core.database.sql_type,
        event_logger=event_logger or logger.info,
    )
    return IngestRuntime(facade, database_admin, catalog, resident)


def configure_logging(config: IngestConfig) -> None:
    """Configure the process logger from the embedded public core settings."""

    if not isinstance(config, IngestConfig):
        raise TypeError("config must be IngestConfig")
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    log_file = config.core.logger.file
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(
        level=int(config.core.logger.level),
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers,
    )


class _DisabledCurrentProjectionAdapter:
    """A terminal no-op projection for policies that produce no artifacts."""

    def begin(
        self,
        revision: int,
        receipt_id: bytes,
    ) -> CurrentProjectionCheckpoint:
        return CurrentProjectionCheckpoint(
            revision,
            receipt_id,
            CurrentProjectionStatus.COMPLETE,
            None,
        )

    def append_page(
        self,
        revision: int,
        items: Sequence[VNextCurrentProjectionItem],
    ) -> None:
        del revision, items
        raise RuntimeError("artifact-disabled projection cannot accept pages")

    def seal(self, revision: int) -> None:
        del revision
        raise RuntimeError("artifact-disabled projection cannot be sealed")

    def reconcile(self, revision: int) -> None:
        del revision
        raise RuntimeError("artifact-disabled projection cannot be reconciled")

    def maintain_cleanup(self) -> CurrentProjectionMaintenanceOutcome:
        return CurrentProjectionMaintenanceOutcome.DONE


def _disabled_publication_guard() -> nullcontext[None]:
    return nullcontext()
