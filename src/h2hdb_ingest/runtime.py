"""Composition root for the greenfield h2hdb-ingest process."""

from __future__ import annotations

__all__ = ["IngestRuntime", "build_runtime", "configure_logging"]

import logging
from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, field
from threading import Lock
from typing import Self

from h2hdb import (
    LibraryActivationCheckpoint,
    LibraryActivationStatus,
    VNextCatalogFacade,
    VNextDatabaseAdminFacade,
    VNextIngestFacade,
    VNextLibraryActivationAdapter,
    VNextLibraryActivationItem,
)

from .config import IngestConfig
from .library import ManagedFilesystemLibraryAdapter
from .maintenance import (
    LibraryMaintenanceAdapter,
    LibraryMaintenanceOutcome,
)
from .metrics import TextIngestMetricSink
from .policy import build_ingest_policy
from .resident import ResidentIngestor
from .service import VNextIngestService

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class IngestRuntime:
    """Own the public facades and deterministic ingest-process lifecycle."""

    facade: VNextIngestFacade
    database_admin: VNextDatabaseAdminFacade
    catalog: VNextCatalogFacade
    resident: ResidentIngestor
    _lifecycle_lock: Lock = field(
        default_factory=Lock,
        init=False,
        repr=False,
        compare=False,
    )
    _closed: bool = field(default=False, init=False, repr=False, compare=False)
    _entered: bool = field(default=False, init=False, repr=False, compare=False)

    def close(self) -> None:
        """Close facade-owned caches exactly once before returning."""

        with self._lifecycle_lock:
            if self._closed:
                return
            self.facade.close()
            object.__setattr__(self, "_closed", True)

    def __enter__(self) -> Self:
        with self._lifecycle_lock:
            if self._closed:
                raise ValueError("ingest runtime is closed")
            if self._entered:
                raise ValueError("ingest runtime context is already entered")
            object.__setattr__(self, "_entered", True)
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


def build_runtime(
    config: IngestConfig,
    *,
    event_logger: Callable[[str], None] | None = None,
) -> IngestRuntime:
    """Build the sole supported source-to-publication runtime."""

    if not isinstance(config, IngestConfig):
        raise TypeError("config must be IngestConfig")
    facade = VNextIngestFacade(config.core)
    try:
        database_admin = VNextDatabaseAdminFacade(config.core)
        catalog = VNextCatalogFacade(config.core)
        runtime_event_logger = event_logger or logger.info
        metrics_sink = TextIngestMetricSink(runtime_event_logger)

        artifact_adapters: dict[bytes, ManagedFilesystemLibraryAdapter] = {}
        finalization_adapters: dict[bytes, ManagedFilesystemLibraryAdapter] = {}
        library_activation: VNextLibraryActivationAdapter
        library_maintenance: LibraryMaintenanceAdapter
        publication_guard: Callable[[], AbstractContextManager[None]]
        if config.paths.library_path is None:
            disabled_library = _DisabledLibraryActivationAdapter()
            library_activation = disabled_library
            library_maintenance = disabled_library
            publication_guard = _disabled_publication_guard
        else:
            library = ManagedFilesystemLibraryAdapter(
                config.paths.library_path,
                source_root=config.paths.download_path,
                render_policy=config.paths.artifact_render_policy(),
                page_render_workers=config.paths.effective_page_render_workers,
                metrics_sink=metrics_sink,
            )
            artifact_adapters[library.adapter_id] = library
            finalization_adapters[library.adapter_id] = library
            library_activation = library
            library_maintenance = library
            publication_guard = library.publication_guard

        service = VNextIngestService(
            source_root=config.paths.download_path,
            policy=build_ingest_policy(config),
            max_rows=config.resident.max_rows,
            artifact_adapters=artifact_adapters,
            finalization_adapters=finalization_adapters,
            library_activation=library_activation,
            publication_guard=publication_guard,
            metrics_sink=metrics_sink,
        )
        resident = ResidentIngestor(
            service=service,
            facade=facade,
            database_admin=database_admin,
            library_maintenance=library_maintenance,
            config=config.resident,
            database_type=config.core.database.sql_type,
            event_logger=runtime_event_logger,
        )
        return IngestRuntime(facade, database_admin, catalog, resident)
    except BaseException as error:
        try:
            facade.close()
        except BaseException as close_error:
            error.add_note(
                "The ingest facade also failed to close after runtime "
                f"construction failed: {close_error!r}"
            )
        raise


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


class _DisabledLibraryActivationAdapter:
    """A terminal no-op activation for policies that produce no artifacts."""

    def begin(
        self,
        revision: int,
        receipt_id: bytes,
    ) -> LibraryActivationCheckpoint:
        return LibraryActivationCheckpoint(
            revision,
            receipt_id,
            LibraryActivationStatus.COMPLETE,
            None,
        )

    def activate_page(
        self,
        revision: int,
        items: Sequence[VNextLibraryActivationItem],
    ) -> None:
        del revision, items
        raise RuntimeError("artifact-disabled library cannot accept pages")

    def seal(self, revision: int) -> None:
        del revision
        raise RuntimeError("artifact-disabled library cannot be sealed")

    def reconcile_page(
        self,
        revision: int,
        receipt_id: bytes,
        *,
        limit: int,
    ) -> LibraryActivationCheckpoint:
        del revision, receipt_id, limit
        raise RuntimeError("artifact-disabled library cannot be reconciled")

    def complete(self, revision: int, receipt_id: bytes) -> None:
        del revision, receipt_id
        raise RuntimeError("artifact-disabled library cannot be completed")

    def maintain_cleanup(self) -> LibraryMaintenanceOutcome:
        return LibraryMaintenanceOutcome.DONE


def _disabled_publication_guard() -> nullcontext[None]:
    return nullcontext()
