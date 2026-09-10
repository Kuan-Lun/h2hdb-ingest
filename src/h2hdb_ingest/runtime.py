"""Composition root for the greenfield h2hdb-ingest process."""

from __future__ import annotations

__all__ = ["IngestRuntime", "build_runtime", "configure_logging"]

import logging
from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, field
from threading import Lock
from types import TracebackType
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

from ._diagnostic_logging import DiagnosticFormatter
from ._resource_cleanup import Closeable, close_resources
from .config import IngestConfig
from .image_qualification import ImageGalleryQualifier
from .library import ManagedFilesystemLibraryAdapter
from .library_identity import LibraryStorageIdentityProvider
from .maintenance import (
    LibraryMaintenanceAdapter,
    LibraryMaintenanceOutcome,
)
from .metrics import TextIngestMetricSink, _configure_metric_log_interval
from .page_workers import _decide_page_render_workers
from .policy import build_ingest_policy
from .progress import IngestProgress
from .resident import ResidentIngestor
from .service import VNextIngestService
from .source_monitor import FilesystemCompletionMarkerProbe

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class IngestRuntime:
    """Own the public facades and deterministic ingest-process lifecycle."""

    facade: VNextIngestFacade
    database_admin: VNextDatabaseAdminFacade
    catalog: VNextCatalogFacade
    resident: ResidentIngestor
    _progress: IngestProgress | None = field(default=None, repr=False, compare=False)
    _lifecycle_lock: Lock = field(
        default_factory=Lock,
        init=False,
        repr=False,
        compare=False,
    )
    _closed: bool = field(default=False, init=False, repr=False, compare=False)
    _entered: bool = field(default=False, init=False, repr=False, compare=False)

    def close(self) -> None:
        """Drain caches and release every facade's database pool."""

        with self._lifecycle_lock:
            if self._closed:
                return
            resources: tuple[Closeable, ...] = (
                self.facade,
                self.catalog,
                self.database_admin,
            )
            if self._progress is not None:
                resources = (self._progress, *resources)
            close_resources(resources)
            object.__setattr__(self, "_closed", True)

    def __enter__(self) -> Self:
        with self._lifecycle_lock:
            if self._closed:
                raise ValueError("ingest runtime is closed")
            if self._entered:
                raise ValueError("ingest runtime context is already entered")
            object.__setattr__(self, "_entered", True)
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exception_type, traceback
        try:
            self.close()
        except BaseException as cleanup_error:
            if exception is None:
                raise
            if cleanup_error is not exception:
                exception.add_note(
                    f"Runtime resource cleanup failed: {cleanup_error!r}"
                )
                for note in getattr(cleanup_error, "__notes__", ()):
                    exception.add_note(note)


def build_runtime(
    config: IngestConfig,
    *,
    event_logger: Callable[[str], None] | None = None,
    temporary_cleanup: Callable[[], object] | None = None,
) -> IngestRuntime:
    """Build the sole supported source-to-publication runtime."""

    if not isinstance(config, IngestConfig):
        raise TypeError("config must be IngestConfig")
    facade = VNextIngestFacade(config.core)
    owned_closers = [facade.close]
    try:
        database_admin = VNextDatabaseAdminFacade(config.core)
        owned_closers.append(database_admin.close)
        catalog = VNextCatalogFacade(config.core)
        owned_closers.append(catalog.close)
        runtime_event_logger = event_logger or logger.info
        # Timing/counter records are diagnostics, independent of the human
        # progress callback and its INFO-level delivery.
        metrics_sink = TextIngestMetricSink(
            logging.getLogger("h2hdb_ingest.metrics").debug
        )
        progress = IngestProgress(
            runtime_event_logger,
            interval_seconds=config.resident.progress_log_interval_seconds,
            emit_debug=logger.debug,
        )
        owned_closers.append(progress.close)

        artifact_adapters: dict[bytes, ManagedFilesystemLibraryAdapter] = {}
        finalization_adapters: dict[bytes, ManagedFilesystemLibraryAdapter] = {}
        library_activation: VNextLibraryActivationAdapter
        library_storage_identity: LibraryStorageIdentityProvider | None
        library_maintenance: LibraryMaintenanceAdapter
        publication_guard: Callable[[], AbstractContextManager[None]]
        qualify_gallery: ImageGalleryQualifier | None = None
        if config.paths.library_path is None:
            disabled_library = _DisabledLibraryActivationAdapter()
            library_activation = disabled_library
            library_storage_identity = None
            library_maintenance = disabled_library
            publication_guard = _disabled_publication_guard
        else:
            # One structured decision record per CBZ-enabled runtime build
            # (the host topology inside it is probed at most once per
            # process): the renderer only receives the selected integer, and
            # nothing below this line logs worker selection again per gallery
            # or page.
            worker_decision = _decide_page_render_workers(
                config.paths.page_render_workers
            )
            runtime_event_logger(worker_decision.log_line())
            qualify_gallery = ImageGalleryQualifier(
                config.paths.artifact_render_policy(),
                workers=worker_decision.selected,
                progress=progress,
            )
            library = ManagedFilesystemLibraryAdapter(
                config.paths.library_path,
                source_root=config.paths.download_path,
                render_policy=config.paths.artifact_render_policy(),
                page_render_workers=worker_decision.selected,
                metrics_sink=metrics_sink,
                progress=progress,
            )
            artifact_adapters[library.adapter_id] = library
            finalization_adapters[library.adapter_id] = library
            library_activation = library
            library_storage_identity = library
            library_maintenance = library
            publication_guard = library.publication_guard

        service = VNextIngestService(
            source_root=config.paths.download_path,
            policy=build_ingest_policy(config),
            max_rows=config.resident.max_rows,
            publication_batch_galleries=config.resident.publication_batch_galleries,
            artifact_adapters=artifact_adapters,
            finalization_adapters=finalization_adapters,
            library_activation=library_activation,
            publication_guard=publication_guard,
            qualify_gallery=qualify_gallery,
            metrics_sink=metrics_sink,
            progress=progress,
        )
        resident = ResidentIngestor(
            service=service,
            source_probe=FilesystemCompletionMarkerProbe(config.paths.download_path),
            facade=facade,
            database_admin=database_admin,
            library_storage_identity=library_storage_identity,
            library_maintenance=library_maintenance,
            config=config.resident,
            database_type=config.core.database.sql_type,
            artifact_release_adapters=finalization_adapters,
            temporary_cleanup=temporary_cleanup,
            event_logger=runtime_event_logger,
            progress=progress,
        )
        progress.start()
        return IngestRuntime(facade, database_admin, catalog, resident, progress)
    except BaseException as error:
        for close in reversed(owned_closers):
            try:
                close()
            except BaseException as close_error:
                error.add_note(
                    "A facade also failed to close after runtime "
                    f"construction failed: {close_error!r}"
                )
        raise


def configure_logging(config: IngestConfig) -> None:
    """Own the process handlers and apply core levels plus dependency verbosity."""

    if not isinstance(config, IngestConfig):
        raise TypeError("config must be IngestConfig")
    _configure_metric_log_interval(config.resident.progress_log_interval_seconds)
    level = int(config.core.logger.level)
    dependency_level = (
        logging.DEBUG if level <= logging.DEBUG else max(level, logging.WARNING)
    )
    # These libraries emit normal decoder/authentication details at INFO.
    # Logger propagation does not re-check the root logger's level, so their
    # explicit thresholds must also honor ERROR/CRITICAL configurations.
    logging.getLogger("mysql.connector").setLevel(dependency_level)
    # Native VIPS processing details are INFO. Python-wrapper DEBUG formats
    # Image objects whose repr logs again; parallel console/file handlers can
    # deadlock on those recursive records. Keep that wrapper tracing disabled
    # even in application DEBUG mode, while retaining native diagnostics.
    logging.getLogger("pyvips").setLevel(max(logging.INFO, dependency_level))
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    log_file = config.core.logger.file
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    formatter = DiagnosticFormatter(config)
    for handler in handlers:
        handler.setFormatter(formatter)
    logging.basicConfig(
        level=level,
        handlers=handlers,
        force=True,
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
