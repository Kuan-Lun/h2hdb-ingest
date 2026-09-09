"""Resident polling loop for the public vNext ingest facade."""

from __future__ import annotations

__all__ = ["IngestSynchronizer", "ResidentIngestor"]

import logging
from collections.abc import Callable, Mapping
from enum import StrEnum
from threading import Event
from time import monotonic
from typing import Protocol

from h2hdb import (
    ArtifactReleaseAdapter,
    GalleryStagingCapacityError,
    SchemaEpochReport,
    VNextCurrentOnlyMaintenanceOutcome,
    VNextDatabaseAdminFacade,
    VNextIngestFacade,
    VNextIngestSession,
    VNextSourceChangedError,
    VNextSourceManifestMismatchError,
)

from .artifact_errors import format_artifact_failure
from .config import ResidentConfig
from .filesystem import FilesystemSourceChangedError
from .library_identity import (
    LibraryStorageIdentity,
    LibraryStorageIdentityMismatchError,
    LibraryStorageIdentityProvider,
)
from .maintenance import (
    LibraryMaintenanceAdapter,
    LibraryMaintenanceOutcome,
    _LibraryStagingSlotConflictError,
)
from .progress import IngestProgress, ProgressWork
from .scratch import ScratchSafetyError
from .service import VNextIngestSynchronizationResult, _IngestStopRequested
from .session import IngestLeaseHeartbeat, IngestSessionController
from .source_monitor import CompletionMarkerProbe, SourceChangeMonitor
from .source_schedule import SourceScanSchedule, SourceScanTicket
from .storage_capacity import storage_capacity_error, storage_capacity_message

logger = logging.getLogger(__name__)


class _ResidentCycleOutcome(StrEnum):
    INGESTED = "INGESTED"
    BATCH_PUBLISHED = "BATCH_PUBLISHED"
    MAINTENANCE_PROGRESSED = "MAINTENANCE_PROGRESSED"
    SOURCE_CHANGED = "SOURCE_CHANGED"
    IDLE = "IDLE"


class _PostflightFailed(Exception):
    """Preserve a callback failure until the heartbeat has stopped."""

    def __init__(self, failure: BaseException) -> None:
        super().__init__(str(failure))
        self.failure = failure


class IngestSynchronizer(Protocol):
    """Cross-phase service invoked under one renewable public session."""

    def synchronize_once(
        self,
        session: IngestSessionController,
        *,
        should_stop: Callable[[], bool] | None = None,
    ) -> VNextIngestSynchronizationResult: ...


class ResidentIngestor:
    def __init__(
        self,
        *,
        service: IngestSynchronizer,
        source_probe: CompletionMarkerProbe,
        facade: VNextIngestFacade,
        database_admin: VNextDatabaseAdminFacade,
        library_storage_identity: LibraryStorageIdentityProvider | None,
        library_maintenance: LibraryMaintenanceAdapter,
        config: ResidentConfig,
        database_type: str,
        artifact_release_adapters: Mapping[bytes, ArtifactReleaseAdapter],
        event_logger: Callable[[str], None] | None = None,
        progress: IngestProgress | None = None,
        temporary_cleanup: Callable[[], object] | None = None,
    ) -> None:
        self._service = service
        self._source_probe = source_probe
        self._facade = facade
        self._database_admin = database_admin
        if library_storage_identity is not None and not isinstance(
            library_storage_identity,
            LibraryStorageIdentityProvider,
        ):
            raise TypeError(
                "library_storage_identity must implement the storage identity protocol"
            )
        self._library_storage_identity = library_storage_identity
        if not isinstance(
            library_maintenance,
            LibraryMaintenanceAdapter,
        ):
            raise TypeError(
                "library_maintenance must implement the bounded maintenance protocol"
            )
        self._library_maintenance = library_maintenance
        if not isinstance(artifact_release_adapters, Mapping):
            raise TypeError("artifact_release_adapters must be a mapping")
        self._artifact_release_adapters = dict(artifact_release_adapters)
        if self._artifact_release_adapters and library_storage_identity is None:
            raise ValueError(
                "artifact-enabled resident requires a library storage identity"
            )
        self._config = config
        self._database_type = database_type.casefold()
        self._event_logger = event_logger or logger.info
        self._progress = progress
        self._temporary_cleanup = temporary_cleanup
        self._capacity_waiting = False
        self._progress_pending = False
        self._scheduled_progress = False
        self._progress_wait_reason = "waiting_for_ingest_lease"
        self._bound_storage_identity: LibraryStorageIdentity | None = None
        self._storage_instance_ready = library_storage_identity is None
        self._last_synchronization_result: VNextIngestSynchronizationResult | None = (
            None
        )

    @property
    def last_synchronization_result(self) -> VNextIngestSynchronizationResult | None:
        """Return the last completed batch; maintenance never replaces this value."""

        return self._last_synchronization_result

    @property
    def deferred_gallery_count(self) -> int:
        """Report new galleries deferred by this process's last completed batch."""

        result = self._last_synchronization_result
        return 0 if result is None else result.deferred_gallery_count

    def initialize(self) -> SchemaEpochReport:
        """Validate an existing READY epoch without creating or migrating it."""

        work = self._begin_progress("startup_check", announce=True)
        try:
            report = self._initialize()
        except BaseException:
            if work is not None:
                work.finish("failed")
            raise
        if work is not None:
            work.finish()
        return report

    def _initialize(self) -> SchemaEpochReport:

        self._bound_storage_identity = None
        self._last_synchronization_result = None
        self._storage_instance_ready = self._library_storage_identity is None
        report = self._database_admin.check()
        self._progress_operation("initialize_storage")
        if self._library_storage_identity is not None:
            identity = self._library_storage_identity.ensure_storage_identity()
            self._database_admin.bind_storage_instance(
                identity.storage_instance_uuid,
            )
            self._bound_storage_identity = identity
        self._run_library_maintenance()
        self._try_current_only_maintenance()
        self._storage_instance_ready = True
        return report

    def process_available(
        self,
        *,
        periodic_scan: bool,
        preflight: Callable[[], None] | None = None,
        postflight: Callable[[], None] | None = None,
        should_stop: Callable[[], bool] | None = None,
    ) -> bool:
        """Process one unit; postflight runs after publication under the live lease."""

        if not self._storage_instance_ready:
            raise RuntimeError(
                "CBZ-enabled resident must initialize its storage instance before "
                "processing"
            )

        return self._process_cycle(
            periodic_scan=periodic_scan,
            preflight=preflight,
            postflight=postflight,
            should_stop=should_stop,
        ) in (
            _ResidentCycleOutcome.INGESTED,
            _ResidentCycleOutcome.BATCH_PUBLISHED,
            _ResidentCycleOutcome.MAINTENANCE_PROGRESSED,
        )

    def _process_cycle(
        self,
        *,
        periodic_scan: bool,
        preflight: Callable[[], None] | None = None,
        postflight: Callable[[], None] | None = None,
        should_stop: Callable[[], bool] | None = None,
        on_scan_started: Callable[[], None] | None = None,
    ) -> _ResidentCycleOutcome:
        if self._progress_stop_requested(should_stop):
            self._finish_progress("stopped", announce=False)
            return _ResidentCycleOutcome.IDLE
        work = self._begin_progress("coordination", announce=False)
        self._progress_pending = periodic_scan or self._scheduled_progress
        self._progress_wait_reason = (
            "waiting_for_ingest_lease" if periodic_scan else "source_quiet_period"
        )
        try:
            outcome = self._process_cycle_step(
                periodic_scan=periodic_scan,
                preflight=preflight,
                postflight=postflight,
                should_stop=should_stop,
                on_scan_started=on_scan_started,
            )
            stopped = should_stop is not None and should_stop()
        except BaseException:
            self._finish_progress("failed")
            raise
        if stopped:
            self._finish_progress("stopped", announce=False)
        elif outcome in (
            _ResidentCycleOutcome.INGESTED,
            _ResidentCycleOutcome.BATCH_PUBLISHED,
        ):
            self._finish_progress("completed")
        elif outcome is _ResidentCycleOutcome.SOURCE_CHANGED:
            self._finish_progress("retry")
        elif outcome is _ResidentCycleOutcome.MAINTENANCE_PROGRESSED:
            if work is not None:
                work.phase("maintenance", announce=False)
        elif self._progress_pending:
            if work is not None:
                work.phase("waiting_for_work", announce=False)
                work.operation(self._progress_wait_reason)
        else:
            self._finish_progress("idle", announce=False)
        return outcome

    def _process_cycle_step(
        self,
        *,
        periodic_scan: bool,
        preflight: Callable[[], None] | None = None,
        postflight: Callable[[], None] | None = None,
        should_stop: Callable[[], bool] | None = None,
        on_scan_started: Callable[[], None] | None = None,
    ) -> _ResidentCycleOutcome:
        try:
            claimed = self._claim_after_maintenance(
                periodic_scan=periodic_scan, should_stop=should_stop
            )
        except Exception as error:
            if storage_capacity_error(error) is None:
                raise
            self._wait_for_storage_capacity(error, operation="ingest coordination")
            return _ResidentCycleOutcome.IDLE
        if isinstance(claimed, _ResidentCycleOutcome):
            return claimed
        lease_duration = self._config.lease_seconds * 1_000_000
        work = self._current_progress()
        if work is not None:
            work.phase("ingest")
            work.advance("attempts_started")
        session = IngestSessionController(
            self._facade,
            claimed,
            lease_duration_microseconds=lease_duration,
            database_type=self._database_type,
        )
        if preflight is not None:
            try:
                preflight()
            except BaseException as error:
                try:
                    session.complete()
                    self._try_library_maintenance()
                    self._try_current_only_maintenance(lease_duration)
                except BaseException as completion_error:
                    error.add_note(
                        "The ingest session could not be completed after preflight "
                        f"failed: {completion_error!r}"
                    )
                raise
        if on_scan_started is not None:
            on_scan_started()
        try:
            with IngestLeaseHeartbeat(
                session,
                interval_seconds=self._config.heartbeat_seconds,
            ) as heartbeat:
                if should_stop is None:
                    outcome = self._service.synchronize_once(session)
                else:
                    outcome = self._service.synchronize_once(
                        session,
                        should_stop=should_stop,
                    )
                heartbeat.raise_if_failed()
                if not isinstance(outcome, VNextIngestSynchronizationResult):
                    raise TypeError(
                        "synchronizer returned an invalid publication result"
                    )
                if postflight is not None:
                    try:
                        postflight()
                    except BaseException as error:
                        raise _PostflightFailed(error) from error
                self._event_logger(
                    "vNext ingest publication batch completed: "
                    f"deferred_galleries={outcome.deferred_gallery_count} "
                    f"known_galleries={outcome.source.staged_galleries}"
                )
        except _PostflightFailed as error:
            # The context manager has stopped renewal before releasing the
            # exact session. Callback failures must not retain the ingest lease
            # or be mistaken for source-change/capacity retry outcomes.
            try:
                session.complete()
                self._try_library_maintenance()
                self._try_current_only_maintenance(lease_duration)
            except BaseException as completion_error:
                error.failure.add_note(
                    "The ingest session could not be completed after postflight "
                    f"failed: {completion_error!r}"
                )
            raise error.failure from None
        except _IngestStopRequested:
            self._event_logger(
                "vNext ingest stopped at a durable bounded-step boundary"
            )
            return _ResidentCycleOutcome.IDLE
        except _LibraryStagingSlotConflictError as error:
            self._progress_pending = True
            self._progress_wait_reason = "waiting_for_staging_slot"
            self._progress_operation("waiting_for_staging_slot")
            # A crashed predecessor can retain the same destination until the
            # new policy makes that candidate inactive.  Release this
            # session's SHARED gate, reconcile the predecessor under
            # EXCLUSIVE, and let the next poll resume the durable new work.
            try:
                session.complete()
            except BaseException as completion_error:
                error.add_note(
                    "The ingest session could not be completed after a stale "
                    f"artifact staging conflict: {completion_error!r}"
                )
                raise error from completion_error
            library_maintenance = self._try_library_maintenance()
            database_maintenance = self._try_current_only_maintenance(lease_duration)
            if (
                library_maintenance is LibraryMaintenanceOutcome.PROGRESSED
                or database_maintenance is VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
            ):
                return _ResidentCycleOutcome.MAINTENANCE_PROGRESSED
            if database_maintenance is VNextCurrentOnlyMaintenanceOutcome.DONE:
                raise error
            return _ResidentCycleOutcome.IDLE
        except GalleryStagingCapacityError as error:
            self._progress_pending = True
            self._progress_wait_reason = "waiting_for_staging_capacity"
            self._progress_operation("waiting_for_staging_capacity")
            # Capacity is bounded backpressure, not a failed resident process.
            # The rejected request committed no rows, while completing the
            # exact session releases its SHARED gate and makes stale terminal
            # staging eligible for bounded EXCLUSIVE maintenance.
            try:
                session.complete()
            except BaseException as completion_error:
                error.add_note(
                    "The ingest session could not be completed after gallery "
                    f"staging capacity was exhausted: {completion_error!r}"
                )
                raise error from completion_error
            library_maintenance = self._try_library_maintenance()
            database_maintenance = self._try_current_only_maintenance(lease_duration)
            if (
                library_maintenance is LibraryMaintenanceOutcome.PROGRESSED
                or database_maintenance is VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
            ):
                return _ResidentCycleOutcome.MAINTENANCE_PROGRESSED
            return _ResidentCycleOutcome.IDLE
        except (VNextSourceChangedError, FilesystemSourceChangedError) as error:
            # A changed completion marker invalidates this attempt, not the
            # resident process. Heartbeat has stopped before releasing its
            # exact session and allowing bounded cleanup to make progress.
            try:
                session.complete()
                self._try_library_maintenance()
                self._try_current_only_maintenance(lease_duration)
            except BaseException as completion_error:
                error.add_note(
                    "The ingest session could not be completed after source "
                    f"mutation: {completion_error!r}"
                )
                raise error from completion_error
            self._event_logger("source changed during synchronization; retry pending")
            return _ResidentCycleOutcome.SOURCE_CHANGED
        except VNextSourceManifestMismatchError as error:
            # The mismatch has already abandoned the exact build.  Completing
            # after heartbeat shutdown makes it immediately eligible for
            # bounded maintenance instead of waiting for lease expiry, but the
            # source-authority failure remains fatal to this resident turn.
            try:
                session.complete()
                self._try_library_maintenance()
                self._try_current_only_maintenance(lease_duration)
            except BaseException as completion_error:
                error.add_note(
                    "The ingest session could not be completed after source "
                    f"synchronization failed: {completion_error!r}"
                )
            raise
        except Exception as error:
            if storage_capacity_error(error) is not None:
                self._wait_for_storage_capacity(error, operation="ingest")
                # Heartbeat and local render resources have already stopped.
                # Release the exact lease so bounded cleanup can reclaim stale
                # work, while retaining source facts for a later retry.
                try:
                    session.complete()
                except BaseException as completion_error:
                    error.add_note(
                        "Releasing the ingest lease after storage pressure also failed: "
                        f"{completion_error!r}"
                    )
                    if storage_capacity_error(completion_error) is None:
                        raise error from completion_error
                    # A full SQLite volume can prevent even lease release.
                    # Its durable expiry still fences the next claimant.
                return _ResidentCycleOutcome.IDLE
            diagnostic = format_artifact_failure(error)
            if diagnostic is not None:
                logger.error("%s", diagnostic)
            raise
        try:
            completion = session.complete()
        except Exception as error:
            if storage_capacity_error(error) is None:
                raise
            self._wait_for_storage_capacity(error, operation="ingest lease completion")
            return _ResidentCycleOutcome.IDLE
        self._capacity_waiting = False
        self._last_synchronization_result = outcome
        try:
            self._try_library_maintenance()
            self._try_current_only_maintenance(lease_duration)
        except Exception as error:
            if storage_capacity_error(error) is None:
                raise
            # Publication and lease completion are already durable. Report
            # cleanup pressure without claiming that this batch rolled back.
            self._wait_for_storage_capacity(error, operation="completed batch cleanup")
        self._event_logger(
            "vNext ingest session completed: "
            f"generation={completion.ingest_generation} "
            f"replayed={completion.replayed}"
        )
        return (
            _ResidentCycleOutcome.BATCH_PUBLISHED
            if outcome.deferred_gallery_count
            else _ResidentCycleOutcome.INGESTED
        )

    def _claim_after_maintenance(
        self,
        *,
        periodic_scan: bool,
        should_stop: Callable[[], bool] | None,
    ) -> VNextIngestSession | _ResidentCycleOutcome:
        if should_stop is not None and should_stop():
            return _ResidentCycleOutcome.IDLE
        self._progress_operation("inspect_storage")
        self._require_current_storage_identity()
        lease_duration = self._config.lease_seconds * 1_000_000
        # A previous bounded sweep may have lost its response, contended on the
        # EXCLUSIVE gate, or remained blocked by a live predecessor.  Retrying
        # once before every claim also provides progress while ingest is idle.
        library_maintenance = self._try_library_maintenance()
        self._progress_pending |= (
            library_maintenance is not LibraryMaintenanceOutcome.DONE
        )
        if library_maintenance is not LibraryMaintenanceOutcome.DONE:
            self._progress_wait_reason = "waiting_for_library_cleanup"
        if should_stop is not None and should_stop():
            return _ResidentCycleOutcome.IDLE
        if library_maintenance is LibraryMaintenanceOutcome.PROGRESSED:
            return _ResidentCycleOutcome.MAINTENANCE_PROGRESSED
        database_maintenance = self._try_current_only_maintenance(lease_duration)
        self._progress_pending |= (
            database_maintenance is not VNextCurrentOnlyMaintenanceOutcome.DONE
        )
        if database_maintenance is not VNextCurrentOnlyMaintenanceOutcome.DONE:
            self._progress_wait_reason = "waiting_for_catalog_cleanup"
        if should_stop is not None and should_stop():
            return _ResidentCycleOutcome.IDLE
        if database_maintenance is VNextCurrentOnlyMaintenanceOutcome.PROGRESSED:
            return _ResidentCycleOutcome.MAINTENANCE_PROGRESSED
        self._require_current_storage_identity()
        if should_stop is not None and should_stop():
            return _ResidentCycleOutcome.IDLE
        self._progress_operation("claim_ingest")
        claimed = self._facade.try_claim_ingest(periodic_scan, lease_duration)
        if (
            claimed is None
            and periodic_scan
            and (should_stop is None or not should_stop())
        ):
            # A due source scan requires a quiescent downloader. A pending
            # durable handoff must still be claimable while it blocks that path.
            claimed = self._facade.try_claim_ingest(False, lease_duration)
        return _ResidentCycleOutcome.IDLE if claimed is None else claimed

    def _wait_for_storage_capacity(
        self, error: BaseException, *, operation: str
    ) -> None:
        self._progress_pending = True
        self._progress_wait_reason = "waiting_for_disk_capacity"
        self._progress_operation("waiting_for_disk_capacity")
        if not self._capacity_waiting:
            diagnostic = format_artifact_failure(error)
            if diagnostic is not None:
                logger.error("%s", diagnostic)
            logger.warning("%s", storage_capacity_message(error, operation=operation))
        self._capacity_waiting = True

    def _require_current_storage_identity(self) -> None:
        """Fail before work if the configured library root changed identity."""

        provider = self._library_storage_identity
        if provider is None:
            return
        expected = self._bound_storage_identity
        if expected is None:
            self._storage_instance_ready = False
            raise RuntimeError("library storage instance has not been bound")
        try:
            observed = provider.ensure_storage_identity()
        except Exception as error:
            if storage_capacity_error(error) is None:
                self._storage_instance_ready = False
            raise
        if observed != expected:
            self._storage_instance_ready = False
            raise LibraryStorageIdentityMismatchError(
                "library storage instance changed after binding"
            )

    def _try_library_maintenance(
        self,
    ) -> LibraryMaintenanceOutcome | None:
        """Make one bounded ingest-owned presentation cleanup attempt."""

        self._progress_operation("library_cleanup")
        try:
            outcome = self._run_library_maintenance()
            if outcome is LibraryMaintenanceOutcome.PROGRESSED:
                work = self._current_progress()
                if work is not None:
                    work.advance("library_cleanup_steps")
            return outcome
        except LibraryStorageIdentityMismatchError, ScratchSafetyError:
            self._storage_instance_ready = False
            raise
        except Exception as error:
            if storage_capacity_error(error) is not None:
                raise
            logger.exception("library maintenance attempt failed")
            return None

    def _run_library_maintenance(self) -> LibraryMaintenanceOutcome:
        if self._temporary_cleanup is not None:
            self._temporary_cleanup()
        outcome = self._library_maintenance.maintain_cleanup()
        if not isinstance(outcome, LibraryMaintenanceOutcome):
            raise TypeError("library maintenance returned an invalid outcome")
        return outcome

    def _try_current_only_maintenance(
        self,
        lease_duration_microseconds: int | None = None,
    ) -> VNextCurrentOnlyMaintenanceOutcome | None:
        """Make one bounded current-only sweep attempt without blocking ingest."""

        duration = lease_duration_microseconds
        if duration is None:
            duration = self._config.lease_seconds * 1_000_000
        self._progress_operation("catalog_cleanup")
        try:
            outcome = self._facade.drain_current_only_maintenance(
                duration,
                artifact_release_adapters=self._artifact_release_adapters,
            )
            if outcome is VNextCurrentOnlyMaintenanceOutcome.PROGRESSED:
                work = self._current_progress()
                if work is not None:
                    work.advance("catalog_cleanup_steps")
            return outcome
        except LibraryStorageIdentityMismatchError, ScratchSafetyError:
            self._storage_instance_ready = False
            raise
        except Exception as error:
            if storage_capacity_error(error) is not None:
                raise
            # The ingest receipt is already durable when this is called after
            # completion.  Maintenance is response-loss safe and the resident
            # retries on the next poll, so a transient failure must not make a
            # completed ingest appear to have rolled back.
            logger.exception("current-only maintenance attempt failed")
            return None

    def _current_progress(self) -> ProgressWork | None:
        return None if self._progress is None else self._progress.current()

    def _begin_progress(self, phase: str, *, announce: bool) -> ProgressWork | None:
        if self._progress is None:
            return None
        work = self._progress.current()
        if work is None:
            work = self._progress.begin(phase, announce=announce)
        return work

    def _progress_operation(self, name: str) -> None:
        work = self._current_progress()
        if work is not None:
            work.operation(name)

    def _finish_progress(self, status: str, *, announce: bool = True) -> None:
        work = self._current_progress()
        if work is not None:
            work.finish(status, announce=announce)

    def _progress_stop_requested(self, should_stop: Callable[[], bool] | None) -> bool:
        try:
            return should_stop is not None and should_stop()
        except BaseException:
            self._finish_progress("failed")
            raise

    def run_forever(self, *, stop: Event | None = None) -> None:
        try:
            self._run_forever(stop=stop)
        except BaseException:
            self._finish_progress("failed")
            raise
        finally:
            self._scheduled_progress = False
            self._finish_progress("stopped", announce=False)

    def _run_forever(self, *, stop: Event | None = None) -> None:
        if not self._storage_instance_ready:
            raise RuntimeError(
                "CBZ-enabled resident must initialize its storage instance before "
                "polling"
            )
        stop_event = stop or Event()
        schedule = SourceScanSchedule(
            quiet_seconds=self._config.source_quiet_seconds,
            max_wait_seconds=self._config.source_max_wait_seconds,
            now=monotonic(),
        )
        with SourceChangeMonitor(
            probe=self._source_probe,
            schedule=schedule,
            interval_seconds=self._config.source_probe_interval_seconds,
            clock=monotonic,
        ) as monitor:

            def should_stop() -> bool:
                monitor.raise_if_failed()
                return stop_event.is_set()

            while not should_stop():
                deadline = schedule.next_scan_at()
                self._scheduled_progress = deadline is not None
                source_due = deadline is not None and monotonic() >= deadline
                ticket: SourceScanTicket | None = None

                def scan_started() -> None:
                    nonlocal ticket
                    ticket = schedule.start_scan(now=monotonic())

                outcome = self._process_cycle(
                    periodic_scan=source_due,
                    should_stop=should_stop,
                    on_scan_started=scan_started,
                )
                if ticket is not None:
                    schedule.finish_scan(
                        ticket,
                        now=monotonic(),
                        succeeded=outcome
                        in (
                            _ResidentCycleOutcome.INGESTED,
                            _ResidentCycleOutcome.BATCH_PUBLISHED,
                        ),
                        pending_batch=outcome is _ResidentCycleOutcome.BATCH_PUBLISHED,
                    )
                if outcome in (
                    _ResidentCycleOutcome.INGESTED,
                    _ResidentCycleOutcome.BATCH_PUBLISHED,
                    _ResidentCycleOutcome.MAINTENANCE_PROGRESSED,
                ):
                    continue
                deadline = schedule.next_scan_at()
                remaining = (
                    0.0 if deadline is None else max(0.0, deadline - monotonic())
                )
                stop_event.wait(
                    min(
                        self._config.poll_seconds,
                        remaining or self._config.poll_seconds,
                    )
                )
