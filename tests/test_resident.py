from __future__ import annotations

import errno
import logging
import sqlite3
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from threading import Event
from typing import cast

import pytest
from h2hdb import (
    ArtifactFailureContext,
    ArtifactReleaseAdapter,
    GalleryStagingCapacityError,
    SchemaEpochReport,
    VNextAnalysisAdvanceResult,
    VNextCurrentOnlyMaintenanceOutcome,
    VNextDatabaseAdminFacade,
    VNextIngestAdvanceResult,
    VNextIngestCompletionReceipt,
    VNextIngestFacade,
    VNextIngestPhase,
    VNextIngestSession,
    VNextIngestSourceReceipt,
    VNextSourceChangedError,
    VNextSourceManifestMismatchError,
)

import h2hdb_ingest.artifact_errors as artifact_errors_module
import h2hdb_ingest.resident as resident_module
from h2hdb_ingest import (
    ArtifactRenderPolicy,
    LibraryStorageIdentity,
    LibraryStorageIdentityMismatchError,
    ManagedFilesystemLibraryAdapter,
    ResidentConfig,
)
from h2hdb_ingest.filesystem import (
    FilesystemCompletionMarker,
    FilesystemSourceChangedError,
)
from h2hdb_ingest.library_identity import LibraryStorageIdentityProvider
from h2hdb_ingest.maintenance import (
    LibraryMaintenanceOutcome,
    _LibraryStagingSlotConflictError,
)
from h2hdb_ingest.progress import IngestProgress
from h2hdb_ingest.resident import IngestSynchronizer, ResidentIngestor
from h2hdb_ingest.scratch import ScratchSafetyError
from h2hdb_ingest.service import VNextIngestSynchronizationResult
from h2hdb_ingest.session import IngestSessionController
from h2hdb_ingest.source_schedule import SourceScanSchedule


@contextmanager
def _empty_source_probe(
    checkpoint: Callable[[], None],
) -> Iterator[Iterator[tuple[tuple[str, ...], FilesystemCompletionMarker]]]:
    checkpoint()
    yield iter(())


def _session() -> VNextIngestSession:
    return VNextIngestSession(
        gate_owner_token=b"g" * 16,
        gate_generation=1,
        gate_slot=0,
        gate_lease_expires_at=10_000_000,
        ingest_generation=2,
        ingest_owner_token=b"i" * 16,
        ingest_lease_expires_at=10_000_000,
        download_generation=None,
        handoff_owner_token=None,
        handoff_kind=None,
        consumed_at=None,
    )


def _synchronized(
    *, deferred_gallery_count: int = 0
) -> VNextIngestSynchronizationResult:
    return VNextIngestSynchronizationResult(
        source=VNextIngestSourceReceipt(b"b" * 16, 1, 1, True, False),
        analysis=VNextAnalysisAdvanceResult(
            b"a" * 16, b"snapshot_manifest", 1, True, True, False, b"m" * 32
        ),
        publication=VNextIngestAdvanceResult(
            VNextIngestPhase.FINALIZATION, 0, True, False
        ),
        deferred_gallery_count=deferred_gallery_count,
    )


class _Facade:
    def __init__(
        self,
        events: list[object],
        *,
        available: bool = True,
        maintenance_results: tuple[VNextCurrentOnlyMaintenanceOutcome, ...] = (
            VNextCurrentOnlyMaintenanceOutcome.DONE,
        ),
    ) -> None:
        self._events = events
        self._available = available
        self._maintenance_results = iter(maintenance_results)

    def try_claim_ingest(
        self,
        periodic: bool,
        lease_duration_microseconds: int,
    ) -> VNextIngestSession | None:
        self._events.append(("claim", periodic, lease_duration_microseconds))
        return _session() if self._available else None

    def complete_ingest(
        self,
        session: VNextIngestSession,
    ) -> VNextIngestCompletionReceipt:
        self._events.append(("complete", session.ingest_generation))
        return VNextIngestCompletionReceipt(
            session.ingest_generation,
            session.ingest_owner_token,
            10,
            session.download_generation,
            False,
        )

    def drain_current_only_maintenance(
        self,
        lease_duration_microseconds: int,
        *,
        artifact_release_adapters: object,
    ) -> VNextCurrentOnlyMaintenanceOutcome:
        del artifact_release_adapters
        self._events.append(("current-only", lease_duration_microseconds))
        return next(
            self._maintenance_results,
            VNextCurrentOnlyMaintenanceOutcome.DONE,
        )


class _Admin:
    def __init__(self, events: list[object]) -> None:
        self._events = events

    def check(self) -> SchemaEpochReport:
        self._events.append("check")
        return cast(SchemaEpochReport, object())

    def bind_storage_instance(self, storage_instance_uuid: bytes) -> object:
        self._events.append(("bind", storage_instance_uuid))
        return object()


class _StorageIdentity:
    def __init__(self, events: list[object], value: bytes) -> None:
        self._events = events
        self._identity = LibraryStorageIdentity(value)

    def ensure_storage_identity(self) -> LibraryStorageIdentity:
        self._events.append("identity")
        return self._identity


class _LibraryMaintenance:
    def __init__(
        self,
        results: tuple[LibraryMaintenanceOutcome, ...] = (
            LibraryMaintenanceOutcome.DONE,
        ),
    ) -> None:
        self._results = iter(results)
        self.calls = 0

    def maintain_cleanup(self) -> LibraryMaintenanceOutcome:
        self.calls += 1
        return next(self._results, LibraryMaintenanceOutcome.DONE)


class _FailingLibraryMaintenance(_LibraryMaintenance):
    def maintain_cleanup(self) -> LibraryMaintenanceOutcome:
        self.calls += 1
        raise RuntimeError("library layout unavailable")


class _InvalidLibraryMaintenance(_LibraryMaintenance):
    def maintain_cleanup(self) -> LibraryMaintenanceOutcome:
        self.calls += 1
        return cast(LibraryMaintenanceOutcome, object())


class _Service:
    def __init__(
        self, events: list[object], *, deferred_gallery_count: int = 0
    ) -> None:
        self._events = events
        self._deferred_gallery_count = deferred_gallery_count

    def synchronize_once(
        self,
        session: IngestSessionController,
        *,
        should_stop: Callable[[], bool] | None = None,
    ) -> VNextIngestSynchronizationResult:
        del should_stop
        self._events.append("synchronize")
        assert session.call(lambda _facade, receipt: receipt.ingest_generation) == 2
        return _synchronized(deferred_gallery_count=self._deferred_gallery_count)


class _ManifestMismatchService:
    def __init__(self, events: list[object]) -> None:
        self._events = events

    def synchronize_once(
        self,
        session: IngestSessionController,
        *,
        should_stop: Callable[[], bool] | None = None,
    ) -> VNextIngestSynchronizationResult:
        del session, should_stop
        self._events.append("synchronize")
        raise VNextSourceManifestMismatchError("source changed")


class _StagingCapacityService:
    def __init__(self, events: list[object]) -> None:
        self._events = events

    def synchronize_once(
        self,
        session: IngestSessionController,
        *,
        should_stop: Callable[[], bool] | None = None,
    ) -> VNextIngestSynchronizationResult:
        del session, should_stop
        self._events.append("synchronize")
        raise GalleryStagingCapacityError(1_500_000)


class _StagingSlotConflictService:
    def __init__(self, events: list[object]) -> None:
        self._events = events

    def synchronize_once(
        self,
        session: IngestSessionController,
        *,
        should_stop: Callable[[], bool] | None = None,
    ) -> VNextIngestSynchronizationResult:
        del session, should_stop
        self._events.append("synchronize")
        raise _LibraryStagingSlotConflictError("stale staging owner")


class _Heartbeat:
    def __init__(
        self,
        controller: IngestSessionController,
        *,
        interval_seconds: float,
    ) -> None:
        del controller, interval_seconds

    def __enter__(self) -> _Heartbeat:
        return self

    def __exit__(self, *args: object) -> None:
        del args

    def raise_if_failed(self) -> None:
        return None


def _resident(
    events: list[object],
    *,
    available: bool = True,
    maintenance_results: tuple[VNextCurrentOnlyMaintenanceOutcome, ...] = (
        VNextCurrentOnlyMaintenanceOutcome.DONE,
    ),
    library_maintenance: _LibraryMaintenance | None = None,
    library_storage_identity: LibraryStorageIdentityProvider | None = None,
    service: IngestSynchronizer | None = None,
    artifact_release_adapters: Mapping[bytes, ArtifactReleaseAdapter] | None = None,
    facade: _Facade | None = None,
    temporary_cleanup: Callable[[], object] | None = None,
    progress_log_interval_seconds: float = 3600,
) -> ResidentIngestor:
    facade = facade or _Facade(
        events,
        available=available,
        maintenance_results=maintenance_results,
    )
    return ResidentIngestor(
        source_probe=_empty_source_probe,
        temporary_cleanup=temporary_cleanup,
        service=service or _Service(events),
        facade=cast(VNextIngestFacade, facade),
        database_admin=cast(VNextDatabaseAdminFacade, _Admin(events)),
        library_storage_identity=library_storage_identity,
        library_maintenance=(library_maintenance or _LibraryMaintenance()),
        config=ResidentConfig(
            progress_log_interval_seconds=progress_log_interval_seconds,
            source_quiet_seconds=5,
            source_max_wait_seconds=60,
            poll_seconds=1,
            lease_seconds=10,
            heartbeat_seconds=5,
        ),
        database_type="sqlite",
        artifact_release_adapters=(
            {} if artifact_release_adapters is None else artifact_release_adapters
        ),
        event_logger=lambda message: events.append(("log", message)),
    )


def test_startup_only_checks_existing_epoch_and_processes_one_session(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger=resident_module.__name__)
    events: list[object] = []
    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _Heartbeat)
    resident = _resident(events)

    resident.initialize()
    assert resident.process_available(periodic_scan=True)

    assert events[:6] == [
        "check",
        ("current-only", 10_000_000),
        ("current-only", 10_000_000),
        ("claim", True, 10_000_000),
        "synchronize",
        (
            "log",
            "vNext ingest publication batch completed: deferred_galleries=0 "
            "waiting_galleries=0 known_galleries=1",
        ),
    ]
    assert events[6] == ("complete", 2)
    assert events[7] == ("current-only", 10_000_000)
    assert len(events) == 8
    assert [(record.levelno, record.getMessage()) for record in caplog.records] == [
        (logging.DEBUG, "vNext ingest session completed: generation=2 replayed=False")
    ]


def test_maintenance_preserves_last_completed_publication_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []
    resident = _resident(
        events,
        service=_Service(events, deferred_gallery_count=7),
        maintenance_results=(
            VNextCurrentOnlyMaintenanceOutcome.DONE,
            VNextCurrentOnlyMaintenanceOutcome.DONE,
            VNextCurrentOnlyMaintenanceOutcome.PROGRESSED,
        ),
    )
    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _Heartbeat)
    initial = resident.last_synchronization_result
    assert initial is None
    assert resident.deferred_gallery_count == 0
    assert resident.process_available(periodic_scan=True)
    published = resident.last_synchronization_result
    assert published is not None
    assert resident.deferred_gallery_count == 7
    assert resident.process_available(periodic_scan=True)
    assert resident.last_synchronization_result is published
    assert resident.deferred_gallery_count == 7
    assert events.count("synchronize") == 1


def test_postflight_runs_after_publication_and_before_heartbeat_and_lease_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []

    class _OrderedHeartbeat(_Heartbeat):
        def __exit__(self, *args: object) -> None:
            del args
            events.append("heartbeat-stop")

    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _OrderedHeartbeat)
    resident = _resident(events)
    assert resident.process_available(
        periodic_scan=True,
        postflight=lambda: events.append("postflight"),
    )
    assert events.index("synchronize") < events.index("postflight")
    assert events.index("postflight") < events.index("heartbeat-stop")
    assert events.index("heartbeat-stop") < events.index(("complete", 2))


@pytest.mark.parametrize("failure_type", [RuntimeError, VNextSourceChangedError])
def test_postflight_failure_stops_heartbeat_and_releases_lease_before_propagating(
    monkeypatch: pytest.MonkeyPatch,
    failure_type: type[RuntimeError],
) -> None:
    events: list[object] = []

    class _OrderedHeartbeat(_Heartbeat):
        def __exit__(self, *args: object) -> None:
            del args
            events.append("heartbeat-stop")

    def fail_postflight() -> None:
        events.append("postflight")
        raise failure_type("publication capture failed")

    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _OrderedHeartbeat)
    resident = _resident(events)
    with pytest.raises(failure_type, match="publication capture failed"):
        resident.process_available(periodic_scan=True, postflight=fail_postflight)
    assert events.index("postflight") < events.index("heartbeat-stop")
    assert events.index("heartbeat-stop") < events.index(("complete", 2))
    assert events.count(("complete", 2)) == 1
    assert resident.last_synchronization_result is None


def test_cbz_startup_binds_local_identity_before_any_maintenance() -> None:
    events: list[object] = []
    storage_uuid = bytes.fromhex("00000000000040008000000000000001")

    class _OrderedMaintenance(_LibraryMaintenance):
        def maintain_cleanup(self) -> LibraryMaintenanceOutcome:
            events.append("library-maintenance")
            return super().maintain_cleanup()

    resident = _resident(
        events,
        library_maintenance=_OrderedMaintenance(),
        library_storage_identity=_StorageIdentity(events, storage_uuid),
    )

    resident.initialize()

    assert events == [
        "check",
        "identity",
        ("bind", storage_uuid),
        "library-maintenance",
        ("current-only", 10_000_000),
    ]


def test_artifact_enabled_resident_requires_storage_identity() -> None:
    events: list[object] = []

    with pytest.raises(
        ValueError,
        match="artifact-enabled resident requires a library storage identity",
    ):
        _resident(
            events,
            artifact_release_adapters={
                b"adapter": cast(ArtifactReleaseAdapter, object())
            },
        )


def test_changed_storage_identity_stops_before_maintenance_or_claim() -> None:
    events: list[object] = []
    first_uuid = bytes.fromhex("00000000000040008000000000000001")
    replacement_uuid = bytes.fromhex("00000000000040008000000000000002")
    identities = iter(
        (
            LibraryStorageIdentity(first_uuid),
            LibraryStorageIdentity(replacement_uuid),
        )
    )

    class _ChangingStorageIdentity:
        def ensure_storage_identity(self) -> LibraryStorageIdentity:
            events.append("identity")
            return next(identities)

    maintenance = _LibraryMaintenance()
    resident = _resident(
        events,
        library_maintenance=maintenance,
        library_storage_identity=_ChangingStorageIdentity(),
    )
    resident.initialize()
    initialized_events = list(events)

    with pytest.raises(RuntimeError, match="changed after binding"):
        resident.process_available(periodic_scan=True)

    assert maintenance.calls == 1
    assert events == [*initialized_events, "identity"]
    with pytest.raises(RuntimeError, match="must initialize"):
        resident.process_available(periodic_scan=True)
    assert events == [*initialized_events, "identity"]


def test_root_swap_after_cycle_check_is_fatal_before_maintenance_or_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []
    source = tmp_path / "download"
    source.mkdir()
    root = tmp_path / "library"
    replacement = tmp_path / "replacement"
    detached = tmp_path / "detached-library"
    for candidate in (root, replacement):
        current = candidate / "current"
        (current / "acquisitions").mkdir(parents=True)
        (current / "artwork").mkdir()
        (candidate / ".h2hdb-coordination").mkdir()
    adapter = ManagedFilesystemLibraryAdapter(
        root,
        source_root=source,
        render_policy=ArtifactRenderPolicy(),
        page_render_workers=1,
    )
    real_ensure = adapter.ensure_storage_identity
    calls = 0

    def ensure_then_swap() -> LibraryStorageIdentity:
        nonlocal calls
        identity = real_ensure()
        calls += 1
        if calls == 2:
            root.rename(detached)
            replacement.rename(root)
        return identity

    monkeypatch.setattr(adapter, "ensure_storage_identity", ensure_then_swap)
    facade = _Facade(events, available=False)
    resident = ResidentIngestor(
        source_probe=_empty_source_probe,
        service=_Service(events),
        facade=cast(VNextIngestFacade, facade),
        database_admin=cast(VNextDatabaseAdminFacade, _Admin(events)),
        library_storage_identity=adapter,
        library_maintenance=adapter,
        config=ResidentConfig(
            source_quiet_seconds=5,
            source_max_wait_seconds=60,
            poll_seconds=1,
            lease_seconds=10,
            heartbeat_seconds=5,
        ),
        database_type="sqlite",
        artifact_release_adapters={adapter.adapter_id: adapter},
        event_logger=lambda message: events.append(("log", message)),
    )
    resident.initialize()
    initialized_events = list(events)

    with pytest.raises(
        LibraryStorageIdentityMismatchError,
        match="root changed after its storage identity was pinned",
    ):
        resident.process_available(periodic_scan=True)

    assert calls == 2
    assert events == initialized_events
    assert not (root / ".h2hdb-state").exists()
    assert not tuple(root.rglob("*.cbz"))
    with pytest.raises(RuntimeError, match="must initialize"):
        resident.process_available(periodic_scan=True)
    assert events == initialized_events


def test_storage_mismatch_from_current_only_maintenance_is_not_best_effort() -> None:
    events: list[object] = []
    storage_uuid = bytes.fromhex("00000000000040008000000000000001")

    class _MismatchDuringMaintenanceFacade(_Facade):
        def __init__(self) -> None:
            super().__init__(events)
            self._attempt = 0

        def drain_current_only_maintenance(
            self,
            lease_duration_microseconds: int,
            *,
            artifact_release_adapters: object,
        ) -> VNextCurrentOnlyMaintenanceOutcome:
            self._attempt += 1
            if self._attempt == 2:
                raise LibraryStorageIdentityMismatchError("replacement root")
            return super().drain_current_only_maintenance(
                lease_duration_microseconds,
                artifact_release_adapters=artifact_release_adapters,
            )

    facade = _MismatchDuringMaintenanceFacade()
    resident = ResidentIngestor(
        source_probe=_empty_source_probe,
        service=_Service(events),
        facade=cast(VNextIngestFacade, facade),
        database_admin=cast(VNextDatabaseAdminFacade, _Admin(events)),
        library_storage_identity=_StorageIdentity(events, storage_uuid),
        library_maintenance=_LibraryMaintenance(),
        config=ResidentConfig(
            source_quiet_seconds=5,
            source_max_wait_seconds=60,
            poll_seconds=1,
            lease_seconds=10,
            heartbeat_seconds=5,
        ),
        database_type="sqlite",
        artifact_release_adapters={},
        event_logger=lambda message: events.append(("log", message)),
    )
    resident.initialize()

    with pytest.raises(LibraryStorageIdentityMismatchError, match="replacement root"):
        resident.process_available(periodic_scan=True)

    assert not any(event[0] == "claim" for event in events if isinstance(event, tuple))
    with pytest.raises(RuntimeError, match="must initialize"):
        resident.process_available(periodic_scan=True)


def test_storage_identity_is_rechecked_after_maintenance_before_claim() -> None:
    events: list[object] = []
    first_uuid = bytes.fromhex("00000000000040008000000000000001")
    replacement_uuid = bytes.fromhex("00000000000040008000000000000002")
    identities = iter(
        (
            LibraryStorageIdentity(first_uuid),
            LibraryStorageIdentity(first_uuid),
            LibraryStorageIdentity(replacement_uuid),
        )
    )

    class _ReplacementAfterMaintenance:
        def ensure_storage_identity(self) -> LibraryStorageIdentity:
            events.append("identity")
            return next(identities)

    resident = _resident(
        events,
        library_storage_identity=_ReplacementAfterMaintenance(),
    )
    resident.initialize()

    with pytest.raises(
        LibraryStorageIdentityMismatchError,
        match="changed after binding",
    ):
        resident.process_available(periodic_scan=True)

    assert not any(event[0] == "claim" for event in events if isinstance(event, tuple))


def test_storage_binding_mismatch_prevents_maintenance_and_claim() -> None:
    events: list[object] = []
    storage_uuid = bytes.fromhex("00000000000040008000000000000001")
    maintenance = _LibraryMaintenance()

    class _MismatchAdmin(_Admin):
        def bind_storage_instance(self, storage_instance_uuid: bytes) -> object:
            super().bind_storage_instance(storage_instance_uuid)
            raise RuntimeError("different storage instance")

    resident = _resident(
        events,
        library_maintenance=maintenance,
        library_storage_identity=_StorageIdentity(events, storage_uuid),
    )
    resident._database_admin = cast(VNextDatabaseAdminFacade, _MismatchAdmin(events))

    with pytest.raises(RuntimeError, match="different storage instance"):
        resident.initialize()
    with pytest.raises(RuntimeError, match="must initialize"):
        resident.process_available(periodic_scan=True)

    assert maintenance.calls == 0
    assert events == ["check", "identity", ("bind", storage_uuid)]


def test_startup_propagates_library_layout_failure() -> None:
    events: list[object] = []
    library_maintenance = _FailingLibraryMaintenance()
    resident = _resident(events, library_maintenance=library_maintenance)

    with pytest.raises(RuntimeError, match="library layout unavailable"):
        resident.initialize()

    assert library_maintenance.calls == 1
    assert events == ["check"]


def test_startup_rejects_invalid_library_maintenance_outcome() -> None:
    events: list[object] = []
    library_maintenance = _InvalidLibraryMaintenance()
    resident = _resident(events, library_maintenance=library_maintenance)

    with pytest.raises(TypeError, match="library maintenance returned an invalid"):
        resident.initialize()

    assert library_maintenance.calls == 1
    assert events == ["check"]


def test_ordinary_claim_contention_is_not_an_error() -> None:
    events: list[object] = []
    resident = _resident(events, available=False)

    assert not resident.process_available(periodic_scan=False)
    assert events == [
        ("current-only", 10_000_000),
        ("claim", False, 10_000_000),
    ]


def test_poll_keeps_library_maintenance_failure_best_effort() -> None:
    events: list[object] = []
    library_maintenance = _FailingLibraryMaintenance()
    resident = _resident(
        events,
        available=False,
        library_maintenance=library_maintenance,
    )

    assert not resident.process_available(periodic_scan=False)

    assert library_maintenance.calls == 1
    assert events == [
        ("current-only", 10_000_000),
        ("claim", False, 10_000_000),
    ]


@pytest.mark.parametrize("operation", ["library_cleanup", "catalog_cleanup"])
def test_maintenance_errors_are_summarized_without_skipping_retries(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    operation: str,
) -> None:
    events: list[object] = []
    now = [0.0]
    failure: str | None = "first error"
    attempts = 0

    def attempt() -> None:
        nonlocal attempts
        attempts += 1
        if failure is not None:
            raise OSError(errno.EIO, failure, "/library/damaged-entry")

    class FailingLibrary(_LibraryMaintenance):
        def maintain_cleanup(self) -> LibraryMaintenanceOutcome:
            if operation == "library_cleanup":
                attempt()
            return super().maintain_cleanup()

    class FailingFacade(_Facade):
        def drain_current_only_maintenance(
            self,
            lease_duration_microseconds: int,
            *,
            artifact_release_adapters: object,
        ) -> VNextCurrentOnlyMaintenanceOutcome:
            if operation == "catalog_cleanup":
                attempt()
            return super().drain_current_only_maintenance(
                lease_duration_microseconds,
                artifact_release_adapters=artifact_release_adapters,
            )

    monkeypatch.setattr(resident_module, "monotonic", lambda: now[0])
    caplog.set_level(logging.INFO, logger=resident_module.__name__)
    resident = _resident(
        events,
        facade=FailingFacade(events, available=False),
        library_maintenance=FailingLibrary(),
        progress_log_interval_seconds=30,
    )
    assert not resident.process_available(periodic_scan=False)
    assert len(caplog.records) == 1
    first = caplog.records[0]
    assert first.levelno == logging.ERROR
    assert f"operation={operation} suppressed_repeats=0" in first.getMessage()
    assert first.exc_info is not None
    assert "/library/damaged-entry" in str(first.exc_info[1])
    for second in range(1, 20):
        now[0] = float(second)
        assert not resident.process_available(periodic_scan=False)
    now[0] = 29.999
    assert not resident.process_available(periodic_scan=False)
    assert len(caplog.records) == 1
    assert attempts == 21
    assert events.count(("claim", False, 10_000_000)) == attempts

    now[0] = 30.0
    assert not resident.process_available(periodic_scan=False)
    assert len(caplog.records) == 2
    assert "suppressed_repeats=20" in caplog.records[-1].getMessage()
    assert caplog.records[-1].exc_info is not None

    now[0] = 31.0
    failure = "different error"
    assert not resident.process_available(periodic_scan=False)
    assert len(caplog.records) == 3
    assert "different error" in str(caplog.records[-1].exc_info)
    now[0] = 32.0
    assert not resident.process_available(periodic_scan=False)
    assert len(caplog.records) == 3

    failure = None
    now[0] = 33.0
    assert not resident.process_available(periodic_scan=False)
    assert len(caplog.records) == 4
    recovered = caplog.records[-1]
    assert recovered.levelno == logging.INFO
    assert recovered.getMessage() == (
        f"Operation recovered: operation={operation} suppressed_repeats=21"
    )
    assert not resident.process_available(periodic_scan=False)
    assert len(caplog.records) == 4
    failure = "different error"
    assert not resident.process_available(periodic_scan=False)
    assert len(caplog.records) == 5
    assert "suppressed_repeats=0" in caplog.records[-1].getMessage()
    assert attempts == 27
    assert events.count(("claim", False, 10_000_000)) == attempts


def test_maintenance_failure_diagnostics_are_independent_per_operation(
    caplog: pytest.LogCaptureFixture,
) -> None:
    events: list[object] = []

    class FailingFacade(_Facade):
        def drain_current_only_maintenance(
            self,
            lease_duration_microseconds: int,
            *,
            artifact_release_adapters: object,
        ) -> VNextCurrentOnlyMaintenanceOutcome:
            del lease_duration_microseconds, artifact_release_adapters
            raise RuntimeError("library layout unavailable")

    resident = _resident(
        events,
        facade=FailingFacade(events, available=False),
        library_maintenance=_FailingLibraryMaintenance(),
    )
    for _ in range(20):
        assert not resident.process_available(periodic_scan=False)
    assert len(caplog.records) == 2
    assert "operation=library_cleanup" in caplog.records[0].getMessage()
    assert "operation=catalog_cleanup" in caplog.records[1].getMessage()
    assert events.count(("claim", False, 10_000_000)) == 20


def test_requested_stop_skips_maintenance_and_does_not_claim_new_work() -> None:
    events: list[object] = []
    library_maintenance = _LibraryMaintenance()
    resident = _resident(events, library_maintenance=library_maintenance)

    assert not resident.process_available(
        periodic_scan=True,
        should_stop=lambda: True,
    )

    assert library_maintenance.calls == 0
    assert events == []


def test_maintenance_progress_skips_ingest_claim() -> None:
    events: list[object] = []
    resident = _resident(
        events,
        maintenance_results=(VNextCurrentOnlyMaintenanceOutcome.PROGRESSED,),
    )

    assert resident.process_available(periodic_scan=True)
    assert events == [("current-only", 10_000_000)]


def test_library_maintenance_progress_skips_database_and_ingest() -> None:
    events: list[object] = []
    library_maintenance = _LibraryMaintenance((LibraryMaintenanceOutcome.PROGRESSED,))
    resident = _resident(
        events,
        library_maintenance=library_maintenance,
    )

    assert resident.process_available(periodic_scan=True)
    assert events == []


def test_blocked_library_maintenance_uses_ordinary_claim_poll() -> None:
    events: list[object] = []
    library_maintenance = _LibraryMaintenance((LibraryMaintenanceOutcome.BLOCKED,))
    resident = _resident(
        events,
        available=False,
        library_maintenance=library_maintenance,
    )

    assert not resident.process_available(periodic_scan=False)
    assert events == [
        ("current-only", 10_000_000),
        ("claim", False, 10_000_000),
    ]


def test_failed_preflight_completes_claim_without_running_service() -> None:
    events: list[object] = []
    resident = _resident(events)

    with pytest.raises(RuntimeError, match="not fresh"):
        resident.process_available(
            periodic_scan=True,
            preflight=lambda: (_ for _ in ()).throw(RuntimeError("not fresh")),
        )

    assert events == [
        ("current-only", 10_000_000),
        ("claim", True, 10_000_000),
        ("complete", 2),
        ("current-only", 10_000_000),
    ]


def test_source_manifest_mismatch_completes_claim_after_heartbeat_stops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []

    class _OrderedHeartbeat(_Heartbeat):
        def __enter__(self) -> _OrderedHeartbeat:
            events.append("heartbeat-start")
            return self

        def __exit__(self, *args: object) -> None:
            del args
            events.append("heartbeat-stop")

    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _OrderedHeartbeat)
    resident = _resident(events, service=_ManifestMismatchService(events))

    with pytest.raises(VNextSourceManifestMismatchError, match="source changed"):
        resident.process_available(periodic_scan=True)

    assert events == [
        ("current-only", 10_000_000),
        ("claim", True, 10_000_000),
        "heartbeat-start",
        "synchronize",
        "heartbeat-stop",
        ("complete", 2),
        ("current-only", 10_000_000),
    ]


def test_staging_capacity_without_cleanup_progress_completes_claim_and_idles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []

    class _OrderedHeartbeat(_Heartbeat):
        def __enter__(self) -> _OrderedHeartbeat:
            events.append("heartbeat-start")
            return self

        def __exit__(self, *args: object) -> None:
            del args
            events.append("heartbeat-stop")

    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _OrderedHeartbeat)
    resident = _resident(events, service=_StagingCapacityService(events))

    assert not resident.process_available(periodic_scan=True)

    assert events == [
        ("current-only", 10_000_000),
        ("claim", True, 10_000_000),
        "heartbeat-start",
        "synchronize",
        "heartbeat-stop",
        ("complete", 2),
        ("current-only", 10_000_000),
    ]


def test_staging_capacity_reports_core_cleanup_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []
    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _Heartbeat)
    resident = _resident(
        events,
        service=_StagingCapacityService(events),
        maintenance_results=(
            VNextCurrentOnlyMaintenanceOutcome.DONE,
            VNextCurrentOnlyMaintenanceOutcome.PROGRESSED,
        ),
    )

    assert resident.process_available(periodic_scan=True)
    assert events == [
        ("current-only", 10_000_000),
        ("claim", True, 10_000_000),
        "synchronize",
        ("complete", 2),
        ("current-only", 10_000_000),
    ]


def test_staging_capacity_reports_library_cleanup_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []
    library_maintenance = _LibraryMaintenance(
        (
            LibraryMaintenanceOutcome.DONE,
            LibraryMaintenanceOutcome.PROGRESSED,
        )
    )
    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _Heartbeat)
    resident = _resident(
        events,
        service=_StagingCapacityService(events),
        library_maintenance=library_maintenance,
    )

    assert resident.process_available(periodic_scan=True)
    assert library_maintenance.calls == 2
    assert events == [
        ("current-only", 10_000_000),
        ("claim", True, 10_000_000),
        "synchronize",
        ("complete", 2),
        ("current-only", 10_000_000),
    ]


def test_staging_slot_conflict_waits_for_contended_cleanup_instead_of_failing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []
    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _Heartbeat)
    resident = _resident(
        events,
        service=_StagingSlotConflictService(events),
        maintenance_results=(
            VNextCurrentOnlyMaintenanceOutcome.DONE,
            VNextCurrentOnlyMaintenanceOutcome.CONTENDED,
        ),
    )

    assert not resident.process_available(periodic_scan=True)
    assert events == [
        ("current-only", 10_000_000),
        ("claim", True, 10_000_000),
        "synchronize",
        ("complete", 2),
        ("current-only", 10_000_000),
    ]


def test_staging_slot_conflict_fails_when_core_has_no_matching_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []
    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _Heartbeat)
    resident = _resident(
        events,
        service=_StagingSlotConflictService(events),
        maintenance_results=(
            VNextCurrentOnlyMaintenanceOutcome.DONE,
            VNextCurrentOnlyMaintenanceOutcome.DONE,
        ),
    )

    with pytest.raises(_LibraryStagingSlotConflictError, match="stale staging owner"):
        resident.process_available(periodic_scan=True)

    assert events == [
        ("current-only", 10_000_000),
        ("claim", True, 10_000_000),
        "synchronize",
        ("complete", 2),
        ("current-only", 10_000_000),
    ]


def test_staging_capacity_preserves_completion_failure_as_note(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []

    class _CompletionFailureFacade(_Facade):
        def complete_ingest(
            self,
            session: VNextIngestSession,
        ) -> VNextIngestCompletionReceipt:
            del session
            raise RuntimeError("completion failed")

    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _Heartbeat)
    facade = _CompletionFailureFacade(events)
    resident = ResidentIngestor(
        source_probe=_empty_source_probe,
        service=_StagingCapacityService(events),
        facade=cast(VNextIngestFacade, facade),
        database_admin=cast(VNextDatabaseAdminFacade, _Admin(events)),
        library_storage_identity=None,
        library_maintenance=_LibraryMaintenance(),
        config=ResidentConfig(
            source_quiet_seconds=5,
            source_max_wait_seconds=60,
            poll_seconds=1,
            lease_seconds=10,
            heartbeat_seconds=5,
        ),
        database_type="sqlite",
        artifact_release_adapters={},
        event_logger=lambda message: events.append(("log", message)),
    )

    with pytest.raises(GalleryStagingCapacityError) as caught:
        resident.process_available(periodic_scan=True)

    assert caught.value.__notes__ == [
        "The ingest session could not be completed after gallery staging "
        "capacity was exhausted: RuntimeError('completion failed')"
    ]
    assert events == [
        ("current-only", 10_000_000),
        ("claim", True, 10_000_000),
        "synchronize",
    ]


def test_startup_contention_is_retried_once_on_the_next_idle_poll() -> None:
    events: list[object] = []
    resident = _resident(
        events,
        available=False,
        maintenance_results=(
            VNextCurrentOnlyMaintenanceOutcome.CONTENDED,
            VNextCurrentOnlyMaintenanceOutcome.DONE,
        ),
    )

    resident.initialize()
    assert not resident.process_available(periodic_scan=False)

    assert events == [
        "check",
        ("current-only", 10_000_000),
        ("current-only", 10_000_000),
        ("claim", False, 10_000_000),
    ]


def test_maintenance_failure_does_not_undo_completed_ingest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []

    class _FailAfterCompletionFacade(_Facade):
        def __init__(self) -> None:
            super().__init__(events)
            self._attempt = 0

        def drain_current_only_maintenance(
            self,
            lease_duration_microseconds: int,
            *,
            artifact_release_adapters: object,
        ) -> VNextCurrentOnlyMaintenanceOutcome:
            self._attempt += 1
            if self._attempt == 2:
                raise RuntimeError("maintenance unavailable")
            return super().drain_current_only_maintenance(
                lease_duration_microseconds,
                artifact_release_adapters=artifact_release_adapters,
            )

    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _Heartbeat)
    resident = ResidentIngestor(
        source_probe=_empty_source_probe,
        service=_Service(events),
        facade=cast(VNextIngestFacade, _FailAfterCompletionFacade()),
        database_admin=cast(VNextDatabaseAdminFacade, _Admin(events)),
        library_storage_identity=None,
        library_maintenance=_LibraryMaintenance(),
        config=ResidentConfig(
            source_quiet_seconds=5,
            source_max_wait_seconds=60,
            poll_seconds=1,
            lease_seconds=10,
            heartbeat_seconds=5,
        ),
        database_type="sqlite",
        artifact_release_adapters={},
        event_logger=lambda message: events.append(("log", message)),
    )

    assert resident.process_available(periodic_scan=True)
    assert ("complete", 2) in events
    assert (
        "log",
        "vNext ingest publication batch completed: deferred_galleries=0 "
        "waiting_galleries=0 known_galleries=1",
    ) in events


def test_run_forever_retries_progress_immediately_without_resetting_source_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []
    resident = _resident(
        events,
        available=False,
        maintenance_results=(
            VNextCurrentOnlyMaintenanceOutcome.PROGRESSED,
            VNextCurrentOnlyMaintenanceOutcome.DONE,
        ),
    )

    class _Stop:
        def __init__(self) -> None:
            self.waited = False

        def is_set(self) -> bool:
            return self.waited

        def wait(self, timeout: float) -> bool:
            events.append(("wait", timeout))
            self.waited = True
            return True

    monkeypatch.setattr(resident_module, "monotonic", lambda: 100.0)
    stop = _Stop()
    resident.run_forever(stop=cast(Event, stop))

    assert events == [
        ("current-only", 10_000_000),
        ("current-only", 10_000_000),
        ("claim", True, 10_000_000),
        ("claim", False, 10_000_000),
        ("wait", 1.0),
    ]


def test_run_forever_waits_instead_of_exiting_on_staging_capacity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []
    resident = _resident(events, service=_StagingCapacityService(events))

    class _Stop:
        def __init__(self) -> None:
            self.waited = False

        def is_set(self) -> bool:
            return self.waited

        def wait(self, timeout: float) -> bool:
            events.append(("wait", timeout))
            self.waited = True
            return True

    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _Heartbeat)
    monkeypatch.setattr(resident_module, "monotonic", lambda: 100.0)
    stop = _Stop()
    resident.run_forever(stop=cast(Event, stop))

    assert events == [
        ("current-only", 10_000_000),
        ("claim", True, 10_000_000),
        "synchronize",
        ("complete", 2),
        ("current-only", 10_000_000),
        ("wait", 1.0),
    ]


def test_run_forever_publishes_pending_batches_without_waiting_for_source_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []
    deferred = iter((2, 1, 0))
    scans: list[float] = []
    now = 100.0

    class _PeriodicFacade(_Facade):
        def try_claim_ingest(
            self, periodic: bool, lease_duration_microseconds: int
        ) -> VNextIngestSession | None:
            claimed = super().try_claim_ingest(periodic, lease_duration_microseconds)
            return claimed if periodic else None

    class _BatchService(_Service):
        def synchronize_once(
            self,
            session: IngestSessionController,
            *,
            should_stop: Callable[[], bool] | None = None,
        ) -> VNextIngestSynchronizationResult:
            del session, should_stop
            scans.append(now)
            return _synchronized(deferred_gallery_count=next(deferred))

    class _Stop:
        waited = False

        def is_set(self) -> bool:
            return self.waited

        def wait(self, timeout: float) -> bool:
            assert len(scans) == 3
            events.append(("wait", timeout))
            self.waited = True
            return True

    facade = _PeriodicFacade(
        events,
        maintenance_results=(
            VNextCurrentOnlyMaintenanceOutcome.DONE,
            VNextCurrentOnlyMaintenanceOutcome.DONE,
            VNextCurrentOnlyMaintenanceOutcome.PROGRESSED,
        ),
    )
    resident = _resident(events, facade=facade, service=_BatchService(events))
    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _Heartbeat)
    monkeypatch.setattr(resident_module, "monotonic", lambda: now)
    resident.run_forever(stop=cast(Event, _Stop()))

    assert scans == [100.0, 100.0, 100.0]
    assert [
        event for event in events if isinstance(event, tuple) and event[0] == "claim"
    ] == [
        ("claim", True, 10_000_000),
        ("claim", True, 10_000_000),
        ("claim", True, 10_000_000),
        ("claim", False, 10_000_000),
    ]
    assert events.count(("complete", 2)) == 3
    assert events[-1] == ("wait", 1.0)


def test_run_forever_immediately_drains_library_progress_then_waits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []
    library_maintenance = _LibraryMaintenance(
        (
            LibraryMaintenanceOutcome.PROGRESSED,
            LibraryMaintenanceOutcome.DONE,
        )
    )
    resident = _resident(
        events,
        available=False,
        library_maintenance=library_maintenance,
    )

    class _Stop:
        def __init__(self) -> None:
            self.waited = False

        def is_set(self) -> bool:
            return self.waited

        def wait(self, timeout: float) -> bool:
            events.append(("wait", timeout))
            self.waited = True
            return True

    monkeypatch.setattr(resident_module, "monotonic", lambda: 100.0)
    stop = _Stop()
    resident.run_forever(stop=cast(Event, stop))

    assert library_maintenance.calls == 2
    assert events == [
        ("current-only", 10_000_000),
        ("claim", True, 10_000_000),
        ("claim", False, 10_000_000),
        ("wait", 1.0),
    ]


def test_due_source_scan_still_consumes_pending_downloader_handoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []

    class _HandoffFacade(_Facade):
        def try_claim_ingest(
            self, periodic: bool, lease_duration_microseconds: int
        ) -> VNextIngestSession | None:
            claimed = super().try_claim_ingest(periodic, lease_duration_microseconds)
            return None if periodic else claimed

    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _Heartbeat)
    resident = _resident(events, facade=_HandoffFacade(events))
    assert resident.process_available(periodic_scan=True)
    assert events[:4] == [
        ("current-only", 10_000_000),
        ("claim", True, 10_000_000),
        ("claim", False, 10_000_000),
        "synchronize",
    ]
    assert ("complete", 2) in events


def test_stop_between_due_claim_and_handoff_fallback_prevents_new_claim() -> None:
    events: list[object] = []
    stopped = Event()

    class _StoppingFacade(_Facade):
        def try_claim_ingest(
            self, periodic: bool, lease_duration_microseconds: int
        ) -> VNextIngestSession | None:
            super().try_claim_ingest(periodic, lease_duration_microseconds)
            stopped.set()
            return None

    resident = _resident(events, facade=_StoppingFacade(events))
    assert not resident.process_available(
        periodic_scan=True, should_stop=stopped.is_set
    )
    assert events == [
        ("current-only", 10_000_000),
        ("claim", True, 10_000_000),
    ]


@pytest.mark.parametrize(
    "source_error", [VNextSourceChangedError, FilesystemSourceChangedError]
)
def test_transient_source_mutation_completes_after_heartbeat_shutdown(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    source_error: type[RuntimeError],
) -> None:
    events: list[object] = []

    class _ChangingService(_Service):
        def synchronize_once(
            self,
            session: IngestSessionController,
            *,
            should_stop: Callable[[], bool] | None = None,
        ) -> VNextIngestSynchronizationResult:
            del session, should_stop
            events.append("synchronize")
            raise source_error("marker changed")

    class _OrderedHeartbeat(_Heartbeat):
        def __exit__(self, *args: object) -> None:
            del args
            events.append("heartbeat-stop")

    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _OrderedHeartbeat)
    resident = _resident(events, service=_ChangingService(events))
    caplog.set_level(logging.DEBUG, logger=resident_module.__name__)
    assert not resident.process_available(periodic_scan=True)
    assert events == [
        ("current-only", 10_000_000),
        ("claim", True, 10_000_000),
        "synchronize",
        "heartbeat-stop",
        ("complete", 2),
        ("current-only", 10_000_000),
    ]
    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.levelno == logging.WARNING
    assert "Ingest work will be retried" in record.getMessage()
    assert f'error_type="{source_error.__name__}"' in record.getMessage()
    assert 'reason="marker changed"' in record.getMessage()


@pytest.mark.parametrize("failure_stage", ["complete", "cleanup"])
def test_source_retry_does_not_announce_success_when_completion_or_cleanup_fails(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    failure_stage: str,
) -> None:
    events: list[object] = []
    original = FilesystemSourceChangedError("incomplete marker at /source/1001")

    class _ChangingService(_Service):
        def synchronize_once(
            self,
            session: IngestSessionController,
            *,
            should_stop: Callable[[], bool] | None = None,
        ) -> VNextIngestSynchronizationResult:
            del session, should_stop
            raise original

    class _FailingFacade(_Facade):
        def complete_ingest(
            self, session: VNextIngestSession
        ) -> VNextIngestCompletionReceipt:
            if failure_stage == "complete":
                raise RuntimeError("completion failed")
            return super().complete_ingest(session)

        def drain_current_only_maintenance(
            self,
            lease_duration_microseconds: int,
            *,
            artifact_release_adapters: object,
        ) -> VNextCurrentOnlyMaintenanceOutcome:
            if failure_stage == "cleanup" and ("complete", 2) in events:
                raise ScratchSafetyError("cleanup authority changed")
            return super().drain_current_only_maintenance(
                lease_duration_microseconds,
                artifact_release_adapters=artifact_release_adapters,
            )

    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _Heartbeat)
    resident = _resident(
        events, facade=_FailingFacade(events), service=_ChangingService(events)
    )
    with pytest.raises(FilesystemSourceChangedError) as caught:
        resident.process_available(periodic_scan=True)
    assert caught.value is original
    assert original.__cause__ is not None
    assert any("could not be completed" in note for note in original.__notes__)
    assert not any(
        "will be retried" in record.getMessage() for record in caplog.records
    )


def test_retry_warning_delivery_failure_preserves_retry_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []

    class _ChangingService(_Service):
        def synchronize_once(
            self,
            session: IngestSessionController,
            *,
            should_stop: Callable[[], bool] | None = None,
        ) -> VNextIngestSynchronizationResult:
            del session, should_stop
            raise FilesystemSourceChangedError("incomplete marker")

    def unavailable_logger(*_args: object, **_kwargs: object) -> None:
        raise OSError("log sink unavailable")

    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _Heartbeat)
    monkeypatch.setattr(resident_module.logger, "warning", unavailable_logger)
    resident = _resident(events, service=_ChangingService(events))
    assert not resident.process_available(periodic_scan=True)
    assert ("complete", 2) in events


@pytest.mark.parametrize(
    "unavailable_attribute",
    ["_h2hdb_ingest_failure_snapshot", "__notes__", "__cause__"],
)
def test_unavailable_retry_metadata_does_not_prevent_session_completion(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    unavailable_attribute: str,
) -> None:
    events: list[object] = []

    class _MetadataError(FilesystemSourceChangedError):
        def __getattribute__(self, name: str) -> object:
            if name == unavailable_attribute:
                raise ValueError("optional diagnostic metadata unavailable")
            return super().__getattribute__(name)

    class _ChangingService(_Service):
        def synchronize_once(
            self,
            session: IngestSessionController,
            *,
            should_stop: Callable[[], bool] | None = None,
        ) -> VNextIngestSynchronizationResult:
            del session, should_stop
            raise _MetadataError("incomplete /source/1001/galleryinfo.txt")

    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _Heartbeat)
    resident = _resident(events, service=_ChangingService(events))
    resident._progress = IngestProgress(lambda _: None)
    assert not resident.process_available(periodic_scan=True)
    assert ("complete", 2) in events
    assert len(caplog.records) == 1
    warning = caplog.records[0]
    assert warning.levelno == logging.WARNING
    assert 'error_type="_MetadataError"' in warning.getMessage()
    assert 'reason="incomplete /source/1001/galleryinfo.txt"' in warning.getMessage()
    assert "diagnostic_context=unavailable" in warning.getMessage()


def test_run_forever_retries_mutation_after_quiet_period_without_process_restart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []
    now = [100.0]
    scans: list[float] = []

    class _Stop:
        def is_set(self) -> bool:
            return len(scans) == 2

        def wait(self, timeout: float) -> bool:
            now[0] += timeout
            assert now[0] <= 106, "transient source retry missed its quiet deadline"
            return self.is_set()

    class _DueOnlyFacade(_Facade):
        def try_claim_ingest(
            self, periodic: bool, lease_duration_microseconds: int
        ) -> VNextIngestSession | None:
            claimed = super().try_claim_ingest(periodic, lease_duration_microseconds)
            return claimed if periodic else None

    class _ChangingOnceService(_Service):
        def synchronize_once(
            self,
            session: IngestSessionController,
            *,
            should_stop: Callable[[], bool] | None = None,
        ) -> VNextIngestSynchronizationResult:
            del session, should_stop
            scans.append(now[0])
            if len(scans) == 1:
                raise VNextSourceChangedError("marker changed")
            return _synchronized()

    monkeypatch.setattr(resident_module, "monotonic", lambda: now[0])
    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _Heartbeat)
    resident = _resident(
        events,
        facade=_DueOnlyFacade(events),
        service=_ChangingOnceService(events),
    )
    resident.run_forever(stop=cast(Event, _Stop()))
    assert scans == [100.0, 105.0]
    assert events.count(("complete", 2)) == 2


def test_run_forever_preserves_scan_time_change_and_then_stays_clean(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []
    now = [100.0]
    scans: list[float] = []
    schedules: list[SourceScanSchedule] = []

    def make_schedule(
        *, quiet_seconds: float, max_wait_seconds: float, now: float
    ) -> SourceScanSchedule:
        schedule = SourceScanSchedule(
            quiet_seconds=quiet_seconds, max_wait_seconds=max_wait_seconds, now=now
        )
        schedules.append(schedule)
        return schedule

    class _Stop:
        def is_set(self) -> bool:
            return now[0] >= 500

        def wait(self, timeout: float) -> bool:
            del timeout
            now[0] += 100
            return self.is_set()

    class _DueOnlyFacade(_Facade):
        def try_claim_ingest(
            self, periodic: bool, lease_duration_microseconds: int
        ) -> VNextIngestSession | None:
            claimed = super().try_claim_ingest(periodic, lease_duration_microseconds)
            return claimed if periodic else None

    class _ChangingService(_Service):
        def synchronize_once(
            self,
            session: IngestSessionController,
            *,
            should_stop: Callable[[], bool] | None = None,
        ) -> VNextIngestSynchronizationResult:
            del session, should_stop
            scans.append(now[0])
            if len(scans) == 1:
                schedules[0].note_change(now=110)
                now[0] = 200
            return _synchronized()

    monkeypatch.setattr(resident_module, "monotonic", lambda: now[0])
    monkeypatch.setattr(resident_module, "SourceScanSchedule", make_schedule)
    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _Heartbeat)
    resident = _resident(
        events,
        facade=_DueOnlyFacade(events),
        service=_ChangingService(events),
    )
    resident.run_forever(stop=cast(Event, _Stop()))
    assert scans == [100.0, 200.0]
    assert events.count(("claim", True, 10_000_000)) == 2
    assert schedules[0].next_scan_at() is None


def test_fatal_artifact_failure_logs_exact_context_and_preserves_error(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    events: list[object] = []
    failure = RuntimeError("render failed")
    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _Heartbeat)

    def context_for_error(error: BaseException) -> ArtifactFailureContext:
        assert error is failure
        return ArtifactFailureContext(7, ("source",), ("gallery",), b"002.jpg", 123)

    monkeypatch.setattr(
        artifact_errors_module, "get_artifact_failure_context", context_for_error
    )

    class _ArtifactFailureService:
        def synchronize_once(
            self,
            session: IngestSessionController,
            *,
            should_stop: Callable[[], bool] | None = None,
        ) -> VNextIngestSynchronizationResult:
            del session, should_stop
            raise failure

    resident = _resident(events, service=_ArtifactFailureService())
    resident.initialize()
    with pytest.raises(RuntimeError) as caught:
        resident.process_available(periodic_scan=True)
    assert caught.value is failure
    errors = [record for record in caplog.records if record.levelname == "ERROR"]
    assert len(errors) == 1
    assert (
        'event="artifact_failed" gid=7 gallery_folder="/source/gallery"'
        in errors[0].message
    )
    assert 'file="002.jpg" source_bytes=123' in errors[0].message
    assert 'reason="render failed"' in errors[0].message
    assert resident.last_synchronization_result is None


@pytest.mark.parametrize("stage", ["claim", "complete", "identity", "cleanup"])
def test_storage_full_coordination_waits_and_recovers(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    stage: str,
) -> None:
    events: list[object] = []
    full = sqlite3.OperationalError("database or disk is full")
    full.sqlite_errorcode = sqlite3.SQLITE_FULL
    pressured = False

    class CapacityFacade(_Facade):
        def try_claim_ingest(
            self, periodic: bool, lease_duration_microseconds: int
        ) -> VNextIngestSession | None:
            if pressured and stage == "claim":
                raise full
            return super().try_claim_ingest(periodic, lease_duration_microseconds)

        def complete_ingest(
            self, session: VNextIngestSession
        ) -> VNextIngestCompletionReceipt:
            if pressured and stage == "complete":
                raise full
            return super().complete_ingest(session)

    class CapacityIdentity(_StorageIdentity):
        def ensure_storage_identity(self) -> LibraryStorageIdentity:
            if pressured and stage == "identity":
                raise OSError(errno.ENOSPC, "identity volume full")
            return super().ensure_storage_identity()

    def cleanup() -> None:
        if pressured and stage == "cleanup":
            raise OSError(errno.EDQUOT, "scratch quota full")

    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _Heartbeat)
    resident = _resident(
        events,
        facade=CapacityFacade(events),
        library_storage_identity=CapacityIdentity(
            events, bytes.fromhex("00000000000040008000000000000001")
        ),
        temporary_cleanup=cleanup,
    )
    resident.initialize()
    events.clear()
    pressured = True
    assert not resident.process_available(periodic_scan=True)
    assert not resident.process_available(periodic_scan=True)
    if stage != "complete":
        assert "synchronize" not in events
    assert caplog.text.count("Storage capacity exhausted:") == 1
    assert "no gallery is rejected or published" not in caplog.text
    pressured = False
    assert resident.process_available(periodic_scan=True)
    assert resident.last_synchronization_result is not None


def test_unsafe_scratch_stops_before_claim_and_requires_reinitialization() -> None:
    events: list[object] = []
    unsafe = False
    failure = ScratchSafetyError("scratch root was replaced")

    def cleanup() -> None:
        if unsafe:
            raise failure

    resident = _resident(events, temporary_cleanup=cleanup)
    resident.initialize()
    events.clear()
    unsafe = True
    with pytest.raises(ScratchSafetyError) as caught:
        resident.process_available(periodic_scan=True)
    assert caught.value is failure
    assert not any(isinstance(event, tuple) and event[0] == "claim" for event in events)
    with pytest.raises(RuntimeError, match="must initialize"):
        resident.process_available(periodic_scan=True)


def test_postflight_capacity_failure_is_fatal_and_releases_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []
    monkeypatch.setattr(resident_module, "IngestLeaseHeartbeat", _Heartbeat)
    failure = OSError(errno.ENOSPC, "capture destination full")

    def postflight() -> None:
        raise failure

    with pytest.raises(OSError) as caught:
        _resident(events).process_available(periodic_scan=True, postflight=postflight)
    assert caught.value is failure
    assert events.count(("complete", 2)) == 1
