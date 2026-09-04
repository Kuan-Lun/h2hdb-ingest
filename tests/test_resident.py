from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from threading import Event
from typing import cast

import pytest
from h2hdb import (
    ArtifactReleaseAdapter,
    GalleryStagingCapacityError,
    SchemaEpochReport,
    VNextCurrentOnlyMaintenanceOutcome,
    VNextDatabaseAdminFacade,
    VNextIngestCompletionReceipt,
    VNextIngestFacade,
    VNextIngestSession,
    VNextSourceManifestMismatchError,
)

import h2hdb_ingest.resident as resident_module
from h2hdb_ingest import (
    ArtifactRenderPolicy,
    LibraryStorageIdentity,
    LibraryStorageIdentityMismatchError,
    ManagedFilesystemLibraryAdapter,
    ResidentConfig,
)
from h2hdb_ingest.library_identity import LibraryStorageIdentityProvider
from h2hdb_ingest.maintenance import (
    LibraryMaintenanceOutcome,
    _LibraryStagingSlotConflictError,
)
from h2hdb_ingest.resident import IngestSynchronizer, ResidentIngestor
from h2hdb_ingest.session import IngestSessionController


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
    def __init__(self, events: list[object]) -> None:
        self._events = events

    def synchronize_once(
        self,
        session: IngestSessionController,
        *,
        should_stop: Callable[[], bool] | None = None,
    ) -> object:
        del should_stop
        self._events.append("synchronize")
        return session.call(lambda _facade, receipt: receipt.ingest_generation)


class _ManifestMismatchService:
    def __init__(self, events: list[object]) -> None:
        self._events = events

    def synchronize_once(
        self,
        session: IngestSessionController,
        *,
        should_stop: Callable[[], bool] | None = None,
    ) -> object:
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
    ) -> object:
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
    ) -> object:
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
) -> ResidentIngestor:
    facade = _Facade(
        events,
        available=available,
        maintenance_results=maintenance_results,
    )
    return ResidentIngestor(
        service=service or _Service(events),
        facade=cast(VNextIngestFacade, facade),
        database_admin=cast(VNextDatabaseAdminFacade, _Admin(events)),
        library_storage_identity=library_storage_identity,
        library_maintenance=(library_maintenance or _LibraryMaintenance()),
        config=ResidentConfig(
            periodic_scan_seconds=60,
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
) -> None:
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
        ("log", "vNext ingest synchronization completed: 2"),
    ]
    assert events[6] == ("complete", 2)
    assert events[7] == ("current-only", 10_000_000)
    assert events[8] == (
        "log",
        "vNext ingest session completed: generation=2 replayed=False",
    )


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
        service=_Service(events),
        facade=cast(VNextIngestFacade, facade),
        database_admin=cast(VNextDatabaseAdminFacade, _Admin(events)),
        library_storage_identity=adapter,
        library_maintenance=adapter,
        config=ResidentConfig(
            periodic_scan_seconds=60,
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
        service=_Service(events),
        facade=cast(VNextIngestFacade, facade),
        database_admin=cast(VNextDatabaseAdminFacade, _Admin(events)),
        library_storage_identity=_StorageIdentity(events, storage_uuid),
        library_maintenance=_LibraryMaintenance(),
        config=ResidentConfig(
            periodic_scan_seconds=60,
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
        service=_StagingCapacityService(events),
        facade=cast(VNextIngestFacade, facade),
        database_admin=cast(VNextDatabaseAdminFacade, _Admin(events)),
        library_storage_identity=None,
        library_maintenance=_LibraryMaintenance(),
        config=ResidentConfig(
            periodic_scan_seconds=60,
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
        service=_Service(events),
        facade=cast(VNextIngestFacade, _FailAfterCompletionFacade()),
        database_admin=cast(VNextDatabaseAdminFacade, _Admin(events)),
        library_storage_identity=None,
        library_maintenance=_LibraryMaintenance(),
        config=ResidentConfig(
            periodic_scan_seconds=60,
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
    assert events[-1] == (
        "log",
        "vNext ingest session completed: generation=2 replayed=False",
    )


def test_run_forever_retries_progress_immediately_without_resetting_periodic(
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
        ("wait", 1.0),
    ]
