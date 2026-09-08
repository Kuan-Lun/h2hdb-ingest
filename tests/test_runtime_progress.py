from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from threading import Barrier, Event, Lock, Thread, get_ident
from threading import enumerate as enumerate_threads
from typing import cast
from zipfile import ZipFile

import pytest
from h2hdb import (
    ArtifactSourceMember,
    CoreConfig,
    DatabaseConfig,
    SchemaEpochReport,
    VNextCurrentOnlyMaintenanceOutcome,
    VNextDatabaseAdminFacade,
    VNextIngestFacade,
    VNextIngestSession,
)
from PIL import Image

import h2hdb_ingest.artifact as artifact_module
from h2hdb_ingest import IngestConfig, IngestPathsConfig, ResidentConfig
from h2hdb_ingest.filesystem import FilesystemCompletionMarker
from h2hdb_ingest.maintenance import LibraryMaintenanceOutcome
from h2hdb_ingest.progress import IngestProgress, ProgressWork
from h2hdb_ingest.resident import ResidentIngestor
from h2hdb_ingest.runtime import build_runtime
from h2hdb_ingest.service import VNextIngestSynchronizationResult
from h2hdb_ingest.session import IngestSessionController


def _config(tmp_path: Path, *, artifacts: bool = False) -> IngestConfig:
    source = tmp_path / "source"
    source.mkdir()
    library: Path | None = None
    if artifacts:
        library = tmp_path / "library"
        library.mkdir()
        for relative in (
            "current",
            "current/acquisitions",
            "current/artwork",
            ".h2hdb-coordination",
        ):
            (library / relative).mkdir()
    return IngestConfig(
        core=CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite", database=str(tmp_path / "catalog.sqlite3")
            )
        ),
        paths=IngestPathsConfig(
            download_path=source,
            library_path=library,
            page_render_workers=2,
        ),
    )


def _gallery(source: Path) -> None:
    folder = source / "1001"
    folder.mkdir()
    for number, color in enumerate(("red", "blue"), start=1):
        Image.new("RGB", (8, 12), color).save(folder / f"{number:03d}.jpg")
    (folder / "galleryinfo.txt").write_text(
        "\n".join(
            (
                "Title: Progress integration 1001",
                "Upload Time: 2024-01-02 03:04",
                "Uploaded By: uploader",
                "Downloaded: 2024-02-03 04:05",
                "Tags: artist:progress, language:english",
                "Uploader's Comments",
                "A comment",
                "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3",
            )
        ),
        encoding="utf-8",
    )


def _progress_records(messages: list[str]) -> list[dict[str, str]]:
    return [
        dict(field.split("=", 1) for field in message.split()[1:])
        for message in messages
        if message.startswith("ingest_progress ")
    ]


@pytest.mark.parametrize(
    "backend",
    ["sqlite", pytest.param("mariadb", marks=(pytest.mark.mariadb, pytest.mark.deep))],
)
@pytest.mark.parametrize("artifacts", [False, True])
def test_runtime_reports_real_source_database_and_parallel_cbz_progress(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
    artifacts: bool,
    backend: str,
) -> None:
    config = _config(tmp_path, artifacts=artifacts)
    if backend == "mariadb":
        config = IngestConfig(
            core=cast(CoreConfig, request.getfixturevalue("mariadb_config")),
            paths=config.paths,
            resident=config.resident,
        )
    _gallery(config.paths.download_path)
    messages: list[str] = []
    worker_ids: set[int] = set()
    workers_lock = Lock()
    rendezvous = Barrier(2)
    original_render = artifact_module._render_page_member

    def render(
        member: ArtifactSourceMember,
        *,
        policy: artifact_module.ArtifactRenderPolicy,
        progress: ProgressWork | None = None,
    ) -> artifact_module._RenderedPageBuffer:
        with workers_lock:
            worker_ids.add(get_ident())
        # Both real page workers must enter before either render can complete.
        rendezvous.wait(timeout=10)
        rendered = original_render(member, policy=policy, progress=progress)
        assert runtime._progress is not None
        snapshot = runtime._progress.snapshot()
        assert snapshot is not None
        assert dict(snapshot.counters)["pages_rendered"] >= 1
        assert "publication_batches_finalized" not in dict(snapshot.counters)
        return rendered

    monkeypatch.setattr(artifact_module, "_render_page_member", render)
    with build_runtime(config, event_logger=messages.append) as runtime:
        runtime.database_admin.initialize()
        runtime.resident.initialize()
        assert runtime.resident.process_available(periodic_scan=True)
        revision = runtime.catalog.get_catalog_revision()
        assert revision.publication_count == 1
        assert revision.artifact_count == int(artifacts)
        records = _progress_records(messages)
        phases = [
            record["phase"] for record in records if record["event"] == "phase_started"
        ]
        assert phases[0] == "startup_check"
        assert phases.count("source") == 1
        assert phases.count("analysis") == 1
        assert phases.count("publication") == 1
        assert phases.index("source") < phases.index("analysis")
        assert phases.index("analysis") < phases.index("publication")
        completed = [
            record
            for record in records
            if record["event"] == "work_finished"
            and "counter.publication_batches_finalized" in record
        ]
        assert len(completed) == 1
        result = completed[0]
        assert result["status"] == "completed"
        assert result["counter.source_galleries"] == "1"
        assert result["counter.publication_batches_finalized"] == "1"
        assert int(result["counter.source_committed_steps"]) > 0
        assert int(result["counter.analysis_committed_steps"]) > 0
        assert int(result["counter.publication_committed_steps"]) > 0
        if artifacts:
            assert len(worker_ids) == 2
            assert result["counter.pages_rendered"] == "2"
            assert result["counter.pages_written"] == "2"
            assert result["counter.archives_rendered"] == "1"
            assert result["counter.presentations_rendered"] == "1"
            library = config.paths.library_path
            assert library is not None
            archives = tuple((library / "current").rglob("*.cbz"))
            assert len(archives) == 1
            with ZipFile(archives[0]) as archive:
                assert (
                    len(
                        [
                            name
                            for name in archive.namelist()
                            if name.startswith("pages/")
                        ]
                    )
                    == 2
                )
        else:
            assert not worker_ids
            assert "counter.pages_rendered" not in result
            assert "counter.archives_rendered" not in result
        assert runtime._progress is not None
        assert runtime._progress.current() is None
        assert runtime.database_admin.check().state == "READY"


@dataclass
class _Clock:
    now: float = 0

    def __call__(self) -> float:
        return self.now


class _Admin:
    def __init__(self) -> None:
        self.calls = 0

    def check(self) -> SchemaEpochReport:
        self.calls += 1
        return cast(SchemaEpochReport, object())


class _Facade:
    def __init__(self, *, available: bool = False) -> None:
        self.available = available
        self.calls = 0

    def drain_current_only_maintenance(
        self,
        lease_duration_microseconds: int,
        *,
        artifact_release_adapters: object,
    ) -> VNextCurrentOnlyMaintenanceOutcome:
        del lease_duration_microseconds, artifact_release_adapters
        self.calls += 1
        return VNextCurrentOnlyMaintenanceOutcome.DONE

    def try_claim_ingest(
        self, periodic_scan: bool, lease_duration_microseconds: int
    ) -> VNextIngestSession | None:
        del periodic_scan, lease_duration_microseconds
        self.calls += 1
        if not self.available:
            return None
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


class _Maintenance:
    def __init__(self, outcome: LibraryMaintenanceOutcome) -> None:
        self.outcome = outcome

    def maintain_cleanup(self) -> LibraryMaintenanceOutcome:
        return self.outcome


class _FailingService:
    def synchronize_once(
        self,
        session: IngestSessionController,
        *,
        should_stop: Callable[[], bool] | None = None,
    ) -> VNextIngestSynchronizationResult:
        del session, should_stop
        raise RuntimeError("broken synchronization")


def _empty_probe(
    checkpoint: Callable[[], None],
) -> Iterator[tuple[tuple[str, ...], FilesystemCompletionMarker]]:
    checkpoint()
    return iter(())


def _resident(
    progress: IngestProgress,
    *,
    facade: _Facade | None = None,
    admin: _Admin | None = None,
    maintenance: LibraryMaintenanceOutcome = LibraryMaintenanceOutcome.DONE,
) -> ResidentIngestor:
    return ResidentIngestor(
        service=_FailingService(),
        source_probe=_empty_probe,
        facade=cast(VNextIngestFacade, facade or _Facade()),
        database_admin=cast(VNextDatabaseAdminFacade, admin or _Admin()),
        library_storage_identity=None,
        library_maintenance=_Maintenance(maintenance),
        config=ResidentConfig(),
        database_type="sqlite",
        artifact_release_adapters={},
        event_logger=lambda _: None,
        progress=progress,
    )


def test_resident_idle_polls_remain_silent_beyond_hourly_interval() -> None:
    clock = _Clock()
    messages: list[str] = []
    progress = IngestProgress(messages.append, clock=clock)
    resident = _resident(progress)
    try:
        for _ in range(4):
            assert not resident.process_available(periodic_scan=False)
            clock.now += 3600
            progress._report_due()
            assert progress.current() is None
        assert messages == []
    finally:
        progress.close()


@pytest.mark.parametrize(
    ("periodic_scan", "maintenance"),
    [
        (True, LibraryMaintenanceOutcome.DONE),
        (False, LibraryMaintenanceOutcome.BLOCKED),
    ],
)
def test_resident_pending_polls_keep_one_hourly_work_generation(
    periodic_scan: bool, maintenance: LibraryMaintenanceOutcome
) -> None:
    clock = _Clock()
    messages: list[str] = []
    progress = IngestProgress(messages.append, clock=clock)
    resident = _resident(progress, maintenance=maintenance)
    try:
        assert not resident.process_available(periodic_scan=periodic_scan)
        work = progress.current()
        assert work is not None
        for _ in range(720):
            clock.now += 5
            assert not resident.process_available(periodic_scan=periodic_scan)
            assert progress.current() is work
            progress._report_due()
        records = _progress_records(messages)
        assert len(records) == 1
        assert records[0]["event"] == "periodic"
        assert records[0]["elapsed_seconds"] == "3600.0"
        assert records[0]["last_progress_age_seconds"] == "3600.0"
        assert not resident.process_available(
            periodic_scan=periodic_scan, should_stop=lambda: True
        )
        assert progress.current() is None
    finally:
        progress.close()


def test_resident_service_failure_finishes_observation_scope() -> None:
    messages: list[str] = []
    progress = IngestProgress(messages.append)
    resident = _resident(progress, facade=_Facade(available=True))
    try:
        with pytest.raises(RuntimeError, match="broken synchronization"):
            resident.process_available(periodic_scan=True)
        assert progress.current() is None
        assert _progress_records(messages)[-1]["status"] == "failed"
    finally:
        progress.close()


def test_blocked_startup_check_reports_without_another_database_call() -> None:
    entered = Event()
    release = Event()
    reported = Event()
    clock = _Clock()
    messages: list[str] = []
    failures: list[BaseException] = []

    class BlockingAdmin(_Admin):
        def check(self) -> SchemaEpochReport:
            self.calls += 1
            entered.set()
            assert release.wait(timeout=10)
            return cast(SchemaEpochReport, object())

    def emit(message: str) -> None:
        messages.append(message)
        if "event=periodic" in message:
            reported.set()

    admin = BlockingAdmin()
    facade = _Facade()
    progress = IngestProgress(emit, clock=clock)
    resident = _resident(progress, facade=facade, admin=admin)

    def initialize() -> None:
        try:
            resident.initialize()
        except BaseException as error:
            failures.append(error)

    initializer = Thread(target=initialize, name="test-progress-startup")
    progress.start()
    initializer.start()
    try:
        assert entered.wait(timeout=5)
        assert admin.calls == 1
        assert facade.calls == 0
        clock.now = 3600
        progress._wake.set()
        assert reported.wait(timeout=5)
        assert initializer.is_alive()
        assert admin.calls == 1
        assert facade.calls == 0
        periodic = [
            record
            for record in _progress_records(messages)
            if record["event"] == "periodic"
        ]
        assert len(periodic) == 1
        assert periodic[0]["phase"] == "startup_check"
        assert periodic[0]["elapsed_seconds"] == "3600.0"
    finally:
        release.set()
        initializer.join(timeout=5)
        progress.close()
    assert not initializer.is_alive()
    assert not failures
    assert progress.current() is None


@pytest.mark.parametrize("fail_close", [False, True])
def test_runtime_close_stops_owned_reporter_even_if_facade_close_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fail_close: bool
) -> None:
    before = set(enumerate_threads())
    runtime = build_runtime(_config(tmp_path), event_logger=lambda _: None)
    reporters = {
        thread
        for thread in enumerate_threads()
        if thread not in before and thread.name == "h2hdb-ingest-progress"
    }
    assert len(reporters) == 1
    original_close = VNextIngestFacade.close

    def close(facade: VNextIngestFacade) -> None:
        original_close(facade)
        raise RuntimeError("facade close failed")

    if fail_close:
        monkeypatch.setattr(VNextIngestFacade, "close", close)
    try:
        if fail_close:
            with pytest.raises(RuntimeError, match="facade close failed"):
                runtime.close()
        else:
            runtime.close()
        assert all(not thread.is_alive() for thread in reporters)
        assert runtime._progress is not None
        assert runtime._progress.current() is None
    finally:
        monkeypatch.setattr(VNextIngestFacade, "close", original_close)
        runtime.close()
