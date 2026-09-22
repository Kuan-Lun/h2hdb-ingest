"""Real process-stop recovery with counted source and artifact work.

This bounded fixture covers source sealing, partial analysis, catalog projection,
and a prepared artifact, with SIGTERM graceful stop and SIGKILL. It measures
actual adapter calls across fresh interpreters, not physical disk traffic.
It is not arbitrary power-loss evidence or a throughput benchmark.
The independent uninterrupted SQLite run supplies the expected artifact bytes
for both SQLite and opt-in MariaDB recovery; no core internals are imported.
"""

from __future__ import annotations

import multiprocessing
import os
import signal
from dataclasses import dataclass, field
from hashlib import sha256
from io import BytesIO
from multiprocessing.connection import Connection
from pathlib import Path
from threading import Event
from time import monotonic, time_ns
from typing import Any, BinaryIO, Literal, cast
from unittest.mock import patch
from zipfile import ZipFile

import h2hdb
import pytest
from h2hdb import (
    ArtifactArchiveRenderEvidence,
    ArtifactSourceMember,
    ArtifactStorageEvidence,
    CatalogRevisionNotFoundError,
    CoreConfig,
    DatabaseAuditReason,
    DatabaseConfig,
    VNextAnalysisAdvanceResult,
    VNextCatalogFacade,
    VNextDatabaseAdminFacade,
    VNextIngestAdvanceResult,
    VNextIngestFacade,
    VNextIngestGalleryObservation,
    VNextIngestPage,
    VNextIngestSession,
    VNextIssuedPublicationStep,
    VNextPreparedAnalysisStep,
    VNextPreparedPublicationStep,
    VNextPreparedSourceStep,
    VNextResolvedIngestPolicy,
    VNextSourceCompletionMarker,
)
from PIL import Image

import h2hdb_ingest.runtime as runtime_module
from h2hdb_ingest import IngestConfig, IngestPathsConfig, ResidentConfig
from h2hdb_ingest.__main__ import _stop_on_termination
from h2hdb_ingest.core_source import VNextFilesystemSourceAdapter
from h2hdb_ingest.library import ManagedFilesystemLibraryAdapter
from h2hdb_ingest.runtime import IngestRuntime, build_runtime

_Phase = Literal[
    "source-sealed",
    "source-checkpoint",
    "analysis-partial",
    "catalog-projection",
    "artifact-prepared",
]
_Snapshot = tuple[tuple[int, int, str, str, tuple[str, ...]], ...]
_RESTART_CLOCK_OFFSET_US = 120_000_000
_INITIAL_GIDS = (4201, 4202)


@dataclass
class _Evidence:
    """Process-local observations, serialized through IPC at durable boundaries."""

    snapshots: list[_Snapshot] = field(default_factory=list)
    inventory_scan_pending: list[bool] = field(default_factory=list)
    source_calls_at_publication: list[tuple[int, int, int]] = field(
        default_factory=list
    )
    rendered_at_publication: list[tuple[int, ...]] = field(default_factory=list)
    observed_galleries: list[tuple[str, ...]] = field(default_factory=list)
    rendered_gids: list[int] = field(default_factory=list)
    marker_calls: int = 0
    locator_page_calls: int = 0
    file_hash_rows: int = 0
    protected_resources: int = 0


@pytest.fixture(
    params=(
        "sqlite",
        pytest.param("mariadb", marks=(pytest.mark.mariadb, pytest.mark.deep)),
    )
)
def process_core_config(request: pytest.FixtureRequest, tmp_path: Path) -> CoreConfig:
    if request.param == "mariadb":
        return cast(CoreConfig, request.getfixturevalue("mariadb_config"))
    return CoreConfig(
        database=DatabaseConfig(
            sql_type="sqlite", database=str(tmp_path / "recover.sqlite3")
        )
    )


def _source(root: Path, gids: tuple[int, ...] = _INITIAL_GIDS) -> None:
    for gid in gids:
        _gallery(root, gid)


def _gallery(root: Path, gid: int) -> None:
    gallery = root / str(gid)
    gallery.mkdir(parents=True)
    for ordinal in (1, 2):
        color = (
            (gid * 31 + ordinal * 53) % 256,
            (gid * 67 + ordinal * 13) % 256,
            (gid * 19 + ordinal * 83) % 256,
        )
        with Image.new("RGB", (8, 12), color) as image:
            image.save(gallery / f"{ordinal:03d}.jpg")
    (gallery / "galleryinfo.txt").write_text(
        "\n".join(
            (
                f"Title: Process lifecycle fixture {gid}",
                "Upload Time: 2024-01-02 03:04",
                "Uploaded By: uploader",
                "Downloaded: 2024-02-03 04:05",
                "Tags: artist:fixture, language:english",
                "Uploader's Comments",
                "Two deterministic source JPEG pages",
                "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3",
            )
        ),
        encoding="utf-8",
    )


def _config(core: CoreConfig, source: Path, library: Path) -> IngestConfig:
    library.mkdir()
    for relative in (
        "current",
        "current/acquisitions",
        "current/artwork",
        ".h2hdb-coordination",
    ):
        (library / relative).mkdir()
    return IngestConfig(
        core=core,
        paths=IngestPathsConfig(
            download_path=source,
            library_path=library,
            page_render_workers=1,
        ),
        resident=ResidentConfig(
            # This fixture verifies process recovery, not a two-second deadline.
            # The 120-second restart clock offset expires the predecessor; use
            # the same 30-second lease as other runtime E2E cases so ordinary
            # process scheduling cannot consume the entire renewal window.
            lease_seconds=30,
            heartbeat_seconds=0.2,
            poll_seconds=0.05,
        ),
    )


def _verify_publication(
    runtime: IngestRuntime, config: IngestConfig, gids: tuple[int, ...]
) -> _Snapshot:
    assert runtime.database_admin.check().state == "READY"
    revision = runtime.catalog.get_catalog_revision()
    assert revision.publication_count == len(gids)
    page = runtime.catalog.discover_publications(revision=revision)
    assert page.total == len(gids)
    assert {item.gid for item in page.publications} == set(gids)
    assert config.paths.library_path is not None
    current = config.paths.library_path / "current"
    summaries: list[tuple[int, int, str, str, tuple[str, ...]]] = []
    for publication in page.publications:
        assert publication.page_count == 2
        assert len(publication.artifacts) == 1
        storage = publication.artifacts[0].storage_object
        archive_path = current.joinpath(*storage.key.segments)
        archive_bytes = archive_path.read_bytes()
        assert sha256(archive_bytes).hexdigest() == storage.sha256
        page_digests = []
        with ZipFile(BytesIO(archive_bytes)) as archive:
            assert archive.testzip() is None
            assert archive.namelist() == [
                "galleryinfo.txt",
                "pages/0000.jpg",
                "pages/0001.jpg",
            ]
            assert (
                archive.read("galleryinfo.txt")
                == (
                    config.paths.download_path
                    / str(publication.gid)
                    / "galleryinfo.txt"
                ).read_bytes()
            )
            for index in range(2):
                resource = runtime.catalog.get_publication_page(
                    publication.publication_id, index, revision=revision
                )
                assert resource is not None and resource.storage_object == storage
                encoded = archive.read(f"pages/{index:04d}.jpg")
                assert (
                    archive_bytes[
                        resource.extent.offset : resource.extent.offset
                        + resource.extent.length
                    ]
                    == encoded
                )
                assert sha256(encoded).hexdigest() == resource.sha256
                with Image.open(BytesIO(encoded)) as image:
                    image.load()
                    assert image.format == "JPEG"
                    assert image.size == (resource.width, resource.height) == (8, 12)
                page_digests.append(resource.sha256)
        assert publication.thumbnail is not None
        thumbnail = publication.thumbnail.storage_object
        thumbnail_path = current.joinpath(*thumbnail.key.segments)
        assert sha256(thumbnail_path.read_bytes()).hexdigest() == thumbnail.sha256
        with Image.open(thumbnail_path) as image:
            image.load()
            assert image.format == "JPEG" and max(image.size) <= 320
        summaries.append(
            (
                publication.gid,
                publication.page_count,
                storage.sha256,
                thumbnail.sha256,
                tuple(page_digests),
            )
        )
    assert len(tuple(current.rglob("*.cbz"))) == len(gids)
    assert not (
        config.paths.library_path / ".h2hdb-coordination" / "ACTIVATING"
    ).exists()
    return tuple(sorted(summaries))


def _pause_after_commit(
    channel: Connection,
    stop: Event,
    runtime: IngestRuntime,
    phase: _Phase,
    evidence: _Evidence,
) -> None:
    with pytest.raises(CatalogRevisionNotFoundError):
        runtime.catalog.get_catalog_revision()
    channel.send(("paused", phase, evidence))
    deadline = monotonic() + 30
    while not stop.wait(0.05):
        if monotonic() >= deadline:
            raise TimeoutError("parent did not terminate paused ingest child")


def _runtime_child(
    config_data: dict[str, Any],
    channel: Connection,
    phase: _Phase | None,
    clock_offset: int,
    expected_publications: tuple[tuple[int, ...], ...],
) -> None:
    config = IngestConfig.model_validate(config_data)
    evidence = _Evidence()
    channel.send(("import", h2hdb.__file__))

    def facade_with_clock(core: CoreConfig) -> VNextIngestFacade:
        return VNextIngestFacade(core, clock=lambda: time_ns() // 1000 + clock_offset)

    try:
        with (
            patch.object(runtime_module, "VNextIngestFacade", facade_with_clock),
            build_runtime(config) as runtime,
            _stop_on_termination() as stop,
        ):
            audit = runtime.resident.initialize(should_stop=stop.is_set)
            channel.send(("audit", audit.reason.value, audit.full_audit is not None))
            source_commit = VNextIngestFacade.commit_source_step
            analysis_commit = VNextIngestFacade.commit_analysis_step
            publication_issue = VNextIngestFacade.issue_publication_step
            publication_commit = VNextIngestFacade.commit_publication_step
            observe = VNextFilesystemSourceAdapter.observe_gallery
            marker = VNextFilesystemSourceAdapter.observe_completion_marker
            list_locators = VNextFilesystemSourceAdapter.list_gallery_locators
            render = ManagedFilesystemLibraryAdapter.render_archive
            protect = ManagedFilesystemLibraryAdapter.protect
            last_operation = ""
            paused = False

            def observe_gallery(
                adapter: VNextFilesystemSourceAdapter,
                locator_components: tuple[str, ...],
            ) -> VNextIngestGalleryObservation:
                nonlocal paused
                if (
                    phase == "source-checkpoint"
                    and evidence.observed_galleries
                    and not paused
                ):
                    # The production source driver reaches the next gallery only
                    # after sealing and retiring the preceding gallery's staging.
                    # This pause is outside a core transaction or lease lock.
                    paused = True
                    _pause_after_commit(channel, stop, runtime, phase, evidence)
                evidence.observed_galleries.append(locator_components)
                return observe(adapter, locator_components)

            def observe_marker(
                adapter: VNextFilesystemSourceAdapter,
                locator_components: tuple[str, ...],
            ) -> VNextSourceCompletionMarker:
                evidence.marker_calls += 1
                return marker(adapter, locator_components)

            def list_gallery_locators(
                adapter: VNextFilesystemSourceAdapter,
                *,
                after_locator: tuple[str, ...] | None,
                limit: int,
            ) -> VNextIngestPage[tuple[str, ...]]:
                evidence.locator_page_calls += 1
                return list_locators(adapter, after_locator=after_locator, limit=limit)

            def render_archive(
                adapter: ManagedFilesystemLibraryAdapter,
                members: tuple[ArtifactSourceMember, ...],
                destination: BinaryIO,
                *,
                gid: int,
            ) -> ArtifactArchiveRenderEvidence:
                evidence.rendered_gids.append(gid)
                return render(adapter, members, destination, gid=gid)

            def protect_resource(
                adapter: ManagedFilesystemLibraryAdapter, *args: Any, **kwargs: Any
            ) -> ArtifactStorageEvidence:
                result = protect(adapter, *args, **kwargs)
                evidence.protected_resources += 1
                return result

            def commit_source(
                facade: VNextIngestFacade,
                session: VNextIngestSession,
                prepared: VNextPreparedSourceStep,
            ) -> VNextIngestAdvanceResult:
                nonlocal paused
                result = source_commit(facade, session, prepared)
                if phase == "source-sealed" and result.terminal and not paused:
                    assert result.source_receipt is not None
                    assert result.source_receipt.sealed
                    paused = True
                    _pause_after_commit(channel, stop, runtime, phase, evidence)
                return result

            def commit_analysis(
                facade: VNextIngestFacade,
                session: VNextIngestSession,
                prepared: VNextPreparedAnalysisStep,
            ) -> VNextAnalysisAdvanceResult:
                nonlocal paused
                result = analysis_commit(facade, session, prepared)
                if result.stage == b"file_hash_decision" and not result.replayed:
                    evidence.file_hash_rows += result.processed_rows
                    if (
                        phase == "analysis-partial"
                        and result.processed_rows > 0
                        and not result.stage_terminal
                        and not paused
                    ):
                        paused = True
                        _pause_after_commit(channel, stop, runtime, phase, evidence)
                return result

            def issue_publication(
                facade: VNextIngestFacade,
                session: VNextIngestSession,
                policy: VNextResolvedIngestPolicy,
            ) -> VNextIssuedPublicationStep:
                nonlocal last_operation
                issued = publication_issue(facade, session, policy)
                last_operation = issued.operation
                return issued

            def commit_publication(
                facade: VNextIngestFacade,
                session: VNextIngestSession,
                prepared: VNextPreparedPublicationStep,
            ) -> VNextIngestAdvanceResult:
                nonlocal paused
                result = publication_commit(facade, session, prepared)
                if (
                    phase == "catalog-projection"
                    and last_operation == "BUILD_CATALOG"
                    and result.processed_rows > 0
                    and not paused
                ):
                    paused = True
                    _pause_after_commit(channel, stop, runtime, phase, evidence)
                elif (
                    phase == "artifact-prepared"
                    and last_operation == "PREPARE_ARTIFACT"
                    and evidence.protected_resources == 2
                    and not paused
                ):
                    # The first preparation commit persists PENDING. The next
                    # commit follows real acquisition + thumbnail protection,
                    # and confirms their durable PREPARED resource bundle.
                    assert len(evidence.rendered_gids) == 1
                    paused = True
                    _pause_after_commit(channel, stop, runtime, phase, evidence)
                return result

            with (
                patch.object(VNextIngestFacade, "commit_source_step", commit_source),
                patch.object(
                    VNextIngestFacade, "commit_analysis_step", commit_analysis
                ),
                patch.object(
                    VNextIngestFacade, "issue_publication_step", issue_publication
                ),
                patch.object(
                    VNextIngestFacade, "commit_publication_step", commit_publication
                ),
                patch.object(
                    VNextFilesystemSourceAdapter, "observe_gallery", observe_gallery
                ),
                patch.object(
                    VNextFilesystemSourceAdapter,
                    "observe_completion_marker",
                    observe_marker,
                ),
                patch.object(
                    VNextFilesystemSourceAdapter,
                    "list_gallery_locators",
                    list_gallery_locators,
                ),
                patch.object(
                    ManagedFilesystemLibraryAdapter, "render_archive", render_archive
                ),
                patch.object(
                    ManagedFilesystemLibraryAdapter, "protect", protect_resource
                ),
            ):
                last_revision = None
                deadline = monotonic() + 50
                while monotonic() < deadline:
                    runtime.resident.process_available(
                        periodic_scan=True, should_stop=stop.is_set
                    )
                    if stop.is_set():
                        assert paused
                        channel.send(("stopped", phase))
                        return
                    try:
                        revision = runtime.catalog.get_catalog_revision()
                    except CatalogRevisionNotFoundError:
                        stop.wait(config.resident.poll_seconds)
                        continue
                    assert phase is None, "selected durable boundary was never reached"
                    if revision == last_revision:
                        stop.wait(config.resident.poll_seconds)
                        continue
                    last_revision = revision
                    result = runtime.resident.last_synchronization_result
                    assert result is not None
                    assert (
                        result.deferred_gallery_count is None
                    ) == result.inventory_scan_pending
                    assert (
                        result.waiting_gallery_count is None
                    ) == result.inventory_scan_pending
                    gids = expected_publications[len(evidence.snapshots)]
                    evidence.snapshots.append(
                        _verify_publication(runtime, config, gids)
                    )
                    evidence.inventory_scan_pending.append(
                        result.inventory_scan_pending
                    )
                    evidence.source_calls_at_publication.append(
                        (
                            len(evidence.observed_galleries),
                            evidence.marker_calls,
                            evidence.locator_page_calls,
                        )
                    )
                    evidence.rendered_at_publication.append(
                        tuple(evidence.rendered_gids)
                    )
                    if len(evidence.snapshots) == len(expected_publications):
                        channel.send(("complete", evidence))
                        return
                raise AssertionError("small restarted runtime did not converge")
    except BaseException as error:
        channel.send(("error", repr(error)))
        raise
    finally:
        channel.close()


def _run_process(
    config: IngestConfig,
    *,
    phase: _Phase | None = None,
    signal_name: str | None = None,
    clock_offset: int = 0,
    expected_audit: DatabaseAuditReason = DatabaseAuditReason.FIRST_RUN,
    expected_publications: tuple[tuple[int, ...], ...] = (_INITIAL_GIDS,),
) -> _Evidence:
    processes = multiprocessing.get_context("spawn")
    parent, child = processes.Pipe(duplex=False)
    process = processes.Process(
        target=_runtime_child,
        args=(
            config.model_dump(mode="json"),
            child,
            phase,
            clock_offset,
            expected_publications,
        ),
    )
    process.start()
    child.close()
    try:
        assert parent.poll(15), "spawned runtime child did not report its core import"
        assert parent.recv() == ("import", h2hdb.__file__)
        assert parent.poll(60), "runtime child did not report its startup audit"
        message = parent.recv()
        assert message == (
            "audit",
            expected_audit.value,
            expected_audit
            not in (
                DatabaseAuditReason.RECENT_AUDIT,
                DatabaseAuditReason.INITIAL_CATCHUP,
            ),
        ), message
        assert parent.poll(60), "runtime child did not report its bounded outcome"
        message = parent.recv()
        if phase is not None:
            assert message[:2] == ("paused", phase), message
            evidence = cast(_Evidence, message[2])
            assert signal_name is not None and process.pid is not None
            requested_signal = getattr(signal, signal_name)
            os.kill(process.pid, requested_signal)
            process.join(30)
            if signal_name == "SIGTERM":
                assert parent.poll(1)
                assert parent.recv() == ("stopped", phase)
                assert process.exitcode == 0
            else:
                assert process.exitcode == -signal.SIGKILL
            return evidence
        process.join(30)
        assert process.exitcode == 0, message
        assert message[0] == "complete", message
        return cast(_Evidence, message[1])
    finally:
        parent.close()
        if process.is_alive():
            process.kill()
            process.join(10)
        assert not process.is_alive()
        process.close()


def _initialized_configs(
    tmp_path: Path, core: CoreConfig, *, max_rows: int = 128
) -> tuple[IngestConfig, IngestConfig]:
    source = tmp_path / "download"
    _source(source)
    reference = _config(
        CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite", database=str(tmp_path / "reference.sqlite3")
            )
        ),
        source,
        tmp_path / "reference-library",
    )
    recovered = _config(core, source, tmp_path / "recovered-library")
    reference = reference.model_copy(
        update={
            "resident": reference.resident.model_copy(update={"max_rows": max_rows})
        }
    )
    recovered = recovered.model_copy(
        update={
            "resident": recovered.resident.model_copy(update={"max_rows": max_rows})
        }
    )
    for config in (reference, recovered):
        admin = VNextDatabaseAdminFacade(config.core)
        try:
            admin.initialize()
        finally:
            admin.close()
    return reference, recovered


def _assert_no_published_head(config: IngestConfig) -> None:
    catalog = VNextCatalogFacade(config.core)
    try:
        with pytest.raises(CatalogRevisionNotFoundError):
            catalog.get_catalog_revision()
    finally:
        catalog.close()


def _assert_resumed_source_without_rescan(evidence: _Evidence) -> None:
    assert evidence.inventory_scan_pending == [True]
    assert evidence.observed_galleries == []
    assert evidence.marker_calls == len(_INITIAL_GIDS)
    assert evidence.locator_page_calls == 0
    assert evidence.source_calls_at_publication == [(0, len(_INITIAL_GIDS), 0)]


@pytest.mark.skipif(os.name != "posix", reason="POSIX SIGTERM/SIGKILL process evidence")
@pytest.mark.parametrize("signal_name", ("SIGTERM", "SIGKILL"))
@pytest.mark.parametrize("phase", ("source-sealed", "catalog-projection"))
def test_fresh_runtime_recovers_committed_pipeline_after_process_stop(
    tmp_path: Path,
    process_core_config: CoreConfig,
    signal_name: str,
    phase: _Phase,
) -> None:
    reference, recovered = _initialized_configs(tmp_path, process_core_config)
    expected = _run_process(reference)
    interrupted = _run_process(recovered, phase=phase, signal_name=signal_name)
    assert interrupted.observed_galleries == [(str(gid),) for gid in _INITIAL_GIDS]
    assert interrupted.locator_page_calls > 0
    assert interrupted.marker_calls > 0
    _assert_no_published_head(recovered)
    resumed = _run_process(
        recovered,
        clock_offset=_RESTART_CLOCK_OFFSET_US,
        expected_audit=(
            DatabaseAuditReason.PREVIOUS_INTERRUPTION
            if signal_name == "SIGKILL"
            else DatabaseAuditReason.INITIAL_CATCHUP
        ),
    )
    assert resumed.snapshots == expected.snapshots
    _assert_resumed_source_without_rescan(resumed)
    assert sorted(resumed.rendered_gids) == list(_INITIAL_GIDS)


@pytest.mark.skipif(os.name != "posix", reason="POSIX SIGKILL process evidence")
def test_fresh_process_resumes_partial_analysis_without_repeating_committed_rows(
    tmp_path: Path, process_core_config: CoreConfig
) -> None:
    reference, recovered = _initialized_configs(
        tmp_path, process_core_config, max_rows=1
    )
    expected = _run_process(reference)
    interrupted = _run_process(
        recovered, phase="analysis-partial", signal_name="SIGKILL"
    )
    assert interrupted.file_hash_rows == 1
    assert interrupted.file_hash_rows < expected.file_hash_rows
    assert interrupted.rendered_gids == []
    _assert_no_published_head(recovered)
    resumed = _run_process(
        recovered,
        clock_offset=_RESTART_CLOCK_OFFSET_US,
        expected_audit=DatabaseAuditReason.PREVIOUS_INTERRUPTION,
    )
    assert resumed.snapshots == expected.snapshots
    _assert_resumed_source_without_rescan(resumed)
    assert (
        interrupted.file_hash_rows + resumed.file_hash_rows == expected.file_hash_rows
    )
    assert sorted(resumed.rendered_gids) == list(_INITIAL_GIDS)


@pytest.mark.skipif(os.name != "posix", reason="POSIX SIGKILL process evidence")
def test_prepared_archive_survives_restart_and_new_gallery_waits_for_next_turn(
    tmp_path: Path, process_core_config: CoreConfig
) -> None:
    reference, recovered = _initialized_configs(tmp_path, process_core_config)
    # A real-clock successor waits for the interrupted lease to expire. This
    # case continues into another source cut, whose database-owned timestamp
    # must remain comparable with the completed publication's timestamp.
    recovered = recovered.model_copy(
        update={"resident": recovered.resident.model_copy(update={"lease_seconds": 10})}
    )
    expected = _run_process(reference)
    interrupted = _run_process(
        recovered,
        phase="artifact-prepared",
        signal_name="SIGKILL",
    )
    assert len(interrupted.rendered_gids) == 1
    assert interrupted.protected_resources == 2
    _assert_no_published_head(recovered)
    _source(recovered.paths.download_path, (4203,))
    resumed = _run_process(
        recovered,
        expected_audit=DatabaseAuditReason.PREVIOUS_INTERRUPTION,
        expected_publications=(_INITIAL_GIDS, (*_INITIAL_GIDS, 4203)),
    )
    assert resumed.snapshots[0] == expected.snapshots[0]
    assert resumed.inventory_scan_pending == [True, False]
    assert resumed.source_calls_at_publication[0] == (0, len(_INITIAL_GIDS), 0)
    assert resumed.observed_galleries == [("4203",)]
    assert resumed.marker_calls > 0 and resumed.locator_page_calls > 0
    assert sorted(
        interrupted.rendered_gids + list(resumed.rendered_at_publication[0])
    ) == list(_INITIAL_GIDS)
    assert sorted(interrupted.rendered_gids + resumed.rendered_gids) == [
        4201,
        4202,
        4203,
    ]
    assert (
        tuple(item for item in resumed.snapshots[1] if item[0] in _INITIAL_GIDS)
        == expected.snapshots[0]
    )


@pytest.mark.skipif(
    os.name != "posix", reason="POSIX SIGKILL first-scan checkpoint evidence"
)
@pytest.mark.parametrize("change_completed_gallery", (False, True))
def test_first_scan_checkpoint_survives_sigkill_and_revalidates_changed_marker(
    tmp_path: Path, process_core_config: CoreConfig, change_completed_gallery: bool
) -> None:
    reference, recovered = _initialized_configs(tmp_path, process_core_config)
    recovered = recovered.model_copy(
        update={"resident": recovered.resident.model_copy(update={"lease_seconds": 10})}
    )
    interrupted = _run_process(
        recovered, phase="source-checkpoint", signal_name="SIGKILL"
    )
    assert len(interrupted.observed_galleries) == 1
    assert interrupted.rendered_gids == []
    _assert_no_published_head(recovered)
    completed = interrupted.observed_galleries[0]
    if change_completed_gallery:
        folder = recovered.paths.download_path.joinpath(*completed)
        with Image.new("RGB", (8, 12), (43, 131, 227)) as image:
            image.save(folder / "001.jpg")
        marker = folder / "galleryinfo.txt"
        marker.write_text(
            marker.read_text(encoding="utf-8").replace(
                "Process lifecycle fixture", "Updated process lifecycle fixture"
            ),
            encoding="utf-8",
        )
    expected = _run_process(reference)
    resumed = _run_process(
        recovered,
        expected_audit=DatabaseAuditReason.PREVIOUS_INTERRUPTION,
    )
    assert resumed.snapshots == expected.snapshots
    assert resumed.inventory_scan_pending == [False]
    assert resumed.marker_calls > 0 and resumed.locator_page_calls > 0
    expected_reads: set[tuple[str, ...]] = {(str(gid),) for gid in _INITIAL_GIDS}
    if not change_completed_gallery:
        expected_reads.remove(completed)
    assert set(resumed.observed_galleries) == expected_reads
    assert len(resumed.observed_galleries) == len(expected_reads)
    assert sorted(resumed.rendered_gids) == list(_INITIAL_GIDS)
