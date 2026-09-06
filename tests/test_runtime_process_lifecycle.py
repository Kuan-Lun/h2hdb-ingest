"""Real process-stop recovery at two committed public pipeline boundaries.

This bounded fixture covers source-sealed and catalog-projection progress,
SIGTERM graceful stop, and SIGKILL. It is not arbitrary power-loss evidence.
The independent uninterrupted SQLite run supplies the expected artifact bytes
for both SQLite and opt-in MariaDB recovery; no core internals are imported.
"""

from __future__ import annotations

import multiprocessing
import os
import signal
from hashlib import sha256
from io import BytesIO
from multiprocessing.connection import Connection
from pathlib import Path
from threading import Event
from time import monotonic, time_ns
from typing import Any, Literal, cast
from unittest.mock import patch
from zipfile import ZipFile

import h2hdb
import pytest
from h2hdb import (
    CatalogRevisionNotFoundError,
    CoreConfig,
    DatabaseConfig,
    VNextCatalogFacade,
    VNextDatabaseAdminFacade,
    VNextIngestAdvanceResult,
    VNextIngestFacade,
    VNextIngestSession,
    VNextIssuedPublicationStep,
    VNextPreparedPublicationStep,
    VNextPreparedSourceStep,
    VNextResolvedIngestPolicy,
)
from PIL import Image

import h2hdb_ingest.runtime as runtime_module
from h2hdb_ingest import IngestConfig, IngestPathsConfig, ResidentConfig
from h2hdb_ingest.__main__ import _stop_on_termination
from h2hdb_ingest.runtime import IngestRuntime, build_runtime

_Phase = Literal["source-sealed", "catalog-projection"]
_Snapshot = tuple[tuple[int, int, str, str, tuple[str, ...]], ...]
_RESTART_CLOCK_OFFSET_US = 120_000_000


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


def _source(root: Path) -> None:
    gallery = root / "4201"
    gallery.mkdir(parents=True)
    for ordinal, color in enumerate(("red", "blue"), 1):
        with Image.new("RGB", (8, 12), color) as image:
            image.save(gallery / f"{ordinal:03d}.jpg")
    (gallery / "galleryinfo.txt").write_text(
        "\n".join(
            (
                "Title: Process lifecycle fixture",
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
        resident=ResidentConfig(lease_seconds=30, heartbeat_seconds=5),
    )


def _verify_publication(runtime: IngestRuntime, config: IngestConfig) -> _Snapshot:
    assert runtime.database_admin.check().state == "READY"
    revision = runtime.catalog.get_catalog_revision()
    assert revision.publication_count == 1
    page = runtime.catalog.discover_publications(revision=revision)
    assert page.total == 1
    assert config.paths.library_path is not None
    current = config.paths.library_path / "current"
    summaries: list[tuple[int, int, str, str, tuple[str, ...]]] = []
    for publication in page.publications:
        assert publication.gid == 4201 and publication.page_count == 2
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
                    config.paths.download_path / "4201" / "galleryinfo.txt"
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
    assert len(tuple(current.rglob("*.cbz"))) == 1
    assert not (
        config.paths.library_path / ".h2hdb-coordination" / "ACTIVATING"
    ).exists()
    return tuple(summaries)


def _pause_after_commit(
    channel: Connection, stop: Event, runtime: IngestRuntime, phase: _Phase
) -> None:
    with pytest.raises(CatalogRevisionNotFoundError):
        runtime.catalog.get_catalog_revision()
    channel.send(("paused", phase))
    deadline = monotonic() + 30
    while not stop.wait(0.05):
        if monotonic() >= deadline:
            raise TimeoutError("parent did not terminate paused ingest child")


def _runtime_child(
    config_data: dict[str, Any],
    channel: Connection,
    phase: _Phase | None,
    clock_offset: int,
) -> None:
    config = IngestConfig.model_validate(config_data)
    channel.send(("import", h2hdb.__file__))

    def facade_with_clock(core: CoreConfig) -> VNextIngestFacade:
        return VNextIngestFacade(core, clock=lambda: time_ns() // 1000 + clock_offset)

    try:
        with (
            patch.object(runtime_module, "VNextIngestFacade", facade_with_clock),
            build_runtime(config) as runtime,
            _stop_on_termination() as stop,
        ):
            runtime.resident.initialize()
            source_commit = VNextIngestFacade.commit_source_step
            publication_issue = VNextIngestFacade.issue_publication_step
            publication_commit = VNextIngestFacade.commit_publication_step
            last_operation = ""
            paused = False

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
                    _pause_after_commit(channel, stop, runtime, phase)
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
                    _pause_after_commit(channel, stop, runtime, phase)
                return result

            with (
                patch.object(VNextIngestFacade, "commit_source_step", commit_source),
                patch.object(
                    VNextIngestFacade, "issue_publication_step", issue_publication
                ),
                patch.object(
                    VNextIngestFacade, "commit_publication_step", commit_publication
                ),
            ):
                for _attempt in range(32):
                    runtime.resident.process_available(
                        periodic_scan=True, should_stop=stop.is_set
                    )
                    if stop.is_set():
                        assert paused
                        channel.send(("stopped", phase))
                        return
                    try:
                        runtime.catalog.get_catalog_revision()
                    except CatalogRevisionNotFoundError:
                        continue
                    assert phase is None, "selected durable boundary was never reached"
                    channel.send(("complete", _verify_publication(runtime, config)))
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
) -> _Snapshot | None:
    processes = multiprocessing.get_context("spawn")
    parent, child = processes.Pipe(duplex=False)
    process = processes.Process(
        target=_runtime_child,
        args=(config.model_dump(mode="json"), child, phase, clock_offset),
    )
    process.start()
    child.close()
    try:
        assert parent.poll(15), "spawned runtime child did not report its core import"
        assert parent.recv() == ("import", h2hdb.__file__)
        assert parent.poll(60), "runtime child did not report its bounded outcome"
        message = parent.recv()
        if phase is not None:
            assert message == ("paused", phase), message
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
            return None
        process.join(30)
        assert process.exitcode == 0, message
        assert message[0] == "complete", message
        return cast(_Snapshot, message[1])
    finally:
        parent.close()
        if process.is_alive():
            process.kill()
            process.join(10)
        assert not process.is_alive()
        process.close()


@pytest.mark.skipif(os.name != "posix", reason="POSIX SIGTERM/SIGKILL process evidence")
@pytest.mark.parametrize("signal_name", ("SIGTERM", "SIGKILL"))
@pytest.mark.parametrize("phase", ("source-sealed", "catalog-projection"))
def test_fresh_runtime_recovers_committed_pipeline_after_process_stop(
    tmp_path: Path,
    process_core_config: CoreConfig,
    signal_name: str,
    phase: _Phase,
) -> None:
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
    recovered = _config(process_core_config, source, tmp_path / "recovered-library")
    for config in (reference, recovered):
        admin = VNextDatabaseAdminFacade(config.core)
        try:
            admin.initialize()
        finally:
            admin.close()
    expected = _run_process(reference)
    assert expected is not None
    _run_process(recovered, phase=phase, signal_name=signal_name)
    catalog = VNextCatalogFacade(recovered.core)
    try:
        with pytest.raises(CatalogRevisionNotFoundError):
            catalog.get_catalog_revision()
    finally:
        catalog.close()
    assert _run_process(recovered, clock_offset=_RESTART_CLOCK_OFFSET_US) == expected
