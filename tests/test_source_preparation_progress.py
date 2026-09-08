"""Real batch admission and reporter visibility before source ingestion starts."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from random import Random
from threading import Event, Lock
from zipfile import ZipFile

import pytest
from h2hdb import (
    CoreConfig,
    DatabaseConfig,
    VNextIngestFacade,
    VNextIngestSourceAdapter,
    VNextPreparedSource,
    VNextResolvedIngestPolicy,
    VNextSourcePreparationObserver,
    VNextSourcePreparationOperation,
    VNextSourcePreparationProgress,
)
from PIL import Image

import h2hdb_ingest.runtime as runtime_module
from h2hdb_ingest import IngestConfig, IngestPathsConfig, ResidentConfig
from h2hdb_ingest.progress import IngestProgress, ProgressSnapshot


@dataclass
class _Clock:
    now: float = 0

    def __call__(self) -> float:
        return self.now


def _config(tmp_path: Path, *, galleries: int, artifacts: bool) -> IngestConfig:
    source = tmp_path / "source"
    source.mkdir()
    for index in range(galleries):
        gid = 10_000 + index
        folder = source / str(gid)
        folder.mkdir()
        # Each gallery has independently seeded pixels, so JPEG quantization
        # cannot collapse adjacent flat colors into duplicate source pages.
        with Image.frombytes(
            "RGB", (8, 12), Random(index).randbytes(8 * 12 * 3)
        ) as picture:
            picture.save(folder / "001.jpg")
        (folder / "galleryinfo.txt").write_text(
            "\n".join(
                (
                    f"Title: Source preparation fixture {gid}",
                    "Upload Time: 2024-01-02 03:04",
                    "Uploaded By: uploader",
                    "Downloaded: 2024-02-03 04:05",
                    "Tags: artist:progress, language:english",
                    "Uploader's Comments",
                    f"Deterministic fixture {gid}",
                    "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3",
                )
            ),
            encoding="utf-8",
        )
    library = tmp_path / "library" if artifacts else None
    if library is not None:
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
        resident=ResidentConfig(publication_batch_galleries=10),
    )


@pytest.mark.parametrize("artifacts,galleries", [(False, 1001), (True, 12)])
def test_batch_of_ten_reports_source_preparation_and_publishes_real_galleries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    artifacts: bool,
    galleries: int,
) -> None:
    """The hourly thread sees the blocked index/selection operation from memory.

    Real folders exceed the admission limit, and only ten deep observations are
    prepared. The CBZ case also drives the real SQLite publication and verifies
    the installed archives; this is not a mocked gallery-count unit test.
    """

    config = _config(tmp_path, galleries=galleries, artifacts=artifacts)
    clock = _Clock()
    messages: list[str] = []
    messages_lock = Lock()
    periodic_emitted = Event()
    awaiting_periodic = Event()
    stages: dict[VNextSourcePreparationOperation, tuple[ProgressSnapshot, str]] = {}
    attempted: set[VNextSourcePreparationOperation] = set()
    observer_errors: list[Exception] = []
    frozen: list[ProgressSnapshot] = []
    tracked: list[IngestProgress] = []

    def emit(message: str) -> None:
        with messages_lock:
            messages.append(message)
        if awaiting_periodic.is_set():
            periodic_emitted.set()

    def progress_factory(
        emit_info: Callable[[str], None],
        *,
        interval_seconds: float,
        emit_debug: Callable[[str], None] | None = None,
    ) -> IngestProgress:
        progress = IngestProgress(
            emit_info,
            interval_seconds=interval_seconds,
            clock=clock,
            emit_debug=emit_debug,
        )
        tracked.append(progress)
        return progress

    original_prepare = VNextIngestFacade.prepare_source

    def prepare_source(
        facade: VNextIngestFacade,
        adapter: VNextIngestSourceAdapter,
        *,
        policy: VNextResolvedIngestPolicy,
        max_new_galleries: int | None = None,
        progress: VNextSourcePreparationObserver | None = None,
    ) -> VNextPreparedSource:
        assert max_new_galleries == 10
        assert progress is not None
        tracker = tracked[0]

        def observe(update: VNextSourcePreparationProgress) -> None:
            progress(update)
            if (
                update.operation
                not in (
                    VNextSourcePreparationOperation.DISCOVERY_TRANSFER,
                    VNextSourcePreparationOperation.BATCH_SELECTION,
                )
                or update.completed == 0
            ):
                return
            if update.operation in attempted:
                return
            attempted.add(update.operation)
            try:
                snapshot = tracker.snapshot()
                assert snapshot is not None
                assert snapshot.phase == "source"
                assert snapshot.operation == "source_" + update.operation.value
                assert snapshot.operation_completed == update.completed
                assert snapshot.operation_total == update.total
                assert snapshot.operation_unit == "galleries"
                assert dict(snapshot.counters)["batch_new_gallery_limit"] == 10
                # The source call remains blocked here while the independent real
                # reporter wakes. No database call or new worker update triggers it.
                periodic_emitted.clear()
                awaiting_periodic.set()
                try:
                    clock.now += 3600
                    tracker._wake.set()
                    assert periodic_emitted.wait(5)
                    with messages_lock:
                        stages[update.operation] = snapshot, messages[-1]
                finally:
                    awaiting_periodic.clear()
            except Exception as error:
                # Core deliberately isolates observer failures. Preserve any
                # assertion for the test without repeating a failed wait per row.
                observer_errors.append(error)

        prepared = original_prepare(
            facade,
            adapter,
            policy=policy,
            max_new_galleries=max_new_galleries,
            progress=observe,
        )
        if observer_errors:
            prepared.close()
            raise observer_errors[0]
        snapshot = tracker.snapshot()
        assert snapshot is not None
        frozen.append(snapshot)
        assert prepared.deferred_gallery_count == galleries - 10
        return prepared

    monkeypatch.setattr(runtime_module, "IngestProgress", progress_factory)
    monkeypatch.setattr(VNextIngestFacade, "prepare_source", prepare_source)
    with runtime_module.build_runtime(config, event_logger=emit) as runtime:
        runtime.database_admin.initialize()
        runtime.resident.initialize()
        assert runtime.resident.process_available(periodic_scan=True)
        revision = runtime.catalog.get_catalog_revision()
        assert revision.publication_count == 10
        assert revision.artifact_count == (10 if artifacts else 0)
        assert runtime.catalog.discover_publications(revision=revision).total == 10
        assert runtime.database_admin.check().state == "READY"
        assert tracked[0].current() is None

    assert len(frozen) == 1
    counters = dict(frozen[0].counters)
    assert counters["galleries_discovered"] == galleries
    assert counters["gallery_indexes_built"] == 10
    assert counters["batch_selected_galleries"] == 10
    assert "archives_rendered" not in counters
    assert set(stages) == {
        VNextSourcePreparationOperation.DISCOVERY_TRANSFER,
        VNextSourcePreparationOperation.BATCH_SELECTION,
    }
    assert (
        "Copying the gallery inventory into the batch plan"
        in stages[VNextSourcePreparationOperation.DISCOVERY_TRANSFER][1]
    )
    assert (
        "Selecting existing and new galleries for this batch"
        in stages[VNextSourcePreparationOperation.BATCH_SELECTION][1]
    )
    assert all("source_discovery" not in message for _, message in stages.values())
    assert all(
        token not in message
        for _, message in stages.values()
        for token in ("operation=", "generation=", "counter.")
    )
    if artifacts:
        assert config.paths.library_path is not None
        archives = tuple((config.paths.library_path / "current").rglob("*.cbz"))
        assert len(archives) == 10
        for path in archives:
            with ZipFile(path) as archive:
                assert archive.testzip() is None
                assert archive.namelist() == ["galleryinfo.txt", "pages/0000.jpg"]
