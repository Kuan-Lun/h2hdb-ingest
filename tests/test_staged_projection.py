from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

import pytest
from h2hdb import CatalogArtifact

from h2hdb_ingest.models import (
    CBZArtifact,
    CBZPreparationSummary,
    CBZStreamingPreparationRequest,
    FileStatSignature,
)
from h2hdb_ingest.staged_projection import (
    PreparedProjectionArtifact,
    ProjectionCheckpoint,
    ProjectionSelectionCursor,
    ProjectionSelectionPage,
    SelectedGalleryCursor,
    SelectedGalleryFile,
    SelectedGalleryFileCursor,
    SelectedGalleryFilePage,
    SelectedGalleryPage,
    SelectedGallerySource,
    StagedProjectionOrchestrator,
    StagedProjectionPhase,
    StagedProjectionSelection,
)


def _digest(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _gallery(tmp_path: Path, number: int) -> SelectedGallerySource:
    timestamp = datetime(2024, 1, number + 1, tzinfo=UTC)
    return SelectedGallerySource(
        gallery_key=f"gallery-{number:03d}",
        folder=tmp_path / f"gallery-{number:03d}",
        gallery_name=f"Friendly Gallery [{number + 1}]",
        gid=number + 1,
        title=f"Title {number}",
        summary=f"Summary {number}",
        upload_account="uploader",
        upload_time=timestamp,
        download_time=timestamp,
        modified_time=timestamp,
        pages=5,
        tags=(("language", "english"),),
        metadata_sha256=_digest(f"metadata-{number}"),
        source_sha256=_digest(f"source-{number}"),
        content_sha256=_digest(f"content-{number}"),
    )


def _files(tmp_path: Path, gallery: SelectedGallerySource) -> list[SelectedGalleryFile]:
    return [
        SelectedGalleryFile(
            gallery_key=gallery.gallery_key,
            file_key=f"file-{number:03d}",
            file_sort_key=f"{number:03d}",
            path=gallery.folder / f"{number:03d}.jpg",
            name=f"{number:03d}.jpg",
            size_bytes=number + 10,
            sha256=_digest(f"{gallery.gallery_key}-file-{number}"),
            signature=FileStatSignature(
                device=1,
                inode=number + 1,
                size_bytes=number + 10,
                modified_ns=100 + number,
                changed_ns=200 + number,
            ),
            excluded=number == 2,
        )
        for number in range(5)
    ]


class MemoryProjectionAdapter:
    def __init__(
        self,
        tmp_path: Path,
        *,
        gallery_count: int,
        phase: StagedProjectionPhase = StagedProjectionPhase.preparing_artifacts,
    ) -> None:
        self.galleries = [_gallery(tmp_path, number) for number in range(gallery_count)]
        self.files = {
            gallery.gallery_key: _files(tmp_path, gallery) for gallery in self.galleries
        }
        self.phase = phase
        self.artifact_after: SelectedGalleryCursor | None = None
        self.selection_after: ProjectionSelectionCursor | None = None
        self.prepared: dict[str, CatalogArtifact] = {}
        self.prepared_batches: dict[str, tuple[PreparedProjectionArtifact, ...]] = {}
        self.prepared_batch_lengths: list[int] = []
        self.staged: dict[str, StagedProjectionSelection] = {}
        self.selection_batches: dict[str, tuple[StagedProjectionSelection, ...]] = {}
        self.events: list[tuple[str, object]] = []
        self.gallery_limits: list[int] = []
        self.file_limits: list[int] = []
        self.selection_limits: list[int] = []
        self.selection_batch_lengths: list[int] = []
        self.fail_record_once_for: str | None = None

    def get_projection_checkpoint(self, build_id: str) -> ProjectionCheckpoint:
        assert build_id == "build-1"
        return ProjectionCheckpoint(
            self.phase,
            artifact_after=self.artifact_after,
            selection_after=self.selection_after,
        )

    def page_selected_galleries(
        self,
        build_id: str,
        *,
        after: SelectedGalleryCursor | None,
        limit: int,
    ) -> SelectedGalleryPage:
        assert build_id == "build-1"
        assert self.phase is StagedProjectionPhase.preparing_artifacts
        self.gallery_limits.append(limit)
        items = [
            gallery
            for gallery in self.galleries
            if after is None or gallery.gallery_key > after.gallery_key
        ][:limit]
        return SelectedGalleryPage(
            tuple(items),
            None if not items else items[-1].cursor,
        )

    def page_selected_gallery_files(
        self,
        build_id: str,
        gallery_key: str,
        *,
        after: SelectedGalleryFileCursor | None,
        limit: int,
    ) -> SelectedGalleryFilePage:
        assert build_id == "build-1"
        self.file_limits.append(limit)
        remaining = [
            source_file
            for source_file in self.files[gallery_key]
            if after is None or source_file.cursor > after
        ]
        items = remaining[:limit]
        next_cursor = items[-1].cursor if len(remaining) > len(items) else None
        return SelectedGalleryFilePage(tuple(items), next_cursor)

    def record_prepared_artifacts(
        self,
        build_id: str,
        prepared: Sequence[PreparedProjectionArtifact],
        *,
        batch_id: str,
    ) -> None:
        assert build_id == "build-1"
        assert batch_id.startswith("prepared_artifacts-")
        materialized = tuple(prepared)
        batch_previous = self.prepared_batches.setdefault(batch_id, materialized)
        assert batch_previous == materialized
        self.prepared_batch_lengths.append(len(materialized))
        for value in materialized:
            previous = self.prepared.setdefault(value.gallery_key, value.artifact)
            assert previous == value.artifact
            self.events.append(("record", value.gallery_key))
        if self.fail_record_once_for in {value.gallery_key for value in materialized}:
            self.fail_record_once_for = None
            raise RuntimeError("injected durable artifact sink failure")

    def advance_artifact_checkpoint(
        self,
        build_id: str,
        *,
        expected_after: SelectedGalleryCursor | None,
        after: SelectedGalleryCursor,
        batch_id: str,
    ) -> None:
        assert build_id == "build-1"
        assert batch_id.startswith("artifact_page-")
        assert expected_after == self.artifact_after
        for gallery in self.galleries:
            if gallery.cursor <= after:
                assert gallery.gallery_key in self.prepared
        self.artifact_after = after
        self.events.append(("artifact-checkpoint", after.gallery_key))

    def complete_artifact_preparation(
        self,
        build_id: str,
        *,
        expected_after: SelectedGalleryCursor | None,
    ) -> None:
        assert build_id == "build-1"
        assert expected_after == self.artifact_after
        self.phase = StagedProjectionPhase.staging_selections
        self.events.append(("artifacts-complete", expected_after))

    def page_projection_selections(
        self,
        build_id: str,
        *,
        after: ProjectionSelectionCursor | None,
        limit: int,
    ) -> ProjectionSelectionPage:
        assert build_id == "build-1"
        assert self.phase is StagedProjectionPhase.staging_selections
        self.selection_limits.append(limit)
        items = [
            StagedProjectionSelection(
                gallery_key=gallery.gallery_key,
                artifact=self.prepared.get(gallery.gallery_key),
                redownload_required=gallery.gid % 2 == 0,
            )
            for gallery in self.galleries
            if after is None or gallery.gallery_key > after.gallery_key
        ][:limit]
        return ProjectionSelectionPage(
            tuple(items),
            None if not items else items[-1].cursor,
        )

    def stage_projection_selections(
        self,
        build_id: str,
        selections: Sequence[StagedProjectionSelection],
        *,
        expected_after: ProjectionSelectionCursor | None,
        after: ProjectionSelectionCursor,
        batch_id: str,
    ) -> None:
        assert build_id == "build-1"
        assert batch_id.startswith("projection_selections-")
        assert expected_after == self.selection_after
        materialized = tuple(selections)
        batch_previous = self.selection_batches.setdefault(batch_id, materialized)
        assert batch_previous == materialized
        self.selection_batch_lengths.append(len(selections))
        for selection in selections:
            previous = self.staged.setdefault(selection.gallery_key, selection)
            assert previous == selection
        self.selection_after = after
        self.events.append(("selection-checkpoint", after.gallery_key))

    def complete_projection_staging(
        self,
        build_id: str,
        *,
        expected_after: ProjectionSelectionCursor | None,
    ) -> None:
        assert build_id == "build-1"
        assert expected_after == self.selection_after
        self.phase = StagedProjectionPhase.complete
        self.events.append(("projection-complete", expected_after))


class StreamingCBZStub:
    def __init__(self, adapter: MemoryProjectionAdapter) -> None:
        self._adapter = adapter
        self.requested_gallery_keys: list[str] = []
        self.request_count_by_gallery: dict[str, int] = {}
        self.protected_page_sizes: list[int] = []
        self.protection_ids: list[str] = []
        self.excluded_by_gallery: dict[str, frozenset[str]] = {}
        self.maximum_request_files_held = 0

    @contextmanager
    def publication_guard(self) -> Iterator[None]:
        self._adapter.events.append(("guard-enter", None))
        try:
            yield
        finally:
            self._adapter.events.append(("guard-exit", None))

    def prepare_paged_stream(
        self,
        requests: Iterable[CBZStreamingPreparationRequest],
        *,
        result_sink: Callable[[CBZArtifact], None] | None = None,
        total: int | None = None,
    ) -> CBZPreparationSummary:
        prepared = 0
        for request in requests:
            assert not hasattr(request.metadata, "files")
            gallery = request.metadata.gallery
            gallery_key = next(
                item.gallery_key
                for item in self._adapter.galleries
                if item.gallery_name == gallery.gallery_name and item.gid == gallery.gid
            )
            self.requested_gallery_keys.append(gallery_key)
            self.request_count_by_gallery[gallery_key] = (
                self.request_count_by_gallery.get(gallery_key, 0) + 1
            )
            excluded: set[str] = set()
            file_count = 0
            for preparation_file in request.open_files():
                self.maximum_request_files_held = max(
                    self.maximum_request_files_held,
                    1,
                )
                file_count += 1
                assert preparation_file.file.signature is not None
                if preparation_file.excluded:
                    excluded.add(preparation_file.file.sha256)
            expected_file_count = getattr(self._adapter, "expected_file_count", None)
            if expected_file_count is None:
                expected_file_count = len(self._adapter.files[gallery_key])
            assert file_count == expected_file_count
            self.excluded_by_gallery[gallery_key] = frozenset(excluded)
            digest = _digest(f"cbz-{gallery_key}")
            artifact = CBZArtifact(
                gallery=gallery,
                path=Path("/artifact-store") / f"{gallery.gid}-{digest}.cbz",
                size_bytes=100 + gallery.gid,
                sha256=digest,
                modified_at=next(
                    item.modified_time
                    for item in self._adapter.galleries
                    if item.gallery_key == gallery_key
                ),
                created=True,
                rebuilt=False,
            )
            if result_sink is not None:
                result_sink(artifact)
            prepared += 1
        if total is not None:
            assert prepared == total
        return CBZPreparationSummary(prepared, prepared, 0)

    def protect_for_publish(
        self,
        artifacts: Iterable[CBZArtifact],
        *,
        protection_id: str,
    ) -> None:
        materialized = tuple(artifacts)
        assert materialized
        for artifact in materialized:
            gallery_key = next(
                item.gallery_key
                for item in self._adapter.galleries
                if item.gallery_name == artifact.gallery.gallery_name
                and item.gid == artifact.gallery.gid
            )
            assert ("record", gallery_key) in self._adapter.events
        self.protected_page_sizes.append(len(materialized))
        self.protection_ids.append(protection_id)
        self._adapter.events.append(("protect", len(materialized)))


def test_streams_pages_to_durable_artifacts_then_lightweight_selections(
    tmp_path: Path,
) -> None:
    adapter = MemoryProjectionAdapter(tmp_path, gallery_count=5)
    cbz = StreamingCBZStub(adapter)
    orchestrator = StagedProjectionOrchestrator(
        adapter=adapter,
        cbz=cbz,
        gallery_page_size=2,
        file_page_size=2,
        selection_batch_size=2,
    )

    summary = orchestrator.run("build-1")

    assert summary.artifacts_prepared == 5
    assert summary.artifacts_created == 5
    assert summary.artifacts_rebuilt == 0
    assert summary.selections_staged == 5
    assert adapter.phase is StagedProjectionPhase.complete
    assert adapter.gallery_limits and max(adapter.gallery_limits) == 2
    assert adapter.file_limits and max(adapter.file_limits) == 2
    assert adapter.selection_limits and max(adapter.selection_limits) == 2
    assert max(adapter.selection_batch_lengths) == 2
    assert max(adapter.prepared_batch_lengths) == 2
    assert cbz.protected_page_sizes == [2, 2, 1]
    assert cbz.protection_ids == ["build-1", "build-1", "build-1"]
    assert len(adapter.prepared) == len(adapter.staged) == 5

    for gallery in adapter.galleries:
        selection = adapter.staged[gallery.gallery_key]
        assert not hasattr(selection, "publication_id")
        assert selection.redownload_required is (gallery.gid % 2 == 0)
        assert selection.artifact is not None
        assert selection.artifact.artifact_id == (
            f"urn:h2h:artifact:cbz:{gallery.gid}:sha256:"
            f"{_digest(f'cbz-{gallery.gallery_key}')}"
        )
        assert selection.artifact.name == f"{gallery.gallery_name}.cbz"
        assert selection.artifact.location.name.startswith(f"{gallery.gid}-")
        expected_excluded = frozenset(
            source_file.sha256
            for source_file in adapter.files[gallery.gallery_key]
            if source_file.excluded
        )
        assert cbz.excluded_by_gallery[gallery.gallery_key] == expected_excluded

    event_names = [name for name, _value in adapter.events]
    assert event_names.index("guard-enter") < event_names.index("record")
    assert event_names.index("record") < event_names.index("protect")
    assert event_names.index("protect") < event_names.index("artifact-checkpoint")
    assert event_names.index("artifact-checkpoint") < event_names.index("guard-exit")

    calls_before_retry = len(cbz.requested_gallery_keys)
    assert orchestrator.run("build-1").artifacts_prepared == 0
    assert len(cbz.requested_gallery_keys) == calls_before_retry


def test_restart_replays_only_uncheckpointed_artifact_page(tmp_path: Path) -> None:
    adapter = MemoryProjectionAdapter(tmp_path, gallery_count=3)
    adapter.fail_record_once_for = adapter.galleries[0].gallery_key
    cbz = StreamingCBZStub(adapter)
    orchestrator = StagedProjectionOrchestrator(
        adapter=adapter,
        cbz=cbz,
        gallery_page_size=2,
        file_page_size=3,
        selection_batch_size=2,
    )

    with pytest.raises(RuntimeError, match="durable artifact sink failure"):
        orchestrator.run("build-1")

    first_key = adapter.galleries[0].gallery_key
    assert first_key in adapter.prepared
    assert adapter.artifact_after is None
    assert adapter.phase.value == StagedProjectionPhase.preparing_artifacts.value

    summary = orchestrator.run("build-1")

    assert summary.artifacts_prepared == 3
    assert summary.selections_staged == 3
    assert cbz.request_count_by_gallery[first_key] == 2
    assert all(key in adapter.staged for key in adapter.prepared)
    assert adapter.phase is StagedProjectionPhase.complete


def test_resume_from_selection_checkpoint_skips_cbz_and_prior_batches(
    tmp_path: Path,
) -> None:
    adapter = MemoryProjectionAdapter(
        tmp_path,
        gallery_count=4,
        phase=StagedProjectionPhase.staging_selections,
    )
    first = adapter.galleries[0]
    adapter.selection_after = ProjectionSelectionCursor(first.gallery_key)
    adapter.staged[first.gallery_key] = StagedProjectionSelection(first.gallery_key)
    cbz = StreamingCBZStub(adapter)
    orchestrator = StagedProjectionOrchestrator(
        adapter=adapter,
        cbz=cbz,
        gallery_page_size=1,
        file_page_size=1,
        selection_batch_size=2,
    )

    summary = orchestrator.run("build-1")

    assert summary.artifacts_prepared == 0
    assert summary.selections_staged == 3
    assert cbz.requested_gallery_keys == []
    assert len(adapter.staged) == 4
    assert adapter.phase is StagedProjectionPhase.complete


def test_cbz_disabled_stages_metadata_authority_keys_without_artifacts(
    tmp_path: Path,
) -> None:
    adapter = MemoryProjectionAdapter(tmp_path, gallery_count=3)
    orchestrator = StagedProjectionOrchestrator(
        adapter=adapter,
        cbz=None,
        gallery_page_size=1,
        file_page_size=1,
        selection_batch_size=2,
    )

    summary = orchestrator.run("build-1")

    assert summary.artifacts_prepared == 0
    assert summary.selections_staged == 3
    assert adapter.gallery_limits == []
    assert all(selection.artifact is None for selection in adapter.staged.values())
    assert set(adapter.staged) == {gallery.gallery_key for gallery in adapter.galleries}


class GiantGalleryProjectionAdapter(MemoryProjectionAdapter):
    def __init__(self, tmp_path: Path, *, file_count: int) -> None:
        super().__init__(tmp_path, gallery_count=1)
        self.expected_file_count = file_count
        self.peak_page_materialized = 0
        self.file_rows_constructed = 0

    def page_selected_gallery_files(
        self,
        build_id: str,
        gallery_key: str,
        *,
        after: SelectedGalleryFileCursor | None,
        limit: int,
    ) -> SelectedGalleryFilePage:
        assert build_id == "build-1"
        assert gallery_key == self.galleries[0].gallery_key
        self.file_limits.append(limit)
        start = 0 if after is None else int(after.file_key.removeprefix("file-")) + 1
        stop = min(start + limit, self.expected_file_count)
        items = tuple(
            SelectedGalleryFile(
                gallery_key=gallery_key,
                file_key=f"file-{number:06d}",
                file_sort_key=f"{number:06d}",
                path=self.galleries[0].folder / f"{number:06d}.jpg",
                name=f"{number:06d}.jpg",
                size_bytes=10,
                sha256=_digest(f"giant-file-{number}"),
                signature=FileStatSignature(
                    device=1,
                    inode=number + 1,
                    size_bytes=10,
                    modified_ns=100,
                    changed_ns=200,
                ),
                excluded=number % 997 == 0,
            )
            for number in range(start, stop)
        )
        self.file_rows_constructed += len(items)
        self.peak_page_materialized = max(self.peak_page_materialized, len(items))
        return SelectedGalleryFilePage(
            items,
            None if stop == self.expected_file_count else items[-1].cursor,
        )


def test_giant_gallery_stays_page_bounded_without_gallery_file_hydration(
    tmp_path: Path,
) -> None:
    adapter = GiantGalleryProjectionAdapter(tmp_path, file_count=5_003)
    cbz = StreamingCBZStub(adapter)
    orchestrator = StagedProjectionOrchestrator(
        adapter=adapter,
        cbz=cbz,
        gallery_page_size=1,
        file_page_size=37,
        selection_batch_size=1,
    )

    summary = orchestrator.run("build-1")

    assert summary.artifacts_prepared == 1
    assert adapter.file_rows_constructed == 5_003
    assert adapter.peak_page_materialized <= 37
    assert max(adapter.file_limits) == 37
    assert len(adapter.file_limits) > 100
    assert cbz.maximum_request_files_held == 1


def test_batch_ids_digest_the_complete_artifact_and_selection_payload(
    tmp_path: Path,
) -> None:
    gallery = _gallery(tmp_path, 0)
    digest = _digest("artifact")
    first_artifact = CatalogArtifact(
        artifact_id="urn:h2h:artifact:cbz:1:sha256:" + digest,
        name="first.cbz",
        location=tmp_path / "first.cbz",
        media_type="application/vnd.comicbook+zip",
        size_bytes=10,
        sha256=digest,
        modified_at=gallery.modified_time,
    )
    renamed_artifact = CatalogArtifact(
        artifact_id=first_artifact.artifact_id,
        name="renamed.cbz",
        location=tmp_path / "renamed.cbz",
        media_type=first_artifact.media_type,
        size_bytes=first_artifact.size_bytes,
        sha256=first_artifact.sha256,
        modified_at=first_artifact.modified_at,
    )

    first_id = StagedProjectionOrchestrator._batch_id(
        "build-1",
        "PREPARED_ARTIFACT",
        PreparedProjectionArtifact(gallery.gallery_key, first_artifact),
    )
    renamed_id = StagedProjectionOrchestrator._batch_id(
        "build-1",
        "PREPARED_ARTIFACT",
        PreparedProjectionArtifact(gallery.gallery_key, renamed_artifact),
    )
    normal_selection_id = StagedProjectionOrchestrator._batch_id(
        "build-1",
        "PROJECTION_SELECTIONS",
        (StagedProjectionSelection(gallery.gallery_key, first_artifact, False),),
    )
    redownload_selection_id = StagedProjectionOrchestrator._batch_id(
        "build-1",
        "PROJECTION_SELECTIONS",
        (StagedProjectionSelection(gallery.gallery_key, first_artifact, True),),
    )

    assert first_id != renamed_id
    assert normal_selection_id != redownload_selection_id
