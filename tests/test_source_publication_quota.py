"""Publication admission is independent of bounded source and catalog pages."""

from __future__ import annotations

from pathlib import Path

import pytest
from h2hdb import (
    VNextCurrentOnlyMaintenanceOutcome,
    VNextIngestFacade,
    VNextIngestSourceAdapter,
    VNextIssuedSourceStep,
    VNextPreparedSource,
    VNextPreparedSourceStep,
    VNextResolvedIngestPolicy,
    VNextSourcePreparationObserver,
)
from test_source_preparation_progress import _config

from h2hdb_ingest import ResidentConfig
from h2hdb_ingest.progress import IngestProgress
from h2hdb_ingest.runtime import build_runtime


@pytest.mark.parametrize("quota", (None, 3))
@pytest.mark.parametrize("galleries", (2, 3, 4))
def test_runtime_honors_full_source_and_explicit_quota(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    quota: int | None,
    galleries: int,
) -> None:
    config = _config(tmp_path, galleries=galleries, artifacts=False).model_copy(
        update={"resident": ResidentConfig(publication_batch_galleries=quota)}
    )
    admissions: list[tuple[int | None, int, int]] = []
    pending: dict[VNextPreparedSource, int | None] = {}
    original = VNextIngestFacade.prepare_source
    original_prepare_step = VNextIngestFacade.prepare_source_step

    def prepare(
        facade: VNextIngestFacade,
        adapter: VNextIngestSourceAdapter,
        *,
        policy: VNextResolvedIngestPolicy,
        max_new_galleries: int | None = None,
        reobserve_gallery_locators: tuple[tuple[str, ...], ...] = (),
        reuse_sealed_observations: bool = True,
        progress: VNextSourcePreparationObserver | None = None,
    ) -> VNextPreparedSource:
        result = original(
            facade,
            adapter,
            policy=policy,
            max_new_galleries=max_new_galleries,
            reobserve_gallery_locators=reobserve_gallery_locators,
            reuse_sealed_observations=reuse_sealed_observations,
            progress=progress,
        )
        assert not result.observation_complete
        for count_name in (
            "gallery_count",
            "deferred_gallery_count",
            "waiting_gallery_count",
        ):
            with pytest.raises(ValueError, match="inventory counts are pending"):
                getattr(result, count_name)
        pending[result] = max_new_galleries
        return result

    def prepare_step(
        facade: VNextIngestFacade,
        prepared: VNextPreparedSource,
        issued: VNextIssuedSourceStep,
    ) -> VNextPreparedSourceStep:
        result = original_prepare_step(facade, prepared, issued)
        if prepared.observation_complete and prepared in pending:
            admissions.append(
                (
                    pending.pop(prepared),
                    prepared.gallery_count,
                    prepared.deferred_gallery_count,
                )
            )
        return result

    monkeypatch.setattr(VNextIngestFacade, "prepare_source", prepare)
    monkeypatch.setattr(VNextIngestFacade, "prepare_source_step", prepare_step)
    messages: list[str] = []
    expected = galleries if quota is None else min(galleries, quota)
    with build_runtime(config, event_logger=messages.append) as runtime:
        runtime.database_admin.initialize()
        runtime.resident.initialize()
        assert runtime.resident.process_available(periodic_scan=True)
        revision = runtime.catalog.get_catalog_revision()
        assert revision.publication_count == expected
        assert (
            runtime.catalog.discover_publications(revision=revision).total == expected
        )
        outcome = runtime.resident.last_synchronization_result
        assert outcome is not None
        assert outcome.deferred_gallery_count == galleries - expected
        assert outcome.waiting_gallery_count == 0
        assert runtime.database_admin.check().state == "READY"
    assert not pending
    assert admissions == [(quota, expected, galleries - expected)]
    description = (
        "all complete galleries selected before publication"
        if quota is None
        else "up to 3 new galleries selected before publication"
    )
    assert any(description in message for message in messages)
    assert not any("up to 0 new galleries" in message for message in messages)


def test_full_source_waiting_gallery_does_not_require_another_publication_batch(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, galleries=4, artifacts=False).model_copy(
        update={"resident": ResidentConfig()}
    )
    (config.paths.download_path / "10000" / "galleryinfo.txt").write_bytes(b"")
    with build_runtime(config, event_logger=lambda _message: None) as runtime:
        runtime.database_admin.initialize()
        runtime.resident.initialize()
        assert runtime.resident.process_available(periodic_scan=True)
        outcome = runtime.resident.last_synchronization_result
        assert outcome is not None
        assert outcome.deferred_gallery_count == 0
        assert outcome.waiting_gallery_count == 1
        revision = runtime.catalog.get_catalog_revision()
        assert revision.publication_count == 3
        assert {
            item.gid
            for item in runtime.catalog.discover_publications(
                revision=revision
            ).publications
        } == {10001, 10002, 10003}
        assert runtime.database_admin.check().state == "READY"


@pytest.mark.deep
@pytest.mark.parametrize("galleries", (127, 128, 129, 1024))
def test_complete_inventory_publishes_once_across_page_boundaries(
    tmp_path: Path, galleries: int
) -> None:
    config = _config(tmp_path, galleries=galleries, artifacts=False).model_copy(
        update={"resident": ResidentConfig()}
    )
    with build_runtime(config, event_logger=lambda _message: None) as runtime:
        runtime.database_admin.initialize()
        runtime.resident.initialize()
        assert runtime.resident.process_available(periodic_scan=True)
        revision = runtime.catalog.get_catalog_revision()
        assert revision.publication_count == galleries
        gids: set[int] = set()
        cursor = None
        while True:
            page = runtime.catalog.discover_publications(
                revision=revision, limit=128, after=cursor
            )
            assert len(page.publications) <= 128
            for publication in page.publications:
                assert publication.gid not in gids
                gids.add(publication.gid)
            cursor = page.next_cursor
            if cursor is None:
                break
        assert gids == set(range(10_000, 10_000 + galleries))
        for _attempt in range(1024):
            outcome = runtime.facade.drain_current_only_maintenance(30_000_000)
            if outcome is VNextCurrentOnlyMaintenanceOutcome.DONE:
                break
            assert outcome is VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
        else:
            pytest.fail("complete inventory did not reach bounded cleanup DONE")
        assert runtime.database_admin.check().state == "READY"


def test_info_full_source_policy_has_no_numeric_quota() -> None:
    messages: list[str] = []
    progress = IngestProgress(messages.append)
    work = progress.begin("policy", announce=False)
    work.set_counter("full_source_selection", 1)
    work.phase("source")
    assert messages == [
        "Ingest stage started: Preparing gallery data for this batch; "
        "all complete galleries selected before publication",
    ]
    progress.close()
