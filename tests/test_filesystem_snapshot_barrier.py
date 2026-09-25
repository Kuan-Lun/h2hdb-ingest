"""Real public Core freeze cannot accept a gallery changed after its last page."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from h2hdb import TagObservation, VNextIngestGalleryObservation, VNextIngestPage
from test_source_preparation_progress import _config

from h2hdb_ingest.core_source import VNextFilesystemSourceAdapter
from h2hdb_ingest.runtime import build_runtime


@pytest.mark.parametrize("artifacts", (False, True))
@pytest.mark.parametrize("mutation", ("page", "marker", "add"))
def test_final_complete_audit_rejects_post_page_mutation_before_core_seal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    artifacts: bool,
    mutation: str,
) -> None:
    """FILE, DIRECTORY and TAG pages can all succeed without sealing a snapshot.

    Edit an already returned file after the last TAG page, when no later bounded
    page can detect it. Both metadata-only and qualified image pipelines must
    defer this gallery at the final full audit and publish no mixed observation.
    """
    config = _config(tmp_path, galleries=1, artifacts=artifacts)
    original = VNextFilesystemSourceAdapter.list_tag_observations
    changed = False

    def mutate_after_last_page(
        adapter: VNextFilesystemSourceAdapter,
        observation: VNextIngestGalleryObservation,
        *,
        after_ordinal: int | None,
        limit: int,
    ) -> VNextIngestPage[TagObservation]:
        nonlocal changed
        result = original(
            adapter, observation, after_ordinal=after_ordinal, limit=limit
        )
        if result.terminal and not changed:
            changed = True
            folder = config.paths.download_path / "10000"
            if mutation == "add":
                (folder / "new.txt").write_bytes(b"not part of frozen directory")
            else:
                path = folder / ("001.jpg" if mutation == "page" else "galleryinfo.txt")
                before = path.stat()
                data = path.read_bytes()
                path.write_bytes(data[:-1] + bytes((data[-1] ^ 1,)))
                # Parent directory and file mtime can remain unchanged. The
                # final audit must still compare the complete entry signatures.
                os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        return result

    monkeypatch.setattr(
        VNextFilesystemSourceAdapter, "list_tag_observations", mutate_after_last_page
    )
    with build_runtime(config, event_logger=lambda _message: None) as runtime:
        runtime.database_admin.initialize()
        runtime.resident.initialize()
        assert runtime.resident.process_available(periodic_scan=True)
        assert changed
        result = runtime.resident.last_synchronization_result
        assert result is not None
        assert result.waiting_gallery_count == 1
        assert result.deferred_gallery_count == 0
        revision = runtime.catalog.get_catalog_revision()
        assert revision.publication_count == 0
        assert runtime.catalog.discover_publications(revision=revision).total == 0
        assert runtime.database_admin.check().state == "READY"
