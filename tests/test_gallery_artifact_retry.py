"""Unavailable old bytes postpone dependent publication without replacing current."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pytest
from h2hdb import ArtifactFailureContext, VNextSourceChangedError
from PIL import Image
from test_gallery_local_retry import _archive, _config, _marker, _page, _synchronize

from h2hdb_ingest._retry_diagnostics import retry_diagnostic
from h2hdb_ingest.artifact_errors import attach_qualification_failure_context
from h2hdb_ingest.runtime import build_runtime


def _second_page(folder: Path, color: str, *, stamp: int) -> None:
    path = folder / "002.jpg"
    with Image.new("RGB", (12, 16), color) as image:
        image.save(path)
    os.utime(path, ns=(stamp, stamp))


def _complete(folder: Path, artist: str, *, stamp: int) -> None:
    _marker(folder, stamp=stamp)
    marker = folder / "galleryinfo.txt"
    marker.write_text(
        marker.read_text(encoding="utf-8").replace("artist:shared", f"artist:{artist}"),
        encoding="utf-8",
    )
    os.utime(marker, ns=(stamp, stamp))


def test_global_spam_change_waits_for_unavailable_published_source_then_converges(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    config = _config(tmp_path)
    source = config.paths.download_path
    first, second, third = (source / str(gid) for gid in (1001, 1002, 1003))
    for folder, artist, color in ((first, "ann", "blue"), (second, "ben", "green")):
        _page(folder, "red", stamp=10)
        _second_page(folder, color, stamp=10)
        _complete(folder, artist, stamp=10)
    caplog.set_level(logging.WARNING, logger="h2hdb_ingest")
    with build_runtime(config) as runtime:
        runtime.database_admin.initialize()
        runtime.resident.initialize()
        _synchronize(runtime)
        original_revision = runtime.catalog.get_catalog_revision()
        original_result = runtime.resident.last_synchronization_result
        assert original_revision.publication_count == 2
        publications = runtime.catalog.discover_publications().publications
        assert {item.page_count for item in publications} == {2}
        original_archives = {gid: _archive(tmp_path, gid) for gid in (1001, 1002)}

        # The third artist turns the red page into global spam, requiring new
        # archives even for unchanged/retained observations. H@H is simultaneously
        # replacing the first gallery's remaining page, and has not committed
        # its marker yet. Its published blue bytes are no longer in the source.
        _second_page(first, "purple", stamp=20)
        _page(third, "red", stamp=20)
        _second_page(third, "white", stamp=20)
        _complete(third, "cid", stamp=20)
        caplog.clear()
        for _ in range(128):
            if not runtime.resident.process_available(periodic_scan=True):
                break
        else:
            pytest.fail("dependent artifact source never reached a classified retry")

        assert runtime.resident.last_synchronization_result is original_result
        assert runtime.catalog.get_catalog_revision() == original_revision
        assert {
            gid: _archive(tmp_path, gid) for gid in (1001, 1002)
        } == original_archives
        assert not tuple((tmp_path / "library/current").rglob("h2h-1003.cbz"))
        warnings = [
            record.getMessage()
            for record in caplog.records
            if record.levelno == logging.WARNING
            and "Ingest work will be retried" in record.getMessage()
        ]
        assert len(warnings) == 1
        diagnostic = warnings[0]
        assert 'phase="publication"' in diagnostic
        assert "artifact_source_retry" in diagnostic
        assert "retry_scope=publication_candidate" in diagnostic
        assert "gid=1001" in diagnostic
        assert str(first) in diagnostic
        assert "002.jpg" in diagnostic
        assert "source_bytes=" in diagnostic
        assert "sealed" in diagnostic
        runtime.database_admin.check()

    # A real runtime restart has no process-local source capture or retry cursor.
    # H@H's final marker admits the replacement and the next source turn can
    # render every artifact under the new global analysis, then advance the head.
    _complete(first, "ann", stamp=20)
    with build_runtime(config) as restarted:
        restarted.resident.initialize()
        _synchronize(restarted)
        revision = restarted.catalog.get_catalog_revision()
        assert revision.publication_count == 3
        assert revision != original_revision
        publications = restarted.catalog.discover_publications().publications
        assert {item.page_count for item in publications} == {1}
        assert _archive(tmp_path, 1001) != original_archives[1001]
        assert _archive(tmp_path, 1002) != original_archives[1002]
        assert _archive(tmp_path, 1003)
        restarted.database_admin.check()


def test_retry_member_context_preserves_primary_failure_and_output_bound() -> None:
    failure = VNextSourceChangedError("original sealed-source digest mismatch")
    attach_qualification_failure_context(
        failure,
        ArtifactFailureContext(
            1001,
            ("root",),
            tuple(f"folder\u2028{index}" for index in range(300)),
            b"002.jpg",
            123,
        ),
    )
    for _ in range(20):
        failure.add_note("\u2028" * 5000)
    diagnostic = retry_diagnostic(failure, None).detail
    assert 'error_type="VNextSourceChangedError"' in diagnostic
    assert 'reason="original sealed-source digest mismatch"' in diagnostic
    assert "artifact_context=" in diagnostic
    assert "artifact_source_retry" in diagnostic
    assert "gid=1001" in diagnostic
    assert "diagnostics_truncated=true" in diagnostic
    assert "\u2028" not in diagnostic
    assert "\n" not in diagnostic
    assert len(diagnostic.encode("utf-8")) <= 32 * 1024
