"""Progress observes actual adapter work without changing its byte authority."""

from __future__ import annotations

import fcntl
from collections.abc import Buffer
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from threading import Event, Thread
from typing import BinaryIO

import pytest
from h2hdb import (
    ArtifactSourceMember,
    ArtifactSourceRole,
    CatalogResourceKind,
    LibraryActivationStatus,
    StorageObjectDescriptor,
    VNextLibraryActivationItem,
)
from PIL import Image

import h2hdb_ingest.artifact as artifact_module
from h2hdb_ingest.artifact import (
    ArtifactPreparationRenderer,
    ArtifactRenderPolicy,
    PresentationImageError,
)
from h2hdb_ingest.core_source import VNextFilesystemSourceAdapter
from h2hdb_ingest.filesystem import FilesystemSource, FilesystemSourceChangedError
from h2hdb_ingest.library import ManagedFilesystemLibraryAdapter
from h2hdb_ingest.progress import IngestProgress
from h2hdb_ingest.storage import acquisition_storage_key

_POLICY = ArtifactRenderPolicy(max_image_short_side=32)


def _members() -> tuple[ArtifactSourceMember, ...]:
    image = BytesIO()
    with Image.new("RGB", (64, 32), "red") as opened:
        opened.save(image, format="PNG")
    return tuple(
        ArtifactSourceMember(
            position=index,
            role=role,
            source_name=name,
            expected_sha256=sha256(content).digest(),
            expected_size_bytes=len(content),
            source=BytesIO(content),
        )
        for index, (role, name, content) in enumerate(
            (
                (ArtifactSourceRole.METADATA, b"galleryinfo.txt", b"Title: test\n"),
                (ArtifactSourceRole.PAGE, b"one.png", image.getvalue()),
                (ArtifactSourceRole.PAGE, b"two.png", image.getvalue()),
            )
        )
    )


def _counters(progress: IngestProgress) -> dict[str, int]:
    snapshot = progress.snapshot()
    assert snapshot is not None
    return dict(snapshot.counters)


@pytest.mark.parametrize("workers", [1, 2])
def test_progress_preserves_archive_and_thumbnail_bytes(workers: int) -> None:
    reference = ArtifactPreparationRenderer(policy=_POLICY, page_render_workers=workers)
    messages: list[str] = []
    progress = IngestProgress(messages.append)
    progress.begin("publication", announce=False)
    observed = ArtifactPreparationRenderer(
        policy=_POLICY, page_render_workers=workers, progress=progress
    )
    reference_archive, observed_archive = BytesIO(), BytesIO()
    reference_render = reference.render_archive(_members(), reference_archive, gid=42)
    observed_render = observed.render_archive(_members(), observed_archive, gid=42)
    reference_thumbnail, observed_thumbnail = BytesIO(), BytesIO()
    assert reference.render_presentation(
        reference_archive, reference_thumbnail, rendered_pages=reference_render.pages
    ) == observed.render_presentation(
        observed_archive, observed_thumbnail, rendered_pages=observed_render.pages
    )
    assert reference_render == observed_render
    assert reference_archive.getvalue() == observed_archive.getvalue()
    assert reference_thumbnail.getvalue() == observed_thumbnail.getvalue()
    assert _counters(progress) == {
        "pages_rendered": 2,
        "pages_written": 2,
        "archives_rendered": 1,
        "presentations_rendered": 1,
    }
    # Per-page updates and operation details never emit their own INFO records.
    assert messages == []


@pytest.mark.parametrize("replace_work", [False, True])
def test_completed_worker_is_visible_before_slow_first_page_and_zip_write(
    monkeypatch: pytest.MonkeyPatch, replace_work: bool
) -> None:
    progress = IngestProgress(lambda _: None)
    work = progress.begin("publication", announce=False)
    members = _members()
    first_started, release_first, second_completed = Event(), Event(), Event()
    render_page = artifact_module._render_page
    advance = work.advance

    def block_first(
        source: BinaryIO, destination: BinaryIO, *, policy: ArtifactRenderPolicy
    ) -> artifact_module.CanonicalImageEvidence:
        if source is members[1].source:
            first_started.set()
            assert release_first.wait(5)
        return render_page(source, destination, policy=policy)

    def record(counter: str, amount: int = 1) -> None:
        advance(counter, amount)
        if counter == "pages_rendered":
            second_completed.set()

    monkeypatch.setattr(artifact_module, "_render_page", block_first)
    monkeypatch.setattr(work, "advance", record)
    renderer = ArtifactPreparationRenderer(
        policy=_POLICY, page_render_workers=2, progress=progress
    )
    archive = BytesIO()
    failures: list[BaseException] = []

    def render() -> None:
        try:
            renderer.render_archive(members, archive, gid=42)
        except BaseException as error:
            failures.append(error)

    thread = Thread(target=render)
    thread.start()
    try:
        assert first_started.wait(5)
        assert second_completed.wait(5)
        assert _counters(progress) == {"pages_rendered": 1}
        # Metadata may already be in borrowed scratch; no page has been
        # serialized and no completed archive inspection may be published.
        assert archive.getvalue().startswith(b"PK\x03\x04")
        assert renderer._inspection is None
        if replace_work:
            work.finish(announce=False)
            progress.begin("next_batch", announce=False)
    finally:
        release_first.set()
        thread.join(5)
    assert not thread.is_alive()
    assert failures == []
    assert _counters(progress) == (
        {}
        if replace_work
        else {"pages_rendered": 2, "pages_written": 2, "archives_rendered": 1}
    )


class _ZeroWriter(BytesIO):
    def write(self, content: Buffer, /) -> int:
        del content
        return 0


def test_failed_destination_never_counts_completed_archive() -> None:
    progress = IngestProgress(lambda _: None)
    progress.begin("publication", announce=False)
    renderer = ArtifactPreparationRenderer(
        policy=_POLICY, page_render_workers=2, progress=progress
    )
    with pytest.raises(PresentationImageError, match="made no progress"):
        renderer.render_archive(_members(), _ZeroWriter(), gid=42)
    # The first metadata write fails before workers are started.
    assert _counters(progress) == {}


def test_failed_archive_finalization_never_counts_completed_archive() -> None:
    class FailedFlush(BytesIO):
        def flush(self) -> None:
            raise OSError("scratch flush failed")

    progress = IngestProgress(lambda _: None)
    progress.begin("publication", announce=False)
    renderer = ArtifactPreparationRenderer(
        policy=_POLICY, page_render_workers=2, progress=progress
    )
    scratch = FailedFlush()
    with pytest.raises(OSError, match="scratch flush failed"):
        renderer.render_archive(_members(), scratch, gid=42)
    assert _counters(progress) == {"pages_rendered": 2, "pages_written": 2}
    assert scratch.getvalue() == b""
    assert renderer._inspection is None


def _gallery(root: Path, gid: int) -> Path:
    folder = root / str(gid)
    folder.mkdir(parents=True)
    (folder / "galleryinfo.txt").write_text(
        "Title: A title\nUpload Time: 2024-01-02 03:04\nUploaded By: uploader\n"
        "Downloaded: 2024-02-03 04:05\nTags: language:english\n"
        "Uploader's Comments\nA comment\n"
        "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3\n",
        encoding="utf-8",
    )
    (folder / "one.jpg").write_bytes(b"image bytes")
    return folder


def test_source_discovery_advances_before_inventory_finishes_and_counts_reads(
    tmp_path: Path,
) -> None:
    root = tmp_path / "source"
    folders = [_gallery(root, gid) for gid in (1001, 1002, 1003)]
    progress = IngestProgress(lambda _: None)
    work = progress.begin("source", announce=False)
    partial_counts: list[int] = []

    def checkpoint() -> None:
        partial_counts.append(_counters(progress).get("galleries_discovered", 0))

    with FilesystemSource(root, progress=work, checkpoint=checkpoint) as source:
        adapter = VNextFilesystemSourceAdapter(source)
        assert adapter.list_gallery_locators(after_locator=None, limit=1).items == (
            ("1001",),
        )
        assert 1 in partial_counts and 2 in partial_counts
        counters = _counters(progress)
        assert counters["galleries_discovered"] == 3
        assert counters["discovery_directories"] == 4
        assert counters["discovery_directories_verified"] == 4
        assert "gallery_indexes_built" not in counters
        marker = source.observe_completion_marker(("1001",))
        bytes_read = _counters(progress)["source_bytes_read"]
        assert bytes_read == (folders[0] / "galleryinfo.txt").stat().st_size
        assert (
            b"".join(marker.content_parts())
            == (folders[0] / "galleryinfo.txt").read_bytes()
        )
        assert _counters(progress)["source_bytes_read"] == bytes_read
        observed = adapter.observe_gallery(("1001",))
        files = adapter.list_file_observations(
            observed, after_name_bytes=None, limit=128
        )
        assert files.terminal
        assert _counters(progress)["gallery_indexes_built"] == 1
        assert _counters(progress)["file_observations_completed"] == 3
        assert _counters(progress)["source_bytes_read"] > bytes_read
        assert "archives_rendered" not in _counters(progress)


def test_changed_source_keeps_read_bytes_without_claiming_a_complete_observation(
    tmp_path: Path,
) -> None:
    root = tmp_path / "source"
    folder = _gallery(root, 1001)
    progress = IngestProgress(lambda _: None)
    work = progress.begin("source", announce=False)
    with FilesystemSource(root, progress=work) as source:
        _, page = source.list_files(("1001",), after_name=None, limit=128)
        observed = next(item for item in page.items if item.name_bytes == b"one.jpg")
        before = _counters(progress)["source_bytes_read"]
        parts = observed.content_parts()
        assert next(parts) == b"image bytes"
        (folder / "one.jpg").write_bytes(b"changed and longer")
        with pytest.raises(FilesystemSourceChangedError, match="after read"):
            tuple(parts)
        counters = _counters(progress)
        assert counters["source_bytes_read"] > before
        assert "file_observations_completed" not in counters


def _library(
    tmp_path: Path, progress: IngestProgress
) -> ManagedFilesystemLibraryAdapter:
    root = tmp_path / "library"
    for path in (
        root / "current" / "acquisitions",
        root / "current" / "artwork",
        root / ".h2hdb-coordination",
    ):
        path.mkdir(parents=True)
    source = tmp_path / "source"
    source.mkdir()
    return ManagedFilesystemLibraryAdapter(
        root, source_root=source, render_policy=_POLICY, progress=progress
    )


def test_library_reports_lock_wait_without_emitting_or_querying_extra_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    messages: list[str] = []
    progress = IngestProgress(messages.append)
    progress.begin("publication", announce=False)
    adapter = _library(tmp_path, progress)
    flock = fcntl.flock
    waits: list[str | None] = []

    def record_lock(descriptor: int, operation: int) -> None:
        if operation == fcntl.LOCK_EX:
            snapshot = progress.snapshot()
            assert snapshot is not None
            waits.append(snapshot.operation)
        flock(descriptor, operation)

    with adapter.publication_guard():
        monkeypatch.setattr(fcntl, "flock", record_lock)
        adapter.begin(1, b"a" * 16)
        adapter.seal(1)
        assert adapter.reconcile_page(1, b"a" * 16, limit=1).status is (
            LibraryActivationStatus.READY
        )
        adapter.complete(1, b"a" * 16)
    assert "library_publication_lock_wait" in waits
    assert "library_state_lock_wait" in waits
    assert messages == []


def test_library_counts_reconciled_resources_and_stale_removals(tmp_path: Path) -> None:
    progress = IngestProgress(lambda _: None)
    progress.begin("publication", announce=False)
    adapter = _library(tmp_path, progress)
    content = b"protected resource"
    digest = sha256(content).digest()
    modified_at = datetime(2026, 1, 1, tzinfo=UTC)
    key = acquisition_storage_key(42)
    publication_key = sha256(
        b"h2hdb-vnext-publication-key\0"
        + (1).to_bytes(4, "big")
        + (42).to_bytes(8, "big")
    ).digest()
    item = VNextLibraryActivationItem(
        publication_key=publication_key,
        gid=42,
        resource_kind=CatalogResourceKind.ACQUISITION,
        storage_object=StorageObjectDescriptor(
            key=key,
            size_bytes=len(content),
            sha256=digest.hex(),
            modified_at=modified_at,
        ),
    )
    adapter.protect(BytesIO(content), key, digest, len(content), modified_at, b"t" * 32)
    for revision, items in ((1, (item,)), (2, ())):
        receipt = bytes((revision,)) * 16
        with adapter.publication_guard():
            adapter.begin(revision, receipt)
            adapter.activate_page(revision, items)
            adapter.seal(revision)
            for _ in range(3):
                result = adapter.reconcile_page(revision, receipt, limit=1)
                if result.status is LibraryActivationStatus.READY:
                    break
            assert result.status is LibraryActivationStatus.READY
            adapter.complete(revision, receipt)
    assert _counters(progress) == {
        "library_resources_reconciled": 1,
        "library_resources_removed": 1,
    }
