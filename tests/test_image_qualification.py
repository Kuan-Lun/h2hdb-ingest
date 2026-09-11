"""Real source decoding, failure classification and useful gallery diagnostics."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from io import BytesIO
from pathlib import Path
from typing import BinaryIO

import pytest
from h2hdb import VNextSourceQualification
from image_fixtures import write_large_source_png
from PIL import Image

import h2hdb_ingest.image_qualification as qualification_module
from h2hdb_ingest.artifact import ArtifactRenderPolicy, load_source_page_image
from h2hdb_ingest.artifact_errors import (
    format_artifact_failure,
    get_page_failure_context,
)
from h2hdb_ingest.core_source import VNextFilesystemSourceAdapter
from h2hdb_ingest.filesystem import (
    FilesystemFileObservation,
    FilesystemSource,
    FilesystemSourceChangedError,
)
from h2hdb_ingest.image_qualification import ImageGalleryQualifier
from h2hdb_ingest.progress import IngestProgress


def _gallery(root: Path) -> Path:
    folder = root / "1234"
    folder.mkdir(parents=True)
    (folder / "galleryinfo.txt").write_text(
        "Title: Qualification fixture\n"
        "Upload Time: 2024-01-02 03:04\n"
        "Uploaded By: uploader\n"
        "Downloaded: 2024-02-03 04:05\n"
        "Tags: artist:shared, group:shared\n"
        "Uploader's Comments\n"
        "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3\n",
        encoding="utf-8",
    )
    return folder


def _assert_deferred_failure_diagnostic(
    failure: BaseException, folder: Path, caplog: pytest.LogCaptureFixture
) -> None:
    diagnostic = format_artifact_failure(failure)
    assert diagnostic is not None
    assert 'event="gallery_image_check_failed"' in diagnostic
    assert f'gallery_folder="{folder}"' in diagnostic
    assert "gid=1234" in diagnostic
    assert 'file="001.jpg"' in diagnostic
    assert "qualification=not_saved" in diagnostic
    assert diagnostic in failure.__notes__
    page = get_page_failure_context(failure)
    assert page is not None and page.source_name == b"001.jpg"
    assert page.expected_size_bytes == (folder / "001.jpg").stat().st_size
    records = [
        record
        for record in caplog.records
        if "gallery_image_check_failed" in record.message
    ]
    assert len(records) == 1
    assert records[0].levelno == logging.DEBUG
    assert records[0].message == diagnostic
    assert not any(record.levelno >= logging.ERROR for record in caplog.records)


@pytest.mark.parametrize("workers", (1, 4))
def test_real_bad_page_rejects_whole_gallery_with_exact_folder_and_filename(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, workers: int
) -> None:
    source_root = tmp_path / "source"
    folder = _gallery(source_root)
    Image.new("RGB", (8, 12), "red").save(folder / "001.jpg")
    (folder / "002.jpg").write_bytes(b"corrupt encoded image")
    messages: list[str] = []
    progress = IngestProgress(messages.append, interval_seconds=3600)
    work = progress.begin("source")
    (folder / "galleryinfo.txt").touch()
    with FilesystemSource(source_root) as source:
        adapter = VNextFilesystemSourceAdapter(
            source,
            qualify_gallery=ImageGalleryQualifier(
                ArtifactRenderPolicy(), workers=workers, progress=progress
            ),
        )
        observed = adapter.observe_gallery(("1234",))
        assert observed.qualification == VNextSourceQualification(
            accepted=False, reason_code="invalid_image", source_name=b"002.jpg"
        )
        # Rejection is a sealed eligibility fact, never source membership loss.
        assert (
            len(
                adapter.list_file_observations(
                    observed, after_name_bytes=None, limit=128
                ).items
            )
            == 3
        )
    work.finish()
    record = next(
        record
        for record in caplog.records
        if "gallery_image_rejected" in record.message
    )
    assert record.levelname == "WARNING"
    assert f'gallery_folder="{folder}"' in record.message
    assert "gid=1234" in record.message
    assert 'file="002.jpg"' in record.message
    assert "source_bytes=21" in record.message
    assert "other_galleries=continue" in record.message
    assert "source_marker_or_render_policy_change" in record.message
    assert "galleries excluded due to image failures 1" in messages[-1]


def test_valid_source_longer_than_old_limit_is_fully_qualified(tmp_path: Path) -> None:
    root = tmp_path / "source"
    folder = _gallery(root)
    with Image.new("RGB", (32, 10000), "green") as image:
        image.save(folder / "001.png")
    (folder / "galleryinfo.txt").touch()
    with FilesystemSource(root) as source:
        observation = source.observe_gallery(("1234",))
        assert (
            ImageGalleryQualifier(ArtifactRenderPolicy(), workers=2)(
                source, ("1234",), observation
            )
            == VNextSourceQualification()
        )


@pytest.mark.parametrize(
    "failure", (OSError("disk failed"), MemoryError("allocation failed"))
)
def test_resource_errors_are_not_cached_as_bad_images(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger="h2hdb_ingest.image_qualification")
    root = tmp_path / "source"
    folder = _gallery(root)
    Image.new("RGB", (8, 12), "blue").save(folder / "001.jpg")

    def fail(_source: BinaryIO, *, policy: ArtifactRenderPolicy) -> Image.Image:
        del policy
        raise failure

    monkeypatch.setattr(qualification_module, "load_source_page_image", fail)
    (folder / "galleryinfo.txt").touch()
    with FilesystemSource(root) as source:
        observation = source.observe_gallery(("1234",))
        with pytest.raises(type(failure)) as caught:
            ImageGalleryQualifier(ArtifactRenderPolicy(), workers=2)(
                source, ("1234",), observation
            )
    assert caught.value is failure
    _assert_deferred_failure_diagnostic(caught.value, folder, caplog)


def test_source_mutation_during_spooling_remains_retryable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger="h2hdb_ingest.image_qualification")
    root = tmp_path / "source"
    folder = _gallery(root)
    Image.new("RGB", (8, 12), "blue").save(folder / "001.jpg")
    original = FilesystemFileObservation.content_parts
    failure = FilesystemSourceChangedError("source changed after exact read")

    def fail(value: FilesystemFileObservation) -> Iterator[bytes]:
        yield from original(value)
        if value.name_bytes == b"001.jpg":
            raise failure

    monkeypatch.setattr(FilesystemFileObservation, "content_parts", fail)
    (folder / "galleryinfo.txt").touch()
    with FilesystemSource(root) as source:
        observation = source.observe_gallery(("1234",))
        with pytest.raises(FilesystemSourceChangedError) as caught:
            ImageGalleryQualifier(ArtifactRenderPolicy(), workers=2)(
                source, ("1234",), observation
            )
    assert caught.value is failure
    _assert_deferred_failure_diagnostic(caught.value, folder, caplog)


def test_growing_source_stops_at_observed_size_before_spooling_more_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger="h2hdb_ingest.image_qualification")
    root = tmp_path / "source"
    folder = _gallery(root)
    Image.new("RGB", (8, 12), "blue").save(folder / "001.jpg")
    original = FilesystemFileObservation.content_parts

    def grow(value: FilesystemFileObservation) -> Iterator[bytes]:
        yield from original(value)
        yield b"concurrent append"
        raise AssertionError("a changed source must stop before reading more data")

    monkeypatch.setattr(FilesystemFileObservation, "content_parts", grow)
    (folder / "galleryinfo.txt").touch()
    with FilesystemSource(root) as source:
        observed = source.observe_gallery(("1234",))
        with pytest.raises(FilesystemSourceChangedError, match="grew beyond") as caught:
            ImageGalleryQualifier(ArtifactRenderPolicy(), workers=2)(
                source, ("1234",), observed
            )
    _assert_deferred_failure_diagnostic(caught.value, folder, caplog)
    assert "gallery_image_rejected" not in caplog.text


def test_metadata_only_adapter_does_not_decode_images(tmp_path: Path) -> None:
    root = tmp_path / "source"
    folder = _gallery(root)
    (folder / "001.jpg").write_bytes(b"not an image")
    (folder / "galleryinfo.txt").touch()
    with FilesystemSource(root) as source:
        assert (
            VNextFilesystemSourceAdapter(source)
            .observe_gallery(("1234",))
            .qualification.accepted
        )


def test_large_encoded_image_qualifies_from_disk_spool_and_closes_it(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "source"
    folder = _gallery(root)
    size = write_large_source_png(folder / "001.png")
    decoded_streams: list[BinaryIO] = []

    def decode(source: BinaryIO, *, policy: ArtifactRenderPolicy) -> Image.Image:
        # Check before fileno(): that API itself could force a memory spool to disk.
        assert getattr(source, "_rolled", False) is True
        assert source.seek(0, 2) == size
        source.seek(0)
        decoded_streams.append(source)
        return load_source_page_image(source, policy=policy)

    monkeypatch.setattr(qualification_module, "load_source_page_image", decode)
    (folder / "galleryinfo.txt").touch()
    with FilesystemSource(root) as source:
        observed = source.observe_gallery(("1234",))
        assert (
            ImageGalleryQualifier(ArtifactRenderPolicy(), workers=2)(
                source, ("1234",), observed
            )
            == VNextSourceQualification()
        )
    assert len(decoded_streams) == 1 and decoded_streams[0].closed
    assert "gallery_image_rejected" not in caplog.text


def test_later_worker_resource_error_is_not_hidden_by_first_corrupt_image(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from h2hdb_ingest.source_image import SourceImageDecodeError

    root = tmp_path / "source"
    folder = _gallery(root)
    (folder / "001.jpg").write_bytes(b"corrupt")
    (folder / "002.jpg").write_bytes(b"storage")
    failure = OSError("worker storage failure")

    def decode(source: BinaryIO, *, policy: ArtifactRenderPolicy) -> Image.Image:
        del policy
        if source.read() == b"corrupt":
            raise SourceImageDecodeError("known bad image")
        raise failure

    monkeypatch.setattr(qualification_module, "load_source_page_image", decode)
    (folder / "galleryinfo.txt").touch()
    with FilesystemSource(root) as source:
        observed = source.observe_gallery(("1234",))
        with pytest.raises(OSError) as caught:
            ImageGalleryQualifier(ArtifactRenderPolicy(), workers=2)(
                source, ("1234",), observed
            )
    assert caught.value is failure


@pytest.mark.parametrize("fault", ("partial", "corruption", "close_error"))
def test_local_spool_failure_never_becomes_a_bad_source_fact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    fault: str,
) -> None:
    caplog.set_level(logging.DEBUG, logger="h2hdb_ingest.image_qualification")
    root = tmp_path / "source"
    folder = _gallery(root)
    Image.new("RGB", (8, 12), "red").save(folder / "001.jpg")
    streams: list[BytesIO] = []

    class BrokenSpool(BytesIO):
        def write(self, buffer: object) -> int:
            assert isinstance(buffer, bytes)
            if fault in {"partial", "close_error"}:
                return super().write(buffer[:-1])
            return super().write(bytes(len(buffer)))

        def close(self) -> None:
            was_closed = self.closed
            super().close()
            if not was_closed and fault == "close_error":
                raise OSError("secondary flush failure")

    def broken_spool(**_kwargs: object) -> BrokenSpool:
        stream = BrokenSpool()
        streams.append(stream)
        return stream

    monkeypatch.setattr(qualification_module, "SpooledTemporaryFile", broken_spool)
    (folder / "galleryinfo.txt").touch()
    with FilesystemSource(root) as source:
        observed = source.observe_gallery(("1234",))
        with pytest.raises(OSError, match="qualification spool") as caught:
            ImageGalleryQualifier(ArtifactRenderPolicy(), workers=2)(
                source, ("1234",), observed
            )
    assert streams and all(stream.closed for stream in streams)
    if fault == "close_error":
        assert any("secondary flush failure" in note for note in caught.value.__notes__)
    _assert_deferred_failure_diagnostic(caught.value, folder, caplog)
    assert "gallery_image_rejected" not in caplog.text
