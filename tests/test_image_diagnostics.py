"""Real native warnings preserve image output and exact concurrent identities."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from contextvars import Context
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from threading import Barrier, Thread
from zipfile import ZipFile

import pytest
from h2hdb import ArtifactSourceMember, ArtifactSourceRole, VNextSourceQualification
from PIL import Image
from PIL.TiffImagePlugin import IFDRational

import h2hdb_ingest.source_image as source_module
from h2hdb_ingest.artifact import (
    ArtifactRenderPolicy,
    artifact_policy_fingerprint_sha256,
    render_archive,
)
from h2hdb_ingest.filesystem import FilesystemSource
from h2hdb_ingest.image_diagnostics import (
    SourceImageLogContext,
    _NativeImageDiagnosticFilter,
    current_image_log_context,
    image_log_scope,
    native_image_log_scope,
)
from h2hdb_ingest.image_qualification import ImageGalleryQualifier
from h2hdb_ingest.page_workers import MAX_PAGE_RENDER_WORKERS


def _jpeg_with_unknown_resolution_unit() -> bytes:
    exif = Image.Exif()
    exif[282] = IFDRational(72)
    exif[283] = IFDRational(72)
    exif[296] = 4  # The valid EXIF ResolutionUnit values are 1, 2 and 3.
    destination = BytesIO()
    with Image.new("RGB", (12, 16), "red") as image:
        image.save(destination, format="JPEG", exif=exif)
    return destination.getvalue()


def _member(
    position: int, role: ArtifactSourceRole, name: bytes, content: bytes
) -> ArtifactSourceMember:
    return ArtifactSourceMember(
        position, role, name, sha256(content).digest(), len(content), BytesIO(content)
    )


def _warnings(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [record for record in caplog.records if record.name == "pyvips"]


@pytest.mark.parametrize("workers", (1, 4))
def test_real_exif_warnings_include_worker_identity_and_cbz_is_complete(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch, workers: int
) -> None:
    page = _jpeg_with_unknown_resolution_unit()
    members = (
        _member(
            0, ArtifactSourceRole.METADATA, b"galleryinfo.txt", b"Title: warning\n"
        ),
        _member(1, ArtifactSourceRole.PAGE, b"first.jpg", page),
        _member(2, ArtifactSourceRole.PAGE, b"second.jpg", page),
    )
    destination = BytesIO()
    policy = ArtifactRenderPolicy()
    fingerprint = artifact_policy_fingerprint_sha256(policy)
    with image_log_scope(
        SourceImageLogContext("archive_render", source_root_components=("source",))
    ):
        evidence = render_archive(
            members,
            destination,
            gid=1234,
            policy=policy,
            page_render_workers=workers,
        )
    assert len(evidence.pages) == 2
    with ZipFile(destination) as archive:
        assert archive.namelist() == [
            "galleryinfo.txt",
            "pages/0000.jpg",
            "pages/0001.jpg",
        ]
        for name in archive.namelist()[1:]:
            with Image.open(BytesIO(archive.read(name))) as image:
                image.load()
                assert image.size == (12, 16)
    records = _warnings(caplog)
    assert records
    exact_files: set[str] = set()
    for record in records:
        message = record.getMessage()
        assert record.levelno == logging.WARNING
        assert 'reason="VIPS: unknown EXIF resolution unit"' in message
        assert 'operation="archive_render"' in message
        assert "gid=1234" in message
        assert 'source_root="/source"' in message
        assert "source_sha256=" + sha256(page).hexdigest() in message
        if "source_attribution=exact" in message:
            if 'file="first.jpg"' in message:
                exact_files.add("first.jpg")
                assert "source_position=1" in message
                assert 'file="second.jpg"' not in message
            else:
                exact_files.add("second.jpg")
                assert 'file="second.jpg"' in message
                assert "source_position=2" in message
        else:
            assert "source_attribution=active_candidates" in message
            assert "omitted_source_candidates=0" in message
            assert 'file="first.jpg"' in message or 'file="second.jpg"' in message
        # Core's spool protocol carries no original gallery locator. Never
        # invent one from a last-opened source or assume the folder is the GID.
        assert "gallery_folder=" not in message
    assert exact_files == {"first.jpg", "second.jpg"}
    assert current_image_log_context() is None
    caplog.clear()
    logger = logging.getLogger("pyvips")
    original_filters = logger.filters
    plain_destination = BytesIO()
    with monkeypatch.context() as patch:
        patch.setattr(
            logger,
            "filters",
            [
                item
                for item in original_filters
                if not isinstance(item, _NativeImageDiagnosticFilter)
            ],
        )
        plain_evidence = render_archive(
            members,
            plain_destination,
            gid=1234,
            policy=policy,
            page_render_workers=workers,
        )
    assert logger.filters is original_filters
    assert plain_destination.getvalue() == destination.getvalue()
    assert plain_evidence == evidence
    assert artifact_policy_fingerprint_sha256(policy) == fingerprint
    plain_records = _warnings(caplog)
    assert plain_records
    assert all(
        record.getMessage() == "VIPS: unknown EXIF resolution unit"
        and record.levelno == logging.WARNING
        for record in plain_records
    )
    for member in members:
        member.source.seek(0)
        assert sha256(member.source.read()).digest() == member.expected_sha256


def test_parallel_real_exif_qualification_warnings_name_the_correct_gallery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    page = _jpeg_with_unknown_resolution_unit()
    for gid in (1234, 5678):
        folder = tmp_path / str(gid)
        folder.mkdir()
        (folder / "galleryinfo.txt").write_text(
            "Title: Qualification warning\n"
            "Upload Time: 2024-01-02 03:04\n"
            "Uploaded By: uploader\n"
            "Downloaded: 2024-02-03 04:05\n"
            "Tags: artist:shared\n"
            "Uploader's Comments\n"
            "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3\n",
            encoding="utf-8",
        )
        (folder / f"{gid}.jpg").write_bytes(page)
    rendezvous = Barrier(2)
    original = source_module._read_header

    def read_header(bridge: source_module._SourceBridge) -> source_module._SourceHeader:
        rendezvous.wait(timeout=5)
        return original(bridge)

    monkeypatch.setattr(source_module, "_read_header", read_header)

    def qualify(gid: int) -> VNextSourceQualification:
        with FilesystemSource(tmp_path) as source:
            observation = source.observe_gallery((str(gid),))
            return ImageGalleryQualifier(ArtifactRenderPolicy(), workers=2)(
                source, (str(gid),), observation
            )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(qualify, (1234, 5678)))
    assert results == (VNextSourceQualification(), VNextSourceQualification())
    records = _warnings(caplog)
    assert records
    seen: set[int] = set()
    for record in records:
        message = record.getMessage()
        assert record.levelno == logging.WARNING
        assert "unknown EXIF resolution unit" in message
        assert 'operation="source_image_qualification"' in message
        if "source_attribution=exact" in message:
            gid = 1234 if "gid=1234 " in message else 5678
            seen.add(gid)
            assert f"gid={gid} " in message
            assert f'gallery_folder="{tmp_path / str(gid)}"' in message
            assert f'file="{gid}.jpg"' in message
            other = 5678 if gid == 1234 else 1234
            assert f"gid={other} " not in message
        else:
            assert "source_attribution=active_candidates" in message
            assert "omitted_source_candidates=0" in message
            for gid in (1234, 5678):
                if f"gid={gid} " in message:
                    assert f'gallery_folder="{tmp_path / str(gid)}"' in message
                    assert f'file="{gid}.jpg"' in message
    assert seen == {1234, 5678}


@pytest.mark.parametrize("extra_sources", [0, 4])
def test_native_thread_without_context_reports_bounded_candidates_and_no_stale_source(
    caplog: pytest.LogCaptureFixture,
    extra_sources: int,
) -> None:
    logger = logging.getLogger("pyvips")
    source_count = MAX_PAGE_RENDER_WORKERS + extra_sources
    with ExitStack() as stack:
        for index in range(source_count):
            stack.enter_context(
                image_log_scope(
                    SourceImageLogContext(
                        "source_image_qualification",
                        gid=index,
                        source_root_components=("source",),
                        gallery_locator_components=(str(index),),
                        source_name=b"a\n\xe2\x80\xae" + b"x" * 2048,
                    )
                )
            )
            stack.enter_context(native_image_log_scope())
        # A foreign native worker cannot inherit the Python caller's context.
        worker = Thread(
            target=logger.warning, args=("foreign warning",), context=Context()
        )
        worker.start()
        worker.join(timeout=5)
        assert not worker.is_alive()
    message = _warnings(caplog)[0].getMessage()
    assert "source_attribution=active_candidates" in message
    assert "source_attribution=exact" not in message
    assert f"active_source_count={source_count}" in message
    assert f"omitted_source_candidates={extra_sources}" in message
    assert message.count("gallery_folder=") == MAX_PAGE_RENDER_WORKERS
    for gid in range(MAX_PAGE_RENDER_WORKERS):
        assert f"gid={gid} " in message
    assert "\\n" in message and "\\u202e" in message
    assert "\n" not in message and "\u202e" not in message
    assert "...[truncated]" in message
    assert len(message) < MAX_PAGE_RENDER_WORKERS * 1500
    logger.warning("outside native decode")
    assert _warnings(caplog)[-1].getMessage() == "outside native decode"
    assert current_image_log_context() is None


def test_failed_scope_releases_identity_and_preserves_error_severity(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("pyvips")
    failure = RuntimeError("decode failed")
    with pytest.raises(RuntimeError) as caught:
        with image_log_scope(SourceImageLogContext("archive_render", gid=42)):
            with native_image_log_scope():
                logger.error("native failure")
                raise failure
    assert caught.value is failure
    record = _warnings(caplog)[0]
    assert record.levelno == logging.ERROR
    assert "event=image_decoder_error" in record.getMessage()
    assert "gid=42" in record.getMessage()
    logger.warning("no stale source")
    assert _warnings(caplog)[-1].getMessage() == "no stale source"
