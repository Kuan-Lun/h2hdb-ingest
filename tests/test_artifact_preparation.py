"""Byte authority and restart refinement for bounded inspection reuse."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import BinaryIO
from zipfile import ZipFile

import pytest
from h2hdb import ArtifactRenderedPage, ArtifactSourceMember, ArtifactSourceRole
from PIL import Image, ImageOps

import h2hdb_ingest.artifact as artifact_module
from h2hdb_ingest.artifact import (
    ArtifactPreparationRenderer,
    ArtifactRenderPolicy,
    PresentationImageError,
    canonical_page_member_name,
    inspect_presentation_archive,
    render_archive,
    render_presentation,
)

_POLICY = ArtifactRenderPolicy(max_image_short_side=32)
_PAGES = (ArtifactRenderedPage(0, 1, canonical_page_member_name(0)),)


def _jpeg() -> bytes:
    output = BytesIO()
    with Image.new("RGB", (64, 32), "red") as source:
        source.save(output, format="JPEG", quality=95)
    return output.getvalue()


def _members() -> tuple[ArtifactSourceMember, ...]:
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
                (ArtifactSourceRole.PAGE, b"page.jpg", _jpeg()),
            )
        )
    )


def _renderer() -> ArtifactPreparationRenderer:
    return ArtifactPreparationRenderer(policy=_POLICY, page_render_workers=1)


def test_same_preparation_reuses_only_one_fully_decoded_inspection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected_archive, expected_thumbnail = BytesIO(), BytesIO()
    expected_render = render_archive(
        _members(),
        expected_archive,
        gid=42,
        policy=_POLICY,
        page_render_workers=1,
    )
    expected_presentation = render_presentation(
        expected_archive,
        expected_thumbnail,
        rendered_pages=_PAGES,
        policy=_POLICY,
    )
    calls: list[bytes] = []
    verify = artifact_module._verify_canonical_jpeg

    def track(content: bytes) -> artifact_module.CanonicalImageEvidence:
        calls.append(sha256(content).digest())
        return verify(content)

    monkeypatch.setattr(artifact_module, "_verify_canonical_jpeg", track)
    renderer = _renderer()
    archive, thumbnail = BytesIO(), BytesIO()
    rendered = renderer.render_archive(_members(), archive, gid=42)
    # Presentation receives a new wrapper/copy, exactly as core's boundaries do.
    presentation = renderer.render_presentation(
        BytesIO(archive.getvalue()),
        thumbnail,
        rendered_pages=rendered.pages,
    )
    assert len(calls) == 1
    assert archive.getvalue() == expected_archive.getvalue()
    assert thumbnail.getvalue() == expected_thumbnail.getvalue()
    assert rendered == expected_render
    assert presentation == expected_presentation
    with ZipFile(BytesIO(archive.getvalue())) as opened:
        assert opened.read(canonical_page_member_name(0)) != _jpeg()
    # An adjacent-use optimization must not turn into a retained gallery cache.
    replay_thumbnail = BytesIO()
    assert (
        renderer.render_presentation(
            BytesIO(archive.getvalue()),
            replay_thumbnail,
            rendered_pages=rendered.pages,
        )
        == expected_presentation
    )
    assert len(calls) == 2
    assert replay_thumbnail.getvalue() == expected_thumbnail.getvalue()


def test_jpeg_validation_fully_decodes_without_pixel_transform_or_copy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    content = _jpeg()

    def forbidden(*args: object, **kwargs: object) -> Image.Image:
        del args, kwargs
        raise AssertionError("verification must not copy or transform pixels")

    monkeypatch.setattr(ImageOps, "exif_transpose", forbidden)
    monkeypatch.setattr(Image.Image, "copy", forbidden)
    evidence = artifact_module._verify_canonical_jpeg(content)
    assert (evidence.width, evidence.height) == (64, 32)
    assert evidence.sha256 == sha256(content).digest()
    # Keep JPEG framing and dimensions, but corrupt the scan header length.
    # Pillow opens this lazily; only the full decoder rejects its scan data.
    scan = content.index(b"\xff\xda")
    malformed = content[:scan] + b"\xff\xda\x00\x08" + content[scan + 4 :]
    with pytest.raises(PresentationImageError, match="truncated or invalid"):
        artifact_module._verify_canonical_jpeg(malformed)


@pytest.mark.parametrize("orientation", [1, 3, 5, 6, 7, 8])
def test_verification_preserves_oriented_dimensions_without_transposing(
    orientation: int,
) -> None:
    output = BytesIO()
    exif = Image.Exif()
    exif[0x0112] = orientation
    with Image.new("RGB", (64, 32), "blue") as source:
        source.save(output, format="JPEG", exif=exif)
    content = output.getvalue()
    previous = artifact_module._load_safe_image(BytesIO(content))
    try:
        actual = artifact_module._verify_canonical_jpeg(content)
        assert (actual.width, actual.height) == previous.size
    finally:
        previous.close()


@pytest.mark.parametrize("change", ["crc", "truncate", "append", "valid_metadata"])
def test_changed_actual_archive_never_uses_retained_inspection(
    change: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    renderer = _renderer()
    archive = BytesIO()
    rendered = renderer.render_archive(_members(), archive, gid=42)
    original = archive.getvalue()
    inspected = inspect_presentation_archive(
        BytesIO(original), (canonical_page_member_name(0),)
    )
    changed = bytearray(original)
    if change == "crc":
        changed[inspected.pages[0].byte_offset + 10] ^= 1
    elif change == "truncate":
        del changed[-10:]
    elif change == "append":
        changed += b"untrusted extra bytes"
    else:
        with ZipFile(BytesIO(original)) as source:
            replacement = BytesIO()
            with ZipFile(replacement, "w") as target:
                for info in source.infolist():
                    content = source.read(info.filename)
                    if info.filename == "galleryinfo.txt":
                        content = b"Title: best\n"
                    target.writestr(info, content)
            changed = bytearray(replacement.getvalue())
        assert len(changed) == len(original)
    calls = 0
    inspect = artifact_module.inspect_presentation_archive

    def track(
        stream: BinaryIO, names: tuple[str, ...]
    ) -> artifact_module.PreparedPresentationEvidence:
        nonlocal calls
        calls += 1
        return inspect(stream, names)

    monkeypatch.setattr(artifact_module, "inspect_presentation_archive", track)
    thumbnail = BytesIO()
    if change == "valid_metadata":
        renderer.render_presentation(
            BytesIO(changed), thumbnail, rendered_pages=rendered.pages
        )
        assert thumbnail.getvalue()
    else:
        with pytest.raises(PresentationImageError):
            renderer.render_presentation(
                BytesIO(changed), thumbnail, rendered_pages=rendered.pages
            )
        assert thumbnail.getvalue() == b""
    assert calls == 1


def test_failed_destination_copy_does_not_publish_inspection_slot() -> None:
    class FailedWriter(BytesIO):
        def flush(self) -> None:
            raise OSError("destination failed after accepting bytes")

    renderer = _renderer()
    destination = FailedWriter()
    with pytest.raises(OSError, match="destination failed"):
        renderer.render_archive(_members(), destination, gid=42)
    assert renderer._inspection is None


def test_cover_is_rehashed_after_inspection_before_thumbnail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    renderer = _renderer()
    archive = BytesIO()
    rendered = renderer.render_archive(_members(), archive, gid=42)
    read_extent = artifact_module._read_extent

    def change(stream: BinaryIO, *, offset: int, size: int) -> bytes:
        content = bytearray(read_extent(stream, offset=offset, size=size))
        content[10] ^= 1
        return bytes(content)

    monkeypatch.setattr(artifact_module, "_read_extent", change)
    thumbnail = BytesIO()
    with pytest.raises(PresentationImageError, match="cover changed"):
        renderer.render_presentation(archive, thumbnail, rendered_pages=rendered.pages)
    assert thumbnail.getvalue() == b""


_CRASH_RENDER = r"""
import os
import signal
import sys
from hashlib import sha256
from io import BytesIO
from h2hdb import ArtifactSourceMember, ArtifactSourceRole
from h2hdb_ingest.artifact import ArtifactPreparationRenderer, ArtifactRenderPolicy
from PIL import Image
output = BytesIO()
with Image.new("RGB", (64, 32), "red") as image:
    image.save(output, format="JPEG", quality=95)
members = tuple(
    ArtifactSourceMember(index, role, name, sha256(content).digest(), len(content), BytesIO(content))
    for index, (role, name, content) in enumerate((
        (ArtifactSourceRole.METADATA, b"galleryinfo.txt", b"Title: test\n"),
        (ArtifactSourceRole.PAGE, b"page.jpg", output.getvalue()),
    ))
)
renderer = ArtifactPreparationRenderer(policy=ArtifactRenderPolicy(max_image_short_side=32), page_render_workers=1)
with open(sys.argv[1], "w+b") as destination:
    if sys.argv[2] == "partial":
        class InterruptedCopy:
            def seek(self, *args): return destination.seek(*args)
            def truncate(self, *args): return destination.truncate(*args)
            def write(self, content):
                destination.write(content[:len(content) // 2])
                destination.flush()
                os.fsync(destination.fileno())
                os.kill(os.getpid(), int(sys.argv[3]))
        renderer.render_archive(members, InterruptedCopy(), gid=42)
    else:
        renderer.render_archive(members, destination, gid=42)
        destination.flush()
        os.fsync(destination.fileno())
        os.kill(os.getpid(), int(sys.argv[3]))
"""


@pytest.mark.skipif(os.name != "posix", reason="POSIX process termination boundaries")
@pytest.mark.parametrize("termination", [signal.SIGTERM, signal.SIGKILL])
@pytest.mark.parametrize("phase", ["partial", "complete"])
def test_process_termination_restarts_without_inspection_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    termination: signal.Signals,
    phase: str,
) -> None:
    path = tmp_path / "archive.cbz"
    completed = subprocess.run(
        [sys.executable, "-c", _CRASH_RENDER, str(path), phase, str(int(termination))],
        check=False,
        capture_output=True,
        timeout=30,
    )
    assert completed.returncode == -termination, completed.stderr.decode()
    calls = 0
    inspect = artifact_module.inspect_presentation_archive

    def track(
        stream: BinaryIO, names: tuple[str, ...]
    ) -> artifact_module.PreparedPresentationEvidence:
        nonlocal calls
        calls += 1
        return inspect(stream, names)

    monkeypatch.setattr(artifact_module, "inspect_presentation_archive", track)
    restarted = _renderer()
    if phase == "partial":
        with path.open("rb") as damaged, pytest.raises(PresentationImageError):
            restarted.render_presentation(damaged, BytesIO(), rendered_pages=_PAGES)
        assert calls == 1
        with path.open("r+b") as destination:
            restarted.render_archive(_members(), destination, gid=42)
    thumbnail = BytesIO()
    before = calls
    with path.open("rb") as archive:
        restarted.render_presentation(archive, thumbnail, rendered_pages=_PAGES)
    assert calls == before + (phase == "complete")
    reference, reference_thumbnail = BytesIO(), BytesIO()
    render_archive(_members(), reference, gid=42, policy=_POLICY, page_render_workers=1)
    render_presentation(
        reference, reference_thumbnail, rendered_pages=_PAGES, policy=_POLICY
    )
    assert path.read_bytes() == reference.getvalue()
    assert thumbnail.getvalue() == reference_thumbnail.getvalue()
