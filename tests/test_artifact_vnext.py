from __future__ import annotations

import random
import struct
import warnings
from collections.abc import Buffer
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from threading import Barrier, Event, Lock, Thread
from typing import BinaryIO, cast
from zipfile import ZIP_DEFLATED, ZIP_STORED, ZipFile, ZipInfo

import pytest
from h2hdb import (
    ArtifactReleaseAdapter,
    ArtifactRenderedPage,
    ArtifactSourceMember,
    ArtifactSourceRole,
    ArtifactStorageAdapter,
    CatalogResourceKind,
    LibraryActivationStatus,
    VNextLibraryActivationItem,
)
from PIL import Image

import h2hdb_ingest.artifact as artifact_module
from h2hdb_ingest.artifact import (
    MAX_ARCHIVE_SIZE_BYTES,
    MAX_DECODED_PIXELS,
    MAX_ENCODED_PAGE_BYTES,
    MAX_IMAGE_LONG_SIDE,
    MAX_PAGE_COUNT,
    MAX_PAGE_RENDER_WORKERS,
    PAGE_JPEG_QUALITY,
    THUMBNAIL_JPEG_QUALITY,
    THUMBNAIL_MAX_SIDE,
    ArtifactImageResampler,
    ArtifactRenderPolicy,
    PresentationImageError,
    artifact_policy_fingerprint_sha256,
    canonical_page_member_name,
    inspect_presentation_archive,
)
from h2hdb_ingest.library import ManagedFilesystemLibraryAdapter
from h2hdb_ingest.metrics import IngestMetric
from h2hdb_ingest.page_workers import resolve_page_render_workers


class _PartialWriter(BytesIO):
    def write(self, content: Buffer, /) -> int:
        view = memoryview(content)
        return super().write(view[: max(1, len(view) // 2)])


class _ZeroWriter(BytesIO):
    def write(self, content: Buffer, /) -> int:
        del content
        return 0


class _NoReadWriter(BytesIO):
    def read(self, size: int | None = -1) -> bytes:
        del size
        raise AssertionError("destination must never be read")


class _ReadSpy(BytesIO):
    def __init__(self, content: bytes) -> None:
        super().__init__(content)
        self.read_count = 0

    def read(self, size: int | None = -1) -> bytes:
        self.read_count += 1
        return super().read(size)


def _source_member(
    position: int,
    role: ArtifactSourceRole,
    name: bytes,
    content: bytes,
) -> ArtifactSourceMember:
    return ArtifactSourceMember(
        position=position,
        role=role,
        source_name=name,
        expected_sha256=sha256(content).digest(),
        expected_size_bytes=len(content),
        source=BytesIO(content),
    )


def _zip_info(name: str, compression: int) -> ZipInfo:
    info = ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = compression
    info.create_system = 3
    info.external_attr = 0o100644 << 16
    return info


def _canonical_archive(*pages: bytes) -> tuple[bytes, tuple[str, ...]]:
    destination = BytesIO()
    names = tuple(canonical_page_member_name(index) for index in range(len(pages)))
    with ZipFile(destination, "w") as writer:
        writer.writestr(
            _zip_info("galleryinfo.txt", ZIP_DEFLATED),
            b"Title: test\n",
        )
        for name, content in zip(names, pages, strict=True):
            writer.writestr(_zip_info(name, ZIP_STORED), content)
    return destination.getvalue(), names


def _provision_library_root(root: Path) -> None:
    for path in (
        root / "current" / "acquisitions",
        root / "current" / "artwork",
        root / ".h2hdb-coordination",
    ):
        path.mkdir(parents=True, exist_ok=True)


def _adapter(
    tmp_path: Path, *, max_image_short_side: int
) -> ManagedFilesystemLibraryAdapter:
    source_root = tmp_path / "download"
    source_root.mkdir(exist_ok=True)
    return ManagedFilesystemLibraryAdapter(
        tmp_path / "library",
        source_root=source_root,
        render_policy=ArtifactRenderPolicy(max_image_short_side=max_image_short_side),
    )


def _rendered_page_bytes(
    adapter: ManagedFilesystemLibraryAdapter,
    content: bytes,
    *,
    source_name: bytes = b"page.png",
    destination: BinaryIO | None = None,
) -> bytes:
    archive = destination or BytesIO()
    adapter.render_archive(
        (
            _source_member(
                2,
                ArtifactSourceRole.METADATA,
                b"galleryinfo.txt",
                b"Title: test\n",
            ),
            _source_member(9, ArtifactSourceRole.PAGE, source_name, content),
        ),
        archive,
        gid=42,
    )
    if not isinstance(archive, BytesIO):
        return b""
    with ZipFile(BytesIO(archive.getvalue())) as opened:
        return opened.read(canonical_page_member_name(0))


def _exercise_render_policy(
    tmp_path: Path,
    policy: ArtifactRenderPolicy,
    *,
    metrics: list[IngestMetric] | None = None,
) -> tuple[bytes, bytes]:
    source_root = tmp_path / "download"
    source_root.mkdir(exist_ok=True)
    adapter = ManagedFilesystemLibraryAdapter(
        tmp_path / "library",
        source_root=source_root,
        render_policy=policy,
        metrics_sink=None if metrics is None else metrics.append,
    )
    source = BytesIO()
    Image.new("RGB", (12, 6), (17, 91, 203)).save(source, format="PNG")
    archive = BytesIO()
    rendered = adapter.render_archive(
        (
            _source_member(
                1,
                ArtifactSourceRole.METADATA,
                b"galleryinfo.txt",
                b"Title: policy execution\n",
            ),
            _source_member(
                2,
                ArtifactSourceRole.PAGE,
                b"page.png",
                source.getvalue(),
            ),
        ),
        archive,
        gid=42,
    )
    thumbnail = BytesIO()
    adapter.render_presentation(
        BytesIO(archive.getvalue()),
        thumbnail,
        rendered_pages=rendered.pages,
    )
    with ZipFile(BytesIO(archive.getvalue())) as opened:
        page = opened.read(canonical_page_member_name(0))
    return page, thumbnail.getvalue()


def _activate(
    adapter: ManagedFilesystemLibraryAdapter,
    revision: int,
    items: tuple[VNextLibraryActivationItem, ...],
) -> None:
    receipt = bytes((revision,)) * 16
    with adapter.publication_guard():
        adapter.begin(revision, receipt)
        adapter.activate_page(revision, items)
        adapter.seal(revision)
        while True:
            checkpoint = adapter.reconcile_page(revision, receipt, limit=128)
            if checkpoint.status is LibraryActivationStatus.READY:
                break
        adapter.complete(revision, receipt)


def _publication_key(gid: int) -> bytes:
    digest = sha256(b"h2hdb-vnext-publication-key\0")
    digest.update((1).to_bytes(4, "big"))
    digest.update(gid.to_bytes(8, "big"))
    return digest.digest()


def test_adapter_policy_fingerprint_is_deterministic_and_configuration_bound() -> None:
    assert artifact_policy_fingerprint_sha256(ArtifactRenderPolicy()) == (
        artifact_policy_fingerprint_sha256(ArtifactRenderPolicy())
    )
    assert artifact_policy_fingerprint_sha256(ArtifactRenderPolicy()) != (
        artifact_policy_fingerprint_sha256(
            ArtifactRenderPolicy(max_image_short_side=769)
        )
    )
    assert artifact_policy_fingerprint_sha256(ArtifactRenderPolicy()) != (
        artifact_policy_fingerprint_sha256(ArtifactRenderPolicy(page_jpeg_quality=89))
    )
    assert artifact_policy_fingerprint_sha256(ArtifactRenderPolicy()) != (
        artifact_policy_fingerprint_sha256(ArtifactRenderPolicy(optimize=False))
    )
    assert artifact_policy_fingerprint_sha256(ArtifactRenderPolicy()) != (
        artifact_policy_fingerprint_sha256(
            ArtifactRenderPolicy(resampler=ArtifactImageResampler.BICUBIC)
        )
    )


@pytest.mark.parametrize("quality", (-1, 96))
def test_render_policy_domain_rejects_unbounded_quality(quality: int) -> None:
    with pytest.raises(ValueError, match="quality"):
        ArtifactRenderPolicy(page_jpeg_quality=quality)
    with pytest.raises(ValueError, match="quality"):
        ArtifactRenderPolicy(thumbnail_jpeg_quality=quality)


def test_render_policy_domain_rejects_foreign_optimize_and_resampler() -> None:
    with pytest.raises(TypeError, match="optimize"):
        ArtifactRenderPolicy(optimize=cast(bool, 1))
    with pytest.raises(TypeError, match="resampler"):
        ArtifactRenderPolicy(resampler=cast(ArtifactImageResampler, "spline"))


@pytest.mark.parametrize("quality", range(96))
def test_every_supported_jpeg_quality_completes_page_and_thumbnail_rendering(
    tmp_path: Path,
    quality: int,
) -> None:
    page, thumbnail = _exercise_render_policy(
        tmp_path,
        ArtifactRenderPolicy(
            max_image_short_side=4,
            page_jpeg_quality=quality,
            thumbnail_jpeg_quality=quality,
        ),
    )

    assert page.startswith(b"\xff\xd8") and page.endswith(b"\xff\xd9")
    assert thumbnail.startswith(b"\xff\xd8") and thumbnail.endswith(b"\xff\xd9")


@pytest.mark.parametrize("optimize", (False, True))
@pytest.mark.parametrize("resampler", tuple(ArtifactImageResampler))
def test_every_optimize_resampler_combination_completes_normal_rendering(
    tmp_path: Path,
    optimize: bool,
    resampler: ArtifactImageResampler,
) -> None:
    page, thumbnail = _exercise_render_policy(
        tmp_path,
        ArtifactRenderPolicy(
            max_image_short_side=4,
            optimize=optimize,
            resampler=resampler,
        ),
    )

    assert page
    assert thumbnail


def test_default_policy_preserves_exact_archive_page_and_thumbnail_bytes(
    tmp_path: Path,
) -> None:
    source = BytesIO()
    image = Image.new("RGB", (1000, 400))
    image.putdata(
        [
            (
                (x * 17 + y * 3) % 256,
                (x * 5 + y * 11) % 256,
                (x * 7 + y * 13) % 256,
            )
            for y in range(400)
            for x in range(1000)
        ]
    )
    image.save(source, format="PNG")
    image.close()
    source_root = tmp_path / "download"
    source_root.mkdir()
    adapter = ManagedFilesystemLibraryAdapter(
        tmp_path / "library",
        source_root=source_root,
        render_policy=ArtifactRenderPolicy(max_image_short_side=200),
    )
    archive = BytesIO()
    rendered = adapter.render_archive(
        (
            _source_member(
                1,
                ArtifactSourceRole.METADATA,
                b"galleryinfo.txt",
                b"Title: exact-default-regression\n",
            ),
            _source_member(
                2,
                ArtifactSourceRole.PAGE,
                b"page.png",
                source.getvalue(),
            ),
        ),
        archive,
        gid=42,
    )
    thumbnail = BytesIO()
    adapter.render_presentation(
        BytesIO(archive.getvalue()),
        thumbnail,
        rendered_pages=rendered.pages,
    )
    with ZipFile(BytesIO(archive.getvalue())) as opened:
        page = opened.read(canonical_page_member_name(0))

    assert len(archive.getvalue()) == 73774
    assert sha256(archive.getvalue()).hexdigest() == (
        "f10e6052d0c3c017fa9665bac5fbabb982ca2529c63cbdf3b05f48c3e1544cbd"
    )
    assert len(thumbnail.getvalue()) == 23572
    assert sha256(thumbnail.getvalue()).hexdigest() == (
        "03144bad897da9d3cbf8d0c2f90219e387802fa5c7b0a9dd0eb31c83ea67ad04"
    )
    assert sha256(page).hexdigest() == (
        "59da0bda799ce96a8b5714583a4737094d9ba449a9e716c773a387ceb1bf9fcc"
    )


def test_artifact_metrics_split_render_inspect_copy_and_thumbnail_phases(
    tmp_path: Path,
) -> None:
    metrics: list[IngestMetric] = []

    _exercise_render_policy(
        tmp_path,
        ArtifactRenderPolicy(max_image_short_side=4),
        metrics=metrics,
    )

    assert [metric.operation for metric in metrics] == [
        "render_archive",
        "render_presentation",
    ]
    assert [value.name for value in metrics[0].phases_ns] == [
        "render_pages",
        "archive_inspect",
        "archive_copy",
    ]
    assert [value.name for value in metrics[1].phases_ns] == [
        "archive_inspect",
        "presentation",
        "thumbnail",
    ]
    assert all(
        value.value >= 0
        for metric in metrics
        for value in (*metric.phases_ns, *metric.counters)
    )


def test_single_library_implements_storage_and_release_ports(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path, max_image_short_side=20)

    assert isinstance(adapter, ArtifactStorageAdapter)
    assert isinstance(adapter, ArtifactReleaseAdapter)


def test_open_source_is_bound_to_exact_no_follow_root(tmp_path: Path) -> None:
    source_root = tmp_path / "download"
    gallery = source_root / "nested" / "gallery"
    gallery.mkdir(parents=True)
    (gallery / "page.jpg").write_bytes(b"exact source")
    adapter = ManagedFilesystemLibraryAdapter(
        tmp_path / "library",
        source_root=source_root,
        render_policy=ArtifactRenderPolicy(max_image_short_side=20),
    )
    root_components = tuple(source_root.resolve().parts[1:])

    with adapter.open_source(
        source_root_components=root_components,
        gallery_locator_components=("nested", "gallery"),
        source_name=b"page.jpg",
    ) as opened:
        assert opened.read() == b"exact source"

    with pytest.raises(RuntimeError, match="another configured root"):
        adapter.open_source(
            source_root_components=("foreign",),
            gallery_locator_components=("nested", "gallery"),
            source_name=b"page.jpg",
        )
    with pytest.raises(ValueError, match="unsafe"):
        adapter.open_source(
            source_root_components=root_components,
            gallery_locator_components=("..",),
            source_name=b"page.jpg",
        )


def test_open_source_rejects_symlinks_and_root_replacement(tmp_path: Path) -> None:
    source_root = tmp_path / "download"
    gallery = source_root / "gallery"
    gallery.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "page.jpg").write_bytes(b"foreign")
    (gallery / "link.jpg").symlink_to(outside / "page.jpg")
    (source_root / "linked-gallery").symlink_to(outside, target_is_directory=True)
    adapter = ManagedFilesystemLibraryAdapter(
        tmp_path / "library",
        source_root=source_root,
        render_policy=ArtifactRenderPolicy(max_image_short_side=20),
    )
    root_components = tuple(source_root.resolve().parts[1:])

    with pytest.raises(RuntimeError, match="not safely openable"):
        adapter.open_source(
            source_root_components=root_components,
            gallery_locator_components=("gallery",),
            source_name=b"link.jpg",
        )
    with pytest.raises(RuntimeError, match="safe directory chain"):
        adapter.open_source(
            source_root_components=root_components,
            gallery_locator_components=("linked-gallery",),
            source_name=b"page.jpg",
        )

    source_root.rename(tmp_path / "moved-download")
    source_root.mkdir()
    with pytest.raises(RuntimeError, match="changed identity"):
        adapter.open_source(
            source_root_components=root_components,
            gallery_locator_components=(),
            source_name=b"missing.jpg",
        )


def test_adapter_owns_deterministic_closed_world_archive_rendering(
    tmp_path: Path,
) -> None:
    adapter = _adapter(tmp_path, max_image_short_side=40)
    gif = BytesIO()
    first = Image.new("RGB", (80, 40), "red")
    second = Image.new("RGB", (80, 40), "blue")
    first.save(gif, format="GIF", save_all=True, append_images=(second,))
    png = BytesIO()
    Image.new("RGB", (40, 80), "green").save(png, format="PNG")
    members = (
        _source_member(1, ArtifactSourceRole.PAGE, b"001.gif", gif.getvalue()),
        _source_member(
            7, ArtifactSourceRole.METADATA, b"galleryinfo.txt", b"Title: test\n"
        ),
        _source_member(11, ArtifactSourceRole.PAGE, b"002.png", png.getvalue()),
    )
    rendered: list[bytes] = []
    evidences = []
    for _ in range(2):
        destination = BytesIO(b"foreign")
        evidences.append(adapter.render_archive(members, destination, gid=42))
        rendered.append(destination.getvalue())

    assert rendered[0] == rendered[1]
    assert evidences[0] == evidences[1]
    evidence = evidences[0]
    assert evidence.artifact_sha256 == sha256(rendered[0]).digest()
    assert evidence.size_bytes == len(rendered[0])
    assert evidence.media_type == "application/vnd.comicbook+zip"
    assert evidence.download_name == "h2h-42.cbz"
    assert tuple(page.page_index for page in evidence.pages) == (0, 1)
    assert tuple(page.source_position for page in evidence.pages) == (1, 11)
    assert tuple(page.locator for page in evidence.pages) == (
        "pages/0000.jpg",
        "pages/0001.jpg",
    )
    with ZipFile(BytesIO(rendered[0])) as archive:
        infos = archive.infolist()
        assert tuple(info.filename for info in infos) == (
            "galleryinfo.txt",
            "pages/0000.jpg",
            "pages/0001.jpg",
        )
        assert tuple(info.compress_type for info in infos) == (
            ZIP_DEFLATED,
            ZIP_STORED,
            ZIP_STORED,
        )
        assert all(not info.extra and not info.comment for info in infos)
        with archive.open("pages/0000.jpg") as page:
            with Image.open(page) as cover:
                red, _, blue = cast(
                    tuple[int, int, int],
                    cover.convert("RGB").getpixel((10, 10)),
                )
                assert cover.format == "JPEG"
                assert red > blue


def test_parallel_and_automatic_rendering_are_byte_identical_to_sequential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pages: list[bytes] = []
    for index in range(4):
        source = BytesIO()
        Image.new(
            "RGB",
            (96 + index, 64 + index),
            (index * 37, 200 - index * 17, 40 + index * 29),
        ).save(source, format="PNG")
        pages.append(source.getvalue())
    members = (
        _source_member(
            0,
            ArtifactSourceRole.METADATA,
            b"galleryinfo.txt",
            b"Title: parallel equivalence\n",
        ),
        *(
            _source_member(
                index + 1,
                ArtifactSourceRole.PAGE,
                f"page-{index}.png".encode(),
                content,
            )
            for index, content in enumerate(pages)
        ),
    )
    policy = ArtifactRenderPolicy(max_image_short_side=64)
    outputs: dict[int | None, tuple[bytes, object]] = {}
    original_resolve = resolve_page_render_workers

    def resolve(configured: int | None) -> int:
        return 10 if configured is None else original_resolve(configured)

    monkeypatch.setattr(
        "h2hdb_ingest.artifact.resolve_page_render_workers",
        resolve,
    )

    for workers in (1, 2, 4, 10, 16, None):
        destination = BytesIO()
        evidence = artifact_module.render_archive(
            members,
            destination,
            gid=42,
            policy=policy,
            page_render_workers=workers,
        )
        outputs[workers] = (destination.getvalue(), evidence)

    assert all(output == outputs[1] for output in outputs.values())


def test_automatic_page_rendering_runs_only_the_bounded_worker_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    page = BytesIO()
    Image.new("RGB", (96, 64), "purple").save(page, format="PNG")
    content = page.getvalue()
    members = (
        _source_member(
            0,
            ArtifactSourceRole.METADATA,
            b"galleryinfo.txt",
            b"Title: parallel bound\n",
        ),
        *(
            _source_member(
                index + 1,
                ArtifactSourceRole.PAGE,
                f"page-{index}.png".encode(),
                content,
            )
            for index in range(MAX_PAGE_RENDER_WORKERS)
        ),
    )
    original = artifact_module._render_page
    rendezvous = Barrier(MAX_PAGE_RENDER_WORKERS)
    guard = Lock()
    active = 0
    maximum_active = 0
    resolved: list[int | None] = []

    def resolve(configured: int | None) -> int:
        resolved.append(configured)
        return MAX_PAGE_RENDER_WORKERS

    monkeypatch.setattr(
        "h2hdb_ingest.artifact.resolve_page_render_workers",
        resolve,
    )

    def observed_render(
        source: BinaryIO,
        destination: BinaryIO,
        *,
        policy: ArtifactRenderPolicy,
    ) -> artifact_module.CanonicalImageEvidence:
        nonlocal active, maximum_active
        with guard:
            active += 1
            maximum_active = max(maximum_active, active)
        try:
            rendezvous.wait(timeout=5)
            return original(source, destination, policy=policy)
        finally:
            with guard:
                active -= 1

    monkeypatch.setattr(artifact_module, "_render_page", observed_render)

    artifact_module.render_archive(
        members,
        BytesIO(),
        gid=42,
        policy=ArtifactRenderPolicy(max_image_short_side=64),
    )

    assert maximum_active == MAX_PAGE_RENDER_WORKERS
    assert resolved == [None]


def test_parallel_image_header_warning_filter_is_serialized_and_restored(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_content = BytesIO()
    second_content = BytesIO()
    Image.new("RGB", (8, 8), "red").save(first_content, format="PNG")
    Image.new("RGB", (8, 8), "blue").save(second_content, format="PNG")
    first_source = BytesIO(first_content.getvalue())
    second_source = BytesIO(second_content.getvalue())
    baseline_filters = tuple(warnings.filters)
    first_header_entered = Event()
    second_header_entered = Event()
    release_first_header = Event()
    release_second_header = Event()
    original_open = Image.open

    def coordinated_open(source: BinaryIO) -> Image.Image:
        if source is first_source:
            first_header_entered.set()
            if not release_first_header.wait(5):
                raise RuntimeError("timed out waiting to release first image header")
        elif source is second_source:
            second_header_entered.set()
            if not release_second_header.wait(5):
                raise RuntimeError("timed out waiting to release second image header")
        return cast(Image.Image, original_open(source))

    monkeypatch.setattr(Image, "open", coordinated_open)
    failures: list[BaseException] = []

    def load(source: BinaryIO) -> None:
        try:
            image = artifact_module._load_safe_image(source)
            image.close()
        except BaseException as error:  # test thread must relay every failure
            failures.append(error)

    first_thread = Thread(target=load, args=(first_source,))
    second_thread = Thread(target=load, args=(second_source,))
    first_thread.start()
    assert first_header_entered.wait(5)
    second_thread.start()
    second_entered_during_first_header = second_header_entered.wait(0.1)

    release_first_header.set()
    first_thread.join(5)
    assert not first_thread.is_alive()
    assert second_header_entered.wait(5)
    release_second_header.set()
    second_thread.join(5)

    assert not second_thread.is_alive()
    assert not second_entered_during_first_header
    assert failures == []
    assert tuple(warnings.filters) == baseline_filters


def test_parallel_image_decode_remains_outside_header_warning_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_content = BytesIO()
    second_content = BytesIO()
    Image.new("RGB", (8, 8), "red").save(first_content, format="PNG")
    Image.new("RGB", (8, 8), "blue").save(second_content, format="PNG")
    first_source = BytesIO(first_content.getvalue())
    second_source = BytesIO(second_content.getvalue())
    first_load_entered = Event()
    release_first_load = Event()
    second_header_entered = Event()
    second_finished = Event()
    original_open = Image.open

    def coordinated_open(source: BinaryIO) -> Image.Image:
        opened = cast(Image.Image, original_open(source))
        if source is second_source:
            second_header_entered.set()
        elif source is first_source:
            original_load = opened.load
            first_call = True

            def blocked_load() -> object:
                nonlocal first_call
                if first_call:
                    first_call = False
                    first_load_entered.set()
                    if not release_first_load.wait(5):
                        raise RuntimeError("timed out waiting to release image decode")
                return original_load()

            monkeypatch.setattr(opened, "load", blocked_load)
        return opened

    monkeypatch.setattr(Image, "open", coordinated_open)
    failures: list[BaseException] = []

    def load(source: BinaryIO, *, finished: Event | None = None) -> None:
        try:
            image = artifact_module._load_safe_image(source)
            image.close()
        except BaseException as error:  # test thread must relay every failure
            failures.append(error)
        finally:
            if finished is not None:
                finished.set()

    first_thread = Thread(target=load, args=(first_source,))
    second_thread = Thread(
        target=load,
        args=(second_source,),
        kwargs={"finished": second_finished},
    )
    first_thread.start()
    assert first_load_entered.wait(5)
    second_thread.start()

    assert second_header_entered.wait(5)
    assert second_finished.wait(5)
    release_first_load.set()
    first_thread.join(5)
    second_thread.join(5)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert failures == []


def test_sequential_page_batch_closes_completed_spools_after_later_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_stream = BytesIO(b"rendered")
    first = artifact_module._RenderedPageBuffer(
        image=cast(artifact_module.CanonicalImageEvidence, object()),
        stream=first_stream,
    )
    calls = 0

    def fail_second(
        _member: ArtifactSourceMember,
        *,
        policy: ArtifactRenderPolicy,
    ) -> artifact_module._RenderedPageBuffer:
        nonlocal calls
        del policy
        calls += 1
        if calls == 1:
            return first
        raise RuntimeError("second page failed")

    monkeypatch.setattr(artifact_module, "_render_page_member", fail_second)
    members = (
        _source_member(1, ArtifactSourceRole.PAGE, b"one.png", b"one"),
        _source_member(2, ArtifactSourceRole.PAGE, b"two.png", b"two"),
    )

    with pytest.raises(RuntimeError, match="second page failed"):
        artifact_module._render_page_batch(
            members,
            policy=ArtifactRenderPolicy(max_image_short_side=64),
            executor=None,
        )

    assert first_stream.closed


@pytest.mark.parametrize(
    ("workers", "error"),
    [
        (True, TypeError),
        (1.0, TypeError),
        (0, ValueError),
        (17, ValueError),
    ],
)
def test_page_render_worker_count_is_strict_and_hard_bounded(
    workers: object,
    error: type[Exception],
) -> None:
    with pytest.raises(error, match="page_render_workers"):
        artifact_module.render_archive(
            (
                _source_member(
                    0,
                    ArtifactSourceRole.METADATA,
                    b"galleryinfo.txt",
                    b"Title: invalid workers\n",
                ),
            ),
            BytesIO(),
            gid=42,
            policy=ArtifactRenderPolicy(max_image_short_side=64),
            page_render_workers=cast(int, workers),
        )


@pytest.mark.parametrize("workers", range(1, 17))
def test_archive_api_accepts_every_explicit_worker_override(workers: int) -> None:
    destination = BytesIO()

    evidence = artifact_module.render_archive(
        (
            _source_member(
                0,
                ArtifactSourceRole.METADATA,
                b"galleryinfo.txt",
                b"Title: explicit worker override\n",
            ),
        ),
        destination,
        gid=42,
        policy=ArtifactRenderPolicy(max_image_short_side=64),
        page_render_workers=workers,
    )

    assert evidence.pages == ()
    assert destination.getbuffer().nbytes == evidence.size_bytes


def test_archive_writer_fails_closed_before_exposing_bounded_destination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _adapter(tmp_path, max_image_short_side=40)
    metadata = _source_member(
        0,
        ArtifactSourceRole.METADATA,
        b"galleryinfo.txt",
        b"Title: test\n",
    )
    destination = BytesIO(b"preserved")
    monkeypatch.setattr(artifact_module, "MAX_ARCHIVE_SIZE_BYTES", 64)

    with pytest.raises(PresentationImageError, match="before member write"):
        adapter.render_archive((metadata,), destination, gid=42)

    assert destination.getvalue() == b"preserved"


def test_metadata_only_archive_has_empty_presentation_and_no_thumbnail(
    tmp_path: Path,
) -> None:
    adapter = _adapter(tmp_path, max_image_short_side=40)
    archive = BytesIO()
    rendered = adapter.render_archive(
        (
            _source_member(
                7,
                ArtifactSourceRole.METADATA,
                b"galleryinfo.txt",
                b"Title: no pages\n",
            ),
        ),
        archive,
        gid=42,
    )
    thumbnail = BytesIO()

    presentation = adapter.render_presentation(
        BytesIO(archive.getvalue()),
        thumbnail,
        rendered_pages=rendered.pages,
    )

    assert rendered.pages == ()
    assert presentation.pages == ()
    assert presentation.thumbnail is None
    assert thumbnail.getvalue() == b""
    with ZipFile(BytesIO(archive.getvalue())) as opened:
        assert opened.namelist() == ["galleryinfo.txt"]


def test_one_mib_incompressible_metadata_uses_deflate_worst_case_bound(
    tmp_path: Path,
) -> None:
    adapter = _adapter(tmp_path, max_image_short_side=40)
    metadata = random.Random(0).randbytes(1024 * 1024)
    destination = BytesIO()

    adapter.render_archive(
        (
            _source_member(
                4,
                ArtifactSourceRole.METADATA,
                b"galleryinfo.txt",
                metadata,
            ),
        ),
        destination,
        gid=42,
    )

    with ZipFile(BytesIO(destination.getvalue())) as archive:
        info = archive.getinfo("galleryinfo.txt")
        assert info.file_size == 1024 * 1024
        assert info.compress_size > info.file_size


def test_archive_writer_validates_source_authority_roles_and_page_caps(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _adapter(tmp_path, max_image_short_side=40)
    metadata = _source_member(
        0,
        ArtifactSourceRole.METADATA,
        b"galleryinfo.txt",
        b"Title: test\n",
    )
    image = BytesIO()
    Image.new("RGB", (40, 20), "red").save(image, format="PNG")
    page = _source_member(
        3,
        ArtifactSourceRole.PAGE,
        b"001.png",
        image.getvalue(),
    )

    changed_digest = ArtifactSourceMember(
        position=page.position,
        role=page.role,
        source_name=page.source_name,
        expected_sha256=b"x" * 32,
        expected_size_bytes=page.expected_size_bytes,
        source=BytesIO(image.getvalue()),
    )
    with pytest.raises(PresentationImageError, match="SHA-256 disagrees"):
        adapter.render_archive((metadata, changed_digest), BytesIO(), gid=42)

    bad_position = _source_member(
        0,
        ArtifactSourceRole.PAGE,
        b"001.png",
        image.getvalue(),
    )
    with pytest.raises(PresentationImageError, match="strictly increasing"):
        adapter.render_archive((metadata, bad_position), BytesIO(), gid=42)

    other_source = _ReadSpy(b"must-not-open")
    forbidden_other = ArtifactSourceMember(
        position=2,
        role=ArtifactSourceRole.OTHER,
        source_name=b"ignored.bin",
        expected_sha256=b"o" * 32,
        expected_size_bytes=MAX_ENCODED_PAGE_BYTES + 1,
        source=other_source,
    )
    with pytest.raises(PresentationImageError, match="must never cross"):
        adapter.render_archive((metadata, forbidden_other), BytesIO(), gid=42)
    assert other_source.read_count == 0

    invalid_page = _source_member(
        1,
        ArtifactSourceRole.PAGE,
        b"001.jpg",
        b"not-an-image",
    )
    with pytest.raises(PresentationImageError, match="truncated or invalid"):
        adapter.render_archive((metadata, invalid_page), BytesIO(), gid=42)

    monkeypatch.setattr(artifact_module, "MAX_PAGE_COUNT", 1)
    second_page = _source_member(
        4,
        ArtifactSourceRole.PAGE,
        b"002.png",
        image.getvalue(),
    )
    with pytest.raises(PresentationImageError, match="4096 pages"):
        adapter.render_archive(
            (metadata, page, second_page),
            BytesIO(),
            gid=42,
        )


def test_presentation_policy_constants_are_exact() -> None:
    assert MAX_ARCHIVE_SIZE_BYTES == (1 << 31) - 1
    assert MAX_PAGE_COUNT == 4096
    assert MAX_ENCODED_PAGE_BYTES == 32 * 1024 * 1024
    assert MAX_DECODED_PIXELS == 40_000_000
    assert MAX_IMAGE_LONG_SIDE == 8192
    assert PAGE_JPEG_QUALITY == 90
    assert THUMBNAIL_MAX_SIDE == 320
    assert THUMBNAIL_JPEG_QUALITY == 85


def test_dimension_and_encoded_size_boundaries_are_inclusive() -> None:
    artifact_module._validate_dimensions(8192, 1, max_long_side=8192)
    artifact_module._validate_dimensions(8000, 5000, max_long_side=8192)
    artifact_module.CanonicalImageEvidence(
        sha256=b"d" * 32,
        size_bytes=32 * 1024 * 1024,
        width=1,
        height=1,
    )

    with pytest.raises(PresentationImageError, match="long side"):
        artifact_module._validate_dimensions(8193, 1, max_long_side=8192)
    with pytest.raises(PresentationImageError, match="40 MP"):
        artifact_module._validate_dimensions(8001, 5000, max_long_side=8192)
    with pytest.raises(ValueError, match="encoded size"):
        artifact_module.CanonicalImageEvidence(
            sha256=b"d" * 32,
            size_bytes=(32 * 1024 * 1024) + 1,
            width=1,
            height=1,
        )


def test_page_index_boundaries_are_inclusive() -> None:
    assert canonical_page_member_name(4095) == "pages/4095.jpg"
    with pytest.raises(ValueError, match="page_index"):
        canonical_page_member_name(4096)


def test_gif_frame_zero_becomes_deterministic_jpeg(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path, max_image_short_side=20)
    source = BytesIO()
    first = Image.new("RGB", (40, 20), "red")
    second = Image.new("RGB", (40, 20), "blue")
    first.save(source, format="GIF", save_all=True, append_images=(second,))

    rendered = [
        _rendered_page_bytes(adapter, source.getvalue(), source_name=b"page.gif")
        for _ in range(2)
    ]

    assert rendered[0] == rendered[1]
    with Image.open(BytesIO(rendered[0])) as image:
        assert image.format == "JPEG"
        red, _, blue = cast(
            tuple[int, int, int],
            image.convert("RGB").getpixel((10, 10)),
        )
        assert red > blue


def test_exif_orientation_is_applied_before_render(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path, max_image_short_side=20)
    source = BytesIO()
    exif = Image.Exif()
    exif[274] = 6
    Image.new("RGB", (40, 20), "red").save(source, format="JPEG", exif=exif)

    rendered = _rendered_page_bytes(
        adapter,
        source.getvalue(),
        source_name=b"oriented.jpg",
    )

    with Image.open(BytesIO(rendered)) as image:
        assert image.size == (20, 40)


def test_alpha_is_composited_on_white_before_jpeg_render(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path, max_image_short_side=20)
    source = BytesIO()
    image = Image.new("RGBA", (40, 20), (255, 0, 0, 255))
    image.paste((0, 0, 255, 0), (0, 0, 20, 20))
    image.save(source, format="PNG")

    rendered = _rendered_page_bytes(adapter, source.getvalue())

    with Image.open(BytesIO(rendered)).convert("RGB") as decoded:
        transparent_pixel = cast(tuple[int, int, int], decoded.getpixel((5, 10)))
        opaque_pixel = cast(tuple[int, int, int], decoded.getpixel((35, 10)))
    assert min(transparent_pixel) > 230
    assert opaque_pixel[0] > 200
    assert max(opaque_pixel[1:]) < 60


def test_truncated_image_is_rejected(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path, max_image_short_side=20)
    source = BytesIO()
    Image.new("RGB", (40, 20), "red").save(source, format="JPEG")
    truncated = source.getvalue()[:-32]

    with pytest.raises(PresentationImageError, match="truncated or invalid"):
        _rendered_page_bytes(adapter, truncated, source_name=b"page.jpg")


def test_decompression_bomb_warning_is_a_hard_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _adapter(tmp_path, max_image_short_side=20)
    source = BytesIO()
    Image.new("RGB", (2, 2), "red").save(source, format="PNG")
    monkeypatch.setattr(Image, "MAX_IMAGE_PIXELS", 1)

    with pytest.raises(PresentationImageError, match="decoded pixel policy"):
        _rendered_page_bytes(adapter, source.getvalue())


def test_encoded_page_limit_is_enforced_before_destination_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _adapter(tmp_path, max_image_short_side=20)
    source = BytesIO()
    Image.new("RGB", (40, 20), "red").save(source, format="PNG")
    source.seek(0)
    destination = BytesIO()
    monkeypatch.setattr(artifact_module, "MAX_ENCODED_PAGE_BYTES", 16)

    with pytest.raises(PresentationImageError, match="encoded-size bound"):
        _rendered_page_bytes(
            adapter,
            source.getvalue(),
            destination=destination,
        )
    assert destination.getvalue() == b""


def test_archive_writer_completes_short_destination_writes(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path, max_image_short_side=20)
    source = BytesIO()
    Image.new("RGB", (40, 20), "red").save(source, format="PNG")
    source.seek(0)

    destination = _PartialWriter()
    rendered = _rendered_page_bytes(
        adapter,
        source.getvalue(),
        destination=destination,
    )

    assert rendered.startswith(b"\xff\xd8")


def test_archive_destination_is_never_read(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path, max_image_short_side=20)
    source = BytesIO()
    Image.new("RGB", (40, 20), "red").save(source, format="PNG")

    rendered = _rendered_page_bytes(
        adapter,
        source.getvalue(),
        destination=_NoReadWriter(),
    )

    assert rendered.startswith(b"\xff\xd8")


def test_archive_writer_rejects_zero_progress_destination(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path, max_image_short_side=20)
    metadata = _source_member(
        0,
        ArtifactSourceRole.METADATA,
        b"galleryinfo.txt",
        b"Title: test\n",
    )

    with pytest.raises(PresentationImageError, match="made no progress"):
        adapter.render_archive((metadata,), _ZeroWriter(), gid=42)


def test_stored_pages_produce_exact_extents_cover_alias_and_thumbnail(
    tmp_path: Path,
) -> None:
    adapter = _adapter(tmp_path, max_image_short_side=40)
    source_pages: list[bytes] = []
    for color in ("red", "blue"):
        source = BytesIO()
        Image.new("RGB", (80, 40), color).save(source, format="PNG")
        source_pages.append(source.getvalue())
    archive = BytesIO()
    rendered = adapter.render_archive(
        (
            _source_member(
                1,
                ArtifactSourceRole.METADATA,
                b"galleryinfo.txt",
                b"Title: test\n",
            ),
            _source_member(4, ArtifactSourceRole.PAGE, b"a.png", source_pages[0]),
            _source_member(9, ArtifactSourceRole.PAGE, b"b.png", source_pages[1]),
        ),
        archive,
        gid=42,
    )
    thumbnail = BytesIO()
    presentation = adapter.render_presentation(
        BytesIO(archive.getvalue()),
        thumbnail,
        rendered_pages=rendered.pages,
    )

    assert len(presentation.pages) == 2
    for page in presentation.pages:
        archive.seek(page.extent.offset)
        content = archive.read(page.extent.length)
        assert sha256(content).digest() == page.sha256
        assert page.media_type == "image/jpeg"
    assert presentation.thumbnail is not None
    assert (
        max(
            presentation.thumbnail.width,
            presentation.thumbnail.height,
        )
        <= THUMBNAIL_MAX_SIDE
    )
    assert sha256(thumbnail.getvalue()).digest() == presentation.thumbnail.sha256
    with Image.open(BytesIO(thumbnail.getvalue())) as opened_thumbnail:
        assert opened_thumbnail.format == "JPEG"

    reversed_positions = tuple(
        ArtifactRenderedPage(
            page_index=page.page_index,
            source_position=rendered.pages[-page.page_index - 1].source_position,
            locator=page.locator,
        )
        for page in rendered.pages
    )
    with pytest.raises(PresentationImageError, match="strictly increasing"):
        adapter.render_presentation(
            BytesIO(archive.getvalue()),
            BytesIO(),
            rendered_pages=reversed_positions,
        )


def test_prepare_and_activate_acquisition_thumbnail_vertical_slice(
    tmp_path: Path,
) -> None:
    root = tmp_path / "library"
    _provision_library_root(root)
    source_root = tmp_path / "download"
    source_root.mkdir()
    adapter = ManagedFilesystemLibraryAdapter(
        root,
        source_root=source_root,
        render_policy=ArtifactRenderPolicy(max_image_short_side=40),
    )
    source = BytesIO()
    Image.new("RGB", (80, 40), "red").save(source, format="PNG")
    source.seek(0)
    archive = BytesIO()
    rendered = adapter.render_archive(
        (
            _source_member(
                1,
                ArtifactSourceRole.METADATA,
                b"galleryinfo.txt",
                b"Title: test\n",
            ),
            _source_member(5, ArtifactSourceRole.PAGE, b"page.png", source.getvalue()),
        ),
        archive,
        gid=42,
    )
    archive_bytes = archive.getvalue()
    gid = 42
    modified_at = datetime(2026, 1, 1, tzinfo=UTC)
    acquisition_key = adapter.storage_key(gid, CatalogResourceKind.ACQUISITION)
    acquisition_token = b"a" * 32
    acquisition_evidence = adapter.protect(
        BytesIO(archive_bytes),
        acquisition_key,
        sha256(archive_bytes).digest(),
        len(archive_bytes),
        modified_at,
        acquisition_token,
    )
    assert acquisition_evidence.storage_object is not None

    thumbnail_bytes = BytesIO()
    prepared = adapter.render_presentation(
        BytesIO(archive_bytes),
        thumbnail_bytes,
        rendered_pages=rendered.pages,
    )

    assert len(prepared.pages) == 1
    assert prepared.thumbnail is not None
    thumbnail_key = adapter.storage_key(gid, CatalogResourceKind.THUMBNAIL)
    thumbnail_evidence = adapter.protect(
        BytesIO(thumbnail_bytes.getvalue()),
        thumbnail_key,
        sha256(thumbnail_bytes.getvalue()).digest(),
        len(thumbnail_bytes.getvalue()),
        modified_at,
        b"t" * 32,
    )
    assert thumbnail_evidence.storage_object is not None
    publication_key = _publication_key(gid)
    acquisition_item = VNextLibraryActivationItem(
        publication_key=publication_key,
        gid=gid,
        resource_kind=CatalogResourceKind.ACQUISITION,
        storage_object=acquisition_evidence.storage_object,
    )
    thumbnail_item = VNextLibraryActivationItem(
        publication_key=publication_key,
        gid=gid,
        resource_kind=CatalogResourceKind.THUMBNAIL,
        storage_object=thumbnail_evidence.storage_object,
    )
    items = tuple(
        sorted(
            (acquisition_item, thumbnail_item),
            key=lambda item: (item.publication_key, item.resource_kind.value),
        )
    )

    _activate(adapter, 1, items)

    acquisition_path = adapter.current_path.joinpath(*acquisition_key.segments)
    thumbnail_path = adapter.current_path.joinpath(
        *thumbnail_evidence.storage_object.key.segments
    )
    assert acquisition_path.read_bytes() == archive_bytes
    assert thumbnail_path.read_bytes() == thumbnail_bytes.getvalue()
    with acquisition_path.open("rb") as installed:
        installed.seek(prepared.pages[0].extent.offset)
        page_content = installed.read(prepared.pages[0].extent.length)
        assert sha256(page_content).digest() == prepared.pages[0].sha256
    with Image.open(thumbnail_path) as thumbnail:
        assert thumbnail.format == "JPEG"
        assert max(thumbnail.size) <= THUMBNAIL_MAX_SIDE

    _activate(adapter, 2, (acquisition_item,))

    assert acquisition_path.read_bytes() == archive_bytes
    assert not thumbnail_path.exists()


def test_thumbnail_render_completes_short_core_destination_writes(
    tmp_path: Path,
) -> None:
    adapter = _adapter(tmp_path, max_image_short_side=40)
    image = BytesIO()
    Image.new("RGB", (80, 40), "red").save(image, format="PNG")
    archive = BytesIO()
    rendered = adapter.render_archive(
        (
            _source_member(
                1,
                ArtifactSourceRole.METADATA,
                b"galleryinfo.txt",
                b"Title: test\n",
            ),
            _source_member(2, ArtifactSourceRole.PAGE, b"page.png", image.getvalue()),
        ),
        archive,
        gid=42,
    )
    destination = _PartialWriter()

    evidence = adapter.render_presentation(
        BytesIO(archive.getvalue()),
        destination,
        rendered_pages=rendered.pages,
    )

    assert evidence.thumbnail is not None
    assert destination.getvalue().startswith(b"\xff\xd8")
    assert sha256(destination.getvalue()).digest() == evidence.thumbnail.sha256


def test_thumbnail_render_rejects_zero_progress_core_destination_write(
    tmp_path: Path,
) -> None:
    adapter = _adapter(tmp_path, max_image_short_side=40)
    image = BytesIO()
    Image.new("RGB", (80, 40), "red").save(image, format="PNG")
    archive = BytesIO()
    rendered = adapter.render_archive(
        (
            _source_member(
                1,
                ArtifactSourceRole.METADATA,
                b"galleryinfo.txt",
                b"Title: test\n",
            ),
            _source_member(2, ArtifactSourceRole.PAGE, b"page.png", image.getvalue()),
        ),
        archive,
        gid=42,
    )

    with pytest.raises(PresentationImageError, match="made no progress"):
        adapter.render_presentation(
            BytesIO(archive.getvalue()),
            _ZeroWriter(),
            rendered_pages=rendered.pages,
        )


def test_archive_size_preflight_rejects_exact_non_zip64_overflow() -> None:
    existing = ["galleryinfo.txt"]
    next_name = "pages/0000.jpg"
    overhead = (
        artifact_module._LOCAL_FILE_HEADER.size
        + len(next_name)
        + 1
        + sum(
            artifact_module._CENTRAL_DIRECTORY_HEADER_BYTES + len(name)
            for name in (*existing, next_name)
        )
        + artifact_module._END_OF_CENTRAL_DIRECTORY.size
    )
    exact_current_size = MAX_ARCHIVE_SIZE_BYTES - overhead
    artifact_module._require_projected_archive_size(
        exact_current_size,
        existing,
        next_name,
        1,
    )
    with pytest.raises(PresentationImageError, match="require ZIP64"):
        artifact_module._require_projected_archive_size(
            exact_current_size + 1,
            existing,
            next_name,
            1,
        )


def test_archive_requires_stored_pages_and_closed_member_set(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path, max_image_short_side=20)
    source = BytesIO()
    Image.new("RGB", (40, 20), "red").save(source, format="PNG")
    page = _rendered_page_bytes(adapter, source.getvalue())
    name = canonical_page_member_name(0)

    def archive(*, compression: int, extra: bool) -> BytesIO:
        destination = BytesIO()
        with ZipFile(destination, "w") as writer:
            writer.writestr(
                _zip_info("galleryinfo.txt", ZIP_DEFLATED),
                b"Title: test\n",
            )
            writer.writestr(_zip_info(name, compression), page)
            if extra:
                writer.writestr(_zip_info("foreign.bin", ZIP_STORED), b"foreign")
        destination.seek(0)
        return destination

    with pytest.raises(PresentationImageError, match="ZIP_STORED"):
        inspect_presentation_archive(
            archive(compression=ZIP_DEFLATED, extra=False),
            (name,),
        )
    with pytest.raises(PresentationImageError, match="member count"):
        inspect_presentation_archive(
            archive(compression=ZIP_STORED, extra=True),
            (name,),
        )


def test_archive_rejects_duplicate_page_members() -> None:
    page = BytesIO()
    Image.new("RGB", (40, 20), "red").save(page, format="JPEG")
    name = canonical_page_member_name(0)
    archive = BytesIO()
    with ZipFile(archive, "w") as writer:
        writer.writestr(
            _zip_info("galleryinfo.txt", ZIP_DEFLATED),
            b"Title: test\n",
        )
        writer.writestr(_zip_info(name, ZIP_STORED), page.getvalue())
        with pytest.warns(UserWarning, match="Duplicate name"):
            writer.writestr(_zip_info(name, ZIP_STORED), page.getvalue())

    archive.seek(0)
    with pytest.raises(PresentationImageError, match="member count"):
        inspect_presentation_archive(archive, (name,))


@pytest.mark.parametrize(
    ("local_version", "local_flags", "central_version", "central_flags", "message"),
    (
        (20, 0x08, (3 << 8) | 20, 0x08, "flags are not canonical"),
        (45, 0, (3 << 8) | 45, 0, "central-directory attributes"),
    ),
)
def test_archive_rejects_data_descriptor_and_zip64_markers(
    local_version: int,
    local_flags: int,
    central_version: int,
    central_flags: int,
    message: str,
) -> None:
    page = BytesIO()
    Image.new("RGB", (40, 20), "red").save(page, format="JPEG")
    archive_bytes, names = _canonical_archive(page.getvalue())
    archive = bytearray(archive_bytes)
    central_offset = struct.unpack_from("<I", archive, len(archive) - 22 + 16)[0]
    struct.pack_into("<H", archive, 4, local_version)
    struct.pack_into("<H", archive, 6, local_flags)
    struct.pack_into("<H", archive, central_offset + 4, central_version)
    struct.pack_into("<H", archive, central_offset + 6, local_version)
    struct.pack_into("<H", archive, central_offset + 8, central_flags)

    with pytest.raises(PresentationImageError, match=message):
        inspect_presentation_archive(BytesIO(archive), names)


@pytest.mark.parametrize(
    ("field_offset", "replacement", "message"),
    (
        (6, struct.pack("<H", 0x0114), "version is not canonical"),
        (12, struct.pack("<H", 1), "timestamp is not canonical"),
        (16, struct.pack("<I", 0), "local CRC disagrees"),
        (20, struct.pack("<I", 1), "sizes are not canonical"),
        (24, struct.pack("<I", 1), "sizes are not canonical"),
        (28, struct.pack("<H", 15), "filename length disagrees"),
        (30, struct.pack("<H", 1), "extra data is not canonical"),
        (32, struct.pack("<H", 1), "extra data is not canonical"),
        (34, struct.pack("<H", 1), "attributes disagree"),
        (36, struct.pack("<H", 1), "attributes disagree"),
        (38, struct.pack("<I", 0), "attributes disagree"),
        (42, struct.pack("<I", 1), "local offset is not canonical"),
    ),
)
def test_archive_rejects_raw_central_fields_normalized_by_zipfile(
    field_offset: int,
    replacement: bytes,
    message: str,
) -> None:
    page = BytesIO()
    Image.new("RGB", (40, 20), "red").save(page, format="JPEG")
    archive_bytes, names = _canonical_archive(page.getvalue())
    archive = bytearray(archive_bytes)
    central_offset = struct.unpack_from("<I", archive, len(archive) - 22 + 16)[0]
    first_name_size, first_extra_size, first_comment_size = struct.unpack_from(
        "<HHH",
        archive,
        central_offset + 28,
    )
    page_header_offset = (
        central_offset
        + artifact_module._CENTRAL_DIRECTORY_HEADER.size
        + first_name_size
        + first_extra_size
        + first_comment_size
    )
    archive[
        page_header_offset + field_offset : page_header_offset
        + field_offset
        + len(replacement)
    ] = replacement

    with pytest.raises(PresentationImageError, match=message):
        inspect_presentation_archive(BytesIO(archive), names)


def test_archive_rejects_noncanonical_names_and_unbounded_page_count() -> None:
    with pytest.raises(PresentationImageError, match="not canonical"):
        inspect_presentation_archive(BytesIO(b"not reached"), ("page.jpg",))
    with pytest.raises(PresentationImageError, match="4096 pages"):
        inspect_presentation_archive(
            BytesIO(b"not reached"),
            tuple(f"pages/{index:04d}.jpg" for index in range(MAX_PAGE_COUNT + 1)),
        )


def test_archive_preflights_total_and_central_directory_bounds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    page = BytesIO()
    Image.new("RGB", (40, 20), "red").save(page, format="JPEG")
    archive_bytes, names = _canonical_archive(page.getvalue())
    monkeypatch.setattr(
        artifact_module,
        "MAX_ARCHIVE_SIZE_BYTES",
        len(archive_bytes) - 1,
    )
    with pytest.raises(PresentationImageError, match="v2 size cap"):
        inspect_presentation_archive(BytesIO(archive_bytes), names)

    monkeypatch.setattr(
        artifact_module,
        "MAX_ARCHIVE_SIZE_BYTES",
        MAX_ARCHIVE_SIZE_BYTES,
    )
    changed = bytearray(archive_bytes)
    central_size_offset = len(changed) - 22 + 12
    central_size = struct.unpack_from("<I", changed, central_size_offset)[0]
    struct.pack_into("<I", changed, central_size_offset, central_size + 1)
    with pytest.raises(PresentationImageError, match="central directory size"):
        inspect_presentation_archive(BytesIO(changed), names)


def test_archive_rejects_reordered_members_and_prefixed_bytes() -> None:
    page = BytesIO()
    Image.new("RGB", (40, 20), "red").save(page, format="JPEG")
    names = (canonical_page_member_name(0), canonical_page_member_name(1))
    reordered = BytesIO()
    with ZipFile(reordered, "w") as writer:
        writer.writestr(
            _zip_info("galleryinfo.txt", ZIP_DEFLATED),
            b"Title: test\n",
        )
        for name in reversed(names):
            writer.writestr(_zip_info(name, ZIP_STORED), page.getvalue())
    reordered.seek(0)
    with pytest.raises(PresentationImageError, match="order or coverage"):
        inspect_presentation_archive(reordered, names)

    canonical, single_name = _canonical_archive(page.getvalue())
    with pytest.raises(PresentationImageError, match="central directory"):
        inspect_presentation_archive(
            BytesIO(b"foreign-prefix" + canonical), single_name
        )


def test_archive_rejects_noncanonical_metadata_and_central_attributes() -> None:
    page = BytesIO()
    Image.new("RGB", (40, 20), "red").save(page, format="JPEG")
    name = canonical_page_member_name(0)
    stored_metadata = BytesIO()
    with ZipFile(stored_metadata, "w") as writer:
        writer.writestr(_zip_info("galleryinfo.txt", ZIP_STORED), b"Title: test\n")
        writer.writestr(_zip_info(name, ZIP_STORED), page.getvalue())
    stored_metadata.seek(0)
    with pytest.raises(PresentationImageError, match="ZIP_DEFLATED"):
        inspect_presentation_archive(stored_metadata, (name,))

    foreign_mode = BytesIO()
    with ZipFile(foreign_mode, "w") as writer:
        writer.writestr(
            _zip_info("galleryinfo.txt", ZIP_DEFLATED),
            b"Title: test\n",
        )
        page_info = _zip_info(name, ZIP_STORED)
        page_info.external_attr = 0o100600 << 16
        writer.writestr(page_info, page.getvalue())
    foreign_mode.seek(0)
    with pytest.raises(PresentationImageError, match="central-directory attributes"):
        inspect_presentation_archive(foreign_mode, (name,))


@pytest.mark.parametrize(
    ("field_offset", "replacement", "message"),
    (
        (8, struct.pack("<H", ZIP_DEFLATED), "local compression disagrees"),
        (14, b"\x00\x00\x00\x00", "local CRC disagrees"),
        (18, b"\x01\x00\x00\x00", "local sizes disagree"),
    ),
)
def test_archive_rejects_local_header_disagreement(
    field_offset: int,
    replacement: bytes,
    message: str,
) -> None:
    page = BytesIO()
    Image.new("RGB", (40, 20), "red").save(page, format="JPEG")
    archive_bytes, names = _canonical_archive(page.getvalue())
    archive = bytearray(archive_bytes)
    with ZipFile(BytesIO(archive_bytes)) as opened:
        header_offset = opened.getinfo(names[0]).header_offset
    archive[
        header_offset + field_offset : header_offset + field_offset + len(replacement)
    ] = replacement

    with pytest.raises(PresentationImageError, match=message):
        inspect_presentation_archive(BytesIO(archive), names)


def test_archive_rejects_local_filename_and_zip64_extra_data() -> None:
    page = BytesIO()
    Image.new("RGB", (40, 20), "red").save(page, format="JPEG")
    archive_bytes, names = _canonical_archive(page.getvalue())
    archive = bytearray(archive_bytes)
    with ZipFile(BytesIO(archive_bytes)) as opened:
        header_offset = opened.getinfo(names[0]).header_offset
    archive[header_offset + 30] = ord("q")

    with pytest.raises(PresentationImageError, match="local filename disagrees"):
        inspect_presentation_archive(BytesIO(archive), names)

    with_extra = BytesIO()
    with ZipFile(with_extra, "w") as writer:
        writer.writestr(
            _zip_info("galleryinfo.txt", ZIP_DEFLATED),
            b"Title: test\n",
        )
        info = _zip_info(names[0], ZIP_STORED)
        info.extra = b"\x01\x00\x00\x00"
        writer.writestr(info, page.getvalue())
    with_extra.seek(0)
    with pytest.raises(PresentationImageError, match="central directory size"):
        inspect_presentation_archive(with_extra, names)


def test_archive_rejects_page_crc_mismatch() -> None:
    page = BytesIO()
    Image.new("RGB", (40, 20), "red").save(page, format="JPEG")
    archive_bytes, names = _canonical_archive(page.getvalue())
    archive = bytearray(archive_bytes)
    with ZipFile(BytesIO(archive_bytes)) as opened:
        page_info = opened.getinfo(names[0])
    data_offset = page_info.header_offset + 30 + len(names[0])
    archive[data_offset + (page_info.file_size // 2)] ^= 0xFF

    with pytest.raises(PresentationImageError, match="page CRC"):
        inspect_presentation_archive(BytesIO(archive), names)


def test_archive_reads_and_crc_validates_bounded_metadata() -> None:
    page = BytesIO()
    Image.new("RGB", (40, 20), "red").save(page, format="JPEG")
    archive_bytes, names = _canonical_archive(page.getvalue())
    archive = bytearray(archive_bytes)
    with ZipFile(BytesIO(archive_bytes)) as opened:
        metadata = opened.getinfo("galleryinfo.txt")
        header_offset = metadata.header_offset
    name_size = struct.unpack_from("<H", archive, header_offset + 26)[0]
    extra_size = struct.unpack_from("<H", archive, header_offset + 28)[0]
    data_offset = header_offset + 30 + name_size + extra_size
    archive[data_offset] ^= 0xFF

    with pytest.raises(PresentationImageError, match="metadata"):
        inspect_presentation_archive(BytesIO(archive), names)


def test_every_worker_path_fails_identically_and_preserves_destination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Differential: the decision-driven automatic path and every manual
    override raise the same failure on the same corrupt page and leave the
    destination bytes untouched, exactly like sequential rendering."""

    pages: list[bytes] = []
    for index in range(3):
        source = BytesIO()
        Image.new("RGB", (32 + index, 24), (index * 40, 90, 200)).save(
            source, format="PNG"
        )
        pages.append(source.getvalue())
    members = (
        _source_member(
            0,
            ArtifactSourceRole.METADATA,
            b"galleryinfo.txt",
            b"Title: failure equivalence\n",
        ),
        *(
            _source_member(
                index + 1,
                ArtifactSourceRole.PAGE,
                f"page-{index}.png".encode(),
                content,
            )
            for index, content in enumerate(pages)
        ),
        _source_member(4, ArtifactSourceRole.PAGE, b"broken.jpg", b"not-an-image"),
    )
    policy = ArtifactRenderPolicy(max_image_short_side=16)
    monkeypatch.setattr(
        "h2hdb_ingest.artifact.resolve_page_render_workers",
        lambda configured: 10 if configured is None else configured,
    )

    failures: list[tuple[type[BaseException], str]] = []
    for workers in (1, 2, 4, 16, None):
        destination = BytesIO(b"preserved")
        for member in members:
            member.source.seek(0)
        with pytest.raises(
            PresentationImageError, match="truncated or invalid"
        ) as raised:
            artifact_module.render_archive(
                members,
                destination,
                gid=42,
                policy=policy,
                page_render_workers=workers,
            )
        failures.append((type(raised.value), str(raised.value)))
        assert destination.getvalue() == b"preserved"

    assert all(failure == failures[0] for failure in failures)
