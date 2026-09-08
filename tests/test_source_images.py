"""Real-codec regressions for source streaming and bounded output pixels."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from pathlib import Path
from threading import Barrier, Event, Thread
from typing import BinaryIO, cast

import pytest
import pyvips  # type: ignore[import-untyped]  # pyvips 3.2 ships no PEP 561 marker.
from PIL import Image

import h2hdb_ingest.source_image as source_module
from h2hdb_ingest.artifact import (
    MAX_DECODED_PIXELS,
    ArtifactImageResampler,
    ArtifactRenderPolicy,
    PresentationImageError,
    SourceImageSizeLimitError,
    artifact_policy_fingerprint_sha256,
    load_source_page_image,
)
from h2hdb_ingest.artifact_errors import (
    attach_page_failure_context,
    get_page_failure_context,
)
from h2hdb_ingest.source_image import _DecodeScheduler


def _png(size: tuple[int, int], *, color: str = "red") -> bytes:
    stream = BytesIO()
    with Image.new("RGB", size, color) as image:
        image.save(stream, format="PNG")
    return stream.getvalue()


def test_source_long_side_is_resized_instead_of_rejected() -> None:
    with load_source_page_image(
        BytesIO(_png((10_000, 10))),
        policy=ArtifactRenderPolicy(),
    ) as image:
        assert image.size == (8000, 8)
        assert image.getpixel((4096, 4)) == (255, 0, 0)


def test_source_decode_is_independent_of_pillow_global_pixel_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    content = _png((40, 30))
    monkeypatch.setattr(Image, "MAX_IMAGE_PIXELS", 1)
    rendezvous = Barrier(2)

    def render(short_side: int) -> tuple[int, int]:
        rendezvous.wait(timeout=5)
        with load_source_page_image(
            BytesIO(content),
            policy=ArtifactRenderPolicy(max_image_short_side=short_side),
        ) as image:
            return image.size

    with ThreadPoolExecutor(max_workers=2) as executor:
        first, second = executor.submit(render, 15), executor.submit(render, 30)
        assert first.result(timeout=5) == (20, 15)
        assert second.result(timeout=5) == (40, 30)
    assert Image.MAX_IMAGE_PIXELS == 1


@pytest.mark.parametrize("resampler", tuple(ArtifactImageResampler))
def test_selected_resampler_controls_final_stage(
    monkeypatch: pytest.MonkeyPatch,
    resampler: ArtifactImageResampler,
) -> None:
    configured = ArtifactRenderPolicy(max_image_short_side=10, resampler=resampler)
    content = _png((100, 100))
    called: list[tuple[tuple[int, int], object]] = []
    original = Image.Image.thumbnail

    def observe(
        image: Image.Image,
        size: tuple[int, int],
        resample: Image.Resampling,
    ) -> None:
        called.append((image.size, resample))
        original(image, size, resample)

    monkeypatch.setattr(Image.Image, "thumbnail", observe)
    with load_source_page_image(BytesIO(content), policy=configured) as image:
        assert image.size == (10, 10)
    assert called == [((20, 20), configured.pillow_resampler)]


@pytest.mark.parametrize("image_format", ["JPEG", "PNG"])
def test_truncated_real_source_uses_strict_native_loader(
    image_format: str,
) -> None:
    stream = BytesIO()
    with Image.new("RGB", (256, 256), "red") as image:
        image.save(stream, format=image_format)
    content = stream.getvalue()
    truncated = content[: max(1, len(content) * 3 // 4)]
    with pytest.raises(PresentationImageError, match="truncated or invalid") as caught:
        load_source_page_image(BytesIO(truncated), policy=ArtifactRenderPolicy())
    attach_page_failure_context(
        caught.value,
        source_position=3,
        source_name=b"exact-original-name.jpg",
        expected_size_bytes=len(truncated),
    )
    context = get_page_failure_context(caught.value)
    assert context is not None
    # Both fixtures have complete headers; the pixel decoder detects truncation.
    assert (context.width, context.height) == (256, 256)
    assert context.source_kind is not None
    assert image_format.casefold() in context.source_kind


def test_io_callback_failure_keeps_original_error_identity() -> None:
    failure = OSError("source disk became unavailable")

    class FailedSource(BytesIO):
        def read(self, size: int | None = -1) -> bytes:
            del size
            raise failure

    with pytest.raises(OSError) as caught:
        load_source_page_image(FailedSource(), policy=ArtifactRenderPolicy())
    assert caught.value is failure


def test_source_reads_are_bounded_and_never_request_all_bytes() -> None:
    class BoundedSource(BytesIO):
        def read(self, size: int | None = -1) -> bytes:
            assert size is not None
            assert 0 < size <= 1024 * 1024
            return super().read(size)

    with load_source_page_image(
        BoundedSource(_png((400, 300))), policy=ArtifactRenderPolicy()
    ) as image:
        assert image.size == (400, 300)


def test_header_marks_progressive_sources_for_exclusive_decode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[bool] = []
    scheduler = _DecodeScheduler()
    original = scheduler.acquire

    def acquire(*, exclusive: bool) -> object:
        observed.append(exclusive)
        return original(exclusive=exclusive)

    monkeypatch.setattr(source_module._SCHEDULER, "acquire", acquire)
    for progressive in (False, True):
        stream = BytesIO()
        with Image.new("RGB", (20, 20), "red") as image:
            image.save(stream, format="JPEG", progressive=progressive)
        with load_source_page_image(
            BytesIO(stream.getvalue()), policy=ArtifactRenderPolicy()
        ):
            pass
    assert observed == [False, False, False, True]


def test_queued_exclusive_decode_drains_regular_jobs_and_blocks_new_jobs() -> None:
    scheduler = _DecodeScheduler()
    exclusive_entered = Event()
    release_exclusive = Event()
    later_regular_entered = Event()
    failures: list[BaseException] = []

    def exclusive_job() -> None:
        try:
            with scheduler.acquire(exclusive=True):
                exclusive_entered.set()
                assert release_exclusive.wait(5)
        except BaseException as error:
            failures.append(error)

    def regular_job() -> None:
        try:
            with scheduler.acquire(exclusive=False):
                later_regular_entered.set()
        except BaseException as error:
            failures.append(error)

    exclusive_thread = Thread(target=exclusive_job)
    regular_thread = Thread(target=regular_job)
    try:
        with scheduler.acquire(exclusive=False):
            exclusive_thread.start()
            with scheduler._condition:
                assert scheduler._condition.wait_for(
                    lambda: scheduler._waiting_exclusive == 1, timeout=5
                )
            regular_thread.start()
            assert not exclusive_entered.is_set()
        assert exclusive_entered.wait(5)
        assert not later_regular_entered.is_set()
    finally:
        release_exclusive.set()
        exclusive_thread.join(5)
        regular_thread.join(5)
    assert not exclusive_thread.is_alive()
    assert not regular_thread.is_alive()
    assert later_regular_entered.is_set()
    assert failures == []


@pytest.mark.parametrize("exclusive", [False, True])
def test_decode_slot_is_released_when_the_decoder_fails(exclusive: bool) -> None:
    scheduler = _DecodeScheduler()
    with pytest.raises(ValueError, match="decoder failed"):
        with scheduler.acquire(exclusive=exclusive):
            raise ValueError("decoder failed")
    with scheduler.acquire(exclusive=not exclusive):
        assert scheduler._exclusive is (not exclusive)


def test_regular_decoders_retain_parallelism() -> None:
    scheduler = _DecodeScheduler()
    rendezvous = Barrier(2)

    def decode() -> None:
        with scheduler.acquire(exclusive=False):
            rendezvous.wait(timeout=5)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first, second = executor.submit(decode), executor.submit(decode)
        first.result(timeout=5)
        second.result(timeout=5)


def test_pipeline_and_native_versions_participate_in_policy_fingerprint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import h2hdb_ingest.artifact as artifact_module

    policy = ArtifactRenderPolicy()
    baseline = artifact_policy_fingerprint_sha256(policy)
    monkeypatch.setattr(artifact_module, "SOURCE_IMAGE_NATIVE_VERSIONS", ("different",))
    assert artifact_policy_fingerprint_sha256(policy) != baseline


def test_pixel_fitting_limits_materialized_output_not_source() -> None:
    width, height = source_module._fit_pixels(
        100_000,
        100_000,
        max_short_side=8192,
        max_long_side=8192,
        max_pixels=MAX_DECODED_PIXELS,
    )
    assert (width, height) == (6324, 6324)
    assert width * height <= MAX_DECODED_PIXELS


@pytest.mark.deep
@pytest.mark.parametrize("suffix", [".jpg", ".png"])
def test_real_hundred_megapixel_source_completes_without_pixel_rejection(
    tmp_path: Path, suffix: str
) -> None:
    # Lazy synthetic pixels avoid a 400 MB Pillow fixture allocation. The file
    # is real, and production decoding must evaluate all 100 million pixels.
    source = pyvips.Image.black(10_000, 10_000, bands=3)
    path = tmp_path / ("hundred-megapixel" + suffix)
    source.write_to_file(str(path))
    with path.open("rb") as stream:
        with load_source_page_image(
            cast(BinaryIO, stream), policy=ArtifactRenderPolicy()
        ) as image:
            assert image.size == (768, 768)
            assert image.getpixel((767, 767)) == (0, 0, 0)


def test_large_output_setting_downscales_to_output_pixel_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import h2hdb_ingest.artifact as artifact_module

    monkeypatch.setattr(artifact_module, "MAX_DECODED_PIXELS", 100)
    with load_source_page_image(
        BytesIO(_png((40, 40))),
        policy=ArtifactRenderPolicy(max_image_short_side=8192),
    ) as image:
        assert image.size == (10, 10)


@pytest.mark.parametrize(
    "failure",
    [
        MemoryError("allocation failed"),
        pyvips.Error("unable to load source", "VipsJpeg: Insufficient memory"),
        pyvips.Error(
            "unable to load source", "unknown decoder or infrastructure failure"
        ),
    ],
)
def test_resource_and_unknown_native_failures_are_not_invalid_source(
    monkeypatch: pytest.MonkeyPatch, failure: BaseException
) -> None:
    def fail_header(bridge: object) -> None:
        del bridge
        raise failure

    monkeypatch.setattr(source_module, "_read_header", fail_header)
    with pytest.raises(type(failure)) as caught:
        load_source_page_image(BytesIO(b"ignored"), policy=ArtifactRenderPolicy())
    assert caught.value is failure


def test_source_byte_policy_error_has_explicit_size_and_limit() -> None:
    error = SourceImageSizeLimitError(33 * 1024 * 1024)
    assert error.size_bytes == 33 * 1024 * 1024
    assert error.limit_bytes == 32 * 1024 * 1024
    assert not isinstance(error, source_module.SourceImageDecodeError)
    assert "source_bytes=34603008 limit_bytes=33554432" in str(error)


def test_parallel_native_error_is_classified_only_after_exclusive_verification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduler = _DecodeScheduler()
    monkeypatch.setattr(source_module, "_SCHEDULER", scheduler)
    calls = 0

    def ambiguous_then_precise(bridge: object) -> None:
        nonlocal calls
        del bridge
        calls += 1
        if calls == 1:
            assert scheduler._active == 1
            raise pyvips.Error("unable to load from source", " ")
        assert scheduler._exclusive
        raise pyvips.Error(
            "unable to load from source",
            "VipsForeignLoad: source is not in a known format",
        )

    monkeypatch.setattr(source_module, "_read_header", ambiguous_then_precise)
    with pytest.raises(PresentationImageError, match="truncated or invalid"):
        load_source_page_image(BytesIO(b"garbage"), policy=ArtifactRenderPolicy())
    assert calls == 2


def test_parallel_healthy_corrupt_and_unknown_native_results_stay_distinct(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = source_module._decode_pixels
    unknown = pyvips.Error("unable to decode", "unknown native allocation failure")
    unknown_calls = 0

    def decode(
        bridge: source_module._SourceBridge,
        header: source_module._SourceHeader,
        *,
        max_short_side: int,
        max_long_side: int,
        max_pixels: int,
        resampler: Image.Resampling,
    ) -> Image.Image:
        nonlocal unknown_calls
        if header.width == 400:
            unknown_calls += 1
            raise unknown
        return original(
            bridge,
            header,
            max_short_side=max_short_side,
            max_long_side=max_long_side,
            max_pixels=max_pixels,
            resampler=resampler,
        )

    monkeypatch.setattr(source_module, "_decode_pixels", decode)
    corrupt = _png((300, 300))
    contents = (_png((200, 200)), corrupt[: len(corrupt) * 3 // 4], _png((400, 400)))
    with ThreadPoolExecutor(max_workers=3) as executor:
        healthy, invalid, uncertain = (
            executor.submit(
                load_source_page_image,
                BytesIO(content),
                policy=ArtifactRenderPolicy(),
            )
            for content in contents
        )
        with healthy.result(timeout=5) as image:
            assert image.size == (200, 200)
        with pytest.raises(PresentationImageError, match="truncated or invalid"):
            invalid.result(timeout=5)
        with pytest.raises(pyvips.Error) as caught:
            uncertain.result(timeout=5)
        assert caught.value is unknown
    assert unknown_calls == 2
