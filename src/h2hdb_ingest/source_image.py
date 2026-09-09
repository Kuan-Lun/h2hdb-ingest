"""Stream source images into bounded intermediate pixels before final resampling.

Pixel and encoded-byte ceilings belong to generated pages, never to source
eligibility. Source bytes are read through fixed-size callbacks.
Most codecs support sequential decoding; progressive JPEG and interlaced PNG
still retain codec-owned full-image state, so those jobs run exclusively.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from fractions import Fraction
from importlib.metadata import version
from math import isqrt
from threading import Condition
from typing import BinaryIO

import pyvips  # type: ignore[import-untyped]  # pyvips 3.2 ships no PEP 561 marker.
from PIL import Image

from .artifact_errors import attach_image_dimensions

# Page workers already bound parallelism. A second native pool per page would
# oversubscribe the machine, while an operation cache retains unrelated galleries.
pyvips.concurrency_set(1)
pyvips.cache_set_max(0)

SOURCE_IMAGE_PIPELINE_ID = b"vips-thumbnail-2x-pillow-final-v1"
SOURCE_IMAGE_NATIVE_VERSIONS = (
    pyvips.__version__,
    version("pyvips-binary"),
    ".".join(str(pyvips.version(part)) for part in range(3)),
)
_READ_CHUNK_BYTES = 1024 * 1024


class SourceImageDecodeError(ValueError):
    """A native decoder could not produce the source image's pixels."""


class _DecodeScheduler:
    """Allow regular decoders together and give queued exclusive jobs priority."""

    def __init__(self) -> None:
        self._condition = Condition()
        self._active = 0
        self._exclusive = False
        self._waiting_exclusive = 0

    @contextmanager
    def acquire(self, *, exclusive: bool) -> Iterator[None]:
        with self._condition:
            if exclusive:
                self._waiting_exclusive += 1
                self._condition.notify_all()
                try:
                    self._condition.wait_for(
                        lambda: not self._active and not self._exclusive
                    )
                    self._exclusive = True
                finally:
                    self._waiting_exclusive -= 1
                    self._condition.notify_all()
            else:
                self._condition.wait_for(
                    lambda: not self._exclusive and not self._waiting_exclusive
                )
                self._active += 1
        try:
            yield
        finally:
            with self._condition:
                if exclusive:
                    self._exclusive = False
                else:
                    self._active -= 1
                self._condition.notify_all()


_SCHEDULER = _DecodeScheduler()


class _SourceBridge:
    """Keep callback exceptions intact instead of losing them across CFFI."""

    def __init__(self, stream: BinaryIO) -> None:
        errors: list[BaseException] = []

        def read(size: int) -> bytes | None:
            try:
                return stream.read(min(size, _READ_CHUNK_BYTES)) or None
            except BaseException as error:
                errors.append(error)
                return None

        def seek(offset: int, whence: int) -> int:
            try:
                return stream.seek(offset, whence)
            except BaseException as error:
                errors.append(error)
                return -1

        self.source = pyvips.SourceCustom()
        self.source.on_read(read)
        self.source.on_seek(seek)
        self.errors = errors

    def check(self) -> None:
        if self.errors:
            raise self.errors[0]


@dataclass(frozen=True, slots=True)
class _SourceHeader:
    width: int
    height: int
    orientation: int
    exclusive: bool
    source_kind: str

    @property
    def oriented_size(self) -> tuple[int, int]:
        if self.orientation in {5, 6, 7, 8}:
            return self.height, self.width
        return self.width, self.height


def _read_header(bridge: _SourceBridge) -> _SourceHeader:
    header = pyvips.Image.new_from_source(
        bridge.source, "fail_on=error", access="sequential"
    )
    bridge.check()
    return _SourceHeader(
        width=header.width,
        height=header.height,
        orientation=int(header.get("orientation"))
        if header.get_typeof("orientation")
        else 1,
        exclusive=bool(header.get("interlaced"))
        if header.get_typeof("interlaced")
        else False,
        source_kind=str(header.get("vips-loader")),
    )


def _fit_pixels(
    width: int,
    height: int,
    *,
    max_short_side: int,
    max_long_side: int,
    max_pixels: int,
) -> tuple[int, int]:
    scale = min(
        Fraction(1),
        Fraction(max_short_side, min(width, height)),
        Fraction(max_long_side, max(width, height)),
    )
    output_width = max(1, width * scale.numerator // scale.denominator)
    output_height = max(1, height * scale.numerator // scale.denominator)
    if output_width * output_height > max_pixels:
        output_width = min(output_width, max(1, isqrt(max_pixels * width // height)))
        output_height = min(output_height, max(1, isqrt(max_pixels * height // width)))
        # Clamping the thin dimension to one pixel can require a second bound.
        if output_width * output_height > max_pixels:
            if width >= height:
                output_width = max_pixels // output_height
            else:
                output_height = max_pixels // output_width
    return output_width, output_height


def _decode_pixels(
    bridge: _SourceBridge,
    header: _SourceHeader,
    *,
    max_short_side: int,
    max_long_side: int,
    max_pixels: int,
    resampler: Image.Resampling,
) -> Image.Image:
    width, height = header.oriented_size
    target = _fit_pixels(
        width,
        height,
        max_short_side=max_short_side,
        max_long_side=max_long_side,
        max_pixels=max_pixels,
    )
    intermediate = _fit_pixels(
        width,
        height,
        max_short_side=2 * min(target),
        max_long_side=2 * max(target),
        max_pixels=max_pixels,
    )
    # thumbnail_source's fail_on keyword is not forwarded to its loader by
    # libvips 8.18. Pass the loader option explicitly, including on header reads.
    small = pyvips.Image.thumbnail_source(
        bridge.source,
        intermediate[0],
        height=intermediate[1],
        size="down",
        option_string="fail_on=error",
    )
    if small.interpretation != "srgb":
        small = small.colourspace("srgb")
    small = small.cast("uchar")
    pixels = small.write_to_memory()
    bridge.check()
    mode = "RGBA" if small.hasalpha() else "RGB"
    image = Image.frombytes(mode, (small.width, small.height), pixels)
    try:
        image.thumbnail(target, resampler)
    except BaseException:
        image.close()
        raise
    return image


def _is_invalid_input(error: pyvips.Error) -> bool:
    # pyvips exposes only native diagnostic text, not a typed error code. Keep
    # this positive list deliberately narrow: unknown native failures (including
    # resource/allocation errors) must never become durable bad-source facts.
    known = (
        "VipsForeignLoad: source is not in a known format",
        "VipsJpeg: premature end of JPEG image",
        "vipspng: libpng read error",
        "vipspng: IDAT: CRC error",
    )
    details = str(error.detail).strip().splitlines()
    return bool(details) and all(line.strip() in known for line in details)


def load_source_image(
    source: BinaryIO,
    *,
    max_short_side: int,
    max_long_side: int,
    max_pixels: int,
    resampler: Image.Resampling,
) -> Image.Image:
    """Fully evaluate source pixels and return an owned, bounded Pillow image.

    Source read/seek failures retain their original type. Only native decode
    failures become SourceImageDecodeError; no source dimensions are rejected.
    ``max_pixels`` and side bounds apply to intermediate and output images.
    """

    bridge = _SourceBridge(source)
    header: _SourceHeader | None = None
    try:
        try:
            with _SCHEDULER.acquire(exclusive=False):
                header = _read_header(bridge)
            with _SCHEDULER.acquire(exclusive=header.exclusive):
                return _decode_pixels(
                    bridge,
                    header,
                    max_short_side=max_short_side,
                    max_long_side=max_long_side,
                    max_pixels=max_pixels,
                    resampler=resampler,
                )
        except pyvips.Error:
            bridge.check()
            # Native diagnostics use a process-global buffer. A concurrent
            # successful loader may clear another job's error, so it cannot be
            # evidence for a durable source rejection. Re-evaluate immutable
            # bytes under an exclusive slot before classifying any failure.
            with _SCHEDULER.acquire(exclusive=True):
                source.seek(0)
                isolated = _SourceBridge(source)
                pyvips.vips_lib.vips_error_clear()
                try:
                    header = _read_header(isolated)
                    return _decode_pixels(
                        isolated,
                        header,
                        max_short_side=max_short_side,
                        max_long_side=max_long_side,
                        max_pixels=max_pixels,
                        resampler=resampler,
                    )
                except pyvips.Error as error:
                    isolated.check()
                    if not _is_invalid_input(error):
                        raise
                    raise SourceImageDecodeError(
                        f"image is truncated or invalid: {error}"
                    ) from error
    except BaseException as error:
        if header is not None:
            attach_image_dimensions(
                error,
                width=header.width,
                height=header.height,
                source_kind=header.source_kind,
            )
        raise
