"""Per-worker image costs, returned to the archive owner without page logging.

Durations are cumulative worker elapsed time, not archive wall time. Native
decode includes the input callbacks and libvips shrink; input reads are its
inclusive subphase. Thread CPU excludes libvips work on other native threads.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from time import monotonic_ns, thread_time_ns
from typing import Literal

ImagePhase = Literal[
    "source_verify",
    "decoder_pipeline",
    "decode_and_shrink",
    "resize",
    "color_convert",
    "jpeg_encode",
    "encoded_copy_hash",
    "decoder_input_read",
]


@dataclass(slots=True)
class ImageWorkMeasurement:
    phases_ns: dict[ImagePhase, int] = field(default_factory=dict)
    elapsed_ns: int = 0
    thread_cpu_ns: int = 0
    decoder_input_bytes: int = 0

    @contextmanager
    def phase(self, name: ImagePhase) -> Iterator[None]:
        started = monotonic_ns()
        try:
            yield
        finally:
            self.phases_ns[name] = (
                self.phases_ns.get(name, 0) + monotonic_ns() - started
            )


_current: ContextVar[ImageWorkMeasurement | None] = ContextVar(
    "image_work_measurement", default=None
)


def current_image_measurement() -> ImageWorkMeasurement | None:
    return _current.get()


@contextmanager
def image_phase(name: ImagePhase) -> Iterator[None]:
    measured = _current.get()
    if measured is None:
        yield
    else:
        with measured.phase(name):
            yield


@contextmanager
def measure_image_work() -> Iterator[ImageWorkMeasurement]:
    measured = ImageWorkMeasurement()
    token = _current.set(measured)
    started, cpu_started = monotonic_ns(), thread_time_ns()
    try:
        yield measured
    finally:
        measured.elapsed_ns = monotonic_ns() - started
        measured.thread_cpu_ns = thread_time_ns() - cpu_started
        _current.reset(token)
