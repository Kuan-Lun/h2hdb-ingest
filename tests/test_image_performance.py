"""Worker image measurements retain independent owners and elapsed semantics."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from h2hdb_ingest._image_performance import (
    ImageWorkMeasurement,
    current_image_measurement,
    image_phase,
    measure_image_work,
)


def test_parallel_workers_return_independent_measurements() -> None:
    ready = Barrier(2)

    def work(number: int) -> ImageWorkMeasurement:
        assert current_image_measurement() is None
        with measure_image_work() as measured:
            ready.wait(timeout=5)
            assert current_image_measurement() is measured
            with image_phase("jpeg_encode"):
                measured.decoder_input_bytes = number
            with image_phase("decoder_pipeline"), image_phase("resize"):
                ready.wait(timeout=5)
        assert current_image_measurement() is None
        return measured

    with ThreadPoolExecutor(max_workers=2) as executor:
        first, second = tuple(executor.map(work, (11, 29)))
    assert first is not second
    assert (first.decoder_input_bytes, second.decoder_input_bytes) == (11, 29)
    for measured in (first, second):
        assert measured.phases_ns["resize"] <= measured.phases_ns["decoder_pipeline"]
        assert (
            measured.phases_ns["jpeg_encode"] + measured.phases_ns["decoder_pipeline"]
            <= measured.elapsed_ns
        )
        assert measured.thread_cpu_ns > 0


def test_failure_restores_outer_owner_and_retains_failed_phase_time() -> None:
    with measure_image_work() as outer:
        with pytest.raises(ValueError, match="render failure"):
            with measure_image_work() as failed, image_phase("jpeg_encode"):
                raise ValueError("render failure")
        assert current_image_measurement() is outer
    assert current_image_measurement() is None
    assert failed.phases_ns["jpeg_encode"] > 0
    assert failed.elapsed_ns >= failed.phases_ns["jpeg_encode"]
    assert not outer.phases_ns
