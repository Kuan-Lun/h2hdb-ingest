"""Attribution must preserve real outcomes and reject missing work evidence."""

from __future__ import annotations

import copy
import json
import os
import runpy
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest
from PIL import Image

import h2hdb_ingest.image_qualification as qualification
import h2hdb_ingest.source_image as source_image

_SCRIPT = Path(__file__).parents[1] / "scripts" / "probe-qualification-phases.py"


def _page(codec: str = "PNG", size: tuple[int, int] = (160, 220)) -> bytes:
    with Image.new("RGB", size, "navy") as image:
        output = BytesIO()
        image.save(output, format=codec)
        return output.getvalue()


@pytest.fixture
def probe() -> dict[str, Any]:
    return runpy.run_path(str(_SCRIPT))


@pytest.mark.parametrize("workers", (1, 4))
@pytest.mark.parametrize("codec", ("JPEG", "PNG", "GIF", "WEBP"))
def test_instrumentation_preserves_complete_real_observation_and_conserves_costs(
    tmp_path: Path, probe: dict[str, Any], workers: int, codec: str
) -> None:
    root = probe["create_fixture"](
        tmp_path / "source",
        [(f"{page:03}.{codec.lower()}", _page(codec)) for page in range(5)],
    )
    plain = probe["run_case"](root, workers=workers, instrumented=False)
    measured = probe["run_case"](root, workers=workers, collect_events=True)
    assert plain["content_oracle"] == measured["content_oracle"]
    assert plain["qualification"] == measured["qualification"]
    assert plain["attribution_status"] == "not_instrumented"
    assert measured["attribution_status"] == "complete"
    assert measured["qualification_wall_ns"] <= measured["observe_gallery_wall_ns"]
    assert (
        measured["observe_gallery_wall_ns"] <= measured["complete_observation_wall_ns"]
    )
    attribution = measured["attribution"]
    assert len(attribution["workers"]) == 5
    assert attribution["peak_live_spools"] <= workers
    assert (
        attribution["counters"]["source_read_bytes"]
        == measured["fixture"]["page_bytes"]
    )
    for worker in attribution["workers"]:
        assert (
            worker["header_calls"]
            == worker["decode_calls"]
            == worker["thumbnail_calls"]
            == 1
        )
        assert worker["phases_ns_inclusive"]["resize"] > 0
        assert worker["worker_other_ns"] >= 0
    probe["validate_attribution"](measured)


@pytest.mark.parametrize("workers", (1, 4))
def test_invalid_page_retains_exact_gallery_rejection(
    tmp_path: Path, probe: dict[str, Any], workers: int
) -> None:
    root = probe["create_fixture"](
        tmp_path / "source",
        [("000.png", _page()), ("001.png", b"invalid source bytes")],
        expected_qualification={
            "accepted": False,
            "reason_code": "invalid_image",
            "source_name": b"001.png",
        },
    )
    plain = probe["run_case"](root, workers=workers, instrumented=False)
    measured = probe["run_case"](root, workers=workers)
    assert plain["content_oracle"] == measured["content_oracle"]
    assert plain["qualification"] == measured["qualification"]
    assert measured["qualification"]["accepted"] is False
    assert any(row["invalid_image"] for row in measured["attribution"]["workers"])


@pytest.mark.parametrize(
    "failure", (OSError("storage failed"), MemoryError("allocation failed"))
)
@pytest.mark.parametrize("instrumented", (False, True))
def test_resource_failures_propagate_and_restore_all_wrappers(
    tmp_path: Path,
    probe: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    failure: BaseException,
    instrumented: bool,
) -> None:
    root = probe["create_fixture"](tmp_path / "source", [("000.png", _page())])
    original_read, original_spool = os.read, qualification._spool
    original_executor, original_thumbnail = (
        vars(qualification)["ThreadPoolExecutor"],
        Image.Image.thumbnail,
    )

    def fail(*_args: object, **_kwargs: object) -> Any:
        raise failure

    monkeypatch.setattr(qualification, "load_source_page_image", fail)
    with pytest.raises(type(failure)) as caught:
        probe["run_case"](root, workers=1, instrumented=instrumented)
    assert caught.value is failure
    assert os.read is original_read and qualification._spool is original_spool
    assert vars(qualification)["ThreadPoolExecutor"] is original_executor
    assert Image.Image.thumbnail is original_thumbnail
    assert probe["_RUN_LOCK"].locked() is False


@pytest.mark.parametrize("omitted", ("resize", "decode_and_shrink", "decoder_pipeline"))
def test_real_missing_phase_negative_control_is_rejected(
    tmp_path: Path, probe: dict[str, Any], monkeypatch: pytest.MonkeyPatch, omitted: str
) -> None:
    root = probe["create_fixture"](
        tmp_path / "source", [("000.png", _page(size=(900, 1200)))]
    )
    original = vars(source_image)["image_phase"]

    @contextmanager
    def phase(name: Any) -> Iterator[None]:
        if name == omitted:
            yield
        else:
            with original(name):
                yield

    monkeypatch.setattr(source_image, "image_phase", phase)
    with pytest.raises(ValueError, match="omitted"):
        probe["run_case"](root, workers=1)


@pytest.mark.parametrize(
    "missing",
    (
        "source_read_bytes",
        "spool_write_bytes",
        "spool_readback_bytes",
        "source_hash_bytes",
        "spool_hash_bytes",
        "decoder_buffer_read_bytes",
    ),
)
def test_byte_attribution_deletion_cannot_be_reported_complete(
    tmp_path: Path, probe: dict[str, Any], missing: str
) -> None:
    root = probe["create_fixture"](tmp_path / "source", [("000.png", _page())])
    result = probe["run_case"](root, workers=1)
    del result["attribution"]["counters"][missing]
    with pytest.raises(ValueError, match=r"conservation|meters disagree"):
        probe["validate_attribution"](result)


@pytest.mark.parametrize(
    "broken",
    ("worker", "spool", "owner_wait", "scheduler_wait", "unclosed", "owner_partition"),
)
def test_omitted_operations_and_nonconserved_intervals_fail_closed(
    tmp_path: Path, probe: dict[str, Any], broken: str
) -> None:
    root = probe["create_fixture"](tmp_path / "source", [("000.png", _page())])
    report = copy.deepcopy(probe["run_case"](root, workers=1))
    measured = report["attribution"]
    if broken == "worker":
        measured["workers"].clear()
    elif broken == "spool":
        measured["event_calls"]["spool"] = 0
    elif broken == "owner_wait":
        measured["event_calls"]["future_wait"] = 0
    elif broken == "scheduler_wait":
        measured["event_calls"]["scheduler_wait"] = 0
    elif broken == "unclosed":
        measured["buffers"][0]["closed_ns"] = None
    else:
        report["qualification_owner_other_ns"] += 1
    with pytest.raises(ValueError):
        probe["validate_attribution"](report)


def test_wrong_expected_qualification_is_not_relabelled_as_agreement(
    tmp_path: Path, probe: dict[str, Any]
) -> None:
    root = probe["create_fixture"](tmp_path / "source", [("000.png", b"bad")])
    with pytest.raises(ValueError, match="qualification differs"):
        probe["run_case"](root, workers=1)


@pytest.mark.parametrize("change", ("bytes", "unknown", "symlink"))
def test_fixture_preflight_rejects_foreign_or_changed_bytes(
    tmp_path: Path, probe: dict[str, Any], change: str
) -> None:
    root = probe["create_fixture"](tmp_path / "source", [("000.png", _page())])
    page = root / "1000000" / "000.png"
    if change == "bytes":
        page.write_bytes(b"changed")
    elif change == "unknown":
        (root / "foreign").write_bytes(b"unknown")
    else:
        page.unlink()
        page.symlink_to(tmp_path / "absent")
    with pytest.raises(ValueError, match="fixture"):
        probe["run_case"](root, workers=1)


def test_probe_rejects_nontemporary_or_existing_fixture_roots(
    tmp_path: Path, probe: dict[str, Any]
) -> None:
    with pytest.raises(ValueError, match="temporary root"):
        probe["create_fixture"](Path.cwd() / "not-a-fixture", [("000.png", _page())])
    with pytest.raises(FileExistsError):
        probe["create_fixture"](tmp_path, [("000.png", _page())])


def test_serial_process_instrumentation_guard(
    tmp_path: Path, probe: dict[str, Any]
) -> None:
    with probe["_RUN_LOCK"]:
        with pytest.raises(RuntimeError, match="concurrently"):
            probe["run_case"](tmp_path, workers=1)


def test_cli_incomplete_worker_is_an_error_document(
    tmp_path: Path, probe: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(*_args: object, **_kwargs: object) -> Any:
        raise RuntimeError("injected incomplete child")

    output = tmp_path / "result.json"
    monkeypatch.setitem(probe["_COST"], "_bounded_worker", fail)
    monkeypatch.setattr(sys, "argv", [str(_SCRIPT), "--output", str(output)])
    assert probe["main"]() == 2
    assert json.loads(output.read_text())["status"] == "incomplete"


def test_duplicate_real_decoding_negative_control_is_rejected(
    tmp_path: Path, probe: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    root = probe["create_fixture"](tmp_path / "source", [("000.png", _page())])
    original = vars(qualification)["load_source_page_image"]

    def duplicate(stream: Any, **kwargs: Any) -> Any:
        with original(stream, **kwargs):
            pass
        stream.seek(0)
        return original(stream, **kwargs)

    monkeypatch.setattr(qualification, "load_source_page_image", duplicate)
    with pytest.raises(ValueError, match="loader path"):
        probe["run_case"](root, workers=1)


def test_legitimate_exclusive_native_retry_is_still_accepted(
    tmp_path: Path, probe: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    root = probe["create_fixture"](tmp_path / "source", [("000.png", _page())])
    original = source_image._decode_pixels
    attempts = 0

    def transient(*args: Any, **kwargs: Any) -> Any:
        nonlocal attempts
        attempts += 1
        if attempts % 2:
            raise vars(source_image)["pyvips"].Error("initial native failure", " ")
        return original(*args, **kwargs)

    monkeypatch.setattr(source_image, "_decode_pixels", transient)
    plain = probe["run_case"](root, workers=1, instrumented=False)
    measured = probe["run_case"](root, workers=1)
    assert plain["content_oracle"] == measured["content_oracle"]
    row = measured["attribution"]["workers"][0]
    assert row["decode_calls"] == 2
    assert row["thumbnail_calls"] == 1


def test_unsafe_concurrent_exclusive_decoders_are_rejected(
    tmp_path: Path, probe: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    from threading import Barrier

    with Image.new("RGB", (160, 220), "red") as image:
        output = BytesIO()
        image.save(output, format="JPEG", progressive=True)
    root = probe["create_fixture"](
        tmp_path / "source",
        [(f"{page:03}.jpg", output.getvalue()) for page in range(2)],
    )
    rendezvous = Barrier(2)
    original = source_image._decode_pixels

    @contextmanager
    def unsafe_acquire(*, exclusive: bool) -> Iterator[None]:
        del exclusive
        yield

    def together(*args: Any, **kwargs: Any) -> Any:
        rendezvous.wait(timeout=5)
        return original(*args, **kwargs)

    monkeypatch.setattr(source_image._SCHEDULER, "acquire", unsafe_acquire)
    monkeypatch.setattr(source_image, "_decode_pixels", together)
    with pytest.raises(ValueError, match="exclusive"):
        probe["run_case"](root, workers=2)


def test_probe_source_change_during_measurement_is_rejected(
    tmp_path: Path, probe: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    root = probe["create_fixture"](tmp_path / "source", [("000.png", _page())])
    retained_source = tmp_path / "probe-source.py"
    retained_source.write_bytes(_SCRIPT.read_bytes())
    monkeypatch.setitem(
        probe["_run_case"].__globals__, "__file__", str(retained_source)
    )
    original = qualification._spool

    def change_probe(*args: Any, **kwargs: Any) -> Any:
        result = original(*args, **kwargs)
        retained_source.write_bytes(b"modified measurement code")
        return result

    monkeypatch.setattr(qualification, "_spool", change_probe)
    with pytest.raises(RuntimeError, match="phase probe source changed"):
        probe["run_case"](root, workers=1)


@pytest.mark.skipif(os.name != "posix", reason="manual CLI owns POSIX process groups")
def test_real_cli_uses_worker_owned_temporary_root_and_completes_all_cases(
    tmp_path: Path,
) -> None:
    import subprocess

    output = tmp_path / "real-cli.json"
    subprocess.run(
        [
            sys.executable,
            str(_SCRIPT),
            "--output",
            str(output),
            "--timeout-seconds",
            "60",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=75,
    )
    report = json.loads(output.read_text())
    assert report["status"] == "completed"
    assert len(report["cases"]) == 16
    assert all(case["attribution_status"] == "complete" for case in report["cases"])
    assert {(case["label"], case["workers"]) for case in report["cases"]} == {
        (f"{codec}-{edge}", workers)
        for codec in ("JPEG", "PNG", "GIF", "WEBP")
        for edge in (256, 1536)
        for workers in (1, 4)
    }


def test_cli_rejects_nominal_completed_count_without_case_evidence(
    probe: dict[str, Any],
) -> None:
    with pytest.raises(KeyError):
        probe["_validate_cli_report"](
            {"status": "completed", "format": 1, "cases": [{}] * 16}
        )
