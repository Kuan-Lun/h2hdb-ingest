"""Attribution must preserve real outcomes and reject missing work evidence."""

from __future__ import annotations

import copy
import json
import os
import runpy
import shutil
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest
from PIL import Image

import h2hdb_ingest.image_qualification as qualification
import h2hdb_ingest.source_image as source_image
from h2hdb_ingest._image_performance import ImageWorkMeasurement
from h2hdb_ingest.metrics import IngestMetric
from h2hdb_ingest.source_performance import SourcePerformance

_SCRIPT = Path(__file__).parents[1] / "scripts" / "probe-qualification-phases.py"


def _page(codec: str = "PNG", size: tuple[int, int] = (160, 220)) -> bytes:
    with Image.new("RGB", size, "navy") as image:
        output = BytesIO()
        image.save(output, format=codec)
        return output.getvalue()


@pytest.fixture
def probe() -> dict[str, Any]:
    return runpy.run_path(str(_SCRIPT))


def test_progress_snapshot_is_not_a_second_completed_source_operation(
    tmp_path: Path, probe: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    original = SourcePerformance.operation

    @contextmanager
    def progress(self: SourcePerformance, sink: Any, **kwargs: Any) -> Iterator[None]:
        with original(self, sink, **kwargs):
            sink(
                IngestMetric(
                    scope="source_progress",
                    operation="synchronize",
                    status="progress",
                    elapsed_ns=1,
                )
            )
            yield

    monkeypatch.setattr(SourcePerformance, "operation", progress)
    root = probe["create_fixture"](tmp_path / "source", [("000.png", _page())])
    result = probe["_measure_case"](root, workers=1)
    assert result["attribution_status"] == "complete"


@pytest.mark.parametrize("workers", (1, 4))
@pytest.mark.parametrize("codec", ("JPEG", "PNG", "GIF", "WEBP"))
def test_instrumentation_preserves_complete_real_observation_and_conserves_costs(
    tmp_path: Path, probe: dict[str, Any], workers: int, codec: str
) -> None:
    root = probe["create_fixture"](
        tmp_path / "source",
        [(f"{page:03}.{codec.lower()}", _page(codec)) for page in range(5)],
    )
    plain = probe["_measure_case"](root, workers=workers, instrumented=False)
    measured = probe["_measure_case"](root, workers=workers, collect_events=True)
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
    plain = probe["_measure_case"](root, workers=workers, instrumented=False)
    measured = probe["_measure_case"](root, workers=workers)
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
        probe["_measure_case"](root, workers=1, instrumented=instrumented)
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
        probe["_measure_case"](root, workers=1)


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
    result = probe["_measure_case"](root, workers=1)
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
    report = copy.deepcopy(probe["_measure_case"](root, workers=1))
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
        probe["_measure_case"](root, workers=1)


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
        probe["_measure_case"](root, workers=1)


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
            probe["_measure_case"](tmp_path, workers=1)


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
        probe["_measure_case"](root, workers=1)


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
    plain = probe["_measure_case"](root, workers=1, instrumented=False)
    measured = probe["_measure_case"](root, workers=1)
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
        probe["_measure_case"](root, workers=2)


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
    with pytest.raises(RuntimeError, match="source changed"):
        probe["_measure_case"](root, workers=1)


@pytest.mark.skipif(os.name != "posix", reason="manual CLI owns POSIX process groups")
def test_real_cli_uses_worker_owned_temporary_root_and_completes_all_cases(
    tmp_path: Path,
) -> None:
    output = tmp_path / "real-cli.json"
    owner = tmp_path / "cli-owner"
    helper = runpy.run_path(str(Path(__file__).with_name("test_check_source_cost.py")))[
        "_run_owned_cli"
    ]
    result = helper(
        owner,
        ["--output", str(output), "--timeout-seconds", "60"],
        script=_SCRIPT,
        timeout=75,
    )
    assert result.returncode == 0, result.stderr
    assert not (owner / "temporary").exists()
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
    with pytest.raises(ValueError):
        probe["_validate_cli_report"](
            {"status": "completed", "format": 1, "cases": [{}] * 16}
        )


@pytest.mark.parametrize(
    "phase",
    (
        "source_read",
        "receipt_source_read",
        "source_hash",
        "spool_write",
        "spool_readback",
        "spool_hash",
        "decoder_buffer_read",
        "header",
    ),
)
@pytest.mark.parametrize("omit_only_first", (False, True))
def test_real_operations_without_corresponding_timings_are_rejected(
    tmp_path: Path,
    probe: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    omit_only_first: bool,
) -> None:
    root = probe["create_fixture"](tmp_path / "source", [("000.png", _page())])
    meter_type = probe["_Meter"]
    original = meter_type.timed
    omitted = 0

    @contextmanager
    def timed(self: Any, name: str, *args: Any, **kwargs: Any) -> Iterator[None]:
        nonlocal omitted
        if name == phase and (not omit_only_first or omitted == 0):
            omitted += 1
            yield  # Actual hashing/read/write and independent counters still execute.
        else:
            with original(self, name, *args, **kwargs):
                yield

    monkeypatch.setattr(meter_type, "timed", timed)
    with pytest.raises(ValueError, match="phase timing"):
        probe["_measure_case"](root, workers=1)
    assert omitted > 0


def _case_grid(case: dict[str, Any], probe: dict[str, Any]) -> dict[str, Any]:
    cases = []
    for codec in ("JPEG", "PNG", "GIF", "WEBP"):
        for edge in (256, 1536):
            for workers in (1, 4):
                item = copy.deepcopy(case)
                item.update(label=f"{codec}-{edge}", workers=workers)
                cases.append(item)
    return {
        "format": 1,
        "status": "completed",
        "cases": cases,
        "matrix_provenance": probe["_source_snapshot"](),
        "execution": case["execution"],
    }


@pytest.mark.parametrize(
    "fault", ("missing", "status", "scope", "digest", "empty_digest")
)
@pytest.mark.skipif(
    os.name != "posix", reason="public probe API owns POSIX worker groups"
)
def test_parent_rejects_missing_or_contradictory_real_observation_oracle(
    tmp_path: Path,
    probe: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    root = probe["create_fixture"](tmp_path / "source", [("000.png", _page())])
    report = _case_grid(probe["run_case"](root, workers=1), probe)
    oracle = report["cases"][0]["content_oracle"]
    if fault == "missing":
        del report["cases"][0]["content_oracle"]
    elif fault in {"status", "scope"}:
        oracle[fault] = "unverified"
    else:
        oracle["observed_sha256"] = "" if fault == "empty_digest" else "f" * 64

    def child(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess([], 0, stdout=json.dumps(report), stderr="")

    monkeypatch.setitem(probe["_COST"], "_bounded_worker", child)
    output = tmp_path / "report.json"
    monkeypatch.setattr(sys, "argv", [str(_SCRIPT), "--output", str(output)])
    assert probe["main"]() == 2
    assert json.loads(output.read_text())["status"] == "incomplete"


@pytest.mark.parametrize("member", ("h2hdb_ingest", "acceptance_sha256"))
def test_matrix_rejects_source_drift_between_real_cases(
    tmp_path: Path,
    probe: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    member: str,
) -> None:
    original = probe["_measure_case"]
    provenance = probe["_COST"]["_provenance"]
    completed = 0
    if member == "h2hdb_ingest":
        watched = tmp_path / "retained-runtime"
        shutil.copytree(_SCRIPT.parents[1] / "src" / "h2hdb_ingest", watched)
        mutated_file = watched / "source_image.py"
    else:
        watched = tmp_path / "retained-helper.py"
        shutil.copyfile(_SCRIPT.with_name("check-source-cost.py"), watched)
        mutated_file = watched

    def watched_provenance(*args: Any, **kwargs: Any) -> Any:
        result = provenance(*args, **kwargs)
        if member == "h2hdb_ingest":
            result[member]["python_source_sha256"] = probe["_IO"]["_package_digest"](
                watched
            )
        else:
            result[member] = sha256(watched.read_bytes()).hexdigest()
        return result

    monkeypatch.setitem(probe["_COST"], "_provenance", watched_provenance)
    expected = probe["_source_snapshot"]()
    assert expected == probe["_LOADED_SOURCE_SNAPSHOT"]

    def case(*args: Any, **kwargs: Any) -> Any:
        nonlocal completed
        value = original(*args, **kwargs)
        completed += 1
        # Real file bytes change after a complete case, while Python still has
        # its previously loaded functions. No invented digest substitutes work.
        with mutated_file.open("ab") as stream:
            stream.write(b"\n# Source changed between measured cases.\n")
        return value

    monkeypatch.setitem(probe["_cli_matrix"].__globals__, "_measure_case", case)
    # This is the private matrix fault seam, not evidence of worker isolation.
    # Real subprocess tests below establish the admission contract separately.
    monkeypatch.setitem(
        probe["_cli_matrix"].__globals__, "_admit_fresh_worker", lambda *_args: {}
    )
    with pytest.raises(RuntimeError, match="source changed"):
        probe["_cli_matrix"](tmp_path, expected_provenance=expected)
    assert completed == 1


@pytest.mark.skipif(
    os.name != "posix", reason="public probe API owns POSIX worker groups"
)
def test_parent_rejects_one_case_with_foreign_runtime_provenance(
    tmp_path: Path, probe: dict[str, Any]
) -> None:
    root = probe["create_fixture"](tmp_path / "source", [("000.png", _page())])
    report = _case_grid(probe["run_case"](root, workers=1), probe)
    report["cases"][0]["provenance"]["h2hdb_ingest"]["python_source_sha256"] = "f" * 64
    with pytest.raises(ValueError, match="stable attribution"):
        probe["_validate_cli_report"](report)


@pytest.mark.parametrize(
    "fault", ("manifest", "unknown_entries", "oversize_member", "aggregate_bytes")
)
def test_actual_unadmitted_inventory_never_reads_gallery_contents(
    tmp_path: Path,
    probe: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    root = probe["create_fixture"](
        tmp_path / "source", [(f"{i:03}.png", bytes(32768)) for i in range(3)]
    )
    folder = root / "1000000"
    if fault == "manifest":
        with (root / probe["_MANIFEST"]).open("wb") as stream:
            stream.truncate(probe["_MAX_MANIFEST_BYTES"] + 1)
    elif fault == "unknown_entries":
        for index in range(500):
            (folder / f"foreign-{index}").write_bytes(b"unknown")
    elif fault == "oversize_member":
        with (folder / "000.png").open("wb") as stream:
            stream.truncate(probe["_MAX_PAGE_BYTES"] + 1)
    else:
        monkeypatch.setitem(probe["_admit_fixture"].__globals__, "_MAX_BYTES", 65536)
    original_scan = os.scandir
    actual_rows = 0
    content_reads = 0

    @contextmanager
    def scanned(path: Any) -> Iterator[Iterator[os.DirEntry[str]]]:
        with original_scan(path) as entries:

            def counted() -> Iterator[os.DirEntry[str]]:
                nonlocal actual_rows
                for entry in entries:
                    if Path(path) == folder:
                        actual_rows += 1
                    yield entry

            yield counted()

    def read_content(*_args: Any, **_kwargs: Any) -> str:
        nonlocal content_reads
        content_reads += 1
        raise AssertionError("unadmitted content must never be opened")

    monkeypatch.setattr(os, "scandir", scanned)
    monkeypatch.setitem(
        probe["_admit_fixture"].__globals__, "_hash_exact_regular", read_content
    )
    with pytest.raises(ValueError, match="fixture"):
        probe["_admit_fixture"](root)
    assert content_reads == 0
    assert actual_rows <= 5  # At most admitted four names plus first unknown entry.


@pytest.mark.skipif(os.name != "posix", reason="manual CLI owns POSIX process groups")
def test_real_cli_outer_sigkill_reclaims_worker_and_qualification_scratch(
    tmp_path: Path,
) -> None:
    owner = tmp_path / "timeout-owner"
    ready = tmp_path / "worker-ready.json"
    output = tmp_path / "timed-out-report.json"
    bootstrap = tmp_path / "stalled_qualification_worker.py"
    bootstrap.write_text(
        "import json,os,pathlib,runpy,signal,sys,time\n"
        f"module=runpy.run_path({str(_SCRIPT)!r})\n"
        "def stall(workspace,**kwargs):\n"
        " from io import BytesIO\n"
        " from PIL import Image\n"
        " from h2hdb_ingest.filesystem import FilesystemSource\n"
        " signal.signal(signal.SIGTERM,signal.SIG_IGN)\n"
        " out=BytesIO()\n"
        " with Image.new('RGB',(16,16),'navy') as image: image.save(out,format='PNG')\n"
        " root=module['create_fixture'](workspace/'temporary'/'timeout-source',[('000.png',out.getvalue())])\n"
        " source=FilesystemSource(root)\n"
        " source.list_gallery_locators(after_locator=None,limit=128)\n"
        " receipt={'pid':os.getpid(),'workspace':str(workspace),\n"
        "          'discovery':source._discovery_temporary.name,\n"
        "          'pycache':sys.pycache_prefix,'fixture':str(root)}\n"
        f" pathlib.Path({str(ready)!r}).write_text(json.dumps(receipt))\n"
        " time.sleep(120)\n"
        " raise AssertionError('outer timeout failed to stop actual worker')\n"
        "module['main'].__globals__['_cli_matrix']=stall\n"
        "module['main'].__globals__['_admit_fresh_worker']=lambda *_args: {}\n"
        "raise SystemExit(module['main']())\n"
    )
    program = (
        f"module=runpy.run_path({str(_SCRIPT)!r})\n"
        "qualification_bound_worker=module['_COST']['_bounded_worker']\n"
        "def stalled_worker(command,**kwargs):\n"
        " command=list(command)\n"
        f" position=command.index({str(_SCRIPT)!r})\n"
        f" command[position]={str(bootstrap)!r}\n"
        " return qualification_bound_worker(command,**kwargs)\n"
        "module['_COST']['_bounded_worker']=stalled_worker\n"
        "raise SystemExit(module['main']())\n"
    )
    helpers = runpy.run_path(str(Path(__file__).with_name("test_check_source_cost.py")))
    with pytest.raises(subprocess.TimeoutExpired):
        helpers["_run_owned_cli"](
            owner,
            ["--output", str(output), "--timeout-seconds", "60"],
            script=_SCRIPT,
            timeout=1,
            worker_ready_path=ready,
            injected_program=program,
            force_supervisor_kill=True,
        )
    receipt = json.loads(ready.read_text())
    recorded = [json.loads(path.read_text()) for path in owner.glob("owned-*.json")]
    assert len(recorded) == 1 and recorded[0]["pid"] == receipt["pid"]
    state = helpers["_process_field"](receipt["pid"], "stat")
    assert state is None or state.startswith("Z")
    assert Path(receipt["workspace"]).is_relative_to(owner / "temporary")
    assert Path(receipt["discovery"]).is_relative_to(
        Path(receipt["workspace"]) / "temporary"
    )
    for key in ("workspace", "discovery", "pycache", "fixture"):
        assert not Path(receipt[key]).exists(), key
    assert not (owner / "temporary").exists()
    assert not output.exists()


@pytest.mark.skipif(os.name != "posix", reason="manual CLI owns POSIX process groups")
@pytest.mark.parametrize("worker_exits", (False, True))
def test_missing_worker_readiness_fails_closed_and_reclaims_owned_group(
    tmp_path: Path,
    worker_exits: bool,
) -> None:
    helpers = runpy.run_path(str(Path(__file__).with_name("test_check_source_cost.py")))
    owner = tmp_path / "unready-owner"
    body = (
        "import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep("
        + ("0.5" if worker_exits else "120")
        + ")"
    )
    program = f"subprocess.Popen([sys.executable,'-c',{body!r}],start_new_session=True).wait()\n"
    with pytest.raises(AssertionError, match="owned start boundary"):
        helpers["_run_owned_cli"](
            owner,
            [],
            injected_program=program,
            force_supervisor_kill=True,
            worker_ready_path=tmp_path / "never-ready",
            timeout=1,
        )
    assert not (owner / "temporary").exists()
    receipts = [json.loads(path.read_text()) for path in owner.glob("owned-*.json")]
    assert len(receipts) == 1
    state = helpers["_process_field"](receipts[0]["pid"], "stat")
    assert state is None or state.startswith("Z")


@pytest.mark.parametrize(
    "phase",
    (
        "source_hash",
        "spool_write",
        "spool_readback",
        "spool_hash",
        "decoder_buffer_read",
    ),
)
@pytest.mark.skipif(
    os.name != "posix", reason="public probe API owns POSIX worker groups"
)
def test_current_cli_report_cannot_drop_independent_operation_counts(
    tmp_path: Path,
    probe: dict[str, Any],
    phase: str,
) -> None:
    root = probe["create_fixture"](tmp_path / "source", [("000.png", _page())])
    report = _case_grid(probe["run_case"](root, workers=1), probe)
    del report["cases"][0]["attribution"]["counters"][phase + "_calls"]
    with pytest.raises(ValueError, match="direct operation counts"):
        probe["_validate_cli_report"](report)


@pytest.mark.parametrize(
    "phase",
    (
        "decoder_input_read",
        "decode_and_shrink",
        "resize",
        "decoder_pipeline",
        "header",
        "scheduler_wait",
    ),
)
def test_one_real_image_phase_contribution_cannot_disappear_into_residual(
    tmp_path: Path, probe: dict[str, Any], monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    root = probe["create_fixture"](tmp_path / "source", [("000.png", _page())])
    original = ImageWorkMeasurement.phase
    omitted = 0

    @contextmanager
    def missing(self: Any, name: Any) -> Iterator[None]:
        nonlocal omitted
        if name == phase and omitted == 0:
            omitted += 1
            yield
        else:
            with original(self, name):
                yield

    monkeypatch.setattr(ImageWorkMeasurement, "phase", missing)
    with pytest.raises(ValueError, match=r"omitted.*timing contribution"):
        probe["_measure_case"](root, workers=1)
    assert omitted == 1


def test_first_native_retry_phase_cannot_hide_behind_positive_second_phase(
    tmp_path: Path, probe: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    root = probe["create_fixture"](tmp_path / "source", [("000.png", _page())])
    original_phase = vars(source_image)["image_phase"]
    native_module = vars(source_image)["pyvips"]
    original_native = native_module.Image.thumbnail_source
    native_calls = 0
    omitted = 0

    def native(*args: Any, **kwargs: Any) -> Any:
        nonlocal native_calls
        native_calls += 1
        result = original_native(*args, **kwargs)
        if native_calls == 1:
            raise native_module.Error("transient native failure", " ")
        return result

    @contextmanager
    def missing(name: Any) -> Iterator[None]:
        nonlocal omitted
        if name == "decode_and_shrink" and omitted == 0:
            omitted += 1
            yield
        else:
            with original_phase(name):
                yield

    monkeypatch.setattr(native_module.Image, "thumbnail_source", native)
    monkeypatch.setattr(source_image, "image_phase", missing)
    with pytest.raises(ValueError, match="operations omitted complete phase timing"):
        probe["_measure_case"](root, workers=1)
    assert native_calls == 2 and omitted == 1


def test_per_page_header_timing_cannot_be_assigned_to_another_worker(
    tmp_path: Path, probe: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    root = probe["create_fixture"](
        tmp_path / "source", [(f"{page:03}.png", _page()) for page in range(2)]
    )
    original = probe["_Meter"].event

    def misattribute(
        self: Any, name: str, owner: str, page: int | None, start: int, end: int
    ) -> None:
        original(self, name, owner, 1 if name == "header" else page, start, end)

    monkeypatch.setattr(probe["_Meter"], "event", misattribute)
    with pytest.raises(ValueError, match=r"header operations lack|nonnegative integer"):
        probe["_measure_case"](root, workers=1)


@pytest.mark.skipif(
    os.name != "posix", reason="public probe API owns POSIX worker groups"
)
def test_single_real_png_cannot_impersonate_complete_cli_matrix(
    tmp_path: Path, probe: dict[str, Any]
) -> None:
    root = probe["create_fixture"](tmp_path / "source", [("000.png", _page())])
    measured = probe["run_case"](root, workers=1, inspect_images=True)
    with pytest.raises(ValueError, match="four-page content"):
        probe["_validate_cli_report"](_case_grid(measured, probe))


@pytest.mark.parametrize("codec,size", (("PNG", (256, 256)), ("JPEG", (256, 255))))
@pytest.mark.skipif(
    os.name != "posix", reason="public probe API owns POSIX worker groups"
)
def test_actual_wrong_codec_or_dimensions_cannot_impersonate_cli_case(
    tmp_path: Path, probe: dict[str, Any], codec: str, size: tuple[int, int]
) -> None:
    root = probe["create_fixture"](
        tmp_path / "source",
        [(f"{page:03}.jpeg", _page(codec, size)) for page in range(4)],
    )
    measured = probe["run_case"](root, workers=1, inspect_images=True)
    with pytest.raises(ValueError, match="codec or dimensions"):
        probe["_validate_cli_report"](_case_grid(measured, probe))


@pytest.mark.skipif(
    os.name != "posix", reason="public probe API owns POSIX worker groups"
)
def test_same_bytes_recreated_in_new_fixture_cannot_hide_cross_worker_drift(
    tmp_path: Path, probe: dict[str, Any]
) -> None:
    pages = [(f"{page:03}.jpeg", _page("JPEG", (256, 256))) for page in range(4)]
    first = probe["run_case"](
        probe["create_fixture"](tmp_path / "first", pages),
        workers=1,
        inspect_images=True,
        label="JPEG-256",
    )
    second = probe["run_case"](
        probe["create_fixture"](tmp_path / "second", pages),
        workers=4,
        inspect_images=True,
        label="JPEG-256",
    )
    assert first["fixture"]["manifest_sha256"] == second["fixture"]["manifest_sha256"]
    report = _case_grid(first, probe)
    report["cases"][1] = second
    with pytest.raises(ValueError, match="different fixture identities"):
        probe["_validate_cli_report"](report)


@pytest.mark.parametrize("replacement", ("oversize", "fifo", "different_inode"))
def test_post_hash_fixture_change_cannot_reach_an_unbounded_oracle_reader(
    tmp_path: Path,
    probe: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    replacement: str,
) -> None:
    if replacement == "fifo" and not hasattr(os, "mkfifo"):
        pytest.skip("FIFO counterexample requires POSIX")
    root = probe["create_fixture"](tmp_path / "source", [("000.png", _page())])
    page = root / "1000000" / "000.png"
    original = probe["_hash_exact_regular"]
    changes = 0
    unbounded_reads = 0

    def hash_then_replace(path: Path, identity: os.stat_result) -> str:
        nonlocal changes
        result = original(path, identity)
        assert isinstance(result, str)
        if path.name == "galleryinfo.txt":
            changes += 1
            if replacement == "oversize":
                with page.open("wb") as stream:
                    stream.truncate(probe["_MAX_PAGE_BYTES"] + 1)
            else:
                page.unlink()
                if replacement == "fifo":
                    os.mkfifo(page)
                else:
                    page.write_bytes(_page())
        return result

    def unsafe_reader(*_args: Any, **_kwargs: Any) -> Any:
        nonlocal unbounded_reads
        unbounded_reads += 1
        raise AssertionError("post-admission oracle must not reopen unbounded paths")

    monkeypatch.setitem(
        probe["_admit_fixture"].__globals__, "_hash_exact_regular", hash_then_replace
    )
    monkeypatch.setitem(probe["_COST"], "_fixture_oracle", unsafe_reader)
    monkeypatch.setattr(Path, "read_bytes", unsafe_reader)
    with pytest.raises(ValueError, match="fixture changed after"):
        probe["_admit_fixture"](root)
    assert changes == 1 and unbounded_reads == 0


def test_manual_case_does_not_use_pillow_header_admission(
    tmp_path: Path, probe: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    root = probe["create_fixture"](tmp_path / "source", [("000.png", _page())])

    def no_header(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("manual qualification must not inherit Pillow pixel caps")

    monkeypatch.setattr(Image, "open", no_header)
    result = probe["_measure_case"](root, workers=1)
    assert result["qualification"]["accepted"] is True
    assert result["fixture"]["image_headers"] == []


@pytest.mark.parametrize("member", ("runtime", "helper", "probe"))
@pytest.mark.parametrize("instrumented", (False, True))
def test_source_change_during_real_fixture_admission_cannot_reset_api_baseline(
    tmp_path: Path,
    probe: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    member: str,
    instrumented: bool,
) -> None:
    root = probe["create_fixture"](tmp_path / "source", [("000.png", _page())])
    original_provenance = probe["_COST"]["_provenance"]
    if member == "runtime":
        watched = tmp_path / "retained-runtime"
        shutil.copytree(_SCRIPT.parents[1] / "src" / "h2hdb_ingest", watched)
        changed_file = watched / "source_image.py"
    else:
        watched = tmp_path / "retained-source.py"
        shutil.copyfile(
            _SCRIPT if member == "probe" else _SCRIPT.with_name("check-source-cost.py"),
            watched,
        )
        changed_file = watched
    if member == "probe":
        monkeypatch.setitem(
            probe["_source_snapshot"].__globals__, "__file__", str(watched)
        )
    else:

        def provenance(*args: Any, **kwargs: Any) -> Any:
            value = original_provenance(*args, **kwargs)
            if member == "runtime":
                value["h2hdb_ingest"]["python_source_sha256"] = probe["_IO"][
                    "_package_digest"
                ](watched)
            else:
                value["acceptance_sha256"] = sha256(watched.read_bytes()).hexdigest()
            return value

        monkeypatch.setitem(probe["_COST"], "_provenance", provenance)
    assert probe["_source_snapshot"]() == probe["_LOADED_SOURCE_SNAPSHOT"]
    original_hash = probe["_hash_exact_regular"]
    changed = 0
    qualification_calls = 0

    def hash_then_change(path: Path, identity: os.stat_result) -> Any:
        nonlocal changed
        result = original_hash(path, identity)
        if not changed:
            changed += 1
            with changed_file.open("ab") as stream:
                stream.write(b"\n# Changed during actual fixture admission.\n")
        return result

    def qualification_started(*_args: Any, **_kwargs: Any) -> Any:
        nonlocal qualification_calls
        qualification_calls += 1
        raise AssertionError("measurement must not begin with different source bytes")

    monkeypatch.setitem(
        probe["_admit_fixture"].__globals__, "_hash_exact_regular", hash_then_change
    )
    monkeypatch.setattr(qualification, "_spool", qualification_started)
    with pytest.raises(RuntimeError, match="source changed"):
        probe["_measure_case"](root, workers=1, instrumented=instrumented)
    assert changed == 1 and qualification_calls == 0
    assert probe["_RUN_LOCK"].locked() is False


def test_shared_process_evidence_cannot_claim_verified_execution(
    tmp_path: Path, probe: dict[str, Any]
) -> None:
    root = probe["create_fixture"](tmp_path / "source", [("000.png", _page())])
    report = probe["_measure_case"](root, workers=1)
    assert report["status"] == "execution_unverified"
    assert report["provenance"]["phase_probe_source_stable"] is False
    with pytest.raises(ValueError, match="fresh-source execution"):
        probe["_validate_execution"](report)
    with pytest.raises(RuntimeError, match="preloaded runtime"):
        probe["_admit_fresh_worker"](tmp_path, probe["_source_snapshot"]())


@pytest.mark.parametrize("invalid", (False, True))
@pytest.mark.skipif(
    os.name != "posix", reason="public probe API owns POSIX worker groups"
)
def test_public_api_measures_in_fresh_worker_despite_poisoned_parent_runtime(
    tmp_path: Path,
    probe: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    invalid: bool,
) -> None:
    expected = {
        "accepted": not invalid,
        "reason_code": "invalid_image" if invalid else None,
        "source_name": b"000.png" if invalid else None,
    }
    root = probe["create_fixture"](
        tmp_path / "source",
        [("000.png", b"invalid bytes" if invalid else _page())],
        expected_qualification=expected,
    )
    private = probe["_measure_case"](root, workers=1)

    def poisoned(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("public measurement executed the caller's cached runtime")

    monkeypatch.setattr(qualification, "_spool", poisoned)
    report = probe["run_case"](root, workers=1)
    assert report["status"] == "completed"
    assert report["qualification"] == private["qualification"]
    assert report["fixture"]["manifest_sha256"] == private["fixture"]["manifest_sha256"]
    assert report["execution"]["isolated"] is True
    assert report["execution"]["preloaded_source_modules"] == []
    assert report["provenance"]["phase_probe_source_stable"] is True


@pytest.mark.skipif(
    os.name != "posix", reason="public probe API owns POSIX worker groups"
)
def test_public_api_rejects_transport_identity_change_and_cleans_workspace(
    tmp_path: Path, probe: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    root = probe["create_fixture"](tmp_path / "source", [("000.png", _page())])
    original_read = probe["_read_exact_regular"]
    original_environment = probe["_COST"]["_fresh_python_environment"]
    workspaces = []

    def environment(command: Any, workspace: Path) -> Any:
        workspaces.append(workspace)
        return original_environment(command, workspace)

    def changed(path: Path, identity: Any, **kwargs: Any) -> Any:
        if path.name == "000.png":
            path.unlink()
            path.write_bytes(_page())
        return original_read(path, identity, **kwargs)

    monkeypatch.setitem(probe["_COST"], "_fresh_python_environment", environment)
    monkeypatch.setitem(
        probe["_transport_fixture"].__globals__, "_read_exact_regular", changed
    )
    with pytest.raises(ValueError, match="changed before content admission"):
        probe["run_case"](root, workers=1)
    assert len(workspaces) == 1 and not workspaces[0].exists()


@pytest.mark.skipif(
    os.name != "posix", reason="public probe API owns POSIX worker groups"
)
def test_public_api_rejects_real_worker_success_for_a_different_fixture(
    tmp_path: Path, probe: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    root = probe["create_fixture"](tmp_path / "source", [("000.png", _page())])
    original = probe["_transport_fixture"]
    changed = 0

    def replace_after_transport(source: Path, destination: Path) -> Any:
        nonlocal changed
        expected = original(source, destination)
        manifest_path = destination / probe["_MANIFEST"]
        other_manifest = json.loads(manifest_path.read_text())
        other_page = _page(size=(300, 450))
        (destination / "1000000" / "000.png").write_bytes(other_page)
        other_manifest["records"][0].update(
            size=len(other_page), sha256=sha256(other_page).hexdigest()
        )
        # Finish this genuinely different gallery after its changed PAGE.
        marker = destination / "1000000" / "galleryinfo.txt"
        marker.write_bytes(marker.read_bytes())
        manifest_path.write_text(json.dumps(other_manifest))
        changed += 1
        return expected

    monkeypatch.setitem(
        probe["run_case"].__globals__, "_transport_fixture", replace_after_transport
    )
    with pytest.raises(ValueError, match="different requested fixture"):
        probe["run_case"](root, workers=1)
    assert changed == 1


@pytest.mark.skipif(
    os.name != "posix", reason="fresh runtime test owns POSIX worker groups"
)
def test_runtime_imported_before_disk_edit_is_never_reported_as_complete(
    tmp_path: Path,
) -> None:
    checkout = tmp_path / "checkout"
    scripts = checkout / "scripts"
    scripts.mkdir(parents=True)
    for name in (
        "probe-qualification-phases.py",
        "check-source-cost.py",
        "probe-source-io.py",
        "_probe_database.py",
        "_probe_environment.py",
        "run-pytest.py",
    ):
        shutil.copyfile(_SCRIPT.with_name(name), scripts / name)
    shutil.copyfile(_SCRIPT.parents[1] / "pyproject.toml", checkout / "pyproject.toml")
    shutil.copytree(
        _SCRIPT.parents[1] / "src" / "h2hdb_ingest", checkout / "src" / "h2hdb_ingest"
    )
    program = (
        f"sys.path.insert(0,{str(checkout / 'src')!r})\n"
        "import h2hdb_ingest.source_image as retained\n"
        "from pathlib import Path\n"
        "import tempfile\n"
        "source=Path(retained.__file__)\n"
        "source.write_text(source.read_text()+'\\nNEW_DISK_ONLY_SENTINEL = True\\n')\n"
        "assert not hasattr(retained,'NEW_DISK_ONLY_SENTINEL')\n"
        f"probe=runpy.run_path({str(scripts / _SCRIPT.name)!r})\n"
        "from io import BytesIO\n"
        "from PIL import Image\n"
        "encoded=BytesIO()\n"
        "with Image.new('RGB',(32,48),'navy') as image: image.save(encoded,format='PNG')\n"
        "root=probe['create_fixture'](Path(tempfile.gettempdir())/'stale-source',[('000.png',encoded.getvalue())])\n"
        "private=probe['_measure_case'](root,workers=1)\n"
        "assert private['status']=='execution_unverified'\n"
        "assert private['provenance']['phase_probe_source_stable'] is False\n"
        "try:\n"
        " probe['run_case'](root,workers=1)\n"
        "except subprocess.CalledProcessError as error:\n"
        " assert 'imported ingest runtime differs' in error.stderr\n"
        "else: raise AssertionError('fresh worker accepted a mismatched loaded checkout')\n"
        "print('cached old runtime remained unverified; fresh environment rejected mismatched source')\n"
    )
    helpers = runpy.run_path(str(Path(__file__).with_name("test_check_source_cost.py")))
    result = helpers["_run_owned_cli"](
        tmp_path / "stale-owner", [], injected_program=program, timeout=30
    )
    assert result.returncode == 0, result.stderr
    assert "cached old runtime remained unverified" in result.stdout


@pytest.mark.parametrize("name", ("page.png", "z.png"))
@pytest.mark.skipif(
    os.name != "posix", reason="public probe API owns POSIX worker groups"
)
def test_fixture_transport_finishes_marker_after_later_sorted_page_names(
    tmp_path: Path, probe: dict[str, Any], name: str
) -> None:
    root = probe["create_fixture"](tmp_path / "source", [(name, _page())])
    report = probe["run_case"](root, workers=1)
    assert report["status"] == "completed"
    assert report["qualification"]["accepted"] is True
    members = {
        bytes.fromhex(row["name_bytes"]): row
        for row in report["fixture"]["source_oracle"]["files"]
    }
    assert (
        members[b"galleryinfo.txt"]["modified_ns"]
        >= members[name.encode()]["modified_ns"]
    )


@pytest.mark.skipif(
    os.name != "posix", reason="public probe API owns POSIX worker groups"
)
def test_source_edit_after_initial_check_cannot_become_a_new_api_baseline(
    tmp_path: Path, probe: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    root = probe["create_fixture"](tmp_path / "source", [("000.png", _page())])
    watched = tmp_path / "retained-helper.py"
    shutil.copyfile(_SCRIPT.with_name("check-source-cost.py"), watched)
    original_provenance = probe["_COST"]["_provenance"]
    original_check = probe["_require_source_snapshot"]
    changed = False

    def provenance(*args: Any, **kwargs: Any) -> Any:
        value = original_provenance(*args, **kwargs)
        value["acceptance_sha256"] = sha256(watched.read_bytes()).hexdigest()
        return value

    def check_then_edit(expected: dict[str, Any]) -> None:
        nonlocal changed
        original_check(expected)
        if not changed:
            with watched.open("ab") as stream:
                stream.write(b"\n# Edit after the first source check.\n")
            changed = True

    def forbidden_worker(*_args: Any, **_kwargs: Any) -> Any:
        pytest.fail("source changed after initial admission; worker must not start")

    monkeypatch.setitem(probe["_COST"], "_provenance", provenance)
    monkeypatch.setitem(
        probe["run_case"].__globals__, "_require_source_snapshot", check_then_edit
    )
    monkeypatch.setitem(probe["_COST"], "_bounded_worker", forbidden_worker)
    with pytest.raises(RuntimeError, match="source changed"):
        probe["run_case"](root, workers=1)
    assert changed
