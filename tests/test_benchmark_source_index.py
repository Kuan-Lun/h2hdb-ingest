from __future__ import annotations

import os
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from threading import Event
from types import ModuleType
from typing import Protocol, cast

import pytest


class _TreeUsage(Protocol):
    def __call__(self, root: Path) -> tuple[int, int]: ...


class _MonitorResources(Protocol):
    def __call__(
        self,
        *,
        stop: Event,
        safety_stop: Event,
        interval_seconds: float,
        benchmark_root: Path,
        database_path: Path,
        failures: list[Exception],
    ) -> None: ...


class _RunPipeline(Protocol):
    def __call__(
        self,
        gallery_count: int,
        *,
        progress_seconds: float,
    ) -> dict[str, object]: ...


def _benchmark_module() -> ModuleType:
    script = Path(__file__).parents[1] / "scripts" / "benchmark-source-index.py"
    spec = spec_from_file_location("h2hdb_ingest_source_benchmark", script)
    if spec is None or spec.loader is None:  # pragma: no cover - fixed local path
        raise RuntimeError("unable to load source-index benchmark")
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_tree_usage_tolerates_artifact_disappearing_during_walk(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _benchmark_module()
    tree_usage = cast(_TreeUsage, module.__dict__["_tree_usage"])
    live = tmp_path / "live.cbz"
    vanished = tmp_path / "vanished.cbz"
    live.write_bytes(b"live")
    vanished.write_bytes(b"renamed concurrently")
    original_stat = Path.stat

    def racing_stat(
        path: Path,
        *,
        follow_symlinks: bool = True,
    ) -> os.stat_result:
        if path == vanished:
            raise FileNotFoundError(path)
        return original_stat(path, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(Path, "stat", racing_stat)

    assert tree_usage(tmp_path) == (1, len(b"live"))


def test_resource_monitor_failure_sets_safety_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _benchmark_module()
    monitor_resources = cast(
        _MonitorResources,
        module.__dict__["_monitor_resources"],
    )
    database_path = tmp_path / "catalog.sqlite3"
    database_path.touch()

    def failed_usage(_root: Path) -> tuple[int, int]:
        raise OSError("synthetic monitor fault")

    monkeypatch.setitem(module.__dict__, "_tree_usage", failed_usage)
    stop = Event()
    safety_stop = Event()
    failures: list[Exception] = []

    monitor_resources(
        stop=stop,
        safety_stop=safety_stop,
        interval_seconds=0.001,
        benchmark_root=tmp_path,
        database_path=database_path,
        failures=failures,
    )

    assert safety_stop.is_set()
    assert len(failures) == 1
    assert isinstance(failures[0], OSError)
    assert str(failures[0]) == "synthetic monitor fault"


def test_resource_monitor_does_not_pollute_source_scan_count() -> None:
    module = _benchmark_module()
    run_pipeline = cast(_RunPipeline, module.__dict__["_run_pipeline"])

    result = run_pipeline(1, progress_seconds=0.05)

    assert result["gallery_scans_including_discovery"] == 5
    assert result["observation_gallery_scans"] == 4
    assert result["acquisition_count"] == 1
    assert result["artwork_count"] == 1
    assert result["output_manifest_sha256"] == (
        "be18c7701def706de9711b83a669944c073c82757e198ef5ba626ef9abb193ae"
    )


def test_pipeline_surfaces_resource_monitor_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _benchmark_module()
    run_pipeline = cast(_RunPipeline, module.__dict__["_run_pipeline"])

    def failed_monitor(
        *,
        stop: Event,
        safety_stop: Event,
        interval_seconds: float,
        benchmark_root: Path,
        database_path: Path,
        failures: list[Exception],
    ) -> None:
        del stop, interval_seconds, benchmark_root, database_path
        failures.append(OSError("synthetic monitor thread failure"))
        safety_stop.set()

    monkeypatch.setitem(module.__dict__, "_monitor_resources", failed_monitor)

    with pytest.raises(
        RuntimeError,
        match="synthetic benchmark resource monitor failed",
    ) as caught:
        run_pipeline(1, progress_seconds=600.0)

    assert isinstance(caught.value.__cause__, OSError)
    assert str(caught.value.__cause__) == "synthetic monitor thread failure"
