from __future__ import annotations

from hashlib import sha256
from io import BytesIO
from pathlib import Path
from threading import Event, Thread
from typing import cast

import pytest
from h2hdb import (
    ArtifactSourceMember,
    ArtifactSourceRole,
    LibraryActivationStatus,
    VNextCatalogFacade,
    VNextDatabaseAdminFacade,
    VNextIngestFacade,
)

import h2hdb_ingest.page_workers as page_workers
import h2hdb_ingest.runtime as runtime_module
from h2hdb_ingest import (
    ArtifactImageResampler,
    ArtifactRenderPolicyConfig,
    ArtifactRenderPreset,
    IngestConfig,
    IngestPathsConfig,
)
from h2hdb_ingest.artifact import ARTIFACT_ADAPTER_ID
from h2hdb_ingest.library import ManagedFilesystemLibraryAdapter
from h2hdb_ingest.page_workers import _CpuTopology, _DarwinTranslation
from h2hdb_ingest.runtime import IngestRuntime, build_runtime
from h2hdb_ingest.service import VNextIngestService


def _source_root(tmp_path: Path) -> Path:
    source = tmp_path / "download"
    source.mkdir()
    (source / "mounted-volume-marker").touch()
    return source


def test_runtime_composes_only_public_vnext_core_facades(tmp_path: Path) -> None:
    config = IngestConfig(paths=IngestPathsConfig(download_path=_source_root(tmp_path)))

    runtime = build_runtime(config)

    assert isinstance(runtime.facade, VNextIngestFacade)
    assert isinstance(runtime.database_admin, VNextDatabaseAdminFacade)
    assert isinstance(runtime.catalog, VNextCatalogFacade)
    assert isinstance(runtime.resident._service, VNextIngestService)
    assert runtime.resident._facade is runtime.facade
    assert runtime.resident._database_admin is runtime.database_admin


def test_runtime_context_close_is_idempotent_and_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = IngestConfig(paths=IngestPathsConfig(download_path=_source_root(tmp_path)))
    close_calls = 0
    original_close = VNextIngestFacade.close

    def observed_close(facade: VNextIngestFacade) -> None:
        nonlocal close_calls
        close_calls += 1
        original_close(facade)

    monkeypatch.setattr(VNextIngestFacade, "close", observed_close)

    runtime = build_runtime(config)
    with runtime as entered:
        assert entered is runtime
        with pytest.raises(ValueError, match="context is already entered"):
            runtime.__enter__()

    runtime.close()
    runtime.close()

    assert close_calls == 1
    with pytest.raises(ValueError, match="ingest runtime is closed"):
        runtime.__enter__()
    with pytest.raises(ValueError, match="ingest facade is closed"):
        runtime.facade.try_claim_ingest(False, 1)


def test_runtime_build_failure_closes_the_already_owned_facade(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = IngestConfig(paths=IngestPathsConfig(download_path=_source_root(tmp_path)))

    class TrackedFacade:
        close_calls = 0

        def close(self) -> None:
            self.close_calls += 1

    tracked = TrackedFacade()

    def fail_admin(_config: object) -> VNextDatabaseAdminFacade:
        raise RuntimeError("composition failed")

    monkeypatch.setattr(
        runtime_module,
        "VNextIngestFacade",
        lambda _config: cast(VNextIngestFacade, tracked),
    )
    monkeypatch.setattr(runtime_module, "VNextDatabaseAdminFacade", fail_admin)

    with pytest.raises(RuntimeError, match="composition failed"):
        build_runtime(config)

    assert tracked.close_calls == 1


def test_concurrent_runtime_close_waits_for_one_exact_facade_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = IngestConfig(paths=IngestPathsConfig(download_path=_source_root(tmp_path)))
    runtime = build_runtime(config)
    close_entered = Event()
    release_close = Event()
    second_started = Event()
    failures: list[BaseException] = []
    close_calls = 0
    original_close = VNextIngestFacade.close

    def blocking_close(facade: VNextIngestFacade) -> None:
        nonlocal close_calls
        close_calls += 1
        close_entered.set()
        if not release_close.wait(5):
            raise RuntimeError("timed out waiting to finish facade close")
        original_close(facade)

    def close_runtime(*, second: bool = False) -> None:
        try:
            if second:
                second_started.set()
            runtime.close()
        except BaseException as error:
            failures.append(error)

    monkeypatch.setattr(VNextIngestFacade, "close", blocking_close)
    first_thread = Thread(target=close_runtime)
    second_thread = Thread(target=close_runtime, kwargs={"second": True})
    first_thread.start()
    assert close_entered.wait(5)
    second_thread.start()
    assert second_started.wait(5)
    assert second_thread.is_alive()

    release_close.set()
    first_thread.join(timeout=5)
    second_thread.join(timeout=5)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert failures == []
    assert close_calls == 1


def test_runtime_is_a_frozen_lifecycle_value(tmp_path: Path) -> None:
    runtime = build_runtime(
        IngestConfig(paths=IngestPathsConfig(download_path=_source_root(tmp_path)))
    )

    facade_attribute = "facade"
    with pytest.raises(AttributeError):
        setattr(runtime, facade_attribute, runtime.facade)

    runtime.close()
    assert isinstance(runtime, IngestRuntime)


def test_artifact_disabled_runtime_uses_a_terminal_noop_library(
    tmp_path: Path,
) -> None:
    config = IngestConfig(paths=IngestPathsConfig(download_path=_source_root(tmp_path)))

    service = build_runtime(config).resident._service

    assert isinstance(service, VNextIngestService)
    assert service._artifact_adapters == {}
    assert service._finalization_adapters == {}
    checkpoint = service._library_activation.begin(3, b"r" * 16)
    assert checkpoint.revision == 3
    assert checkpoint.receipt_id == b"r" * 16
    assert checkpoint.status is LibraryActivationStatus.COMPLETE


def test_cbz_runtime_shares_one_adapter_for_protection_and_release(
    tmp_path: Path,
) -> None:
    source = _source_root(tmp_path)
    library_root = tmp_path / "library"
    config = IngestConfig(
        paths=IngestPathsConfig(
            download_path=source,
            library_path=library_root,
        )
    )

    service = build_runtime(config).resident._service

    assert isinstance(service, VNextIngestService)
    storage = service._artifact_adapters[ARTIFACT_ADAPTER_ID]
    assert id(service._finalization_adapters[ARTIFACT_ADAPTER_ID]) == id(storage)
    assert isinstance(service._library_activation, ManagedFilesystemLibraryAdapter)
    assert service._publication_guard == service._library_activation.publication_guard


def test_runtime_passes_effective_render_policy_and_one_metric_sink_to_adapter(
    tmp_path: Path,
) -> None:
    source = _source_root(tmp_path)
    config = IngestConfig(
        paths=IngestPathsConfig(
            download_path=source,
            library_path=tmp_path / "library",
            max_image_short_side=321,
            page_render_workers=4,
            render_policy=ArtifactRenderPolicyConfig(
                preset=ArtifactRenderPreset.BENCHMARK_LOW_COST,
                thumbnail_jpeg_quality=73,
            ),
        )
    )

    service = build_runtime(config).resident._service
    assert isinstance(service, VNextIngestService)
    adapter = service._artifact_adapters[ARTIFACT_ADAPTER_ID]

    assert isinstance(adapter, ManagedFilesystemLibraryAdapter)
    assert adapter._render_policy.max_image_short_side == 321
    assert adapter._render_policy.page_jpeg_quality == 70
    assert adapter._render_policy.thumbnail_jpeg_quality == 73
    assert not adapter._render_policy.optimize
    assert adapter._render_policy.resampler is ArtifactImageResampler.BILINEAR
    assert adapter._page_render_workers == 4
    assert adapter._metrics_sink is service._metrics_sink


def _fixed_topology(monkeypatch: pytest.MonkeyPatch, topology: _CpuTopology) -> None:
    page_workers._detect_cpu_topology.cache_clear()
    monkeypatch.setattr(page_workers, "_probe_cpu_topology", lambda: topology)


_APPLE_SILICON = _CpuTopology(
    platform="darwin",
    machine="arm64",
    process_cpu_count=14,
    cpu_count=14,
    darwin_performance_cores=10,
    darwin_physical_cores=14,
    darwin_translation=_DarwinTranslation.NATIVE,
)
_CONTAINER = _CpuTopology(
    platform="linux",
    machine="aarch64",
    process_cpu_count=4,
    cpu_count=14,
    darwin_performance_cores=None,
    darwin_physical_cores=None,
    darwin_translation=_DarwinTranslation.NOT_PROBED,
)


def _worker_lines(events: list[str]) -> list[str]:
    return [event for event in events if event.startswith("page_render_workers ")]


def test_runtime_decides_omitted_page_workers_once_and_logs_the_decision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source_root(tmp_path)
    _fixed_topology(monkeypatch, _APPLE_SILICON)
    events: list[str] = []
    config = IngestConfig(
        paths=IngestPathsConfig(
            download_path=source,
            library_path=tmp_path / "library",
        )
    )
    try:
        service = build_runtime(config, event_logger=events.append).resident._service
    finally:
        page_workers._detect_cpu_topology.cache_clear()
    assert isinstance(service, VNextIngestService)
    adapter = service._artifact_adapters[ARTIFACT_ADAPTER_ID]

    assert isinstance(adapter, ManagedFilesystemLibraryAdapter)
    assert adapter._page_render_workers == 10
    assert events == [
        "page_render_workers mode=auto configured=none selected=10 detected=10 "
        "hard_cap=16 platform=darwin machine=arm64 process_cpu_count=14 "
        "cpu_count=14 darwin_performance_cores=10 darwin_physical_cores=14 "
        "darwin_translation=native reason=darwin-performance-cores"
    ]
    assert str(tmp_path) not in events[0]


def test_runtime_logs_a_manual_override_as_manual_next_to_the_host_facts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source_root(tmp_path)
    _fixed_topology(monkeypatch, _CONTAINER)
    events: list[str] = []
    config = IngestConfig(
        paths=IngestPathsConfig(
            download_path=source,
            library_path=tmp_path / "library",
            page_render_workers=10,
        )
    )
    try:
        service = build_runtime(config, event_logger=events.append).resident._service
    finally:
        page_workers._detect_cpu_topology.cache_clear()
    assert isinstance(service, VNextIngestService)
    adapter = service._artifact_adapters[ARTIFACT_ADAPTER_ID]

    assert isinstance(adapter, ManagedFilesystemLibraryAdapter)
    assert adapter._page_render_workers == 10
    assert events == [
        "page_render_workers mode=manual configured=10 selected=10 detected=none "
        "hard_cap=16 platform=linux machine=aarch64 process_cpu_count=4 "
        "cpu_count=14 darwin_performance_cores=none darwin_physical_cores=none "
        "darwin_translation=not-probed reason=manual-override"
    ]


def test_runtime_logs_the_worker_decision_once_not_per_archive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source_root(tmp_path)
    _fixed_topology(monkeypatch, _APPLE_SILICON)
    events: list[str] = []
    config = IngestConfig(
        paths=IngestPathsConfig(
            download_path=source,
            library_path=tmp_path / "library",
        )
    )
    try:
        runtime = build_runtime(config, event_logger=events.append)
    finally:
        page_workers._detect_cpu_topology.cache_clear()
    service = runtime.resident._service
    assert isinstance(service, VNextIngestService)
    adapter = service._artifact_adapters[ARTIFACT_ADAPTER_ID]
    assert isinstance(adapter, ManagedFilesystemLibraryAdapter)

    for gid in (1, 2, 3):
        metadata = b"Title: once\n"
        adapter.render_archive(
            (
                ArtifactSourceMember(
                    position=0,
                    role=ArtifactSourceRole.METADATA,
                    source_name=b"galleryinfo.txt",
                    expected_sha256=sha256(metadata).digest(),
                    expected_size_bytes=len(metadata),
                    source=BytesIO(metadata),
                ),
            ),
            BytesIO(),
            gid=gid,
        )

    assert len(_worker_lines(events)) == 1
    assert sum(event.startswith("ingest_metric ") for event in events) == 3


def test_artifact_disabled_runtime_does_not_log_or_probe_worker_selection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def reject_probe() -> _CpuTopology:
        raise AssertionError("a runtime without a library must not probe workers")

    page_workers._detect_cpu_topology.cache_clear()
    monkeypatch.setattr(page_workers, "_probe_cpu_topology", reject_probe)
    events: list[str] = []
    config = IngestConfig(paths=IngestPathsConfig(download_path=_source_root(tmp_path)))
    try:
        build_runtime(config, event_logger=events.append)
    finally:
        page_workers._detect_cpu_topology.cache_clear()

    assert _worker_lines(events) == []
