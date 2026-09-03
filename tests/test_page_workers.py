from __future__ import annotations

import dataclasses
import os
import platform
import subprocess
import sys
from ctypes import c_int, sizeof
from errno import EACCES, ENOENT
from itertools import product
from typing import Any

import pytest

import h2hdb_ingest.page_workers as page_workers
from h2hdb_ingest.page_workers import (
    MAX_PAGE_RENDER_WORKERS,
    _CpuTopology,
    _DarwinTranslation,
    _decide_page_render_workers,
    _detect_cpu_topology,
    _PageRenderWorkerDecision,
    _PageRenderWorkerMode,
    _PageRenderWorkerReason,
    default_page_render_workers,
    resolve_page_render_workers,
)


def _darwin(
    *,
    machine: str = "arm64",
    performance: int | None = None,
    physical: int | None = None,
    translation: _DarwinTranslation = _DarwinTranslation.NATIVE,
    process_count: int | None = 14,
    cpu_count: int | None = 14,
) -> _CpuTopology:
    return _CpuTopology(
        platform="darwin",
        machine=machine,
        process_cpu_count=process_count,
        cpu_count=cpu_count,
        darwin_performance_cores=performance,
        darwin_physical_cores=physical,
        darwin_translation=translation,
    )


def _linux(
    *,
    machine: str = "aarch64",
    process_count: int | None,
    cpu_count: int | None,
) -> _CpuTopology:
    return _CpuTopology(
        platform="linux",
        machine=machine,
        process_cpu_count=process_count,
        cpu_count=cpu_count,
        darwin_performance_cores=None,
        darwin_physical_cores=None,
        darwin_translation=_DarwinTranslation.NOT_PROBED,
    )


def _legacy_automatic_workers(topology: _CpuTopology) -> int:
    """Independent port of the pre-decision integer policy used as an oracle.

    This mirrors the previous ``_darwin_default_page_render_workers`` and
    non-Darwin ``_detect_default_page_render_workers`` code path by path so the
    decision value can be proved to select the same worker count.
    """

    def plausible(value: int | None) -> int | None:
        if type(value) is not int or not 1 <= value <= 1024:
            return None
        return value

    if topology.platform == "darwin":
        performance = plausible(topology.darwin_performance_cores)
        if performance is not None:
            return min(performance, 16)
        if topology.machine.casefold() in {"x86_64", "amd64", "i386", "i686"}:
            if topology.darwin_translation is not _DarwinTranslation.NATIVE:
                return 1
            physical = plausible(topology.darwin_physical_cores)
            if physical is not None:
                return min(physical, 16)
        return 1
    available = topology.process_cpu_count
    if available is None:
        available = topology.cpu_count
    detected = plausible(available)
    return 1 if detected is None else min(detected, 16)


# --- normal automatic selection -------------------------------------------


@pytest.mark.parametrize(
    ("reported", "expected"),
    [(1, 1), (10, 10), (16, 16), (17, 16), (1024, 16)],
)
def test_darwin_prefers_highest_performance_physical_cores(
    reported: int,
    expected: int,
) -> None:
    decision = _decide_page_render_workers(
        None,
        _darwin(performance=reported, physical=reported + 4),
    )

    assert decision.mode is _PageRenderWorkerMode.AUTO
    assert decision.configured is None
    assert decision.selected == expected
    assert decision.detected == reported
    assert decision.hard_cap == MAX_PAGE_RENDER_WORKERS
    assert decision.reason is _PageRenderWorkerReason.DARWIN_PERFORMANCE_CORES
    assert not decision.is_fallback


@pytest.mark.parametrize("machine", ("x86_64", "AMD64", "i386", "i686"))
def test_native_intel_macos_falls_back_to_total_physical_not_logical_cores(
    machine: str,
) -> None:
    decision = _decide_page_render_workers(
        None,
        _darwin(
            machine=machine,
            physical=6,
            process_count=12,
            cpu_count=12,
        ),
    )

    assert decision.selected == 6
    assert decision.detected == 6
    assert decision.reason is (
        _PageRenderWorkerReason.DARWIN_INTEL_NATIVE_PHYSICAL_CORES
    )


@pytest.mark.parametrize(
    ("process_count", "cpu_count", "expected", "reason"),
    [
        (8, 64, 8, _PageRenderWorkerReason.PROCESS_CPU_COUNT),
        (32, 64, 16, _PageRenderWorkerReason.PROCESS_CPU_COUNT),
        (None, 6, 6, _PageRenderWorkerReason.CPU_COUNT),
        (None, 1024, 16, _PageRenderWorkerReason.CPU_COUNT),
    ],
)
def test_non_darwin_uses_process_availability_then_host_count(
    process_count: int | None,
    cpu_count: int | None,
    expected: int,
    reason: _PageRenderWorkerReason,
) -> None:
    decision = _decide_page_render_workers(
        None,
        _linux(process_count=process_count, cpu_count=cpu_count),
    )

    assert decision.selected == expected
    assert decision.reason is reason
    assert decision.detected == (
        process_count if process_count is not None else cpu_count
    )


# --- boundary and fallback -----------------------------------------------


@pytest.mark.parametrize("performance", (None, 0, 1025))
def test_apple_silicon_missing_or_implausible_performance_authority_is_one(
    performance: int | None,
) -> None:
    decision = _decide_page_render_workers(
        None,
        _darwin(performance=performance, physical=14, process_count=14),
    )

    assert decision.selected == 1
    assert decision.detected is None
    assert decision.is_fallback
    assert decision.reason is (
        _PageRenderWorkerReason.DARWIN_PERFORMANCE_CORES_UNAVAILABLE_FALLBACK
    )


def test_unknown_darwin_architecture_never_reinterprets_total_cpu_count() -> None:
    decision = _decide_page_render_workers(
        None,
        _darwin(machine="future-arm", physical=64, process_count=64, cpu_count=64),
    )

    assert decision.selected == 1
    assert decision.reason is (
        _PageRenderWorkerReason.DARWIN_PERFORMANCE_CORES_UNAVAILABLE_FALLBACK
    )


def test_darwin_decision_never_uses_logical_cpu_counts() -> None:
    without_authority = _darwin(process_count=64, cpu_count=64)
    assert _decide_page_render_workers(None, without_authority).selected == 1

    for process_count, cpu_count in product((None, 1, 64), repeat=2):
        varied = dataclasses.replace(
            without_authority,
            process_cpu_count=process_count,
            cpu_count=cpu_count,
        )
        assert _decide_page_render_workers(None, varied).selected == 1
        with_authority = dataclasses.replace(varied, darwin_performance_cores=10)
        assert _decide_page_render_workers(None, with_authority).selected == 10


@pytest.mark.parametrize("physical", (None, 0, 1025))
def test_native_intel_missing_physical_authority_falls_back_to_one(
    physical: int | None,
) -> None:
    decision = _decide_page_render_workers(
        None,
        _darwin(machine="x86_64", physical=physical, process_count=8),
    )

    assert decision.selected == 1
    assert decision.reason is (
        _PageRenderWorkerReason.DARWIN_INTEL_PHYSICAL_CORES_UNAVAILABLE_FALLBACK
    )


@pytest.mark.parametrize(
    ("process_count", "cpu_count"),
    [(None, None), (0, 8), (1025, 8), (None, 0), (None, 1025)],
)
def test_non_darwin_unavailable_or_implausible_counts_fall_back_to_one(
    process_count: int | None,
    cpu_count: int | None,
) -> None:
    decision = _decide_page_render_workers(
        None,
        _linux(process_count=process_count, cpu_count=cpu_count),
    )

    assert decision.selected == 1
    assert decision.detected is None
    assert decision.reason is _PageRenderWorkerReason.CPU_COUNT_UNAVAILABLE_FALLBACK


# --- Rosetta --------------------------------------------------------------


def test_rosetta_translated_intel_process_never_uses_total_physical_cores() -> None:
    decision = _decide_page_render_workers(
        None,
        _darwin(
            machine="x86_64",
            physical=14,
            translation=_DarwinTranslation.TRANSLATED,
        ),
    )

    assert decision.selected == 1
    assert decision.reason is _PageRenderWorkerReason.DARWIN_INTEL_TRANSLATED_FALLBACK
    assert decision.topology.darwin_translation is _DarwinTranslation.TRANSLATED


def test_unknown_translation_state_on_intel_machine_fails_conservatively() -> None:
    decision = _decide_page_render_workers(
        None,
        _darwin(
            machine="x86_64",
            physical=14,
            translation=_DarwinTranslation.UNKNOWN,
        ),
    )

    assert decision.selected == 1
    assert decision.reason is (
        _PageRenderWorkerReason.DARWIN_INTEL_TRANSLATION_UNKNOWN_FALLBACK
    )


def test_translation_state_is_irrelevant_after_performance_core_authority() -> None:
    for translation in _DarwinTranslation:
        if translation is _DarwinTranslation.NOT_PROBED:
            continue
        decision = _decide_page_render_workers(
            None,
            _darwin(machine="x86_64", performance=8, translation=translation),
        )
        assert decision.selected == 8
        assert decision.reason is _PageRenderWorkerReason.DARWIN_PERFORMANCE_CORES


# --- container / non-Darwin honesty --------------------------------------


def test_linux_topology_cannot_carry_macos_host_core_facts() -> None:
    with pytest.raises(ValueError, match="only a Darwin process"):
        _CpuTopology(
            platform="linux",
            machine="aarch64",
            process_cpu_count=4,
            cpu_count=14,
            darwin_performance_cores=10,
            darwin_physical_cores=None,
            darwin_translation=_DarwinTranslation.NOT_PROBED,
        )
    with pytest.raises(ValueError, match="only a Darwin process"):
        _CpuTopology(
            platform="linux",
            machine="aarch64",
            process_cpu_count=4,
            cpu_count=14,
            darwin_performance_cores=None,
            darwin_physical_cores=None,
            darwin_translation=_DarwinTranslation.NATIVE,
        )
    with pytest.raises(ValueError, match="must record its translation probe"):
        _CpuTopology(
            platform="darwin",
            machine="arm64",
            process_cpu_count=14,
            cpu_count=14,
            darwin_performance_cores=10,
            darwin_physical_cores=14,
            darwin_translation=_DarwinTranslation.NOT_PROBED,
        )


def test_container_visible_vcpus_are_the_only_linux_authority() -> None:
    decision = _decide_page_render_workers(
        None,
        _linux(process_count=4, cpu_count=14),
    )

    assert decision.selected == 4
    assert decision.reason is _PageRenderWorkerReason.PROCESS_CPU_COUNT
    assert decision.topology.darwin_performance_cores is None
    assert decision.topology.darwin_physical_cores is None
    assert decision.topology.darwin_translation is _DarwinTranslation.NOT_PROBED
    assert decision.log_fields()[9:12] == (
        ("darwin_performance_cores", "none"),
        ("darwin_physical_cores", "none"),
        ("darwin_translation", "not-probed"),
    )


def test_probe_never_invokes_sysctl_outside_darwin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(os, "process_cpu_count", lambda: 4)
    monkeypatch.setattr(os, "cpu_count", lambda: 14)

    def reject_sysctl(_name: str) -> int | None:
        raise AssertionError("non-Darwin detection must not invoke sysctl")

    def reject_translation() -> _DarwinTranslation:
        raise AssertionError("non-Darwin detection must not probe Rosetta")

    monkeypatch.setattr(page_workers, "_read_darwin_sysctl", reject_sysctl)
    monkeypatch.setattr(
        page_workers,
        "_read_darwin_translation_status",
        reject_translation,
    )

    assert page_workers._probe_cpu_topology() == _linux(
        machine="x86_64",
        process_count=4,
        cpu_count=14,
    )


def test_darwin_probe_records_every_fact_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requested: list[str] = []
    translation_probes = 0

    def read_sysctl(name: str) -> int | None:
        requested.append(name)
        return {"hw.perflevel0.physicalcpu": 10, "hw.physicalcpu": 14}[name]

    def read_translation() -> _DarwinTranslation:
        nonlocal translation_probes
        translation_probes += 1
        return _DarwinTranslation.NATIVE

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(platform, "machine", lambda: "arm64")
    monkeypatch.setattr(os, "process_cpu_count", lambda: 14)
    monkeypatch.setattr(os, "cpu_count", lambda: 14)
    monkeypatch.setattr(page_workers, "_read_darwin_sysctl", read_sysctl)
    monkeypatch.setattr(
        page_workers,
        "_read_darwin_translation_status",
        read_translation,
    )

    assert page_workers._probe_cpu_topology() == _darwin(
        performance=10,
        physical=14,
    )
    assert requested == ["hw.perflevel0.physicalcpu", "hw.physicalcpu"]
    assert translation_probes == 1


# --- manual override ------------------------------------------------------


@pytest.mark.parametrize("configured", range(1, 17))
def test_manual_override_is_exact_and_clearly_marked(configured: int) -> None:
    topology = _linux(process_count=2, cpu_count=2)

    decision = _decide_page_render_workers(configured, topology)

    assert decision.mode is _PageRenderWorkerMode.MANUAL
    assert decision.configured == configured
    assert decision.selected == configured
    assert decision.detected is None
    assert decision.reason is _PageRenderWorkerReason.MANUAL_OVERRIDE
    assert decision.topology == topology
    assert decision.log_fields()[:3] == (
        ("mode", "manual"),
        ("configured", str(configured)),
        ("selected", str(configured)),
    )
    assert resolve_page_render_workers(configured) == configured


@pytest.mark.parametrize("configured", range(1, 17))
def test_explicit_worker_override_is_preserved_without_detection(
    monkeypatch: pytest.MonkeyPatch,
    configured: int,
) -> None:
    def reject_detection() -> _CpuTopology:
        raise AssertionError("an explicit override must not inspect the platform")

    monkeypatch.setattr(page_workers, "_detect_cpu_topology", reject_detection)

    assert resolve_page_render_workers(configured) == configured


@pytest.mark.parametrize("configured", (True, 1.0, "2"))
def test_worker_override_rejects_coerced_values(configured: object) -> None:
    with pytest.raises(TypeError, match="page_render_workers"):
        resolve_page_render_workers(configured)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="page_render_workers"):
        _decide_page_render_workers(
            configured,  # type: ignore[arg-type]
            _linux(process_count=2, cpu_count=2),
        )


@pytest.mark.parametrize("configured", (0, 17))
def test_worker_override_rejects_values_outside_hard_cap(configured: int) -> None:
    with pytest.raises(ValueError, match="from 1 through 16"):
        resolve_page_render_workers(configured)
    with pytest.raises(ValueError, match="from 1 through 16"):
        _decide_page_render_workers(configured, _linux(process_count=2, cpu_count=2))


def test_decision_rejects_a_foreign_topology() -> None:
    with pytest.raises(TypeError, match="topology"):
        _decide_page_render_workers(None, object())  # type: ignore[arg-type]


# --- decision value self-consistency -------------------------------------


@pytest.mark.parametrize(
    "fields",
    [
        {"selected": 0},
        {"selected": 17},
        {"hard_cap": 8},
        {"configured": 4},
        {"reason": _PageRenderWorkerReason.MANUAL_OVERRIDE},
        {"detected": None},
        {"selected": 4, "detected": 10},
    ],
)
def test_automatic_decision_rejects_inconsistent_fields(
    fields: dict[str, Any],
) -> None:
    consistent = _decide_page_render_workers(None, _darwin(performance=10))
    with pytest.raises(ValueError):
        dataclasses.replace(consistent, **fields)


@pytest.mark.parametrize(
    "fields",
    [
        {"selected": 5},
        {"detected": 4},
        {"reason": _PageRenderWorkerReason.PROCESS_CPU_COUNT},
        {"selected": 2, "detected": 10},
    ],
)
def test_fallback_and_manual_decisions_reject_inconsistent_fields(
    fields: dict[str, Any],
) -> None:
    fallback = _decide_page_render_workers(None, _darwin())
    manual = _decide_page_render_workers(4, _darwin())
    with pytest.raises(ValueError):
        dataclasses.replace(fallback, **fields)
    with pytest.raises(ValueError):
        dataclasses.replace(manual, **fields)


def test_decision_rejects_foreign_mode_reason_and_topology_types() -> None:
    decision = _decide_page_render_workers(None, _darwin(performance=10))
    with pytest.raises(TypeError, match="mode"):
        dataclasses.replace(decision, mode="auto")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="reason"):
        dataclasses.replace(decision, reason="manual")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="topology"):
        dataclasses.replace(decision, topology=object())  # type: ignore[arg-type]


# --- cache ----------------------------------------------------------------


def test_topology_probe_is_cached_once_per_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probes = 0

    def probe() -> _CpuTopology:
        nonlocal probes
        probes += 1
        return _darwin(performance=10, physical=14)

    _detect_cpu_topology.cache_clear()
    monkeypatch.setattr(page_workers, "_probe_cpu_topology", probe)
    try:
        assert _detect_cpu_topology() == _darwin(performance=10, physical=14)
        assert default_page_render_workers() == 10
        assert resolve_page_render_workers(None) == 10
        automatic = _decide_page_render_workers(None)
        manual = _decide_page_render_workers(3)
        assert automatic.selected == 10
        assert manual.selected == 3
        assert automatic.topology is manual.topology
        for _ in range(3):
            automatic.log_line()
        assert probes == 1
    finally:
        _detect_cpu_topology.cache_clear()


# --- structured log -------------------------------------------------------


def test_log_line_is_structured_and_carries_no_private_data() -> None:
    decision = _decide_page_render_workers(
        None,
        _darwin(performance=10, physical=14, process_count=14, cpu_count=14),
    )

    assert decision.log_line() == (
        "page_render_workers mode=auto configured=none selected=10 detected=10 "
        "hard_cap=16 platform=darwin machine=arm64 process_cpu_count=14 "
        "cpu_count=14 darwin_performance_cores=10 darwin_physical_cores=14 "
        "darwin_translation=native reason=darwin-performance-cores"
    )
    assert [key for key, _value in decision.log_fields()] == [
        "mode",
        "configured",
        "selected",
        "detected",
        "hard_cap",
        "platform",
        "machine",
        "process_cpu_count",
        "cpu_count",
        "darwin_performance_cores",
        "darwin_physical_cores",
        "darwin_translation",
        "reason",
    ]
    assert "/" not in decision.log_line()


def test_manual_and_fallback_log_lines_state_their_mode_and_reason() -> None:
    manual = _decide_page_render_workers(
        10,
        _linux(process_count=4, cpu_count=14),
    )
    fallback = _decide_page_render_workers(
        None,
        _darwin(
            machine="x86_64", physical=14, translation=_DarwinTranslation.TRANSLATED
        ),
    )

    assert manual.log_line() == (
        "page_render_workers mode=manual configured=10 selected=10 detected=none "
        "hard_cap=16 platform=linux machine=aarch64 process_cpu_count=4 "
        "cpu_count=14 darwin_performance_cores=none darwin_physical_cores=none "
        "darwin_translation=not-probed reason=manual-override"
    )
    assert fallback.log_line() == (
        "page_render_workers mode=auto configured=none selected=1 detected=none "
        "hard_cap=16 platform=darwin machine=x86_64 process_cpu_count=14 "
        "cpu_count=14 darwin_performance_cores=none darwin_physical_cores=14 "
        "darwin_translation=translated reason=darwin-intel-translated-fallback"
    )


def test_log_renders_an_empty_machine_as_unknown() -> None:
    decision = _decide_page_render_workers(
        None, _linux(machine="", process_count=2, cpu_count=2)
    )

    assert ("machine", "unknown") in decision.log_fields()


# --- differential: decision equals the previous integer policy ------------


def _topology_matrix() -> list[_CpuTopology]:
    counts: tuple[int | None, ...] = (None, 0, 1, 4, 16, 17, 1024, 1025)
    topologies: list[_CpuTopology] = []
    for (
        machine,
        performance,
        physical,
        translation,
        process_count,
        cpu_count,
    ) in product(
        ("arm64", "x86_64", "i386", "future-arm"),
        counts,
        counts,
        (
            _DarwinTranslation.NATIVE,
            _DarwinTranslation.TRANSLATED,
            _DarwinTranslation.UNKNOWN,
        ),
        (None, 2, 64),
        (None, 2, 64),
    ):
        topologies.append(
            _darwin(
                machine=machine,
                performance=performance,
                physical=physical,
                translation=translation,
                process_count=process_count,
                cpu_count=cpu_count,
            )
        )
    for machine, process_count, cpu_count in product(
        ("x86_64", "aarch64", ""),
        counts,
        counts,
    ):
        topologies.append(
            _linux(machine=machine, process_count=process_count, cpu_count=cpu_count)
        )
    return topologies


def test_decision_selects_exactly_the_previous_integer_policy() -> None:
    matrix = _topology_matrix()
    assert len(matrix) > 5000

    for topology in matrix:
        decision = _decide_page_render_workers(None, topology)
        assert decision.selected == _legacy_automatic_workers(topology), topology
        assert 1 <= decision.selected <= MAX_PAGE_RENDER_WORKERS
        assert decision.is_fallback == (decision.detected is None)
        if decision.detected is not None:
            assert decision.selected == min(decision.detected, MAX_PAGE_RENDER_WORKERS)
        for configured in range(1, 17):
            assert _decide_page_render_workers(configured, topology).selected == (
                configured
            )


def test_integer_api_projects_the_same_decision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for topology in (
        _darwin(performance=10, physical=14),
        _darwin(machine="x86_64", physical=6),
        _darwin(
            machine="x86_64", physical=6, translation=_DarwinTranslation.TRANSLATED
        ),
        _linux(process_count=3, cpu_count=8),
        _linux(process_count=None, cpu_count=None),
    ):
        _detect_cpu_topology.cache_clear()
        monkeypatch.setattr(page_workers, "_probe_cpu_topology", lambda t=topology: t)
        try:
            expected = _decide_page_render_workers(None, topology).selected
            assert default_page_render_workers() == expected
            assert resolve_page_render_workers(None) == expected
            assert _decide_page_render_workers(None).selected == expected
        finally:
            _detect_cpu_topology.cache_clear()


# --- Darwin readers -------------------------------------------------------


def test_darwin_sysctl_reader_accepts_only_bounded_decimal_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def completed(
        _command: tuple[str, str, str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess((), 0, stdout="10\n", stderr="")

    monkeypatch.setattr(subprocess, "run", completed)

    assert page_workers._read_darwin_sysctl("hw.perflevel0.physicalcpu") == 10


@pytest.mark.parametrize(
    ("invocation", "expected"),
    (
        ((0, 0, 0, sizeof(c_int)), _DarwinTranslation.NATIVE),
        ((0, 0, 1, sizeof(c_int)), _DarwinTranslation.TRANSLATED),
        ((-1, ENOENT, 0, sizeof(c_int)), _DarwinTranslation.NATIVE),
        ((-1, EACCES, 0, sizeof(c_int)), _DarwinTranslation.UNKNOWN),
        ((0, 0, 2, sizeof(c_int)), _DarwinTranslation.UNKNOWN),
        ((0, 0, 0, sizeof(c_int) + 1), _DarwinTranslation.UNKNOWN),
        (None, _DarwinTranslation.UNKNOWN),
    ),
)
def test_darwin_translation_reader_distinguishes_missing_oid_from_failure(
    monkeypatch: pytest.MonkeyPatch,
    invocation: tuple[int, int, int, int] | None,
    expected: _DarwinTranslation,
) -> None:
    monkeypatch.setattr(
        page_workers,
        "_invoke_darwin_translation_sysctl",
        lambda: invocation,
    )

    assert page_workers._read_darwin_translation_status() is expected


@pytest.mark.parametrize("stdout", ("", "0", "-1", "1.5", "ten", "1025"))
def test_darwin_sysctl_reader_rejects_malformed_or_implausible_output(
    monkeypatch: pytest.MonkeyPatch,
    stdout: str,
) -> None:
    def completed(
        _command: tuple[str, str, str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess((), 0, stdout=stdout, stderr="")

    monkeypatch.setattr(subprocess, "run", completed)

    assert page_workers._read_darwin_sysctl("hw.perflevel0.physicalcpu") is None


def test_darwin_sysctl_reader_falls_back_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def failure(
        *args: object,
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess((), 1, stdout="10", stderr="denied")

    monkeypatch.setattr(subprocess, "run", failure)
    assert page_workers._read_darwin_sysctl("hw.perflevel0.physicalcpu") is None

    def timeout(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(("sysctl",), 1)

    monkeypatch.setattr(subprocess, "run", timeout)
    assert page_workers._read_darwin_sysctl("hw.perflevel0.physicalcpu") is None


def test_darwin_sysctl_reader_rejects_unknown_authority() -> None:
    with pytest.raises(ValueError, match="unsupported Darwin"):
        page_workers._read_darwin_sysctl("hw.logicalcpu")


def test_live_host_probe_is_a_valid_decision_input() -> None:
    topology = _detect_cpu_topology()
    decision = _decide_page_render_workers(None, topology)

    assert isinstance(topology, _CpuTopology)
    assert isinstance(decision, _PageRenderWorkerDecision)
    assert decision.selected == default_page_render_workers()
    assert decision.selected == _legacy_automatic_workers(topology)
