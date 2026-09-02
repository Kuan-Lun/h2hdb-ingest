from __future__ import annotations

import os
import platform
import subprocess
import sys

import pytest

import h2hdb_ingest.page_workers as page_workers


@pytest.mark.parametrize(
    ("reported", "expected"),
    [(1, 1), (10, 10), (16, 16), (17, 16)],
)
def test_darwin_prefers_highest_performance_physical_cores(
    reported: int,
    expected: int,
) -> None:
    requested: list[str] = []

    def read_sysctl(name: str) -> int | None:
        requested.append(name)
        return reported

    assert (
        page_workers._darwin_default_page_render_workers("arm64", read_sysctl)
        == expected
    )
    assert requested == ["hw.perflevel0.physicalcpu"]


def test_apple_silicon_missing_performance_authority_falls_back_to_one() -> None:
    requested: list[str] = []

    def read_sysctl(name: str) -> int | None:
        requested.append(name)
        return 14 if name == "hw.physicalcpu" else None

    assert page_workers._darwin_default_page_render_workers("arm64", read_sysctl) == 1
    assert requested == ["hw.perflevel0.physicalcpu"]


@pytest.mark.parametrize("machine", ("x86_64", "AMD64", "i386", "i686"))
def test_intel_macos_falls_back_to_total_physical_not_logical_cores(
    machine: str,
) -> None:
    requested: list[str] = []

    def read_sysctl(name: str) -> int | None:
        requested.append(name)
        if name == "sysctl.proc_translated":
            return 0
        return 6 if name == "hw.physicalcpu" else None

    assert page_workers._darwin_default_page_render_workers(machine, read_sysctl) == 6
    assert requested == [
        "hw.perflevel0.physicalcpu",
        "sysctl.proc_translated",
        "hw.physicalcpu",
    ]


@pytest.mark.parametrize("translated", (1, None))
def test_rosetta_or_unknown_translation_state_never_uses_total_physical_cores(
    translated: int | None,
) -> None:
    requested: list[str] = []

    def read_sysctl(name: str) -> int | None:
        requested.append(name)
        if name == "sysctl.proc_translated":
            return translated
        return 14 if name == "hw.physicalcpu" else None

    assert page_workers._darwin_default_page_render_workers("x86_64", read_sysctl) == 1
    assert requested == [
        "hw.perflevel0.physicalcpu",
        "sysctl.proc_translated",
    ]


def test_unknown_darwin_architecture_never_reinterprets_total_cpu_count() -> None:
    requested: list[str] = []

    def read_sysctl(name: str) -> int | None:
        requested.append(name)
        return 64 if name == "hw.physicalcpu" else None

    assert (
        page_workers._darwin_default_page_render_workers("future-arm", read_sysctl) == 1
    )
    assert requested == ["hw.perflevel0.physicalcpu"]


def test_darwin_detection_never_calls_logical_cpu_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(platform, "machine", lambda: "arm64")
    monkeypatch.setattr(page_workers, "_read_darwin_sysctl", lambda _name: None)

    def reject_logical_fallback() -> int:
        raise AssertionError("Darwin must not treat logical CPUs as performance cores")

    monkeypatch.setattr(os, "process_cpu_count", reject_logical_fallback)
    monkeypatch.setattr(os, "cpu_count", reject_logical_fallback)

    assert page_workers._detect_default_page_render_workers() == 1


@pytest.mark.parametrize(
    ("process_count", "system_count", "expected"),
    [(8, 64, 8), (32, 64, 16), (None, 6, 6), (None, None, 1)],
)
def test_non_darwin_default_uses_bounded_process_cpu_availability(
    monkeypatch: pytest.MonkeyPatch,
    process_count: int | None,
    system_count: int | None,
    expected: int,
) -> None:
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(os, "process_cpu_count", lambda: process_count)
    monkeypatch.setattr(os, "cpu_count", lambda: system_count)

    def reject_sysctl(_name: str) -> int | None:
        raise AssertionError("non-Darwin detection must not invoke sysctl")

    monkeypatch.setattr(page_workers, "_read_darwin_sysctl", reject_sysctl)

    assert page_workers._detect_default_page_render_workers() == expected


def test_automatic_detection_is_cached_once_per_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def detect() -> int:
        nonlocal calls
        calls += 1
        return 10

    page_workers.default_page_render_workers.cache_clear()
    monkeypatch.setattr(page_workers, "_detect_default_page_render_workers", detect)
    try:
        assert page_workers.default_page_render_workers() == 10
        assert page_workers.default_page_render_workers() == 10
        assert calls == 1
    finally:
        page_workers.default_page_render_workers.cache_clear()


@pytest.mark.parametrize("configured", range(1, 17))
def test_explicit_worker_override_is_preserved_without_detection(
    monkeypatch: pytest.MonkeyPatch,
    configured: int,
) -> None:
    def reject_detection() -> int:
        raise AssertionError("an explicit override must not inspect the platform")

    monkeypatch.setattr(page_workers, "default_page_render_workers", reject_detection)

    assert page_workers.resolve_page_render_workers(configured) == configured


@pytest.mark.parametrize("configured", (True, 1.0, "2"))
def test_worker_override_rejects_coerced_values(configured: object) -> None:
    with pytest.raises(TypeError, match="page_render_workers"):
        page_workers.resolve_page_render_workers(configured)  # type: ignore[arg-type]


@pytest.mark.parametrize("configured", (0, 17))
def test_worker_override_rejects_values_outside_hard_cap(configured: int) -> None:
    with pytest.raises(ValueError, match="from 1 through 16"):
        page_workers.resolve_page_render_workers(configured)


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


@pytest.mark.parametrize("reported", (0, 1))
def test_darwin_sysctl_reader_accepts_translation_state(
    monkeypatch: pytest.MonkeyPatch,
    reported: int,
) -> None:
    def completed(
        _command: tuple[str, str, str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess((), 0, stdout=f"{reported}\n", stderr="")

    monkeypatch.setattr(subprocess, "run", completed)

    assert page_workers._read_darwin_sysctl("sysctl.proc_translated") == reported


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
