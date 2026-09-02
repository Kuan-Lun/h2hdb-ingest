"""Bounded process-level policy for concurrent image page rendering."""

from __future__ import annotations

__all__ = [
    "MAX_PAGE_RENDER_WORKERS",
    "default_page_render_workers",
    "resolve_page_render_workers",
]

import os
import platform
import subprocess
import sys
from collections.abc import Callable
from functools import cache

MAX_PAGE_RENDER_WORKERS = 16

_DARWIN_SYSCTL = "/usr/sbin/sysctl"
_DARWIN_PERFORMANCE_CORES = "hw.perflevel0.physicalcpu"
_DARWIN_PHYSICAL_CORES = "hw.physicalcpu"
_DARWIN_PROCESS_TRANSLATED = "sysctl.proc_translated"
_DARWIN_CONSERVATIVE_FALLBACK = 1
_MAX_PLAUSIBLE_DETECTED_CPUS = 1024

_DarwinSysctlReader = Callable[[str], int | None]


def _validated_detected_cpu_count(value: object) -> int | None:
    if type(value) is not int or not 1 <= value <= _MAX_PLAUSIBLE_DETECTED_CPUS:
        return None
    return value


def _bounded_detected_workers(value: object) -> int:
    detected = _validated_detected_cpu_count(value)
    if detected is None:
        return 1
    return min(detected, MAX_PAGE_RENDER_WORKERS)


def _read_darwin_sysctl(name: str) -> int | None:
    """Read one fixed Darwin CPU-count sysctl without invoking a shell."""

    if name not in {
        _DARWIN_PERFORMANCE_CORES,
        _DARWIN_PHYSICAL_CORES,
        _DARWIN_PROCESS_TRANSLATED,
    }:
        raise ValueError("unsupported Darwin worker-count sysctl")
    try:
        completed = subprocess.run(
            (_DARWIN_SYSCTL, "-n", name),
            check=False,
            capture_output=True,
            text=True,
            timeout=1,
        )
    except OSError, subprocess.SubprocessError:
        return None
    if completed.returncode != 0:
        return None
    raw = completed.stdout.strip()
    if not raw.isascii() or not raw.isdecimal():
        return None
    value = int(raw)
    if name == _DARWIN_PROCESS_TRANSLATED:
        return value if value in {0, 1} else None
    return _validated_detected_cpu_count(value)


def _darwin_default_page_render_workers(
    machine: str,
    read_sysctl: _DarwinSysctlReader,
) -> int:
    """Prefer highest-performance physical cores and fail conservatively."""

    performance_cores = _validated_detected_cpu_count(
        read_sysctl(_DARWIN_PERFORMANCE_CORES)
    )
    if performance_cores is not None:
        return min(performance_cores, MAX_PAGE_RENDER_WORKERS)

    # Intel Macs do not expose heterogeneous performance levels. Their total
    # physical-core count is therefore a safe fallback, but only after the
    # process is positively identified as native: Rosetta reports x86_64 too.
    # On translated, Apple Silicon, or unknown Darwin processes, never
    # reinterpret a logical/total CPU count as missing performance-core
    # authority. An unreadable translation flag also fails conservatively.
    if machine.casefold() in {"x86_64", "amd64", "i386", "i686"}:
        translated = read_sysctl(_DARWIN_PROCESS_TRANSLATED)
        if type(translated) is not int or translated != 0:
            return _DARWIN_CONSERVATIVE_FALLBACK
        physical_cores = _validated_detected_cpu_count(
            read_sysctl(_DARWIN_PHYSICAL_CORES)
        )
        if physical_cores is not None:
            return min(physical_cores, MAX_PAGE_RENDER_WORKERS)
    return _DARWIN_CONSERVATIVE_FALLBACK


def _running_platform() -> str:
    return sys.platform


def _detect_default_page_render_workers() -> int:
    if _running_platform() == "darwin":
        return _darwin_default_page_render_workers(
            platform.machine(),
            _read_darwin_sysctl,
        )
    available = os.process_cpu_count()
    if available is None:
        available = os.cpu_count()
    return _bounded_detected_workers(available)


@cache
def default_page_render_workers() -> int:
    """Return one process-cached automatic worker count in the safe range."""

    return _detect_default_page_render_workers()


def resolve_page_render_workers(configured: int | None = None) -> int:
    """Resolve automatic workers or preserve one strict bounded override."""

    if configured is None:
        return default_page_render_workers()
    if type(configured) is not int:
        raise TypeError("page_render_workers must be int or None for automatic")
    if not 1 <= configured <= MAX_PAGE_RENDER_WORKERS:
        raise ValueError(
            f"page_render_workers must be from 1 through {MAX_PAGE_RENDER_WORKERS}"
        )
    return configured
