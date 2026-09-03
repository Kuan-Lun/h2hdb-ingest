"""Bounded process-level policy for concurrent image page rendering.

The policy is split into three layers so that every automatic or manual worker
decision is observable without re-probing the host or guessing reasons back
from an integer:

1. :func:`_detect_cpu_topology` probes the host at most once per process
   (single flight across threads, never inherited across ``fork``) and returns
   an immutable :class:`_CpuTopology` fact record.
2. :func:`_decide_page_render_workers` is a pure function from the optional
   configured override and one topology to an immutable
   :class:`_PageRenderWorkerDecision` that carries the mode, configured and
   selected values, the raw authority value before hard-capping, the hard cap,
   the topology facts, and the closed selection or fallback reason.
3. :func:`resolve_page_render_workers` and :func:`default_page_render_workers`
   remain the integer API used by renderers; they are projections of the same
   decision and never select a different worker count.
"""

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
import threading
from ctypes import CDLL, POINTER, byref, c_char_p, c_int, c_size_t, c_void_p, sizeof
from ctypes import get_errno as _get_errno
from ctypes import set_errno as _set_errno
from dataclasses import dataclass
from enum import StrEnum
from errno import ENOENT

MAX_PAGE_RENDER_WORKERS = 16

_DARWIN_PLATFORM = "darwin"
_DARWIN_SYSCTL = "/usr/sbin/sysctl"
_DARWIN_PERFORMANCE_CORES = "hw.perflevel0.physicalcpu"
_DARWIN_PHYSICAL_CORES = "hw.physicalcpu"
_DARWIN_PROCESS_TRANSLATED = "sysctl.proc_translated"
_DARWIN_INTEL_MACHINES = frozenset({"x86_64", "amd64", "i386", "i686"})
_CONSERVATIVE_FALLBACK = 1
_MAX_PLAUSIBLE_DETECTED_CPUS = 1024


class _DarwinTranslation(StrEnum):
    """Rosetta status of the running Darwin process."""

    NATIVE = "native"
    TRANSLATED = "translated"
    UNKNOWN = "unknown"
    NOT_PROBED = "not-probed"


class _PageRenderWorkerMode(StrEnum):
    AUTO = "auto"
    MANUAL = "manual"


class _PageRenderWorkerReason(StrEnum):
    """Closed selection and fallback reasons; fallbacks always select one."""

    MANUAL_OVERRIDE = "manual-override"
    DARWIN_PERFORMANCE_CORES = "darwin-performance-cores"
    DARWIN_INTEL_NATIVE_PHYSICAL_CORES = "darwin-intel-native-physical-cores"
    DARWIN_INTEL_TRANSLATED_FALLBACK = "darwin-intel-translated-fallback"
    DARWIN_INTEL_TRANSLATION_UNKNOWN_FALLBACK = (
        "darwin-intel-translation-unknown-fallback"
    )
    DARWIN_INTEL_PHYSICAL_CORES_UNAVAILABLE_FALLBACK = (
        "darwin-intel-physical-cores-unavailable-fallback"
    )
    DARWIN_PERFORMANCE_CORES_UNAVAILABLE_FALLBACK = (
        "darwin-performance-cores-unavailable-fallback"
    )
    PROCESS_CPU_COUNT = "process-cpu-count"
    CPU_COUNT = "cpu-count"
    CPU_COUNT_UNAVAILABLE_FALLBACK = "cpu-count-unavailable-fallback"


_FALLBACK_REASONS = frozenset(
    {
        _PageRenderWorkerReason.DARWIN_INTEL_TRANSLATED_FALLBACK,
        _PageRenderWorkerReason.DARWIN_INTEL_TRANSLATION_UNKNOWN_FALLBACK,
        _PageRenderWorkerReason.DARWIN_INTEL_PHYSICAL_CORES_UNAVAILABLE_FALLBACK,
        _PageRenderWorkerReason.DARWIN_PERFORMANCE_CORES_UNAVAILABLE_FALLBACK,
        _PageRenderWorkerReason.CPU_COUNT_UNAVAILABLE_FALLBACK,
    }
)


def _require_optional_count(value: object, *, field: str) -> int | None:
    if value is None:
        return None
    if type(value) is not int:
        raise TypeError(f"{field} must be int or None")
    if value < 0:
        raise ValueError(f"{field} must not be negative")
    return value


@dataclass(frozen=True, slots=True)
class _CpuTopology:
    """Immutable host facts from the at-most-one topology probe of a process.

    Darwin-only facts are ``None``/``NOT_PROBED`` on every other platform: a
    Linux container on a macOS host cannot see the host's performance and
    efficiency cores, and this record refuses to pretend otherwise.
    """

    platform: str
    machine: str
    process_cpu_count: int | None
    cpu_count: int | None
    darwin_performance_cores: int | None
    darwin_physical_cores: int | None
    darwin_translation: _DarwinTranslation

    def __post_init__(self) -> None:
        if type(self.platform) is not str or not self.platform:
            raise TypeError("platform must be a non-empty str")
        if type(self.machine) is not str:
            raise TypeError("machine must be str")
        _require_optional_count(self.process_cpu_count, field="process_cpu_count")
        _require_optional_count(self.cpu_count, field="cpu_count")
        _require_optional_count(
            self.darwin_performance_cores,
            field="darwin_performance_cores",
        )
        _require_optional_count(
            self.darwin_physical_cores,
            field="darwin_physical_cores",
        )
        if not isinstance(self.darwin_translation, _DarwinTranslation):
            raise TypeError("darwin_translation must be _DarwinTranslation")
        if self.is_darwin:
            if self.darwin_translation is _DarwinTranslation.NOT_PROBED:
                raise ValueError("a Darwin topology must record its translation probe")
        elif (
            self.darwin_performance_cores is not None
            or self.darwin_physical_cores is not None
            or self.darwin_translation is not _DarwinTranslation.NOT_PROBED
        ):
            raise ValueError("only a Darwin process can carry Darwin CPU facts")

    @property
    def is_darwin(self) -> bool:
        return self.platform == _DARWIN_PLATFORM

    @property
    def is_intel_machine(self) -> bool:
        return self.machine.casefold() in _DARWIN_INTEL_MACHINES


@dataclass(frozen=True, slots=True)
class _PageRenderWorkerDecision:
    """One immutable, self-consistent worker decision and its evidence.

    ``detected`` is the raw plausible authority value before hard-capping and
    is ``None`` for a manual override or a conservative fallback constant.
    """

    mode: _PageRenderWorkerMode
    configured: int | None
    selected: int
    detected: int | None
    hard_cap: int
    reason: _PageRenderWorkerReason
    topology: _CpuTopology

    def __post_init__(self) -> None:
        if not isinstance(self.mode, _PageRenderWorkerMode):
            raise TypeError("mode must be _PageRenderWorkerMode")
        if not isinstance(self.reason, _PageRenderWorkerReason):
            raise TypeError("reason must be _PageRenderWorkerReason")
        if not isinstance(self.topology, _CpuTopology):
            raise TypeError("topology must be _CpuTopology")
        if self.hard_cap != MAX_PAGE_RENDER_WORKERS:
            raise ValueError("hard_cap must be the fixed page-render worker cap")
        if type(self.selected) is not int or not 1 <= self.selected <= self.hard_cap:
            raise ValueError("selected workers must be from 1 through the hard cap")
        _require_optional_count(self.detected, field="detected")
        if self.mode is _PageRenderWorkerMode.MANUAL:
            if self.configured != self.selected or self.detected is not None:
                raise ValueError("a manual decision preserves its configured value")
            if self.reason is not _PageRenderWorkerReason.MANUAL_OVERRIDE:
                raise ValueError("a manual decision must record the manual reason")
            return
        if self.configured is not None:
            raise ValueError("an automatic decision has no configured value")
        if self.reason is _PageRenderWorkerReason.MANUAL_OVERRIDE:
            raise ValueError("an automatic decision cannot record the manual reason")
        if self.reason in _FALLBACK_REASONS:
            if self.detected is not None or self.selected != _CONSERVATIVE_FALLBACK:
                raise ValueError("a fallback decision selects exactly one worker")
        elif self.detected is None or self.selected != min(
            self.detected, self.hard_cap
        ):
            raise ValueError("a detected decision selects its hard-capped authority")

    @property
    def is_fallback(self) -> bool:
        return self.reason in _FALLBACK_REASONS

    def log_fields(self) -> tuple[tuple[str, str], ...]:
        """Return the ordered structured fields of the runtime-build log record.

        The runtime logs one such record each time it builds a CBZ-enabled
        runtime; the topology inside it is the once-per-process probe.
        """

        topology = self.topology
        return (
            ("mode", self.mode.value),
            ("configured", _render_optional(self.configured)),
            ("selected", str(self.selected)),
            ("detected", _render_optional(self.detected)),
            ("hard_cap", str(self.hard_cap)),
            ("platform", topology.platform),
            ("machine", topology.machine or "unknown"),
            ("process_cpu_count", _render_optional(topology.process_cpu_count)),
            ("cpu_count", _render_optional(topology.cpu_count)),
            (
                "darwin_performance_cores",
                _render_optional(topology.darwin_performance_cores),
            ),
            (
                "darwin_physical_cores",
                _render_optional(topology.darwin_physical_cores),
            ),
            ("darwin_translation", topology.darwin_translation.value),
            ("reason", self.reason.value),
        )

    def log_line(self) -> str:
        """Render the structured record as one ``key=value`` log line."""

        return "page_render_workers " + " ".join(
            f"{key}={value}" for key, value in self.log_fields()
        )


def _render_optional(value: int | None) -> str:
    return "none" if value is None else str(value)


def _plausible_cpu_count(value: int | None) -> int | None:
    if value is None or not 1 <= value <= _MAX_PLAUSIBLE_DETECTED_CPUS:
        return None
    return value


def _read_darwin_sysctl(name: str) -> int | None:
    """Read one fixed Darwin CPU-count sysctl without invoking a shell."""

    if name not in {_DARWIN_PERFORMANCE_CORES, _DARWIN_PHYSICAL_CORES}:
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
    return _plausible_cpu_count(int(raw))


def _invoke_darwin_translation_sysctl() -> tuple[int, int, int, int] | None:
    """Call Apple's fixed Rosetta sysctl and retain errno and ABI evidence."""

    try:
        library = CDLL(None, use_errno=True)
        sysctlbyname = library.sysctlbyname
        sysctlbyname.argtypes = [
            c_char_p,
            c_void_p,
            POINTER(c_size_t),
            c_void_p,
            c_size_t,
        ]
        sysctlbyname.restype = c_int
        value = c_int()
        size = c_size_t(sizeof(value))
        _set_errno(0)
        result = int(
            sysctlbyname(
                _DARWIN_PROCESS_TRANSLATED.encode("ascii"),
                byref(value),
                byref(size),
                None,
                0,
            )
        )
    except AttributeError, OSError, TypeError, ValueError:
        return None
    return result, _get_errno(), value.value, size.value


def _read_darwin_translation_status() -> _DarwinTranslation:
    invocation = _invoke_darwin_translation_sysctl()
    if invocation is None:
        return _DarwinTranslation.UNKNOWN
    result, error_number, value, returned_size = invocation
    if result != 0:
        # Apple documents ENOENT as a native (non-Rosetta) process.
        if error_number == ENOENT:
            return _DarwinTranslation.NATIVE
        return _DarwinTranslation.UNKNOWN
    if returned_size != sizeof(c_int):
        return _DarwinTranslation.UNKNOWN
    if value == 0:
        return _DarwinTranslation.NATIVE
    if value == 1:
        return _DarwinTranslation.TRANSLATED
    return _DarwinTranslation.UNKNOWN


def _running_platform() -> str:
    return sys.platform


def _probe_cpu_topology() -> _CpuTopology:
    running = _running_platform()
    machine = platform.machine()
    process_count = os.process_cpu_count()
    total_count = os.cpu_count()
    if running != _DARWIN_PLATFORM:
        return _CpuTopology(
            platform=running,
            machine=machine,
            process_cpu_count=process_count,
            cpu_count=total_count,
            darwin_performance_cores=None,
            darwin_physical_cores=None,
            darwin_translation=_DarwinTranslation.NOT_PROBED,
        )
    return _CpuTopology(
        platform=running,
        machine=machine,
        process_cpu_count=process_count,
        cpu_count=total_count,
        darwin_performance_cores=_read_darwin_sysctl(_DARWIN_PERFORMANCE_CORES),
        darwin_physical_cores=_read_darwin_sysctl(_DARWIN_PHYSICAL_CORES),
        darwin_translation=_read_darwin_translation_status(),
    )


# The process-wide topology cache is keyed by PID and guarded by one lock so
# that concurrent first calls probe exactly once (single flight) and a fork
# child never reuses its parent's facts.  ``os.register_at_fork`` replaces the
# lock in the child: a probe in flight on another parent thread at fork time
# would otherwise leave the child a lock nobody can release.
_topology_lock = threading.Lock()
_topology_cache: tuple[int, _CpuTopology] | None = None


def _reset_cpu_topology_cache() -> None:
    """Forget any probed topology and start a fresh, unlocked single flight."""

    global _topology_lock, _topology_cache
    _topology_lock = threading.Lock()
    _topology_cache = None


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_cpu_topology_cache)


def _detect_cpu_topology() -> _CpuTopology:
    """Return this process's topology, probing at most once per process.

    Every caller in the same process receives the same immutable record.  A
    fork child observes a different PID, so it probes its own topology instead
    of inheriting the parent's cached facts.
    """

    global _topology_cache
    pid = os.getpid()
    cached = _topology_cache
    if cached is not None and cached[0] == pid:
        return cached[1]
    with _topology_lock:
        cached = _topology_cache
        if cached is not None and cached[0] == pid:
            return cached[1]
        topology = _probe_cpu_topology()
        _topology_cache = (pid, topology)
        return topology


def _detected_decision(
    topology: _CpuTopology,
    detected: int,
    reason: _PageRenderWorkerReason,
) -> _PageRenderWorkerDecision:
    return _PageRenderWorkerDecision(
        mode=_PageRenderWorkerMode.AUTO,
        configured=None,
        selected=min(detected, MAX_PAGE_RENDER_WORKERS),
        detected=detected,
        hard_cap=MAX_PAGE_RENDER_WORKERS,
        reason=reason,
        topology=topology,
    )


def _fallback_decision(
    topology: _CpuTopology,
    reason: _PageRenderWorkerReason,
) -> _PageRenderWorkerDecision:
    return _PageRenderWorkerDecision(
        mode=_PageRenderWorkerMode.AUTO,
        configured=None,
        selected=_CONSERVATIVE_FALLBACK,
        detected=None,
        hard_cap=MAX_PAGE_RENDER_WORKERS,
        reason=reason,
        topology=topology,
    )


def _darwin_automatic_decision(topology: _CpuTopology) -> _PageRenderWorkerDecision:
    """Prefer highest-performance physical cores and fail conservatively."""

    performance_cores = _plausible_cpu_count(topology.darwin_performance_cores)
    if performance_cores is not None:
        return _detected_decision(
            topology,
            performance_cores,
            _PageRenderWorkerReason.DARWIN_PERFORMANCE_CORES,
        )

    # Intel Macs do not expose heterogeneous performance levels. Their total
    # physical-core count is therefore a safe fallback, but only after the
    # process is positively identified as native: Rosetta reports x86_64 too.
    # On translated, Apple Silicon, or unknown Darwin processes, never
    # reinterpret a logical/total CPU count as missing performance-core
    # authority. An unreadable translation flag also fails conservatively.
    if not topology.is_intel_machine:
        return _fallback_decision(
            topology,
            _PageRenderWorkerReason.DARWIN_PERFORMANCE_CORES_UNAVAILABLE_FALLBACK,
        )
    translation = topology.darwin_translation
    if translation is _DarwinTranslation.TRANSLATED:
        return _fallback_decision(
            topology,
            _PageRenderWorkerReason.DARWIN_INTEL_TRANSLATED_FALLBACK,
        )
    if translation is not _DarwinTranslation.NATIVE:
        return _fallback_decision(
            topology,
            _PageRenderWorkerReason.DARWIN_INTEL_TRANSLATION_UNKNOWN_FALLBACK,
        )
    physical_cores = _plausible_cpu_count(topology.darwin_physical_cores)
    if physical_cores is None:
        return _fallback_decision(
            topology,
            _PageRenderWorkerReason.DARWIN_INTEL_PHYSICAL_CORES_UNAVAILABLE_FALLBACK,
        )
    return _detected_decision(
        topology,
        physical_cores,
        _PageRenderWorkerReason.DARWIN_INTEL_NATIVE_PHYSICAL_CORES,
    )


def _automatic_decision(topology: _CpuTopology) -> _PageRenderWorkerDecision:
    if topology.is_darwin:
        return _darwin_automatic_decision(topology)
    # Other platforms use the process CPU availability first. A reported but
    # implausible availability is a fallback, never a reason to consult the
    # host-wide count, which may exceed a container's actual allotment.
    if topology.process_cpu_count is not None:
        available = _plausible_cpu_count(topology.process_cpu_count)
        if available is None:
            return _fallback_decision(
                topology,
                _PageRenderWorkerReason.CPU_COUNT_UNAVAILABLE_FALLBACK,
            )
        return _detected_decision(
            topology,
            available,
            _PageRenderWorkerReason.PROCESS_CPU_COUNT,
        )
    total = _plausible_cpu_count(topology.cpu_count)
    if total is None:
        return _fallback_decision(
            topology,
            _PageRenderWorkerReason.CPU_COUNT_UNAVAILABLE_FALLBACK,
        )
    return _detected_decision(topology, total, _PageRenderWorkerReason.CPU_COUNT)


def _require_manual_workers(configured: object) -> int:
    if type(configured) is not int:
        raise TypeError("page_render_workers must be int or None for automatic")
    if not 1 <= configured <= MAX_PAGE_RENDER_WORKERS:
        raise ValueError(
            f"page_render_workers must be from 1 through {MAX_PAGE_RENDER_WORKERS}"
        )
    return configured


def _decide_page_render_workers(
    configured: int | None = None,
    topology: _CpuTopology | None = None,
) -> _PageRenderWorkerDecision:
    """Return the immutable worker decision for one optional override.

    A manual override never inspects the platform to choose its value, but the
    decision still records the process-cached topology so the runtime-build
    log can show what the host looks like next to the configured count.
    """

    if topology is not None and not isinstance(topology, _CpuTopology):
        raise TypeError("topology must be _CpuTopology or None")
    if configured is not None:
        selected = _require_manual_workers(configured)
        facts = _detect_cpu_topology() if topology is None else topology
        return _PageRenderWorkerDecision(
            mode=_PageRenderWorkerMode.MANUAL,
            configured=selected,
            selected=selected,
            detected=None,
            hard_cap=MAX_PAGE_RENDER_WORKERS,
            reason=_PageRenderWorkerReason.MANUAL_OVERRIDE,
            topology=facts,
        )
    facts = _detect_cpu_topology() if topology is None else topology
    return _automatic_decision(facts)


def default_page_render_workers() -> int:
    """Return the automatic worker count from the process-cached topology."""

    return _automatic_decision(_detect_cpu_topology()).selected


def resolve_page_render_workers(configured: int | None = None) -> int:
    """Resolve automatic workers or preserve one strict bounded override."""

    if configured is None:
        return default_page_render_workers()
    return _require_manual_workers(configured)
