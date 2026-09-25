"""Fail local acceptance when pre-CBZ adapter work exceeds declared budgets.

Only disposable deterministic fixtures are accepted. This is a source-adapter
cost contract, not a full ingest or NAS completion-time acceptance result.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import runpy
import signal
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from dataclasses import asdict
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from time import monotonic, perf_counter, process_time
from typing import Any
from unittest.mock import patch

from h2hdb_ingest.artifact import ArtifactRenderPolicy
from h2hdb_ingest.core_source import VNextFilesystemSourceAdapter
from h2hdb_ingest.filesystem import FilesystemSource
from h2hdb_ingest.image_qualification import ImageGalleryQualifier
from h2hdb_ingest.source_performance import SourcePerformance

_HELPERS = runpy.run_path(str(Path(__file__).with_name("probe-source-io.py")))
_PROCESS = runpy.run_path(str(Path(__file__).with_name("run-pytest.py")))
_FORMAT = 1
_GIT_TIMEOUT = 10.0
_MODEL = {
    "dimensions": "P PAGE files, one completion marker, one gallery; fixed seed 47029",
    "units": "actual Python os.read bytes/calls, DirEntry.stat calls, scandir rows, decoded PAGE calls; logical I/O, never physical disk bytes",
    "budgets": {
        "source_page_read_bytes": "<= encoded PAGE bytes for each complete first/retry observation; one source-byte pass is the improvement target",
        "entry_stat_calls": "<= 8 * (P + 1) per complete observation; a constant allowance independent of number of pages returned by the adapter",
        "decode_calls": "== P when qualification enabled, == 0 in metadata-only or marker-only mode",
        "marker_only_page_bytes": "== 0; this probes adapter marker admission, not durable core reuse",
    },
    "budget_rationale": "One source pass can derive receipts while qualification consumes the same bytes. Eight entry passes allow initial indexing, component boundary checks and final revalidation without accepting a full directory rescan per returned page. These are targets, not claims that current implementation meets them.",
    "counterexamples": "an extra PAGE byte read, an extra entry scan above the fixed row budget, or omitted telemetry must fail",
    "limits": "No database, global analysis, publication, CBZ, cleanup, NAS timing or full-corpus SLA is measured. Decode remains required by the current qualification contract. No cache flush; synthetic local cache is warm.",
}


class _Interrupted(Exception):
    """A deliberate stop after the first FILE receipt page."""


class _WorkerInterrupted(RuntimeError):
    """A supervisor signal converted to bounded owned-process cleanup."""


@contextmanager
def _controlled_termination():
    pending: int | None = None
    deferred = True

    def handle(number: int, _frame: object) -> None:
        nonlocal pending, deferred
        if pending is None:
            pending = number
        if not deferred:
            deferred = True
            raise _WorkerInterrupted(f"source supervisor received signal {pending}")

    def defer(value: bool) -> None:
        nonlocal deferred
        deferred = value
        if not value and pending is not None:
            deferred = True
            raise _WorkerInterrupted(f"source supervisor received signal {pending}")

    previous = {
        number: signal.getsignal(number)
        for number in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    }
    try:
        for number in previous:
            signal.signal(number, handle)
        yield defer
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


class _Entry:
    def __init__(self, entry: os.DirEntry[str], meter: _Entries) -> None:
        self.entry, self.meter = entry, meter

    def __getattr__(self, name: str) -> Any:
        return getattr(self.entry, name)

    def stat(self, *, follow_symlinks: bool = True) -> os.stat_result:
        self.meter.stat_calls += 1
        if self.meter.revalidating:
            self.meter.revalidation_stat_calls += 1
        return self.entry.stat(follow_symlinks=follow_symlinks)


class _Scan:
    def __init__(self, iterator: Any, meter: _Entries) -> None:
        self.iterator, self.meter = iterator, meter

    def __enter__(self) -> _Scan:
        self.iterator.__enter__()
        return self

    def __exit__(self, *args: object) -> None:
        self.iterator.__exit__(*args)

    def __iter__(self) -> _Scan:
        return self

    def __next__(self) -> _Entry:
        value = next(self.iterator)
        self.meter.rows += 1
        if self.meter.revalidating:
            self.meter.revalidation_rows += 1
        return _Entry(value, self.meter)

    def close(self) -> None:
        self.iterator.close()


class _Entries:
    """Observe actual entry operations independently of production telemetry."""

    def __init__(self) -> None:
        self.rows = self.stat_calls = self.revalidation_calls = 0
        self.revalidation_rows = self.revalidation_stat_calls = 0
        self.revalidating = False
        self.revalidation_seconds = 0.0

    @contextmanager
    def instrument(self):
        original_scan = os.scandir
        original_revalidate = FilesystemSource._require_gallery_unchanged

        def scan(path: Any = ".") -> _Scan:
            return _Scan(original_scan(path), self)

        def revalidate(source: FilesystemSource, index: Any) -> None:
            before = self.revalidating
            self.revalidating = True
            self.revalidation_calls += 1
            started = perf_counter()
            try:
                original_revalidate(source, index)
            finally:
                self.revalidation_seconds += perf_counter() - started
                self.revalidating = before

        with (
            patch.object(os, "scandir", scan),
            patch.object(FilesystemSource, "_require_gallery_unchanged", revalidate),
        ):
            yield

    def report(self) -> dict[str, int | float]:
        return {
            "scandir_rows": self.rows,
            "entry_stat_calls": self.stat_calls,
            "revalidation_calls": self.revalidation_calls,
            "revalidation_rows": self.revalidation_rows,
            "revalidation_stat_calls": self.revalidation_stat_calls,
            "revalidation_seconds_inclusive": self.revalidation_seconds,
        }


def _canonical_digest(value: Any) -> str:
    return sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), default=bytes.hex
        ).encode()
    ).hexdigest()


def _fixture_oracle(root: Path) -> dict[str, Any]:
    """Independent known-fixture values, never derived from adapter output."""
    folder = root / "1000000"
    files, directories = [], []
    for path in sorted(folder.iterdir(), key=lambda item: os.fsencode(item.name)):
        identity = path.stat(follow_symlinks=False)
        common = {
            "name_bytes": os.fsencode(path.name),
            "device": identity.st_dev,
            "inode": identity.st_ino,
            "modified_ns": identity.st_mtime_ns,
            "changed_ns": identity.st_ctime_ns,
        }
        data = path.read_bytes()
        if len(data) != identity.st_size:
            raise ValueError("fixture changed during independent oracle construction")
        files.append(
            {
                **common,
                "sha256": sha256(data).hexdigest(),
                "size_bytes": len(data),
                "artifact_role": "metadata"
                if path.name == "galleryinfo.txt"
                else "page",
            }
        )
        directories.append({**common, "size_bytes": len(data), "file_type": 0})
    marker = next(value for value in files if value["name_bytes"] == b"galleryinfo.txt")
    accepted = {"accepted": True, "reason_code": None, "source_name": None}
    return {
        "locator": ("1000000",),
        "marker": {"file": marker, "observation_version": 3},
        "metadata": {
            "gid": 1000000,
            "title": "Source I/O probe 1000000",
            "comment": "Synthetic offline fixture",
            "upload_account": "synthetic",
            "upload_time": int(datetime(2024, 1, 2, 3, 4, tzinfo=UTC).timestamp())
            * 1_000_000,
            "download_time": int(datetime(2024, 2, 3, 4, 5, tzinfo=UTC).timestamp())
            * 1_000_000,
            "modified_time": marker["modified_ns"] // 1_000,
            "scan_observation_version": 3,
            "source_file_count": len(files),
            "page_count": len(files) - 1,
            "qualification_policy_sha256": bytes(32),
            "qualification": accepted,
        },
        "qualification": accepted,
        "files": files,
        "directories": directories,
        "tags": [
            {"namespace": "artist", "value": "synthetic-1000000"},
            {"namespace": "language", "value": "english"},
        ],
    }


def _file_value(value: Any) -> dict[str, Any]:
    return {
        "name_bytes": value.name_bytes,
        "sha256": value.content.file_sha256.hex(),
        "size_bytes": value.content.size_bytes,
        "artifact_role": value.artifact_role.value,
        **{
            name: getattr(value, name)
            for name in ("device", "inode", "modified_ns", "changed_ns")
        },
    }


def _observe(
    adapter: VNextFilesystemSourceAdapter, *, mode: str, oracle: dict[str, Any]
) -> tuple[dict[str, int], dict[str, Any]]:
    locator = ("1000000",)
    marker = adapter.observe_completion_marker(locator)
    actual = {
        "marker": {
            "file": _file_value(marker.file),
            "observation_version": marker.observation_version,
        }
    }
    expected = {"marker": oracle["marker"]} if mode == "marker_only" else oracle
    rows = {"file_rows": 0, "directory_rows": 0, "tag_rows": 0}
    if actual["marker"] != oracle["marker"]:
        raise ValueError("source marker differs from independent fixture oracle")
    if mode != "marker_only":
        observed = adapter.observe_gallery(locator)
        actual.update(
            locator=observed.locator_components,
            metadata=asdict(observed.metadata),
            qualification=asdict(observed.qualification),
        )
        for name in ("locator", "metadata", "qualification"):
            if actual[name] != oracle[name]:
                raise ValueError(
                    f"source {name} differs from independent fixture oracle"
                )
        for component, method, limit, cursor_name, key in (
            (
                "files",
                adapter.list_file_observations,
                256,
                "after_name_bytes",
                "file_rows",
            ),
            (
                "directories",
                adapter.list_directory_observations,
                192,
                "after_name_bytes",
                "directory_rows",
            ),
            ("tags", adapter.list_tag_observations, 256, "after_ordinal", "tag_rows"),
        ):
            values, cursor = [], None
            while True:
                page = method(observed, **{cursor_name: cursor}, limit=limit)
                start = len(values)
                for item in page.items:
                    if component == "files":
                        value = _file_value(item)
                    elif component == "directories":
                        value = asdict(item)
                    else:
                        value = {"namespace": item.namespace, "value": item.value}
                    values.append(value)
                end = len(values)
                terminal = end == len(oracle[component])
                if (
                    not page.items
                    or values[start:] != oracle[component][start:end]
                    or page.terminal != terminal
                    or page.next_after
                    != (
                        None
                        if terminal
                        else end - 1
                        if component == "tags"
                        else values[-1]["name_bytes"]
                    )
                ):
                    raise ValueError(
                        f"source {component} sequence/cursor differs from independent fixture oracle"
                    )
                rows[key] = end
                if mode == "interrupted" and component == "files":
                    raise _Interrupted(
                        "after first FILE receipt page; no durable core seal"
                    )
                if terminal:
                    break
                cursor = page.next_after
            actual[component] = values
        if adapter.observe_completion_marker(locator) != marker:
            raise RuntimeError("synthetic source completion marker changed")
    expected_digest, actual_digest = (
        _canonical_digest(expected),
        _canonical_digest(actual),
    )
    if actual_digest != expected_digest:
        raise ValueError(
            "complete source sequence differs from independent fixture oracle"
        )
    return rows, {
        "status": "verified",
        "scope": "completion_marker"
        if mode == "marker_only"
        else "complete_observation",
        "expected_sha256": expected_digest,
        "observed_sha256": actual_digest,
        "fixture_source_oracle_sha256": _canonical_digest(oracle),
    }


def _case(
    root: Path, *, mode: str, workers: int, oracle: dict[str, Any] | None = None
) -> dict[str, Any]:
    # Oracle construction is excluded from adapter timing and independent I/O counts.
    oracle = _fixture_oracle(root) if oracle is None else oracle
    oracle_evidence: dict[str, Any] = {}
    meter = _HELPERS["_Meter"](root)
    entries = _Entries()
    performance = SourcePerformance()
    metrics: list[Any] = []
    rows: dict[str, int] = {}
    interrupted = False
    wall, cpu = perf_counter(), process_time()
    try:
        with (
            meter.instrument(),
            entries.instrument(),
            performance.operation(metrics.append, interruptions=(_Interrupted,)),
            FilesystemSource(root, performance=performance) as source,
        ):
            adapter = VNextFilesystemSourceAdapter(
                source,
                qualify_gallery=(
                    None
                    if mode in {"marker_only", "metadata_only"}
                    else ImageGalleryQualifier(ArtifactRenderPolicy(), workers=workers)
                ),
                performance=performance,
            )
            rows, oracle_evidence = _observe(adapter, mode=mode, oracle=oracle)
    except _Interrupted:
        interrupted = True
    result = {
        **meter.report(),
        "entries": entries.report(),
        "elapsed_seconds": perf_counter() - wall,
        "process_cpu_seconds": process_time() - cpu,
        "returned_rows": rows,
        "content_oracle": oracle_evidence,
        "mode": mode,
        "interrupted": interrupted,
    }
    if len(metrics) != 1:
        raise RuntimeError("source acceptance lost its production metric")
    metric = metrics[0]
    counters = {item.name: item.value for item in metric.counters}
    expected = {
        "read_calls": sum(
            v["read_calls"] for k, v in result["io"].items() if k.startswith("source.")
        ),
        "logical_bytes_read": sum(
            v["read_bytes"] for k, v in result["io"].items() if k.startswith("source.")
        ),
        "qualified_galleries": result["qualified_galleries"],
    }
    if any(counters.get(name, 0) != value for name, value in expected.items()):
        raise RuntimeError(
            "production source counters differ from independent I/O meter"
        )
    if metric.status != ("interrupted" if interrupted else "completed"):
        raise RuntimeError("production source status differs from observed outcome")
    result["production_telemetry"] = {
        "status": metric.status,
        "counters": counters,
        "phases_ns_inclusive": {item.name: item.value for item in metric.phases_ns},
        "absent_zero_counters": ["qualified_galleries"]
        if mode in {"metadata_only", "marker_only"}
        and "qualified_galleries" not in counters
        else [],
    }
    result["telemetry_comparison"] = {"matched": True, "independent_counters": expected}
    qualification_seconds = result["operation_seconds_nonadditive"].get(
        "qualification_total", 0.0
    )
    result["wall_breakdown"] = {
        "qualification_seconds": qualification_seconds,
        "other_source_seconds": max(
            0.0, result["elapsed_seconds"] - qualification_seconds
        ),
        "scope": "two non-overlapping main-thread intervals; other includes FILE receipts, entry indexing/revalidation outside qualification, markers and context teardown",
    }
    result["attributed_costs_nonadditive"] = sorted(
        [
            {"name": "directory_revalidation", "seconds": entries.revalidation_seconds},
            *(
                {"name": name, "seconds": value}
                for name, value in result["operation_seconds_nonadditive"].items()
            ),
            *(
                {"name": "source_" + name, "seconds": value / 1_000_000_000}
                for name, value in result["production_telemetry"][
                    "phases_ns_inclusive"
                ].items()
            ),
        ],
        key=lambda item: item["seconds"],
        reverse=True,
    )
    return result


def _costs(case: dict[str, Any], *, pages: int, page_bytes: int) -> dict[str, Any]:
    try:
        if pages < 1 or page_bytes < 1:
            raise ValueError("fixture must contain nonempty PAGE inputs")
        if case["mode"] not in {
            "first",
            "metadata_only",
            "marker_only",
            "retry",
            "interrupted",
        }:
            raise ValueError("unknown measured source mode")
        source_io = {k: v for k, v in case["io"].items() if k.startswith("source.")}
        if not source_io or not case["source_files"]:
            raise ValueError("source measurement is missing, not zero-cost")
        for name in ("read_calls", "read_bytes"):
            values = [item[name] for item in source_io.values()]
            if any(type(v) is not int or v < 0 for v in values):
                raise ValueError("source I/O counters are malformed")
            if sum(values) != sum(item[name] for item in case["source_files"].values()):
                raise ValueError("source file and phase read totals disagree")
        independent = {
            "read_calls": sum(v["read_calls"] for v in source_io.values()),
            "logical_bytes_read": sum(v["read_bytes"] for v in source_io.values()),
            "qualified_galleries": case["qualified_galleries"],
        }
        if case["telemetry_comparison"]["independent_counters"] != independent:
            raise ValueError("independent read reconciliation is missing or differs")
        no_images = case["mode"] in {"marker_only", "metadata_only"}
        telemetry = case["production_telemetry"]
        expected_absent = (
            ["qualified_galleries"]
            if no_images and "qualified_galleries" not in telemetry["counters"]
            else []
        )
        if telemetry["absent_zero_counters"] != expected_absent:
            raise ValueError("absent counters lack exact zero-work mode evidence")
        for name, value in independent.items():
            recorded = 0 if name in expected_absent else telemetry["counters"][name]
            if recorded != value:
                raise ValueError("production telemetry differs from independent counts")
        if telemetry["status"] != (
            "interrupted" if case["interrupted"] else "completed"
        ):
            raise ValueError("source telemetry has the wrong completion status")
        phases = telemetry["phases_ns_inclusive"]
        required_phases = {"read", "hash"}
        if case["mode"] != "marker_only":
            required_phases.update(("gallery_index", "metadata_parse"))
        if not no_images:
            required_phases.add("qualification")
        if (
            not isinstance(phases, dict)
            or not required_phases <= set(phases)
            or any(type(v) is not int or v < 0 for v in phases.values())
        ):
            raise ValueError(
                "required source phase attribution is missing or malformed"
            )
        if telemetry["counters"].get("clock_failures", 0):
            raise ValueError("source phase attribution had clock failures")
        if not isinstance(case["interrupted"], bool):
            raise ValueError("production telemetry differs from independent counts")
        observed = {
            "source_page_read_bytes": sum(
                v["read_bytes"]
                for k, v in case["io"].items()
                if k.startswith("source.") and k.endswith(".page")
            ),
            "entry_stat_calls": case["entries"]["entry_stat_calls"],
            "decode_calls": case["decode_calls"],
        }
        if any(type(v) is not int or v < 0 for v in observed.values()):
            raise ValueError("cost counters must be nonnegative integers")
        for name in ("elapsed_seconds", "process_cpu_seconds"):
            if (
                not isinstance(case[name], (int, float))
                or not math.isfinite(case[name])
                or case[name] < 0
            ):
                raise ValueError("timing evidence must be finite and nonnegative")
        if case["telemetry_comparison"]["matched"] is not True:
            raise ValueError("production telemetry has not been reconciled")
        if case["interrupted"]:
            return {
                "status": "incomplete",
                "reason": "partial injected attempt; retry is measured separately",
            }
        oracle = case["content_oracle"]
        if (
            oracle["status"] != "verified"
            or oracle["scope"]
            != (
                "completion_marker"
                if case["mode"] == "marker_only"
                else "complete_observation"
            )
            or oracle["observed_sha256"] != oracle["expected_sha256"]
            or len(oracle["observed_sha256"]) != 64
            or len(oracle["fixture_source_oracle_sha256"]) != 64
        ):
            raise ValueError("source content oracle evidence is missing or differs")
        expected_rows = (
            {"file_rows": 0, "directory_rows": 0, "tag_rows": 0}
            if case["mode"] == "marker_only"
            else {"file_rows": pages + 1, "directory_rows": pages + 1, "tag_rows": 2}
        )
        if case["returned_rows"] != expected_rows:
            raise ValueError("adapter did not complete the exact fixture components")
        if case["qualified_galleries"] != int(not no_images) or case[
            "accepted_galleries"
        ] != int(not no_images):
            raise ValueError("adapter did not qualify the complete expected gallery")
        bounds = {
            "source_page_read_bytes": 0
            if case["mode"] == "marker_only"
            else page_bytes,
            "entry_stat_calls": 8 * (pages + 1),
            "decode_calls": 0 if no_images else pages,
        }
        checks = {
            name: {
                "observed": value,
                "bound": bounds[name],
                "unit": {
                    "source_page_read_bytes": "logical source PAGE bytes",
                    "entry_stat_calls": "DirEntry.stat calls",
                    "decode_calls": "decoded PAGE images",
                }[name],
                "rationale": _MODEL["budgets"][name],
                "comparison": "equal" if name == "decode_calls" else "at_most",
                "met": value == bounds[name]
                if name == "decode_calls"
                else value <= bounds[name],
            }
            for name, value in observed.items()
        }
    except (KeyError, TypeError, ValueError) as error:
        return {"status": "incomplete", "reason": str(error)}
    return {
        "status": "satisfied" if all(c["met"] for c in checks.values()) else "violated",
        "checks": checks,
    }


def _dimensions(
    sizes: tuple[int, ...], edge: int, image_cases: bool
) -> tuple[tuple[int, int], ...]:
    values = [(pages, edge) for pages in sizes]
    if image_cases:
        values.extend(((4, 1024), (1, 2048)))
    return tuple(dict.fromkeys(values))


def _run(
    *,
    sizes: tuple[int, ...],
    edge: int,
    codecs: tuple[str, ...],
    repeats: int,
    workers: int,
    image_cases: bool,
    workspace: Path,
) -> dict[str, Any]:
    initial_provenance = _provenance(include_git=False)
    cases: list[dict[str, Any]] = []
    setups: list[dict[str, Any]] = []
    dimensions = _dimensions(sizes, edge, image_cases)
    with tempfile.TemporaryDirectory(
        prefix="adapter-cost-", dir=workspace
    ) as temporary:
        for codec in codecs:
            for pages, image_edge in dimensions:
                root = Path(temporary) / f"{codec}-{pages}-{image_edge}"
                started = perf_counter()
                _HELPERS["_fixture"](root, 1, pages, image_edge, codec)
                manifest = _HELPERS["_manifest"](root)
                oracle = _fixture_oracle(root)
                fixture = {
                    "pages": pages,
                    "dimensions": [image_edge, image_edge],
                    "codec": codec,
                    "seed": 47029,
                    "source_oracle_sha256": _canonical_digest(oracle),
                    "aggregate_pixels": pages * image_edge * image_edge,
                    **manifest,
                }
                setups.append(
                    {"fixture": fixture, "setup_seconds": perf_counter() - started}
                )
                for cycle in range(repeats):
                    for mode in ("first", "metadata_only", "marker_only"):
                        case = _case(root, mode=mode, workers=workers, oracle=oracle)
                        case.update(fixture=fixture, cycle=cycle)
                        case["acceptance"] = _costs(
                            case, pages=pages, page_bytes=manifest["page_bytes"]
                        )
                        cases.append(case)
                partial = _case(
                    root, mode="interrupted", workers=workers, oracle=oracle
                )
                if partial["interrupted"] is not True:
                    raise RuntimeError("injected interruption did not occur")
                retry = _case(root, mode="retry", workers=workers, oracle=oracle)
                retry.update(
                    fixture=fixture, cycle=0, prior_interrupted_attempt=partial
                )
                retry["acceptance"] = _costs(
                    retry, pages=pages, page_bytes=manifest["page_bytes"]
                )
                cases.append(retry)
    statuses = {case["acceptance"]["status"] for case in cases}
    acceptance = (
        "incomplete"
        if "incomplete" in statuses
        else "violated"
        if "violated" in statuses
        else "satisfied"
    )
    if _provenance(include_git=False) != initial_provenance:
        raise RuntimeError("runtime or probe source changed during measurement")
    return {
        "status": "completed",
        "schema_version": _FORMAT,
        "acceptance": {"status": acceptance, "model": _MODEL},
        "provenance": {**initial_provenance, "measurement_source_stable": True},
        "fixture_setup": setups,
        "cases": cases,
        "scope": {
            "qualification": "real decode/shrink/resize, no JPEG encode or CBZ creation",
            "first": "fresh FilesystemSource for every cycle; full adapter components and marker closure",
            "marker_only": "fresh adapter completion-marker probe; durable unchanged-observation reuse must be tested by core integration separately",
            "retry": "fresh adapter after deliberate stop after first FILE page; no durable partial gallery receipt is assumed",
            "measurements": "elapsed includes source context setup/teardown and linear independent response verification; fixture generation and oracle byte reads are separate; CPU includes native decoder threads; nested operation times are nonadditive",
            "acceptance": "work-unit targets only; passing would not establish NAS 24h/7d throughput",
        },
    }


def _provenance(*, include_git: bool = True) -> dict[str, Any]:
    result = _HELPERS["_provenance"]()
    repository = Path(__file__).resolve().parents[1]
    expected_ingest = _HELPERS["_package_digest"](repository / "src" / "h2hdb_ingest")
    if result["h2hdb_ingest"]["python_source_sha256"] != expected_ingest:
        raise ValueError("imported ingest runtime differs from the checked-out source")
    result["ingest_checkout_source_verified"] = True
    result["ingest_checkout_source_sha256"] = expected_ingest
    if include_git:
        result["checkout_commit"] = _git_text(repository, "rev-parse", "HEAD")
        result["checkout_dirty"] = bool(_git_text(repository, "status", "--porcelain"))
    result["acceptance_sha256"] = sha256(Path(__file__).read_bytes()).hexdigest()
    result["process_owner_sha256"] = sha256(
        Path(__file__).with_name("run-pytest.py").read_bytes()
    ).hexdigest()
    return result


def _git_text(repository: Path, *arguments: str) -> str:
    # Parent-side provenance is also bounded; Git can block on a filesystem or
    # spawn descendants just as the measured worker can.
    with tempfile.TemporaryDirectory(prefix="h2hdb-source-cost-git-") as workspace:
        result = _bounded_worker(
            ["git", "-C", str(repository), *arguments],
            timeout=_GIT_TIMEOUT,
            workspace=Path(workspace),
        )
    return result.stdout.strip()


def _validate_report(
    report: Any,
    *,
    dimensions: set[tuple[int, int, str]],
    repeats: int,
    initial_provenance: dict[str, Any] | None = None,
) -> None:
    if (
        not isinstance(report, dict)
        or report.get("status") != "completed"
        or report.get("schema_version") != _FORMAT
        or not isinstance(report.get("cases"), list)
    ):
        raise ValueError("worker returned incomplete source acceptance evidence")
    current_provenance = _provenance(include_git=False)
    if initial_provenance is not None and current_provenance != initial_provenance:
        raise ValueError("supervisor source changed while worker was running")
    if report["provenance"]["measurement_source_stable"] is not True:
        raise ValueError("worker lacks stable measurement source evidence")
    for key, value in current_provenance.items():
        if report["provenance"][key] != value:
            raise ValueError("worker source provenance differs from reviewed checkout")
    # Git runs only in the supervisor, avoiding nested detached process groups
    # inside the measured worker. Exact source/script digests are compared first.
    repository = Path(__file__).resolve().parents[1]
    report["provenance"]["checkout_commit"] = _git_text(repository, "rev-parse", "HEAD")
    report["provenance"]["checkout_dirty"] = bool(
        _git_text(repository, "status", "--porcelain")
    )
    report["provenance"]["git_measurement_scope"] = (
        "supervisor after exact worker source/script digest comparison"
    )
    expected = {
        (pages, edge, codec, mode, cycle)
        for pages, edge, codec in dimensions
        for mode in ("first", "metadata_only", "marker_only")
        for cycle in range(repeats)
    }
    expected.update(
        (pages, edge, codec, "retry", 0) for pages, edge, codec in dimensions
    )
    actual = set()
    for case in report["cases"]:
        fixture = case["fixture"]
        key = (
            fixture["pages"],
            fixture["dimensions"][0],
            fixture["codec"],
            case["mode"],
            case["cycle"],
        )
        if (
            key in actual
            or fixture["dimensions"] != [key[1], key[1]]
            or len(fixture["page_encoded_bytes"]) != key[0]
            or fixture["page_bytes"] != sum(fixture["page_encoded_bytes"])
            or case["content_oracle"]["fixture_source_oracle_sha256"]
            != fixture["source_oracle_sha256"]
        ):
            raise ValueError("worker returned inconsistent source dimensions")
        if (
            case["mode"] == "retry"
            and case["prior_interrupted_attempt"]["interrupted"] is not True
        ):
            raise ValueError("retry evidence lacks the interrupted attempt")
        actual.add(key)
        case["acceptance"] = _costs(
            case, pages=fixture["pages"], page_bytes=fixture["page_bytes"]
        )
    if actual != expected:
        raise ValueError("worker omitted a required source acceptance case")
    statuses = {case["acceptance"]["status"] for case in report["cases"]}
    report["acceptance"] = {
        "status": "incomplete"
        if "incomplete" in statuses
        else "violated"
        if "violated" in statuses
        else "satisfied",
        "model": _MODEL,
    }


def _bounded_worker(
    command: list[str],
    *,
    timeout: float,
    workspace: Path,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Reuse the repository's process-tree ownership with file-backed output."""
    if os.name != "posix":
        raise ValueError("source cost acceptance supervisor currently requires POSIX")
    deadline = monotonic() + timeout
    execution_deadline = deadline - min(5.0, timeout / 2)
    output, errors = workspace / "worker.stdout", workspace / "worker.stderr"
    with (
        output.open("wb") as stdout,
        errors.open("wb") as stderr,
        _controlled_termination() as defer,
    ):
        owner = None
        empty = False
        try:
            # Defer signals until ownership is established, including the small
            # interval between Popen returning and assigning the owner.
            process = subprocess.Popen(
                command, stdout=stdout, stderr=stderr, start_new_session=True, env=env
            )
            owner = _PROCESS["_OwnedProcess"](process)
            defer(False)
            process.wait(timeout=max(0.0, execution_deadline - monotonic()))
            empty = _PROCESS["_wait_for_owned_tree_exit"](
                owner, deadline=min(deadline, monotonic() + 0.25)
            )
            if not empty:
                raise RuntimeError("source worker left surviving descendants")
        finally:
            defer(True)
            if owner is not None and not empty:
                empty = _PROCESS["_terminate_owned_tree"](
                    owner, deadline=min(deadline, monotonic() + 5.0)
                )
            if owner is not None and not empty:
                raise RuntimeError("source worker process-tree cleanup is incomplete")
        defer(False)
    result = subprocess.CompletedProcess(
        command, process.returncode, output.read_text(), errors.read_text()
    )
    result.check_returncode()
    return result


def _fresh_python_environment(
    command: list[str], workspace: Path
) -> tuple[list[str], dict[str, str]]:
    # Timestamp-valid stale bytecode in the checkout/site-packages cannot be
    # consulted. Both native/Python temp data and new caches are owned by the
    # supervisor and are removed even after SIGKILL skips worker teardown.
    cache, temporary = workspace / "pycache", workspace / "temporary"
    cache.mkdir()
    temporary.mkdir()
    return (
        [command[0], "-X", f"pycache_prefix={cache}", *command[1:]],
        {
            **os.environ,
            "TMPDIR": str(temporary),
            "TMP": str(temporary),
            "TEMP": str(temporary),
        },
    )


def _worker_binding(workspace: Path) -> dict[str, str]:
    expected = {
        "bytecode": "fresh supervisor-owned cache; compile source without existing .pyc",
        "pycache_prefix": str(workspace / "pycache"),
        "temporary_root": str(workspace / "temporary"),
    }
    if (
        sys.pycache_prefix != expected["pycache_prefix"]
        or tempfile.gettempdir() != expected["temporary_root"]
    ):
        raise ValueError("worker lacks fresh bytecode cache and owned temporary root")
    return expected


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    integer = _HELPERS["_bounded_integer"]
    parser.add_argument("--sizes", default="127,128,129,512")
    parser.add_argument("--edge", type=integer(16, 2048), default=128)
    parser.add_argument("--codec", choices=("png", "jpeg", "both"), default="both")
    parser.add_argument("--repeats", type=integer(2, 4), default=2)
    parser.add_argument("--workers", type=integer(1, 4), default=1)
    parser.add_argument("--skip-image-cases", action="store_true")
    parser.add_argument("--timeout", type=integer(10, 3600), default=300)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--workspace", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    try:
        sizes = tuple(int(v) for v in args.sizes.split(","))
        if (
            not sizes
            or len(sizes) > 8
            or len(set(sizes)) != len(sizes)
            or any(not 1 <= v <= 4096 for v in sizes)
        ):
            raise ValueError("sizes must contain 1..8 unique PAGE counts in 1..4096")
        for size in sizes:
            _HELPERS["_require_fixture_budget"](1, size, args.edge)
    except ValueError as error:
        parser.error(str(error))
    if args.worker:
        if args.workspace is None:
            parser.error("internal worker requires supervisor-owned workspace")
        binding = _worker_binding(args.workspace)
        _provenance(include_git=False)  # Reject runtime drift before fixture work.
        report = _run(
            sizes=sizes,
            edge=args.edge,
            codecs=("png", "jpeg") if args.codec == "both" else (args.codec,),
            repeats=args.repeats,
            workers=args.workers,
            image_cases=not args.skip_image_cases,
            workspace=args.workspace,
        )
        report["execution_binding"] = binding
        print(json.dumps(report))
        return 0
    if args.output is None or os.path.lexists(args.output):
        parser.error("--output must be a new report path")
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--sizes",
        args.sizes,
        "--edge",
        str(args.edge),
        "--codec",
        args.codec,
        "--repeats",
        str(args.repeats),
        "--workers",
        str(args.workers),
    ]
    if args.skip_image_cases:
        command.append("--skip-image-cases")
    try:
        initial_provenance = _provenance(include_git=False)
        with tempfile.TemporaryDirectory(prefix="h2hdb-source-cost-") as workspace:
            fresh_command, worker_env = _fresh_python_environment(
                [*command, "--workspace", workspace], Path(workspace)
            )
            child = _bounded_worker(
                fresh_command,
                timeout=args.timeout,
                workspace=Path(workspace),
                env=worker_env,
            )
        report = json.loads(child.stdout)
        binding = report["execution_binding"]
        if binding != {
            "bytecode": "fresh supervisor-owned cache; compile source without existing .pyc",
            "pycache_prefix": str(Path(workspace) / "pycache"),
            "temporary_root": str(Path(workspace) / "temporary"),
        }:
            raise ValueError("worker runtime binding differs from supervisor ownership")
        dimensions = _dimensions(sizes, args.edge, not args.skip_image_cases)
        codecs = ("png", "jpeg") if args.codec == "both" else (args.codec,)
        _validate_report(
            report,
            dimensions={(p, e, c) for p, e in dimensions for c in codecs},
            repeats=args.repeats,
            initial_provenance=initial_provenance,
        )
    except (
        OSError,
        subprocess.SubprocessError,
        ValueError,
        AttributeError,
        KeyError,
        TypeError,
        RuntimeError,
    ) as error:
        report = {
            "status": "error",
            "schema_version": _FORMAT,
            "acceptance": {"status": "incomplete", "model": _MODEL},
            "error_type": type(error).__name__,
            "error": str(error),
        }
        if isinstance(error, subprocess.CalledProcessError):
            report["worker_stderr"] = error.stderr[-16000:]
    try:
        _HELPERS["_atomic_report"](args.output, report)
    except (OSError, TypeError, ValueError) as error:
        print(
            json.dumps(
                {
                    "status": "error",
                    "acceptance": "incomplete",
                    "error_type": type(error).__name__,
                    "error": f"cannot store source cost report: {error}",
                }
            ),
            file=sys.stderr,
        )
        return 2
    print(
        json.dumps(
            {
                "status": report["status"],
                "acceptance": report["acceptance"]["status"],
                "output": str(args.output),
            }
        )
    )
    return {"satisfied": 0, "violated": 1, "incomplete": 2}[
        report["acceptance"]["status"]
    ]


if __name__ == "__main__":
    raise SystemExit(main())
