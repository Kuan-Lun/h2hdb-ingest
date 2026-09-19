"""A/B actual snapshot capture wall time against the frozen pre-batching writer.

Manual, offline, disposable experiment. Source counts cross the 128-item cap;
all bytes and receipts are checked outside timed capture. No simulated latency,
NAS claim, timing assertion in CI, cache flush, or durable source DB is involved.
"""

from __future__ import annotations

import argparse
import importlib.machinery
import importlib.util
import json
import platform
import random
import sqlite3
import statistics
import subprocess
import sys
import tempfile
from dataclasses import asdict, replace
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from time import perf_counter_ns, process_time_ns

from h2hdb import FileContentReceipt

from h2hdb_ingest.filesystem import (
    FilesystemArtifactSourceRole,
    FilesystemFileObservation,
    FilesystemStat,
)
from h2hdb_ingest.metrics import IngestMetric
from h2hdb_ingest.source_monitor import (
    FilesystemCompletionMarkerProbe,
    SourceChangeMonitor,
)
from h2hdb_ingest.source_performance import SourcePerformance
from h2hdb_ingest.source_schedule import SourceScanSchedule
from h2hdb_ingest.source_snapshot import SourceSnapshotStore

_ROOT = Path(__file__).resolve().parents[1]
_BASELINE = _ROOT / "tests/fixtures/source_snapshot_0_25_3.py.txt"
_BASELINE_SHA256 = "ba1898e0e543e93407eafb3f0b03db8eacd9732feb8b20d886740867a49ade5d"

_OBSERVER_BASELINE = _ROOT / "tests/fixtures/filesystem_content_parts_0_25_3.py.txt"
_OBSERVER_BASELINE_SHA256 = (
    "46bcb2a32cd9bdec67ec00b72392e15d5b02004f8dae12f0a4894a47883428e7"
)


def _observation_type(variant: str):
    if variant == "bounded_page":
        return FilesystemFileObservation
    if sha256(_OBSERVER_BASELINE.read_bytes()).hexdigest() != _OBSERVER_BASELINE_SHA256:
        raise RuntimeError("historical filesystem observer fixture changed")
    name = "h2hdb_ingest._observer_benchmark_baseline"
    loader = importlib.machinery.SourceFileLoader(name, str(_OBSERVER_BASELINE))
    spec = importlib.util.spec_from_loader(name, loader)
    if spec is None:
        raise RuntimeError("cannot load historical observer fixture")
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module.HistoricalFileObservation


def _baseline_type():
    if sha256(_BASELINE.read_bytes()).hexdigest() != _BASELINE_SHA256:
        raise RuntimeError("historical source snapshot fixture changed")
    name = "h2hdb_ingest._snapshot_benchmark_baseline"
    loader = importlib.machinery.SourceFileLoader(name, str(_BASELINE))
    spec = importlib.util.spec_from_loader(name, loader)
    if spec is None:
        raise RuntimeError("cannot load historical snapshot fixture")
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module.SourceSnapshotStore


def _fixture(root: Path, count: int, size: int) -> tuple[Path, ...]:
    root.mkdir()
    payload = random.Random(293811).randbytes(size)
    result = []
    for position in range(count):
        path = root / f"{position:04d}.jpg"
        # Unique content detects order/locator/name mistakes, even in a page.
        path.write_bytes(position.to_bytes(8, "big") + payload[8:])
        result.append(path)
    return tuple(result)


def _capture(paths: tuple[Path, ...], variant: str) -> dict[str, object]:
    performance = SourcePerformance()
    observation_type = _observation_type(variant)
    observed = tuple(
        observation_type(
            path.parent,
            path.name.encode(),
            FilesystemStat.from_os_stat(path.stat()),
            FilesystemArtifactSourceRole.PAGE,
            _source_performance=performance,
        )
        for path in paths
    )
    factory = (
        _baseline_type() if variant == "historical_per_file" else SourceSnapshotStore
    )
    with factory() as store:
        # Actual SQLite trace counts, independent from production metric counters.
        statements: dict[str, int] = {}

        def trace(sql: str) -> None:
            verb = sql.split()[0]
            statements[verb] = statements.get(verb, 0) + 1

        store._connection.set_trace_callback(trace)
        started = perf_counter_ns()
        cpu_started = process_time_ns()
        if variant == "historical_per_file":
            receipts = tuple(store.capture(("gallery",), item) for item in observed)
        else:
            receipts = tuple(
                receipt
                for start in range(0, len(observed), 128)
                for receipt in store.capture_many(
                    ("gallery",), observed[start : start + 128], performance=performance
                )
            )
        cpu_ns = process_time_ns() - cpu_started
        elapsed_ns = perf_counter_ns() - started
        capture_statements = dict(statements)
        verified = 0
        expected_digest = sha256()
        for path, receipt in zip(paths, receipts, strict=True):
            expected = path.read_bytes()
            if receipt != FileContentReceipt.from_parts((expected,)):
                raise RuntimeError("receipt differs from independent source bytes")
            stream = store.open_source(("gallery",), path.name.encode())
            if stream is None:
                raise RuntimeError("captured member missing")
            with stream:
                if stream.read() != expected:
                    raise RuntimeError(
                        "captured bytes differ from independent source bytes"
                    )
            expected_digest.update(receipt.file_sha256)
            verified += 1
        commits = (
            len(paths)
            if variant == "historical_per_file"
            else (len(paths) + 127) // 128
        )
        if capture_statements.get("COMMIT") != commits:
            raise RuntimeError(
                "capture transaction count differs from predeclared bound"
            )
        metric = replace(
            performance.metric(
                status="completed", scope="source_snapshot", operation="capture"
            ),
            elapsed_ns=elapsed_ns,
        )
        counters = {v.name: v.value for v in metric.counters}
        if counters["logical_bytes_read"] != sum(path.stat().st_size for path in paths):
            raise RuntimeError("read metric differs from fixture size")
        return {
            "variant": variant,
            "elapsed_ns": elapsed_ns,
            "process_cpu_ns": cpu_ns,
            "sqlite_statements": capture_statements,
            "source_metrics": asdict(metric),
            "verified_files": verified,
            "receipt_digest": expected_digest.hexdigest(),
        }


def _one_variant(
    paths: tuple[Path, ...], variant: str, monitor_root: Path | None
) -> dict[str, object]:
    monitor_metrics: list[IngestMetric] = []
    started = datetime.now(UTC).isoformat()
    if monitor_root is None:
        result = _capture(paths, variant)
    else:
        schedule = SourceScanSchedule(quiet_seconds=300, max_wait_seconds=1800, now=0)
        with SourceChangeMonitor(
            probe=FilesystemCompletionMarkerProbe(
                monitor_root, metrics_sink=monitor_metrics.append
            ),
            schedule=schedule,
            interval_seconds=0,
        ) as monitor:
            result = _capture(paths, variant)
            monitor.raise_if_failed()
        monitor.raise_if_failed()
    result.update(
        {
            "started_at": started,
            "finished_at": datetime.now(UTC).isoformat(),
            "monitor_metrics": [asdict(item) for item in monitor_metrics],
        }
    )
    return result


def _matrix(args: argparse.Namespace) -> dict[str, object]:
    started = datetime.now(UTC).isoformat()
    baseline = _baseline_type()
    del baseline
    with tempfile.TemporaryDirectory(
        prefix="snapshot-ab-", dir=args.workspace
    ) as folder:
        workspace = Path(folder)
        monitor_root = None
        if args.monitor_galleries:
            monitor_root = workspace / "monitor"
            monitor_root.mkdir()
            for number in range(args.monitor_galleries):
                gallery = monitor_root / str(number)
                gallery.mkdir()
                (gallery / "galleryinfo.txt").write_bytes(b"completed marker\n" * 128)
        rows = []
        for size in args.bytes:
            paths = _fixture(workspace / f"files-{size}", max(args.counts), size)
            for count in args.counts:
                samples = []
                for repetition in range(args.repetitions):
                    order = ("historical_per_file", "batch_only", "bounded_page")
                    rotation = repetition % len(order)
                    order = order[rotation:] + order[:rotation]
                    pair = [
                        _one_variant(paths[:count], variant, monitor_root)
                        for variant in order
                    ]
                    if len({run["receipt_digest"] for run in pair}) != 1:
                        raise RuntimeError("A/B logical results differ")
                    samples.append(
                        {"repetition": repetition, "order": order, "runs": pair}
                    )
                medians = {
                    variant: statistics.median(
                        run["elapsed_ns"]
                        for sample in samples
                        for run in sample["runs"]
                        if run["variant"] == variant
                    )
                    for variant in ("historical_per_file", "batch_only", "bounded_page")
                }
                rows.append(
                    {
                        "files": count,
                        "bytes_per_file": size,
                        "total_bytes": count * size,
                        "samples": samples,
                        "median_elapsed_ns": medians,
                        "candidate_over_baseline": medians["bounded_page"]
                        / medians["historical_per_file"],
                        "candidate_over_batch_only": medians["bounded_page"]
                        / medians["batch_only"],
                    }
                )
        return {
            "status": "completed",
            "format_version": 1,
            "started_at": started,
            "finished_at": datetime.now(UTC).isoformat(),
            "command": sys.argv,
            "python": sys.version,
            "platform": platform.platform(),
            "sqlite": sqlite3.sqlite_version,
            "repetitions": args.repetitions,
            "monitor_galleries": args.monitor_galleries,
            "isolated": args.isolated,
            "baseline_sha256": _BASELINE_SHA256,
            "observer_baseline_sha256": _OBSERVER_BASELINE_SHA256,
            "source_sha256": {
                path.relative_to(_ROOT).as_posix(): sha256(
                    path.read_bytes()
                ).hexdigest()
                for path in (_ROOT / "src/h2hdb_ingest").rglob("*.py")
            },
            "probe_sha256": sha256(Path(__file__).read_bytes()).hexdigest(),
            "correctness": "Independent original bytes, FileContentReceipt, reopened capture and ordered digest all exact",
            "unexecuted": [],
            "skipped": [],
            "errors": [],
            "cases": rows,
            "limits": [
                "Local filesystem and warm OS cache; no cache flush or NAS speedup claim",
                "Capture wall excludes fixture, store create/close, exact-byte oracle and monitor shutdown",
                "All variants include actual source metrics and SQLite trace overhead; batch_only and bounded_page include identical snapshot phase metrics",
                "historical_per_file uses frozen per-file writer and frozen observer; batch_only uses bounded writer and frozen observer; bounded_page also removes only the unused expected-sha-absent observer hash",
                "Synthetic bounded byte payloads exercise snapshot I/O, not image decode, core persistence or publication",
                "Background monitor profile is zero-delay repeated metadata inventory, not a NAS contention model",
                "Logical read/write measurements do not measure physical device traffic or fsync separately",
            ],
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workspace", type=Path, default=Path(tempfile.gettempdir()))
    parser.add_argument("--counts", type=int, nargs="+", default=[127, 128, 129, 512])
    parser.add_argument("--bytes", type=int, nargs="+", default=[4096, 2097152])
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--monitor-galleries", type=int, default=0)
    parser.add_argument(
        "--isolated",
        action="store_true",
        help="operator confirms no competing benchmark/gate",
    )
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not (
        all(1 <= count <= 512 for count in args.counts)
        and all(8 <= size <= 4 * 1024 * 1024 for size in args.bytes)
        and 1 <= args.repetitions <= 7
        and 0 <= args.monitor_galleries <= 4096
        and 10 <= args.timeout <= 1800
    ):
        parser.error("fixture/repetition/timeout exceeds bounded experiment limits")
    if args.worker:
        print(json.dumps(_matrix(args)))
        return 0
    if args.output.exists():
        parser.error("output already exists")
    # The supervisor owns scratch even if the worker is killed at the deadline.
    with tempfile.TemporaryDirectory(
        prefix="snapshot-ab-owner-", dir=args.workspace
    ) as workspace:
        try:
            completed = subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    *sys.argv[1:],
                    "--worker",
                    "--workspace",
                    workspace,
                ],
                capture_output=True,
                text=True,
                check=False,
                timeout=args.timeout,
            )
        except subprocess.TimeoutExpired as error:
            report = {
                "status": "failed",
                "command": sys.argv,
                "error": "worker deadline exceeded",
                "timeout_seconds": error.timeout,
            }
        else:
            if completed.returncode:
                report = {
                    "status": "failed",
                    "command": sys.argv,
                    "returncode": completed.returncode,
                    "stderr": completed.stderr[-16000:],
                }
            else:
                report = json.loads(completed.stdout)
    report["supervisor_scratch_removed"] = not Path(workspace).exists()
    with args.output.open("x") as stream:
        json.dump(report, stream, indent=2, sort_keys=True)
        stream.write("\n")
    print(json.dumps({"status": report["status"], "output": str(args.output)}))
    return 0 if report["status"] == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
