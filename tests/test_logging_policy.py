"""Exercise the process logging policy with real SQLite and native image work."""

from __future__ import annotations

import json
import logging
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import cast
from zipfile import ZipFile

import pytest
from h2hdb import CoreConfig, DatabaseConfig, LoggerConfig
from PIL import Image

from h2hdb_ingest import IngestConfig, IngestPathsConfig, ResidentConfig
from h2hdb_ingest.runtime import build_runtime, configure_logging
from h2hdb_ingest.scratch import DiskScratch


@dataclass(frozen=True)
class _ProcessLogs:
    console: str
    file: str
    result: dict[str, object]

    def messages(self, level: str) -> list[str]:
        marker = f"[{level}] "
        return [
            line.split(marker, 1)[1]
            for line in self.file.splitlines()
            if marker in line
        ]


def _gallery(source: Path, gid: int, page_count: int, *, corrupt: bool = False) -> None:
    gallery = source / str(gid)
    gallery.mkdir(parents=True)
    for page in range(page_count):
        path = gallery / f"{page + 1:03d}.png"
        if corrupt:
            path.write_bytes(b"this is not a valid image")
        else:
            # Different source bytes prevent content deduplication from hiding work.
            color = ((gid * 37 + page * 17) % 256, page * 23, gid % 256)
            with Image.new("RGB", (24, 36), color) as image:
                image.save(path)
    (gallery / "galleryinfo.txt").write_text(
        "\n".join(
            (
                f"Title: Logging integration {gid}",
                "Upload Time: 2024-01-02 03:04",
                "Uploaded By: uploader",
                "Downloaded: 2024-02-03 04:05",
                f"Tags: artist:logging-{gid}, language:english",
                "Uploader's Comments",
                "Offline logging integration fixture",
                "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3",
            )
        ),
        encoding="utf-8",
    )


def _run_workload(root: Path, level: str, gallery_count: int, page_count: int) -> None:
    """Run as a fresh interpreter so pytest cannot install or intercept handlers."""
    source = root / "source"
    library = root / "library"
    library.mkdir()
    for relative in (
        "current",
        "current/acquisitions",
        "current/artwork",
        ".h2hdb-coordination",
    ):
        (library / relative).mkdir()
    for number in range(gallery_count):
        _gallery(source, 1001 + number, page_count)
    _gallery(source, 9001, 1, corrupt=True)
    log_file = root / "ingest.log"
    config = IngestConfig(
        core=CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite", database=str(root / "catalog.sqlite3")
            ),
            logger=LoggerConfig.model_validate({"level": level, "file": log_file}),
        ),
        paths=IngestPathsConfig(
            download_path=source, library_path=library, page_render_workers=2
        ),
        resident=ResidentConfig(
            publication_batch_galleries=10, lease_seconds=30, heartbeat_seconds=5
        ),
    )
    configure_logging(config)
    events: list[str] = []

    def event(message: str) -> None:
        events.append(message)
        logging.getLogger("h2hdb_ingest.runtime").info(message)

    # The real native decoder below provides libvips INFO evidence. Explicit
    # dependency diagnostics additionally exercise warning/error preservation.
    for name in ("pyvips", "mysql.connector"):
        dependency = logging.getLogger(name)
        dependency.debug("%s debug diagnostic", name)
        dependency.info("%s routine connection diagnostic", name)
        dependency.warning("%s warning retained", name)
        dependency.error("%s error retained", name)
    with DiskScratch(library) as scratch:
        with build_runtime(
            config, event_logger=event, temporary_cleanup=scratch.cleanup_page
        ) as runtime:
            runtime.database_admin.initialize()
            runtime.resident.initialize()
            assert runtime.resident.process_available(periodic_scan=True)
            revision = runtime.catalog.get_catalog_revision()
            assert revision.publication_count == gallery_count
            assert revision.artifact_count == gallery_count
            assert runtime.database_admin.check().state == "READY"
            assert runtime._progress is not None
            assert runtime._progress.current() is None
            before_idle = log_file.read_text(encoding="utf-8")
            runtime._progress._report_due()
            assert log_file.read_text(encoding="utf-8") == before_idle
    archives = sorted((library / "current" / "acquisitions").rglob("*.cbz"))
    actual_page_counts = []
    for path in archives:
        with ZipFile(path) as archive:
            actual_page_counts.append(
                sum(name.startswith("pages/") for name in archive.namelist())
            )
    thumbnails = tuple((library / "current" / "artwork").rglob("*.jpg"))
    assert len(thumbnails) == gallery_count
    logging.shutdown()
    print(
        json.dumps(
            {
                "publications": len(archives),
                "page_counts": actual_page_counts,
                "progress_interval_seconds": config.resident.progress_log_interval_seconds,
                "events": events,
            }
        )
    )


def _capture_process(root: Path, *arguments: str) -> _ProcessLogs:
    completed = subprocess.run(
        (
            sys.executable,
            str(Path(__file__).resolve()),
            str(root),
            *arguments,
        ),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    return _ProcessLogs(
        console=completed.stderr,
        file=(root / "ingest.log").read_text(encoding="utf-8"),
        result=cast(dict[str, object], json.loads(completed.stdout)),
    )


@pytest.fixture(scope="module")
def process_logs(tmp_path_factory: pytest.TempPathFactory) -> dict[str, _ProcessLogs]:
    logs: dict[str, _ProcessLogs] = {}
    for name, level, gallery_count, page_count in (
        ("small", "info", 1, 2),
        ("larger", "info", 3, 4),
        ("debug", "debug", 3, 4),
    ):
        root = tmp_path_factory.mktemp(f"logging-{name}")
        logs[name] = _capture_process(
            root, "workload", level, str(gallery_count), str(page_count)
        )
    return logs


def test_real_native_rendering_keeps_console_and_file_info_volume_per_batch(
    process_logs: dict[str, _ProcessLogs],
) -> None:
    small = process_logs["small"]
    larger = process_logs["larger"]
    assert small.result["page_counts"] == [2]
    assert larger.result["page_counts"] == [4, 4, 4]
    assert larger.result["publications"] == 3
    for logs in (small, larger):
        assert Counter(logs.console.splitlines()) == Counter(logs.file.splitlines())
        assert "VIPS:" not in logs.file
        assert "ingest_metric " not in logs.file
        assert "routine connection diagnostic" not in logs.file
        assert "debug diagnostic" not in logs.file
        assert logs.result["progress_interval_seconds"] == 3600
        info = logs.messages("INFO")
        assert any("Preparing gallery data for this batch" in line for line in info)
        assert any("Analyzing gallery selection" in line for line in info)
        assert any("Preparing and publishing the catalog" in line for line in info)
        assert any("Ingest work completed" in line for line in info)
    # Six times as many rendered pages in the same publication batch must not
    # produce an INFO entry per gallery, image, thumbnail or native operation.
    assert len(small.messages("INFO")) == len(larger.messages("INFO"))
    assert [line.split()[0] for line in small.messages("INFO")] == [
        line.split()[0] for line in larger.messages("INFO")
    ]


def test_debug_retains_native_and_per_artifact_metrics_without_polluting_events(
    process_logs: dict[str, _ProcessLogs],
) -> None:
    logs = process_logs["debug"]
    assert Counter(logs.console.splitlines()) == Counter(logs.file.splitlines())
    assert "VIPS:" in logs.file
    assert "thumbnailing source" in logs.file
    # Native INFO is enabled for troubleshooting. Wrapper DEBUG formats
    # Image.__repr__, which logs recursively and deadlocks parallel handlers.
    assert "pyvips debug diagnostic" not in logs.file
    assert "mysql.connector debug diagnostic" in logs.file
    metrics = [
        line for line in logs.messages("DEBUG") if line.startswith("ingest_metric ")
    ]
    assert (
        sum("scope=artifact operation=render_archive " in line for line in metrics) == 3
    )
    assert (
        sum("scope=artifact operation=render_presentation " in line for line in metrics)
        == 3
    )
    assert any("scope=publication " in line for line in metrics)
    events = cast(list[str], logs.result["events"])
    assert events
    assert not any(message.startswith("ingest_metric ") for message in events)


def test_dependency_and_real_rejection_diagnostics_remain_visible_with_context(
    process_logs: dict[str, _ProcessLogs],
) -> None:
    for logs in process_logs.values():
        for name in ("pyvips", "mysql.connector"):
            for level in ("WARNING", "ERROR"):
                message = next(
                    line
                    for line in logs.messages(level)
                    if line.startswith(f"{name} {level.lower()} retained")
                )
                assert f'logger="{name}"' in message
                assert 'source_root="' in message
                assert 'library_root="' in message
                assert 'database_backend="sqlite"' in message
                assert 'database_path="' in message
        rejected = [
            line
            for line in logs.messages("WARNING")
            if "gallery_image_rejected" in line
        ]
        assert len(rejected) == 1
        assert 'gallery_folder="' in rejected[0]
        assert '/source/9001"' in rejected[0]
        assert 'file="001.png"' in rejected[0]
        assert "gid=9001" in rejected[0]
        assert "other_galleries=continue" in rejected[0]


class _PreviousHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.closed_by_configuration = False

    def emit(self, record: logging.LogRecord) -> None:
        raise AssertionError(f"stale handler received {record.getMessage()}")

    def close(self) -> None:
        self.closed_by_configuration = True
        super().close()


def _run_severity_probe(root: Path, level: str) -> None:
    previous = _PreviousHandler()
    logging.basicConfig(level=logging.CRITICAL, handlers=[previous])
    log_file = root / "ingest.log"

    def config(configured_level: str) -> IngestConfig:
        return IngestConfig(
            core=CoreConfig(
                logger=LoggerConfig.model_validate(
                    {"level": configured_level, "file": log_file}
                )
            ),
            paths=IngestPathsConfig(download_path=root),
        )

    # Explicit process configuration must replace an existing root handler;
    # changing DEBUG to a stricter level must also reapply dependency thresholds.
    configure_logging(config("debug"))
    first_handlers = tuple(logging.getLogger().handlers)
    configure_logging(config(level))
    active_handlers = tuple(logging.getLogger().handlers)
    assert previous.closed_by_configuration
    assert len(first_handlers) == len(active_handlers) == 2
    assert not set(first_handlers).intersection(active_handlers)
    assert all(vars(handler).get("_closed") is True for handler in first_handlers)
    for name in (
        "h2hdb_ingest",
        "pyvips",
        "mysql.connector",
        "mysql.connector.authentication",
    ):
        for severity in (
            logging.DEBUG,
            logging.INFO,
            logging.WARNING,
            logging.ERROR,
            logging.CRITICAL,
        ):
            logging.getLogger(name).log(
                severity, "%s %s diagnostic", name, logging.getLevelName(severity)
            )
    logging.shutdown()
    print(json.dumps({"handlers": len(active_handlers)}))


@pytest.mark.parametrize("level", ("debug", "info", "warning", "error", "critical"))
def test_reconfiguration_replaces_handlers_and_applies_all_severity_thresholds(
    tmp_path: Path, level: str
) -> None:
    logs = _capture_process(tmp_path, "severity", level)
    assert logs.result["handlers"] == 2
    assert logs.console == logs.file
    configured = getattr(logging, level.upper())
    expected: list[str] = []
    for name in (
        "h2hdb_ingest",
        "pyvips",
        "mysql.connector",
        "mysql.connector.authentication",
    ):
        threshold = configured
        if name != "h2hdb_ingest" and configured > logging.DEBUG:
            threshold = max(threshold, logging.WARNING)
        if name == "pyvips":
            threshold = max(threshold, logging.INFO)
        for severity in (
            logging.DEBUG,
            logging.INFO,
            logging.WARNING,
            logging.ERROR,
            logging.CRITICAL,
        ):
            if severity >= threshold:
                expected.append(f"{name} {logging.getLevelName(severity)} diagnostic")
    messages = [
        line.split("] ", 1)[1].split(" | logger=", 1)[0]
        for line in logs.file.splitlines()
    ]
    assert messages == expected
    for line in logs.file.splitlines():
        if any(f"[{level}]" in line for level in ("WARNING", "ERROR", "CRITICAL")):
            assert 'source_root="' in line
            assert 'database_backend="mariadb"' in line
            assert 'database_host="localhost"' in line
            assert "database_port=3306" in line
            assert 'database_name="h2h"' in line
            assert line.count(" | logger=") == 1


if __name__ == "__main__":
    if sys.argv[2] == "workload":
        _run_workload(
            Path(sys.argv[1]), sys.argv[3], int(sys.argv[4]), int(sys.argv[5])
        )
    elif sys.argv[2] == "severity":
        _run_severity_probe(Path(sys.argv[1]), sys.argv[3])
    else:
        raise ValueError("unknown logging test harness mode")
