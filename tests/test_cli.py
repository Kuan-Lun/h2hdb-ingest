from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import h2hdb_ingest.__main__ as cli
from h2hdb_ingest import IngestConfig, IngestPathsConfig
from h2hdb_ingest.config import CBZGrouping


def test_once_uses_the_coordinated_resident_path(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    events: list[object] = []
    cbz_arguments: dict[str, object] = {}
    scanner_arguments: dict[str, object] = {}
    service_arguments: dict[str, object] = {}
    config = SimpleNamespace(
        core=SimpleNamespace(database=SimpleNamespace(sql_type="sqlite")),
        paths=SimpleNamespace(
            download_path=tmp_path,
            cbz_path=tmp_path / "cbz",
            artifact_store_path=tmp_path / "artifacts",
            hash_workers=1,
            scan_batch_galleries=17,
            scan_batch_files=101,
            max_image_short_side=16,
            cbz_workers=2,
            stale_temp_age_seconds=3600,
            cbz_grouping=CBZGrouping.flat,
            cbz_sort="no",
        ),
        resident=object(),
        ensure_paths=lambda: events.append("ensure-paths"),
    )

    def log_event(message: str) -> None:
        del message

    database = SimpleNamespace(logger=SimpleNamespace(info=log_event))
    cache = object()
    scanner = object()
    stager = object()
    planner = object()
    staged_service = object()
    cbz_reconciler = object()

    class FakeResident:
        def __init__(self, **kwargs: object) -> None:
            assert kwargs["service"] is staged_service
            assert kwargs["coordinator"] is database
            assert kwargs["database_type"] == "sqlite"
            assert kwargs["event_logger"] == database.logger.info

        def initialize(self) -> None:
            events.append("initialize")

        def process_available(self, *, periodic_scan: bool) -> bool:
            events.append(("process", periodic_scan))
            return True

    def fake_cbz_reconciler(**kwargs: object) -> object:
        cbz_arguments.update(kwargs)
        events.append("cbz")
        return cbz_reconciler

    def fake_filesystem_scanner(*args: object, **kwargs: object) -> object:
        scanner_arguments["args"] = args
        scanner_arguments.update(kwargs)
        return scanner

    def fake_source_stager(**kwargs: object) -> object:
        assert kwargs == {
            "scanner": scanner,
            "coordinator": database,
            "hash_cache": cache,
        }
        return stager

    def fake_staged_service(**kwargs: object) -> object:
        service_arguments.update(kwargs)
        return staged_service

    monkeypatch.setattr(cli, "load_config", lambda path: config)
    monkeypatch.setattr(cli, "H2HDB", lambda core: database)
    monkeypatch.setattr(cli, "ResidentIngestor", FakeResident)
    monkeypatch.setattr(cli, "CBZReconciler", fake_cbz_reconciler)
    monkeypatch.setattr(cli, "CoreFileHashCache", lambda coordinator: cache)
    monkeypatch.setattr(cli, "FilesystemScanner", fake_filesystem_scanner)
    monkeypatch.setattr(cli, "FilesystemSourceStager", fake_source_stager)
    monkeypatch.setattr(cli, "StagedDeduplicationPlanner", lambda: planner)
    monkeypatch.setattr(cli, "StagedIngestService", fake_staged_service)
    monkeypatch.setattr(cli, "catalog_scope_key", lambda paths: "scope-key")

    cli.main(["--config", str(tmp_path / "ingest.json"), "--once"])

    assert events[0] == "ensure-paths"
    assert events[1] == "cbz"
    assert cbz_arguments["cbz_path"] == tmp_path / "cbz"
    assert cbz_arguments["artifact_store_path"] == tmp_path / "artifacts"
    assert cbz_arguments["max_image_short_side"] == 16
    assert cbz_arguments["workers"] == 2
    assert cbz_arguments["stale_temp_age_seconds"] == 3600
    assert cbz_arguments["event_logger"] == database.logger.info
    assert scanner_arguments["args"] == (tmp_path,)
    assert scanner_arguments["hash_workers"] == 1
    assert scanner_arguments["hash_cache"] is cache
    assert scanner_arguments["max_galleries"] == 17
    assert scanner_arguments["max_files"] == 101
    assert scanner_arguments["event_logger"] == database.logger.info
    assert service_arguments == {
        "source_stager": stager,
        "planner": planner,
        "catalog": database,
        "database_admin": database,
        "catalog_reader": database,
        "source_root": tmp_path,
        "scope_key": "scope-key",
        "cbz": cbz_reconciler,
        "event_logger": database.logger.info,
    }
    assert events[2:] == ["initialize", ("process", True)]


def test_cli_rejects_legacy_cbz_sort_modes_before_opening_database(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    config = SimpleNamespace(
        paths=SimpleNamespace(cbz_path=tmp_path / "cbz", cbz_sort="gid"),
        ensure_paths=lambda: None,
    )
    opened = False

    def open_database(core: object) -> object:
        nonlocal opened
        del core
        opened = True
        return object()

    monkeypatch.setattr(cli, "load_config", lambda path: config)
    monkeypatch.setattr(cli, "H2HDB", open_database)

    with pytest.raises(ValueError, match="supports only cbz_sort='no'"):
        cli.main(["--config", str(tmp_path / "ingest.json"), "--once"])

    assert not opened


def test_cli_rejects_empty_download_mount_before_opening_database(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    download_path = tmp_path / "download"
    download_path.mkdir()
    config = IngestConfig(paths=IngestPathsConfig(download_path=download_path))
    database_opened = False

    def open_database(core: object) -> object:
        nonlocal database_opened
        del core
        database_opened = True
        return object()

    monkeypatch.setattr(cli, "load_config", lambda path: config)
    monkeypatch.setattr(cli, "H2HDB", open_database)

    with pytest.raises(ValueError, match="gallery volume is mounted"):
        cli.main(["--config", str(tmp_path / "ingest.json"), "--once"])

    assert not database_opened
