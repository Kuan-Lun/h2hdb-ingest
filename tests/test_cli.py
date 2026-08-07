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
    config = SimpleNamespace(
        core=SimpleNamespace(database=SimpleNamespace(sql_type="sqlite")),
        paths=SimpleNamespace(
            download_path=tmp_path,
            cbz_path=tmp_path / "cbz",
            artifact_store_path=tmp_path / "artifacts",
            hash_workers=1,
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

    class FakeResident:
        def __init__(self, **kwargs: object) -> None:
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
        return object()

    monkeypatch.setattr(cli, "load_config", lambda path: config)
    monkeypatch.setattr(cli, "H2HDB", lambda core: database)
    monkeypatch.setattr(cli, "ResidentIngestor", FakeResident)
    monkeypatch.setattr(cli, "CBZReconciler", fake_cbz_reconciler)

    cli.main(["--config", str(tmp_path / "ingest.json"), "--once"])

    assert events[0] == "ensure-paths"
    assert events[1] == "cbz"
    assert cbz_arguments["cbz_path"] == tmp_path / "cbz"
    assert cbz_arguments["artifact_store_path"] == tmp_path / "artifacts"
    assert cbz_arguments["max_image_short_side"] == 16
    assert cbz_arguments["workers"] == 2
    assert cbz_arguments["stale_temp_age_seconds"] == 3600
    assert cbz_arguments["event_logger"] == database.logger.info
    assert events[2:] == ["initialize", ("process", True)]


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
