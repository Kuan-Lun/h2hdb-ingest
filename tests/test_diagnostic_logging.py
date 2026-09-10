from __future__ import annotations

import logging
from pathlib import Path

import pytest
from h2hdb import CoreConfig, DatabaseConfig

from h2hdb_ingest import IngestConfig, IngestPathsConfig
from h2hdb_ingest._diagnostic_logging import DiagnosticFormatter, command_diagnostics


def _config(root: Path, *, backend: str = "sqlite") -> IngestConfig:
    return IngestConfig(
        core=CoreConfig(
            database=DatabaseConfig(
                sql_type=backend,
                host="database.internal",
                port=3307,
                database="catalog"
                if backend == "mariadb"
                else str(root / "catalog.db"),
                user="private-user",
                password="private-password",
            )
        ),
        paths=IngestPathsConfig(
            download_path=root / "來源\n\u202e資料", library_path=root / "library"
        ),
    )


@pytest.mark.parametrize("backend", ["sqlite", "mariadb"])
def test_warning_targets_are_safe_and_identical_across_handlers(
    tmp_path: Path, backend: str
) -> None:
    config = _config(tmp_path, backend=backend)
    record = logging.LogRecord(
        "mysql.connector.authentication",
        logging.WARNING,
        __file__,
        1,
        "connection %s",
        ("interrupted",),
        None,
    )
    console = DiagnosticFormatter(config)
    file = DiagnosticFormatter(config)
    first = console.format(record)
    assert first == file.format(record) == console.format(record)
    assert record.getMessage() == "connection interrupted"
    assert "message" not in record.__dict__
    assert first.count("source_root=") == 1
    assert "來源\\n\\u202e資料" in first
    assert "\n" not in first
    assert "\u202e" not in first
    assert 'logger="mysql.connector.authentication"' in first
    assert f'library_root="{tmp_path}/library"' in first
    assert "private-user" not in first
    assert "private-password" not in first
    if backend == "mariadb":
        assert 'database_host="database.internal" database_port=3307' in first
        assert 'database_name="catalog"' in first
    else:
        assert f'database_path="{tmp_path}/catalog.db"' in first
        assert "database.internal" not in first


def test_error_keeps_traceback_and_puts_target_on_first_line(tmp_path: Path) -> None:
    failure = OSError("cannot clean library")
    record = logging.LogRecord(
        "h2hdb_ingest.resident",
        logging.ERROR,
        __file__,
        1,
        "Operation failed: operation=library_cleanup",
        (),
        (OSError, failure, None),
    )
    text = DiagnosticFormatter(_config(tmp_path)).format(record)
    first, rest = text.split("\n", 1)
    assert "operation=library_cleanup" in first
    assert f'library_root="{tmp_path}/library"' in first
    assert "OSError: cannot clean library" in rest
    assert record.exc_text is None


def test_fatal_command_keeps_exception_identity_and_configuration_location(
    tmp_path: Path,
) -> None:
    failure = RuntimeError("database not ready")
    with pytest.raises(RuntimeError) as caught:
        with command_diagnostics(tmp_path / "ingest.json", _config(tmp_path)):
            raise failure
    assert caught.value is failure
    note = caught.value.__notes__[-1]
    assert f'config_file="{tmp_path}/ingest.json"' in note
    assert f'database_path="{tmp_path}/catalog.db"' in note
    assert "private-password" not in note


@pytest.mark.parametrize("failure_type", [KeyboardInterrupt, SystemExit])
def test_command_diagnostics_preserves_cancellation(
    tmp_path: Path,
    failure_type: type[BaseException],
) -> None:
    failure = failure_type("stop")
    with pytest.raises(failure_type) as caught:
        with command_diagnostics(tmp_path / "ingest.json", _config(tmp_path)):
            raise failure
    assert caught.value is failure
    assert not getattr(failure, "__notes__", ())
