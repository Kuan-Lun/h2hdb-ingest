"""Reject unsafe probe targets before any external database connection."""

from __future__ import annotations

import json
import runpy
from pathlib import Path
from typing import Any

import pytest
from h2hdb import CoreConfig, DatabaseConfig


@pytest.fixture
def database_probe() -> dict[str, Any]:
    return runpy.run_path(
        str(Path(__file__).parents[1] / "scripts" / "_probe_database.py")
    )


def test_probe_stdin_requires_distinct_same_backend_configs(
    tmp_path: Path, database_probe: dict[str, Any]
) -> None:
    core = CoreConfig(
        database=DatabaseConfig(
            sql_type="sqlite", database=str(tmp_path / "new.sqlite3")
        )
    )
    raw = core.model_dump(mode="json")
    assert database_probe["parse_configs"](json.dumps([raw]), count=1) == (core,)
    with pytest.raises(ValueError, match="exactly 2"):
        database_probe["parse_configs"](json.dumps([raw]), count=2)
    with pytest.raises(ValueError, match="distinct"):
        database_probe["parse_configs"](json.dumps([raw, raw]), count=2)


@pytest.mark.parametrize("host", ["192.168.0.169", "database.example", "127.0.0.2"])
def test_probe_rejects_nonlocal_server_before_connect(
    database_probe: dict[str, Any], monkeypatch: pytest.MonkeyPatch, host: str
) -> None:
    monkeypatch.setenv("H2HDB_TEST_MARIADB", "1")
    core = CoreConfig(
        database=DatabaseConfig(
            sql_type="mariadb",
            host=host,
            user="h2hdb_ingest",
            database="h2hdb_ingest_test_abcdef123456",
        )
    )
    with pytest.raises(ValueError, match="local pytest-owned"):
        database_probe["validate_owned"](core)


@pytest.mark.parametrize(
    "enabled,name,user",
    [
        ("0", "h2hdb_ingest_test_abcdef123456", "h2hdb_ingest"),
        ("1", "h2hdb", "h2hdb_ingest"),
        ("1", "h2hdb_ingest_test_abcdef123456", "root"),
    ],
)
def test_probe_rejects_unowned_server_config_before_connect(
    database_probe: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    enabled: str,
    name: str,
    user: str,
) -> None:
    monkeypatch.setenv("H2HDB_TEST_MARIADB", enabled)
    core = CoreConfig(
        database=DatabaseConfig(
            sql_type="mariadb", host="127.0.0.1", user=user, database=name
        )
    )
    with pytest.raises(ValueError, match="local pytest-owned"):
        database_probe["validate_owned"](core)


def test_probe_rejects_existing_sqlite_target_without_mutating_it(
    tmp_path: Path, database_probe: dict[str, Any]
) -> None:
    path = tmp_path / "keep.sqlite3"
    path.write_bytes(b"must remain unchanged")
    core = CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))
    with pytest.raises(ValueError, match="new absolute"):
        database_probe["validate_owned"](core)
    assert path.read_bytes() == b"must remain unchanged"
