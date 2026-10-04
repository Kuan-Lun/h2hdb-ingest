"""Owned synthetic probe databases; never accept a production server or catalog.

Pytest owns local disposable database lifetimes. The optional stdin envelope
keeps credentials out of process arguments and saved reports. Without an envelope
probes create their existing private SQLite fixture under the owned workspace.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

from h2hdb import CoreConfig, DatabaseConfig
from h2hdb.vnext_schema_provider import GeneratedVNextSchemaProvider


def _connect(config: CoreConfig) -> Any:
    import mysql.connector

    database = config.database
    return mysql.connector.connect(
        host=database.host,
        port=database.port,
        user=database.user,
        password=database.password,
        database=database.database,
    )


def validate_owned(config: CoreConfig, *, empty: bool = True) -> None:
    database = config.database
    if database.sql_type == "sqlite":
        path = Path(database.database)
        if not path.is_absolute() or (empty and path.exists()):
            raise ValueError("probe SQLite target must be a new absolute fixture path")
        return
    if (
        os.environ.get("H2HDB_TEST_MARIADB") != "1"
        or database.host not in {"127.0.0.1", "localhost", "::1"}
        or re.fullmatch(r"h2hdb_ingest_test_[a-f0-9]{12}", database.database) is None
        or database.user != "h2hdb_ingest"
    ):
        raise ValueError(
            "probe MariaDB must be an explicitly enabled local pytest-owned database"
        )
    if empty:
        with (
            closing(_connect(config)) as connection,
            closing(connection.cursor()) as cursor,
        ):
            cursor.execute("SHOW TABLES")
            if cursor.fetchall():
                raise ValueError("probe MariaDB target must be empty")


def parse_configs(value: str, *, count: int) -> tuple[CoreConfig, ...]:
    decoded = json.loads(value)
    if not isinstance(decoded, list) or len(decoded) != count:
        raise ValueError(
            f"probe requires exactly {count} owned database configurations"
        )
    configs = tuple(CoreConfig.model_validate(item) for item in decoded)
    if len({item.database.sql_type for item in configs}) != 1:
        raise ValueError("probe configurations must use the same backend")
    if len({item.database.database for item in configs}) != count:
        raise ValueError("probe configurations must name distinct owned databases")
    for config in configs:
        validate_owned(config)
    return configs


def default_config(root: Path, name: str = "catalog") -> CoreConfig:
    return CoreConfig(
        database=DatabaseConfig(
            sql_type="sqlite", database=str(root / f"{name}.sqlite3")
        )
    )


def clone_database(source: CoreConfig, target: CoreConfig) -> None:
    """Clone only a quiescent synthetic fixture, with bounded resident row buffers.

    The caller verifies the cloned authority with the public full READY audit.
    This is test setup, not a supported migration or a live database backup tool.
    """
    validate_owned(source, empty=False)
    validate_owned(target)
    if source.database.sql_type != target.database.sql_type:
        raise ValueError("fixture clone requires matching backends")
    if source.database.sql_type == "sqlite":
        with (
            closing(sqlite3.connect(source.database.database)) as original,
            closing(sqlite3.connect(target.database.database)) as copied,
        ):
            original.backup(copied)
        return
    if (source.database.host, source.database.port) != (
        target.database.host,
        target.database.port,
    ):
        raise ValueError("fixture clone requires the same owned local server")
    with closing(_connect(source)) as original, closing(_connect(target)) as copied:
        with closing(original.cursor()) as reader, closing(copied.cursor()) as writer:
            reader.execute("START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT")
            reader.execute("SHOW FULL TABLES")
            tables = reader.fetchall()
            if len(tables) > 4096 or any(
                kind not in {"BASE TABLE", "VIEW"} for _, kind in tables
            ):
                raise ValueError(
                    "fixture clone accepts only bounded table/view schemas"
                )
            writer.execute("SET FOREIGN_KEY_CHECKS=0")
            try:
                for name, kind in tables:
                    if kind != "BASE TABLE":
                        continue
                    if re.fullmatch(r"[a-z][a-z0-9_]*", name) is None:
                        raise ValueError(
                            "fixture clone rejected an unexpected table name"
                        )
                    reader.execute(f"SHOW CREATE TABLE `{name}`")
                    writer.execute(reader.fetchone()[1])
                views = {name for name, kind in tables if kind == "VIEW"}
                view_order = [
                    statement.creates.name
                    for schema_slice in GeneratedVNextSchemaProvider(
                        "mariadb"
                    ).definition.slices
                    for statement in schema_slice.statements
                    if statement.creates.name in views
                ]
                if set(view_order) != views:
                    raise ValueError("fixture clone contains non-generated views")
                for name in view_order:
                    reader.execute(f"SHOW CREATE VIEW `{name}`")
                    ddl = reader.fetchone()[1].replace(
                        f"`{source.database.database}`.",
                        f"`{target.database.database}`.",
                    )
                    writer.execute(ddl)
                row_count = 0
                for name, kind in tables:
                    if kind != "BASE TABLE":
                        continue
                    reader.execute(f"SELECT * FROM `{name}`")
                    placeholders = ",".join(["%s"] * len(reader.description))
                    while rows := reader.fetchmany(128):
                        row_count += len(rows)
                        if row_count > 1_000_000:
                            raise ValueError("fixture clone row budget exceeded")
                        writer.executemany(
                            f"INSERT INTO `{name}` VALUES ({placeholders})", rows
                        )
                copied.commit()
            finally:
                writer.execute("SET FOREIGN_KEY_CHECKS=1")
        original.rollback()
