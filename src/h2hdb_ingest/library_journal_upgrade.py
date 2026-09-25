"""One-off, explicitly offline conversion of an exact v4 library journal.

Normal runtime never calls this module and admits only the current exact shape.
The old shape is checksum pinned; SQLite commits its index and version control
together, without an intermediate marker, fallback reader or migration ledger.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from hashlib import sha256
from pathlib import Path
from uuid import UUID

from ._library_journal import FORMAT_VERSION, SCHEMA, require_exact_schema
from ._library_layout import validate_precreated_library_layout
from ._library_maintenance import locked_library

_OLD_SCHEMA_SHA256 = "bb111a077fea7b0f44c7420ec17ade51905f6aed31cd7ccb5fa0f244baf3ab0d"
_INDEX_SQL = (
    "CREATE INDEX IF NOT EXISTS protection_cleanup_eligible_idx\n"
    "    ON protection_tokens(token)\n"
    "    WHERE state = 'RELEASED' AND staging_leaf IS NOT NULL;\n"
)
_INVENTORY = (
    "SELECT type, name, tbl_name, sql FROM sqlite_master "
    "WHERE name NOT GLOB 'sqlite_*' ORDER BY type, name"
)
_STATE_COLUMNS = (
    "singleton, format_version, current_revision, current_receipt_id, "
    "pending_revision, pending_receipt_id, phase, last_cursor"
)


def _old_schema() -> str:
    """Admit only the reviewed old authoring surface, never a guessed variant."""
    if FORMAT_VERSION != 5 or SCHEMA.count(_INDEX_SQL) != 1:
        raise RuntimeError("journal converter requires the exact v5 software")
    old = (
        SCHEMA.replace(_INDEX_SQL, "")
        .replace("format_version = 5", "format_version = 4")
        .replace("VALUES (1, 5, 'IDLE')", "VALUES (1, 4, 'IDLE')")
    )
    if sha256(old.encode()).hexdigest() != _OLD_SCHEMA_SHA256:
        raise RuntimeError("journal converter old schema checksum differs")
    return old


def _admit(connection: sqlite3.Connection, old_schema: str) -> int:
    with sqlite3.connect(":memory:") as reference:
        reference.executescript(old_schema)
        old_shape = reference.execute(_INVENTORY).fetchmany(128)
    if connection.execute(_INVENTORY).fetchmany(128) == old_shape:
        if connection.execute(
            "SELECT singleton, format_version FROM library_state LIMIT 2"
        ).fetchmany(2) != [(1, 4)]:
            raise RuntimeError("unsupported library journal v4 control")
        return 4
    require_exact_schema(connection)
    return 5


def _identity(connection: sqlite3.Connection) -> bytes:
    rows = connection.execute(
        "SELECT singleton, storage_instance_uuid FROM library_storage_identity LIMIT 2"
    ).fetchmany(2)
    if (
        len(rows) != 1
        or rows[0][0] != 1
        or not isinstance(rows[0][1], bytes)
        or len(rows[0][1]) != 16
        or UUID(bytes=rows[0][1]).version != 4
    ):
        raise RuntimeError("library storage identity must be one UUIDv4")
    return bytes(rows[0][1])


def _integrity(connection: sqlite3.Connection) -> None:
    if connection.execute("PRAGMA integrity_check").fetchmany(2) != [("ok",)]:
        raise RuntimeError("library journal SQLite integrity check failed")


def upgrade_library_journal(
    root: Path, *, checkpoint: Callable[[str], None] | None = None
) -> str:
    """Preserve all authorities and bytes; a stopped v4/v5 library is required."""
    old_schema = _old_schema()  # Reject stale software before touching the library.
    validate_precreated_library_layout(root, durable=False)

    def notify(name: str) -> None:
        if checkpoint is not None:
            checkpoint(name)

    with locked_library(root) as (_files, connection, revalidate):
        revalidate()
        connection.execute("BEGIN IMMEDIATE")
        try:
            version = _admit(connection, old_schema)
            identity = _identity(connection)
            notify("integrity_started")
            _integrity(connection)
            notify("journal_validated")
            if version == 5:
                connection.rollback()
                result = "already_upgraded"
            else:
                before = connection.execute(
                    f"SELECT {_STATE_COLUMNS} FROM library_state"
                ).fetchone()
                assert before is not None  # Exact singleton admission above.
                connection.execute(
                    "ALTER TABLE library_state RENAME TO upgrade_v4_library_state"
                )
                # Create under its final name: renaming a temporary table would
                # rewrite sqlite_master SQL and differ from the canonical schema.
                connection.execute(SCHEMA.split(";", 1)[0])
                connection.execute(
                    f"INSERT INTO library_state ({_STATE_COLUMNS}) "
                    "SELECT singleton, 5, current_revision, current_receipt_id, "
                    "pending_revision, pending_receipt_id, phase, last_cursor "
                    "FROM upgrade_v4_library_state"
                )
                connection.execute("DROP TABLE upgrade_v4_library_state")
                notify("control_recreated")
                notify("index_started")
                connection.execute(_INDEX_SQL)
                notify("index_created")
                require_exact_schema(connection)
                after = connection.execute(
                    f"SELECT {_STATE_COLUMNS} FROM library_state"
                ).fetchone()
                if (
                    after != (before[0], 5, *before[2:])
                    or _identity(connection) != identity
                ):
                    raise RuntimeError("journal conversion changed retained authority")
                notify("target_integrity_started")
                _integrity(connection)
                notify("target_validated")
                revalidate()
                connection.commit()
                result = "upgraded"
                notify("transaction_committed")
        except BaseException:
            connection.rollback()
            raise
    # The maintenance guard has fsynced and revalidated the journal and parent.
    notify("durable_complete")
    return result
