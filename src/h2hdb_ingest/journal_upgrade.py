"""One-time operator conversion of the frozen v3 journal to the current format.

Normal ingest never imports this module or accepts the v3 format. The caller
owns the exclusive maintenance locks and commits the format conversion together
with the initial relocation session, before changing any artifact authority.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator

from ._library_journal import FORMAT_VERSION, SCHEMA, require_exact_schema

# Frozen source contract of the released 0.21.2 journal; not a runtime fallback.
V3_SCHEMA = r"""
CREATE TABLE IF NOT EXISTS library_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    format_version INTEGER NOT NULL CHECK (format_version = 3),
    current_revision INTEGER NULL,
    current_receipt_id BLOB NULL,
    pending_revision INTEGER NULL,
    pending_receipt_id BLOB NULL,
    phase TEXT NOT NULL CHECK (
        phase IN ('IDLE', 'OPEN', 'SEALED', 'ACTIVATING', 'READY')
    ),
    last_cursor BLOB NULL CHECK (
        last_cursor IS NULL OR length(last_cursor) = 33
    )
);
INSERT OR IGNORE INTO library_state
    (singleton, format_version, phase) VALUES (1, 3, 'IDLE');
CREATE TABLE IF NOT EXISTS library_storage_identity (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    storage_instance_uuid BLOB NOT NULL CHECK (length(storage_instance_uuid) = 16)
);
CREATE TABLE IF NOT EXISTS protection_tokens (
    token BLOB PRIMARY KEY CHECK (length(token) = 32),
    storage_codec TEXT NOT NULL,
    storage_path TEXT NOT NULL,
    object_sha256 BLOB NOT NULL CHECK (length(object_sha256) = 32),
    size_bytes INTEGER NOT NULL CHECK (size_bytes > 0),
    published_modified_at TEXT NULL CHECK (
        published_modified_at IS NULL OR length(published_modified_at) > 0
    ),
    state TEXT NOT NULL CHECK (
        state IN ('WRITING', 'STAGED', 'INSTALLED', 'RELEASED')
    ),
    staging_leaf TEXT NULL,
    device BLOB NULL,
    inode BLOB NULL,
    modified_ns INTEGER NULL,
    changed_ns INTEGER NULL
);
CREATE INDEX IF NOT EXISTS protection_object_idx ON protection_tokens (
    storage_codec, storage_path, object_sha256, size_bytes,
    published_modified_at, state
);
CREATE UNIQUE INDEX IF NOT EXISTS protection_one_active_stage_idx
    ON protection_tokens(storage_path)
    WHERE state IN ('WRITING', 'STAGED');
CREATE TABLE IF NOT EXISTS current_entries (
    publication_key BLOB NOT NULL CHECK (length(publication_key) = 32),
    resource_kind TEXT NOT NULL CHECK (
        resource_kind IN ('acquisition', 'thumbnail')
    ),
    storage_path TEXT NOT NULL UNIQUE,
    storage_codec TEXT NOT NULL,
    gid INTEGER NOT NULL,
    object_sha256 BLOB NOT NULL CHECK (length(object_sha256) = 32),
    size_bytes INTEGER NOT NULL CHECK (size_bytes > 0),
    published_modified_at TEXT NOT NULL CHECK (length(published_modified_at) > 0),
    device BLOB NOT NULL CHECK (length(device) = 8),
    inode BLOB NOT NULL CHECK (length(inode) = 8),
    modified_ns INTEGER NOT NULL,
    changed_ns INTEGER NOT NULL,
    PRIMARY KEY (publication_key, resource_kind),
    UNIQUE (gid, resource_kind)
);
CREATE TABLE IF NOT EXISTS pending_entries (
    activation_revision INTEGER NOT NULL,
    publication_key BLOB NOT NULL CHECK (length(publication_key) = 32),
    gid INTEGER NOT NULL,
    resource_kind TEXT NOT NULL CHECK (
        resource_kind IN ('acquisition', 'thumbnail')
    ),
    storage_codec TEXT NOT NULL,
    storage_path TEXT NOT NULL,
    object_sha256 BLOB NOT NULL CHECK (length(object_sha256) = 32),
    size_bytes INTEGER NOT NULL CHECK (size_bytes > 0),
    published_modified_at TEXT NOT NULL CHECK (length(published_modified_at) > 0),
    operation_started INTEGER NOT NULL CHECK (operation_started IN (0, 1)),
    activated INTEGER NOT NULL CHECK (activated IN (0, 1)),
    device BLOB NULL,
    inode BLOB NULL,
    modified_ns INTEGER NULL,
    changed_ns INTEGER NULL,
    PRIMARY KEY (activation_revision, publication_key, resource_kind),
    UNIQUE (activation_revision, gid, resource_kind),
    UNIQUE (activation_revision, storage_path)
);
CREATE INDEX IF NOT EXISTS pending_entries_activation_idx
    ON pending_entries(
        activation_revision, activated, publication_key, resource_kind
    );
CREATE TABLE IF NOT EXISTS pending_removals (
    activation_revision INTEGER NOT NULL,
    publication_key BLOB NOT NULL CHECK (length(publication_key) = 32),
    resource_kind TEXT NOT NULL CHECK (
        resource_kind IN ('acquisition', 'thumbnail')
    ),
    storage_codec TEXT NOT NULL,
    storage_path TEXT NOT NULL,
    object_sha256 BLOB NOT NULL CHECK (length(object_sha256) = 32),
    size_bytes INTEGER NOT NULL CHECK (size_bytes > 0),
    device BLOB NOT NULL CHECK (length(device) = 8),
    inode BLOB NOT NULL CHECK (length(inode) = 8),
    modified_ns INTEGER NOT NULL,
    changed_ns INTEGER NOT NULL,
    operation_started INTEGER NOT NULL CHECK (operation_started IN (0, 1)),
    PRIMARY KEY (activation_revision, publication_key, resource_kind),
    UNIQUE (activation_revision, storage_path)
);
"""


def _statements(source: str) -> Iterator[str]:
    statement = ""
    for line in source.splitlines(keepends=True):
        statement += line
        if sqlite3.complete_statement(statement):
            yield statement
            statement = ""
    if statement.strip():
        raise RuntimeError("incomplete journal schema statement")


def _require_v3_schema(connection: sqlite3.Connection) -> None:
    reference = sqlite3.connect(":memory:")
    try:
        reference.executescript(V3_SCHEMA)
        query = (
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        )
        expected = reference.execute(query).fetchall()
        actual = connection.execute(query).fetchmany(len(expected) + 1)
        if actual != expected:
            raise RuntimeError(
                "v3 journal schema is foreign or drifted; preserved unchanged"
            )
    finally:
        reference.close()


def upgrade_v3(connection: sqlite3.Connection) -> bool:
    """Convert exactly v3 inside the caller's relocation-start transaction."""
    if not connection.in_transaction:
        raise RuntimeError("journal upgrade requires the relocation transaction")
    row = connection.execute(
        "SELECT format_version FROM library_state WHERE singleton = 1"
    ).fetchone()
    if row == (FORMAT_VERSION,):
        require_exact_schema(connection)
        return False
    if row != (3,):
        raise RuntimeError("only the released v3 journal can be upgraded by this tool")
    _require_v3_schema(connection)
    state = connection.execute(
        "SELECT singleton, current_revision, current_receipt_id, pending_revision, "
        "pending_receipt_id, phase, last_cursor FROM library_state"
    ).fetchmany(2)
    if len(state) != 1 or state[0][0] != 1:
        raise RuntimeError("v3 journal publication state is not singular")
    connection.execute("DROP TABLE library_state")
    for statement in _statements(SCHEMA):
        connection.execute(statement)
    connection.execute(
        "UPDATE library_state SET current_revision = ?, current_receipt_id = ?, "
        "pending_revision = ?, pending_receipt_id = ?, phase = ?, last_cursor = ? "
        "WHERE singleton = 1",
        tuple(state[0][1:]),
    )
    require_exact_schema(connection)
    return True
