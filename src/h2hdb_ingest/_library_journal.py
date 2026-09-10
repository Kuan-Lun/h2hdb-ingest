"""Single authoring surface for the private activation journal, format 4."""

from __future__ import annotations

import sqlite3
from uuid import UUID

FORMAT_VERSION = 4

SCHEMA = """
CREATE TABLE IF NOT EXISTS library_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    format_version INTEGER NOT NULL CHECK (format_version = 4),
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
    (singleton, format_version, phase) VALUES (1, 4, 'IDLE');
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
CREATE INDEX IF NOT EXISTS protection_relocation_path_idx
    ON protection_tokens(storage_path, token);
CREATE INDEX IF NOT EXISTS pending_relocation_path_idx
    ON pending_entries(storage_path, activation_revision);
CREATE INDEX IF NOT EXISTS removal_relocation_path_idx
    ON pending_removals(storage_path, activation_revision);
CREATE TABLE IF NOT EXISTS library_relocation_session (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    session_id BLOB NOT NULL CHECK (length(session_id) = 16),
    storage_instance_uuid BLOB NOT NULL CHECK (length(storage_instance_uuid) = 16),
    root_device BLOB NOT NULL CHECK (length(root_device) = 8),
    root_inode BLOB NOT NULL CHECK (length(root_inode) = 8),
    phase TEXT NOT NULL CHECK (
        phase IN ('CLEANUP', 'SCAN', 'TOKENS', 'AUDIT', 'FINALIZING', 'COMPLETE')
    ),
    cursor TEXT NOT NULL,
    verified_files INTEGER NOT NULL CHECK (verified_files >= 0),
    original_marker BLOB NULL
);
CREATE TABLE IF NOT EXISTS library_relocation_files (
    session_id BLOB NOT NULL CHECK (length(session_id) = 16),
    relative_path TEXT NOT NULL,
    object_sha256 BLOB NULL CHECK (
        object_sha256 IS NULL OR length(object_sha256) = 32
    ),
    size_bytes INTEGER NULL CHECK (size_bytes IS NULL OR size_bytes >= 0),
    device BLOB NULL CHECK (device IS NULL OR length(device) = 8),
    inode BLOB NULL CHECK (inode IS NULL OR length(inode) = 8),
    modified_ns INTEGER NULL,
    changed_ns INTEGER NULL,
    old_device BLOB NULL,
    old_inode BLOB NULL,
    old_modified_ns INTEGER NULL,
    old_changed_ns INTEGER NULL,
    PRIMARY KEY (session_id, relative_path)
);
"""


def create_fresh_journal(
    connection: sqlite3.Connection, storage_instance_uuid: bytes
) -> None:
    """Create one journal and its identity in an atomic transaction."""
    identity = UUID(bytes=storage_instance_uuid)
    if identity.version != 4:
        raise ValueError("library storage identity must be UUIDv4")
    connection.executescript(
        "BEGIN IMMEDIATE;\n" + SCHEMA + "\nINSERT INTO library_storage_identity "
        "(singleton, storage_instance_uuid) VALUES "
        f"(1, X'{identity.hex}');\nCOMMIT;\n"
    )


def require_exact_schema(connection: sqlite3.Connection) -> None:
    """Reject unknown tables, indexes, triggers, views and altered SQL."""
    query = (
        "SELECT type, name, tbl_name, sql FROM sqlite_master "
        "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
    )
    reference = sqlite3.connect(":memory:")
    try:
        create_fresh_journal(
            reference, bytes.fromhex("00000000000040008000000000000001")
        )
        expected = reference.execute(query).fetchmany(128)
        actual = connection.execute(query).fetchmany(128)
        if actual != expected:
            raise RuntimeError(
                "unsupported library activation journal shape (expected v4)"
            )
    finally:
        reference.close()
    row = connection.execute(
        "SELECT singleton, format_version FROM library_state LIMIT 2"
    ).fetchmany(2)
    if row != [(1, FORMAT_VERSION)]:
        raise RuntimeError(
            "unsupported library activation journal format (expected v4)"
        )


def require_no_relocation(connection: sqlite3.Connection) -> None:
    """Normal runtime must not modify a maintenance session after a crash."""
    row = connection.execute(
        "SELECT phase FROM library_relocation_session WHERE singleton = 1"
    ).fetchone()
    if row is not None and row[0] != "COMPLETE":
        raise RuntimeError(
            "library relocation is unfinished; rerun the library relocation tool"
        )
