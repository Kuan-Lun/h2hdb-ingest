"""The delivered executable must work without installed ingest dependencies."""

from __future__ import annotations

import sqlite3
import subprocess
import sys
import zipfile
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pytest

from h2hdb_ingest._storage_paths import STORAGE_OBJECT_CODEC, storage_path
from h2hdb_ingest.journal_upgrade import V3_SCHEMA, upgrade_v3


def test_standalone_tool_upgrades_moved_v3_without_site_packages(
    tmp_path: Path,
) -> None:
    root = tmp_path / "moved library"
    for directory in (
        "current/acquisitions",
        "current/artwork",
        ".h2hdb-coordination",
        ".h2hdb-state/staging",
        ".h2hdb-state/quarantine",
        ".h2hdb-state/journal",
        ".h2hdb-state/locks",
    ):
        (root / directory).mkdir(parents=True, exist_ok=True)
    (root / ".h2hdb-coordination/publication.lock").touch()
    (root / ".h2hdb-state/locks/state.lock").touch()
    key = storage_path(7, "acquisition")
    target = root / "current" / key
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = b"verified existing acquisition bytes"
    target.write_bytes(payload)
    original = target.stat()
    identity = uuid4().bytes
    database = root / ".h2hdb-state/journal/library-activation.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.executescript(V3_SCHEMA)
        connection.execute(
            "INSERT INTO library_storage_identity VALUES (1, ?)", (identity,)
        )
        connection.execute(
            "UPDATE library_state SET current_revision=1, current_receipt_id=?",
            (b"r" * 16,),
        )
        connection.execute(
            "INSERT INTO current_entries VALUES (?, 'acquisition', ?, ?, 7, ?, ?, ?, ?, ?, ?, ?)",
            (
                sha256(b"publication").digest(),
                key,
                STORAGE_OBJECT_CODEC,
                sha256(payload).digest(),
                len(payload),
                "2026-01-01T00:00:00+00:00",
                original.st_dev.to_bytes(8, "big"),
                original.st_ino.to_bytes(8, "big"),
                original.st_mtime_ns,
                original.st_ctime_ns,
            ),
        )
    replacement = target.with_suffix(".replacement")
    replacement.write_bytes(payload)
    replacement.replace(target)
    assert target.stat().st_ino != original.st_ino

    tool = tmp_path / "h2hdb-library-relocate.pyz"
    repository = Path(__file__).resolve().parents[1]
    subprocess.run(
        [
            sys.executable,
            str(repository / "scripts/build-relocation-tool.py"),
            "--output",
            str(tool),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    with zipfile.ZipFile(tool) as archive:
        assert "h2hdb_ingest/library.py" not in archive.namelist()
        assert "h2hdb_ingest/artifact.py" not in archive.namelist()
    command = [
        sys.executable,
        "-I",
        "-S",
        str(tool),
        "--library",
        str(root),
        "--upgrade-v3",
    ]
    result = subprocess.run(
        command, cwd=tmp_path, check=True, capture_output=True, text=True, timeout=30
    )
    assert "Library relocation complete" in result.stdout
    assert target.read_bytes() == payload
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT format_version FROM library_state"
        ).fetchone() == (4,)
        assert connection.execute(
            "SELECT storage_instance_uuid FROM library_storage_identity"
        ).fetchone() == (identity,)
        assert connection.execute("SELECT inode FROM current_entries").fetchone() == (
            target.stat().st_ino.to_bytes(8, "big"),
        )


def test_v3_conversion_rolls_back_together_with_session_creation() -> None:
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(V3_SCHEMA)
        connection.execute(
            "INSERT INTO library_storage_identity VALUES (1, ?)", (uuid4().bytes,)
        )
        connection.commit()
        connection.execute("BEGIN IMMEDIATE")
        assert upgrade_v3(connection)
        connection.rollback()
        assert connection.execute(
            "SELECT format_version FROM library_state"
        ).fetchone() == (3,)
        assert (
            connection.execute(
                "SELECT name FROM sqlite_master WHERE name='library_relocation_session'"
            ).fetchone()
            is None
        )
        with pytest.raises(RuntimeError, match="transaction"):
            upgrade_v3(connection)
    finally:
        connection.close()
