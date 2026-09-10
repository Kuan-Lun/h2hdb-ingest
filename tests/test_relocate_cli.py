from __future__ import annotations

import sqlite3
import subprocess
import sys
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pytest

import h2hdb_ingest.relocate as cli
from h2hdb_ingest._library_journal import create_fresh_journal
from h2hdb_ingest._storage_paths import STORAGE_OBJECT_CODEC, storage_path


def test_relocation_cli_verifies_current_journal_and_preserves_artifact(
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
    target.parent.mkdir(parents=True)
    payload = b"verified existing acquisition bytes"
    target.write_bytes(payload)
    original = target.stat()
    identity = uuid4().bytes
    database = root / ".h2hdb-state/journal/library-activation.sqlite3"
    with sqlite3.connect(database) as connection:
        create_fresh_journal(connection, identity)
        connection.execute(
            "UPDATE library_state SET current_revision=1, current_receipt_id=?",
            (b"r" * 16,),
        )
        publication_key = sha256(
            b"h2hdb-vnext-publication-key\0"
            + (1).to_bytes(4, "big")
            + (7).to_bytes(8, "big")
        ).digest()
        connection.execute(
            "INSERT INTO current_entries VALUES "
            "(?, 'acquisition', ?, ?, 7, ?, ?, ?, ?, ?, ?, ?)",
            (
                publication_key,
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

    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-m",
            "h2hdb_ingest.relocate",
            "--library",
            str(root),
            "--batch-size",
            "1",
        ],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert "Library relocation complete" in result.stdout
    assert "verified_files=1" in result.stdout
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


def test_relocation_cli_rejects_removed_upgrade_option_before_library_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = tmp_path / "must-not-be-opened"

    def reject_library_io(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("argument rejection must precede library I/O")

    monkeypatch.setattr(cli, "relocate_library", reject_library_io)

    with pytest.raises(SystemExit) as failure:
        cli.main(["--library", str(root), "--upgrade-v3"])

    assert failure.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "unrecognized arguments: --upgrade-v3" in captured.err
    assert not root.exists()
