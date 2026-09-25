from __future__ import annotations

import fcntl
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path
from time import monotonic, sleep
from typing import Literal

import pytest
import test_library_relocation as fixtures

from h2hdb_ingest._library_journal import require_exact_schema
from h2hdb_ingest.library_journal_upgrade import (
    _admit,
    _old_schema,
    upgrade_library_journal,
)
from h2hdb_ingest.library_relocation import relocate_library


def _create_v4(connection: sqlite3.Connection, identity: bytes) -> None:
    # Independent historical DDL from committed v4 source (2d737cf).
    connection.executescript(
        (Path(__file__).parent / "fixtures/library-journal-v4.sql").read_text()
    )
    connection.execute(
        "INSERT INTO library_storage_identity VALUES (1, ?)", (identity,)
    )
    connection.commit()


@pytest.fixture
def old_library(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(fixtures, "create_fresh_journal", _create_v4)
    root = tmp_path / "library"
    fixtures._library_root(root, count=2, thumbnail=True)
    return root


def _facts(root: Path) -> dict[str, list[tuple[object, ...]]]:
    with sqlite3.connect(fixtures._journal(root)) as connection:
        tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND name NOT GLOB 'sqlite_*' ORDER BY name"
        ).fetchall()
        return {
            name: sorted(
                connection.execute(f'SELECT * FROM "{name}"').fetchall(), key=repr
            )
            for (name,) in tables
        }


def _files(root: Path) -> dict[str, tuple[bytes, int, int]]:
    return {
        path.relative_to(root).as_posix(): (
            path.read_bytes(),
            path.stat().st_dev,
            path.stat().st_ino,
        )
        for path in root.rglob("*")
        if path.is_file() and ".h2hdb-state/journal/" not in path.as_posix()
    }


def _converted(
    before: dict[str, list[tuple[object, ...]]],
) -> dict[str, list[tuple[object, ...]]]:
    result = dict(before)
    old = before["library_state"][0]
    result["library_state"] = [(old[0], 5, *old[2:])]
    return result


def _version(root: Path) -> int:
    with sqlite3.connect(fixtures._journal(root)) as connection:
        return _admit(connection, _old_schema())


@pytest.mark.parametrize("phase", ["IDLE", "OPEN", "ACTIVATING"])
def test_conversion_preserves_nonempty_authority_and_artifacts_and_replays(
    old_library: Path, phase: str
) -> None:
    root = old_library
    if phase != "IDLE":
        with sqlite3.connect(fixtures._journal(root)) as connection:
            connection.execute(
                "UPDATE library_state SET phase=?, pending_revision=2, pending_receipt_id=?, last_cursor=?",
                (phase, b"s" * 16, b"c" * 33),
            )
            connection.execute(
                "INSERT INTO pending_entries SELECT 2, publication_key, gid, resource_kind, "
                "storage_codec, storage_path, object_sha256, size_bytes, published_modified_at, "
                "1, 0, device, inode, modified_ns, changed_ns FROM current_entries"
            )
            connection.execute(
                "INSERT INTO pending_removals SELECT 2, publication_key, resource_kind, "
                "storage_codec, storage_path, object_sha256, size_bytes, device, inode, "
                "modified_ns, changed_ns, 1 FROM current_entries"
            )
        (root / ".h2hdb-coordination/ACTIVATING").write_bytes(b"existing marker\n")
    with sqlite3.connect(fixtures._journal(root)) as connection:
        for number, state in enumerate(("INSTALLED", "RELEASED", "WRITING", "STAGED")):
            connection.execute(
                "INSERT INTO protection_tokens VALUES (?, 'managed-filesystem-v2', ?, ?, 1, "
                "NULL, ?, ?, NULL, NULL, NULL, NULL)",
                (
                    bytes([number]) * 32,
                    f"fixture-{number}",
                    b"h" * 32,
                    state,
                    None if number < 2 else f"stage-{number}",
                ),
            )
    before, files, inode = (
        _facts(root),
        _files(root),
        fixtures._journal(root).stat().st_ino,
    )
    events: list[str] = []
    assert upgrade_library_journal(root, checkpoint=events.append) == "upgraded"
    assert events[-2:] == ["transaction_committed", "durable_complete"]
    assert _facts(root) == _converted(before)
    assert _files(root) == files
    assert fixtures._journal(root).stat().st_ino == inode
    assert _version(root) == 5
    assert upgrade_library_journal(root) == "already_upgraded"
    assert _facts(root) == _converted(before)
    assert _files(root) == files


def test_empty_journal_converts_and_normal_runtime_opens(old_library: Path) -> None:
    with sqlite3.connect(fixtures._journal(old_library)) as connection:
        connection.execute("DELETE FROM current_entries")
    assert upgrade_library_journal(old_library) == "upgraded"
    with sqlite3.connect(fixtures._journal(old_library)) as connection:
        require_exact_schema(connection)
    fixtures._adapter(old_library)._ensure_layout()


def test_pending_relocation_facts_are_preserved_and_new_relocator_can_continue(
    old_library: Path,
) -> None:
    root = old_library
    # The source and destination are the same temporary root in this offline
    # compatibility test; authority verification still hashes the actual bytes.
    with sqlite3.connect(fixtures._journal(root)) as connection:
        stat = root.stat()
        connection.execute(
            "INSERT INTO library_relocation_session VALUES (1, ?, ?, ?, ?, 'CLEANUP', '', 0, NULL)",
            (
                b"s" * 16,
                fixtures._STORAGE_UUID,
                stat.st_dev.to_bytes(8, "big"),
                stat.st_ino.to_bytes(8, "big"),
            ),
        )
    before = _facts(root)
    assert upgrade_library_journal(root) == "upgraded"
    assert _facts(root) == _converted(before)
    with pytest.raises(RuntimeError, match="relocation is unfinished"):
        fixtures._adapter(root)._ensure_layout()
    assert relocate_library(root).complete
    fixtures._assert_current_authority(root)


@pytest.mark.parametrize(
    "checkpoint",
    ["journal_validated", "control_recreated", "index_created", "target_validated"],
)
def test_exception_rolls_back_exact_old_shape_and_all_rows(
    old_library: Path, checkpoint: str
) -> None:
    before, files = _facts(old_library), _files(old_library)

    def fail(name: str) -> None:
        if name == checkpoint:
            raise RuntimeError("injected conversion fault")

    with pytest.raises(RuntimeError, match="injected conversion fault"):
        upgrade_library_journal(old_library, checkpoint=fail)
    assert _version(old_library) == 4
    assert _facts(old_library) == before
    assert _files(old_library) == files
    assert upgrade_library_journal(old_library) == "upgraded"


def test_commit_response_loss_replays_without_rewriting_facts(
    old_library: Path,
) -> None:
    before = _facts(old_library)

    def lose_response(name: str) -> None:
        if name == "transaction_committed":
            raise RuntimeError("response lost")

    with pytest.raises(RuntimeError, match="response lost"):
        upgrade_library_journal(old_library, checkpoint=lose_response)
    assert _version(old_library) == 5
    assert upgrade_library_journal(old_library) == "already_upgraded"
    assert _facts(old_library) == _converted(before)


@pytest.mark.parametrize(
    "mutation",
    [
        "index",
        "table",
        "trigger",
        "view",
        "control",
        "uuid",
        "partial",
        "sqlite_prefix",
    ],
)
def test_foreign_or_malformed_journal_is_preserved(
    old_library: Path, mutation: str
) -> None:
    with sqlite3.connect(fixtures._journal(old_library)) as connection:
        match mutation:
            case "sqlite_prefix":
                connection.execute("CREATE TABLE sqliteXforeign(value)")
            case "index":
                connection.execute(
                    "CREATE INDEX foreign_idx ON protection_tokens(state)"
                )
            case "table":
                connection.execute("CREATE TABLE foreign_table(value)")
            case "trigger":
                connection.execute(
                    "CREATE TRIGGER foreign_trigger AFTER INSERT ON protection_tokens BEGIN SELECT 1; END"
                )
            case "view":
                connection.execute(
                    "CREATE VIEW foreign_view AS SELECT token FROM protection_tokens"
                )
            case "control":
                connection.execute("DELETE FROM library_state")
            case "uuid":
                connection.execute(
                    "UPDATE library_storage_identity SET storage_instance_uuid=?",
                    (bytes(16),),
                )
            case "partial":
                connection.execute(
                    "CREATE INDEX protection_cleanup_eligible_idx ON protection_tokens(token) WHERE state='RELEASED' AND staging_leaf IS NOT NULL"
                )
    before, contents = _facts(old_library), fixtures._journal(old_library).read_bytes()
    with pytest.raises(RuntimeError):
        upgrade_library_journal(old_library)
    assert _facts(old_library) == before
    assert fixtures._journal(old_library).read_bytes() == contents


@pytest.mark.parametrize("version", [1, 2, 3, 6])
def test_wrong_version_is_not_repaired(old_library: Path, version: int) -> None:
    with sqlite3.connect(fixtures._journal(old_library)) as connection:
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute("UPDATE library_state SET format_version=?", (version,))
    before = fixtures._journal(old_library).read_bytes()
    with pytest.raises(RuntimeError, match="control"):
        upgrade_library_journal(old_library)
    assert fixtures._journal(old_library).read_bytes() == before


@pytest.mark.parametrize(
    "lock", [".h2hdb-coordination/publication.lock", ".h2hdb-state/locks/state.lock"]
)
def test_locks_block_conversion_before_any_change(old_library: Path, lock: str) -> None:
    before = fixtures._journal(old_library).read_bytes()
    with (old_library / lock).open("rb") as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            upgrade_library_journal(old_library)
    assert fixtures._journal(old_library).read_bytes() == before


@pytest.mark.parametrize(
    "path",
    [
        ".h2hdb-state/journal/library-activation.sqlite3",
        ".h2hdb-state/locks/state.lock",
        ".h2hdb-coordination/publication.lock",
    ],
)
@pytest.mark.parametrize("kind", ["symlink", "hardlink"])
def test_unsafe_control_files_are_never_followed_or_modified(
    old_library: Path, path: str, kind: str
) -> None:
    target = old_library / path
    outside = old_library.parent / "outside"
    if kind == "symlink":
        target.rename(outside)
        target.symlink_to(outside)
    else:
        outside.hardlink_to(target)
    before = outside.read_bytes()
    with pytest.raises((OSError, RuntimeError)):
        upgrade_library_journal(old_library)
    assert outside.read_bytes() == before


def test_missing_journal_is_not_created(old_library: Path) -> None:
    fixtures._journal(old_library).unlink()
    with pytest.raises(FileNotFoundError):
        upgrade_library_journal(old_library)
    assert not fixtures._journal(old_library).exists()


def test_runtime_and_relocation_refuse_v4_without_automatic_upgrade(
    old_library: Path,
) -> None:
    before = fixtures._journal(old_library).read_bytes()
    with pytest.raises(RuntimeError, match="offline journal-v4-to-v5"):
        fixtures._adapter(old_library)._ensure_layout()
    with pytest.raises(RuntimeError, match="expected v5"):
        relocate_library(old_library)
    assert fixtures._journal(old_library).read_bytes() == before


@pytest.mark.deep
@pytest.mark.parametrize("checkpoint", ["index_created", "transaction_committed"])
def test_real_kill_recovers_atomic_old_or_new_journal(
    old_library: Path, checkpoint: str
) -> None:
    reached = old_library.parent / "reached"
    before, files = _facts(old_library), _files(old_library)
    program = """
import sqlite3
import sys
from pathlib import Path
from threading import Event
from h2hdb_ingest.library_journal_upgrade import upgrade_library_journal
original_connect = sqlite3.connect
def connect(database, *args, **kwargs):
    connection = original_connect(database, *args, **kwargs)
    if str(database).endswith("?mode=rw"):
        connection.execute("PRAGMA cache_size=1")
    return connection
sqlite3.connect = connect
database = Path(sys.argv[1]) / ".h2hdb-state/journal/library-activation.sqlite3"
before_bytes = database.read_bytes()
def checkpoint(name):
    if name == sys.argv[2]:
        if name == "index_created":
            journal = Path(str(database) + "-journal").read_bytes()
            assert journal[:8] == bytes.fromhex("d9d505f920a163d7"), journal[:8]
            assert database.read_bytes() != before_bytes
        Path(sys.argv[3]).write_text(name)
        Event().wait(90)
upgrade_library_journal(Path(sys.argv[1]), checkpoint=checkpoint)
"""
    process = subprocess.Popen(
        [
            sys.executable,
            "-I",
            "-c",
            program,
            str(old_library),
            checkpoint,
            str(reached),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = monotonic() + 15
        while (
            not reached.exists() and process.poll() is None and monotonic() < deadline
        ):
            sleep(0.01)
        if not reached.exists():
            process.kill()
            stdout, stderr = process.communicate(timeout=5)
            pytest.fail(f"kill checkpoint was not reached: {stdout} {stderr}")
        process.kill()
        process.communicate(timeout=5)
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)
    expected = "upgraded" if checkpoint == "index_created" else "already_upgraded"
    assert upgrade_library_journal(old_library) == expected
    assert _facts(old_library) == _converted(before)
    assert _files(old_library) == files


def test_cli_requires_offline_assertion_before_library_access(tmp_path: Path) -> None:
    script = Path(__file__).parents[1] / "scripts/upgrade-library-journal-v4-to-v5.py"
    result = subprocess.run(
        [sys.executable, str(script), "--library", str(tmp_path / "absent")],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 2
    assert "--consumers-stopped" in result.stderr
    assert not (tmp_path / "absent").exists()


def test_v5_exact_schema_rejects_sqlite_wildcard_prefix_foreign_object(
    old_library: Path,
) -> None:
    assert upgrade_library_journal(old_library) == "upgraded"
    with sqlite3.connect(fixtures._journal(old_library)) as connection:
        connection.execute("CREATE TABLE sqliteXforeign(value)")
        with pytest.raises(RuntimeError, match="shape"):
            require_exact_schema(connection)
    with pytest.raises(RuntimeError, match="shape"):
        upgrade_library_journal(old_library)


@pytest.mark.parametrize("replacement", ["root", "journal_parent", "journal_leaf"])
def test_changed_namespace_during_sqlite_open_is_rejected_before_first_sql(
    old_library: Path, monkeypatch: pytest.MonkeyPatch, replacement: str
) -> None:
    outside = old_library.parent / "outside"
    shutil.copytree(old_library, outside)
    original_bytes = fixtures._journal(old_library).read_bytes()
    outside_bytes = fixtures._journal(outside).read_bytes()
    opened: list[sqlite3.Connection] = []
    statements: list[str] = []
    original_connect = sqlite3.connect
    retained = old_library.parent / "retained"

    def connect(
        database: str,
        *,
        uri: bool = False,
        isolation_level: Literal["DEFERRED", "EXCLUSIVE", "IMMEDIATE"] | None = None,
    ) -> sqlite3.Connection:
        if str(database).endswith("?mode=rw"):
            if replacement == "root":
                old_library.rename(retained)
                old_library.symlink_to(outside, target_is_directory=True)
            elif replacement == "journal_parent":
                journal_parent = fixtures._journal(old_library).parent
                journal_parent.rename(retained)
                journal_parent.symlink_to(
                    fixtures._journal(outside).parent, target_is_directory=True
                )
            else:
                fixtures._journal(old_library).rename(retained)
                fixtures._journal(old_library).write_bytes(outside_bytes)
        connection = original_connect(
            database, uri=uri, isolation_level=isolation_level
        )
        opened.append(connection)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(sqlite3, "connect", connect)
    with pytest.raises((RuntimeError, OSError)):
        upgrade_library_journal(old_library)
    assert not statements
    assert fixtures._journal(outside).read_bytes() == outside_bytes
    original = (
        fixtures._journal(retained)
        if replacement == "root"
        else retained / "library-activation.sqlite3"
        if replacement == "journal_parent"
        else retained
    )
    assert original.read_bytes() == original_bytes
    assert len(opened) == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        opened[0].execute("SELECT 1")


@pytest.mark.parametrize(
    "path", [".h2hdb-coordination/publication.lock", ".h2hdb-state/locks/state.lock"]
)
def test_replaced_lock_before_commit_rolls_back_and_releases_descriptors(
    old_library: Path, path: str
) -> None:
    before = fixtures._journal(old_library).read_bytes()
    retained = old_library.parent / "old-lock"

    def replace(name: str) -> None:
        if name == "target_validated":
            lock = old_library / path
            lock.rename(retained)
            lock.write_bytes(b"replacement lock")

    with pytest.raises(RuntimeError, match="changed identity"):
        upgrade_library_journal(old_library, checkpoint=replace)
    assert fixtures._journal(old_library).read_bytes() == before
    with retained.open("rb") as descriptor:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_new_journal_hardlink_before_commit_rolls_back(old_library: Path) -> None:
    before = fixtures._journal(old_library).read_bytes()
    outside = old_library.parent / "hardlink"

    def link(name: str) -> None:
        if name == "target_validated":
            outside.hardlink_to(fixtures._journal(old_library))

    with pytest.raises(RuntimeError, match="changed identity"):
        upgrade_library_journal(old_library, checkpoint=link)
    assert fixtures._journal(old_library).read_bytes() == before
    assert outside.read_bytes() == before
