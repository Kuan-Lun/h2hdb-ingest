"""Explicit, resumable verification of a library after moving its mount.

Only this maintenance entry point may replace a saved filesystem identity with
an independently hashed identity. It never deletes, renames, or rewrites artifact
bytes, and it preserves the publication state machine and receipt.
"""

from __future__ import annotations

import json
import os
import sqlite3
import stat
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from hashlib import sha256
from pathlib import Path
from uuid import UUID, uuid4

from ._library_journal import require_exact_schema
from ._relocation_files import LibraryFiles, ObservedFile, Signature, unsigned
from ._storage_paths import validate_storage_path

_MAX_BATCH = 128
_DATABASE = ".h2hdb-state/journal/library-activation.sqlite3"
_MARKER = ".h2hdb-coordination/ACTIVATING"
_TABLES = (
    "current_entries",
    "pending_entries",
    "pending_removals",
    "protection_tokens",
)
_TYPE = Callable[[str], None]
_Upgrade = Callable[[sqlite3.Connection], bool]


@dataclass(frozen=True, slots=True)
class RelocationResult:
    session_id: bytes
    verified_files: int
    complete: bool


@dataclass(frozen=True, slots=True)
class _Session:
    session_id: bytes
    identity: bytes
    root_device: bytes
    root_inode: bytes
    phase: str
    cursor: str
    verified_files: int
    original_marker: bytes | None

    @property
    def marker(self) -> bytes:
        if self.original_marker is not None:
            return self.original_marker
        return (
            json.dumps(
                {
                    "format": "h2hdb-library-relocation-v1",
                    "session_id": self.session_id.hex(),
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("ascii")
            + b"\n"
        )


def _notify(callback: _TYPE | None, event: str) -> None:
    if callback is not None:
        callback(event)


def relocate_library(
    root: Path,
    *,
    batch_size: int = _MAX_BATCH,
    progress: _TYPE | None = None,
    fault: _TYPE | None = None,
    _upgrade: _Upgrade | None = None,
) -> RelocationResult:
    """Verify every authority in bounded steps; rerun after interruption."""
    first_step = True
    while True:
        result = relocate_library_step(
            root,
            batch_size=batch_size,
            progress=progress,
            fault=fault,
            _upgrade=_upgrade,
            _restart_complete=first_step,
            _restart_audit=first_step,
        )
        first_step = False
        if result.complete:
            return result


def relocate_library_step(
    root: Path,
    *,
    batch_size: int = _MAX_BATCH,
    progress: _TYPE | None = None,
    fault: _TYPE | None = None,
    _upgrade: _Upgrade | None = None,
    _restart_complete: bool = False,
    _restart_audit: bool = False,
) -> RelocationResult:
    """Internal single-process/test driver for one hard-capped page.

    Process restart recovery must enter ``relocate_library`` so an interrupted
    final audit is repeated before the durable reader fence can be removed.
    """
    if type(batch_size) is not int or not 1 <= batch_size <= _MAX_BATCH:
        raise ValueError("relocation batch_size must be between 1 and 128")
    with _locked_library(root) as (files, connection):
        session = _session(connection)
        if session is None or (
            session.phase == "COMPLETE"
            and (
                _restart_complete
                or session.root_device != unsigned(files.identity.st_dev)
                or session.root_inode != unsigned(files.identity.st_ino)
            )
        ):
            session = _begin(files, connection, _upgrade)
            _notify(fault, "session_committed")
        else:
            require_exact_schema(connection)
        _require_session_root(files, connection, session)
        if _restart_audit and session.phase in {"AUDIT", "FINALIZING"}:
            _ensure_marker(files, session)
            connection.execute(
                "UPDATE library_relocation_session SET phase = 'AUDIT', cursor = '' WHERE singleton = 1"
            )
            session = replace(session, phase="AUDIT", cursor="")
        if session.phase == "COMPLETE":
            return RelocationResult(session.session_id, session.verified_files, True)
        if session.phase != "FINALIZING":
            _ensure_marker(files, session)
            _notify(fault, "marker_durable")
        if session.phase == "CLEANUP":
            _cleanup(connection, session, batch_size)
        elif session.phase == "SCAN":
            _scan(files, connection, session, batch_size, progress, fault)
        elif session.phase == "TOKENS":
            _tokens(files, connection, session, batch_size, progress, fault)
        elif session.phase == "AUDIT":
            _audit(files, connection, session, batch_size, fault)
        elif session.phase == "FINALIZING":
            _finalize(files, connection, session, fault)
        else:
            raise RuntimeError("library relocation session phase is corrupt")
        current = _session(connection)
        if current is None:
            raise RuntimeError("library relocation session disappeared")
        return RelocationResult(
            current.session_id, current.verified_files, current.phase == "COMPLETE"
        )


@contextmanager
def _locked_library(root: Path) -> Iterator[tuple[LibraryFiles, sqlite3.Connection]]:
    # The deployment uses POSIX flock for both ingest and OPDS readers.
    import fcntl

    files = LibraryFiles(root)
    descriptors: list[int] = []
    connection: sqlite3.Connection | None = None
    try:
        for path in (
            ".h2hdb-coordination/publication.lock",
            ".h2hdb-state/locks/state.lock",
        ):
            with files.parent(path) as (parent, leaf):
                if parent is None:
                    raise RuntimeError(f"library lock directory is missing: {path}")
                descriptor = os.open(
                    leaf, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent
                )
                descriptors.append(descriptor)
                value = os.fstat(descriptor)
                if not stat.S_ISREG(value.st_mode) or value.st_nlink != 1:
                    raise RuntimeError(
                        f"library lock is not a single-link regular file: {path}"
                    )
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                named = os.stat(leaf, dir_fd=parent, follow_symlinks=False)
                if value.st_nlink != 1 or (value.st_dev, value.st_ino) != (
                    named.st_dev,
                    named.st_ino,
                ):
                    raise RuntimeError(
                        f"library relocation lock changed identity: {path}"
                    )
                os.fsync(descriptor)
                os.fsync(parent)
        with files.parent(_DATABASE) as (parent, leaf):
            if parent is None:
                raise RuntimeError("existing library journal is missing")
            descriptor = os.open(
                leaf, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent
            )
            descriptors.append(descriptor)
            database_identity = os.fstat(descriptor)
            if (
                not stat.S_ISREG(database_identity.st_mode)
                or database_identity.st_nlink != 1
            ):
                raise RuntimeError("library journal is not a single-link regular file")
            connection = sqlite3.connect(
                (files.root / _DATABASE).as_uri() + "?mode=rw",
                uri=True,
                isolation_level=None,
            )
            named = os.stat(leaf, dir_fd=parent, follow_symlinks=False)
            if (named.st_dev, named.st_ino) != (
                database_identity.st_dev,
                database_identity.st_ino,
            ):
                raise RuntimeError("library journal changed while opening SQLite")
            if connection.execute("PRAGMA journal_mode").fetchone() != ("delete",):
                raise RuntimeError("relocation requires SQLite DELETE journal mode")
            connection.execute("PRAGMA synchronous = FULL")
            if connection.execute("PRAGMA synchronous").fetchone() != (2,):
                raise RuntimeError("relocation requires SQLite FULL synchronization")
            yield files, connection
            named = os.stat(leaf, dir_fd=parent, follow_symlinks=False)
            if (named.st_dev, named.st_ino) != (
                database_identity.st_dev,
                database_identity.st_ino,
            ):
                raise RuntimeError("library journal changed identity during relocation")
            os.fsync(descriptor)
            os.fsync(parent)
    finally:
        if connection is not None:
            connection.close()
        for descriptor in reversed(descriptors):
            os.close(descriptor)
        files.close()


def _session(connection: sqlite3.Connection) -> _Session | None:
    exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'library_relocation_session'"
    ).fetchone()
    if exists is None:
        return None
    row = connection.execute(
        "SELECT session_id, storage_instance_uuid, root_device, root_inode, "
        "phase, cursor, verified_files, original_marker FROM library_relocation_session "
        "WHERE singleton = 1"
    ).fetchone()
    return None if row is None else _Session(*row)


def _require_session_root(
    files: LibraryFiles, connection: sqlite3.Connection, session: _Session
) -> None:
    files.require_root()
    _storage_uuid(session.identity)
    identity = connection.execute(
        "SELECT storage_instance_uuid FROM library_storage_identity WHERE singleton = 1"
    ).fetchone()
    if identity != (session.identity,):
        raise RuntimeError("library storage UUID changed during relocation")
    if (session.root_device, session.root_inode) != (
        unsigned(files.identity.st_dev),
        unsigned(files.identity.st_ino),
    ):
        raise RuntimeError(
            "unfinished relocation belongs to another root identity; restore the authorized destination"
        )


def _storage_uuid(value: object) -> bytes:
    """Enforce the runtime UUIDv4 contract before authorizing a relocation."""
    if type(value) is not bytes or len(value) != 16:
        raise RuntimeError("library storage UUID must be exactly 16 bytes")
    if UUID(bytes=value).version != 4:
        raise RuntimeError("library storage UUID must be UUIDv4")
    return value


def _begin(
    files: LibraryFiles, connection: sqlite3.Connection, upgrade: _Upgrade | None
) -> _Session:
    identity_row = connection.execute(
        "SELECT storage_instance_uuid FROM library_storage_identity WHERE singleton = 1"
    ).fetchone()
    identity = _storage_uuid(None if identity_row is None else identity_row[0])
    marker = files.read_control(_MARKER)
    if marker is not None:
        state = connection.execute(
            "SELECT current_revision, current_receipt_id, pending_revision, pending_receipt_id FROM library_state WHERE singleton = 1"
        ).fetchone()
        if state is None or marker not in {
            _publication_marker(state[0], state[1]),
            _publication_marker(state[2], state[3]),
        }:
            raise RuntimeError("library has a foreign ACTIVATING marker")
    session = _Session(
        uuid4().bytes,
        identity,
        unsigned(files.identity.st_dev),
        unsigned(files.identity.st_ino),
        "CLEANUP",
        "",
        0,
        marker,
    )
    connection.execute("BEGIN IMMEDIATE")
    try:
        if upgrade is not None:
            upgrade(connection)
        require_exact_schema(connection)
        connection.execute(
            "INSERT OR REPLACE INTO library_relocation_session "
            "(singleton, session_id, storage_instance_uuid, root_device, root_inode, phase, cursor, verified_files, original_marker) "
            "VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                session.session_id,
                session.identity,
                session.root_device,
                session.root_inode,
                session.phase,
                session.cursor,
                session.verified_files,
                marker,
            ),
        )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    return session


def _publication_marker(revision: object, receipt: object) -> bytes | None:
    if type(revision) is not int or not isinstance(receipt, bytes):
        return None
    return (
        json.dumps(
            {
                "format": "h2hdb-library-activation-v2",
                "receipt_id": receipt.hex(),
                "revision": revision,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
        + b"\n"
    )


def _ensure_marker(files: LibraryFiles, session: _Session) -> None:
    marker = files.read_control(_MARKER)
    if marker != session.marker:
        if session.original_marker is not None:
            raise RuntimeError("original publication marker changed during relocation")
        if marker is not None and not session.marker.startswith(marker):
            raise RuntimeError("library relocation marker changed contents")
        files.create_control(_MARKER, session.marker)


def _cleanup(connection: sqlite3.Connection, session: _Session, limit: int) -> None:
    connection.execute("BEGIN IMMEDIATE")
    try:
        removed = connection.execute(
            "DELETE FROM library_relocation_files WHERE rowid IN ("
            "SELECT rowid FROM library_relocation_files WHERE session_id != ? LIMIT ?)",
            (session.session_id, limit),
        ).rowcount
        if removed < limit:
            connection.execute(
                "UPDATE library_relocation_session SET phase = 'SCAN' WHERE singleton = 1"
            )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise


def _active_filter(table: str) -> str:
    if table == "protection_tokens":
        return " AND state IN ('WRITING', 'STAGED')"
    if table in {"pending_entries", "pending_removals"}:
        return " AND activation_revision = (SELECT pending_revision FROM library_state WHERE singleton = 1)"
    return ""


def _paths(connection: sqlite3.Connection, cursor: str, limit: int) -> list[str]:
    query = " UNION ".join(
        f"SELECT storage_path FROM {table} WHERE storage_path > ?"
        + _active_filter(table)
        for table in _TABLES
    )
    return [
        str(row[0])
        for row in connection.execute(
            query + " ORDER BY storage_path LIMIT ?",
            (*([cursor] * len(_TABLES)), limit),
        ).fetchmany(limit)
    ]


def _rows(
    connection: sqlite3.Connection, table: str, path: str
) -> tuple[sqlite3.Row, ...]:
    cursor = connection.execute(
        f"SELECT * FROM {table} WHERE storage_path = ?"
        + _active_filter(table)
        + " LIMIT 2",
        (path,),
    )
    cursor.row_factory = sqlite3.Row
    result = tuple(cursor.fetchmany(2))
    # Current paths, each active revision path and active stage paths all have
    # unique indexes. Terminal history belongs to its separate keyset pass.
    if len(result) > 1:
        raise RuntimeError(f"duplicate active journal authority for relocation: {path}")
    return result


def _signature(row: sqlite3.Row) -> Signature | None:
    values = (row["device"], row["inode"], row["modified_ns"], row["changed_ns"])
    if all(value is None for value in values):
        return None
    if any(value is None for value in values):
        raise RuntimeError("library relocation encountered an incomplete signature")
    if (
        not isinstance(values[0], bytes)
        or len(values[0]) != 8
        or not isinstance(values[1], bytes)
        or len(values[1]) != 8
    ):
        raise RuntimeError("library relocation encountered a corrupt inode identity")
    return Signature(
        values[0], values[1], int(row["size_bytes"]), int(values[2]), int(values[3])
    )


def _matches(file: ObservedFile, row: sqlite3.Row) -> bool:
    return (
        file.signature is not None
        and file.digest == row["object_sha256"]
        and file.signature.size == row["size_bytes"]
    )


def _quarantine(path: str, digest: bytes, suffix: str) -> str:
    digest_name = sha256(
        b"h2hdb-library-quarantine-v2\0" + path.encode("ascii") + b"\0" + digest
    ).hexdigest()
    return f".h2hdb-state/quarantine/{digest_name}{suffix}"


def _scan(
    files: LibraryFiles,
    connection: sqlite3.Connection,
    session: _Session,
    limit: int,
    progress: _TYPE | None,
    fault: _TYPE | None,
) -> None:
    paths = _paths(connection, session.cursor, limit)
    for path in paths:
        group = {table: _rows(connection, table, path) for table in _TABLES}
        observed, bindings = _resolve(files, path, group)
        _notify(fault, "resource_verified")
        for value in observed:
            files.require_observed(value)
        connection.execute("BEGIN IMMEDIATE")
        try:
            for table, rows in group.items():
                if tuple(tuple(row) for row in _rows(connection, table, path)) != tuple(
                    tuple(row) for row in rows
                ):
                    raise RuntimeError(
                        "library authority changed during relocation verification"
                    )
            for table, row, observed_file in bindings:
                _bind(connection, table, row, observed_file)
            for value in observed:
                _save_file(connection, session.session_id, value)
            connection.execute(
                "UPDATE library_relocation_session SET cursor = ?, verified_files = verified_files + ? WHERE singleton = 1",
                (path, sum(value.signature is not None for value in observed)),
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        _notify(fault, "resource_committed")
        _notify(progress, f"verified {path}")
    if len(paths) < limit:
        connection.execute(
            "UPDATE library_relocation_session SET phase = 'TOKENS', cursor = '' WHERE singleton = 1"
        )
        _notify(fault, "scan_complete")


def _resolve(
    files: LibraryFiles, path: str, group: dict[str, tuple[sqlite3.Row, ...]]
) -> tuple[tuple[ObservedFile, ...], list[tuple[str, sqlite3.Row, ObservedFile]]]:
    all_rows = tuple(row for rows in group.values() for row in rows)
    if not all_rows:
        raise RuntimeError("library relocation resource disappeared")
    gid, kind = validate_storage_path(str(all_rows[0]["storage_codec"]), path)
    for row in all_rows:
        if validate_storage_path(
            str(row["storage_codec"]), str(row["storage_path"])
        ) != (gid, kind):
            raise RuntimeError(
                "library journal disagrees with its canonical storage path"
            )
        if "gid" in row.keys() and row["gid"] != gid:
            raise RuntimeError("library journal GID disagrees with storage path")
        if "resource_kind" in row.keys() and row["resource_kind"] != kind:
            raise RuntimeError(
                "library journal resource kind disagrees with storage path"
            )
        if (
            not isinstance(row["object_sha256"], bytes)
            or len(row["object_sha256"]) != 32
            or type(row["size_bytes"]) is not int
            or row["size_bytes"] <= 0
        ):
            raise RuntimeError("library journal has invalid content authority")
        _signature(row)
    if any(len(group[table]) > 1 for table in _TABLES[:-1]):
        raise RuntimeError("ambiguous simultaneous library revision authorities")
    current = next(iter(group["current_entries"]), None)
    pending = next(iter(group["pending_entries"]), None)
    removal = next(iter(group["pending_removals"]), None)
    if current is not None and _signature(current) is None:
        raise RuntimeError("current library authority lacks an inode signature")
    if removal is not None:
        authority_columns = (
            "publication_key",
            "resource_kind",
            "storage_codec",
            "storage_path",
            "object_sha256",
            "size_bytes",
            "device",
            "inode",
            "modified_ns",
            "changed_ns",
        )
        if current is None or any(
            removal[column] != current[column] for column in authority_columns
        ):
            raise RuntimeError(
                "pending removal disagrees with current durable authority"
            )
    if pending is not None and pending["activated"]:
        if current is None or any(
            pending[column] != current[column]
            for column in (
                "publication_key",
                "resource_kind",
                "object_sha256",
                "size_bytes",
                "device",
                "inode",
                "modified_ns",
                "changed_ns",
            )
        ):
            raise RuntimeError(
                "completed pending activation disagrees with current authority"
            )
    suffix = ".cbz" if kind == "acquisition" else ".jpg"
    maximum = max(int(row["size_bytes"]) for row in all_rows)
    observations: dict[str, ObservedFile] = {}

    def observe(name: str) -> ObservedFile:
        if name not in observations:
            observations[name] = files.observe(name, maximum_size=maximum)
        return observations[name]

    visible = observe("current/" + path)
    current_destination: ObservedFile | None = None
    quarantine = (
        None
        if current is None
        else observe(_quarantine(path, current["object_sha256"], suffix))
    )
    stages: list[tuple[sqlite3.Row, ObservedFile, ObservedFile]] = []
    bindings: list[tuple[str, sqlite3.Row, ObservedFile]] = []
    for token in group["protection_tokens"]:
        token_bytes = token["token"]
        if not isinstance(token_bytes, bytes) or len(token_bytes) != 32:
            raise RuntimeError("library protection token is corrupt")
        name = sha256(token_bytes).hexdigest()
        if token["staging_leaf"] != name + suffix:
            raise RuntimeError("library token staging leaf is not canonical")
        stage = observe(".h2hdb-state/staging/" + name + suffix)
        temporary = observe(".h2hdb-state/staging/." + name + ".tmp")
        if stage.signature is not None and temporary.signature is not None:
            raise RuntimeError(
                "ambiguous staging rename duplicate; preserve both files"
            )
        if stage.signature is not None and not _matches(stage, token):
            raise RuntimeError(
                f"library relocation hash mismatch: {stage.relative_path}"
            )
        partial = (
            token["state"] in {"WRITING", "RELEASED"} and _signature(token) is None
        )
        if temporary.signature is not None and (
            temporary.signature.size > token["size_bytes"]
            or (not partial and not _matches(temporary, token))
        ):
            raise RuntimeError(
                f"library relocation temporary hash/size mismatch: {temporary.relative_path}"
            )
        if token["state"] == "WRITING" and _signature(token) is not None:
            raise RuntimeError("WRITING token has unexpected sealed inode authority")
        if token["state"] == "STAGED" and _signature(token) is None:
            raise RuntimeError("STAGED token lacks sealed inode authority")
        stages.append((token, stage, temporary))
    installed_stage = None
    for token, stage, temporary in stages:
        candidate = (
            token["state"] == "STAGED"
            and stage.signature is None
            and temporary.signature is None
            and pending is not None
            and pending["operation_started"] == 1
            and pending["activated"] == 0
            and token["object_sha256"] == pending["object_sha256"]
            and token["size_bytes"] == pending["size_bytes"]
            and _matches(visible, token)
        )
        if candidate:
            if installed_stage is not None:
                raise RuntimeError(
                    "multiple staging tokens claim the relocated current"
                )
            installed_stage = token
    if current is not None:
        if quarantine is not None and quarantine.signature is not None:
            if not _matches(quarantine, current) or not (
                (removal is not None and removal["operation_started"] == 1)
                or (
                    pending is not None
                    and pending["operation_started"] == 1
                    and pending["activated"] == 0
                )
            ):
                raise RuntimeError("quarantine lacks authorized publication capture")
            if visible.signature is not None and installed_stage is None:
                raise RuntimeError(
                    "ambiguous current/quarantine duplicate; preserve both files"
                )
            current_destination = quarantine
        elif visible.signature is not None and installed_stage is None:
            if not _matches(visible, current):
                raise RuntimeError(
                    f"library relocation hash mismatch: {visible.relative_path}"
                )
            current_destination = visible
        elif installed_stage is not None:
            if _matches(visible, current):
                original = _signature(current)
                original_stage = _signature(installed_stage)
                if (
                    visible.signature is None
                    or original is None
                    or original_stage is None
                    or (
                        not visible.signature.same_inode(original_stage)
                        or visible.signature.same_inode(original)
                    )
                ):
                    raise RuntimeError(
                        "ambiguous byte-identical response-lost replacement after relocation"
                    )
            # Old quarantine was already deleted after installing the new inode.
        elif removal is None or removal["operation_started"] != 1:
            raise RuntimeError(f"managed current and quarantine are missing: {path}")
        if current_destination is not None:
            bindings.append(("current_entries", current, current_destination))
    elif quarantine is not None:
        raise RuntimeError("quarantine lacks current authority")
    if (
        visible.signature is not None
        and current_destination is not visible
        and installed_stage is None
    ):
        raise RuntimeError(f"unknown current library file: {visible.relative_path}")
    for token, stage, temporary in stages:
        destination = stage
        if installed_stage is token:
            destination = visible
        elif token["state"] == "STAGED" and stage.signature is None:
            raise RuntimeError(
                "sealed staging file is missing without exact install replay"
            )
        if token["state"] == "INSTALLED":
            raise RuntimeError("INSTALLED token retained private staging authority")
        if _signature(token) is not None and destination.signature is not None:
            bindings.append(("protection_tokens", token, destination))
        elif (
            token["state"] == "RELEASED"
            and temporary.signature is not None
            and _signature(token) is not None
        ):
            # A retained cleanup signature must match the complete surviving temp.
            bindings.append(("protection_tokens", token, temporary))
    for pending_table, pending_row in (
        ("pending_entries", pending),
        ("pending_removals", removal),
    ):
        if pending_row is None:
            continue
        pending_destination: ObservedFile | None
        if pending_table == "pending_entries":
            if bool(pending_row["activated"]) != (_signature(pending_row) is not None):
                raise RuntimeError(
                    "pending activation signature disagrees with completion"
                )
            if not pending_row["activated"]:
                continue
            pending_destination = visible
        else:
            pending_destination = current_destination
        if pending_destination is None:
            continue
        if not _matches(pending_destination, pending_row):
            raise RuntimeError("pending authority disagrees with verified artifact")
        bindings.append((pending_table, pending_row, pending_destination))
    for table, row, destination in bindings:
        original = _signature(row)
        if original is None or destination.signature is None:
            raise RuntimeError(f"incomplete relocation mapping for {table}")
        prior = observations[destination.relative_path]
        if prior.old_signature is not None and prior.old_signature != original:
            raise RuntimeError(
                "journal references disagree about the original inode authority"
            )
        observations[destination.relative_path] = replace(prior, old_signature=original)
    return tuple(observations.values()), bindings


def _bind(
    connection: sqlite3.Connection, table: str, row: sqlite3.Row, file: ObservedFile
) -> None:
    if file.signature is None:
        raise RuntimeError("cannot bind an absent relocation file")
    names = tuple(row.keys())
    where = " AND ".join(f"{name} IS ?" for name in names)
    affected = connection.execute(
        f"UPDATE {table} SET device = ?, inode = ?, modified_ns = ?, changed_ns = ? WHERE {where}",
        (*file.signature.sql(), *tuple(row)),
    ).rowcount
    if affected != 1:
        raise RuntimeError("library relocation authority changed before commit")


def _save_file(
    connection: sqlite3.Connection, session: bytes, file: ObservedFile
) -> None:
    signature = file.signature
    original = file.old_signature
    connection.execute(
        "INSERT INTO library_relocation_files (session_id, relative_path, object_sha256, size_bytes, "
        "device, inode, modified_ns, changed_ns, old_device, old_inode, old_modified_ns, old_changed_ns) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            session,
            file.relative_path,
            file.digest,
            None if signature is None else signature.size,
            *((None, None, None, None) if signature is None else signature.sql()),
            *((None, None, None, None) if original is None else original.sql()),
        ),
    )


def _tokens(
    files: LibraryFiles,
    connection: sqlite3.Connection,
    session: _Session,
    limit: int,
    progress: _TYPE | None,
    fault: _TYPE | None,
) -> None:
    cursor = connection.execute(
        "SELECT * FROM protection_tokens WHERE state = 'RELEASED' AND staging_leaf IS NOT NULL "
        "AND token > ? ORDER BY token LIMIT ?",
        (bytes.fromhex(session.cursor), limit),
    )
    cursor.row_factory = sqlite3.Row
    for row in (rows := cursor.fetchmany(limit)):
        token = row["token"]
        if not isinstance(token, bytes) or len(token) != 32:
            raise RuntimeError("released protection token is corrupt")
        _, kind = validate_storage_path(row["storage_codec"], row["storage_path"])
        suffix = ".cbz" if kind == "acquisition" else ".jpg"
        name = sha256(token).hexdigest()
        if row["staging_leaf"] != name + suffix:
            raise RuntimeError("released staging authority has an unsafe leaf")
        digest = row["object_sha256"]
        size = row["size_bytes"]
        if (
            not isinstance(digest, bytes)
            or len(digest) != 32
            or type(size) is not int
            or size <= 0
        ):
            raise RuntimeError("released staging content authority is corrupt")
        original = _signature(row)
        stage = files.observe(
            ".h2hdb-state/staging/" + name + suffix, maximum_size=size
        )
        temporary = files.observe(
            ".h2hdb-state/staging/." + name + ".tmp", maximum_size=size
        )
        if stage.signature is not None and temporary.signature is not None:
            raise RuntimeError(
                "ambiguous released staging duplicate; preserve both files"
            )
        if stage.signature is not None and not _matches(stage, row):
            raise RuntimeError("released staged artifact hash mismatch")
        if (
            temporary.signature is not None
            and original is not None
            and not _matches(temporary, row)
        ):
            raise RuntimeError("released complete temporary hash mismatch")
        destination = stage if stage.signature is not None else temporary
        stage = replace(stage, old_signature=original if destination is stage else None)
        temporary = replace(
            temporary, old_signature=original if destination is temporary else None
        )
        _notify(fault, "resource_verified")
        files.require_observed(stage)
        files.require_observed(temporary)
        connection.execute("BEGIN IMMEDIATE")
        try:
            if destination.signature is not None and original is not None:
                _bind(connection, "protection_tokens", row, destination)
            _save_file(connection, session.session_id, stage)
            _save_file(connection, session.session_id, temporary)
            connection.execute(
                "UPDATE library_relocation_session SET cursor = ?, verified_files = verified_files + ? WHERE singleton = 1",
                (token.hex(), int(destination.signature is not None)),
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        _notify(fault, "resource_committed")
        _notify(progress, f"verified released token {token.hex()}")
    if len(rows) < limit:
        connection.execute(
            "UPDATE library_relocation_session SET phase = 'AUDIT', cursor = '' WHERE singleton = 1"
        )
        _notify(fault, "tokens_complete")


def _audit(
    files: LibraryFiles,
    connection: sqlite3.Connection,
    session: _Session,
    limit: int,
    fault: _TYPE | None,
) -> None:
    rows = connection.execute(
        "SELECT relative_path, object_sha256, size_bytes, device, inode, modified_ns, changed_ns "
        "FROM library_relocation_files WHERE session_id = ? AND relative_path > ? ORDER BY relative_path LIMIT ?",
        (session.session_id, session.cursor, limit),
    ).fetchmany(limit)
    for row in rows:
        recorded = ObservedFile(
            row[0],
            row[1],
            None
            if row[1] is None
            else Signature(row[3], row[4], row[2], row[5], row[6]),
        )
        files.require_observed(recorded)
        # The scan independently hashed and synced each inode; the final pass
        # proves none of those inode facts changed while later files were read.
        _notify(fault, "audit_verified")
        files.require_observed(recorded)
        connection.execute(
            "UPDATE library_relocation_session SET cursor = ? WHERE singleton = 1",
            (row[0],),
        )
    if len(rows) < limit:
        connection.execute(
            "UPDATE library_relocation_session SET phase = 'FINALIZING', cursor = '' WHERE singleton = 1"
        )
        _notify(fault, "audit_complete")


def _finalize(
    files: LibraryFiles,
    connection: sqlite3.Connection,
    session: _Session,
    fault: _TYPE | None,
) -> None:
    if session.original_marker is None:
        files.remove_control(_MARKER, session.marker)
    elif files.read_control(_MARKER) != session.original_marker:
        raise RuntimeError("original publication marker changed during relocation")
    _notify(fault, "marker_restored")
    connection.execute(
        "UPDATE library_relocation_session SET phase = 'COMPLETE' WHERE singleton = 1"
    )
    _notify(fault, "complete_committed")
