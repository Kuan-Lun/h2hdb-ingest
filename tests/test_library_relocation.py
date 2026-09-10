from __future__ import annotations

import fcntl
import os
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from shutil import copytree
from threading import Event, Thread

import pytest
from h2hdb import (
    CatalogResourceKind,
    LibraryActivationStatus,
    StorageObjectDescriptor,
    StorageObjectKey,
    VNextLibraryActivationItem,
)

import h2hdb_ingest.library as library_module
from h2hdb_ingest.artifact import ArtifactRenderPolicy
from h2hdb_ingest.journal_upgrade import V3_SCHEMA, upgrade_v3
from h2hdb_ingest.library import ManagedFilesystemLibraryAdapter
from h2hdb_ingest.library_relocation import (
    relocate_library,
    relocate_library_step,
)
from h2hdb_ingest.storage import acquisition_storage_key, thumbnail_storage_key

_STORAGE_UUID = bytes.fromhex("123456789abc40008000123456789abc")
_RECEIPT = b"r" * 16
_MODIFIED = datetime(2026, 1, 1, tzinfo=UTC)


def _journal(root: Path) -> Path:
    return root / ".h2hdb-state" / "journal" / "library-activation.sqlite3"


def _item(
    gid: int, payload: bytes, *, thumbnail: bool = False
) -> VNextLibraryActivationItem:
    key = thumbnail_storage_key(gid) if thumbnail else acquisition_storage_key(gid)
    return VNextLibraryActivationItem(
        publication_key=sha256(
            b"h2hdb-vnext-publication-key\0"
            + (1).to_bytes(4, "big")
            + gid.to_bytes(8, "big")
        ).digest(),
        gid=gid,
        resource_kind=(
            CatalogResourceKind.THUMBNAIL
            if thumbnail
            else CatalogResourceKind.ACQUISITION
        ),
        storage_object=StorageObjectDescriptor(
            key=key,
            size_bytes=len(payload),
            sha256=sha256(payload).hexdigest(),
            modified_at=_MODIFIED,
        ),
    )


def _stat_values(path: Path) -> tuple[bytes, bytes, int, int]:
    value = path.stat(follow_symlinks=False)
    return (
        value.st_dev.to_bytes(8, "big"),
        value.st_ino.to_bytes(8, "big"),
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _target(root: Path, key: StorageObjectKey) -> Path:
    return root / "current" / Path(*key.segments)


def _legacy_root(
    root: Path,
    *,
    count: int = 1,
    thumbnail: bool = False,
) -> tuple[VNextLibraryActivationItem, ...]:
    for relative in (
        "current/acquisitions",
        "current/artwork",
        ".h2hdb-coordination",
        ".h2hdb-state/staging",
        ".h2hdb-state/quarantine",
        ".h2hdb-state/journal",
        ".h2hdb-state/locks",
    ):
        (root / relative).mkdir(parents=True, exist_ok=True)
    (root / ".h2hdb-coordination" / "publication.lock").touch()
    (root / ".h2hdb-state" / "locks" / "state.lock").touch()
    items: list[VNextLibraryActivationItem] = []
    with sqlite3.connect(_journal(root)) as connection:
        connection.executescript(V3_SCHEMA)
        connection.execute(
            "INSERT INTO library_storage_identity VALUES (1, ?)", (_STORAGE_UUID,)
        )
        connection.execute(
            "UPDATE library_state SET current_revision = 1, current_receipt_id = ?",
            (_RECEIPT,),
        )
        for gid in range(1, count + 1):
            for is_thumbnail in (False, True) if thumbnail else (False,):
                payload = f"relocated resource {gid} thumbnail={is_thumbnail}".encode()
                item = _item(gid, payload, thumbnail=is_thumbnail)
                target = _target(root, item.storage_object.key)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(payload)
                connection.execute(
                    "INSERT INTO current_entries VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        item.publication_key,
                        item.resource_kind.value,
                        "/".join(item.storage_object.key.segments),
                        item.storage_object.key.codec,
                        gid,
                        bytes.fromhex(item.storage_object.sha256),
                        item.storage_object.size_bytes,
                        _MODIFIED.isoformat(),
                        *_stat_values(target),
                    ),
                )
                items.append(item)
    return tuple(items)


def _adapter(root: Path) -> ManagedFilesystemLibraryAdapter:
    source = root.parent / "source"
    source.mkdir(exist_ok=True)
    return ManagedFilesystemLibraryAdapter(
        root,
        source_root=source,
        render_policy=ArtifactRenderPolicy(),
    )


def _upgrade(root: Path) -> None:
    with sqlite3.connect(_journal(root)) as connection:
        connection.execute("BEGIN IMMEDIATE")
        assert upgrade_v3(connection)


def _assert_current_authority(root: Path) -> None:
    with sqlite3.connect(_journal(root)) as connection:
        rows = connection.execute(
            "SELECT storage_path, object_sha256, size_bytes, device, inode, "
            "modified_ns, changed_ns FROM current_entries ORDER BY storage_path"
        ).fetchall()
    for path, digest, size, *signature in rows:
        target = root / "current" / path
        assert sha256(target.read_bytes()).digest() == digest
        assert target.stat().st_size == size
        assert _stat_values(target) == tuple(signature)


def _fail_once(checkpoint: str) -> Callable[[str], None]:
    raised = False

    def fault(observed: str) -> None:
        nonlocal raised
        if observed == checkpoint and not raised:
            raised = True
            raise RuntimeError(f"fault: {checkpoint}")

    return fault


def _protect(
    adapter: ManagedFilesystemLibraryAdapter,
    item: VNextLibraryActivationItem,
    payload: bytes,
    token: bytes,
) -> None:
    descriptor = item.storage_object
    assert adapter.protect(
        BytesIO(payload),
        descriptor.key,
        bytes.fromhex(descriptor.sha256),
        descriptor.size_bytes,
        descriptor.modified_at,
        token,
    ).stored


def _activate(
    adapter: ManagedFilesystemLibraryAdapter,
    items: tuple[VNextLibraryActivationItem, ...],
) -> None:
    with adapter.publication_guard():
        adapter.begin(2, b"s" * 16)
        adapter.activate_page(
            2,
            tuple(
                sorted(
                    items,
                    key=lambda item: (item.publication_key, item.resource_kind.value),
                )
            ),
        )
        adapter.seal(2)
        checkpoint = adapter.reconcile_page(2, b"s" * 16, limit=128)
        while checkpoint.status is not LibraryActivationStatus.READY:
            checkpoint = adapter.reconcile_page(2, b"s" * 16, limit=128)
        adapter.complete(2, b"s" * 16)


def test_relocation_upgrades_complete_v3_copy_preserving_catalog_and_bytes(
    tmp_path: Path,
) -> None:
    original = tmp_path / "original"
    items = _legacy_root(original, count=2, thumbnail=True)
    moved = tmp_path / "moved"
    copytree(original, moved)
    first = items[0].storage_object.key
    assert _stat_values(_target(original, first)) != _stat_values(_target(moved, first))

    result = relocate_library(moved, batch_size=2, _upgrade=upgrade_v3)

    assert result.complete
    assert result.verified_files == len(items)
    assert len(result.session_id) == 16
    _assert_current_authority(moved)
    with sqlite3.connect(_journal(moved)) as connection:
        assert connection.execute(
            "SELECT format_version, current_revision, current_receipt_id, "
            "pending_revision, phase FROM library_state"
        ).fetchone() == (4, 1, _RECEIPT, None, "IDLE")
        assert connection.execute(
            "SELECT storage_instance_uuid FROM library_storage_identity"
        ).fetchone() == (_STORAGE_UUID,)
    adapter = _adapter(moved)
    assert adapter.ensure_storage_identity().storage_instance_uuid == _STORAGE_UUID
    with adapter.publication_guard():
        assert adapter.begin(1, _RECEIPT).status is LibraryActivationStatus.COMPLETE
    for item in items:
        assert (
            _target(original, item.storage_object.key).read_bytes()
            == _target(moved, item.storage_object.key).read_bytes()
        )


def test_normal_runtime_rejects_v3_without_implicitly_upgrading(tmp_path: Path) -> None:
    root = tmp_path / "library"
    _legacy_root(root)
    original_database = _journal(root).read_bytes()
    original_paths = tuple(sorted(path.relative_to(root) for path in root.rglob("*")))

    with pytest.raises(RuntimeError, match=r"journal|format|upgrade"):
        _adapter(root).ensure_storage_identity()

    with sqlite3.connect(_journal(root)) as connection:
        assert connection.execute(
            "SELECT format_version FROM library_state"
        ).fetchone() == (3,)
    assert _journal(root).read_bytes() == original_database
    assert (
        tuple(sorted(path.relative_to(root) for path in root.rglob("*")))
        == original_paths
    )


def test_relocation_accepts_ctime_only_change_after_hash_verification(
    tmp_path: Path,
) -> None:
    root = tmp_path / "library"
    (item,) = _legacy_root(root)
    _upgrade(root)
    target = _target(root, item.storage_object.key)
    before = target.stat()
    target.chmod(0o600 if before.st_mode & 0o777 != 0o600 else 0o644)
    after = target.stat()
    assert (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) == (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    )
    assert before.st_ctime_ns != after.st_ctime_ns

    assert relocate_library(root).complete
    _assert_current_authority(root)


@pytest.mark.parametrize(
    "checkpoint",
    (
        "session_committed",
        "marker_durable",
        "resource_verified",
        "resource_committed",
        "scan_complete",
        "tokens_complete",
        "audit_verified",
        "audit_complete",
        "marker_restored",
        "complete_committed",
    ),
)
def test_relocation_reopens_after_durable_boundary_response_loss(
    tmp_path: Path,
    checkpoint: str,
) -> None:
    original = tmp_path / "original"
    _legacy_root(original, count=2)
    moved = tmp_path / "moved"
    copytree(original, moved)

    with pytest.raises(RuntimeError, match=f"fault: {checkpoint}"):
        relocate_library(
            moved,
            batch_size=1,
            fault=_fail_once(checkpoint),
            _upgrade=upgrade_v3,
        )

    result = relocate_library(moved, batch_size=1, _upgrade=upgrade_v3)

    assert result.complete
    _assert_current_authority(moved)
    assert not (moved / ".h2hdb-coordination" / "ACTIVATING").exists()
    assert (
        _adapter(moved).ensure_storage_identity().storage_instance_uuid == _STORAGE_UUID
    )


def test_relocation_step_bounds_verified_resources_and_resumes(tmp_path: Path) -> None:
    original = tmp_path / "original"
    _legacy_root(original, count=5)
    moved = tmp_path / "moved"
    copytree(original, moved)
    observed: list[str] = []

    first = relocate_library_step(
        moved,
        batch_size=2,
        fault=observed.append,
        _upgrade=upgrade_v3,
    )

    assert not first.complete
    assert observed.count("resource_verified") <= 2
    assert observed.count("audit_verified") <= 2
    with pytest.raises(RuntimeError, match="relocat"):
        _adapter(moved).ensure_storage_identity()
    completed = first
    maximum_verified = 0
    while not completed.complete:
        observed.clear()
        completed = relocate_library_step(
            moved, batch_size=2, fault=observed.append, _upgrade=upgrade_v3
        )
        maximum_verified = max(maximum_verified, observed.count("resource_verified"))
        assert observed.count("resource_verified") <= 2
        assert observed.count("audit_verified") <= 2
    assert completed.complete
    assert maximum_verified == 2
    assert completed.session_id == first.session_id
    _assert_current_authority(moved)


@pytest.mark.parametrize("batch_size", (0, -1, 129))
def test_relocation_rejects_batch_outside_hard_cap(
    tmp_path: Path,
    batch_size: int,
) -> None:
    root = tmp_path / "library"
    _legacy_root(root)

    with pytest.raises((TypeError, ValueError), match=r"batch|128|limit"):
        relocate_library_step(root, batch_size=batch_size, _upgrade=upgrade_v3)

    with sqlite3.connect(_journal(root)) as connection:
        assert connection.execute(
            "SELECT format_version FROM library_state"
        ).fetchone() == (3,)


@pytest.mark.parametrize("corruption", ("bytes", "size", "symlink", "missing", "link"))
def test_relocation_preserves_unverified_or_unsafe_current(
    tmp_path: Path,
    corruption: str,
) -> None:
    original = tmp_path / "original"
    (item,) = _legacy_root(original)
    moved = tmp_path / "moved"
    copytree(original, moved)
    target = _target(moved, item.storage_object.key)
    payload = target.read_bytes()
    outside = tmp_path / "outside"
    outside.write_bytes(payload)
    if corruption == "bytes":
        target.write_bytes(bytes(value ^ 1 for value in payload))
    elif corruption == "size":
        target.write_bytes(payload + b"extra")
    elif corruption == "symlink":
        target.unlink()
        target.symlink_to(outside)
    elif corruption == "missing":
        target.unlink()
    else:
        os.link(target, tmp_path / "other-link")
    before = None if corruption == "missing" else target.lstat()
    before_bytes = None if corruption == "missing" else target.read_bytes()

    with pytest.raises(RuntimeError):
        relocate_library(moved, _upgrade=upgrade_v3)

    assert outside.read_bytes() == payload
    if before is None:
        assert not target.exists()
    else:
        after = target.lstat()
        assert (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mode,
            after.st_nlink,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ) == (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mode,
            before.st_nlink,
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        assert target.read_bytes() == before_bytes
    with pytest.raises(RuntimeError, match="relocat"):
        _adapter(moved).ensure_storage_identity()


def test_relocation_detects_change_between_hash_and_journal_commit(
    tmp_path: Path,
) -> None:
    root = tmp_path / "library"
    (item,) = _legacy_root(root)
    target = _target(root, item.storage_object.key)
    changed = False

    def mutate_after_hash(checkpoint: str) -> None:
        nonlocal changed
        if checkpoint == "resource_verified" and not changed:
            changed = True
            target.write_bytes(b"x" * item.storage_object.size_bytes)

    with pytest.raises(RuntimeError):
        relocate_library(root, fault=mutate_after_hash, _upgrade=upgrade_v3)

    assert changed
    assert target.read_bytes() == b"x" * item.storage_object.size_bytes
    with pytest.raises(RuntimeError, match="relocat"):
        _adapter(root).ensure_storage_identity()


def test_relocation_preserves_unreferenced_files_without_certifying_them(
    tmp_path: Path,
) -> None:
    root = tmp_path / "library"
    _legacy_root(root)
    unknown = root / "current" / "acquisitions" / "operator-file.cbz"
    unknown.write_bytes(b"unreferenced file outside journal authority")
    before = _stat_values(unknown)

    result = relocate_library(root, _upgrade=upgrade_v3)

    assert result.complete
    assert result.verified_files == 1
    assert unknown.read_bytes() == b"unreferenced file outside journal authority"
    assert _stat_values(unknown) == before
    with sqlite3.connect(_journal(root)) as connection:
        assert (
            connection.execute(
                "SELECT 1 FROM library_relocation_files WHERE relative_path = ?",
                (unknown.relative_to(root).as_posix(),),
            ).fetchone()
            is None
        )


def test_normal_runtime_still_rejects_byte_identical_foreign_inode(
    tmp_path: Path,
) -> None:
    root = tmp_path / "library"
    (item,) = _legacy_root(root)
    _upgrade(root)
    adapter = _adapter(root)
    adapter.ensure_storage_identity()
    target = _target(root, item.storage_object.key)
    replacement = target.with_suffix(".replacement")
    replacement.write_bytes(target.read_bytes())
    assert replacement.stat().st_ino != target.stat().st_ino
    replacement.replace(target)

    with pytest.raises(RuntimeError, match="durable inode authority"):
        _activate(adapter, (item,))

    assert sha256(target.read_bytes()).hexdigest() == item.storage_object.sha256


def test_relocation_rebinds_staged_authority_before_normal_activation(
    tmp_path: Path,
) -> None:
    original = tmp_path / "original"
    (existing,) = _legacy_root(original)
    _upgrade(original)
    adapter = _adapter(original)
    payload = b"new staged acquisition"
    staged = _item(2, payload)
    token = b"t" * 32
    _protect(adapter, staged, payload, token)
    stage_relative = ".h2hdb-state/staging/" + sha256(token).hexdigest() + ".cbz"
    moved = tmp_path / "moved"
    copytree(original, moved)

    assert relocate_library(moved).complete

    with sqlite3.connect(_journal(moved)) as connection:
        assert connection.execute(
            "SELECT state, device, inode, modified_ns, changed_ns "
            "FROM protection_tokens WHERE token = ?",
            (token,),
        ).fetchone() == ("STAGED", *_stat_values(moved / stage_relative))
    _activate(_adapter(moved), (existing, staged))
    assert _target(moved, staged.storage_object.key).read_bytes() == payload
    assert not (moved / stage_relative).exists()
    _assert_current_authority(moved)


class _PartialSource(BytesIO):
    def __init__(self, payload: bytes) -> None:
        super().__init__(payload)
        self._first = True

    def read(self, size: int | None = -1) -> bytes:
        if self._first:
            self._first = False
            return super().read(min(3, size) if size is not None and size >= 0 else 3)
        raise RuntimeError("fault: partial source")


def test_relocation_preserves_writing_partial_without_sealing_it(
    tmp_path: Path,
) -> None:
    original = tmp_path / "original"
    _legacy_root(original)
    _upgrade(original)
    adapter = _adapter(original)
    payload = b"incomplete staged acquisition"
    staged = _item(2, payload)
    token = b"p" * 32
    descriptor = staged.storage_object
    with pytest.raises(RuntimeError, match="partial source"):
        adapter.protect(
            _PartialSource(payload),
            descriptor.key,
            bytes.fromhex(descriptor.sha256),
            descriptor.size_bytes,
            descriptor.modified_at,
            token,
        )
    temporary = ".h2hdb-state/staging/." + sha256(token).hexdigest() + ".tmp"
    moved = tmp_path / "moved"
    copytree(original, moved)
    partial = (moved / temporary).read_bytes()
    assert 0 < len(partial) < len(payload)

    assert relocate_library(moved).complete

    assert (moved / temporary).read_bytes() == partial
    with sqlite3.connect(_journal(moved)) as connection:
        assert connection.execute(
            "SELECT state, device, inode, modified_ns, changed_ns "
            "FROM protection_tokens WHERE token = ?",
            (token,),
        ).fetchone() == ("WRITING", None, None, None, None)
    _adapter(moved).release(
        descriptor.key,
        bytes.fromhex(descriptor.sha256),
        descriptor.size_bytes,
        token,
    )
    assert not (moved / temporary).exists()


def test_relocation_preserves_pending_install_receipt_and_resumes_rename_loss(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = tmp_path / "original"
    _legacy_root(original)
    _upgrade(original)
    adapter = _adapter(original)
    payload = b"replacement whose rename was committed"
    replacement = _item(1, payload)
    _protect(adapter, replacement, payload, b"n" * 32)

    def stop_before_sql(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("fault: rename before journal")

    with monkeypatch.context() as scoped:
        scoped.setattr(adapter, "_commit_pending_install", stop_before_sql)
        with pytest.raises(RuntimeError, match="rename before journal"):
            _activate(adapter, (replacement,))
    marker = (original / ".h2hdb-coordination" / "ACTIVATING").read_bytes()
    moved = tmp_path / "moved"
    copytree(original, moved)

    assert relocate_library(moved).complete

    assert (moved / ".h2hdb-coordination" / "ACTIVATING").read_bytes() == marker
    resumed = _adapter(moved)
    with resumed.publication_guard():
        resumed.begin(2, b"s" * 16)
        checkpoint = resumed.reconcile_page(2, b"s" * 16, limit=128)
        while checkpoint.status is not LibraryActivationStatus.READY:
            checkpoint = resumed.reconcile_page(2, b"s" * 16, limit=128)
        resumed.complete(2, b"s" * 16)
    assert _target(moved, replacement.storage_object.key).read_bytes() == payload
    assert not (moved / ".h2hdb-coordination" / "ACTIVATING").exists()
    _assert_current_authority(moved)


@pytest.mark.parametrize("checkpoint", ("audit_complete", "marker_restored"))
def test_relocation_rechecks_files_after_finalization_process_loss(
    tmp_path: Path,
    checkpoint: str,
) -> None:
    root = tmp_path / "library"
    (item,) = _legacy_root(root)
    with pytest.raises(RuntimeError, match=f"fault: {checkpoint}"):
        relocate_library(root, fault=_fail_once(checkpoint), _upgrade=upgrade_v3)
    target = _target(root, item.storage_object.key)
    changed = b"q" * item.storage_object.size_bytes
    target.write_bytes(changed)

    with pytest.raises(RuntimeError):
        relocate_library(root, _upgrade=upgrade_v3)

    assert target.read_bytes() == changed
    with pytest.raises(RuntimeError, match="relocat"):
        _adapter(root).ensure_storage_identity()


def test_relocation_rejects_contended_publication_lock_before_journal_upgrade(
    tmp_path: Path,
) -> None:
    root = tmp_path / "library"
    _legacy_root(root)
    with (root / ".h2hdb-coordination" / "publication.lock").open("rb") as reader:
        fcntl.flock(reader.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
        try:
            with pytest.raises(BlockingIOError):
                relocate_library(root, _upgrade=upgrade_v3)
        finally:
            fcntl.flock(reader.fileno(), fcntl.LOCK_UN)
    with sqlite3.connect(_journal(root)) as connection:
        assert connection.execute(
            "SELECT format_version FROM library_state"
        ).fetchone() == (3,)


def test_relocation_refuses_replaced_root_during_unfinished_session(
    tmp_path: Path,
) -> None:
    root = tmp_path / "library"
    _legacy_root(root)
    assert not relocate_library_step(root, _upgrade=upgrade_v3).complete
    replacement = tmp_path / "another-root"
    copytree(root, replacement)

    with pytest.raises(RuntimeError, match=r"root|identity"):
        relocate_library_step(replacement, _upgrade=upgrade_v3)


def test_relocation_rebinds_quarantined_pending_removal_and_resumes_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = tmp_path / "original"
    (item,) = _legacy_root(original)
    _upgrade(original)
    adapter = _adapter(original)

    def stop_before_unlink(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("fault: quarantine before unlink")

    with monkeypatch.context() as scoped:
        scoped.setattr(adapter, "_unlink_quarantined", stop_before_unlink)
        with pytest.raises(RuntimeError, match="quarantine before unlink"):
            _activate(adapter, ())
    assert not _target(original, item.storage_object.key).exists()
    quarantines = tuple((original / ".h2hdb-state" / "quarantine").iterdir())
    assert len(quarantines) == 1
    moved = tmp_path / "moved"
    copytree(original, moved)

    assert relocate_library(moved).complete

    quarantined = moved / ".h2hdb-state" / "quarantine" / quarantines[0].name
    with sqlite3.connect(_journal(moved)) as connection:
        assert connection.execute(
            "SELECT device, inode, modified_ns, changed_ns FROM pending_removals"
        ).fetchone() == _stat_values(quarantined)
    resumed = _adapter(moved)
    with resumed.publication_guard():
        resumed.begin(2, b"s" * 16)
        checkpoint = resumed.reconcile_page(2, b"s" * 16, limit=128)
        while checkpoint.status is not LibraryActivationStatus.READY:
            checkpoint = resumed.reconcile_page(2, b"s" * 16, limit=128)
        resumed.complete(2, b"s" * 16)
    assert not quarantined.exists()
    assert not _target(moved, item.storage_object.key).exists()


def test_relocation_preserves_ambiguous_staging_publish_duplicate(
    tmp_path: Path,
) -> None:
    original = tmp_path / "original"
    _legacy_root(original)
    _upgrade(original)
    payload = b"ambiguous staging candidate"
    item = _item(2, payload)
    token = b"d" * 32
    _protect(_adapter(original), item, payload, token)
    stage_directory = original / ".h2hdb-state" / "staging"
    leaf = sha256(token).hexdigest()
    (stage_directory / f".{leaf}.tmp").write_bytes(payload)
    moved = tmp_path / "moved"
    copytree(original, moved)

    with pytest.raises(RuntimeError, match=r"ambiguous|duplicate"):
        relocate_library(moved)

    assert (moved / ".h2hdb-state" / "staging" / f"{leaf}.cbz").read_bytes() == payload
    assert (moved / ".h2hdb-state" / "staging" / f".{leaf}.tmp").read_bytes() == payload


def test_relocation_does_not_treat_completed_pending_history_as_current(
    tmp_path: Path,
) -> None:
    original = tmp_path / "original"
    (item,) = _legacy_root(original)
    target = _target(original, item.storage_object.key)
    with sqlite3.connect(_journal(original)) as connection:
        connection.execute("UPDATE library_state SET current_revision = 3")
        for revision in (1, 2):
            stale_payload = f"obsolete artifact revision {revision}".encode()
            connection.execute(
                "INSERT INTO pending_entries VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?, ?, ?)",
                (
                    revision,
                    item.publication_key,
                    item.gid,
                    item.resource_kind.value,
                    item.storage_object.key.codec,
                    "/".join(item.storage_object.key.segments),
                    sha256(stale_payload).digest(),
                    len(stale_payload),
                    _MODIFIED.isoformat(),
                    *_stat_values(target),
                ),
            )
        before = connection.execute(
            "SELECT * FROM pending_entries ORDER BY activation_revision"
        ).fetchall()
    moved = tmp_path / "moved"
    copytree(original, moved)

    assert relocate_library(moved, _upgrade=upgrade_v3).complete

    _assert_current_authority(moved)
    with sqlite3.connect(_journal(moved)) as connection:
        assert (
            connection.execute(
                "SELECT * FROM pending_entries ORDER BY activation_revision"
            ).fetchall()
            == before
        )


def test_relocation_pages_retained_released_tokens_for_one_storage_path(
    tmp_path: Path,
) -> None:
    original = tmp_path / "original"
    (item,) = _legacy_root(original)
    payload = b"retained released staging bytes"
    count = 129
    with sqlite3.connect(_journal(original)) as connection:
        for number in range(1, count + 1):
            token = number.to_bytes(32, "big")
            leaf = sha256(token).hexdigest() + ".cbz"
            path = original / ".h2hdb-state" / "staging" / leaf
            path.write_bytes(payload)
            connection.execute(
                "INSERT INTO protection_tokens VALUES "
                "(?, ?, ?, ?, ?, ?, 'RELEASED', ?, ?, ?, ?, ?)",
                (
                    token,
                    item.storage_object.key.codec,
                    "/".join(item.storage_object.key.segments),
                    sha256(payload).digest(),
                    len(payload),
                    _MODIFIED.isoformat(),
                    leaf,
                    *_stat_values(path),
                ),
            )
    moved = tmp_path / "moved"
    copytree(original, moved)

    result = relocate_library(moved, batch_size=32, _upgrade=upgrade_v3)

    assert result.complete
    with sqlite3.connect(_journal(moved)) as connection:
        rows = connection.execute(
            "SELECT staging_leaf, state, device, inode, modified_ns, changed_ns "
            "FROM protection_tokens ORDER BY token"
        ).fetchall()
    assert len(rows) == count
    for leaf, state, *signature in rows:
        path = moved / ".h2hdb-state" / "staging" / leaf
        assert state == "RELEASED"
        assert path.read_bytes() == payload
        assert tuple(signature) == _stat_values(path)
    _assert_current_authority(moved)


def test_relocation_session_rejects_changed_storage_uuid(tmp_path: Path) -> None:
    root = tmp_path / "library"
    _legacy_root(root)
    assert not relocate_library_step(root, _upgrade=upgrade_v3).complete
    with sqlite3.connect(_journal(root)) as connection:
        connection.execute(
            "UPDATE library_storage_identity SET storage_instance_uuid = ?",
            (bytes.fromhex("123456789abc40008000123456789abd"),),
        )

    with pytest.raises(RuntimeError, match="UUID"):
        relocate_library_step(root, _upgrade=upgrade_v3)


def test_explicit_relocation_revalidates_after_completed_session(
    tmp_path: Path,
) -> None:
    root = tmp_path / "library"
    (item,) = _legacy_root(root)
    first = relocate_library(root, _upgrade=upgrade_v3)
    target = _target(root, item.storage_object.key)
    target.chmod(0o600 if target.stat().st_mode & 0o777 != 0o600 else 0o644)

    second = relocate_library(root)

    assert second.complete
    assert second.session_id != first.session_id
    _assert_current_authority(root)


def test_runtime_rejects_active_relocation_before_private_layout_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    _legacy_root(root)
    assert not relocate_library_step(root, _upgrade=upgrade_v3).complete

    def reject_layout_mutation(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("active relocation must fence layout writes")

    monkeypatch.setattr(
        library_module, "_ensure_managed_directory", reject_layout_mutation
    )
    with pytest.raises(RuntimeError, match="relocat"):
        _adapter(root).ensure_storage_identity()


def test_relocation_cannot_cross_protect_io_after_state_lock_is_released(
    tmp_path: Path,
) -> None:
    root = tmp_path / "library"
    _legacy_root(root)
    _upgrade(root)
    adapter = _adapter(root)
    payload = b"writer retains publication lock while source I/O runs"
    item = _item(2, payload)
    entered = Event()
    proceed = Event()
    errors: list[BaseException] = []

    class BlockedSource(BytesIO):
        def read(self, size: int | None = -1) -> bytes:
            entered.set()
            if not proceed.wait(5):
                raise RuntimeError("timed out releasing the protected source")
            return super().read(size)

    def protect() -> None:
        descriptor = item.storage_object
        try:
            assert adapter.protect(
                BlockedSource(payload),
                descriptor.key,
                bytes.fromhex(descriptor.sha256),
                descriptor.size_bytes,
                descriptor.modified_at,
                b"w" * 32,
            ).stored
        except BaseException as error:  # relay a test thread's failure to its owner
            errors.append(error)

    writer = Thread(target=protect)
    writer.start()
    try:
        assert entered.wait(5)
        with (root / ".h2hdb-state" / "locks" / "state.lock").open("rb") as state:
            fcntl.flock(state.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(state.fileno(), fcntl.LOCK_UN)
        with sqlite3.connect(_journal(root)) as connection:
            assert connection.execute(
                "SELECT state FROM protection_tokens WHERE token = ?", (b"w" * 32,)
            ).fetchone() == ("WRITING",)
        with pytest.raises(BlockingIOError):
            relocate_library_step(root)
        with sqlite3.connect(_journal(root)) as connection:
            assert (
                connection.execute(
                    "SELECT * FROM library_relocation_session"
                ).fetchall()
                == []
            )
    finally:
        proceed.set()
        writer.join(5)
    assert not writer.is_alive()
    assert errors == []
    with sqlite3.connect(_journal(root)) as connection:
        assert connection.execute(
            "SELECT state FROM protection_tokens WHERE token = ?", (b"w" * 32,)
        ).fetchone() == ("STAGED",)


@pytest.mark.parametrize(
    "invalid_uuid",
    (
        bytes(16),
        bytes.fromhex("123456789abc50008000123456789abc"),
        bytes.fromhex("123456789abc40000000123456789abc"),
    ),
    ids=("nil", "version-five", "invalid-variant"),
)
def test_relocation_rejects_invalid_storage_uuid_before_upgrading(
    tmp_path: Path,
    invalid_uuid: bytes,
) -> None:
    root = tmp_path / "library"
    _legacy_root(root)
    with sqlite3.connect(_journal(root)) as connection:
        connection.execute(
            "UPDATE library_storage_identity SET storage_instance_uuid = ?",
            (invalid_uuid,),
        )
    original_database = _journal(root).read_bytes()

    with pytest.raises(RuntimeError, match="UUIDv4"):
        relocate_library(root, _upgrade=upgrade_v3)

    assert _journal(root).read_bytes() == original_database
    with sqlite3.connect(_journal(root)) as connection:
        assert connection.execute(
            "SELECT format_version FROM library_state"
        ).fetchone() == (3,)
