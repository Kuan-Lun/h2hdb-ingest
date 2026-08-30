from __future__ import annotations

import ctypes
import fcntl
import json
import os
import stat
import sys
from collections.abc import Callable
from hashlib import sha256
from io import BytesIO
from itertools import pairwise
from pathlib import Path
from typing import cast

import pytest
from h2hdb import (
    LibraryActivationStatus,
    VNextLibraryActivationItem,
    artifact_storage_key,
)

import h2hdb_ingest.library as library_module
from h2hdb_ingest.library import ManagedFilesystemLibraryAdapter
from h2hdb_ingest.maintenance import LibraryMaintenanceOutcome


def _publication_key(gid: int) -> bytes:
    digest = sha256(b"h2hdb-vnext-publication-key\0")
    digest.update((1).to_bytes(4, "big"))
    digest.update(gid.to_bytes(8, "big"))
    return digest.digest()


def _item(gid: int, payload: bytes) -> VNextLibraryActivationItem:
    return VNextLibraryActivationItem(
        publication_key=_publication_key(gid),
        gid=gid,
        source_gallery_name=f"gallery-{gid}",
        upload_time=0,
        storage_key=artifact_storage_key(gid),
        artifact_sha256=sha256(payload).digest(),
        size_bytes=len(payload),
    )


def _adapter(root: Path) -> ManagedFilesystemLibraryAdapter:
    if not root.exists():
        _provision_library_root(root)
    return ManagedFilesystemLibraryAdapter(root, max_image_short_side=768)


def _provision_library_root(root: Path) -> None:
    root.mkdir(mode=0o700, exist_ok=True)
    root.chmod(0o700)
    for path in (root / "current", root / ".h2hdb-coordination"):
        path.mkdir(mode=0o755, exist_ok=True)
        path.chmod(0o755)


class _PartialThenError(BytesIO):
    def __init__(self, payload: bytes) -> None:
        super().__init__(payload)
        self._first_read = True

    def read(self, size: int | None = -1) -> bytes:
        if self._first_read:
            self._first_read = False
            bound = 3 if size is None or size < 0 else min(3, size)
            return super().read(bound)
        raise RuntimeError("fault: interrupted staging write")


def _protect(
    adapter: ManagedFilesystemLibraryAdapter,
    item: VNextLibraryActivationItem,
    payload: bytes,
    token_byte: int,
) -> bytes:
    token = bytes((token_byte,)) * 184
    evidence = adapter.protect(
        BytesIO(payload),
        item.storage_key,
        item.artifact_sha256,
        item.size_bytes,
        token,
    )
    assert evidence.stored
    return token


def _activate(
    adapter: ManagedFilesystemLibraryAdapter,
    revision: int,
    receipt: bytes,
    items: tuple[VNextLibraryActivationItem, ...],
) -> None:
    with adapter.publication_guard():
        checkpoint = adapter.begin(revision, receipt)
        assert checkpoint.status is LibraryActivationStatus.SPOOL
        adapter.activate_page(revision, items)
        adapter.seal(revision)
        while True:
            checkpoint = adapter.reconcile_page(revision, receipt, limit=128)
            if checkpoint.status is LibraryActivationStatus.READY:
                break
        adapter.complete(revision, receipt)


def test_layout_requires_a_preexisting_real_library_root(tmp_path: Path) -> None:
    root = tmp_path / "missing-library"
    adapter = ManagedFilesystemLibraryAdapter(root, max_image_short_side=768)

    with pytest.raises(RuntimeError, match="pre-existing real directory"):
        with adapter.publication_guard():
            pass

    assert not root.exists()


def test_layout_durably_accepts_preexisting_reader_mount_roots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    _provision_library_root(root)
    current = root / "current"
    coordination = root / ".h2hdb-coordination"
    expected_identities = {
        (current.stat().st_dev, current.stat().st_ino),
        (coordination.stat().st_dev, coordination.stat().st_ino),
    }
    fsynced: list[tuple[int, int]] = []
    original_fsync = os.fsync

    def record_fsync(descriptor: int) -> None:
        value = os.fstat(descriptor)
        fsynced.append((value.st_dev, value.st_ino))
        original_fsync(descriptor)

    monkeypatch.setattr("h2hdb_ingest.library.os.fsync", record_fsync)
    adapter = ManagedFilesystemLibraryAdapter(root, max_image_short_side=768)
    adapter._ensure_layout()

    assert {
        (current.stat().st_dev, current.stat().st_ino),
        (coordination.stat().st_dev, coordination.stat().st_ino),
    } == expected_identities
    assert expected_identities <= set(fsynced)
    assert stat.S_IMODE(current.stat().st_mode) == 0o755
    assert stat.S_IMODE(coordination.stat().st_mode) == 0o755
    assert stat.S_IMODE((coordination / "publication.lock").stat().st_mode) == 0o644
    assert stat.S_IMODE((root / ".h2hdb-state").stat().st_mode) == 0o700


def test_layout_replay_never_chmods_existing_managed_entries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    adapter._ensure_layout()

    def reject_fchmod(descriptor: int, mode: int) -> None:
        del descriptor, mode
        raise AssertionError("existing managed entries must not be chmodded")

    monkeypatch.setattr("h2hdb_ingest.library.os.fchmod", reject_fchmod)
    ManagedFilesystemLibraryAdapter(root, max_image_short_side=768)._ensure_layout()


def test_layout_rejects_precreated_reader_mode_without_mutation(
    tmp_path: Path,
) -> None:
    root = tmp_path / "library"
    _provision_library_root(root)
    current = root / "current"
    current.chmod(0o777)

    adapter = ManagedFilesystemLibraryAdapter(root, max_image_short_side=768)
    with pytest.raises(RuntimeError) as caught:
        adapter._ensure_layout()

    message = str(caught.value)
    assert "current library has unsafe host metadata" in message
    assert "actual_mode=0o777" in message
    assert "expected_mode=0o755" in message
    assert stat.S_IMODE(current.stat().st_mode) == 0o777
    assert not (root / ".h2hdb-state").exists()


def test_layout_rejects_private_directory_mode_drift_without_repair(
    tmp_path: Path,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    adapter._ensure_layout()
    state = root / ".h2hdb-state"
    state.chmod(0o755)

    with pytest.raises(RuntimeError, match="library state has unsafe host metadata"):
        ManagedFilesystemLibraryAdapter(
            root,
            max_image_short_side=768,
        )._ensure_layout()

    assert stat.S_IMODE(state.stat().st_mode) == 0o755


def test_layout_rejects_existing_lock_mode_drift_without_repair(
    tmp_path: Path,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    adapter._ensure_layout()
    state_lock = root / ".h2hdb-state" / "locks" / "state.lock"
    state_lock.chmod(0o644)

    with pytest.raises(RuntimeError, match="changed durable identity or mode"):
        ManagedFilesystemLibraryAdapter(
            root,
            max_image_short_side=768,
        )._ensure_layout()

    assert stat.S_IMODE(state_lock.stat().st_mode) == 0o644


def test_layout_rejects_database_mode_before_sqlite_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    adapter._ensure_layout()
    database = root / ".h2hdb-state" / "journal" / "library-activation.sqlite3"
    database.chmod(0o644)

    def reject_connect(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError("unsafe database metadata must fail before SQLite open")

    monkeypatch.setattr("h2hdb_ingest.library.sqlite3.connect", reject_connect)
    with pytest.raises(RuntimeError, match="changed durable identity or mode"):
        ManagedFilesystemLibraryAdapter(
            root,
            max_image_short_side=768,
        )._ensure_layout()

    assert stat.S_IMODE(database.stat().st_mode) == 0o644


def test_layout_rejects_foreign_owned_preexisting_child_before_chmod(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    current = root / "current"
    current.mkdir(mode=0o777)
    current.chmod(0o777)
    actual_uid = current.stat().st_uid
    expected_uid = actual_uid + 1
    monkeypatch.setattr("h2hdb_ingest.library.os.geteuid", lambda: expected_uid)

    with pytest.raises(RuntimeError) as caught:
        library_module._ensure_managed_directory(
            current,
            0o755,
            label="current library",
        )

    message = str(caught.value)
    assert "current library owner UID mismatch" in message
    assert f"actual_uid={actual_uid}" in message
    assert f"expected_uid={expected_uid}" in message
    assert stat.S_IMODE(current.stat().st_mode) == 0o777


@pytest.mark.parametrize("legacy_kind", ("directory", "file", "symlink"))
def test_layout_rejects_legacy_coordination_before_creating_new_sibling(
    tmp_path: Path,
    legacy_kind: str,
) -> None:
    root = tmp_path / "library"
    _provision_library_root(root)
    state = root / ".h2hdb-state"
    state.mkdir(mode=0o700)
    state.chmod(0o700)
    legacy = state / "coordination"
    if legacy_kind == "directory":
        legacy.mkdir(mode=0o755)
    elif legacy_kind == "file":
        legacy.write_bytes(b"legacy")
    else:
        outside = tmp_path / "outside"
        outside.mkdir()
        legacy.symlink_to(outside, target_is_directory=True)

    adapter = ManagedFilesystemLibraryAdapter(root, max_image_short_side=768)
    with pytest.raises(RuntimeError, match="unsupported legacy"):
        adapter._ensure_layout()

    assert legacy.exists() or legacy.is_symlink()
    assert (root / ".h2hdb-coordination").is_dir()


def test_layout_mkdir_response_loss_replays_child_before_parent_fsync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    _provision_library_root(root)
    adapter = ManagedFilesystemLibraryAdapter(root, max_image_short_side=768)
    original_mkdir = os.mkdir
    interrupted = False

    def mkdir_then_interrupt(
        path: str | bytes | Path,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> None:
        nonlocal interrupted
        original_mkdir(path, mode, dir_fd=dir_fd)
        if str(path) == ".h2hdb-state" and not interrupted:
            interrupted = True
            raise RuntimeError("fault: layout-mkdir-return-lost")

    with monkeypatch.context() as scoped:
        scoped.setattr("h2hdb_ingest.library.os.mkdir", mkdir_then_interrupt)
        with pytest.raises(RuntimeError, match="layout-mkdir-return-lost"):
            adapter._ensure_layout()

    assert (root / ".h2hdb-state").is_dir()
    fsynced: list[tuple[int, int]] = []
    original_fsync = os.fsync

    def record_fsync(descriptor: int) -> None:
        value = os.fstat(descriptor)
        fsynced.append((value.st_dev, value.st_ino))
        original_fsync(descriptor)

    with monkeypatch.context() as scoped:
        scoped.setattr("h2hdb_ingest.library.os.fsync", record_fsync)
        adapter._ensure_layout()

    state = root / ".h2hdb-state"
    expected_edges = (
        (root / "current", root),
        (root / ".h2hdb-coordination", root),
        (state, root),
        (state / "staging", state),
        (state / "quarantine", state),
        (state / "journal", state),
        (state / "locks", state),
        (state / "locks" / "state.lock", state / "locks"),
        (
            root / ".h2hdb-coordination" / "publication.lock",
            root / ".h2hdb-coordination",
        ),
        (state / "journal" / "library-activation.sqlite3", state / "journal"),
    )
    observed_pairs = set(pairwise(fsynced))
    for child, parent in expected_edges:
        child_identity = (child.stat().st_dev, child.stat().st_ino)
        parent_identity = (parent.stat().st_dev, parent.stat().st_ino)
        assert (child_identity, parent_identity) in observed_pairs

    coordination = root / ".h2hdb-coordination"
    assert stat.S_IMODE((root / "current").stat().st_mode) == 0o755
    assert stat.S_IMODE(coordination.stat().st_mode) == 0o755
    assert stat.S_IMODE((coordination / "publication.lock").stat().st_mode) == 0o644
    assert stat.S_IMODE(state.stat().st_mode) == 0o700
    for private_child in ("staging", "quarantine", "journal", "locks"):
        assert stat.S_IMODE((state / private_child).stat().st_mode) == 0o700
    assert stat.S_IMODE((state / "locks" / "state.lock").stat().st_mode) == 0o600
    assert {path.name for path in state.iterdir()} == {
        "journal",
        "locks",
        "quarantine",
        "staging",
    }


def test_layout_child_swap_after_parent_fsync_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    _provision_library_root(root)
    adapter = ManagedFilesystemLibraryAdapter(root, max_image_short_side=768)
    root_identity = (root.stat().st_dev, root.stat().st_ino)
    current = root / "current"
    saved = root / "saved-current"
    original_fsync = os.fsync
    swapped = False

    def swap_after_parent_fsync(descriptor: int) -> None:
        nonlocal swapped
        original_fsync(descriptor)
        value = os.fstat(descriptor)
        if (value.st_dev, value.st_ino) == root_identity and not swapped:
            current.rename(saved)
            current.mkdir(mode=0o755)
            swapped = True

    monkeypatch.setattr("h2hdb_ingest.library.os.fsync", swap_after_parent_fsync)
    with pytest.raises(RuntimeError, match="changed identity"):
        adapter._ensure_layout()

    assert swapped
    assert current.is_dir()
    assert saved.is_dir()


def test_activation_moves_staging_into_one_canonical_current_file(
    tmp_path: Path,
) -> None:
    adapter = _adapter(tmp_path / "library")
    payload = b"canonical-cbz"
    item = _item(1234567, payload)
    _protect(adapter, item, payload, 1)

    _activate(adapter, 1, b"r" * 16, (item,))

    target = adapter.current_path.joinpath(*item.storage_key.segments)
    assert target.read_bytes() == payload
    assert stat.S_IMODE(target.stat().st_mode) == 0o644
    for shard in (target.parent, target.parent.parent, target.parent.parent.parent):
        assert stat.S_IMODE(shard.stat().st_mode) == 0o755
    assert item.storage_key.segments[0] == "hash-v1"
    assert len(item.storage_key.segments[1]) == 2
    assert len(item.storage_key.segments[2]) == 1
    state = tmp_path / "library" / ".h2hdb-state"
    assert not list((state / "staging").glob("*.cbz"))
    assert not list((state / "quarantine").glob("*.cbz"))
    assert not (tmp_path / "library" / ".h2hdb-coordination" / "ACTIVATING").exists()
    assert list((tmp_path / "library").rglob("*.cbz")) == [target]


def test_existing_shard_mode_drift_fails_without_repair(tmp_path: Path) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    payload = b"canonical-cbz"
    item = _item(7654321, payload)
    _protect(adapter, item, payload, 1)
    _activate(adapter, 1, b"r" * 16, (item,))
    shard = adapter.current_path / item.storage_key.segments[0]
    shard.chmod(0o777)

    with pytest.raises(RuntimeError, match="library shard mode mismatch"):
        with library_module._open_directory_chain(
            adapter.current_path,
            item.storage_key.segments[:-1],
            create=True,
        ):
            pass

    assert stat.S_IMODE(shard.stat().st_mode) == 0o777


def test_replacement_keeps_open_reader_inode_and_removes_old_persistent_name(
    tmp_path: Path,
) -> None:
    adapter = _adapter(tmp_path / "library")
    first = _item(91, b"first")
    _protect(adapter, first, b"first", 1)
    _activate(adapter, 1, b"a" * 16, (first,))
    target = adapter.current_path.joinpath(*first.storage_key.segments)

    second = _item(91, b"second")
    _protect(adapter, second, b"second", 2)
    with target.open("rb") as opened, adapter.publication_guard():
        adapter.begin(2, b"b" * 16)
        adapter.activate_page(2, (second,))
        adapter.seal(2)
        while True:
            checkpoint = adapter.reconcile_page(2, b"b" * 16, limit=128)
            if checkpoint.status is LibraryActivationStatus.READY:
                break
        assert opened.read() == b"first"
        assert target.read_bytes() == b"second"
        adapter.complete(2, b"b" * 16)

    assert list((tmp_path / "library").rglob("*.cbz")) == [target]
    assert target.stat().st_nlink == 1
    _activate(adapter, 3, b"c" * 16, (second,))
    assert target.read_bytes() == b"second"
    assert target.stat().st_nlink == 1


def test_byte_identical_staged_candidate_is_renamed_over_old_current(
    tmp_path: Path,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(92, b"same")
    _protect(adapter, item, b"same", 91)
    _activate(adapter, 1, b"a" * 16, (item,))
    target = adapter.current_path.joinpath(*item.storage_key.segments)
    old_inode = (target.stat().st_dev, target.stat().st_ino)

    token = _protect(adapter, item, b"same", 92)
    stage = root / ".h2hdb-state" / "staging" / f"{sha256(token).hexdigest()}.cbz"
    stage_inode = (stage.stat().st_dev, stage.stat().st_ino)
    assert stage_inode != old_inode

    _activate(adapter, 2, b"b" * 16, (item,))

    assert (target.stat().st_dev, target.stat().st_ino) == stage_inode
    assert target.stat().st_nlink == 1
    assert not stage.exists()
    assert not list((root / ".h2hdb-state" / "quarantine").iterdir())


def test_ready_marker_survives_process_stop_and_replay_clears_it(
    tmp_path: Path,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(77, b"payload")
    _protect(adapter, item, b"payload", 7)
    receipt = b"z" * 16
    with adapter.publication_guard():
        adapter.begin(1, receipt)
        adapter.activate_page(1, (item,))
        adapter.seal(1)
        while True:
            checkpoint = adapter.reconcile_page(1, receipt, limit=128)
            if checkpoint.status is LibraryActivationStatus.READY:
                break

    marker = root / ".h2hdb-coordination" / "ACTIVATING"
    assert json.loads(marker.read_text(encoding="ascii")) == {
        "format": "h2hdb-library-activation-v1",
        "receipt_id": receipt.hex(),
        "revision": 1,
    }
    assert stat.S_IMODE(marker.stat().st_mode) == 0o644

    restarted = _adapter(root)
    with restarted.publication_guard():
        checkpoint = restarted.begin(1, receipt)
        assert checkpoint.status is LibraryActivationStatus.READY
        restarted.reconcile_page(1, receipt, limit=128)
        restarted.complete(1, receipt)

    assert not marker.exists()


def test_normal_stop_after_first_bounded_activation_page_restarts(
    tmp_path: Path,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item_payloads = tuple((gid, f"payload-{gid}".encode()) for gid in range(1, 130))
    items = tuple(
        sorted(
            (_item(gid, payload) for gid, payload in item_payloads),
            key=lambda item: item.publication_key,
        )
    )
    payload_by_gid = dict(item_payloads)
    for item in items:
        _protect(adapter, item, payload_by_gid[item.gid], item.gid)

    receipt = b"m" * 16
    with adapter.publication_guard():
        adapter.begin(1, receipt)
        adapter.activate_page(1, items[:128])
        adapter.activate_page(1, items[128:])
        adapter.seal(1)
        checkpoint = adapter.reconcile_page(1, receipt, limit=128)
        assert checkpoint.status is LibraryActivationStatus.RECONCILE
        assert checkpoint.cursor is not None

    marker = root / ".h2hdb-coordination" / "ACTIVATING"
    assert marker.is_file()
    assert len(tuple(adapter.current_path.rglob("*.cbz"))) == 128

    restarted = _adapter(root)
    with restarted.publication_guard():
        checkpoint = restarted.begin(1, receipt)
        assert checkpoint.status is LibraryActivationStatus.RECONCILE
        while True:
            checkpoint = restarted.reconcile_page(1, receipt, limit=128)
            if checkpoint.status is LibraryActivationStatus.READY:
                break
        restarted.complete(1, receipt)

    assert len(tuple(restarted.current_path.rglob("*.cbz"))) == 129
    assert not list((root / ".h2hdb-state" / "staging").glob("*.cbz"))
    assert not marker.exists()


def test_completion_is_constant_state_flip_and_private_spool_cleanup_is_bounded(
    tmp_path: Path,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    payloads = {gid: f"payload-{gid}".encode() for gid in range(180, 189)}
    items = tuple(
        sorted(
            (_item(gid, payload) for gid, payload in payloads.items()),
            key=lambda item: item.publication_key,
        )
    )
    for item in items:
        _protect(adapter, item, payloads[item.gid], item.gid)

    _activate(adapter, 1, b"c" * 16, items)

    assert adapter.maintain_cleanup() is LibraryMaintenanceOutcome.PROGRESSED
    assert adapter.maintain_cleanup() is LibraryMaintenanceOutcome.DONE
    assert len(tuple(adapter.current_path.rglob("*.cbz"))) == len(items)


def test_publication_exclusive_lock_survives_complete_until_guard_exit(
    tmp_path: Path,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(130, b"payload")
    _protect(adapter, item, b"payload", 130)
    receipt = b"l" * 16
    lock_path = root / ".h2hdb-coordination" / "publication.lock"

    def shared_lock_is_blocked() -> bool:
        descriptor = os.open(lock_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            return False
        finally:
            os.close(descriptor)

    with adapter.publication_guard():
        adapter.begin(1, receipt)
        assert shared_lock_is_blocked()
        adapter.activate_page(1, (item,))
        adapter.seal(1)
        checkpoint = adapter.reconcile_page(1, receipt, limit=128)
        while checkpoint.status is not LibraryActivationStatus.READY:
            checkpoint = adapter.reconcile_page(1, receipt, limit=128)
        assert shared_lock_is_blocked()
        adapter.complete(1, receipt)
        assert shared_lock_is_blocked()

    assert not shared_lock_is_blocked()


@pytest.mark.parametrize(
    "boundary",
    (
        "marker-written",
        "operation-authorized",
        "rename-return-lost",
        "installed-before-journal",
        "journal-transaction-aborted",
        "entry-journal-return-lost",
    ),
)
def test_install_fault_boundaries_recover_exact_current_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(140, b"payload")
    _protect(adapter, item, b"payload", 140)
    receipt = b"f" * 16

    with monkeypatch.context() as scoped:
        if boundary == "marker-written":
            original = cast(
                Callable[..., object],
                adapter._write_marker,
            )

            def marker_then_interrupt(*args: object, **kwargs: object) -> object:
                original(*args, **kwargs)
                raise RuntimeError("fault: marker-written")

            scoped.setattr(adapter, "_write_marker", marker_then_interrupt)
        elif boundary == "operation-authorized":

            def reject_install(*args: object, **kwargs: object) -> object:
                del args, kwargs
                raise RuntimeError("fault: operation-authorized")

            scoped.setattr(adapter, "_install_staged", reject_install)
        elif boundary == "rename-return-lost":
            original_rename = library_module._rename_noreplace

            def rename_then_interrupt(
                source: str,
                destination: str,
                *,
                source_descriptor: int,
                destination_descriptor: int,
            ) -> None:
                original_rename(
                    source,
                    destination,
                    source_descriptor=source_descriptor,
                    destination_descriptor=destination_descriptor,
                )
                if destination.startswith("h2h-"):
                    raise RuntimeError("fault: rename-return-lost")

            scoped.setattr(
                library_module,
                "_rename_noreplace",
                rename_then_interrupt,
            )
        elif boundary in {
            "installed-before-journal",
            "journal-transaction-aborted",
        }:
            original = cast(
                Callable[..., object],
                adapter._terminalize_stage_in_transaction,
            )

            def terminalize_with_interrupt(
                *args: object,
                **kwargs: object,
            ) -> object:
                if boundary == "installed-before-journal":
                    raise RuntimeError(f"fault: {boundary}")
                original(*args, **kwargs)
                raise RuntimeError(f"fault: {boundary}")

            scoped.setattr(
                adapter,
                "_terminalize_stage_in_transaction",
                terminalize_with_interrupt,
            )
        else:
            original = cast(
                Callable[..., object],
                adapter._activate_pending,
            )

            def activate_then_interrupt(*args: object, **kwargs: object) -> object:
                original(*args, **kwargs)
                raise RuntimeError("fault: entry-journal-return-lost")

            scoped.setattr(adapter, "_activate_pending", activate_then_interrupt)

        with adapter.publication_guard():
            adapter.begin(1, receipt)
            adapter.activate_page(1, (item,))
            adapter.seal(1)
            with pytest.raises(RuntimeError, match="fault:"):
                adapter.reconcile_page(1, receipt, limit=128)

    restarted = _adapter(root)
    with restarted.publication_guard():
        checkpoint = restarted.begin(1, receipt)
        while checkpoint.status is not LibraryActivationStatus.READY:
            checkpoint = restarted.reconcile_page(1, receipt, limit=128)
        restarted.complete(1, receipt)

    target = restarted.current_path.joinpath(*item.storage_key.segments)
    assert target.read_bytes() == b"payload"
    assert list(root.rglob("*.cbz")) == [target]
    assert not (root / ".h2hdb-coordination" / "ACTIVATING").exists()


def test_renamed_stage_authority_and_current_entry_commit_atomically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(141, b"payload")
    token = _protect(adapter, item, b"payload", 141)
    original_terminalize = cast(
        Callable[..., object],
        adapter._terminalize_stage_in_transaction,
    )

    def terminalize_then_interrupt(*args: object, **kwargs: object) -> object:
        original_terminalize(*args, **kwargs)
        raise RuntimeError("fault: journal-transaction-aborted")

    with monkeypatch.context() as scoped:
        scoped.setattr(
            adapter,
            "_terminalize_stage_in_transaction",
            terminalize_then_interrupt,
        )
        with adapter.publication_guard():
            adapter.begin(1, b"j" * 16)
            adapter.activate_page(1, (item,))
            adapter.seal(1)
            with pytest.raises(RuntimeError, match="journal-transaction-aborted"):
                adapter.reconcile_page(1, b"j" * 16, limit=128)

    target = adapter.current_path.joinpath(*item.storage_key.segments)
    assert target.read_bytes() == b"payload"
    with adapter._exclusive_state() as connection:
        assert connection.execute(
            "SELECT state, staging_leaf IS NOT NULL, device IS NOT NULL "
            "FROM protection_tokens WHERE token = ?",
            (token,),
        ).fetchone() == ("STAGED", 1, 1)
        assert connection.execute(
            "SELECT activated FROM pending_entries WHERE activation_revision = 1"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT COUNT(*) FROM current_entries"
        ).fetchone() == (0,)

    restarted = _adapter(root)
    with restarted.publication_guard():
        checkpoint = restarted.begin(1, b"j" * 16)
        while checkpoint.status is not LibraryActivationStatus.READY:
            checkpoint = restarted.reconcile_page(1, b"j" * 16, limit=128)
        restarted.complete(1, b"j" * 16)


def test_rename_response_loss_replay_fsyncs_both_directories(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(142, b"payload")
    _protect(adapter, item, b"payload", 142)
    original_rename = library_module._rename_noreplace

    def rename_then_interrupt(
        source: str,
        destination: str,
        *,
        source_descriptor: int,
        destination_descriptor: int,
    ) -> None:
        original_rename(
            source,
            destination,
            source_descriptor=source_descriptor,
            destination_descriptor=destination_descriptor,
        )
        if destination.startswith("h2h-"):
            raise RuntimeError("fault: rename-before-directory-fsync")

    receipt = b"k" * 16
    with monkeypatch.context() as scoped:
        scoped.setattr(
            library_module,
            "_rename_noreplace",
            rename_then_interrupt,
        )
        with adapter.publication_guard():
            adapter.begin(1, receipt)
            adapter.activate_page(1, (item,))
            adapter.seal(1)
            with pytest.raises(RuntimeError, match="rename-before-directory-fsync"):
                adapter.reconcile_page(1, receipt, limit=128)

    target = adapter.current_path.joinpath(*item.storage_key.segments)
    staging = root / ".h2hdb-state" / "staging"
    identities = {
        "current": (target.parent.stat().st_dev, target.parent.stat().st_ino),
        "staging": (staging.stat().st_dev, staging.stat().st_ino),
    }
    fsynced: list[tuple[int, int]] = []
    original_fsync = os.fsync

    def record_fsync(descriptor: int) -> None:
        value = os.fstat(descriptor)
        fsynced.append((value.st_dev, value.st_ino))
        original_fsync(descriptor)

    restarted = _adapter(root)
    with monkeypatch.context() as scoped:
        scoped.setattr("h2hdb_ingest.library.os.fsync", record_fsync)
        with restarted.publication_guard():
            checkpoint = restarted.begin(1, receipt)
            while checkpoint.status is not LibraryActivationStatus.READY:
                checkpoint = restarted.reconcile_page(1, receipt, limit=128)
            restarted.complete(1, receipt)

    assert identities["current"] in fsynced
    assert identities["staging"] in fsynced


def test_recovery_rejects_byte_identical_foreign_current_inode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(143, b"payload")
    token = _protect(adapter, item, b"payload", 143)
    receipt = b"n" * 16

    with monkeypatch.context() as scoped:

        def interrupt_after_authorization(*args: object, **kwargs: object) -> object:
            del args, kwargs
            raise RuntimeError("fault: operation-authorized")

        scoped.setattr(adapter, "_install_staged", interrupt_after_authorization)
        with adapter.publication_guard():
            adapter.begin(1, receipt)
            adapter.activate_page(1, (item,))
            adapter.seal(1)
            with pytest.raises(RuntimeError, match="operation-authorized"):
                adapter.reconcile_page(1, receipt, limit=128)

    stage = root / ".h2hdb-state" / "staging" / f"{sha256(token).hexdigest()}.cbz"
    saved = tmp_path / "authorized-stage.cbz"
    stage.rename(saved)
    target = adapter.current_path.joinpath(*item.storage_key.segments)
    target.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
    target.write_bytes(b"payload")
    assert (target.stat().st_dev, target.stat().st_ino) != (
        saved.stat().st_dev,
        saved.stat().st_ino,
    )

    restarted = _adapter(root)
    with restarted.publication_guard():
        restarted.begin(1, receipt)
        with pytest.raises(RuntimeError, match="authority disagree"):
            restarted.reconcile_page(1, receipt, limit=128)

    assert target.read_bytes() == b"payload"
    assert saved.read_bytes() == b"payload"


def test_stage_current_same_inode_duplicate_heals_to_current(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(184, b"payload")
    token = _protect(adapter, item, b"payload", 184)
    receipt = b"y" * 16

    with monkeypatch.context() as scoped:

        def interrupt_after_authorization(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("fault: operation-authorized")

        scoped.setattr(adapter, "_install_staged", interrupt_after_authorization)
        with adapter.publication_guard():
            adapter.begin(1, receipt)
            adapter.activate_page(1, (item,))
            adapter.seal(1)
            with pytest.raises(RuntimeError, match="operation-authorized"):
                adapter.reconcile_page(1, receipt, limit=128)

    stage = root / ".h2hdb-state" / "staging" / f"{sha256(token).hexdigest()}.cbz"
    target = adapter.current_path.joinpath(*item.storage_key.segments)
    target.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
    os.link(stage, target)

    restarted = _adapter(root)
    with restarted.publication_guard():
        checkpoint = restarted.begin(1, receipt)
        while checkpoint.status is not LibraryActivationStatus.READY:
            checkpoint = restarted.reconcile_page(1, receipt, limit=128)
        restarted.complete(1, receipt)

    assert not stage.exists()
    assert target.read_bytes() == b"payload"
    assert target.stat().st_nlink == 1
    assert list(root.rglob("*.cbz")) == [target]


def test_stage_current_duplicate_unlink_response_loss_replays_current(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(197, b"payload")
    token = _protect(adapter, item, b"payload", 197)
    receipt = b"f" * 16

    with monkeypatch.context() as scoped:

        def interrupt_after_authorization(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("fault: operation-authorized")

        scoped.setattr(adapter, "_install_staged", interrupt_after_authorization)
        with adapter.publication_guard():
            adapter.begin(1, receipt)
            adapter.activate_page(1, (item,))
            adapter.seal(1)
            with pytest.raises(RuntimeError, match="operation-authorized"):
                adapter.reconcile_page(1, receipt, limit=128)

    stage = root / ".h2hdb-state" / "staging" / f"{sha256(token).hexdigest()}.cbz"
    target = adapter.current_path.joinpath(*item.storage_key.segments)
    target.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
    os.link(stage, target)
    original_unlink = os.unlink
    interrupted = False

    def unlink_then_interrupt(
        path: str | bytes | Path,
        *,
        dir_fd: int | None = None,
    ) -> None:
        nonlocal interrupted
        original_unlink(path, dir_fd=dir_fd)
        if str(path) == stage.name and not interrupted:
            interrupted = True
            raise RuntimeError("fault: duplicate-unlink-return-lost")

    restarted = _adapter(root)
    with monkeypatch.context() as scoped:
        scoped.setattr("h2hdb_ingest.library.os.unlink", unlink_then_interrupt)
        with restarted.publication_guard():
            restarted.begin(1, receipt)
            with pytest.raises(RuntimeError, match="duplicate-unlink-return-lost"):
                restarted.reconcile_page(1, receipt, limit=128)

    assert interrupted
    assert not stage.exists()
    assert target.read_bytes() == b"payload"
    resumed = _adapter(root)
    with resumed.publication_guard():
        checkpoint = resumed.begin(1, receipt)
        while checkpoint.status is not LibraryActivationStatus.READY:
            checkpoint = resumed.reconcile_page(1, receipt, limit=128)
        resumed.complete(1, receipt)

    assert target.stat().st_nlink == 1
    assert list(root.rglob("*.cbz")) == [target]


def test_stage_current_different_inode_duplicate_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(185, b"payload")
    token = _protect(adapter, item, b"payload", 185)
    receipt = b"z" * 16

    with monkeypatch.context() as scoped:

        def interrupt_after_authorization(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("fault: operation-authorized")

        scoped.setattr(adapter, "_install_staged", interrupt_after_authorization)
        with adapter.publication_guard():
            adapter.begin(1, receipt)
            adapter.activate_page(1, (item,))
            adapter.seal(1)
            with pytest.raises(RuntimeError, match="operation-authorized"):
                adapter.reconcile_page(1, receipt, limit=128)

    stage = root / ".h2hdb-state" / "staging" / f"{sha256(token).hexdigest()}.cbz"
    target = adapter.current_path.joinpath(*item.storage_key.segments)
    target.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
    target.write_bytes(b"payload")
    target.chmod(0o644)

    restarted = _adapter(root)
    with restarted.publication_guard():
        restarted.begin(1, receipt)
        with pytest.raises(RuntimeError, match="do not share exact inode authority"):
            restarted.reconcile_page(1, receipt, limit=128)

    assert stage.read_bytes() == target.read_bytes() == b"payload"
    assert stage.stat().st_ino != target.stat().st_ino


@pytest.mark.parametrize(
    "boundary",
    (
        "removal-authorized",
        "quarantine-return-lost",
        "unlink-return-lost",
        "removal-journal-return-lost",
    ),
)
def test_stale_removal_fault_boundaries_recover_without_unknown_deletion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    retained = _item(150, b"retained")
    removed = _item(151, b"removed")
    _protect(adapter, retained, b"retained", 150)
    _protect(adapter, removed, b"removed", 151)
    first_items = tuple(
        sorted((retained, removed), key=lambda item: item.publication_key)
    )
    _activate(adapter, 1, b"a" * 16, first_items)
    receipt = b"g" * 16

    with monkeypatch.context() as scoped:
        if boundary in {"removal-authorized", "quarantine-return-lost"}:
            original = cast(
                Callable[..., object],
                adapter._quarantine_current,
            )

            def quarantine_with_interrupt(
                *args: object,
                **kwargs: object,
            ) -> object:
                if boundary == "removal-authorized":
                    raise RuntimeError(f"fault: {boundary}")
                original(*args, **kwargs)
                raise RuntimeError(f"fault: {boundary}")

            scoped.setattr(adapter, "_quarantine_current", quarantine_with_interrupt)
        elif boundary == "unlink-return-lost":
            original_unlink = os.unlink

            def unlink_then_interrupt(
                path: str | bytes | Path,
                *,
                dir_fd: int | None = None,
            ) -> None:
                original_unlink(path, dir_fd=dir_fd)
                if dir_fd is not None:
                    raise RuntimeError("fault: unlink-return-lost")

            scoped.setattr("h2hdb_ingest.library.os.unlink", unlink_then_interrupt)
        else:
            original = cast(Callable[..., object], adapter._remove_stale)

            def remove_then_interrupt(*args: object, **kwargs: object) -> object:
                original(*args, **kwargs)
                raise RuntimeError("fault: removal-journal-return-lost")

            scoped.setattr(adapter, "_remove_stale", remove_then_interrupt)

        with adapter.publication_guard():
            adapter.begin(2, receipt)
            adapter.activate_page(2, (retained,))
            adapter.seal(2)
            adapter.reconcile_page(2, receipt, limit=128)
            with pytest.raises(RuntimeError, match="fault:"):
                adapter.reconcile_page(2, receipt, limit=128)

    restarted = _adapter(root)
    with restarted.publication_guard():
        checkpoint = restarted.begin(2, receipt)
        while checkpoint.status is not LibraryActivationStatus.READY:
            checkpoint = restarted.reconcile_page(2, receipt, limit=128)
        restarted.complete(2, receipt)

    retained_path = restarted.current_path.joinpath(*retained.storage_key.segments)
    removed_path = restarted.current_path.joinpath(*removed.storage_key.segments)
    assert retained_path.read_bytes() == b"retained"
    assert not removed_path.exists()
    assert list(root.rglob("*.cbz")) == [retained_path]


@pytest.mark.parametrize(
    "boundary",
    ("current-journal-before-marker-remove", "marker-remove-return-lost"),
)
def test_completion_fault_boundaries_recover_terminal_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(160, b"payload")
    _protect(adapter, item, b"payload", 160)
    receipt = b"h" * 16

    with monkeypatch.context() as scoped, adapter.publication_guard():
        adapter.begin(1, receipt)
        adapter.activate_page(1, (item,))
        adapter.seal(1)
        checkpoint = adapter.reconcile_page(1, receipt, limit=128)
        while checkpoint.status is not LibraryActivationStatus.READY:
            checkpoint = adapter.reconcile_page(1, receipt, limit=128)
        original = cast(Callable[..., object], adapter._remove_marker)

        def remove_marker_with_interrupt(*args: object, **kwargs: object) -> object:
            if boundary == "marker-remove-return-lost":
                original(*args, **kwargs)
            raise RuntimeError(f"fault: {boundary}")

        scoped.setattr(adapter, "_remove_marker", remove_marker_with_interrupt)
        with pytest.raises(RuntimeError, match="fault:"):
            adapter.complete(1, receipt)

    restarted = _adapter(root)
    with restarted.publication_guard():
        checkpoint = restarted.begin(1, receipt)
        assert checkpoint.status is LibraryActivationStatus.COMPLETE

    assert not (root / ".h2hdb-coordination" / "ACTIVATING").exists()


def test_marker_unlink_response_loss_fsyncs_absence_before_complete_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(179, b"payload")
    _protect(adapter, item, b"payload", 179)
    receipt = b"v" * 16
    original_unlink = os.unlink

    def unlink_then_interrupt(
        path: str | bytes | Path,
        *,
        dir_fd: int | None = None,
    ) -> None:
        original_unlink(path, dir_fd=dir_fd)
        if str(path) == "ACTIVATING":
            raise RuntimeError("fault: marker-unlink-return-lost")

    with adapter.publication_guard():
        adapter.begin(1, receipt)
        adapter.activate_page(1, (item,))
        adapter.seal(1)
        checkpoint = adapter.reconcile_page(1, receipt, limit=128)
        while checkpoint.status is not LibraryActivationStatus.READY:
            checkpoint = adapter.reconcile_page(1, receipt, limit=128)
        with monkeypatch.context() as scoped:
            scoped.setattr("h2hdb_ingest.library.os.unlink", unlink_then_interrupt)
            with pytest.raises(RuntimeError, match="marker-unlink-return-lost"):
                adapter.complete(1, receipt)

    coordination = root / ".h2hdb-coordination"
    coordination_identity = (
        coordination.stat().st_dev,
        coordination.stat().st_ino,
    )
    fsynced: list[tuple[int, int]] = []
    original_fsync = os.fsync

    def record_fsync(descriptor: int) -> None:
        value = os.fstat(descriptor)
        fsynced.append((value.st_dev, value.st_ino))
        original_fsync(descriptor)

    restarted = _adapter(root)
    with monkeypatch.context() as scoped:
        scoped.setattr("h2hdb_ingest.library.os.fsync", record_fsync)
        with restarted.publication_guard():
            checkpoint = restarted.begin(1, receipt)

    assert checkpoint.status is LibraryActivationStatus.COMPLETE
    assert coordination_identity in fsynced


def test_unknown_target_fails_closed_before_activation(
    tmp_path: Path,
) -> None:
    adapter = _adapter(tmp_path / "library")
    item = _item(5, b"managed")
    _protect(adapter, item, b"managed", 5)
    target = adapter.current_path.joinpath(*item.storage_key.segments)
    target.parent.mkdir(parents=True)
    target.write_bytes(b"unknown")

    with adapter.publication_guard():
        adapter.begin(1, b"r" * 16)
        adapter.activate_page(1, (item,))
        adapter.seal(1)
        with pytest.raises(RuntimeError, match="unknown library path"):
            adapter.reconcile_page(1, b"r" * 16, limit=128)

    assert target.read_bytes() == b"unknown"


def test_release_is_terminal_and_deletes_only_exact_managed_staging(
    tmp_path: Path,
) -> None:
    adapter = _adapter(tmp_path / "library")
    item = _item(8, b"unused")
    token = _protect(adapter, item, b"unused", 8)

    released = adapter.release(
        item.storage_key,
        item.artifact_sha256,
        item.size_bytes,
        token,
    )

    assert released.released
    assert not list((tmp_path / "library" / ".h2hdb-state" / "staging").glob("*.cbz"))
    assert not adapter.protect(
        BytesIO(b"unused"),
        item.storage_key,
        item.artifact_sha256,
        item.size_bytes,
        token,
    ).stored


def test_staged_unlink_response_loss_fsyncs_absence_before_authority_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(178, b"payload")
    token = _protect(adapter, item, b"payload", 178)
    original_unlink = os.unlink
    interrupted = False

    def unlink_then_interrupt(
        path: str | bytes | Path,
        *,
        dir_fd: int | None = None,
    ) -> None:
        nonlocal interrupted
        original_unlink(path, dir_fd=dir_fd)
        if str(path).endswith(".cbz") and not interrupted:
            interrupted = True
            raise RuntimeError("fault: staged-unlink-return-lost")

    with monkeypatch.context() as scoped:
        scoped.setattr("h2hdb_ingest.library.os.unlink", unlink_then_interrupt)
        with pytest.raises(RuntimeError, match="staged-unlink-return-lost"):
            adapter.release(
                item.storage_key,
                item.artifact_sha256,
                item.size_bytes,
                token,
            )

    staging = root / ".h2hdb-state" / "staging"
    staging_identity = (staging.stat().st_dev, staging.stat().st_ino)
    fsynced: list[tuple[int, int]] = []
    original_fsync = os.fsync

    def record_fsync(descriptor: int) -> None:
        value = os.fstat(descriptor)
        fsynced.append((value.st_dev, value.st_ino))
        original_fsync(descriptor)

    with monkeypatch.context() as scoped:
        scoped.setattr("h2hdb_ingest.library.os.fsync", record_fsync)
        assert adapter.maintain_cleanup() is LibraryMaintenanceOutcome.PROGRESSED

    assert staging_identity in fsynced
    with adapter._exclusive_state() as connection:
        assert connection.execute(
            "SELECT state, staging_leaf FROM protection_tokens WHERE token = ?",
            (token,),
        ).fetchone() == ("RELEASED", None)


def test_staging_rename_response_loss_replays_writing_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(170, b"payload")
    token = bytes((170,)) * 184
    original_rename = library_module._rename_noreplace
    interrupted = False

    def rename_then_interrupt(
        source: str,
        destination: str,
        *,
        source_descriptor: int,
        destination_descriptor: int,
    ) -> None:
        nonlocal interrupted
        original_rename(
            source,
            destination,
            source_descriptor=source_descriptor,
            destination_descriptor=destination_descriptor,
        )
        if destination.endswith(".cbz") and not interrupted:
            interrupted = True
            raise RuntimeError("fault: staging-rename-return-lost")

    with monkeypatch.context() as scoped:
        scoped.setattr(library_module, "_rename_noreplace", rename_then_interrupt)
        with pytest.raises(RuntimeError, match="staging-rename-return-lost"):
            adapter.protect(
                BytesIO(b"payload"),
                item.storage_key,
                item.artifact_sha256,
                item.size_bytes,
                token,
            )

    staging = root / ".h2hdb-state" / "staging"
    staging_identity = (staging.stat().st_dev, staging.stat().st_ino)
    fsynced: list[tuple[int, int]] = []
    original_fsync = os.fsync

    def record_fsync(descriptor: int) -> None:
        value = os.fstat(descriptor)
        fsynced.append((value.st_dev, value.st_ino))
        original_fsync(descriptor)

    with monkeypatch.context() as scoped:
        scoped.setattr("h2hdb_ingest.library.os.fsync", record_fsync)
        assert adapter.protect(
            BytesIO(b"payload"),
            item.storage_key,
            item.artifact_sha256,
            item.size_bytes,
            token,
        ).stored

    assert staging_identity in fsynced
    _activate(adapter, 1, b"p" * 16, (item,))
    assert adapter.current_path.joinpath(*item.storage_key.segments).read_bytes() == (
        b"payload"
    )


def test_staging_publish_same_inode_duplicate_heals_to_final_name(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(180, b"payload")
    token = bytes((180,)) * 184
    original_rename = library_module._rename_noreplace

    def interrupt_before_rename(
        source: str,
        destination: str,
        *,
        source_descriptor: int,
        destination_descriptor: int,
    ) -> None:
        if destination.endswith(".cbz"):
            raise RuntimeError("fault: stage-publish-before-rename")
        original_rename(
            source,
            destination,
            source_descriptor=source_descriptor,
            destination_descriptor=destination_descriptor,
        )

    with monkeypatch.context() as scoped:
        scoped.setattr(library_module, "_rename_noreplace", interrupt_before_rename)
        with pytest.raises(RuntimeError, match="stage-publish-before-rename"):
            adapter.protect(
                BytesIO(b"payload"),
                item.storage_key,
                item.artifact_sha256,
                item.size_bytes,
                token,
            )

    staging = root / ".h2hdb-state" / "staging"
    stem = sha256(token).hexdigest()
    temporary = staging / f".{stem}.tmp"
    final = staging / f"{stem}.cbz"
    os.link(temporary, final)
    assert temporary.stat().st_ino == final.stat().st_ino

    assert adapter.protect(
        BytesIO(b"payload"),
        item.storage_key,
        item.artifact_sha256,
        item.size_bytes,
        token,
    ).stored

    assert not temporary.exists()
    assert final.read_bytes() == b"payload"
    assert final.stat().st_nlink == 1


def test_staging_duplicate_unlink_response_loss_replays_final_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(195, b"payload")
    token = bytes((195,)) * 184

    def interrupt_before_rename(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("fault: stage-publish-before-rename")

    with monkeypatch.context() as scoped:
        scoped.setattr(library_module, "_rename_noreplace", interrupt_before_rename)
        with pytest.raises(RuntimeError, match="stage-publish-before-rename"):
            adapter.protect(
                BytesIO(b"payload"),
                item.storage_key,
                item.artifact_sha256,
                item.size_bytes,
                token,
            )

    staging = root / ".h2hdb-state" / "staging"
    stem = sha256(token).hexdigest()
    temporary = staging / f".{stem}.tmp"
    final = staging / f"{stem}.cbz"
    os.link(temporary, final)
    original_unlink = os.unlink
    interrupted = False

    def unlink_then_interrupt(
        path: str | bytes | Path,
        *,
        dir_fd: int | None = None,
    ) -> None:
        nonlocal interrupted
        original_unlink(path, dir_fd=dir_fd)
        if str(path) == temporary.name and not interrupted:
            interrupted = True
            raise RuntimeError("fault: duplicate-unlink-return-lost")

    with monkeypatch.context() as scoped:
        scoped.setattr("h2hdb_ingest.library.os.unlink", unlink_then_interrupt)
        with pytest.raises(RuntimeError, match="duplicate-unlink-return-lost"):
            adapter.protect(
                BytesIO(b"payload"),
                item.storage_key,
                item.artifact_sha256,
                item.size_bytes,
                token,
            )

    assert interrupted
    assert not temporary.exists()
    assert final.read_bytes() == b"payload"
    assert adapter.protect(
        BytesIO(b"payload"),
        item.storage_key,
        item.artifact_sha256,
        item.size_bytes,
        token,
    ).stored
    assert final.stat().st_nlink == 1


def test_staging_publish_different_inode_duplicate_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(181, b"payload")
    token = bytes((181,)) * 184

    def interrupt_before_rename(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("fault: stage-publish-before-rename")

    with monkeypatch.context() as scoped:
        scoped.setattr(library_module, "_rename_noreplace", interrupt_before_rename)
        with pytest.raises(RuntimeError, match="stage-publish-before-rename"):
            adapter.protect(
                BytesIO(b"payload"),
                item.storage_key,
                item.artifact_sha256,
                item.size_bytes,
                token,
            )

    staging = root / ".h2hdb-state" / "staging"
    stem = sha256(token).hexdigest()
    temporary = staging / f".{stem}.tmp"
    final = staging / f"{stem}.cbz"
    final.write_bytes(b"payload")
    final.chmod(0o644)

    with pytest.raises(RuntimeError, match="do not share exact inode authority"):
        adapter.protect(
            BytesIO(b"payload"),
            item.storage_key,
            item.artifact_sha256,
            item.size_bytes,
            token,
        )

    assert temporary.read_bytes() == final.read_bytes() == b"payload"
    assert temporary.stat().st_ino != final.stat().st_ino


@pytest.mark.parametrize("corruption", ("leaf", "signature"))
def test_staging_duplicate_requires_exact_writing_journal_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    corruption: str,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(192, b"payload")
    token = bytes((192,)) * 184

    def interrupt_before_rename(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("fault: stage-publish-before-rename")

    with monkeypatch.context() as scoped:
        scoped.setattr(library_module, "_rename_noreplace", interrupt_before_rename)
        with pytest.raises(RuntimeError, match="stage-publish-before-rename"):
            adapter.protect(
                BytesIO(b"payload"),
                item.storage_key,
                item.artifact_sha256,
                item.size_bytes,
                token,
            )

    staging = root / ".h2hdb-state" / "staging"
    stem = sha256(token).hexdigest()
    temporary = staging / f".{stem}.tmp"
    final = staging / f"{stem}.cbz"
    os.link(temporary, final)
    with adapter._exclusive_state() as connection:
        connection.execute("BEGIN IMMEDIATE")
        if corruption == "leaf":
            connection.execute(
                "UPDATE protection_tokens SET staging_leaf = 'foreign.cbz' "
                "WHERE token = ?",
                (token,),
            )
        else:
            connection.execute(
                "UPDATE protection_tokens SET device = 1 WHERE token = ?",
                (token,),
            )
        connection.commit()

    with pytest.raises(RuntimeError, match="journal authority is inconsistent"):
        adapter.protect(
            BytesIO(b"payload"),
            item.storage_key,
            item.artifact_sha256,
            item.size_bytes,
            token,
        )

    assert temporary.read_bytes() == final.read_bytes() == b"payload"
    assert temporary.stat().st_ino == final.stat().st_ino
    assert temporary.stat().st_nlink == 2


def test_staging_duplicate_changed_ctime_across_fsync_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(193, b"payload")
    token = bytes((193,)) * 184

    def interrupt_before_rename(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("fault: stage-publish-before-rename")

    with monkeypatch.context() as scoped:
        scoped.setattr(library_module, "_rename_noreplace", interrupt_before_rename)
        with pytest.raises(RuntimeError, match="stage-publish-before-rename"):
            adapter.protect(
                BytesIO(b"payload"),
                item.storage_key,
                item.artifact_sha256,
                item.size_bytes,
                token,
            )

    staging = root / ".h2hdb-state" / "staging"
    stem = sha256(token).hexdigest()
    temporary = staging / f".{stem}.tmp"
    final = staging / f"{stem}.cbz"
    os.link(temporary, final)
    adapter._ensure_layout()
    staging_identity = (staging.stat().st_dev, staging.stat().st_ino)
    original_fsync = os.fsync
    changed = False

    def change_ctime_after_first_staging_fsync(descriptor: int) -> None:
        nonlocal changed
        original_fsync(descriptor)
        value = os.fstat(descriptor)
        if (value.st_dev, value.st_ino) != staging_identity or changed:
            return
        before = temporary.stat().st_ctime_ns
        temporary.chmod(0o600)
        temporary.chmod(0o644)
        assert temporary.stat().st_ctime_ns != before
        changed = True

    with monkeypatch.context() as scoped:
        scoped.setattr(adapter, "_ensure_layout", lambda: None)
        scoped.setattr(
            "h2hdb_ingest.library.os.fsync",
            change_ctime_after_first_staging_fsync,
        )
        with pytest.raises(RuntimeError, match="changed across directory sync"):
            adapter.protect(
                BytesIO(b"payload"),
                item.storage_key,
                item.artifact_sha256,
                item.size_bytes,
                token,
            )

    assert changed
    assert temporary.read_bytes() == final.read_bytes() == b"payload"
    assert temporary.stat().st_nlink == 2


def test_staged_journal_leaf_must_match_protection_token(tmp_path: Path) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(194, b"payload")
    token = _protect(adapter, item, b"payload", 194)
    with adapter._exclusive_state() as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "UPDATE protection_tokens SET staging_leaf = 'foreign.cbz' WHERE token = ?",
            (token,),
        )
        connection.commit()

    with pytest.raises(RuntimeError, match="leaf disagrees with its token"):
        adapter.protect(
            BytesIO(b"payload"),
            item.storage_key,
            item.artifact_sha256,
            item.size_bytes,
            token,
        )

    expected = root / ".h2hdb-state" / "staging" / f"{sha256(token).hexdigest()}.cbz"
    assert expected.read_bytes() == b"payload"


def test_marker_rename_response_loss_replay_fsyncs_coordination_before_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(177, b"payload")
    _protect(adapter, item, b"payload", 177)
    receipt = b"u" * 16
    original_rename = library_module._rename_noreplace

    def rename_then_interrupt(
        source: str,
        destination: str,
        *,
        source_descriptor: int,
        destination_descriptor: int,
    ) -> None:
        original_rename(
            source,
            destination,
            source_descriptor=source_descriptor,
            destination_descriptor=destination_descriptor,
        )
        if destination == "ACTIVATING":
            raise RuntimeError("fault: marker-rename-return-lost")

    with monkeypatch.context() as scoped:
        scoped.setattr(library_module, "_rename_noreplace", rename_then_interrupt)
        with adapter.publication_guard():
            adapter.begin(1, receipt)
            adapter.activate_page(1, (item,))
            adapter.seal(1)
            with pytest.raises(RuntimeError, match="marker-rename-return-lost"):
                adapter.reconcile_page(1, receipt, limit=128)

    coordination = root / ".h2hdb-coordination"
    coordination_identity = (
        coordination.stat().st_dev,
        coordination.stat().st_ino,
    )
    fsynced: list[tuple[int, int]] = []
    original_fsync = os.fsync

    def record_fsync(descriptor: int) -> None:
        value = os.fstat(descriptor)
        fsynced.append((value.st_dev, value.st_ino))
        original_fsync(descriptor)

    restarted = _adapter(root)
    with monkeypatch.context() as scoped:
        scoped.setattr("h2hdb_ingest.library.os.fsync", record_fsync)
        with restarted.publication_guard():
            checkpoint = restarted.begin(1, receipt)
            while checkpoint.status is not LibraryActivationStatus.READY:
                checkpoint = restarted.reconcile_page(1, receipt, limit=128)
            restarted.complete(1, receipt)

    assert coordination_identity in fsynced


def test_marker_publish_same_inode_duplicate_heals_to_final_name(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(182, b"payload")
    _protect(adapter, item, b"payload", 182)
    receipt = b"w" * 16

    def interrupt_before_rename(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("fault: marker-publish-before-rename")

    with monkeypatch.context() as scoped:
        scoped.setattr(library_module, "_rename_noreplace", interrupt_before_rename)
        with adapter.publication_guard():
            adapter.begin(1, receipt)
            adapter.activate_page(1, (item,))
            adapter.seal(1)
            with pytest.raises(RuntimeError, match="marker-publish-before-rename"):
                adapter.reconcile_page(1, receipt, limit=128)

    payload = library_module._marker_payload(1, receipt)
    coordination = root / ".h2hdb-coordination"
    temporary = coordination / f".ACTIVATING-{sha256(payload).hexdigest()}.tmp"
    marker = coordination / "ACTIVATING"
    os.link(temporary, marker)

    restarted = _adapter(root)
    with restarted.publication_guard():
        checkpoint = restarted.begin(1, receipt)
        assert not temporary.exists()
        assert marker.stat().st_nlink == 1
        while checkpoint.status is not LibraryActivationStatus.READY:
            checkpoint = restarted.reconcile_page(1, receipt, limit=128)
        restarted.complete(1, receipt)

    assert not marker.exists()


def test_marker_duplicate_unlink_response_loss_replays_final_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(196, b"payload")
    _protect(adapter, item, b"payload", 196)
    receipt = b"e" * 16

    def interrupt_before_rename(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("fault: marker-publish-before-rename")

    with monkeypatch.context() as scoped:
        scoped.setattr(library_module, "_rename_noreplace", interrupt_before_rename)
        with adapter.publication_guard():
            adapter.begin(1, receipt)
            adapter.activate_page(1, (item,))
            adapter.seal(1)
            with pytest.raises(RuntimeError, match="marker-publish-before-rename"):
                adapter.reconcile_page(1, receipt, limit=128)

    payload = library_module._marker_payload(1, receipt)
    coordination = root / ".h2hdb-coordination"
    temporary = coordination / f".ACTIVATING-{sha256(payload).hexdigest()}.tmp"
    marker = coordination / "ACTIVATING"
    os.link(temporary, marker)
    original_unlink = os.unlink
    interrupted = False

    def unlink_then_interrupt(
        path: str | bytes | Path,
        *,
        dir_fd: int | None = None,
    ) -> None:
        nonlocal interrupted
        original_unlink(path, dir_fd=dir_fd)
        if str(path) == temporary.name and not interrupted:
            interrupted = True
            raise RuntimeError("fault: duplicate-unlink-return-lost")

    restarted = _adapter(root)
    with monkeypatch.context() as scoped:
        scoped.setattr("h2hdb_ingest.library.os.unlink", unlink_then_interrupt)
        with restarted.publication_guard():
            with pytest.raises(RuntimeError, match="foreign contents or metadata"):
                restarted.begin(1, receipt)

    assert interrupted
    assert not temporary.exists()
    assert marker.read_bytes() == payload
    resumed = _adapter(root)
    with resumed.publication_guard():
        checkpoint = resumed.begin(1, receipt)
        while checkpoint.status is not LibraryActivationStatus.READY:
            checkpoint = resumed.reconcile_page(1, receipt, limit=128)
        resumed.complete(1, receipt)

    assert not marker.exists()


def test_marker_publish_different_inode_duplicate_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(183, b"payload")
    _protect(adapter, item, b"payload", 183)
    receipt = b"x" * 16

    def interrupt_before_rename(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("fault: marker-publish-before-rename")

    with monkeypatch.context() as scoped:
        scoped.setattr(library_module, "_rename_noreplace", interrupt_before_rename)
        with adapter.publication_guard():
            adapter.begin(1, receipt)
            adapter.activate_page(1, (item,))
            adapter.seal(1)
            with pytest.raises(RuntimeError, match="marker-publish-before-rename"):
                adapter.reconcile_page(1, receipt, limit=128)

    payload = library_module._marker_payload(1, receipt)
    coordination = root / ".h2hdb-coordination"
    temporary = coordination / f".ACTIVATING-{sha256(payload).hexdigest()}.tmp"
    marker = coordination / "ACTIVATING"
    marker.write_bytes(payload)
    marker.chmod(0o644)

    restarted = _adapter(root)
    with restarted.publication_guard():
        with pytest.raises(RuntimeError, match="foreign contents or metadata"):
            restarted.begin(1, receipt)

    assert temporary.read_bytes() == marker.read_bytes() == payload
    assert temporary.stat().st_ino != marker.stat().st_ino


def test_staging_temporary_exact_prefix_resumes_without_deleting_it(
    tmp_path: Path,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(172, b"payload")
    token = bytes((172,)) * 184
    adapter._ensure_layout()
    temporary = root / ".h2hdb-state" / "staging" / f".{sha256(token).hexdigest()}.tmp"
    temporary.write_bytes(b"pay")
    temporary.chmod(0o644)

    assert adapter.protect(
        BytesIO(b"payload"),
        item.storage_key,
        item.artifact_sha256,
        item.size_bytes,
        token,
    ).stored

    staged = temporary.with_name(f"{sha256(token).hexdigest()}.cbz")
    assert not temporary.exists()
    assert staged.read_bytes() == b"payload"


def test_pending_partial_staging_release_tombstones_and_fences_delayed_protect(
    tmp_path: Path,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(175, b"payload")
    token = bytes((175,)) * 184
    temporary = root / ".h2hdb-state" / "staging" / f".{sha256(token).hexdigest()}.tmp"

    with pytest.raises(RuntimeError, match="interrupted staging write"):
        adapter.protect(
            _PartialThenError(b"payload"),
            item.storage_key,
            item.artifact_sha256,
            item.size_bytes,
            token,
        )
    assert temporary.read_bytes() == b"pay"

    assert adapter.release(
        item.storage_key,
        item.artifact_sha256,
        item.size_bytes,
        token,
    ).released
    assert not temporary.exists()
    assert not adapter.protect(
        BytesIO(b"payload"),
        item.storage_key,
        item.artifact_sha256,
        item.size_bytes,
        token,
    ).stored
    assert not list((root / ".h2hdb-state" / "staging").iterdir())


@pytest.mark.parametrize("boundary", ("before-unlink", "unlink-return-lost"))
def test_pending_partial_release_response_loss_replays_terminal_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(176, b"payload")
    token = bytes((176,)) * 184
    temporary = root / ".h2hdb-state" / "staging" / f".{sha256(token).hexdigest()}.tmp"
    with pytest.raises(RuntimeError, match="interrupted staging write"):
        adapter.protect(
            _PartialThenError(b"payload"),
            item.storage_key,
            item.artifact_sha256,
            item.size_bytes,
            token,
        )

    with monkeypatch.context() as scoped:
        if boundary == "before-unlink":

            def interrupt_cleanup(*args: object, **kwargs: object) -> None:
                del args, kwargs
                raise RuntimeError("fault: partial-cleanup-before-unlink")

            scoped.setattr(adapter, "_remove_stage_temporary", interrupt_cleanup)
        else:
            original_unlink = os.unlink

            def unlink_then_interrupt(
                path: str | bytes | Path,
                *,
                dir_fd: int | None = None,
            ) -> None:
                original_unlink(path, dir_fd=dir_fd)
                raise RuntimeError("fault: partial-unlink-return-lost")

            scoped.setattr("h2hdb_ingest.library.os.unlink", unlink_then_interrupt)
        with pytest.raises(RuntimeError, match="fault: partial"):
            adapter.release(
                item.storage_key,
                item.artifact_sha256,
                item.size_bytes,
                token,
            )

    assert not adapter.protect(
        BytesIO(b"payload"),
        item.storage_key,
        item.artifact_sha256,
        item.size_bytes,
        token,
    ).stored
    staging = root / ".h2hdb-state" / "staging"
    staging_identity = (staging.stat().st_dev, staging.stat().st_ino)
    fsynced: list[tuple[int, int]] = []
    original_fsync = os.fsync

    def record_fsync(descriptor: int) -> None:
        value = os.fstat(descriptor)
        fsynced.append((value.st_dev, value.st_ino))
        original_fsync(descriptor)

    with monkeypatch.context() as scoped:
        scoped.setattr("h2hdb_ingest.library.os.fsync", record_fsync)
        assert adapter.release(
            item.storage_key,
            item.artifact_sha256,
            item.size_bytes,
            token,
        ).released

    assert staging_identity in fsynced
    assert not temporary.exists()
    assert adapter.maintain_cleanup() is LibraryMaintenanceOutcome.PROGRESSED


def test_staging_temporary_leaf_swap_before_publish_preserves_both_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(173, b"payload")
    token = bytes((173,)) * 184
    token_name = sha256(token).hexdigest()
    temporary = root / ".h2hdb-state" / "staging" / f".{token_name}.tmp"
    staged = temporary.with_name(f"{token_name}.cbz")
    saved = tmp_path / "saved-staging-temporary"
    original_rename = library_module._rename_noreplace
    swapped = False

    def rename_after_source_swap(
        source: str,
        destination: str,
        *,
        source_descriptor: int,
        destination_descriptor: int,
    ) -> None:
        nonlocal swapped
        if destination.endswith(".cbz") and not swapped:
            temporary.rename(saved)
            temporary.write_bytes(b"foreign")
            temporary.chmod(0o644)
            swapped = True
        original_rename(
            source,
            destination,
            source_descriptor=source_descriptor,
            destination_descriptor=destination_descriptor,
        )

    monkeypatch.setattr(
        library_module,
        "_rename_noreplace",
        rename_after_source_swap,
    )
    with pytest.raises(RuntimeError, match="published staged artifact is foreign"):
        adapter.protect(
            BytesIO(b"payload"),
            item.storage_key,
            item.artifact_sha256,
            item.size_bytes,
            token,
        )

    assert swapped
    assert not staged.exists()
    assert temporary.read_bytes() == b"foreign"
    assert saved.read_bytes() == b"payload"


def test_marker_temporary_leaf_swap_before_publish_preserves_both_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(174, b"payload")
    _protect(adapter, item, b"payload", 174)
    receipt = b"q" * 16
    payload = library_module._marker_payload(1, receipt)
    coordination = root / ".h2hdb-coordination"
    temporary = coordination / f".ACTIVATING-{sha256(payload).hexdigest()}.tmp"
    marker = coordination / "ACTIVATING"
    saved = tmp_path / "saved-marker-temporary"
    original_rename = library_module._rename_noreplace
    swapped = False

    def rename_after_source_swap(
        source: str,
        destination: str,
        *,
        source_descriptor: int,
        destination_descriptor: int,
    ) -> None:
        nonlocal swapped
        if destination == "ACTIVATING" and not swapped:
            temporary.rename(saved)
            temporary.write_bytes(b"foreign")
            temporary.chmod(0o644)
            swapped = True
        original_rename(
            source,
            destination,
            source_descriptor=source_descriptor,
            destination_descriptor=destination_descriptor,
        )

    monkeypatch.setattr(
        library_module,
        "_rename_noreplace",
        rename_after_source_swap,
    )
    with adapter.publication_guard():
        adapter.begin(1, receipt)
        adapter.activate_page(1, (item,))
        adapter.seal(1)
        with pytest.raises(
            RuntimeError,
            match="published library ACTIVATING marker is foreign",
        ):
            adapter.reconcile_page(1, receipt, limit=128)

    assert swapped
    assert not marker.exists()
    assert temporary.read_bytes() == b"foreign"
    assert saved.read_bytes() == payload


def test_release_tombstone_response_loss_is_terminal_and_bounded_cleanup_replays(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(171, b"payload")
    token = _protect(adapter, item, b"payload", 171)

    with monkeypatch.context() as scoped:

        def interrupt_before_unlink(*args: object, **kwargs: object) -> object:
            del args, kwargs
            raise RuntimeError("fault: release-tombstone-durable")

        scoped.setattr(adapter, "_remove_stage_from_row", interrupt_before_unlink)
        with pytest.raises(RuntimeError, match="release-tombstone-durable"):
            adapter.release(
                item.storage_key,
                item.artifact_sha256,
                item.size_bytes,
                token,
            )

    assert adapter.release(
        item.storage_key,
        item.artifact_sha256,
        item.size_bytes,
        token,
    ).released
    assert adapter.maintain_cleanup() is LibraryMaintenanceOutcome.PROGRESSED
    assert not list((root / ".h2hdb-state" / "staging").glob("*.cbz"))
    assert not adapter.protect(
        BytesIO(b"payload"),
        item.storage_key,
        item.artifact_sha256,
        item.size_bytes,
        token,
    ).stored


def test_intermediate_shard_symlink_fails_closed(tmp_path: Path) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(44, b"payload")
    _protect(adapter, item, b"payload", 4)
    outside = tmp_path / "outside"
    outside.mkdir()
    current = root / "current"
    (current / "hash-v1").symlink_to(outside, target_is_directory=True)

    with adapter.publication_guard():
        adapter.begin(1, b"s" * 16)
        adapter.activate_page(1, (item,))
        adapter.seal(1)
        with pytest.raises((OSError, RuntimeError)):
            adapter.reconcile_page(1, b"s" * 16, limit=128)

    assert not list(outside.rglob("*.cbz"))


def test_intermediate_shard_swap_during_replace_is_detected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(45, b"payload")
    _protect(adapter, item, b"payload", 4)
    outside = tmp_path / "outside"
    outside.mkdir()
    original_rename = library_module._rename_noreplace
    swapped = False

    def rename_with_swap(
        source: str,
        destination: str,
        *,
        source_descriptor: int,
        destination_descriptor: int,
    ) -> None:
        nonlocal swapped
        if destination.startswith("h2h-") and not swapped:
            shard = root / "current" / "hash-v1"
            moved = root / "current" / "detached-hash-v1"
            shard.rename(moved)
            shard.symlink_to(outside, target_is_directory=True)
            swapped = True
        original_rename(
            source,
            destination,
            source_descriptor=source_descriptor,
            destination_descriptor=destination_descriptor,
        )

    monkeypatch.setattr(library_module, "_rename_noreplace", rename_with_swap)
    with adapter.publication_guard():
        adapter.begin(1, b"t" * 16)
        adapter.activate_page(1, (item,))
        adapter.seal(1)
        with pytest.raises(RuntimeError, match="shard"):
            adapter.reconcile_page(1, b"t" * 16, limit=128)

    assert swapped
    assert not list(outside.rglob("*.cbz"))


def test_stale_unlink_response_loss_replays_as_completed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    retained = _item(50, b"retained")
    removed = _item(51, b"removed")
    _protect(adapter, retained, b"retained", 5)
    _protect(adapter, removed, b"removed", 6)
    _activate(adapter, 1, b"a" * 16, (retained, removed))

    original_unlink = os.unlink
    injected = False

    def unlink_then_interrupt(
        path: str | bytes | Path,
        *,
        dir_fd: int | None = None,
    ) -> None:
        nonlocal injected
        original_unlink(path, dir_fd=dir_fd)
        if dir_fd is not None and not injected:
            injected = True
            raise RuntimeError("simulated SIGKILL boundary")

    monkeypatch.setattr("h2hdb_ingest.library.os.unlink", unlink_then_interrupt)
    receipt = b"b" * 16
    with adapter.publication_guard():
        adapter.begin(2, receipt)
        adapter.activate_page(2, (retained,))
        adapter.seal(2)
        adapter.reconcile_page(2, receipt, limit=128)
        with pytest.raises(RuntimeError, match="SIGKILL"):
            adapter.reconcile_page(2, receipt, limit=128)

    monkeypatch.setattr("h2hdb_ingest.library.os.unlink", original_unlink)
    quarantine = root / ".h2hdb-state" / "quarantine"
    quarantine_identity = (quarantine.stat().st_dev, quarantine.stat().st_ino)
    fsynced: list[tuple[int, int]] = []
    original_fsync = os.fsync

    def record_fsync(descriptor: int) -> None:
        value = os.fstat(descriptor)
        fsynced.append((value.st_dev, value.st_ino))
        original_fsync(descriptor)

    restarted = _adapter(root)
    with monkeypatch.context() as scoped:
        scoped.setattr("h2hdb_ingest.library.os.fsync", record_fsync)
        with restarted.publication_guard():
            checkpoint = restarted.begin(2, receipt)
            assert checkpoint.status is LibraryActivationStatus.RECONCILE
            while True:
                checkpoint = restarted.reconcile_page(2, receipt, limit=128)
                if checkpoint.status is LibraryActivationStatus.READY:
                    break
            restarted.complete(2, receipt)

    assert quarantine_identity in fsynced
    assert not restarted.current_path.joinpath(*removed.storage_key.segments).exists()


def test_stale_capture_rename_response_loss_fsyncs_both_directories_on_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    retained = _item(52, b"retained")
    removed = _item(53, b"removed")
    _protect(adapter, retained, b"retained", 52)
    _protect(adapter, removed, b"removed", 53)
    first_items = tuple(
        sorted((retained, removed), key=lambda item: item.publication_key)
    )
    _activate(adapter, 1, b"a" * 16, first_items)
    original_rename = library_module._rename_noreplace
    interrupted = False

    def rename_then_interrupt(
        source: str,
        destination: str,
        *,
        source_descriptor: int,
        destination_descriptor: int,
    ) -> None:
        nonlocal interrupted
        original_rename(
            source,
            destination,
            source_descriptor=source_descriptor,
            destination_descriptor=destination_descriptor,
        )
        if source.startswith("h2h-") and not interrupted:
            interrupted = True
            raise RuntimeError("fault: stale-capture-rename-return-lost")

    receipt = b"c" * 16
    with monkeypatch.context() as scoped:
        scoped.setattr(library_module, "_rename_noreplace", rename_then_interrupt)
        with adapter.publication_guard():
            adapter.begin(2, receipt)
            adapter.activate_page(2, (retained,))
            adapter.seal(2)
            adapter.reconcile_page(2, receipt, limit=128)
            with pytest.raises(RuntimeError, match="stale-capture-rename-return-lost"):
                adapter.reconcile_page(2, receipt, limit=128)

    removed_path = adapter.current_path.joinpath(*removed.storage_key.segments)
    current_parent_identity = (
        removed_path.parent.stat().st_dev,
        removed_path.parent.stat().st_ino,
    )
    quarantine = root / ".h2hdb-state" / "quarantine"
    quarantine_identity = (quarantine.stat().st_dev, quarantine.stat().st_ino)
    fsynced: list[tuple[int, int]] = []
    original_fsync = os.fsync

    def record_fsync(descriptor: int) -> None:
        value = os.fstat(descriptor)
        fsynced.append((value.st_dev, value.st_ino))
        original_fsync(descriptor)

    restarted = _adapter(root)
    with monkeypatch.context() as scoped:
        scoped.setattr("h2hdb_ingest.library.os.fsync", record_fsync)
        with restarted.publication_guard():
            checkpoint = restarted.begin(2, receipt)
            while checkpoint.status is not LibraryActivationStatus.READY:
                checkpoint = restarted.reconcile_page(2, receipt, limit=128)
            restarted.complete(2, receipt)

    assert current_parent_identity in fsynced
    assert quarantine_identity in fsynced
    assert fsynced.index(quarantine_identity) < fsynced.index(current_parent_identity)
    assert not removed_path.exists()


def test_fresh_current_capture_fsyncs_quarantine_before_source_parent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(54, b"payload")
    _protect(adapter, item, b"payload", 54)
    _activate(adapter, 1, b"a" * 16, (item,))
    target = adapter.current_path.joinpath(*item.storage_key.segments)
    signature = library_module._Signature.from_stat(target.lstat())
    quarantine = root / ".h2hdb-state" / "quarantine"
    quarantine_leaf = library_module._quarantine_leaf(
        "/".join(item.storage_key.segments),
        item.artifact_sha256,
    )
    current_parent_identity = (
        target.parent.stat().st_dev,
        target.parent.stat().st_ino,
    )
    quarantine_identity = (quarantine.stat().st_dev, quarantine.stat().st_ino)
    fsynced: list[tuple[int, int]] = []
    original_fsync = os.fsync

    def record_fsync(descriptor: int) -> None:
        value = os.fstat(descriptor)
        fsynced.append((value.st_dev, value.st_ino))
        original_fsync(descriptor)

    with monkeypatch.context() as scoped:
        scoped.setattr("h2hdb_ingest.library.os.fsync", record_fsync)
        adapter._quarantine_current(
            item.storage_key,
            quarantine_leaf,
            expected_sha256=item.artifact_sha256,
            expected_size=item.size_bytes,
            expected_signature=signature,
        )

    assert fsynced.index(quarantine_identity) < fsynced.index(current_parent_identity)
    assert (quarantine / quarantine_leaf).read_bytes() == b"payload"
    assert not target.exists()


def test_replacement_leaf_race_never_overwrites_unknown_current(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    first = _item(61, b"first")
    second = _item(61, b"second")
    _protect(adapter, first, b"first", 61)
    _activate(adapter, 1, b"a" * 16, (first,))
    _protect(adapter, second, b"second", 62)
    original_rename = library_module._rename_noreplace

    def rename_after_foreign_appears(
        source: str,
        destination: str,
        *,
        source_descriptor: int,
        destination_descriptor: int,
    ) -> None:
        if not destination.startswith("h2h-"):
            original_rename(
                source,
                destination,
                source_descriptor=source_descriptor,
                destination_descriptor=destination_descriptor,
            )
            return
        descriptor = os.open(
            destination,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o644,
            dir_fd=destination_descriptor,
        )
        with os.fdopen(descriptor, "wb", closefd=True) as foreign:
            foreign.write(b"foreign")
        original_rename(
            source,
            destination,
            source_descriptor=source_descriptor,
            destination_descriptor=destination_descriptor,
        )

    monkeypatch.setattr(
        library_module,
        "_rename_noreplace",
        rename_after_foreign_appears,
    )
    with adapter.publication_guard():
        adapter.begin(2, b"b" * 16)
        adapter.activate_page(2, (second,))
        adapter.seal(2)
        with pytest.raises(RuntimeError, match="unknown library target appeared"):
            adapter.reconcile_page(2, b"b" * 16, limit=128)

    target = adapter.current_path.joinpath(*second.storage_key.segments)
    quarantine = tuple((root / ".h2hdb-state" / "quarantine").glob("*.cbz"))
    assert target.read_bytes() == b"foreign"
    assert len(quarantine) == 1
    assert quarantine[0].read_bytes() == b"first"
    assert [
        path.read_bytes() for path in (root / ".h2hdb-state" / "staging").glob("*.cbz")
    ] == [b"second"]


def test_quarantine_destination_race_never_overwrites_unknown_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    first = _item(62, b"first")
    second = _item(62, b"second")
    _protect(adapter, first, b"first", 63)
    _activate(adapter, 1, b"a" * 16, (first,))
    _protect(adapter, second, b"second", 64)
    original_rename = library_module._rename_noreplace

    def rename_after_destination_appears(
        source: str,
        destination: str,
        *,
        source_descriptor: int,
        destination_descriptor: int,
    ) -> None:
        if not destination.endswith(".cbz"):
            original_rename(
                source,
                destination,
                source_descriptor=source_descriptor,
                destination_descriptor=destination_descriptor,
            )
            return
        descriptor = os.open(
            destination,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
            dir_fd=destination_descriptor,
        )
        with os.fdopen(descriptor, "wb", closefd=True) as foreign:
            foreign.write(b"foreign")
        original_rename(
            source,
            destination,
            source_descriptor=source_descriptor,
            destination_descriptor=destination_descriptor,
        )

    monkeypatch.setattr(
        library_module,
        "_rename_noreplace",
        rename_after_destination_appears,
    )
    with adapter.publication_guard():
        adapter.begin(2, b"b" * 16)
        adapter.activate_page(2, (second,))
        adapter.seal(2)
        with pytest.raises(RuntimeError, match="destination is occupied"):
            adapter.reconcile_page(2, b"b" * 16, limit=128)

    target = adapter.current_path.joinpath(*first.storage_key.segments)
    quarantine = tuple((root / ".h2hdb-state" / "quarantine").glob("*.cbz"))
    assert target.read_bytes() == b"first"
    assert len(quarantine) == 1
    assert quarantine[0].read_bytes() == b"foreign"


def test_quarantine_leaf_swap_immediately_before_unlink_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    retained = _item(63, b"retained")
    removed = _item(64, b"removed")
    _protect(adapter, retained, b"retained", 65)
    _protect(adapter, removed, b"removed", 66)
    _activate(
        adapter,
        1,
        b"a" * 16,
        tuple(sorted((retained, removed), key=lambda item: item.publication_key)),
    )
    receipt = b"b" * 16
    with adapter.publication_guard():
        adapter.begin(2, receipt)
        adapter.activate_page(2, (retained,))
        adapter.seal(2)
        adapter.reconcile_page(2, receipt, limit=128)

        original_verify = library_module._verify_regular_at
        verified_count = 0
        saved = tmp_path / "captured-removed.cbz"

        def verify_then_swap(
            descriptor: int,
            leaf: str,
            *,
            expected_sha256: bytes,
            expected_size: int,
            label: str,
        ) -> object:
            nonlocal verified_count
            result = original_verify(
                descriptor,
                leaf,
                expected_sha256=expected_sha256,
                expected_size=expected_size,
                label=label,
            )
            if label == "quarantined stale library artifact":
                verified_count += 1
                if verified_count == 2:
                    quarantine = root / ".h2hdb-state" / "quarantine" / leaf
                    quarantine.rename(saved)
                    quarantine.write_bytes(b"foreign")
            return result

        monkeypatch.setattr(library_module, "_verify_regular_at", verify_then_swap)
        with pytest.raises(RuntimeError, match="immediately before unlink"):
            adapter.reconcile_page(2, receipt, limit=128)

    quarantine = tuple((root / ".h2hdb-state" / "quarantine").glob("*.cbz"))
    assert verified_count == 2
    assert len(quarantine) == 1
    assert quarantine[0].read_bytes() == b"foreign"
    assert saved.read_bytes() == b"removed"


def test_missing_current_and_quarantine_after_authorization_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    first = _item(65, b"first")
    second = _item(65, b"second")
    _protect(adapter, first, b"first", 67)
    _activate(adapter, 1, b"a" * 16, (first,))
    _protect(adapter, second, b"second", 68)
    original_quarantine = cast(Callable[..., object], adapter._quarantine_current)

    def quarantine_then_remove(*args: object, **kwargs: object) -> None:
        original_quarantine(*args, **kwargs)
        quarantine = next((root / ".h2hdb-state" / "quarantine").glob("*.cbz"))
        quarantine.unlink()
        raise RuntimeError("fault: quarantine disappeared")

    monkeypatch.setattr(adapter, "_quarantine_current", quarantine_then_remove)
    with adapter.publication_guard():
        adapter.begin(2, b"b" * 16)
        adapter.activate_page(2, (second,))
        adapter.seal(2)
        with pytest.raises(RuntimeError, match="quarantine disappeared"):
            adapter.reconcile_page(2, b"b" * 16, limit=128)

    restarted = _adapter(root)
    with restarted.publication_guard():
        restarted.begin(2, b"b" * 16)
        with pytest.raises(RuntimeError, match="both disappeared"):
            restarted.reconcile_page(2, b"b" * 16, limit=128)


def test_current_quarantine_same_inode_duplicate_heals_during_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    first = _item(186, b"first")
    second = _item(186, b"second")
    _protect(adapter, first, b"first", 186)
    _activate(adapter, 1, b"a" * 16, (first,))
    token = _protect(adapter, second, b"second", 187)
    receipt = b"b" * 16

    with monkeypatch.context() as scoped:

        def interrupt_after_authorization(
            *_args: object,
            **_kwargs: object,
        ) -> None:
            raise RuntimeError("fault: capture-operation-authorized")

        scoped.setattr(
            adapter,
            "_capture_replaced_current",
            interrupt_after_authorization,
        )
        with adapter.publication_guard():
            adapter.begin(2, receipt)
            adapter.activate_page(2, (second,))
            adapter.seal(2)
            with pytest.raises(RuntimeError, match="capture-operation-authorized"):
                adapter.reconcile_page(2, receipt, limit=128)

    target = adapter.current_path.joinpath(*first.storage_key.segments)
    quarantine = root / ".h2hdb-state" / "quarantine"
    quarantine_leaf = library_module._quarantine_leaf(
        "/".join(first.storage_key.segments),
        first.artifact_sha256,
    )
    duplicate = quarantine / quarantine_leaf
    os.link(target, duplicate)
    stage = root / ".h2hdb-state" / "staging" / f"{sha256(token).hexdigest()}.cbz"

    restarted = _adapter(root)
    with restarted.publication_guard():
        checkpoint = restarted.begin(2, receipt)
        while checkpoint.status is not LibraryActivationStatus.READY:
            checkpoint = restarted.reconcile_page(2, receipt, limit=128)
        restarted.complete(2, receipt)

    assert target.read_bytes() == b"second"
    assert target.stat().st_nlink == 1
    assert not duplicate.exists()
    assert not stage.exists()
    assert list(root.rglob("*.cbz")) == [target]


def test_current_quarantine_duplicate_unlink_response_loss_replays(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    first = _item(198, b"first")
    second = _item(198, b"second")
    _protect(adapter, first, b"first", 198)
    _activate(adapter, 1, b"a" * 16, (first,))
    _protect(adapter, second, b"second", 199)
    receipt = b"g" * 16

    with monkeypatch.context() as scoped:

        def interrupt_after_authorization(
            *_args: object,
            **_kwargs: object,
        ) -> None:
            raise RuntimeError("fault: capture-operation-authorized")

        scoped.setattr(
            adapter,
            "_capture_replaced_current",
            interrupt_after_authorization,
        )
        with adapter.publication_guard():
            adapter.begin(2, receipt)
            adapter.activate_page(2, (second,))
            adapter.seal(2)
            with pytest.raises(RuntimeError, match="capture-operation-authorized"):
                adapter.reconcile_page(2, receipt, limit=128)

    target = adapter.current_path.joinpath(*first.storage_key.segments)
    quarantine = root / ".h2hdb-state" / "quarantine"
    quarantine_leaf = library_module._quarantine_leaf(
        "/".join(first.storage_key.segments),
        first.artifact_sha256,
    )
    duplicate = quarantine / quarantine_leaf
    os.link(target, duplicate)
    original_unlink = os.unlink
    interrupted = False

    def unlink_then_interrupt(
        path: str | bytes | Path,
        *,
        dir_fd: int | None = None,
    ) -> None:
        nonlocal interrupted
        original_unlink(path, dir_fd=dir_fd)
        if str(path) == target.name and not interrupted:
            interrupted = True
            raise RuntimeError("fault: duplicate-unlink-return-lost")

    restarted = _adapter(root)
    with monkeypatch.context() as scoped:
        scoped.setattr("h2hdb_ingest.library.os.unlink", unlink_then_interrupt)
        with restarted.publication_guard():
            restarted.begin(2, receipt)
            with pytest.raises(RuntimeError, match="duplicate-unlink-return-lost"):
                restarted.reconcile_page(2, receipt, limit=128)

    assert interrupted
    assert not target.exists()
    assert duplicate.read_bytes() == b"first"
    resumed = _adapter(root)
    with resumed.publication_guard():
        checkpoint = resumed.begin(2, receipt)
        while checkpoint.status is not LibraryActivationStatus.READY:
            checkpoint = resumed.reconcile_page(2, receipt, limit=128)
        resumed.complete(2, receipt)

    assert target.read_bytes() == b"second"
    assert target.stat().st_nlink == 1
    assert not duplicate.exists()
    assert list(root.rglob("*.cbz")) == [target]


def test_current_quarantine_different_inode_duplicate_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    first = _item(188, b"first")
    second = _item(188, b"second")
    _protect(adapter, first, b"first", 188)
    _activate(adapter, 1, b"a" * 16, (first,))
    token = _protect(adapter, second, b"second", 189)
    receipt = b"c" * 16

    with monkeypatch.context() as scoped:

        def interrupt_after_authorization(
            *_args: object,
            **_kwargs: object,
        ) -> None:
            raise RuntimeError("fault: capture-operation-authorized")

        scoped.setattr(
            adapter,
            "_capture_replaced_current",
            interrupt_after_authorization,
        )
        with adapter.publication_guard():
            adapter.begin(2, receipt)
            adapter.activate_page(2, (second,))
            adapter.seal(2)
            with pytest.raises(RuntimeError, match="capture-operation-authorized"):
                adapter.reconcile_page(2, receipt, limit=128)

    target = adapter.current_path.joinpath(*first.storage_key.segments)
    quarantine = root / ".h2hdb-state" / "quarantine"
    quarantine_leaf = library_module._quarantine_leaf(
        "/".join(first.storage_key.segments),
        first.artifact_sha256,
    )
    duplicate = quarantine / quarantine_leaf
    duplicate.write_bytes(b"first")
    duplicate.chmod(0o644)
    stage = root / ".h2hdb-state" / "staging" / f"{sha256(token).hexdigest()}.cbz"

    restarted = _adapter(root)
    with restarted.publication_guard():
        restarted.begin(2, receipt)
        with pytest.raises(RuntimeError, match="do not share exact inode authority"):
            restarted.reconcile_page(2, receipt, limit=128)

    assert target.read_bytes() == duplicate.read_bytes() == b"first"
    assert (target.stat().st_dev, target.stat().st_ino) != (
        duplicate.stat().st_dev,
        duplicate.stat().st_ino,
    )
    assert stage.read_bytes() == b"second"


def test_stale_current_quarantine_same_inode_duplicate_heals(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    retained = _item(190, b"retained")
    removed = _item(191, b"removed")
    _protect(adapter, retained, b"retained", 190)
    _protect(adapter, removed, b"removed", 191)
    _activate(
        adapter,
        1,
        b"a" * 16,
        tuple(sorted((retained, removed), key=lambda item: item.publication_key)),
    )
    receipt = b"d" * 16

    with monkeypatch.context() as scoped:

        def interrupt_after_authorization(
            *_args: object,
            **_kwargs: object,
        ) -> None:
            raise RuntimeError("fault: stale-capture-operation-authorized")

        scoped.setattr(
            adapter,
            "_quarantine_current",
            interrupt_after_authorization,
        )
        with adapter.publication_guard():
            adapter.begin(2, receipt)
            adapter.activate_page(2, (retained,))
            adapter.seal(2)
            adapter.reconcile_page(2, receipt, limit=128)
            with pytest.raises(
                RuntimeError,
                match="stale-capture-operation-authorized",
            ):
                adapter.reconcile_page(2, receipt, limit=128)

    target = adapter.current_path.joinpath(*removed.storage_key.segments)
    quarantine = root / ".h2hdb-state" / "quarantine"
    quarantine_leaf = library_module._quarantine_leaf(
        "/".join(removed.storage_key.segments),
        removed.artifact_sha256,
    )
    duplicate = quarantine / quarantine_leaf
    os.link(target, duplicate)

    restarted = _adapter(root)
    with restarted.publication_guard():
        checkpoint = restarted.begin(2, receipt)
        while checkpoint.status is not LibraryActivationStatus.READY:
            checkpoint = restarted.reconcile_page(2, receipt, limit=128)
        restarted.complete(2, receipt)

    assert not target.exists()
    assert not duplicate.exists()
    retained_path = restarted.current_path.joinpath(*retained.storage_key.segments)
    assert list(root.rglob("*.cbz")) == [retained_path]


@pytest.mark.parametrize(
    "boundary",
    (
        "capture-return-lost",
        "rename-return-lost",
        "quarantine-retirement-return-lost",
    ),
)
def test_replacement_response_loss_reconciles_one_exact_current_link(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    first = _item(66, b"first")
    second = _item(66, b"second")
    _protect(adapter, first, b"first", 69)
    _activate(adapter, 1, b"a" * 16, (first,))
    _protect(adapter, second, b"second", 70)

    with monkeypatch.context() as scoped:
        if boundary == "capture-return-lost":
            original_capture = cast(Callable[..., object], adapter._quarantine_current)

            def capture_then_interrupt(*args: object, **kwargs: object) -> None:
                original_capture(*args, **kwargs)
                raise RuntimeError("fault: capture-return-lost")

            scoped.setattr(adapter, "_quarantine_current", capture_then_interrupt)
        elif boundary == "rename-return-lost":
            original_rename = library_module._rename_noreplace

            def rename_then_interrupt(
                source: str,
                destination: str,
                *,
                source_descriptor: int,
                destination_descriptor: int,
            ) -> None:
                original_rename(
                    source,
                    destination,
                    source_descriptor=source_descriptor,
                    destination_descriptor=destination_descriptor,
                )
                if destination.startswith("h2h-"):
                    raise RuntimeError("fault: rename-return-lost")

            scoped.setattr(
                library_module,
                "_rename_noreplace",
                rename_then_interrupt,
            )
        else:
            original_retire = cast(
                Callable[..., object],
                adapter._retire_replaced_current,
            )

            def retire_then_interrupt(*args: object, **kwargs: object) -> object:
                original_retire(*args, **kwargs)
                raise RuntimeError("fault: retirement-return-lost")

            scoped.setattr(
                adapter,
                "_retire_replaced_current",
                retire_then_interrupt,
            )

        with adapter.publication_guard():
            adapter.begin(2, b"b" * 16)
            adapter.activate_page(2, (second,))
            adapter.seal(2)
            with pytest.raises(RuntimeError, match="fault:"):
                adapter.reconcile_page(2, b"b" * 16, limit=128)

    restarted = _adapter(root)
    with restarted.publication_guard():
        checkpoint = restarted.begin(2, b"b" * 16)
        while checkpoint.status is not LibraryActivationStatus.READY:
            checkpoint = restarted.reconcile_page(2, b"b" * 16, limit=128)
        restarted.complete(2, b"b" * 16)

    target = restarted.current_path.joinpath(*second.storage_key.segments)
    assert target.read_bytes() == b"second"
    assert target.stat().st_nlink == 1
    assert list(root.rglob("*.cbz")) == [target]
    assert not list((root / ".h2hdb-state" / "staging").iterdir())
    assert not list((root / ".h2hdb-state" / "quarantine").iterdir())
    _activate(restarted, 3, b"c" * 16, (second,))


def test_foreign_current_captured_by_race_is_restored_not_deleted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    first = _item(67, b"first")
    second = _item(67, b"second")
    _protect(adapter, first, b"first", 71)
    _activate(adapter, 1, b"a" * 16, (first,))
    _protect(adapter, second, b"second", 72)
    target = adapter.current_path.joinpath(*first.storage_key.segments)
    saved = tmp_path / "saved-first.cbz"
    original_rename = library_module._rename_noreplace
    raced = False

    def rename_after_source_swap(
        source: str,
        destination: str,
        *,
        source_descriptor: int,
        destination_descriptor: int,
    ) -> None:
        nonlocal raced
        if destination.endswith(".cbz") and not raced:
            target.rename(saved)
            target.write_bytes(b"foreign")
            raced = True
        original_rename(
            source,
            destination,
            source_descriptor=source_descriptor,
            destination_descriptor=destination_descriptor,
        )

    monkeypatch.setattr(
        library_module,
        "_rename_noreplace",
        rename_after_source_swap,
    )
    with adapter.publication_guard():
        adapter.begin(2, b"b" * 16)
        adapter.activate_page(2, (second,))
        adapter.seal(2)
        with pytest.raises(RuntimeError, match="captured a foreign"):
            adapter.reconcile_page(2, b"b" * 16, limit=128)

    assert raced
    assert target.read_bytes() == b"foreign"
    assert saved.read_bytes() == b"first"
    assert not list((root / ".h2hdb-state" / "quarantine").iterdir())


@pytest.mark.parametrize("cross_directory", (False, True))
def test_same_inode_healing_fsyncs_survivor_between_directory_barriers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cross_directory: bool,
) -> None:
    source_directory = tmp_path / "source"
    source_directory.mkdir(mode=0o700)
    destination_directory = (
        tmp_path / "destination" if cross_directory else source_directory
    )
    if cross_directory:
        destination_directory.mkdir(mode=0o700)
    source = source_directory / "source.cbz"
    destination = destination_directory / "destination.cbz"
    source.write_bytes(b"payload")
    source.chmod(0o644)
    os.link(source, destination)
    expected = library_module._Signature.from_stat(source.lstat())
    source_identity = (
        source_directory.stat().st_dev,
        source_directory.stat().st_ino,
    )
    destination_identity = (
        destination_directory.stat().st_dev,
        destination_directory.stat().st_ino,
    )
    file_identity = (source.stat().st_dev, source.stat().st_ino)
    watched = {source_identity, destination_identity, file_identity}
    events: list[tuple[int, int]] = []
    original_fsync = os.fsync

    def record_fsync(descriptor: int) -> None:
        value = os.fstat(descriptor)
        identity = (value.st_dev, value.st_ino)
        if identity in watched:
            events.append(identity)
        original_fsync(descriptor)

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    source_descriptor = os.open(source_directory, flags)
    destination_descriptor = (
        os.open(destination_directory, flags) if cross_directory else source_descriptor
    )
    try:
        with monkeypatch.context() as scoped:
            scoped.setattr("h2hdb_ingest.library.os.fsync", record_fsync)
            survivor = library_module._heal_same_inode_rename_duplicate(
                source_descriptor=source_descriptor,
                source_leaf=source.name,
                destination_descriptor=destination_descriptor,
                destination_leaf=destination.name,
                expected_sha256=sha256(b"payload").digest(),
                expected_size=len(b"payload"),
                expected_identity=expected,
                label="test rename duplicate",
            )
    finally:
        if cross_directory:
            os.close(destination_descriptor)
        os.close(source_descriptor)

    expected_events = (
        [
            destination_identity,
            source_identity,
            file_identity,
            source_identity,
            destination_identity,
        ]
        if cross_directory
        else [source_identity, file_identity, source_identity]
    )
    assert events == expected_events
    assert not source.exists()
    assert destination.read_bytes() == b"payload"
    assert destination.stat().st_nlink == 1
    assert library_module._Signature.from_stat(destination.lstat()) == survivor


class _FakeRenameFunction:
    def __init__(self, result: int = 0) -> None:
        self.argtypes: object | None = None
        self.restype: object | None = None
        self.calls: list[tuple[object, ...]] = []
        self.result = result

    def __call__(self, *args: object) -> int:
        self.calls.append(args)
        return self.result


class _FakeSyscallLibrary:
    def __init__(self, syscall: _FakeRenameFunction) -> None:
        self.syscall = syscall


class _FakeUname:
    def __init__(self, machine: str) -> None:
        self.machine = machine


@pytest.mark.parametrize(("machine", "number"), (("x86_64", 316), ("aarch64", 276)))
def test_linux_no_replace_uses_syscall_when_libc_lacks_wrapper(
    monkeypatch: pytest.MonkeyPatch,
    machine: str,
    number: int,
) -> None:
    function = _FakeRenameFunction()
    library = _FakeSyscallLibrary(function)
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(os, "uname", lambda: _FakeUname(machine))
    monkeypatch.setattr(ctypes, "CDLL", lambda *_args, **_kwargs: library)

    library_module._rename_noreplace(
        "source",
        "destination",
        source_descriptor=11,
        destination_descriptor=12,
    )

    assert function.calls == [(number, 11, b"source", 12, b"destination", 0x00000001)]


def test_linux_no_replace_unknown_syscall_architecture_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    function = _FakeRenameFunction()
    library = _FakeSyscallLibrary(function)
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(os, "uname", lambda: _FakeUname("mips64"))
    monkeypatch.setattr(ctypes, "CDLL", lambda *_args, **_kwargs: library)

    with pytest.raises(RuntimeError, match="unavailable"):
        library_module._rename_noreplace(
            "source",
            "destination",
            source_descriptor=11,
            destination_descriptor=12,
        )

    assert function.calls == []
