from __future__ import annotations

import fcntl
import json
import os
import stat
from collections.abc import Callable
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import cast

import pytest
from h2hdb import (
    LibraryActivationStatus,
    VNextLibraryActivationItem,
    artifact_storage_key,
)

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
    return ManagedFilesystemLibraryAdapter(root, max_image_short_side=768)


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
    assert not (state / "coordination" / "ACTIVATING").exists()
    assert list((tmp_path / "library").rglob("*.cbz")) == [target]


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

    marker = root / ".h2hdb-state" / "coordination" / "ACTIVATING"
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

    marker = root / ".h2hdb-state" / "coordination" / "ACTIVATING"
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
    lock_path = root / ".h2hdb-state" / "coordination" / "publication.lock"

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
        "replace-return-lost",
        "installed-before-token-retirement",
        "token-retired-before-entry-journal",
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
        elif boundary == "replace-return-lost":
            original_replace = os.replace

            def replace_then_interrupt(
                source: str | Path,
                destination: str | Path,
                *,
                src_dir_fd: int | None = None,
                dst_dir_fd: int | None = None,
            ) -> None:
                original_replace(
                    source,
                    destination,
                    src_dir_fd=src_dir_fd,
                    dst_dir_fd=dst_dir_fd,
                )
                if dst_dir_fd is not None:
                    raise RuntimeError("fault: replace-return-lost")

            scoped.setattr("h2hdb_ingest.library.os.replace", replace_then_interrupt)
        elif boundary in {
            "installed-before-token-retirement",
            "token-retired-before-entry-journal",
        }:
            original = cast(
                Callable[..., object],
                adapter._retire_staged_candidates,
            )

            def retire_with_interrupt(*args: object, **kwargs: object) -> object:
                if boundary == "installed-before-token-retirement":
                    raise RuntimeError(f"fault: {boundary}")
                original(*args, **kwargs)
                raise RuntimeError(f"fault: {boundary}")

            scoped.setattr(
                adapter,
                "_retire_staged_candidates",
                retire_with_interrupt,
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
    assert not (root / ".h2hdb-state" / "coordination" / "ACTIVATING").exists()


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
            original_unlink = Path.unlink

            def unlink_then_interrupt(path: Path, missing_ok: bool = False) -> None:
                original_unlink(path, missing_ok=missing_ok)
                if path.parent.name == "quarantine":
                    raise RuntimeError("fault: unlink-return-lost")

            scoped.setattr(Path, "unlink", unlink_then_interrupt)
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

    assert not (root / ".h2hdb-state" / "coordination" / "ACTIVATING").exists()


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


def test_staging_rename_response_loss_replays_writing_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    item = _item(170, b"payload")
    token = bytes((170,)) * 184
    original_replace = os.replace
    interrupted = False

    def replace_then_interrupt(
        source: str | Path,
        destination: str | Path,
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
    ) -> None:
        nonlocal interrupted
        original_replace(
            source,
            destination,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
        )
        destination_path = Path(destination)
        if destination_path.parent.name == "staging" and not interrupted:
            interrupted = True
            raise RuntimeError("fault: staging-rename-return-lost")

    with monkeypatch.context() as scoped:
        scoped.setattr("h2hdb_ingest.library.os.replace", replace_then_interrupt)
        with pytest.raises(RuntimeError, match="staging-rename-return-lost"):
            adapter.protect(
                BytesIO(b"payload"),
                item.storage_key,
                item.artifact_sha256,
                item.size_bytes,
                token,
            )

    assert adapter.protect(
        BytesIO(b"payload"),
        item.storage_key,
        item.artifact_sha256,
        item.size_bytes,
        token,
    ).stored
    _activate(adapter, 1, b"p" * 16, (item,))
    assert adapter.current_path.joinpath(*item.storage_key.segments).read_bytes() == (
        b"payload"
    )


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
    original_replace = os.replace
    swapped = False

    def replace_with_swap(
        source: str | Path,
        destination: str | Path,
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
    ) -> None:
        nonlocal swapped
        if dst_dir_fd is not None and not swapped:
            shard = root / "current" / "hash-v1"
            moved = root / "current" / "detached-hash-v1"
            shard.rename(moved)
            shard.symlink_to(outside, target_is_directory=True)
            swapped = True
        original_replace(
            source,
            destination,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
        )

    monkeypatch.setattr("h2hdb_ingest.library.os.replace", replace_with_swap)
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

    original_unlink = Path.unlink
    injected = False

    def unlink_then_interrupt(path: Path, missing_ok: bool = False) -> None:
        nonlocal injected
        original_unlink(path, missing_ok=missing_ok)
        if path.parent.name == "quarantine" and not injected:
            injected = True
            raise RuntimeError("simulated SIGKILL boundary")

    monkeypatch.setattr(Path, "unlink", unlink_then_interrupt)
    receipt = b"b" * 16
    with adapter.publication_guard():
        adapter.begin(2, receipt)
        adapter.activate_page(2, (retained,))
        adapter.seal(2)
        adapter.reconcile_page(2, receipt, limit=128)
        with pytest.raises(RuntimeError, match="SIGKILL"):
            adapter.reconcile_page(2, receipt, limit=128)

    monkeypatch.setattr(Path, "unlink", original_unlink)
    restarted = _adapter(root)
    with restarted.publication_guard():
        checkpoint = restarted.begin(2, receipt)
        assert checkpoint.status is LibraryActivationStatus.RECONCILE
        while True:
            checkpoint = restarted.reconcile_page(2, receipt, limit=128)
            if checkpoint.status is LibraryActivationStatus.READY:
                break
        restarted.complete(2, receipt)

    assert not restarted.current_path.joinpath(*removed.storage_key.segments).exists()
