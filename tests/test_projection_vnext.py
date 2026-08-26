from __future__ import annotations

import os
import sqlite3
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest

import h2hdb_ingest.projection as projection_module
from h2hdb_ingest.artifact import (
    _QUARANTINE_PAYLOAD_NAME,
    ManagedFilesystemArtifactAdapter,
    _rename_noreplace,
    _verify_regular_at,
)
from h2hdb_ingest.config import CBZGrouping
from h2hdb_ingest.projection import (
    _MAX_CLEANUP_ARTIFACTS_PER_ATTEMPT,
    CurrentProjectionAdapter,
    CurrentProjectionCheckpoint,
    CurrentProjectionItem,
    CurrentProjectionStatus,
)


def _item(
    artifact_root: Path,
    key: int,
    *,
    gid: int,
    name: str,
    payload: bytes,
) -> CurrentProjectionItem:
    assert key == gid
    digest = sha256(payload).digest()
    hexdigest = digest.hex()
    locator = ("sha256", hexdigest[:2], f"{hexdigest}.cbz")
    path = artifact_root.joinpath(*locator)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    publication = sha256(b"h2hdb-vnext-publication-key\0")
    publication.update((1).to_bytes(4, "big"))
    publication.update(gid.to_bytes(8, "big"))
    return CurrentProjectionItem(
        publication_key=publication.digest(),
        gid=gid,
        source_gallery_name=name,
        upload_time=0,
        artifact_locator_components=locator,
        artifact_sha256=digest,
        size_bytes=len(payload),
    )


def _registered_item(
    adapter: CurrentProjectionAdapter,
    artifact_root: Path,
    key: int,
    *,
    gid: int,
    name: str,
    payload: bytes,
    token: bytes,
) -> CurrentProjectionItem:
    item = _item(
        artifact_root,
        key,
        gid=gid,
        name=name,
        payload=payload,
    )
    evidence = adapter._artifact_adapter.protect(
        BytesIO(payload),
        item.artifact_locator_components,
        token,
    )
    assert evidence.stored
    return item


def _adapter(
    tmp_path: Path,
    *,
    grouping: CBZGrouping = CBZGrouping.flat,
) -> tuple[CurrentProjectionAdapter, Path, Path]:
    artifact_root = tmp_path / "artifacts"
    current_root = tmp_path / "current"
    artifacts = ManagedFilesystemArtifactAdapter(
        artifact_root,
        max_image_short_side=2400,
    )
    return (
        CurrentProjectionAdapter(
            artifact_store_path=artifact_root,
            cbz_path=current_root,
            grouping=grouping,
            artifact_adapter=artifacts,
        ),
        artifact_root,
        current_root,
    )


def _begin(
    adapter: CurrentProjectionAdapter,
    revision: int,
) -> CurrentProjectionStatus:
    checkpoint = adapter.begin(revision, revision.to_bytes(16, "big"))
    assert isinstance(checkpoint, CurrentProjectionCheckpoint)
    return checkpoint.status


def test_complete_intent_is_installed_and_stale_managed_paths_are_removed(
    tmp_path: Path,
) -> None:
    adapter, artifact_root, current_root = _adapter(tmp_path)
    first = _item(artifact_root, 1, gid=1, name="first", payload=b"first")
    second = _item(artifact_root, 2, gid=2, name="second", payload=b"second")

    with adapter.publication_guard():
        assert _begin(adapter, 1) is CurrentProjectionStatus.SPOOL
        adapter.append_page(1, (first, second))
        adapter.seal(1)
        adapter.reconcile(1)
        assert _begin(adapter, 1) is CurrentProjectionStatus.COMPLETE

    assert (current_root / "1 - first.cbz").read_bytes() == b"first"
    assert (current_root / "2 - second.cbz").read_bytes() == b"second"

    with adapter.publication_guard():
        assert _begin(adapter, 2) is CurrentProjectionStatus.SPOOL
        adapter.append_page(2, (second,))
        adapter.seal(2)
        adapter.reconcile(2)

    assert not (current_root / "1 - first.cbz").exists()
    assert (current_root / "2 - second.cbz").read_bytes() == b"second"


def test_open_spool_restart_preserves_exact_receipt_and_keyset_cursor(
    tmp_path: Path,
) -> None:
    adapter, artifact_root, current_root = _adapter(tmp_path)
    first = _item(artifact_root, 1, gid=1, name="first", payload=b"first")
    second = _item(artifact_root, 2, gid=2, name="second", payload=b"second")
    receipt = b"r" * 16

    with adapter.publication_guard():
        initial = adapter.begin(1, receipt)
        assert initial.status is CurrentProjectionStatus.SPOOL
        assert initial.last_publication_key is None
        adapter.append_page(1, (first,))

        resumed = adapter.begin(1, receipt)
        assert resumed.status is CurrentProjectionStatus.SPOOL
        assert resumed.receipt_id == receipt
        assert resumed.last_publication_key == first.publication_key
        with pytest.raises(RuntimeError, match="another publication receipt"):
            adapter.begin(1, b"f" * 16)

        adapter.append_page(1, (second,))
        adapter.seal(1)
        adapter.reconcile(1)

    assert (current_root / "1 - first.cbz").read_bytes() == b"first"
    assert (current_root / "2 - second.cbz").read_bytes() == b"second"


def test_unknown_existing_path_is_never_replaced(tmp_path: Path) -> None:
    adapter, artifact_root, current_root = _adapter(tmp_path)
    item = _item(artifact_root, 1, gid=1, name="first", payload=b"artifact")
    current_root.mkdir()
    target = current_root / "1 - first.cbz"
    target.write_bytes(b"unknown")

    with adapter.publication_guard():
        _begin(adapter, 1)
        adapter.append_page(1, (item,))
        adapter.seal(1)
        with pytest.raises(RuntimeError, match="unknown current-view path"):
            adapter.reconcile(1)

    assert target.read_bytes() == b"unknown"


def test_externally_changed_stale_managed_path_is_not_deleted(tmp_path: Path) -> None:
    adapter, artifact_root, current_root = _adapter(tmp_path)
    item = _item(artifact_root, 1, gid=1, name="first", payload=b"artifact")
    with adapter.publication_guard():
        _begin(adapter, 1)
        adapter.append_page(1, (item,))
        adapter.seal(1)
        adapter.reconcile(1)
    target = current_root / "1 - first.cbz"
    target.write_bytes(b"external mutation")

    with adapter.publication_guard():
        _begin(adapter, 2)
        adapter.seal(2)
        with pytest.raises(RuntimeError, match="externally changed managed path"):
            adapter.reconcile(2)

    assert target.read_bytes() == b"external mutation"


def test_externally_changed_managed_path_is_not_replaced(tmp_path: Path) -> None:
    adapter, artifact_root, current_root = _adapter(tmp_path)
    first = _item(artifact_root, 1, gid=1, name="first", payload=b"first")
    with adapter.publication_guard():
        _begin(adapter, 1)
        adapter.append_page(1, (first,))
        adapter.seal(1)
        adapter.reconcile(1)

    target = current_root / "1 - first.cbz"
    target.write_bytes(b"external replacement")
    replacement = _item(
        artifact_root,
        1,
        gid=1,
        name="first",
        payload=b"second",
    )
    with adapter.publication_guard():
        _begin(adapter, 2)
        adapter.append_page(2, (replacement,))
        adapter.seal(2)
        with pytest.raises(
            RuntimeError,
            match="replace externally changed managed path",
        ):
            adapter.reconcile(2)

    assert target.read_bytes() == b"external replacement"


def test_stale_path_is_rechecked_immediately_before_delete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, artifact_root, current_root = _adapter(tmp_path)
    item = _item(artifact_root, 1, gid=1, name="first", payload=b"artifact")
    with adapter.publication_guard():
        _begin(adapter, 1)
        adapter.append_page(1, (item,))
        adapter.seal(1)
        adapter.reconcile(1)
    target = current_root / "1 - first.cbz"
    original = adapter._materialize

    def materialize_then_mutate(*args: Any, **kwargs: Any) -> None:
        original(*args, **kwargs)
        target.write_bytes(b"changed after preflight")

    monkeypatch.setattr(adapter, "_materialize", materialize_then_mutate)
    with adapter.publication_guard():
        _begin(adapter, 2)
        adapter.seal(2)
        with pytest.raises(RuntimeError, match="externally changed managed path"):
            adapter.reconcile(2)

    assert target.read_bytes() == b"changed after preflight"


def test_stale_cleanup_rejects_nested_parent_symlink_and_retries_in_root(
    tmp_path: Path,
) -> None:
    adapter, artifact_root, current_root = _adapter(
        tmp_path,
        grouping=CBZGrouping.date_yyyy,
    )
    item = _item(artifact_root, 1, gid=1, name="first", payload=b"artifact")
    with adapter.publication_guard():
        _begin(adapter, 1)
        adapter.append_page(1, (item,))
        adapter.seal(1)
        adapter.reconcile(1)

    year_directory = current_root / "1970"
    outside_directory = tmp_path / "outside-current-year"
    year_directory.rename(outside_directory)
    year_directory.symlink_to(outside_directory, target_is_directory=True)
    outside_target = outside_directory / "1 - first.cbz"

    with adapter.publication_guard():
        _begin(adapter, 2)
        adapter.seal(2)
        with pytest.raises(RuntimeError, match="current projection parent is unsafe"):
            adapter.reconcile(2)

    assert outside_target.read_bytes() == b"artifact"
    year_directory.unlink()
    outside_directory.rename(year_directory)

    with adapter.publication_guard():
        assert _begin(adapter, 2) is CurrentProjectionStatus.RECONCILE
        adapter.reconcile(2)

    assert not (year_directory / "1 - first.cbz").exists()


def test_stale_cleanup_rejects_current_root_symlink_without_touching_target(
    tmp_path: Path,
) -> None:
    adapter, artifact_root, current_root = _adapter(tmp_path)
    item = _item(artifact_root, 1, gid=1, name="first", payload=b"artifact")
    with adapter.publication_guard():
        _begin(adapter, 1)
        adapter.append_page(1, (item,))
        adapter.seal(1)
        adapter.reconcile(1)

    outside_root = tmp_path / "outside-current-root"
    current_root.rename(outside_root)
    current_root.symlink_to(outside_root, target_is_directory=True)
    outside_target = outside_root / "1 - first.cbz"
    with pytest.raises(RuntimeError, match=r"current projection.*safe directory"):
        with adapter.publication_guard():
            pass

    assert outside_target.read_bytes() == b"artifact"
    current_root.unlink()
    outside_root.rename(current_root)
    with adapter.publication_guard():
        _begin(adapter, 2)
        adapter.seal(2)
        adapter.reconcile(2)
    assert not (current_root / "1 - first.cbz").exists()


def test_stale_cleanup_preserves_quarantine_when_parent_moves_during_rename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, artifact_root, current_root = _adapter(
        tmp_path,
        grouping=CBZGrouping.date_yyyy,
    )
    item = _item(artifact_root, 1, gid=1, name="first", payload=b"artifact")
    with adapter.publication_guard():
        _begin(adapter, 1)
        adapter.append_page(1, (item,))
        adapter.seal(1)
        adapter.reconcile(1)

    year_directory = current_root / "1970"
    outside_directory = tmp_path / "moved-current-parent"
    target_name = "1 - first.cbz"
    original_rename = os.rename
    original_capture = _rename_noreplace
    injected = False

    def swap_parent_then_capture(*args: Any, **kwargs: Any) -> None:
        nonlocal injected
        source = str(args[0])
        destination = str(args[1])
        if (
            not injected
            and source == target_name
            and destination == _QUARANTINE_PAYLOAD_NAME
        ):
            injected = True
            original_rename(year_directory, outside_directory)
            year_directory.symlink_to(outside_directory, target_is_directory=True)
        original_capture(*args, **kwargs)

    monkeypatch.setattr(
        projection_module,
        "_rename_noreplace",
        swap_parent_then_capture,
    )
    with adapter.publication_guard():
        _begin(adapter, 2)
        adapter.seal(2)
        with pytest.raises(RuntimeError, match="current projection parent changed"):
            adapter.reconcile(2)

    quarantines = tuple(
        (current_root / projection_module._CURRENT_QUARANTINE_NAME).glob(
            "stale-*/payload"
        )
    )
    assert len(quarantines) == 1
    assert quarantines[0].read_bytes() == b"artifact"
    year_directory.unlink()
    original_rename(outside_directory, year_directory)
    with adapter.publication_guard():
        assert _begin(adapter, 2) is CurrentProjectionStatus.RECONCILE
        adapter.reconcile(2)
    assert not quarantines[0].exists()


def test_stale_cleanup_rejects_leaf_symlink_without_deleting_outside_file(
    tmp_path: Path,
) -> None:
    adapter, artifact_root, current_root = _adapter(tmp_path)
    item = _item(artifact_root, 1, gid=1, name="first", payload=b"artifact")
    with adapter.publication_guard():
        _begin(adapter, 1)
        adapter.append_page(1, (item,))
        adapter.seal(1)
        adapter.reconcile(1)

    target = current_root / "1 - first.cbz"
    outside = tmp_path / "outside-current.cbz"
    outside.write_bytes(b"outside")
    target.unlink()
    target.symlink_to(outside)
    with adapter.publication_guard():
        _begin(adapter, 2)
        adapter.seal(2)
        with pytest.raises(RuntimeError, match="not a regular file"):
            adapter.reconcile(2)

    assert target.is_symlink()
    assert outside.read_bytes() == b"outside"
    target.unlink()
    with adapter.publication_guard():
        assert _begin(adapter, 2) is CurrentProjectionStatus.RECONCILE
        adapter.reconcile(2)
    assert outside.read_bytes() == b"outside"


def test_stale_cleanup_quarantines_leaf_swapped_after_signature_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, artifact_root, current_root = _adapter(tmp_path)
    item = _item(artifact_root, 1, gid=1, name="first", payload=b"artifact")
    with adapter.publication_guard():
        _begin(adapter, 1)
        adapter.append_page(1, (item,))
        adapter.seal(1)
        adapter.reconcile(1)

    target = current_root / "1 - first.cbz"
    saved_managed = tmp_path / "saved-current.cbz"
    external_payload = b"external stale race"
    original_rename = os.rename
    original_capture = _rename_noreplace
    injected = False

    def swap_leaf_then_capture(*args: Any, **kwargs: Any) -> None:
        nonlocal injected
        source = str(args[0])
        destination = str(args[1])
        if (
            not injected
            and source == target.name
            and destination == _QUARANTINE_PAYLOAD_NAME
        ):
            injected = True
            original_rename(target, saved_managed)
            target.write_bytes(external_payload)
        original_capture(*args, **kwargs)

    monkeypatch.setattr(
        projection_module,
        "_rename_noreplace",
        swap_leaf_then_capture,
    )
    with adapter.publication_guard():
        _begin(adapter, 2)
        adapter.seal(2)
        with pytest.raises(RuntimeError, match="quarantine failed verification"):
            adapter.reconcile(2)

    quarantines = tuple(
        (current_root / projection_module._CURRENT_QUARANTINE_NAME).glob(
            "stale-*/payload"
        )
    )
    assert len(quarantines) == 1
    assert quarantines[0].read_bytes() == external_payload
    assert saved_managed.read_bytes() == b"artifact"

    quarantines[0].unlink()
    original_rename(saved_managed, quarantines[0])
    with adapter.publication_guard():
        assert _begin(adapter, 2) is CurrentProjectionStatus.RECONCILE
        adapter.reconcile(2)
    assert not target.exists()


def test_managed_replace_rejects_nested_parent_symlink_and_retries_in_root(
    tmp_path: Path,
) -> None:
    adapter, artifact_root, current_root = _adapter(
        tmp_path,
        grouping=CBZGrouping.date_yyyy,
    )
    first = _item(artifact_root, 1, gid=1, name="first", payload=b"first")
    with adapter.publication_guard():
        _begin(adapter, 1)
        adapter.append_page(1, (first,))
        adapter.seal(1)
        adapter.reconcile(1)

    replacement = _item(
        artifact_root,
        1,
        gid=1,
        name="first",
        payload=b"second",
    )
    year_directory = current_root / "1970"
    outside_directory = tmp_path / "outside-replace-year"
    year_directory.rename(outside_directory)
    year_directory.symlink_to(outside_directory, target_is_directory=True)
    outside_target = outside_directory / "1 - first.cbz"
    with adapter.publication_guard():
        _begin(adapter, 2)
        adapter.append_page(2, (replacement,))
        adapter.seal(2)
        with pytest.raises(RuntimeError, match="current projection parent is unsafe"):
            adapter.reconcile(2)

    assert outside_target.read_bytes() == b"first"
    year_directory.unlink()
    outside_directory.rename(year_directory)
    with adapter.publication_guard():
        assert _begin(adapter, 2) is CurrentProjectionStatus.RECONCILE
        adapter.reconcile(2)
    assert (year_directory / "1 - first.cbz").read_bytes() == b"second"


def test_managed_replace_quarantines_leaf_swapped_after_signature_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, artifact_root, current_root = _adapter(tmp_path)
    first = _item(artifact_root, 1, gid=1, name="first", payload=b"first")
    with adapter.publication_guard():
        _begin(adapter, 1)
        adapter.append_page(1, (first,))
        adapter.seal(1)
        adapter.reconcile(1)

    replacement = _item(
        artifact_root,
        1,
        gid=1,
        name="first",
        payload=b"second",
    )
    target = current_root / "1 - first.cbz"
    saved_managed = tmp_path / "saved-replaced.cbz"
    external_payload = b"external replace race"
    original_rename = os.rename
    original_capture = _rename_noreplace
    injected = False

    def swap_leaf_then_capture(*args: Any, **kwargs: Any) -> None:
        nonlocal injected
        source = str(args[0])
        destination = str(args[1])
        if (
            not injected
            and source == target.name
            and destination == _QUARANTINE_PAYLOAD_NAME
        ):
            injected = True
            original_rename(target, saved_managed)
            target.write_bytes(external_payload)
        original_capture(*args, **kwargs)

    monkeypatch.setattr(
        projection_module,
        "_rename_noreplace",
        swap_leaf_then_capture,
    )
    with adapter.publication_guard():
        _begin(adapter, 2)
        adapter.append_page(2, (replacement,))
        adapter.seal(2)
        with pytest.raises(RuntimeError, match="quarantine failed verification"):
            adapter.reconcile(2)

    quarantines = tuple(
        (current_root / projection_module._CURRENT_QUARANTINE_NAME).glob(
            "replace-*/payload"
        )
    )
    assert len(quarantines) == 1
    assert quarantines[0].read_bytes() == external_payload
    assert saved_managed.read_bytes() == b"first"

    quarantines[0].unlink()
    original_rename(saved_managed, quarantines[0])
    with adapter.publication_guard():
        assert _begin(adapter, 2) is CurrentProjectionStatus.RECONCILE
        adapter.reconcile(2)
    assert target.read_bytes() == b"second"


def test_managed_replace_destination_race_preserves_unknown_private_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, artifact_root, current_root = _adapter(tmp_path)
    first = _item(artifact_root, 1, gid=1, name="first", payload=b"first")
    with adapter.publication_guard():
        _begin(adapter, 1)
        adapter.append_page(1, (first,))
        adapter.seal(1)
        adapter.reconcile(1)

    replacement = _item(
        artifact_root,
        1,
        gid=1,
        name="first",
        payload=b"second",
    )
    target = current_root / "1 - first.cbz"
    external_payload = b"unknown replacement destination"
    original_capture = _rename_noreplace
    injected = False

    def create_destination_then_capture(*args: Any, **kwargs: Any) -> None:
        nonlocal injected
        if not injected:
            injected = True
            descriptor = os.open(
                _QUARANTINE_PAYLOAD_NAME,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
                dir_fd=int(kwargs["destination_descriptor"]),
            )
            with os.fdopen(descriptor, "wb", closefd=True) as destination:
                destination.write(external_payload)
                destination.flush()
                os.fsync(destination.fileno())
        original_capture(*args, **kwargs)

    monkeypatch.setattr(
        projection_module,
        "_rename_noreplace",
        create_destination_then_capture,
    )
    with adapter.publication_guard():
        _begin(adapter, 2)
        adapter.append_page(2, (replacement,))
        adapter.seal(2)
        with pytest.raises(RuntimeError, match="quarantine destination changed"):
            adapter.reconcile(2)

    quarantines = tuple(
        (current_root / projection_module._CURRENT_QUARANTINE_NAME).glob(
            "replace-*/payload"
        )
    )
    assert len(quarantines) == 1
    assert quarantines[0].read_bytes() == external_payload
    assert target.read_bytes() == b"first"
    with sqlite3.connect(
        artifact_root / ".h2hdb-vnext-current-projection.sqlite3"
    ) as state:
        assert state.execute(
            "SELECT current_revision, phase FROM projection_state WHERE singleton = 1"
        ).fetchone() == (1, "APPLYING")

    quarantines[0].unlink()
    with adapter.publication_guard():
        assert _begin(adapter, 2) is CurrentProjectionStatus.RECONCILE
        adapter.reconcile(2)
    assert target.read_bytes() == b"second"


def test_stale_cleanup_rechecks_private_payload_before_unlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, artifact_root, current_root = _adapter(tmp_path)
    item = _item(artifact_root, 1, gid=1, name="first", payload=b"artifact")
    with adapter.publication_guard():
        _begin(adapter, 1)
        adapter.append_page(1, (item,))
        adapter.seal(1)
        adapter.reconcile(1)

    external_payload = b"unknown stale post-verify bytes"
    saved_managed = tmp_path / "saved-stale-private-payload.cbz"
    original_verify = _verify_regular_at
    injected = False

    def verify_then_mutate(
        parent_descriptor: int,
        name: str,
        *,
        expected_sha256: bytes,
        expected_size: int,
    ) -> os.stat_result:
        nonlocal injected
        result = original_verify(
            parent_descriptor,
            name,
            expected_sha256=expected_sha256,
            expected_size=expected_size,
        )
        if not injected and name == _QUARANTINE_PAYLOAD_NAME:
            injected = True
            os.rename(name, saved_managed, src_dir_fd=parent_descriptor)
            descriptor = os.open(
                name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
                dir_fd=parent_descriptor,
            )
            with os.fdopen(descriptor, "wb", closefd=True) as destination:
                destination.write(external_payload)
                destination.flush()
                os.fsync(destination.fileno())
        return result

    monkeypatch.setattr(
        projection_module,
        "_verify_regular_at",
        verify_then_mutate,
    )
    with adapter.publication_guard():
        _begin(adapter, 2)
        adapter.seal(2)
        with pytest.raises(RuntimeError, match="quarantine failed verification"):
            adapter.reconcile(2)

    quarantines = tuple(
        (current_root / projection_module._CURRENT_QUARANTINE_NAME).glob(
            "stale-*/payload"
        )
    )
    assert len(quarantines) == 1
    assert quarantines[0].read_bytes() == external_payload
    assert saved_managed.read_bytes() == b"artifact"
    with sqlite3.connect(
        artifact_root / ".h2hdb-vnext-current-projection.sqlite3"
    ) as state:
        assert state.execute(
            "SELECT current_revision, phase FROM projection_state WHERE singleton = 1"
        ).fetchone() == (1, "APPLYING")

    quarantines[0].unlink()
    saved_managed.rename(quarantines[0])
    with adapter.publication_guard():
        assert _begin(adapter, 2) is CurrentProjectionStatus.RECONCILE
        adapter.reconcile(2)
    assert not quarantines[0].exists()


def test_applying_intent_recovers_after_copy_before_state_update(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, artifact_root, current_root = _adapter(tmp_path)
    item = _item(artifact_root, 1, gid=1, name="first", payload=b"artifact")
    original = adapter._atomic_copy
    crashed = False

    def copy_then_crash(
        *args: Any, **kwargs: Any
    ) -> tuple[bytes, bytes, int, int, int]:
        nonlocal crashed
        result = original(*args, **kwargs)
        if not crashed:
            crashed = True
            raise RuntimeError("injected crash")
        return result

    monkeypatch.setattr(adapter, "_atomic_copy", copy_then_crash)
    with adapter.publication_guard():
        _begin(adapter, 1)
        adapter.append_page(1, (item,))
        adapter.seal(1)
        with pytest.raises(RuntimeError, match="injected crash"):
            adapter.reconcile(1)
        assert _begin(adapter, 1) is CurrentProjectionStatus.RECONCILE
        adapter.reconcile(1)

    assert (current_root / "1 - first.cbz").read_bytes() == b"artifact"


def test_projection_requires_guard_and_rejects_duplicate_publication_keys(
    tmp_path: Path,
) -> None:
    adapter, artifact_root, _current_root = _adapter(tmp_path)
    first = _item(artifact_root, 1, gid=1, name="same", payload=b"first")
    collision = _item(artifact_root, 1, gid=1, name="same", payload=b"second")

    with pytest.raises(RuntimeError, match="publication_guard"):
        _begin(adapter, 1)
    with adapter.publication_guard():
        _begin(adapter, 1)
        with pytest.raises(ValueError, match="strictly increasing"):
            adapter.append_page(1, (first, collision))


def test_projection_authorities_have_exact_epoch2_domains(tmp_path: Path) -> None:
    adapter, artifact_root, _current_root = _adapter(tmp_path)
    valid = _item(artifact_root, 1, gid=1, name="first", payload=b"artifact")

    with pytest.raises(ValueError, match="32\\.\\.32 bytes"):
        CurrentProjectionItem(
            publication_key=b"short",
            gid=valid.gid,
            source_gallery_name=valid.source_gallery_name,
            upload_time=valid.upload_time,
            artifact_locator_components=valid.artifact_locator_components,
            artifact_sha256=valid.artifact_sha256,
            size_bytes=valid.size_bytes,
        )
    with adapter.publication_guard():
        with pytest.raises(ValueError, match="positive signed int63"):
            _begin(adapter, 0)
        with pytest.raises(ValueError, match="exactly 16 bytes"):
            adapter.begin(1, b"short")


def test_two_revisions_prune_old_immutable_cbz_and_ingest_state(
    tmp_path: Path,
) -> None:
    adapter, artifact_root, current_root = _adapter(tmp_path)
    first_token = b"1" * 184
    second_token = b"2" * 184
    first = _registered_item(
        adapter,
        artifact_root,
        1,
        gid=1,
        name="first",
        payload=b"first immutable",
        token=first_token,
    )
    first_path = artifact_root.joinpath(*first.artifact_locator_components)

    with adapter.publication_guard():
        _begin(adapter, 1)
        adapter.append_page(1, (first,))
        adapter.seal(1)
        adapter.reconcile(1)
        adapter._artifact_adapter.release(
            first.artifact_locator_components,
            first_token,
        )

    second = _registered_item(
        adapter,
        artifact_root,
        2,
        gid=2,
        name="second",
        payload=b"second immutable",
        token=second_token,
    )
    second_path = artifact_root.joinpath(*second.artifact_locator_components)
    with adapter.publication_guard():
        _begin(adapter, 2)
        adapter.append_page(2, (second,))
        adapter.seal(2)
        adapter.reconcile(2)
        adapter._artifact_adapter.release(
            second.artifact_locator_components,
            second_token,
        )

    assert not first_path.exists()
    assert second_path.read_bytes() == b"second immutable"
    assert not (current_root / "1 - first.cbz").exists()
    assert (current_root / "2 - second.cbz").read_bytes() == b"second immutable"
    with sqlite3.connect(artifact_root / ".h2hdb-vnext-artifacts.sqlite3") as state:
        assert state.execute(
            "SELECT locator FROM artifacts ORDER BY locator"
        ).fetchall() == [("/".join(second.artifact_locator_components),)]
        assert state.execute(
            "SELECT token, state FROM protection_tokens ORDER BY token"
        ).fetchall() == [
            (first_token, "RELEASED"),
            (second_token, "RELEASED"),
        ]
    assert not adapter._artifact_adapter.protect(
        BytesIO(b"first immutable"),
        first.artifact_locator_components,
        first_token,
    ).stored
    assert not first_path.exists()


def test_two_revision_cleanup_rejects_symlink_and_recovers_without_touching_current(
    tmp_path: Path,
) -> None:
    adapter, artifact_root, current_root = _adapter(tmp_path)
    first_token = b"1" * 184
    second_token = b"2" * 184
    first_payload = b"first immutable"
    first = _registered_item(
        adapter,
        artifact_root,
        1,
        gid=1,
        name="first",
        payload=first_payload,
        token=first_token,
    )
    first_path = artifact_root.joinpath(*first.artifact_locator_components)
    with adapter.publication_guard():
        _begin(adapter, 1)
        adapter.append_page(1, (first,))
        adapter.seal(1)
        adapter.reconcile(1)
        adapter._artifact_adapter.release(
            first.artifact_locator_components,
            first_token,
        )

    outside = tmp_path / "outside.cbz"
    outside.write_bytes(first_payload)
    first_path.unlink()
    first_path.symlink_to(outside)
    second = _registered_item(
        adapter,
        artifact_root,
        2,
        gid=2,
        name="second",
        payload=b"second immutable",
        token=second_token,
    )
    second_path = artifact_root.joinpath(*second.artifact_locator_components)
    with adapter.publication_guard():
        _begin(adapter, 2)
        adapter.append_page(2, (second,))
        adapter.seal(2)
        with pytest.raises(RuntimeError, match="not a regular file"):
            adapter.reconcile(2)

    assert first_path.is_symlink()
    assert outside.read_bytes() == first_payload
    assert second_path.read_bytes() == b"second immutable"
    assert (current_root / "2 - second.cbz").read_bytes() == b"second immutable"

    first_path.unlink()
    first_path.write_bytes(first_payload)
    with adapter.publication_guard():
        assert _begin(adapter, 2) is CurrentProjectionStatus.COMPLETE
        adapter._artifact_adapter.release(
            second.artifact_locator_components,
            second_token,
        )
    assert not first_path.exists()
    assert second_path.exists()


def test_cleanup_attempt_is_bounded_and_resumes_from_durable_cursor(
    tmp_path: Path,
) -> None:
    adapter, artifact_root, _current_root = _adapter(tmp_path)
    total = _MAX_CLEANUP_ARTIFACTS_PER_ATTEMPT + 3
    registered: list[tuple[CurrentProjectionItem, bytes]] = []
    for index in range(1, total + 1):
        token = index.to_bytes(8, "big") + b"t" * 176
        registered.append(
            (
                _registered_item(
                    adapter,
                    artifact_root,
                    index,
                    gid=index,
                    name=f"gallery-{index}",
                    payload=f"immutable-{index}".encode(),
                    token=token,
                ),
                token,
            )
        )

    with adapter.publication_guard():
        _begin(adapter, 1)
        adapter.append_page(
            1,
            tuple(
                sorted(
                    (item for item, _token in registered),
                    key=lambda x: x.publication_key,
                )
            ),
        )
        adapter.seal(1)
        adapter.reconcile(1)
        for item, token in registered:
            adapter._artifact_adapter.release(
                item.artifact_locator_components,
                token,
            )

        _begin(adapter, 2)
        adapter.seal(2)
        adapter.reconcile(2)

    with sqlite3.connect(artifact_root / ".h2hdb-vnext-artifacts.sqlite3") as state:
        assert state.execute("SELECT COUNT(*) FROM artifacts").fetchone() == (3,)
        assert state.execute("SELECT COUNT(*) FROM protection_tokens").fetchone() == (
            total,
        )
        assert state.execute(
            "SELECT COUNT(*) FROM artifact_cleanup_candidates"
        ).fetchone() == (3,)
        assert state.execute(
            "SELECT after_sha256 IS NOT NULL FROM artifact_cleanup_state "
            "WHERE singleton = 1"
        ).fetchone() == (1,)
    with sqlite3.connect(
        artifact_root / ".h2hdb-vnext-current-projection.sqlite3"
    ) as state:
        assert state.execute(
            "SELECT COUNT(*) FROM artifact_cleanup_candidates"
        ).fetchone() == (3,)

    restarted, _artifact_root, _current_root = _adapter(tmp_path)
    with restarted.publication_guard():
        assert _begin(restarted, 2) is CurrentProjectionStatus.COMPLETE

    assert all(
        not artifact_root.joinpath(*item.artifact_locator_components).exists()
        for item, _token in registered
    )
    with sqlite3.connect(artifact_root / ".h2hdb-vnext-artifacts.sqlite3") as state:
        assert state.execute("SELECT COUNT(*) FROM artifacts").fetchone() == (0,)
        assert state.execute("SELECT COUNT(*) FROM protection_tokens").fetchone() == (
            total,
        )
        assert state.execute(
            "SELECT COUNT(*) FROM artifact_cleanup_candidates"
        ).fetchone() == (0,)
        assert state.execute(
            "SELECT after_sha256 FROM artifact_cleanup_state WHERE singleton = 1"
        ).fetchone() == (None,)
    with sqlite3.connect(
        artifact_root / ".h2hdb-vnext-current-projection.sqlite3"
    ) as state:
        assert state.execute(
            "SELECT COUNT(*) FROM artifact_cleanup_candidates"
        ).fetchone() == (0,)

    first, first_token = registered[0]
    assert not adapter._artifact_adapter.protect(
        BytesIO(b"immutable-1"),
        first.artifact_locator_components,
        first_token,
    ).stored


def test_cleanup_interruption_replays_durable_candidates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, artifact_root, _current_root = _adapter(tmp_path)
    registered: list[tuple[CurrentProjectionItem, bytes]] = []
    for index in range(1, 4):
        token = index.to_bytes(8, "big") + b"r" * 176
        registered.append(
            (
                _registered_item(
                    adapter,
                    artifact_root,
                    index,
                    gid=index,
                    name=f"gallery-{index}",
                    payload=f"released-{index}".encode(),
                    token=token,
                ),
                token,
            )
        )

    with adapter.publication_guard():
        _begin(adapter, 1)
        adapter.append_page(
            1,
            tuple(
                sorted(
                    (item for item, _token in registered),
                    key=lambda x: x.publication_key,
                )
            ),
        )
        adapter.seal(1)
        adapter.reconcile(1)
        for item, token in registered:
            adapter._artifact_adapter.release(
                item.artifact_locator_components,
                token,
            )

    original = adapter._artifact_adapter._prune_released_artifact_in_state
    completed: list[bytes] = []

    def prune_then_interrupt(
        connection: sqlite3.Connection,
        digest: bytes,
    ) -> bool:
        if completed:
            raise RuntimeError("injected cleanup interruption")
        result = original(connection, digest)
        completed.append(digest)
        return result

    monkeypatch.setattr(
        adapter._artifact_adapter,
        "_prune_released_artifact_in_state",
        prune_then_interrupt,
    )
    with adapter.publication_guard():
        _begin(adapter, 2)
        adapter.seal(2)
        with pytest.raises(RuntimeError, match="injected cleanup interruption"):
            adapter.reconcile(2)

    assert len(completed) == 1
    with sqlite3.connect(
        artifact_root / ".h2hdb-vnext-current-projection.sqlite3"
    ) as state:
        assert state.execute(
            "SELECT current_revision, phase FROM projection_state WHERE singleton = 1"
        ).fetchone() == (2, "IDLE")
        assert state.execute(
            "SELECT COUNT(*) FROM artifact_cleanup_candidates"
        ).fetchone() == (0,)
    with sqlite3.connect(artifact_root / ".h2hdb-vnext-artifacts.sqlite3") as state:
        assert state.execute("SELECT COUNT(*) FROM artifacts").fetchone() == (2,)
        assert state.execute(
            "SELECT COUNT(*) FROM artifact_cleanup_candidates"
        ).fetchone() == (3,)

    restarted, _artifact_root, _current_root = _adapter(tmp_path)
    with restarted.publication_guard():
        assert _begin(restarted, 2) is CurrentProjectionStatus.COMPLETE

    assert all(
        not artifact_root.joinpath(*item.artifact_locator_components).exists()
        for item, _token in registered
    )
    with sqlite3.connect(
        artifact_root / ".h2hdb-vnext-current-projection.sqlite3"
    ) as state:
        assert state.execute(
            "SELECT COUNT(*) FROM artifact_cleanup_candidates"
        ).fetchone() == (0,)
    with sqlite3.connect(artifact_root / ".h2hdb-vnext-artifacts.sqlite3") as state:
        assert state.execute(
            "SELECT COUNT(*) FROM artifact_cleanup_candidates"
        ).fetchone() == (0,)


def test_released_artifact_that_was_never_projected_is_cleaned(tmp_path: Path) -> None:
    adapter, artifact_root, current_root = _adapter(tmp_path)
    token = b"n" * 184
    payload = b"prepared but never projected"
    item = _registered_item(
        adapter,
        artifact_root,
        1,
        gid=1,
        name="never-current",
        payload=payload,
        token=token,
    )
    artifact_path = artifact_root.joinpath(*item.artifact_locator_components)
    adapter._artifact_adapter.release(item.artifact_locator_components, token)

    with sqlite3.connect(artifact_root / ".h2hdb-vnext-artifacts.sqlite3") as state:
        assert state.execute(
            "SELECT artifact_sha256 FROM artifact_cleanup_candidates"
        ).fetchall() == [(item.artifact_sha256,)]

    with adapter.publication_guard():
        _begin(adapter, 1)
        adapter.seal(1)
        adapter.reconcile(1)

    assert not artifact_path.exists()
    assert tuple(current_root.rglob("*.cbz")) == ()
    with sqlite3.connect(artifact_root / ".h2hdb-vnext-artifacts.sqlite3") as state:
        assert state.execute("SELECT COUNT(*) FROM artifacts").fetchone() == (0,)
        assert state.execute(
            "SELECT token, state FROM protection_tokens"
        ).fetchall() == [(token, "RELEASED")]
        assert state.execute(
            "SELECT COUNT(*) FROM artifact_cleanup_candidates"
        ).fetchone() == (0,)
    assert not adapter._artifact_adapter.protect(
        BytesIO(payload),
        item.artifact_locator_components,
        token,
    ).stored


def test_cleanup_candidate_remains_fenced_until_protection_is_released(
    tmp_path: Path,
) -> None:
    adapter, artifact_root, _current_root = _adapter(tmp_path)
    token = b"p" * 184
    item = _registered_item(
        adapter,
        artifact_root,
        1,
        gid=1,
        name="protected",
        payload=b"still protected",
        token=token,
    )
    artifact_path = artifact_root.joinpath(*item.artifact_locator_components)

    with adapter.publication_guard():
        _begin(adapter, 1)
        adapter.append_page(1, (item,))
        adapter.seal(1)
        adapter.reconcile(1)
        _begin(adapter, 2)
        adapter.seal(2)
        adapter.reconcile(2)

    assert artifact_path.read_bytes() == b"still protected"
    with sqlite3.connect(artifact_root / ".h2hdb-vnext-artifacts.sqlite3") as state:
        assert state.execute(
            "SELECT artifact_sha256 FROM artifact_cleanup_candidates"
        ).fetchall() == [(item.artifact_sha256,)]

    adapter._artifact_adapter.release(item.artifact_locator_components, token)
    with adapter.publication_guard():
        assert _begin(adapter, 2) is CurrentProjectionStatus.COMPLETE

    assert not artifact_path.exists()
    with sqlite3.connect(artifact_root / ".h2hdb-vnext-artifacts.sqlite3") as state:
        assert state.execute(
            "SELECT COUNT(*) FROM artifact_cleanup_candidates"
        ).fetchone() == (0,)
        assert state.execute(
            "SELECT token, state FROM protection_tokens"
        ).fetchall() == [(token, "RELEASED")]
