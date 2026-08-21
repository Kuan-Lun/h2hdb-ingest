from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

from h2hdb_ingest.config import CBZGrouping
from h2hdb_ingest.projection import (
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


def _adapter(tmp_path: Path) -> tuple[CurrentProjectionAdapter, Path, Path]:
    artifact_root = tmp_path / "artifacts"
    current_root = tmp_path / "current"
    return (
        CurrentProjectionAdapter(
            artifact_store_path=artifact_root,
            cbz_path=current_root,
            grouping=CBZGrouping.flat,
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
