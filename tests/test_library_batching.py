from __future__ import annotations

import os
import signal
import sqlite3
import subprocess
import sys
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from pathlib import Path
from typing import cast

import pytest
from h2hdb import CatalogResourceKind, LibraryActivationStatus
from library_fixtures import (
    _activate,
    _ActivationItem,
    _adapter,
    _item,
    _item_kind,
    _protect,
)

import h2hdb_ingest.library as library_module
from h2hdb_ingest.library import ManagedFilesystemLibraryAdapter

_RECEIPT = b"b" * 16


def _ordered(items: tuple[_ActivationItem, ...]) -> tuple[_ActivationItem, ...]:
    return tuple(
        sorted(items, key=lambda item: (item.publication_key, item.resource_kind.value))
    )


def _journal(root: Path) -> Path:
    return root / ".h2hdb-state/journal/library-activation.sqlite3"


def _rows(root: Path, sql: str) -> list[tuple[object, ...]]:
    with closing(sqlite3.connect(_journal(root))) as connection:
        return connection.execute(sql).fetchall()


def _mixed_page(
    root: Path,
) -> tuple[
    ManagedFilesystemLibraryAdapter, tuple[_ActivationItem, ...], dict[int, bytes]
]:
    adapter = _adapter(root)
    old = _ordered((_item(1, b"unchanged"), _item(2, b"old")))
    for item in old:
        _protect(adapter, item, b"unchanged" if item.gid == 1 else b"old", item.gid)
    _activate(adapter, 1, b"a" * 16, old)
    payloads = {1: b"unchanged", 2: b"replacement", 3: b"new"}
    items = _ordered(
        (
            old[0] if old[0].gid == 1 else old[1],
            _item(2, payloads[2]),
            _item(3, payloads[3]),
            _item_kind(3, payloads[3], CatalogResourceKind.THUMBNAIL),
        )
    )
    for index, item in enumerate(items, start=10):
        if item.gid != 1:
            _protect(adapter, item, payloads[item.gid], index)
    _spool(adapter, items)
    return adapter, items, payloads


def _spool(
    adapter: ManagedFilesystemLibraryAdapter, items: tuple[_ActivationItem, ...]
) -> None:
    with adapter.publication_guard():
        adapter.begin(2, _RECEIPT)
        for offset in range(0, len(items), 128):
            adapter.activate_page(2, items[offset : offset + 128])
        adapter.seal(2)


def _resume(root: Path) -> ManagedFilesystemLibraryAdapter:
    adapter = _adapter(root)
    with adapter.publication_guard():
        checkpoint = adapter.begin(2, _RECEIPT)
        while checkpoint.status != LibraryActivationStatus.READY:
            checkpoint = adapter.reconcile_page(2, _RECEIPT, limit=128)
        adapter.complete(2, _RECEIPT)
    return adapter


def _assert_installed(
    root: Path, items: tuple[_ActivationItem, ...], payloads: dict[int, bytes]
) -> None:
    assert _rows(
        root, "SELECT current_revision, pending_revision, phase FROM library_state"
    ) == [(2, None, "IDLE")]
    assert _rows(root, "SELECT COUNT(*) FROM current_entries") == [(len(items),)]
    assert _rows(
        root,
        "SELECT COUNT(*) FROM pending_entries WHERE activation_revision=2 AND activated=0",
    ) == [(0,)]
    for item in items:
        target = root / "current" / Path(*item.storage_key.segments)
        assert target.read_bytes() == payloads[item.gid]
        signature = library_module._Signature.from_stat(target.stat())
        with closing(sqlite3.connect(_journal(root))) as connection:
            assert (
                connection.execute(
                    "SELECT device, inode, size_bytes, modified_ns, changed_ns FROM current_entries WHERE publication_key=? AND resource_kind=?",
                    (item.publication_key, item.resource_kind.value),
                ).fetchone()
                == signature.sql()
            )
    assert not (root / ".h2hdb-coordination/ACTIVATING").exists()


def _fail_after(
    monkeypatch: pytest.MonkeyPatch,
    adapter: ManagedFilesystemLibraryAdapter,
    method: str,
    occurrence: int,
) -> None:
    original = cast(Callable[..., object], getattr(adapter, method))
    calls = 0

    def fail(*args: object, **kwargs: object) -> object:
        nonlocal calls
        result = original(*args, **kwargs)
        calls += 1
        if calls == occurrence:
            raise RuntimeError("batch interruption")
        return result

    monkeypatch.setattr(adapter, method, fail)


@pytest.mark.parametrize(
    ("method", "occurrence", "committed"),
    (
        ("_reserve_pending_installs", 1, False),
        ("_perform_pending_install", 1, False),
        ("_perform_pending_install", 3, False),
        ("_perform_pending_install", 4, False),
        ("_terminalize_pending_install_in_transaction", 1, False),
        ("_record_reconcile_cursor_in_transaction", 1, False),
        ("_commit_pending_installs", 1, True),
    ),
)
def test_install_page_recovers_mixed_durable_boundaries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    occurrence: int,
    committed: bool,
) -> None:
    root = tmp_path / "library"
    adapter, items, payloads = _mixed_page(root)
    original_current = _rows(
        root, "SELECT * FROM current_entries ORDER BY publication_key, resource_kind"
    )
    with monkeypatch.context() as scoped:
        _fail_after(scoped, adapter, method, occurrence)
        with (
            adapter.publication_guard(),
            pytest.raises(RuntimeError, match="batch interruption"),
        ):
            adapter.begin(2, _RECEIPT)
            adapter.reconcile_page(2, _RECEIPT, limit=128)
    assert (root / ".h2hdb-coordination/ACTIVATING").is_file()
    assert _rows(
        root,
        "SELECT operation_started, activated FROM pending_entries WHERE activation_revision=2",
    ) == [(1, int(committed))] * len(items)
    if not committed:
        assert (
            _rows(
                root,
                "SELECT * FROM current_entries ORDER BY publication_key, resource_kind",
            )
            == original_current
        )
        assert _rows(root, "SELECT last_cursor FROM library_state") == [(None,)]
        assert _rows(
            root, "SELECT COUNT(*) FROM protection_tokens WHERE state='STAGED'"
        ) == [(3,)]
    else:
        assert _rows(root, "SELECT last_cursor FROM library_state") == [
            (library_module._activation_key(items[-1]),)
        ]
    replay_flags: list[bool] = []
    original = ManagedFilesystemLibraryAdapter._perform_pending_install

    def record(
        self: ManagedFilesystemLibraryAdapter, plan: library_module._PendingInstall
    ) -> tuple[library_module._Signature, bytes | None]:
        replay_flags.append(plan.fresh_authorization)
        return original(self, plan)

    monkeypatch.setattr(
        ManagedFilesystemLibraryAdapter, "_perform_pending_install", record
    )
    _resume(root)
    assert replay_flags == ([] if committed else [False] * len(items))
    _assert_installed(root, items, payloads)


@pytest.mark.parametrize(
    ("method", "occurrence", "committed"),
    (
        ("_reserve_pending_installs", 1, False),
        ("_perform_pending_install", 2, False),
        ("_terminalize_pending_install_in_transaction", 1, False),
        ("_commit_pending_installs", 1, True),
    ),
)
def test_install_batch_real_sigkill_reopens_sqlite_journal(
    tmp_path: Path, method: str, occurrence: int, committed: bool
) -> None:
    root = tmp_path / "library"
    _, items, payloads = _mixed_page(root)
    tests = Path(__file__).parent
    source = tests.parent / "src"
    program = """
import os, signal, sys
from pathlib import Path
from library_fixtures import _adapter
adapter = _adapter(Path(sys.argv[1]))
method = sys.argv[2]
remaining = int(sys.argv[3])
original = getattr(adapter, method)
def kill(*args, **kwargs):
    global remaining
    result = original(*args, **kwargs)
    remaining -= 1
    if not remaining:
        os.kill(os.getpid(), signal.SIGKILL)
    return result
setattr(adapter, method, kill)
with adapter.publication_guard():
    adapter.begin(2, b'b' * 16)
    adapter.reconcile_page(2, b'b' * 16, limit=128)
raise RuntimeError('fault did not fire')
"""
    environment = dict(
        os.environ, PYTHONPATH=os.pathsep.join((str(source), str(tests)))
    )
    result = subprocess.run(
        [sys.executable, "-c", program, str(root), method, str(occurrence)],
        env=environment,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == -signal.SIGKILL, result.stderr
    assert _rows(
        root,
        "SELECT operation_started, activated FROM pending_entries WHERE activation_revision=2",
    ) == [(1, int(committed))] * len(items)
    _resume(root)
    _assert_installed(root, items, payloads)


@pytest.mark.parametrize(
    ("method", "occurrence", "committed"),
    (
        ("_reserve_pending_removals", 1, False),
        ("_perform_pending_removal", 1, False),
        ("_perform_pending_removal", 3, False),
        ("_terminalize_pending_removal_in_transaction", 1, False),
        ("_record_reconcile_cursor_in_transaction", 1, False),
        ("_commit_pending_removals", 1, True),
    ),
)
def test_removal_page_recovers_partial_io_and_atomic_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    occurrence: int,
    committed: bool,
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    items = _ordered(tuple(_item(gid, b"stale") for gid in range(1, 4)))
    for item in items:
        _protect(adapter, item, b"stale", item.gid)
    _activate(adapter, 1, b"a" * 16, items)
    _spool(adapter, ())
    with monkeypatch.context() as scoped:
        _fail_after(scoped, adapter, method, occurrence)
        with (
            adapter.publication_guard(),
            pytest.raises(RuntimeError, match="batch interruption"),
        ):
            adapter.begin(2, _RECEIPT)
            adapter.reconcile_page(2, _RECEIPT, limit=128)
    assert _rows(root, "SELECT COUNT(*) FROM current_entries") == [
        (0 if committed else 3,)
    ]
    assert _rows(root, "SELECT operation_started FROM pending_removals") == (
        [] if committed else [(1,)] * 3
    )
    _resume(root)
    assert _rows(root, "SELECT COUNT(*) FROM current_entries") == [(0,)]
    for item in items:
        assert not (root / "current" / Path(*item.storage_key.segments)).exists()
    assert not tuple((root / ".h2hdb-state/quarantine").iterdir())


@pytest.mark.parametrize("count", (1, 128, 129))
def test_install_and_removal_journal_connections_are_per_page(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, count: int
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    items = _ordered(tuple(_item(gid, b"bounded") for gid in range(1, count + 1)))
    for item in items:
        _protect(adapter, item, b"bounded", item.gid)
    _spool(adapter, items)
    connections = 0
    original = adapter._connection

    @contextmanager
    def counted() -> Iterator[sqlite3.Connection]:
        nonlocal connections
        connections += 1
        with original() as connection:
            assert connection.execute("PRAGMA synchronous").fetchone() == (2,)
            yield connection

    monkeypatch.setattr(adapter, "_connection", counted)
    with adapter.publication_guard():
        adapter.begin(2, _RECEIPT)
        connections = 0
        checkpoint = adapter.reconcile_page(2, _RECEIPT, limit=128)
        assert (checkpoint.status, connections) == (
            LibraryActivationStatus.RECONCILE,
            3,
        )
        assert connections == 3  # state/fence, reserve, terminalize including cursor
        assert _rows(
            root,
            "SELECT COUNT(*) FROM pending_entries WHERE activation_revision=2 AND activated=1",
        ) == [(min(count, 128),)]
        if count == 129:
            assert _rows(
                root,
                "SELECT operation_started FROM pending_entries WHERE activation_revision=2 AND activated=0",
            ) == [(0,)]
        while checkpoint.status != LibraryActivationStatus.READY:
            checkpoint = adapter.reconcile_page(2, _RECEIPT, limit=128)
        adapter.complete(2, _RECEIPT)
        adapter.begin(3, b"c" * 16)
        adapter.seal(3)
        connections = 0
        checkpoint = adapter.reconcile_page(3, b"c" * 16, limit=128)
        assert checkpoint.status == LibraryActivationStatus.RECONCILE
        assert (
            connections == 5
        )  # state, empty install reserve, removal spool/reserve/commit
        assert _rows(root, "SELECT COUNT(*) FROM current_entries") == [
            (max(0, count - 128),)
        ]


@pytest.mark.parametrize("mutation", ("inode", "symlink"))
def test_install_batch_revalidates_earlier_outcome_before_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation: str
) -> None:
    root = tmp_path / "library"
    adapter, items, payloads = _mixed_page(root)
    original = adapter._perform_pending_install
    first_target = root / "current" / Path(*items[0].storage_key.segments)
    foreign = tmp_path / "foreign"
    foreign.write_bytes(payloads[items[0].gid])
    calls = 0

    def mutate(
        plan: library_module._PendingInstall,
    ) -> tuple[library_module._Signature, bytes | None]:
        nonlocal calls
        result = original(plan)
        calls += 1
        if calls == len(items):
            if mutation == "inode":
                os.replace(foreign, first_target)
            else:
                first_target.unlink()
                first_target.symlink_to(foreign)
        return result

    monkeypatch.setattr(adapter, "_perform_pending_install", mutate)
    with adapter.publication_guard(), pytest.raises(RuntimeError):
        adapter.begin(2, _RECEIPT)
        adapter.reconcile_page(2, _RECEIPT, limit=128)
    assert _rows(
        root, "SELECT activated FROM pending_entries WHERE activation_revision=2"
    ) == [(0,)] * len(items)
    assert first_target.read_bytes() == payloads[items[0].gid]
    assert (root / ".h2hdb-coordination/ACTIVATING").is_file()


def test_install_reservation_failure_rolls_back_entire_page(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "library"
    adapter, items, payloads = _mixed_page(root)
    with monkeypatch.context() as scoped:
        _fail_after(scoped, adapter, "_reserve_pending_install_in_transaction", 1)
        with (
            adapter.publication_guard(),
            pytest.raises(RuntimeError, match="batch interruption"),
        ):
            adapter.begin(2, _RECEIPT)
            adapter.reconcile_page(2, _RECEIPT, limit=128)
    assert _rows(
        root,
        "SELECT operation_started, activated FROM pending_entries WHERE activation_revision=2",
    ) == [(0, 0)] * len(items)
    _resume(root)
    _assert_installed(root, items, payloads)


def test_install_page_retains_each_rows_fresh_or_replayed_authorization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "library"
    adapter, items, payloads = _mixed_page(root)
    with monkeypatch.context() as scoped:
        _fail_after(scoped, adapter, "_reserve_pending_installs", 1)
        with (
            adapter.publication_guard(),
            pytest.raises(RuntimeError, match="batch interruption"),
        ):
            adapter.begin(2, _RECEIPT)
            adapter.reconcile_page(2, _RECEIPT, limit=1)
    flags: list[bool] = []
    original = ManagedFilesystemLibraryAdapter._perform_pending_install

    def record(
        self: ManagedFilesystemLibraryAdapter, plan: library_module._PendingInstall
    ) -> tuple[library_module._Signature, bytes | None]:
        flags.append(plan.fresh_authorization)
        return original(self, plan)

    monkeypatch.setattr(
        ManagedFilesystemLibraryAdapter, "_perform_pending_install", record
    )
    _resume(root)
    assert flags == [False, True, True, True]
    _assert_installed(root, items, payloads)


def test_batch_terminal_path_checks_release_state_lock_and_keep_reader_fence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import fcntl

    root = tmp_path / "library"
    adapter, items, _ = _mixed_page(root)
    original = adapter._require_current_authority
    checked = 0

    def verify(*args: object, **kwargs: object) -> library_module._Signature:
        nonlocal checked
        if kwargs.get("label") == "activation terminal library artifact":
            with (root / ".h2hdb-state/locks/state.lock").open("rb") as stream:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(stream, fcntl.LOCK_UN)
            with (root / ".h2hdb-coordination/publication.lock").open("rb") as stream:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(stream, fcntl.LOCK_SH | fcntl.LOCK_NB)
            assert (root / ".h2hdb-coordination/ACTIVATING").is_file()
            checked += 1
        return cast(Callable[..., library_module._Signature], original)(*args, **kwargs)

    monkeypatch.setattr(adapter, "_require_current_authority", verify)
    with adapter.publication_guard():
        adapter.begin(2, _RECEIPT)
        adapter.reconcile_page(2, _RECEIPT, limit=128)
    assert checked == len(items)


def test_failed_batch_keeps_all_staged_tokens_protected_from_release(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "library"
    adapter, items, payloads = _mixed_page(root)
    with monkeypatch.context() as scoped:
        _fail_after(scoped, adapter, "_perform_pending_install", 2)
        with (
            adapter.publication_guard(),
            pytest.raises(RuntimeError, match="batch interruption"),
        ):
            adapter.begin(2, _RECEIPT)
            adapter.reconcile_page(2, _RECEIPT, limit=128)
    releaser = _adapter(root)
    with closing(sqlite3.connect(_journal(root))) as connection:
        for item in items:
            if item.gid == 1:
                continue
            row = connection.execute(
                "SELECT token FROM protection_tokens WHERE storage_path=? AND state='STAGED'",
                ("/".join(item.storage_key.segments),),
            ).fetchone()
            assert row is not None
            with pytest.raises(RuntimeError, match="unfinished library activation"):
                releaser.release(
                    item.storage_key,
                    item.artifact_sha256,
                    item.size_bytes,
                    bytes(row[0]),
                )
    _resume(root)
    _assert_installed(root, items, payloads)


@pytest.mark.parametrize("location", ("current", "quarantine"))
def test_removal_batch_preserves_reappeared_earlier_path_without_advancing_cursor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, location: str
) -> None:
    root = tmp_path / "library"
    adapter = _adapter(root)
    items = _ordered(tuple(_item(gid, b"stale") for gid in range(1, 4)))
    for item in items:
        _protect(adapter, item, b"stale", item.gid)
    _activate(adapter, 1, b"a" * 16, items)
    _spool(adapter, ())
    first = items[0]
    foreign = (
        root / "current" / Path(*first.storage_key.segments)
        if location == "current"
        else root
        / ".h2hdb-state/quarantine"
        / library_module._quarantine_leaf(
            "/".join(first.storage_key.segments), first.artifact_sha256
        )
    )
    original = adapter._perform_pending_removal
    calls = 0

    def recreate(plan: library_module._PendingRemoval) -> None:
        nonlocal calls
        original(plan)
        calls += 1
        if calls == len(items):
            assert not foreign.exists()
            # Byte equality never grants the new inode the deleted row's authority.
            foreign.write_bytes(b"stale")

    with monkeypatch.context() as scoped:
        scoped.setattr(adapter, "_perform_pending_removal", recreate)
        with (
            adapter.publication_guard(),
            pytest.raises(RuntimeError, match="reappeared before commit"),
        ):
            adapter.begin(2, _RECEIPT)
            adapter.reconcile_page(2, _RECEIPT, limit=128)
    assert _rows(root, "SELECT COUNT(*) FROM current_entries") == [(3,)]
    assert _rows(root, "SELECT operation_started FROM pending_removals") == [(1,)] * 3
    assert _rows(root, "SELECT last_cursor FROM library_state") == [(None,)]
    assert foreign.read_bytes() == b"stale"
    with pytest.raises(RuntimeError):
        _resume(root)
    assert foreign.read_bytes() == b"stale"
    assert _rows(root, "SELECT last_cursor FROM library_state") == [(None,)]
    assert (root / ".h2hdb-coordination/ACTIVATING").is_file()
