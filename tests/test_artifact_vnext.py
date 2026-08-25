from __future__ import annotations

import ctypes
import errno
import os
import sqlite3
import sys
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest
from h2hdb import (
    ArtifactReleaseAdapter,
    ArtifactStorageAdapter,
    ArtifactTransformKind,
    VNextArtifactProducer,
)
from PIL import Image

import h2hdb_ingest.artifact as artifact_module
from h2hdb_ingest.artifact import (
    ArtifactProducerIdentity,
    ManagedFilesystemArtifactAdapter,
)


def _locator(payload: bytes) -> tuple[str, str, str]:
    digest = sha256(payload).hexdigest()
    return ("sha256", digest[:2], f"{digest}.cbz")


def test_producer_identity_matches_public_core_domain() -> None:
    identity = ArtifactProducerIdentity.current()
    producer = VNextArtifactProducer(
        identity.writer_id,
        identity.python_abi,
        identity.pillow_build,
        identity.libjpeg_build,
        identity.zlib_build,
    )

    assert producer.fingerprint_sha256 == identity.fingerprint_sha256


def test_protect_and_release_are_monotone_and_idempotent(tmp_path: Path) -> None:
    adapter = ManagedFilesystemArtifactAdapter(
        tmp_path / "artifacts",
        max_image_short_side=768,
    )
    assert isinstance(adapter, ArtifactStorageAdapter)
    assert isinstance(adapter, ArtifactReleaseAdapter)
    payload = b"an immutable archive"
    locator = _locator(payload)
    token = b"t" * 184

    first = adapter.protect(BytesIO(payload), locator, token)
    replay = adapter.protect(BytesIO(payload), locator, token)
    target = tmp_path / "artifacts" / Path(*locator)

    assert first.stored
    assert replay.stored
    assert target.read_bytes() == payload
    with pytest.raises(RuntimeError, match="does not match its locator"):
        adapter.protect(BytesIO(b"different"), locator, token)
    assert adapter.release(locator, token).released
    assert adapter.release(locator, token).released
    assert not adapter.protect(BytesIO(payload), locator, token).stored
    assert target.read_bytes() == payload


def test_protect_rejects_bytes_that_disagree_with_locator(tmp_path: Path) -> None:
    adapter = ManagedFilesystemArtifactAdapter(
        tmp_path / "artifacts",
        max_image_short_side=768,
    )

    with pytest.raises(RuntimeError, match="does not match its locator"):
        adapter.protect(BytesIO(b"different"), _locator(b"expected"), b"x" * 184)


def test_cleanup_rejects_nested_parent_symlink_and_retries_in_store(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifacts"
    adapter = ManagedFilesystemArtifactAdapter(
        artifact_root,
        max_image_short_side=768,
    )
    payload = b"released immutable archive"
    digest = sha256(payload).digest()
    locator = _locator(payload)
    token = b"s" * 184
    assert adapter.protect(BytesIO(payload), locator, token).stored
    assert adapter.release(locator, token).released

    prefix_directory = artifact_root / locator[0] / locator[1]
    outside_directory = tmp_path / "outside-artifact-prefix"
    prefix_directory.rename(outside_directory)
    prefix_directory.symlink_to(outside_directory, target_is_directory=True)
    outside_target = outside_directory / locator[2]

    with pytest.raises(RuntimeError, match="artifact locator parent is unsafe"):
        adapter._prune_cleanup_candidates(
            is_retained=lambda _digest: False,
            limit=8,
        )

    assert outside_target.read_bytes() == payload
    with sqlite3.connect(artifact_root / ".h2hdb-vnext-artifacts.sqlite3") as state:
        assert state.execute("SELECT COUNT(*) FROM artifacts").fetchone() == (1,)
        assert state.execute(
            "SELECT artifact_sha256 FROM artifact_cleanup_candidates"
        ).fetchall() == [(digest,)]

    prefix_directory.unlink()
    outside_directory.rename(prefix_directory)
    adapter._prune_cleanup_candidates(
        is_retained=lambda _digest: False,
        limit=8,
    )

    assert not (prefix_directory / locator[2]).exists()
    with sqlite3.connect(artifact_root / ".h2hdb-vnext-artifacts.sqlite3") as state:
        assert state.execute("SELECT COUNT(*) FROM artifacts").fetchone() == (0,)
        assert state.execute(
            "SELECT COUNT(*) FROM artifact_cleanup_candidates"
        ).fetchone() == (0,)
        assert state.execute(
            "SELECT token, state FROM protection_tokens"
        ).fetchall() == [(token, "RELEASED")]


def test_cleanup_rejects_store_root_symlink_and_retries_at_original_root(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifacts"
    adapter = ManagedFilesystemArtifactAdapter(
        artifact_root,
        max_image_short_side=768,
    )
    payload = b"root swap archive"
    locator = _locator(payload)
    token = b"r" * 184
    adapter.protect(BytesIO(payload), locator, token)
    adapter.release(locator, token)

    outside_root = tmp_path / "outside-artifact-root"
    artifact_root.rename(outside_root)
    artifact_root.symlink_to(outside_root, target_is_directory=True)
    outside_target = outside_root.joinpath(*locator)

    with pytest.raises(RuntimeError, match="artifact store is not a directory"):
        adapter._prune_cleanup_candidates(
            is_retained=lambda _digest: False,
            limit=8,
        )

    assert outside_target.read_bytes() == payload
    artifact_root.unlink()
    outside_root.rename(artifact_root)
    assert (
        adapter._prune_cleanup_candidates(
            is_retained=lambda _digest: False,
            limit=8,
        )
        == 1
    )
    assert not artifact_root.joinpath(*locator).exists()


def test_cleanup_quarantines_leaf_swapped_immediately_before_delete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_root = tmp_path / "artifacts"
    adapter = ManagedFilesystemArtifactAdapter(
        artifact_root,
        max_image_short_side=768,
    )
    payload = b"managed bytes before race"
    external_payload = b"external replacement during race"
    digest = sha256(payload).digest()
    locator = _locator(payload)
    token = b"q" * 184
    adapter.protect(BytesIO(payload), locator, token)
    adapter.release(locator, token)
    target = artifact_root.joinpath(*locator)
    saved_managed = tmp_path / "saved-managed.cbz"
    original_rename = os.rename
    original_capture = artifact_module._rename_noreplace
    injected = False

    def swap_leaf_then_capture(*args: Any, **kwargs: Any) -> None:
        nonlocal injected
        source = str(args[0])
        destination = str(args[1])
        if (
            not injected
            and source == locator[-1]
            and destination == artifact_module._QUARANTINE_PAYLOAD_NAME
        ):
            injected = True
            original_rename(target, saved_managed)
            target.write_bytes(external_payload)
        original_capture(*args, **kwargs)

    monkeypatch.setattr(
        artifact_module,
        "_rename_noreplace",
        swap_leaf_then_capture,
    )
    with pytest.raises(RuntimeError, match="quarantine failed verification"):
        adapter._prune_cleanup_candidates(
            is_retained=lambda _digest: False,
            limit=8,
        )

    quarantines = tuple(
        (artifact_root / artifact_module._ARTIFACT_QUARANTINE_NAME).glob(
            "artifact-*/payload"
        )
    )
    assert len(quarantines) == 1
    assert quarantines[0].read_bytes() == external_payload
    assert saved_managed.read_bytes() == payload
    with sqlite3.connect(artifact_root / ".h2hdb-vnext-artifacts.sqlite3") as state:
        assert state.execute("SELECT COUNT(*) FROM artifacts").fetchone() == (1,)
        assert state.execute(
            "SELECT artifact_sha256 FROM artifact_cleanup_candidates"
        ).fetchall() == [(digest,)]

    quarantines[0].unlink()
    original_rename(saved_managed, target)
    assert (
        adapter._prune_cleanup_candidates(
            is_retained=lambda _digest: False,
            limit=8,
        )
        == 1
    )
    assert not target.exists()


def test_cleanup_preserves_quarantine_when_parent_is_swapped_during_rename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_root = tmp_path / "artifacts"
    adapter = ManagedFilesystemArtifactAdapter(
        artifact_root,
        max_image_short_side=768,
    )
    payload = b"managed bytes in moving parent"
    locator = _locator(payload)
    token = b"m" * 184
    adapter.protect(BytesIO(payload), locator, token)
    adapter.release(locator, token)
    prefix_directory = artifact_root / locator[0] / locator[1]
    outside_directory = tmp_path / "moved-artifact-parent"
    original_rename = os.rename
    original_capture = artifact_module._rename_noreplace
    injected = False

    def swap_parent_then_capture(*args: Any, **kwargs: Any) -> None:
        nonlocal injected
        source = str(args[0])
        destination = str(args[1])
        if (
            not injected
            and source == locator[-1]
            and destination == artifact_module._QUARANTINE_PAYLOAD_NAME
        ):
            injected = True
            original_rename(prefix_directory, outside_directory)
            prefix_directory.symlink_to(outside_directory, target_is_directory=True)
        original_capture(*args, **kwargs)

    monkeypatch.setattr(
        artifact_module,
        "_rename_noreplace",
        swap_parent_then_capture,
    )
    with pytest.raises(RuntimeError, match="artifact locator parent changed"):
        adapter._prune_cleanup_candidates(
            is_retained=lambda _digest: False,
            limit=8,
        )

    quarantines = tuple(
        (artifact_root / artifact_module._ARTIFACT_QUARANTINE_NAME).glob(
            "artifact-*/payload"
        )
    )
    assert len(quarantines) == 1
    assert quarantines[0].read_bytes() == payload
    prefix_directory.unlink()
    original_rename(outside_directory, prefix_directory)

    assert (
        adapter._prune_cleanup_candidates(
            is_retained=lambda _digest: False,
            limit=8,
        )
        == 1
    )
    assert not quarantines[0].exists()


def test_cleanup_destination_race_never_overwrites_unknown_quarantine_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_root = tmp_path / "artifacts"
    adapter = ManagedFilesystemArtifactAdapter(
        artifact_root,
        max_image_short_side=768,
    )
    payload = b"managed destination-race bytes"
    external_payload = b"unknown quarantine destination"
    digest = sha256(payload).digest()
    locator = _locator(payload)
    token = b"d" * 184
    adapter.protect(BytesIO(payload), locator, token)
    adapter.release(locator, token)
    target = artifact_root.joinpath(*locator)
    original_capture = artifact_module._rename_noreplace
    injected = False

    def create_destination_then_capture(*args: Any, **kwargs: Any) -> None:
        nonlocal injected
        if not injected:
            injected = True
            descriptor = os.open(
                artifact_module._QUARANTINE_PAYLOAD_NAME,
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
        artifact_module,
        "_rename_noreplace",
        create_destination_then_capture,
    )
    with pytest.raises(RuntimeError, match="destination changed during capture"):
        adapter._prune_cleanup_candidates(
            is_retained=lambda _digest: False,
            limit=8,
        )

    quarantines = tuple(
        (artifact_root / artifact_module._ARTIFACT_QUARANTINE_NAME).glob(
            "artifact-*/payload"
        )
    )
    assert len(quarantines) == 1
    assert quarantines[0].read_bytes() == external_payload
    assert target.read_bytes() == payload
    with sqlite3.connect(artifact_root / ".h2hdb-vnext-artifacts.sqlite3") as state:
        assert state.execute("SELECT COUNT(*) FROM artifacts").fetchone() == (1,)
        assert state.execute(
            "SELECT artifact_sha256 FROM artifact_cleanup_candidates"
        ).fetchall() == [(digest,)]

    quarantines[0].unlink()
    assert (
        adapter._prune_cleanup_candidates(
            is_retained=lambda _digest: False,
            limit=8,
        )
        == 1
    )
    assert not target.exists()


def test_cleanup_rechecks_private_payload_before_unlink_and_does_not_ack_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_root = tmp_path / "artifacts"
    adapter = ManagedFilesystemArtifactAdapter(
        artifact_root,
        max_image_short_side=768,
    )
    payload = b"managed verify-unlink bytes"
    external_payload = b"unknown post-verify bytes"
    digest = sha256(payload).digest()
    locator = _locator(payload)
    token = b"v" * 184
    adapter.protect(BytesIO(payload), locator, token)
    adapter.release(locator, token)
    original_verify = artifact_module._verify_regular_at
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
        if not injected and name == artifact_module._QUARANTINE_PAYLOAD_NAME:
            injected = True
            os.unlink(name, dir_fd=parent_descriptor)
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
        artifact_module,
        "_verify_regular_at",
        verify_then_mutate,
    )
    with pytest.raises(RuntimeError, match="changed before unlink"):
        adapter._prune_cleanup_candidates(
            is_retained=lambda _digest: False,
            limit=8,
        )

    quarantines = tuple(
        (artifact_root / artifact_module._ARTIFACT_QUARANTINE_NAME).glob(
            "artifact-*/payload"
        )
    )
    assert len(quarantines) == 1
    assert quarantines[0].read_bytes() == external_payload
    with sqlite3.connect(artifact_root / ".h2hdb-vnext-artifacts.sqlite3") as state:
        assert state.execute("SELECT COUNT(*) FROM artifacts").fetchone() == (1,)
        assert state.execute(
            "SELECT artifact_sha256 FROM artifact_cleanup_candidates"
        ).fetchall() == [(digest,)]

    quarantines[0].write_bytes(payload)
    assert (
        adapter._prune_cleanup_candidates(
            is_retained=lambda _digest: False,
            limit=8,
        )
        == 1
    )
    assert not quarantines[0].exists()


def test_cleanup_rejects_unsafe_preexisting_private_namespace(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifacts"
    adapter = ManagedFilesystemArtifactAdapter(
        artifact_root,
        max_image_short_side=768,
    )
    payload = b"private namespace metadata"
    locator = _locator(payload)
    token = b"u" * 184
    adapter.protect(BytesIO(payload), locator, token)
    adapter.release(locator, token)
    namespace = artifact_root / artifact_module._ARTIFACT_QUARANTINE_NAME
    namespace.mkdir(mode=0o755)

    with pytest.raises(RuntimeError, match="private directory metadata is unsafe"):
        adapter._prune_cleanup_candidates(
            is_retained=lambda _digest: False,
            limit=8,
        )

    assert artifact_root.joinpath(*locator).read_bytes() == payload
    namespace.chmod(0o700)
    assert (
        adapter._prune_cleanup_candidates(
            is_retained=lambda _digest: False,
            limit=8,
        )
        == 1
    )


class _FakeRenameFunction:
    def __init__(self, result: int = 0) -> None:
        self.argtypes: list[object] = []
        self.restype: object | None = None
        self.calls: list[tuple[object, ...]] = []
        self.result = result

    def __call__(self, *args: object) -> int:
        self.calls.append(args)
        return self.result


class _FakeLinuxLibrary:
    def __init__(self, function: _FakeRenameFunction) -> None:
        self.renameat2 = function


def test_linux_no_replace_dispatch_uses_renameat2_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    function = _FakeRenameFunction()
    library = _FakeLinuxLibrary(function)
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(ctypes, "CDLL", lambda *_args, **_kwargs: library)

    artifact_module._rename_noreplace(
        "source",
        "payload",
        source_descriptor=11,
        destination_descriptor=12,
    )

    assert len(function.calls) == 1
    assert function.calls[0][0] == 11
    assert function.calls[0][2] == 12
    assert function.calls[0][4] == 0x00000001


def test_linux_no_replace_maps_eexist_to_file_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    function = _FakeRenameFunction(-1)
    library = _FakeLinuxLibrary(function)
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(ctypes, "CDLL", lambda *_args, **_kwargs: library)
    monkeypatch.setattr(
        ctypes,
        "get_errno",
        lambda: errno.EEXIST,
    )

    with pytest.raises(FileExistsError):
        artifact_module._rename_noreplace(
            "source",
            "payload",
            source_descriptor=11,
            destination_descriptor=12,
        )


def test_render_member_uses_registered_bounded_image_transforms(tmp_path: Path) -> None:
    adapter = ManagedFilesystemArtifactAdapter(
        tmp_path / "artifacts",
        max_image_short_side=20,
    )
    source = BytesIO()
    Image.new("RGBA", (80, 40), (255, 0, 0, 0)).save(source, format="PNG")
    source.seek(0)
    destination = BytesIO()

    adapter.render_member(
        source,
        ArtifactTransformKind.JPEG_NORMALIZE,
        destination,
    )
    destination.seek(0)
    with Image.open(destination) as rendered:
        assert rendered.format == "JPEG"
        assert rendered.mode == "RGB"
        assert min(rendered.size) == 20

    with pytest.raises(ValueError, match="core owns RAW_COPY"):
        adapter.render_member(
            BytesIO(b"raw"),
            ArtifactTransformKind.RAW_COPY,
            BytesIO(),
        )
