import json
import os
import sqlite3
import stat
import subprocess
import sys
import tempfile
from collections.abc import Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, datetime
from errno import EIO
from functools import partial
from hashlib import sha256
from pathlib import Path
from threading import Event, Lock, Thread, get_ident
from time import sleep, time
from types import TracebackType
from typing import Any
from zipfile import ZipFile

import pytest
from PIL import Image

from h2hdb_ingest import (
    CBZGrouping,
    CBZReconciler,
    CBZSourceChangedError,
    DeduplicationPolicy,
    FilesystemScanner,
)
from h2hdb_ingest import cbz as cbz_module
from h2hdb_ingest.models import (
    CBZArtifact,
    CBZGalleryDescriptor,
    CBZPreparationFile,
    CBZPreparationMetadata,
    CBZPreparationRequest,
    CBZStreamingPreparationRequest,
    DeduplicationPlan,
    FileStatSignature,
    ScannedFile,
)


def _write_gallery(root: Path, *, gid: int = 41) -> Path:
    folder = root / f"Friendly Gallery [{gid}]"
    folder.mkdir(parents=True)
    (folder / "galleryinfo.txt").write_text(
        "\n".join(
            (
                f"Title: Gallery {gid}",
                "Upload Time: 2024-01-02 03:04",
                "Uploaded By: uploader",
                "Downloaded: 2024-01-03 04:05",
                "Tags: artist:Example, language:english",
                "Uploader's Comments:",
                "Summary",
                "Downloaded from E-Hentai Galleries by the Hentai@Home "
                "Downloader <3",
            )
        ),
        encoding="utf-8",
    )
    Image.new("RGB", (32, 24), (255, 0, 0)).save(folder / "001.png")
    return folder


def _plan(galleries: Path, *, incumbent: bool = False) -> DeduplicationPlan:
    return DeduplicationPolicy().select(
        FilesystemScanner(galleries, hash_workers=1).scan(),
        incumbent_gallery_name_by_gid=(
            {41: "Friendly Gallery [41]"} if incumbent else None
        ),
    )


def _reconciler(tmp_path: Path) -> CBZReconciler:
    return CBZReconciler(
        artifact_store_path=tmp_path / "artifacts",
        cbz_path=tmp_path / "komga",
        max_image_short_side=16,
    )


def _sqlite_state(artifact_store: Path) -> dict[str, Any]:
    with sqlite3.connect(
        artifact_store / cbz_module.STATE_DATABASE_FILE_NAME
    ) as connection:
        owned_rows = tuple(
            connection.execute("SELECT name, published FROM artifacts ORDER BY name")
        )
        protections: dict[str, list[str]] = {}
        for protection_id, artifact_name in connection.execute("""
            SELECT protection_id, artifact_name
            FROM protections
            ORDER BY protection_id, artifact_name
            """):
            protections.setdefault(protection_id, []).append(artifact_name)
        current = {
            name: {
                "artifact": artifact_name,
                "signature": {
                    "device": device,
                    "inode": inode,
                    "sizeBytes": size_bytes,
                    "modifiedNs": modified_ns,
                    "changedNs": changed_ns,
                },
            }
            for (
                name,
                artifact_name,
                device,
                inode,
                size_bytes,
                modified_ns,
                changed_ns,
            ) in connection.execute(
                """
                SELECT
                    path_name,
                    artifact_name,
                    device,
                    inode,
                    size_bytes,
                    modified_ns,
                    changed_ns
                FROM current_projection
                ORDER BY path_name
                """
            )
        }
        pending = dict(connection.execute("""
                SELECT path_name, artifact_name
                FROM pending_projection
                ORDER BY path_name
                """))
        current_revision, pending_revision = connection.execute("""
            SELECT current_revision, pending_revision
            FROM projection_revision
            WHERE singleton = 1
            """).fetchone()
    protected = sorted(
        artifact_name for names in protections.values() for artifact_name in names
    )
    return {
        "current": current,
        "currentRevision": current_revision,
        "owned": [name for name, _published in owned_rows],
        "pending": pending,
        "pendingRevision": pending_revision,
        "protected": protected,
        "protections": protections,
        "published": [name for name, published in owned_rows if published],
    }


def _file_signature(path: Path) -> FileStatSignature:
    observed = path.stat()
    return FileStatSignature(
        device=observed.st_dev,
        inode=observed.st_ino,
        size_bytes=observed.st_size,
        modified_ns=observed.st_mtime_ns,
        changed_ns=observed.st_ctime_ns,
    )


def _streaming_metadata(gid: int) -> CBZPreparationMetadata:
    return CBZPreparationMetadata(
        gallery=CBZGalleryDescriptor(
            gallery_name=f"Streaming Gallery [{gid}]",
            gid=gid,
            upload_time=datetime(2024, 1, 2, tzinfo=UTC),
        ),
        source_digest=sha256(f"source-{gid}".encode()).hexdigest(),
        content_digest=sha256(f"content-{gid}".encode()).hexdigest(),
    )


def _publish(
    reconciler: CBZReconciler,
    artifacts: tuple[CBZArtifact, ...],
) -> None:
    reconciler.protect_for_publish(artifacts)
    reconciler.finalize_published(artifacts)


def test_noncurrent_state_version_is_rejected(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    (artifacts / cbz_module.STATE_FILE_NAME).write_text(
        json.dumps({"version": 0}),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="Unsupported CBZ state version"):
        _reconciler(tmp_path).prepare(DeduplicationPlan((), (), ()))


def test_current_projection_requires_artifact_identity_and_signature(
    tmp_path: Path,
) -> None:
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    (artifacts / cbz_module.STATE_FILE_NAME).write_text(
        json.dumps(
            {
                "version": cbz_module.STATE_VERSION,
                "current": {
                    "Friendly Gallery [41].cbz": {
                        "artifact": None,
                        "signature": None,
                    }
                },
                "currentRevision": None,
                "owned": [],
                "pending": {},
                "pendingRevision": None,
                "protected": [],
                "published": [],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="Invalid CBZ state file"):
        _reconciler(tmp_path).prepare(DeduplicationPlan((), (), ()))


def test_v1_protection_state_migrates_to_legacy_sentinel_and_roundtrips(
    tmp_path: Path,
) -> None:
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    staged = artifacts / f"41-{'a' * 64}.cbz"
    staged.write_bytes(b"legacy protected artifact")
    state_path = artifacts / cbz_module.STATE_FILE_NAME
    state_path.write_text(
        json.dumps(
            {
                "version": 1,
                "current": {},
                "currentRevision": None,
                "owned": [staged.name],
                "pending": {},
                "pendingRevision": None,
                "protected": [staged.name],
                "published": [],
            }
        ),
        encoding="utf-8",
    )

    reconciler = _reconciler(tmp_path)
    reconciler.release_publish_protection("unrelated-build")

    migrated = _sqlite_state(artifacts)
    assert migrated["protections"] == {cbz_module.LEGACY_PROTECTION_ID: [staged.name]}
    assert migrated["protected"] == [staged.name]
    assert staged.is_file()
    assert json.loads(state_path.read_text(encoding="utf-8"))["version"] == 1

    _reconciler(tmp_path).release_publish_protection(cbz_module.LEGACY_PROTECTION_ID)
    assert not staged.exists()


def test_v2_scoped_protections_migrate_to_sqlite_without_rewriting_backup(
    tmp_path: Path,
) -> None:
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    first = artifacts / f"41-{'a' * 64}.cbz"
    second = artifacts / f"42-{'b' * 64}.cbz"
    first.write_bytes(b"first")
    second.write_bytes(b"second")
    legacy_document = {
        "version": cbz_module.STATE_VERSION,
        "current": {},
        "currentRevision": 3,
        "owned": [first.name, second.name],
        "pending": {},
        "pendingRevision": None,
        "protected": [first.name, second.name],
        "protections": {
            "build-a": [first.name],
            "build-b": [first.name, second.name],
        },
        "published": [],
    }
    state_path = artifacts / cbz_module.STATE_FILE_NAME
    state_path.write_text(json.dumps(legacy_document), encoding="utf-8")

    _reconciler(tmp_path).release_publish_protection("unrelated")

    migrated = _sqlite_state(artifacts)
    assert migrated["currentRevision"] == 3
    assert migrated["protections"] == legacy_document["protections"]
    assert json.loads(state_path.read_text(encoding="utf-8")) == legacy_document
    assert (
        stat.S_IMODE((artifacts / cbz_module.STATE_DATABASE_FILE_NAME).stat().st_mode)
        == 0o600
    )
    assert (
        stat.S_IMODE(
            (artifacts / cbz_module.STATE_DATABASE_MARKER_FILE_NAME).stat().st_mode
        )
        == 0o600
    )


def test_sqlite_migration_is_retryable_after_marker_write_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    staged = artifacts / f"41-{'a' * 64}.cbz"
    staged.write_bytes(b"protected")
    (artifacts / cbz_module.STATE_FILE_NAME).write_text(
        json.dumps(
            {
                "version": 1,
                "current": {},
                "currentRevision": None,
                "owned": [staged.name],
                "pending": {},
                "pendingRevision": None,
                "protected": [staged.name],
                "published": [],
            }
        ),
        encoding="utf-8",
    )
    interrupted = _reconciler(tmp_path)

    def fail_marker() -> None:
        raise OSError(EIO, "injected marker failure")

    monkeypatch.setattr(
        interrupted,
        "_write_state_database_marker_if_missing",
        fail_marker,
    )
    with pytest.raises(OSError, match="marker failure"):
        interrupted.release_publish_protection("unrelated")

    assert (artifacts / cbz_module.STATE_DATABASE_FILE_NAME).is_file()
    assert not (artifacts / cbz_module.STATE_DATABASE_MARKER_FILE_NAME).exists()
    recovered = _reconciler(tmp_path)
    recovered.release_publish_protection("unrelated")
    assert _sqlite_state(artifacts)["protected"] == [staged.name]


def test_missing_sqlite_after_migration_fails_closed(tmp_path: Path) -> None:
    reconciler = _reconciler(tmp_path)
    reconciler.prepare(DeduplicationPlan((), (), ()))
    artifacts = tmp_path / "artifacts"
    (artifacts / cbz_module.STATE_DATABASE_FILE_NAME).unlink()

    with pytest.raises(RuntimeError, match="restore.*from backup"):
        _reconciler(tmp_path).prepare(DeduplicationPlan((), (), ()))


def test_corrupt_sqlite_state_fails_closed(tmp_path: Path) -> None:
    reconciler = _reconciler(tmp_path)
    reconciler.prepare(DeduplicationPlan((), (), ()))
    artifacts = tmp_path / "artifacts"
    (artifacts / cbz_module.STATE_DATABASE_FILE_NAME).write_bytes(b"not sqlite")

    with pytest.raises(RuntimeError, match="Unable to read CBZ SQLite state"):
        _reconciler(tmp_path).prepare(DeduplicationPlan((), (), ()))


def test_build_scoped_shared_protection_survives_release_of_one_build(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(galleries)
    reconciler = _reconciler(tmp_path)
    (artifact,) = reconciler.prepare(_plan(galleries))

    reconciler.protect_for_publish((artifact,), protection_id="build-a")
    reconciler.protect_for_publish((artifact,), protection_id="build-b")
    reconciler.release_publish_protection("build-a")

    after_first_release = _sqlite_state(tmp_path / "artifacts")
    assert "build-a" not in after_first_release["protections"]
    assert after_first_release["protections"]["build-b"] == [artifact.path.name]
    assert artifact.path.is_file()

    reconciler.release_publish_protection("build-b")

    after_both_released = _sqlite_state(tmp_path / "artifacts")
    assert after_both_released["protections"] == {}
    assert not artifact.path.exists()


def test_finalize_releases_only_the_named_build_protection(tmp_path: Path) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(galleries)
    reconciler = _reconciler(tmp_path)
    (artifact,) = reconciler.prepare(_plan(galleries))
    reconciler.protect_for_publish((artifact,), protection_id="build-a")
    reconciler.protect_for_publish((artifact,), protection_id="build-b")

    reconciler.finalize_published(
        (artifact,),
        revision=1,
        protection_id="build-a",
    )

    finalized = _sqlite_state(tmp_path / "artifacts")
    assert "build-a" not in finalized["protections"]
    assert finalized["protections"]["build-b"] == [artifact.path.name]
    assert artifact.path.name in finalized["published"]
    reconciler.release_publish_protection("build-b")
    assert artifact.path.is_file()


def test_large_delta_state_never_materializes_or_serializes_the_full_set(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reconciler = _reconciler(tmp_path)
    reconciler.prepare(DeduplicationPlan((), (), ()))
    artifact_store = tmp_path / "artifacts"
    artifact_count = 2_048
    artifacts: list[CBZArtifact] = []

    def reject_full_state(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("delta state path must not materialize full state")

    monkeypatch.setattr(reconciler, "_replace_state_rows", reject_full_state)
    monkeypatch.setattr(json, "dumps", reject_full_state)

    with reconciler.publication_guard():
        for gid in range(1, artifact_count + 1):
            digest = sha256(str(gid).encode()).hexdigest()
            path = artifact_store / f"{gid}-{digest}.cbz"
            path.touch()
            reconciler._record_owned_artifact(path.name, gid=gid)
            artifacts.append(
                CBZArtifact(
                    gallery=CBZGalleryDescriptor(
                        gallery_name=f"Gallery [{gid}]",
                        gid=gid,
                        upload_time=datetime(2024, 1, 2, tzinfo=UTC),
                    ),
                    path=path,
                    size_bytes=0,
                    sha256=digest,
                    modified_at=datetime(2024, 1, 2, tzinfo=UTC),
                    created=True,
                    rebuilt=False,
                )
            )
        for offset in range(0, artifact_count, 128):
            reconciler.protect_for_publish(
                artifacts[offset : offset + 128],
                protection_id="large-build",
            )
        reconciler.release_publish_protection("unrelated-build")

    with sqlite3.connect(
        artifact_store / cbz_module.STATE_DATABASE_FILE_NAME
    ) as connection:
        assert connection.execute("SELECT COUNT(*) FROM artifacts").fetchone() == (
            artifact_count,
        )
        assert connection.execute("""
            SELECT COUNT(*) FROM protections
            WHERE protection_id = 'large-build'
            """).fetchone() == (artifact_count,)


def test_large_projection_finalize_uses_sqlite_reconciliation_pages(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reconciler = _reconciler(tmp_path)
    reconciler.prepare(DeduplicationPlan((), (), ()))
    artifact_store = tmp_path / "artifacts"
    artifact_count = 513
    rows: list[tuple[str, int]] = []
    for gid in range(1, artifact_count + 1):
        digest = sha256(f"projection-{gid}".encode()).hexdigest()
        name = f"{gid}-{digest}.cbz"
        (artifact_store / name).touch()
        rows.append((name, gid))
    with sqlite3.connect(
        artifact_store / cbz_module.STATE_DATABASE_FILE_NAME
    ) as connection:
        connection.executemany(
            "INSERT INTO artifacts(name, gid, published) VALUES (?, ?, 0)",
            rows,
        )

    def reject_full_collection(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("projection finalize must use its SQLite delta path")

    for legacy_helper in (
        "_read_projection_journal",
        "_plan_current_view",
        "_replace_pending_projection",
        "_materialize_current_view",
        "_remove_stale_current_paths",
        "_commit_projection_state",
    ):
        monkeypatch.setattr(reconciler, legacy_helper, reject_full_collection)
    monkeypatch.setattr(cbz_module, "PROJECTION_RECONCILIATION_PAGE_SIZE", 17)
    iterations = 0

    def artifacts() -> Iterator[CBZArtifact]:
        nonlocal iterations
        iterations += 1
        if iterations > 1:
            raise AssertionError("published artifacts must be consumed once")
        for gid in range(1, artifact_count + 1):
            digest = sha256(f"projection-{gid}".encode()).hexdigest()
            yield CBZArtifact(
                gallery=CBZGalleryDescriptor(
                    gallery_name=f"Scale Gallery [{gid}]",
                    gid=gid,
                    upload_time=datetime(2024, 1, 2, tzinfo=UTC),
                ),
                path=artifact_store / f"{gid}-{digest}.cbz",
                size_bytes=0,
                sha256=digest,
                modified_at=datetime(2024, 1, 2, tzinfo=UTC),
                created=False,
                rebuilt=False,
            )

    reconciler.finalize_published(artifacts(), revision=11)

    assert iterations == 1
    with sqlite3.connect(
        artifact_store / cbz_module.STATE_DATABASE_FILE_NAME
    ) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM current_projection"
        ).fetchone() == (artifact_count,)
        assert connection.execute(
            "SELECT COUNT(*) FROM pending_projection"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT COUNT(*) FROM artifacts WHERE published = 1"
        ).fetchone() == (artifact_count,)
    assert (tmp_path / "komga" / "Scale Gallery [1].cbz").is_file()
    assert (tmp_path / "komga" / f"Scale Gallery [{artifact_count}].cbz").is_file()


def test_rebuild_keeps_history_but_komga_has_one_friendly_current_file(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    folder = _write_gallery(galleries)
    reconciler = _reconciler(tmp_path)
    first = reconciler.prepare(_plan(galleries))
    _publish(reconciler, first)

    Image.new("RGB", (32, 24), (0, 255, 0)).save(folder / "001.png")
    second = reconciler.prepare(_plan(galleries, incumbent=True))
    _publish(reconciler, second)

    current_files = list((tmp_path / "komga").rglob("*.cbz"))
    assert current_files == [tmp_path / "komga" / "Friendly Gallery [41].cbz"]
    assert current_files[0].read_bytes() == second[0].path.read_bytes()
    assert not current_files[0].samefile(second[0].path)
    assert first[0].path.is_file()
    assert second[0].path.is_file()
    assert first[0].path != second[0].path


def test_removed_winner_removes_only_its_managed_current_file(tmp_path: Path) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(galleries)
    reconciler = _reconciler(tmp_path)
    artifacts = reconciler.prepare(_plan(galleries))
    _publish(reconciler, artifacts)
    current = tmp_path / "komga" / "Friendly Gallery [41].cbz"
    assert current.is_file()

    reconciler.finalize_published(())

    assert not current.exists()
    assert artifacts[0].path.is_file()


def test_unknown_cbz_is_never_replaced_or_removed(tmp_path: Path) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(galleries)
    komga = tmp_path / "komga"
    komga.mkdir()
    unknown = komga / "Friendly Gallery [41].cbz"
    unknown.write_bytes(b"operator-owned file")
    reconciler = _reconciler(tmp_path)
    artifacts = reconciler.prepare(_plan(galleries))

    _publish(reconciler, artifacts)

    assert unknown.read_bytes() == b"operator-owned file"
    managed = komga / "Friendly Gallery [41] [41].cbz"
    assert managed.is_file()
    reconciler.finalize_published(())
    assert unknown.read_bytes() == b"operator-owned file"
    assert not managed.exists()


def test_uncommitted_artifact_does_not_change_komga_current_view(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    folder = _write_gallery(galleries)
    reconciler = _reconciler(tmp_path)
    first = reconciler.prepare(_plan(galleries))
    _publish(reconciler, first)
    current = tmp_path / "komga" / "Friendly Gallery [41].cbz"
    original = current.read_bytes()

    Image.new("RGB", (32, 24), (0, 0, 255)).save(folder / "001.png")
    staged = reconciler.prepare(_plan(galleries, incumbent=True))
    reconciler.protect_for_publish(staged)

    assert staged[0].path != first[0].path
    assert staged[0].path.is_file()
    assert current.read_bytes() == original


def test_current_projection_is_an_independent_atomic_copy(tmp_path: Path) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(galleries)
    reconciler = _reconciler(tmp_path)
    artifacts = reconciler.prepare(_plan(galleries))
    reconciler.protect_for_publish(artifacts)

    reconciler.finalize_published(artifacts)

    current = tmp_path / "komga" / "Friendly Gallery [41].cbz"
    assert current.read_bytes() == artifacts[0].path.read_bytes()
    assert current.stat().st_ino != artifacts[0].path.stat().st_ino
    artifact_bytes = artifacts[0].path.read_bytes()

    current.write_bytes(b"Komga-side mutation")

    assert artifacts[0].path.read_bytes() == artifact_bytes


def test_reusable_artifact_is_verified_with_one_digest_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(galleries)
    reconciler = _reconciler(tmp_path)
    original_digest = cbz_module._sha256_file
    digest_reads: list[Path] = []

    def recording_digest(path: Path) -> str:
        digest_reads.append(path)
        return original_digest(path)

    monkeypatch.setattr(cbz_module, "_sha256_file", recording_digest)
    first = reconciler.prepare(_plan(galleries))
    assert len(digest_reads) == 1
    digest_reads.clear()

    second = reconciler.prepare(_plan(galleries))

    assert second[0].path == first[0].path
    assert digest_reads == [first[0].path]


def test_parallel_prepare_is_bounded_and_returns_plan_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    galleries = tmp_path / "galleries"
    for gid in range(41, 49):
        folder = _write_gallery(galleries, gid=gid)
        Image.new("RGB", (32, 24), (gid, 0, 0)).save(folder / "001.png")
    plan = _plan(galleries)
    events: list[str] = []
    reconciler = CBZReconciler(
        artifact_store_path=tmp_path / "artifacts",
        cbz_path=tmp_path / "komga",
        max_image_short_side=16,
        workers=2,
        event_logger=events.append,
        progress_interval_seconds=0.005,
    )
    original_ensure = reconciler._ensure_cbz
    original_record_owned = reconciler._record_owned_artifact
    release = Event()
    counter_lock = Lock()
    active = 0
    peak_active = 0
    completed_gids: list[int] = []
    state_writer_threads: set[int] = set()

    def delayed_ensure(*args: Any, **kwargs: Any) -> CBZArtifact:
        nonlocal active, peak_active
        gallery = args[0]
        with counter_lock:
            active += 1
            peak_active = max(peak_active, active)
        try:
            assert release.wait(5)
            sleep((50 - gallery.gid) * 0.002)
            artifact = original_ensure(*args, **kwargs)
            completed_gids.append(gallery.gid)
            return artifact
        finally:
            with counter_lock:
                active -= 1

    def recording_owned(name: str, *, gid: int) -> None:
        state_writer_threads.add(get_ident())
        original_record_owned(name, gid=gid)

    real_executor = ThreadPoolExecutor
    outstanding = 0
    peak_outstanding = 0
    submitted = 0

    class RecordingExecutor:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self._delegate = real_executor(*args, **kwargs)

        def __enter__(self) -> RecordingExecutor:
            self._delegate.__enter__()
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc_value: BaseException | None,
            traceback: TracebackType | None,
        ) -> bool | None:
            return self._delegate.__exit__(exc_type, exc_value, traceback)

        def submit(
            self,
            function: Any,
            /,
            *args: Any,
            **kwargs: Any,
        ) -> Future[Any]:
            nonlocal outstanding, peak_outstanding, submitted
            future = self._delegate.submit(function, *args, **kwargs)
            with counter_lock:
                outstanding += 1
                submitted += 1
                peak_outstanding = max(peak_outstanding, outstanding)
                if submitted == 4:
                    release.set()

            def finished(_future: Future[Any]) -> None:
                nonlocal outstanding
                with counter_lock:
                    outstanding -= 1

            future.add_done_callback(finished)
            return future

    monkeypatch.setattr(cbz_module, "ThreadPoolExecutor", RecordingExecutor)
    monkeypatch.setattr(reconciler, "_ensure_cbz", delayed_ensure)
    monkeypatch.setattr(reconciler, "_record_owned_artifact", recording_owned)
    caller_thread = get_ident()

    artifacts = reconciler.prepare(plan)

    assert [artifact.gallery.gid for artifact in artifacts] == [
        gallery.gid for gallery in plan.winners
    ]
    assert completed_gids != [gallery.gid for gallery in plan.winners]
    assert peak_active == 2
    assert peak_outstanding <= 4
    assert state_writer_threads == {caller_thread}
    assert (
        len([event for event in events if event.startswith("CBZ book prepared")]) == 8
    )
    assert any(event.startswith("CBZ preparation in progress") for event in events)


def test_streaming_prepare_is_bounded_and_sinks_lightweight_artifacts(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    for gid in range(41, 53):
        _write_gallery(galleries, gid=gid)
    plan = _plan(galleries)
    reconciler = CBZReconciler(
        artifact_store_path=tmp_path / "artifacts",
        cbz_path=tmp_path / "komga",
        max_image_short_side=16,
        workers=2,
    )
    artifacts: list[CBZArtifact] = []
    requests_produced = 0
    peak_unsunk_requests = 0

    def requests() -> Iterator[CBZPreparationRequest]:
        nonlocal requests_produced, peak_unsunk_requests
        for gallery in plan.winners:
            requests_produced += 1
            peak_unsunk_requests = max(
                peak_unsunk_requests,
                requests_produced - len(artifacts),
            )
            yield CBZPreparationRequest(gallery=gallery)

    def record(artifact: CBZArtifact) -> None:
        state = _sqlite_state(tmp_path / "artifacts")
        assert artifact.path.name in state["owned"]
        artifacts.append(artifact)

    summary = reconciler.prepare_stream(requests(), result_sink=record)

    assert requests_produced == len(plan.winners)
    assert peak_unsunk_requests <= 4
    assert summary.prepared == len(plan.winners)
    assert summary.created == len(plan.winners)
    assert summary.rebuilt == 0
    assert summary.reused == 0
    assert {artifact.gallery.gid for artifact in artifacts} == {
        gallery.gid for gallery in plan.winners
    }
    assert all(not hasattr(artifact.gallery, "files") for artifact in artifacts)


def test_paged_streaming_giant_gallery_bounds_pages_and_in_flight_requests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.txt"
    source.write_bytes(b"one small source member")
    signature = _file_signature(source)
    source_file = ScannedFile(
        path=source,
        name=source.name,
        size_bytes=signature.size_bytes,
        sha256=sha256(source.read_bytes()).hexdigest(),
        signature=signature,
    )
    page_limit = 43
    giant_file_count = 3_007
    peak_page_materialized = 0
    requests_produced = 0
    sunk: list[CBZArtifact] = []
    peak_unsunk_requests = 0

    def open_files(file_count: int) -> Iterator[CBZPreparationFile]:
        nonlocal peak_page_materialized
        for start in range(0, file_count, page_limit):
            page = tuple(
                CBZPreparationFile(
                    file_key=f"file-{number:06d}",
                    file=source_file,
                )
                for number in range(start, min(start + page_limit, file_count))
            )
            peak_page_materialized = max(peak_page_materialized, len(page))
            yield from page

    def requests() -> Iterator[CBZStreamingPreparationRequest]:
        nonlocal peak_unsunk_requests, requests_produced
        for gid in range(41, 46):
            requests_produced += 1
            peak_unsunk_requests = max(
                peak_unsunk_requests,
                requests_produced - len(sunk),
            )
            file_count = giant_file_count if gid == 41 else 1
            yield CBZStreamingPreparationRequest(
                metadata=_streaming_metadata(gid),
                open_files=partial(open_files, file_count),
            )

    reconciler = CBZReconciler(
        artifact_store_path=tmp_path / "artifacts",
        cbz_path=tmp_path / "komga",
        max_image_short_side=16,
        workers=2,
    )

    def reject_unbounded_zipfile(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError("paged CBZ construction must not use ZipFile.filelist")

    monkeypatch.setattr(cbz_module, "ZipFile", reject_unbounded_zipfile)

    def record(artifact: CBZArtifact) -> None:
        sunk.append(artifact)

    summary = reconciler.prepare_paged_stream(requests(), result_sink=record, total=5)

    assert summary.prepared == 5
    assert requests_produced == 5
    assert peak_page_materialized <= page_limit
    assert peak_unsunk_requests <= 2 * 2
    assert len(sunk) == 5
    with ZipFile(next(item.path for item in sunk if item.gallery.gid == 41)) as archive:
        assert len(archive.infolist()) == giant_file_count
        assert archive.testzip() is None


def test_paged_streaming_rejects_source_mutation_after_staging(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.txt"
    source.write_bytes(b"staged bytes")
    signature = _file_signature(source)
    staged_file = ScannedFile(
        path=source,
        name=source.name,
        size_bytes=signature.size_bytes,
        sha256=sha256(source.read_bytes()).hexdigest(),
        signature=signature,
    )
    request = CBZStreamingPreparationRequest(
        metadata=_streaming_metadata(41),
        open_files=lambda: iter((CBZPreparationFile("file-1", staged_file),)),
    )
    source.write_bytes(b"mutated bytes with a different size")

    with pytest.raises(CBZSourceChangedError, match="changed before CBZ selection"):
        _reconciler(tmp_path).prepare_paged_stream((request,), total=1)


def test_paged_streaming_rejects_source_disappearance_after_staging(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.txt"
    source.write_bytes(b"staged bytes")
    signature = _file_signature(source)
    staged_file = ScannedFile(
        path=source,
        name=source.name,
        size_bytes=signature.size_bytes,
        sha256=sha256(source.read_bytes()).hexdigest(),
        signature=signature,
    )
    request = CBZStreamingPreparationRequest(
        metadata=_streaming_metadata(41),
        open_files=lambda: iter((CBZPreparationFile("file-1", staged_file),)),
    )
    source.unlink()

    with pytest.raises(
        CBZSourceChangedError,
        match="Unable to verify staged source file before CBZ selection",
    ):
        _reconciler(tmp_path).prepare_paged_stream((request,), total=1)


def test_paged_streaming_classifies_disappearance_between_stat_and_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.txt"
    source.write_bytes(b"staged bytes")
    signature = _file_signature(source)
    staged_file = ScannedFile(
        path=source,
        name=source.name,
        size_bytes=signature.size_bytes,
        sha256=sha256(source.read_bytes()).hexdigest(),
        signature=signature,
    )
    request = CBZStreamingPreparationRequest(
        metadata=_streaming_metadata(41),
        open_files=lambda: iter((CBZPreparationFile("file-1", staged_file),)),
    )
    reconciler = _reconciler(tmp_path)
    original_verify = reconciler._verify_staged_file_signature
    removed = False

    def remove_after_pre_read_stat(
        file: ScannedFile,
        *,
        stage: str,
        required: bool = False,
    ) -> None:
        nonlocal removed
        original_verify(file, stage=stage, required=required)
        if stage == "before CBZ read":
            source.unlink()
            removed = True

    monkeypatch.setattr(
        reconciler,
        "_verify_staged_file_signature",
        remove_after_pre_read_stat,
    )

    with pytest.raises(
        CBZSourceChangedError,
        match="Unable to verify staged source file during CBZ read",
    ):
        reconciler.prepare_paged_stream((request,), total=1)

    assert removed


def test_paged_streaming_rejects_source_mutation_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.png"
    Image.new("RGB", (32, 24), (255, 0, 0)).save(source)
    signature = _file_signature(source)
    staged_file = ScannedFile(
        path=source,
        name=source.name,
        size_bytes=signature.size_bytes,
        sha256=sha256(source.read_bytes()).hexdigest(),
        signature=signature,
    )
    request = CBZStreamingPreparationRequest(
        metadata=_streaming_metadata(41),
        open_files=lambda: iter((CBZPreparationFile("file-1", staged_file),)),
    )
    reconciler = _reconciler(tmp_path)
    original_write = reconciler._write_normalized_image
    mutated = False

    def mutate_after_read(
        file: ScannedFile,
        output: tempfile.SpooledTemporaryFile[bytes],
    ) -> None:
        nonlocal mutated
        original_write(file, output)
        source.write_bytes(b"changed while preparing the staged CBZ source")
        mutated = True

    monkeypatch.setattr(reconciler, "_write_normalized_image", mutate_after_read)

    with pytest.raises(CBZSourceChangedError, match="changed after CBZ read"):
        reconciler.prepare_paged_stream((request,), total=1)

    assert mutated


def test_stream_sink_failure_keeps_artifact_owned_for_iterable_protection(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(galleries)
    (gallery,) = _plan(galleries).winners
    reconciler = _reconciler(tmp_path)
    delivered: list[CBZArtifact] = []

    def fail_after_delivery(artifact: CBZArtifact) -> None:
        delivered.append(artifact)
        raise RuntimeError("injected artifact sink failure")

    with pytest.raises(RuntimeError, match="injected artifact sink failure"):
        reconciler.prepare_stream(
            iter((CBZPreparationRequest(gallery=gallery),)),
            result_sink=fail_after_delivery,
            total=1,
        )

    (artifact,) = delivered
    failed_state = _sqlite_state(tmp_path / "artifacts")
    assert artifact.path.name in failed_state["owned"]
    assert artifact.path.name not in failed_state["protected"]

    reconciler.protect_for_publish(iter(delivered))

    protected_state = _sqlite_state(tmp_path / "artifacts")
    assert artifact.path.name in protected_state["protected"]

    reconciler.finalize_published(iter(delivered), revision=1)

    published_state = _sqlite_state(tmp_path / "artifacts")
    assert artifact.path.name in published_state["published"]
    assert artifact.path.name not in published_state["protected"]
    assert (tmp_path / "komga" / "Friendly Gallery [41].cbz").is_file()


def test_parallel_prepare_drains_workers_and_records_successes_before_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    galleries = tmp_path / "galleries"
    for gid in range(41, 45):
        folder = _write_gallery(galleries, gid=gid)
        Image.new("RGB", (32, 24), (gid, 0, 0)).save(folder / "001.png")
    plan = _plan(galleries)
    reconciler = CBZReconciler(
        artifact_store_path=tmp_path / "artifacts",
        cbz_path=tmp_path / "komga",
        max_image_short_side=16,
        workers=2,
    )
    original_ensure = reconciler._ensure_cbz
    another_worker_started = Event()
    failed_gid = plan.winners[0].gid
    completed_gids: list[int] = []

    def flaky_ensure(*args: Any, **kwargs: Any) -> CBZArtifact:
        gallery = args[0]
        if gallery.gid == failed_gid:
            assert another_worker_started.wait(5)
            raise RuntimeError("injected parallel CBZ failure")
        another_worker_started.set()
        sleep(0.02)
        artifact = original_ensure(*args, **kwargs)
        completed_gids.append(gallery.gid)
        return artifact

    monkeypatch.setattr(reconciler, "_ensure_cbz", flaky_ensure)

    with pytest.raises(RuntimeError, match="injected parallel CBZ failure"):
        reconciler.prepare(plan)

    assert sorted(completed_gids) == sorted(
        gallery.gid for gallery in plan.winners if gallery.gid != failed_gid
    )
    state = _sqlite_state(tmp_path / "artifacts")
    assert len(state["owned"]) == 3

    retry = CBZReconciler(
        artifact_store_path=tmp_path / "artifacts",
        cbz_path=tmp_path / "komga",
        max_image_short_side=16,
        workers=2,
    ).prepare(plan)
    assert sum(artifact.created for artifact in retry) == 1
    assert sum(not artifact.created and not artifact.rebuilt for artifact in retry) == 3


def test_content_addressed_filename_does_not_hide_corrupt_artifact(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(galleries)
    reconciler = _reconciler(tmp_path)
    (first,) = reconciler.prepare(_plan(galleries))
    first.path.write_bytes(b"corrupt artifact with a trusted-looking filename")

    (rebuilt,) = reconciler.prepare(_plan(galleries))

    assert rebuilt.path != first.path
    assert rebuilt.path.name.startswith(f"41-{rebuilt.sha256}")
    assert rebuilt.path.read_bytes() != first.path.read_bytes()


def test_manifest_with_incompatible_resize_policy_is_rebuilt(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    folder = _write_gallery(galleries)
    Image.new("RGB", (100, 400), (10, 20, 30)).save(folder / "001.png")
    plan = _plan(galleries)
    gallery = plan.winners[0]
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    old_path = artifacts / "old.cbz"
    with ZipFile(old_path, "w") as archive:
        archive.writestr("001.jpg", b"incompatible-low-resolution-image")
        archive.comment = json.dumps(
            {
                "version": cbz_module.CBZ_MANIFEST_VERSION,
                "sourceDigest": gallery.source_digest,
                "contentDigest": gallery.content_digest,
                "excludedFileSha256s": [],
                "resizePolicy": "long-edge-no-upscale",
                "maxImageShortSide": 50,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    old_digest = sha256(old_path.read_bytes()).hexdigest()
    old_path = old_path.rename(artifacts / f"{gallery.gid}-{old_digest}.cbz")
    (artifacts / ".h2hdb-cbz-state.json").write_text(
        json.dumps(
            {
                "version": cbz_module.STATE_VERSION,
                "current": {},
                "currentRevision": None,
                "owned": [old_path.name],
                "pending": {},
                "pendingRevision": None,
                "protected": [],
                "published": [old_path.name],
            }
        ),
        encoding="utf-8",
    )
    reconciler = CBZReconciler(
        artifact_store_path=artifacts,
        cbz_path=tmp_path / "komga",
        max_image_short_side=50,
        workers=1,
    )

    (rebuilt,) = reconciler.prepare(plan)

    assert rebuilt.rebuilt and not rebuilt.created
    assert rebuilt.path != old_path
    with ZipFile(rebuilt.path) as archive:
        manifest = json.loads(archive.comment.decode("utf-8"))
    assert manifest["version"] == cbz_module.CBZ_MANIFEST_VERSION
    assert manifest["resizePolicy"] == "webtoon-short-side-no-upscale-v1"


def test_unchanged_projection_is_not_read_or_copied_after_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(galleries)
    reconciler = _reconciler(tmp_path)
    artifacts = reconciler.prepare(_plan(galleries))
    reconciler.protect_for_publish(artifacts)
    reconciler.finalize_published(artifacts, revision=5)
    state = _sqlite_state(tmp_path / "artifacts")
    projection = state["current"]["Friendly Gallery [41].cbz"]
    assert projection["artifact"] == artifacts[0].path.name
    assert set(projection["signature"]) == {
        "changedNs",
        "device",
        "inode",
        "modifiedNs",
        "sizeBytes",
    }

    restarted = _reconciler(tmp_path)
    reused = restarted.prepare(_plan(galleries, incumbent=True))
    restarted.protect_for_publish(reused)
    artifact_path = reused[0].path
    original_open = Path.open

    def reject_artifact_read(
        self: Path,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> object:
        if self == artifact_path and "r" in mode:
            raise AssertionError("unchanged projection must not read its artifact")
        return original_open(self, mode, buffering, encoding, errors, newline)

    def unexpected_copy(
        source: Path,
        target: Path,
        *,
        replace_managed: bool,
    ) -> None:
        del source, target, replace_managed
        raise AssertionError("unchanged projection must not be read or copied")

    monkeypatch.setattr(Path, "open", reject_artifact_read)
    monkeypatch.setattr(restarted, "_atomic_copy", unexpected_copy)
    restarted.finalize_published(reused, revision=5)


def test_same_size_external_projection_mutation_is_recopied(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(galleries)
    reconciler = _reconciler(tmp_path)
    artifacts = reconciler.prepare(_plan(galleries))
    reconciler.protect_for_publish(artifacts)
    reconciler.finalize_published(artifacts, revision=5)
    current = tmp_path / "komga" / "Friendly Gallery [41].cbz"
    original = current.read_bytes()
    mutated = bytearray(original)
    mutated[len(mutated) // 2] ^= 0xFF
    current.write_bytes(mutated)
    assert current.stat().st_size == len(original)

    restarted = _reconciler(tmp_path)
    reused = restarted.prepare(_plan(galleries, incumbent=True))
    restarted.protect_for_publish(reused)
    original_copy = restarted._atomic_copy
    copies = 0

    def recording_copy(
        source: Path,
        target: Path,
        *,
        replace_managed: bool,
    ) -> None:
        nonlocal copies
        copies += 1
        original_copy(source, target, replace_managed=replace_managed)

    monkeypatch.setattr(restarted, "_atomic_copy", recording_copy)
    restarted.finalize_published(reused, revision=5)

    assert copies == 1
    assert current.read_bytes() == artifacts[0].path.read_bytes()


def test_failed_materialization_does_not_claim_or_overwrite_an_unmanaged_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(galleries)
    reconciler = _reconciler(tmp_path)
    artifacts = reconciler.prepare(_plan(galleries))
    reconciler.protect_for_publish(artifacts)

    def failed_copy(source: object, target: object, *, length: int) -> None:
        del source, target, length
        raise OSError(EIO, "injected materialization failure")

    with monkeypatch.context() as failed:
        failed.setattr("h2hdb_ingest.cbz.shutil.copyfileobj", failed_copy)
        with pytest.raises(OSError, match="materialization failure"):
            reconciler.finalize_published(artifacts)

    unmanaged = tmp_path / "komga" / "Friendly Gallery [41].cbz"
    unmanaged.write_bytes(b"user file created after failed materialization")
    reconciler.finalize_published(artifacts)

    assert unmanaged.read_bytes() == b"user file created after failed materialization"
    assert (tmp_path / "komga" / "Friendly Gallery [41] [41].cbz").is_file()


def test_partial_projection_is_journaled_and_retry_does_not_leave_duplicates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(galleries, gid=41)
    second_folder = _write_gallery(galleries, gid=42)
    Image.new("RGB", (32, 24), (0, 255, 0)).save(second_folder / "001.png")
    reconciler = _reconciler(tmp_path)
    artifacts = reconciler.prepare(_plan(galleries))
    reconciler.protect_for_publish(artifacts)
    original_copy = reconciler._atomic_copy
    copies = 0

    def fail_second_copy(
        source: Path,
        target: Path,
        *,
        replace_managed: bool,
    ) -> None:
        nonlocal copies
        copies += 1
        if copies == 2:
            raise OSError(EIO, "injected second projection failure")
        original_copy(source, target, replace_managed=replace_managed)

    monkeypatch.setattr(reconciler, "_atomic_copy", fail_second_copy)
    with pytest.raises(OSError, match="second projection failure"):
        reconciler.finalize_published(artifacts, revision=7)
    interrupted_state = _sqlite_state(tmp_path / "artifacts")
    assert interrupted_state["pendingRevision"] == 7
    assert sorted(interrupted_state["pending"]) == [
        "Friendly Gallery [41].cbz",
        "Friendly Gallery [42].cbz",
    ]

    recovered = _reconciler(tmp_path)
    recovered.finalize_published(artifacts, revision=7)

    assert sorted(path.name for path in (tmp_path / "komga").glob("*.cbz")) == [
        "Friendly Gallery [41].cbz",
        "Friendly Gallery [42].cbz",
    ]
    recovered_state = _sqlite_state(tmp_path / "artifacts")
    assert recovered_state["currentRevision"] == 7
    assert recovered_state["pending"] == {}
    assert set(recovered_state["current"]) == {
        "Friendly Gallery [41].cbz",
        "Friendly Gallery [42].cbz",
    }
    assert all(
        projection["artifact"] and projection["signature"]
        for projection in recovered_state["current"].values()
    )


def test_interrupted_streaming_plan_preserves_the_previous_durable_projection(
    tmp_path: Path,
) -> None:
    galleries = tmp_path / "galleries"
    first_folder = _write_gallery(galleries, gid=41)
    reconciler = _reconciler(tmp_path)
    first = reconciler.prepare(_plan(galleries))
    reconciler.protect_for_publish(first)
    reconciler.finalize_published(first, revision=1)
    current = tmp_path / "komga" / "Friendly Gallery [41].cbz"
    original = current.read_bytes()

    second_folder = _write_gallery(galleries, gid=42)
    Image.new("RGB", (32, 24), (0, 255, 0)).save(first_folder / "001.png")
    Image.new("RGB", (32, 24), (0, 0, 255)).save(second_folder / "001.png")
    second = reconciler.prepare(_plan(galleries, incumbent=True))
    reconciler.protect_for_publish(second, protection_id="build-two")

    def interrupted_artifacts() -> Iterator[CBZArtifact]:
        yield second[0]
        raise OSError(EIO, "injected projection planning interruption")

    with pytest.raises(OSError, match="planning interruption"):
        reconciler.finalize_published(
            interrupted_artifacts(),
            revision=2,
            protection_id="build-two",
        )

    interrupted_state = _sqlite_state(tmp_path / "artifacts")
    assert interrupted_state["currentRevision"] == 1
    assert interrupted_state["pending"] == {}
    assert current.read_bytes() == original

    reconciler.finalize_published(
        iter(second),
        revision=2,
        protection_id="build-two",
    )
    recovered_state = _sqlite_state(tmp_path / "artifacts")
    assert recovered_state["currentRevision"] == 2
    assert recovered_state["pending"] == {}
    assert set(recovered_state["current"]) == {
        "Friendly Gallery [41].cbz",
        "Friendly Gallery [42].cbz",
    }


def test_older_revision_cannot_overwrite_newer_projection(tmp_path: Path) -> None:
    galleries = tmp_path / "galleries"
    folder = _write_gallery(galleries)
    reconciler = _reconciler(tmp_path)
    first = reconciler.prepare(_plan(galleries))
    reconciler.protect_for_publish(first)
    reconciler.finalize_published(first, revision=1)
    Image.new("RGB", (32, 24), (0, 255, 0)).save(folder / "001.png")
    second = reconciler.prepare(_plan(galleries, incumbent=True))
    reconciler.protect_for_publish(second)
    reconciler.finalize_published(second, revision=2)
    current = tmp_path / "komga" / "Friendly Gallery [41].cbz"
    newest_bytes = current.read_bytes()

    with pytest.raises(RuntimeError, match="newer Komga projection"):
        reconciler.finalize_published(first, revision=1)

    assert current.read_bytes() == newest_bytes


def test_publication_guard_closes_revision_check_projection_interleaving(
    tmp_path: Path,
) -> None:
    first = _reconciler(tmp_path)
    second = _reconciler(tmp_path)
    catalog_revisions = [1]
    projected_revisions: list[int] = []
    first_checked = Event()
    second_attempted_publish = Event()
    second_published = Event()

    def publish_second_revision() -> None:
        assert first_checked.wait(1)
        second_attempted_publish.set()
        with second.publication_guard():
            catalog_revisions.append(2)
            projected_revisions.append(2)
            second_published.set()

    publisher = Thread(target=publish_second_revision)
    publisher.start()
    with first.publication_guard():
        checked_revision = catalog_revisions[-1]
        first_checked.set()
        assert second_attempted_publish.wait(1)
        assert not second_published.is_set()
        assert catalog_revisions[-1] == checked_revision
        projected_revisions.append(checked_revision)
    publisher.join(timeout=1)

    assert not publisher.is_alive()
    assert catalog_revisions == [1, 2]
    assert projected_revisions == [1, 2]


def test_publication_guard_is_released_after_failure(tmp_path: Path) -> None:
    first = _reconciler(tmp_path)
    second = _reconciler(tmp_path)

    with pytest.raises(RuntimeError, match="injected guarded failure"):
        with first.publication_guard():
            raise RuntimeError("injected guarded failure")

    with second.publication_guard():
        pass


def test_publication_guard_is_released_when_lock_owner_process_crashes(
    tmp_path: Path,
) -> None:
    artifact_store = tmp_path / "artifacts"
    artifact_store.mkdir()
    lock_path = artifact_store / ".h2hdb-cbz-state.lock"
    process = subprocess.Popen(
        (
            sys.executable,
            "-c",
            (
                "import fcntl, sys, time; "
                "lock_file = open(sys.argv[1], 'a+b'); "
                "fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX); "
                "print('locked', flush=True); "
                "time.sleep(3600)"
            ),
            str(lock_path),
        ),
        stdout=subprocess.PIPE,
        text=True,
    )
    assert process.stdout is not None
    assert process.stdout.readline().strip() == "locked"
    acquired = Event()
    attempted = Event()

    def acquire_after_crash() -> None:
        attempted.set()
        with _reconciler(tmp_path).publication_guard():
            acquired.set()

    waiter = Thread(target=acquire_after_crash)
    waiter.start()
    try:
        assert attempted.wait(1)
        assert not acquired.is_set()
        process.kill()
        process.wait(timeout=5)
        waiter.join(timeout=5)
        assert acquired.is_set()
        assert not waiter.is_alive()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)


def test_sigkill_style_stale_temp_cleanup_is_exact_aged_and_flock_guarded(
    tmp_path: Path,
) -> None:
    artifacts = tmp_path / "artifacts"
    artifact_group = artifacts / "2024" / "01"
    komga = tmp_path / "komga"
    projection_group = komga / "2024" / "01"
    artifact_group.mkdir(parents=True)
    projection_group.mkdir(parents=True)
    old_artifact = artifact_group / (f"{cbz_module.ARTIFACT_TEMP_PREFIX}{'a' * 32}.tmp")
    old_projection = projection_group / (
        f"{cbz_module.PROJECTION_TEMP_PREFIX}{'b' * 32}.tmp"
    )
    old_state = artifacts / f"{cbz_module.STATE_TEMP_PREFIX}{'c' * 32}.tmp"
    recent = artifact_group / f"{cbz_module.ARTIFACT_TEMP_PREFIX}{'d' * 32}.tmp"
    lookalike = artifact_group / (f"{cbz_module.ARTIFACT_TEMP_PREFIX}{'e' * 31}.tmp")
    operator_temp = artifact_group / ".41-operator-owned.tmp"
    unowned_final = artifact_group / f"41-{'f' * 64}.cbz"
    for path in (
        old_artifact,
        old_projection,
        old_state,
        recent,
        lookalike,
        operator_temp,
        unowned_final,
    ):
        path.write_bytes(path.name.encode("ascii"))
    stale_time = time() - 120
    for path in (old_artifact, old_projection, old_state, lookalike, operator_temp):
        os.utime(path, (stale_time, stale_time))

    outside = tmp_path / "outside-owned-name"
    outside.write_bytes(b"operator data")
    symlink_temp = artifact_group / (f"{cbz_module.ARTIFACT_TEMP_PREFIX}{'0' * 32}.tmp")
    try:
        symlink_temp.symlink_to(outside)
    except OSError as error:
        pytest.skip(f"symlinks are unavailable: {error}")
    events: list[str] = []
    holder = CBZReconciler(
        artifact_store_path=artifacts,
        cbz_path=komga,
        max_image_short_side=16,
        workers=1,
        stale_temp_age_seconds=60,
    )
    cleaner = CBZReconciler(
        artifact_store_path=artifacts,
        cbz_path=komga,
        max_image_short_side=16,
        workers=1,
        stale_temp_age_seconds=60,
        event_logger=events.append,
    )
    cleanup_started = Event()
    cleanup_finished = Event()

    def clean_after_crash_owner_releases_flock() -> None:
        cleanup_started.set()
        cleaner.prepare(DeduplicationPlan((), (), ()))
        cleanup_finished.set()

    with holder.publication_guard():
        cleanup_thread = Thread(target=clean_after_crash_owner_releases_flock)
        cleanup_thread.start()
        assert cleanup_started.wait(1)
        sleep(0.05)
        assert not cleanup_finished.is_set()
        assert old_artifact.exists()
    cleanup_thread.join(timeout=5)

    assert cleanup_finished.is_set()
    assert not old_artifact.exists()
    assert not old_projection.exists()
    assert not old_state.exists()
    assert recent.is_file()
    assert lookalike.is_file()
    assert operator_temp.is_file()
    assert unowned_final.is_file()
    assert symlink_temp.is_symlink()
    assert outside.read_bytes() == b"operator data"
    assert any("artifact_builds=1 projections=1 states=1" in event for event in events)


@pytest.mark.parametrize("escaped_root_name", ["artifacts", "komga"])
def test_grouping_cannot_follow_a_root_symlink_outside_its_root(
    tmp_path: Path,
    escaped_root_name: str,
) -> None:
    galleries = tmp_path / "galleries"
    _write_gallery(galleries)
    artifacts = tmp_path / "artifacts"
    komga = tmp_path / "komga"
    artifacts.mkdir()
    komga.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    try:
        (
            (artifacts if escaped_root_name == "artifacts" else komga) / "2024"
        ).symlink_to(
            outside,
            target_is_directory=True,
        )
    except OSError as error:
        pytest.skip(f"symlinks are unavailable: {error}")
    reconciler = CBZReconciler(
        artifact_store_path=artifacts,
        cbz_path=komga,
        max_image_short_side=16,
        grouping=CBZGrouping.date_yyyy,
    )

    if escaped_root_name == "artifacts":
        with pytest.raises(RuntimeError, match="outside artifact_store_path"):
            reconciler.prepare(_plan(galleries))
    else:
        prepared = reconciler.prepare(_plan(galleries))
        reconciler.protect_for_publish(prepared)
        with pytest.raises(RuntimeError, match="outside cbz_path"):
            reconciler.finalize_published(prepared)

    assert list(outside.iterdir()) == []
