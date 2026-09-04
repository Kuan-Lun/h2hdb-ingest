from __future__ import annotations

import multiprocessing
import os
import signal
import sqlite3
from pathlib import Path
from typing import NoReturn, cast

import pytest
from h2hdb import (
    CoreConfig,
    StorageInstanceBindingMismatchError,
    VNextDatabaseAdminFacade,
)
from PIL import Image

from h2hdb_ingest import (
    IngestConfig,
    IngestPathsConfig,
    LibraryStorageIdentity,
    ResidentConfig,
)
from h2hdb_ingest.runtime import build_runtime

pytestmark = [pytest.mark.deep, pytest.mark.mariadb]

_CHILD_DEADLINE_SECONDS = 30.0


def _provision_library_root(root: Path) -> None:
    root.mkdir(mode=0o777)
    root.chmod(0o777)
    current = root / "current"
    for path in (
        current,
        current / "acquisitions",
        current / "artwork",
        root / ".h2hdb-coordination",
    ):
        path.mkdir(mode=0o777)
        path.chmod(0o777)


def _crash_after_local_identity_before_core_bind(config: IngestConfig) -> NoReturn:
    runtime = build_runtime(config)
    real_admin = runtime.database_admin

    class _CrashBeforeBindAdmin:
        def check(self) -> object:
            return real_admin.check()

        def bind_storage_instance(self, storage_instance_uuid: bytes) -> NoReturn:
            del storage_instance_uuid
            os.kill(os.getpid(), signal.SIGKILL)
            os._exit(91)

    runtime.resident._database_admin = cast(
        VNextDatabaseAdminFacade,
        _CrashBeforeBindAdmin(),
    )
    runtime.resident.initialize()
    os._exit(92)


@pytest.mark.skipif(not hasattr(signal, "SIGKILL"), reason="POSIX SIGKILL only")
def test_live_mariadb_restart_replays_uuid_after_process_loss(
    tmp_path: Path,
    mariadb_config: CoreConfig,
) -> None:
    source = tmp_path / "download"
    gallery = source / "7001"
    gallery.mkdir(parents=True)
    (gallery / "galleryinfo.txt").write_text(
        "\n".join(
            (
                "Title: Storage binding crash",
                "Upload Time: 2024-01-02 03:04",
                "Uploaded By: uploader",
                "Downloaded: 2024-02-03 04:05",
                "Tags: artist:test, language:english",
                "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3",
            )
        ),
        encoding="utf-8",
    )
    Image.new("RGB", (8, 12), "red").save(gallery / "001.jpg")
    library_root = tmp_path / "library"
    _provision_library_root(library_root)
    config = IngestConfig(
        core=mariadb_config,
        paths=IngestPathsConfig(
            download_path=source,
            library_path=library_root,
            page_render_workers=1,
        ),
        resident=ResidentConfig(lease_seconds=30, heartbeat_seconds=5),
    )
    VNextDatabaseAdminFacade(mariadb_config).initialize()

    process = multiprocessing.get_context("spawn").Process(
        target=_crash_after_local_identity_before_core_bind,
        args=(config,),
    )
    process.start()
    process.join(_CHILD_DEADLINE_SECONDS)
    if process.is_alive():
        process.kill()
        process.join()
        pytest.fail("crash-injection child exceeded its 30 second deadline")
    assert process.exitcode == -signal.SIGKILL

    journal = library_root / ".h2hdb-state" / "journal" / "library-activation.sqlite3"
    with sqlite3.connect(journal) as connection:
        row = connection.execute(
            "SELECT storage_instance_uuid FROM library_storage_identity "
            "WHERE singleton = 1"
        ).fetchone()
    assert row is not None
    identity = LibraryStorageIdentity(row[0])

    with build_runtime(config) as restarted:
        restarted.resident.initialize()
        assert (
            restarted.database_admin.bind_storage_instance(
                identity.storage_instance_uuid
            ).storage_instance_uuid
            == identity.storage_instance_uuid
        )
        assert restarted.resident.process_available(periodic_scan=True)
        publication = restarted.catalog.discover_publications().publications[0]
        current = library_root.joinpath(
            "current",
            *publication.artifacts[0].storage_object.key.segments,
        )
        assert current.is_file()

    wrong_root = tmp_path / "wrong-library"
    _provision_library_root(wrong_root)
    wrong_config = config.model_copy(
        update={"paths": config.paths.model_copy(update={"library_path": wrong_root})}
    )
    with build_runtime(wrong_config) as wrong_runtime:
        with pytest.raises(
            StorageInstanceBindingMismatchError,
            match="different storage instance",
        ):
            wrong_runtime.resident.initialize()
        with pytest.raises(RuntimeError, match="must initialize"):
            wrong_runtime.resident.process_available(periodic_scan=True)
    assert not tuple((wrong_root / "current").rglob("*.cbz"))
