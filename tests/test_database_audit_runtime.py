"""Real SQLite scheduler integration through public core and resident entry points."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from threading import Event, Thread
from time import monotonic

import pytest
from h2hdb import (
    CoreConfig,
    DatabaseAuditReason,
    DatabaseAuditReport,
    DatabaseAuditSession,
    DatabaseConfig,
    VNextDatabaseAdminFacade,
)

from h2hdb_ingest import IngestConfig, IngestPathsConfig, ResidentConfig
from h2hdb_ingest.database_audit import IngestDatabaseAudit
from h2hdb_ingest.runtime import IngestRuntime, build_runtime


def _config(tmp_path: Path, *, short_interval: bool = False) -> IngestConfig:
    source = tmp_path / "source"
    gallery = source / "1001"
    gallery.mkdir(parents=True)
    (gallery / "001.jpg").write_bytes(b"deterministic metadata-only page")
    (gallery / "galleryinfo.txt").write_text(
        "\n".join(
            (
                "Title: Scheduled audit fixture",
                "Upload Time: 2024-01-02 03:04",
                "Uploaded By: fixture",
                "Downloaded: 2024-02-03 04:05",
                "Tags: artist:audit, language:english",
                "Uploader's Comments",
                "fixture",
                "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3",
            )
        ),
        encoding="utf-8",
    )
    return IngestConfig(
        core=CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite", database=str(tmp_path / "catalog.sqlite3")
            )
        ),
        paths=IngestPathsConfig(download_path=source),
        resident=ResidentConfig(
            lease_seconds=2,
            heartbeat_seconds=0.05,
            poll_seconds=0.01,
            database_audit_minimum_interval_seconds=1 if short_interval else 604800,
            database_audit_duration_multiplier=1 if short_interval else 100,
        ),
    )


def _initialize(config: IngestConfig) -> None:
    admin = VNextDatabaseAdminFacade(config.core)
    try:
        admin.initialize()
    finally:
        admin.close()


def _publish(runtime: IngestRuntime) -> None:
    for _ in range(64):
        runtime.resident.process_available(periodic_scan=True)
        if runtime.resident.last_synchronization_result is not None:
            return
    raise AssertionError("small SQLite fixture did not complete")


def _schedule(runtime: IngestRuntime) -> DatabaseAuditReport:
    session = runtime.resident.database_audit.session
    assert session is not None
    return runtime.database_admin.check_ingest_runtime_if_due(session)


def test_completed_catchup_clean_restart_reuses_successful_audit(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    _initialize(config)
    with build_runtime(config) as first:
        startup = first.resident.initialize()
        assert startup.reason is DatabaseAuditReason.FIRST_RUN
        assert startup.full_audit is not None and startup.initial_catchup_pending
        _publish(first)
        schedule = _schedule(first)
        assert not schedule.initial_catchup_pending
        assert schedule.last_full_audit_at == startup.last_full_audit_at
        assert schedule.next_full_audit_at >= startup.next_full_audit_at
        revision = first.catalog.get_catalog_revision()
    with build_runtime(config) as restarted:
        quick = restarted.resident.initialize()
        assert quick.reason is DatabaseAuditReason.RECENT_AUDIT
        assert quick.full_audit is None
        assert quick.last_full_audit_at == schedule.last_full_audit_at
        assert quick.next_full_audit_at == schedule.next_full_audit_at
        _publish(restarted)
        assert restarted.catalog.get_catalog_revision() == revision


def test_incomplete_folder_does_not_block_initial_catchup_hint(tmp_path: Path) -> None:
    config = _config(tmp_path)
    incomplete = config.paths.download_path / "1002"
    incomplete.mkdir()
    (incomplete / "galleryinfo.txt").write_text(
        "Title: Still downloading", encoding="utf-8"
    )
    _initialize(config)
    with build_runtime(config) as runtime:
        startup = runtime.resident.initialize()
        _publish(runtime)
        result = runtime.resident.last_synchronization_result
        assert result is not None
        assert result.deferred_gallery_count == 0 and result.waiting_gallery_count == 1
        schedule = _schedule(runtime)
        assert not schedule.initial_catchup_pending
        assert schedule.last_full_audit_at == startup.last_full_audit_at


def test_escaped_exception_requires_full_audit_after_lease_expiry(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    _initialize(config)
    with (
        pytest.raises(ValueError, match="application failed"),
        build_runtime(config) as first,
    ):
        first.resident.initialize()
        raise ValueError("application failed")
    with build_runtime(config) as restarted:
        report = restarted.resident.initialize()
        assert report.reason is DatabaseAuditReason.PREVIOUS_INTERRUPTION
        assert report.full_audit is not None


def test_sqlite_full_audit_serializes_same_process_renewal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path, short_interval=True)
    _initialize(config)
    admin = VNextDatabaseAdminFacade(config.core)
    audit = IngestDatabaseAudit(
        admin, config.resident, lambda _: None, database_type="sqlite"
    )
    entered, release, renewal_requested, renewed = Event(), Event(), Event(), Event()
    failures: list[BaseException] = []
    original_check = VNextDatabaseAdminFacade.check_ingest_runtime_if_due

    def observed_check(
        owner: VNextDatabaseAdminFacade,
        session: DatabaseAuditSession,
        *,
        on_check: Callable[[DatabaseAuditReason], None] | None = None,
    ) -> DatabaseAuditReport:
        def observe(reason: DatabaseAuditReason) -> None:
            if reason is DatabaseAuditReason.SCHEDULE_DUE:
                entered.set()
                if not release.wait(10):
                    failures.append(TimeoutError("full-audit release was not received"))
            if on_check is not None:
                on_check(reason)

        return original_check(owner, session, on_check=observe)

    monkeypatch.setattr(
        VNextDatabaseAdminFacade, "check_ingest_runtime_if_due", observed_check
    )
    audit.start()
    audit.initial_catchup_complete()

    def check_until_due() -> None:
        try:
            deadline = monotonic() + 10
            while not entered.is_set():
                if monotonic() >= deadline:
                    raise TimeoutError("one-second audit schedule did not become due")
                audit.check_between_sessions()
                release.wait(0.02)
        except BaseException as error:
            failures.append(error)

    def renew() -> None:
        renewal_requested.set()
        try:
            audit.renew()
            renewed.set()
        except BaseException as error:
            failures.append(error)

    checker = Thread(target=check_until_due)
    renewal = Thread(target=renew)
    checker.start()
    try:
        assert entered.wait(10)
        renewal.start()
        assert renewal_requested.wait(2)
        assert not renewed.wait(0.05), (
            "renewal entered while the full audit owns the controller"
        )
        release.set()
        checker.join(10)
        renewal.join(10)
        assert not checker.is_alive() and not renewal.is_alive()
        assert renewed.is_set() and not failures
        assert not audit.failed
    finally:
        release.set()
        checker.join(10)
        if renewal.ident is not None:
            renewal.join(10)
        audit.close()
        session = audit.session
        assert session is not None
        admin.finish_ingest_runtime(session)
        admin.close()
