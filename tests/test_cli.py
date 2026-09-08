from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from h2hdb import CatalogRevisionNotFoundError

import h2hdb_ingest.__main__ as cli
import h2hdb_ingest.bootstrap as bootstrap
from h2hdb_ingest import IngestConfig, IngestPathsConfig


class _Resident:
    def __init__(self, events: list[object], *, available: bool = True) -> None:
        self._events = events
        self._available = available
        self.last_synchronization_result: object | None = None
        self.deferred_gallery_count = 0

    def initialize(self) -> None:
        self._events.append("initialize")

    def process_available(
        self,
        *,
        periodic_scan: bool,
        preflight: Any = None,
        postflight: Any = None,
        should_stop: Any = None,
    ) -> bool:
        del should_stop
        self._events.append(("process", periodic_scan))
        if preflight is not None:
            preflight()
        if self._available:
            if postflight is not None:
                postflight()
            self.last_synchronization_result = object()
        return self._available

    def run_forever(self, *, stop: object) -> None:
        self._events.append(("run-forever", stop))


class _Runtime:
    def __init__(
        self,
        events: list[object],
        *,
        resident: _Resident,
        catalog: _Catalog | None = None,
    ) -> None:
        self._events = events
        self.resident = resident
        self.catalog = catalog
        self.closed = False

    def __enter__(self) -> _Runtime:
        if self.closed:
            raise AssertionError("test runtime was reopened")
        return self

    def __exit__(self, *_exc: object) -> None:
        if not self.closed:
            self.closed = True
            self._events.append("runtime-close")


def _cli_config(events: list[object]) -> SimpleNamespace:
    return SimpleNamespace(ensure_paths=lambda: events.append("ensure-paths"))


def test_once_checks_epoch_then_uses_one_periodic_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []
    config = _cli_config(events)
    resident = _Resident(events)

    monkeypatch.setattr(cli, "load_config", lambda path: config)
    monkeypatch.setattr(
        cli,
        "configure_logging",
        lambda value: events.append(("logging", value)),
    )
    monkeypatch.setattr(
        cli,
        "build_runtime",
        lambda value: _Runtime(
            events,
            resident=resident if value is config else _Resident(events),
        ),
    )

    cli.main(["--config", str(tmp_path / "ingest.json"), "--once"])

    assert events == [
        "ensure-paths",
        ("logging", config),
        "initialize",
        ("process", True),
        "runtime-close",
    ]


def test_resident_mode_runs_forever_after_epoch_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []
    config = _cli_config(events)
    resident = _Resident(events)
    monkeypatch.setattr(cli, "load_config", lambda path: config)
    monkeypatch.setattr(cli, "configure_logging", lambda value: None)
    monkeypatch.setattr(
        cli,
        "build_runtime",
        lambda value: _Runtime(events, resident=resident),
    )

    cli.main(["--config", str(tmp_path / "ingest.json")])

    assert events[:2] == ["ensure-paths", "initialize"]
    resident_event = events[2]
    assert isinstance(resident_event, tuple)
    assert resident_event[0] == "run-forever"
    assert events[3] == "runtime-close"


def test_once_reports_ordinary_claim_contention(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []
    config = _cli_config(events)
    resident = _Resident(events, available=False)
    monkeypatch.setattr(cli, "load_config", lambda path: config)
    monkeypatch.setattr(cli, "configure_logging", lambda value: None)
    monkeypatch.setattr(
        cli,
        "build_runtime",
        lambda value: _Runtime(events, resident=resident),
    )

    with pytest.raises(RuntimeError, match="No gallery ingest lease"):
        cli.main(["--config", str(tmp_path / "ingest.json"), "--once"])

    assert events[-1] == "runtime-close"


def test_resident_exception_closes_runtime_before_propagation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []
    config = _cli_config(events)

    class FailingResident(_Resident):
        def run_forever(self, *, stop: object) -> None:
            del stop
            self._events.append("run-failed")
            raise RuntimeError("resident failed")

    resident = FailingResident(events)
    runtime = _Runtime(events, resident=resident)
    monkeypatch.setattr(cli, "load_config", lambda path: config)
    monkeypatch.setattr(cli, "configure_logging", lambda value: None)
    monkeypatch.setattr(cli, "build_runtime", lambda value: runtime)

    with pytest.raises(RuntimeError, match="resident failed"):
        cli.main(["--config", str(tmp_path / "ingest.json")])

    assert runtime.closed
    assert events[-2:] == ["run-failed", "runtime-close"]


@pytest.mark.parametrize(
    "failure_type",
    (KeyboardInterrupt, SystemExit),
)
def test_resident_base_exception_closes_runtime_before_propagation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_type: type[BaseException],
) -> None:
    events: list[object] = []
    config = _cli_config(events)

    class InterruptedResident(_Resident):
        def run_forever(self, *, stop: object) -> None:
            del stop
            self._events.append("run-interrupted")
            raise failure_type()

    runtime = _Runtime(events, resident=InterruptedResident(events))
    monkeypatch.setattr(cli, "load_config", lambda path: config)
    monkeypatch.setattr(cli, "configure_logging", lambda value: None)
    monkeypatch.setattr(cli, "build_runtime", lambda value: runtime)

    with pytest.raises(failure_type):
        cli.main(["--config", str(tmp_path / "ingest.json")])

    assert runtime.closed
    assert events[-2:] == ["run-interrupted", "runtime-close"]


def test_cli_rejects_empty_download_mount_before_opening_facades(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    download_path = tmp_path / "download"
    download_path.mkdir()
    config = IngestConfig(paths=IngestPathsConfig(download_path=download_path))
    opened = False

    def open_runtime(value: object) -> object:
        nonlocal opened
        del value
        opened = True
        return object()

    monkeypatch.setattr(cli, "load_config", lambda path: config)
    monkeypatch.setattr(cli, "build_runtime", open_runtime)

    with pytest.raises(ValueError, match="gallery volume is mounted"):
        cli.main(["--config", str(tmp_path / "ingest.json"), "--once"])

    assert not opened


class _Catalog:
    def __init__(self, revisions: list[object]) -> None:
        self._revisions = revisions

    def get_catalog_revision(self) -> object:
        value = self._revisions.pop(0)
        if isinstance(value, BaseException):
            raise value
        return value


def _bootstrap_config(tmp_path: Path) -> IngestConfig:
    download = tmp_path / "download"
    gallery = download / "gallery"
    gallery.mkdir(parents=True)
    (gallery / "galleryinfo.txt").write_text("metadata", encoding="utf-8")
    return IngestConfig(paths=IngestPathsConfig(download_path=download))


def test_bootstrap_requires_no_current_revision_then_publishes_nonempty_catalog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    events: list[object] = []
    config = _bootstrap_config(tmp_path)
    catalog = _Catalog(
        [
            CatalogRevisionNotFoundError(0),
            SimpleNamespace(revision=1, publication_count=7),
        ]
    )
    runtime = _Runtime(events, resident=_Resident(events), catalog=catalog)
    monkeypatch.setattr(bootstrap, "load_config", lambda path: config)
    monkeypatch.setattr(bootstrap, "configure_logging", lambda value: None)
    monkeypatch.setattr(bootstrap, "build_runtime", lambda value: runtime)

    assert bootstrap.main(["--config", str(tmp_path / "ingest.json")]) == 0

    assert events == ["initialize", ("process", True), "runtime-close"]
    assert "revision=1 publications=7" in capsys.readouterr().out


def test_bootstrap_refuses_an_existing_catalog_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    events: list[object] = []
    config = _bootstrap_config(tmp_path)
    runtime = _Runtime(
        events,
        resident=_Resident(events),
        catalog=_Catalog([SimpleNamespace(revision=9, publication_count=3)]),
    )
    monkeypatch.setattr(bootstrap, "load_config", lambda path: config)
    monkeypatch.setattr(bootstrap, "configure_logging", lambda value: None)
    monkeypatch.setattr(bootstrap, "build_runtime", lambda value: runtime)

    with pytest.raises(SystemExit) as stopped:
        bootstrap.main(["--config", str(tmp_path / "ingest.json")])

    assert stopped.value.code == 2
    assert "current_revision=9" in capsys.readouterr().err
    assert runtime.closed
    assert events[-1] == "runtime-close"


class _BatchResident(_Resident):
    def __init__(self, events: list[object], outcomes: tuple[int | None, ...]) -> None:
        super().__init__(events)
        self._outcomes = iter(outcomes)

    def process_available(
        self,
        *,
        periodic_scan: bool,
        preflight: Any = None,
        postflight: Any = None,
        should_stop: Any = None,
    ) -> bool:
        deferred = next(self._outcomes)
        if deferred is None:
            self._events.append("maintenance")
            return True
        result = super().process_available(
            periodic_scan=periodic_scan,
            preflight=preflight,
            postflight=postflight,
            should_stop=should_stop,
        )
        self.deferred_gallery_count = deferred
        return result


def test_bootstrap_continues_empty_batches_and_maintenance_until_first_nonempty(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    events: list[object] = []
    config = _bootstrap_config(tmp_path)
    first = SimpleNamespace(revision=1, publication_count=0)
    runtime = _Runtime(
        events,
        resident=_BatchResident(events, (None, 2, None, 0)),
        catalog=_Catalog(
            [
                CatalogRevisionNotFoundError(0),
                first,
                first,
                SimpleNamespace(revision=2, publication_count=2),
            ]
        ),
    )
    monkeypatch.setattr(bootstrap, "load_config", lambda path: config)
    monkeypatch.setattr(bootstrap, "configure_logging", lambda value: None)
    monkeypatch.setattr(bootstrap, "build_runtime", lambda value: runtime)

    assert bootstrap.main(["--config", str(tmp_path / "ingest.json")]) == 0
    assert events == [
        "initialize",
        "maintenance",
        ("process", True),
        "maintenance",
        ("process", True),
        "runtime-close",
    ]
    assert "revision=2 publications=2" in capsys.readouterr().out


def test_bootstrap_rejects_another_publication_between_its_empty_batches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    events: list[object] = []
    config = _bootstrap_config(tmp_path)
    runtime = _Runtime(
        events,
        resident=_BatchResident(events, (2, 0)),
        catalog=_Catalog(
            [
                CatalogRevisionNotFoundError(0),
                SimpleNamespace(revision=1, publication_count=0),
                SimpleNamespace(revision=2, publication_count=1),
            ]
        ),
    )
    monkeypatch.setattr(bootstrap, "load_config", lambda path: config)
    monkeypatch.setattr(bootstrap, "configure_logging", lambda value: None)
    monkeypatch.setattr(bootstrap, "build_runtime", lambda value: runtime)

    with pytest.raises(SystemExit) as stopped:
        bootstrap.main(["--config", str(tmp_path / "ingest.json")])
    assert stopped.value.code == 2
    assert "current_revision=2" in capsys.readouterr().err
    assert runtime.closed


def test_bootstrap_reports_empty_final_batch_without_retrying_forever(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    events: list[object] = []
    config = _bootstrap_config(tmp_path)
    runtime = _Runtime(
        events,
        resident=_BatchResident(events, (0,)),
        catalog=_Catalog(
            [
                CatalogRevisionNotFoundError(0),
                SimpleNamespace(revision=1, publication_count=0),
            ]
        ),
    )
    monkeypatch.setattr(bootstrap, "load_config", lambda path: config)
    monkeypatch.setattr(bootstrap, "configure_logging", lambda value: None)
    monkeypatch.setattr(bootstrap, "build_runtime", lambda value: runtime)

    with pytest.raises(SystemExit) as stopped:
        bootstrap.main(["--config", str(tmp_path / "ingest.json")])
    assert stopped.value.code == 1
    assert "did not publish a non-empty catalog" in capsys.readouterr().err
    assert events.count(("process", True)) == 1


def test_bootstrap_captures_its_publication_before_releasing_the_ingest_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    events: list[object] = []
    config = _bootstrap_config(tmp_path)

    class _RacingCatalog(_Catalog):
        lease_held = False
        published = False
        reads_after_release = 0

        def get_catalog_revision(self) -> object:
            if not self.published:
                raise CatalogRevisionNotFoundError(0)
            if self.lease_held:
                return SimpleNamespace(revision=1, publication_count=2)
            self.reads_after_release += 1
            return SimpleNamespace(revision=9, publication_count=99)

    catalog = _RacingCatalog([])

    class _RacingResident(_Resident):
        def process_available(
            self,
            *,
            periodic_scan: bool,
            preflight: Any = None,
            postflight: Any = None,
            should_stop: Any = None,
        ) -> bool:
            del periodic_scan, should_stop
            catalog.lease_held = True
            preflight()
            catalog.published = True
            if postflight is not None:
                postflight()
            self.last_synchronization_result = object()
            catalog.lease_held = False
            return True

    runtime = _Runtime(events, resident=_RacingResident(events), catalog=catalog)
    monkeypatch.setattr(bootstrap, "load_config", lambda path: config)
    monkeypatch.setattr(bootstrap, "configure_logging", lambda value: None)
    monkeypatch.setattr(bootstrap, "build_runtime", lambda value: runtime)

    assert bootstrap.main(["--config", str(tmp_path / "ingest.json")]) == 0
    assert "revision=1 publications=2" in capsys.readouterr().out
    assert catalog.reads_after_release == 0


def test_bootstrap_rejects_a_source_without_gallery_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    download = tmp_path / "download"
    download.mkdir()
    (download / "mounted-volume-marker").touch()
    config = IngestConfig(paths=IngestPathsConfig(download_path=download))
    opened = False

    def open_runtime(value: object) -> object:
        nonlocal opened
        del value
        opened = True
        return object()

    monkeypatch.setattr(bootstrap, "load_config", lambda path: config)
    monkeypatch.setattr(bootstrap, "build_runtime", open_runtime)

    with pytest.raises(SystemExit) as stopped:
        bootstrap.main(["--config", str(tmp_path / "ingest.json")])

    assert stopped.value.code == 2
    assert not opened
