from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import cast

import pytest
from h2hdb import (
    ArtifactReleaseAdapter,
    ArtifactStorageAdapter,
    CurrentProjectionCheckpoint,
    CurrentProjectionStatus,
    VNextAnalysisAdvanceResult,
    VNextCurrentProjectionAdapter,
    VNextCurrentProjectionItem,
    VNextIngestAdvanceResult,
    VNextIngestFacade,
    VNextIngestPhase,
    VNextIngestSession,
    VNextIngestSourceAdapter,
    VNextIngestSourceReceipt,
    VNextResolvedIngestPolicy,
)

import h2hdb_ingest.service as service_module
from h2hdb_ingest import IngestConfig, IngestPathsConfig, build_ingest_policy
from h2hdb_ingest.service import (
    VNextIngestService,
    synchronize_analysis,
    synchronize_publication,
    synchronize_source,
)
from h2hdb_ingest.session import IngestSessionController


def _session(*, lease_expires_at: int = 10_000_000) -> VNextIngestSession:
    return VNextIngestSession(
        gate_owner_token=b"g" * 16,
        gate_generation=1,
        gate_slot=0,
        gate_lease_expires_at=lease_expires_at,
        ingest_generation=2,
        ingest_owner_token=b"i" * 16,
        ingest_lease_expires_at=lease_expires_at,
        download_generation=None,
        handoff_owner_token=None,
        handoff_kind=None,
        consumed_at=None,
    )


class _PreparedSource:
    def __init__(self, events: list[object]) -> None:
        self._events = events

    def __enter__(self) -> _PreparedSource:
        self._events.append("enter")
        return self

    def __exit__(self, *exc: object) -> None:
        del exc
        self._events.append("close")


class _PreparedAnalysis:
    def __init__(self, events: list[object]) -> None:
        self._events = events

    def __enter__(self) -> _PreparedAnalysis:
        self._events.append("analysis-enter")
        return self

    def __exit__(self, *exc: object) -> None:
        del exc
        self._events.append("analysis-close")


class _AnalysisFacade:
    def __init__(self, events: list[object]) -> None:
        self._events = events
        self._step = 0
        self.forced_result: object | None = None

    def prepare_analysis(
        self,
        build_id: bytes,
        policy: object,
        *,
        max_rows: int,
    ) -> _PreparedAnalysis:
        self._events.append(("prepare-analysis", build_id, policy, max_rows))
        return _PreparedAnalysis(self._events)

    def issue_analysis_step(
        self,
        session: VNextIngestSession,
        prepared: _PreparedAnalysis,
    ) -> tuple[str, int]:
        del prepared
        self._events.append(
            ("analysis-issue", self._step, session.ingest_lease_expires_at)
        )
        return ("analysis-issued", self._step)

    def prepare_analysis_step(
        self,
        prepared: _PreparedAnalysis,
        issued: tuple[str, int],
    ) -> tuple[str, int]:
        del prepared
        self._events.append(("analysis-prepare-step", issued[1]))
        return ("analysis-prepared", issued[1])

    def commit_analysis_step(
        self,
        session: VNextIngestSession,
        prepared_step: tuple[str, int],
    ) -> object:
        del prepared_step
        self._events.append(
            ("analysis-commit", self._step, session.ingest_lease_expires_at)
        )
        if self.forced_result is not None:
            return self.forced_result
        terminal = self._step == 1
        self._step += 1
        return VNextAnalysisAdvanceResult(
            b"a" * 16,
            b"snapshot_manifest" if terminal else b"changed_gallery",
            0 if terminal else 1,
            terminal,
            terminal,
            False,
            b"s" * 32 if terminal else None,
        )


def test_analysis_orchestration_keeps_preparation_outside_bounded_calls() -> None:
    events: list[object] = []
    facade = _AnalysisFacade(events)
    policy = cast(VNextResolvedIngestPolicy, object())
    controller = IngestSessionController(
        cast(VNextIngestFacade, facade),
        _session(),
        lease_duration_microseconds=10_000_000,
        database_type="sqlite",
    )

    result = synchronize_analysis(
        controller,
        policy,
        VNextIngestSourceReceipt(b"b" * 16, 3, 3, True, False),
        max_rows=64,
    )

    assert result.analysis_id == b"a" * 16
    assert result.snapshot_manifest_sha256 == b"s" * 32
    assert events == [
        ("prepare-analysis", b"b" * 16, policy, 64),
        "analysis-enter",
        ("analysis-issue", 0, 10_000_000),
        ("analysis-prepare-step", 0),
        ("analysis-commit", 0, 10_000_000),
        ("analysis-issue", 1, 10_000_000),
        ("analysis-prepare-step", 1),
        ("analysis-commit", 1, 10_000_000),
        "analysis-close",
    ]


def test_analysis_orchestration_requires_a_sealed_source_receipt() -> None:
    events: list[object] = []
    controller = IngestSessionController(
        cast(VNextIngestFacade, _AnalysisFacade(events)),
        _session(),
        lease_duration_microseconds=10_000_000,
        database_type="sqlite",
    )

    try:
        synchronize_analysis(
            controller,
            cast(VNextResolvedIngestPolicy, object()),
            VNextIngestSourceReceipt(b"b" * 16, 3, 3, False, False),
            max_rows=64,
        )
    except RuntimeError as error:
        assert "sealed source receipt" in str(error)
    else:
        raise AssertionError("unsealed source receipt was accepted")
    assert events == []


def test_analysis_orchestration_rejects_a_foreign_phase_result() -> None:
    events: list[object] = []
    facade = _AnalysisFacade(events)
    facade.forced_result = VNextIngestAdvanceResult(
        VNextIngestPhase.PUBLICATION,
        0,
        True,
        False,
    )
    controller = IngestSessionController(
        cast(VNextIngestFacade, facade),
        _session(),
        lease_duration_microseconds=10_000_000,
        database_type="sqlite",
    )

    try:
        synchronize_analysis(
            controller,
            cast(VNextResolvedIngestPolicy, object()),
            VNextIngestSourceReceipt(b"b" * 16, 3, 3, True, False),
            max_rows=64,
        )
    except TypeError as error:
        assert "foreign result" in str(error)
    else:
        raise AssertionError("foreign analysis result was accepted")
    assert events[-1] == "analysis-close"


def test_analysis_orchestration_rejects_an_unsealed_terminal_snapshot() -> None:
    events: list[object] = []
    facade = _AnalysisFacade(events)
    facade.forced_result = VNextAnalysisAdvanceResult(
        b"a" * 16,
        b"snapshot_manifest",
        0,
        False,
        True,
        False,
        b"s" * 32,
    )
    controller = IngestSessionController(
        cast(VNextIngestFacade, facade),
        _session(),
        lease_duration_microseconds=10_000_000,
        database_type="sqlite",
    )

    try:
        synchronize_analysis(
            controller,
            cast(VNextResolvedIngestPolicy, object()),
            VNextIngestSourceReceipt(b"b" * 16, 3, 3, True, False),
            max_rows=64,
        )
    except RuntimeError as error:
        assert "sealed snapshot result" in str(error)
    else:
        raise AssertionError("unsealed terminal analysis result was accepted")
    assert events[-1] == "analysis-close"


def test_analysis_orchestration_rejects_a_terminal_non_analysis_stage() -> None:
    events: list[object] = []
    facade = _AnalysisFacade(events)
    facade.forced_result = VNextAnalysisAdvanceResult(
        b"a" * 16,
        b"publication",
        0,
        True,
        True,
        False,
        b"s" * 32,
    )
    controller = IngestSessionController(
        cast(VNextIngestFacade, facade),
        _session(),
        lease_duration_microseconds=10_000_000,
        database_type="sqlite",
    )

    try:
        synchronize_analysis(
            controller,
            cast(VNextResolvedIngestPolicy, object()),
            VNextIngestSourceReceipt(b"b" * 16, 3, 3, True, False),
            max_rows=64,
        )
    except RuntimeError as error:
        assert "sealed snapshot result" in str(error)
    else:
        raise AssertionError("wrong terminal analysis stage was accepted")
    assert events[-1] == "analysis-close"


class _Facade:
    def __init__(self, events: list[object]) -> None:
        self._events = events
        self._step = 0

    def prepare_source(self, adapter: object) -> _PreparedSource:
        self._events.append(("prepare-source", adapter))
        return _PreparedSource(self._events)

    def issue_source_step(
        self,
        session: VNextIngestSession,
        policy: object,
        prepared: _PreparedSource,
    ) -> tuple[str, int]:
        del policy, prepared
        self._events.append(("issue", self._step, session.ingest_lease_expires_at))
        return ("issued", self._step)

    def prepare_source_step(
        self,
        prepared: _PreparedSource,
        issued: tuple[str, int],
    ) -> tuple[str, int]:
        del prepared
        self._events.append(("prepare-step", issued[1]))
        return ("prepared", issued[1])

    def commit_source_step(
        self,
        session: VNextIngestSession,
        prepared_step: tuple[str, int],
    ) -> VNextIngestAdvanceResult:
        del prepared_step
        terminal = self._step == 1
        self._events.append(("commit", self._step, session.ingest_lease_expires_at))
        self._step += 1
        receipt = (
            VNextIngestSourceReceipt(b"b" * 16, 3, 3, True, False) if terminal else None
        )
        return VNextIngestAdvanceResult(
            VNextIngestPhase.SOURCE,
            1,
            terminal,
            False,
            receipt,
        )


def test_source_orchestration_keeps_local_preparation_between_bounded_calls() -> None:
    events: list[object] = []
    facade = _Facade(events)
    adapter = cast(VNextIngestSourceAdapter, object())
    controller = IngestSessionController(
        cast(VNextIngestFacade, facade),
        _session(),
        lease_duration_microseconds=10_000_000,
        database_type="sqlite",
    )

    receipt = synchronize_source(
        controller,
        cast(VNextResolvedIngestPolicy, object()),
        adapter,
    )

    assert receipt.build_id == b"b" * 16
    assert events == [
        ("prepare-source", adapter),
        "enter",
        ("issue", 0, 10_000_000),
        ("prepare-step", 0),
        ("commit", 0, 10_000_000),
        ("issue", 1, 10_000_000),
        ("prepare-step", 1),
        ("commit", 1, 10_000_000),
        "close",
    ]


def test_source_orchestration_rejects_terminal_result_without_sealed_receipt() -> None:
    events: list[object] = []
    facade = _Facade(events)
    facade._step = 1
    original = facade.commit_source_step

    def commit_without_receipt(
        session: VNextIngestSession,
        prepared_step: tuple[str, int],
    ) -> VNextIngestAdvanceResult:
        result = original(session, prepared_step)
        return VNextIngestAdvanceResult(
            result.phase,
            result.processed_rows,
            True,
            result.replayed,
            None,
        )

    facade.commit_source_step = commit_without_receipt  # type: ignore[method-assign]
    controller = IngestSessionController(
        cast(VNextIngestFacade, facade),
        _session(),
        lease_duration_microseconds=10_000_000,
        database_type="sqlite",
    )

    try:
        synchronize_source(
            controller,
            cast(VNextResolvedIngestPolicy, object()),
            cast(VNextIngestSourceAdapter, object()),
        )
    except RuntimeError as error:
        assert "sealed source receipt" in str(error)
    else:
        raise AssertionError("missing source receipt was accepted")


class _Projection:
    def __init__(self) -> None:
        self.guarded = False

    @contextmanager
    def publication_guard(self) -> Iterator[None]:
        assert not self.guarded
        self.guarded = True
        try:
            yield
        finally:
            self.guarded = False

    def begin(
        self,
        revision: int,
        receipt_id: bytes,
    ) -> CurrentProjectionCheckpoint:
        return CurrentProjectionCheckpoint(
            revision,
            receipt_id,
            CurrentProjectionStatus.COMPLETE,
            None,
        )

    def append_page(
        self,
        revision: int,
        items: Sequence[VNextCurrentProjectionItem],
    ) -> None:
        del revision, items

    def seal(self, revision: int) -> None:
        del revision

    def reconcile(self, revision: int) -> None:
        del revision


class _PreparedPublication:
    def __init__(self, events: list[object], step: int) -> None:
        self._events = events
        self.step = step

    def __enter__(self) -> _PreparedPublication:
        self._events.append(("publication-enter", self.step))
        return self

    def __exit__(self, *exc: object) -> None:
        del exc
        self._events.append(("publication-close", self.step))


class _PublicationFacade:
    def __init__(self, events: list[object]) -> None:
        self._events = events
        self._step = 0

    def issue_publication_step(
        self,
        session: VNextIngestSession,
        policy: object,
    ) -> tuple[str, int]:
        self._events.append(
            ("publication-issue", self._step, session.ingest_lease_expires_at, policy)
        )
        return ("issued", self._step)

    def prepare_publication_step(
        self,
        issued: tuple[str, int],
        *,
        artifact_adapters: Mapping[bytes, ArtifactStorageAdapter],
        finalization_adapters: Mapping[bytes, ArtifactReleaseAdapter],
        current_projection: VNextCurrentProjectionAdapter,
    ) -> _PreparedPublication:
        self._events.append(
            (
                "publication-prepare",
                issued[1],
                artifact_adapters,
                finalization_adapters,
                current_projection,
            )
        )
        return _PreparedPublication(self._events, issued[1])

    def commit_publication_step(
        self,
        session: VNextIngestSession,
        prepared: _PreparedPublication,
    ) -> VNextIngestAdvanceResult:
        self._events.append(
            (
                "publication-commit",
                prepared.step,
                session.ingest_lease_expires_at,
            )
        )
        terminal = self._step == 1
        phase = (
            VNextIngestPhase.FINALIZATION if terminal else VNextIngestPhase.PUBLICATION
        )
        self._step += 1
        return VNextIngestAdvanceResult(phase, 1, terminal, False)


def test_publication_orchestration_closes_each_prepared_step() -> None:
    events: list[object] = []
    facade = _PublicationFacade(events)
    controller = IngestSessionController(
        cast(VNextIngestFacade, facade),
        _session(),
        lease_duration_microseconds=10_000_000,
        database_type="sqlite",
    )
    projection = _Projection()
    storage = cast(ArtifactStorageAdapter, object())
    release = cast(ArtifactReleaseAdapter, object())
    policy = cast(VNextResolvedIngestPolicy, object())

    result = synchronize_publication(
        controller,
        policy,
        artifact_adapters={b"store": storage},
        finalization_adapters={b"store": release},
        current_projection=projection,
    )

    assert result.phase is VNextIngestPhase.FINALIZATION
    assert result.terminal
    assert [event[0] for event in events if isinstance(event, tuple)] == [
        "publication-issue",
        "publication-prepare",
        "publication-enter",
        "publication-commit",
        "publication-close",
        "publication-issue",
        "publication-prepare",
        "publication-enter",
        "publication-commit",
        "publication-close",
    ]


def test_complete_service_holds_one_outer_guard_through_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []
    projection = _Projection()
    config = IngestConfig(paths=IngestPathsConfig(download_path=tmp_path))
    policy = build_ingest_policy(config)
    resolved = cast(VNextResolvedIngestPolicy, object())
    source_receipt = VNextIngestSourceReceipt(b"s" * 16, 1, 1, True, False)
    analysis = VNextAnalysisAdvanceResult(
        b"a" * 16,
        b"snapshot_manifest",
        1,
        True,
        True,
        False,
        b"m" * 32,
    )
    publication = VNextIngestAdvanceResult(
        VNextIngestPhase.FINALIZATION,
        0,
        True,
        False,
    )

    class _Source:
        def __init__(self, root: Path) -> None:
            assert root == tmp_path

        def __enter__(self) -> _Source:
            events.append("source-enter")
            return self

        def __exit__(self, *exc: object) -> None:
            del exc
            events.append("source-close")

    class _PolicyFacade:
        def ensure_policy(
            self,
            receipt: VNextIngestSession,
            natural: object,
        ) -> VNextResolvedIngestPolicy:
            assert natural is policy
            events.append(("ensure-policy", receipt.ingest_generation))
            return resolved

    def fake_source(
        session: object,
        selected: object,
        adapter: object,
    ) -> VNextIngestSourceReceipt:
        del session, adapter
        assert selected is resolved
        events.append("source")
        return source_receipt

    def fake_analysis(
        session: object,
        selected: object,
        receipt: object,
        max_rows: int,
    ) -> VNextAnalysisAdvanceResult:
        del session
        assert selected is resolved and receipt is source_receipt and max_rows == 128
        events.append("analysis")
        return analysis

    def fake_publication(
        session: object,
        selected: object,
        **kwargs: object,
    ) -> VNextIngestAdvanceResult:
        del session, kwargs
        assert selected is resolved and projection.guarded
        events.append("publication")
        return publication

    def fake_adapter(source: object) -> object:
        events.append("adapter")
        return source

    monkeypatch.setattr(service_module, "FilesystemSource", _Source)
    monkeypatch.setattr(
        service_module,
        "VNextFilesystemSourceAdapter",
        fake_adapter,
    )
    monkeypatch.setattr(service_module, "synchronize_source", fake_source)
    monkeypatch.setattr(service_module, "synchronize_analysis", fake_analysis)
    monkeypatch.setattr(
        service_module,
        "synchronize_publication",
        fake_publication,
    )
    controller = IngestSessionController(
        cast(VNextIngestFacade, _PolicyFacade()),
        _session(),
        lease_duration_microseconds=10_000_000,
        database_type="sqlite",
    )
    service = VNextIngestService(
        source_root=tmp_path,
        policy=policy,
        max_rows=128,
        artifact_adapters={},
        finalization_adapters={},
        current_projection=projection,
        publication_guard=projection.publication_guard,
    )

    outcome = service.synchronize_once(controller)

    assert outcome.source is source_receipt
    assert outcome.analysis is analysis
    assert outcome.publication is publication
    assert events == [
        ("ensure-policy", 2),
        "source-enter",
        "adapter",
        "source",
        "source-close",
        "analysis",
        "publication",
    ]
