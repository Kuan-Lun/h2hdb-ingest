"""Greenfield orchestration over public vNext issue/prepare/commit calls."""

from __future__ import annotations

__all__ = [
    "VNextIngestService",
    "VNextIngestSynchronizationResult",
    "synchronize_analysis",
    "synchronize_publication",
    "synchronize_source",
]

from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from time import monotonic_ns

from h2hdb import (
    ArtifactReleaseAdapter,
    ArtifactStorageAdapter,
    VNextAnalysisAdvanceResult,
    VNextIngestAdvanceResult,
    VNextIngestPhase,
    VNextIngestPolicy,
    VNextIngestSourceAdapter,
    VNextIngestSourceReceipt,
    VNextLibraryActivationAdapter,
    VNextResolvedIngestPolicy,
)

from .core_source import VNextFilesystemSourceAdapter
from .filesystem import FilesystemSource
from .metrics import (
    IngestMetric,
    IngestMetricOperation,
    IngestMetricSink,
    IngestMetricValue,
    emit_ingest_metric,
)
from .session import IngestSessionController

_ANALYSIS_SNAPSHOT_STAGE = b"snapshot_manifest"


class _IngestStopRequested(Exception):
    """Normal resident termination observed between bounded durable steps."""


@dataclass(slots=True)
class _PublicationMetricAccumulator:
    issue_ns: int = 0
    prepare_ns: int = 0
    commit_ns: int = 0
    cleanup_ns: int = 0
    steps: int = 0
    processed_rows: int = 0
    replayed_steps: int = 0


def _never_stop() -> bool:
    return False


@dataclass(frozen=True, slots=True)
class VNextIngestSynchronizationResult:
    """Terminal receipts from one complete source-to-publication turn."""

    source: VNextIngestSourceReceipt
    analysis: VNextAnalysisAdvanceResult
    publication: VNextIngestAdvanceResult

    def __post_init__(self) -> None:
        if not isinstance(self.source, VNextIngestSourceReceipt):
            raise TypeError("source must be VNextIngestSourceReceipt")
        self.source.__post_init__()
        if not self.source.sealed:
            raise ValueError("synchronization source receipt must be sealed")
        if not isinstance(self.analysis, VNextAnalysisAdvanceResult):
            raise TypeError("analysis must be VNextAnalysisAdvanceResult")
        self.analysis.__post_init__()
        if not self.analysis.terminal:
            raise ValueError("synchronization analysis result must be terminal")
        if not isinstance(self.publication, VNextIngestAdvanceResult):
            raise TypeError("publication must be VNextIngestAdvanceResult")
        self.publication.__post_init__()
        if (
            not self.publication.terminal
            or self.publication.phase is not VNextIngestPhase.FINALIZATION
        ):
            raise ValueError("synchronization publication result must be terminal")


class VNextIngestService:
    """Drive one complete ingest turn while preserving every I/O boundary."""

    def __init__(
        self,
        *,
        source_root: Path,
        policy: VNextIngestPolicy,
        max_rows: int,
        artifact_adapters: Mapping[bytes, ArtifactStorageAdapter],
        finalization_adapters: Mapping[bytes, ArtifactReleaseAdapter],
        library_activation: VNextLibraryActivationAdapter,
        publication_guard: Callable[[], AbstractContextManager[None]],
        metrics_sink: IngestMetricSink | None = None,
    ) -> None:
        if not isinstance(source_root, Path):
            raise TypeError("source_root must be Path")
        if not isinstance(policy, VNextIngestPolicy):
            raise TypeError("policy must be VNextIngestPolicy")
        policy.__post_init__()
        if type(max_rows) is not int:
            raise TypeError("max_rows must be int")
        if not 1 <= max_rows <= 128:
            raise ValueError("max_rows must be from 1 through 128")
        if not isinstance(library_activation, VNextLibraryActivationAdapter):
            raise TypeError(
                "library_activation must implement VNextLibraryActivationAdapter"
            )
        if not callable(publication_guard):
            raise TypeError("publication_guard must be callable")
        if metrics_sink is not None and not callable(metrics_sink):
            raise TypeError("metrics_sink must be callable")
        self._source_root = source_root
        self._policy = policy
        self._max_rows = max_rows
        self._artifact_adapters = dict(artifact_adapters)
        self._finalization_adapters = dict(finalization_adapters)
        self._library_activation = library_activation
        self._publication_guard = publication_guard
        self._metrics_sink = metrics_sink

    def synchronize_once(
        self,
        session: IngestSessionController,
        *,
        should_stop: Callable[[], bool] | None = None,
    ) -> VNextIngestSynchronizationResult:
        """Synchronize one exact filesystem snapshot through finalization."""

        if not isinstance(session, IngestSessionController):
            raise TypeError("session must be IngestSessionController")
        stop_requested = should_stop or _never_stop
        _raise_if_stopping(stop_requested)
        resolved = session.call(
            lambda facade, receipt: facade.ensure_policy(receipt, self._policy)
        )
        with FilesystemSource(self._source_root) as source:
            source_receipt = synchronize_source(
                session,
                resolved,
                VNextFilesystemSourceAdapter(source),
                should_stop=stop_requested,
            )
        analysis = synchronize_analysis(
            session,
            resolved,
            source_receipt,
            self._max_rows,
            should_stop=stop_requested,
        )
        with self._publication_guard():
            publication = synchronize_publication(
                session,
                resolved,
                artifact_adapters=self._artifact_adapters,
                finalization_adapters=self._finalization_adapters,
                library_activation=self._library_activation,
                should_stop=stop_requested,
                metrics_sink=self._metrics_sink,
            )
        return VNextIngestSynchronizationResult(
            source_receipt,
            analysis,
            publication,
        )


def synchronize_analysis(
    session: IngestSessionController,
    policy: VNextResolvedIngestPolicy,
    source_receipt: VNextIngestSourceReceipt,
    max_rows: int,
    *,
    should_stop: Callable[[], bool] = _never_stop,
) -> VNextAnalysisAdvanceResult:
    """Drive bounded core analysis while leaving preparation outside the lock."""

    if not isinstance(session, IngestSessionController):
        raise TypeError("session must be IngestSessionController")
    if not isinstance(source_receipt, VNextIngestSourceReceipt):
        raise TypeError("source_receipt must be VNextIngestSourceReceipt")
    source_receipt.__post_init__()
    if not source_receipt.sealed:
        raise RuntimeError("analysis requires a sealed source receipt")
    if type(max_rows) is not int:
        raise TypeError("analysis max_rows must be int")
    if not 1 <= max_rows <= 128:
        raise ValueError("analysis max_rows must be from 1 through 128")

    prepared = session.outside_session(
        lambda facade: facade.prepare_analysis(
            source_receipt.build_id,
            policy,
            max_rows=max_rows,
        )
    )
    with prepared:
        while True:
            _raise_if_stopping(should_stop)
            issued = session.call(
                lambda facade, receipt: facade.issue_analysis_step(
                    receipt,
                    prepared,
                )
            )
            # outside_session invokes its callback before this loop advances.
            prepared_step = session.outside_session(
                lambda facade: facade.prepare_analysis_step(
                    prepared,
                    issued,  # noqa: B023
                )
            )
            # call invokes its callback before prepared_step can be reassigned.
            result = session.call(
                lambda facade, receipt: facade.commit_analysis_step(
                    receipt,
                    prepared_step,  # noqa: B023
                )
            )
            _require_analysis_result(result)
            _raise_if_stopping(should_stop)
            if not result.terminal:
                continue
            if (
                not result.stage_terminal
                or result.stage != _ANALYSIS_SNAPSHOT_STAGE
                or result.snapshot_manifest_sha256 is None
            ):
                raise RuntimeError(
                    "terminal analysis advancement lacks a sealed snapshot result"
                )
            return result


def synchronize_publication(
    session: IngestSessionController,
    policy: VNextResolvedIngestPolicy,
    *,
    artifact_adapters: Mapping[bytes, ArtifactStorageAdapter],
    finalization_adapters: Mapping[bytes, ArtifactReleaseAdapter],
    library_activation: VNextLibraryActivationAdapter,
    should_stop: Callable[[], bool] = _never_stop,
    metrics_sink: IngestMetricSink | None = None,
) -> VNextIngestAdvanceResult:
    """Drive publication and finalization inside the caller's outer guard."""

    if not isinstance(session, IngestSessionController):
        raise TypeError("session must be IngestSessionController")
    if not isinstance(library_activation, VNextLibraryActivationAdapter):
        raise TypeError(
            "library_activation must implement VNextLibraryActivationAdapter"
        )
    if metrics_sink is not None and not callable(metrics_sink):
        raise TypeError("metrics_sink must be callable")
    started_ns = monotonic_ns()
    operation_metrics: dict[str, _PublicationMetricAccumulator] = {}
    while True:
        _raise_if_stopping(should_stop)
        issue_started_ns = monotonic_ns()
        issued = session.call(
            lambda facade, receipt: facade.issue_publication_step(receipt, policy)
        )
        issue_ns = monotonic_ns() - issue_started_ns
        operation = issued.operation
        if type(operation) is not str or not operation:
            raise RuntimeError("issued publication operation name is invalid")
        aggregate = operation_metrics.setdefault(
            operation,
            _PublicationMetricAccumulator(),
        )
        aggregate.issue_ns += issue_ns
        # outside_session invokes its callback before this loop advances.
        prepare_started_ns = monotonic_ns()
        prepared = session.outside_session(
            lambda facade: facade.prepare_publication_step(
                issued,  # noqa: B023
                artifact_adapters=artifact_adapters,
                finalization_adapters=finalization_adapters,
                library_activation=library_activation,
            )
        )
        aggregate.prepare_ns += monotonic_ns() - prepare_started_ns
        with prepared:
            # call invokes its callback before prepared can be reassigned.
            commit_started_ns = monotonic_ns()
            result = session.call(
                lambda facade, receipt: facade.commit_publication_step(
                    receipt,
                    prepared,  # noqa: B023
                )
            )
            aggregate.commit_ns += monotonic_ns() - commit_started_ns
            cleanup_started_ns = monotonic_ns()
        aggregate.cleanup_ns += monotonic_ns() - cleanup_started_ns
        _require_publication_result(result)
        aggregate.steps += 1
        aggregate.processed_rows += result.processed_rows
        aggregate.replayed_steps += int(result.replayed)
        _raise_if_stopping(should_stop)
        if result.terminal:
            if result.phase is not VNextIngestPhase.FINALIZATION:
                raise RuntimeError("terminal publication advancement is not finalized")
            _emit_publication_metric(
                metrics_sink,
                started_ns=started_ns,
                operation_metrics=operation_metrics,
            )
            return result


def synchronize_source(
    session: IngestSessionController,
    policy: VNextResolvedIngestPolicy,
    adapter: VNextIngestSourceAdapter,
    *,
    should_stop: Callable[[], bool] = _never_stop,
) -> VNextIngestSourceReceipt:
    """Drive source ingestion while keeping all local I/O outside the lease lock."""

    if not isinstance(session, IngestSessionController):
        raise TypeError("session must be IngestSessionController")
    prepared = session.outside_session(lambda facade: facade.prepare_source(adapter))
    with prepared:
        while True:
            _raise_if_stopping(should_stop)
            issued = session.call(
                lambda facade, receipt: facade.issue_source_step(
                    receipt,
                    policy,
                    prepared,
                )
            )
            # outside_session invokes its callback before this loop advances.
            prepared_step = session.outside_session(
                lambda facade: facade.prepare_source_step(
                    prepared,
                    issued,  # noqa: B023
                )
            )
            # call invokes its callback before prepared_step can be reassigned.
            result = session.call(
                lambda facade, receipt: facade.commit_source_step(
                    receipt,
                    prepared_step,  # noqa: B023
                )
            )
            _require_source_result(result)
            _raise_if_stopping(should_stop)
            if not result.terminal:
                continue
            source_receipt = result.source_receipt
            if source_receipt is None or not source_receipt.sealed:
                raise RuntimeError(
                    "terminal source advancement lacks a sealed source receipt"
                )
            return source_receipt


def _require_analysis_result(result: VNextAnalysisAdvanceResult) -> None:
    if not isinstance(result, VNextAnalysisAdvanceResult):
        raise TypeError("commit_analysis_step returned a foreign result")
    result.__post_init__()


def _require_source_result(result: VNextIngestAdvanceResult) -> None:
    if not isinstance(result, VNextIngestAdvanceResult):
        raise TypeError("commit_source_step returned a foreign result")
    result.__post_init__()
    if result.phase is not VNextIngestPhase.SOURCE:
        raise RuntimeError("source advancement returned another ingest phase")


def _require_publication_result(result: VNextIngestAdvanceResult) -> None:
    if not isinstance(result, VNextIngestAdvanceResult):
        raise TypeError("commit_publication_step returned a foreign result")
    result.__post_init__()
    if result.phase not in {
        VNextIngestPhase.PUBLICATION,
        VNextIngestPhase.FINALIZATION,
    }:
        raise RuntimeError("publication advancement returned another ingest phase")


def _emit_publication_metric(
    sink: IngestMetricSink | None,
    *,
    started_ns: int,
    operation_metrics: Mapping[str, _PublicationMetricAccumulator],
) -> None:
    operations = tuple(
        IngestMetricOperation(
            operation=operation,
            phases_ns=(
                IngestMetricValue("issue", aggregate.issue_ns),
                IngestMetricValue("prepare", aggregate.prepare_ns),
                IngestMetricValue("commit", aggregate.commit_ns),
                IngestMetricValue("cleanup", aggregate.cleanup_ns),
            ),
            counters=(
                IngestMetricValue("steps", aggregate.steps),
                IngestMetricValue("processed_rows", aggregate.processed_rows),
                IngestMetricValue("replayed_steps", aggregate.replayed_steps),
            ),
        )
        for operation, aggregate in operation_metrics.items()
    )
    emit_ingest_metric(
        sink,
        IngestMetric(
            scope="publication",
            operation="synchronize",
            elapsed_ns=monotonic_ns() - started_ns,
            counters=(
                IngestMetricValue(
                    "steps",
                    sum(aggregate.steps for aggregate in operation_metrics.values()),
                ),
                IngestMetricValue(
                    "processed_rows",
                    sum(
                        aggregate.processed_rows
                        for aggregate in operation_metrics.values()
                    ),
                ),
                IngestMetricValue(
                    "replayed_steps",
                    sum(
                        aggregate.replayed_steps
                        for aggregate in operation_metrics.values()
                    ),
                ),
            ),
            operations=operations,
        ),
    )


def _raise_if_stopping(should_stop: Callable[[], bool]) -> None:
    if should_stop():
        raise _IngestStopRequested
