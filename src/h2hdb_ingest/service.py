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

from h2hdb import (
    ArtifactReleaseAdapter,
    ArtifactStorageAdapter,
    VNextAnalysisAdvanceResult,
    VNextCurrentProjectionAdapter,
    VNextIngestAdvanceResult,
    VNextIngestPhase,
    VNextIngestPolicy,
    VNextIngestSourceAdapter,
    VNextIngestSourceReceipt,
    VNextResolvedIngestPolicy,
)

from .core_source import VNextFilesystemSourceAdapter
from .filesystem import FilesystemSource
from .session import IngestSessionController

_ANALYSIS_SNAPSHOT_STAGE = b"snapshot_manifest"


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
        current_projection: VNextCurrentProjectionAdapter,
        publication_guard: Callable[[], AbstractContextManager[None]],
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
        if not isinstance(current_projection, VNextCurrentProjectionAdapter):
            raise TypeError(
                "current_projection must implement VNextCurrentProjectionAdapter"
            )
        if not callable(publication_guard):
            raise TypeError("publication_guard must be callable")
        self._source_root = source_root
        self._policy = policy
        self._max_rows = max_rows
        self._artifact_adapters = dict(artifact_adapters)
        self._finalization_adapters = dict(finalization_adapters)
        self._current_projection = current_projection
        self._publication_guard = publication_guard

    def synchronize_once(
        self,
        session: IngestSessionController,
    ) -> VNextIngestSynchronizationResult:
        """Synchronize one exact filesystem snapshot through finalization."""

        if not isinstance(session, IngestSessionController):
            raise TypeError("session must be IngestSessionController")
        resolved = session.call(
            lambda facade, receipt: facade.ensure_policy(receipt, self._policy)
        )
        with FilesystemSource(self._source_root) as source:
            source_receipt = synchronize_source(
                session,
                resolved,
                VNextFilesystemSourceAdapter(source),
            )
        analysis = synchronize_analysis(
            session,
            resolved,
            source_receipt,
            self._max_rows,
        )
        with self._publication_guard():
            publication = synchronize_publication(
                session,
                resolved,
                artifact_adapters=self._artifact_adapters,
                finalization_adapters=self._finalization_adapters,
                current_projection=self._current_projection,
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
    current_projection: VNextCurrentProjectionAdapter,
) -> VNextIngestAdvanceResult:
    """Drive publication and finalization inside the caller's outer guard."""

    if not isinstance(session, IngestSessionController):
        raise TypeError("session must be IngestSessionController")
    if not isinstance(current_projection, VNextCurrentProjectionAdapter):
        raise TypeError(
            "current_projection must implement VNextCurrentProjectionAdapter"
        )
    while True:
        issued = session.call(
            lambda facade, receipt: facade.issue_publication_step(receipt, policy)
        )
        # outside_session invokes its callback before this loop advances.
        prepared = session.outside_session(
            lambda facade: facade.prepare_publication_step(
                issued,  # noqa: B023
                artifact_adapters=artifact_adapters,
                finalization_adapters=finalization_adapters,
                current_projection=current_projection,
            )
        )
        with prepared:
            # call invokes its callback before prepared can be reassigned.
            result = session.call(
                lambda facade, receipt: facade.commit_publication_step(
                    receipt,
                    prepared,  # noqa: B023
                )
            )
        _require_publication_result(result)
        if result.terminal:
            if result.phase is not VNextIngestPhase.FINALIZATION:
                raise RuntimeError("terminal publication advancement is not finalized")
            return result


def synchronize_source(
    session: IngestSessionController,
    policy: VNextResolvedIngestPolicy,
    adapter: VNextIngestSourceAdapter,
) -> VNextIngestSourceReceipt:
    """Drive source ingestion while keeping all local I/O outside the lease lock."""

    if not isinstance(session, IngestSessionController):
        raise TypeError("session must be IngestSessionController")
    prepared = session.outside_session(lambda facade: facade.prepare_source(adapter))
    with prepared:
        while True:
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
