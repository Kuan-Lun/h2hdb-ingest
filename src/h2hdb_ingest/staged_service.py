"""Restartable, bounded orchestration for one filesystem catalog publication.

The durable source build and projection are owned by core.  This service only
orders the ingest-owned filesystem work around those public APIs.  In
particular, a pending publication receipt is recovered before a new source scan
can start, and the artifact-store publication guard is always acquired before
the core database gate.
"""

from __future__ import annotations

__all__ = [
    "CBZPublicationCoordinator",
    "DatabaseGate",
    "IngestSynchronizer",
    "StagedCatalogCoordinator",
    "StagedIngestService",
]

import logging
from collections.abc import Callable, Iterable, Iterator
from contextlib import AbstractContextManager, nullcontext
from pathlib import Path
from typing import Protocol, runtime_checkable

from h2hdb import (
    CatalogBuild,
    CatalogBuildAnalyzer,
    CatalogBuildBatchConflictError,
    CatalogBuildCoordinator,
    CatalogBuildPhase,
    CatalogBuildProjectionCoordinator,
    CatalogOperationalGenerationStaleError,
    CatalogProjectionArtifactCursor,
    CatalogProjectionBatchConflictError,
    CatalogProjectionPublicationReceipt,
    CatalogProjectionPublicationState,
    CatalogReader,
    GalleryIngestTurn,
)

from .cbz import CBZSourceChangedError
from .core_analysis import CoreStagedDeduplicationAdapter
from .core_projection import CoreStagedProjectionAdapter
from .models import CBZArtifact, CBZGalleryDescriptor, SyncOutcome
from .scanner import GalleryScanError
from .staged_deduplication import StagedDeduplicationPlanner
from .staged_projection import CBZStreamPreparer, StagedProjectionOrchestrator
from .staging import CatalogScopeMismatchError, FilesystemSourceStager

CORE_MAX_PUBLISHED_ARTIFACT_PAGE_SIZE = 200
CORE_MAX_OPERATIONAL_PAGE_SIZE = 1_000
CORE_MAX_CLEANUP_PAGE_SIZE = 1_000
CORE_MAX_CLEANUP_CANDIDATES = 1_000
logger = logging.getLogger(__name__)


@runtime_checkable
class IngestSynchronizer(Protocol):
    """Neutral resident-loop boundary shared by legacy and staged services."""

    def synchronize_once(self, turn: GalleryIngestTurn) -> SyncOutcome: ...


@runtime_checkable
class StagedCatalogCoordinator(
    CatalogBuildCoordinator,
    CatalogBuildAnalyzer,
    CatalogBuildProjectionCoordinator,
    Protocol,
):
    """The public core capabilities needed by staged ingest."""


class CBZPublicationCoordinator(CBZStreamPreparer, Protocol):
    """Artifact preparation plus durable publication reconciliation."""

    def release_publish_protection(self, protection_id: str) -> None: ...

    def finalize_published(
        self,
        artifacts: Iterable[CBZArtifact],
        *,
        revision: int | None = None,
        protection_id: str,
    ) -> None: ...


class DatabaseGate(Protocol):
    def database_gate(
        self,
        *,
        timeout_seconds: int | None = None,
    ) -> AbstractContextManager[None]: ...


class StagedIngestService:
    """Drive one durable source build through joint catalog publication.

    All corpus-sized reads and writes are delegated to the paged staging,
    analysis, and projection components.  The only publication critical
    section contains the constant-time pointer transaction, revision checks,
    and CBZ current-view finalization.
    """

    def __init__(
        self,
        *,
        source_stager: FilesystemSourceStager,
        planner: StagedDeduplicationPlanner,
        catalog: StagedCatalogCoordinator,
        database_admin: DatabaseGate,
        catalog_reader: CatalogReader,
        source_root: Path,
        scope_key: str,
        cbz: CBZPublicationCoordinator | None,
        projection_gallery_page_size: int = 64,
        projection_file_page_size: int = 512,
        projection_selection_batch_size: int = 128,
        published_artifact_page_size: int = 200,
        operational_page_size: int = 1_000,
        cleanup_candidate_limit: int = 8,
        cleanup_page_size: int = 1_000,
        event_logger: Callable[[str], None] | None = None,
    ) -> None:
        if not scope_key:
            raise ValueError("scope_key must not be blank")
        for label, value in (
            ("projection_gallery_page_size", projection_gallery_page_size),
            ("projection_file_page_size", projection_file_page_size),
            ("projection_selection_batch_size", projection_selection_batch_size),
            ("published_artifact_page_size", published_artifact_page_size),
            ("operational_page_size", operational_page_size),
            ("cleanup_candidate_limit", cleanup_candidate_limit),
            ("cleanup_page_size", cleanup_page_size),
        ):
            if value <= 0:
                raise ValueError(f"{label} must be positive")
        self._source_stager = source_stager
        self._planner = planner
        self._catalog = catalog
        self._database_admin = database_admin
        self._catalog_reader = catalog_reader
        self._source_root = source_root
        self._scope_key = scope_key
        self._cbz = cbz
        self._projection_gallery_page_size = projection_gallery_page_size
        self._projection_file_page_size = projection_file_page_size
        self._projection_selection_batch_size = projection_selection_batch_size
        self._published_artifact_page_size = min(
            published_artifact_page_size,
            CORE_MAX_PUBLISHED_ARTIFACT_PAGE_SIZE,
        )
        self._operational_page_size = min(
            operational_page_size,
            CORE_MAX_OPERATIONAL_PAGE_SIZE,
        )
        self._cleanup_candidate_limit = min(
            cleanup_candidate_limit,
            CORE_MAX_CLEANUP_CANDIDATES,
        )
        self._cleanup_page_size = min(
            cleanup_page_size,
            CORE_MAX_CLEANUP_PAGE_SIZE,
        )
        self._event_logger = event_logger or logger.info

    def synchronize_once(self, turn: GalleryIngestTurn) -> SyncOutcome:
        self._cleanup_obsolete_builds(turn)
        pending = self._catalog.get_catalog_projection_publication_receipt(
            pending_only=True
        )
        if pending is not None:
            pending_build = self._require_build(pending.build_id)
            if pending_build.scope_key != self._scope_key:
                raise CatalogScopeMismatchError(
                    "The committed catalog projection belongs to a different "
                    "ingest scope. Restore the source and CBZ policy/roots used by "
                    "that build before finalizing its protected artifacts: "
                    f"build_id={pending_build.build_id}"
                )
            finalized = self._finalize_receipt(pending_build, pending, turn)
            self._cleanup_obsolete_builds(turn)
            return self._outcome(
                pending_build,
                finalized,
                immediate_rescan_required=True,
            )

        build: CatalogBuild | None = None
        artifacts_created = 0
        artifacts_rebuilt = 0
        try:
            build = self._source_stager.begin_or_resume(
                scope_key=self._scope_key,
                ingest_turn=turn,
            )
            build = self._source_stager.stage(build, ingest_turn=turn)

            if build.phase is CatalogBuildPhase.analyzing:
                self._planner.run(
                    CoreStagedDeduplicationAdapter(self._catalog, build, turn),
                    build_id=build.build_id,
                )
                build = self._catalog.complete_catalog_analysis(
                    build,
                    ingest_turn=turn,
                )

            if build.phase not in {
                CatalogBuildPhase.artifacts,
                CatalogBuildPhase.sealed,
            }:
                raise RuntimeError(
                    "Source build did not reach artifact preparation: "
                    f"phase={build.phase.value}"
                )

            projection_adapter = CoreStagedProjectionAdapter(
                self._catalog,
                build,
                turn,
                source_root=self._source_root,
            )
            projection_adapter.begin_or_resume(artifacts_required=self._cbz is not None)
            projection_summary = StagedProjectionOrchestrator(
                adapter=projection_adapter,
                cbz=self._cbz,
                gallery_page_size=self._projection_gallery_page_size,
                file_page_size=self._projection_file_page_size,
                selection_batch_size=self._projection_selection_batch_size,
            ).run(build.build_id)
            artifacts_created = projection_summary.artifacts_created
            artifacts_rebuilt = projection_summary.artifacts_rebuilt

            while True:
                # Revalidate even when resuming an already sealed pre-publication
                # build: the process may have been down while source files changed.
                # A committed receipt is handled above because its pointer can no
                # longer be rolled back; that path requests one immediate rescan.
                self._prepare_operations(build, turn)
                # Operational preparation can span many short transactions. Run
                # the clean filesystem pass after it, as close as possible to the
                # constant-time seal/pointer cutover.
                self._source_stager.validate(build)
                if build.phase is CatalogBuildPhase.artifacts:
                    self._catalog.seal_catalog_build_projection(
                        build,
                        ingest_turn=turn,
                    )
                    build = self._catalog.seal_catalog_build(
                        build,
                        ingest_turn=turn,
                    )
                if build.phase is not CatalogBuildPhase.sealed:
                    raise RuntimeError(
                        "Source build did not reach publication seal: "
                        f"phase={build.phase.value}"
                    )
                try:
                    receipt = self._publish_and_finalize(build, turn)
                except CatalogOperationalGenerationStaleError:
                    # A deletion request changed after the bounded preparation.
                    # Core discarded that invisible generation atomically; repeat
                    # the clean validation/preparation pass on the same sealed build.
                    continue
                break
            self._cleanup_obsolete_builds(turn)
            return self._outcome(
                build,
                receipt,
                cbz_created=artifacts_created,
                cbz_rebuilt=artifacts_rebuilt,
                immediate_rescan_required=False,
            )
        except Exception as error:
            if build is not None:
                self._settle_failed_build(build, turn, error)
            raise

    def _prepare_operations(
        self,
        build: CatalogBuild,
        turn: GalleryIngestTurn,
    ) -> None:
        """Advance bounded invisible downloader/deletion cutover work."""

        while True:
            state = self._catalog.prepare_catalog_build_operations(
                build,
                max_rows=self._operational_page_size,
                ingest_turn=turn,
            )
            if state.complete:
                return

    def _cleanup_obsolete_builds(self, turn: GalleryIngestTurn) -> None:
        """Best-effort bounded cleanup discovered from durable core state."""

        try:
            candidates = self._catalog.list_catalog_build_cleanup_candidates(
                limit=self._cleanup_candidate_limit,
            )
            for candidate in candidates:
                if not self._prepare_abandoned_cleanup(candidate):
                    continue
                self._prune_build(candidate, turn)
        except Exception as error:
            # Cleanup is retryable and must never turn a committed publication
            # into a failed ingest result. The candidate remains durably listable.
            self._event_logger(
                "Deferred catalog build cleanup after a retryable failure: "
                f"{error!r}"
            )

    def _prepare_abandoned_cleanup(self, build: CatalogBuild) -> bool:
        """Release scoped artifact protection before deleting its DB descriptor."""

        if build.phase is not CatalogBuildPhase.abandoned:
            return True
        if build.scope_key != self._scope_key:
            self._event_logger(
                "Deferred abandoned catalog build cleanup because its ingest "
                f"scope is not active: build_id={build.build_id}"
            )
            return False
        projection = self._catalog.get_catalog_build_projection(build.build_id)
        if projection is None or not projection.artifacts_required:
            return True
        if self._cbz is None:
            self._event_logger(
                "Deferred abandoned catalog build cleanup because its protected "
                "CBZ coordination domain is unavailable: "
                f"build_id={build.build_id}"
            )
            return False
        # Release is idempotent. It must commit before the only durable build ID
        # and projection descriptor are pruned, so a crash can retry this ordering.
        self._cbz.release_publish_protection(build.build_id)
        return True

    def _prune_build(
        self,
        build: CatalogBuild,
        turn: GalleryIngestTurn,
    ) -> None:
        while True:
            projection = self._catalog.prune_catalog_build_projection(
                build,
                max_rows=self._cleanup_page_size,
                ingest_turn=turn,
            )
            if projection.complete:
                break
        while True:
            source = self._catalog.prune_catalog_build(
                build.build_id,
                max_rows=self._cleanup_page_size,
            )
            if source.complete:
                return

    def _publish_and_finalize(
        self,
        build: CatalogBuild,
        turn: GalleryIngestTurn,
    ) -> CatalogProjectionPublicationReceipt:
        with self._publication_guard():
            try:
                with self._database_admin.database_gate():
                    published = self._catalog.publish_catalog_build_with_projection(
                        build,
                        ingest_turn=turn,
                    )
                receipt = published.receipt
                build = published.build
            except Exception as publish_error:
                # The commit result is ambiguous until the durable receipt is read.
                # A confirmed receipt means its protected artifacts must be retained
                # and finalization can safely continue idempotently.
                recovered_receipt = (
                    self._catalog.get_catalog_projection_publication_receipt(
                        build.build_id
                    )
                )
                if recovered_receipt is None:
                    raise
                receipt = recovered_receipt
                published_build = self._catalog.get_catalog_build(build.build_id)
                if published_build is None:
                    raise RuntimeError(
                        "A projection receipt exists without its source build"
                    ) from publish_error
                build = published_build
                publish_error.add_note(
                    "Joint publication committed despite the reported error; "
                    "continuing from its durable receipt."
                )
            return self._finalize_receipt_locked(build, receipt, turn)

    def _finalize_receipt(
        self,
        build: CatalogBuild,
        receipt: CatalogProjectionPublicationReceipt,
        turn: GalleryIngestTurn,
    ) -> CatalogProjectionPublicationReceipt:
        with self._publication_guard():
            return self._finalize_receipt_locked(build, receipt, turn)

    def _finalize_receipt_locked(
        self,
        build: CatalogBuild,
        receipt: CatalogProjectionPublicationReceipt,
        turn: GalleryIngestTurn,
    ) -> CatalogProjectionPublicationReceipt:
        if build.build_id != receipt.build_id:
            raise RuntimeError(
                "Publication receipt belongs to a different source build"
            )
        if receipt.state is CatalogProjectionPublicationState.projection_finalized:
            return receipt

        revision = receipt.catalog_revision.revision
        projection = self._catalog.get_catalog_build_projection(build.build_id)
        if projection is None:
            raise RuntimeError(
                "Publication receipt exists without its projection descriptor"
            )
        self._require_current_revision(revision)
        if projection.artifacts_required and self._cbz is None:
            raise RuntimeError(
                "The committed catalog projection requires CBZ reconciliation, "
                "but this ingest process has no CBZ configuration"
            )
        if projection.artifacts_required:
            assert self._cbz is not None
            self._cbz.finalize_published(
                self._published_cbz_artifacts(receipt),
                revision=revision,
                protection_id=build.build_id,
            )
        self._require_current_revision(revision)
        with self._database_admin.database_gate():
            finalized = self._catalog.acknowledge_catalog_projection_finalized(
                build,
                catalog_revision=revision,
                ingest_turn=turn,
            )
        if (
            finalized.state
            is not CatalogProjectionPublicationState.projection_finalized
        ):
            raise RuntimeError("Core did not acknowledge projection finalization")
        return finalized

    def _published_cbz_artifacts(
        self,
        receipt: CatalogProjectionPublicationReceipt,
    ) -> Iterator[CBZArtifact]:
        after: CatalogProjectionArtifactCursor | None = None
        seen = 0
        while True:
            page = self._catalog.list_published_catalog_projection_artifacts(
                receipt.build_id,
                after=after,
                limit=self._published_artifact_page_size,
            )
            if page.revision.revision != receipt.catalog_revision.revision:
                raise RuntimeError(
                    "Published artifact page belongs to a different catalog revision"
                )
            if len(page.items) > self._published_artifact_page_size:
                raise RuntimeError(
                    "Published artifact page exceeded its requested limit"
                )
            if not page.items:
                break
            if page.next_cursor is None:
                raise RuntimeError("A non-empty published artifact page has no cursor")
            if after is not None and page.next_cursor <= after:
                raise RuntimeError("Published artifact cursor did not advance")
            for item in page.items:
                artifact = item.artifact
                yield CBZArtifact(
                    gallery=CBZGalleryDescriptor(
                        gallery_name=item.gallery_name,
                        gid=item.gid,
                        upload_time=item.upload_time,
                    ),
                    path=artifact.location,
                    size_bytes=artifact.size_bytes,
                    sha256=artifact.sha256,
                    modified_at=artifact.modified_at,
                    created=False,
                    rebuilt=False,
                )
                seen += 1
            after = page.next_cursor
        if seen != receipt.selected_galleries:
            raise RuntimeError(
                "Published CBZ artifact count does not match selected galleries: "
                f"expected={receipt.selected_galleries} actual={seen}"
            )

    def _require_current_revision(self, expected: int) -> None:
        actual = self._catalog_reader.get_catalog_revision().revision
        if actual != expected:
            raise RuntimeError(
                "Catalog revision changed during projection finalization: "
                f"expected={expected} actual={actual}"
            )

    def _publication_guard(self) -> AbstractContextManager[None]:
        if self._cbz is None:
            return nullcontext()
        return self._cbz.publication_guard()

    def _require_build(self, build_id: str) -> CatalogBuild:
        build = self._catalog.get_catalog_build(build_id)
        if build is None:
            raise RuntimeError(f"Catalog build is missing: {build_id}")
        return build

    def _settle_failed_build(
        self,
        build: CatalogBuild,
        turn: GalleryIngestTurn,
        error: Exception,
    ) -> None:
        """Abandon before releasing artifacts, but retain on any ambiguity."""

        try:
            receipt = self._catalog.get_catalog_projection_publication_receipt(
                build.build_id
            )
        except Exception as receipt_error:
            error.add_note(
                "Could not determine whether catalog publication committed; "
                "build protection was retained: "
                f"{receipt_error!r}"
            )
            return
        if receipt is not None:
            error.add_note(
                "Catalog publication has a durable receipt; build protection was "
                "retained for projection recovery."
            )
            return

        try:
            current = self._catalog.get_catalog_build(build.build_id)
        except Exception as build_error:
            error.add_note(
                "Could not inspect the failed catalog build; build protection was "
                f"retained: {build_error!r}"
            )
            return
        if current is None or current.phase is CatalogBuildPhase.published:
            error.add_note(
                "The failed build could not be proven abandoned; build protection "
                "was retained."
            )
            return
        deterministic_failure = isinstance(
            error,
            (
                CBZSourceChangedError,
                GalleryScanError,
                CatalogBuildBatchConflictError,
                CatalogProjectionBatchConflictError,
            ),
        )
        if (
            current.phase is not CatalogBuildPhase.abandoned
            and not deterministic_failure
        ):
            error.add_note(
                "The pre-publication failure may have followed a committed "
                "durable batch; the working build and artifact protection were "
                "retained for idempotent recovery."
            )
            return
        if current.phase is not CatalogBuildPhase.abandoned:
            try:
                current = self._catalog.abandon_catalog_build(
                    current,
                    ingest_turn=turn,
                )
            except Exception as abandon_error:
                error.add_note(
                    "The failed catalog build could not be abandoned; build "
                    f"protection was retained: {abandon_error!r}"
                )
                return
        if current.phase is not CatalogBuildPhase.abandoned:
            return
        if self._cbz is not None:
            try:
                self._cbz.release_publish_protection(build.build_id)
            except Exception as release_error:
                error.add_note(
                    "The abandoned build's artifact protection could not be "
                    f"released: {release_error!r}"
                )
                return
        try:
            self._prune_build(current, turn)
        except Exception as prune_error:
            error.add_note(
                "The abandoned catalog build could not be fully pruned and will "
                f"remain a durable cleanup candidate: {prune_error!r}"
            )

    @staticmethod
    def _outcome(
        build: CatalogBuild,
        receipt: CatalogProjectionPublicationReceipt,
        *,
        cbz_created: int = 0,
        cbz_rebuilt: int = 0,
        immediate_rescan_required: bool,
    ) -> SyncOutcome:
        if build.expected_gallery_count is None:
            raise RuntimeError("Published source build has no expected gallery count")
        return SyncOutcome(
            revision=receipt.catalog_revision.revision,
            scanned=build.expected_gallery_count,
            published=receipt.selected_galleries,
            new=receipt.new_galleries,
            changed=receipt.changed_galleries,
            removed=receipt.removed_galleries,
            duplicate_losers=receipt.duplicate_losers,
            cbz_created=cbz_created,
            cbz_rebuilt=cbz_rebuilt,
            immediate_rescan_required=immediate_rescan_required,
        )
