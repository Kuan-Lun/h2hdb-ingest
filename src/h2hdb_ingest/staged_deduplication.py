"""Bounded orchestration for durable, staged deduplication.

The core build API currently exposes source-gallery pages, but those pages use
offsets and hydrate every file in each selected gallery.  That is not a usable
contract for analysing a multi-million-file build.  This module therefore
defines the producer-facing, storage-neutral adapter contract needed by the
deduplication policy:

* keyset pages of database-computed file-hash aggregates;
* keyset pages of source files annotated with the persisted spam decision;
* keyset pages of content and GID candidates, including active incumbents; and
* idempotent, bounded writes for each derived decision plus a durable phase
  completion marker.

An adapter is expected to implement the page queries with database grouping
and joins.  In particular, ``page_file_hash_aggregates`` must omit the exact
leaf name ``galleryinfo.txt`` and return one row per hash.  Its three aggregate
counts preserve the legacy spam rule without transporting every artist string
to the ingest process.  ``page_gallery_file_hashes`` must retain one row per
  file occurrence and order by ``(gallery_key, file_sha256, file_key)``; this
lets the planner hash incrementally while preserving duplicate occurrences.
It must also emit the documented empty-gallery sentinel when a gallery has no
file rows, so that the planner durably records a null content digest.  Content
and GID candidate rows carry the incumbent selected from the active source
snapshot, and their ``already_uploaded`` flag must use the legacy exact
case-folded tag-value rule.
The candidate page queries must be derived from the decisions written by the
preceding phase.  No core repository or connector implementation is imported
here.

The last phase deliberately emits a complete record for every gallery.  Its
``content_sha256`` is the effective digest derived here (not the scanner's raw
digest), ``duplicate_of_gallery_name`` is populated only for a content loser,
and ``selected`` is true only for the final GID winner.
"""

__all__ = [
    "AnalysisScanCompletion",
    "ContentCandidateCursor",
    "ContentCandidatePage",
    "ContentCandidateRow",
    "ContentOwnershipDecision",
    "FileHashAggregateCursor",
    "FileHashAggregatePage",
    "FileHashAggregate",
    "GalleryContentDigest",
    "GalleryAnalysisCursor",
    "GalleryAnalysisDecision",
    "GalleryAnalysisPage",
    "GalleryFileHashCursor",
    "GalleryFileHashPage",
    "GalleryFileHashRow",
    "GallerySourceFileCursor",
    "GallerySourceFilePage",
    "GallerySourceFileRow",
    "GallerySourceManifest",
    "GidCandidateCursor",
    "GidCandidatePage",
    "GidCandidateRow",
    "GidWinnerDecision",
    "StagedDeduplicationAdapter",
    "StagedDeduplicationPhase",
    "StagedDeduplicationPlanner",
    "StagedDeduplicationSummary",
]

import json
from collections.abc import Callable, Iterator, Sequence
from dataclasses import asdict, dataclass, is_dataclass
from enum import StrEnum
from hashlib import sha256
from itertools import chain, groupby
from typing import Any, Protocol, TypeVar, cast

from .deduplication import (
    SPAM_ARTIST_RATIO_THRESHOLD,
    SPAM_FILE_MINIMUM_OCCURRENCES,
    DeduplicationCandidate,
    select_content_owner,
    select_gid_winner,
)

_T = TypeVar("_T")


def _batch_id(phase: str, values: Sequence[object]) -> str:
    """Content-address a write so changed page boundaries cannot conflict."""

    payload = [
        asdict(cast(Any, value)) if is_dataclass(value) else value for value in values
    ]
    digest = sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return f"dedup:{phase}:{digest}"


def _validate_sha256(value: str, *, label: str) -> None:
    if len(value) != 64:
        raise ValueError(f"{label} must contain 64 hexadecimal characters")
    try:
        bytes.fromhex(value)
    except ValueError as error:
        raise ValueError(f"{label} is not hexadecimal") from error


def _validate_gallery_name(value: str) -> None:
    if not value:
        raise ValueError("gallery_name must not be blank")


def _validate_gallery_key(value: str) -> None:
    _validate_sha256(value, label="Gallery key")


@dataclass(frozen=True, slots=True)
class AnalysisScanCompletion:
    """Storage-neutral proof that an analysis input reached its terminal page."""

    after_value: str
    token_sha256: str

    def __post_init__(self) -> None:
        _validate_sha256(self.token_sha256, label="Analysis completion token")


@dataclass(frozen=True, order=True, slots=True)
class FileHashAggregateCursor:
    file_sha256: str


@dataclass(frozen=True, slots=True)
class FileHashAggregate:
    """Database aggregate needed to classify one repeated file hash."""

    file_sha256: str
    occurrence_count: int
    distinct_artist_count: int
    maximum_gallery_artist_count: int

    def __post_init__(self) -> None:
        _validate_sha256(self.file_sha256, label="File SHA-256")
        if self.occurrence_count <= 0:
            raise ValueError("occurrence_count must be positive")
        if (
            min(
                self.distinct_artist_count,
                self.maximum_gallery_artist_count,
            )
            < 0
        ):
            raise ValueError("artist counts must not be negative")
        if self.maximum_gallery_artist_count > self.distinct_artist_count:
            raise ValueError(
                "maximum gallery artist count cannot exceed distinct artists"
            )

    @property
    def cursor(self) -> FileHashAggregateCursor:
        return FileHashAggregateCursor(self.file_sha256)


@dataclass(frozen=True, slots=True)
class FileHashAggregatePage:
    items: tuple[FileHashAggregate, ...]
    completion: AnalysisScanCompletion | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "items", tuple(self.items))
        if self.items and self.completion is not None:
            raise ValueError("Only an empty terminal page can carry scan completion")


@dataclass(frozen=True, order=True, slots=True)
class GalleryFileHashCursor:
    gallery_key: str
    file_sha256: str
    file_key: str

    def __post_init__(self) -> None:
        _validate_gallery_key(self.gallery_key)


@dataclass(frozen=True, slots=True)
class GalleryFileHashRow:
    """One staged source file in stable gallery/hash/file-key order."""

    gallery_name: str
    gallery_key: str
    file_key: str
    file_name: str | None
    file_sha256: str
    excluded_as_spam: bool

    def __post_init__(self) -> None:
        _validate_gallery_name(self.gallery_name)
        _validate_gallery_key(self.gallery_key)
        if self.file_name is None:
            if self.file_sha256 or self.file_key or self.excluded_as_spam:
                raise ValueError(
                    "An empty-gallery sentinel must use empty digest and file key"
                )
        else:
            if not self.file_name:
                raise ValueError("file_name must not be blank")
            if not self.file_key:
                raise ValueError("file_key must not be blank")
            _validate_sha256(self.file_sha256, label="File SHA-256")

    @property
    def cursor(self) -> GalleryFileHashCursor:
        return GalleryFileHashCursor(
            self.gallery_key,
            self.file_sha256,
            self.file_key,
        )


@dataclass(frozen=True, slots=True)
class GalleryFileHashPage:
    items: tuple[GalleryFileHashRow, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "items", tuple(self.items))


@dataclass(frozen=True, order=True, slots=True)
class GallerySourceFileCursor:
    gallery_key: str
    file_sort_key: str
    file_name: str
    file_key: str

    def __post_init__(self) -> None:
        _validate_gallery_key(self.gallery_key)


@dataclass(frozen=True, slots=True)
class GallerySourceFileRow:
    """One source-manifest row in historical v1 filename order."""

    gallery_name: str
    gallery_key: str
    file_sort_key: str
    file_name: str | None
    file_key: str
    size_bytes: int
    file_sha256: str
    empty_gallery_metadata_sha256: str | None = None

    def __post_init__(self) -> None:
        _validate_gallery_name(self.gallery_name)
        _validate_gallery_key(self.gallery_key)
        if self.file_name is None:
            if any(
                (
                    self.file_sort_key,
                    self.file_key,
                    self.size_bytes,
                    self.file_sha256,
                )
            ):
                raise ValueError("An empty-gallery source sentinel must be empty")
            if self.empty_gallery_metadata_sha256 is None:
                raise ValueError(
                    "An empty-gallery source sentinel requires its metadata digest"
                )
            _validate_sha256(
                self.empty_gallery_metadata_sha256,
                label="Metadata SHA-256",
            )
        else:
            if not self.file_name or not self.file_key:
                raise ValueError("Source file name and key must not be blank")
            if self.file_sort_key != self.file_name.casefold():
                raise ValueError("file_sort_key must be the Python-casefolded name")
            if self.size_bytes < 0:
                raise ValueError("Source file size must not be negative")
            _validate_sha256(self.file_sha256, label="File SHA-256")
            if self.empty_gallery_metadata_sha256 is not None:
                raise ValueError("A regular source row cannot carry sentinel metadata")

    @property
    def cursor(self) -> GallerySourceFileCursor:
        return GallerySourceFileCursor(
            self.gallery_key,
            self.file_sort_key,
            self.file_name or "",
            self.file_key,
        )


@dataclass(frozen=True, slots=True)
class GallerySourceFilePage:
    items: tuple[GallerySourceFileRow, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "items", tuple(self.items))


@dataclass(frozen=True, slots=True)
class GallerySourceManifest:
    gallery_name: str
    source_manifest_sha256: str
    source_manifest_version: int = 1

    def __post_init__(self) -> None:
        _validate_gallery_name(self.gallery_name)
        _validate_sha256(
            self.source_manifest_sha256,
            label="Source manifest SHA-256",
        )
        if self.source_manifest_version != 1:
            raise ValueError("The staged canonical source manifest must use version 1")


@dataclass(frozen=True, slots=True)
class GalleryContentDigest:
    gallery_name: str
    content_sha256: str | None
    duplicate_hash_deletion_candidate: bool = False

    def __post_init__(self) -> None:
        _validate_gallery_name(self.gallery_name)
        if self.content_sha256 is not None:
            _validate_sha256(self.content_sha256, label="Content SHA-256")


@dataclass(frozen=True, order=True, slots=True)
class ContentCandidateCursor:
    content_sha256: str
    gallery_key: str

    def __post_init__(self) -> None:
        _validate_sha256(self.content_sha256, label="Content SHA-256")
        _validate_gallery_key(self.gallery_key)


@dataclass(frozen=True, slots=True)
class ContentCandidateRow:
    candidate: DeduplicationCandidate
    incumbent_gallery_name: str | None
    gallery_key: str

    def __post_init__(self) -> None:
        digest = self.candidate.content_digest
        if digest is None:
            raise ValueError("A content candidate must have a content digest")
        _validate_sha256(digest, label="Content SHA-256")
        _validate_gallery_key(self.gallery_key)

    @property
    def cursor(self) -> ContentCandidateCursor:
        digest = self.candidate.content_digest
        assert digest is not None
        return ContentCandidateCursor(digest, self.gallery_key)


@dataclass(frozen=True, slots=True)
class ContentCandidatePage:
    items: tuple[ContentCandidateRow, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "items", tuple(self.items))


@dataclass(frozen=True, slots=True)
class ContentOwnershipDecision:
    content_sha256: str
    owner_gallery_name: str

    def __post_init__(self) -> None:
        _validate_sha256(self.content_sha256, label="Content SHA-256")
        _validate_gallery_name(self.owner_gallery_name)


@dataclass(frozen=True, order=True, slots=True)
class GidCandidateCursor:
    gid: int
    gallery_key: str

    def __post_init__(self) -> None:
        if self.gid <= 0:
            raise ValueError("gid must be positive")
        _validate_gallery_key(self.gallery_key)


@dataclass(frozen=True, slots=True)
class GidCandidateRow:
    candidate: DeduplicationCandidate
    incumbent_gallery_name: str | None
    gallery_key: str

    def __post_init__(self) -> None:
        _validate_gallery_key(self.gallery_key)

    @property
    def cursor(self) -> GidCandidateCursor:
        return GidCandidateCursor(
            self.candidate.gid,
            self.gallery_key,
        )


@dataclass(frozen=True, slots=True)
class GidCandidatePage:
    items: tuple[GidCandidateRow, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "items", tuple(self.items))


@dataclass(frozen=True, slots=True)
class GidWinnerDecision:
    gid: int
    winner_gallery_name: str

    def __post_init__(self) -> None:
        if self.gid <= 0:
            raise ValueError("gid must be positive")
        _validate_gallery_name(self.winner_gallery_name)


@dataclass(frozen=True, order=True, slots=True)
class GalleryAnalysisCursor:
    gallery_key: str

    def __post_init__(self) -> None:
        _validate_gallery_key(self.gallery_key)


@dataclass(frozen=True, slots=True)
class GalleryAnalysisDecision:
    """Final core analysis values for one staged source gallery."""

    gallery_name: str
    gallery_key: str
    content_sha256: str | None
    selected: bool
    duplicate_of_gallery_name: str | None = None

    def __post_init__(self) -> None:
        _validate_gallery_name(self.gallery_name)
        _validate_gallery_key(self.gallery_key)
        if self.content_sha256 is not None:
            _validate_sha256(self.content_sha256, label="Content SHA-256")
        if self.duplicate_of_gallery_name is not None:
            _validate_gallery_name(self.duplicate_of_gallery_name)
            if self.duplicate_of_gallery_name == self.gallery_name:
                raise ValueError("A duplicate gallery cannot target itself")
            if self.content_sha256 is None:
                raise ValueError("A duplicate gallery must have a content digest")
            if self.selected:
                raise ValueError("A duplicate gallery cannot be selected")

    @property
    def cursor(self) -> GalleryAnalysisCursor:
        return GalleryAnalysisCursor(self.gallery_key)


@dataclass(frozen=True, slots=True)
class GalleryAnalysisPage:
    items: tuple[GalleryAnalysisDecision, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "items", tuple(self.items))


class StagedDeduplicationPhase(StrEnum):
    source_manifests = "SOURCE_MANIFESTS"
    file_spam = "FILE_SPAM"
    content_digests = "CONTENT_DIGESTS"
    content_owners = "CONTENT_OWNERS"
    gid_winners = "GID_WINNERS"
    final_analyses = "FINAL_ANALYSES"


class StagedDeduplicationAdapter(Protocol):
    """Durable page and batch operations required from the core adapter."""

    def is_deduplication_phase_complete(
        self,
        build_id: str,
        phase: StagedDeduplicationPhase,
    ) -> bool: ...

    def page_gallery_source_files(
        self,
        build_id: str,
        *,
        after: GallerySourceFileCursor | None,
        limit: int,
    ) -> GallerySourceFilePage: ...

    def stage_gallery_source_manifests(
        self,
        build_id: str,
        manifests: Sequence[GallerySourceManifest],
        *,
        batch_id: str,
    ) -> None: ...

    def page_file_hash_aggregates(
        self,
        build_id: str,
        *,
        after: FileHashAggregateCursor | None,
        limit: int,
    ) -> FileHashAggregatePage: ...

    def stage_excluded_file_hashes(
        self,
        build_id: str,
        hashes: Sequence[str],
        *,
        batch_id: str,
    ) -> None: ...

    def page_gallery_file_hashes(
        self,
        build_id: str,
        *,
        after: GalleryFileHashCursor | None,
        limit: int,
    ) -> GalleryFileHashPage: ...

    def stage_gallery_content_digests(
        self,
        build_id: str,
        digests: Sequence[GalleryContentDigest],
        *,
        batch_id: str,
    ) -> None: ...

    def page_content_candidates(
        self,
        build_id: str,
        *,
        after: ContentCandidateCursor | None,
        limit: int,
    ) -> ContentCandidatePage: ...

    def stage_content_owners(
        self,
        build_id: str,
        decisions: Sequence[ContentOwnershipDecision],
        *,
        batch_id: str,
    ) -> None: ...

    def page_gid_candidates(
        self,
        build_id: str,
        *,
        after: GidCandidateCursor | None,
        limit: int,
    ) -> GidCandidatePage: ...

    def stage_gid_winners(
        self,
        build_id: str,
        decisions: Sequence[GidWinnerDecision],
        *,
        batch_id: str,
    ) -> None: ...

    def page_final_gallery_analyses(
        self,
        build_id: str,
        *,
        after: GalleryAnalysisCursor | None,
        limit: int,
    ) -> GalleryAnalysisPage: ...

    def stage_final_gallery_analyses(
        self,
        build_id: str,
        decisions: Sequence[GalleryAnalysisDecision],
        *,
        batch_id: str,
    ) -> None: ...

    def complete_deduplication_phase(
        self,
        build_id: str,
        phase: StagedDeduplicationPhase,
        *,
        scan_completion: AnalysisScanCompletion | None = None,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class StagedDeduplicationSummary:
    gallery_source_manifests: int
    excluded_file_hashes: int
    gallery_content_digests: int
    content_groups: int
    gid_groups: int
    gallery_analyses: int


class StagedDeduplicationPlanner:
    """Run the legacy policy reducers over bounded, durable keyset pages."""

    def __init__(self, *, page_size: int = 1_000, write_batch_size: int = 1_000):
        if page_size <= 0:
            raise ValueError("page_size must be positive")
        if write_batch_size <= 0:
            raise ValueError("write_batch_size must be positive")
        self._page_size = page_size
        self._write_batch_size = write_batch_size

    def run(
        self,
        adapter: StagedDeduplicationAdapter,
        *,
        build_id: str,
    ) -> StagedDeduplicationSummary:
        if not build_id:
            raise ValueError("build_id must not be blank")
        manifested = self._run_phase(
            adapter,
            build_id,
            StagedDeduplicationPhase.source_manifests,
            self._derive_gallery_source_manifests,
        )
        excluded = self._run_phase(
            adapter,
            build_id,
            StagedDeduplicationPhase.file_spam,
            self._classify_spam_hashes,
        )
        digested = self._run_phase(
            adapter,
            build_id,
            StagedDeduplicationPhase.content_digests,
            self._derive_gallery_content_digests,
        )
        content_groups = self._run_phase(
            adapter,
            build_id,
            StagedDeduplicationPhase.content_owners,
            self._select_content_owners,
        )
        gid_groups = self._run_phase(
            adapter,
            build_id,
            StagedDeduplicationPhase.gid_winners,
            self._select_gid_winners,
        )
        gallery_analyses = self._run_phase(
            adapter,
            build_id,
            StagedDeduplicationPhase.final_analyses,
            self._stage_final_analyses,
        )
        return StagedDeduplicationSummary(
            gallery_source_manifests=manifested,
            excluded_file_hashes=excluded,
            gallery_content_digests=digested,
            content_groups=content_groups,
            gid_groups=gid_groups,
            gallery_analyses=gallery_analyses,
        )

    @staticmethod
    def _run_phase(
        adapter: StagedDeduplicationAdapter,
        build_id: str,
        phase: StagedDeduplicationPhase,
        operation: Callable[[StagedDeduplicationAdapter, str], int],
    ) -> int:
        if adapter.is_deduplication_phase_complete(build_id, phase):
            return 0
        return operation(adapter, build_id)

    def _derive_gallery_source_manifests(
        self,
        adapter: StagedDeduplicationAdapter,
        build_id: str,
    ) -> int:
        pending: list[GallerySourceManifest] = []
        gallery_count = 0
        rows = self._gallery_source_files(adapter, build_id)
        for _gallery_key, group in groupby(rows, key=lambda row: row.gallery_key):
            first_row = next(group)
            gallery_name = first_row.gallery_name
            source_hasher = sha256()
            source_hasher.update(b'{"files":[')
            metadata_sha256: str | None = None
            first = True
            for row in chain((first_row,), group):
                if row.gallery_name != gallery_name:
                    raise ValueError("A gallery key resolved to multiple gallery names")
                if row.file_name is None:
                    metadata_sha256 = row.empty_gallery_metadata_sha256
                    continue
                if not first:
                    source_hasher.update(b",")
                source_hasher.update(
                    json.dumps(
                        {
                            "name": row.file_name,
                            "size": row.size_bytes,
                            "sha256": row.file_sha256,
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode()
                )
                first = False
                if row.file_name == "galleryinfo.txt":
                    metadata_sha256 = row.file_sha256
            if metadata_sha256 is None:
                raise ValueError(
                    f"Gallery {gallery_name!r} has no galleryinfo.txt metadata row"
                )
            source_hasher.update(b'],"metadata":')
            source_hasher.update(json.dumps(metadata_sha256).encode())
            source_hasher.update(b',"version":1}')
            pending.append(
                GallerySourceManifest(gallery_name, source_hasher.hexdigest())
            )
            gallery_count += 1
            if len(pending) == self._write_batch_size:
                self._stage_source_manifest_batch(adapter, build_id, pending)
                pending.clear()
        if pending:
            self._stage_source_manifest_batch(adapter, build_id, pending)
        adapter.complete_deduplication_phase(
            build_id,
            StagedDeduplicationPhase.source_manifests,
        )
        return gallery_count

    @staticmethod
    def _stage_source_manifest_batch(
        adapter: StagedDeduplicationAdapter,
        build_id: str,
        values: Sequence[GallerySourceManifest],
    ) -> None:
        adapter.stage_gallery_source_manifests(
            build_id,
            tuple(values),
            batch_id=_batch_id("source-manifests", values),
        )

    def _classify_spam_hashes(
        self,
        adapter: StagedDeduplicationAdapter,
        build_id: str,
    ) -> int:
        excluded: list[str] = []
        excluded_count = 0
        after: FileHashAggregateCursor | None = None
        completion: AnalysisScanCompletion | None = None
        while True:
            page = adapter.page_file_hash_aggregates(
                build_id,
                after=after,
                limit=self._page_size,
            )
            if not page.items:
                completion = page.completion
                break
            if page.completion is not None:
                raise ValueError(
                    "A non-terminal file-hash page carried a completion token"
                )
            self._validate_page(
                tuple(item.cursor for item in page.items),
                after=after,
                label="file-hash aggregate",
            )
            for aggregate in page.items:
                spam = (
                    aggregate.occurrence_count >= SPAM_FILE_MINIMUM_OCCURRENCES
                    and aggregate.maximum_gallery_artist_count > 0
                    and aggregate.distinct_artist_count
                    > SPAM_ARTIST_RATIO_THRESHOLD
                    * aggregate.maximum_gallery_artist_count
                )
                if not spam:
                    continue
                excluded.append(aggregate.file_sha256)
                excluded_count += 1
                if len(excluded) == self._write_batch_size:
                    self._stage_excluded_batch(
                        adapter,
                        build_id,
                        excluded,
                    )
                    excluded.clear()
            after = page.items[-1].cursor
        if excluded:
            self._stage_excluded_batch(
                adapter,
                build_id,
                excluded,
            )
        if completion is None:
            raise ValueError(
                "The file-hash aggregate stream ended without a completion token"
            )
        adapter.complete_deduplication_phase(
            build_id,
            StagedDeduplicationPhase.file_spam,
            scan_completion=completion,
        )
        return excluded_count

    @staticmethod
    def _stage_excluded_batch(
        adapter: StagedDeduplicationAdapter,
        build_id: str,
        values: Sequence[str],
    ) -> None:
        adapter.stage_excluded_file_hashes(
            build_id,
            tuple(values),
            batch_id=_batch_id("file-spam", values),
        )

    def _derive_gallery_content_digests(
        self,
        adapter: StagedDeduplicationAdapter,
        build_id: str,
    ) -> int:
        pending: list[GalleryContentDigest] = []
        gallery_count = 0
        rows = self._gallery_file_hashes(adapter, build_id)
        for _gallery_key, group in groupby(rows, key=lambda row: row.gallery_key):
            first_row = next(group)
            gallery_name = first_row.gallery_name
            content_hasher = sha256()
            has_content = False
            file_count = 0
            duplicate_hash_groups = 0
            current_file_sha256: str | None = None
            current_hash_occurrences = 0
            for row in chain((first_row,), group):
                if row.gallery_name != gallery_name:
                    raise ValueError("A gallery key resolved to multiple gallery names")
                if row.file_name is not None:
                    file_count += 1
                    if row.file_sha256 == current_file_sha256:
                        current_hash_occurrences += 1
                    else:
                        if current_hash_occurrences > 1:
                            duplicate_hash_groups += 1
                        current_file_sha256 = row.file_sha256
                        current_hash_occurrences = 1
                if (
                    row.file_name is None
                    or row.file_name == "galleryinfo.txt"
                    or row.excluded_as_spam
                ):
                    continue
                content_hasher.update(bytes.fromhex(row.file_sha256))
                has_content = True
            if current_hash_occurrences > 1:
                duplicate_hash_groups += 1
            digest = content_hasher.hexdigest() if has_content else None
            # Preserve the historical deletion-candidate rule without another
            # corpus pass.  SQL previously selected a gallery when
            # duplicate_groups / (files_count - duplicate_groups) > 0.9.
            # Integer cross-multiplication avoids float/backend differences;
            # the first guard also preserves SQL's false result for division
            # by zero.
            duplicate_hash_deletion_candidate = (
                file_count > duplicate_hash_groups
                and 10 * duplicate_hash_groups
                > 9 * (file_count - duplicate_hash_groups)
            )
            pending.append(
                GalleryContentDigest(
                    gallery_name,
                    digest,
                    duplicate_hash_deletion_candidate,
                )
            )
            gallery_count += 1
            if len(pending) == self._write_batch_size:
                self._stage_content_digest_batch(
                    adapter,
                    build_id,
                    pending,
                )
                pending.clear()
        if pending:
            self._stage_content_digest_batch(
                adapter,
                build_id,
                pending,
            )
        adapter.complete_deduplication_phase(
            build_id,
            StagedDeduplicationPhase.content_digests,
        )
        return gallery_count

    @staticmethod
    def _stage_content_digest_batch(
        adapter: StagedDeduplicationAdapter,
        build_id: str,
        values: Sequence[GalleryContentDigest],
    ) -> None:
        adapter.stage_gallery_content_digests(
            build_id,
            tuple(values),
            batch_id=_batch_id("content-digests", values),
        )

    def _select_content_owners(
        self,
        adapter: StagedDeduplicationAdapter,
        build_id: str,
    ) -> int:
        pending: list[ContentOwnershipDecision] = []
        group_count = 0
        rows = self._content_candidates(adapter, build_id)
        for digest, group in groupby(
            rows,
            key=lambda row: row.candidate.content_digest,
        ):
            assert digest is not None
            first = next(group)
            incumbent = first.incumbent_gallery_name

            def candidates() -> Iterator[DeduplicationCandidate]:
                yield first.candidate
                for row in group:
                    if row.incumbent_gallery_name != incumbent:
                        raise ValueError(
                            "Content candidates disagree about the active incumbent"
                        )
                    yield row.candidate

            owner = select_content_owner(
                candidates(),
                incumbent_gallery_name=incumbent,
            )
            pending.append(ContentOwnershipDecision(digest, owner.gallery_name))
            group_count += 1
            if len(pending) == self._write_batch_size:
                self._stage_content_owner_batch(
                    adapter,
                    build_id,
                    pending,
                )
                pending.clear()
        if pending:
            self._stage_content_owner_batch(
                adapter,
                build_id,
                pending,
            )
        adapter.complete_deduplication_phase(
            build_id,
            StagedDeduplicationPhase.content_owners,
        )
        return group_count

    @staticmethod
    def _stage_content_owner_batch(
        adapter: StagedDeduplicationAdapter,
        build_id: str,
        values: Sequence[ContentOwnershipDecision],
    ) -> None:
        adapter.stage_content_owners(
            build_id,
            tuple(values),
            batch_id=_batch_id("content-owners", values),
        )

    def _select_gid_winners(
        self,
        adapter: StagedDeduplicationAdapter,
        build_id: str,
    ) -> int:
        pending: list[GidWinnerDecision] = []
        group_count = 0
        rows = self._gid_candidates(adapter, build_id)
        for gid, group in groupby(rows, key=lambda row: row.candidate.gid):
            first = next(group)
            incumbent = first.incumbent_gallery_name

            def candidates() -> Iterator[DeduplicationCandidate]:
                yield first.candidate
                for row in group:
                    if row.incumbent_gallery_name != incumbent:
                        raise ValueError(
                            "GID candidates disagree about the active incumbent"
                        )
                    yield row.candidate

            winner = select_gid_winner(
                candidates(),
                incumbent_gallery_name=incumbent,
            )
            pending.append(GidWinnerDecision(gid, winner.gallery_name))
            group_count += 1
            if len(pending) == self._write_batch_size:
                self._stage_gid_winner_batch(
                    adapter,
                    build_id,
                    pending,
                )
                pending.clear()
        if pending:
            self._stage_gid_winner_batch(
                adapter,
                build_id,
                pending,
            )
        adapter.complete_deduplication_phase(
            build_id,
            StagedDeduplicationPhase.gid_winners,
        )
        return group_count

    def _stage_final_analyses(
        self,
        adapter: StagedDeduplicationAdapter,
        build_id: str,
    ) -> int:
        pending: list[GalleryAnalysisDecision] = []
        gallery_count = 0
        for decision in self._final_gallery_analyses(adapter, build_id):
            pending.append(decision)
            gallery_count += 1
            if len(pending) == self._write_batch_size:
                self._stage_final_analysis_batch(
                    adapter,
                    build_id,
                    pending,
                )
                pending.clear()
        if pending:
            self._stage_final_analysis_batch(
                adapter,
                build_id,
                pending,
            )
        adapter.complete_deduplication_phase(
            build_id,
            StagedDeduplicationPhase.final_analyses,
        )
        return gallery_count

    @staticmethod
    def _stage_final_analysis_batch(
        adapter: StagedDeduplicationAdapter,
        build_id: str,
        values: Sequence[GalleryAnalysisDecision],
    ) -> None:
        adapter.stage_final_gallery_analyses(
            build_id,
            tuple(values),
            batch_id=_batch_id("final-analyses", values),
        )

    @staticmethod
    def _stage_gid_winner_batch(
        adapter: StagedDeduplicationAdapter,
        build_id: str,
        values: Sequence[GidWinnerDecision],
    ) -> None:
        adapter.stage_gid_winners(
            build_id,
            tuple(values),
            batch_id=_batch_id("gid-winners", values),
        )

    def _gallery_source_files(
        self,
        adapter: StagedDeduplicationAdapter,
        build_id: str,
    ) -> Iterator[GallerySourceFileRow]:
        after: GallerySourceFileCursor | None = None
        while True:
            page = adapter.page_gallery_source_files(
                build_id,
                after=after,
                limit=self._page_size,
            )
            if not page.items:
                return
            self._validate_page(
                tuple(item.cursor for item in page.items),
                after=after,
                label="gallery source file",
            )
            yield from page.items
            after = page.items[-1].cursor

    def _gallery_file_hashes(
        self,
        adapter: StagedDeduplicationAdapter,
        build_id: str,
    ) -> Iterator[GalleryFileHashRow]:
        after: GalleryFileHashCursor | None = None
        while True:
            page = adapter.page_gallery_file_hashes(
                build_id,
                after=after,
                limit=self._page_size,
            )
            if not page.items:
                return
            self._validate_page(
                tuple(item.cursor for item in page.items),
                after=after,
                label="gallery file hash",
            )
            yield from page.items
            after = page.items[-1].cursor

    def _content_candidates(
        self,
        adapter: StagedDeduplicationAdapter,
        build_id: str,
    ) -> Iterator[ContentCandidateRow]:
        after: ContentCandidateCursor | None = None
        while True:
            page = adapter.page_content_candidates(
                build_id,
                after=after,
                limit=self._page_size,
            )
            if not page.items:
                return
            self._validate_page(
                tuple(item.cursor for item in page.items),
                after=after,
                label="content candidate",
            )
            yield from page.items
            after = page.items[-1].cursor

    def _gid_candidates(
        self,
        adapter: StagedDeduplicationAdapter,
        build_id: str,
    ) -> Iterator[GidCandidateRow]:
        after: GidCandidateCursor | None = None
        while True:
            page = adapter.page_gid_candidates(
                build_id,
                after=after,
                limit=self._page_size,
            )
            if not page.items:
                return
            self._validate_page(
                tuple(item.cursor for item in page.items),
                after=after,
                label="GID candidate",
            )
            yield from page.items
            after = page.items[-1].cursor

    def _final_gallery_analyses(
        self,
        adapter: StagedDeduplicationAdapter,
        build_id: str,
    ) -> Iterator[GalleryAnalysisDecision]:
        after: GalleryAnalysisCursor | None = None
        while True:
            page = adapter.page_final_gallery_analyses(
                build_id,
                after=after,
                limit=self._page_size,
            )
            if not page.items:
                return
            self._validate_page(
                tuple(item.cursor for item in page.items),
                after=after,
                label="final gallery analysis",
            )
            yield from page.items
            after = page.items[-1].cursor

    def _validate_page(
        self,
        cursors: tuple[_T, ...],
        *,
        after: _T | None,
        label: str,
    ) -> None:
        if len(cursors) > self._page_size:
            raise ValueError(f"{label} page exceeded the requested limit")
        previous = after
        for cursor in cursors:
            if previous is not None and cursor <= previous:  # type: ignore[operator]
                raise ValueError(
                    f"{label} page did not advance its keyset cursor strictly"
                )
            previous = cursor
