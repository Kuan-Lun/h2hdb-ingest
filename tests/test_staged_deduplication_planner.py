import json
from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from pathlib import Path

from h2hdb_ingest.deduplication import (
    SPAM_FILE_MINIMUM_OCCURRENCES,
    DeduplicationCandidate,
    DeduplicationPolicy,
)
from h2hdb_ingest.models import ScannedFile, ScannedGallery
from h2hdb_ingest.staged_deduplication import (
    ContentCandidateCursor,
    ContentCandidatePage,
    ContentCandidateRow,
    ContentOwnershipDecision,
    FileHashAggregate,
    FileHashAggregatePage,
    GalleryAnalysisCursor,
    GalleryAnalysisDecision,
    GalleryAnalysisPage,
    GalleryContentDigest,
    GalleryFileHashCursor,
    GalleryFileHashPage,
    GalleryFileHashRow,
    GallerySourceFileCursor,
    GallerySourceFilePage,
    GallerySourceFileRow,
    GallerySourceManifest,
    GidCandidateCursor,
    GidCandidatePage,
    GidCandidateRow,
    GidWinnerDecision,
    StagedDeduplicationPhase,
    StagedDeduplicationPlanner,
)


def _digest(label: str) -> str:
    return sha256(label.encode()).hexdigest()


def _gallery_key(name: str) -> str:
    return _digest(f"gallery-key:{name}")


@dataclass(frozen=True, slots=True)
class _Gallery:
    name: str
    gid: int
    title: str
    download_time: datetime
    artists: frozenset[str]
    files: tuple[tuple[str, str], ...]
    already_uploaded: bool = False


class _MemoryAdapter:
    """Small relational stand-in; every public read is a keyset page."""

    def __init__(
        self,
        galleries: Sequence[_Gallery],
        *,
        content_incumbents: dict[str, str],
        gid_incumbents: dict[int, str],
    ) -> None:
        self._galleries = tuple(galleries)
        self._content_incumbents = content_incumbents
        self._gid_incumbents = gid_incumbents
        self.excluded_hashes: set[str] = set()
        self.source_manifests: dict[str, GallerySourceManifest] = {}
        self.content_digests: dict[str, str | None] = {}
        self.duplicate_hash_deletion_candidates: dict[str, bool] = {}
        self.content_owners: dict[str, str] = {}
        self.gid_winners: dict[int, str] = {}
        self.final_analyses: dict[str, GalleryAnalysisDecision] = {}
        self.completed_phases: list[StagedDeduplicationPhase] = []
        self.page_calls: Counter[str] = Counter()
        self.write_batch_sizes: list[int] = []
        self.batch_ids: list[str] = []
        self._spam_after = ""
        self._spam_generation = 1

    @staticmethod
    def _check_build(build_id: str) -> None:
        assert build_id == "build-for-test"

    def _file_hash_aggregates(self) -> tuple[FileHashAggregate, ...]:
        occurrences: Counter[str] = Counter()
        galleries_by_hash: dict[str, set[str]] = defaultdict(set)
        gallery_by_name = {gallery.name: gallery for gallery in self._galleries}
        for gallery in self._galleries:
            for file_name, digest in gallery.files:
                if file_name == "galleryinfo.txt":
                    continue
                occurrences[digest] += 1
                galleries_by_hash[digest].add(gallery.name)
        aggregates = []
        for digest, occurrence_count in occurrences.items():
            if occurrence_count < SPAM_FILE_MINIMUM_OCCURRENCES:
                continue
            gallery_names = galleries_by_hash[digest]
            artists = {
                artist
                for gallery_name in gallery_names
                for artist in gallery_by_name[gallery_name].artists
            }
            maximum = max(
                (len(gallery_by_name[name].artists) for name in gallery_names),
                default=0,
            )
            aggregates.append(
                FileHashAggregate(
                    digest,
                    occurrence_count,
                    len(artists),
                    maximum,
                    SPAM_FILE_MINIMUM_OCCURRENCES,
                )
            )
        return tuple(sorted(aggregates, key=lambda item: item.file_sha256))

    def _gallery_source_files(self) -> tuple[GallerySourceFileRow, ...]:
        rows: list[GallerySourceFileRow] = []
        for gallery in self._galleries:
            if not gallery.files:
                rows.append(
                    GallerySourceFileRow(
                        gallery_name=gallery.name,
                        gallery_key=_gallery_key(gallery.name),
                        file_sort_key="",
                        file_name=None,
                        file_key="",
                        size_bytes=0,
                        file_sha256="",
                        empty_gallery_metadata_sha256=_digest(
                            f"metadata:{gallery.name}"
                        ),
                    )
                )
                continue
            rows.extend(
                GallerySourceFileRow(
                    gallery_name=gallery.name,
                    gallery_key=_gallery_key(gallery.name),
                    file_sort_key=file_name.casefold(),
                    file_name=file_name,
                    file_key=_digest(f"file-key:{gallery.name}:{position}:{file_name}"),
                    size_bytes=1,
                    file_sha256=digest,
                )
                for position, (file_name, digest) in enumerate(gallery.files)
            )
        return tuple(sorted(rows, key=lambda item: item.cursor))

    def page_gallery_source_files(
        self,
        build_id: str,
        *,
        after: GallerySourceFileCursor | None,
        limit: int,
    ) -> GallerySourceFilePage:
        self._check_build(build_id)
        self.page_calls["source_files"] += 1
        values = tuple(
            item
            for item in self._gallery_source_files()
            if after is None or item.cursor > after
        )[:limit]
        return GallerySourceFilePage(values)

    def stage_gallery_source_manifests(
        self,
        build_id: str,
        manifests: Sequence[GallerySourceManifest],
        *,
        batch_id: str,
    ) -> None:
        self._check_build(build_id)
        self._record_batch(manifests, batch_id)
        self.source_manifests.update(
            (manifest.gallery_name, manifest) for manifest in manifests
        )

    def get_file_spam_page(
        self,
        build_id: str,
        *,
        minimum_occurrences: int,
        limit: int,
    ) -> FileHashAggregatePage:
        self._check_build(build_id)
        assert minimum_occurrences == SPAM_FILE_MINIMUM_OCCURRENCES
        self.page_calls["spam"] += 1
        values = tuple(
            item
            for item in self._file_hash_aggregates()
            if item.file_sha256 > self._spam_after
        )[:limit]
        next_cursor = values[-1].file_sha256 if values else self._spam_after
        return FileHashAggregatePage(
            items=values,
            minimum_occurrences=minimum_occurrences,
            checkpoint_generation=self._spam_generation,
            start_cursor_sha256=self._spam_after,
            next_cursor_sha256=next_cursor,
            input_sha256=_digest(
                json.dumps(
                    [item.file_sha256 for item in values],
                    separators=(",", ":"),
                )
            ),
            page_limit=limit,
        )

    def apply_file_spam_page(
        self,
        build_id: str,
        page: FileHashAggregatePage,
        hashes: Sequence[str],
    ) -> None:
        self._check_build(build_id)
        assert page.checkpoint_generation == self._spam_generation
        assert page.start_cursor_sha256 == self._spam_after
        assert set(hashes) <= {item.file_sha256 for item in page.items}
        self.excluded_hashes.update(hashes)
        self._spam_after = page.next_cursor_sha256
        self._spam_generation += 1

    def _gallery_file_hashes(self) -> tuple[GalleryFileHashRow, ...]:
        rows: list[GalleryFileHashRow] = []
        for gallery in self._galleries:
            if not gallery.files:
                rows.append(
                    GalleryFileHashRow(
                        gallery_name=gallery.name,
                        gallery_key=_gallery_key(gallery.name),
                        file_key="",
                        file_sha256="",
                        metadata_file=False,
                        excluded_as_spam=False,
                    )
                )
                continue
            rows.extend(
                GalleryFileHashRow(
                    gallery_name=gallery.name,
                    gallery_key=_gallery_key(gallery.name),
                    file_key=_digest(file_name),
                    file_sha256=digest,
                    metadata_file=file_name == "galleryinfo.txt",
                    excluded_as_spam=digest in self.excluded_hashes,
                )
                for file_name, digest in gallery.files
            )
        return tuple(sorted(rows, key=lambda item: item.cursor))

    def page_gallery_file_hashes(
        self,
        build_id: str,
        *,
        after: GalleryFileHashCursor | None,
        limit: int,
    ) -> GalleryFileHashPage:
        self._check_build(build_id)
        self.page_calls["files"] += 1
        values = tuple(
            item
            for item in self._gallery_file_hashes()
            if after is None or item.cursor > after
        )[:limit]
        return GalleryFileHashPage(values)

    def stage_gallery_content_digests(
        self,
        build_id: str,
        digests: Sequence[GalleryContentDigest],
        *,
        batch_id: str,
    ) -> None:
        self._check_build(build_id)
        self._record_batch(digests, batch_id)
        self.content_digests.update(
            (value.gallery_name, value.content_sha256) for value in digests
        )
        self.duplicate_hash_deletion_candidates.update(
            (
                value.gallery_name,
                value.duplicate_hash_deletion_candidate,
            )
            for value in digests
        )

    def _candidate(self, gallery: _Gallery) -> DeduplicationCandidate:
        return DeduplicationCandidate(
            gallery_name=gallery.name,
            gid=gallery.gid,
            title=gallery.title,
            download_time=gallery.download_time,
            content_digest=self.content_digests[gallery.name],
            already_uploaded=gallery.already_uploaded,
        )

    def page_content_candidates(
        self,
        build_id: str,
        *,
        after: ContentCandidateCursor | None,
        limit: int,
    ) -> ContentCandidatePage:
        self._check_build(build_id)
        self.page_calls["content"] += 1
        rows = tuple(
            sorted(
                (
                    ContentCandidateRow(
                        self._candidate(gallery),
                        self._content_incumbents.get(
                            self.content_digests[gallery.name] or ""
                        ),
                        _gallery_key(gallery.name),
                    )
                    for gallery in self._galleries
                    if self.content_digests[gallery.name] is not None
                ),
                key=lambda item: item.cursor,
            )
        )
        values = tuple(item for item in rows if after is None or item.cursor > after)[
            :limit
        ]
        return ContentCandidatePage(values)

    def stage_content_owners(
        self,
        build_id: str,
        decisions: Sequence[ContentOwnershipDecision],
        *,
        batch_id: str,
    ) -> None:
        self._check_build(build_id)
        self._record_batch(decisions, batch_id)
        self.content_owners.update(
            (decision.content_sha256, decision.owner_gallery_name)
            for decision in decisions
        )

    def page_gid_candidates(
        self,
        build_id: str,
        *,
        after: GidCandidateCursor | None,
        limit: int,
    ) -> GidCandidatePage:
        self._check_build(build_id)
        self.page_calls["gid"] += 1
        rows = tuple(
            sorted(
                (
                    GidCandidateRow(
                        self._candidate(gallery),
                        self._gid_incumbents.get(gallery.gid),
                        _gallery_key(gallery.name),
                    )
                    for gallery in self._galleries
                    if self._is_content_winner(gallery)
                ),
                key=lambda item: item.cursor,
            )
        )
        values = tuple(item for item in rows if after is None or item.cursor > after)[
            :limit
        ]
        return GidCandidatePage(values)

    def _is_content_winner(self, gallery: _Gallery) -> bool:
        digest = self.content_digests[gallery.name]
        return digest is None or self.content_owners[digest] == gallery.name

    def stage_gid_winners(
        self,
        build_id: str,
        decisions: Sequence[GidWinnerDecision],
        *,
        batch_id: str,
    ) -> None:
        self._check_build(build_id)
        self._record_batch(decisions, batch_id)
        self.gid_winners.update(
            (decision.gid, decision.winner_gallery_name) for decision in decisions
        )

    def _analysis(self, gallery: _Gallery) -> GalleryAnalysisDecision:
        digest = self.content_digests[gallery.name]
        owner = None if digest is None else self.content_owners[digest]
        duplicate_of = owner if owner is not None and owner != gallery.name else None
        content_winner = owner is None or owner == gallery.name
        selected = content_winner and self.gid_winners[gallery.gid] == gallery.name
        return GalleryAnalysisDecision(
            gallery_name=gallery.name,
            gallery_key=_gallery_key(gallery.name),
            content_sha256=digest,
            selected=selected,
            duplicate_of_gallery_name=duplicate_of,
        )

    def page_final_gallery_analyses(
        self,
        build_id: str,
        *,
        after: GalleryAnalysisCursor | None,
        limit: int,
    ) -> GalleryAnalysisPage:
        self._check_build(build_id)
        self.page_calls["final"] += 1
        rows = tuple(
            sorted(
                (self._analysis(gallery) for gallery in self._galleries),
                key=lambda item: item.cursor,
            )
        )
        values = tuple(item for item in rows if after is None or item.cursor > after)[
            :limit
        ]
        return GalleryAnalysisPage(values)

    def stage_final_gallery_analyses(
        self,
        build_id: str,
        decisions: Sequence[GalleryAnalysisDecision],
        *,
        batch_id: str,
    ) -> None:
        self._check_build(build_id)
        self._record_batch(decisions, batch_id)
        self.final_analyses.update(
            (decision.gallery_name, decision) for decision in decisions
        )

    def complete_deduplication_phase(
        self,
        build_id: str,
        phase: StagedDeduplicationPhase,
    ) -> None:
        self._check_build(build_id)
        self.completed_phases.append(phase)

    def is_deduplication_phase_complete(
        self,
        build_id: str,
        phase: StagedDeduplicationPhase,
    ) -> bool:
        self._check_build(build_id)
        return phase in self.completed_phases

    def _record_batch(self, values: Sequence[object], batch_id: str) -> None:
        self.write_batch_sizes.append(len(values))
        self.batch_ids.append(batch_id)


def _fixtures() -> tuple[tuple[_Gallery, ...], str, str]:
    metadata = _digest("metadata")
    spam = _digest("spam")
    first = _digest("first")
    second = _digest("second")
    downloaded = datetime(2025, 1, 1)
    galleries = (
        _Gallery(
            "dup_a",
            101,
            "same",
            downloaded,
            frozenset({"artist-a"}),
            (
                ("galleryinfo.txt", metadata),
                ("001.jpg", second),
                ("002.jpg", first),
                ("003.jpg", first),
                ("spam-a.bin", spam),
            ),
        ),
        _Gallery(
            "dup_b",
            102,
            "same",
            downloaded,
            frozenset({"artist-b"}),
            (
                ("galleryinfo.txt", metadata),
                ("001.jpg", first),
                ("002.jpg", second),
                ("003.jpg", first),
                ("spam-b.bin", spam),
            ),
        ),
        _Gallery(
            "empty_after_spam",
            700,
            "empty",
            downloaded,
            frozenset({"artist-c"}),
            (("galleryinfo.txt", metadata), ("spam-c.bin", spam)),
        ),
        _Gallery(
            "gid_a",
            500,
            "same",
            downloaded,
            frozenset({"artist-d"}),
            (("galleryinfo.txt", metadata), ("001.jpg", _digest("gid-a"))),
        ),
        _Gallery(
            "gid_b",
            500,
            "same",
            downloaded,
            frozenset({"artist-e"}),
            (("galleryinfo.txt", metadata), ("001.jpg", _digest("gid-b"))),
        ),
        _Gallery(
            "zero_files",
            800,
            "zero",
            downloaded,
            frozenset(),
            (),
        ),
    )
    duplicate_digest = sha256(
        b"".join(
            sorted(
                (
                    bytes.fromhex(first),
                    bytes.fromhex(first),
                    bytes.fromhex(second),
                )
            )
        )
    ).hexdigest()
    return galleries, spam, duplicate_digest


def _legacy_gallery(gallery: _Gallery) -> ScannedGallery:
    tags = tuple(("artist", artist) for artist in sorted(gallery.artists))
    if gallery.already_uploaded:
        tags += (("status", "already uploaded"),)
    return ScannedGallery(
        folder=Path("/source") / gallery.name,
        gallery_name=gallery.name,
        gid=gallery.gid,
        title=gallery.title,
        summary="",
        upload_account="account",
        upload_time=gallery.download_time,
        download_time=gallery.download_time,
        modified_time=gallery.download_time,
        pages=len(gallery.files),
        tags=tags,
        files=tuple(
            ScannedFile(
                path=Path("/source") / gallery.name / file_name,
                name=file_name,
                size_bytes=1,
                sha256=digest,
            )
            for file_name, digest in gallery.files
        ),
        metadata_sha256=_digest(f"metadata:{gallery.name}"),
        source_digest=_digest(f"source:{gallery.name}"),
        content_digest=None,
    )


def test_staged_planner_matches_legacy_policy_across_one_row_pages() -> None:
    galleries, spam, duplicate_digest = _fixtures()
    adapter = _MemoryAdapter(
        galleries,
        content_incumbents={duplicate_digest: "dup_a"},
        gid_incumbents={500: "gid_a"},
    )

    summary = StagedDeduplicationPlanner(page_size=1, write_batch_size=2).run(
        adapter,
        build_id="build-for-test",
    )

    legacy = DeduplicationPolicy().select(
        tuple(_legacy_gallery(gallery) for gallery in galleries),
        incumbent_gallery_name_by_content_sha256={duplicate_digest: "dup_a"},
        incumbent_gallery_name_by_gid={500: "gid_a"},
    )
    legacy_content = {
        gallery.gallery_name: gallery.content_digest
        for gallery in legacy.canonical_galleries
    }
    legacy_duplicates = dict(legacy.duplicate_of_by_gallery_name)
    legacy_winners = {gallery.gallery_name for gallery in legacy.winners}

    assert adapter.excluded_hashes == set(legacy.excluded_file_sha256s) == {spam}
    for gallery in galleries:
        files = sorted(gallery.files, key=lambda item: (item[0].casefold(), item[0]))
        metadata_sha256 = next(
            (digest for file_name, digest in files if file_name == "galleryinfo.txt"),
            _digest(f"metadata:{gallery.name}"),
        )
        expected_source_digest = sha256(
            json.dumps(
                {
                    "version": 1,
                    "metadata": metadata_sha256,
                    "files": [
                        {"name": name, "size": 1, "sha256": digest}
                        for name, digest in files
                    ],
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        assert (
            adapter.source_manifests[gallery.name].source_manifest_sha256
            == expected_source_digest
        )
    assert adapter.content_digests == legacy_content
    assert (
        {
            name: decision.duplicate_of_gallery_name
            for name, decision in adapter.final_analyses.items()
            if decision.duplicate_of_gallery_name is not None
        }
        == legacy_duplicates
        == {"dup_b": "dup_a"}
    )
    assert (
        {name for name, decision in adapter.final_analyses.items() if decision.selected}
        == legacy_winners
        == {"dup_a", "empty_after_spam", "gid_a", "zero_files"}
    )
    assert adapter.final_analyses["gid_b"].duplicate_of_gallery_name is None
    assert adapter.final_analyses["gid_b"].selected is False
    assert adapter.final_analyses["empty_after_spam"].content_sha256 is None
    assert adapter.final_analyses["zero_files"].content_sha256 is None

    digest_without_duplicate = sha256(
        b"".join(
            sorted(
                (
                    bytes.fromhex(_digest("first")),
                    bytes.fromhex(_digest("second")),
                )
            )
        )
    ).hexdigest()
    assert adapter.content_digests["dup_a"] == duplicate_digest
    assert adapter.content_digests["dup_a"] != digest_without_duplicate
    assert adapter.content_owners[duplicate_digest] == "dup_a"
    assert adapter.gid_winners[500] == "gid_a"
    assert summary.gallery_analyses == len(galleries)


def test_duplicate_hash_deletion_ratio_preserves_strict_legacy_threshold() -> None:
    downloaded = datetime(2025, 1, 2, 3, 4, 5)

    def files(label: str, duplicate_groups: int) -> tuple[tuple[str, str], ...]:
        values: list[tuple[str, str]] = [
            ("galleryinfo.txt", _digest(f"metadata:{label}"))
        ]
        for index in range(duplicate_groups):
            digest = _digest(f"duplicate:{label}:{index}")
            values.extend(
                (
                    (f"{index:03d}-a.jpg", digest),
                    (f"{index:03d}-b.jpg", digest),
                )
            )
        return tuple(values)

    galleries = (
        # 9 duplicate groups / (19 files - 9 groups) == 0.9: not selected.
        _Gallery(
            "ratio_equal",
            901,
            "equal",
            downloaded,
            frozenset(),
            files("equal", 9),
        ),
        # 10 / (21 - 10) > 0.9: selected by the historical rule.
        _Gallery(
            "ratio_above",
            902,
            "above",
            downloaded,
            frozenset(),
            files("above", 10),
        ),
    )
    adapter = _MemoryAdapter(
        galleries,
        content_incumbents={},
        gid_incumbents={},
    )

    StagedDeduplicationPlanner(page_size=1, write_batch_size=1).run(
        adapter,
        build_id="build-for-test",
    )

    assert adapter.duplicate_hash_deletion_candidates == {
        "ratio_equal": False,
        "ratio_above": True,
    }


def test_staged_planner_bounds_every_page_and_write_batch() -> None:
    galleries, _spam, duplicate_digest = _fixtures()
    adapter = _MemoryAdapter(
        galleries,
        content_incumbents={duplicate_digest: "dup_a"},
        gid_incumbents={500: "gid_a"},
    )

    summary = StagedDeduplicationPlanner(page_size=2, write_batch_size=2).run(
        adapter,
        build_id="build-for-test",
    )

    assert max(adapter.write_batch_sizes) <= 2
    assert all(count > 1 for count in adapter.page_calls.values())
    assert len(adapter.batch_ids) == len(set(adapter.batch_ids))
    assert adapter.completed_phases == list(StagedDeduplicationPhase)
    assert summary.excluded_file_hashes == 1
    assert summary.gallery_source_manifests == len(galleries)
    assert summary.gallery_content_digests == len(galleries)
    assert summary.content_groups == 3
    assert summary.gid_groups == 4
    assert summary.gallery_analyses == len(galleries)


def test_completed_phases_resume_without_replaying_pages_or_writes() -> None:
    galleries, _spam, duplicate_digest = _fixtures()
    adapter = _MemoryAdapter(
        galleries,
        content_incumbents={duplicate_digest: "dup_a"},
        gid_incumbents={500: "gid_a"},
    )
    planner = StagedDeduplicationPlanner(page_size=1, write_batch_size=2)
    planner.run(adapter, build_id="build-for-test")
    pages_before = adapter.page_calls.copy()
    batches_before = tuple(adapter.batch_ids)

    resumed = planner.run(adapter, build_id="build-for-test")

    assert adapter.page_calls == pages_before
    assert tuple(adapter.batch_ids) == batches_before
    assert resumed == type(resumed)(0, 0, 0, 0, 0, 0)


def test_file_spam_applies_each_server_page_before_requesting_the_next() -> None:
    galleries, _spam, duplicate_digest = _fixtures()

    adapter = _MemoryAdapter(
        galleries,
        content_incumbents={duplicate_digest: "dup_a"},
        gid_incumbents={500: "gid_a"},
    )
    StagedDeduplicationPlanner(page_size=1, write_batch_size=2).run(
        adapter,
        build_id="build-for-test",
    )

    # One generation advance per non-empty page plus the empty terminal page
    # proves the planner never accumulates decisions across page boundaries.
    assert adapter._spam_generation == adapter.page_calls["spam"] + 1
    assert StagedDeduplicationPhase.file_spam in adapter.completed_phases
