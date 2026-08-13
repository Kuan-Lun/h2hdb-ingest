"""Pure full and incremental evaluators for the proposed vNext policy.

This module is deliberately an independent test oracle, not production
implementation.  It spells out the policy formula from immutable input values
and does not import production reducers, the core repository, connectors, SQL,
or persistence adapters.  Differential tests compare it with the current
implementation so a shared implementation bug cannot make both sides pass.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from typing import Literal

GALLERYINFO_FILE_NAME = "galleryinfo.txt"
ALREADY_UPLOADED_TAG_VALUE = "already uploaded"
SPAM_FILE_MINIMUM_OCCURRENCES = 3
SPAM_ARTIST_RATIO_THRESHOLD = 2


def _validate_sha256(value: str) -> None:
    if len(value) != 64:
        raise ValueError("A file digest must contain 64 hexadecimal characters")
    try:
        bytes.fromhex(value)
    except ValueError as error:
        raise ValueError("A file digest must be hexadecimal") from error


def stable_gallery_key(
    source_scope_key: str,
    locator_segments: tuple[str, ...],
) -> str:
    """Derive a stable total-order key from one scope and exact nested locator."""

    _validate_sha256(source_scope_key)
    if not locator_segments:
        raise ValueError("A gallery locator must contain at least one segment")
    encoded_segments: list[bytes] = []
    for segment in locator_segments:
        encoded = segment.encode("utf-8")
        if (
            not encoded
            or len(encoded) > 255
            or segment in {".", ".."}
            or "/" in segment
            or "\\" in segment
            or "\0" in segment
        ):
            raise ValueError("Gallery locator segments must be exact safe UTF-8 names")
        encoded_segments.append(encoded)

    digest = sha256(b"h2hdb-vnext-reference-gallery-key-v1\0")
    digest.update((1).to_bytes(4, "big"))
    digest.update(bytes.fromhex(source_scope_key))
    digest.update(len(encoded_segments).to_bytes(4, "big"))
    for encoded in encoded_segments:
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class ReferenceFile:
    """One canonical source-file observation used by the policy oracle."""

    name: str
    sha256: str
    size_bytes: int

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("A source-file name must not be blank")
        _validate_sha256(self.sha256)
        if self.size_bytes < 0:
            raise ValueError("A source-file size must not be negative")


@dataclass(frozen=True, slots=True)
class ReferenceGallery:
    """All gallery values observable by deduplication or artifact creation."""

    gallery_key: str
    gallery_name: str
    gid: int
    title: str
    download_time: datetime
    tags: tuple[tuple[str, str], ...] = ()
    files: tuple[ReferenceFile, ...] = ()

    def __post_init__(self) -> None:
        if not self.gallery_key:
            raise ValueError("gallery_key must be the complete stable locator key")
        if not self.gallery_name:
            raise ValueError("gallery_name must not be blank")
        if self.gid <= 0:
            raise ValueError("gid must be positive")
        object.__setattr__(self, "tags", tuple(self.tags))
        object.__setattr__(self, "files", tuple(self.files))
        file_names = tuple(source_file.name for source_file in self.files)
        if len(set(file_names)) != len(file_names):
            raise ValueError("Source-file names must be unique within a gallery")

    @property
    def artists(self) -> frozenset[str]:
        return frozenset(value for name, value in self.tags if name == "artist")

    @property
    def already_uploaded(self) -> bool:
        return any(
            value.casefold() == ALREADY_UPLOADED_TAG_VALUE for _name, value in self.tags
        )

    @property
    def policy_hashes(self) -> tuple[str, ...]:
        """File occurrences observed by spam and effective-content policy."""

        return tuple(
            source_file.sha256
            for source_file in self.files
            if source_file.name != GALLERYINFO_FILE_NAME
        )


GallerySnapshot = Mapping[str, ReferenceGallery]


@dataclass(frozen=True, slots=True)
class HashGalleryEvidence:
    """Exact per-gallery evidence for one file hash, including duplicates."""

    gallery_key: str
    occurrence_count: int
    artists: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.gallery_key:
            raise ValueError("gallery_key must not be blank")
        if self.occurrence_count <= 0:
            raise ValueError("occurrence_count must be positive")
        if tuple(sorted(set(self.artists))) != self.artists:
            raise ValueError("artists must be unique and sorted")


@dataclass(frozen=True, slots=True)
class FileHashEvidence:
    """The complete corpus evidence observed by the spam classifier."""

    galleries: tuple[HashGalleryEvidence, ...]

    def __post_init__(self) -> None:
        if not self.galleries:
            raise ValueError("File-hash evidence must not be empty")
        keys = tuple(item.gallery_key for item in self.galleries)
        if tuple(sorted(set(keys))) != keys:
            raise ValueError("Evidence galleries must be unique and sorted")

    @property
    def occurrence_count(self) -> int:
        return sum(item.occurrence_count for item in self.galleries)


def spam_occurrence_threshold_met(evidence: FileHashEvidence) -> bool:
    """Use exact file occurrences, not gallery membership, at the spam gate."""

    return evidence.occurrence_count >= SPAM_FILE_MINIMUM_OCCURRENCES


@dataclass(frozen=True, slots=True)
class PolicyContext:
    """Immutable versioned policy; there is deliberately no active incumbent."""

    policy_version: int = 1

    def __post_init__(self) -> None:
        if self.policy_version <= 0:
            raise ValueError("policy_version must be positive")


@dataclass(frozen=True, slots=True)
class SourceSnapshotContext:
    """Exact source-head context that authorizes one incremental baseline."""

    source_scope_key: str = "reference-scope"
    channel: str = "default"
    source_revision: int = 1
    head_generation: int = 1

    def __post_init__(self) -> None:
        if not self.source_scope_key:
            raise ValueError("source_scope_key must not be blank")
        if not self.channel:
            raise ValueError("channel must not be blank")
        if self.source_revision <= 0:
            raise ValueError("source_revision must be positive")
        if self.head_generation < 0:
            raise ValueError("head_generation must not be negative")


@dataclass(frozen=True, slots=True)
class ReferenceCandidate:
    """Independent, complete comparator row with a unique final tie-break."""

    gallery_key: str
    gid: int
    title: str
    download_time: datetime
    content_sha256: str | None
    already_uploaded: bool


@dataclass(frozen=True, slots=True)
class ArtifactMemberPlanEntry:
    """One exact ordered source/generated entry in the CBZ input contract."""

    position: int
    entry_kind: Literal["source", "generated"]
    source_name: str | None
    generated_identity: str | None
    source_sha256: str | None
    source_size_bytes: int | None
    payload_sha256: str
    excluded: bool
    file_role: str
    archive_member_name: str | None
    transform_kind: str

    def __post_init__(self) -> None:
        if self.position < 0:
            raise ValueError("A member-plan position must not be negative")
        if self.entry_kind == "source":
            if not self.source_name:
                raise ValueError("A source entry requires its exact source name")
            if self.generated_identity is not None:
                raise ValueError("A source entry cannot have a generated identity")
            if self.source_sha256 is None or self.source_size_bytes is None:
                raise ValueError("A source entry requires its digest and size")
            _validate_sha256(self.source_sha256)
            if self.source_size_bytes < 0:
                raise ValueError("A source-entry size must not be negative")
        elif self.entry_kind == "generated":
            if not self.generated_identity:
                raise ValueError("A generated entry requires its exact identity")
            if (
                self.source_name is not None
                or self.source_sha256 is not None
                or self.source_size_bytes is not None
            ):
                raise ValueError("A generated entry cannot have source identity fields")
        else:
            raise ValueError(f"Unsupported member-plan entry kind: {self.entry_kind}")
        _validate_sha256(self.payload_sha256)
        if not self.file_role:
            raise ValueError("A member-plan entry requires a file role")
        if not self.transform_kind:
            raise ValueError("A member-plan entry requires a transform kind")
        if self.excluded:
            if self.archive_member_name is not None:
                raise ValueError("An excluded entry cannot have an archive member name")
        elif not self.archive_member_name:
            raise ValueError("An emitted entry requires an archive member name")


@dataclass(frozen=True, slots=True)
class ArtifactInput:
    """Every policy value capable of changing one selected CBZ artifact."""

    gallery: ReferenceGallery
    source_manifest_sha256: str
    member_plan: tuple[ArtifactMemberPlanEntry, ...]
    effective_content_sha256: str | None
    content_owner_gallery_key: str | None
    selected: bool
    policy_version: int


@dataclass(frozen=True, slots=True)
class Evaluation:
    """One complete derived state, suitable for field-wise comparison."""

    context: PolicyContext
    source_context: SourceSnapshotContext
    snapshot: dict[str, ReferenceGallery]
    evidence: dict[str, FileHashEvidence]
    spam: dict[str, bool]
    content: dict[str, str | None]
    content_owners: dict[str, str]
    gid_winners: dict[int, str]
    artifact_inputs: dict[str, ArtifactInput]
    artifacts: dict[str, ArtifactInput]


@dataclass(frozen=True, slots=True)
class ArtifactDelta:
    create: frozenset[str]
    rebuild: frozenset[str]
    delete: frozenset[str]
    unchanged: frozenset[str]


@dataclass(frozen=True, slots=True)
class IncrementalEvaluation:
    """Incremental result plus each exact invalidation frontier."""

    state: Evaluation
    evidence_impacted: frozenset[str]
    changed_spam: frozenset[str]
    content_impacted: frozenset[str]
    content_groups_impacted: frozenset[str]
    content_owner_tombstones: frozenset[str]
    gid_groups_impacted: frozenset[int]
    gid_winner_tombstones: frozenset[int]
    artifact_delta: ArtifactDelta


def _snapshot_copy(snapshot: GallerySnapshot) -> dict[str, ReferenceGallery]:
    copied = dict(snapshot)
    for gallery_key, gallery in copied.items():
        if gallery_key != gallery.gallery_key:
            raise ValueError("Snapshot keys must equal gallery_key")
    return copied


def exact_hash_evidence(snapshot: GallerySnapshot) -> dict[str, FileHashEvidence]:
    """Build exact per-hash evidence without applying the spam policy."""

    occurrences: dict[str, Counter[str]] = defaultdict(Counter)
    for gallery_key, gallery in snapshot.items():
        for file_sha256 in gallery.policy_hashes:
            occurrences[file_sha256][gallery_key] += 1

    evidence: dict[str, FileHashEvidence] = {}
    for file_sha256, by_gallery in occurrences.items():
        evidence[file_sha256] = FileHashEvidence(
            tuple(
                HashGalleryEvidence(
                    gallery_key=gallery_key,
                    occurrence_count=by_gallery[gallery_key],
                    artists=tuple(sorted(snapshot[gallery_key].artists)),
                )
                for gallery_key in sorted(by_gallery)
            )
        )
    return evidence


def _classify_spam(evidence: FileHashEvidence) -> bool:
    if not spam_occurrence_threshold_met(evidence):
        return False
    distinct_artists: set[str] = set()
    maximum_gallery_artists = 0
    for item in evidence.galleries:
        if not item.artists:
            continue
        distinct_artists.update(item.artists)
        maximum_gallery_artists = max(maximum_gallery_artists, len(item.artists))
    if maximum_gallery_artists == 0:
        return False
    return len(distinct_artists) / maximum_gallery_artists > SPAM_ARTIST_RATIO_THRESHOLD


def _full_spam(evidence: Mapping[str, FileHashEvidence]) -> dict[str, bool]:
    return {
        file_sha256: _classify_spam(hash_evidence)
        for file_sha256, hash_evidence in evidence.items()
    }


def _gallery_content(
    gallery: ReferenceGallery,
    spam: Mapping[str, bool],
) -> str | None:
    ordered = sorted(
        bytes.fromhex(file_sha256)
        for file_sha256 in gallery.policy_hashes
        if not spam.get(file_sha256, False)
    )
    return sha256(b"".join(ordered)).hexdigest() if ordered else None


def _candidate(
    gallery: ReferenceGallery,
    content_sha256: str | None,
) -> ReferenceCandidate:
    return ReferenceCandidate(
        gallery_key=gallery.gallery_key,
        gid=gallery.gid,
        title=gallery.title,
        download_time=gallery.download_time,
        content_sha256=content_sha256,
        already_uploaded=gallery.already_uploaded,
    )


def _content_candidates(
    snapshot: GallerySnapshot,
    content: Mapping[str, str | None],
) -> dict[str, ReferenceCandidate]:
    return {
        gallery_name: _candidate(gallery, content[gallery_name])
        for gallery_name, gallery in snapshot.items()
        if content[gallery_name] is not None
    }


def _content_groups(
    candidates: Mapping[str, ReferenceCandidate],
) -> dict[str, list[ReferenceCandidate]]:
    groups: dict[str, list[ReferenceCandidate]] = defaultdict(list)
    for candidate in candidates.values():
        assert candidate.content_sha256 is not None
        groups[candidate.content_sha256].append(candidate)
    return groups


def _base_priority(candidate: ReferenceCandidate) -> tuple[bool, int, datetime]:
    return (
        not candidate.already_uploaded,
        len(candidate.title),
        candidate.download_time,
    )


def _select_content_owner(candidates: list[ReferenceCandidate]) -> str:
    if not candidates:
        raise ValueError("A content group must not be empty")
    return max(
        candidates,
        key=lambda candidate: (
            *_base_priority(candidate),
            candidate.gid,
            candidate.gallery_key,
        ),
    ).gallery_key


def _gid_candidates(
    snapshot: GallerySnapshot,
    content: Mapping[str, str | None],
    content_owners: Mapping[str, str],
) -> dict[str, ReferenceCandidate]:
    candidates: dict[str, ReferenceCandidate] = {}
    for gallery_key, gallery in snapshot.items():
        content_sha256 = content[gallery_key]
        if content_sha256 is None or content_owners[content_sha256] == gallery_key:
            candidates[gallery_key] = _candidate(gallery, content_sha256)
    return candidates


def _gid_groups(
    candidates: Mapping[str, ReferenceCandidate],
) -> dict[int, list[ReferenceCandidate]]:
    groups: dict[int, list[ReferenceCandidate]] = defaultdict(list)
    for candidate in candidates.values():
        groups[candidate.gid].append(candidate)
    return groups


def _select_gid_winner(candidates: list[ReferenceCandidate]) -> str:
    if not candidates:
        raise ValueError("A GID group must not be empty")
    return max(
        candidates,
        key=lambda candidate: (*_base_priority(candidate), candidate.gallery_key),
    ).gallery_key


def _hash_framed_fields(*fields: bytes) -> str:
    digest = sha256(b"h2hdb-vnext-reference-source-manifest-v1\0")
    for field in fields:
        digest.update(len(field).to_bytes(8, "big"))
        digest.update(field)
    return digest.hexdigest()


def _source_manifest_sha256(gallery: ReferenceGallery) -> str:
    """Independent exact input digest used only by the reference artifact map."""

    fields = [
        gallery.gallery_key.encode(),
        gallery.gallery_name.encode(),
        str(gallery.gid).encode("ascii"),
        gallery.title.encode(),
        gallery.download_time.isoformat().encode("ascii"),
    ]
    fields.extend(
        namespace.encode() + b"\0" + value.encode() for namespace, value in gallery.tags
    )
    fields.extend(
        source_file.name.encode()
        + b"\0"
        + source_file.size_bytes.to_bytes(8, "big")
        + bytes.fromhex(source_file.sha256)
        for source_file in sorted(
            gallery.files,
            key=lambda item: (item.name.casefold(), item.name),
        )
    )
    return _hash_framed_fields(*fields)


def _artifact_member_plan(
    gallery: ReferenceGallery,
    spam: Mapping[str, bool],
) -> tuple[ArtifactMemberPlanEntry, ...]:
    ordered = sorted(gallery.files, key=lambda item: (item.name.casefold(), item.name))
    result: list[ArtifactMemberPlanEntry] = []
    for position, source_file in enumerate(ordered):
        metadata = source_file.name == GALLERYINFO_FILE_NAME
        excluded = not metadata and spam.get(source_file.sha256, False)
        result.append(
            ArtifactMemberPlanEntry(
                position=position,
                entry_kind="source",
                source_name=source_file.name,
                generated_identity=None,
                source_sha256=source_file.sha256,
                source_size_bytes=source_file.size_bytes,
                payload_sha256=source_file.sha256,
                excluded=excluded,
                file_role="metadata" if metadata else "content",
                archive_member_name=None if excluded else source_file.name,
                transform_kind="source_copy",
            )
        )
    return tuple(result)


def _artifact_inputs(
    snapshot: GallerySnapshot,
    spam: Mapping[str, bool],
    content: Mapping[str, str | None],
    content_owners: Mapping[str, str],
    gid_winners: Mapping[int, str],
    context: PolicyContext,
) -> dict[str, ArtifactInput]:
    result: dict[str, ArtifactInput] = {}
    for gallery_key, gallery in snapshot.items():
        content_sha256 = content[gallery_key]
        owner = None if content_sha256 is None else content_owners[content_sha256]
        is_content_owner = owner is None or owner == gallery_key
        result[gallery_key] = ArtifactInput(
            gallery=gallery,
            source_manifest_sha256=_source_manifest_sha256(gallery),
            member_plan=_artifact_member_plan(gallery, spam),
            effective_content_sha256=content_sha256,
            content_owner_gallery_key=owner,
            selected=(is_content_owner and gid_winners[gallery.gid] == gallery_key),
            policy_version=context.policy_version,
        )
    return result


def _evaluation(
    *,
    context: PolicyContext,
    source_context: SourceSnapshotContext,
    snapshot: dict[str, ReferenceGallery],
    evidence: dict[str, FileHashEvidence],
    spam: dict[str, bool],
    content: dict[str, str | None],
    content_owners: dict[str, str],
    gid_winners: dict[int, str],
) -> Evaluation:
    artifact_inputs = _artifact_inputs(
        snapshot,
        spam,
        content,
        content_owners,
        gid_winners,
        context,
    )
    return Evaluation(
        context=context,
        source_context=source_context,
        snapshot=snapshot,
        evidence=evidence,
        spam=spam,
        content=content,
        content_owners=content_owners,
        gid_winners=gid_winners,
        artifact_inputs=artifact_inputs,
        artifacts={
            gallery_name: artifact_input
            for gallery_name, artifact_input in artifact_inputs.items()
            if artifact_input.selected
        },
    )


def evaluate_full(
    snapshot: GallerySnapshot,
    *,
    context: PolicyContext | None = None,
    source_context: SourceSnapshotContext | None = None,
) -> Evaluation:
    """Evaluate the entire snapshot through the current policy semantics."""

    selected_context = context if context is not None else PolicyContext()
    selected_source_context = (
        source_context if source_context is not None else SourceSnapshotContext()
    )
    copied_snapshot = _snapshot_copy(snapshot)
    evidence = exact_hash_evidence(copied_snapshot)
    spam = _full_spam(evidence)
    content = {
        gallery_name: _gallery_content(gallery, spam)
        for gallery_name, gallery in copied_snapshot.items()
    }
    content_groups = _content_groups(_content_candidates(copied_snapshot, content))
    content_owners = {
        content_sha256: _select_content_owner(candidates)
        for content_sha256, candidates in content_groups.items()
    }
    gid_groups = _gid_groups(_gid_candidates(copied_snapshot, content, content_owners))
    gid_winners = {
        gid: _select_gid_winner(candidates) for gid, candidates in gid_groups.items()
    }
    return _evaluation(
        context=selected_context,
        source_context=selected_source_context,
        snapshot=copied_snapshot,
        evidence=evidence,
        spam=spam,
        content=content,
        content_owners=content_owners,
        gid_winners=gid_winners,
    )


def artifact_delta(
    old_artifacts: Mapping[str, ArtifactInput],
    new_artifacts: Mapping[str, ArtifactInput],
) -> ArtifactDelta:
    """Derive the exact operational sets; absent-to-absent is no CBZ action."""

    create: set[str] = set()
    rebuild: set[str] = set()
    delete: set[str] = set()
    unchanged: set[str] = set()
    for gallery_name in old_artifacts.keys() | new_artifacts.keys():
        old = old_artifacts.get(gallery_name)
        new = new_artifacts.get(gallery_name)
        if old is None:
            assert new is not None
            create.add(gallery_name)
        elif new is None:
            delete.add(gallery_name)
        elif old != new:
            rebuild.add(gallery_name)
        else:
            unchanged.add(gallery_name)
    return ArtifactDelta(
        create=frozenset(create),
        rebuild=frozenset(rebuild),
        delete=frozenset(delete),
        unchanged=frozenset(unchanged),
    )


def _candidate_impacted_groups(
    old_candidates: Mapping[str, ReferenceCandidate],
    new_candidates: Mapping[str, ReferenceCandidate],
    *,
    group_of: Callable[[ReferenceCandidate], object],
) -> tuple[set[str], set[object]]:
    changed = {
        gallery_name
        for gallery_name in old_candidates.keys() | new_candidates.keys()
        if old_candidates.get(gallery_name) != new_candidates.get(gallery_name)
    }
    groups: set[object] = set()
    for gallery_name in changed:
        old = old_candidates.get(gallery_name)
        new = new_candidates.get(gallery_name)
        if old is not None:
            groups.add(group_of(old))
        if new is not None:
            groups.add(group_of(new))
    return changed, groups


def evaluate_incremental(
    old: Evaluation,
    new_snapshot: GallerySnapshot,
    *,
    context: PolicyContext | None = None,
    source_context: SourceSnapshotContext | None = None,
    baseline_context: SourceSnapshotContext | None = None,
) -> IncrementalEvaluation:
    """Apply exact vNext deltas, carrying unaffected old decisions forward."""

    selected_context = context if context is not None else old.context
    if selected_context != old.context:
        raise ValueError(
            "A policy-context change requires depth-zero full recomputation"
        )
    selected_source_context = (
        source_context if source_context is not None else old.source_context
    )
    selected_baseline_context = (
        baseline_context if baseline_context is not None else old.source_context
    )
    if selected_baseline_context != old.source_context:
        raise ValueError(
            "The source baseline revision and generation must exactly match "
            "the inherited state"
        )
    if selected_source_context.channel != selected_baseline_context.channel:
        raise ValueError("A channel change requires depth-zero full recomputation")
    if (
        selected_source_context.source_scope_key
        != selected_baseline_context.source_scope_key
    ):
        raise ValueError("A source-scope change requires depth-zero full recomputation")
    snapshot = _snapshot_copy(new_snapshot)

    evidence = exact_hash_evidence(snapshot)
    evidence_impacted = frozenset(
        file_sha256
        for file_sha256 in old.evidence.keys() | evidence.keys()
        if old.evidence.get(file_sha256) != evidence.get(file_sha256)
    )
    spam = {
        file_sha256: (
            _classify_spam(hash_evidence)
            if file_sha256 in evidence_impacted
            else old.spam[file_sha256]
        )
        for file_sha256, hash_evidence in evidence.items()
    }
    changed_spam = frozenset(
        file_sha256
        for file_sha256 in old.spam.keys() | spam.keys()
        if old.spam.get(file_sha256, False) != spam.get(file_sha256, False)
    )

    content_impacted_names: set[str] = set()
    for gallery_name in old.snapshot.keys() | snapshot.keys():
        old_gallery = old.snapshot.get(gallery_name)
        new_gallery = snapshot.get(gallery_name)
        if old_gallery != new_gallery:
            content_impacted_names.add(gallery_name)
            continue
        assert old_gallery is not None and new_gallery is not None
        if changed_spam.intersection(old_gallery.policy_hashes):
            content_impacted_names.add(gallery_name)

    content = {
        gallery_name: (
            _gallery_content(gallery, spam)
            if gallery_name in content_impacted_names
            else old.content[gallery_name]
        )
        for gallery_name, gallery in snapshot.items()
    }

    old_content_candidates = _content_candidates(old.snapshot, old.content)
    new_content_candidates = _content_candidates(snapshot, content)
    _changed_content_candidates, content_group_objects = _candidate_impacted_groups(
        old_content_candidates,
        new_content_candidates,
        group_of=lambda candidate: candidate.content_sha256,
    )
    content_groups_impacted = frozenset(
        value for value in content_group_objects if isinstance(value, str)
    )
    content_groups = _content_groups(new_content_candidates)
    content_owners = {
        content_sha256: (
            _select_content_owner(candidates)
            if content_sha256 in content_groups_impacted
            else old.content_owners[content_sha256]
        )
        for content_sha256, candidates in content_groups.items()
    }
    content_owner_tombstones = frozenset(
        old.content_owners.keys() - content_owners.keys()
    )
    if not content_owner_tombstones <= content_groups_impacted:
        raise AssertionError("Every content-owner tombstone must be in the workset")

    old_gid_candidates = _gid_candidates(
        old.snapshot,
        old.content,
        old.content_owners,
    )
    new_gid_candidates = _gid_candidates(snapshot, content, content_owners)
    _changed_gid_candidates, gid_group_objects = _candidate_impacted_groups(
        old_gid_candidates,
        new_gid_candidates,
        group_of=lambda candidate: candidate.gid,
    )
    gid_groups_impacted = frozenset(
        value for value in gid_group_objects if isinstance(value, int)
    )
    gid_groups = _gid_groups(new_gid_candidates)
    gid_winners = {
        gid: (
            _select_gid_winner(candidates)
            if gid in gid_groups_impacted
            else old.gid_winners[gid]
        )
        for gid, candidates in gid_groups.items()
    }
    gid_winner_tombstones = frozenset(old.gid_winners.keys() - gid_winners.keys())
    if not gid_winner_tombstones <= gid_groups_impacted:
        raise AssertionError("Every GID-winner tombstone must be in the workset")

    state = _evaluation(
        context=selected_context,
        source_context=selected_source_context,
        snapshot=snapshot,
        evidence=evidence,
        spam=spam,
        content=content,
        content_owners=content_owners,
        gid_winners=gid_winners,
    )
    return IncrementalEvaluation(
        state=state,
        evidence_impacted=evidence_impacted,
        changed_spam=changed_spam,
        content_impacted=frozenset(content_impacted_names),
        content_groups_impacted=content_groups_impacted,
        content_owner_tombstones=content_owner_tombstones,
        gid_groups_impacted=gid_groups_impacted,
        gid_winner_tombstones=gid_winner_tombstones,
        artifact_delta=artifact_delta(old.artifacts, state.artifacts),
    )
