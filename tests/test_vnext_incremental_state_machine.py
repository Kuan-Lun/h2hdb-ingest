from __future__ import annotations

import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256

import pytest
from hypothesis import HealthCheck, given, note, settings
from hypothesis import strategies as st
from hypothesis.stateful import (
    RuleBasedStateMachine,
    initialize,
    invariant,
    precondition,
    rule,
)
from reference.vnext_incremental import (
    GALLERYINFO_FILE_NAME,
    ArtifactDelta,
    Evaluation,
    PolicyContext,
    ReferenceFile,
    ReferenceGallery,
    SourceSnapshotContext,
    artifact_delta,
    evaluate_full,
    evaluate_incremental,
    stable_gallery_key,
)

_BASE_TIME = datetime(2024, 1, 1, tzinfo=UTC)
_HASH_POOL = tuple(
    sha256(f"property-hash-{index}".encode()).hexdigest() for index in range(5)
)
_SCOPE_KEYS = tuple(
    sha256(f"property-scope-{index}".encode()).hexdigest() for index in range(3)
)
_CHANNELS = ("stable", "preview")
_POLICY_VERSIONS = (1, 2)
_LEAF_NAMES = ("same", "gallery-a", "gallery-b")
_PARENT_NAMES = ("parent-a", "parent-b", "nested")
_ARTISTS = (None, "artist-a", "artist-b", "artist-c")


def _configure_hypothesis_profiles() -> None:
    settings.register_profile(
        "pr",
        max_examples=75,
        stateful_step_count=20,
        deadline=None,
        derandomize=True,
        suppress_health_check=(HealthCheck.too_slow,),
    )
    settings.register_profile(
        "nightly",
        max_examples=500,
        stateful_step_count=80,
        deadline=None,
        suppress_health_check=(HealthCheck.too_slow,),
    )
    selected = os.environ.get("H2HDB_HYPOTHESIS_PROFILE", "pr")
    if selected not in {"pr", "nightly"}:
        raise RuntimeError("H2HDB_HYPOTHESIS_PROFILE must be either 'pr' or 'nightly'")
    settings.load_profile(selected)


_configure_hypothesis_profiles()


@st.composite
def _gallery_strategy(
    draw: st.DrawFn,
    *,
    slot: int | None = None,
    source_scope_key: str | None = None,
) -> ReferenceGallery:
    selected_slot = (
        draw(st.integers(min_value=0, max_value=5)) if slot is None else slot
    )
    scope_key = (
        draw(st.sampled_from(_SCOPE_KEYS))
        if source_scope_key is None
        else source_scope_key
    )
    parent = draw(st.sampled_from(_PARENT_NAMES))
    leaf = draw(st.sampled_from(_LEAF_NAMES))
    gallery_key = stable_gallery_key(
        scope_key,
        (parent, f"slot-{selected_slot}", leaf),
    )
    gid = draw(st.integers(min_value=1, max_value=4))
    title_suffix = draw(st.text(alphabet="abcXYZ", min_size=0, max_size=5))
    artist = draw(st.sampled_from(_ARTISTS))
    tags: tuple[tuple[str, str], ...] = () if artist is None else (("artist", artist),)
    if draw(st.booleans()):
        tags += (("status", "already uploaded"),)
    content_hashes = draw(
        st.lists(
            st.sampled_from(_HASH_POOL),
            min_size=0,
            max_size=5,
        )
    )
    include_metadata = draw(st.booleans())
    files = tuple(
        ReferenceFile(
            name=f"{position:03}.jpg",
            sha256=file_sha256,
            size_bytes=position,
        )
        for position, file_sha256 in enumerate(content_hashes, start=1)
    )
    if include_metadata:
        files += (
            ReferenceFile(
                name=GALLERYINFO_FILE_NAME,
                sha256=sha256(f"metadata-{selected_slot}".encode()).hexdigest(),
                size_bytes=1,
            ),
        )
    return ReferenceGallery(
        gallery_key=gallery_key,
        gallery_name=leaf,
        gid=gid,
        title=f"title-{selected_slot}-{title_suffix}",
        download_time=_BASE_TIME + timedelta(minutes=selected_slot),
        tags=tags,
        files=files,
    )


@st.composite
def _snapshot_strategy(
    draw: st.DrawFn,
    *,
    source_scope_key: str | None = None,
) -> dict[str, ReferenceGallery]:
    selected_scope = (
        draw(st.sampled_from(_SCOPE_KEYS))
        if source_scope_key is None
        else source_scope_key
    )
    slots = draw(
        st.lists(
            st.integers(min_value=0, max_value=5),
            min_size=0,
            max_size=6,
            unique=True,
        )
    )
    galleries = [
        draw(_gallery_strategy(slot=slot, source_scope_key=selected_scope))
        for slot in slots
    ]
    return {gallery.gallery_key: gallery for gallery in galleries}


@st.composite
def _valid_context_transition(
    draw: st.DrawFn,
) -> tuple[PolicyContext, SourceSnapshotContext, SourceSnapshotContext]:
    policy = PolicyContext(draw(st.sampled_from(_POLICY_VERSIONS)))
    source_revision = draw(st.integers(min_value=1, max_value=20))
    head_generation = draw(st.integers(min_value=0, max_value=20))
    baseline = SourceSnapshotContext(
        source_scope_key=draw(st.sampled_from(_SCOPE_KEYS)),
        channel=draw(st.sampled_from(_CHANNELS)),
        source_revision=source_revision,
        head_generation=head_generation,
    )
    target = replace(
        baseline,
        source_revision=source_revision + draw(st.integers(min_value=1, max_value=5)),
        head_generation=head_generation + draw(st.integers(min_value=1, max_value=5)),
    )
    return policy, baseline, target


@st.composite
def _transition_case(
    draw: st.DrawFn,
) -> tuple[
    dict[str, ReferenceGallery],
    dict[str, ReferenceGallery],
    tuple[PolicyContext, SourceSnapshotContext, SourceSnapshotContext],
]:
    contexts = draw(_valid_context_transition())
    _policy, baseline, _target = contexts
    old_snapshot = draw(_snapshot_strategy(source_scope_key=baseline.source_scope_key))
    new_snapshot = draw(_snapshot_strategy(source_scope_key=baseline.source_scope_key))
    return old_snapshot, new_snapshot, contexts


@st.composite
def _snapshot_context_case(
    draw: st.DrawFn,
) -> tuple[
    dict[str, ReferenceGallery],
    tuple[PolicyContext, SourceSnapshotContext, SourceSnapshotContext],
]:
    contexts = draw(_valid_context_transition())
    _policy, baseline, _target = contexts
    snapshot = draw(_snapshot_strategy(source_scope_key=baseline.source_scope_key))
    return snapshot, contexts


def _assert_operation_partition(
    old: Evaluation,
    new: Evaluation,
    delta: ArtifactDelta,
) -> None:
    parts = (delta.create, delta.rebuild, delta.delete, delta.unchanged)
    for index, left in enumerate(parts):
        for right in parts[index + 1 :]:
            assert left.isdisjoint(right)
    assert frozenset().union(*parts) == old.artifacts.keys() | new.artifacts.keys()
    assert delta == artifact_delta(old.artifacts, new.artifacts)


def _assert_incremental_equals_full(
    old_snapshot: dict[str, ReferenceGallery],
    new_snapshot: dict[str, ReferenceGallery],
    policy: PolicyContext,
    baseline: SourceSnapshotContext,
    target: SourceSnapshotContext,
) -> None:
    old = evaluate_full(
        old_snapshot,
        context=policy,
        source_context=baseline,
    )
    incremental = evaluate_incremental(
        old,
        new_snapshot,
        context=policy,
        source_context=target,
        baseline_context=baseline,
    )
    full = evaluate_full(
        new_snapshot,
        context=policy,
        source_context=target,
    )

    note(f"old keys: {sorted(old_snapshot)}")
    note(f"new keys: {sorted(new_snapshot)}")
    assert incremental.state.source_context == target
    assert incremental.state.context == full.context
    assert incremental.state.snapshot == full.snapshot
    assert incremental.state.evidence == full.evidence
    assert incremental.state.spam == full.spam
    assert incremental.state.content == full.content
    assert incremental.state.content_owners == full.content_owners
    assert incremental.state.gid_winners == full.gid_winners
    assert incremental.state.artifact_inputs == full.artifact_inputs
    assert incremental.state.artifacts == full.artifacts
    assert incremental.content_owner_tombstones == frozenset(
        old.content_owners.keys() - full.content_owners.keys()
    )
    assert incremental.content_owner_tombstones <= incremental.content_groups_impacted
    assert incremental.gid_winner_tombstones == frozenset(
        old.gid_winners.keys() - full.gid_winners.keys()
    )
    assert incremental.gid_winner_tombstones <= incremental.gid_groups_impacted
    _assert_operation_partition(old, full, incremental.artifact_delta)


@given(case=_transition_case())
def test_generated_incremental_transition_equals_full_recompute(
    case: tuple[
        dict[str, ReferenceGallery],
        dict[str, ReferenceGallery],
        tuple[PolicyContext, SourceSnapshotContext, SourceSnapshotContext],
    ],
) -> None:
    old_snapshot, new_snapshot, contexts = case
    policy, baseline, target = contexts
    _assert_incremental_equals_full(
        old_snapshot,
        new_snapshot,
        policy,
        baseline,
        target,
    )


@given(
    case=_snapshot_context_case(),
    mismatch=st.sampled_from(("policy", "channel", "scope", "revision", "generation")),
)
def test_generated_invalid_incremental_context_is_rejected(
    case: tuple[
        dict[str, ReferenceGallery],
        tuple[PolicyContext, SourceSnapshotContext, SourceSnapshotContext],
    ],
    mismatch: str,
) -> None:
    snapshot, contexts = case
    policy, baseline, target = contexts
    old = evaluate_full(snapshot, context=policy, source_context=baseline)
    selected_policy = policy
    selected_baseline = baseline
    selected_target = target
    if mismatch == "policy":
        selected_policy = PolicyContext(policy.policy_version + 1)
    elif mismatch == "channel":
        selected_target = replace(
            target,
            channel="preview" if baseline.channel == "stable" else "stable",
        )
    elif mismatch == "scope":
        alternate_scope = next(
            scope for scope in _SCOPE_KEYS if scope != baseline.source_scope_key
        )
        selected_target = replace(target, source_scope_key=alternate_scope)
    elif mismatch == "revision":
        selected_baseline = replace(
            baseline,
            source_revision=baseline.source_revision + 1,
        )
    else:
        selected_baseline = replace(
            baseline,
            head_generation=baseline.head_generation + 1,
        )

    with pytest.raises(ValueError):
        evaluate_incremental(
            old,
            snapshot,
            context=selected_policy,
            source_context=selected_target,
            baseline_context=selected_baseline,
        )


class IncrementalSnapshotStateMachine(RuleBasedStateMachine):
    """Exercise long add/delete/modify sequences against clean recomputation."""

    def __init__(self) -> None:
        super().__init__()
        self._snapshot: dict[str, ReferenceGallery] = {}
        self._policy = PolicyContext()
        self._source = SourceSnapshotContext(
            source_scope_key=_SCOPE_KEYS[0],
            channel="stable",
            source_revision=1,
            head_generation=0,
        )
        self._state = evaluate_full(
            self._snapshot,
            context=self._policy,
            source_context=self._source,
        )

    @initialize()
    def initialize_empty_snapshot(self) -> None:
        self._snapshot = {}
        self._state = evaluate_full(
            self._snapshot,
            context=self._policy,
            source_context=self._source,
        )

    def _advance(self, new_snapshot: dict[str, ReferenceGallery]) -> None:
        baseline = self._source
        target = replace(
            baseline,
            source_revision=baseline.source_revision + 1,
            head_generation=baseline.head_generation + 1,
        )
        incremental = evaluate_incremental(
            self._state,
            new_snapshot,
            context=self._policy,
            source_context=target,
            baseline_context=baseline,
        )
        full = evaluate_full(
            new_snapshot,
            context=self._policy,
            source_context=target,
        )
        assert incremental.state == full
        _assert_operation_partition(self._state, full, incremental.artifact_delta)
        assert incremental.content_owner_tombstones <= (
            incremental.content_groups_impacted
        )
        assert incremental.gid_winner_tombstones <= incremental.gid_groups_impacted
        self._snapshot = new_snapshot
        self._source = target
        self._state = incremental.state

    @rule(gallery=_gallery_strategy(source_scope_key=_SCOPE_KEYS[0]))
    def add_or_replace_gallery(self, gallery: ReferenceGallery) -> None:
        new_snapshot = dict(self._snapshot)
        new_snapshot[gallery.gallery_key] = gallery
        self._advance(new_snapshot)

    @precondition(lambda self: bool(self._snapshot))
    @rule(index=st.integers(min_value=0, max_value=100))
    def delete_gallery(self, index: int) -> None:
        key = sorted(self._snapshot)[index % len(self._snapshot)]
        new_snapshot = dict(self._snapshot)
        del new_snapshot[key]
        self._advance(new_snapshot)

    @precondition(lambda self: bool(self._snapshot))
    @rule(
        index=st.integers(min_value=0, max_value=100),
        gid=st.integers(min_value=1, max_value=4),
        content_hashes=st.lists(
            st.sampled_from(_HASH_POOL),
            min_size=0,
            max_size=5,
        ),
        metadata_only=st.booleans(),
    )
    def modify_gallery(
        self,
        index: int,
        gid: int,
        content_hashes: list[str],
        metadata_only: bool,
    ) -> None:
        key = sorted(self._snapshot)[index % len(self._snapshot)]
        old_gallery = self._snapshot[key]
        files = tuple(
            ReferenceFile(
                name=f"{position:03}.jpg",
                sha256=file_sha256,
                size_bytes=position,
            )
            for position, file_sha256 in enumerate(content_hashes, start=1)
        )
        if metadata_only:
            files = (
                ReferenceFile(
                    name=GALLERYINFO_FILE_NAME,
                    sha256=sha256(f"state-meta-{key}".encode()).hexdigest(),
                    size_bytes=1,
                ),
            )
        new_snapshot = dict(self._snapshot)
        new_snapshot[key] = replace(old_gallery, gid=gid, files=files)
        self._advance(new_snapshot)

    @invariant()
    def current_state_is_a_full_recompute(self) -> None:
        assert self._state == evaluate_full(
            self._snapshot,
            context=self._policy,
            source_context=self._source,
        )


TestIncrementalSnapshotStateMachine = IncrementalSnapshotStateMachine.TestCase
