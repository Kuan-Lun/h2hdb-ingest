from __future__ import annotations

import ast
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path

import pytest
from reference.vnext_incremental import (
    GALLERYINFO_FILE_NAME,
    ArtifactDelta,
    Evaluation,
    IncrementalEvaluation,
    PolicyContext,
    ReferenceFile,
    ReferenceGallery,
    SourceSnapshotContext,
    artifact_delta,
    evaluate_full,
    evaluate_incremental,
    spam_occurrence_threshold_met,
    stable_gallery_key,
)

from h2hdb_ingest.deduplication import DeduplicationPolicy
from h2hdb_ingest.models import ScannedFile, ScannedGallery

_BASE_TIME = datetime(2024, 1, 1, tzinfo=UTC)


def _digest(label: str) -> str:
    return sha256(label.encode()).hexdigest()


def _gallery(
    name: str,
    *,
    gid: int,
    title: str | None = None,
    artist: str | None = None,
    hashes: tuple[str, ...] = (),
    metadata_only: bool = False,
    metadata_hash: str | None = None,
    download_offset: int = 0,
    gallery_key: str | None = None,
) -> ReferenceGallery:
    tags = () if artist is None else (("artist", artist),)
    files = tuple(
        ReferenceFile(f"{index:03}.jpg", file_sha256, size_bytes=index)
        for index, file_sha256 in enumerate(hashes, start=1)
    )
    if metadata_only or metadata_hash is not None:
        files += (
            ReferenceFile(
                GALLERYINFO_FILE_NAME,
                metadata_hash or _digest(f"meta:{name}"),
                size_bytes=1,
            ),
        )
    return ReferenceGallery(
        gallery_key=gallery_key or name,
        gallery_name=name,
        gid=gid,
        title=title if title is not None else name,
        download_time=_BASE_TIME + timedelta(minutes=download_offset),
        tags=tags,
        files=files,
    )


def _assert_fieldwise_equal(
    incremental: IncrementalEvaluation,
    full: Evaluation,
) -> None:
    assert incremental.state.context == full.context
    assert incremental.state.source_context == full.source_context
    assert incremental.state.snapshot == full.snapshot
    assert incremental.state.evidence == full.evidence
    assert incremental.state.spam == full.spam
    assert incremental.state.content == full.content
    assert incremental.state.content_owners == full.content_owners
    assert incremental.state.gid_winners == full.gid_winners
    assert incremental.state.artifact_inputs == full.artifact_inputs
    assert incremental.state.artifacts == full.artifacts


def _transition(
    old_snapshot: dict[str, ReferenceGallery],
    new_snapshot: dict[str, ReferenceGallery],
) -> tuple[Evaluation, IncrementalEvaluation, Evaluation]:
    old = evaluate_full(old_snapshot)
    incremental = evaluate_incremental(old, new_snapshot)
    full = evaluate_full(new_snapshot)
    _assert_fieldwise_equal(incremental, full)
    assert incremental.artifact_delta == artifact_delta(
        old.artifacts,
        full.artifacts,
    )
    return old, incremental, full


def _as_scanned_gallery(gallery: ReferenceGallery) -> ScannedGallery:
    files = tuple(
        ScannedFile(
            path=Path("/reference") / gallery.gallery_name / source_file.name,
            name=source_file.name,
            size_bytes=source_file.size_bytes,
            sha256=source_file.sha256,
        )
        for source_file in gallery.files
    )
    return ScannedGallery(
        folder=Path("/reference") / gallery.gallery_name,
        gallery_name=gallery.gallery_name,
        gid=gallery.gid,
        title=gallery.title,
        summary="",
        upload_account="reference",
        upload_time=_BASE_TIME,
        download_time=gallery.download_time,
        modified_time=gallery.download_time,
        pages=len(gallery.policy_hashes),
        tags=gallery.tags,
        files=files,
        metadata_sha256=_digest(f"metadata:{gallery.gallery_name}"),
        source_digest=_digest(f"source:{gallery.gallery_name}"),
        content_digest=None,
    )


def test_full_reference_refines_the_current_deduplication_policy() -> None:
    shared = _digest("shared")
    snapshot = {
        gallery.gallery_name: gallery
        for gallery in (
            _gallery("a", gid=1, artist="A", hashes=(shared,)),
            _gallery("b", gid=2, artist="B", hashes=(shared,)),
            _gallery("c", gid=3, artist="C", hashes=(shared,)),
            _gallery("metadata", gid=4, metadata_only=True),
        )
    }

    reference = evaluate_full(snapshot)
    plan = DeduplicationPolicy().select(
        _as_scanned_gallery(gallery) for gallery in snapshot.values()
    )

    assert reference.spam == {shared: True}
    assert plan.excluded_file_sha256s == frozenset({shared})
    assert {
        gallery.gallery_name: gallery.content_digest
        for gallery in plan.canonical_galleries
    } == reference.content
    assert {gallery.gallery_name for gallery in plan.winners} == set(
        reference.artifacts
    )
    assert dict(plan.duplicate_of_by_gallery_name) == {
        gallery_name: artifact_input.content_owner_gallery_key
        for gallery_name, artifact_input in reference.artifact_inputs.items()
        if artifact_input.content_owner_gallery_key not in {None, gallery_name}
    }


def test_reference_oracle_imports_no_production_module() -> None:
    source = (Path(__file__).parent / "reference" / "vnext_incremental.py").read_text()
    tree = ast.parse(source)
    imported_modules = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imported_modules.update(
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    )

    assert not any(
        module == "h2hdb_ingest" or module.startswith("h2hdb_ingest.")
        for module in imported_modules
    )


def test_third_artist_crosses_spam_threshold_and_invalidates_old_galleries() -> None:
    shared = _digest("shared")
    stable_hash = _digest("stable")
    first = _gallery("first", gid=1, artist="A", hashes=(shared,))
    second = _gallery("second", gid=2, artist="B", hashes=(shared,))
    third = _gallery("third", gid=3, artist="C", hashes=(shared,))
    stable = _gallery("stable", gid=4, artist="D", hashes=(stable_hash,))
    old_snapshot = {item.gallery_name: item for item in (first, second, stable)}
    new_snapshot = {**old_snapshot, third.gallery_name: third}

    old, incremental, full = _transition(old_snapshot, new_snapshot)

    assert old.spam[shared] is False
    assert full.spam[shared] is True
    assert incremental.evidence_impacted == frozenset({shared})
    assert incremental.changed_spam == frozenset({shared})
    assert incremental.content_impacted == frozenset({"first", "second", "third"})
    assert full.content["first"] is None
    assert full.content["second"] is None
    assert "stable" not in incremental.content_impacted
    assert incremental.artifact_delta.unchanged == frozenset({"stable"})


def test_metadata_digest_collision_never_contributes_artist_evidence() -> None:
    shared = _digest("shared-with-metadata")
    first = _gallery("first", gid=1, artist="A", hashes=(shared, shared))
    second = _gallery("second", gid=2, artist="B", hashes=(shared,))
    metadata_only = _gallery(
        "metadata-only",
        gid=3,
        artist="C",
        metadata_hash=shared,
    )
    snapshot = {
        gallery.gallery_name: gallery for gallery in (first, second, metadata_only)
    }

    reference = evaluate_full(snapshot)
    evidence = reference.evidence[shared]
    assert evidence.occurrence_count == 3
    assert {item.gallery_key for item in evidence.galleries} == {
        "first",
        "second",
    }
    assert reference.spam[shared] is False

    legacy = DeduplicationPolicy().select(
        _as_scanned_gallery(gallery) for gallery in snapshot.values()
    )
    assert shared not in legacy.excluded_file_sha256s


def test_duplicate_hash_occurrences_in_one_gallery_cross_the_occurrence_gate() -> None:
    shared = _digest("same-gallery-duplicate")
    below = _gallery("below", gid=5, artist="A", hashes=(shared, shared))
    at_threshold = _gallery(
        "at-threshold",
        gid=5,
        artist="A",
        hashes=(shared, shared, shared),
    )

    below_evidence = evaluate_full({below.gallery_key: below}).evidence[shared]
    threshold_evidence = evaluate_full(
        {at_threshold.gallery_key: at_threshold}
    ).evidence[shared]

    assert below_evidence.occurrence_count == 2
    assert not spam_occurrence_threshold_met(below_evidence)
    assert threshold_evidence.occurrence_count == 3
    assert spam_occurrence_threshold_met(threshold_evidence)
    assert len(threshold_evidence.galleries) == 1


def test_removing_third_artist_reverses_spam_and_restores_old_content() -> None:
    shared = _digest("shared")
    first = _gallery("first", gid=1, artist="A", hashes=(shared,))
    second = _gallery("second", gid=2, artist="B", hashes=(shared,))
    third = _gallery("third", gid=3, artist="C", hashes=(shared,))
    old_snapshot = {item.gallery_name: item for item in (first, second, third)}
    new_snapshot = {item.gallery_name: item for item in (first, second)}

    old, incremental, full = _transition(old_snapshot, new_snapshot)

    assert old.spam[shared] is True
    assert full.spam[shared] is False
    assert incremental.changed_spam == frozenset({shared})
    assert incremental.content_impacted == frozenset({"first", "second", "third"})
    assert full.content["first"] is not None
    assert full.content["second"] == full.content["first"]


def test_empty_and_metadata_only_additions_are_content_impacted() -> None:
    empty = _gallery("empty", gid=11)
    metadata = _gallery("metadata", gid=12, metadata_only=True)

    _old, incremental, full = _transition(
        {},
        {empty.gallery_name: empty, metadata.gallery_name: metadata},
    )

    assert incremental.evidence_impacted == frozenset()
    assert incremental.changed_spam == frozenset()
    assert incremental.content_impacted == frozenset({"empty", "metadata"})
    assert full.content == {"empty": None, "metadata": None}
    assert incremental.artifact_delta == ArtifactDelta(
        create=frozenset({"empty", "metadata"}),
        rebuild=frozenset(),
        delete=frozenset(),
        unchanged=frozenset(),
    )


def test_content_owner_change_fans_out_to_old_and_new_gid_groups() -> None:
    shared = _digest("owner-shared")
    former_owner = _gallery(
        "former-owner",
        gid=21,
        title="long former owner title",
        hashes=(shared,),
    )
    challenger = _gallery(
        "challenger",
        gid=22,
        title="short",
        hashes=(shared,),
    )
    promoted = _gallery(
        "challenger",
        gid=22,
        title="an even longer challenger title",
        hashes=(shared,),
    )
    old_snapshot = {
        former_owner.gallery_name: former_owner,
        challenger.gallery_name: challenger,
    }
    new_snapshot = {
        former_owner.gallery_name: former_owner,
        promoted.gallery_name: promoted,
    }

    old, incremental, full = _transition(old_snapshot, new_snapshot)
    content_sha256 = old.content["former-owner"]
    assert content_sha256 is not None

    assert old.content_owners[content_sha256] == "former-owner"
    assert full.content_owners[content_sha256] == "challenger"
    assert incremental.content_groups_impacted == frozenset({content_sha256})
    assert incremental.gid_groups_impacted == frozenset({21, 22})
    assert incremental.artifact_delta.create == frozenset({"challenger"})
    assert incremental.artifact_delta.delete == frozenset({"former-owner"})


def test_last_content_member_move_and_delete_emit_old_group_tombstones() -> None:
    old_hash = _digest("old-content-group")
    new_hash = _digest("new-content-group")
    original = _gallery("only", gid=23, hashes=(old_hash,))
    moved = _gallery("only", gid=23, hashes=(new_hash,))

    old, moved_incremental, moved_full = _transition(
        {original.gallery_key: original},
        {moved.gallery_key: moved},
    )
    old_group = old.content[original.gallery_key]
    new_group = moved_full.content[moved.gallery_key]
    assert old_group is not None and new_group is not None
    assert old_group != new_group
    assert moved_incremental.content_groups_impacted == frozenset(
        {old_group, new_group}
    )
    assert moved_incremental.content_owner_tombstones == frozenset({old_group})
    assert old_group not in moved_incremental.state.content_owners

    _old, deleted_incremental, deleted_full = _transition(
        {original.gallery_key: original},
        {},
    )
    assert deleted_full.content_owners == {}
    assert deleted_incremental.content_groups_impacted == frozenset({old_group})
    assert deleted_incremental.content_owner_tombstones == frozenset({old_group})


def test_same_gid_comparator_change_moves_the_winner() -> None:
    first = _gallery(
        "first",
        gid=31,
        title="long first title",
        hashes=(_digest("first-content"),),
    )
    second = _gallery(
        "second",
        gid=31,
        title="short",
        hashes=(_digest("second-content"),),
    )
    promoted = _gallery(
        "second",
        gid=31,
        title="the newly longest second title",
        hashes=(_digest("second-content"),),
    )

    old, incremental, full = _transition(
        {first.gallery_name: first, second.gallery_name: second},
        {first.gallery_name: first, promoted.gallery_name: promoted},
    )

    assert old.gid_winners == {31: "first"}
    assert full.gid_winners == {31: "second"}
    assert incremental.gid_groups_impacted == frozenset({31})
    assert incremental.artifact_delta.create == frozenset({"second"})
    assert incremental.artifact_delta.delete == frozenset({"first"})


def test_last_gid_member_move_and_delete_emit_old_group_tombstones() -> None:
    original = _gallery("only-gid", gid=32, hashes=(_digest("gid-content"),))
    moved = replace(original, gid=33)

    _old, moved_incremental, moved_full = _transition(
        {original.gallery_key: original},
        {moved.gallery_key: moved},
    )
    assert moved_full.gid_winners == {33: original.gallery_key}
    assert moved_incremental.gid_groups_impacted == frozenset({32, 33})
    assert moved_incremental.gid_winner_tombstones == frozenset({32})

    _old, deleted_incremental, deleted_full = _transition(
        {original.gallery_key: original},
        {},
    )
    assert deleted_full.gid_winners == {}
    assert deleted_incremental.gid_groups_impacted == frozenset({32})
    assert deleted_incremental.gid_winner_tombstones == frozenset({32})


def test_duplicate_nested_leaf_names_use_full_locator_key_as_final_tie_break() -> None:
    scope_key = _digest("nested-scope")
    first_key = stable_gallery_key(scope_key, ("parent-a", "same"))
    second_key = stable_gallery_key(scope_key, ("parent-b", "same"))
    first = _gallery(
        "same",
        gallery_key=first_key,
        gid=41,
        title="same title",
        hashes=(_digest("first"),),
    )
    second = _gallery(
        "same",
        gallery_key=second_key,
        gid=41,
        title="same title",
        hashes=(_digest("second"),),
    )

    forward = evaluate_full({first.gallery_key: first, second.gallery_key: second})
    reverse = evaluate_full({second.gallery_key: second, first.gallery_key: first})

    expected = max(first_key, second_key)
    assert first_key != second_key
    assert stable_gallery_key(scope_key, ("parent-a", "same")) == first_key
    assert forward.gid_winners == {41: expected}
    assert reverse.gid_winners == forward.gid_winners
    assert set(forward.artifacts) == {expected}


def test_content_owner_tie_uses_full_locator_key_independent_of_insertion() -> None:
    shared = _digest("same-content")
    scope_key = _digest("content-owner-scope")
    first_key = stable_gallery_key(scope_key, ("parent-a", "same"))
    second_key = stable_gallery_key(scope_key, ("parent-b", "same"))
    first = _gallery(
        "same",
        gallery_key=first_key,
        gid=41,
        title="same title",
        hashes=(shared,),
    )
    second = _gallery(
        "same",
        gallery_key=second_key,
        gid=41,
        title="same title",
        hashes=(shared,),
    )

    forward = evaluate_full({first.gallery_key: first, second.gallery_key: second})
    reverse = evaluate_full({second.gallery_key: second, first.gallery_key: first})
    content_sha256 = forward.content[first.gallery_key]

    assert content_sha256 is not None
    expected = max(first_key, second_key)
    assert forward.content_owners == {content_sha256: expected}
    assert reverse.content_owners == forward.content_owners
    assert forward.gid_winners == reverse.gid_winners == {41: expected}


def test_gallery_key_is_required_instead_of_falling_back_to_leaf_name() -> None:
    try:
        ReferenceGallery(
            gallery_key="",
            gallery_name="same",
            gid=41,
            title="same",
            download_time=_BASE_TIME,
        )
    except ValueError as error:
        assert "complete stable locator key" in str(error)
    else:  # pragma: no cover - protects the identity boundary
        raise AssertionError("ReferenceGallery unexpectedly inferred a leaf key")


def test_member_plan_difference_forces_rebuild_even_when_other_inputs_match() -> None:
    gallery = _gallery(
        "member-plan",
        gid=42,
        hashes=(_digest("one"), _digest("two")),
    )
    evaluation = evaluate_full({gallery.gallery_key: gallery})
    original = evaluation.artifacts[gallery.gallery_key]
    first_entry = original.member_plan[0]
    assert first_entry.entry_kind == "source"
    assert first_entry.generated_identity is None
    assert first_entry.source_name == "001.jpg"
    assert first_entry.source_sha256 == _digest("one")
    assert first_entry.source_size_bytes == 1
    assert first_entry.payload_sha256 == first_entry.source_sha256
    assert first_entry.file_role == "content"
    assert first_entry.excluded is False
    assert first_entry.archive_member_name == first_entry.source_name
    assert first_entry.transform_kind == "source_copy"
    changed_entry = replace(
        first_entry,
        archive_member_name="renamed.jpg",
    )
    changed = replace(
        original,
        member_plan=(changed_entry, *original.member_plan[1:]),
    )

    assert artifact_delta(
        {gallery.gallery_key: original},
        {gallery.gallery_key: changed},
    ).rebuild == {gallery.gallery_key}


def test_policy_context_has_no_mutable_incumbent_inputs() -> None:
    assert PolicyContext(policy_version=7).policy_version == 7
    try:
        PolicyContext(content_incumbents={})  # type: ignore[call-arg]
    except TypeError:
        pass
    else:  # pragma: no cover - protects the no-incumbent contract
        raise AssertionError("PolicyContext unexpectedly accepted an incumbent map")


def test_policy_change_requires_depth_zero_and_rebuilds_selected() -> None:
    snapshot = {
        gallery.gallery_key: gallery
        for gallery in (
            _gallery("first", gid=51, hashes=(_digest("first"),)),
            _gallery("second", gid=52, hashes=(_digest("second"),)),
        )
    }
    old = evaluate_full(snapshot, context=PolicyContext(policy_version=1))

    try:
        evaluate_incremental(
            old,
            snapshot,
            context=PolicyContext(policy_version=2),
        )
    except ValueError as error:
        assert "depth-zero full recomputation" in str(error)
    else:  # pragma: no cover - protects the generation inheritance boundary
        raise AssertionError("A policy change unexpectedly inherited old derived state")

    compacted = evaluate_full(snapshot, context=PolicyContext(policy_version=2))
    assert artifact_delta(old.artifacts, compacted.artifacts) == ArtifactDelta(
        create=frozenset(),
        rebuild=frozenset(snapshot),
        delete=frozenset(),
        unchanged=frozenset(),
    )


def test_incremental_inheritance_requires_exact_policy_channel_scope_and_base() -> None:
    gallery = _gallery("context", gid=60, hashes=(_digest("context"),))
    snapshot = {gallery.gallery_key: gallery}
    old_source = SourceSnapshotContext(
        source_scope_key=_digest("scope-a"),
        channel="stable",
        source_revision=7,
        head_generation=11,
    )
    old = evaluate_full(snapshot, source_context=old_source)
    next_source = replace(old_source, source_revision=8, head_generation=12)

    valid = evaluate_incremental(
        old,
        snapshot,
        source_context=next_source,
        baseline_context=old_source,
    )
    assert valid.state.source_context == next_source

    with pytest.raises(ValueError, match="policy-context change"):
        evaluate_incremental(old, snapshot, context=PolicyContext(policy_version=2))
    with pytest.raises(ValueError, match="channel change"):
        evaluate_incremental(
            old,
            snapshot,
            source_context=replace(next_source, channel="preview"),
            baseline_context=old_source,
        )
    with pytest.raises(ValueError, match="source-scope change"):
        evaluate_incremental(
            old,
            snapshot,
            source_context=replace(
                next_source,
                source_scope_key=_digest("scope-b"),
            ),
            baseline_context=old_source,
        )
    with pytest.raises(ValueError, match="revision and generation"):
        evaluate_incremental(
            old,
            snapshot,
            source_context=next_source,
            baseline_context=replace(old_source, head_generation=10),
        )


def test_added_gallery_crossing_spam_threshold_has_exact_operation_partition() -> None:
    shared = _digest("operation-shared")
    retained = _digest("retained-after-spam")
    stable_hash = _digest("operation-stable")
    content_owner = _gallery(
        "content-owner",
        gid=20,
        title="the longest original content owner",
        artist="A",
        hashes=(shared,),
    )
    future_gid_winner = _gallery(
        "future-gid-winner",
        gid=10,
        title="a longer gid challenger",
        artist="B",
        hashes=(shared,),
    )
    deleted_after_spam = _gallery(
        "deleted-after-spam",
        gid=10,
        title="short",
        artist="A",
        hashes=(shared, retained),
    )
    added = _gallery(
        "added",
        gid=30,
        artist="C",
        hashes=(shared,),
    )
    stable = _gallery(
        "stable",
        gid=40,
        artist="D",
        hashes=(stable_hash,),
    )
    old_snapshot = {
        gallery.gallery_key: gallery
        for gallery in (
            content_owner,
            future_gid_winner,
            deleted_after_spam,
            stable,
        )
    }
    new_snapshot = {**old_snapshot, added.gallery_key: added}

    old, incremental, full = _transition(old_snapshot, new_snapshot)

    assert old.spam[shared] is False
    assert full.spam[shared] is True
    assert set(old.artifacts) == {
        content_owner.gallery_key,
        deleted_after_spam.gallery_key,
        stable.gallery_key,
    }
    assert incremental.artifact_delta == ArtifactDelta(
        create=frozenset({future_gid_winner.gallery_key, added.gallery_key}),
        rebuild=frozenset({content_owner.gallery_key}),
        delete=frozenset({deleted_after_spam.gallery_key}),
        unchanged=frozenset({stable.gallery_key}),
    )
    operations = incremental.artifact_delta
    assert (
        operations.create
        | operations.rebuild
        | operations.delete
        | operations.unchanged
    ) == old.artifacts.keys() | full.artifacts.keys()
    assert not (
        operations.create & operations.rebuild
        or operations.create & operations.delete
        or operations.create & operations.unchanged
        or operations.rebuild & operations.delete
        or operations.rebuild & operations.unchanged
        or operations.delete & operations.unchanged
    )
