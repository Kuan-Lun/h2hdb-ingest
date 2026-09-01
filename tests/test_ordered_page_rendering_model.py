from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LEAN_MODEL = ROOT / "verification" / "lean" / "OrderedPageRendering.lean"
TLA_MODEL = ROOT / "verification" / "tla" / "OrderedPageRendering.tla"
SMALL_PROFILE = ROOT / "verification" / "tla" / "OrderedPageRenderingSmall.cfg"


def test_lean_model_proves_order_serialization_and_publish_last() -> None:
    model = LEAN_MODEL.read_text(encoding="utf-8")
    prose = " ".join(model.split())

    for definition in {
        "ValidWorkerCount",
        "nextBatchSize",
        "BoundedExecution",
        "orderedCollect",
        "serializeOrdered",
        "publishLast",
    }:
        assert f"def {definition}" in model or f"structure {definition}" in model
    for theorem in {
        "arbitrary_bounded_schedule_ordered_collect_equals_sequential_map",
        "completion_schedule_contains_every_page_exactly_once",
        "worker_count_is_valid_and_every_batch_is_worker_bounded",
        "worker_and_every_batch_are_hard_bounded_by_four",
        "ordered_serialization_equals_sequential_serialization",
        "every_deterministic_serializer_observes_sequential_input",
        "every_prepublication_failure_preserves_destination",
    }:
        assert f"theorem {theorem}" in model

    assert "deterministic pure function" in prose
    assert "any permutation" in prose
    assert "do not prove Pillow determinism or thread safety" in prose
    assert "Python future/executor behavior" in prose
    assert "filesystem semantics" in prose
    assert "final destination write itself fails" in prose


def test_tla_model_explores_bounded_schedules_failures_and_publish_last() -> None:
    model = TLA_MODEL.read_text(encoding="utf-8")
    prose = " ".join(model.split())

    for action in {
        "StartBatch",
        "CompletePage",
        "CollectPage",
        "FinishBatch",
        "WorkerFailure",
        "ValidationFailure",
        "SerializationFailure",
        "PublishLast",
    }:
        assert f"{action} ==" in model or f"{action}(page) ==" in model
    for invariant in {
        "WorkerCountHardBound",
        "BatchSizeHardBound",
        "FinishedWithinCurrentBatch",
        "OrderedCollectIsSequentialPrefix",
        "AllCollectedBeforePostProcessing",
        "ReadyOrPublishedHasSequentialSerialization",
        "FailurePreservesDestination",
        "DestinationChangesOnlyAtPublish",
        "PublishedDestinationIsSequential",
    }:
        assert f"{invariant} ==" in model

    assert "arbitrary non-empty batches" in prose
    assert "finish each page in any order" in prose
    assert "PublishLast is the sole action that changes destination" in prose
    assert "does not establish Pillow determinism or thread safety" in prose
    assert "Python executor or future semantics" in prose
    assert "filesystem atomicity or durability" in prose
    assert "failure during the final destination write itself" in prose


def test_tla_small_profile_wires_four_worker_safety_contract() -> None:
    profile = SMALL_PROFILE.read_text(encoding="utf-8")

    assert "SPECIFICATION Spec" in profile
    assert "PageCount = 4" in profile
    assert "MaxWorkers = 4" in profile
    assert "WorkerCountHardBound" in profile
    assert "BatchSizeHardBound" in profile
    assert "OrderedCollectIsSequentialPrefix" in profile
    assert "FailurePreservesDestination" in profile
    assert "PublishedDestinationIsSequential" in profile
    assert "not a Python/Pillow/filesystem refinement proof" in profile
    assert "PROPERTY" not in profile
