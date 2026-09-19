"""Counterexamples keep time-budget proposals separate from measured evidence."""

from __future__ import annotations

import runpy
from pathlib import Path
from typing import Any

import pytest

_SCRIPT = Path(__file__).parents[1] / "scripts/probe-publication-budget.py"


def _module() -> dict[str, Any]:
    return runpy.run_path(str(_SCRIPT))


def test_pure_variable_cost_has_same_total_work_but_earlier_first_publication() -> None:
    simulate = _module()["simulate"]
    args = {
        "galleries": 1000,
        "fixed_seconds": lambda _: 0,
        "gallery_seconds": lambda _: 60,
    }
    fixed = simulate(**args, strategy="fixed_100")
    adaptive = simulate(**args, strategy="two_success_throughput")
    assert adaptive["first_usable_seconds"] < fixed["first_usable_seconds"]
    assert adaptive["total_catchup_seconds"] == fixed["total_catchup_seconds"] == 60000
    assert adaptive["target_missed_batches"] == 0
    assert fixed["target_missed_batches"] == 10


def test_fixed_cost_counterexample_rejects_naive_two_sample_controller() -> None:
    simulate = _module()["simulate"]
    args = {
        "galleries": 1000,
        "fixed_seconds": lambda _: 5400,
        "gallery_seconds": lambda _: 30,
    }
    fixed = simulate(**args, strategy="fixed_100")
    adaptive = simulate(**args, strategy="two_success_throughput")
    fallback = simulate(**args, strategy="minimum_miss_fallback")
    assert adaptive["minimum_batch_misses"] > 900
    assert adaptive["total_catchup_seconds"] > fixed["total_catchup_seconds"] * 50
    assert fallback["total_catchup_seconds"] < adaptive["total_catchup_seconds"]
    assert fallback["target_miss_fraction"] == 1
    assert fallback["fallback_activated_after_batch"] is not None
    # Throughput fallback deliberately sacrifices the target; it is not a fix.
    with pytest.raises(AssertionError):
        assert fallback["target_missed_batches"] == 0


def test_controller_retains_only_two_successes_and_ignores_failed_work() -> None:
    controller = _module()["ThroughputController"](
        target_seconds=3600, cap=100, fallback=False
    )
    controller.completed(3, 480, remaining=True)
    assert controller.limit == 6
    controller.completed(6, 660, remaining=True)
    assert controller.limit == 12
    controller.completed(12, 1020, remaining=True)
    assert list(controller.samples) == [(6, 660), (12, 1020)]
    assert controller.limit == 24
    controller.failed()
    assert controller.limit == 12
    assert list(controller.samples) == [(6, 660), (12, 1020)]


def test_short_tail_is_real_completed_work_and_restart_recalibrates() -> None:
    simulate = _module()["simulate"]
    args = {
        "galleries": 217,
        "fixed_seconds": lambda _: 300,
        "gallery_seconds": lambda _: 30,
    }
    fixed = simulate(**args, strategy="fixed_100")
    assert fixed["final_batches"][-1]["count"] == 17
    restarted = simulate(
        **args, strategy="two_success_throughput", restart_after_batch=2
    )
    assert restarted["initial_batches"][2]["count"] == 3
    assert restarted["final_batches"][-1]["published"] == 217


def test_report_separates_synthetic_and_nonidentifiable_nas_counterfactual() -> None:
    report = _module()["report"](counterfactual_galleries=100)
    assert report["status"] == "completed"
    assert len(report["synthetic_cases"]) == 12
    assert (
        report["identifiability"]["two_equal_counts"]["identifies_fixed_cost"] is False
    )
    assert (
        report["identifiability"]["two_different_counts"]["stationarity_verified"]
        is False
    )
    for model in report["nas_counterfactual"]["models"]:
        assert (
            model["assumed_fixed_seconds"] + 100 * model["assumed_per_gallery_seconds"]
            == 10800
        )
    assert "not runtime" in _SCRIPT.read_text()


def test_same_target_oracle_separates_control_overhead_from_target_tradeoff() -> None:
    simulate = _module()["simulate"]
    args = {
        "galleries": 1000,
        "fixed_seconds": lambda _: 3300,
        "gallery_seconds": lambda _: 30,
    }
    oracle = simulate(**args, strategy="known_cost_target_oracle")
    adaptive = simulate(**args, strategy="two_success_throughput")
    fixed = simulate(**args, strategy="fixed_100")
    assert oracle["published_batches"] == 100
    assert oracle["total_catchup_seconds"] == 360000
    assert oracle["all_batches_within_target"]
    assert oracle["initial_batches"][0]["count"] == 10
    assert adaptive["all_batches_within_target"]
    assert adaptive["total_catchup_seconds"] / oracle["total_catchup_seconds"] == 3.145
    assert not fixed["all_batches_within_target"]
    assert fixed["total_catchup_seconds"] < oracle["total_catchup_seconds"]


def test_oracle_does_not_claim_feasible_when_one_gallery_exceeds_target() -> None:
    simulate = _module()["simulate"]
    oracle = simulate(
        galleries=10,
        fixed_seconds=lambda _: 5400,
        gallery_seconds=lambda _: 30,
        strategy="known_cost_target_oracle",
    )
    assert not oracle["all_batches_within_target"]
    assert oracle["oracle_constraint_infeasible_batches"] == 10
    assert oracle["published_batches"] == 10
