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


def test_calibrated_distinct_counts_avoid_the_stationary_three_gallery_trap() -> None:
    module = _module()
    controller = module["CalibratedController"](target_seconds=3600, cap=100)
    controller.completed(3, 3390, remaining=True)
    assert controller.limit == 6
    controller.completed(6, 3480, remaining=True)
    assert controller.fit == (3300, 30)
    assert controller.limit == 10
    assert controller.state()["stationarity_verified"] is False
    result = module["simulate"](
        galleries=1000,
        fixed_seconds=lambda _: 3300,
        gallery_seconds=lambda _: 30,
        strategy="calibrated_two_distinct",
    )
    assert [row["count"] for row in result["initial_batches"][:3]] == [3, 6, 10]
    assert result["total_catchup_seconds"] == 373200
    assert result["total_catchup_seconds"] / 360000 == pytest.approx(1.0366666666666666)
    assert result["published_batches"] == 104
    assert result["all_batches_within_target"]


def test_calibration_explores_unknown_equal_counts_without_unsafe_known_probe() -> None:
    cls = _module()["CalibratedController"]
    controller = cls(target_seconds=3600, cap=100)
    controller.completed(3, 3390, remaining=True)
    controller.completed(3, 3390, remaining=True)
    assert controller.fit is None
    assert controller.limit != 3
    one = cls(target_seconds=3600, cap=100)
    one.completed(3, 3900, remaining=True)
    one.completed(6, 4500, remaining=True)
    assert one.fit == (3300, 200)
    for _ in range(40):
        one.completed(1, 3500, remaining=True)
        assert one.limit == 1
    assert one.event_counts["no_target_safe_distinct_probe"] > 0
    assert len(one.events) <= 32
    assert len(one.samples) == 2


def test_calibration_retains_nonpositive_slope_and_negative_intercept_as_unknown() -> (
    None
):
    cls = _module()["CalibratedController"]
    for first, second in ((390, 390), (390, 300), (360, 7600)):
        controller = cls(target_seconds=3600, cap=100)
        controller.completed(3, first, remaining=True)
        controller.completed(6, second, remaining=True)
        assert controller.fit is None
        assert controller.status == "unknown_bounded_exploration"
        assert controller.event_counts["invalid_affine_fit_unknown"] == 1
        assert controller.state()["stationarity_verified"] is False
    for invalid in (0, -1, float("nan"), float("inf")):
        with pytest.raises(ValueError):
            cls(target_seconds=invalid, cap=100)
        with pytest.raises(ValueError):
            cls(target_seconds=3600, cap=100).completed(3, invalid, remaining=True)


def test_calibration_reports_infeasibility_and_fallback_does_not_claim_hourly_success() -> (
    None
):
    simulate = _module()["simulate"]
    result = simulate(
        galleries=1000,
        fixed_seconds=lambda _: 5400,
        gallery_seconds=lambda _: 30,
        strategy="calibrated_two_distinct",
    )
    assert result["fallback_activated_after_batch"] == 2
    assert result["total_catchup_seconds"] == 111000
    assert result["target_missed_batches"] == result["published_batches"] == 15
    assert not result["all_batches_within_target"]
    assert result["controller_event_counts"]["throughput_fallback"] > 0


def test_calibration_failure_is_not_trained_and_restart_discards_old_fit() -> None:
    module = _module()
    controller = module["CalibratedController"](target_seconds=3600, cap=100)
    controller.completed(3, 390, remaining=True)
    before = controller.state()
    controller.failed()
    assert controller.state() == before
    assert controller.event_counts["failed_attempt_not_trained"] == 1
    result = module["simulate"](
        galleries=1000,
        fixed_seconds=lambda _: 300,
        gallery_seconds=lambda _: 30,
        strategy="calibrated_two_distinct",
        failure_attempt=2,
        restart_after_batch=4,
    )
    assert result["failed_attempts"] == 1
    assert [row["count"] for row in result["initial_batches"][4:6]] == [3, 6]
    assert result["controller_event_counts"]["process_restart_resets_controller"] == 1
    assert result["controller_event_counts"]["failed_attempt_not_trained"] == 1


def test_calibration_exposes_positive_fit_ambiguity_and_nonstationary_misses() -> None:
    report = _module()["report"](counterfactual_galleries=100)
    worlds = report["calibration_counterexamples"]["worlds"]
    stationary = worlds["stationary_affine_world"][1]["initial_batches"]
    heterogeneous = worlds["same_first_two_then_heterogeneous_world"][1][
        "initial_batches"
    ]
    assert all(
        stationary[i]["controller"] == heterogeneous[i]["controller"] for i in range(2)
    )
    assert stationary[2]["count"] == heterogeneous[2]["count"] == 12
    assert stationary[2]["elapsed_seconds"] == 1020
    assert heterogeneous[2]["elapsed_seconds"] == 6000
    assert (
        worlds["same_first_two_then_heterogeneous_world"][0]["target_missed_batches"]
        == 0
    )
    assert (
        worlds["same_first_two_then_heterogeneous_world"][1]["target_missed_batches"]
        == 1
    )
    for case, expected_misses in (
        ("cold_start", 1),
        ("huge_first_gallery", 1),
        ("huge_later_gallery", 1),
        ("load_increases", 3),
        ("catalog_fixed_cost_grows", 23),
    ):
        result = report["synthetic_cases"][case][-1]
        assert result["strategy"] == "calibrated_two_distinct"
        assert result["target_missed_batches"] == expected_misses
    one = worlds["only_one_fits_target"][1]
    assert one["target_missed_batches"] == 2  # Initial 3/6 exceed even though n=1 fits.
    assert worlds["only_one_fits_target"][0]["target_missed_batches"] == 0


def test_calibration_preserves_all_48_original_comparator_results() -> None:
    # Captured before this strategy was added: the values must not be derived
    # from the candidate simulation or updated merely to make a change pass.
    expected = {
        "catalog_fixed_cost_grows": (
            (2300.0, 45500.0, 10, 7, 0),
            (360.0, 316885.0, 67, 60, 0),
            (360.0, 108985.0, 27, 20, 0),
            (2300.0, 1602840.0, 364, 343, 0),
        ),
        "cold_start": (
            (10500.0, 40200.0, 10, 1, 0),
            (7590.0, 42600.0, 18, 1, 0),
            (7590.0, 42600.0, 18, 1, 0),
            (7530.0, 40500.0, 11, 1, 0),
        ),
        "failure": (
            (3300.0, 36300.0, 10, 0, 1),
            (390.0, 35280.0, 16, 0, 1),
            (390.0, 35280.0, 16, 0, 1),
            (3300.0, 36300.0, 10, 0, 1),
        ),
        "fixed_cost_exceeds_target": (
            (8400.0, 84000.0, 10, 10, 0),
            (5490.0, 5419200.0, 998, 998, 0),
            (5490.0, 94800.0, 12, 12, 0),
            (5430.0, 5430000.0, 1000, 1000, 0),
        ),
        "fixed_cost_near_target": (
            (6300.0, 63000.0, 10, 10, 0),
            (3390.0, 1132200.0, 334, 0, 0),
            (3390.0, 1132200.0, 334, 0, 0),
            (3600.0, 360000.0, 100, 0, 0),
        ),
        "huge_first_gallery": (
            (9480.0, 30180.0, 10, 1, 0),
            (7540.0, 32580.0, 18, 1, 0),
            (7540.0, 32580.0, 18, 1, 0),
            (7500.0, 30480.0, 11, 1, 0),
        ),
        "huge_later_gallery": (
            (9480.0, 30180.0, 10, 1, 0),
            (360.0, 32280.0, 17, 1, 0),
            (360.0, 32280.0, 17, 1, 0),
            (360.0, 30780.0, 12, 1, 0),
        ),
        "load_increases": (
            (2300.0, 73000.0, 10, 5, 0),
            (360.0, 77500.0, 25, 4, 0),
            (360.0, 77500.0, 25, 4, 0),
            (2300.0, 77200.0, 24, 0, 0),
        ),
        "moderate_fixed_cost": (
            (3600.0, 36000.0, 10, 0, 0),
            (690.0, 39000.0, 15, 0, 0),
            (690.0, 39000.0, 15, 0, 0),
            (3600.0, 36000.0, 10, 0, 0),
        ),
        "no_fixed_cost": (
            (6000.0, 60000.0, 10, 10, 0),
            (180.0, 60000.0, 21, 0, 0),
            (180.0, 60000.0, 21, 0, 0),
            (3600.0, 60000.0, 17, 0, 0),
        ),
        "restart": (
            (3300.0, 33000.0, 10, 0, 0),
            (390.0, 35400.0, 18, 0, 0),
            (390.0, 35400.0, 18, 0, 0),
            (3300.0, 33000.0, 10, 0, 0),
        ),
        "short_tail": (
            (3300.0, 7410.0, 3, 0, 0),
            (390.0, 8610.0, 7, 0, 0),
            (390.0, 8610.0, 7, 0, 0),
            (3300.0, 7410.0, 3, 0, 0),
        ),
    }
    report = _module()["report"](counterfactual_galleries=100)
    fields = (
        "first_usable_seconds",
        "total_catchup_seconds",
        "published_batches",
        "target_missed_batches",
        "failed_attempts",
    )
    strategies = (
        "fixed_100",
        "two_success_throughput",
        "minimum_miss_fallback",
        "known_cost_target_oracle",
    )
    assert len(expected) == 12
    checked = 0
    for name, numbers in expected.items():
        actual = report["synthetic_cases"][name]
        assert len(actual) == 5
        for strategy, row, values in zip(strategies, actual[:4], numbers, strict=True):
            assert row["strategy"] == strategy
            assert tuple(row[field] for field in fields) == values
            checked += 1
    assert checked == 48
