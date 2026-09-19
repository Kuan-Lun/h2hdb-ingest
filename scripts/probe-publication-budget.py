"""Deterministic publication-budget counterexamples; not runtime or NAS evidence.

Compare fixed 100 with two-success throughput feedback and an explicit fallback
that abandons the interval target after a minimum batch misses it. A second
controller calibrates with published model batches of 3 and 6, then fits two
distinct-count observations without claiming verified stationarity. Hidden fixed
cost and per-gallery cost belong to the synthetic oracle, never the controller.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter, deque
from collections.abc import Callable
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Literal, NotRequired, TypedDict, Unpack

Strategy = Literal[
    "fixed_100",
    "two_success_throughput",
    "minimum_miss_fallback",
    "known_cost_target_oracle",
    "calibrated_two_distinct",
]
_STRATEGIES: tuple[Strategy, ...] = (
    "fixed_100",
    "two_success_throughput",
    "minimum_miss_fallback",
    "known_cost_target_oracle",
    "calibrated_two_distinct",
)


class _Workload(TypedDict):
    galleries: int
    fixed_seconds: Callable[[int], float]
    gallery_seconds: Callable[[int], float]
    target_seconds: NotRequired[float]
    cold_start_seconds: NotRequired[float]
    failure_attempt: NotRequired[int | None]
    restart_after_batch: NotRequired[int | None]


class ThroughputController:
    """Experimental controller using only successful batch count and wall time."""

    def __init__(self, *, target_seconds: float, cap: int, fallback: bool) -> None:
        if target_seconds <= 0 or not math.isfinite(target_seconds) or cap < 1:
            raise ValueError("invalid controller target or cap")
        self.target = target_seconds
        self.cap = cap
        self.limit = min(3, cap)
        self.fallback = fallback
        self.fallback_active = False
        self.samples: deque[tuple[int, float]] = deque(maxlen=2)

    def completed(self, count: int, elapsed: float, *, remaining: bool) -> None:
        if count <= 0 or elapsed <= 0 or not math.isfinite(elapsed):
            raise ValueError("positive completed work and elapsed are required")
        self.samples.append((count, elapsed))
        if self.fallback and count == 1 and remaining and elapsed > self.target:
            # This proves the observed minimum batch missed, not that future
            # fixed cost is irreducible. Fallback deliberately changes priority.
            self.fallback_active = True
        if self.fallback_active:
            self.limit = self.cap
            return
        estimate = math.floor(
            self.target
            * sum(n for n, _ in self.samples)
            / sum(seconds for _, seconds in self.samples)
        )
        self.limit = max(1, min(self.cap, self.limit * 2, estimate))

    def failed(self) -> None:
        self.limit = max(1, self.limit // 2)
        # An uncommitted attempt is not a successful throughput observation.


@dataclass(frozen=True, slots=True)
class _Observation:
    count: int
    elapsed: float


class CalibratedController:
    """Only count/elapsed observations enter; model constants remain in simulator.

    A fit is conditional on stationarity, never identified physical ground truth.
    Keep newest observations at two distinct counts. Four repeated equal-count
    successes request a target-safe neighboring-count probe. When only one
    count fits, keep the model provisional without forcing an unsafe probe.
    """

    def __init__(self, *, target_seconds: float, cap: int) -> None:
        if target_seconds <= 0 or not math.isfinite(target_seconds) or cap < 1:
            raise ValueError("invalid controller target or cap")
        self.target = target_seconds
        self.cap = cap
        self.limit = min(3, cap)
        self.samples: deque[_Observation] = deque(maxlen=2)
        self.fit: tuple[float, float] | None = None
        self.fit_validations = 0
        self.fallback_active = False
        self.fallback_reason: str | None = None
        self.successes = 0
        self.last_count: int | None = None
        self.equal_count_run = 0
        self.events: deque[dict[str, object]] = deque(maxlen=32)
        self.event_counts: Counter[str] = Counter()
        self.status = "cold_calibration"

    def failed(self) -> None:
        # Failure supplies no successful cost datum. Preserve the bounded
        # calibration plan; the simulator reports failed elapsed in user gaps.
        self._event("failed_attempt_not_trained")

    def _event(self, event: str, **values: object) -> None:
        self.event_counts[event] += 1
        self.events.append({"event": event, "after_success": self.successes, **values})

    def _unknown_exploration(self, sample: _Observation) -> None:
        self.fit = None
        self.fit_validations = 0
        self.samples.clear()
        self.samples.append(sample)
        if sample.count == 1 and sample.elapsed > self.target:
            self.fallback_active = True
            self.fallback_reason = "observed_minimum_miss_not_proof_of_irreducible_F"
            self._event("throughput_fallback", reason=self.fallback_reason)
        if self.fallback_active or sample.elapsed <= self.target:
            self.limit = min(self.cap, max(sample.count + 1, sample.count * 2))
        else:
            self.limit = max(1, sample.count // 2)
        if self.limit == sample.count and self.cap > 1:
            self.limit = sample.count - 1 if sample.count > 1 else 2
        self.status = "unknown_bounded_exploration"

    def completed(self, count: int, elapsed: float, *, remaining: bool) -> None:
        if count < 1 or elapsed <= 0 or not math.isfinite(elapsed):
            raise ValueError("invalid successful observation")
        self.successes += 1
        current = _Observation(count, elapsed)
        self.equal_count_run = (
            self.equal_count_run + 1 if count == self.last_count else 1
        )
        self.last_count = count
        if not remaining:
            self.status = "complete"
            return
        if self.successes == 1:
            self.samples.append(current)
            self.limit = min(6, self.cap)
            self.status = "cold_calibration_second_distinct_count"
            self._event(
                "initial_published_model_calibration",
                count=count,
                next_limit=self.limit,
            )
            return
        if self.fit is not None:
            old_fixed, old_variable = self.fit
            prediction = old_fixed + old_variable * count
            error = elapsed - prediction
            tolerance = 0.10 * max(self.target, elapsed, prediction)
            if abs(error) > tolerance:
                self._event(
                    "prior_fit_inconsistent",
                    observed_seconds=elapsed,
                    predicted_seconds=prediction,
                    residual_seconds=error,
                    tolerance_seconds=tolerance,
                )
                self.fallback_active = False
                self.fallback_reason = None
                self._unknown_exploration(current)
                return
            self.fit_validations += 1
        if self.samples and self.samples[-1].count == count:
            self.samples[-1] = current
        else:
            self.samples.append(current)
        if len(self.samples) < 2:
            self._event("equal_count_not_identifiable", count=count)
            self._unknown_exploration(current)
            return
        first, second = self.samples
        if first.count == second.count:
            raise AssertionError("sample retention must use distinct counts")
        variable = (second.elapsed - first.elapsed) / (second.count - first.count)
        fixed = second.elapsed - variable * second.count
        # F=0 is the valid boundary no-fixed-cost model; negative F is unknown.
        if (
            not math.isfinite(fixed)
            or not math.isfinite(variable)
            or variable <= 0
            or fixed < 0
        ):
            self._event(
                "invalid_affine_fit_unknown",
                fitted_F=fixed,
                fitted_v=variable,
                reason="v_nonpositive_or_F_negative_or_nonfinite",
            )
            self._unknown_exploration(current)
            return
        self.fit = (fixed, variable)
        feasible = math.floor((self.target - fixed) / variable)
        if feasible < 1:
            self.fallback_active = True
            self.fallback_reason = (
                "conditional_affine_model_has_no_feasible_positive_count"
            )
            self.limit = min(self.cap, max(count + 1, count * 2))
            self.status = "throughput_fallback_target_infeasible_under_fit"
            self._event(
                "throughput_fallback",
                fitted_F=fixed,
                fitted_v=variable,
                reason=self.fallback_reason,
                stationarity_verified=False,
            )
        else:
            if self.fallback_active:
                self._event(
                    "throughput_fallback_cleared", fitted_F=fixed, fitted_v=variable
                )
            self.fallback_active = False
            self.fallback_reason = None
            self.limit = max(1, min(self.cap, count * 2, feasible))
            self.status = "conditional_affine_prediction"
        if self.equal_count_run >= 4 and self.cap > 1:
            old_limit = self.limit
            # A downward neighboring-count probe cannot make a stationary
            # positive-v feasible batch infeasible; also works at the cap.
            if (
                count == 1
                and not self.fallback_active
                and fixed + 2 * variable > self.target
            ):
                # A known conditional fit with n=1 as its only feasible choice
                # has no distinct-count probe that preserves that same target.
                # Keep the fit provisional; do not manufacture avoidable misses.
                self._event(
                    "no_target_safe_distinct_probe",
                    repeated_count=count,
                    stationarity_verified=False,
                )
            else:
                self.limit = (
                    max(1, min(self.limit, count - 1))
                    if count > 1
                    else min(self.cap, 2)
                )
                self._event(
                    "bounded_distinct_count_probe",
                    repeated_count=count,
                    old_limit=old_limit,
                    next_limit=self.limit,
                )
        self._event(
            "affine_fit",
            fitted_F=fixed,
            fitted_v=variable,
            sample_counts=[first.count, second.count],
            sample_seconds=[first.elapsed, second.elapsed],
            stationarity_verified=False,
        )

    def state(self) -> dict[str, object]:
        return {
            "next_limit": self.limit,
            "status": self.status,
            "fit": None if self.fit is None else {"F": self.fit[0], "v": self.fit[1]},
            "fit_validations": self.fit_validations,
            "stationarity_verified": False,
            "fallback": self.fallback_active,
            "fallback_reason": self.fallback_reason,
            "samples": [{"n": x.count, "seconds": x.elapsed} for x in self.samples],
        }


def _controller(
    strategy: Strategy, target_seconds: float
) -> ThroughputController | CalibratedController:
    if strategy == "calibrated_two_distinct":
        return CalibratedController(target_seconds=target_seconds, cap=100)
    return ThroughputController(
        target_seconds=target_seconds,
        cap=100,
        fallback=strategy == "minimum_miss_fallback",
    )


def simulate(
    *,
    galleries: int,
    fixed_seconds: Callable[[int], float],
    gallery_seconds: Callable[[int], float],
    strategy: Strategy,
    target_seconds: float = 3600,
    cold_start_seconds: float = 0,
    failure_attempt: int | None = None,
    restart_after_batch: int | None = None,
) -> dict[str, object]:
    if galleries < 1 or strategy not in _STRATEGIES:
        raise ValueError("invalid workload or strategy")
    controller = _controller(strategy, target_seconds)
    controller_events: Counter[str] = Counter()
    publication_gap = 0.0
    publication_gap_misses = 0
    published = 0
    total = 0.0
    batches = 0
    attempts = 0
    misses = 0
    minimum_misses = 0
    failures = 0
    first_usable = None
    fallback_at = None
    first_rows: list[dict[str, object]] = []
    tail: deque[dict[str, object]] = deque(maxlen=8)
    largest = 0.0
    while published < galleries:
        attempts += 1
        limit = 100 if strategy == "fixed_100" else controller.limit
        count = min(limit, galleries - published)
        fixed = fixed_seconds(published)
        if strategy == "known_cost_target_oracle":
            # This oracle knows future costs. It is a feasibility comparator,
            # never an implementable controller based on the same observations.
            budget = (
                target_seconds - fixed - (cold_start_seconds if attempts == 1 else 0)
            )
            running = 0.0
            admitted = 0
            for position in range(published, min(galleries, published + 100)):
                running += gallery_seconds(position)
                if running > budget:
                    break
                admitted += 1
            count = max(1, admitted)
        variable = sum(
            gallery_seconds(position)
            for position in range(published, published + count)
        )
        elapsed = fixed + variable + (cold_start_seconds if attempts == 1 else 0)
        if not (
            elapsed > 0 and math.isfinite(elapsed) and fixed >= 0 and variable >= 0
        ):
            raise ValueError("invalid workload cost")
        total += elapsed
        publication_gap += elapsed
        largest = max(largest, elapsed)
        if failure_attempt == attempts:
            controller.failed()
            failures += 1
            continue
        published += count
        batches += 1
        if first_usable is None:
            first_usable = total
        missed = elapsed > target_seconds
        publication_gap_misses += int(publication_gap > target_seconds)
        misses += int(missed)
        minimum_misses += int(missed and count == 1)
        controller.completed(count, elapsed, remaining=published < galleries)
        if fallback_at is None and controller.fallback_active:
            fallback_at = batches
        row: dict[str, object] = {
            "batch": batches,
            "count": count,
            "published": published,
            "elapsed_seconds": elapsed,
            "fixed_seconds_oracle": fixed,
            "variable_seconds_oracle": variable,
            "target_missed": missed,
            "next_limit": (
                None
                if strategy == "known_cost_target_oracle"
                else 100
                if strategy == "fixed_100"
                else controller.limit
            ),
            "fallback_active": controller.fallback_active,
        }
        if isinstance(controller, CalibratedController):
            row["controller"] = controller.state()
            row["publication_gap_seconds"] = publication_gap
        publication_gap = 0.0
        if len(first_rows) < 8:
            first_rows.append(row)
        tail.append(row)
        if restart_after_batch == batches:
            if isinstance(controller, CalibratedController):
                controller_events.update(controller.event_counts)
                controller_events["process_restart_resets_controller"] += 1
            controller = _controller(strategy, target_seconds)
    result: dict[str, object] = {
        "strategy": strategy,
        "galleries": galleries,
        "target_seconds": target_seconds,
        "first_usable_seconds": first_usable,
        "total_catchup_seconds": total,
        "published_batches": batches,
        "attempts": attempts,
        "failed_attempts": failures,
        "target_missed_batches": misses,
        "target_miss_fraction": misses / batches,
        "minimum_batch_misses": minimum_misses,
        "known_cost_oracle": strategy == "known_cost_target_oracle",
        "all_batches_within_target": misses == 0,
        "oracle_constraint_infeasible_batches": (
            minimum_misses if strategy == "known_cost_target_oracle" else None
        ),
        "largest_attempt_seconds": largest,
        "fallback_activated_after_batch": fallback_at,
        "initial_batches": first_rows,
        "final_batches": list(tail),
    }
    if isinstance(controller, CalibratedController):
        controller_events.update(controller.event_counts)
        result["controller_event_counts"] = dict(controller_events)
        result["last_controller_events"] = list(controller.events)
        result["publication_gap_misses_including_failures"] = publication_gap_misses
    return result


def _strategies(**workload: Unpack[_Workload]) -> list[dict[str, object]]:
    return [simulate(strategy=strategy, **workload) for strategy in _STRATEGIES]


def _identifiability() -> dict[str, object]:
    return {
        "two_equal_counts": {
            "observations": [
                {"n": 100, "seconds": 10800},
                {"n": 100, "seconds": 10800},
            ],
            "indistinguishable_models": [
                {"fixed_seconds": fixed, "per_gallery_seconds": (10800 - fixed) / 100}
                for fixed in (0, 1800, 3600, 5400, 7200)
            ],
            "identifies_fixed_cost": False,
        },
        "two_different_counts": {
            "observations": [{"n": 3, "seconds": 480}, {"n": 6, "seconds": 660}],
            "stationary_affine_solution": {
                "fixed_seconds": 300,
                "per_gallery_seconds": 60,
            },
            "stationarity_verified": False,
            "counterexample": "F=0 with per-gallery costs 160 then 110 produces the same two observations",
        },
    }


def _calibration_counterexamples() -> dict[str, object]:
    worlds: dict[str, _Workload] = {
        "stationary_affine_world": {
            "galleries": 100,
            "fixed_seconds": lambda _: 300,
            "gallery_seconds": lambda _: 60,
        },
        "same_first_two_then_heterogeneous_world": {
            "galleries": 100,
            "fixed_seconds": lambda _: 0,
            "gallery_seconds": lambda n: 160 if n < 3 else 110 if n < 9 else 500,
        },
        "only_one_fits_target": {
            "galleries": 25,
            "fixed_seconds": lambda _: 3300,
            "gallery_seconds": lambda _: 200,
        },
    }
    return {
        "worlds": {
            name: [
                simulate(strategy=strategy, **workload)
                for strategy in ("known_cost_target_oracle", "calibrated_two_distinct")
            ]
            for name, workload in worlds.items()
        },
        "positive_fit_ambiguity": {
            "same_first_two_observations": [
                {"n": 3, "seconds": 480},
                {"n": 6, "seconds": 660},
            ],
            "same_conditional_fit": {"F": 300, "v": 60},
            "same_next_selected_count": 12,
            "stationary_world_next_seconds": 1020,
            "heterogeneous_world_next_seconds": 6000,
            "controller_can_identify_world_from_these_samples": False,
        },
    }


def _constant(cost: float) -> Callable[[int], float]:
    def at_position(_position: int) -> float:
        return cost

    return at_position


def report(*, counterfactual_galleries: int = 127000) -> dict[str, object]:
    scenarios: dict[str, _Workload] = {
        "no_fixed_cost": {
            "galleries": 1000,
            "fixed_seconds": lambda _: 0,
            "gallery_seconds": lambda _: 60,
        },
        "moderate_fixed_cost": {
            "galleries": 1000,
            "fixed_seconds": lambda _: 600,
            "gallery_seconds": lambda _: 30,
        },
        "fixed_cost_near_target": {
            "galleries": 1000,
            "fixed_seconds": lambda _: 3300,
            "gallery_seconds": lambda _: 30,
        },
        "fixed_cost_exceeds_target": {
            "galleries": 1000,
            "fixed_seconds": lambda _: 5400,
            "gallery_seconds": lambda _: 30,
        },
        "cold_start": {
            "galleries": 1000,
            "fixed_seconds": lambda _: 300,
            "gallery_seconds": lambda _: 30,
            "cold_start_seconds": 7200,
        },
        "huge_first_gallery": {
            "galleries": 1000,
            "fixed_seconds": lambda _: 300,
            "gallery_seconds": lambda n: 7200 if n == 0 else 20,
        },
        "huge_later_gallery": {
            "galleries": 1000,
            "fixed_seconds": lambda _: 300,
            "gallery_seconds": lambda n: 7200 if n == 3 else 20,
        },
        "load_increases": {
            "galleries": 1000,
            "fixed_seconds": lambda _: 300,
            "gallery_seconds": lambda n: 20 if n < 500 else 120,
        },
        "catalog_fixed_cost_grows": {
            "galleries": 1000,
            "fixed_seconds": lambda n: 300 + 5 * n,
            "gallery_seconds": lambda _: 20,
        },
        "short_tail": {
            "galleries": 217,
            "fixed_seconds": lambda _: 300,
            "gallery_seconds": lambda _: 30,
        },
        "failure": {
            "galleries": 1000,
            "fixed_seconds": lambda _: 300,
            "gallery_seconds": lambda _: 30,
            "failure_attempt": 2,
        },
        "restart": {
            "galleries": 1000,
            "fixed_seconds": lambda _: 300,
            "gallery_seconds": lambda _: 30,
            "restart_after_batch": 4,
        },
    }
    counterfactual = []
    for fixed in (0, 1800, 3600, 5400, 7200):
        per_gallery = (10800 - fixed) / 100
        counterfactual.append(
            {
                "assumed_fixed_seconds": fixed,
                "assumed_per_gallery_seconds": per_gallery,
                "fits_observed_100_gallery_seconds": 10800,
                "results": _strategies(
                    galleries=counterfactual_galleries,
                    fixed_seconds=_constant(fixed),
                    gallery_seconds=_constant(per_gallery),
                ),
            }
        )
    return {
        "status": "completed",
        "format_version": 2,
        "probe_sha256": sha256(Path(__file__).read_bytes()).hexdigest(),
        "evidence_kind": "deterministic mathematical simulation, not measured execution time",
        "model": "T(batch)=fixed_cost(already_published)+sum(gallery_cost)+optional cold_start; serial whole-batch publication",
        "controller_inputs": "Feedback controllers receive only successful count, elapsed, remaining-work flag and failure event; no hidden fixed cost, per-gallery cost or future workload",
        "oracle_inputs": "known_cost_target_oracle knows fixed cost and future gallery costs and chooses the largest feasible batch up to 100; it is not a deployable controller",
        "comparison_rule": "Compare target-feasible controllers against the same-target oracle; fixed_100 catch-up is a separate throughput reference that may violate the target",
        "controller_priority": "minimum_miss_fallback explicitly abandons the one-hour target and resumes cap=100 after a one-gallery miss",
        "synthetic_cases": {
            name: _strategies(**workload) for name, workload in scenarios.items()
        },
        "identifiability": _identifiability(),
        "calibration_counterexamples": _calibration_counterexamples(),
        "calibrated_policy": {
            "initial_published_model_counts": [3, 6],
            "retained_samples": "latest successes at two distinct counts",
            "max_growth_factor": 2,
            "cap": 100,
            "same_count_exploration_after": 4,
            "consistency_tolerance_fraction": 0.10,
            "consistency_tolerance_denominator": "max(target, observed, predicted)",
            "invalid_fit": "nonfinite values, v<=0 or F<0 remain unknown; F=0 is a valid boundary model",
            "stationarity_verified": False,
            "fallback": "conditional no-feasible-count model or observed minimum miss switches to explicit throughput priority",
            "trace_bound": 32,
        },
        "nas_counterfactual": {
            "observed_anchor": "Previously analyzed round 82: 100 additions, rounded whole-turn total 10800 seconds",
            "assumed_remaining_galleries": counterfactual_galleries,
            "scope": "Sensitivity only: equal-size NAS rounds cannot identify F/v; no forecast, extrapolation validation or deployment evidence",
            "models": counterfactual,
        },
        "limits": [
            "First usable means this model's first complete batch; actual OPDS correctness still requires runtime integration",
            "No runtime/config change implements any controller in this experiment",
            "A one-gallery miss does not prove future fixed cost is irreducible; huge gallery and cold start can cause it",
            "Fallback improves modeled catch-up in high fixed cost but violates the user's interval objective",
            "Two samples cannot distinguish fixed cost from changing image sizes, host load or catalog growth",
            "Only naive total/n feedback is rejected by its counterexamples; distinct-count calibration can avoid the stationary fixed-cost trap",
            "Positive two-point F/v fits do not prove stationarity; cold start, heterogeneous galleries and changed load can cause feasible-deadline misses",
            "Calibration and neighboring-count exploration add real modeled publication overhead; a one-count-only feasible model has no safe distinct-count probe",
            "The oracle greedily selects the largest feasible next batch; it is not a global completion-time optimality proof under arbitrary future failures",
            "Successful-batch target misses exclude failed attempts; calibrated publication_gap_misses_including_failures additionally reports them",
            "These modeled times are not benchmark wall time",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--counterfactual-galleries", type=int, default=127000)
    args = parser.parse_args()
    if not 1 <= args.counterfactual_galleries <= 200000:
        parser.error("counterfactual galleries must be 1..200000")
    with args.output.open("x") as stream:
        json.dump(
            report(counterfactual_galleries=args.counterfactual_galleries),
            stream,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        stream.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
