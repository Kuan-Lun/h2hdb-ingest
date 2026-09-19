"""Deterministic publication-budget counterexamples; not runtime or NAS evidence.

Compare fixed 100 with two-success throughput feedback and an explicit fallback
that abandons the interval target after a minimum batch misses it. Hidden fixed
cost and per-gallery cost belong to the synthetic oracle, never the controller.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import deque
from collections.abc import Callable
from hashlib import sha256
from pathlib import Path
from typing import Literal, NotRequired, TypedDict, Unpack

Strategy = Literal[
    "fixed_100",
    "two_success_throughput",
    "minimum_miss_fallback",
    "known_cost_target_oracle",
]
_STRATEGIES: tuple[Strategy, ...] = (
    "fixed_100",
    "two_success_throughput",
    "minimum_miss_fallback",
    "known_cost_target_oracle",
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
    controller = ThroughputController(
        target_seconds=target_seconds,
        cap=100,
        fallback=strategy == "minimum_miss_fallback",
    )
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
        if len(first_rows) < 8:
            first_rows.append(row)
        tail.append(row)
        if restart_after_batch == batches:
            controller = ThroughputController(
                target_seconds=target_seconds,
                cap=100,
                fallback=strategy == "minimum_miss_fallback",
            )
    return {
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
        "format_version": 1,
        "probe_sha256": sha256(Path(__file__).read_bytes()).hexdigest(),
        "evidence_kind": "deterministic mathematical simulation, not measured execution time",
        "model": "T(batch)=fixed_cost(already_published)+sum(gallery_cost)+optional cold_start; serial whole-batch publication",
        "controller_inputs": "Two-success and fallback controllers receive only successful count and elapsed, not hidden fixed cost",
        "oracle_inputs": "known_cost_target_oracle knows fixed cost and future gallery costs and chooses the largest feasible batch up to 100; it is not a deployable controller",
        "comparison_rule": "Compare target-feasible controllers against the same-target oracle; fixed_100 catch-up is a separate throughput reference that may violate the target",
        "controller_priority": "minimum_miss_fallback explicitly abandons the one-hour target and resumes cap=100 after a one-gallery miss",
        "synthetic_cases": {
            name: _strategies(**workload) for name, workload in scenarios.items()
        },
        "identifiability": _identifiability(),
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
