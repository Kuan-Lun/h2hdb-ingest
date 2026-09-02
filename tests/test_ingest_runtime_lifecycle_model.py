from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LEAN_MODEL = ROOT / "verification" / "lean" / "IngestRuntimeLifecycle.lean"
TLA_MODEL = ROOT / "verification" / "tla" / "IngestRuntimeLifecycle.tla"
SMALL_PROFILE = ROOT / "verification" / "tla" / "IngestRuntimeLifecycleSmall.cfg"


def test_lean_model_proves_bracketed_idempotent_runtime_close() -> None:
    model = LEAN_MODEL.read_text(encoding="utf-8")
    prose = " ".join(model.split())

    for definition in {
        "close",
        "closeWithDelegateCount",
        "bracketedCliExit",
        "enter",
        "invoke",
        "cleanupConstructionFailure",
    }:
        assert f"def {definition}" in model
    for theorem in {
        "close_is_idempotent",
        "two_linearized_closes_call_delegate_once",
        "every_cli_exit_kind_closes",
        "entered_runtime_rejects_nested_entry",
        "closed_runtime_rejects_reentry",
        "closed_runtime_rejects_later_work",
        "partial_construction_failure_closes_owned_facade",
        "construction_cleanup_is_idempotent",
    }:
        assert f"theorem {theorem}" in model

    assert "do not prove Python lock or context-manager behavior" in prose
    assert "core cache cleanup" in prose
    assert "production code refines this model" not in prose


def test_tla_model_checks_build_cli_close_and_fail_closed_interleavings() -> None:
    model = TLA_MODEL.read_text(encoding="utf-8")
    prose = " ".join(model.split())

    for action in {
        "AllocateFacade",
        "BuildSuccess",
        "BuildFailure",
        "StartCli",
        "FinishCli(kind)",
        "ExplicitClose(actor)",
        "AttemptReentry",
        "AttemptOperation",
    }:
        assert f"{action} ==" in model
    for invariant in {
        "FacadeOwnershipIsExact",
        "DelegateCloseIsExactOnce",
        "ReturnedCloseIsTerminal",
        "EveryCliExitIsClosed",
        "BuildFailureClosesOwnedFacade",
        "ClosedRuntimeNeverReopens",
        "ReentryFailsClosed",
        "OperationFailsClosed",
    }:
        assert f"{invariant} ==" in model

    for exit_kind in {
        "NORMAL",
        "ONCE",
        "EXCEPTION",
        "SYSTEM_EXIT",
        "KEYBOARD_INTERRUPT",
    }:
        assert f'"{exit_kind}"' in model
    assert "abstract atomic linearization point" in prose
    assert "does not prove Python Lock scheduling or waiting" in prose
    assert "core cache behavior" in prose


def test_tla_small_profile_wires_all_runtime_lifecycle_invariants() -> None:
    profile = SMALL_PROFILE.read_text(encoding="utf-8")

    assert "SPECIFICATION Spec" in profile
    assert "DelegateCloseIsExactOnce" in profile
    assert "EveryCliExitIsClosed" in profile
    assert "BuildFailureClosesOwnedFacade" in profile
    assert "ClosedRuntimeNeverReopens" in profile
    assert "ReentryFailsClosed" in profile
    assert "OperationFailsClosed" in profile
    assert "not a Python/core/signal refinement proof" in profile
    assert "PROPERTY" not in profile
