import Std

/-!
# Ingest runtime lifecycle

This model describes the small ownership contract at the ingest composition
boundary. Closing an open runtime closes its one owned core ingest facade;
later closes are idempotent. Every modeled CLI body outcome is bracketed by the
same close operation. An entered or closed runtime rejects reentry, and a
closed runtime rejects later work.

These theorems are mathematical statements about the functions below. They do
not prove Python lock or context-manager behavior, exception unwinding, signal
delivery, core cache cleanup, object lifetime, or that `build_runtime` and the
two CLI implementations refine the model. Runtime, concurrency, construction-
fault, and CLI tests remain required evidence for those boundaries.
-/

namespace H2HDBIngest.Verification.IngestRuntimeLifecycle

inductive RuntimeState where
  | ready
  | entered
  | closed
deriving DecidableEq, Repr

def close : RuntimeState → RuntimeState
  | .ready => .closed
  | .entered => .closed
  | .closed => .closed

theorem close_is_idempotent (state : RuntimeState) :
    close (close state) = close state := by
  cases state <;> rfl

def closeWithDelegateCount : RuntimeState × Nat → RuntimeState × Nat
  | (.ready, count) => (.closed, count + 1)
  | (.entered, count) => (.closed, count + 1)
  | (.closed, count) => (.closed, count)

theorem two_linearized_closes_call_delegate_once :
    closeWithDelegateCount (closeWithDelegateCount (.ready, 0)) =
      (.closed, 1) := by
  rfl

inductive CliExitKind where
  | normal
  | once
  | exception
  | systemExit
  | keyboardInterrupt
deriving DecidableEq, Repr

def bracketedCliExit (_kind : CliExitKind) (state : RuntimeState) :
    RuntimeState :=
  close state

theorem every_cli_exit_kind_closes
    (kind : CliExitKind)
    (state : RuntimeState) :
    bracketedCliExit kind state = .closed := by
  cases state <;> rfl

inductive RuntimeEntryResult where
  | entered
  | rejected
deriving DecidableEq, Repr

def enter : RuntimeState → RuntimeEntryResult
  | .ready => .entered
  | .entered => .rejected
  | .closed => .rejected

def invoke : RuntimeState → RuntimeEntryResult
  | .ready => .entered
  | .entered => .entered
  | .closed => .rejected

theorem entered_runtime_rejects_nested_entry : enter .entered = .rejected := by
  rfl

theorem closed_runtime_rejects_reentry : enter .closed = .rejected := by
  rfl

theorem closed_runtime_rejects_later_work : invoke .closed = .rejected := by
  rfl

inductive FacadeOwnership where
  | absent
  | ownedOpen
  | ownedClosed
deriving DecidableEq, Repr

def cleanupConstructionFailure : FacadeOwnership → FacadeOwnership
  | .absent => .absent
  | .ownedOpen => .ownedClosed
  | .ownedClosed => .ownedClosed

theorem partial_construction_failure_closes_owned_facade :
    cleanupConstructionFailure .ownedOpen = .ownedClosed := by
  rfl

theorem construction_cleanup_is_idempotent (state : FacadeOwnership) :
    cleanupConstructionFailure (cleanupConstructionFailure state) =
      cleanupConstructionFailure state := by
  cases state <;> rfl

end H2HDBIngest.Verification.IngestRuntimeLifecycle
