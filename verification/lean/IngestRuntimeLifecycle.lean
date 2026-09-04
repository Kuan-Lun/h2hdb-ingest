import Std

/-!
# Ingest runtime lifecycle

This model describes the small ownership contract at the ingest composition
boundary. Closing an open runtime closes its one owned core ingest facade;
later closes are idempotent. Every modeled CLI body outcome is bracketed by the
same close operation. An entered or closed runtime rejects reentry, and a
closed runtime rejects later work.

For a CBZ-enabled runtime, startup must also pass the ordered database check,
local durable UUID read, and immutable core binding before either maintenance
or writer work is allowed. The filesystem adapter pins both that logical UUID
and the observed root identity. Every later operation boundary must match the
exact pair; changing either member fails closed and remains blocked under
retry.

These theorems are mathematical statements about the functions below. They do
not prove Python lock or context-manager behavior, exception unwinding, signal
delivery, core cache cleanup, object lifetime, a root replacement between one
successful guard and its next POSIX syscall, or that `build_runtime` and the
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

inductive StorageStartupState where
  | unchecked
  | checked
  | identified
  | bound
  | mismatch
deriving DecidableEq, Repr

def checkDatabase : StorageStartupState → StorageStartupState
  | .unchecked => .checked
  | state => state

def readLocalIdentity : StorageStartupState → StorageStartupState
  | .checked => .identified
  | state => state

def bindStorage (sameIdentity : Bool) : StorageStartupState → StorageStartupState
  | .identified => if sameIdentity then .bound else .mismatch
  | .mismatch => .mismatch
  | state => state

def verifyCycleIdentity (sameIdentity : Bool) :
    StorageStartupState → StorageStartupState
  | .bound => if sameIdentity then .bound else .mismatch
  | .mismatch => .mismatch
  | state => state

inductive WriterResult where
  | allowed
  | rejected
deriving DecidableEq, Repr

def runMaintenance : StorageStartupState → WriterResult
  | .bound => .allowed
  | _ => .rejected

def runWriterWork : StorageStartupState → WriterResult
  | .bound => .allowed
  | _ => .rejected

theorem writer_work_rejected_until_storage_is_bound
    (state : StorageStartupState)
    (unbound : state ≠ .bound) :
    runWriterWork state = .rejected := by
  cases state <;> simp_all [runWriterWork]

theorem maintenance_rejected_until_storage_is_bound
    (state : StorageStartupState)
    (unbound : state ≠ .bound) :
    runMaintenance state = .rejected := by
  cases state <;> simp_all [runMaintenance]

theorem ordered_matching_startup_enables_maintenance_and_work :
    let state := bindStorage true (readLocalIdentity (checkDatabase .unchecked))
    runMaintenance state = .allowed ∧ runWriterWork state = .allowed := by
  decide

theorem binding_mismatch_remains_blocked :
    let state := bindStorage false (readLocalIdentity (checkDatabase .unchecked))
    bindStorage true state = .mismatch ∧
      runMaintenance state = .rejected ∧
      runWriterWork state = .rejected := by
  decide

theorem matching_cycle_identity_preserves_work_authority :
    let state := bindStorage true (readLocalIdentity (checkDatabase .unchecked))
    verifyCycleIdentity true state = .bound ∧
      runMaintenance (verifyCycleIdentity true state) = .allowed ∧
      runWriterWork (verifyCycleIdentity true state) = .allowed := by
  decide

theorem replaced_root_is_rejected_before_maintenance_and_work :
    let state := bindStorage true (readLocalIdentity (checkDatabase .unchecked))
    verifyCycleIdentity false state = .mismatch ∧
      runMaintenance (verifyCycleIdentity false state) = .rejected ∧
      runWriterWork (verifyCycleIdentity false state) = .rejected := by
  decide

structure StorageObservation where
  storageUuid : Nat
  rootIdentity : Nat
deriving DecidableEq, Repr

inductive StorageGuardState where
  | pinned (expected : StorageObservation)
  | mismatch
deriving DecidableEq, Repr

def verifyPinnedStorage (observed : StorageObservation) :
    StorageGuardState → StorageGuardState
  | .pinned expected =>
      if observed = expected then .pinned expected else .mismatch
  | .mismatch => .mismatch

def runGuardedWrite : StorageGuardState → WriterResult
  | .pinned _ => .allowed
  | .mismatch => .rejected

theorem exact_pinned_storage_pair_preserves_write_authority
    (expected : StorageObservation) :
    runGuardedWrite (verifyPinnedStorage expected (.pinned expected)) =
      .allowed := by
  simp [verifyPinnedStorage, runGuardedWrite]

theorem same_uuid_different_root_is_rejected :
    let expected : StorageObservation := ⟨1, 10⟩
    let replacement : StorageObservation := ⟨1, 20⟩
    verifyPinnedStorage replacement (.pinned expected) = .mismatch ∧
      runGuardedWrite (verifyPinnedStorage replacement (.pinned expected)) =
        .rejected := by
  decide

theorem same_root_different_uuid_is_rejected :
    let expected : StorageObservation := ⟨1, 10⟩
    let corrupted : StorageObservation := ⟨2, 10⟩
    verifyPinnedStorage corrupted (.pinned expected) = .mismatch ∧
      runGuardedWrite (verifyPinnedStorage corrupted (.pinned expected)) =
        .rejected := by
  decide

theorem pinned_storage_mismatch_remains_blocked_under_retry :
    let expected : StorageObservation := ⟨1, 10⟩
    let replacement : StorageObservation := ⟨1, 20⟩
    verifyPinnedStorage expected
        (verifyPinnedStorage replacement (.pinned expected)) = .mismatch ∧
      runGuardedWrite
          (verifyPinnedStorage expected
            (verifyPinnedStorage replacement (.pinned expected))) =
        .rejected := by
  decide

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
