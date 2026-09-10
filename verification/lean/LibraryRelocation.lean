import Std

/-!
# Explicit library relocation authority

The current-format journal is an input assumption throughout this model.
The immutable UUID, resource key and independently computed digest identify
the logical artifact. A relocation session may replace physical observations
only after a new, stable byte traversal agrees with that digest and the exact
session and previous journal row still match. Normal runtime work remains
blocked while that durable maintenance session is active.

The predicates below explicitly assume successful independent hashing and
stable descriptor/name observations. They do not prove collision resistance,
POSIX locks, filesystem durability, SQLite isolation, complete journal-directed
inventory, or that production code establishes those premises. The finite TLA+ model and
runtime fault, tamper, lock and replay tests supply separate evidence.
-/

namespace H2HDBIngest.Verification.LibraryRelocation

structure Authority where
  libraryUuid : Nat
  resourceKey : Nat
  digest : Nat
  physicalIdentity : Nat
deriving DecidableEq, Repr

structure Observation where
  recomputedDigest : Nat
  physicalIdentity : Nat
  stable : Bool
deriving DecidableEq, Repr

structure Session where
  token : Nat
  active : Bool
deriving DecidableEq, Repr

def canRebind (session : Session) (suppliedToken : Nat)
    (expected loaded : Authority) (observed : Observation) : Bool :=
  session.active && decide (session.token = suppliedToken) &&
    decide (expected = loaded) && observed.stable &&
    decide (observed.recomputedDigest = expected.digest)

def rebind (session : Session) (suppliedToken : Nat)
    (expected loaded : Authority) (observed : Observation) : Option Authority :=
  if canRebind session suppliedToken expected loaded observed then
    some { expected with physicalIdentity := observed.physicalIdentity }
  else none

theorem accepted_rebinding_preserves_logical_authority
    (session : Session) (suppliedToken : Nat)
    (expected loaded result : Authority) (observed : Observation)
    (accepted : rebind session suppliedToken expected loaded observed = some result) :
    result.libraryUuid = expected.libraryUuid ∧
      result.resourceKey = expected.resourceKey ∧
      result.digest = expected.digest := by
  unfold rebind at accepted
  split at accepted
  · cases accepted
    exact ⟨rfl, rfl, rfl⟩
  · contradiction

theorem inactive_session_cannot_rebind
    (session : Session) (suppliedToken : Nat)
    (expected loaded : Authority) (observed : Observation)
    (inactive : session.active = false) :
    rebind session suppliedToken expected loaded observed = none := by
  simp [rebind, canRebind, inactive]

theorem changed_bytes_cannot_rebind
    (session : Session) (suppliedToken : Nat)
    (expected loaded : Authority) (observed : Observation)
    (changed : observed.recomputedDigest ≠ expected.digest) :
    rebind session suppliedToken expected loaded observed = none := by
  simp [rebind, canRebind, changed]

theorem stale_session_token_cannot_rebind
    (session : Session) (suppliedToken : Nat)
    (expected loaded : Authority) (observed : Observation)
    (stale : session.token ≠ suppliedToken) :
    rebind session suppliedToken expected loaded observed = none := by
  simp [rebind, canRebind, stale]

theorem stale_row_cannot_rebind
    (session : Session) (suppliedToken : Nat)
    (expected loaded : Authority) (observed : Observation)
    (changed : expected ≠ loaded) :
    rebind session suppliedToken expected loaded observed = none := by
  simp [rebind, canRebind, changed]

theorem unstable_observation_cannot_rebind
    (session : Session) (suppliedToken : Nat)
    (expected loaded : Authority) (observed : Observation)
    (unstable : observed.stable = false) :
    rebind session suppliedToken expected loaded observed = none := by
  simp [rebind, canRebind, unstable]

def normalRuntimeAllowed (session : Session) : Bool := !session.active

theorem unfinished_relocation_blocks_normal_runtime (session : Session)
    (active : session.active = true) : normalRuntimeAllowed session = false := by
  simp [normalRuntimeAllowed, active]

structure VerifiedResource where
  expectedDigest : Nat
  recomputedDigest : Nat
  digestExact : recomputedDigest = expectedDigest

theorem every_completed_resource_has_recomputed_digest
    (resources : List VerifiedResource) :
    ∀ resource ∈ resources, resource.recomputedDigest = resource.expectedDigest := by
  intro resource _
  exact resource.digestExact

end H2HDBIngest.Verification.LibraryRelocation
