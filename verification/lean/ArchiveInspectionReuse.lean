import Std

/-!
# Single-use archive inspection reuse

The adapter remembers only fully inspected facts after finalizing its
caller-owned, unpublished scratch stream. The next presentation independently
computes size and SHA-256 over the actual bytes and checks exact ordered locators
before reuse.
The retained slot is consumed even when those checks fail; restart is empty.

These theorems concern a pure inspector and mathematical bytes. In particular,
`collisionFreeAt` is an explicit assumption about the compared byte strings;
Lean does not prove SHA-256 collision resistance. The runtime must also ensure
that the read-only core-owned stream does not change during inspection and use.
Core performs before/after archive hashes and exact extent verification; ingest
rehashes cover bytes immediately before decoding them. Python I/O, Pillow full
decoding, thread locks, and process termination are not proved by these theorems.
The implementation refinement is exercised by test_artifact_preparation.py's
byte-for-byte differential, malformed-JPEG, tampering, failed-finalization, and
actual SIGTERM/SIGKILL partial/complete scratch-write restart tests.
-/

namespace H2HDBIngest.Verification.ArchiveInspectionReuse

structure Inspection (Bytes Digest Facts : Type)
    (hash : Bytes → Digest) (inspect : Bytes → Facts) where
  bytes : Bytes
  digest : Digest
  facts : Facts
  computedDigest : hash bytes = digest
  fullyInspected : inspect bytes = facts

/-- No caller digest appears in this predicate: `observed` is read and hashed. -/
def ActualBytesMatch
    {Bytes Digest Facts : Type}
    (hash : Bytes → Digest) (inspect : Bytes → Facts)
    (saved : Inspection Bytes Digest Facts hash inspect)
    (observed : Bytes) : Prop :=
  hash observed = saved.digest

theorem accepted_reuse_equals_full_inspection
    {Bytes Digest Facts : Type}
    (hash : Bytes → Digest) (inspect : Bytes → Facts)
    (saved : Inspection Bytes Digest Facts hash inspect)
    (observed : Bytes)
    (collisionFreeAt : ∀ candidate, hash candidate = hash saved.bytes →
      candidate = saved.bytes)
    (accepted : ActualBytesMatch hash inspect saved observed) :
    inspect observed = saved.facts := by
  have digestEqual : hash observed = hash saved.bytes := by
    exact accepted.trans saved.computedDigest.symm
  rw [collisionFreeAt observed digestEqual]
  exact saved.fullyInspected

theorem changed_digest_cannot_reuse
    {Bytes Digest Facts : Type}
    (hash : Bytes → Digest) (inspect : Bytes → Facts)
    (saved : Inspection Bytes Digest Facts hash inspect)
    (observed : Bytes)
    (changed : hash observed ≠ saved.digest) :
    ¬ ActualBytesMatch hash inspect saved observed := by
  exact changed

/-- The cache has one slot; it cannot retain one entry per gallery. -/
def remember {Entry : Type} (_previous : Option Entry) (verified : Entry) : Option Entry :=
  some verified

def retainedEntries {Entry : Type} : Option Entry → Nat
  | none => 0
  | some _ => 1

theorem retained_entry_count_is_at_most_one
    {Entry : Type} (slot : Option Entry) : retainedEntries slot ≤ 1 := by
  cases slot <;> simp [retainedEntries]

theorem remember_replaces_previous_gallery
    {Entry : Type} (previous : Option Entry) (verified : Entry) :
    remember previous verified = some verified := rfl

/-- Taking the slot occurs under the adapter lock, before I/O or comparison. -/
def take {Entry : Type} (slot : Option Entry) : Option Entry × Option Entry :=
  (slot, none)

theorem reuse_attempt_consumes_slot
    {Entry : Type} (slot : Option Entry) : (take slot).2 = none := rfl

def restart {Entry : Type} (_slot : Option Entry) : Option Entry := none

theorem restart_requires_full_inspection
    {Entry : Type} (slot : Option Entry) : restart slot = none := rfl

/-- A failed scratch write/inspection/flush cannot publish newly inspected facts. -/
def publishAfterFinalization
    {Entry : Type} (previous : Option Entry) (verified : Entry)
    (finalized : Bool) : Option Entry :=
  if finalized then remember previous verified else previous

theorem failed_finalization_does_not_publish_new_facts
    {Entry : Type} (previous : Option Entry) (verified : Entry) :
    publishAfterFinalization previous verified false = previous := by
  simp [publishAfterFinalization]

end H2HDBIngest.Verification.ArchiveInspectionReuse
