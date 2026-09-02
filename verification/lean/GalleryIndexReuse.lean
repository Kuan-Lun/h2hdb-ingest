import Std

/-!
# Gallery-scoped immutable source index

This file models the pure part of reusing one gallery index.  A legacy keyset
page and an indexed keyset page both use the same explicit strict-after
decision to select entries after an optional key, retain at most `limit + 1`
candidates to determine the terminal flag, and return at most `limit` entries.
Under the explicit exact-snapshot premise, every page and every deterministic
downstream result is unchanged.  A separate exact boundary predicate rejects a
current list that differs from the captured list, and the active-cache model
can contain at most one gallery payload.

The production audit canonicalizes entry names and stat facts and compares a
SHA-256 digest, while file reads additionally check exact stat and content
facts.  Lean does not prove SHA-256 collision resistance, Python/SQLite
refinement, directory iteration semantics, parser behavior, concurrent POSIX
mutation detection, or CBZ serialization.  Runtime differential, paging,
mutation, and exact-byte tests remain necessary evidence at those boundaries.
The model states no asymptotic improvement: production still performs a fresh
full-entry audit before returning every bounded page.
-/

namespace H2HDBIngest.Verification.GalleryIndexReuse

structure KeysetPage (Entry : Type) where
  items : List Entry
  terminal : Bool

def selectPage {Key Entry : Type}
    (key : Entry → Key)
    (strictlyAfter : Key → Key → Bool)
    (values : List Entry)
    (after : Option Key)
    (limit : Nat) : KeysetPage Entry :=
  let remaining :=
    match after with
    | none => values
    | some cursor => values.filter fun entry => strictlyAfter cursor (key entry)
  let bounded := remaining.take (limit + 1)
  ⟨bounded.take limit, decide (bounded.length ≤ limit)⟩

structure ImmutableIndex (Entry : Type) where
  entries : List Entry

def legacyPage {Key Entry : Type}
    (key : Entry → Key)
    (strictlyAfter : Key → Key → Bool)
    (source : List Entry)
    (after : Option Key)
    (limit : Nat) : KeysetPage Entry :=
  selectPage key strictlyAfter source after limit

def indexedPage {Key Entry : Type}
    (key : Entry → Key)
    (strictlyAfter : Key → Key → Bool)
    (index : ImmutableIndex Entry)
    (after : Option Key)
    (limit : Nat) : KeysetPage Entry :=
  selectPage key strictlyAfter index.entries after limit

theorem indexed_keyset_page_equals_legacy_page
    {Key Entry : Type}
    (source : List Entry)
    (index : ImmutableIndex Entry)
    (capturedExactly : index.entries = source)
    (key : Entry → Key)
    (strictlyAfter : Key → Key → Bool)
    (after : Option Key)
    (limit : Nat) :
    indexedPage key strictlyAfter index after limit =
      legacyPage key strictlyAfter source after limit := by
  simp [indexedPage, legacyPage, capturedExactly]

def deterministicOutput
    (serialize : List Entry → Output)
    (values : List Entry) : Output :=
  serialize values

theorem deterministic_output_is_unchanged
    (source : List Entry)
    (index : ImmutableIndex Entry)
    (capturedExactly : index.entries = source)
    (serialize : List Entry → Output) :
    deterministicOutput serialize index.entries =
      deterministicOutput serialize source := by
  simp [deterministicOutput, capturedExactly]

def boundaryAccepts
    [DecidableEq Entry]
    (index : ImmutableIndex Entry)
    (current : List Entry) : Bool :=
  decide (current = index.entries)

theorem accepted_boundary_is_exact
    [DecidableEq Entry]
    (index : ImmutableIndex Entry)
    (current : List Entry)
    (accepted : boundaryAccepts index current = true) :
    current = index.entries := by
  simpa [boundaryAccepts] using accepted

theorem changed_boundary_is_rejected
    [DecidableEq Entry]
    (index : ImmutableIndex Entry)
    (current : List Entry)
    (changed : current ≠ index.entries) :
    boundaryAccepts index current = false := by
  simp [boundaryAccepts, changed]

structure ActiveGalleryIndex (Gallery Entry : Type) where
  active : Option (Gallery × ImmutableIndex Entry)

def activate
    (gallery : Gallery)
    (index : ImmutableIndex Entry) : ActiveGalleryIndex Gallery Entry :=
  ⟨some (gallery, index)⟩

theorem activation_replaces_instead_of_accumulating
    (_old : ActiveGalleryIndex Gallery Entry)
    (gallery : Gallery)
    (index : ImmutableIndex Entry) :
    (activate gallery index).active = some (gallery, index) := by
  rfl

theorem active_cache_cardinality_is_at_most_one
    (cache : ActiveGalleryIndex Gallery Entry) :
    cache.active.toList.length ≤ 1 := by
  cases cache.active <;> simp

end H2HDBIngest.Verification.GalleryIndexReuse
