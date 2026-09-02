import Std

/-!
# Gallery-scoped immutable source index

This file models the pure part of reusing one gallery index.  A legacy page is
selected from the source list; an indexed page is selected from an immutable
list captured from that source.  Under the explicit exact-snapshot premise,
every page and every deterministic downstream result is unchanged.  A separate
exact boundary predicate rejects a current list that differs from the captured
list, and the active-cache model can contain at most one gallery payload.

The production audit canonicalizes entry names and stat facts and compares a
SHA-256 digest, while file reads additionally check exact stat and content
facts.  Lean does not prove SHA-256 collision resistance, Python/SQLite
refinement, directory iteration semantics, parser behavior, concurrent POSIX
mutation detection, or CBZ serialization.  Runtime differential, paging,
mutation, and exact-byte tests remain necessary evidence at those boundaries.
-/

namespace H2HDBIngest.Verification.GalleryIndexReuse

def selectPage (values : List Entry) (offset limit : Nat) : List Entry :=
  (values.drop offset).take limit

structure ImmutableIndex (Entry : Type) where
  entries : List Entry

def legacyPage
    (source : List Entry)
    (offset limit : Nat) : List Entry :=
  selectPage source offset limit

def indexedPage
    (index : ImmutableIndex Entry)
    (offset limit : Nat) : List Entry :=
  selectPage index.entries offset limit

theorem indexed_page_equals_legacy_page
    (source : List Entry)
    (index : ImmutableIndex Entry)
    (capturedExactly : index.entries = source)
    (offset limit : Nat) :
    indexedPage index offset limit = legacyPage source offset limit := by
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
