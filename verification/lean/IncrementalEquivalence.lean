import Std

/-!
# Incremental analysis and CBZ equivalence

Pure vNext policy model.  A snapshot maps a gallery id to `none` (absent) or a
gallery input; consequently deletion is distinct from an existing empty
gallery.  The proofs do not assert that scanner, SQL, transactions, or runtime
invalidation implement this model.

Explicit assumptions:

* file and artifact hashes are collision-free identifiers for canonical bytes;
* spam classification, effective-content reduction, final selection, ownership,
  and artifact construction are deterministic functions of explicit inputs;
* `changedSpam` is exact; and the old derived maps are full-recompute results;
* hash shards form an exact partition of the file-hash domain.
-/

namespace H2HDBIngest.Verification.IncrementalEquivalence

abbrev GalleryId := Nat
abbrev FileHash := Nat
abbrev Digest := Nat
abbrev OwnerId := Nat
abbrev PolicyVersion := Nat
abbrev ArtifactHash := Nat
abbrev Pred (α : Type) := α → Prop

structure GalleryInput where
  files : List FileHash
  sourceManifest : Digest
deriving DecidableEq, Repr

abbrev Snapshot := GalleryId → Option GalleryInput
abbrev SpamState := FileHash → Bool
abbrev ContentState := GalleryId → Option Digest

def exactChangedSpam (oldSpam newSpam : SpamState) : Pred FileHash :=
  fun hash => oldSpam hash ≠ newSpam hash

def usesChangedSpam
    (gallery : GalleryInput)
    (changed : Pred FileHash) : Prop :=
  ∃ hash ∈ gallery.files, changed hash

/--
Content invalidation covers additions/deletions/any gallery input change and
spam changes intersecting either the old or new file multiset.
-/
def ContentImpacted
    (oldSnapshot newSnapshot : Snapshot)
    (changedSpam : Pred FileHash)
    (gallery : GalleryId) : Prop :=
  oldSnapshot gallery ≠ newSnapshot gallery ∨
    (∃ input, oldSnapshot gallery = some input ∧
      usesChangedSpam input changedSpam) ∨
    (∃ input, newSnapshot gallery = some input ∧
      usesChangedSpam input changedSpam)

def fullContent
    (effectiveContent : List FileHash → SpamState → Option Digest)
    (snapshot : Snapshot)
    (spam : SpamState) : ContentState :=
  fun gallery => (snapshot gallery).bind fun input =>
    effectiveContent input.files spam

def incrementalContent
    (effectiveContent : List FileHash → SpamState → Option Digest)
    (oldContent : ContentState)
    (newSnapshot : Snapshot)
    (newSpam : SpamState)
    (impacted : Pred GalleryId)
    [DecidablePred impacted] : ContentState :=
  fun gallery =>
    if impacted gallery then
      fullContent effectiveContent newSnapshot newSpam gallery
    else
      oldContent gallery

/-- A content reducer observes spam decisions only for hashes in its files. -/
def ContentRespectsRelevantSpam
    (effectiveContent : List FileHash → SpamState → Option Digest) : Prop :=
  ∀ files left right,
    (∀ hash ∈ files, left hash = right hash) →
    effectiveContent files left = effectiveContent files right

theorem spam_agrees_outside_exactChanged
    {oldSpam newSpam : SpamState}
    {files : List FileHash}
    (disjoint : ¬ ∃ hash ∈ files, exactChangedSpam oldSpam newSpam hash) :
    ∀ hash ∈ files, oldSpam hash = newSpam hash := by
  intro hash hashMem
  apply Classical.byContradiction
  intro differs
  exact disjoint ⟨hash, hashMem, differs⟩

theorem unaffectedContent_eq
    {effectiveContent : List FileHash → SpamState → Option Digest}
    (respects : ContentRespectsRelevantSpam effectiveContent)
    {oldSnapshot newSnapshot : Snapshot}
    {oldSpam newSpam : SpamState}
    {gallery : GalleryId}
    (unaffected :
      ¬ ContentImpacted oldSnapshot newSnapshot
        (exactChangedSpam oldSpam newSpam) gallery) :
    fullContent effectiveContent oldSnapshot oldSpam gallery =
      fullContent effectiveContent newSnapshot newSpam gallery := by
  have sameSnapshot : oldSnapshot gallery = newSnapshot gallery := by
    apply Classical.byContradiction
    intro differs
    exact unaffected (Or.inl differs)
  cases oldValue : oldSnapshot gallery with
  | none => simp [fullContent, oldValue, ← sameSnapshot]
  | some input =>
      have noIntersection :
          ¬ ∃ hash ∈ input.files, exactChangedSpam oldSpam newSpam hash := by
        intro intersects
        exact unaffected (Or.inr (Or.inl ⟨input, oldValue, intersects⟩))
      simp only [fullContent, oldValue, Option.bind_some]
      rw [← sameSnapshot, oldValue]
      simp only [Option.bind_some]
      exact respects input.files oldSpam newSpam
        (spam_agrees_outside_exactChanged noIntersection)

theorem incrementalContent_eq_fullRecompute
    (effectiveContent : List FileHash → SpamState → Option Digest)
    (respects : ContentRespectsRelevantSpam effectiveContent)
    (oldSnapshot newSnapshot : Snapshot)
    (oldSpam newSpam : SpamState)
    (oldContent : ContentState)
    (oldCorrect : oldContent = fullContent effectiveContent oldSnapshot oldSpam) :
    let impacted : Pred GalleryId := fun gallery =>
      ContentImpacted oldSnapshot newSnapshot
        (exactChangedSpam oldSpam newSpam) gallery
    haveI : DecidablePred impacted := fun _ => Classical.propDecidable _
    incrementalContent effectiveContent oldContent newSnapshot newSpam impacted =
      fullContent effectiveContent newSnapshot newSpam := by
  dsimp only
  funext gallery
  by_cases changed :
      ContentImpacted oldSnapshot newSnapshot
        (exactChangedSpam oldSpam newSpam) gallery
  · simp [incrementalContent, changed]
  · simp [incrementalContent, changed, oldCorrect]
    exact unaffectedContent_eq respects changed

structure ArtifactInput where
  gallery : GalleryInput
  effectiveContent : Option Digest
  selected : Bool
  owner : Option OwnerId
  policyVersion : PolicyVersion
deriving DecidableEq, Repr

abbrev ArtifactInputs := GalleryId → Option ArtifactInput
abbrev ArtifactState := GalleryId → Option ArtifactHash

/-- `none` is deletion/no artifact; unselected rows also produce no CBZ. -/
def fullArtifacts
    (artifactFor : GalleryId → ArtifactInput → ArtifactHash)
    (inputs : ArtifactInputs) : ArtifactState :=
  fun gallery =>
    (inputs gallery).bind fun input =>
      if input.selected then some (artifactFor gallery input) else none

/-- Exact artifact impact, including create/delete and every policy input. -/
def ArtifactImpacted
    (oldInputs newInputs : ArtifactInputs)
    (gallery : GalleryId) : Prop :=
  oldInputs gallery ≠ newInputs gallery

def incrementalArtifacts
    (artifactFor : GalleryId → ArtifactInput → ArtifactHash)
    (oldArtifacts : ArtifactState)
    (newInputs : ArtifactInputs)
    (impacted : Pred GalleryId)
    [DecidablePred impacted] : ArtifactState :=
  fun gallery =>
    if impacted gallery then
      fullArtifacts artifactFor newInputs gallery
    else
      oldArtifacts gallery

theorem unaffectedArtifact_eq
    {artifactFor : GalleryId → ArtifactInput → ArtifactHash}
    {oldInputs newInputs : ArtifactInputs}
    {gallery : GalleryId}
    (unaffected : ¬ ArtifactImpacted oldInputs newInputs gallery) :
    fullArtifacts artifactFor oldInputs gallery =
      fullArtifacts artifactFor newInputs gallery := by
  have sameInput : oldInputs gallery = newInputs gallery := by
    apply Classical.byContradiction
    exact fun differs => unaffected differs
  simp [fullArtifacts, sameInput]

theorem incrementalArtifacts_eq_fullRecompute
    (artifactFor : GalleryId → ArtifactInput → ArtifactHash)
    (oldInputs newInputs : ArtifactInputs)
    (oldArtifacts : ArtifactState)
    (oldCorrect : oldArtifacts = fullArtifacts artifactFor oldInputs) :
    let impacted : Pred GalleryId :=
      fun gallery => ArtifactImpacted oldInputs newInputs gallery
    haveI : DecidablePred impacted := fun _ => Classical.propDecidable _
    incrementalArtifacts artifactFor oldArtifacts newInputs impacted =
      fullArtifacts artifactFor newInputs := by
  dsimp only
  funext gallery
  by_cases changed : ArtifactImpacted oldInputs newInputs gallery
  · simp [incrementalArtifacts, changed]
  · simp [incrementalArtifacts, changed, oldCorrect]
    exact unaffectedArtifact_eq changed

def rebuildSet (old new : ArtifactState) : Pred GalleryId :=
  fun gallery => ∃ oldHash newHash,
    old gallery = some oldHash ∧ new gallery = some newHash ∧
    oldHash ≠ newHash

def deleteSet (old new : ArtifactState) : Pred GalleryId :=
  fun gallery => (∃ hash, old gallery = some hash) ∧ new gallery = none

def createSet (old new : ArtifactState) : Pred GalleryId :=
  fun gallery => old gallery = none ∧ ∃ hash, new gallery = some hash

theorem cbzRebuildSet_incremental_eq_fullRecompute
    {old incremental full : ArtifactState}
    (equivalent : incremental = full) :
    rebuildSet old incremental = rebuildSet old full := by
  rw [equivalent]

theorem cbzDeleteSet_incremental_eq_fullRecompute
    {old incremental full : ArtifactState}
    (equivalent : incremental = full) :
    deleteSet old incremental = deleteSet old full := by
  rw [equivalent]

theorem cbzCreateSet_incremental_eq_fullRecompute
    {old incremental full : ArtifactState}
    (equivalent : incremental = full) :
    createSet old incremental = createSet old full := by
  rw [equivalent]

/--
End-to-end corollary: from an old full-recompute artifact cache and exact
`ArtifactInput` invalidation, all three operational CBZ sets match a fresh full
recompute.  Unlike the three extensional lemmas above, map equivalence here is
derived by `incrementalArtifacts_eq_fullRecompute`, not assumed.
-/
theorem incrementalCbzSets_eq_fullRecompute
    (artifactFor : GalleryId → ArtifactInput → ArtifactHash)
    (oldInputs newInputs : ArtifactInputs)
    (oldArtifacts : ArtifactState)
    (oldCorrect : oldArtifacts = fullArtifacts artifactFor oldInputs) :
    let impacted : Pred GalleryId :=
      fun gallery => ArtifactImpacted oldInputs newInputs gallery
    haveI : DecidablePred impacted := fun _ => Classical.propDecidable _
    let incremental :=
      incrementalArtifacts artifactFor oldArtifacts newInputs impacted
    let full := fullArtifacts artifactFor newInputs
    rebuildSet oldArtifacts incremental = rebuildSet oldArtifacts full ∧
      deleteSet oldArtifacts incremental = deleteSet oldArtifacts full ∧
      createSet oldArtifacts incremental = createSet oldArtifacts full := by
  dsimp only
  have mapsEqual :=
    incrementalArtifacts_eq_fullRecompute
      artifactFor oldInputs newInputs oldArtifacts oldCorrect
  exact ⟨congrArg (rebuildSet oldArtifacts) mapsEqual,
    congrArg (deleteSet oldArtifacts) mapsEqual,
    congrArg (createSet oldArtifacts) mapsEqual⟩

/-! ## Exact corpus-evidence invalidation for spam decisions -/

/--
All corpus information observed by the deterministic spam classifier for one
hash.  `payload` abstracts occurrence count plus the exact artist-set evidence;
equality therefore means the classifier sees identical input.
-/
structure SpamEvidence where
  payload : List Nat
deriving DecidableEq, Repr

abbrev EvidenceState := FileHash → SpamEvidence

def fullSpam
    (classify : SpamEvidence → Bool)
    (evidence : EvidenceState) : SpamState :=
  fun hash => classify (evidence hash)

def EvidenceImpacted
    (oldEvidence newEvidence : EvidenceState)
    (hash : FileHash) : Prop :=
  oldEvidence hash ≠ newEvidence hash

def incrementalSpam
    (classify : SpamEvidence → Bool)
    (oldSpam : SpamState)
    (newEvidence : EvidenceState)
    (impacted : Pred FileHash)
    [DecidablePred impacted] : SpamState :=
  fun hash =>
    if impacted hash then classify (newEvidence hash) else oldSpam hash

theorem incrementalSpam_eq_fullClassifier
    (classify : SpamEvidence → Bool)
    (oldEvidence newEvidence : EvidenceState)
    (oldSpam : SpamState)
    (oldCorrect : oldSpam = fullSpam classify oldEvidence) :
    let impacted : Pred FileHash :=
      fun hash => EvidenceImpacted oldEvidence newEvidence hash
    haveI : DecidablePred impacted := fun _ => Classical.propDecidable _
    incrementalSpam classify oldSpam newEvidence impacted =
      fullSpam classify newEvidence := by
  dsimp only
  funext hash
  by_cases changed : EvidenceImpacted oldEvidence newEvidence hash
  · simp [incrementalSpam, changed, fullSpam]
  · have sameEvidence : oldEvidence hash = newEvidence hash := by
      apply Classical.byContradiction
      exact fun differs => changed differs
    simp [incrementalSpam, changed, oldCorrect, fullSpam, sameEvidence]

theorem exactEvidenceDelta_contains_globalSpamChanges
    (classify : SpamEvidence → Bool)
    (oldEvidence newEvidence : EvidenceState)
    {hash : FileHash}
    (spamChanged :
      fullSpam classify oldEvidence hash ≠ fullSpam classify newEvidence hash) :
    EvidenceImpacted oldEvidence newEvidence hash := by
  apply Classical.byContradiction
  intro evidenceUnchanged
  have sameEvidence : oldEvidence hash = newEvidence hash := by
    apply Classical.byContradiction
    exact fun differs => evidenceUnchanged differs
  exact spamChanged (by simp [fullSpam, sameEvidence])

/-! ## Hash-shard locality and coverage -/

def shardClassifier
    (shardOf : FileHash → Nat)
    (shard : Nat)
    (classify : FileHash → Bool) : FileHash → Option Bool :=
  fun hash => if shardOf hash = shard then some (classify hash) else none

def mergedShards
    (shardOf : FileHash → Nat)
    (classify : FileHash → Bool) : FileHash → Bool :=
  fun hash => (shardClassifier shardOf (shardOf hash) classify hash).getD false

theorem hashShard_locality
    (shardOf : FileHash → Nat)
    (classify : FileHash → Bool)
    {hash : FileHash} {shard : Nat}
    (outside : shardOf hash ≠ shard) :
    shardClassifier shardOf shard classify hash = none := by
  simp [shardClassifier, outside]

theorem hashShards_union_eq_fullClassifier
    (shardOf : FileHash → Nat)
    (classify : FileHash → Bool) :
    mergedShards shardOf classify = classify := by
  funext hash
  simp [mergedShards, shardClassifier]

/-! ## Composed incremental pipeline -/

/--
The deterministic global policy is deliberately allowed to inspect the whole
snapshot, content map, and spam map.  Consequently adding one gallery may
change selection or ownership for any gallery, rather than only the new row.
`deriveArtifactInputs` records every such policy result in `ArtifactInput`, so
exact input inequality is also exact artifact invalidation.
-/
structure GlobalArtifactPolicy where
  policyVersion : PolicyVersion
  selected : Snapshot → ContentState → SpamState → GalleryId → Bool
  owner :
    Snapshot → ContentState → SpamState → GalleryId → Option OwnerId

def deriveArtifactInputs
    (policy : GlobalArtifactPolicy)
    (snapshot : Snapshot)
    (content : ContentState)
    (spam : SpamState) : ArtifactInputs :=
  fun gallery => (snapshot gallery).map fun input =>
    { gallery := input
      effectiveContent := content gallery
      selected := policy.selected snapshot content spam gallery
      owner := policy.owner snapshot content spam gallery
      policyVersion := policy.policyVersion }

/--
The explicit semantic boundary for an addition.  It deliberately requires no
files and no evidence delta: an empty or metadata-only gallery may still alter
a global winner, selection, owner, or artifact decision.
-/
def GalleryAdded
    (oldSnapshot newSnapshot : Snapshot)
    (gallery : GalleryId) : Prop :=
  ∃ input,
    oldSnapshot gallery = none ∧
      newSnapshot gallery = some input

theorem galleryAdded_contentImpacted
    (oldSnapshot newSnapshot : Snapshot)
    (changedSpam : Pred FileHash)
    (gallery : GalleryId)
    (addition : GalleryAdded oldSnapshot newSnapshot gallery) :
    ContentImpacted oldSnapshot newSnapshot changedSpam gallery := by
  rcases addition with ⟨input, oldAbsent, newPresent⟩
  apply Or.inl
  intro same
  rw [oldAbsent, newPresent] at same
  contradiction

/--
A genuinely composed incremental/full-recompute theorem for arbitrary old and
new snapshots, including additions, deletions, and modifications.  It derives:

1. incremental spam equality with a full corpus classifier;
2. incremental content equality with full recomputation;
3. equality of globally derived artifact inputs (selection and owner included);
4. incremental artifact equality; and
5. exact equality of CBZ rebuild/delete/create sets.

The artifact-map equality is a conclusion, never a premise.  The only cache
premises say that the old spam, content, and artifact caches were correct.
Pure Lean functions model deterministic evidence, policy, and artifact
construction.  This theorem still does not prove scanner, SQL, transaction,
or runtime invalidation behavior.
-/
theorem incrementalPipeline_eq_fullRecompute
    (evidenceOf : Snapshot → EvidenceState)
    (classify : SpamEvidence → Bool)
    (effectiveContent : List FileHash → SpamState → Option Digest)
    (contentRespectsSpam : ContentRespectsRelevantSpam effectiveContent)
    (policy : GlobalArtifactPolicy)
    (artifactFor : GalleryId → ArtifactInput → ArtifactHash)
    (oldSnapshot newSnapshot : Snapshot)
    (oldSpam : SpamState)
    (oldContent : ContentState)
    (oldArtifacts : ArtifactState)
    (oldSpamCorrect :
      oldSpam = fullSpam classify (evidenceOf oldSnapshot))
    (oldContentCorrect :
      oldContent =
        fullContent effectiveContent oldSnapshot oldSpam)
    (oldArtifactsCorrect :
      oldArtifacts = fullArtifacts artifactFor
        (deriveArtifactInputs policy oldSnapshot oldContent oldSpam)) :
    let oldEvidence := evidenceOf oldSnapshot
    let newEvidence := evidenceOf newSnapshot
    let spamImpacted : Pred FileHash :=
      fun hash => EvidenceImpacted oldEvidence newEvidence hash
    haveI : DecidablePred spamImpacted :=
      fun _ => Classical.propDecidable _
    let incrementalSpamState :=
      incrementalSpam classify oldSpam newEvidence spamImpacted
    let fullSpamState := fullSpam classify newEvidence
    let contentImpacted : Pred GalleryId := fun gallery =>
      ContentImpacted oldSnapshot newSnapshot
        (exactChangedSpam oldSpam incrementalSpamState) gallery
    haveI : DecidablePred contentImpacted :=
      fun _ => Classical.propDecidable _
    let incrementalContentState :=
      incrementalContent effectiveContent oldContent newSnapshot
        incrementalSpamState contentImpacted
    let fullContentState :=
      fullContent effectiveContent newSnapshot fullSpamState
    let oldArtifactInputs :=
      deriveArtifactInputs policy oldSnapshot oldContent oldSpam
    let incrementalArtifactInputs :=
      deriveArtifactInputs policy newSnapshot incrementalContentState
        incrementalSpamState
    let fullArtifactInputs :=
      deriveArtifactInputs policy newSnapshot fullContentState fullSpamState
    let artifactImpacted : Pred GalleryId := fun gallery =>
      ArtifactImpacted oldArtifactInputs incrementalArtifactInputs gallery
    haveI : DecidablePred artifactImpacted :=
      fun _ => Classical.propDecidable _
    let incrementalArtifactState :=
      incrementalArtifacts artifactFor oldArtifacts
        incrementalArtifactInputs artifactImpacted
    let fullArtifactState :=
      fullArtifacts artifactFor fullArtifactInputs
    incrementalSpamState = fullSpamState ∧
      incrementalContentState = fullContentState ∧
      incrementalArtifactInputs = fullArtifactInputs ∧
      incrementalArtifactState = fullArtifactState ∧
      rebuildSet oldArtifacts incrementalArtifactState =
        rebuildSet oldArtifacts fullArtifactState ∧
      deleteSet oldArtifacts incrementalArtifactState =
        deleteSet oldArtifacts fullArtifactState ∧
      createSet oldArtifacts incrementalArtifactState =
        createSet oldArtifacts fullArtifactState := by
  dsimp only
  letI spamImpactDecidable : DecidablePred (fun hash =>
      EvidenceImpacted (evidenceOf oldSnapshot)
        (evidenceOf newSnapshot) hash) :=
    fun _ => Classical.propDecidable _
  letI contentImpactDecidable : DecidablePred (fun gallery =>
      ContentImpacted oldSnapshot newSnapshot
        (exactChangedSpam oldSpam
          (incrementalSpam classify oldSpam (evidenceOf newSnapshot)
            (fun hash =>
              EvidenceImpacted (evidenceOf oldSnapshot)
                (evidenceOf newSnapshot) hash))) gallery) :=
    fun _ => Classical.propDecidable _
  have spamExact :=
    incrementalSpam_eq_fullClassifier classify
      (evidenceOf oldSnapshot) (evidenceOf newSnapshot)
      oldSpam oldSpamCorrect
  have contentExact :=
    incrementalContent_eq_fullRecompute effectiveContent
      contentRespectsSpam oldSnapshot newSnapshot oldSpam
      (incrementalSpam classify oldSpam (evidenceOf newSnapshot)
        (fun hash =>
          EvidenceImpacted (evidenceOf oldSnapshot)
            (evidenceOf newSnapshot) hash))
      oldContent oldContentCorrect
  have contentExactFull :
      incrementalContent effectiveContent oldContent newSnapshot
          (incrementalSpam classify oldSpam (evidenceOf newSnapshot)
            (fun hash =>
              EvidenceImpacted (evidenceOf oldSnapshot)
                (evidenceOf newSnapshot) hash))
          (fun gallery =>
            ContentImpacted oldSnapshot newSnapshot
              (exactChangedSpam oldSpam
                (incrementalSpam classify oldSpam (evidenceOf newSnapshot)
                  (fun hash =>
                    EvidenceImpacted (evidenceOf oldSnapshot)
                      (evidenceOf newSnapshot) hash))) gallery) =
        fullContent effectiveContent newSnapshot
          (fullSpam classify (evidenceOf newSnapshot)) := by
    rw [contentExact, spamExact]
  have artifactInputsExact :
      deriveArtifactInputs policy newSnapshot
          (incrementalContent effectiveContent oldContent newSnapshot
            (incrementalSpam classify oldSpam (evidenceOf newSnapshot)
              (fun hash =>
                EvidenceImpacted (evidenceOf oldSnapshot)
                  (evidenceOf newSnapshot) hash))
            (fun gallery =>
              ContentImpacted oldSnapshot newSnapshot
                (exactChangedSpam oldSpam
                  (incrementalSpam classify oldSpam (evidenceOf newSnapshot)
                    (fun hash =>
                      EvidenceImpacted (evidenceOf oldSnapshot)
                        (evidenceOf newSnapshot) hash))) gallery))
          (incrementalSpam classify oldSpam (evidenceOf newSnapshot)
            (fun hash =>
              EvidenceImpacted (evidenceOf oldSnapshot)
                (evidenceOf newSnapshot) hash)) =
        deriveArtifactInputs policy newSnapshot
          (fullContent effectiveContent newSnapshot
            (fullSpam classify (evidenceOf newSnapshot)))
          (fullSpam classify (evidenceOf newSnapshot)) := by
    rw [contentExactFull, spamExact]
  letI artifactImpactDecidable : DecidablePred (fun gallery =>
      ArtifactImpacted
        (deriveArtifactInputs policy oldSnapshot oldContent oldSpam)
        (deriveArtifactInputs policy newSnapshot
          (incrementalContent effectiveContent oldContent newSnapshot
            (incrementalSpam classify oldSpam (evidenceOf newSnapshot)
              (fun hash =>
                EvidenceImpacted (evidenceOf oldSnapshot)
                  (evidenceOf newSnapshot) hash))
            (fun candidate =>
              ContentImpacted oldSnapshot newSnapshot
                (exactChangedSpam oldSpam
                  (incrementalSpam classify oldSpam (evidenceOf newSnapshot)
                    (fun hash =>
                      EvidenceImpacted (evidenceOf oldSnapshot)
                        (evidenceOf newSnapshot) hash))) candidate))
          (incrementalSpam classify oldSpam (evidenceOf newSnapshot)
            (fun hash =>
              EvidenceImpacted (evidenceOf oldSnapshot)
                (evidenceOf newSnapshot) hash))) gallery) :=
    fun _ => Classical.propDecidable _
  have artifactsExactIncrementalInputs :=
    incrementalArtifacts_eq_fullRecompute artifactFor
      (deriveArtifactInputs policy oldSnapshot oldContent oldSpam)
      (deriveArtifactInputs policy newSnapshot
        (incrementalContent effectiveContent oldContent newSnapshot
          (incrementalSpam classify oldSpam (evidenceOf newSnapshot)
            (fun hash =>
              EvidenceImpacted (evidenceOf oldSnapshot)
                (evidenceOf newSnapshot) hash))
          (fun gallery =>
            ContentImpacted oldSnapshot newSnapshot
              (exactChangedSpam oldSpam
                (incrementalSpam classify oldSpam (evidenceOf newSnapshot)
                  (fun hash =>
                    EvidenceImpacted (evidenceOf oldSnapshot)
                      (evidenceOf newSnapshot) hash))) gallery))
        (incrementalSpam classify oldSpam (evidenceOf newSnapshot)
          (fun hash =>
            EvidenceImpacted (evidenceOf oldSnapshot)
              (evidenceOf newSnapshot) hash)))
      oldArtifacts oldArtifactsCorrect
  have artifactsExact :
      incrementalArtifacts artifactFor oldArtifacts
          (deriveArtifactInputs policy newSnapshot
            (incrementalContent effectiveContent oldContent newSnapshot
              (incrementalSpam classify oldSpam (evidenceOf newSnapshot)
                (fun hash =>
                  EvidenceImpacted (evidenceOf oldSnapshot)
                    (evidenceOf newSnapshot) hash))
              (fun gallery =>
                ContentImpacted oldSnapshot newSnapshot
                  (exactChangedSpam oldSpam
                    (incrementalSpam classify oldSpam (evidenceOf newSnapshot)
                      (fun hash =>
                        EvidenceImpacted (evidenceOf oldSnapshot)
                          (evidenceOf newSnapshot) hash))) gallery))
            (incrementalSpam classify oldSpam (evidenceOf newSnapshot)
              (fun hash =>
                EvidenceImpacted (evidenceOf oldSnapshot)
                  (evidenceOf newSnapshot) hash)))
          (fun gallery =>
            ArtifactImpacted
              (deriveArtifactInputs policy oldSnapshot oldContent oldSpam)
              (deriveArtifactInputs policy newSnapshot
                (incrementalContent effectiveContent oldContent newSnapshot
                  (incrementalSpam classify oldSpam (evidenceOf newSnapshot)
                    (fun hash =>
                      EvidenceImpacted (evidenceOf oldSnapshot)
                        (evidenceOf newSnapshot) hash))
                  (fun candidate =>
                    ContentImpacted oldSnapshot newSnapshot
                      (exactChangedSpam oldSpam
                        (incrementalSpam classify oldSpam
                          (evidenceOf newSnapshot)
                          (fun hash =>
                            EvidenceImpacted (evidenceOf oldSnapshot)
                              (evidenceOf newSnapshot) hash))) candidate))
                (incrementalSpam classify oldSpam (evidenceOf newSnapshot)
                  (fun hash =>
                    EvidenceImpacted (evidenceOf oldSnapshot)
                      (evidenceOf newSnapshot) hash))) gallery) =
        fullArtifacts artifactFor
          (deriveArtifactInputs policy newSnapshot
            (fullContent effectiveContent newSnapshot
              (fullSpam classify (evidenceOf newSnapshot)))
            (fullSpam classify (evidenceOf newSnapshot))) := by
    rw [artifactsExactIncrementalInputs, artifactInputsExact]
  exact ⟨spamExact, contentExactFull,
    artifactInputsExact, artifactsExact,
    congrArg (rebuildSet oldArtifacts) artifactsExact,
    congrArg (deleteSet oldArtifacts) artifactsExact,
    congrArg (createSet oldArtifacts) artifactsExact⟩

/--
Addition corollary for the generic operational theorem.  `GalleryAdded` alone
is sufficient: even an empty or metadata-only gallery is content-impacted by
snapshot inequality, while the generic global policy accounts for any winner,
selection, ownership, and CBZ consequences elsewhere in the corpus.
-/
theorem addedGallery_incrementalPipeline_eq_fullRecompute
    (evidenceOf : Snapshot → EvidenceState)
    (classify : SpamEvidence → Bool)
    (effectiveContent : List FileHash → SpamState → Option Digest)
    (contentRespectsSpam : ContentRespectsRelevantSpam effectiveContent)
    (policy : GlobalArtifactPolicy)
    (artifactFor : GalleryId → ArtifactInput → ArtifactHash)
    (oldSnapshot newSnapshot : Snapshot)
    (addedGallery : GalleryId)
    (oldSpam : SpamState)
    (oldContent : ContentState)
    (oldArtifacts : ArtifactState)
    (addition : GalleryAdded oldSnapshot newSnapshot addedGallery)
    (oldSpamCorrect :
      oldSpam = fullSpam classify (evidenceOf oldSnapshot))
    (oldContentCorrect :
      oldContent =
        fullContent effectiveContent oldSnapshot oldSpam)
    (oldArtifactsCorrect :
      oldArtifacts = fullArtifacts artifactFor
        (deriveArtifactInputs policy oldSnapshot oldContent oldSpam)) :
    let oldEvidence := evidenceOf oldSnapshot
    let newEvidence := evidenceOf newSnapshot
    let spamImpacted : Pred FileHash :=
      fun hash => EvidenceImpacted oldEvidence newEvidence hash
    haveI : DecidablePred spamImpacted :=
      fun _ => Classical.propDecidable _
    let incrementalSpamState :=
      incrementalSpam classify oldSpam newEvidence spamImpacted
    let fullSpamState := fullSpam classify newEvidence
    let contentImpacted : Pred GalleryId := fun gallery =>
      ContentImpacted oldSnapshot newSnapshot
        (exactChangedSpam oldSpam incrementalSpamState) gallery
    haveI : DecidablePred contentImpacted :=
      fun _ => Classical.propDecidable _
    let incrementalContentState :=
      incrementalContent effectiveContent oldContent newSnapshot
        incrementalSpamState contentImpacted
    let fullContentState :=
      fullContent effectiveContent newSnapshot fullSpamState
    let oldArtifactInputs :=
      deriveArtifactInputs policy oldSnapshot oldContent oldSpam
    let incrementalArtifactInputs :=
      deriveArtifactInputs policy newSnapshot incrementalContentState
        incrementalSpamState
    let fullArtifactInputs :=
      deriveArtifactInputs policy newSnapshot fullContentState fullSpamState
    let artifactImpacted : Pred GalleryId := fun gallery =>
      ArtifactImpacted oldArtifactInputs incrementalArtifactInputs gallery
    haveI : DecidablePred artifactImpacted :=
      fun _ => Classical.propDecidable _
    let incrementalArtifactState :=
      incrementalArtifacts artifactFor oldArtifacts
        incrementalArtifactInputs artifactImpacted
    let fullArtifactState :=
      fullArtifacts artifactFor fullArtifactInputs
    ContentImpacted oldSnapshot newSnapshot
        (exactChangedSpam oldSpam incrementalSpamState) addedGallery ∧
      incrementalSpamState = fullSpamState ∧
      incrementalContentState = fullContentState ∧
      incrementalArtifactInputs = fullArtifactInputs ∧
      incrementalArtifactState = fullArtifactState ∧
      rebuildSet oldArtifacts incrementalArtifactState =
        rebuildSet oldArtifacts fullArtifactState ∧
      deleteSet oldArtifacts incrementalArtifactState =
        deleteSet oldArtifacts fullArtifactState ∧
      createSet oldArtifacts incrementalArtifactState =
        createSet oldArtifacts fullArtifactState := by
  dsimp only
  letI spamImpactDecidable : DecidablePred (fun hash =>
      EvidenceImpacted (evidenceOf oldSnapshot)
        (evidenceOf newSnapshot) hash) :=
    fun _ => Classical.propDecidable _
  letI contentImpactDecidable : DecidablePred (fun gallery =>
      ContentImpacted oldSnapshot newSnapshot
        (exactChangedSpam oldSpam
          (incrementalSpam classify oldSpam (evidenceOf newSnapshot)
            (fun hash =>
              EvidenceImpacted (evidenceOf oldSnapshot)
                (evidenceOf newSnapshot) hash))) gallery) :=
    fun _ => Classical.propDecidable _
  letI artifactImpactDecidable : DecidablePred (fun gallery =>
      ArtifactImpacted
        (deriveArtifactInputs policy oldSnapshot oldContent oldSpam)
        (deriveArtifactInputs policy newSnapshot
          (incrementalContent effectiveContent oldContent newSnapshot
            (incrementalSpam classify oldSpam (evidenceOf newSnapshot)
              (fun hash =>
                EvidenceImpacted (evidenceOf oldSnapshot)
                  (evidenceOf newSnapshot) hash))
            (fun candidate =>
              ContentImpacted oldSnapshot newSnapshot
                (exactChangedSpam oldSpam
                  (incrementalSpam classify oldSpam (evidenceOf newSnapshot)
                    (fun hash =>
                      EvidenceImpacted (evidenceOf oldSnapshot)
                        (evidenceOf newSnapshot) hash))) candidate))
          (incrementalSpam classify oldSpam (evidenceOf newSnapshot)
            (fun hash =>
              EvidenceImpacted (evidenceOf oldSnapshot)
                (evidenceOf newSnapshot) hash))) gallery) :=
    fun _ => Classical.propDecidable _
  have pipelineExact :=
    incrementalPipeline_eq_fullRecompute evidenceOf classify
      effectiveContent contentRespectsSpam policy artifactFor
      oldSnapshot newSnapshot oldSpam oldContent oldArtifacts
      oldSpamCorrect oldContentCorrect oldArtifactsCorrect
  exact ⟨galleryAdded_contentImpacted oldSnapshot newSnapshot
      (exactChangedSpam oldSpam
        (incrementalSpam classify oldSpam (evidenceOf newSnapshot)
          (fun hash =>
            EvidenceImpacted (evidenceOf oldSnapshot)
              (evidenceOf newSnapshot) hash)))
      addedGallery addition,
    pipelineExact⟩

end H2HDBIngest.Verification.IncrementalEquivalence
