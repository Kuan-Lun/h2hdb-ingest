import Std

/-!
# Bounded concurrent page rendering with ordered publication

The renderer is modeled as a deterministic pure function from one indexed page
to its exact bytes and evidence.  A `BoundedExecution` may complete pages in any
permutation and group them into arbitrary batches, provided every page occurs
exactly once, worker count is in `1..16`, and every batch is no larger than that
worker count.  `orderedCollect` deliberately ignores completion order and reads
the exact result at increasing page index.

The main theorem proves that ordered collection equals the sequential map for
arbitrary page count, valid batching, and completion schedule.  Consequently
any deterministic canonical serializer receives the same ordered bytes and
evidence.  A separate publish-last model exposes a completed archive only after
worker execution, validation, and serialization all succeed; each earlier
failure leaves the prior destination unchanged.

These are mathematical results under the explicit pure-renderer and exact
completion assumptions.  They do not prove Pillow determinism or thread
safety, Python future/executor behavior, spool cleanup, filesystem semantics,
ZIP serialization, or that production code refines this model.  The model also
does not claim destination preservation when the final destination write itself
fails.  Exact-byte, concurrency, and fault tests remain required implementation
evidence.
-/

namespace H2HDBIngest.Verification.OrderedPageRendering

def HardWorkerCap : Nat := 16

def ValidWorkerCount (workers : Nat) : Prop :=
  1 ≤ workers ∧ workers ≤ HardWorkerCap

def automaticWorkerCount (detected : Nat) : Nat :=
  if detected < 1 then 1
  else if HardWorkerCap < detected then HardWorkerCap
  else detected

def resolveWorkerCount (configured : Option Nat) (detected : Nat) : Nat :=
  match configured with
  | none => automaticWorkerCount detected
  | some workers => workers

theorem automatic_worker_count_is_valid (detected : Nat) :
    ValidWorkerCount (automaticWorkerCount detected) := by
  unfold automaticWorkerCount ValidWorkerCount HardWorkerCap
  split
  · omega
  · split <;> omega

theorem automatic_resolution_is_valid (detected : Nat) :
    ValidWorkerCount (resolveWorkerCount none detected) := by
  exact automatic_worker_count_is_valid detected

theorem explicit_worker_override_is_exact
    (configured detected : Nat) :
    resolveWorkerCount (some configured) detected = configured := by
  rfl

theorem valid_explicit_worker_override_stays_valid
    (configured detected : Nat)
    (valid : ValidWorkerCount configured) :
    ValidWorkerCount (resolveWorkerCount (some configured) detected) := by
  simpa [resolveWorkerCount] using valid

/-!
## Worker decision shape

`CpuTopology` abstracts the once-per-process host probe, `WorkerDecision` the
immutable decision value, and `decide` the pure selection from an optional
override and one topology.  Darwin-only facts are options so a non-Darwin
topology carries none of them.  The theorems below state what the decision
*shape* guarantees: manual overrides are exact and marked, automatic selection
is always in `1..16`, a Darwin decision never depends on logical CPU counts, a
non-Darwin decision never depends on Darwin facts, every fallback reason
selects exactly one worker, and the selected count equals the earlier
`resolveWorkerCount` policy over the same abstract authority.  `observe` is a
pure projection of a decision into its log record.

None of this proves that `sysctl`, Python CPU discovery, configuration
parsing, or Python logging refines these definitions; mocked-topology,
differential, cache, and runtime log tests remain that implementation evidence.
-/

inductive WorkerMode where
  | auto
  | manual
deriving DecidableEq, Repr

inductive DarwinTranslation where
  | native
  | translated
  | unknown
  | notProbed
deriving DecidableEq, Repr

structure CpuTopology where
  darwin : Bool
  intelMachine : Bool
  processCpuCount : Option Nat
  cpuCount : Option Nat
  darwinPerformanceCores : Option Nat
  darwinPhysicalCores : Option Nat
  darwinTranslation : DarwinTranslation
deriving DecidableEq, Repr

inductive WorkerReason where
  | manualOverride
  | darwinPerformanceCores
  | darwinIntelNativePhysicalCores
  | darwinIntelTranslatedFallback
  | darwinIntelTranslationUnknownFallback
  | darwinIntelPhysicalCoresUnavailableFallback
  | darwinPerformanceCoresUnavailableFallback
  | processCpuCount
  | cpuCount
  | cpuCountUnavailableFallback
deriving DecidableEq, Repr

def WorkerReason.isFallback : WorkerReason → Bool
  | .darwinIntelTranslatedFallback => true
  | .darwinIntelTranslationUnknownFallback => true
  | .darwinIntelPhysicalCoresUnavailableFallback => true
  | .darwinPerformanceCoresUnavailableFallback => true
  | .cpuCountUnavailableFallback => true
  | _ => false

structure WorkerDecision where
  mode : WorkerMode
  configured : Option Nat
  selected : Nat
  detected : Option Nat
  reason : WorkerReason
deriving DecidableEq, Repr

def MaxPlausibleCpus : Nat := 1024

/-- A reported count is authority only when it is positive and plausible. -/
def plausible (value : Option Nat) : Option Nat :=
  match value with
  | none => none
  | some n => if 1 ≤ n ∧ n ≤ MaxPlausibleCpus then some n else none

def detectedDecision (authority : Nat) (reason : WorkerReason) : WorkerDecision :=
  ⟨.auto, none, min authority HardWorkerCap, some authority, reason⟩

def fallbackDecision (reason : WorkerReason) : WorkerDecision :=
  ⟨.auto, none, 1, none, reason⟩

def darwinAutomatic (topology : CpuTopology) : WorkerDecision :=
  match plausible topology.darwinPerformanceCores with
  | some performance => detectedDecision performance .darwinPerformanceCores
  | none =>
    if topology.intelMachine then
      match topology.darwinTranslation with
      | .translated => fallbackDecision .darwinIntelTranslatedFallback
      | .native =>
        match plausible topology.darwinPhysicalCores with
        | some physical => detectedDecision physical .darwinIntelNativePhysicalCores
        | none => fallbackDecision .darwinIntelPhysicalCoresUnavailableFallback
      | .unknown => fallbackDecision .darwinIntelTranslationUnknownFallback
      | .notProbed => fallbackDecision .darwinIntelTranslationUnknownFallback
    else fallbackDecision .darwinPerformanceCoresUnavailableFallback

def otherAutomatic (topology : CpuTopology) : WorkerDecision :=
  match topology.processCpuCount with
  | some reported =>
    match plausible (some reported) with
    | some available => detectedDecision available .processCpuCount
    | none => fallbackDecision .cpuCountUnavailableFallback
  | none =>
    match plausible topology.cpuCount with
    | some total => detectedDecision total .cpuCount
    | none => fallbackDecision .cpuCountUnavailableFallback

def automaticDecision (topology : CpuTopology) : WorkerDecision :=
  if topology.darwin then darwinAutomatic topology else otherAutomatic topology

def decide (configured : Option Nat) (topology : CpuTopology) : WorkerDecision :=
  match configured with
  | some workers => ⟨.manual, some workers, workers, none, .manualOverride⟩
  | none => automaticDecision topology

theorem manual_decision_is_exact_and_marked
    (workers : Nat) (topology : CpuTopology) :
    decide (some workers) topology =
      ⟨.manual, some workers, workers, none, .manualOverride⟩ := by
  rfl

theorem decide_none (topology : CpuTopology) :
    decide none topology = automaticDecision topology := by
  rfl

theorem plausible_eq_some_iff (value : Option Nat) (n : Nat) :
    plausible value = some n ↔
      value = some n ∧ 1 ≤ n ∧ n ≤ MaxPlausibleCpus := by
  unfold plausible
  split
  · simp
  · rename_i m
    split
    · rename_i h
      constructor
      · intro e
        simp at e
        subst e
        exact ⟨rfl, h⟩
      · intro e
        obtain ⟨e, _⟩ := e
        simp at e
        subst e
        rfl
    · rename_i h
      constructor
      · intro e
        simp at e
      · intro e
        obtain ⟨e, hb⟩ := e
        simp at e
        subst e
        exact absurd hb h

/-- Every automatic decision is either a hard-capped plausible authority with a
non-fallback reason or the conservative single worker with a fallback reason. -/
theorem automatic_decision_shape (topology : CpuTopology) :
    (∃ authority reason, 1 ≤ authority ∧ authority ≤ MaxPlausibleCpus ∧
        reason.isFallback = false ∧
        automaticDecision topology = detectedDecision authority reason) ∨
      (∃ reason, reason.isFallback = true ∧
        automaticDecision topology = fallbackDecision reason) := by
  unfold automaticDecision darwinAutomatic otherAutomatic
  split <;> split <;> (try split) <;> (try split) <;> (try split) <;>
    first
    | (refine Or.inl ⟨_, _, ?_, ?_, rfl, rfl⟩ <;> simp_all [plausible_eq_some_iff])
    | exact Or.inr ⟨_, rfl, rfl⟩

theorem automatic_decision_has_no_configured_value (topology : CpuTopology) :
    (decide none topology).mode = .auto ∧
      (decide none topology).configured = none := by
  rw [decide_none]
  rcases automatic_decision_shape topology with ⟨_, _, _, _, _, h⟩ | ⟨_, _, h⟩ <;>
    rw [h] <;> exact ⟨rfl, rfl⟩

theorem detected_decision_is_valid
    (authority : Nat) (reason : WorkerReason) (positive : 1 ≤ authority) :
    ValidWorkerCount (detectedDecision authority reason).selected := by
  simp only [detectedDecision, ValidWorkerCount, HardWorkerCap]
  omega

theorem fallback_decision_is_valid (reason : WorkerReason) :
    ValidWorkerCount (fallbackDecision reason).selected := by
  simp [fallbackDecision, ValidWorkerCount, HardWorkerCap]

theorem automatic_decision_is_valid (topology : CpuTopology) :
    ValidWorkerCount (decide none topology).selected := by
  rw [decide_none]
  rcases automatic_decision_shape topology with
    ⟨authority, reason, positive, _, _, h⟩ | ⟨reason, _, h⟩
  · rw [h]
    exact detected_decision_is_valid authority reason positive
  · rw [h]
    exact fallback_decision_is_valid reason

theorem every_decision_is_valid
    (configured : Option Nat) (topology : CpuTopology)
    (valid : ∀ workers, configured = some workers → ValidWorkerCount workers) :
    ValidWorkerCount (decide configured topology).selected := by
  cases configured with
  | none => exact automatic_decision_is_valid topology
  | some workers => exact valid workers rfl

theorem darwin_decision_ignores_logical_cpu_counts
    (topology : CpuTopology) (processCpuCount cpuCount : Option Nat)
    (darwin : topology.darwin = true) :
    decide none { topology with
      processCpuCount := processCpuCount, cpuCount := cpuCount } =
      decide none topology := by
  simp [decide, automaticDecision, darwinAutomatic, darwin]

theorem non_darwin_decision_ignores_darwin_facts
    (topology : CpuTopology)
    (performance physical : Option Nat) (translation : DarwinTranslation)
    (other : topology.darwin = false) :
    decide none { topology with
      darwinPerformanceCores := performance,
      darwinPhysicalCores := physical,
      darwinTranslation := translation } =
      decide none topology := by
  simp [decide, automaticDecision, otherAutomatic, other]

theorem performance_cores_take_priority
    (topology : CpuTopology) (performance : Nat)
    (darwin : topology.darwin = true)
    (authority : plausible topology.darwinPerformanceCores = some performance) :
    decide none topology =
      detectedDecision performance .darwinPerformanceCores := by
  simp [decide, automaticDecision, darwinAutomatic, darwin, authority]

theorem translated_intel_process_falls_back_to_one
    (topology : CpuTopology)
    (darwin : topology.darwin = true)
    (missing : plausible topology.darwinPerformanceCores = none)
    (intel : topology.intelMachine = true)
    (translated : topology.darwinTranslation = .translated) :
    decide none topology = fallbackDecision .darwinIntelTranslatedFallback := by
  simp [decide, automaticDecision, darwinAutomatic, darwin, missing, intel,
    translated]

theorem unknown_translation_intel_process_falls_back_to_one
    (topology : CpuTopology)
    (darwin : topology.darwin = true)
    (missing : plausible topology.darwinPerformanceCores = none)
    (intel : topology.intelMachine = true)
    (unknown : topology.darwinTranslation = .unknown ∨
      topology.darwinTranslation = .notProbed) :
    decide none topology =
      fallbackDecision .darwinIntelTranslationUnknownFallback := by
  rcases unknown with h | h <;>
    simp [decide, automaticDecision, darwinAutomatic, darwin, missing, intel, h]

theorem non_intel_darwin_without_performance_authority_falls_back_to_one
    (topology : CpuTopology)
    (darwin : topology.darwin = true)
    (missing : plausible topology.darwinPerformanceCores = none)
    (other : topology.intelMachine = false) :
    decide none topology =
      fallbackDecision .darwinPerformanceCoresUnavailableFallback := by
  simp [decide, automaticDecision, darwinAutomatic, darwin, missing, other]

theorem fallback_reason_selects_exactly_one
    (configured : Option Nat) (topology : CpuTopology)
    (fallback : (decide configured topology).reason.isFallback = true) :
    (decide configured topology).selected = 1 ∧
      (decide configured topology).detected = none := by
  cases configured with
  | some workers => simp [decide, WorkerReason.isFallback] at fallback
  | none =>
    rw [decide_none] at fallback ⊢
    rcases automatic_decision_shape topology with
      ⟨_, _, _, _, detected, h⟩ | ⟨_, _, h⟩
    · rw [h] at fallback
      simp [detectedDecision] at fallback
      simp_all
    · rw [h]
      exact ⟨rfl, rfl⟩

theorem detected_reason_selects_capped_authority
    (topology : CpuTopology)
    (detected : (decide none topology).reason.isFallback = false) :
    ∃ authority, 1 ≤ authority ∧
      (decide none topology).detected = some authority ∧
      (decide none topology).selected = min authority HardWorkerCap := by
  rw [decide_none] at detected ⊢
  rcases automatic_decision_shape topology with
    ⟨authority, _, positive, _, _, h⟩ | ⟨_, fallback, h⟩
  · rw [h]
    exact ⟨authority, positive, rfl, rfl⟩
  · rw [h] at detected
    simp [fallbackDecision] at detected
    simp_all

/-- The abstract authority the earlier integer policy received as `detected`;
zero stands for "no plausible authority" and is clamped to one. -/
def legacyDetected (topology : CpuTopology) : Nat :=
  if topology.darwin then
    match plausible topology.darwinPerformanceCores with
    | some performance => performance
    | none =>
      if topology.intelMachine then
        match topology.darwinTranslation with
        | .native => (plausible topology.darwinPhysicalCores).getD 0
        | _ => 0
      else 0
  else
    match topology.processCpuCount with
    | some reported => (plausible (some reported)).getD 0
    | none => (plausible topology.cpuCount).getD 0

theorem plausible_authority_clamps_like_previous_policy
    (authority : Nat) (bounds : 1 ≤ authority ∧ authority ≤ MaxPlausibleCpus) :
    min authority HardWorkerCap = automaticWorkerCount authority := by
  unfold automaticWorkerCount HardWorkerCap
  split
  · omega
  · split <;> omega

theorem no_authority_clamps_to_one : automaticWorkerCount 0 = 1 := by
  rfl

theorem decision_selects_exactly_the_previous_policy
    (configured : Option Nat) (topology : CpuTopology) :
    (decide configured topology).selected =
      resolveWorkerCount configured (legacyDetected topology) := by
  cases configured with
  | some workers => rfl
  | none =>
    rw [decide_none]
    unfold resolveWorkerCount automaticDecision darwinAutomatic otherAutomatic
      legacyDetected
    split <;> split <;> (try split) <;> (try split) <;> (try split) <;>
      simp_all [detectedDecision, fallbackDecision, no_authority_clamps_to_one] <;>
      exact plausible_authority_clamps_like_previous_policy _
        (by simp_all [plausible_eq_some_iff])

/-- The structured startup log is a pure projection of one decision. -/
structure LogRecord where
  mode : WorkerMode
  configured : Option Nat
  selected : Nat
  detected : Option Nat
  hardCap : Nat
  topology : CpuTopology
  reason : WorkerReason
deriving DecidableEq, Repr

def observe (decision : WorkerDecision) (topology : CpuTopology) : LogRecord :=
  ⟨decision.mode, decision.configured, decision.selected, decision.detected,
    HardWorkerCap, topology, decision.reason⟩

theorem observation_reports_the_decision_unchanged
    (configured : Option Nat) (topology : CpuTopology) :
    (observe (decide configured topology) topology).selected =
        (decide configured topology).selected ∧
      (observe (decide configured topology) topology).mode =
        (decide configured topology).mode ∧
      (observe (decide configured topology) topology).reason =
        (decide configured topology).reason ∧
      (observe (decide configured topology) topology).hardCap = HardWorkerCap := by
  exact ⟨rfl, rfl, rfl, rfl⟩

def nextBatchSize (workers remaining : Nat) : Nat :=
  min workers remaining

theorem next_batch_size_le_worker_count
    (workers remaining : Nat) :
    nextBatchSize workers remaining ≤ workers := by
  exact Nat.min_le_left workers remaining

theorem next_batch_size_is_hard_bounded
    (workers remaining : Nat)
    (valid : ValidWorkerCount workers) :
    nextBatchSize workers remaining ≤ HardWorkerCap := by
  exact Nat.le_trans
    (next_batch_size_le_worker_count workers remaining) valid.2

def canonicalOrder (pageCount : Nat) : List (Fin pageCount) :=
  List.ofFn fun index => index

structure BoundedExecution
    (pageCount : Nat)
    (Page Result : Type)
    (pages : Fin pageCount → Page)
    (render : Page → Result) where
  workerCount : Nat
  workerCountValid : ValidWorkerCount workerCount
  /-- Arbitrary worker-completion order. -/
  completionOrder : List (Fin pageCount)
  completionOrderExact : completionOrder.Perm (canonicalOrder pageCount)
  /-- Arbitrary batch assignment; every actual batch is worker-bounded. -/
  batchOf : Fin pageCount → Nat
  batchBounded : ∀ batch,
    (completionOrder.filter fun page => batchOf page == batch).length ≤
      workerCount
  /-- Results are addressable by original index after workers finish. -/
  completedAt : Fin pageCount → Result
  completionExact : ∀ index, completedAt index = render (pages index)

def sequentialMap
    {pageCount : Nat}
    {Page Result : Type}
    (pages : Fin pageCount → Page)
    (render : Page → Result) : List Result :=
  List.ofFn fun index => render (pages index)

def orderedCollect
    {pageCount : Nat}
    {Page Result : Type}
    {pages : Fin pageCount → Page}
    {render : Page → Result}
    (execution : BoundedExecution pageCount Page Result pages render) :
    List Result :=
  List.ofFn execution.completedAt

theorem arbitrary_bounded_schedule_ordered_collect_equals_sequential_map
    {pageCount : Nat}
    {Page Result : Type}
    (pages : Fin pageCount → Page)
    (render : Page → Result)
    (execution : BoundedExecution pageCount Page Result pages render) :
    orderedCollect execution = sequentialMap pages render := by
  unfold orderedCollect sequentialMap
  rw [show execution.completedAt = fun index => render (pages index) by
    funext index
    exact execution.completionExact index]

theorem completion_schedule_contains_every_page_exactly_once
    {pageCount : Nat}
    {Page Result : Type}
    {pages : Fin pageCount → Page}
    {render : Page → Result}
    (execution : BoundedExecution pageCount Page Result pages render) :
    execution.completionOrder.Perm (canonicalOrder pageCount) :=
  execution.completionOrderExact

def scheduledBatchSize
    {pageCount : Nat}
    {Page Result : Type}
    {pages : Fin pageCount → Page}
    {render : Page → Result}
    (execution : BoundedExecution pageCount Page Result pages render)
    (batch : Nat) : Nat :=
  (execution.completionOrder.filter fun page =>
    execution.batchOf page == batch).length

theorem scheduled_batch_size_le_worker_count
    {pageCount : Nat}
    {Page Result : Type}
    {pages : Fin pageCount → Page}
    {render : Page → Result}
    (execution : BoundedExecution pageCount Page Result pages render)
    (batch : Nat) :
    scheduledBatchSize execution batch ≤ execution.workerCount :=
  execution.batchBounded batch

theorem worker_count_is_valid_and_every_batch_is_worker_bounded
    {pageCount : Nat}
    {Page Result : Type}
    {pages : Fin pageCount → Page}
    {render : Page → Result}
    (execution : BoundedExecution pageCount Page Result pages render)
    (batch : Nat) :
    ValidWorkerCount execution.workerCount ∧
      scheduledBatchSize execution batch ≤ execution.workerCount :=
  ⟨execution.workerCountValid,
    scheduled_batch_size_le_worker_count execution batch⟩

theorem worker_and_every_batch_are_hard_bounded
    {pageCount : Nat}
    {Page Result : Type}
    {pages : Fin pageCount → Page}
    {render : Page → Result}
    (execution : BoundedExecution pageCount Page Result pages render)
    (batch : Nat) :
    execution.workerCount ≤ HardWorkerCap ∧
      scheduledBatchSize execution batch ≤ HardWorkerCap := by
  exact ⟨execution.workerCountValid.2,
    Nat.le_trans (scheduled_batch_size_le_worker_count execution batch)
      execution.workerCountValid.2⟩

structure RenderedPage where
  bytes : List Nat
  evidence : Nat
deriving DecidableEq, Repr

structure SerializedArchive where
  bytes : List Nat
  pageEvidence : List Nat
deriving DecidableEq, Repr

def serializeOrdered
    (metadata : List Nat)
    (pages : List RenderedPage) : SerializedArchive :=
  ⟨metadata ++ pages.flatMap RenderedPage.bytes,
    pages.map RenderedPage.evidence⟩

theorem ordered_serialization_equals_sequential_serialization
    {pageCount : Nat}
    {Page : Type}
    (pages : Fin pageCount → Page)
    (render : Page → RenderedPage)
    (execution :
      BoundedExecution pageCount Page RenderedPage pages render)
    (metadata : List Nat) :
    serializeOrdered metadata (orderedCollect execution) =
      serializeOrdered metadata (sequentialMap pages render) := by
  rw [arbitrary_bounded_schedule_ordered_collect_equals_sequential_map
    pages render execution]

theorem every_deterministic_serializer_observes_sequential_input
    {pageCount : Nat}
    {Page Result Archive : Type}
    (pages : Fin pageCount → Page)
    (render : Page → Result)
    (execution : BoundedExecution pageCount Page Result pages render)
    (serialize : List Result → Archive) :
    serialize (orderedCollect execution) =
      serialize (sequentialMap pages render) := by
  rw [arbitrary_bounded_schedule_ordered_collect_equals_sequential_map
    pages render execution]

inductive PreparationResult where
  | ready (archive : SerializedArchive)
  | workerFailure
  | validationFailure
  | serializationFailure
deriving DecidableEq, Repr

/-- Destination mutation is the last step and occurs only for `ready`. -/
def publishLast
    (destination : List Nat)
    (prepared : PreparationResult) : List Nat :=
  match prepared with
  | .ready archive => archive.bytes
  | .workerFailure => destination
  | .validationFailure => destination
  | .serializationFailure => destination

theorem worker_failure_preserves_destination
    (destination : List Nat) :
    publishLast destination .workerFailure = destination := by
  rfl

theorem validation_failure_preserves_destination
    (destination : List Nat) :
    publishLast destination .validationFailure = destination := by
  rfl

theorem serialization_failure_preserves_destination
    (destination : List Nat) :
    publishLast destination .serializationFailure = destination := by
  rfl

theorem every_prepublication_failure_preserves_destination
    (destination : List Nat)
    (result : PreparationResult)
    (failed : result = .workerFailure ∨ result = .validationFailure ∨
      result = .serializationFailure) :
    publishLast destination result = destination := by
  cases result <;> simp_all [publishLast]

end H2HDBIngest.Verification.OrderedPageRendering
