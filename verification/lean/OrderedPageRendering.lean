import Std

/-!
# Bounded concurrent page rendering with ordered publication

The renderer is modeled as a deterministic pure function from one indexed page
to its exact bytes and evidence.  A `BoundedExecution` may complete pages in any
permutation and group them into arbitrary batches, provided every page occurs
exactly once, worker count is in `1..4`, and every batch is no larger than that
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

def ValidWorkerCount (workers : Nat) : Prop :=
  1 ≤ workers ∧ workers ≤ 4

def nextBatchSize (workers remaining : Nat) : Nat :=
  min workers remaining

theorem next_batch_size_le_worker_count
    (workers remaining : Nat) :
    nextBatchSize workers remaining ≤ workers := by
  exact Nat.min_le_left workers remaining

theorem next_batch_size_is_hard_bounded
    (workers remaining : Nat)
    (valid : ValidWorkerCount workers) :
    nextBatchSize workers remaining ≤ 4 := by
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

theorem worker_and_every_batch_are_hard_bounded_by_four
    {pageCount : Nat}
    {Page Result : Type}
    {pages : Fin pageCount → Page}
    {render : Page → Result}
    (execution : BoundedExecution pageCount Page Result pages render)
    (batch : Nat) :
    execution.workerCount ≤ 4 ∧ scheduledBatchSize execution batch ≤ 4 := by
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
