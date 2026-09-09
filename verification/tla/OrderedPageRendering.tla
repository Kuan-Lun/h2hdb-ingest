------------------------- MODULE OrderedPageRendering -------------------------
EXTENDS FiniteSets, Naturals, Sequences, TLC

(***************************************************************************
Finite abstract model for bounded concurrent page rendering.

Rendered(page) is a deterministic pure page renderer.  A run chooses a worker
count and arbitrary non-empty batches no larger than that count.  CompletePage
may finish each page in any order, while CollectPage can append only the next
canonical page index. Abstract validation and serialization happen after every
page has been collected. READY contains the exact sequential serialization.

The abstract staged value is not a second CBZ file or a storage-I/O schedule.
Runtime writes directly into caller-owned, unpublished scratch: scratch bytes
may change during rendering. Failure attempts to discard those bytes, but
cleanup itself can fail. FAILED deliberately imposes no empty-scratch or
prior-destination-preservation requirement. Reader-visible publication belongs
to the separate library activation protocol, not to this renderer model.

TLC exhaustively checks the configured finite PageCount and MaxWorkers.  This
model does not establish Pillow determinism or thread safety, Python executor
or future semantics, cancellation and spool cleanup, ZIP behavior, filesystem
atomicity or durability, or refinement by the production implementation.
***************************************************************************)

CONSTANTS PageCount, MaxWorkers

ASSUME /\ PageCount \in Nat \ {0}
       /\ MaxWorkers = 16

Pages == 1..PageCount

Rendered(page) == page * 10

SequentialPages == [page \in Pages |-> Rendered(page)]

ArchiveHeader == <<999>>
SequentialArchive == ArchiveHeader \o SequentialPages
FailedStaging == <<888>>

VARIABLES
    workerCount,
    phase,
    nextPage,
    batchStart,
    batchEnd,
    completed,
    collectCursor,
    collected,
    staged,
    failureKind,
    lastEvent

vars ==
    <<workerCount, phase, nextPage, batchStart, batchEnd, completed,
      collectCursor, collected, staged, failureKind, lastEvent>>

CurrentBatch ==
    IF batchStart = 0 THEN {} ELSE batchStart..batchEnd

Init ==
    /\ workerCount \in 1..MaxWorkers
    /\ phase = "RENDERING"
    /\ nextPage = 1
    /\ batchStart = 0
    /\ batchEnd = 0
    /\ completed = {}
    /\ collectCursor = 0
    /\ collected = <<>>
    /\ staged = <<>>
    /\ failureKind = "NONE"
    /\ lastEvent = "INIT"

StartBatch ==
    LET remaining == PageCount - nextPage + 1
        largest == IF workerCount < remaining THEN workerCount ELSE remaining
    IN
    /\ phase = "RENDERING"
    /\ nextPage \in Pages
    /\ batchStart = 0
    /\ \E size \in 1..largest :
        /\ batchStart' = nextPage
        /\ batchEnd' = nextPage + size - 1
        /\ collectCursor' = nextPage
    /\ completed' = {}
    /\ lastEvent' = "START_BATCH"
    /\ UNCHANGED <<workerCount, phase, nextPage, collected, staged,
                    failureKind>>

CompletePage(page) ==
    /\ phase = "RENDERING"
    /\ batchStart # 0
    /\ page \in CurrentBatch \ completed
    /\ completed' = completed \cup {page}
    /\ lastEvent' = "COMPLETE_PAGE"
    /\ UNCHANGED <<workerCount, phase, nextPage, batchStart, batchEnd,
                    collectCursor, collected, staged,
                    failureKind>>

CollectPage ==
    /\ phase = "RENDERING"
    /\ batchStart # 0
    /\ collectCursor \in completed
    /\ collectCursor \leq batchEnd
    /\ collected' = Append(collected, Rendered(collectCursor))
    /\ collectCursor' = collectCursor + 1
    /\ lastEvent' = "COLLECT_PAGE"
    /\ UNCHANGED <<workerCount, phase, nextPage, batchStart, batchEnd,
                    completed, staged, failureKind>>

FinishBatch ==
    /\ phase = "RENDERING"
    /\ batchStart # 0
    /\ collectCursor = batchEnd + 1
    /\ nextPage' = batchEnd + 1
    /\ phase' = IF batchEnd = PageCount THEN "VALIDATING" ELSE "RENDERING"
    /\ batchStart' = 0
    /\ batchEnd' = 0
    /\ completed' = {}
    /\ collectCursor' = 0
    /\ lastEvent' = "FINISH_BATCH"
    /\ UNCHANGED <<workerCount, collected, staged, failureKind>>

WorkerFailure ==
    /\ phase = "RENDERING"
    /\ phase' = "FAILED"
    /\ failureKind' = "WORKER"
    /\ lastEvent' = "WORKER_FAILURE"
    /\ UNCHANGED <<workerCount, nextPage, batchStart, batchEnd, completed,
                    collectCursor, collected, staged>>

ValidationSuccess ==
    /\ phase = "VALIDATING"
    /\ phase' = "SERIALIZING"
    /\ lastEvent' = "VALIDATION_SUCCESS"
    /\ UNCHANGED <<workerCount, nextPage, batchStart, batchEnd, completed,
                    collectCursor, collected, staged,
                    failureKind>>

ValidationFailure ==
    /\ phase = "VALIDATING"
    /\ phase' = "FAILED"
    /\ failureKind' = "VALIDATION"
    /\ lastEvent' = "VALIDATION_FAILURE"
    /\ UNCHANGED <<workerCount, nextPage, batchStart, batchEnd, completed,
                    collectCursor, collected, staged>>

SerializationSuccess ==
    /\ phase = "SERIALIZING"
    /\ staged' = ArchiveHeader \o collected
    /\ phase' = "READY"
    /\ lastEvent' = "SERIALIZATION_SUCCESS"
    /\ UNCHANGED <<workerCount, nextPage, batchStart, batchEnd, completed,
                    collectCursor, collected, failureKind>>

SerializationFailure ==
    /\ phase = "SERIALIZING"
    /\ staged' = FailedStaging
    /\ phase' = "FAILED"
    /\ failureKind' = "SERIALIZATION"
    /\ lastEvent' = "SERIALIZATION_FAILURE"
    /\ UNCHANGED <<workerCount, nextPage, batchStart, batchEnd, completed,
                    collectCursor, collected>>

TerminalStutter ==
    /\ phase \in {"FAILED", "READY"}
    /\ UNCHANGED vars

Next ==
    \/ StartBatch
    \/ \E page \in Pages : CompletePage(page)
    \/ CollectPage
    \/ FinishBatch
    \/ WorkerFailure
    \/ ValidationSuccess
    \/ ValidationFailure
    \/ SerializationSuccess
    \/ SerializationFailure
    \/ TerminalStutter

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ workerCount \in 1..MaxWorkers
    /\ phase \in
        {"RENDERING", "VALIDATING", "SERIALIZING", "READY", "FAILED"}
    /\ nextPage \in 1..(PageCount + 1)
    /\ batchStart \in 0..PageCount
    /\ batchEnd \in 0..PageCount
    /\ completed \subseteq Pages
    /\ collectCursor \in 0..(PageCount + 1)
    /\ collected \in Seq(Nat)
    /\ staged \in Seq(Nat)
    /\ failureKind \in {"NONE", "WORKER", "VALIDATION", "SERIALIZATION"}
    /\ lastEvent \in
        {"INIT", "START_BATCH", "COMPLETE_PAGE", "COLLECT_PAGE",
         "FINISH_BATCH", "WORKER_FAILURE", "VALIDATION_SUCCESS",
         "VALIDATION_FAILURE", "SERIALIZATION_SUCCESS",
         "SERIALIZATION_FAILURE"}

WorkerCountHardBound ==
    /\ 1 \leq workerCount
    /\ workerCount \leq MaxWorkers
    /\ workerCount \leq 16

BatchSizeHardBound ==
    /\ Cardinality(CurrentBatch) \leq workerCount
    /\ Cardinality(CurrentBatch) \leq MaxWorkers

FinishedWithinCurrentBatch ==
    completed \subseteq CurrentBatch

OrderedCollectIsSequentialPrefix ==
    /\ Len(collected) \leq PageCount
    /\ collected = [page \in 1..Len(collected) |-> Rendered(page)]

AllCollectedBeforePostProcessing ==
    phase \in {"VALIDATING", "SERIALIZING", "READY"}
        => collected = SequentialPages

ReadyHasSequentialSerialization ==
    phase = "READY" => staged = SequentialArchive

Safety ==
    /\ TypeOK
    /\ WorkerCountHardBound
    /\ BatchSizeHardBound
    /\ FinishedWithinCurrentBatch
    /\ OrderedCollectIsSequentialPrefix
    /\ AllCollectedBeforePostProcessing
    /\ ReadyHasSequentialSerialization

=============================================================================
