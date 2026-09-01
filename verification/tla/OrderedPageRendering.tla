------------------------- MODULE OrderedPageRendering -------------------------
EXTENDS FiniteSets, Naturals, Sequences, TLC

(***************************************************************************
Finite refinement model for bounded concurrent page rendering.

Rendered(page) is a deterministic pure page renderer.  A run chooses a worker
count and arbitrary non-empty batches no larger than that count.  CompletePage
may finish each page in any order, while CollectPage can append only the next
canonical page index.  Validation and serialization happen after every page
has been collected.  PublishLast is the sole action that changes destination.

TLC exhaustively checks the configured finite PageCount and MaxWorkers.  This
model does not establish Pillow determinism or thread safety, Python executor
or future semantics, cancellation and spool cleanup, ZIP behavior, filesystem
atomicity or durability, or refinement by the production implementation.  It
does not model a failure during the final destination write itself.
***************************************************************************)

CONSTANTS PageCount, MaxWorkers

ASSUME /\ PageCount \in Nat \ {0}
       /\ MaxWorkers = 4

Pages == 1..PageCount

Rendered(page) == page * 10

SequentialPages == [page \in Pages |-> Rendered(page)]

ArchiveHeader == <<999>>
SequentialArchive == ArchiveHeader \o SequentialPages
OriginalDestination == <<777>>
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
    destination,
    failureKind,
    lastEvent

vars ==
    <<workerCount, phase, nextPage, batchStart, batchEnd, completed,
      collectCursor, collected, staged, destination, failureKind, lastEvent>>

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
    /\ destination = OriginalDestination
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
                    destination, failureKind>>

CompletePage(page) ==
    /\ phase = "RENDERING"
    /\ batchStart # 0
    /\ page \in CurrentBatch \ completed
    /\ completed' = completed \cup {page}
    /\ lastEvent' = "COMPLETE_PAGE"
    /\ UNCHANGED <<workerCount, phase, nextPage, batchStart, batchEnd,
                    collectCursor, collected, staged, destination,
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
                    completed, staged, destination, failureKind>>

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
    /\ UNCHANGED <<workerCount, collected, staged, destination, failureKind>>

WorkerFailure ==
    /\ phase = "RENDERING"
    /\ phase' = "FAILED"
    /\ failureKind' = "WORKER"
    /\ lastEvent' = "WORKER_FAILURE"
    /\ UNCHANGED <<workerCount, nextPage, batchStart, batchEnd, completed,
                    collectCursor, collected, staged, destination>>

ValidationSuccess ==
    /\ phase = "VALIDATING"
    /\ phase' = "SERIALIZING"
    /\ lastEvent' = "VALIDATION_SUCCESS"
    /\ UNCHANGED <<workerCount, nextPage, batchStart, batchEnd, completed,
                    collectCursor, collected, staged, destination,
                    failureKind>>

ValidationFailure ==
    /\ phase = "VALIDATING"
    /\ phase' = "FAILED"
    /\ failureKind' = "VALIDATION"
    /\ lastEvent' = "VALIDATION_FAILURE"
    /\ UNCHANGED <<workerCount, nextPage, batchStart, batchEnd, completed,
                    collectCursor, collected, staged, destination>>

SerializationSuccess ==
    /\ phase = "SERIALIZING"
    /\ staged' = ArchiveHeader \o collected
    /\ phase' = "READY"
    /\ lastEvent' = "SERIALIZATION_SUCCESS"
    /\ UNCHANGED <<workerCount, nextPage, batchStart, batchEnd, completed,
                    collectCursor, collected, destination, failureKind>>

SerializationFailure ==
    /\ phase = "SERIALIZING"
    /\ staged' = FailedStaging
    /\ phase' = "FAILED"
    /\ failureKind' = "SERIALIZATION"
    /\ lastEvent' = "SERIALIZATION_FAILURE"
    /\ UNCHANGED <<workerCount, nextPage, batchStart, batchEnd, completed,
                    collectCursor, collected, destination>>

PublishLast ==
    /\ phase = "READY"
    /\ destination' = staged
    /\ phase' = "PUBLISHED"
    /\ lastEvent' = "PUBLISH_LAST"
    /\ UNCHANGED <<workerCount, nextPage, batchStart, batchEnd, completed,
                    collectCursor, collected, staged, failureKind>>

TerminalStutter ==
    /\ phase \in {"FAILED", "PUBLISHED"}
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
    \/ PublishLast
    \/ TerminalStutter

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ workerCount \in 1..MaxWorkers
    /\ phase \in
        {"RENDERING", "VALIDATING", "SERIALIZING", "READY", "PUBLISHED",
         "FAILED"}
    /\ nextPage \in 1..(PageCount + 1)
    /\ batchStart \in 0..PageCount
    /\ batchEnd \in 0..PageCount
    /\ completed \subseteq Pages
    /\ collectCursor \in 0..(PageCount + 1)
    /\ collected \in Seq(Nat)
    /\ staged \in Seq(Nat)
    /\ destination \in Seq(Nat)
    /\ failureKind \in {"NONE", "WORKER", "VALIDATION", "SERIALIZATION"}
    /\ lastEvent \in
        {"INIT", "START_BATCH", "COMPLETE_PAGE", "COLLECT_PAGE",
         "FINISH_BATCH", "WORKER_FAILURE", "VALIDATION_SUCCESS",
         "VALIDATION_FAILURE", "SERIALIZATION_SUCCESS",
         "SERIALIZATION_FAILURE", "PUBLISH_LAST"}

WorkerCountHardBound ==
    /\ 1 \leq workerCount
    /\ workerCount \leq MaxWorkers
    /\ workerCount \leq 4

BatchSizeHardBound ==
    /\ Cardinality(CurrentBatch) \leq workerCount
    /\ Cardinality(CurrentBatch) \leq MaxWorkers

FinishedWithinCurrentBatch ==
    completed \subseteq CurrentBatch

OrderedCollectIsSequentialPrefix ==
    /\ Len(collected) \leq PageCount
    /\ collected = [page \in 1..Len(collected) |-> Rendered(page)]

AllCollectedBeforePostProcessing ==
    phase \in {"VALIDATING", "SERIALIZING", "READY", "PUBLISHED"}
        => collected = SequentialPages

ReadyOrPublishedHasSequentialSerialization ==
    phase \in {"READY", "PUBLISHED"} => staged = SequentialArchive

FailurePreservesDestination ==
    phase = "FAILED" => destination = OriginalDestination

DestinationChangesOnlyAtPublish ==
    phase # "PUBLISHED" => destination = OriginalDestination

PublishedDestinationIsSequential ==
    phase = "PUBLISHED" => destination = SequentialArchive

Safety ==
    /\ TypeOK
    /\ WorkerCountHardBound
    /\ BatchSizeHardBound
    /\ FinishedWithinCurrentBatch
    /\ OrderedCollectIsSequentialPrefix
    /\ AllCollectedBeforePostProcessing
    /\ ReadyOrPublishedHasSequentialSerialization
    /\ FailurePreservesDestination
    /\ DestinationChangesOnlyAtPublish
    /\ PublishedDestinationIsSequential

=============================================================================
