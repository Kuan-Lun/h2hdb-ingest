------------------------- MODULE IngestRuntimeLifecycle ----------------------
EXTENDS FiniteSets, Naturals, TLC

(***************************************************************************
Finite refinement model for the ingest runtime ownership boundary.

Build may fail after acquiring the one facade. A completed runtime may receive
two arbitrarily ordered close calls, or finish a CLI body through any modeled
normal or exceptional outcome. Close is an abstract atomic linearization point:
the first close transitions the facade to CLOSED and later closes replay without
another delegate call. Nested or post-close reentry is rejected, as is work
after close.

TLC checks every reachable state of this fixed finite model. It does not prove
Python Lock scheduling or waiting, context-manager and BaseException unwinding,
signal delivery, core cache behavior, destructor behavior, or that production
Python refines these transitions. Deterministic thread, fault, and CLI tests are
the implementation evidence at those boundaries.
***************************************************************************)

Actors == {"FIRST", "SECOND"}
ExitKinds ==
    {"NORMAL", "ONCE", "EXCEPTION", "SYSTEM_EXIT", "KEYBOARD_INTERRUPT"}

VARIABLES
    buildState,
    runtimeState,
    facadeState,
    delegateCloseCount,
    closeReturned,
    cliState,
    cliOutcome,
    reentryAttempted,
    reentryRejected,
    operationAttempted,
    operationRejected,
    everClosed,
    lastEvent

vars ==
    <<buildState, runtimeState, facadeState, delegateCloseCount,
      closeReturned, cliState, cliOutcome, reentryAttempted,
      reentryRejected, operationAttempted, operationRejected, everClosed,
      lastEvent>>

Init ==
    /\ buildState = "START"
    /\ runtimeState = "ABSENT"
    /\ facadeState = "ABSENT"
    /\ delegateCloseCount = 0
    /\ closeReturned = {}
    /\ cliState = "NOT_STARTED"
    /\ cliOutcome = "NONE"
    /\ reentryAttempted = FALSE
    /\ reentryRejected = FALSE
    /\ operationAttempted = FALSE
    /\ operationRejected = FALSE
    /\ everClosed = FALSE
    /\ lastEvent = "INIT"

AllocateFacade ==
    /\ buildState = "START"
    /\ buildState' = "FACADE_OWNED"
    /\ facadeState' = "OPEN"
    /\ lastEvent' = "ALLOCATE_FACADE"
    /\ UNCHANGED <<runtimeState, delegateCloseCount, closeReturned,
                    cliState, cliOutcome, reentryAttempted, reentryRejected,
                    operationAttempted, operationRejected, everClosed>>

BuildSuccess ==
    /\ buildState = "FACADE_OWNED"
    /\ buildState' = "COMPLETE"
    /\ runtimeState' = "OPEN"
    /\ lastEvent' = "BUILD_SUCCESS"
    /\ UNCHANGED <<facadeState, delegateCloseCount, closeReturned,
                    cliState, cliOutcome, reentryAttempted, reentryRejected,
                    operationAttempted, operationRejected, everClosed>>

BuildFailure ==
    /\ buildState = "FACADE_OWNED"
    /\ buildState' = "FAILED"
    /\ facadeState' = "CLOSED"
    /\ delegateCloseCount' = 1
    /\ everClosed' = TRUE
    /\ lastEvent' = "BUILD_FAILURE_CLOSE"
    /\ UNCHANGED <<runtimeState, closeReturned, cliState, cliOutcome,
                    reentryAttempted, reentryRejected, operationAttempted,
                    operationRejected>>

StartCli ==
    /\ buildState = "COMPLETE"
    /\ runtimeState = "OPEN"
    /\ cliState = "NOT_STARTED"
    /\ cliState' = "RUNNING"
    /\ lastEvent' = "START_CLI"
    /\ UNCHANGED <<buildState, runtimeState, facadeState,
                    delegateCloseCount, closeReturned, cliOutcome,
                    reentryAttempted, reentryRejected, operationAttempted,
                    operationRejected, everClosed>>

FinishCli(kind) ==
    /\ cliState = "RUNNING"
    /\ kind \in ExitKinds
    /\ cliState' = "DONE"
    /\ cliOutcome' = kind
    /\ runtimeState' = "CLOSED"
    /\ facadeState' = "CLOSED"
    /\ delegateCloseCount' =
        IF facadeState = "OPEN"
        THEN delegateCloseCount + 1
        ELSE delegateCloseCount
    /\ everClosed' = TRUE
    /\ lastEvent' = "CLI_CONTEXT_CLOSE"
    /\ UNCHANGED <<buildState, closeReturned, reentryAttempted,
                    reentryRejected, operationAttempted, operationRejected>>

ExplicitClose(actor) ==
    /\ buildState = "COMPLETE"
    /\ actor \in Actors \ closeReturned
    /\ runtimeState \in {"OPEN", "CLOSED"}
    /\ closeReturned' = closeReturned \cup {actor}
    /\ runtimeState' = "CLOSED"
    /\ facadeState' = "CLOSED"
    /\ delegateCloseCount' =
        IF facadeState = "OPEN"
        THEN delegateCloseCount + 1
        ELSE delegateCloseCount
    /\ everClosed' = TRUE
    /\ lastEvent' = "EXPLICIT_CLOSE"
    /\ UNCHANGED <<buildState, cliState, cliOutcome, reentryAttempted,
                    reentryRejected, operationAttempted, operationRejected>>

AttemptReentry ==
    /\ \/ cliState = "RUNNING"
       \/ runtimeState = "CLOSED"
    /\ ~reentryAttempted
    /\ reentryAttempted' = TRUE
    /\ reentryRejected' = TRUE
    /\ lastEvent' = "REJECT_REENTRY"
    /\ UNCHANGED <<buildState, runtimeState, facadeState,
                    delegateCloseCount, closeReturned, cliState, cliOutcome,
                    operationAttempted, operationRejected, everClosed>>

AttemptOperation ==
    /\ runtimeState = "CLOSED"
    /\ ~operationAttempted
    /\ operationAttempted' = TRUE
    /\ operationRejected' = TRUE
    /\ lastEvent' = "REJECT_OPERATION"
    /\ UNCHANGED <<buildState, runtimeState, facadeState,
                    delegateCloseCount, closeReturned, cliState, cliOutcome,
                    reentryAttempted, reentryRejected, everClosed>>

TerminalStutter ==
    /\ \/ buildState = "FAILED"
       \/ runtimeState = "CLOSED"
    /\ UNCHANGED vars

Next ==
    \/ AllocateFacade
    \/ BuildSuccess
    \/ BuildFailure
    \/ StartCli
    \/ \E kind \in ExitKinds : FinishCli(kind)
    \/ \E actor \in Actors : ExplicitClose(actor)
    \/ AttemptReentry
    \/ AttemptOperation
    \/ TerminalStutter

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ buildState \in {"START", "FACADE_OWNED", "COMPLETE", "FAILED"}
    /\ runtimeState \in {"ABSENT", "OPEN", "CLOSED"}
    /\ facadeState \in {"ABSENT", "OPEN", "CLOSED"}
    /\ delegateCloseCount \in 0..1
    /\ closeReturned \subseteq Actors
    /\ cliState \in {"NOT_STARTED", "RUNNING", "DONE"}
    /\ cliOutcome \in ExitKinds \cup {"NONE"}
    /\ reentryAttempted \in BOOLEAN
    /\ reentryRejected \in BOOLEAN
    /\ operationAttempted \in BOOLEAN
    /\ operationRejected \in BOOLEAN
    /\ everClosed \in BOOLEAN
    /\ lastEvent \in
        {"INIT", "ALLOCATE_FACADE", "BUILD_SUCCESS",
         "BUILD_FAILURE_CLOSE", "START_CLI", "CLI_CONTEXT_CLOSE",
         "EXPLICIT_CLOSE", "REJECT_REENTRY", "REJECT_OPERATION"}

FacadeOwnershipIsExact ==
    /\ (facadeState = "ABSENT" <=> buildState = "START")
    /\ (runtimeState = "OPEN" => facadeState = "OPEN")
    /\ (runtimeState = "CLOSED" => facadeState = "CLOSED")
    /\ (buildState = "FAILED" => facadeState = "CLOSED")

DelegateCloseIsExactOnce ==
    (delegateCloseCount = 1) <=> (facadeState = "CLOSED")

ReturnedCloseIsTerminal ==
    closeReturned # {} =>
        /\ runtimeState = "CLOSED"
        /\ facadeState = "CLOSED"

EveryCliExitIsClosed ==
    cliState = "DONE" =>
        /\ cliOutcome \in ExitKinds
        /\ runtimeState = "CLOSED"
        /\ facadeState = "CLOSED"

BuildFailureClosesOwnedFacade ==
    buildState = "FAILED" =>
        /\ runtimeState = "ABSENT"
        /\ facadeState = "CLOSED"
        /\ delegateCloseCount = 1

ClosedRuntimeNeverReopens ==
    everClosed => runtimeState # "OPEN"

ReentryFailsClosed ==
    reentryAttempted =>
        /\ reentryRejected
        /\ \/ cliState = "RUNNING"
           \/ runtimeState = "CLOSED"

OperationFailsClosed ==
    operationAttempted =>
        /\ operationRejected
        /\ runtimeState = "CLOSED"

=============================================================================
