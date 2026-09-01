------------------------- MODULE LibraryIoReservation ------------------------
EXTENDS TLC

(***************************************************************************
Finite crash model for the filesystem library's split critical sections.

The durable journal reserves WRITING before source I/O and operationStarted
before activation I/O.  PROTECT represents one exact-object token stripe;
unrelated stripes are intentionally outside this single-object model.  The
publication gate is released by a crash, while journal, independently verified
digest authority, and filesystem locations survive.  Release of this exact
object is rejected while its durable activation entry is unfinished, including
the rename-to-terminalize crash gap.

This model deliberately treats fsync/rename/flock and exact inode authority as
abstract atomic facts.  TLC explores every reachable state only for the finite
constants in its configuration.  It does not prove that Python, SQLite, POSIX
filesystems, or any particular platform refines these transitions.
***************************************************************************)

CONSTANTS Digests, NoDigest

ASSUME /\ Digests # {}
       /\ NoDigest \notin Digests

DigestOrNone == Digests \cup {NoDigest}

VARIABLES
    running,
    gate,
    journal,
    reservedDigest,
    temporaryDigest,
    stagedDigest,
    currentDigest,
    activationPending,
    operationStarted,
    activated,
    everReleased,
    lastCopyAttempt,
    lastFenceAttempt,
    lastReleaseAttempt

DurableVariables ==
    <<journal, reservedDigest, temporaryDigest, stagedDigest, currentDigest,
      activationPending, operationStarted, activated, everReleased,
      lastCopyAttempt, lastFenceAttempt, lastReleaseAttempt>>

vars == <<running, gate, DurableVariables>>

Init ==
    /\ running = TRUE
    /\ gate = "FREE"
    /\ journal = "NONE"
    /\ reservedDigest = NoDigest
    /\ temporaryDigest = NoDigest
    /\ stagedDigest = NoDigest
    /\ currentDigest = NoDigest
    /\ activationPending = FALSE
    /\ operationStarted = FALSE
    /\ activated = FALSE
    /\ everReleased = FALSE
    /\ lastCopyAttempt =
        [expected |-> NoDigest, observed |-> NoDigest, accepted |-> FALSE]
    /\ lastFenceAttempt =
        [expected |-> NoDigest, observed |-> NoDigest, accepted |-> FALSE]
    /\ lastReleaseAttempt = [wasPending |-> FALSE, accepted |-> FALSE]

Crash ==
    /\ running
    /\ running' = FALSE
    /\ gate' = "FREE"
    /\ UNCHANGED DurableVariables

Restart ==
    /\ ~running
    /\ running' = TRUE
    /\ UNCHANGED <<gate, DurableVariables>>

ReserveProtect(digest) ==
    /\ running
    /\ gate = "FREE"
    /\ journal = "NONE"
    /\ ~activationPending
    /\ digest \in Digests
    /\ gate' = "PROTECT"
    /\ journal' = "WRITING"
    /\ reservedDigest' = digest
    /\ UNCHANGED <<running, temporaryDigest, stagedDigest, currentDigest,
                    activationPending, operationStarted, activated,
                    everReleased, lastCopyAttempt, lastFenceAttempt,
                    lastReleaseAttempt>>

ResumeProtect ==
    /\ running
    /\ gate = "FREE"
    /\ journal = "WRITING"
    /\ ~activationPending
    /\ gate' = "PROTECT"
    /\ UNCHANGED <<running, DurableVariables>>

CopyAndVerify(observed) ==
    LET accepted == observed = reservedDigest
    IN
    /\ running
    /\ gate = "PROTECT"
    /\ journal = "WRITING"
    /\ observed \in Digests
    /\ temporaryDigest' = observed
    /\ stagedDigest' = IF accepted THEN observed ELSE stagedDigest
    /\ lastCopyAttempt' =
        [expected |-> reservedDigest,
         observed |-> observed,
         accepted |-> accepted]
    /\ UNCHANGED <<running, gate, journal, reservedDigest, currentDigest,
                    activationPending, operationStarted, activated,
                    everReleased, lastFenceAttempt, lastReleaseAttempt>>

TerminalizeProtect ==
    /\ running
    /\ gate = "PROTECT"
    /\ journal = "WRITING"
    /\ reservedDigest \in Digests
    /\ stagedDigest = reservedDigest
    /\ journal' = "STAGED"
    /\ temporaryDigest' = NoDigest
    /\ lastFenceAttempt' =
        [expected |-> reservedDigest,
         observed |-> stagedDigest,
         accepted |-> TRUE]
    /\ UNCHANGED <<running, gate, reservedDigest, stagedDigest,
                    currentDigest, activationPending, operationStarted,
                    activated, everReleased, lastCopyAttempt,
                    lastReleaseAttempt>>

ReplayProtectTerminalize ==
    /\ running
    /\ gate = "PROTECT"
    /\ journal = "STAGED"
    /\ stagedDigest = reservedDigest
    /\ lastFenceAttempt' =
        [expected |-> reservedDigest,
         observed |-> stagedDigest,
         accepted |-> TRUE]
    /\ UNCHANGED <<running, gate, journal, reservedDigest, temporaryDigest,
                    stagedDigest, currentDigest, activationPending,
                    operationStarted, activated, everReleased,
                    lastCopyAttempt, lastReleaseAttempt>>

AttemptStaleTerminalize(supplied) ==
    /\ running
    /\ supplied \in Digests
    /\ ~(gate = "PROTECT" /\ journal \in {"WRITING", "STAGED"}
          /\ supplied = reservedDigest /\ stagedDigest = reservedDigest)
    /\ lastFenceAttempt' =
        [expected |-> reservedDigest,
         observed |-> supplied,
         accepted |-> FALSE]
    /\ UNCHANGED <<running, gate, journal, reservedDigest, temporaryDigest,
                    stagedDigest, currentDigest, activationPending,
                    operationStarted, activated, everReleased,
                    lastCopyAttempt, lastReleaseAttempt>>

FinishProtect ==
    /\ running
    /\ gate = "PROTECT"
    /\ journal = "STAGED"
    /\ gate' = "FREE"
    /\ UNCHANGED <<running, DurableVariables>>

BeginActivation ==
    /\ running
    /\ gate = "FREE"
    /\ journal = "STAGED"
    /\ ~activationPending
    /\ stagedDigest = reservedDigest
    /\ gate' = "ACTIVATE"
    /\ activationPending' = TRUE
    /\ operationStarted' = TRUE
    /\ UNCHANGED <<running, journal, reservedDigest, temporaryDigest,
                    stagedDigest, currentDigest, activated, everReleased,
                    lastCopyAttempt, lastFenceAttempt, lastReleaseAttempt>>

ResumeActivation ==
    /\ running
    /\ gate = "FREE"
    /\ activationPending
    /\ operationStarted
    /\ journal \in {"STAGED", "INSTALLED"}
    /\ gate' = "ACTIVATE"
    /\ UNCHANGED <<running, DurableVariables>>

MoveStagedToCurrent ==
    /\ running
    /\ gate = "ACTIVATE"
    /\ activationPending
    /\ operationStarted
    /\ journal = "STAGED"
    /\ stagedDigest = reservedDigest
    /\ currentDigest' = stagedDigest
    /\ stagedDigest' = NoDigest
    /\ UNCHANGED <<running, gate, journal, reservedDigest, temporaryDigest,
                    activationPending, operationStarted, activated,
                    everReleased, lastCopyAttempt, lastFenceAttempt,
                    lastReleaseAttempt>>

CommitActivation ==
    /\ running
    /\ gate = "ACTIVATE"
    /\ activationPending
    /\ operationStarted
    /\ journal = "STAGED"
    /\ currentDigest = reservedDigest
    /\ journal' = "INSTALLED"
    /\ activated' = TRUE
    /\ UNCHANGED <<running, gate, reservedDigest, temporaryDigest,
                    stagedDigest, currentDigest, activationPending,
                    operationStarted, everReleased, lastCopyAttempt,
                    lastFenceAttempt, lastReleaseAttempt>>

CompleteActivation ==
    /\ running
    /\ gate = "ACTIVATE"
    /\ activationPending
    /\ journal = "INSTALLED"
    /\ activated
    /\ gate' = "FREE"
    /\ activationPending' = FALSE
    /\ operationStarted' = FALSE
    /\ UNCHANGED <<running, journal, reservedDigest, temporaryDigest,
                    stagedDigest, currentDigest, activated, everReleased,
                    lastCopyAttempt, lastFenceAttempt, lastReleaseAttempt>>

AcquireRelease ==
    /\ running
    /\ gate = "FREE"
    /\ ~activationPending
    /\ journal \in {"WRITING", "STAGED", "INSTALLED", "RELEASED"}
    /\ gate' = "RELEASE"
    /\ lastReleaseAttempt' = [wasPending |-> FALSE, accepted |-> TRUE]
    /\ UNCHANGED <<running, journal, reservedDigest, temporaryDigest,
                    stagedDigest, currentDigest, activationPending,
                    operationStarted, activated, everReleased,
                    lastCopyAttempt, lastFenceAttempt>>

AttemptReleaseDuringActivation ==
    /\ running
    /\ activationPending
    /\ lastReleaseAttempt' = [wasPending |-> TRUE, accepted |-> FALSE]
    /\ UNCHANGED <<running, gate, journal, reservedDigest, temporaryDigest,
                    stagedDigest, currentDigest, activationPending,
                    operationStarted, activated, everReleased,
                    lastCopyAttempt, lastFenceAttempt>>

TombstoneRelease ==
    /\ running
    /\ gate = "RELEASE"
    /\ ~activationPending
    /\ journal \in {"WRITING", "STAGED", "INSTALLED", "RELEASED"}
    /\ journal' = "RELEASED"
    /\ everReleased' = TRUE
    /\ UNCHANGED <<running, gate, reservedDigest, temporaryDigest,
                    stagedDigest, currentDigest, activationPending,
                    operationStarted, activated, lastCopyAttempt,
                    lastFenceAttempt, lastReleaseAttempt>>

CleanupReleased ==
    /\ running
    /\ gate = "RELEASE"
    /\ journal = "RELEASED"
    /\ temporaryDigest' = NoDigest
    /\ stagedDigest' = NoDigest
    /\ UNCHANGED <<running, gate, journal, reservedDigest, currentDigest,
                    activationPending, operationStarted, activated,
                    everReleased, lastCopyAttempt, lastFenceAttempt,
                    lastReleaseAttempt>>

FinishRelease ==
    /\ running
    /\ gate = "RELEASE"
    /\ journal = "RELEASED"
    /\ temporaryDigest = NoDigest
    /\ stagedDigest = NoDigest
    /\ gate' = "FREE"
    /\ UNCHANGED <<running, DurableVariables>>

Next ==
    \/ Crash
    \/ Restart
    \/ \E digest \in Digests : ReserveProtect(digest)
    \/ ResumeProtect
    \/ \E observed \in Digests : CopyAndVerify(observed)
    \/ TerminalizeProtect
    \/ ReplayProtectTerminalize
    \/ \E supplied \in Digests : AttemptStaleTerminalize(supplied)
    \/ FinishProtect
    \/ BeginActivation
    \/ ResumeActivation
    \/ MoveStagedToCurrent
    \/ CommitActivation
    \/ CompleteActivation
    \/ AcquireRelease
    \/ AttemptReleaseDuringActivation
    \/ TombstoneRelease
    \/ CleanupReleased
    \/ FinishRelease

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ running \in BOOLEAN
    /\ gate \in {"FREE", "PROTECT", "ACTIVATE", "RELEASE"}
    /\ journal \in {"NONE", "WRITING", "STAGED", "INSTALLED", "RELEASED"}
    /\ reservedDigest \in DigestOrNone
    /\ temporaryDigest \in DigestOrNone
    /\ stagedDigest \in DigestOrNone
    /\ currentDigest \in DigestOrNone
    /\ activationPending \in BOOLEAN
    /\ operationStarted \in BOOLEAN
    /\ activated \in BOOLEAN
    /\ everReleased \in BOOLEAN
    /\ lastCopyAttempt \in
        [expected : DigestOrNone, observed : DigestOrNone, accepted : BOOLEAN]
    /\ lastFenceAttempt \in
        [expected : DigestOrNone, observed : DigestOrNone, accepted : BOOLEAN]
    /\ lastReleaseAttempt \in [wasPending : BOOLEAN, accepted : BOOLEAN]

VerifiedBytesNeverChangeDigest ==
    /\ stagedDigest = NoDigest \/ stagedDigest = reservedDigest
    /\ currentDigest = NoDigest \/ currentDigest = reservedDigest

AcceptedCopyWasIndependentlyVerified ==
    ~lastCopyAttempt.accepted \/
      lastCopyAttempt.observed = lastCopyAttempt.expected

AcceptedFenceWasExact ==
    ~lastFenceAttempt.accepted \/
      /\ lastFenceAttempt.expected = reservedDigest
      /\ lastFenceAttempt.observed = reservedDigest

StagedJournalHasExactDurableBytes ==
    journal # "STAGED" \/
      stagedDigest = reservedDigest \/
      (activationPending /\ operationStarted /\ currentDigest = reservedDigest)

InstalledJournalHasExactCurrent ==
    journal # "INSTALLED" \/
      (activated /\ currentDigest = reservedDigest /\ stagedDigest = NoDigest)

ActivationReservationIsDurable ==
    ~operationStarted \/
      (activationPending /\ journal \in {"STAGED", "INSTALLED"})

ReleaseCannotCrossPendingActivation ==
    /\ gate # "RELEASE" \/ ~activationPending
    /\ ~lastReleaseAttempt.accepted \/ ~lastReleaseAttempt.wasPending

ReleasedTokenIsTerminal ==
    ~everReleased \/ journal = "RELEASED"

Safety ==
    /\ TypeOK
    /\ VerifiedBytesNeverChangeDigest
    /\ AcceptedCopyWasIndependentlyVerified
    /\ AcceptedFenceWasExact
    /\ StagedJournalHasExactDurableBytes
    /\ InstalledJournalHasExactCurrent
    /\ ActivationReservationIsDurable
    /\ ReleaseCannotCrossPendingActivation
    /\ ReleasedTokenIsTerminal

=============================================================================
