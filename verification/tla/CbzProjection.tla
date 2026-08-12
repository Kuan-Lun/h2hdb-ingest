---------------------------- MODULE CbzProjection ----------------------------
EXTENDS FiniteSets, TLC

(***************************************************************************
Filesystem-facing refinement of a successfully published core revision.

Core publication is abstracted as CorePublish.  It atomically advances the
core revision and creates a durable publication receipt.  h2hdb-ingest then
writes a complete local pending-projection journal before it changes any
friendly CBZ path.  A crash preserves the artifact store, both journal layers,
and any partially materialized friendly projection.  Restart can therefore
repeat materialization and finalization idempotently.

This module intentionally owns filesystem notions that do not belong in the
CatalogCore model: managed paths, unknown paths, immutable CBZ artifacts, and
deletion of stale friendly files.
***************************************************************************)

CONSTANTS
    Artifacts,
    Paths,
    Revisions,
    NoArtifact,
    NoPath,
    NoRevision

ASSUME /\ Artifacts # {}
       /\ Paths # {}
       /\ Revisions # {}
       /\ NoArtifact \notin Artifacts
       /\ NoPath \notin Paths
       /\ NoRevision \notin Revisions

ProjectionSet == [Paths -> Artifacts \cup {NoArtifact}]

EmptyProjection == [path \in Paths |-> NoArtifact]

ProjectionArtifacts(projection) ==
    {projection[path] :
        path \in {candidate \in Paths : projection[candidate] # NoArtifact}}

ProjectionPaths(projection) ==
    {path \in Paths : projection[path] # NoArtifact}

VARIABLES
    running,

    artifactPresent,
    protectedArtifacts,
    publishedArtifacts,

    usedRevisions,
    revisionProjection,
    coreActiveRevision,
    receiptRevision,

    pendingRevision,
    pendingProjection,
    currentRevision,
    friendlyProjection,
    managedPaths,
    unknownPaths,

    lastDeleteAttempt,
    lastGcAttempt

ArtifactVariables ==
    <<artifactPresent, protectedArtifacts, publishedArtifacts>>

CoreVariables ==
    <<usedRevisions, revisionProjection, coreActiveRevision, receiptRevision>>

ProjectionVariables ==
    <<pendingRevision, pendingProjection, currentRevision,
      friendlyProjection, managedPaths, unknownPaths>>

AuditVariables == <<lastDeleteAttempt, lastGcAttempt>>

vars == <<running, ArtifactVariables, CoreVariables,
          ProjectionVariables, AuditVariables>>

ActiveProjection ==
    IF coreActiveRevision = NoRevision
    THEN EmptyProjection
    ELSE revisionProjection[coreActiveRevision]

ActiveArtifacts == ProjectionArtifacts(ActiveProjection)

PendingArtifacts ==
    IF pendingRevision = NoRevision
    THEN {}
    ELSE ProjectionArtifacts(pendingProjection)

FriendlyArtifacts == ProjectionArtifacts(friendlyProjection)

RecoveryJournalExists ==
    /\ coreActiveRevision # NoRevision
    /\ \/ receiptRevision = coreActiveRevision
       \/ pendingRevision = coreActiveRevision

Init ==
    /\ running = TRUE

    /\ artifactPresent = {}
    /\ protectedArtifacts = {}
    /\ publishedArtifacts = {}

    /\ usedRevisions = {}
    /\ revisionProjection =
        [revision \in Revisions |-> EmptyProjection]
    /\ coreActiveRevision = NoRevision
    /\ receiptRevision = NoRevision

    /\ pendingRevision = NoRevision
    /\ pendingProjection = EmptyProjection
    /\ currentRevision = NoRevision
    /\ friendlyProjection = EmptyProjection
    /\ managedPaths = {}
    /\ unknownPaths = {}

    /\ lastDeleteAttempt =
        [ path       |-> NoPath,
          wasManaged |-> FALSE,
          accepted   |-> FALSE ]
    /\ lastGcAttempt =
        [ artifact     |-> NoArtifact,
          wasActive    |-> FALSE,
          wasProtected |-> FALSE,
          accepted     |-> FALSE ]

(***************************************************************************
Crash and restart affect only process availability.  All other variables are
durable artifact-store or core-database state.
***************************************************************************)

Crash ==
    /\ running
    /\ running' = FALSE
    /\ UNCHANGED ArtifactVariables
    /\ UNCHANGED CoreVariables
    /\ UNCHANGED ProjectionVariables
    /\ UNCHANGED AuditVariables

Restart ==
    /\ ~running
    /\ running' = TRUE
    /\ UNCHANGED ArtifactVariables
    /\ UNCHANGED CoreVariables
    /\ UNCHANGED ProjectionVariables
    /\ UNCHANGED AuditVariables

(***************************************************************************
Immutable artifact-store preparation.  A reused published artifact does not
need a second temporary protection; a new artifact must be protected before
the abstract core publication can select it.
***************************************************************************)

CreateArtifact(artifact) ==
    /\ running
    /\ artifact \in Artifacts \ artifactPresent
    /\ artifactPresent' = artifactPresent \cup {artifact}
    /\ UNCHANGED <<running, protectedArtifacts, publishedArtifacts>>
    /\ UNCHANGED CoreVariables
    /\ UNCHANGED ProjectionVariables
    /\ UNCHANGED AuditVariables

ProtectArtifact(artifact) ==
    /\ running
    /\ artifact \in artifactPresent
    /\ artifact \notin protectedArtifacts
    /\ protectedArtifacts' = protectedArtifacts \cup {artifact}
    /\ UNCHANGED <<running, artifactPresent, publishedArtifacts>>
    /\ UNCHANGED CoreVariables
    /\ UNCHANGED ProjectionVariables
    /\ UNCHANGED AuditVariables

(***************************************************************************
CorePublish represents the joint source/catalog transaction in h2hdb core.
The DB head and durable publication receipt change in one external step.  It
cannot start another publication until the previous receipt is finalized.
***************************************************************************)

CorePublish(revision, projection) ==
    LET selected == ProjectionArtifacts(projection)
    IN
    /\ running
    /\ revision \in Revisions \ usedRevisions
    /\ projection \in ProjectionSet
    /\ receiptRevision = NoRevision
    /\ pendingRevision = NoRevision
    /\ selected \subseteq artifactPresent
    /\ selected \subseteq protectedArtifacts \cup publishedArtifacts
    /\ usedRevisions' = usedRevisions \cup {revision}
    /\ revisionProjection' =
        [revisionProjection EXCEPT ![revision] = projection]
    /\ coreActiveRevision' = revision
    /\ receiptRevision' = revision
    /\ UNCHANGED <<running, ArtifactVariables>>
    /\ UNCHANGED ProjectionVariables
    /\ UNCHANGED AuditVariables

(***************************************************************************
WritePendingJournal is a complete, atomic intent write.  Only after this step
may individual friendly paths be copied/replaced or stale managed paths be
removed.  Unknown paths are never replaced or deleted.
***************************************************************************)

WritePendingJournal ==
    /\ running
    /\ receiptRevision # NoRevision
    /\ receiptRevision = coreActiveRevision
    /\ pendingRevision = NoRevision
    /\ pendingRevision' = receiptRevision
    /\ pendingProjection' = revisionProjection[receiptRevision]
    /\ UNCHANGED <<running, ArtifactVariables>>
    /\ UNCHANGED CoreVariables
    /\ UNCHANGED
        <<currentRevision, friendlyProjection, managedPaths, unknownPaths>>
    /\ UNCHANGED AuditVariables

MaterializePlannedPath(path) ==
    LET artifact == pendingProjection[path]
    IN
    /\ running
    /\ pendingRevision # NoRevision
    /\ path \in Paths
    /\ artifact # NoArtifact
    /\ artifact \in artifactPresent
    /\ path \notin unknownPaths
    /\ friendlyProjection[path] # artifact
    /\ friendlyProjection' =
        [friendlyProjection EXCEPT ![path] = artifact]
    /\ managedPaths' = managedPaths \cup {path}
    /\ UNCHANGED <<running, ArtifactVariables>>
    /\ UNCHANGED CoreVariables
    /\ UNCHANGED
        <<pendingRevision, pendingProjection, currentRevision, unknownPaths>>
    /\ UNCHANGED AuditVariables

DeleteStaleManagedPath(path) ==
    /\ running
    /\ pendingRevision # NoRevision
    /\ path \in managedPaths
    /\ pendingProjection[path] = NoArtifact
    /\ friendlyProjection[path] # NoArtifact
    /\ friendlyProjection' =
        [friendlyProjection EXCEPT ![path] = NoArtifact]
    /\ managedPaths' = managedPaths \ {path}
    /\ lastDeleteAttempt' =
        [ path       |-> path,
          wasManaged |-> TRUE,
          accepted   |-> TRUE ]
    /\ UNCHANGED <<running, ArtifactVariables>>
    /\ UNCHANGED CoreVariables
    /\ UNCHANGED
        <<pendingRevision, pendingProjection, currentRevision, unknownPaths>>
    /\ UNCHANGED lastGcAttempt

RejectUnknownPathDeletion(path) ==
    /\ running
    /\ path \in unknownPaths
    /\ lastDeleteAttempt' =
        [ path       |-> path,
          wasManaged |-> FALSE,
          accepted   |-> FALSE ]
    /\ UNCHANGED <<running, ArtifactVariables>>
    /\ UNCHANGED CoreVariables
    /\ UNCHANGED ProjectionVariables
    /\ UNCHANGED lastGcAttempt

(***************************************************************************
FinalizeProjection is the artifact-store transaction that records the new
friendly projection revision, publishes the selected immutable artifacts,
clears temporary protection, and acknowledges the core receipt.  It is enabled
only after every planned copy and managed deletion has reached its target.
***************************************************************************)

FinalizeProjection ==
    LET selected == ProjectionArtifacts(pendingProjection)
    IN
    /\ running
    /\ pendingRevision # NoRevision
    /\ pendingRevision = coreActiveRevision
    /\ receiptRevision = pendingRevision
    /\ friendlyProjection = pendingProjection
    /\ currentRevision' = pendingRevision
    /\ pendingRevision' = NoRevision
    /\ pendingProjection' = EmptyProjection
    /\ receiptRevision' = NoRevision
    /\ publishedArtifacts' = publishedArtifacts \cup selected
    /\ protectedArtifacts' = protectedArtifacts \ selected
    /\ UNCHANGED <<running, artifactPresent>>
    /\ UNCHANGED
        <<usedRevisions, revisionProjection, coreActiveRevision>>
    /\ UNCHANGED <<friendlyProjection, managedPaths, unknownPaths>>
    /\ UNCHANGED AuditVariables

(***************************************************************************
External unknown paths model files that are not recorded as managed by the
artifact store.  Reconciliation may neither replace nor delete them.
***************************************************************************)

CreateUnknownPath(path) ==
    /\ path \in Paths \ (managedPaths \cup unknownPaths)
    /\ friendlyProjection[path] = NoArtifact
    /\ unknownPaths' = unknownPaths \cup {path}
    /\ UNCHANGED <<running, ArtifactVariables>>
    /\ UNCHANGED CoreVariables
    /\ UNCHANGED
        <<pendingRevision, pendingProjection, currentRevision,
          friendlyProjection, managedPaths>>
    /\ UNCHANGED AuditVariables

RemoveUnknownPath(path) ==
    /\ path \in unknownPaths
    /\ unknownPaths' = unknownPaths \ {path}
    /\ UNCHANGED <<running, ArtifactVariables>>
    /\ UNCHANGED CoreVariables
    /\ UNCHANGED
        <<pendingRevision, pendingProjection, currentRevision,
          friendlyProjection, managedPaths>>
    /\ UNCHANGED AuditVariables

(***************************************************************************
Artifact GC may delete only an unreferenced staging artifact.  Published,
protected, active, pending, and currently materialized artifacts all survive.
***************************************************************************)

AttemptArtifactGc(artifact) ==
    LET wasActive == artifact \in ActiveArtifacts
        wasProtected == artifact \in protectedArtifacts
        accepted ==
            /\ artifact \in artifactPresent
            /\ artifact \notin publishedArtifacts
            /\ ~wasActive
            /\ ~wasProtected
            /\ artifact \notin PendingArtifacts
            /\ artifact \notin FriendlyArtifacts
    IN
    /\ artifact \in Artifacts
    /\ lastGcAttempt' =
        [ artifact     |-> artifact,
          wasActive    |-> wasActive,
          wasProtected |-> wasProtected,
          accepted     |-> accepted ]
    /\ artifactPresent' =
        IF accepted THEN artifactPresent \ {artifact} ELSE artifactPresent
    /\ UNCHANGED <<running, protectedArtifacts, publishedArtifacts>>
    /\ UNCHANGED CoreVariables
    /\ UNCHANGED ProjectionVariables
    /\ UNCHANGED lastDeleteAttempt

Next ==
    \/ Crash
    \/ Restart
    \/ \E artifact \in Artifacts : CreateArtifact(artifact)
    \/ \E artifact \in Artifacts : ProtectArtifact(artifact)
    \/ \E revision \in Revisions,
          projection \in ProjectionSet :
        CorePublish(revision, projection)
    \/ WritePendingJournal
    \/ \E path \in Paths : MaterializePlannedPath(path)
    \/ \E path \in Paths : DeleteStaleManagedPath(path)
    \/ \E path \in Paths : RejectUnknownPathDeletion(path)
    \/ FinalizeProjection
    \/ \E path \in Paths : CreateUnknownPath(path)
    \/ \E path \in Paths : RemoveUnknownPath(path)
    \/ \E artifact \in Artifacts : AttemptArtifactGc(artifact)

Spec == Init /\ [][Next]_vars

(***************************************************************************
Safety properties checked by CbzProjectionSmall.cfg.
***************************************************************************)

TypeOK ==
    /\ running \in BOOLEAN
    /\ artifactPresent \subseteq Artifacts
    /\ protectedArtifacts \subseteq Artifacts
    /\ publishedArtifacts \subseteq Artifacts
    /\ usedRevisions \subseteq Revisions
    /\ revisionProjection \in [Revisions -> ProjectionSet]
    /\ coreActiveRevision \in Revisions \cup {NoRevision}
    /\ receiptRevision \in Revisions \cup {NoRevision}
    /\ pendingRevision \in Revisions \cup {NoRevision}
    /\ pendingProjection \in ProjectionSet
    /\ currentRevision \in Revisions \cup {NoRevision}
    /\ friendlyProjection \in ProjectionSet
    /\ managedPaths \subseteq Paths
    /\ unknownPaths \subseteq Paths
    /\ lastDeleteAttempt \in
        [ path       : Paths \cup {NoPath},
          wasManaged : BOOLEAN,
          accepted   : BOOLEAN ]
    /\ lastGcAttempt \in
        [ artifact     : Artifacts \cup {NoArtifact},
          wasActive    : BOOLEAN,
          wasProtected : BOOLEAN,
          accepted     : BOOLEAN ]

ProjectionIsActiveOrHasPendingJournal ==
    /\ \/ currentRevision = coreActiveRevision
       \/ RecoveryJournalExists
    /\ \/ friendlyProjection = ActiveProjection
       \/ RecoveryJournalExists

PendingJournalIsCompleteAndCurrent ==
    /\ IF receiptRevision = NoRevision
       THEN TRUE
       ELSE
           /\ receiptRevision = coreActiveRevision
           /\ receiptRevision \in usedRevisions
    /\ IF pendingRevision = NoRevision
       THEN pendingProjection = EmptyProjection
       ELSE
           /\ pendingRevision = coreActiveRevision
           /\ receiptRevision = pendingRevision
           /\ pendingProjection = revisionProjection[pendingRevision]

StableProjectionMatchesActiveRevision ==
    IF /\ receiptRevision = NoRevision
       /\ pendingRevision = NoRevision
    THEN
        /\ currentRevision = coreActiveRevision
        /\ friendlyProjection = ActiveProjection
    ELSE TRUE

ActiveOrProtectedArtifactCannotBeGc ==
    /\ ActiveArtifacts \subseteq artifactPresent
    /\ protectedArtifacts \subseteq artifactPresent
    /\ ~lastGcAttempt.accepted
       \/ /\ ~lastGcAttempt.wasActive
          /\ ~lastGcAttempt.wasProtected

PublishedAndProjectedArtifactsRemainPresent ==
    /\ publishedArtifacts \subseteq artifactPresent
    /\ FriendlyArtifacts \subseteq artifactPresent
    /\ PendingArtifacts \subseteq artifactPresent

OnlyManagedFriendlyPathsMayBeDeleted ==
    /\ managedPaths = ProjectionPaths(friendlyProjection)
    /\ managedPaths \cap unknownPaths = {}
    /\ ~lastDeleteAttempt.accepted
       \/ lastDeleteAttempt.wasManaged

CoreReceiptKeepsActiveArtifactsRecoverable ==
    IF receiptRevision = NoRevision
    THEN TRUE
    ELSE
        /\ receiptRevision = coreActiveRevision
        /\ ActiveArtifacts \subseteq
            protectedArtifacts \cup publishedArtifacts

Safety ==
    /\ TypeOK
    /\ ProjectionIsActiveOrHasPendingJournal
    /\ PendingJournalIsCompleteAndCurrent
    /\ StableProjectionMatchesActiveRevision
    /\ ActiveOrProtectedArtifactCannotBeGc
    /\ PublishedAndProjectedArtifactsRemainPresent
    /\ OnlyManagedFriendlyPathsMayBeDeleted
    /\ CoreReceiptKeepsActiveArtifactsRecoverable

=============================================================================
