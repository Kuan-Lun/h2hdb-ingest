------------------------ MODULE CbzLibraryActivation -------------------------
EXTENDS FiniteSets, TLC

(***************************************************************************
Finite crash model for single-copy acquisition and thumbnail activation.

PrepareInvisible seals a core publication without moving the reader head.
StartActivation creates the durable maintenance marker.  Individual current
paths are then replaced or removed in bounded replayable actions.  Only a
complete library can become READY; FinalizeReaderHead performs the core CAS,
and CompleteActivation removes the marker last.  Crash changes only process
availability, so every intermediate state is restartable.
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

LibrarySet == [Paths -> Artifacts \cup {NoArtifact}]
EmptyLibrary == [path \in Paths |-> NoArtifact]

ArtifactsOf(library) ==
    {library[path] : path \in {p \in Paths : library[p] # NoArtifact}}

PathsOf(library) == {path \in Paths : library[path] # NoArtifact}

VARIABLES
    running,
    stagedArtifacts,

    usedRevisions,
    revisionLibrary,
    readerHead,

    pendingRevision,
    pendingLibrary,
    activationPhase,
    activatingMarker,

    currentRevision,
    currentLibrary,
    managedPaths,
    unknownPaths,

    lastDeleteAttempt,
    lastStageGcAttempt

CoreVariables == <<usedRevisions, revisionLibrary, readerHead>>
ActivationVariables ==
    <<pendingRevision, pendingLibrary, activationPhase, activatingMarker>>
LibraryVariables ==
    <<currentRevision, currentLibrary, managedPaths, unknownPaths>>
AuditVariables == <<lastDeleteAttempt, lastStageGcAttempt>>
vars ==
    <<running, stagedArtifacts, CoreVariables, ActivationVariables,
      LibraryVariables, AuditVariables>>

ReaderLibrary ==
    IF readerHead = NoRevision
    THEN EmptyLibrary
    ELSE revisionLibrary[readerHead]

PendingArtifacts ==
    IF pendingRevision = NoRevision
    THEN {}
    ELSE ArtifactsOf(pendingLibrary)

Init ==
    /\ running = TRUE
    /\ stagedArtifacts = {}
    /\ usedRevisions = {}
    /\ revisionLibrary = [revision \in Revisions |-> EmptyLibrary]
    /\ readerHead = NoRevision
    /\ pendingRevision = NoRevision
    /\ pendingLibrary = EmptyLibrary
    /\ activationPhase = "IDLE"
    /\ activatingMarker = FALSE
    /\ currentRevision = NoRevision
    /\ currentLibrary = EmptyLibrary
    /\ managedPaths = {}
    /\ unknownPaths = {}
    /\ lastDeleteAttempt =
        [path |-> NoPath, wasManaged |-> FALSE, accepted |-> FALSE]
    /\ lastStageGcAttempt =
        [artifact |-> NoArtifact, wasPending |-> FALSE, accepted |-> FALSE]

Crash ==
    /\ running
    /\ running' = FALSE
    /\ UNCHANGED <<stagedArtifacts, CoreVariables, ActivationVariables,
                    LibraryVariables, AuditVariables>>

Restart ==
    /\ ~running
    /\ running' = TRUE
    /\ UNCHANGED <<stagedArtifacts, CoreVariables, ActivationVariables,
                    LibraryVariables, AuditVariables>>

StageArtifact(artifact) ==
    /\ running
    /\ artifact \in Artifacts \ stagedArtifacts
    /\ stagedArtifacts' = stagedArtifacts \cup {artifact}
    /\ UNCHANGED <<running, CoreVariables, ActivationVariables,
                    LibraryVariables, AuditVariables>>

PrepareInvisible(revision, library) ==
    LET selected == ArtifactsOf(library)
    IN
    /\ running
    /\ revision \in Revisions \ usedRevisions
    /\ library \in LibrarySet
    /\ pendingRevision = NoRevision
    /\ activationPhase = "IDLE"
    /\ ~activatingMarker
    /\ selected \subseteq stagedArtifacts \cup ArtifactsOf(currentLibrary)
    /\ usedRevisions' = usedRevisions \cup {revision}
    /\ revisionLibrary' = [revisionLibrary EXCEPT ![revision] = library]
    /\ pendingRevision' = revision
    /\ pendingLibrary' = library
    /\ activationPhase' = "SPOOL"
    /\ UNCHANGED <<running, stagedArtifacts, readerHead, activatingMarker,
                    LibraryVariables, AuditVariables>>

StartActivation ==
    /\ running
    /\ pendingRevision # NoRevision
    /\ activationPhase = "SPOOL"
    /\ activationPhase' = "ACTIVATING"
    /\ activatingMarker' = TRUE
    /\ UNCHANGED <<running, stagedArtifacts, CoreVariables,
                    pendingRevision, pendingLibrary,
                    LibraryVariables, AuditVariables>>

InstallOne(path) ==
    LET artifact == pendingLibrary[path]
    IN
    /\ running
    /\ activationPhase = "ACTIVATING"
    /\ path \in Paths
    /\ artifact # NoArtifact
    /\ artifact \in stagedArtifacts \cup ArtifactsOf(currentLibrary)
    /\ path \notin unknownPaths
    /\ currentLibrary[path] # artifact
    /\ currentLibrary' = [currentLibrary EXCEPT ![path] = artifact]
    /\ managedPaths' = managedPaths \cup {path}
    /\ stagedArtifacts' = stagedArtifacts \ {artifact}
    /\ UNCHANGED <<running, CoreVariables, ActivationVariables,
                    currentRevision, unknownPaths, AuditVariables>>

RemoveOneStale(path) ==
    /\ running
    /\ activationPhase = "ACTIVATING"
    /\ path \in managedPaths
    /\ pendingLibrary[path] = NoArtifact
    /\ currentLibrary[path] # NoArtifact
    /\ currentLibrary' = [currentLibrary EXCEPT ![path] = NoArtifact]
    /\ managedPaths' = managedPaths \ {path}
    /\ lastDeleteAttempt' =
        [path |-> path, wasManaged |-> TRUE, accepted |-> TRUE]
    /\ UNCHANGED <<running, stagedArtifacts, CoreVariables,
                    ActivationVariables, currentRevision, unknownPaths,
                    lastStageGcAttempt>>

RejectUnknownDeletion(path) ==
    /\ running
    /\ path \in unknownPaths
    /\ lastDeleteAttempt' =
        [path |-> path, wasManaged |-> FALSE, accepted |-> FALSE]
    /\ UNCHANGED <<running, stagedArtifacts, CoreVariables,
                    ActivationVariables, LibraryVariables,
                    lastStageGcAttempt>>

MarkReady ==
    /\ running
    /\ activationPhase = "ACTIVATING"
    /\ currentLibrary = pendingLibrary
    /\ activationPhase' = "READY"
    /\ UNCHANGED <<running, stagedArtifacts, CoreVariables,
                    pendingRevision, pendingLibrary, activatingMarker,
                    LibraryVariables, AuditVariables>>

FinalizeReaderHead ==
    /\ running
    /\ activationPhase = "READY"
    /\ activatingMarker
    /\ pendingRevision # NoRevision
    /\ currentLibrary = pendingLibrary
    /\ readerHead' = pendingRevision
    /\ activationPhase' = "FINALIZED"
    /\ UNCHANGED <<running, stagedArtifacts, usedRevisions, revisionLibrary,
                    pendingRevision, pendingLibrary, activatingMarker,
                    LibraryVariables, AuditVariables>>

CompleteActivation ==
    /\ running
    /\ activationPhase = "FINALIZED"
    /\ readerHead = pendingRevision
    /\ currentLibrary = pendingLibrary
    /\ currentRevision' = pendingRevision
    /\ pendingRevision' = NoRevision
    /\ pendingLibrary' = EmptyLibrary
    /\ activationPhase' = "IDLE"
    /\ activatingMarker' = FALSE
    /\ UNCHANGED <<running, stagedArtifacts, CoreVariables,
                    currentLibrary, managedPaths, unknownPaths,
                    AuditVariables>>

CreateUnknownPath(path) ==
    /\ path \in Paths \ (managedPaths \cup unknownPaths)
    /\ currentLibrary[path] = NoArtifact
    /\ unknownPaths' = unknownPaths \cup {path}
    /\ UNCHANGED <<running, stagedArtifacts, CoreVariables,
                    ActivationVariables, currentRevision, currentLibrary,
                    managedPaths, AuditVariables>>

RemoveUnknownPath(path) ==
    /\ path \in unknownPaths
    /\ unknownPaths' = unknownPaths \ {path}
    /\ UNCHANGED <<running, stagedArtifacts, CoreVariables,
                    ActivationVariables, currentRevision, currentLibrary,
                    managedPaths, AuditVariables>>

AttemptStageGc(artifact) ==
    LET wasPending == artifact \in PendingArtifacts
        accepted == artifact \in stagedArtifacts /\ ~wasPending
    IN
    /\ artifact \in Artifacts
    /\ stagedArtifacts' =
        IF accepted THEN stagedArtifacts \ {artifact} ELSE stagedArtifacts
    /\ lastStageGcAttempt' =
        [artifact |-> artifact, wasPending |-> wasPending, accepted |-> accepted]
    /\ UNCHANGED <<running, CoreVariables, ActivationVariables,
                    LibraryVariables, lastDeleteAttempt>>

Next ==
    \/ Crash
    \/ Restart
    \/ \E artifact \in Artifacts : StageArtifact(artifact)
    \/ \E revision \in Revisions, library \in LibrarySet :
        PrepareInvisible(revision, library)
    \/ StartActivation
    \/ \E path \in Paths : InstallOne(path)
    \/ \E path \in Paths : RemoveOneStale(path)
    \/ \E path \in Paths : RejectUnknownDeletion(path)
    \/ MarkReady
    \/ FinalizeReaderHead
    \/ CompleteActivation
    \/ \E path \in Paths : CreateUnknownPath(path)
    \/ \E path \in Paths : RemoveUnknownPath(path)
    \/ \E artifact \in Artifacts : AttemptStageGc(artifact)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ running \in BOOLEAN
    /\ stagedArtifacts \subseteq Artifacts
    /\ usedRevisions \subseteq Revisions
    /\ revisionLibrary \in [Revisions -> LibrarySet]
    /\ readerHead \in Revisions \cup {NoRevision}
    /\ pendingRevision \in Revisions \cup {NoRevision}
    /\ pendingLibrary \in LibrarySet
    /\ activationPhase \in {"IDLE", "SPOOL", "ACTIVATING", "READY", "FINALIZED"}
    /\ activatingMarker \in BOOLEAN
    /\ currentRevision \in Revisions \cup {NoRevision}
    /\ currentLibrary \in LibrarySet
    /\ managedPaths \subseteq Paths
    /\ unknownPaths \subseteq Paths
    /\ lastDeleteAttempt \in
        [path : Paths \cup {NoPath}, wasManaged : BOOLEAN, accepted : BOOLEAN]
    /\ lastStageGcAttempt \in
        [artifact : Artifacts \cup {NoArtifact},
         wasPending : BOOLEAN,
         accepted : BOOLEAN]

MarkerExactlyCoversCutover ==
    activatingMarker =
        (activationPhase \in {"ACTIVATING", "READY", "FINALIZED"})

PendingJournalIsComplete ==
    IF pendingRevision = NoRevision
    THEN /\ pendingLibrary = EmptyLibrary
         /\ activationPhase = "IDLE"
    ELSE /\ pendingRevision \in usedRevisions
         /\ pendingLibrary = revisionLibrary[pendingRevision]
         /\ activationPhase # "IDLE"

UnmarkedReadersSeeOneExactLibrary ==
    IF ~activatingMarker THEN currentLibrary = ReaderLibrary ELSE TRUE

ReaderHeadMovesOnlyAfterReady ==
    IF activationPhase = "FINALIZED"
    THEN /\ readerHead = pendingRevision
         /\ currentLibrary = pendingLibrary
         /\ activatingMarker
    ELSE TRUE

InvisiblePublicationDoesNotMoveReaderHead ==
    IF activationPhase \in {"SPOOL", "ACTIVATING", "READY"}
    THEN readerHead # pendingRevision
    ELSE TRUE

OnlyManagedPathsMayBeDeleted ==
    /\ managedPaths = PathsOf(currentLibrary)
    /\ managedPaths \cap unknownPaths = {}
    /\ ~lastDeleteAttempt.accepted \/ lastDeleteAttempt.wasManaged

PendingStagingCannotBeGc ==
    ~lastStageGcAttempt.accepted \/ ~lastStageGcAttempt.wasPending

Safety ==
    /\ TypeOK
    /\ MarkerExactlyCoversCutover
    /\ PendingJournalIsComplete
    /\ UnmarkedReadersSeeOneExactLibrary
    /\ ReaderHeadMovesOnlyAfterReady
    /\ InvisiblePublicationDoesNotMoveReaderHead
    /\ OnlyManagedPathsMayBeDeleted
    /\ PendingStagingCannotBeGc

=============================================================================
