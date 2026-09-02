---------------------------- MODULE GalleryIndexReuse -------------------------
EXTENDS FiniteSets, Naturals, TLC

(***************************************************************************
Finite state model for a single active-gallery immutable index.

Build captures one source version and records a fixed gallery audit.  Switching
gallery replaces the active payload.  A return is possible only after the
modeled boundary audit finds the active version equal to the current version;
a changed active source is rejected.  Rebuilding a previously audited gallery
after mutation is also rejected.

TLC exhaustively checks only the configured finite galleries and two source
versions.  The equality below is an abstract exact audit.  This model does not
prove SHA-256 collision resistance, Python or SQLite refinement, bounded-batch
implementation, filesystem/stat behavior, parsing, or CBZ byte equivalence.
***************************************************************************)

CONSTANT Galleries

None == "NONE"

VARIABLES
    sourceVersion,
    fixedKnown,
    fixedVersion,
    activeGallery,
    activeVersion,
    outcome,
    lastEvent,
    lastGallery

vars ==
    <<sourceVersion, fixedKnown, fixedVersion, activeGallery, activeVersion,
      outcome, lastEvent, lastGallery>>

Init ==
    /\ sourceVersion = [gallery \in Galleries |-> 0]
    /\ fixedKnown = {}
    /\ fixedVersion = [gallery \in Galleries |-> 0]
    /\ activeGallery = None
    /\ activeVersion = 0
    /\ outcome = "NONE"
    /\ lastEvent = "INIT"
    /\ lastGallery = None

Build(gallery) ==
    /\ gallery \in Galleries
    /\ activeGallery # gallery
    /\ IF gallery \in fixedKnown /\
           sourceVersion[gallery] # fixedVersion[gallery]
       THEN /\ activeGallery' = None
            /\ activeVersion' = 0
            /\ fixedKnown' = fixedKnown
            /\ fixedVersion' = fixedVersion
            /\ outcome' = "REJECT"
            /\ lastEvent' = "BUILD_REJECT"
       ELSE /\ activeGallery' = gallery
            /\ activeVersion' = sourceVersion[gallery]
            /\ fixedKnown' = fixedKnown \cup {gallery}
            /\ fixedVersion' =
                [fixedVersion EXCEPT ![gallery] = sourceVersion[gallery]]
            /\ outcome' = "NONE"
            /\ lastEvent' = "BUILD"
    /\ lastGallery' = gallery
    /\ UNCHANGED sourceVersion

ReadPage(gallery) ==
    /\ gallery \in Galleries
    /\ activeGallery = gallery
    /\ outcome' =
        IF sourceVersion[gallery] = activeVersion
        THEN "RETURN"
        ELSE "REJECT"
    /\ lastEvent' = "PAGE"
    /\ lastGallery' = gallery
    /\ UNCHANGED <<sourceVersion, fixedKnown, fixedVersion, activeGallery,
                    activeVersion>>

Mutate(gallery) ==
    /\ gallery \in Galleries
    /\ sourceVersion[gallery] = 0
    /\ sourceVersion' = [sourceVersion EXCEPT ![gallery] = 1]
    /\ outcome' = "NONE"
    /\ lastEvent' = "MUTATE"
    /\ lastGallery' = gallery
    /\ UNCHANGED <<fixedKnown, fixedVersion, activeGallery, activeVersion>>

TerminalStutter ==
    /\ UNCHANGED vars

Next ==
    \/ \E gallery \in Galleries : Build(gallery)
    \/ \E gallery \in Galleries : ReadPage(gallery)
    \/ \E gallery \in Galleries : Mutate(gallery)
    \/ TerminalStutter

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ sourceVersion \in [Galleries -> 0..1]
    /\ fixedKnown \subseteq Galleries
    /\ fixedVersion \in [Galleries -> 0..1]
    /\ activeGallery \in Galleries \cup {None}
    /\ activeVersion \in 0..1
    /\ outcome \in {"NONE", "RETURN", "REJECT"}
    /\ lastEvent \in {"INIT", "BUILD", "BUILD_REJECT", "PAGE", "MUTATE"}
    /\ lastGallery \in Galleries \cup {None}

ActiveCacheContainsAtMostOneGallery ==
    activeGallery \in Galleries \cup {None}

ReturnedPagePassedCurrentBoundaryAudit ==
    lastEvent = "PAGE" /\ outcome = "RETURN"
        => /\ activeGallery = lastGallery
           /\ activeGallery # None
           /\ activeVersion = sourceVersion[lastGallery]

ChangedActiveGalleryFailsClosed ==
    lastEvent = "PAGE" /\ activeGallery = lastGallery /\
      activeVersion # sourceVersion[lastGallery]
        => outcome = "REJECT"

ChangedFixedAuditCannotBeReactivated ==
    lastEvent = "BUILD_REJECT"
        => /\ lastGallery \in fixedKnown
           /\ sourceVersion[lastGallery] # fixedVersion[lastGallery]
           /\ activeGallery = None
           /\ outcome = "REJECT"

Safety ==
    /\ TypeOK
    /\ ActiveCacheContainsAtMostOneGallery
    /\ ReturnedPagePassedCurrentBoundaryAudit
    /\ ChangedActiveGalleryFailsClosed
    /\ ChangedFixedAuditCannotBeReactivated

=============================================================================
