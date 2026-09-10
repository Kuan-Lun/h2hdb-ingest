--------------------------- MODULE LibraryRelocation --------------------------
EXTENDS TLC

(***************************************************************************
Finite explicit-relocation protocol, separate from publication activation.

Upgrading the journal and starting the maintenance session are one durable
transaction. The reader fence must then be durable before any physical
authority is rebound. Each resource is independently hashed and checked
against the exact still-loaded row before its references are atomically
updated. A second bounded audit covers all rebound resources. Crash discards
uncommitted observations, but retains the maintenance session and progress.
Normal runtime cannot resume until the original publication marker is restored
and the maintenance session is durably complete.

SCAN abstracts both the active-resource and retained-token passes; each modeled
resource is one journal-authorized resource group or retained token. Initial
cleanup of previous relocation receipts does not change artifact authority and
is omitted. Locks, hashing, exact name/descriptor checks, fsync, atomic SQLite
transactions and complete journal-directed inventory are premises, not proved
operating-system effects. This
model permits a failed byte/stability check between observation and commit.
Unrestricted external mutation after verification is outside the single-writer
contract. TLC checks only the finite constants supplied by Small.cfg.
***************************************************************************)

CONSTANTS Resources, NoResource

ASSUME /\ Resources # {}
       /\ NoResource \notin Resources

VARIABLES running, owner, format, phase, marker, originalMarker,
          committed, audited, observation, observationValid,
          rebound, normalAccepted

Durable == <<format, phase, marker, originalMarker, committed, audited, rebound>>
vars == <<running, owner, Durable, observation, observationValid, normalAccepted>>

Init ==
    /\ running = TRUE
    /\ owner = "NONE"
    /\ format \in {3, 4}
    /\ phase = "NONE"
    /\ originalMarker \in BOOLEAN
    /\ marker = originalMarker
    /\ committed = {}
    /\ audited = {}
    /\ observation = NoResource
    /\ observationValid = FALSE
    /\ rebound = {}
    /\ normalAccepted = FALSE

Acquire ==
    /\ running
    /\ owner = "NONE"
    /\ owner' = "RELOCATE"
    /\ normalAccepted' = FALSE
    /\ UNCHANGED <<running, Durable, observation, observationValid>>

StartSession ==
    /\ running /\ owner = "RELOCATE" /\ phase = "NONE"
    /\ format' = 4
    /\ phase' = "SCAN"
    /\ UNCHANGED <<running, owner, marker, originalMarker, committed, audited,
                    observation, observationValid, rebound, normalAccepted>>

FenceReaders ==
    /\ running /\ owner = "RELOCATE"
    /\ phase \in {"SCAN", "AUDIT"}
    /\ marker' = TRUE
    /\ UNCHANGED <<running, owner, format, phase, originalMarker, committed,
                    audited, observation, observationValid, rebound, normalAccepted>>

Observe(resource, valid) ==
    /\ running /\ owner = "RELOCATE" /\ phase = "SCAN" /\ marker
    /\ resource \in Resources \ committed
    /\ valid \in BOOLEAN
    /\ observation' = resource
    /\ observationValid' = valid
    /\ UNCHANGED <<running, owner, Durable, normalAccepted>>

InvalidateObservation ==
    /\ observation \in Resources
    /\ observationValid' = FALSE
    /\ UNCHANGED <<running, owner, Durable, observation, normalAccepted>>

CommitResource ==
    /\ running /\ owner = "RELOCATE" /\ phase = "SCAN" /\ marker
    /\ observation \in Resources \ committed
    /\ observationValid
    /\ committed' = committed \cup {observation}
    /\ rebound' = rebound \cup {observation}
    /\ observation' = NoResource
    /\ observationValid' = FALSE
    /\ UNCHANGED <<running, owner, format, phase, marker, originalMarker,
                    audited, normalAccepted>>

BeginAudit ==
    /\ running /\ owner = "RELOCATE" /\ phase = "SCAN" /\ marker
    /\ committed = Resources
    /\ phase' = "AUDIT"
    /\ UNCHANGED <<running, owner, format, marker, originalMarker, committed,
                    audited, observation, observationValid, rebound, normalAccepted>>

Audit(resource) ==
    /\ running /\ owner = "RELOCATE" /\ phase = "AUDIT" /\ marker
    /\ resource \in committed \ audited
    /\ audited' = audited \cup {resource}
    /\ UNCHANGED <<running, owner, format, phase, marker, originalMarker,
                    committed, observation, observationValid, rebound, normalAccepted>>

SealAudit ==
    /\ running /\ owner = "RELOCATE" /\ phase = "AUDIT" /\ marker
    /\ audited = Resources
    /\ phase' = "RESTORE"
    /\ UNCHANGED <<running, owner, format, marker, originalMarker, committed,
                    audited, observation, observationValid, rebound, normalAccepted>>

RestoreMarker ==
    /\ running /\ owner = "RELOCATE" /\ phase = "RESTORE"
    /\ marker' = originalMarker
    /\ UNCHANGED <<running, owner, format, phase, originalMarker, committed,
                    audited, observation, observationValid, rebound, normalAccepted>>

Complete ==
    /\ running /\ owner = "RELOCATE" /\ phase = "RESTORE"
    /\ marker = originalMarker
    /\ phase' = "COMPLETE"
    /\ owner' = "NONE"
    /\ UNCHANGED <<running, format, marker, originalMarker, committed,
                    audited, observation, observationValid, rebound, normalAccepted>>

NormalWork ==
    /\ running /\ owner = "NONE" /\ format = 4
    /\ phase \in {"NONE", "COMPLETE"}
    /\ normalAccepted' = TRUE
    /\ UNCHANGED <<running, owner, Durable, observation, observationValid>>

Crash ==
    /\ running
    /\ running' = FALSE
    /\ owner' = "NONE"
    /\ observation' = NoResource
    /\ observationValid' = FALSE
    /\ normalAccepted' = FALSE
    /\ UNCHANGED Durable

Restart ==
    /\ ~running
    /\ running' = TRUE
    /\ UNCHANGED <<owner, Durable, observation, observationValid, normalAccepted>>

Next == Acquire \/ StartSession \/ FenceReaders \/
        (\E resource \in Resources, valid \in BOOLEAN : Observe(resource, valid)) \/
        InvalidateObservation \/ CommitResource \/ BeginAudit \/
        (\E resource \in Resources : Audit(resource)) \/ SealAudit \/
        RestoreMarker \/ Complete \/ NormalWork \/ Crash \/ Restart

TypeOK ==
    /\ running \in BOOLEAN
    /\ owner \in {"NONE", "RELOCATE"}
    /\ format \in {3, 4}
    /\ phase \in {"NONE", "SCAN", "AUDIT", "RESTORE", "COMPLETE"}
    /\ marker \in BOOLEAN /\ originalMarker \in BOOLEAN
    /\ committed \subseteq Resources /\ audited \subseteq Resources
    /\ rebound \subseteq Resources
    /\ observation \in Resources \cup {NoResource}
    /\ observationValid \in BOOLEAN /\ normalAccepted \in BOOLEAN

ActiveMaintenanceUsesNewFormat == phase # "NONE" => format = 4
NormalWorkRequiresFinishedMaintenance ==
    normalAccepted => phase \in {"NONE", "COMPLETE"}
EveryRebindingWasCommitted == rebound = committed
AuditNeverPrecedesRebinding == audited \subseteq committed
CompleteRequiresWholeAudit == phase = "COMPLETE" => audited = Resources
PublishedFenceRestored == phase = "COMPLETE" => marker = originalMarker
RebindingRequiresReaderFence ==
    phase \in {"SCAN", "AUDIT"} /\ committed # {} => marker
CrashDropsUncommittedEvidence == ~running => observation = NoResource

Spec == Init /\ [][Next]_vars
=============================================================================
