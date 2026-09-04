----------------------- MODULE PytestProcessSupervision -----------------------
EXTENDS Integers

(***************************************************************************)
(* Finite safety model for the bounded pytest process owner. The start gate *)
(* prevents target creation before ownership. `treeEmptyProven` distinguishes *)
(* a synchronous empty-tree query from the Windows kill-on-last-handle OS    *)
(* contract used when cleanup itself cannot provide that query receipt.      *)
(***************************************************************************)

CONSTANT MaxTick

VARIABLES phase,
          owned,
          gateOpen,
          active,
          cause,
          terminationFailed,
          treeEmptyProven,
          exitCode,
          tick,
          secondPhaseStarted

vars == <<phase, owned, gateOpen, active, cause, terminationFailed,
          treeEmptyProven, exitCode, tick, secondPhaseStarted>>

Phases == {"unstarted", "gated", "owned", "running", "cleanup",
           "empty", "returned"}
Causes == {"none", "clean", "failure", "survivor", "timeout", "interrupt"}
ExitCodes == {-1, 0, 7, 124, 125, 130}

Init ==
    /\ phase = "unstarted"
    /\ owned = FALSE
    /\ gateOpen = FALSE
    /\ active = 0
    /\ cause = "none"
    /\ terminationFailed = FALSE
    /\ treeEmptyProven = FALSE
    /\ exitCode = -1
    /\ tick = 0
    /\ secondPhaseStarted = FALSE

CreateGatedSupervisor ==
    /\ phase = "unstarted"
    /\ tick < MaxTick
    /\ phase' = "gated"
    /\ active' = 1
    /\ UNCHANGED <<owned, gateOpen, cause, terminationFailed,
                    treeEmptyProven, exitCode, tick, secondPhaseStarted>>

AssignOwner ==
    /\ phase = "gated"
    /\ phase' = "owned"
    /\ owned' = TRUE
    /\ UNCHANGED <<gateOpen, active, cause, terminationFailed,
                    treeEmptyProven, exitCode, tick, secondPhaseStarted>>

OpenStartGate ==
    /\ phase = "owned"
    /\ tick < MaxTick
    /\ phase' = "running"
    /\ gateOpen' = TRUE
    /\ active' = 2
    /\ UNCHANGED <<owned, cause, terminationFailed, treeEmptyProven,
                    exitCode, tick, secondPhaseStarted>>

CleanExit ==
    /\ phase = "running"
    /\ phase' = "empty"
    /\ active' = 0
    /\ cause' = "clean"
    /\ treeEmptyProven' = TRUE
    /\ exitCode' = 0
    /\ UNCHANGED <<owned, gateOpen, terminationFailed, tick,
                    secondPhaseStarted>>

FailureExit ==
    /\ phase = "running"
    /\ phase' = "empty"
    /\ active' = 0
    /\ cause' = "failure"
    /\ treeEmptyProven' = TRUE
    /\ exitCode' = 7
    /\ UNCHANGED <<owned, gateOpen, terminationFailed, tick,
                    secondPhaseStarted>>

LeaderExitWithSurvivor ==
    /\ phase = "running"
    /\ phase' = "cleanup"
    /\ active' = 1
    /\ cause' = "survivor"
    /\ UNCHANGED <<owned, gateOpen, terminationFailed, treeEmptyProven,
                    exitCode, tick, secondPhaseStarted>>

Timeout ==
    /\ phase = "running"
    /\ phase' = "cleanup"
    /\ cause' = "timeout"
    /\ UNCHANGED <<owned, gateOpen, active, terminationFailed,
                    treeEmptyProven, exitCode, tick, secondPhaseStarted>>

Interrupt ==
    /\ phase = "running"
    /\ phase' = "cleanup"
    /\ cause' = "interrupt"
    /\ UNCHANGED <<owned, gateOpen, active, terminationFailed,
                    treeEmptyProven, exitCode, tick, secondPhaseStarted>>

ExactOutcome ==
    CASE cause = "timeout" -> 124
      [] cause = "interrupt" -> 130
      [] OTHER -> 125

TerminationSucceeds ==
    /\ phase = "cleanup"
    /\ tick <= MaxTick
    /\ phase' = "empty"
    /\ active' = 0
    /\ treeEmptyProven' = TRUE
    /\ exitCode' = ExactOutcome
    /\ UNCHANGED <<owned, gateOpen, cause, terminationFailed, tick,
                    secondPhaseStarted>>

TerminationFails ==
    /\ phase = "cleanup"
    /\ tick < MaxTick
    /\ terminationFailed' = TRUE
    /\ UNCHANGED <<phase, owned, gateOpen, active, cause, treeEmptyProven,
                    exitCode, tick, secondPhaseStarted>>

TaskkillSucceeds ==
    /\ phase = "cleanup"
    /\ terminationFailed
    /\ tick <= MaxTick
    /\ phase' = "empty"
    /\ active' = 0
    /\ treeEmptyProven' = TRUE
    /\ exitCode' = ExactOutcome
    /\ UNCHANGED <<owned, gateOpen, cause, terminationFailed, tick,
                    secondPhaseStarted>>

(***************************************************************************)
(* These two transitions abstract successful kill-on-last-Job-handle at the *)
(* runner process boundary. They deliberately do not create an empty-query  *)
(* receipt; therefore only infrastructure-failure exit 125 is permitted.    *)
(***************************************************************************)
TaskkillFailsAndJobCloses ==
    /\ phase = "cleanup"
    /\ terminationFailed
    /\ tick <= MaxTick
    /\ phase' = "empty"
    /\ active' = 0
    /\ treeEmptyProven' = FALSE
    /\ exitCode' = 125
    /\ UNCHANGED <<owned, gateOpen, cause, terminationFailed, tick,
                    secondPhaseStarted>>

CleanupDeadlineExpires ==
    /\ phase = "cleanup"
    /\ tick = MaxTick
    /\ phase' = "empty"
    /\ active' = 0
    /\ treeEmptyProven' = FALSE
    /\ exitCode' = 125
    /\ UNCHANGED <<owned, gateOpen, cause, terminationFailed, tick,
                    secondPhaseStarted>>

EstablishmentDeadlineExpires ==
    /\ phase \in {"gated", "owned"}
    /\ tick = MaxTick
    /\ phase' = "empty"
    /\ active' = 0
    /\ cause' = "failure"
    /\ treeEmptyProven' = FALSE
    /\ exitCode' = 125
    /\ UNCHANGED <<owned, gateOpen, terminationFailed, tick,
                    secondPhaseStarted>>

PublishResult ==
    /\ phase = "empty"
    /\ phase' = "returned"
    /\ UNCHANGED <<owned, gateOpen, active, cause, terminationFailed,
                    treeEmptyProven, exitCode, tick, secondPhaseStarted>>

StartSecondPhase ==
    /\ phase = "returned"
    /\ active = 0
    /\ secondPhaseStarted' = TRUE
    /\ UNCHANGED <<phase, owned, gateOpen, active, cause, terminationFailed,
                    treeEmptyProven, exitCode, tick>>

AdvanceTime ==
    /\ phase \in {"gated", "owned", "running", "cleanup"}
    /\ tick < MaxTick
    /\ tick' = tick + 1
    /\ UNCHANGED <<phase, owned, gateOpen, active, cause, terminationFailed,
                    treeEmptyProven, exitCode, secondPhaseStarted>>

Next ==
    \/ CreateGatedSupervisor
    \/ AssignOwner
    \/ OpenStartGate
    \/ CleanExit
    \/ FailureExit
    \/ LeaderExitWithSurvivor
    \/ Timeout
    \/ Interrupt
    \/ TerminationSucceeds
    \/ TerminationFails
    \/ TaskkillSucceeds
    \/ TaskkillFailsAndJobCloses
    \/ CleanupDeadlineExpires
    \/ EstablishmentDeadlineExpires
    \/ PublishResult
    \/ StartSecondPhase
    \/ AdvanceTime

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ phase \in Phases
    /\ owned \in BOOLEAN
    /\ gateOpen \in BOOLEAN
    /\ active \in 0..2
    /\ cause \in Causes
    /\ terminationFailed \in BOOLEAN
    /\ treeEmptyProven \in BOOLEAN
    /\ exitCode \in ExitCodes
    /\ tick \in 0..MaxTick
    /\ secondPhaseStarted \in BOOLEAN

GateRequiresOwnership == gateOpen => owned
TargetRequiresOwnership == active = 2 => owned /\ gateOpen
ReturnedTreeIsEmptyByProofOrJobClose == phase = "returned" => active = 0
SemanticReceiptRequiresEmptyProof ==
    phase = "returned" /\ exitCode \in {0, 7, 124, 130} => treeEmptyProven
SuccessfulReturnIsClean ==
    phase = "returned" /\ exitCode = 0 => cause = "clean" /\ active = 0
SurvivorCannotSucceed ==
    phase = "returned" /\ cause = "survivor" => exitCode = 125
TimeoutReceiptIsExact == exitCode = 124 => cause = "timeout"
InterruptReceiptIsExact == exitCode = 130 => cause = "interrupt"
SecondPhaseRequiresEmptyReceipt ==
    secondPhaseStarted => phase = "returned" /\ active = 0
DeadlineNeverExtends == tick <= MaxTick

=============================================================================
