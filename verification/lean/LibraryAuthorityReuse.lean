import Std

/-!
# Verified artifact digest reuse

This file models the narrow mathematical step used by the filesystem library:
after bytes have been independently hashed once, their digest may be reused if
the same private inode authority still denotes the same bytes.  A rename may
change `ctime`, so the filesystem implementation separately checks the stable
device/inode/size/mtime content identity and the private single-link contract.

The key premise below, `observedBytes = authority.verifiedBytes`, is deliberate.
Lean does not prove that POSIX metadata, `flock`, `fsync`, Python file objects,
SQLite rows, or the production checks establish that premise.  Fault,
concurrency, differential, and traversal tests are the refinement evidence at
that boundary.  Likewise, collision resistance is outside this theorem: the
result states equality with the digest that was actually computed and stored.
-/

namespace H2HDBIngest.Verification.LibraryAuthorityReuse

structure VerifiedAuthority
    (Bytes Digest Identity : Type)
    (hash : Bytes → Digest) where
  verifiedBytes : Bytes
  verifiedDigest : Digest
  inodeIdentity : Identity
  digestWasComputed : hash verifiedBytes = verifiedDigest

/--
An observed inode refines a verified authority only when both its identity and
its bytes remain those covered by the original independent digest traversal.
The bytes premise is what the private-inode filesystem contract must justify.
-/
structure AuthorityPreserved
    {Bytes Digest Identity : Type}
    {hash : Bytes → Digest}
    (authority : VerifiedAuthority Bytes Digest Identity hash)
    (observedBytes : Bytes)
    (observedIdentity : Identity) : Prop where
  identityExact : observedIdentity = authority.inodeIdentity
  bytesExact : observedBytes = authority.verifiedBytes

theorem reused_digest_equals_recomputed_digest
    {Bytes Digest Identity : Type}
    (hash : Bytes → Digest)
    (authority : VerifiedAuthority Bytes Digest Identity hash)
    (observedBytes : Bytes)
    (observedIdentity : Identity)
    (preserved : AuthorityPreserved authority observedBytes observedIdentity) :
    hash observedBytes = authority.verifiedDigest := by
  rw [preserved.bytesExact]
  exact authority.digestWasComputed

/-- A caller-supplied digest is accepted only after recomputing exact bytes. -/
def CopyDigestAccepted
    {Bytes Digest : Type}
    [DecidableEq Digest]
    (hash : Bytes → Digest)
    (source : Bytes)
    (expected : Digest) : Bool :=
  decide (hash source = expected)

theorem accepted_copy_digest_was_independently_computed
    {Bytes Digest : Type}
    [DecidableEq Digest]
    (hash : Bytes → Digest)
    (source : Bytes)
    (expected : Digest)
    (accepted : CopyDigestAccepted hash source expected = true) :
    hash source = expected := by
  simpa [CopyDigestAccepted] using accepted

theorem mismatched_caller_digest_is_rejected
    {Bytes Digest : Type}
    [DecidableEq Digest]
    (hash : Bytes → Digest)
    (source : Bytes)
    (expected : Digest)
    (mismatch : hash source ≠ expected) :
    CopyDigestAccepted hash source expected = false := by
  simp [CopyDigestAccepted, mismatch]

inductive JournalState where
  | writing
  | staged
  | installed
  | released
deriving DecidableEq, Repr

def ExactWritingFence
    {Digest Token : Type}
    [DecidableEq Digest]
    [DecidableEq Token]
    (durableState : JournalState)
    (durableToken suppliedToken : Token)
    (durableDigest suppliedDigest : Digest) : Bool :=
  decide (
    durableState = .writing ∧
      durableToken = suppliedToken ∧
      durableDigest = suppliedDigest)

theorem released_state_rejects_stale_writing_terminalization
    {Digest Token : Type}
    [DecidableEq Digest]
    [DecidableEq Token]
    (durableToken suppliedToken : Token)
    (durableDigest suppliedDigest : Digest) :
    ExactWritingFence .released durableToken suppliedToken
      durableDigest suppliedDigest = false := by
  simp [ExactWritingFence]

theorem replaced_token_rejects_stale_writing_terminalization
    {Digest Token : Type}
    [DecidableEq Digest]
    [DecidableEq Token]
    (durableToken suppliedToken : Token)
    (durableDigest suppliedDigest : Digest)
    (stale : durableToken ≠ suppliedToken) :
    ExactWritingFence .writing durableToken suppliedToken
      durableDigest suppliedDigest = false := by
  simp [ExactWritingFence, stale]

end H2HDBIngest.Verification.LibraryAuthorityReuse
