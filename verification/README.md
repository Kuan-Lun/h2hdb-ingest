# Formal verification

This directory contains executable specifications owned by filesystem ingest.

- `lean/IncrementalEquivalence.lean` proves that exact file/spam invalidation
  produces the same gallery content, spam decisions, and CBZ create/rebuild/
  delete sets as clean full recomputation. A snapshot uses `Option`, so a
  deleted gallery is distinct from an existing empty gallery.
- `lean/OrderedPageRendering.lean` proves that, for a deterministic pure page
  renderer, ordered collection after arbitrary worker completion schedules and
  worker-bounded batches equals the sequential map. Therefore deterministic
  serialization receives the same ordered results. It also proves that worker,
  validation, and serialization failures preserve the destination when
  publication is the final step.
- `lean/IngestRuntimeLifecycle.lean` proves the abstract runtime close is
  idempotent, two linearized callers delegate close once, every modeled CLI
  exit kind reaches closed state, entered/closed runtimes reject reentry,
  closed runtimes reject work, and a partially built runtime closes its owned
  facade. Its storage section separately proves that write authority requires
  the exact pinned UUID/root-identity pair, that changing either member is
  rejected, and that a mismatch remains blocked under retry.
- `lean/GalleryIndexReuse.lean` proves that exact immutable-index capture keeps
  every keyset page and deterministic downstream result equal to legacy source
  selection, that an exact changed boundary is rejected, and that the abstract
  active-gallery cache contains at most one payload.
- `lean/LibraryAuthorityReuse.lean` proves that a previously computed digest
  equals recomputation only under an explicit preserved-bytes authority
  premise, that caller digests are accepted only after independent
  recomputation, and that released/replaced journal facts reject stale exact
  fences.
- `tla/CbzLibraryActivation.tla` model-checks crash recovery between
  reader-invisible
  core publication, bounded local library activation, and reader-head
  finalization, including maintenance-marker and unknown-path safety.
- `tla/OrderedPageRendering.tla` model-checks finite choices of worker count,
  batch size, page-completion interleavings, ordered collection, validation and
  serialization failures, and publish-last destination safety. Its required
  `Small` profile uses four pages and explores worker counts `1..16`.
- `tla/IngestRuntimeLifecycle.tla` model-checks partial construction failure,
  two arbitrarily ordered explicit close callers, normal/one-shot/exception/
  `SystemExit`/`KeyboardInterrupt` CLI exits, and fail-closed reentry/work.
- `tla/GalleryIndexReuse.tla` model-checks two finite galleries across active
  index reuse, replacement on gallery switches, mutation before a page audit,
  rejection when a changed gallery is rebuilt against its fixed audit, and
  preservation of the previous active payload after that rejected switch.
- `tla/LibraryIoReservation.tla` model-checks WRITING and activation
  reservations across unlocked I/O, crash/restart and response loss, exact
  terminalization, wrong caller digests, stale fence attempts, and the rule
  that release of an exact object cannot cross its unfinished durable
  activation entry. Its single-object `PROTECT` gate corresponds to one
  bounded runtime token-lock stripe; unrelated stripes remain concurrent and
  are outside this model.
- `tla/PytestProcessSupervision.tla` model-checks the repository test runner's
  start gate, ownership-before-start ordering, normal exit with a surviving
  descendant, timeout and interruption cleanup, termination and `taskkill`
  failure, the fixed cleanup deadline, and the rule that another phase starts
  only after an empty-tree receipt.

The incremental Lean proof assumes collision-free canonical hash identities,
deterministic policy/artifact functions, an exact evidence delta, exact shard
membership, and an old derived cache equal to a prior full recomputation. Each
TLA+ model checks a finite transition system. None of these results by itself
proves that scanner, MariaDB, SQLite state, or filesystem code implements those
functions or transitions. Differential and fault-injection tests remain
required when the vNext design is implemented.

The ordered-rendering proofs additionally assume that one page has a pure,
deterministic render result and that indexed worker results are exact. They do
not prove Pillow determinism or thread safety, Python `ThreadPoolExecutor` and
`Future` behavior, cancellation or spool cleanup, ZIP implementation details,
filesystem atomicity or durability, or that the Python batching and
publish-last code refines the models. The preservation result covers failures
before publication; it does not claim that a failure during the final
destination write is atomic. Exact-byte differential, measured concurrency-
bound, validation/serialization fault, and destination-preservation tests
remain the implementation evidence for those boundaries.

The worker-policy Lean theorems prove only that natural-number auto selection is
clamped into `1..16`, that a valid explicit override is preserved exactly, and
that every modeled batch respects the same hard cap. TLC exhaustively explores
that finite worker range for its four-page profile. Neither proves that Darwin
`sysctl`, Python CPU discovery, or configuration parsing refines those inputs;
mocked platform, validation, cache, API, and runtime tests provide that evidence.
The cap is a scheduling/resource-count bound, not a memory or RSS proof: decoded
image size and Pillow/intermediate allocation remain unmodeled.

The same Lean file also models the immutable worker *decision* value that the
runtime logs once per CBZ-enabled runtime build. An abstract `CpuTopology`
(platform kind, Intel-machine flag, optional process and host CPU counts,
optional Darwin performance and physical core counts, and a native/translated/
unknown/not-probed Rosetta state) stands for the host probe, which the runtime
performs at most once per process; a pure `select` maps the optional override
and that topology to a `WorkerSelection` (mode, configured and selected values,
raw detected authority, closed reason); and `decide` embeds that selection, the
fixed hard cap, and the very topology it was selected from into one
`WorkerDecision`. Its theorems prove that a manual override is exact and marked
manual, that every automatic decision stays in `1..16`, that a Darwin selection
never depends on process or host logical CPU counts, that a non-Darwin
selection never depends on Darwin facts (a container cannot claim to know the
host's performance and efficiency cores), that every fallback reason selects
exactly one worker while every detected reason selects its hard-capped
plausible authority, that the selected count equals the earlier
`resolveWorkerCount` policy applied to the same abstract authority, and that
the decision embeds exactly the topology it was decided from. `observe`
projects every log field, the topology included, from that one decision and
from nothing else. The `OrderedPageRendering` TLA+ model is unchanged because
the decision only feeds its existing `workerCount \in 1..MaxWorkers` boundary.
None of this proves that the Python probe, `sysctl` parsing, the PID-aware
single-flight topology cache, or Python logging refines these definitions; the
mocked-topology differential matrix, the concurrent and fork cache tests, and
the log-format and runtime tests provide that evidence.

The runtime-lifecycle models treat facade close as an atomic linearization point
and CLI unwinding as an abstract bracketed exit. They do not prove Python
`Lock` scheduling or waiting, context-manager or `BaseException` mechanics,
signal delivery, core cache cleanup, destructor behavior, or production
refinement. Deterministic concurrent-close, partial-construction fault, real
core fail-after-close, and CLI exit-path tests provide those implementation
checks. The storage observation is likewise an operation-boundary abstraction:
it does not prove that pathname identity cannot change between one successful
guard and its next POSIX syscall. Real directory replacement, journal-UUID
corruption, maintenance/claim-boundary, and fatal-propagation tests connect the
modeled exact-pair decision to those named implementation seams.

The pytest-supervision model treats Job termination, kill-on-close, POSIX group
termination, and an empty-tree query as abstract transitions. It does not prove
Win32 or POSIX kernel behavior, Python signal delivery, `taskkill`, or venv
launcher behavior. Deterministic fake-clock/API tests refine exit and deadline
decisions; the dedicated `windows-latest` process-tree matrix supplies the real
Windows Job Object, console-break, forced-exit, descendant, and venv evidence.
An exact `0`, timeout, or interrupt receipt requires a synchronous empty-tree
query. The model's infrastructure-failure `125` path may instead rely on the
documented Windows kill-on-last-Job-handle contract at runner process exit; it
does not describe that fallback as an observed empty-tree receipt. A failed
`CloseHandle` call likewise permits only `125`, after which operating-system
process teardown remains the final handle owner.

The gallery-index proofs use exact list equality for capture and boundary
audits. Production uses canonical stat facts and SHA-256 plus independent exact
file-read checks; collision resistance and the mapping from POSIX/SQLite/Python
operations to the model are explicit assumptions, not Lean or TLC results. The
TLA+ profile explores only two galleries with one possible mutation each. The
runtime semantic differential, full public-field direct-stat oracle,
scan-count, active-cache cardinality and rollback, source-mutation, and exact
archive tests provide implementation evidence at those boundaries.

Gallery-index reuse is a structural call-count and constant-factor
optimization, not an asymptotic claim for one unbounded gallery. For `M`
direct entries and fixed page size `B`, the required fresh full-entry audit
after each bounded API page performs `Θ(M² / B)` filesystem `scandir`/`stat`
entry observations. Including the audit's SQLite B-tree primary-key
maintenance gives a conservative `O(M² log M / B)` bound, with metadata and
tag parsing/paging costs accounted for separately. The reusable active index
removes repeated reusable-index construction and metadata parsing; a corpus of
`N` galleries is `O(N)` only when per-gallery entry cardinality is bounded.

The library-authority proofs intentionally assume that the same private inode
authority still denotes the same bytes. They do not prove `flock` exclusion,
single-link ownership, metadata/rename behavior, `fsync` durability, Python
descriptor lifetime, SQLite transaction isolation, or that production
reservation and terminalization code refines the model. The TLA+ gate and
filesystem steps are abstract atomic transitions; its finite success is not an
unbounded proof. Fault/restart, same-adapter concurrency, exact-byte, traversal,
and state-lock tests remain required refinement evidence.

## New-gallery invalidation

Adding one gallery is a corpus change, not merely a local insert. Its evidence
can alter the global spam classification for hashes already used by old
galleries. The verified incremental rule therefore computes the exact changed
spam set, invalidates galleries whose old or new file multiset intersects that
set, and then recomputes every changed artifact input, including content,
selection, owner, and policy version. From the resulting old/new artifact maps
it derives the exact CBZ rebuild, delete, and create sets; old CBZs are rebuilt
or removed when the new corpus changes their outcome.

Content-owner and GID-winner worksets are likewise formed from every changed
candidate's old and new group keys. A group whose final member moves or is
deleted therefore remains in the workset and emits an explicit tombstone even
though the new candidate state no longer contains a row from which to discover
that key. The Lean completeness and locality theorems derive this property from
candidate inequality and group membership; they do not assume equality of the
incremental and full winner maps.

Spam evidence counts exact file occurrences, including duplicate hashes inside
one gallery, rather than counting only gallery membership. Incremental cache
inheritance also pins the exact policy and source baseline revision/generation.
The baseline and target source contexts must have the same registered channel
and source scope; a policy, channel, or scope change requires a depth-zero full
recomputation.

Hash shards are an execution partition only. The shard theorem applies when
the classifier input for a hash is completely contained in that hash's exact
evidence shard; it is not permission to ignore global evidence changes.

The vNext winner reducers have no active-head or incumbent input. Their final
tie-break is the required stable, unique full gallery-locator key, so candidate
insertion order cannot change a content owner or GID winner. A policy-version
change cannot inherit an older analysis generation: it starts a depth-zero full
recomputation. Because policy version and the exact ordered CBZ member plan are
both part of `ArtifactInput`, either difference invalidates the selected
artifact and therefore appears in the exact rebuild set.

The Python model in `tests/reference/vnext_incremental.py` is an independent
cross-component oracle: it imports no production reducer or persistence code.
Production analysis and deduplication are implemented and authorized by the
`h2hdb` core package, not this consumer. The retained model exercises vNext
semantics such as stable nested-locator gallery keys, old-group tombstones,
source-baseline isolation, policy compaction, and the exact disjoint
create/rebuild/delete/unchanged artifact-operation partition. It must not be
used as a second runtime implementation in ingest.

`tests/test_vnext_incremental_state_machine.py` uses Hypothesis to generate
small old/new snapshots and longer state-machine sequences containing
additions, deletions, modifications, empty and metadata-only galleries,
duplicate file hashes, and nested galleries with equal leaf names. Every valid
transition compares the incremental oracle with a clean full recomputation at
each evidence, spam, content, owner, winner, artifact-input, artifact, and
operation layer. Generated policy/channel/scope/base mismatches must fail
closed instead of inheriting derived state.

The deterministic `pr` profile is the default. CI/nightly jobs select the
larger profile with `H2HDB_HYPOTHESIS_PROFILE=nightly`; neither profile uses a
wall-clock assertion or deadline.

```bash
.venv/bin/pytest tests/test_vnext_incremental_state_machine.py
H2HDB_HYPOTHESIS_PROFILE=nightly \
  .venv/bin/pytest tests/test_vnext_incremental_state_machine.py
```

## Commands

```bash
.venv/bin/python scripts/verify-formal.py lean
.venv/bin/python scripts/fetch-formal-tools.py
.venv/bin/python scripts/verify-formal.py tla \
  --tla-jar .formal-tools/tla2tools-1.7.4.jar
```

Tool versions and checksums are pinned in `tools.lock.toml`. The `Small` TLA+
configuration is the required finite profile; `Deep` is manual/nightly. A TLC
success covers every reachable state for the chosen finite constants, not
arbitrary corpus size. The default `auto` runtime uses host Java when available,
then falls back to the digest-pinned, network-off Docker runtime.

Run the larger profile explicitly:

```bash
.venv/bin/python scripts/verify-formal.py tla \
  --tla-jar .formal-tools/tla2tools-1.7.4.jar --deep
```
