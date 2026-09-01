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
  `Small` profile uses four pages and explores worker counts `1..4`.
- `tla/LibraryIoReservation.tla` model-checks WRITING and activation
  reservations across unlocked I/O, crash/restart and response loss, exact
  terminalization, wrong caller digests, stale fence attempts, and the rule
  that release of an exact object cannot cross its unfinished durable
  activation entry. Its single-object `PROTECT` gate corresponds to one
  bounded runtime token-lock stripe; unrelated stripes remain concurrent and
  are outside this model.

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
