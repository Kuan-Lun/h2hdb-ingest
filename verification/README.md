# Formal verification

This directory contains executable specifications owned by filesystem ingest.

- `lean/IncrementalEquivalence.lean` proves that exact file/spam invalidation
  produces the same gallery content, spam decisions, and CBZ create/rebuild/
  delete sets as clean full recomputation. A snapshot uses `Option`, so a
  deleted gallery is distinct from an existing empty gallery.
- `tla/CbzLibraryActivation.tla` model-checks crash recovery between
  reader-invisible
  core publication, bounded local library activation, and reader-head
  finalization, including maintenance-marker and unknown-path safety.

The Lean proof assumes collision-free canonical hash identities, deterministic
policy/artifact functions, an exact evidence delta, exact shard membership, and
an old derived cache equal to a prior full recomputation. The TLA+ model checks
a finite transition system. Neither result by itself proves that scanner,
MariaDB, SQLite state, or filesystem code implements those functions or
transitions. Differential and fault-injection tests remain required when the
vNext design is implemented.

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
