# Formal verification

This directory contains executable specifications owned by filesystem ingest.

- `lean/IncrementalEquivalence.lean` proves that exact file/spam invalidation
  produces the same gallery content, spam decisions, and CBZ create/rebuild/
  delete sets as clean full recomputation. A snapshot uses `Option`, so a
  deleted gallery is distinct from an existing empty gallery.
- `tla/CbzProjection.tla` model-checks crash recovery between core publication
  and local projection finalization, complete pending journals, unknown-path
  protection, and artifact garbage-collection safety.

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

Hash shards are an execution partition only. The shard theorem applies when
the classifier input for a hash is completely contained in that hash's exact
evidence shard; it is not permission to ignore global evidence changes.

## Commands

```bash
uv run --no-sync python scripts/verify-formal.py lean
uv run --no-sync python scripts/fetch-formal-tools.py
uv run --no-sync python scripts/verify-formal.py tla \
  --tla-jar .formal-tools/tla2tools-1.7.4.jar
```

Tool versions and checksums are pinned in `tools.lock.toml`. The `Small` TLA+
configuration is the required finite profile; `Deep` is manual/nightly. A TLC
success covers every reachable state for the chosen finite constants, not
arbitrary corpus size.

Run the larger profile explicitly:

```bash
uv run --no-sync python scripts/verify-formal.py tla \
  --tla-jar .formal-tools/tla2tools-1.7.4.jar --deep
```
