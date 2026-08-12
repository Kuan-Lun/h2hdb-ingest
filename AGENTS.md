# Agent Instructions

## Project

`h2hdb-ingest` is the filesystem-facing ingest service for H2HDB. Python must
be run through `uv run --no-sync`, using the repository-local virtual
environment. The supported Python version is defined by `requires-python` in
`pyproject.toml`.

## Ownership Boundary

This repository owns:

- filesystem scanning and gallery discovery;
- `galleryinfo.txt` parsing orchestration;
- source hashing and deduplication policy;
- CBZ creation, rebuilding, and final reconciliation;
- the resident ingest loop and ingest-lease heartbeat orchestration.

Keep the src layout (`src/h2hdb_ingest`); the public import package is
`h2hdb_ingest`. Do not move the package to the repository root merely to match
an older project.

The `h2hdb` core package exclusively owns connectors, transactions, database
schema and migrations, durable queues, token-fenced coordination, catalog
repositories, and catalog publication. Depend only on core public interfaces;
do not import connector or repository internals and do not add schema here.
Catalog publication must follow successful final CBZ reconciliation.
Prepare new CBZ files without overwriting currently published artifacts. Before
entering the revision transaction, durably protect every selected artifact from
pruning. After commit, promote those files to published state. Retain immutable
published and commit-ambiguous protected artifacts for historical revisions;
prune only abandoned staging artifacts. Consumer startup performs a
compatibility check and must never migrate core schema.

CBZ operation uses two distinct, non-nested roots. `artifact_store_path` owns
content-addressed immutable artifacts and reconciliation state for OPDS;
`cbz_path` owns only the current friendly-file projection for Komga. Update the
Komga projection only after catalog publication succeeds, and remove only paths
recorded as managed in artifact-store state. Never replace or delete an unknown
file in the Komga root.

Every CBZ-enabled publisher must use the same shared `artifact_store_path` as
its coordination domain. Acquire the artifact-store publication flock before
the core database gate and hold it from immediately before catalog publication
through projection finalization; never acquire these locks in the reverse
order. The OS releases the flock if a process exits, and the durable pending
projection journal is the source of truth for crash recovery.
Persist the current projection's artifact identity and regular-file stat
signature. Skip a projection copy only when both still match; any external
mutation must be reverified with an atomic copy.

## Environment and Commands

This is a standalone repository, not a uv workspace. `uv.lock` is ignored and
must not become a build, test, or runtime input.

```bash
uv venv --python 3.14
uv pip install -e ".[dev]"
uv run --no-sync ruff check .
uv run --no-sync black --check .
uv run --no-sync mypy src tests
uv run --no-sync pytest
uv run --no-sync python -m build
```

If the environment is damaged, run `scripts/rebuild-env.sh`.

## Formal Verification

The executable vNext specifications live in `verification/`; they are design
models, not evidence that the current runtime implements the model. Run them
with the pinned Lean toolchain and checksum-pinned TLC release:

```bash
uv run --no-sync python scripts/verify-formal.py lean
uv run --no-sync python scripts/fetch-formal-tools.py
uv run --no-sync python scripts/verify-formal.py tla \
  --tla-jar .formal-tools/tla2tools-1.7.4.jar
```

Use `--deep` only for the larger manual/nightly TLA+ profile; the default
`Small` profile is the finite required check. TLC success exhausts reachable
states only for the selected constants. Lean theorems are unbounded over their
stated mathematical inputs, but rely on explicit assumptions such as exact
delta detection, correct prior caches, collision-free canonical hash identity,
and deterministic policy/artifact functions. Differential tests and crash/fault
injection are still required for implementation conformance.

A newly added gallery changes the corpus evidence and can change spam answers
for hashes and galleries that already existed. Incremental processing must
derive the exact global spam delta, invalidate every affected old or new file
multiset, recompute selection/ownership/policy artifact inputs, and derive the
exact CBZ rebuild, delete, and create sets. Never treat a new gallery as a
local-only shard update unless the locality premise is established by the
classifier model.

## Shared Finalization

The repository keeps provider-neutral hooks in `scripts/hooks/`.

- After changing Python, run `bash scripts/hooks/finalize-python.sh`.
- After changing Markdown, run `bash scripts/hooks/finalize-markdown.sh`.

Do not create agent-specific copies of these implementations.

## Design and Compatibility

The project is pre-1.0. Prefer clear responsibility boundaries and the cleanest
end state over compatibility shims or deprecated aliases. Follow SOLID
principles and keep network waits or filesystem work outside core database
transactions and the database gate.

`CLAUDE.md` documents the same repository rules. Keep both files synchronized
when changing workflow, ownership, testing, or tooling conventions.
