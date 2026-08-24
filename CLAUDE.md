# CLAUDE.md

This file provides repository guidance to Claude Code and other coding agents.

## Communication

- Claude 必須以繁體中文回答所有對話內容；程式碼、指令、檔名與專有名詞維持原文。

## Project

`h2hdb-ingest` is the filesystem-facing service for H2HDB vNext. It owns
keyset-paged discovery, `galleryinfo.txt` parsing, exact source observations,
artifact rendering/storage, crash-safe Komga current-view reconciliation, and
the resident loop with its ingest-lease heartbeat.

Keep the modern src layout at `src/h2hdb_ingest`; consumers still import
`h2hdb_ingest`. Do not flatten it to match older repositories.

The `h2hdb` core package exclusively owns database connectors, transactions,
schema-epoch administration, durable queues, token fencing, source checkpoints,
analysis/deduplication policy, artifact selection, and catalog publication. Use
only public vNext facades, protocols, and domain receipts. Never import core
internals, recreate the removed `H2HDB` compatibility API, create schema, or
supply registry surrogate IDs. Startup calls
`VNextDatabaseAdminFacade.check()` only and never initializes or migrates.

Keep the session controller lock around bounded database issue/commit calls
only. Filesystem scans, hashing, rendering, artifact storage, projection
spooling, and other local I/O belong in the prepare step outside that lock so
the heartbeat can renew the exact session receipt.

Prepare content-addressed CBZ artifacts without overwriting currently published
files. Durably protect selected artifacts before the catalog revision
transaction, then promote them to published state after commit. Retain immutable
published and commit-ambiguous protected artifacts for historical revisions;
prune only abandoned staging artifacts. Startup performs only the core schema
compatibility check and must not run migrations.

CBZ operation uses two distinct, non-nested roots. `artifact_store_path` owns
content-addressed immutable artifacts and reconciliation state for OPDS;
`cbz_path` owns only the current friendly-file projection for Komga. Update the
Komga projection only after catalog publication succeeds, and remove only paths
recorded as managed in artifact-store state. Never replace or delete an unknown
file in the Komga root.

Every CBZ-enabled publisher must use the same shared `artifact_store_path` as
its coordination domain. Acquire the artifact-store publication flock before
the bounded core publication calls and hold it through projection and core
finalization; never acquire these locks in the reverse order. The OS releases
the flock if a process exits, and the durable pending projection journal is the
source of truth for crash recovery.
Persist the current projection's artifact identity and regular-file stat
signature. Skip a projection copy only when both still match; any external
mutation must be reverified with an atomic copy.

## Development

This is an independent repository and not part of a uv workspace. `uv.lock` is
ignored and must not be used by build, test, or runtime workflows. Always run
Python through `uv run --no-sync` after installing the editable environment.

```bash
uv venv --python 3.14
uv pip install -e ".[dev]"
uv run --no-sync ruff check .
uv run --no-sync black --check .
uv run --no-sync mypy src tests
uv run --no-sync pytest
uv run --no-sync python -m build
```

The real MariaDB resident-lifecycle E2E pins MariaDB 10.11.11 through
testcontainers. It is opt-in for ordinary local runs and mandatory in the PyPI
validation workflow. Run it locally with Docker available:

```bash
H2HDB_TEST_MARIADB=1 uv run --no-sync pytest tests/test_runtime_e2e.py
```

Use `scripts/rebuild-env.sh` to recreate a damaged environment.

## Branch Discipline

Do not create or switch to a development branch. All development work must be
performed directly on the repository's primary branch (`main`).

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

The retained incremental oracle and Lean model describe global analysis
semantics, but production analysis is implemented and authorized by `h2hdb`.
They must never become a second runtime deduplication implementation here.

## Shared Finalization

Provider-neutral hooks live in `scripts/hooks/` and are shared by humans and
all coding agents.

- After Python changes, run `bash scripts/hooks/finalize-python.sh`.
- After Markdown changes, run `bash scripts/hooks/finalize-markdown.sh`.

Do not duplicate their implementation under an agent-specific directory.

## Design

This project is pre-1.0. Prefer the cleanest architecture over compatibility
shims and deprecated aliases. Follow SOLID principles. Keep filesystem work,
compression, sleeps, and network operations outside core transactions and the
database gate.

Keep this document synchronized with `AGENTS.md` whenever ownership, workflow,
testing, or tooling conventions change.
