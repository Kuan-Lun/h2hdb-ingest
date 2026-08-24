# h2hdb-ingest

`h2hdb-ingest` is the filesystem-facing application for the H2HDB vNext
schema epoch. It observes downloaded galleries, renders and stores immutable
CBZ artifacts, maintains the current Komga projection, and drives the public
core ingest facade under a renewable lease.

The boundary is intentionally strict. The `h2hdb` package owns schema and
epoch administration, transactions, queues, source checkpoints, analysis and
deduplication policy, artifact selection, catalog publication, and release.
This package imports only public vNext domain types, protocols, and facades. It
does not initialize or migrate a database and has no legacy `H2HDB` API path.

## Runtime model

One ingest turn performs these steps:

1. Claim a public vNext ingest session and start its lease heartbeat.
2. Idempotently resolve the immutable natural ingest policy in core.
3. Freeze one filesystem discovery snapshot in a temporary SQLite keyset index.
4. Drive source, analysis, artifact, publication, and finalization state
   machines through bounded public facade calls.
5. If CBZ output is enabled, spool the pinned publication projection to local
   SQLite, reconcile the current Komga view, and only then finalize publication
   in core.
6. Complete the ingest session.

Database operations and local work are split explicitly. The controller lock
is held only for a bounded core issue or commit call. Directory walks, hashing,
parsing, image conversion, ZIP creation, projection spooling, and filesystem
reconciliation run outside that lock, so lease renewal is never blocked by
corpus-sized I/O.

Filesystem pages are deterministic and keyset-addressed. The first gallery
request builds one spill-to-disk discovery index in O(N) work; later pages use
indexed lookup instead of rescanning the corpus. Gallery file, directory, and
tag observations are also bounded and audited. A source mutation observed
between pages fails closed.

## Artifact and Komga roots

CBZ-enabled deployments use two different, non-nested roots:

- `artifact_store_path` contains immutable, content-addressed artifacts and
  ingest-owned coordination journals. Published artifacts remain available to
  historical catalog revisions.
- `cbz_path` contains only the friendly current projection for Komga.

The artifact adapter verifies the expected SHA-256 while materializing each
archive and never mutates an existing content-addressed artifact. Protection
and release tokens are monotone and crash-safe.

The current projection is built from a complete pinned core publication. It is
spooled before any friendly path changes, uses atomic copies, and records both
artifact identity and a durable regular-file stat signature. Unknown paths are
never overwritten or removed. A managed path that became a symlink, directory,
or externally changed file fails closed, including a change between stale-path
preflight and deletion.

Every publisher for one catalog must share the same `artifact_store_path`.
Ingest holds its artifact-store publication flock across the bounded core
publication calls, local projection, and core finalization. Never acquire the
database gate and publication flock in the opposite order.

## Installation and commands

The distribution command is hyphenated while the Python package uses an
underscore:

```bash
h2hdb-ingest --config /config/h2hdb-ingest.json
python -m h2hdb_ingest --config /config/h2hdb-ingest.json
```

Use `--once` for one coordinated turn. Core schema initialization is a
separate operator action; normal ingest startup only runs
`VNextDatabaseAdminFacade.check()` against an existing READY epoch.

For a fresh, already initialized epoch, publish the first nonempty source with:

```bash
h2hdb-ingest-bootstrap --config /config/h2hdb-ingest.json
```

The bootstrap command refuses an empty source and a catalog that already has a
published revision. It does not create or migrate schema.

## Configuration

The ingest configuration embeds the public core configuration. A minimal
SQLite example with artifacts enabled is:

```json
{
  "core": {
    "database": {
      "sql_type": "sqlite",
      "database": "/data/h2h.sqlite"
    }
  },
  "paths": {
    "download_path": "/download",
    "artifact_store_path": "/opds-artifacts",
    "cbz_path": "/komga-library",
    "max_image_short_side": 768,
    "cbz_grouping": "flat"
  },
  "resident": {
    "periodic_scan_seconds": 1800,
    "poll_seconds": 5,
    "lease_seconds": 300,
    "heartbeat_seconds": 60,
    "max_rows": 128
  }
}
```

`cbz_path` and `artifact_store_path` must either both be configured or both be
`null`; setting both to `null` disables artifact output. Grouping accepts
`flat`, `date-yyyy`, `date-yyyy-mm`, or `date-yyyy-mm-dd`.

`max_rows` is constrained to 1–128. Core also fixes publication and projection
pages at 128 rows. These limits bound each database or adapter step, not the
total corpus size.

Core environment placeholders are resolved before validation. A complete
string such as `"${H2HDB_RW_DB_PASSWORD}"` is substituted recursively; missing
variables and unknown configuration fields fail startup.

The download root must already be a nonempty directory. When artifacts are
enabled, ingest creates the two output roots if needed. Containers that share
the artifact or current-view mounts should use compatible numeric UID/GID or an
operator-managed ACL.

## Development

This repository uses a `src` layout and a repository-local virtual environment:

```bash
uv venv --python 3.14
uv pip install -e "../h2h-galleryinfo-parser.clone" \
  -e "../h2hdb.clone" -e ".[dev]"
uv run --no-sync ruff check .
uv run --no-sync black --check .
uv run --no-sync mypy src tests scripts/bootstrap-catalog.py
uv run --no-sync pytest
uv run --no-sync python -m build
```

The source-to-analysis-to-publication resident E2E runs against SQLite by
default. Enable its pinned MariaDB 10.11.11 testcontainer with Docker available:

```bash
H2HDB_TEST_MARIADB=1 uv run --no-sync pytest tests/test_runtime_e2e.py
```

The PyPI validation workflow always enables the MariaDB case.

The opt-in private-corpus regression test automatically uses
`.local-test-data/hath-download`. This repository-local directory is ignored by
Git and must contain a complete Hentai@Home download tree. Set
`H2HDB_INGEST_TEST_DOWNLOAD_PATH` only when testing a corpus stored elsewhere.

The independent Python oracle and Lean model under `verification/` specify
cross-component analysis semantics; core owns the production implementation.
The TLA+ model covers crash-safe current-projection behavior. Run the required
formal checks with:

```bash
uv run --no-sync python scripts/verify-formal.py lean
uv run --no-sync python scripts/fetch-formal-tools.py
uv run --no-sync python scripts/verify-formal.py tla \
  --tla-jar .formal-tools/tla2tools-1.7.4.jar
```
