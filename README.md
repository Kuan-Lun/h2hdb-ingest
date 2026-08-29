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
7. Advance one bounded page in each ingest-owned CBZ cleanup queue.
8. After the SHARED session gate is released, make one response-loss-safe core
   current-only maintenance attempt. A typed local or core `PROGRESSED` outcome
   is retried immediately without resetting the periodic deadline; `BLOCKED`,
   `CONTENDED`, `DONE`, and transient failures use the ordinary idle poll
   cadence.

Core gallery-staging capacity exhaustion is bounded backpressure rather than a
fatal resident error. After the rejected request commits no rows, the resident
stops the heartbeat, completes the exact ingest session, and runs both bounded
maintenance actions. Any `PROGRESSED` result is retried immediately; otherwise
the resident uses the ordinary idle poll cadence so it cannot busy-loop. If
exact session completion fails, the original capacity exception is preserved
with the completion failure attached as a note. Source-manifest mismatch
remains fail-closed.

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

- `artifact_store_path` contains content-addressed artifacts and ingest-owned
  coordination journals. It retains current and bounded pending/protected
  artifacts; released old artifacts are reclaimed after a new current
  projection reconciles.
- `cbz_path` contains only the friendly current projection for Komga.

The artifact adapter verifies the expected SHA-256 while materializing each
archive and never mutates an existing content-addressed artifact. Protection
and release transitions are crash-safe. When neither the current nor pending
projection references a released artifact, bounded cleanup verifies its exact
digest and regular-file type, unlinks it, and removes its artifact row. Cleanup
is driven by a durable released-digest queue; the terminal `RELEASED` token
tombstone remains permanently so a delayed protect cannot resurrect reclaimed
bytes. Symlinks, unknown paths, and externally changed bytes fail closed.
Cleanup first captures a managed leaf with an atomic no-replace rename into a
private mode-0700 quarantine namespace under the same filesystem root. Only the
publication-lock-holding adapter may mutate that namespace. Unexpected owner,
mode, inode, or directory entries preserve the quarantined bytes and fail
closed without acknowledging the durable candidate.
`CurrentProjectionMaintenanceAdapter.maintain_cleanup()` is the public bounded
resident action: one call advances at most eight projection-outbox candidates
and eight artifact-queue candidates. It reports `PROGRESSED` only when durable
work committed and more remains, `BLOCKED` when protected bytes leave work but
no progress, and `DONE` when both queues are empty. The resident calls it before
every ingest claim and once after session completion, so a backlog larger than
one page drains without a restart or another publication.

The current projection is built from the complete current core publication. It is
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
./scripts/rebuild-env.sh
./scripts/check-fast.sh
./scripts/check-full.sh
```

Dependencies resolve from the configured package index. An integration task
may override a dependency explicitly without relying on sibling checkouts:

```bash
./scripts/rebuild-env.sh \
  --source h2hdb=/tmp/h2hdb.whl \
  --source h2h-galleryinfo-parser='git+https://github.com/Kuan-Lun/h2h-galleryinfo-parser.git@ref'
```

The source-to-analysis-to-publication resident E2E runs against SQLite by
default. Enable its pinned MariaDB 10.11.11 testcontainer with Docker available:

```bash
H2HDB_TEST_MARIADB=1 .venv/bin/pytest tests/test_runtime_e2e.py
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
.venv/bin/python scripts/verify-formal.py lean
.venv/bin/python scripts/fetch-formal-tools.py
.venv/bin/python scripts/verify-formal.py tla \
  --tla-jar .formal-tools/tla2tools-1.7.4.jar
```

TLC uses host Java when available. If no working JRE is present, the default
`auto` mode falls back to the digest-pinned, network-off Docker runtime in
`tools.lock.toml`.

## License

GNU General Public License v3.0 only. See [LICENSE](LICENSE).
