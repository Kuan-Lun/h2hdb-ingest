# h2hdb-ingest

`h2hdb-ingest` is the filesystem-facing application for the H2HDB vNext
schema epoch. It observes downloaded galleries, renders and activates canonical
CBZ artifacts in one shared library, and drives the public
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
5. If CBZ output is enabled, stage complete verified CBZs privately, spool the
   pinned stable storage keys, and activate bounded pages under a durable
   maintenance marker.
6. Finalize the reader-visible core head only after the library reaches
   `READY`, then clear the marker.
7. Complete the ingest session.
8. Advance one bounded page in the ingest-owned private cleanup queue.
9. After the SHARED session gate is released, make one response-loss-safe core
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
parsing, image conversion, ZIP creation, activation spooling, and filesystem
reconciliation run outside that lock, so lease renewal is never blocked by
corpus-sized I/O.

Filesystem pages are deterministic and keyset-addressed. The first gallery
request builds one spill-to-disk discovery index in O(N) work; later pages use
indexed lookup instead of rescanning the corpus. Gallery file, directory, and
tag observations are also bounded and audited. A source mutation observed
between pages fails closed.

## Single CBZ library

CBZ-enabled deployments configure one `library_path` parent:

```text
library/
├── current/                         # pre-created reader bind source
├── .h2hdb-coordination/             # pre-created reader bind source
│   ├── publication.lock
│   └── ACTIVATING                   # only during unfinished cutover
└── .h2hdb-state/                    # ingest-private
    ├── staging/                     # complete candidates
    ├── quarantine/                  # stale-removal recovery
    ├── journal/                     # SQLite activation journal
    └── locks/                       # adapter state lock
```

Core owns the stable GID storage key. The registered
`gid-sha256-12-v1` codec resolves to
`hash-v1/<2 hex>/<1 hex>/h2h-<gid>.cbz`; content, title, date, and revision
changes therefore retain the same Komga identity. Ingest never derives a
second locator or grouping layout.

`protect()` resumes only an exact private staging prefix, then hashes,
size-checks, and fsyncs the complete file. A reader-invisible core publication
is then spooled to the activation journal. Each `reconcile_page()` advances at
most 128 installs or removals. It creates the durable `ACTIVATING` marker while
holding `publication.lock` exclusively, uses atomic no-replace capture and a
same-filesystem no-replace rename for changed paths, and records exact
regular-file identities. Stage authority becomes terminal in the same SQLite
transaction that records current authority. Core advances the reader-visible
head only after durable `READY`; `complete()` then removes and fsyncs the
marker before unlocking.

If publishing a staging leaf or activation marker loses its response after the
rename, replay verifies the exact leaf, re-fsyncs its owning directory, and
verifies the same identity again before advancing the SQLite journal.
Conservative power-loss recovery may expose both rename names. Replay collapses
them only when the journal facts and digest identify one exact shared two-link
inode; it syncs both directories, removes the source duplicate, syncs again,
fsyncs the survivor inode, and revalidates its post-unlink identity before
retiring journal authority. Byte-identical names on different inodes are
preserved and fail closed.

OPDS holds a shared flock while resolving the current head and opening the
file. Lock contention, a present/invalid marker, an unknown target, an
intermediate symlink, or externally changed managed bytes fails closed. Komga
and OPDS therefore read the same persistent CBZ; staging/quarantine bytes may
exist only during a pending or interrupted activation. SIGINT/SIGTERM stops at
the next bounded durable step and does not claim new work. A forced container
kill leaves the marker and journal so restart continues before readers resume.

The `library_path` parent, `.h2hdb-state`, and `.h2hdb-coordination` are
ingest-owned, single-writer namespaces. No other process, including one running
under the same UID, may mutate them; Komga and OPDS mounts remain read-only.
Races on public `current` entries still fail closed and preserve unknown bytes.

A terminal release tombstones its protection token before cleanup, so delayed
or response-lost `protect()` calls cannot recreate bytes. A durable `WRITING`
row authorizes cleanup of its deterministic partial private temp after the
adapter captures that temp's stable digest and file identity. If replay sees an
authorized stage, temp, quarantine, or marker already absent, it re-fsyncs the
owning directory and confirms absence before retiring the journal authority.

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
    "library_path": "/hentai/library",
    "max_image_short_side": 768
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

`library_path` points at the parent containing `current`,
`.h2hdb-coordination`, and `.h2hdb-state`.
Setting it to `null` disables artifact output. `download_path` and
`library_path` must be distinct and non-nested. When enabled, it must already
exist with empty `current` and `.h2hdb-coordination` reader bind sources. All
three paths must be real directories. Compose owns the service identity, mount
scope, and read-only/read-write policy; host ACLs or modes need only let that
identity perform the mounted operation. Ingest validates and fsyncs these
externally provisioned roots, but does not enforce or change their UID, GID, or
POSIX mode. Creation calls provide conservative initial modes for new entries
without treating the resulting metadata as a replay contract. Do not
pre-create `.h2hdb-state`: ingest owns and durably creates that private tree.
The former
`.h2hdb-state/coordination` layout is unsupported and is neither read nor
migrated; any such entry makes startup fail closed before private state is
modified.

`max_rows` is constrained to 1–128. Core also fixes publication and activation
pages at 128 rows. These limits bound each database or adapter step, not the
total corpus size.

Core environment placeholders are resolved before validation. A complete
string such as `"${H2HDB_RW_DB_PASSWORD}"` is substituted recursively; missing
variables and unknown configuration fields fail startup.

The download root must already be a nonempty directory. Under the pre-existing
library mount, ingest durably validates the pre-created `current` and
`.h2hdb-coordination` mount roots, creates the private state directories,
journal, and permanent locks, and revalidates every managed identity. Mount
only `current` into Komga. Mount `current` and `.h2hdb-coordination` separately
and read-only into OPDS; never expose `.h2hdb-state`, staging, quarantine, or
the journal to readers.

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
The TLA+ model covers crash-safe single-library activation behavior. Run the required
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
