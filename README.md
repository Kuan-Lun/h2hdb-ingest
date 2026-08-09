# h2hdb-ingest

`h2hdb-ingest` owns the filesystem-facing ingest side of H2HDB. Its boundary
includes gallery scanning, `galleryinfo.txt` parsing orchestration, hashing,
deduplication policy, CBZ creation and reconciliation, and the resident ingest
loop with lease-heartbeat orchestration.

CBZ storage has two deliberately separate roots. `artifact_store_path` contains
immutable content-addressed files such as `123-<sha256>.cbz`, plus ingest's
reconciliation state. Those paths are published to the catalog and remain
readable for historical OPDS revisions. `cbz_path` is a current-only Komga
library containing one friendly filename for each current winner. After a
catalog revision commits, ingest atomically replaces that friendly projection,
using an independent, durably flushed copy. The mutable projection never shares
an inode with an immutable artifact.

A synchronization prepares every new immutable artifact without replacing
published files, durably protects the selected files from pruning under its
build ID, publishes
the complete catalog revision transactionally, and only then updates the Komga
view. Immutable published artifacts and files with a commit-ambiguous outcome
remain available to historical catalog revisions; only abandoned staging
artifacts are pruned. A failed database publish therefore leaves both the
active catalog and the current Komga view unchanged. Reconciliation removes
only friendly paths recorded in its own state; unknown operator-owned files are
never replaced or deleted.
Protection state is reference-like: the same content-addressed artifact may be
protected by multiple in-progress builds, and releasing one build cannot make
the artifact pruneable while another build still protects it. Legacy callers
without a build ID use a reserved compatibility protection.

All CBZ-enabled ingest publishers for one catalog must share the same
`artifact_store_path`; it is also their publication coordination domain. Ingest
takes its cross-process publication flock before the database gate and holds it
from immediately before catalog publication through Komga projection
finalization. This prevents a newer revision from committing between an older
publisher's revision check and atomic projection swaps. Process exit releases
the flock automatically; the fsynced pending-projection journal lets the next
publisher recover partial work without claiming unknown files.
The current projection state records both the selected artifact identity and a
regular-file stat signature. Unchanged files are not recopied on later scans or
after restart; symlinks, identity changes, and external mutations (including
same-size writes) invalidate the signature and force an atomic refresh.
Reconciliation state is stored in the ingest-owned indexed SQLite file
`.h2hdb-cbz-state.sqlite3`; it does not use or extend the core catalog schema.
Each prepared artifact and each page of build-scoped protections is committed
as a small delta instead of rewriting a corpus-sized JSON set. Projection
finalization uses file-backed SQLite temporary indexes and 256-row keyset pages
for friendly-name planning, materialization, stale removal, and artifact prune.
It atomically records the complete pending intent before modifying the Komga
tree, so a crash remains resumable without retaining every selected gallery in
Python memory.

Existing `.h2hdb-cbz-state.json` version 1 and version 2 files are validated and
migrated automatically. The old JSON file is retained unchanged as a migration
backup, while `.h2hdb-cbz-state.sqlite3.ready` records that SQLite became
authoritative. A missing database after that marker exists fails closed instead
of replaying a possibly stale JSON backup. Back up and restore the SQLite file
and ready marker together while holding the artifact-store publication lock (or
while every publisher is stopped); do not delete only the database or only the
marker. Unsupported, corrupt, and structurally unsafe legacy or SQLite state is
refused.

Every scan publishes a full canonical source snapshot through the core public
API. It retains the raw gallery name, GID, title (including an empty title),
comment, uploader, timestamps, tags, and every source-file name and SHA-256.
Only deduplication winners become catalog publications. Same-content losers
retain an explicit duplicate owner; two folders with the same GID but different
content remain distinct canonical source records.

The primary scanner API yields batches bounded by both gallery count and file
count. A gallery larger than the file bound is emitted as multiple chunks, and
only its final chunk carries the digests that seal that gallery. Discovery does
not first materialize or globally sort every gallery or file. The compatibility
`scan()` API still materializes its complete result and is not suitable for a
large bootstrap; the installed CLI uses only the staged API. The streamed
source-manifest digest uses the order-independent
version 2 format; the staging database derives the effective, historically
compatible content digest from the complete set of raw file hashes before
publication.

File SHA-256 reuse crosses a durable-cache boundary keyed by the resolved scan
root, relative file locator, device, inode, size, mtime, and ctime. Cache lookup
and recording are bulk operations. A matching cache hit does not read file
bytes; a miss verifies the complete stat signature before and after streaming
the file. Hashing work has at most twice `hash_workers` tasks in flight. Callers
that do not provide durable cache storage receive a process-local compatibility
cache with a bounded least-recently-used window. CBZ output is streamed member
by member, and transformed images spill to a temporary file above a bounded
memory threshold. The staged projection path opens source rows in bounded
keyset pages inside the CBZ worker and never hydrates all files from even one
giant gallery. Database adapters must create a fresh worker-local read
transaction for each page call. Every streamed row carries its exclusion
decision and complete staged stat signature; included source files are checked
immediately before and after their read. The ZIP central directory is spooled
to a temporary file rather than retained as one in-memory entry list. Scan,
synchronization, and maintenance summaries are emitted through the configured
core logger. Long parsing and hashing phases also emit progress heartbeats at
least once per minute. Failed maintenance leaves the ingest turn unacknowledged
so it can be recovered after lease expiry.

Database connectors, schema migrations, durable queues, coordination fencing,
and catalog persistence remain owned by the `h2hdb` core package. This package
uses only the core public interfaces and never owns database schema.
Global deduplication and downloader/deletion cutover are restartable keyset
state machines. Operational preparation is completed in transactions of at
most 1,000 rows, then ingest performs the final clean filesystem observation
immediately before the constant-time source/catalog pointer transaction. A
deletion-generation race refreshes the same sealed build instead of restarting
its hashes. Inactive and abandoned source/projection builds remain durable
cleanup candidates and are deleted child-first in bounded transactions; an
abandoned CBZ protection is released before its only build descriptor can be
removed. Historical catalog revision rows and immutable published artifacts are
retained.

The distribution and installed command use the hyphenated name
`h2hdb-ingest`. The Python import package uses an underscore, so the equivalent
module invocation is `python -m h2hdb_ingest`; `h2hdb-ingest.__main__` is not a
valid Python module path.

Run one of the following after the database has been initialized by core:

```bash
h2hdb-ingest --config /config/h2hdb-ingest.json
python -m h2hdb_ingest --config /config/h2hdb-ingest.json
```

Use `--once` for a single coordinated scan. Core's command is reserved for
schema initialization and validation; it does not own the resident filesystem
ingest loop.

## Configuration

The ingest config embeds the core database connection and adds filesystem and
resident settings. A minimal SQLite example is:

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
    "cbz_path": "/komga-library",
    "artifact_store_path": "/opds-artifacts",
    "max_image_short_side": 768,
    "cbz_grouping": "flat",
    "cbz_sort": "no",
    "cbz_workers": 4,
    "stale_temp_age_seconds": 60,
    "hash_workers": 4,
    "scan_batch_galleries": 128,
    "scan_batch_files": 2048
  }
}
```

All JSON loaders share core's exact environment-placeholder syntax. A string
value such as `"password": "${H2HDB_RW_DB_PASSWORD}"` or
`"download_path": "${H2HDB_DOWNLOAD_PATH}"` is resolved recursively before
Pydantic validation. Only a whole `${ENV_NAME}` string is substituted; inline
interpolation remains literal, and missing or invalid names fail startup
without exposing the environment value. Unknown configuration fields remain
errors.

`scan_batch_galleries` and `scan_batch_files` are independent upper bounds for
one scanner batch. They default to 128 galleries and 2,048 files. The file
bound also splits a single unusually large gallery, so it cannot make a batch
grow without limit. Configuration is hard-capped at 200 galleries and 2,048
files per batch so tuning cannot accidentally recreate a corpus-sized database
transaction. A filesystem walk still visits every gallery, but gallery headers,
file chunks, cache entries, analysis decisions, and projection rows are each
persisted in separate bounded transactions rather than one transaction per
gallery or one transaction for the entire corpus.

An unfinished build is identified by a SHA-256 scope fingerprint covering the
resolved source and CBZ roots, parser/scanner/manifest/deduplication policy
versions, and CBZ semantic settings. Worker counts and batch sizes are excluded,
so throughput tuning can resume existing work. A semantic or root change fails
closed while an incompatible build is unfinished; ingest does not silently
abandon it because that could orphan protected artifacts.

`cbz_path` and `artifact_store_path` must either both be configured or both be
`null`. They must be different, non-nested directories. Setting both to `null`
disables CBZ generation. Grouping accepts `flat`, `date-yyyy`, `date-yyyy-mm`,
or `date-yyyy-mm-dd` and is applied to both roots. The staged production path
currently requires `cbz_sort` to be `no`; startup rejects the historical sort
modes instead of silently publishing a different order. `max_image_short_side`
uses a webtoon-aware policy: portrait images are bounded by width,
landscape images by height, aspect ratio is preserved, and smaller images are
never enlarged. This policy is part of the artifact manifest, so artifacts
whose manifest does not match it are rebuilt instead of silently reused.

CBZ preparation uses `cbz_workers` workers (defaulting to at most four) with at
most twice that many books submitted at once. Results remain in deterministic
plan order, completed artifacts are durably recorded even when another worker
fails, and failures are raised only after submitted workers drain. Ingest logs
each completed book and a progress heartbeat at least once per minute.

After acquiring the shared artifact-store publication lock, each scan removes
only ingest-owned temporary files whose exact private UUID name is at least
`stale_temp_age_seconds` old (60 seconds by default). Artifact-build,
projection, state-marker, and SQLite-migration leftovers from a killed process
are covered;
symlinks, non-regular files, current concurrent work, completed CBZ artifacts,
lookalike names, and operator files are retained. Thus crash residue is cleaned
on the next scan once it is at least 60 seconds old.

Mount only `cbz_path` into Komga: it is the mutable current library. Mount only
`artifact_store_path` into OPDS: it is immutable publication storage. The OPDS
container must see `artifact_store_path` at the same absolute path used by
ingest because that absolute artifact location is recorded in the catalog.
New immutable artifacts, SQLite reconciliation state, and its migration marker
are intentionally created with owner-only permissions. In Docker Compose,
ingest and every OPDS or Komga-side process that reads the shared bind mount must
therefore run with the same numeric UID/GID. An operator may instead manage an
explicit shared-group ACL, but ingest does not weaken artifact permissions
automatically. Validate this with `stat` and a read check from each consumer
container before deployment.
The download directory must already exist and contain at least one entry;
startup rejects an empty directory to catch a missing container volume mount
before an empty snapshot can be published.

For a fresh deployment, initialize the core schema and then publish the initial
filesystem snapshot explicitly:

```bash
h2hdb-ingest-bootstrap \
  --config ingest.json
```

The script refuses an empty gallery mount and any database that already has a
published catalog revision. It first claims the ingest lease and only then
checks that the revision is still zero, so concurrent bootstrap/resident
processes cannot both pass the initial check. Normal resident startup is the
steady-state command after this initial publication. The bootstrap command is
installed with the wheel; `python -m h2hdb_ingest.bootstrap` is equivalent.
If the database revision commits but bootstrap then exits during CBZ
finalization or maintenance, recover with normal `h2hdb-ingest --once` rather
than rerunning bootstrap; the bootstrap guard intentionally rejects every
nonzero revision.

A requested gallery deletion does not remove a source record while its folder
still exists. This lets the core deletion view continue resolving the original
friendly path. After the folder disappears, ingest removes it from the next
snapshot without creating a redownload request for that explicitly deleted GID.

## Development

This project uses the modern src layout: distribution code lives under
`src/h2hdb_ingest`, while the public import remains `import h2hdb_ingest`.
The extra source root prevents tests from accidentally importing an uninstalled
working-tree package.

Create a repository-local environment and install the package in editable mode:

```bash
uv venv --python 3.14
uv pip install -e ".[dev]"
```

Run the standard checks through that environment:

```bash
uv run --no-sync ruff check .
uv run --no-sync mypy src tests
uv run --no-sync pytest
uv run --no-sync python -m build
```

Use `scripts/rebuild-env.sh` to recreate a damaged environment. This repository
does not use a uv workspace and does not commit or depend on `uv.lock`.
`--no-sync` keeps execution tied to the explicitly rebuilt editable environment.

Initialize the database schema explicitly through the core administration CLI
before starting ingest. Ingest startup performs only a schema compatibility
check and never runs migrations.

## License

GNU General Public License v3.0. See [LICENSE](LICENSE).
