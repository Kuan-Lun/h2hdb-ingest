# h2hdb-ingest

`h2hdb-ingest` turns completed Hentai@Home downloads into an H2HDB catalog and
an optional comic library for Komga and OPDS readers. It watches your download
folders, updates the catalog as the collection changes, and produces CBZ files
and thumbnails. Your original downloads remain the source collection.

Use this service to prepare and maintain the library. Use Komga or
[`h2hdb-opds`](https://github.com/Kuan-Lun/h2hdb-opds) to browse and read it.
Readers receive read-only access to the published files.

## Before you start

You need:

- Python 3.14 or newer.
- A nonempty download directory containing completed galleries with
  `galleryinfo.txt` metadata. Nested collection folders are supported.
- An H2HDB database, using SQLite or MariaDB. This release requires
  `h2hdb>=0.41.0,<0.42.0` and schema epoch 3, version 8.
- For CBZ output, a separate writable library directory and enough disk space
  for image processing, one gallery's verified render input, database plans,
  and all output awaiting publication.

Ingest writes to the database and library. Do not point it at a library managed
by another writer, and do not edit its generated files manually. The download
and library directories must be distinct; neither may contain the other.

## Install

Create a Python environment and install the package:

```bash
python3.14 -m venv .venv
source .venv/bin/activate
python -m pip install h2hdb-ingest
h2hdb-ingest --help
```

The package installs its compatible H2HDB and image-processing dependencies.
Run the following commands from this activated environment. Replace the example
paths with paths available to your service account or container.

## Set up the database

For a new SQLite catalog, save this as `core.json`:

```json
{
  "database": {
    "sql_type": "sqlite",
    "database": "/data/h2hdb/catalog.sqlite"
  }
}
```

Create `/data/h2hdb` first, then initialize the empty database:

```bash
mkdir -p /data/h2hdb
python -m h2hdb migrate --config core.json
python -m h2hdb check --config core.json
```

`migrate` creates a new schema or resumes its matching interrupted
initialization. It does not upgrade arbitrary existing databases. Ingest itself
never initializes or migrates the schema.

For MariaDB, set `sql_type` to `mariadb` and supply `host`, `port`, `user`,
`password`, and `database`. Use the same connection in the ingest configuration
below, with an account that can write to the catalog. See the
[H2HDB administration guide](https://github.com/Kuan-Lun/h2hdb#readme)
for database setup and upgrades.

## Prepare the library

For CBZs and thumbnails, create these directories before starting ingest:

```bash
mkdir -p /data/h2hdb/library/current/acquisitions
mkdir -p /data/h2hdb/library/current/artwork
mkdir -p /data/h2hdb/library/.h2hdb-coordination
```

They must be real directories, not symlinks, on the same filesystem. For a
container deployment, create them on the host before creating the reader
containers. Ensure the ingest account can write to them. Ingest creates its own
private `.h2hdb-state` directory; do not create or modify that directory yourself.

Mount these paths for the respective services:

| Service | Host library subtree | Access |
| --- | --- | --- |
| `h2hdb-ingest` | Entire `library/` directory | Read-write |
| Komga | `library/current/acquisitions/` | Read-only |
| `h2hdb-opds` | `library/current/` | Read-only |
| `h2hdb-opds` | `library/.h2hdb-coordination/` | Read-only |

Komga should receive only `acquisitions/`; `artwork/` contains standalone
thumbnails, not comic books. Keep `.h2hdb-state` private to ingest. Other
processes must not modify the library, including its coordination directory.

Skip library preparation if you only want catalog metadata.

## Configure ingest

Save this as `ingest.json`:

```json
{
  "core": {
    "database": {
      "sql_type": "sqlite",
      "database": "/data/h2hdb/catalog.sqlite"
    }
  },
  "paths": {
    "download_path": "/data/hath-download",
    "library_path": "/data/h2hdb/library"
  }
}
```

The database settings match `core.json`, but ingest places them inside `core`.
Use paths as seen by the ingest process. `download_path` must already exist and
be nonempty. Set `library_path` to `null` to publish metadata without decoding
images or producing CBZs and thumbnails.

Optional settings can be added to `paths` or a top-level `resident` object:

| Setting | Default | When to change it |
| --- | --- | --- |
| `paths.max_image_short_side` | `768` | Choose the maximum short-side pixels for generated pages; accepts 1–8192. Images keep their aspect ratio and are never enlarged. |
| `paths.page_render_workers` | `null` | Set 1–16 concurrent page workers, or leave automatic selection enabled. Lower it if image processing puts too much pressure on memory. |
| `resident.publication_batch_galleries` | `null` | Select all eligible complete galleries before publication. An explicit integer from 1 through 1,000,000 limits newly admitted galleries per publication. |
| `resident.progress_log_interval_seconds` | `60` | Set the interval, in positive seconds, between progress summaries while work is active. |
| `resident.source_quiet_seconds` | `300` | Wait this long without another observed source change before synchronizing. |
| `resident.source_max_wait_seconds` | `1800` | Synchronize after this maximum wait despite continuing source changes. Must be at least the quiet interval. |
| `resident.source_probe_interval_seconds` | `30` | Pause this long between completed source-monitor passes. |

For image output, `paths.render_policy` accepts `page_jpeg_quality` (default
90), `thumbnail_jpeg_quality` (85), `optimize` (`true`), and `resampler`
(`"lanczos"`). Quality values are integers from 0 through 95. Other resamplers
are `nearest`, `box`, `bilinear`, `hamming`, and `bicubic`. Changing rendering
settings can require galleries to be checked and artifacts rebuilt.

Automatic worker selection is capped at 16 and logged at startup. A Docker
container uses the CPU availability visible inside the container; it cannot
infer the host's macOS performance-core count. An explicit worker count
provides control when the automatic choice does not suit your host.

JSON strings consisting exactly of `${ENV_NAME}` can read environment variables,
for example `"password": "${H2HDB_PASSWORD}"`. Inline substitution such as
`"db-${INSTANCE}"` is unsupported. Missing variables and unknown configuration
fields cause startup to fail.

## Run

Start the resident service to process existing downloads and watch for changes:

```bash
h2hdb-ingest --config ingest.json
```

For a single coordinated publication attempt:

```bash
h2hdb-ingest --config ingest.json --once
```

A one-shot run selects the complete inventory by default. An explicit admission
limit still applies, and incomplete or changing galleries require a later turn.
Use resident mode to continue processing that pending work.
A one-shot run fails if it cannot complete a publication, for example because
of lease contention or insufficient storage.

To require a nonempty first publication in a fresh, initialized catalog:

```bash
h2hdb-ingest-bootstrap --config ingest.json
```

Bootstrap refuses a catalog that already has a publication. It stops after the
first nonempty catalog; start the resident service afterward to process the
remaining galleries. The equivalent resident module command is
`python -m h2hdb_ingest --config ingest.json`.

Use `Ctrl+C` or send `SIGTERM` for a graceful stop. Shutdown completes the
current bounded step and resource cleanup. A full database audit already in
progress must finish before a graceful stop can take effect.

## What to expect

The first run inventories the source collection. By default,
`resident.publication_batch_galleries` is `null`: ingest selects all eligible
complete galleries in one turn before global analysis and publication. This
prioritizes total catch-up time by avoiding repeated whole-collection analysis
and validation after small additions. Readers see the result after the complete
turn; database operations and filesystem pages retain their own bounded limits.

An explicit positive value preserves incremental publication. For example,
`100` still admits at most 100 previously unknown galleries per publication,
while applying changes and confirmed deletions to known galleries. It does not
limit the inventory or total published books. Existing settings are not silently
overridden: change an explicit `100` to `null` to select full-collection catch-up.
This setting does not impose a time or disk-space budget.

Source observation retains immutable hashes and metadata, without copying every
gallery's images until publication ends. Rendering rereads the original files
and verifies them against that observation. Core keeps one gallery's verified
render-input spool while preparing its artifact, then releases it. Changed or
missing source bytes cannot be published under the earlier observation and must
be observed again. Keep the source collection available throughout the turn.

After interruption, ingest first resumes an unpublished, sealed source batch
when its root and policies still match. Newly downloaded galleries are picked up
after that batch publishes; their arrival does not discard already committed
analysis or prepared CBZs. Rendering still verifies live source bytes. A source
failure requests fresh observation and can require a replacement batch.

The resumed batch has no fresh deferred/waiting inventory counts. INFO reports
that a new inventory is pending, and resident mode immediately schedules it after
publication instead of declaring initial catch-up complete. For Python callers,
`VNextIngestSourceSynchronizationResult`, `VNextIngestSynchronizationResult` and
`ResidentIngestor.deferred_gallery_count` can now report `None` for these unknown
counts. Both result counts are `None` together, and `inventory_scan_pending`
identifies that state. A one-shot run completes the resumed batch; use resident
mode or another invocation to include newly arrived galleries.

During the first scan, each gallery's completed image checks and source facts
are persisted before checking the next gallery. Restart performs a fresh marker
inventory and reuses matching completed observations. At most the current
gallery's unsealed checks are lost when markers remain stable; changed galleries
must be checked again. These checkpoints retain metadata and hashes, not copies
of the entire source image collection.

This update requires the Core schema-7-to-8 offline converter. It preserves the
database contents and complete library, including CBZs and private state; do not
clear the database or rebuild the library to perform this upgrade.

Keep `galleryinfo.txt` as the completion marker: finish writing a gallery's
images before writing its metadata. Incomplete or changing galleries wait for
a later turn while other complete galleries continue. A completed gallery is
a discovery leaf, so galleries nested inside it are not discovered. Removing
a completion marker temporarily retains the last published observation;
confirmed removal of the gallery removes it from the source collection.

After startup, observed changes trigger work using the quiet and maximum-wait
settings. An unchanged source does not trigger repeated full synchronization.
The old `periodic_scan_seconds` setting is not accepted.

With library output enabled:

- Supported page suffixes are `.avif`, `.bmp`, `.gif`, `.jpeg`, `.jpg`, `.png`,
  and `.webp`, ignoring ASCII case. Other regular files are not rendered.
- Every accepted page becomes a JPEG. Animated GIFs use the first frame.
- A gallery with an undecodable page is excluded as a whole; ingest does not
  silently publish a book with missing pages. The logs identify the rejection.
  Repair the source and rewrite its completion marker to trigger another check.
- A selected gallery produces `h2h-<gid>.cbz`, with `galleryinfo.txt` and ordered
  pages. Page zero supplies the full-size cover, and a separate thumbnail has a
  maximum side of 320 pixels. A gallery without eligible pages has a
  metadata-only CBZ and no cover or thumbnail.

Output is limited to 4096 pages per gallery, 32 MiB per encoded JPEG page,
8192 pixels on the long side, 40 megapixels per output page, and
2,147,483,647 bytes per CBZ. Large source images are reduced without enlargement;
source dimensions and file size alone do not exclude them. Some image codecs
still need large memory buffers, so worker limits are not a fixed memory ceiling.

Ingest stores output under `current/acquisitions/` and `current/artwork/` using
managed paths. Do not rename those files. Deduplication and spam decisions use
the whole known collection, so adding galleries can replace or remove earlier
published books; the book count need not increase with every batch.

## Monitor and maintain the service

INFO logs show startup checks, the current activity, measured progress, and
publication results. A rendered CBZ is not necessarily published yet; wait for
publication completion before expecting it in a reader. An idle service emits
no periodic progress message. Enable detailed diagnostics with
`"logger": {"level": "DEBUG"}` inside `core`.

Each finished or failed source turn emits an INFO `ingest_metric` summary with
its status, work generation, selected/waiting/deferred galleries, source rows,
and logical bytes read. Adapter timings separate discovery, gallery indexing,
metadata parsing, reads, hashes, and image qualification. These timings are
inclusive: qualification can contain reads and hashes, so do not add them to
estimate total wall time.
Logical bytes include rereads and do not measure physical disk traffic. A killed
process can leave a turn without a terminal summary; absence is not zero cost.

Source progress at INFO states whether selection covers all complete galleries
or admits up to the explicit number of new galleries. Unbounded admission is not
reported as a zero-gallery quota.

Background inventory emits its own INFO `scope=source_monitor operation=inventory`
summary, including status, completed marker rows, logical read bytes, discovery,
read/hash work and total pass time including index reconciliation. These passes
can overlap foreground ingest; do not add their elapsed times to foreground wall
time or interpret a failed/interrupted pass as a completed inventory. The
`completion_marker_files_observed` and `completion_marker_bytes_observed`
counters identify completed observations of `galleryinfo.txt`, rather than
images. Read-call counters also include EOF calls.
Each inventory rereads the markers; the index uses their fingerprints to decide
which galleries need foreground work. It does not skip marker reads based only
on an unchanged filesystem timestamp.

From a development checkout, measure source reads and marker reuse with local,
disposable real-image fixtures:

```bash
.venv/bin/python scripts/probe-source-io.py --galleries 2 --pages 2 \
  --codec jpeg --workers 1 --output /tmp/source-io.json
.venv/bin/python scripts/probe-source-backlog.py --inventory 129 \
  --output /tmp/source-backlog.json
```

The source matrix compares a new inventory, unchanged markers, a changed policy,
and changed source bytes. Independent read counters check production telemetry.
The backlog probe intentionally keeps an eight-gallery quota over three real
publications, measuring how a fixed inventory affects repeated selection; it
also checks cleanup and a subsequent work claim. Neither probe measures physical
disk traffic or proves a NAS completion time. Run without concurrent builds or
benchmarks when comparing wall times. The synthetic controller sensitivity tool
`probe-publication-budget.py --output /tmp/publication-budget.json` separately
compares first publication, target misses and total catch-up under fixed/per-gallery
cost assumptions; its modeled times are not runtime measurements.

Successful batches also emit `publication` and `artifact_totals` summaries at
INFO. The latter aggregates all completed render calls in that batch; individual
artifact metrics remain available at DEBUG. Subtracting the aggregate
`render_archive.elapsed` from publication elapsed excludes the entire archive
renderer, including its input checks and final archive inspection, rather than
only compression and packing. `render_presentation` separately measures thumbnail
production. These are wall times for completed calls, not the sum of overlapping
page worker durations. `render_batches` includes source verification, decode,
resize and JPEG encoding; `archive_page_write` measures serial ZIP_STORED copying.
The existing `render_pages` is inclusive and may overlap worker execution.
Partial failed render calls have unknown remaining cost and must not be treated
as zero.

The INFO archive totals also distinguish worker source verification, native
decode/shrink, final resize, JPEG encoding, encoded-buffer copying/hashing, and
main-thread ZIP metadata writes and close. `worker_elapsed_sum` adds elapsed
worker durations and can exceed archive wall time. `worker_thread_cpu_sum`
excludes other libvips native threads. The worker decoder pipeline includes
scheduling/header work, decode/shrink and final resize; decoder input reads are
inclusive suboperations. Do not add these overlapping measurements or subtract
parallel JPEG worker durations from publication wall time. A compression-free
counterfactual requires a separate controlled experiment.

INFO `scope=adapter_io` summaries correlate with the publication generation and
report source opens, protection, layout checks, staging, journal
transactions, lock waits, fsync and rename. Snapshots are cumulative, emitted at
completed outer operation boundaries after 60 seconds and at completion or
failure; subtract consecutive snapshots when computing interval costs. Inclusive
wall time contains nested operations; exclusive wall time excludes them. Bytes
are actual logical transfers, not device traffic. An operation still in progress
is absent until it returns; the progress heartbeat identifies pending work.
Core's publication summary separately attributes source copy/rehash, archive and
presentation verification, and protection-boundary hashing. These Core and
adapter measurements describe overlapping layers and must not be added together.
Foreground source, publication, artifact totals and adapter I/O remain distinct
summaries; the background monitor is a concurrent measurement.

To exercise these boundaries with deterministic real JPEG files, public ingest,
independent archive/raster checks, cleanup and a subsequent work claim:

```bash
.venv/bin/python scripts/probe-artifact-io.py --galleries 32 --pages 64 \
  --edge 512 --workers 4 --isolated --timeout 900 \
  --output /tmp/artifact-io.json
```

Only pass `--isolated` when other benchmarks and builds are stopped. The report
records source hashes, raw INFO measurements and logical amplification; its local
wall time is not a NAS throughput prediction. A small fixture with
`--fsync-delay-ms 2 --fsync-delay-kind directory` or `file` injects a known delay
at the adapter boundary to check attribution. It still executes the actual
fsync and all publication checks; the artificial delay is not a device model.

Core records source actions, ingest permission checks and cleanup candidate
checks separately. Long-operation progress identifies a pending connector call;
completed SQL totals exclude that call until it returns. Publication completion,
cleanup `DONE`, and the next successful work claim are distinct milestones.
Compare all three when investigating a delay between batches.

Ingest periodically audits the database. A first start, unclean previous shutdown,
changed validator, or due audit requires a full check. A recent successful audit
and clean shutdown can allow a quick startup check. For an explicit full check:

```bash
python -m h2hdb check --config core.json
```

The default audit interval is the larger of seven days and 100 times the last
full audit's duration. Advanced deployments can change
`resident.database_audit_minimum_interval_seconds` and
`resident.database_audit_duration_multiplier`; audits run between work sessions.

Keep free space available in the library filesystem for one gallery's verified
source spool, page processing, a CBZ being written, and every prepared CBZ and
thumbnail awaiting publication. Database and disk-backed discovery/analysis plans
also require space. Removing the full-turn source-byte copy does not make total
scratch or pending output constant-sized. Disk-full or quota
errors keep work pending for retry instead of publishing incomplete files. Free
space or increase the quota, then let resident mode retry. Do not manually remove
private journal, staging, or coordination files to clear an error.

After an interruption, restart ingest with the same database and complete library.
It resumes pending publication and cleanup. Readers may remain unavailable while
an `ACTIVATING` marker or publication lock protects unfinished work. Unknown
files, changed bytes, and unexpected symlinks are preserved and reported for
inspection instead of being silently removed.

## Upgrade or move an existing installation

Upgrade ingest and H2HDB together within their declared dependency ranges.
Back up the database and the complete library before offline maintenance.
Existing CBZs and the format-v4 library journal do not need rebuilding for the
source observation checkpoint feature.

An exact H2HDB schema-version-7 database can use the core project's one-time
offline `upgrade-source-collection-schema.py` tool to reach schema version 8. Stop consumers
and follow the [core upgrade instructions](https://github.com/Kuan-Lun/h2hdb#readme).
For schema 6, first use Core 0.40.0's `upgrade-audit-schema.py` and its environment
to reach schema 7, then use the new converter.
Other older schemas require a new database and catalog rebuild from the source;
normal ingest startup does not convert them.

Legacy libraries containing `current/hash-v1`, `.h2hdb-state/coordination`, or
activation journals version 1, 2, or 3 are rejected. Keep their files intact and
rebuild into a fresh library paired with a fresh database. Preserve the original
download tree; the relocation command does not upgrade these old layouts.

To move a current-format library to another path or filesystem:

1. Stop ingest, readers, and every other process that could modify the library.
2. Move the entire library, including `.h2hdb-state` and
   `.h2hdb-coordination`. Keep the existing core database.
3. Update all reader and writer paths or mounts to the new location.
4. Run verification with the new library path visible to the command:

   ```bash
   h2hdb-ingest-relocate --library /new/location/library
   ```

5. Restart ingest and readers only after the command reports completion.

Relocation verifies managed files and retains the library identity, catalog,
and artifact contents. If interrupted, rerun the same command with the same
destination to resume. It preserves incomplete or ambiguous files and reports
the condition; it does not adopt or remove unrelated files. Copying only the
CBZ files into an unrelated library does not preserve the database binding.

## Troubleshooting

| Symptom | Action |
| --- | --- |
| `download_path is empty` | Check the source mount and configured path. |
| `must be a pre-existing real directory` | Create the required library directories and check that none is a symlink. |
| Database is not `READY` | For a new empty database, run the core administrator's `migrate`; for an existing database, inspect the reported version or audit failure before taking action. |
| An image is rejected | Read the gallery/file rejection in the logs, repair the source, then update `galleryinfo.txt`. |
| Storage-capacity error | Check free space and quotas on the reported filesystem; leave pending private state intact. |
| `library relocation is unfinished` | Keep services stopped and rerun relocation at the same destination. |
| Library identity changed | Check for a replaced mount or directory. For an intentional complete move, run relocation; do not pair the database with an unrelated root. |
| Unsupported legacy library | Rebuild into a new library and database from retained downloads. |

For background on the verification evidence and its limits, see
[Library reliability](verification/README.md). For a problem report, include the
package versions, database backend, command, and relevant error context in the
[issue tracker](https://github.com/Kuan-Lun/h2hdb-ingest/issues), with credentials
and private paths removed.

## License

GNU General Public License v3.0 only. See [LICENSE](LICENSE).
