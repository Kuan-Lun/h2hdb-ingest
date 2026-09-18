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
  `h2hdb>=0.39.2,<0.40.0` and schema epoch 3, version 7.
- For CBZ output, a separate writable library directory and enough disk space
  for source snapshots, image processing, and publication staging.

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
| `resident.publication_batch_galleries` | `1000` | Admit 1–1,000,000 previously unknown galleries per publication batch. |
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

A one-shot run is not a promise to import every new gallery: the admission limit
still applies. Use resident mode to continue processing the remaining collection.
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

The first run inventories the source collection. Each publication batch admits
up to the configured number of new galleries while applying changes and
confirmed deletions to previously known galleries. The limit is not a cap on
the total inventory, processing time, or number of published books. Completed
batches are available to readers while ingest prepares later batches.

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
logical bytes read and snapshot bytes. Adapter timings separate discovery,
gallery indexing, metadata parsing, reads, hashes, image qualification and
snapshot capture. These timings are inclusive: qualification and snapshot capture
can contain reads and hashes, so do not add them to estimate total wall time.
Logical bytes include rereads and do not measure physical disk traffic. A killed
process can leave a turn without a terminal summary; absence is not zero cost.

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

Keep free space available in the library filesystem for temporary source copies,
page processing, a CBZ being written, and publication staging. Disk-full or quota
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
current database audit-scheduling feature.

An exact H2HDB schema-version-6 database can use the core project's one-time
offline `upgrade-audit-schema.py` tool to reach schema version 7. Stop consumers
and follow the [core upgrade instructions](https://github.com/Kuan-Lun/h2hdb#readme).
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
