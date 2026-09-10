# h2hdb-ingest

`h2hdb-ingest` watches a completed Hentai@Home download tree, publishes its
metadata to an H2HDB catalog, and optionally builds the CBZ, cover, thumbnail,
and page-location data used by Komga and OPDS readers.

This service writes data. Komga and `h2hdb-opds` only receive read-only views
of the finished library.

## What it produces

When `library_path` is enabled, each selected gallery has these resources:

- one acquisition CBZ named `h2h-<gid>.cbz`;
- page zero as the full-size cover, without a second cover copy;
- one standalone `thumbnail-320.jpg` derived from page zero;
- verified byte offsets for every page, so OPDS can serve a page without
  opening or decompressing the ZIP during the request.

Every eligible page becomes a deterministic JPEG. Eligible filenames use an
ASCII case-insensitive `.avif`, `.bmp`, `.gif`, `.jpeg`, `.jpg`, `.png`, or
`.webp` suffix. Other regular files remain source observations but are not
pages; they are never opened by the artifact renderer. Animated GIF input uses
its first frame. Sources are streamed through libvips and are not rejected for
pixel count, dimensions, or encoded file size. Source data is read in bounded
chunks and spooled to disk above 4 MiB; larger files require temporary disk space,
not an equally large Python byte buffer. Truncated or undecodable images still
fail the artifact. Generated JPEG pages remain limited to 32 MiB, and a gallery
may contain at most 4096 pages. The generated page fits within the configured
short-side limit (768 pixels by default), an 8192-pixel long side, and
40 megapixels, preserving aspect ratio without enlarging images.

Source reduction uses libvips's sequential thumbnail pipeline, including
JPEG shrink-on-load where available, to produce an intermediate no larger than
twice the target dimensions and 40 megapixels. Pillow then applies the configured
resampler for the final resize when needed. The default is LANCZOS, page JPEG
quality 90, thumbnail JPEG quality 85, and optimized encoding. The separate
thumbnail has a maximum side of 320 pixels. Decoder, native library versions,
and both resizing stages participate in the artifact policy fingerprint.

Progressive JPEG and interlaced PNG still require codec-owned full-image
buffers. They run one at a time, exclusively with respect to other source
pixel decoding. Regular images retain the configured page-worker concurrency;
libvips uses one native worker per page and does not cache past operations.
This lowers memory pressure without claiming a hard process memory ceiling or
rejecting large legitimate images. The manual image-memory benchmark in
`scripts/benchmark-source-images.py` generates real 100-megapixel fixtures and
reports per-process peak RSS for all four formats.

When CBZ generation is enabled, each new or changed gallery passes a source
qualification step before global analysis and deduplication. It evaluates every
PAGE with the same decoder and the configured bounded page-worker pool. A
rejected image excludes the entire gallery from artifact selection; no page is
silently omitted from a book. The service preserves the source facts and the
precise rejection reason, then retries qualification when the source marker or
artifact policy changes. Only qualified galleries participate in analysis.
Metadata-only operation does not decode images.

The canonical CBZ contains only:

```text
galleryinfo.txt
pages/0000.jpg
pages/0001.jpg
...
```

`galleryinfo.txt` uses DEFLATE. Page members use `ZIP_STORED`, which makes their
verified byte ranges directly readable. ZIP comments, ZIP64, extra fields,
data descriptors, duplicate names, and any other members are rejected. Source
`galleryinfo.txt` metadata must be 1 byte through 1 MiB even when artifact
generation is disabled; this bounds parsing and cancellation latency. The
writer and verifier allow the corresponding worst-case DEFLATE size. The
non-ZIP64 archive limit is 2,147,483,647 bytes.

A gallery with no eligible pages still has a valid metadata-only acquisition
containing `galleryinfo.txt`. Its presentation page list is empty and it has no
cover extent or thumbnail resource.

## Important upgrade notice

This release requires the H2HDB `0.36` compatibility lane and schema epoch 3,
schema version 6. Verified completion markers are stored with immutable source
observations so unchanged galleries can reuse those observations across restarts.
Image qualifications are sealed with the exact source observation and render policy;
rejected galleries stay in source membership while only accepted galleries enter
analysis and publication. Changing a completion marker or render policy rechecks
qualification. Known malformed images no longer abort publication of every other gallery.

Schema-version-5 and older databases are rejected. Initialize a new empty database with
the H2HDB administrator command, then rebuild the catalog from the source
download tree. There is no in-place schema migration or older-core fallback.

Presentation storage v2 is intentionally not compatible with the old library
layout. There is no in-place migration or compatibility fallback.

Startup rejects these known legacy states without deleting them:

- `current/hash-v1`;
- `.h2hdb-state/coordination`;
- a version-1, version-2, or version-3 activation journal.

Rebuild artifacts into a fresh library root. This prevents old and new paths
from silently coexisting under one reader mount.

## Prepare the library directories

Before starting ingest, create these four real directories on the same
filesystem:

```text
library/
├── current/
│   ├── acquisitions/
│   └── artwork/
└── .h2hdb-coordination/
```

Do not pre-create `.h2hdb-state`; ingest creates and owns it. After operation,
the private version-4 journal contains one immutable UUIDv4 for this library
root. Startup stores the same UUID in h2hdb before any cleanup or ingest work;
an exact restart is idempotent, while pairing the database with another root
fails closed. Within one process, the filesystem adapter pins both that UUID
and the root directory's device/inode identity. It rechecks the pair at managed
operation boundaries, and the resident checks again after maintenance before
claiming work. A replacement observed at one of those boundaries is a fatal
`LibraryStorageIdentityMismatchError`; when the replacement is already present
at the guard, the adapter does not create a private layout there. This is not a
continuously held mount lock and does not claim to stop an external actor
swapping the path between one passed guard and its immediately following POSIX
syscall. Only format-v4 journals are accepted; there is no automatic database
rebind. Moving the complete existing library preserves its UUID; the explicit
relocation tool revalidates files before adopting their new filesystem
identities. A new unrelated storage UUID still requires a fresh database and
rebuild.

After operation,
the complete layout is:

```text
library/
├── current/
│   ├── acquisitions/
│   │   └── hash-v2/<2 hex>/<1 hex>/h2h-<gid>.cbz
│   └── artwork/
│       └── hash-v2/<2 hex>/<1 hex>/h2h-<gid>/thumbnail-320.jpg
├── .h2hdb-coordination/
│   ├── publication.lock
│   └── ACTIVATING                 # unfinished publication or relocation
└── .h2hdb-state/                  # ingest-private; never mount into a reader
    ├── staging/
    ├── quarantine/
    ├── journal/
    └── locks/
```

The shard is deterministic but deliberately opaque to H2HDB core. Its digest
is derived from the GID by the ingest-owned `managed-filesystem-v2` codec.

## Mount the right subtree

The reader mounts are deliberately different:

| Service | Mount source | Access |
| --- | --- | --- |
| `h2hdb-ingest` | the whole `library/` parent | read-write |
| Komga | `library/current/acquisitions/` | read-only |
| `h2hdb-opds` | `library/current/` | read-only |
| `h2hdb-opds` | `library/.h2hdb-coordination/` | read-only |

Do not mount all of `current/` into Komga. The `artwork/` subtree contains
standalone JPEG thumbnails and is not a Komga comic library. OPDS needs all of
`current/` because it serves both acquisitions and artwork.

The library parent, `.h2hdb-state`, and `.h2hdb-coordination` are ingest-owned
single-writer namespaces. No other process may modify them, even if it uses the
same operating-system account.

## Configuration

A minimal SQLite configuration with artifacts enabled is:

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
    "max_image_short_side": 768,
    "render_policy": {
      "page_jpeg_quality": 90,
      "thumbnail_jpeg_quality": 85,
      "optimize": true,
      "resampler": "lanczos"
    }
  },
  "resident": {
    "publication_batch_galleries": 1000,
    "progress_log_interval_seconds": 3600,
    "source_quiet_seconds": 300,
    "source_max_wait_seconds": 1800,
    "source_probe_interval_seconds": 30,
    "poll_seconds": 5,
    "lease_seconds": 300,
    "heartbeat_seconds": 60,
    "max_rows": 128
  }
}
```

Set `library_path` to `null` to publish catalog metadata without producing
artifacts. `download_path` must already be a nonempty directory. The download
and library roots must be distinct and must not contain one another.

`publication_batch_galleries` is a strict integer from 1 through 1,000,000,
defaulting to 1,000. Each synchronization admits at most that many galleries
that are absent from the currently published source membership, then completes
analysis, CBZ creation, library activation, and catalog publication for the
cumulative known collection. Already known galleries remain in the collection;
their changes and confirmed deletions are applied in the same synchronization
and do not consume this quota. A batch is therefore not a fixed limit on its
total work or its number of resulting books.

Every batch takes a fresh complete inventory of gallery completion markers.
New folders, including names preceding an earlier scan position, are eligible
in subsequent batches. Missing folders are removed only after this complete
inventory proves their absence. Incomplete or changing observations fail the
attempt rather than treating an interrupted scan as evidence of deletion.
Unchanged markers reuse verified source observations without rereading image
bytes. Metadata discovery, analysis, catalog indexes, and library reconciliation
still contribute work; the first batch must finish the full metadata inventory.

After a batch publishes, the resident immediately continues any deferred new
galleries, with bounded maintenance and a fresh ingest claim between batches.
It does not wait for another source change or the quiet period. Each published
batch is complete and downloadable through the current catalog while later
batches are prepared. OPDS serves only that current publication; activation
retains its existing publication fence. New evidence can change deduplication
or spam decisions, so later batches can replace or remove earlier CBZs and
catalog entries. The known source collection grows progressively, but the number
of published books need not increase on every batch.

While work is active, INFO summaries name the current work in plain English,
show completed items and the total when known, and report changes since the
previous summary. The current operation's elapsed time is separate from the
whole work's elapsed time. Unknown totals and unavailable completion counts are
explicit; a quiet counter does not by itself establish that work is stuck.
For example, an hourly summary can say:

```text
Ingest progress: Copying the gallery inventory into the batch plan; 4,096 / 131,256 galleries completed; since previous report (1h 0m 0s): +4,096 galleries completed; last measured advance 30m 0s ago; current operation elapsed 2h 0m 0s; work elapsed 2h 1m 19s; batch limit 10 new galleries; CBZs rendered this work 0; catalog publication pending
```

The batch limit applies to newly included galleries. A complete inventory still
precedes selection; the inventory total can therefore greatly exceed that limit.
The selected batch includes previously known galleries as well as new ones.
CBZs rendered during this work and catalog batches published are separate results:
rendering a CBZ does not mean readers can already acquire it. Metadata-only work
does not show a CBZ count.

`resident.progress_log_interval_seconds` defaults to 3,600 seconds and must be
finite and positive. A dedicated thread reads an in-memory snapshot; it never
polls the catalog, inspects files, or acquires the ingest session lock. Phase
transitions and work completion are reported immediately. Operation changes only
update the next summary; they do not emit a log for every gallery or page. Nested
preparation restores its enclosing operation when it returns or fails, so the
summary does not keep reporting a completed discovery operation. The interval
only controls periodic progress summaries and repeated maintenance-failure
diagnostics. It does not emit a completion record for every image or gallery.

An idle resident with no pending work emits no periodic progress record.
Pending scans, lease waits and cleanup retain their original work timer across
polls. Startup validation and blocked preparation or library-lock calls remain
observable while the reporting thread can run. Summaries describe observations,
not a wall-clock timeout or a promise that a blocked operation will finish.

Normal INFO output contains startup details, phase boundaries, publication-batch
results and periodic progress. Detailed `ingest_metric` records (including archive,
thumbnail and publication timings), session receipts and ordinary source-change
retries are DEBUG diagnostics. The optional `build_runtime(event_logger=...)`
callback receives human progress/events; metrics use the
`h2hdb_ingest.metrics` logger independently.

At normal verbosity, `pyvips` and `mysql.connector` retain WARNING and higher
severity, respecting stricter ERROR/CRITICAL settings. Setting
`core.logger.level` to `DEBUG` enables native VIPS processing details and ingest
metrics. Python-wrapper pyvips DEBUG tracing remains disabled: formatting an
image can recursively log and deadlock parallel console/file handlers. Native
VIPS details are emitted at INFO by that dependency and are enabled only in the
application's DEBUG mode. Explicit `configure_logging` replaces and closes the
previous process handlers, so reconfiguration neither ignores settings nor
duplicates console/file output.

WARNING, ERROR and CRITICAL output includes the logger/thread, configured source
and library roots, and the SQLite path or MariaDB host/port/database. These are
resource locations, not a claim that every failure concerns all listed resources;
database usernames and passwords are never added to this context. CLI failures
also identify the configuration file while retaining their original exit behavior.

Image decoder diagnostics include an operation, GID, source filename and position.
Qualification also identifies the complete gallery folder; archive rendering
includes the source SHA-256 because its public input is an immutable source spool,
which does not carry the original gallery folder. `source_attribution=exact`
identifies the current Python page worker. A libvips background thread may have no
Python worker context: `source_attribution=active_candidates` then lists active
source candidates up to the page-worker limit (currently 16), plus the count
omitted, without claiming any candidate is the confirmed source. This covers an
entire supported worker batch. Unknown native diagnostics outside active image
work retain their original message with the configured process locations.

For example, `unknown EXIF resolution unit` remains a warning and does not by
itself exclude a gallery or prevent CBZ creation. Actual image rejection still
uses `gallery_image_rejected` with `action=exclude_gallery_from_publication`.
Diagnostic fields escape control characters and bound long text. Human log text
gains fields; API, configuration, retry/qualification rules, rendering policy and
CBZ bytes are unchanged. Consumers that compare entire log lines must accommodate
the additional fields.

Storage-capacity diagnostics retain the operation, capacity error type/code,
exception filenames when available, and the original wrapper reason. Scratch free
space describes only the scratch filesystem; it does not prove which volume is
full. The source monitor also identifies its owned working directory and marker
index when available. Cleanup failures put the operation and cause on the first
line as well as retaining the traceback; metric delivery failures identify the
metric scope and operation.

Repeated failures of each maintenance operation or metric delivery are summarized
using bounded in-memory state: the first failure and a changed error retain their
traceback, identical failures are counted until the configured interval, and one
INFO record reports recovery. Metric recovery requires the same failing callback
to succeed; switching to another failing callback starts a new diagnostic sequence.
Only the latest failed metric callback is retained, without a growing registry.
This changes diagnostics only; it does not reduce maintenance retries or replay
lost metrics. Fatal image-check diagnostics are
attached to the original exception and reported by the resident, allowing
storage-pressure retries to coalesce without losing gallery/file context.
Distinct rejected galleries retain their WARNING with the exact folder, file and
reason; their unchanged qualification results are reused normally.

The detailed `ingest_progress event=... generation=... counter...` records are
emitted at DEBUG. Counters belong to one process-local work generation and are
not recovery authority. Page workers increment `pages_rendered` only after
successful render and source verification; ordered ZIP writes update
`pages_written` separately. `archives_rendered` does not mean the archive has
been published. `publication_batches_finalized` advances only after successful
finalization. Source receipt counts describe the sealed cumulative source;
`*_operation_rows` count acknowledged non-replayed database operation rows, not
distinct galleries. Retries can perform additional work; restarts create fresh
observation counters. Late worker updates cannot change a newer work generation.
An ended stage describes a transition; only the final work status describes
whether that work completed, failed or is being retried.

The resident reconciles the source immediately on startup. After that, a source
change schedules synchronization after `source_quiet_seconds` without another
observed change, or after `source_max_wait_seconds` from the first observed
change, whichever comes first. Monitoring continues during synchronization.
Changes observed during that work remain pending: the next maximum wait starts
at completion, while an already elapsed quiet period allows an immediate retry.
An unchanged source does not trigger periodic full synchronization. The former
`periodic_scan_seconds` option is rejected.

Each monitor pass discovers gallery folders and reads and hashes only their
`galleryinfo.txt` completion markers. It compares content, size, device, inode,
mtime and ctime, so rewriting identical metadata with a changed stat still
signals completion. Writers must finish gallery changes before writing the
completion marker. Completed galleries are terminal discovery leaves; nested
collection directories are supported, but galleries below another completed
gallery are not discovered. The monitor does not enumerate a completed
gallery's image entries. Its comparison index spills to disposable local disk
and uses no core database connection.

`source_probe_interval_seconds` is the pause between completed metadata passes;
filesystem latency also contributes to detection delay. The quiet and maximum
waits use observed changes, not an unavailable writer timestamp. Transient
source changes during observation retain a retry after the quiet period.
Empty markers and metadata missing required fields are treated as incomplete
writes, including on startup: they are never published and retry after the
quiet period. Marker probes only hash bytes; full observation still rejects
invalid UTF-8, invalid dates, unsafe paths, and metadata larger than 1 MiB.
Downloader handoffs and bounded cleanup remain eligible between source scans.

JPEG qualities are strict integers from 0 through 95. Supported resamplers are
`nearest`, `box`, `bilinear`, `hamming`, `bicubic`, and `lanczos`. An explicit
`"preset": "benchmark-low-cost"` selects quality 70, unoptimized encoding, and
the bilinear resampler for local performance experiments; it never changes the
default, and any fields supplied beside the preset override its values.
`page_render_workers` may be omitted (or set to `null`) to choose a bounded
process-cached default, or set to a strict integer from 1 through 16 to override
that default exactly. CBZ members are always serialized in canonical page order,
so worker selection does not change archive bytes or member order. On macOS the
automatic policy reads `hw.perflevel0.physicalcpu` once through the fixed
`/usr/sbin/sysctl` executable and uses only that highest-performance physical-core
count. A native Intel process may fall back to `hw.physicalcpu` only after
`sysctl.proc_translated` confirms it is not running through Rosetta; following
Apple's contract, a missing translation OID also means native, while any other
invocation failure remains unknown and falls back to one. Translated, Apple
Silicon, and unknown Darwin processes likewise fall back to one worker if
performance-core authority is missing, malformed, or unavailable. They never
reinterpret logical or total CPU counts as performance cores. Other platforms
use the process CPU availability, then the host CPU count, and finally one.
Every detected value is capped at 16, and the host topology is probed at most
once per process (concurrent first calls share one probe, and a forked child
probes again) rather than per render request.

When a CBZ-enabled runtime is built, the service logs the worker decision
exactly once as one structured `page_render_workers` line, for example:

```text
page_render_workers mode=auto configured=none selected=10 detected=10 hard_cap=16 platform=darwin machine=arm64 process_cpu_count=14 cpu_count=14 darwin_performance_cores=10 darwin_physical_cores=14 darwin_translation=native reason=darwin-performance-cores
```

`mode` is `auto` or `manual`; a manual override keeps its exact configured
value and reports `reason=manual-override` next to the same host facts.
`detected` is the raw authority before the hard cap and `none` for a manual
override or a conservative fallback. `platform` and `machine` come from Python,
`process_cpu_count` and `cpu_count` from `os.process_cpu_count()` and
`os.cpu_count()`, and the three `darwin_*` fields are `none`/`not-probed` on
every non-Darwin process because a Linux container cannot observe the macOS
host's performance and efficiency cores. `darwin_translation` is `native`,
`translated` (Rosetta), or `unknown` when the probe failed. The closed
`reason` set names the authority that was selected or the fallback that forced
one worker (`darwin-performance-cores`,
`darwin-intel-native-physical-cores`, `darwin-intel-translated-fallback`,
`darwin-intel-translation-unknown-fallback`,
`darwin-intel-physical-cores-unavailable-fallback`,
`darwin-performance-cores-unavailable-fallback`, `process-cpu-count`,
`cpu-count`, `cpu-count-unavailable-fallback`). The line never contains a
path, gallery metadata, or other private data, the host probe runs once per
process, and no per-gallery or per-page record repeats the decision. A runtime
whose `library_path` is `null` renders nothing and therefore logs no decision.

Docker Desktop runs the service inside a Linux guest, so a container on a macOS
host cannot query the host's Darwin performance-level sysctls. Automatic
selection there uses only the process/container-visible Linux vCPU count and
cannot infer which host CPUs are performance or efficiency cores. On a measured
M4 Pro host with 10 performance cores, requiring that measured count means setting
`"page_render_workers": 10` explicitly; the override remains subject to the
hard cap but is not adjusted to the container's visible CPU count.

The macOS source choice follows Apple's
[processor performance-level guidance](https://developer.apple.com/documentation/kernel/1387446-sysctlbyname/determining_system_capabilities)
and uses Apple's documented
[`sysctl.proc_translated` Rosetta signal](https://developer.apple.com/documentation/apple-silicon/about-the-rosetta-translation-environment)
to avoid treating an emulated `x86_64` process as native Intel hardware.

Worker count is a concurrency limit, not memory admission control. A single
40-megapixel RGBA buffer is about 153 MiB, but that is not a complete per-worker
upper bound: decoded input, copied or resized images, color conversion or alpha
composition, JPEG encoding, and allocator overhead can coexist. A 2 GiB tmpfs
limits temporary-file capacity; it is neither a process RSS cap nor reserved
memory, and its pages can add to container or virtual-machine memory pressure.
Memory-constrained installations should set an explicit value such as 2 or 4
based on measurements; the implementation does not claim a fixed RSS upper
bound.

Configuration rejects unknown fields. A complete string value such as
`"${H2HDB_RW_DB_PASSWORD}"` is replaced from the environment before validation;
a missing variable fails startup.

## Disk scratch and interrupted work

CBZ-enabled command-line runs automatically use
`<library_path>/.h2hdb-state/scratch-v1/run-<id>/data` for Python and native
working files. With the Compose library mount, this is on the comics disk,
not the container's `/tmp` tmpfs. No extra configuration field is required.
The process temporarily sets `TMPDIR` and Python's temporary-directory default;
worker threads use the same private workspace. Embedded callers can wrap their
runtime in `DiskScratch` from `h2hdb_ingest.scratch` and pass
`scratch.cleanup_page` as `build_runtime(..., temporary_cleanup=...)`.

Each workspace has a verified ownership record and an advisory lease. Startup,
each resident maintenance cycle, and shutdown perform bounded cleanup of abandoned
owned workspaces. Startup completes one scan in bounded pages before allocating new
working storage. Live owners, unknown directories, symlinks and foreign hard
links are preserved. Anonymous temporary files disappear when their handles
close, including process termination; remaining owned directories are recovered
after a crash. Cleanup never scans the published tree for arbitrary `.tmp` files.

The renderer writes and verifies a CBZ directly in one unpublished scratch stream.
Its destination must support read, write and seek; failures discard partial bytes
instead of preserving prior destination contents. The final protected staging
and atomic publication path continue to publish only complete, verified files.
The complete verified source snapshot still occupies disk space while rendering,
and completed output has its existing page and non-ZIP64 bounds. Sources have no
64 MiB per-file or 4 GiB per-gallery policy limit.

Storage exhaustion or quota errors preserve the gallery's eligibility and pending
work. Resident processing waits for the normal poll cadence and retries, logging
the scratch directory and observed free bytes; it does not publish a partial CBZ
or mark a valid image as rejected. One-shot commands return an unsuccessful result
when storage pressure prevents publication. Disk capacity is still finite: allow
room for the verified source snapshot, one output CBZ, concurrent page spools and
persistent publication staging.

Custom source-monitor probes now return a context-managed iterator. The monitor
opens, consumes and closes each probe in its own worker thread, including stop
and error paths; callers must update old bare-generator probes.

## Run the service

H2HDB core schema creation is a separate administrator action. Normal ingest
startup checks that the database already has a READY schema epoch; it never
creates or migrates the core schema.

Run one coordinated scan:

```bash
h2hdb-ingest --config /config/h2hdb-ingest.json --once
```

Run the resident service:

```bash
h2hdb-ingest --config /config/h2hdb-ingest.json
```

Python embedders should use `with build_runtime(config) as runtime:` or call
`runtime.close()` explicitly. Close is idempotent, releases the core ingest
facade's process-local caches, and makes later context entry or ingest-facade
operations fail closed. Both command-line entry points close the runtime after
normal resident/one-shot completion and while unwinding exceptions,
`KeyboardInterrupt`, or `SystemExit`. A failure after `build_runtime` has acquired
the facade also closes that partial ownership before propagating the error.

The equivalent module command is:

```bash
python -m h2hdb_ingest --config /config/h2hdb-ingest.json
```

For the first nonempty publication in a fresh, already initialized catalog:

```bash
h2hdb-ingest-bootstrap --config /config/h2hdb-ingest.json
```

Bootstrap refuses an empty source or a catalog that already has a published
revision. It advances publication batches until the first nonempty catalog,
continuing past an empty batch when new galleries remain deferred. It fails if
the final batch is empty or another publisher changes its catalog head between
batches. Normal resident mode continues the remaining collection afterward.

## Crash and restart behavior

To move an existing library, stop ingest and readers, move the entire library
including `.h2hdb-state` and `.h2hdb-coordination`, and update every reader and
writer mount consistently. Keep the existing core database and library files.
This is offline maintenance: keep ingest, readers, and every process that could
modify the library stopped until verification completes. Run the maintenance
command from an installed release with the new library root mounted:

```bash
h2hdb-ingest-relocate --library /hentai/library
```

The command accepts only the current format-v4 journal. It retains the library
UUID, catalog publication receipts, resource paths and artifact contents. Its
independent durable relocation session blocks normal ingest until verification
finishes. SHA-256 and size are verified before file identities are updated;
source galleries are not reprocessed and the core database is not rebuilt.
Batches contain at most 128 logical resources. A stopped or failed run is
resumed with the same command and the same destination. Incomplete or ambiguous
files are preserved and reported rather than accepted as complete artifacts.
Verification covers journal-managed resources and their
authorized staging/quarantine names; unreferenced files are not adopted or
removed. A normal startup also refuses a different root identity after a
completed relocation, before beginning source processing.

The tool holds the publication and state locks during each step and durably
fences readers with `ACTIVATING` before updating artifact identities. It retains
an existing publication marker unchanged. When the relocation session created
its own marker, an interrupted write can resume only if the retained bytes are
an exact prefix of that session's expected marker payload; a foreign prefix is
preserved and rejected. This control-file recovery does not treat incomplete
artifact bytes as a completed CBZ or thumbnail. The original publication marker
is retained at completion; a marker created only for relocation is removed.

Restart ingest and readers only after the command reports completion. An
unfinished catalog publication resumes its original receipt after relocation;
the maintenance operation does not replace its publication phase or cursor.

Ingest first writes complete candidates into private staging and verifies their
size and SHA-256. It activates acquisitions and thumbnails in bounded pages of
at most 128 resources while holding the publication fence. Files move into
`current/` with same-filesystem, no-replace renames; they are never copied or
hard-linked into a second persistent tree.

The H2HDB reader head advances only after the library journal reaches `READY`.
An interrupted rename, journal update, or marker update is replayed from exact
digest and filesystem identity evidence on restart. Unknown files, symlinks,
changed bytes, or ambiguous inode identities fail closed and are preserved for
operator inspection.

`SIGINT` and `SIGTERM` stop between bounded durable steps. A forced kill may
leave `ACTIVATING`, private staged bytes, or quarantine bytes; restart resumes
the same receipt before readers are allowed through the shared fence.

Every claimed synchronization resolves its configured policy first, then,
before constructing or reading the filesystem source, finishes the sole
durable `DB_COMMITTED` publication (if any) and revalidates that the published
head's library activation is `COMPLETE`. Recovery uses the staged bytes and
durable receipt from the interrupted turn; it does not require the old source
files to remain present and does not bind the new ingest generation to that old
snapshot. The same synchronization then observes the current source and runs
its cumulative admitted batch through the complete requested policy tuple, including source-manifest,
analysis, artifact/render, display-title, operational, and artifact-required
choices. A successful synchronization means that batch is fully published and
finalized; its result separately reports the number of deferred new galleries.
There is no deferred policy application within the published batch. A stop or
adapter error before finalization propagates without
reporting synchronization success, and the next claim resumes from the durable
boundary.

If a process stops before the database commit but after protecting artifact
bytes, a later policy takeover can leave an unpublished predecessor in private
staging. The resident passes the same filesystem adapter to core maintenance,
which terminally tombstones and removes those exact predecessor resources in
single-resource attempts. If a successor first encounters the same staging
destination, that collision is bounded backpressure: the resident completes
the current session, performs the release under the exclusive maintenance
gate, and resumes the durable successor on the next poll. Reader-visible
`current/` bytes are not part of orphan release. Resources removed or replaced
by a successfully published successor are still deleted by the normal exact
activation reconciliation.

## Common startup failures

- **`download_path is empty`**: check that the download volume is mounted.
- **`must be a pre-existing real directory`**: create the required library
  directories before starting the container; symlinks are not accepted.
- **`unsupported legacy ... fresh library root`**: older journals and the old
  artifact layout require a fresh v4 library. Both normal ingest and the
  relocation command reject journals outside the exact current format.
- **`library relocation is unfinished`**: keep the original library and rerun
  the relocation command against the same destination to resume verification.
- **`library ... changed identity`**: another process modified a managed path;
  stop all writers and inspect the mount before retrying. After an intentional
  complete-library move, use the relocation command to validate the new files.
- **database is not READY**: use the H2HDB administrator command to initialize
  a new empty database or resume its matching interrupted initialization.
  An older or drifted schema requires a fresh database and catalog rebuild.

## Development

The project requires Python 3.14 and uses a repository-local environment:

```bash
./scripts/rebuild-env.sh
./scripts/check-fast.sh
./scripts/check-full.sh
```

The merge pytest runner owns the complete test process tree under one absolute
300-second deadline. POSIX uses a new process group. Windows uses a start-gated,
kill-on-close Job Object so a normally exiting pytest parent cannot leave an
unobserved worker behind; a surviving child fails the gate even after cleanup.
The repository's `windows-latest` target separately exercises real Job Object,
console-break, forced-parent-exit, timeout, and venv-launch behavior without
running the application or a live database.

An explicit integration dependency can be supplied without relying on a
sibling checkout:

```bash
./scripts/rebuild-env.sh --source h2hdb=/tmp/h2hdb.whl
```

SQLite integration tests run by default. With Docker available, enable the
pinned MariaDB 10.11.11 case explicitly:

```bash
H2HDB_TEST_MARIADB=1 .venv/bin/pytest tests/test_runtime_e2e.py
```

Private corpus regressions are excluded from `check-full` and require an
explicit marker. They read `.local-test-data/hath-download` by default; set
`H2HDB_INGEST_TEST_DOWNLOAD_PATH` to select another source:

```bash
H2HDB_INGEST_TEST_PRIVATE_CORPUS=1 \
H2HDB_TEST_MARIADB=1 \
.venv/bin/pytest tests/test_local_download_corpus.py
```

### Installed distribution pipeline smoke

`scripts/check-full.sh` builds the candidate ingest wheel and executes
`scripts/smoke-installed-pipeline.py` with Python isolated mode, in addition to
CLI/import checks. The smoke uses temporary SQLite and gallery/library roots,
two valid galleries (including a source longer than 8192 pixels), one corrupt
gallery, cross-namespace equal tag values, real CBZ/thumbnail bytes, runtime
restart, source repair, and a newer publication revision.

Ingest must load from its installed wheel in the smoke environment. The default
local gate explicitly shares the development environment's installed dependency
wheels; `--allow-external-core-wheel` permits core's wheel at that reported path,
but rejects editable or source-checkout imports. The printed origin records are
part of the evidence; this is not a claim that all dependencies were freshly
installed in the smoke environment.

For a fully isolated environment containing candidate core and ingest wheels:

```sh
/path/to/smoke-venv/bin/python -I scripts/smoke-installed-pipeline.py
```

With a compatible installed `h2hdb-opds` wheel and `httpx`, add `--opds` to check
the real OPDS feed, complete CBZ downloads, byte ranges, and thumbnails through
in-process ASGI. The probe opens no network socket. It reports incompatible
package constraints and exits unsuccessfully even if diagnostic HTTP requests
succeed; no dependency overrides or third-repository changes are performed.

## License

GNU General Public License v3.0 only. See [LICENSE](LICENSE).
