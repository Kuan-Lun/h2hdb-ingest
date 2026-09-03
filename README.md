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
its first frame. A source is rejected if it is
truncated, cannot be decoded, is larger than 40 megapixels, has a side longer
than 8192 pixels, or if its source or rendered JPEG exceeds 32 MiB. A gallery
may contain at most 4096 pages. The configurable short-side limit defaults to
768 pixels; images are never enlarged. The canonical render policy defaults to
page JPEG quality 90, thumbnail JPEG quality 85, optimized encoding, and the
LANCZOS resampler. The separate thumbnail has a maximum side of 320 pixels.

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

Presentation storage v2 is intentionally not compatible with the old library
layout. There is no in-place migration or compatibility fallback.

Startup rejects these known legacy states without deleting them:

- `current/hash-v1`;
- `.h2hdb-state/coordination`;
- a version-1 activation journal.

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
│   └── ACTIVATING                 # present only during an unfinished cutover
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
    "periodic_scan_seconds": 1800,
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
revision.

## Crash and restart behavior

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
it through the complete requested policy tuple, including source-manifest,
analysis, artifact/render, display-title, operational, and artifact-required
choices. A successful synchronization therefore means that policy is fully
published and finalized; there is no hidden deferred-policy success or operator
retry step. A stop or adapter error before that point propagates without
reporting synchronization success, and the next claim resumes from the durable
boundary.

## Common startup failures

- **`download_path is empty`**: check that the download volume is mounted.
- **`must be a pre-existing real directory`**: create the required library
  directories before starting the container; symlinks are not accepted.
- **`unsupported legacy ... fresh library root`**: keep the old tree as a
  backup and configure an empty v2 root for a full artifact rebuild.
- **`library ... changed identity`**: another process modified a managed path;
  stop all writers and inspect the mount before retrying.
- **database is not READY**: initialize or repair the schema with the H2HDB
  administrator command, not with ingest.

## Development

The project requires Python 3.14 and uses a repository-local environment:

```bash
./scripts/rebuild-env.sh
./scripts/check-fast.sh
./scripts/check-full.sh
```

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

## License

GNU General Public License v3.0 only. See [LICENSE](LICENSE).
