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
data descriptors, duplicate names, and any other members are rejected. The
metadata may be at most 1 MiB after decompression; the writer and verifier allow
the corresponding worst-case DEFLATE size. The non-ZIP64 archive limit is
2,147,483,647 bytes.

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
    "page_render_workers": 2,
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
`page_render_workers` is a strict integer from 1 through 4. It bounds concurrent
image decoding/encoding while CBZ members are always serialized in canonical
page order; the default of 2 limits worst-case decoded-image memory on developer
machines.

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

The private corpus regression is opt-in through
`.local-test-data/hath-download` or `H2HDB_INGEST_TEST_DOWNLOAD_PATH`.

## License

GNU General Public License v3.0 only. See [LICENSE](LICENSE).
