"""Publish the first catalog revision for an already initialized vNext epoch."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from h2hdb import CatalogRevisionNotFoundError

from .config import load_config
from .runtime import build_runtime, configure_logging


class _AlreadyPublished(RuntimeError):
    pass


def main(arguments: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Publish the first nonempty H2HDB vNext catalog revision"
    )
    parser.add_argument("--config", type=Path, required=True)
    parsed = parser.parse_args(arguments)

    config = load_config(parsed.config)
    config.ensure_paths()
    if next(config.paths.download_path.rglob("galleryinfo.txt"), None) is None:
        parser.exit(
            2,
            "No galleryinfo.txt was found below download_path; refusing to "
            "publish an empty initial catalog.\n",
        )
    configure_logging(config)
    runtime = build_runtime(config)
    runtime.resident.initialize()

    def require_unpublished_catalog() -> None:
        try:
            current = runtime.catalog.get_catalog_revision()
        except CatalogRevisionNotFoundError as error:
            if error.revision == 0:
                return
            raise
        raise _AlreadyPublished(str(current.revision))

    try:
        processed = runtime.resident.process_available(
            periodic_scan=True,
            preflight=require_unpublished_catalog,
        )
    except _AlreadyPublished as error:
        parser.exit(
            2,
            "Initial catalog reconciliation has already run: "
            f"current_revision={error}.\n",
        )
    if not processed:
        parser.exit(2, "No gallery ingest lease is currently available.\n")

    published = runtime.catalog.get_catalog_revision()
    if published.revision <= 0 or published.publication_count <= 0:
        parser.exit(
            1,
            "Initial reconciliation did not publish a non-empty catalog.\n",
        )
    print(
        "Initial catalog reconciliation completed: "
        f"revision={published.revision} "
        f"publications={published.publication_count}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
