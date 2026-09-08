"""Publish the first catalog revision for an already initialized vNext epoch."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from h2hdb import CatalogRevision, CatalogRevisionNotFoundError

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
    with build_runtime(config) as runtime:
        runtime.resident.initialize()
        expected: CatalogRevision | None = None
        captured: list[CatalogRevision] = []

        def require_expected_catalog() -> None:
            try:
                current = runtime.catalog.get_catalog_revision()
            except CatalogRevisionNotFoundError as error:
                if error.revision == 0 and expected is None:
                    return
                raise
            if current != expected:
                raise _AlreadyPublished(str(current.revision))

        def capture_published_catalog() -> None:
            captured.append(runtime.catalog.get_catalog_revision())

        while True:
            captured.clear()
            try:
                processed = runtime.resident.process_available(
                    periodic_scan=True,
                    preflight=require_expected_catalog,
                    postflight=capture_published_catalog,
                )
            except _AlreadyPublished as error:
                parser.exit(
                    2,
                    "Initial catalog reconciliation found an unexpected publication: "
                    f"current_revision={error}.\n",
                )
            if not processed:
                parser.exit(
                    2, "No gallery ingest or maintenance progress is available.\n"
                )
            if not captured:
                continue
            published = captured[0]
            if published.publication_count > 0:
                print(
                    "Initial catalog reconciliation completed: "
                    f"revision={published.revision} "
                    f"publications={published.publication_count}."
                )
                break
            if not runtime.resident.deferred_gallery_count:
                parser.exit(
                    1,
                    "Initial reconciliation did not publish a non-empty catalog.\n",
                )
            expected = published
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
