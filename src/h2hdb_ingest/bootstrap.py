"""Fresh catalog bootstrap command."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from h2hdb import H2HDB

from .__main__ import main as ingest_main
from .config import load_config


def main(arguments: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Perform the one-time filesystem-to-canonical reconciliation and "
            "publish the initial catalog revision"
        )
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
    database = H2HDB(config.core)
    database.check_compatibility()
    current = database.get_catalog_revision()
    if current.revision != 0:
        parser.exit(
            2,
            "Initial catalog reconciliation has already run: "
            f"current_revision={current.revision}.\n",
        )

    ingest_main(["--config", str(parsed.config), "--once"])
    published = H2HDB(config.core).get_catalog_revision()
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
