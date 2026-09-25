#!/usr/bin/env python3
"""Upgrade an exact v4 library journal offline; preserve its UUID and artifacts."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from time import perf_counter

from h2hdb_ingest.library_journal_upgrade import upgrade_library_journal


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", required=True, type=Path)
    parser.add_argument(
        "--consumers-stopped",
        required=True,
        action="store_true",
        help="Confirm ingest, readers, relocation and every library writer are stopped",
    )
    args = parser.parse_args()
    started = perf_counter()
    messages = {
        "integrity_started": "Checking SQLite integrity of the admitted journal; this scans journal data and indexes.",
        "index_started": "Building the cleanup index from retained protection tokens.",
        "target_integrity_started": "Checking SQLite integrity after index construction.",
        "journal_validated": "Exact journal shape, UUID and SQLite integrity verified.",
        "control_recreated": "Prepared v5 control; retained publication and relocation facts remain unchanged.",
        "index_created": "Built the cleanup eligibility index; validating before commit.",
        "target_validated": "Exact v5 journal validated; committing the atomic conversion.",
        "transaction_committed": "Journal v5 transaction committed; synchronizing its file and directory.",
        "durable_complete": "Journal and directory synchronized; conversion is durable.",
    }

    def progress(name: str) -> None:
        print(
            f"{messages[name]} elapsed_seconds={perf_counter() - started:.3f}",
            flush=True,
        )

    try:
        result = upgrade_library_journal(args.library, checkpoint=progress)
    except (OSError, RuntimeError, ValueError, sqlite3.Error) as error:
        print(
            f"Journal upgrade stopped: {error}\n"
            "Keep consumers stopped and preserve the library. Correct the reported "
            "condition and rerun this tool; it accepts only exact v4 or completed v5.",
            file=sys.stderr,
        )
        raise SystemExit(1) from error
    print(
        f"Library journal conversion: {result}; UUID, facts and CBZ/artwork bytes retained"
    )


if __name__ == "__main__":
    main()
