"""Offline command for verifying and adopting a relocated library."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from collections.abc import Sequence
from pathlib import Path

from .library_relocation import relocate_library


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Verify a moved library and refresh its journal file identities. "
            "Stop ingest and readers first; keep the complete library, including "
            ".h2hdb-state. This command preserves the library UUID and never "
            "rebuilds the core database or renders CBZ files."
        )
    )
    parser.add_argument("--library", required=True, type=Path)
    parser.add_argument(
        "--batch-size", type=int, default=128, help="resources per checkpoint (1..128)"
    )
    arguments = parser.parse_args(argv)
    if not 1 <= arguments.batch_size <= 128:
        parser.error("--batch-size must be between 1 and 128")
    print("Preparing the library journal for relocation verification...", flush=True)
    try:
        result = relocate_library(
            arguments.library,
            batch_size=arguments.batch_size,
            progress=lambda message: print(message, flush=True),
        )
    except (OSError, RuntimeError, ValueError, sqlite3.Error) as error:
        print(
            f"Library relocation stopped: {error}\n"
            "Files have been preserved. Correct the reported condition and rerun "
            "the same command to resume the journal checkpoint.",
            file=sys.stderr,
        )
        raise SystemExit(1) from error
    print(
        f"Library relocation complete: session={result.session_id.hex()} "
        f"verified_files={result.verified_files}. "
        "The core database and existing artifact contents have been retained.",
        flush=True,
    )


if __name__ == "__main__":
    main()
