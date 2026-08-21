"""Resident and one-shot entry point for greenfield vNext ingest."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from .config import load_config
from .runtime import build_runtime, configure_logging


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Observe galleries and publish an H2HDB vNext catalog"
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--once", action="store_true")
    arguments = parser.parse_args(argv)

    config = load_config(arguments.config)
    config.ensure_paths()
    configure_logging(config)
    runtime = build_runtime(config)
    runtime.resident.initialize()
    if arguments.once:
        if not runtime.resident.process_available(periodic_scan=True):
            raise RuntimeError("No gallery ingest lease is currently available")
        return
    runtime.resident.run_forever()


if __name__ == "__main__":
    main()
