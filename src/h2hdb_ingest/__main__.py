"""Resident and one-shot entry point for greenfield vNext ingest."""

from __future__ import annotations

import argparse
import signal
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from threading import Event
from types import FrameType

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
    with _stop_on_termination() as stop:
        runtime.resident.initialize()
        if arguments.once:
            processed = runtime.resident.process_available(
                periodic_scan=True,
                should_stop=stop.is_set,
            )
            if stop.is_set():
                return
            if not processed:
                raise RuntimeError("No gallery ingest lease is currently available")
            return
        runtime.resident.run_forever(stop=stop)


@contextmanager
def _stop_on_termination() -> Iterator[Event]:
    """Translate container stop signals into a no-new-work resident stop."""

    stop = Event()

    def request_stop(_signum: int, _frame: FrameType | None) -> None:
        stop.set()

    signals = (signal.SIGINT, signal.SIGTERM)
    previous = {value: signal.getsignal(value) for value in signals}
    try:
        for value in signals:
            signal.signal(value, request_stop)
        yield stop
    finally:
        for value, handler in previous.items():
            signal.signal(value, handler)


if __name__ == "__main__":
    main()
