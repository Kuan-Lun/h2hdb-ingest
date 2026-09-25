"""Dev-only fresh Python worker bytecode and temporary-root ownership."""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path


def fresh_python_environment(
    command: list[str], workspace: Path
) -> tuple[list[str], dict[str, str]]:
    # Timestamp-valid stale bytecode in the checkout/site-packages cannot be
    # consulted. Both native/Python temp data and new caches are owned by the
    # supervisor and are removed even after SIGKILL skips worker teardown.
    cache, temporary = workspace / "pycache", workspace / "temporary"
    cache.mkdir()
    temporary.mkdir()
    return (
        [command[0], "-X", f"pycache_prefix={cache}", *command[1:]],
        {
            **os.environ,
            "TMPDIR": str(temporary),
            "TMP": str(temporary),
            "TEMP": str(temporary),
        },
    )


def worker_binding(workspace: Path) -> dict[str, str]:
    expected = {
        "bytecode": "fresh supervisor-owned cache; compile source without existing .pyc",
        "pycache_prefix": str(workspace / "pycache"),
        "temporary_root": str(workspace / "temporary"),
    }
    if (
        sys.pycache_prefix != expected["pycache_prefix"]
        or tempfile.gettempdir() != expected["temporary_root"]
    ):
        raise ValueError("worker lacks fresh bytecode cache and owned temporary root")
    return expected
