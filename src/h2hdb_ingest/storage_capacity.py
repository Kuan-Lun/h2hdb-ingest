"""Recognize temporary storage pressure without classifying a gallery as invalid."""

import errno
import json
import shutil
import sqlite3
import tempfile


def storage_capacity_error(error: BaseException) -> BaseException | None:
    current: BaseException | None = error
    seen: set[int] = set()
    for _ in range(32):
        if current is None or id(current) in seen:
            break
        seen.add(id(current))
        if isinstance(current, OSError) and current.errno in (
            errno.ENOSPC,
            errno.EDQUOT,
        ):
            return current
        if isinstance(current, sqlite3.Error):
            code = getattr(current, "sqlite_errorcode", None)
            if isinstance(code, int) and code & 0xFF == sqlite3.SQLITE_FULL:
                return current
        # Only explicit causes describe the failed operation. Ambient context
        # can be an older ENOSPC while a new lease-fencing failure is raised.
        current = current.__cause__
    return None


def storage_capacity_message(error: BaseException, *, operation: str) -> str:
    directory = tempfile.gettempdir()
    try:
        free_bytes: int | None = shutil.disk_usage(directory).free
    except OSError:
        free_bytes = None
    return "Storage capacity exhausted: " + json.dumps(
        {
            "operation": operation,
            "scratch_directory": directory,
            "scratch_free_bytes": free_bytes,
            "reason": str(storage_capacity_error(error) or error),
            "action": "preserve gallery eligibility and retry durable work when space is available",
        },
        ensure_ascii=False,
    )
