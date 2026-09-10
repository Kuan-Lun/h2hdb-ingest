"""Recognize temporary storage pressure without classifying a gallery as invalid."""

import errno
import json
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path

from ._log_fields import diagnostic_text, quote_log_field


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


def storage_capacity_message(
    error: BaseException,
    *,
    operation: str,
    working_directory: Path | None = None,
    index_path: Path | None = None,
) -> str:
    directory = tempfile.gettempdir()
    try:
        free_bytes: int | None = shutil.disk_usage(directory).free
    except OSError:
        free_bytes = None
    cause = storage_capacity_error(error) or error
    details: dict[str, str | int | bool | None] = {
        "operation": operation,
        # These describe the scratch filesystem; an exception can originate
        # from a different source, library, or database volume.
        "scratch_directory": directory,
        "scratch_free_bytes": free_bytes,
        "error_type": type(cause).__name__,
        "reason": diagnostic_text(cause),
        "error_has_filename": False,
        "action": "preserve gallery eligibility and retry durable work when space is available",
    }
    if cause is not error:
        details["outer_error_type"] = type(error).__name__
        details["outer_reason"] = diagnostic_text(error)
    if isinstance(cause, OSError):
        details["errno"] = cause.errno
        for field, filename in (
            ("failed_path", cause.filename),
            ("failed_path2", cause.filename2),
        ):
            if filename is not None:
                details[field] = os.fsdecode(filename)
                details["error_has_filename"] = True
    if isinstance(cause, sqlite3.Error):
        code = getattr(cause, "sqlite_errorcode", None)
        if isinstance(code, int):
            details["sqlite_errorcode"] = code
    if working_directory is not None:
        details["working_directory"] = str(working_directory)
    if index_path is not None:
        details["index_path"] = str(index_path)
    # Preserve structured JSON and human-readable paths while escaping
    # directionality/control characters just like gallery diagnostic fields.
    return (
        "Storage capacity exhausted: {"
        + ", ".join(
            quote_log_field(key)
            + ": "
            + (quote_log_field(value) if isinstance(value, str) else json.dumps(value))
            for key, value in details.items()
        )
        + "}"
    )
