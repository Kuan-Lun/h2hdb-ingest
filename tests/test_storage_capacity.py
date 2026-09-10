from __future__ import annotations

import errno
import json
import sqlite3
import tempfile
from pathlib import Path

import pytest

from h2hdb_ingest.storage_capacity import (
    storage_capacity_error,
    storage_capacity_message,
)


@pytest.mark.parametrize("code", (errno.ENOSPC, errno.EDQUOT))
def test_explicit_capacity_cause_is_recognized(code: int) -> None:
    capacity = OSError(code, "storage exhausted")
    wrapper = RuntimeError("render failed")
    wrapper.__cause__ = capacity
    assert storage_capacity_error(wrapper) is capacity


def test_lease_failure_during_capacity_recovery_is_not_swallowed() -> None:
    try:
        raise OSError(errno.ENOSPC, "render storage exhausted")
    except OSError as capacity:
        try:
            raise RuntimeError("ingest session ownership was lost")
        except RuntimeError as lease_failure:
            assert lease_failure.__context__ is capacity
            assert storage_capacity_error(lease_failure) is None


def test_sqlite_full_is_capacity_but_other_operational_errors_are_not() -> None:
    full = sqlite3.OperationalError("database or disk is full")
    full.sqlite_errorcode = sqlite3.SQLITE_FULL
    assert storage_capacity_error(full) is full
    locked = sqlite3.OperationalError("database is locked")
    locked.sqlite_errorcode = sqlite3.SQLITE_BUSY
    assert storage_capacity_error(locked) is None


def test_cyclic_causes_do_not_block_failure_classification() -> None:
    first = RuntimeError("first")
    second = RuntimeError("second")
    first.__cause__ = second
    second.__cause__ = first
    assert storage_capacity_error(first) is None


def test_capacity_diagnostic_identifies_both_exception_paths_without_assuming_scratch(
    tmp_path: Path,
) -> None:
    source = str(tmp_path / "source" / "原始\n\u202epage.jpg")
    destination = str(tmp_path / "library" / "gallery.cbz")
    cause = OSError(errno.ENOSPC, "out of space\n\u202e", source, None, destination)
    wrapper = RuntimeError(f"artifact staging failed for source {source}")
    wrapper.__cause__ = cause

    message = storage_capacity_message(wrapper, operation="stage artifact")
    detail = json.loads(message.removeprefix("Storage capacity exhausted: "))
    assert detail["error_type"] == "OSError"
    assert detail["errno"] == errno.ENOSPC
    assert detail["failed_path"] == source
    assert detail["failed_path2"] == destination
    assert detail["error_has_filename"] is True
    assert detail["scratch_directory"] == tempfile.gettempdir()
    assert detail["scratch_directory"] not in (source, destination)
    assert detail["reason"] == str(cause)
    assert detail["outer_error_type"] == "RuntimeError"
    assert detail["outer_reason"] == str(wrapper)
    assert "\n" not in message
    assert "\u202e" not in message
    assert "原始" in message


def test_sqlite_capacity_diagnostic_keeps_index_context_separate_from_error_identity(
    tmp_path: Path,
) -> None:
    cause = sqlite3.OperationalError("database or disk is full")
    cause.sqlite_errorcode = sqlite3.SQLITE_FULL
    index_path = tmp_path / "markers.sqlite3"
    message = storage_capacity_message(
        cause,
        operation="source metadata probe",
        working_directory=tmp_path,
        index_path=index_path,
    )
    detail = json.loads(message.removeprefix("Storage capacity exhausted: "))
    assert detail["error_type"] == "OperationalError"
    assert detail["sqlite_errorcode"] == sqlite3.SQLITE_FULL
    assert detail["index_path"] == str(index_path)
    assert detail["working_directory"] == str(tmp_path)
    assert detail["error_has_filename"] is False
    assert "failed_path" not in detail
    assert "errno" not in detail
    assert "outer_reason" not in detail


def test_capacity_diagnostic_survives_unprintable_oserror() -> None:
    class UnprintableError(OSError):
        def __str__(self) -> str:
            raise ValueError("broken diagnostic")

    message = storage_capacity_message(
        UnprintableError(errno.ENOSPC, "full"), operation="ingest"
    )
    detail = json.loads(message.removeprefix("Storage capacity exhausted: "))
    assert detail["error_type"] == "UnprintableError"
    assert detail["reason"] == "<diagnostic text unavailable>"
