from __future__ import annotations

import errno
import sqlite3

import pytest

from h2hdb_ingest.storage_capacity import storage_capacity_error


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
