from __future__ import annotations

import errno
import logging

import pytest

from h2hdb_ingest._retry_diagnostics import retry_diagnostic
from h2hdb_ingest.filesystem import FilesystemSourceChangedError
from h2hdb_ingest.progress import IngestProgress


def test_nested_retry_keeps_original_activity_cause_and_notes_after_cleanup(
    caplog: pytest.LogCaptureFixture,
) -> None:
    info: list[str] = []
    progress = IngestProgress(info.append)
    work = progress.begin("source", announce=False)
    work.operation("source_prepare")
    original = FilesystemSourceChangedError("source observation changed")
    original.add_note('gallery="1001"\nsource path contains\u2028a separator')
    cause = FileNotFoundError(errno.ENOENT, "source disappeared", "/source/a\nb.jpg")
    with pytest.raises(FilesystemSourceChangedError) as caught:
        with work.activity("source_source_freeze"):
            with work.activity("source_file_read"):
                work.advance("source_bytes_read", 100)
                raise original from cause
    assert caught.value is original
    current = progress.snapshot()
    assert current is not None
    assert current.operation == "source_prepare"
    failure = retry_diagnostic(original, current)
    assert failure.snapshot is not None
    assert failure.snapshot.operation == "source_file_read"
    assert dict(failure.snapshot.counters)["source_bytes_read"] == 100
    work.operation("catalog_cleanup")
    work.finish("retry", failure=failure)
    assert progress.current() is None
    assert not info
    assert len(caplog.records) == 1
    warning = caplog.records[0]
    assert warning.levelno == logging.WARNING
    message = warning.getMessage()
    assert "Reading and verifying source file contents" in message
    assert "Cleaning up" not in message
    assert 'phase="source" operation="source_file_read"' in message
    assert 'error_type="FilesystemSourceChangedError"' in message
    assert 'cause.1.error_type="FileNotFoundError"' in message
    assert "/source/a" in message and "b.jpg" in message
    assert "gallery=" in message
    assert "\\n" in message and "\\u2028" in message
    assert "\n" not in message and "\u2028" not in message


def test_retry_diagnostic_does_not_reuse_context_from_another_work_generation() -> None:
    progress = IngestProgress(lambda _: None)
    work = progress.begin("source", announce=False)
    error = FilesystemSourceChangedError("marker unavailable")
    with pytest.raises(FilesystemSourceChangedError):
        with work.activity("source_completion_marker"):
            raise error
    work.finish(announce=False)
    replacement = progress.begin("publication", announce=False)
    replacement.operation("library_activation")
    failure = retry_diagnostic(error, progress.snapshot())
    assert failure.snapshot is not None
    assert failure.snapshot.generation == replacement.generation
    assert 'phase="publication" operation="library_activation"' in failure.detail
    assert "source_completion_marker" not in failure.detail
    replacement.finish(announce=False)
    other_progress = IngestProgress(lambda _: None)
    other_work = other_progress.begin("analysis", announce=False)
    assert other_work.generation == work.generation
    other_work.operation("analysis_prepare")
    other_failure = retry_diagnostic(error, other_progress.snapshot())
    assert other_failure.snapshot is not None
    assert other_failure.snapshot.phase == "analysis"
    assert "source_completion_marker" not in other_failure.detail
    other_work.finish(announce=False)


def test_retry_diagnostic_bounds_notes_and_cyclic_exception_chains() -> None:
    error = FilesystemSourceChangedError("marker unavailable")
    for _ in range(20):
        error.add_note("x" * 1000)
    cause = OSError("source path vanished")
    cause.__cause__ = error
    error.__cause__ = cause
    failure = retry_diagnostic(error, None)
    assert 'error_type="FilesystemSourceChangedError"' in failure.detail
    assert 'cause.1.error_type="OSError"' in failure.detail
    assert "cause.2" not in failure.detail
    assert "notes_omitted=4" in failure.detail
    assert len(failure.detail) < 32 * 1024


def test_retry_diagnostic_bounds_total_utf8_output_after_control_escaping() -> None:
    error = FilesystemSourceChangedError("original reason")
    for _ in range(20):
        error.add_note("\u2028" * 5000)
    failure = retry_diagnostic(error, None)
    assert 'reason="original reason"' in failure.detail
    assert "diagnostics_truncated=true" in failure.detail
    assert len(failure.detail.encode("utf-8")) <= 32 * 1024
    assert "\u2028" not in failure.detail
