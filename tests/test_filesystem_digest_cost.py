"""Only expected digests justify the observer's additional SHA-256 pass."""

from __future__ import annotations

import os
from dataclasses import replace
from hashlib import sha256
from pathlib import Path

import pytest
from h2hdb import FileContentReceipt

from h2hdb_ingest.filesystem import (
    FilesystemArtifactSourceRole,
    FilesystemFileObservation,
    FilesystemSourceChangedError,
    FilesystemStat,
)
from h2hdb_ingest.source_performance import SourcePerformance


def _observation(path: Path) -> FilesystemFileObservation:
    return FilesystemFileObservation(
        folder=path.parent,
        name_bytes=path.name.encode(),
        stat=FilesystemStat.from_os_stat(path.stat()),
        artifact_role=FilesystemArtifactSourceRole.PAGE,
    )


def _hash_calls(performance: SourcePerformance) -> int:
    return {
        value.name: value.value
        for value in performance.metric(status="completed").counters
    }.get("hash_calls", 0)


@pytest.mark.parametrize("expected", ("absent", "matching", "different"))
def test_receipt_always_hashes_exact_bytes_and_expected_digest_still_rejects(
    tmp_path: Path,
    expected: str,
) -> None:
    path = tmp_path / "001.jpg"
    payload = b"actual source bytes" * 4096
    path.write_bytes(payload)
    performance = SourcePerformance()
    digest = (
        None
        if expected == "absent"
        else sha256(payload if expected == "matching" else b"foreign").digest()
    )
    observed = replace(
        _observation(path), expected_sha256=digest, _source_performance=performance
    )
    if expected == "different":
        with pytest.raises(
            FilesystemSourceChangedError, match="metadata bytes changed"
        ):
            FileContentReceipt.from_parts(observed.content_parts())
    else:
        receipt = FileContentReceipt.from_parts(observed.content_parts())
        assert receipt == FileContentReceipt.from_parts((payload,))
        assert receipt.file_sha256 == sha256(payload).digest()
    assert (_hash_calls(performance) > 0) == (expected != "absent")


@pytest.mark.parametrize("has_expected_digest", (False, True))
def test_stat_change_during_actual_read_fails_with_or_without_expected_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    has_expected_digest: bool,
) -> None:
    path = tmp_path / "001.jpg"
    path.write_bytes(b"original")
    observed = replace(
        _observation(path),
        expected_sha256=sha256(b"original").digest() if has_expected_digest else None,
    )
    original_read = os.read
    changed = False

    def mutate(descriptor: int, count: int) -> bytes:
        nonlocal changed
        part = original_read(descriptor, count)
        if part and not changed:
            changed = True
            path.write_bytes(b"changed producer content")
        return part

    monkeypatch.setattr(os, "read", mutate)
    with pytest.raises(FilesystemSourceChangedError, match="changed after read"):
        FileContentReceipt.from_parts(observed.content_parts())


@pytest.mark.parametrize("has_expected_digest", (False, True))
def test_independent_hash_constructor_meter_checks_executed_sha_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, has_expected_digest: bool
) -> None:
    import h2hdb_ingest.filesystem as filesystem_module

    path = tmp_path / "001.jpg"
    payload = b"independently counted actual source hash work" * 4096
    path.write_bytes(payload)
    constructions: list[bool] = []
    updated_bytes: list[int] = []

    class CountedHash:
        def __init__(self) -> None:
            constructions.append(True)
            self._digest = sha256()

        def update(self, part: bytes) -> None:
            updated_bytes.append(len(part))
            self._digest.update(part)

        def digest(self) -> bytes:
            return self._digest.digest()

    monkeypatch.setattr(filesystem_module, "sha256", CountedHash)
    observed = replace(
        _observation(path),
        expected_sha256=sha256(payload).digest() if has_expected_digest else None,
    )
    # This meter is independent from SourcePerformance phase/counter reporting.
    receipt = FileContentReceipt.from_parts(observed.content_parts())
    assert receipt == FileContentReceipt.from_parts((payload,))
    assert len(constructions) == int(has_expected_digest)
    assert sum(updated_bytes) == (len(payload) if has_expected_digest else 0)
