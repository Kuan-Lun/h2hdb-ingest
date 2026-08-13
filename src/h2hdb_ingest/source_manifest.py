from __future__ import annotations

__all__ = [
    "CANONICAL_SOURCE_MANIFEST_VERSION",
    "CanonicalManifestAccumulator",
    "CanonicalManifestDigests",
]

import heapq
import json
import sys
from collections.abc import Iterable, Iterator, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Protocol, Self

from h2hdb import CANONICAL_SOURCE_MANIFEST_VERSION

_DEFAULT_MEMORY_LIMIT_BYTES = 8 * 1024 * 1024
_DEFAULT_MERGE_FAN_IN = 32
_RUN_ENCODING = "ascii"
_METADATA_FILE_NAME = "galleryinfo.txt"
_POINTER_BYTES = 8 if sys.maxsize > 2**32 else 4
_MERGE_ENTRY_OVERHEAD_BYTES = 128


class ManifestSourceFile(Protocol):
    """The scanner fields needed to derive canonical gallery digests."""

    @property
    def name(self) -> str: ...

    @property
    def size_bytes(self) -> int: ...

    @property
    def sha256(self) -> str: ...


@dataclass(frozen=True, slots=True)
class CanonicalManifestDigests:
    canonical_source_manifest_sha256: str
    raw_content_sha256: str | None


@dataclass(frozen=True, slots=True)
class _CanonicalFileRecord:
    ordinal: int
    name: str
    folded_name: str
    size_bytes: int
    sha256: str
    content_hash: bytes | None

    @classmethod
    def from_source(
        cls,
        source_file: ManifestSourceFile,
        *,
        ordinal: int,
    ) -> _CanonicalFileRecord:
        name = source_file.name
        file_sha256 = source_file.sha256
        return cls(
            ordinal=ordinal,
            name=name,
            folded_name=name.casefold(),
            size_bytes=source_file.size_bytes,
            sha256=file_sha256,
            content_hash=(
                None if name == _METADATA_FILE_NAME else bytes.fromhex(file_sha256)
            ),
        )

    @property
    def manifest_sort_key(self) -> tuple[str, str, int]:
        # Python's sort is stable.  The ordinal preserves that final tie-break
        # across independently sorted spill runs when duplicate names exist.
        return (self.folded_name, self.name, self.ordinal)


def _record_memory_cost(record: _CanonicalFileRecord) -> int:
    """Conservative charge for retained objects plus list/sort pointers."""

    return (
        sys.getsizeof(record)
        + sys.getsizeof(record.ordinal)
        + sys.getsizeof(record.name)
        + sys.getsizeof(record.folded_name)
        + sys.getsizeof(record.size_bytes)
        + sys.getsizeof(record.sha256)
        + (0 if record.content_hash is None else sys.getsizeof(record.content_hash))
        + 3 * _POINTER_BYTES
    )


def _hash_memory_cost(value: bytes) -> int:
    return sys.getsizeof(value) + 3 * _POINTER_BYTES


def _encode_run_record(record: _CanonicalFileRecord) -> bytes:
    # ensure_ascii=True makes every record one ASCII line even when a POSIX
    # filename contains newlines, non-ASCII text, or surrogateescape values.
    return (
        json.dumps(
            [record.ordinal, record.name, record.size_bytes, record.sha256],
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode(_RUN_ENCODING)
        + b"\n"
    )


def _decode_run_record(encoded: bytes) -> _CanonicalFileRecord:
    value = json.loads(encoded)
    if (
        not isinstance(value, list)
        or len(value) != 4
        or not isinstance(value[0], int)
        or not isinstance(value[1], str)
        or not isinstance(value[2], int)
        or not isinstance(value[3], str)
    ):
        raise RuntimeError("Canonical manifest spill record is invalid")
    ordinal, name, size_bytes, file_sha256 = value
    return _CanonicalFileRecord(
        ordinal=ordinal,
        name=name,
        folded_name=name.casefold(),
        size_bytes=size_bytes,
        sha256=file_sha256,
        content_hash=(
            None if name == _METADATA_FILE_NAME else bytes.fromhex(file_sha256)
        ),
    )


class CanonicalManifestAccumulator:
    """Derive historical canonical-v1 digests with bounded retained memory.

    Canonical v1 orders files by Python's stable
    ``(casefold(name), name)`` sort and hashes compact, sorted-key JSON.  The
    accumulator preserves that byte contract while spilling sorted runs once
    its charged in-memory records reach ``memory_limit_bytes``.

    The limit covers retained records and conservative list/sort pointer
    charges.  Encoding or merging one unusually large filename necessarily
    needs that single record in addition to the budget; merge fan-in is fixed
    and reduced automatically for large records.  Temporary files are private
    to a lazily created directory and are removed by ``finish`` or ``close``.
    """

    def __init__(
        self,
        *,
        memory_limit_bytes: int = _DEFAULT_MEMORY_LIMIT_BYTES,
        temporary_directory: Path | None = None,
        merge_fan_in: int = _DEFAULT_MERGE_FAN_IN,
    ) -> None:
        if memory_limit_bytes <= 0:
            raise ValueError("memory_limit_bytes must be positive")
        if merge_fan_in < 2:
            raise ValueError("merge_fan_in must be at least two")
        self._memory_limit_bytes = memory_limit_bytes
        self._temporary_directory = temporary_directory
        self._merge_fan_in = merge_fan_in
        self._buffer: list[_CanonicalFileRecord] = []
        self._buffer_bytes = 0
        self._largest_record_bytes = 1
        self._next_ordinal = 0
        self._manifest_run_count = 0
        self._workspace: TemporaryDirectory[str] | None = None
        self._closed = False

    def __enter__(self) -> Self:
        self._ensure_open()
        return self

    def __exit__(self, *_error: object) -> None:
        self.close()

    def add(self, source_file: ManifestSourceFile) -> None:
        self._ensure_open()
        try:
            record = _CanonicalFileRecord.from_source(
                source_file,
                ordinal=self._next_ordinal,
            )
            record_bytes = _record_memory_cost(record)
            self._largest_record_bytes = max(
                self._largest_record_bytes,
                record_bytes,
            )
            if self._buffer and (
                self._buffer_bytes + record_bytes > self._memory_limit_bytes
            ):
                self._spill_manifest_buffer()
            self._buffer.append(record)
            self._buffer_bytes += record_bytes
            self._next_ordinal += 1
            if self._buffer_bytes > self._memory_limit_bytes:
                # A single record can exceed the budget.  Spill it immediately
                # so the excess is transient rather than retained.
                self._spill_manifest_buffer()
        except BaseException:
            self.close()
            raise

    def finish(self, metadata_sha256: str) -> CanonicalManifestDigests:
        self._ensure_open()
        try:
            if self._manifest_run_count == 0:
                source_digest, content_digest = self._finish_in_memory(metadata_sha256)
            else:
                if self._buffer:
                    self._spill_manifest_buffer()
                manifest_runs = self._compact_manifest_runs(self._manifest_run_count)
                source_digest = self._hash_canonical_manifest_runs(
                    manifest_runs,
                    metadata_sha256,
                )
                content_digest = self._hash_raw_content_from_manifest_runs(
                    manifest_runs
                )
            return CanonicalManifestDigests(
                canonical_source_manifest_sha256=source_digest,
                raw_content_sha256=content_digest,
            )
        finally:
            self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._buffer.clear()
        self._buffer_bytes = 0
        self._manifest_run_count = 0
        if self._workspace is not None:
            self._workspace.cleanup()
            self._workspace = None

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("Canonical manifest accumulator is closed")

    def _finish_in_memory(self, metadata_sha256: str) -> tuple[str, str | None]:
        self._buffer.sort(key=lambda record: record.manifest_sort_key)
        source_digest = self._hash_canonical_records(
            self._buffer,
            metadata_sha256,
        )
        if not any(record.content_hash is not None for record in self._buffer):
            return source_digest, None
        self._buffer.sort(key=lambda record: record.content_hash or b"")
        content_digest = sha256()
        for record in self._buffer:
            if record.content_hash is not None:
                content_digest.update(record.content_hash)
        return source_digest, content_digest.hexdigest()

    @staticmethod
    def _hash_canonical_records(
        records: Iterable[_CanonicalFileRecord],
        metadata_sha256: str,
    ) -> str:
        digest = sha256()
        digest.update(b'{"files":[')
        separator = b""
        for record in records:
            digest.update(separator)
            digest.update(
                json.dumps(
                    {
                        "name": record.name,
                        "sha256": record.sha256,
                        "size": record.size_bytes,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            )
            separator = b","
        digest.update(b'],"metadata":')
        digest.update(json.dumps(metadata_sha256).encode())
        digest.update(b',"version":1}')
        return digest.hexdigest()

    def _spill_manifest_buffer(self) -> None:
        if not self._buffer:
            return
        self._buffer.sort(key=lambda record: record.manifest_sort_key)
        path = self._run_path("manifest", 0, self._manifest_run_count)
        try:
            with path.open("xb") as destination:
                for record in self._buffer:
                    destination.write(_encode_run_record(record))
        except BaseException:
            path.unlink(missing_ok=True)
            raise
        self._manifest_run_count += 1
        self._buffer.clear()
        self._buffer_bytes = 0

    def _compact_manifest_runs(self, run_count: int) -> list[Path]:
        fan_in = self._effective_merge_fan_in()
        generation = 0
        while run_count > fan_in:
            compacted_count = 0
            for offset in range(0, run_count, fan_in):
                group = [
                    self._run_path("manifest", generation, index)
                    for index in range(offset, min(offset + fan_in, run_count))
                ]
                output = self._run_path(
                    "manifest",
                    generation + 1,
                    compacted_count,
                )
                if len(group) == 1:
                    group[0].replace(output)
                    compacted_count += 1
                    continue
                try:
                    with output.open("xb") as destination:
                        with self._merged_manifest_records(group) as records:
                            for record in records:
                                destination.write(_encode_run_record(record))
                except BaseException:
                    output.unlink(missing_ok=True)
                    raise
                for path in group:
                    path.unlink()
                compacted_count += 1
            generation += 1
            run_count = compacted_count
        return [
            self._run_path("manifest", generation, index) for index in range(run_count)
        ]

    def _effective_merge_fan_in(self) -> int:
        budget_fan_in = max(
            2,
            self._memory_limit_bytes
            // (self._largest_record_bytes + _MERGE_ENTRY_OVERHEAD_BYTES),
        )
        return min(self._merge_fan_in, budget_fan_in)

    @contextmanager
    def _merged_manifest_records(
        self,
        paths: Sequence[Path],
    ) -> Iterator[Iterator[_CanonicalFileRecord]]:
        with ExitStack() as stack:
            sources = [
                stack.enter_context(path.open("rb", buffering=0)) for path in paths
            ]
            records = (
                (_decode_run_record(line) for line in source) for source in sources
            )
            yield heapq.merge(
                *records,
                key=lambda record: record.manifest_sort_key,
            )

    def _hash_canonical_manifest_runs(
        self,
        runs: Sequence[Path],
        metadata_sha256: str,
    ) -> str:
        with self._merged_manifest_records(runs) as records:
            return self._hash_canonical_records(records, metadata_sha256)

    def _hash_raw_content_from_manifest_runs(
        self,
        manifest_runs: Sequence[Path],
    ) -> str | None:
        buffer: list[bytes] = []
        buffer_bytes = 0
        hash_run_count = 0
        content_count = 0

        def spill() -> None:
            nonlocal buffer_bytes, hash_run_count
            if not buffer:
                return
            buffer.sort()
            path = self._run_path("content", 0, hash_run_count)
            try:
                with path.open("xb") as destination:
                    for value in buffer:
                        destination.write(value)
            except BaseException:
                path.unlink(missing_ok=True)
                raise
            hash_run_count += 1
            buffer.clear()
            buffer_bytes = 0

        for manifest_run in manifest_runs:
            with manifest_run.open("rb", buffering=0) as source:
                for line in source:
                    record = _decode_run_record(line)
                    if record.name == _METADATA_FILE_NAME:
                        continue
                    assert record.content_hash is not None
                    value = record.content_hash
                    value_bytes = _hash_memory_cost(value)
                    if buffer and (
                        buffer_bytes + value_bytes > self._memory_limit_bytes
                    ):
                        spill()
                    buffer.append(value)
                    buffer_bytes += value_bytes
                    content_count += 1
                    if buffer_bytes > self._memory_limit_bytes:
                        spill()

        if content_count == 0:
            return None
        if hash_run_count == 0:
            buffer.sort()
            digest = sha256()
            for value in buffer:
                digest.update(value)
            return digest.hexdigest()

        spill()
        hash_runs = self._compact_hash_runs(hash_run_count)
        digest = sha256()
        with self._merged_hashes(hash_runs) as hashes:
            for value in hashes:
                digest.update(value)
        return digest.hexdigest()

    def _compact_hash_runs(self, run_count: int) -> list[Path]:
        fan_in = self._effective_merge_fan_in()
        generation = 0
        while run_count > fan_in:
            compacted_count = 0
            for offset in range(0, run_count, fan_in):
                group = [
                    self._run_path("content", generation, index)
                    for index in range(offset, min(offset + fan_in, run_count))
                ]
                output = self._run_path(
                    "content",
                    generation + 1,
                    compacted_count,
                )
                if len(group) == 1:
                    group[0].replace(output)
                    compacted_count += 1
                    continue
                try:
                    with output.open("xb") as destination:
                        with self._merged_hashes(group) as hashes:
                            for value in hashes:
                                destination.write(value)
                except BaseException:
                    output.unlink(missing_ok=True)
                    raise
                for path in group:
                    path.unlink()
                compacted_count += 1
            generation += 1
            run_count = compacted_count
        return [
            self._run_path("content", generation, index) for index in range(run_count)
        ]

    @contextmanager
    def _merged_hashes(
        self,
        paths: Sequence[Path],
    ) -> Iterator[Iterator[bytes]]:
        with ExitStack() as stack:
            sources = [
                stack.enter_context(path.open("rb", buffering=0)) for path in paths
            ]
            hashes = (self._iter_hashes(source) for source in sources)
            yield heapq.merge(*hashes)

    @staticmethod
    def _iter_hashes(source: object) -> Iterator[bytes]:
        read = getattr(source, "read")
        while value := read(32):
            if len(value) != 32:
                raise RuntimeError("Canonical manifest content spill is truncated")
            yield value

    def _run_path(self, kind: str, generation: int, index: int) -> Path:
        if self._workspace is None:
            self._workspace = TemporaryDirectory(
                prefix="h2hdb-source-manifest-",
                dir=self._temporary_directory,
            )
        return Path(self._workspace.name) / f"{kind}-g{generation:08d}-r{index:08d}.run"
