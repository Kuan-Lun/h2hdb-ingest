from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Self

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from h2hdb_ingest.source_manifest import (
    CanonicalManifestAccumulator,
    CanonicalManifestDigests,
)

_HASH_POOL = tuple(
    sha256(f"source-manifest-property-{index}".encode()).hexdigest()
    for index in range(5)
)
_EXTRA_NAME_POOL = (
    "galleryinfo.txt",
    "duplicate.jpg",
    "DUPLICATE.jpg",
    "SS.jpg",
    "ß.jpg",
    "漫 畫.jpg ",
    "同名.jpg",
    "é.jpg",
    "e\u0301.jpg",
    "line\nbreak.jpg",
    "emoji-🧪.bin",
)


@dataclass(frozen=True, slots=True)
class _SourceFile:
    name: str
    size_bytes: int
    sha256: str


@dataclass(frozen=True, slots=True)
class _ManifestCase:
    source_files: tuple[_SourceFile, ...]
    metadata_sha256: str


def _legacy_v1_reference(case: _ManifestCase) -> CanonicalManifestDigests:
    files = sorted(
        case.source_files,
        key=lambda source_file: (source_file.name.casefold(), source_file.name),
    )
    payload = {
        "version": 1,
        "metadata": case.metadata_sha256,
        "files": [
            {
                "name": source_file.name,
                "size": source_file.size_bytes,
                "sha256": source_file.sha256,
            }
            for source_file in files
        ],
    }
    content_hashes = sorted(
        bytes.fromhex(source_file.sha256)
        for source_file in files
        if source_file.name != "galleryinfo.txt"
    )
    return CanonicalManifestDigests(
        canonical_source_manifest_sha256=sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        raw_content_sha256=(
            sha256(b"".join(content_hashes)).hexdigest() if content_hashes else None
        ),
    )


@st.composite
def _manifest_cases(draw: st.DrawFn) -> _ManifestCase:
    sizes = draw(
        st.lists(
            st.integers(min_value=0, max_value=2**32),
            min_size=18,
            max_size=18,
        )
    )
    duplicate_name = draw(st.sampled_from(("duplicate.jpg", "同名.jpg", "ß.jpg")))
    fixed_names = (
        "galleryinfo.txt",
        duplicate_name,
        duplicate_name,
        "SS.jpg",
        "ß.jpg",
        "同名-一.jpg",
        "同名-二.jpg",
        "é.jpg",
        "e\u0301.jpg",
        "line\nbreak.jpg",
        "emoji-🧪.bin",
        "العربية.jpg",
        "日本語.jpg",
        "한국어.jpg",
        "Кириллица.jpg",
        "Ａ.jpg",
        "000.jpg",
        "999.jpg",
    )
    fixed_files = tuple(
        _SourceFile(
            name=name,
            size_bytes=size,
            sha256=_HASH_POOL[index % len(_HASH_POOL)],
        )
        for index, (name, size) in enumerate(zip(fixed_names, sizes, strict=True))
    )
    extras = draw(
        st.lists(
            st.builds(
                _SourceFile,
                name=st.sampled_from(_EXTRA_NAME_POOL),
                size_bytes=st.integers(min_value=0, max_value=2**32),
                sha256=st.sampled_from(_HASH_POOL),
            ),
            min_size=0,
            max_size=10,
        )
    )
    source_files = tuple(draw(st.permutations((*fixed_files, *extras))))
    return _ManifestCase(
        source_files=source_files,
        metadata_sha256=draw(st.sampled_from(_HASH_POOL)),
    )


@pytest.mark.parametrize("merge_fan_in", (2, 3, 7, 16))
@settings(
    max_examples=25,
    deadline=None,
    derandomize=True,
    suppress_health_check=(HealthCheck.function_scoped_fixture,),
)
@given(case=_manifest_cases())
def test_forced_spill_matches_legacy_and_in_memory_for_randomized_manifests(
    tmp_path: Path,
    merge_fan_in: int,
    case: _ManifestCase,
) -> None:
    in_memory_root = tmp_path / f"in-memory-{merge_fan_in}"
    spill_root = tmp_path / f"spill-{merge_fan_in}"
    in_memory_root.mkdir(exist_ok=True)
    spill_root.mkdir(exist_ok=True)
    expected = _legacy_v1_reference(case)
    in_memory = CanonicalManifestAccumulator(
        memory_limit_bytes=16 * 1024 * 1024,
        temporary_directory=in_memory_root,
        merge_fan_in=merge_fan_in,
    )
    forced_spill = CanonicalManifestAccumulator(
        memory_limit_bytes=1,
        temporary_directory=spill_root,
        merge_fan_in=merge_fan_in,
    )

    for source_file in case.source_files:
        in_memory.add(source_file)
        forced_spill.add(source_file)

    assert not tuple(in_memory_root.iterdir())
    assert tuple(spill_root.rglob("*.run"))
    assert in_memory.finish(case.metadata_sha256) == expected
    assert forced_spill.finish(case.metadata_sha256) == expected
    assert not tuple(in_memory_root.iterdir())
    assert not tuple(spill_root.iterdir())


class _FailingRunWriter:
    def __init__(self, delegate: Any) -> None:
        self._delegate = delegate

    def __enter__(self) -> Self:
        self._delegate.__enter__()
        return self

    def __exit__(self, *error: object) -> Any:
        return self._delegate.__exit__(*error)

    def write(self, value: bytes) -> int:
        self._delegate.write(value[:1])
        self._delegate.flush()
        raise OSError("injected spill write failure")


class _FailingRunReader:
    def __init__(self, delegate: Any) -> None:
        self._delegate = delegate

    def __enter__(self) -> Self:
        self._delegate.__enter__()
        return self

    def __exit__(self, *error: object) -> Any:
        return self._delegate.__exit__(*error)

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> bytes:
        next(self._delegate)
        raise OSError("injected spill read failure")


def test_partial_spill_write_failure_removes_workspace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spill_root = tmp_path / "write-failure"
    spill_root.mkdir()
    original_open = Path.open

    def open_with_write_failure(
        path: Path,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> Any:
        destination = original_open(
            path,
            mode,
            buffering,
            encoding,
            errors,
            newline,
        )
        if mode == "xb" and path.suffix == ".run" and spill_root in path.parents:
            return _FailingRunWriter(destination)
        return destination

    monkeypatch.setattr(Path, "open", open_with_write_failure)
    accumulator = CanonicalManifestAccumulator(
        memory_limit_bytes=1,
        temporary_directory=spill_root,
    )

    with pytest.raises(OSError, match="injected spill write failure"):
        accumulator.add(_SourceFile("001.jpg", 4, _HASH_POOL[0]))

    assert not tuple(spill_root.iterdir())


def test_spill_read_failure_removes_workspace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spill_root = tmp_path / "read-failure"
    spill_root.mkdir()
    accumulator = CanonicalManifestAccumulator(
        memory_limit_bytes=1,
        temporary_directory=spill_root,
        merge_fan_in=2,
    )
    accumulator.add(_SourceFile("001.jpg", 4, _HASH_POOL[0]))
    accumulator.add(_SourceFile("002.jpg", 4, _HASH_POOL[1]))
    assert tuple(spill_root.rglob("*.run"))
    original_open = Path.open
    injected = False

    def open_with_read_failure(
        path: Path,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> Any:
        nonlocal injected
        source = original_open(path, mode, buffering, encoding, errors, newline)
        if (
            not injected
            and mode == "rb"
            and path.suffix == ".run"
            and spill_root in path.parents
        ):
            injected = True
            return _FailingRunReader(source)
        return source

    monkeypatch.setattr(Path, "open", open_with_read_failure)

    with pytest.raises(OSError, match="injected spill read failure"):
        accumulator.finish(_HASH_POOL[2])

    assert injected
    assert not tuple(spill_root.iterdir())


def test_truncated_content_run_failure_removes_workspace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spill_root = tmp_path / "truncated-run"
    spill_root.mkdir()
    accumulator = CanonicalManifestAccumulator(
        memory_limit_bytes=1,
        temporary_directory=spill_root,
        merge_fan_in=2,
    )
    for index in range(4):
        accumulator.add(_SourceFile(f"{index:03}.jpg", index, _HASH_POOL[index]))
    original_compact = CanonicalManifestAccumulator._compact_hash_runs
    truncated = False

    def compact_then_truncate(
        current: CanonicalManifestAccumulator,
        run_count: int,
    ) -> list[Path]:
        nonlocal truncated
        paths = original_compact(current, run_count)
        victim = paths[0]
        encoded = victim.read_bytes()
        assert len(encoded) >= 32
        victim.write_bytes(encoded[:-1])
        truncated = True
        return paths

    monkeypatch.setattr(
        CanonicalManifestAccumulator,
        "_compact_hash_runs",
        compact_then_truncate,
    )

    with pytest.raises(
        RuntimeError,
        match="Canonical manifest content spill is truncated",
    ):
        accumulator.finish(_HASH_POOL[4])

    assert truncated
    assert not tuple(spill_root.iterdir())
