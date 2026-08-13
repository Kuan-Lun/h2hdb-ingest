import json
from hashlib import sha256
from pathlib import Path
from typing import cast

import pytest

from h2hdb_ingest.models import ScannedFile
from h2hdb_ingest.source_manifest import (
    CANONICAL_SOURCE_MANIFEST_VERSION,
    CanonicalManifestAccumulator,
    CanonicalManifestDigests,
)


def _source_file(name: str, content: bytes) -> ScannedFile:
    return ScannedFile(
        path=Path(name),
        name=name,
        size_bytes=len(content),
        sha256=sha256(content).hexdigest(),
    )


def _legacy_v1_reference(
    source_files: tuple[ScannedFile, ...],
    metadata_sha256: str,
) -> CanonicalManifestDigests:
    files = sorted(
        source_files,
        key=lambda source_file: (source_file.name.casefold(), source_file.name),
    )
    payload = {
        "version": 1,
        "metadata": metadata_sha256,
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


@pytest.mark.parametrize(
    "source_files",
    (
        (),
        (_source_file("galleryinfo.txt", b"metadata"),),
        (
            _source_file("galleryinfo.txt", b"metadata"),
            _source_file("SS.jpg", b"latin"),
            _source_file("ß.jpg", b"unicode-casefold"),
            _source_file("漫 畫.jpg ", b"trailing-space"),
        ),
    ),
    ids=("empty", "metadata-only", "unicode-and-trailing-space"),
)
@pytest.mark.parametrize("chunk_size", (1, 2, 100))
def test_canonical_manifest_is_legacy_v1_byte_equivalent_across_chunks(
    source_files: tuple[ScannedFile, ...],
    chunk_size: int,
) -> None:
    metadata_sha256 = sha256(b"metadata").hexdigest()
    accumulator = CanonicalManifestAccumulator()

    for offset in range(0, len(source_files), chunk_size):
        for source_file in reversed(source_files[offset : offset + chunk_size]):
            accumulator.add(source_file)

    assert CANONICAL_SOURCE_MANIFEST_VERSION == 1
    assert accumulator.finish(metadata_sha256) == _legacy_v1_reference(
        source_files,
        metadata_sha256,
    )


def test_spill_and_non_spill_paths_are_exactly_equivalent_and_clean_up(
    tmp_path: Path,
) -> None:
    metadata_sha256 = sha256(b"metadata").hexdigest()
    duplicate_content = b"duplicate-content"
    source_files = (
        _source_file("galleryinfo.txt", b"metadata"),
        _source_file("SS.jpg", b"latin"),
        _source_file("ß.jpg", b"unicode-casefold"),
        _source_file("漫 \u756b.jpg ", b"trailing-space"),
        _source_file("line\nbreak.jpg", duplicate_content),
        _source_file("surrogate-\udcff.jpg", duplicate_content),
        _source_file("duplicate-name.jpg", b"first"),
        _source_file("duplicate-name.jpg", b"second"),
    )
    added_files = tuple(reversed(source_files))
    expected = _legacy_v1_reference(added_files, metadata_sha256)
    non_spill_root = tmp_path / "non-spill"
    spill_root = tmp_path / "spill"
    non_spill_root.mkdir()
    spill_root.mkdir()

    non_spill = CanonicalManifestAccumulator(
        memory_limit_bytes=1024 * 1024,
        temporary_directory=non_spill_root,
    )
    spilled = CanonicalManifestAccumulator(
        memory_limit_bytes=1,
        temporary_directory=spill_root,
        merge_fan_in=2,
    )
    for source_file in added_files:
        non_spill.add(source_file)
        spilled.add(source_file)

    assert not tuple(non_spill_root.iterdir())
    assert tuple(spill_root.iterdir())
    assert non_spill.finish(metadata_sha256) == expected
    assert spilled.finish(metadata_sha256) == expected
    assert not tuple(non_spill_root.iterdir())
    assert not tuple(spill_root.iterdir())


def test_spill_result_is_independent_of_add_chunk_boundaries(tmp_path: Path) -> None:
    metadata_sha256 = sha256(b"metadata").hexdigest()
    source_files = tuple(
        [_source_file("galleryinfo.txt", b"metadata")]
        + [
            _source_file(f"{index:04d}.jpg", f"page-{index % 3}".encode())
            for index in range(40)
        ]
    )
    expected = _legacy_v1_reference(source_files, metadata_sha256)

    for chunk_size in (1, 3, 17, len(source_files)):
        spill_root = tmp_path / f"spill-{chunk_size}"
        spill_root.mkdir()
        accumulator = CanonicalManifestAccumulator(
            memory_limit_bytes=512,
            temporary_directory=spill_root,
            merge_fan_in=2,
        )
        for offset in range(0, len(source_files), chunk_size):
            for source_file in source_files[offset : offset + chunk_size]:
                accumulator.add(source_file)

        assert accumulator.finish(metadata_sha256) == expected
        assert not tuple(spill_root.iterdir())


class _FailingSourceFile:
    @property
    def name(self) -> str:
        raise RuntimeError("injected source failure")

    size_bytes = 1
    sha256 = "0" * 64


def test_spill_files_are_cleaned_when_finish_fails(tmp_path: Path) -> None:
    spill_root = tmp_path / "spill"
    spill_root.mkdir()
    accumulator = CanonicalManifestAccumulator(
        memory_limit_bytes=1,
        temporary_directory=spill_root,
    )
    accumulator.add(_source_file("001.jpg", b"page"))
    assert tuple(spill_root.iterdir())

    with pytest.raises(TypeError):
        accumulator.finish(cast(str, object()))

    assert not tuple(spill_root.iterdir())


def test_spill_files_are_cleaned_when_add_fails(tmp_path: Path) -> None:
    spill_root = tmp_path / "spill"
    spill_root.mkdir()
    accumulator = CanonicalManifestAccumulator(
        memory_limit_bytes=1,
        temporary_directory=spill_root,
    )
    accumulator.add(_source_file("001.jpg", b"page"))
    assert tuple(spill_root.iterdir())

    with pytest.raises(RuntimeError, match="injected source failure"):
        accumulator.add(_FailingSourceFile())

    assert not tuple(spill_root.iterdir())
