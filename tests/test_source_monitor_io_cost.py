"""Independent real I/O counts distinguish marker polling from image reads."""

from __future__ import annotations

import os
import stat
from collections import Counter
from collections.abc import Iterator
from pathlib import Path

import pytest

from h2hdb_ingest.filesystem import FilesystemCompletionMarker, FilesystemSource
from h2hdb_ingest.metrics import IngestMetric
from h2hdb_ingest.source_monitor import FilesystemCompletionMarkerProbe, _MarkerIndex


def _checkpoint() -> None:
    pass


@pytest.mark.parametrize("galleries", [127, 128, 129])
@pytest.mark.parametrize("read_images", [False, True])
def test_polling_reads_all_markers_and_no_images_across_repeated_pages(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    galleries: int,
    read_images: bool,
) -> None:
    root = tmp_path / "source"
    root.mkdir()
    payload = b"completion marker bytes"
    for number in range(galleries):
        folder = root / str(number)
        folder.mkdir()
        (folder / "galleryinfo.txt").write_bytes(payload)
        (folder / "001.jpg").write_bytes(b"image content")
    counts: Counter[str] = Counter()
    files: dict[int, str] = {}
    real_open, real_read, real_close = os.open, os.read, os.close

    def opened(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        descriptor = real_open(path, flags, mode, dir_fd=dir_fd)
        if stat.S_ISREG(os.fstat(descriptor).st_mode):
            kind = (
                "marker" if os.fsdecode(path).endswith("galleryinfo.txt") else "image"
            )
            files[descriptor] = kind
            counts[f"{kind}_opens"] += 1
        return descriptor

    def read(descriptor: int, size: int) -> bytes:
        part = real_read(descriptor, size)
        if descriptor in files:
            kind = files[descriptor]
            counts[f"{kind}_reads"] += 1
            counts[f"{kind}_bytes"] += len(part)
        return part

    def close(descriptor: int) -> None:
        files.pop(descriptor, None)
        real_close(descriptor)

    monkeypatch.setattr(os, "open", opened)
    monkeypatch.setattr(os, "read", read)
    monkeypatch.setattr(os, "close", close)
    if read_images:
        real_markers = FilesystemSource.iter_completion_markers

        def regressed(
            source: FilesystemSource,
        ) -> Iterator[tuple[tuple[str, ...], FilesystemCompletionMarker]]:
            for locator, marker in real_markers(source):
                descriptor = os.open(root.joinpath(*locator, "001.jpg"), os.O_RDONLY)
                try:
                    os.read(descriptor, 1024)
                finally:
                    os.close(descriptor)
                yield locator, marker

        monkeypatch.setattr(FilesystemSource, "iter_completion_markers", regressed)

    records: list[IngestMetric] = []
    probe = FilesystemCompletionMarkerProbe(root, metrics_sink=records.append)
    index = _MarkerIndex(tmp_path / "markers.sqlite3")
    changed: list[bool] = []
    try:
        for cycle in range(5):
            if cycle == 2:
                (root / "0" / "001.jpg").write_bytes(b"changed image")
            if cycle == 3:
                (root / "0" / "galleryinfo.txt").write_bytes(b"x" + payload[1:])
            counts.clear()
            changed.clear()
            with probe(_checkpoint) as markers:
                index.reconcile(
                    markers,
                    changed=lambda: changed.append(True),
                    checkpoint=_checkpoint,
                )
            assert len(changed) == (galleries if cycle == 0 else int(cycle == 3))
            assert counts["marker_opens"] == galleries
            assert counts["marker_reads"] == 2 * galleries  # Payload and EOF.
            assert counts["marker_bytes"] == len(payload) * galleries
            metric = {item.name: item.value for item in records.pop().counters}
            assert metric["completion_marker_files_observed"] == galleries
            assert metric["completion_marker_bytes_observed"] == counts["marker_bytes"]
            if read_images:
                # Same scheduling outcome, but the independent cost oracle rejects
                # the deliberately regressed implementation's image I/O.
                with pytest.raises(AssertionError, match="image reads"):
                    assert counts["image_reads"] == 0, "unexpected image reads"
            else:
                assert counts["image_opens"] == 0
                assert counts["image_reads"] == 0
                assert counts["image_bytes"] == 0
    finally:
        index.close()
