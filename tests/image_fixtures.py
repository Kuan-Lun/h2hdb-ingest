"""Bounded-memory construction of genuine large encoded image fixtures."""

from __future__ import annotations

import struct
import zlib
from pathlib import Path
from typing import BinaryIO


def _png_chunk(stream: BinaryIO, kind: bytes, content: bytes) -> None:
    stream.write(struct.pack(">I", len(content)))
    stream.write(kind)
    stream.write(content)
    stream.write(struct.pack(">I", zlib.crc32(kind + content)))


def write_large_source_png(path: Path) -> int:
    """Write a valid 33 MiB RGB PNG using scanlines, without padding or a raster."""

    width, height = 4096, 2816
    row = b"\0" + b"\x31\x7a\xc3" * width
    compressor = zlib.compressobj(level=0)
    with path.open("wb") as stream:
        stream.write(b"\x89PNG\r\n\x1a\n")
        _png_chunk(
            stream, b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
        )
        for _ in range(height):
            compressed = compressor.compress(row)
            if compressed:
                _png_chunk(stream, b"IDAT", compressed)
        _png_chunk(stream, b"IDAT", compressor.flush())
        _png_chunk(stream, b"IEND", b"")
    size = path.stat().st_size
    assert size > 32 * 1024 * 1024
    return size
