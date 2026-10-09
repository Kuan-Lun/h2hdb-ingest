"""Bounded exact byte-stream operations shared by archive and image processing."""

from __future__ import annotations

from hashlib import sha256
from typing import BinaryIO

from .model import PresentationImageError

_COPY_BUFFER_BYTES = 1024 * 1024


def _copy_exact_bytes(
    source: BinaryIO,
    destination: BinaryIO,
    *,
    size: int,
    label: str,
) -> None:
    remaining = size
    while remaining:
        part = source.read(min(_COPY_BUFFER_BYTES, remaining))
        if type(part) is not bytes or not part:
            raise PresentationImageError(f"{label} ended before its exact size")
        _write_all(destination, part, label=label)
        remaining -= len(part)
    trailing = source.read(1)
    if type(trailing) is not bytes:
        raise PresentationImageError(f"{label} did not yield bytes")
    if trailing:
        raise PresentationImageError(f"{label} exceeds its exact size")


def _write_all(destination: BinaryIO, content: bytes, *, label: str) -> None:
    view = memoryview(content)
    offset = 0
    while offset < len(view):
        written = destination.write(view[offset:])
        if type(written) is not int or written <= 0 or written > len(view) - offset:
            raise PresentationImageError(f"{label} destination write made no progress")
        offset += written


def _read_extent(archive: BinaryIO, *, offset: int, size: int) -> bytes:
    archive.seek(offset)
    content = archive.read(size)
    if len(content) != size:
        raise PresentationImageError("page byte extent ended unexpectedly")
    return content


def _stream_digest(source: BinaryIO, size: int) -> bytes:
    digest = sha256()
    remaining = size
    while remaining:
        chunk = source.read(min(_COPY_BUFFER_BYTES, remaining))
        if not chunk:
            raise PresentationImageError("archive ended before its observed size")
        digest.update(chunk)
        remaining -= len(chunk)
    if source.read(1):
        raise PresentationImageError("archive grew while it was inspected")
    return digest.digest()
