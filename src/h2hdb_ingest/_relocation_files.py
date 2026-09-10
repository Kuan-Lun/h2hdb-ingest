"""Descriptor-relative, bounded-memory I/O used by offline relocation."""

from __future__ import annotations

import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path, PurePosixPath

_BUFFER = 1024 * 1024


def unsigned(value: int) -> bytes:
    return value.to_bytes(8, "big", signed=False)


@dataclass(frozen=True, slots=True)
class Signature:
    device: bytes
    inode: bytes
    size: int
    modified_ns: int
    changed_ns: int

    @classmethod
    def of(cls, value: os.stat_result) -> Signature:
        return cls(
            unsigned(value.st_dev),
            unsigned(value.st_ino),
            value.st_size,
            value.st_mtime_ns,
            value.st_ctime_ns,
        )

    def sql(self) -> tuple[bytes, bytes, int, int]:
        return self.device, self.inode, self.modified_ns, self.changed_ns

    def same_inode(self, other: Signature) -> bool:
        return (self.device, self.inode, self.size, self.modified_ns) == (
            other.device,
            other.inode,
            other.size,
            other.modified_ns,
        )


@dataclass(frozen=True, slots=True)
class ObservedFile:
    relative_path: str
    digest: bytes | None
    signature: Signature | None
    old_signature: Signature | None = None


def _regular(value: os.stat_result, path: str) -> None:
    if not stat.S_ISREG(value.st_mode) or value.st_nlink != 1:
        raise RuntimeError(f"relocation requires a regular single-link file: {path}")


class LibraryFiles:
    """Pin the mount root and reject changed names throughout every operation."""

    def __init__(self, root: Path) -> None:
        self.root = root.absolute()
        self.descriptor = os.open(
            self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        )
        self.identity = os.fstat(self.descriptor)
        try:
            self.require_root()
        except BaseException:
            os.close(self.descriptor)
            raise

    def close(self) -> None:
        os.close(self.descriptor)

    def require_root(self) -> None:
        value = self.root.lstat()
        if not stat.S_ISDIR(value.st_mode) or (value.st_dev, value.st_ino) != (
            self.identity.st_dev,
            self.identity.st_ino,
        ):
            raise RuntimeError("library relocation root changed identity")

    @contextmanager
    def parent(self, path: str) -> Iterator[tuple[int | None, str]]:
        parts = PurePosixPath(path).parts
        if (
            not parts
            or path != "/".join(parts)
            or any(part in {".", "..", ""} or "\\" in part for part in parts)
        ):
            raise RuntimeError(f"unsafe library relocation path: {path}")
        opened = [os.dup(self.descriptor)]
        missing = False
        missing_component = ""
        try:
            for component in parts[:-1]:
                try:
                    descriptor = os.open(
                        component,
                        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                        dir_fd=opened[-1],
                    )
                except FileNotFoundError:
                    os.fsync(opened[-1])
                    missing = True
                    missing_component = component
                    break
                opened.append(descriptor)
            yield (None if missing else opened[-1]), parts[-1]
            self.require_root()
            if missing:
                self._require_absent(opened[-1], missing_component)
            for index, component in enumerate(parts[: len(opened) - 1]):
                named = os.stat(component, dir_fd=opened[index], follow_symlinks=False)
                pinned = os.fstat(opened[index + 1])
                if not stat.S_ISDIR(named.st_mode) or (named.st_dev, named.st_ino) != (
                    pinned.st_dev,
                    pinned.st_ino,
                ):
                    raise RuntimeError(
                        f"library directory changed during relocation: {path}"
                    )
        finally:
            for descriptor in reversed(opened):
                os.close(descriptor)

    def observe(self, path: str, *, maximum_size: int) -> ObservedFile:
        with self.parent(path) as (parent, leaf):
            if parent is None:
                return ObservedFile(path, None, None)
            try:
                before = os.stat(leaf, dir_fd=parent, follow_symlinks=False)
            except FileNotFoundError:
                os.fsync(parent)
                self._require_absent(parent, leaf)
                return ObservedFile(path, None, None)
            _regular(before, path)
            if before.st_size > maximum_size:
                raise RuntimeError(
                    f"library relocation file exceeds authorized size: {path}"
                )
            descriptor = os.open(
                leaf, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent
            )
            try:
                opened = os.fstat(descriptor)
                _regular(opened, path)
                if Signature.of(opened) != Signature.of(before):
                    raise RuntimeError(
                        f"library relocation file changed while opening: {path}"
                    )
                digest = sha256()
                remaining = maximum_size + 1
                while remaining:
                    part = os.read(descriptor, min(_BUFFER, remaining))
                    if not part:
                        break
                    remaining -= len(part)
                    digest.update(part)
                after = os.fstat(descriptor)
                if remaining == 0 or Signature.of(after) != Signature.of(opened):
                    raise RuntimeError(
                        f"library relocation file changed while hashing: {path}"
                    )
                os.fsync(descriptor)
                os.fsync(parent)
                named = os.stat(leaf, dir_fd=parent, follow_symlinks=False)
                _regular(named, path)
                if Signature.of(named) != Signature.of(after):
                    raise RuntimeError(
                        f"library relocation file changed after hashing: {path}"
                    )
                return ObservedFile(path, digest.digest(), Signature.of(after))
            finally:
                os.close(descriptor)

    def require_observed(self, observed: ObservedFile) -> None:
        with self.parent(observed.relative_path) as (parent, leaf):
            if parent is None:
                if observed.signature is not None:
                    raise RuntimeError(
                        f"relocated directory disappeared: {observed.relative_path}"
                    )
                return
            if observed.signature is None:
                os.fsync(parent)
                self._require_absent(parent, leaf)
                return
            value = os.stat(leaf, dir_fd=parent, follow_symlinks=False)
            _regular(value, observed.relative_path)
            if Signature.of(value) != observed.signature:
                raise RuntimeError(
                    f"relocated file changed identity: {observed.relative_path}"
                )

    def read_control(self, path: str, *, maximum_size: int = 4096) -> bytes | None:
        with self.parent(path) as (parent, leaf):
            if parent is None:
                return None
            try:
                descriptor = os.open(
                    leaf, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent
                )
            except FileNotFoundError:
                return None
            try:
                before = os.fstat(descriptor)
                _regular(before, path)
                if before.st_size > maximum_size:
                    raise RuntimeError(f"library control file is oversized: {path}")
                result = os.read(descriptor, maximum_size + 1)
                if len(result) != before.st_size:
                    raise RuntimeError(
                        f"library control file changed while reading: {path}"
                    )
                named = os.stat(leaf, dir_fd=parent, follow_symlinks=False)
                if Signature.of(before) != Signature.of(named):
                    raise RuntimeError(f"library control file changed identity: {path}")
                os.fsync(descriptor)
                os.fsync(parent)
                return result
            finally:
                os.close(descriptor)

    def create_control(self, path: str, payload: bytes) -> None:
        """Durably finish the exact session-owned marker, including short writes."""
        with self.parent(path) as (parent, leaf):
            if parent is None:
                raise RuntimeError(f"missing library control directory: {path}")
            try:
                descriptor = os.open(
                    leaf,
                    os.O_WRONLY
                    | os.O_CREAT
                    | os.O_EXCL
                    | os.O_NOFOLLOW
                    | os.O_NONBLOCK,
                    0o600,
                    dir_fd=parent,
                )
                offset = 0
            except FileExistsError:
                prefix = self.read_control(path, maximum_size=len(payload))
                if prefix is None or not payload.startswith(prefix):
                    raise RuntimeError(
                        "library relocation marker has foreign contents"
                    ) from None
                descriptor = os.open(
                    leaf, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent
                )
                offset = len(prefix)
                try:
                    if os.read(descriptor, len(payload) + 1) != prefix:
                        raise RuntimeError(
                            "library relocation marker changed before replay"
                        )
                except BaseException:
                    os.close(descriptor)
                    raise
            try:
                before = os.fstat(descriptor)
                _regular(before, path)
                if before.st_size != offset:
                    raise RuntimeError(
                        "library relocation marker changed during replay"
                    )
                os.lseek(descriptor, offset, os.SEEK_SET)
                view = memoryview(payload)[offset:]
                while view:
                    written = os.write(descriptor, view)
                    if written == 0:
                        raise RuntimeError(
                            "library relocation marker write made no progress"
                        )
                    view = view[written:]
                os.fsync(descriptor)
                opened = os.fstat(descriptor)
                named = os.stat(leaf, dir_fd=parent, follow_symlinks=False)
                _regular(named, path)
                if Signature.of(opened) != Signature.of(named):
                    raise RuntimeError(
                        "library relocation marker changed while creating"
                    )
                os.fsync(parent)
            finally:
                os.close(descriptor)
            if self.read_control(path, maximum_size=len(payload)) != payload:
                raise RuntimeError("library relocation marker changed after creation")

    def remove_control(self, path: str, expected: bytes) -> None:
        observed = self.observe(path, maximum_size=len(expected))
        if observed.signature is None:
            return
        if observed.digest != sha256(
            expected
        ).digest() or observed.signature.size != len(expected):
            raise RuntimeError("refusing to remove a foreign library relocation marker")
        with self.parent(path) as (parent, leaf):
            if parent is None:
                raise RuntimeError("library relocation marker directory disappeared")
            self.require_observed(observed)
            os.unlink(leaf, dir_fd=parent)
            os.fsync(parent)
            self._require_absent(parent, leaf)

    @staticmethod
    def _require_absent(parent: int, leaf: str) -> None:
        try:
            os.stat(leaf, dir_fd=parent, follow_symlinks=False)
        except FileNotFoundError:
            return
        raise RuntimeError(
            f"unexpected library file appeared during relocation: {leaf}"
        )
