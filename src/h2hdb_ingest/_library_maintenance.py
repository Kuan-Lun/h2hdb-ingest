"""Descriptor-pinned exclusive access shared by offline library maintenance."""

from __future__ import annotations

import os
import sqlite3
import stat
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path

from ._relocation_files import LibraryFiles

_DATABASE = ".h2hdb-state/journal/library-activation.sqlite3"


@contextmanager
def locked_library(
    root: Path,
) -> Iterator[tuple[LibraryFiles, sqlite3.Connection, Callable[[], None]]]:
    """Pin offline controls in the deployment's single-writer namespace.

    SQLite requires a pathname on supported platforms. Fresh checks detect
    persistent substitutions, but do not authorize concurrent namespace writers
    or claim protection against arbitrary rename-and-restore races during open.
    """
    # The deployment uses POSIX flock for both ingest and OPDS readers.
    import fcntl

    files = LibraryFiles(root)
    resources = ExitStack()
    resources.callback(files.close)
    descriptors: dict[str, int] = {}
    connection: sqlite3.Connection | None = None

    def revalidate() -> None:
        files.require_root()
        for path, held in descriptors.items():
            with files.parent(path) as (parent, leaf):
                if parent is None:
                    raise RuntimeError(
                        f"library maintenance directory disappeared: {path}"
                    )
                opened = os.fstat(held)
                named = os.stat(leaf, dir_fd=parent, follow_symlinks=False)
                if (
                    not stat.S_ISREG(opened.st_mode)
                    or not stat.S_ISREG(named.st_mode)
                    or opened.st_nlink != 1
                    or named.st_nlink != 1
                    or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
                ):
                    raise RuntimeError(
                        f"library maintenance file changed identity: {path}"
                    )
        files.require_root()

    try:
        for path in (
            ".h2hdb-coordination/publication.lock",
            ".h2hdb-state/locks/state.lock",
        ):
            with files.parent(path) as (parent, leaf):
                if parent is None:
                    raise RuntimeError(f"library lock directory is missing: {path}")
                descriptor = os.open(
                    leaf, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent
                )
                resources.callback(os.close, descriptor)
                descriptors[path] = descriptor
                value = os.fstat(descriptor)
                if not stat.S_ISREG(value.st_mode) or value.st_nlink != 1:
                    raise RuntimeError(
                        f"library lock is not a single-link regular file: {path}"
                    )
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                named = os.stat(leaf, dir_fd=parent, follow_symlinks=False)
                if value.st_nlink != 1 or (value.st_dev, value.st_ino) != (
                    named.st_dev,
                    named.st_ino,
                ):
                    raise RuntimeError(
                        f"library maintenance lock changed identity: {path}"
                    )
                os.fsync(descriptor)
                os.fsync(parent)
        with files.parent(_DATABASE) as (parent, leaf):
            if parent is None:
                raise RuntimeError("existing library journal is missing")
            descriptor = os.open(
                leaf, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent
            )
            resources.callback(os.close, descriptor)
            descriptors[_DATABASE] = descriptor
            database_identity = os.fstat(descriptor)
            if (
                not stat.S_ISREG(database_identity.st_mode)
                or database_identity.st_nlink != 1
            ):
                raise RuntimeError("library journal is not a single-link regular file")
            revalidate()
            connection = sqlite3.connect(
                (files.root / _DATABASE).as_uri() + "?mode=rw",
                uri=True,
                isolation_level=None,
            )
            resources.callback(connection.close)
            # Check the fresh root/ancestors and held locks before even the
            # first SQLite read: that read may recover a hot rollback journal.
            revalidate()
            if connection.execute("PRAGMA journal_mode").fetchone() != ("delete",):
                raise RuntimeError(
                    "library maintenance requires SQLite DELETE journal mode"
                )
            connection.execute("PRAGMA synchronous = FULL")
            if connection.execute("PRAGMA synchronous").fetchone() != (2,):
                raise RuntimeError(
                    "library maintenance requires SQLite FULL synchronization"
                )
            yield files, connection, revalidate
            revalidate()
            os.fsync(descriptor)
            os.fsync(parent)
            revalidate()
    finally:
        resources.close()
