"""Process-owned disk scratch with bounded recovery of abandoned workspaces."""

from __future__ import annotations

import errno
import fcntl
import json
import logging
import os
import re
import stat
import tempfile
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from threading import Lock
from types import TracebackType
from uuid import uuid4

from ._library_layout import STATE_DIRECTORY_NAME, validate_precreated_library_layout
from ._resource_cleanup import close_resources, owned_resource

_SCRATCH_NAME = "scratch-v1"
_REGISTRY_LOCK = "registry.lock"
_OWNER_NAME = "owner.json"
_DATA_NAME = "data"
_RUN_NAME = re.compile(r"run-[0-9a-f]{32}")
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
_FILE_FLAGS = os.O_RDWR | os.O_NOFOLLOW
_OWNER_MAX_BYTES = 512
_MAX_CLEANUP_PAGE = 128
_PROCESS_OWNER = Lock()
logger = logging.getLogger(__name__)


class ScratchSafetyError(RuntimeError):
    """The managed scratch namespace no longer has its verified identity."""


@dataclass(frozen=True, slots=True)
class ScratchCleanupResult:
    checked_entries: int
    removed_entries: int
    pending: bool


@dataclass(frozen=True, slots=True)
class _Cleanup:
    action: Callable[[], object]

    def close(self) -> None:
        self.action()


def _identity(value: os.stat_result) -> tuple[int, int]:
    return value.st_dev, value.st_ino


def _require_directory(descriptor: int, parent: int, leaf: str) -> os.stat_result:
    opened = os.fstat(descriptor)
    visible = os.stat(leaf, dir_fd=parent, follow_symlinks=False)
    if (
        not stat.S_ISDIR(opened.st_mode)
        or not stat.S_ISDIR(visible.st_mode)
        or _identity(opened) != _identity(visible)
    ):
        raise ScratchSafetyError(f"scratch directory changed identity: {leaf!r}")
    return opened


def _require_file(descriptor: int, parent: int, leaf: str) -> os.stat_result:
    opened = os.fstat(descriptor)
    visible = os.stat(leaf, dir_fd=parent, follow_symlinks=False)
    if (
        not stat.S_ISREG(opened.st_mode)
        or not stat.S_ISREG(visible.st_mode)
        or opened.st_nlink != 1
        or _identity(opened) != _identity(visible)
    ):
        raise ScratchSafetyError(f"scratch control file changed identity: {leaf!r}")
    return opened


def _open_directory(parent: int, leaf: str, *, create: bool) -> int:
    if create:
        try:
            os.mkdir(leaf, 0o700, dir_fd=parent)
        except FileExistsError:
            pass
    descriptor = os.open(leaf, _DIRECTORY_FLAGS, dir_fd=parent)
    try:
        _require_directory(descriptor, parent, leaf)
        if create:
            os.fsync(descriptor)
            os.fsync(parent)
            _require_directory(descriptor, parent, leaf)
        return descriptor
    except BaseException as error:
        close_resources((_Cleanup(partial(os.close, descriptor)),), error=error)
        raise


@contextmanager
def _registry(descriptor: int) -> Iterator[None]:
    fcntl.flock(descriptor, fcntl.LOCK_EX)
    with owned_resource(_Cleanup(partial(fcntl.flock, descriptor, fcntl.LOCK_UN))):
        yield


def _owner_payload(leaf: str, run: os.stat_result, data: os.stat_result) -> bytes:
    return json.dumps(
        ["h2hdb-disk-scratch-v1", leaf, *_identity(run), *_identity(data)],
        separators=(",", ":"),
    ).encode("ascii")


@dataclass(slots=True)
class _Frame:
    descriptor: int
    parent: int
    leaf: str
    entries: Iterator[os.DirEntry[str]]
    close: Callable[[], None]
    blocked: bool = False


def _frame(parent: int, leaf: str) -> _Frame:
    owned = ExitStack()
    try:
        descriptor = _open_directory(parent, leaf, create=False)
        owned.callback(os.close, descriptor)
        entries = owned.enter_context(os.scandir(descriptor))
        return _Frame(descriptor, parent, leaf, entries, owned.close)
    except BaseException as error:
        close_resources((owned,), error=error)
        raise


class _Reaper:
    def __init__(
        self,
        parent: int,
        leaf: str,
        run_descriptor: int,
        owner_descriptor: int,
        data_identity: tuple[int, int],
    ) -> None:
        self.parent = parent
        self.leaf = leaf
        self.run_descriptor = run_descriptor
        self.owner_descriptor = owner_descriptor
        self.data_identity = data_identity
        self.frames: list[_Frame] = []
        self.closed = False
        self.retiring = False
        try:
            frame = _frame(run_descriptor, _DATA_NAME)
        except FileNotFoundError:
            # Recovery after data rmdir but before the control files were retired.
            pass
        else:
            try:
                if _identity(os.fstat(frame.descriptor)) != data_identity:
                    raise ScratchSafetyError("scratch data directory changed identity")
            except BaseException as error:
                close_resources((frame,), error=error)
                raise
            self.frames.append(frame)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        frames, self.frames = self.frames, []
        close_resources(
            (
                *reversed(frames),
                _Cleanup(partial(os.close, self.owner_descriptor)),
                _Cleanup(partial(os.close, self.run_descriptor)),
            )
        )

    def _verify(self) -> None:
        _require_directory(self.run_descriptor, self.parent, self.leaf)
        if not self.retiring:
            _require_file(self.owner_descriptor, self.run_descriptor, _OWNER_NAME)
        for frame in self.frames:
            _require_directory(frame.descriptor, frame.parent, frame.leaf)

    def step(self) -> tuple[int, bool]:
        """Perform one bounded directory-entry or directory-retirement step."""
        self._verify()
        if not self.frames:
            # Only the verified owner file may remain in an owned run root.
            with os.scandir(self.run_descriptor) as entries:
                if any(entry.name != _OWNER_NAME for entry in entries):
                    return 0, True
            if not self.retiring:
                _require_file(self.owner_descriptor, self.run_descriptor, _OWNER_NAME)
                os.unlink(_OWNER_NAME, dir_fd=self.run_descriptor)
                os.fsync(self.run_descriptor)
                self.retiring = True
                return 1, False
            _require_directory(self.run_descriptor, self.parent, self.leaf)
            os.rmdir(self.leaf, dir_fd=self.parent)
            os.fsync(self.parent)
            return 1, True
        frame = self.frames[-1]
        try:
            entry = next(frame.entries)
        except StopIteration:
            self.frames.pop()
            with owned_resource(frame):
                _require_directory(frame.descriptor, frame.parent, frame.leaf)
                if frame.blocked:
                    if self.frames:
                        self.frames[-1].blocked = True
                    else:
                        return 0, True
                    return 0, False
                os.rmdir(frame.leaf, dir_fd=frame.parent)
                os.fsync(frame.parent)
                return 1, False
        visible = os.stat(entry.name, dir_fd=frame.descriptor, follow_symlinks=False)
        if stat.S_ISDIR(visible.st_mode) and visible.st_dev == self.data_identity[0]:
            child = _frame(frame.descriptor, entry.name)
            try:
                if _identity(os.fstat(child.descriptor)) != _identity(visible):
                    raise ScratchSafetyError("scratch descendant changed identity")
            except BaseException as error:
                close_resources((child,), error=error)
                raise
            self.frames.append(child)
            return 0, False
        if not stat.S_ISREG(visible.st_mode) or visible.st_nlink != 1:
            # Never follow symlinks, cross mounts or remove foreign hard links.
            frame.blocked = True
            return 0, False
        confirmed = os.stat(entry.name, dir_fd=frame.descriptor, follow_symlinks=False)
        if (
            _identity(confirmed) != _identity(visible)
            or confirmed.st_mode != visible.st_mode
            or confirmed.st_nlink != 1
        ):
            raise ScratchSafetyError("scratch file changed identity before removal")
        os.unlink(entry.name, dir_fd=frame.descriptor)
        os.fsync(frame.descriptor)
        return 1, False


class DiskScratch:
    """Route process-wide temporary I/O into a private, leased library workspace.

    Enter once around the complete CLI/runtime lifetime, before native temporary
    storage is initialized. All worker threads share this setting. Close workers
    before exiting. Foreign live owners are retained, and unmarked directories
    are never adopted; initialization crashes can therefore leave empty unknown
    directories but cannot leave unowned source bytes. Cleanup resumes in bounded
    pages and does not require retaining Python objects across process restarts.
    """

    def __init__(self, library_path: Path) -> None:
        self._library = Path(os.path.abspath(library_path))
        self._owned = ExitStack()
        self._cleanup_lock = Lock()
        self._root = -1
        self._state = -1
        self._scratch = -1
        self._registry = -1
        self._owner = -1
        self._path: Path | None = None
        self._entered = False
        self._closed = False
        self._redirected = False
        self._previous_tempdir: str | None = None
        self._previous_tmpdir: str | None = None
        self._scan: _Frame | None = None
        self._reaper: _Reaper | None = None

    @property
    def path(self) -> Path:
        if self._path is None or self._closed:
            raise RuntimeError("scratch context is not active")
        return self._path

    def _own_directory(self, parent: int, leaf: str, *, create: bool) -> int:
        descriptor = _open_directory(parent, leaf, create=create)
        self._owned.callback(os.close, descriptor)
        return descriptor

    def _verify_roots(self) -> None:
        visible = self._library.lstat()
        opened = os.fstat(self._root)
        if not stat.S_ISDIR(visible.st_mode) or _identity(visible) != _identity(opened):
            raise ScratchSafetyError("library root changed while scratch was active")
        _require_directory(self._state, self._root, STATE_DIRECTORY_NAME)
        _require_directory(self._scratch, self._state, _SCRATCH_NAME)
        _require_file(self._registry, self._scratch, _REGISTRY_LOCK)

    def __enter__(self) -> DiskScratch:
        if self._entered or self._closed:
            raise RuntimeError("scratch context cannot be re-entered")
        if not _PROCESS_OWNER.acquire(blocking=False):
            raise RuntimeError("another process-wide scratch context is already active")
        self._entered = True
        try:
            expected_root = self._library.lstat()
            validate_precreated_library_layout(self._library, durable=True)
            self._root = os.open(self._library, _DIRECTORY_FLAGS)
            self._owned.callback(os.close, self._root)
            if _identity(os.fstat(self._root)) != _identity(expected_root):
                raise ScratchSafetyError(
                    "library root changed during scratch initialization"
                )
            self._state = self._own_directory(
                self._root, STATE_DIRECTORY_NAME, create=True
            )
            self._scratch = self._own_directory(self._state, _SCRATCH_NAME, create=True)
            self._registry = os.open(
                _REGISTRY_LOCK, _FILE_FLAGS | os.O_CREAT, 0o600, dir_fd=self._scratch
            )
            self._owned.callback(os.close, self._registry)
            _require_file(self._registry, self._scratch, _REGISTRY_LOCK)
            os.fsync(self._registry)
            os.fsync(self._scratch)
            # Recover abandoned bytes before allocating a new workspace: a prior
            # killed process may have exhausted the available scratch filesystem.
            while self.cleanup_page().pending:
                pass
            with _registry(self._registry):
                self._verify_roots()
                self._create_run()
            self._previous_tmpdir = os.environ.get("TMPDIR")
            self._previous_tempdir = tempfile.tempdir
            self._redirected = True
            os.environ["TMPDIR"] = str(self.path)
            tempfile.tempdir = str(self.path)
            logger.info(
                "Working files use disk scratch at %s; abandoned owned files "
                "are recovered at startup and during resident maintenance",
                self.path,
            )
            return self
        except BaseException as error:
            close_resources(
                (
                    _Cleanup(self._restore),
                    _Cleanup(self._dispose),
                    _Cleanup(self._finish),
                ),
                error=error,
            )
            raise

    def _create_run(self) -> None:
        leaf = "run-" + uuid4().hex
        os.mkdir(leaf, 0o700, dir_fd=self._scratch)
        run = self._own_directory(self._scratch, leaf, create=False)
        data = self._own_directory(run, _DATA_NAME, create=True)
        self._owner = os.open(
            _OWNER_NAME, _FILE_FLAGS | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=run
        )
        fcntl.flock(self._owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        payload = _owner_payload(leaf, os.fstat(run), os.fstat(data))
        if os.write(self._owner, payload) != len(payload):
            raise OSError("scratch owner marker was only partially written")
        os.fsync(self._owner)
        os.fsync(data)
        os.fsync(run)
        os.fsync(self._scratch)
        _require_directory(run, self._scratch, leaf)
        _require_directory(data, run, _DATA_NAME)
        _require_file(self._owner, run, _OWNER_NAME)
        self._path = (
            self._library / STATE_DIRECTORY_NAME / _SCRATCH_NAME / leaf / _DATA_NAME
        )

    def _claim(self, leaf: str) -> _Reaper | None:
        if _RUN_NAME.fullmatch(leaf) is None:
            return None
        with owned_resource(ExitStack()) as owned:
            try:
                run = _open_directory(self._scratch, leaf, create=False)
                owned.callback(os.close, run)
                visible_owner = os.stat(_OWNER_NAME, dir_fd=run, follow_symlinks=False)
                if (
                    not stat.S_ISREG(visible_owner.st_mode)
                    or visible_owner.st_nlink != 1
                ):
                    return None
                owner = os.open(_OWNER_NAME, _FILE_FLAGS, dir_fd=run)
                owned.callback(os.close, owner)
                fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
                value = _require_file(owner, run, _OWNER_NAME)
                if not 1 <= value.st_size <= _OWNER_MAX_BYTES:
                    return None
                raw = os.read(owner, _OWNER_MAX_BYTES + 1)
                try:
                    record = json.loads(raw)
                except ValueError, UnicodeDecodeError:
                    return None
                if (
                    not isinstance(record, list)
                    or len(record) != 6
                    or record[:2] != ["h2hdb-disk-scratch-v1", leaf]
                    or any(type(item) is not int or item < 0 for item in record[2:])
                    or tuple(record[2:4]) != _identity(os.fstat(run))
                    or json.dumps(record, separators=(",", ":")).encode("ascii") != raw
                ):
                    return None
                with os.scandir(run) as entries:
                    if any(
                        entry.name not in {_OWNER_NAME, _DATA_NAME} for entry in entries
                    ):
                        return None
                reaper = _Reaper(
                    self._scratch, leaf, run, owner, (record[4], record[5])
                )
                owned.pop_all()
                return reaper
            except OSError as error:
                if error.errno in {
                    errno.EAGAIN,
                    errno.EACCES,
                    errno.ENOENT,
                    errno.ENOTDIR,
                    errno.ELOOP,
                }:
                    return None
                raise
            except ScratchSafetyError:
                return None

    def cleanup_page(self, *, limit: int = _MAX_CLEANUP_PAGE) -> ScratchCleanupResult:
        if type(limit) is not int or not 1 <= limit <= _MAX_CLEANUP_PAGE:
            raise ValueError("scratch cleanup limit must be from 1 through 128")
        if not self._entered or self._closed:
            raise RuntimeError("scratch context is not active")
        checked = removed = 0
        with self._cleanup_lock, _registry(self._registry):
            self._verify_roots()
            if self._scan is None:
                self._scan = _frame(self._state, _SCRATCH_NAME)
            while checked < limit:
                checked += 1
                if self._reaper is not None:
                    try:
                        count, done = self._reaper.step()
                    except BaseException as error:
                        reaper, self._reaper = self._reaper, None
                        close_resources((reaper,), error=error)
                        raise
                    removed += count
                    if done:
                        reaper, self._reaper = self._reaper, None
                        reaper.close()
                    continue
                try:
                    entry = next(self._scan.entries)
                except StopIteration:
                    scan, self._scan = self._scan, None
                    scan.close()
                    return ScratchCleanupResult(checked, removed, False)
                self._reaper = self._claim(entry.name)
            return ScratchCleanupResult(checked, removed, True)

    def _restore(self) -> None:
        if not self._redirected:
            return
        self._redirected = False
        tempfile.tempdir = self._previous_tempdir
        if self._previous_tmpdir is None:
            os.environ.pop("TMPDIR", None)
        else:
            os.environ["TMPDIR"] = self._previous_tmpdir

    def _release_workspace(self) -> None:
        owner, self._owner = self._owner, -1
        scan, self._scan = self._scan, None
        close_resources(
            (
                *(() if owner < 0 else (_Cleanup(partial(os.close, owner)),)),
                *(() if scan is None else (scan,)),
            )
        )

    def _dispose(self) -> None:
        reaper, self._reaper = self._reaper, None
        close_resources(
            (
                *(() if reaper is None else (reaper,)),
                _Cleanup(self._release_workspace),
                self._owned,
            )
        )

    def _finish(self) -> None:
        self._closed = True
        _PROCESS_OWNER.release()

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exception_type, traceback
        close_resources(
            (
                _Cleanup(self._restore),
                _Cleanup(self._release_workspace),
                _Cleanup(self.cleanup_page),
                _Cleanup(self._dispose),
                _Cleanup(self._finish),
            ),
            error=exception,
        )
