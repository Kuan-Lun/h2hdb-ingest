"""Pure validation for deployment-provisioned library mount roots."""

from __future__ import annotations

import os
import stat
from pathlib import Path

CURRENT_DIRECTORY_NAME = "current"
ACQUISITIONS_DIRECTORY_NAME = "acquisitions"
ARTWORK_DIRECTORY_NAME = "artwork"
COORDINATION_DIRECTORY_NAME = ".h2hdb-coordination"
STATE_DIRECTORY_NAME = ".h2hdb-state"
UNSUPPORTED_LEGACY_COORDINATION_NAME = "coordination"
UNSUPPORTED_LEGACY_CURRENT_NAME = "hash-v1"

_DIRECTORY_FLAGS = (
    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
)


class LibraryLayoutValidationError(RuntimeError):
    """Raised when a host-provisioned library path is structurally unsafe."""


def validate_precreated_library_layout(root: Path, *, durable: bool) -> None:
    """Validate external roots without enforcing host permission policy."""

    root_visible = _lstat_required(root, label="library root")
    _require_directory_type(
        root_visible,
        path=root,
        label="library root",
    )
    try:
        root_descriptor = os.open(root, _DIRECTORY_FLAGS)
    except OSError as error:
        raise LibraryLayoutValidationError(
            f"library root is not safely openable: {root}"
        ) from error
    try:
        _require_opened_directory(
            root_descriptor,
            root_visible,
            path=root,
            label="library root",
        )
        for leaf, label in (
            (CURRENT_DIRECTORY_NAME, "current library"),
            (COORDINATION_DIRECTORY_NAME, "library coordination"),
        ):
            _validate_child_directory(
                root_descriptor,
                root,
                leaf,
                label=label,
                durable=durable,
            )
        _validate_current_subtrees(
            root_descriptor,
            root,
            durable=durable,
        )
        _reject_legacy_coordination(
            root_descriptor,
            root,
            durable=durable,
        )
        root_durable = os.fstat(root_descriptor)
        root_visible = root.lstat()
        _require_opened_directory(
            root_descriptor,
            root_visible,
            path=root,
            label="library root",
            opened=root_durable,
        )
    finally:
        os.close(root_descriptor)


def _validate_current_subtrees(
    root_descriptor: int,
    root: Path,
    *,
    durable: bool,
) -> None:
    current_path = root / CURRENT_DIRECTORY_NAME
    try:
        current_descriptor = os.open(
            CURRENT_DIRECTORY_NAME,
            _DIRECTORY_FLAGS,
            dir_fd=root_descriptor,
        )
    except OSError as error:
        raise LibraryLayoutValidationError(
            f"current library is not safely openable: {current_path}"
        ) from error
    try:
        _reject_legacy_current_layout(current_descriptor, current_path)
        for leaf, label in (
            (ACQUISITIONS_DIRECTORY_NAME, "acquisition library"),
            (ARTWORK_DIRECTORY_NAME, "artwork library"),
        ):
            _validate_child_directory(
                current_descriptor,
                current_path,
                leaf,
                label=label,
                durable=durable,
            )
        if durable:
            os.fsync(current_descriptor)
            os.fsync(root_descriptor)
    finally:
        os.close(current_descriptor)


def _reject_legacy_current_layout(
    current_descriptor: int,
    current_path: Path,
) -> None:
    legacy_path = current_path / UNSUPPORTED_LEGACY_CURRENT_NAME
    try:
        os.stat(
            UNSUPPORTED_LEGACY_CURRENT_NAME,
            dir_fd=current_descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        return
    raise LibraryLayoutValidationError(
        "unsupported legacy current/hash-v1 artifact layout at "
        f"{legacy_path}; rebuild into a fresh library root because automatic "
        "migration is not supported"
    )


def _validate_child_directory(
    parent_descriptor: int,
    parent: Path,
    leaf: str,
    *,
    label: str,
    durable: bool,
) -> None:
    path = parent / leaf
    visible = _stat_at_required(parent_descriptor, leaf, path=path, label=label)
    _require_directory_type(
        visible,
        path=path,
        label=label,
    )
    try:
        descriptor = os.open(leaf, _DIRECTORY_FLAGS, dir_fd=parent_descriptor)
    except OSError as error:
        raise LibraryLayoutValidationError(
            f"{label} is not safely openable: {path}"
        ) from error
    try:
        _require_opened_directory(
            descriptor,
            visible,
            path=path,
            label=label,
        )
        if durable:
            os.fsync(descriptor)
            os.fsync(parent_descriptor)
        durable_value = os.fstat(descriptor)
        visible = os.stat(leaf, dir_fd=parent_descriptor, follow_symlinks=False)
        _require_opened_directory(
            descriptor,
            visible,
            path=path,
            label=label,
            opened=durable_value,
        )
    finally:
        os.close(descriptor)


def _reject_legacy_coordination(
    root_descriptor: int,
    root: Path,
    *,
    durable: bool,
) -> None:
    state_path = root / STATE_DIRECTORY_NAME
    try:
        visible = os.stat(
            STATE_DIRECTORY_NAME,
            dir_fd=root_descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        return
    _require_directory_type(
        visible,
        path=state_path,
        label="library state",
    )
    try:
        state_descriptor = os.open(
            STATE_DIRECTORY_NAME,
            _DIRECTORY_FLAGS,
            dir_fd=root_descriptor,
        )
    except OSError as error:
        raise LibraryLayoutValidationError(
            f"library state is not safely openable: {state_path}"
        ) from error
    try:
        _require_opened_directory(
            state_descriptor,
            visible,
            path=state_path,
            label="library state",
        )
        try:
            os.stat(
                UNSUPPORTED_LEGACY_COORDINATION_NAME,
                dir_fd=state_descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            pass
        else:
            raise LibraryLayoutValidationError(
                "unsupported legacy library coordination layout; "
                "a fresh library root is required"
            )
        if durable:
            os.fsync(state_descriptor)
            os.fsync(root_descriptor)
        durable_value = os.fstat(state_descriptor)
        visible = os.stat(
            STATE_DIRECTORY_NAME,
            dir_fd=root_descriptor,
            follow_symlinks=False,
        )
        _require_opened_directory(
            state_descriptor,
            visible,
            path=state_path,
            label="library state",
            opened=durable_value,
        )
    finally:
        os.close(state_descriptor)


def _lstat_required(path: Path, *, label: str) -> os.stat_result:
    try:
        return path.lstat()
    except FileNotFoundError as error:
        raise LibraryLayoutValidationError(
            f"{label} must be a pre-existing real directory: {path}"
        ) from error


def _stat_at_required(
    parent_descriptor: int,
    leaf: str,
    *,
    path: Path,
    label: str,
) -> os.stat_result:
    try:
        return os.stat(leaf, dir_fd=parent_descriptor, follow_symlinks=False)
    except FileNotFoundError as error:
        raise LibraryLayoutValidationError(
            f"{label} must be a pre-existing real directory: {path}"
        ) from error


def _require_opened_directory(
    descriptor: int,
    visible: os.stat_result,
    *,
    path: Path,
    label: str,
    opened: os.stat_result | None = None,
) -> None:
    opened = opened or os.fstat(descriptor)
    if (opened.st_dev, opened.st_ino) != (visible.st_dev, visible.st_ino):
        raise LibraryLayoutValidationError(f"{label} changed identity: {path}")
    _require_directory_type(
        opened,
        path=path,
        label=label,
    )
    _require_directory_type(
        visible,
        path=path,
        label=label,
    )


def _require_directory_type(
    value: os.stat_result,
    *,
    path: Path,
    label: str,
) -> None:
    if not stat.S_ISDIR(value.st_mode) or stat.S_ISLNK(value.st_mode):
        raise LibraryLayoutValidationError(f"{label} is not a real directory: {path}")
