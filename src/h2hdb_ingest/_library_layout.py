"""Pure validation for deployment-provisioned library mount roots."""

from __future__ import annotations

import os
import stat
from pathlib import Path

LIBRARY_ROOT_MODE = 0o700
PUBLIC_DIRECTORY_MODE = 0o755
PRIVATE_MODE = 0o700
CURRENT_DIRECTORY_NAME = "current"
COORDINATION_DIRECTORY_NAME = ".h2hdb-coordination"
STATE_DIRECTORY_NAME = ".h2hdb-state"
UNSUPPORTED_LEGACY_COORDINATION_NAME = "coordination"

_DIRECTORY_FLAGS = (
    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
)


class LibraryLayoutValidationError(RuntimeError):
    """Raised when host-provisioned library metadata is not authoritative."""


def validate_precreated_library_layout(root: Path, *, durable: bool) -> None:
    """Validate external roots without creating or changing their metadata."""

    expected_uid = os.geteuid()
    expected_gid = os.getegid()
    root_visible = _lstat_required(root, label="library root")
    _require_directory_metadata(
        root_visible,
        path=root,
        label="library root",
        expected_uid=expected_uid,
        expected_gid=expected_gid,
        expected_mode=LIBRARY_ROOT_MODE,
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
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            expected_mode=LIBRARY_ROOT_MODE,
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
                expected_uid=expected_uid,
                expected_gid=expected_gid,
                expected_mode=PUBLIC_DIRECTORY_MODE,
                durable=durable,
            )
        _reject_legacy_coordination(
            root_descriptor,
            root,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            durable=durable,
        )
        root_durable = os.fstat(root_descriptor)
        root_visible = root.lstat()
        _require_opened_directory(
            root_descriptor,
            root_visible,
            path=root,
            label="library root",
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            expected_mode=LIBRARY_ROOT_MODE,
            opened=root_durable,
        )
    finally:
        os.close(root_descriptor)


def _validate_child_directory(
    parent_descriptor: int,
    parent: Path,
    leaf: str,
    *,
    label: str,
    expected_uid: int,
    expected_gid: int,
    expected_mode: int,
    durable: bool,
) -> None:
    path = parent / leaf
    visible = _stat_at_required(parent_descriptor, leaf, path=path, label=label)
    _require_directory_metadata(
        visible,
        path=path,
        label=label,
        expected_uid=expected_uid,
        expected_gid=expected_gid,
        expected_mode=expected_mode,
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
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            expected_mode=expected_mode,
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
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            expected_mode=expected_mode,
            opened=durable_value,
        )
    finally:
        os.close(descriptor)


def _reject_legacy_coordination(
    root_descriptor: int,
    root: Path,
    *,
    expected_uid: int,
    expected_gid: int,
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
    _require_directory_metadata(
        visible,
        path=state_path,
        label="library state",
        expected_uid=expected_uid,
        expected_gid=expected_gid,
        expected_mode=PRIVATE_MODE,
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
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            expected_mode=PRIVATE_MODE,
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
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            expected_mode=PRIVATE_MODE,
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
    expected_uid: int,
    expected_gid: int,
    expected_mode: int,
    opened: os.stat_result | None = None,
) -> None:
    opened = opened or os.fstat(descriptor)
    if (opened.st_dev, opened.st_ino) != (visible.st_dev, visible.st_ino):
        raise LibraryLayoutValidationError(f"{label} changed identity: {path}")
    _require_directory_metadata(
        opened,
        path=path,
        label=label,
        expected_uid=expected_uid,
        expected_gid=expected_gid,
        expected_mode=expected_mode,
    )
    _require_directory_metadata(
        visible,
        path=path,
        label=label,
        expected_uid=expected_uid,
        expected_gid=expected_gid,
        expected_mode=expected_mode,
    )


def _require_directory_metadata(
    value: os.stat_result,
    *,
    path: Path,
    label: str,
    expected_uid: int,
    expected_gid: int,
    expected_mode: int,
) -> None:
    if not stat.S_ISDIR(value.st_mode) or stat.S_ISLNK(value.st_mode):
        raise LibraryLayoutValidationError(f"{label} is not a real directory: {path}")
    actual_mode = stat.S_IMODE(value.st_mode)
    violations: list[str] = []
    if value.st_uid != expected_uid:
        violations.append(
            f"owner UID mismatch (actual_uid={value.st_uid}, "
            f"expected_uid={expected_uid})"
        )
    if value.st_gid != expected_gid:
        violations.append(
            f"owner GID mismatch (actual_gid={value.st_gid}, "
            f"expected_gid={expected_gid})"
        )
    if actual_mode != expected_mode:
        violations.append(
            f"mode mismatch (actual_mode={actual_mode:#05o}, "
            f"expected_mode={expected_mode:#05o})"
        )
    if violations:
        raise LibraryLayoutValidationError(
            f"{label} has unsafe host metadata: {path}; " + "; ".join(violations)
        )
