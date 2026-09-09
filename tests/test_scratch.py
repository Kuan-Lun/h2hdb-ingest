"""Disk placement, process leases, and bounded scratch recovery regressions."""

from __future__ import annotations

import errno
import json
import os
import select
import subprocess
import sys
import tempfile
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import pytest

import h2hdb_ingest.scratch as scratch_module
from h2hdb_ingest.scratch import DiskScratch, ScratchSafetyError


def _library(root: Path) -> Path:
    for relative in ("current/acquisitions", "current/artwork", ".h2hdb-coordination"):
        (root / relative).mkdir(parents=True, exist_ok=True)
    return root


def _drain(scratch: DiskScratch, *, limit: int = 7) -> int:
    removed = 0
    for _ in range(1000):
        result = scratch.cleanup_page(limit=limit)
        assert result.checked_entries <= limit
        assert result.removed_entries <= result.checked_entries
        removed += result.removed_entries
        if not result.pending:
            return removed
    raise AssertionError("bounded scratch cleanup failed to complete its scan")


@pytest.mark.parametrize("prior", (None, "configured-before-scratch"))
def test_process_tempfiles_and_worker_threads_use_disk_and_restore_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, prior: str | None
) -> None:
    root = _library(tmp_path / "library")
    if prior is None:
        monkeypatch.delenv("TMPDIR", raising=False)
    else:
        monkeypatch.setenv("TMPDIR", prior)
    cached = tempfile.tempdir
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    with DiskScratch(root) as scratch:
        work_path = scratch.path
        assert work_path.is_relative_to(root / ".h2hdb-state" / "scratch-v1")
        assert os.environ["TMPDIR"] == tempfile.gettempdir() == str(work_path)

        def create(_index: int) -> Path:
            with tempfile.NamedTemporaryFile() as stream:
                path = Path(stream.name)
                assert path.parent == work_path
                stream.write(b"temporary source bytes")
                return path

        with ThreadPoolExecutor(max_workers=4) as workers:
            created = list(workers.map(create, range(12)))
        assert not any(path.exists() for path in created)
        with tempfile.TemporaryDirectory(prefix="core-generic-") as directory:
            assert Path(directory).parent == work_path
        with tempfile.TemporaryFile() as stream:
            stream.write(b"anonymous scratch")
            stream.flush()
            assert os.fstat(stream.fileno()).st_nlink == 0
        assert list((root / "current" / "acquisitions").iterdir()) == []
        assert list((root / "current" / "artwork").iterdir()) == []
    assert not work_path.exists()
    assert os.environ.get("TMPDIR") == prior
    assert tempfile.tempdir == str(tmp_path)
    monkeypatch.setattr(tempfile, "tempdir", cached)


def test_nested_context_is_rejected_without_changing_active_process_settings(
    tmp_path: Path,
) -> None:
    root = _library(tmp_path / "library")
    previous = tempfile.tempdir
    with pytest.raises(ValueError, match="original failure"):
        with DiskScratch(root) as scratch:
            expected = str(scratch.path)
            with pytest.raises(RuntimeError, match="already active"):
                with DiskScratch(root):
                    raise AssertionError("nested context must not enter")
            assert tempfile.gettempdir() == os.environ["TMPDIR"] == expected
            raise ValueError("original failure")
    assert tempfile.tempdir == previous
    with DiskScratch(root):
        pass


@pytest.mark.parametrize("unsafe", ("root", "state", "scratch", "registry"))
def test_symlink_control_paths_are_rejected_without_touching_target(
    tmp_path: Path, unsafe: str
) -> None:
    root = _library(tmp_path / "library")
    external = tmp_path / "external"
    external.mkdir()
    valuable = external / "keep"
    valuable.write_bytes(b"unowned")
    if unsafe == "root":
        selected = tmp_path / "root-link"
        selected.symlink_to(root, target_is_directory=True)
    else:
        selected = root
        state = root / ".h2hdb-state"
        if unsafe == "state":
            state.symlink_to(external, target_is_directory=True)
        else:
            state.mkdir()
            container = state / "scratch-v1"
            if unsafe == "scratch":
                container.symlink_to(external, target_is_directory=True)
            else:
                container.mkdir()
                (container / "registry.lock").symlink_to(valuable)
    previous = tempfile.tempdir
    with pytest.raises((OSError, RuntimeError)):
        with DiskScratch(selected):
            raise AssertionError("unsafe scratch must not enter")
    assert valuable.read_bytes() == b"unowned"
    assert tempfile.tempdir == previous
    with DiskScratch(_library(tmp_path / "valid")):
        pass


@pytest.mark.parametrize("marker_kind", ("invalid_json", "directory", "symlink"))
def test_unknown_entries_and_forged_run_markers_are_preserved(
    tmp_path: Path, marker_kind: str
) -> None:
    root = _library(tmp_path / "library")
    with DiskScratch(root) as scratch:
        container = scratch.path.parent.parent
        unknown = container / ("run-" + "1" * 32)
        unknown.mkdir()
        marker = unknown / "owner.json"
        if marker_kind == "directory":
            marker.mkdir()
        elif marker_kind == "symlink":
            marker.symlink_to(tmp_path / "not-owned-marker")
        else:
            marker.write_text("{}")
        (unknown / "keep").write_bytes(b"unknown")
        external = tmp_path / "external"
        external.mkdir()
        (external / "keep").write_bytes(b"external")
        linked = container / ("run-" + "2" * 32)
        linked.symlink_to(external, target_is_directory=True)
        ordinary = container / "not-a-workspace"
        ordinary.write_bytes(b"unowned")
        _drain(scratch)
        assert (unknown / "keep").read_bytes() == b"unknown"
        assert linked.is_symlink()
        assert (external / "keep").read_bytes() == b"external"
        assert ordinary.read_bytes() == b"unowned"


_CHILD = """
import json, pathlib, sys, tempfile
from h2hdb_ingest.scratch import DiskScratch
with DiskScratch(pathlib.Path(sys.argv[1])) as scratch:
    for number in range(int(sys.argv[2])):
        (scratch.path / f"source-{number:04d}").write_bytes(b"orphan source")
    nested = scratch.path / "nested" / "deeper"
    nested.mkdir(parents=True)
    (nested / "plan.sqlite3").write_bytes(b"orphan plan")
    print(json.dumps(str(scratch.path)), flush=True)
    sys.stdin.buffer.read(1)
"""


@contextmanager
def _child(
    root: Path, *, files: int = 5
) -> Iterator[tuple[subprocess.Popen[str], Path]]:
    process = subprocess.Popen(
        [sys.executable, "-u", "-c", _CHILD, str(root), str(files)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout is not None
        ready, _, _ = select.select([process.stdout], [], [], 15)
        if not ready:
            raise AssertionError("scratch child did not publish its ready path")
        line = process.stdout.readline()
        if not line:
            assert process.stderr is not None
            raise AssertionError(process.stderr.read())
        yield process, Path(json.loads(line))
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=10)
        for stream in (process.stdin, process.stdout, process.stderr):
            if stream is not None:
                stream.close()


def test_live_process_is_retained_and_sigkill_work_is_reaped_in_bounded_pages(
    tmp_path: Path,
) -> None:
    root = _library(tmp_path / "library")
    with _child(root, files=280) as (process, child_path):
        with DiskScratch(root) as scratch:
            for _ in range(3):
                _drain(scratch)
            assert len(list(child_path.glob("source-*"))) == 280
            process.kill()
            process.wait(timeout=10)
            before = len(list(child_path.glob("source-*")))
            result = scratch.cleanup_page(limit=3)
            assert result.checked_entries <= 3
            assert result.removed_entries <= 3
            assert len(list(child_path.glob("source-*"))) >= before - 3
            assert _drain(scratch, limit=3) >= 280
            assert not child_path.parent.exists()


def test_new_process_context_recovers_sigkill_leftovers(tmp_path: Path) -> None:
    root = _library(tmp_path / "library")
    with _child(root) as (process, child_path):
        process.kill()
        process.wait(timeout=10)
        assert (child_path / "source-0000").is_file()
        with DiskScratch(root) as scratch:
            _drain(scratch)
            assert not child_path.parent.exists()


def test_owned_run_symlink_and_foreign_hard_link_are_preserved(tmp_path: Path) -> None:
    root = _library(tmp_path / "library")
    external = tmp_path / "external-data"
    external.write_bytes(b"external")
    with _child(root) as (process, child_path):
        linked = child_path / "symlink"
        linked.symlink_to(external)
        hard_link = child_path / "hard-link"
        os.link(external, hard_link)
        process.kill()
        process.wait(timeout=10)
        with DiskScratch(root) as scratch:
            _drain(scratch)
            assert linked.is_symlink()
            assert hard_link.is_file()
            assert external.read_bytes() == b"external"
            assert not (child_path / "source-0000").exists()


def test_changed_library_root_aborts_cleanup_before_deleting_files(
    tmp_path: Path,
) -> None:
    root = _library(tmp_path / "library")
    moved = tmp_path / "moved-library"
    with pytest.raises(ScratchSafetyError, match="library root changed"):
        with DiskScratch(root) as scratch:
            work = scratch.path
            (work / "keep").write_bytes(b"preserve")
            root.rename(moved)
            _library(root)
            scratch.cleanup_page()
    assert (moved / work.relative_to(root) / "keep").read_bytes() == b"preserve"
    with DiskScratch(root):
        pass


def test_initialization_failure_preserves_resource_error_and_releases_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _library(tmp_path / "library")
    failure = OSError("disk is full")
    original = scratch_module._owner_payload

    def fail(*_args: object) -> bytes:
        raise failure

    previous = tempfile.tempdir
    monkeypatch.setattr(scratch_module, "_owner_payload", fail)
    with pytest.raises(OSError) as caught:
        with DiskScratch(root):
            raise AssertionError("failed initialization cannot expose scratch")
    assert caught.value is failure
    assert tempfile.tempdir == previous
    monkeypatch.setattr(scratch_module, "_owner_payload", original)
    with DiskScratch(root):
        pass


def test_abandoned_bytes_are_reclaimed_before_allocating_new_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _library(tmp_path / "library")
    with _child(root, files=1) as (process, child_path):
        process.kill()
        process.wait(timeout=10)
        original = DiskScratch._create_run

        def require_space(scratch: DiskScratch) -> None:
            if child_path.exists():
                raise OSError("no free space until abandoned source bytes are removed")
            original(scratch)

        monkeypatch.setattr(DiskScratch, "_create_run", require_space)
        with DiskScratch(root) as scratch:
            assert scratch.path.is_dir()
            assert not child_path.exists()


@pytest.mark.parametrize("limit", (0, -1, 129, True))
def test_cleanup_rejects_unbounded_or_invalid_limits(
    tmp_path: Path, limit: int
) -> None:
    with DiskScratch(_library(tmp_path / "library")) as scratch:
        with pytest.raises(ValueError, match="from 1 through 128"):
            scratch.cleanup_page(limit=limit)


def test_initialization_write_error_survives_owner_close_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _library(tmp_path / "library")
    scratch = DiskScratch(root)
    write_error = OSError(errno.ENOSPC, "owner marker write failed")
    close_error = OSError(errno.EIO, "owner descriptor close failed")
    owner = -1
    descriptors: list[int] = []
    original_directory = DiskScratch._own_directory
    original_close = os.close

    def remember_directory(
        self: DiskScratch, parent: int, leaf: str, *, create: bool
    ) -> int:
        descriptor = original_directory(self, parent, leaf, create=create)
        descriptors.append(descriptor)
        return descriptor

    def fail_write(descriptor: int, _payload: bytes) -> int:
        nonlocal owner
        owner = descriptor
        descriptors.extend((scratch._root, scratch._registry, owner))
        raise write_error

    def fail_close(descriptor: int) -> None:
        original_close(descriptor)
        if descriptor == owner:
            raise close_error

    previous = tempfile.tempdir
    previous_native = os.environ.get("TMPDIR")
    with monkeypatch.context() as faults:
        faults.setattr(DiskScratch, "_own_directory", remember_directory)
        faults.setattr(os, "write", fail_write)
        faults.setattr(os, "close", fail_close)
        with pytest.raises(OSError) as caught:
            with scratch:
                raise AssertionError("failed initialization cannot expose scratch")
    assert caught.value is write_error
    assert any(
        "owner descriptor close failed" in note for note in write_error.__notes__
    )
    assert tempfile.tempdir == previous
    assert os.environ.get("TMPDIR") == previous_native
    for descriptor in descriptors:
        with pytest.raises(OSError) as closed:
            os.fstat(descriptor)
        assert closed.value.errno == errno.EBADF
    with DiskScratch(root):
        pass


@pytest.mark.parametrize("body_failure", (False, True))
def test_dispose_failure_preserves_body_failure_and_releases_process_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, body_failure: bool
) -> None:
    root = _library(tmp_path / "library")
    body_error = ValueError("original operation failed")
    dispose_error = OSError(errno.EIO, "final dispose failed")
    original = DiskScratch._dispose
    previous = tempfile.tempdir
    previous_native = os.environ.get("TMPDIR")

    def fail_dispose(self: DiskScratch) -> None:
        original(self)
        raise dispose_error

    with monkeypatch.context() as faults:
        faults.setattr(DiskScratch, "_dispose", fail_dispose)
        with pytest.raises((ValueError, OSError)) as caught:
            with DiskScratch(root) as scratch:
                path = scratch.path
                if body_failure:
                    raise body_error
    assert caught.value is (body_error if body_failure else dispose_error)
    if body_failure:
        assert any("final dispose failed" in note for note in body_error.__notes__)
    assert not path.exists()
    assert tempfile.tempdir == previous
    assert os.environ.get("TMPDIR") == previous_native
    with DiskScratch(root):
        pass


def test_reaper_attempts_all_frame_and_owner_closes_after_multiple_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _library(tmp_path / "library")
    with _child(root) as (process, child_path):
        with DiskScratch(root) as scratch:
            process.kill()
            process.wait(timeout=10)
            with scratch_module._registry(scratch._registry):
                reaper = scratch._claim(child_path.parent.name)
            assert reaper is not None
            for _ in range(30):
                if len(reaper.frames) == 3:
                    break
                _, done = reaper.step()
                assert not done
            assert len(reaper.frames) == 3
            frames = tuple(reversed(reaper.frames))
            descriptors = (
                *(frame.descriptor for frame in frames),
                reaper.owner_descriptor,
                reaper.run_descriptor,
            )
            first_error = OSError(errno.EIO, "deepest frame close failed")
            owner_error = OSError(errno.EIO, "reaper owner close failed")
            frame_close = frames[0].close
            original_close = os.close
            attempted: list[int] = []

            def fail_frame_close() -> None:
                frame_close()
                raise first_error

            def fail_owner_close(descriptor: int) -> None:
                attempted.append(descriptor)
                original_close(descriptor)
                if descriptor == reaper.owner_descriptor:
                    raise owner_error

            frames[0].close = fail_frame_close
            with monkeypatch.context() as faults:
                faults.setattr(os, "close", fail_owner_close)
                with pytest.raises(OSError) as caught:
                    reaper.close()
            assert caught.value is first_error
            assert any(
                "reaper owner close failed" in note for note in first_error.__notes__
            )
            assert reaper.frames == []
            assert reaper.closed
            for descriptor in descriptors:
                with pytest.raises(OSError) as closed:
                    os.fstat(descriptor)
                assert closed.value.errno == errno.EBADF
            # ExitStack captured frame close callbacks before fault injection;
            # the owner and run callbacks still both execute after the failures.
            assert attempted == [reaper.owner_descriptor, reaper.run_descriptor]
            reaper.close()
            _drain(scratch)
            assert not child_path.parent.exists()


def test_startup_finishes_paginated_scan_past_unknown_entries_before_allocating(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _library(tmp_path / "library")
    with _child(root, files=140) as (process, child_path):
        container = child_path.parent.parent
        unknown: list[Path] = []
        for number in range(130):
            directory = container / f"run-{number:032x}"
            directory.mkdir()
            (directory / "owner.json").write_text("{}")
            unknown.append(directory)
        process.kill()
        process.wait(timeout=10)
        original_frame = scratch_module._frame
        original_create = DiskScratch._create_run
        original_page = DiskScratch.cleanup_page
        startup_pages = 0

        def ordered_frame(parent: int, leaf: str) -> scratch_module._Frame:
            frame = original_frame(parent, leaf)
            if leaf == "scratch-v1":
                frame.entries = iter(
                    sorted(
                        frame.entries,
                        key=lambda entry: (
                            entry.name == child_path.parent.name,
                            entry.name,
                        ),
                    )
                )
            return frame

        def require_recovered_space(self: DiskScratch) -> None:
            assert not child_path.exists(), (
                "dead owner must be reached before allocation"
            )
            assert startup_pages >= 3
            original_create(self)

        def page(
            self: DiskScratch, *, limit: int = 128
        ) -> scratch_module.ScratchCleanupResult:
            nonlocal startup_pages
            result = original_page(self, limit=limit)
            startup_pages += 1
            assert result.checked_entries <= limit
            assert result.removed_entries <= result.checked_entries
            return result

        monkeypatch.setattr(scratch_module, "_frame", ordered_frame)
        monkeypatch.setattr(DiskScratch, "_create_run", require_recovered_space)
        monkeypatch.setattr(DiskScratch, "cleanup_page", page)
        with DiskScratch(root):
            assert not child_path.parent.exists()
            assert all(
                (directory / "owner.json").read_text() == "{}" for directory in unknown
            )
