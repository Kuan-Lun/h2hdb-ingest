"""Dev-only attribution of real gallery qualification on disposable synthetic bytes.

The callable API creates a fixture with ``create_fixture(root, [(name, bytes)])``
and measures it in a fresh owned process with ``run_case(root, workers=...)``.
Shared-process helpers are private and never attest to loaded runtime provenance.
The CLI creates its own four-codec matrix; it never accepts an existing corpus.
Worker elapsed sums, nested decoder reads and owner wall time are separate views.
Native decode/colour/shrink/materialization is fused, not a measured pure CPU cost.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import runpy
import stat
import subprocess
import sys
import tempfile
from collections import Counter, defaultdict
from collections.abc import Iterator, Sequence
from contextlib import ExitStack, contextmanager, nullcontext
from contextvars import ContextVar
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from threading import Lock
from time import monotonic_ns, process_time_ns
from typing import Any, BinaryIO
from unittest.mock import patch


def _runtime_imports() -> tuple[Any, ...]:
    # Capture the import boundary before loading any measured package. Imports
    # belong in this function so no on-disk snapshot can disguise sys.modules
    # objects retained by a caller from an earlier source version.
    preloaded = tuple(
        name
        for name in sys.modules
        if any(
            name == package or name.startswith(package + ".")
            for package in ("h2hdb", "h2hdb_ingest", "PIL", "pyvips")
        )
    )
    from PIL import Image

    import h2hdb_ingest.image_qualification as qualification
    import h2hdb_ingest.source_image as source_image
    from h2hdb_ingest._image_performance import ImageWorkMeasurement, measure_image_work
    from h2hdb_ingest.artifact import ArtifactRenderPolicy
    from h2hdb_ingest.core_source import VNextFilesystemSourceAdapter
    from h2hdb_ingest.filesystem import FilesystemSource
    from h2hdb_ingest.page_workers import MAX_PAGE_RENDER_WORKERS
    from h2hdb_ingest.source_performance import SourcePerformance

    return (
        preloaded,
        Image,
        qualification,
        source_image,
        ImageWorkMeasurement,
        measure_image_work,
        ArtifactRenderPolicy,
        VNextFilesystemSourceAdapter,
        FilesystemSource,
        MAX_PAGE_RENDER_WORKERS,
        SourcePerformance,
    )


(
    _PRELOADED_SOURCE_MODULES,
    Image,
    qualification,
    source_image,
    ImageWorkMeasurement,
    measure_image_work,
    ArtifactRenderPolicy,
    VNextFilesystemSourceAdapter,
    FilesystemSource,
    MAX_PAGE_RENDER_WORKERS,
    SourcePerformance,
) = _runtime_imports()

_COST = runpy.run_path(str(Path(__file__).with_name("check-source-cost.py")))
_IO = _COST["_HELPERS"]
_MANIFEST = ".qualification-synthetic.json"
_FORMAT = 1
_MAX_BYTES = 2 * 1024**3
_MAX_PAGE_BYTES = 256 * 1024**2
_MAX_MANIFEST_BYTES = 4 * 1024**2
_PAGE: ContextVar[int | None] = ContextVar("probe_worker_page", default=None)
_SPOOL: ContextVar[dict[str, int] | None] = ContextVar("probe_spool_page", default=None)
_RUN_LOCK = Lock()
_SCOPE = {
    "supervision": "fresh process startup, fixture admission/transport and teardown are excluded from reported observation wall/CPU; run_case timeout bounds its owned worker, not caller-side fixture transport",
    "wall": "complete adapter observation includes setup/teardown and independent response comparison; fixture creation, oracle reads and manifest checks are excluded",
    "observe_gallery": "one adapter observe_gallery call, including metadata discovery and qualification",
    "qualification": "one actual ImageGalleryQualifier call; no JPEG encoding or CBZ creation",
    "worker": "cumulative concurrent worker elapsed; cannot be added to owner wall or interpreted as process CPU",
    "native": "decode_and_shrink includes libvips decode/colour/shrink/materialization and Pillow frombytes; lazy native operations are fused, not separately measured CPU work",
    "io": "logical source and stream boundary bytes; memory spools included; excludes native scratch and physical HDD I/O",
    "hash": "source_hash times _spool expected_digest.update; spool_hash times its readback digest; optional content_parts verification and FILE receipt hashes are not included and remain in their enclosing residuals",
    "overlap": "decoder_input_read is nested in header/native decode; scheduler slots include decode; owner waits overlap workers; these sums are nonadditive",
    "cache": "no cache flush; synthetic local cache is warm; not a NAS completion-time acceptance",
    "residual": "explicit owner/worker/spool residuals include uninstrumented glue, guards and measurement overhead; they are not renamed as decode or I/O",
}
_DECISION_BUDGETS = {
    "qualification_minimum_improvement_fraction": 0.20,
    "complete_observation_minimum_improvement_fraction": 0.10,
    "maximum_stable_case_regression_fraction": 0.10,
    "scope": "predeclared future candidate decision thresholds; attribution alone cannot pass these or a NAS SLA",
}


def _temporary_root(root: Path) -> Path:
    absolute = root.absolute()
    resolved = root.resolve()
    temporary_roots = {Path(tempfile.gettempdir()).resolve(), Path("/tmp").resolve()}
    if absolute.is_symlink() or not any(
        resolved != parent and resolved.is_relative_to(parent)
        for parent in temporary_roots
    ):
        raise ValueError("fixture must be a real child of a local temporary root")
    return resolved


def create_fixture(
    root: Path,
    pages: Sequence[tuple[str, bytes]],
    *,
    expected_qualification: dict[str, Any] | None = None,
) -> Path:
    """Create one known gallery; input bytes must be generated by the caller.

    Root must be absent, below the process's local temporary root or /tmp. The
    API intentionally rejects an existing gallery tree and non-temporary paths.
    """
    root = _temporary_root(root)
    if not 1 <= len(pages) <= 4096:
        raise ValueError("fixture must contain 1..4096 PAGE inputs")
    names: set[str] = set()
    records: list[dict[str, Any]] = []
    total = 0
    for name, content in pages:
        if (
            Path(name).name != name
            or name in {"", ".", ".."}
            or name in names
            or not name.isascii()
            or Path(name).suffix.lower()
            not in {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".avif"}
            or not isinstance(content, bytes)
            or not 1 <= len(content) <= _MAX_PAGE_BYTES
        ):
            raise ValueError("invalid synthetic PAGE name or bytes")
        names.add(name)
        total += len(content)
        records.append(
            {"name": name, "size": len(content), "sha256": sha256(content).hexdigest()}
        )
    if total > _MAX_BYTES:
        raise ValueError("synthetic fixture exceeds its encoded byte budget")
    expected = expected_qualification or {
        "accepted": True,
        "reason_code": None,
        "source_name": None,
    }
    from h2hdb import VNextSourceQualification

    VNextSourceQualification(**expected)
    if (
        expected["source_name"] is not None
        and os.fsdecode(expected["source_name"]) not in names
    ):
        raise ValueError("expected rejection must name a synthetic PAGE")
    root.mkdir(parents=True, exist_ok=False)
    gallery = root / "1000000"
    gallery.mkdir()
    for name, content in pages:
        (gallery / name).write_bytes(content)
    metadata = _IO["_metadata"](1000000)
    (gallery / "galleryinfo.txt").write_bytes(metadata)
    records.append(
        {
            "name": "galleryinfo.txt",
            "size": len(metadata),
            "sha256": sha256(metadata).hexdigest(),
        }
    )
    manifest = {
        "format": _FORMAT,
        "records": sorted(records, key=lambda value: value["name"]),
        "expected_qualification": {
            **expected,
            "source_name": None
            if expected["source_name"] is None
            else expected["source_name"].hex(),
        },
    }
    (root / _MANIFEST).write_text(json.dumps(manifest, sort_keys=True))
    return root


def _admit_fixture(
    root: Path, *, inspect_images: bool = False
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    root = _temporary_root(root)
    root_entries = _bounded_entries(root, {"1000000", _MANIFEST})
    manifest_identity = root_entries[_MANIFEST].stat(follow_symlinks=False)
    if (
        not stat.S_ISREG(manifest_identity.st_mode)
        or not 1 <= manifest_identity.st_size <= _MAX_MANIFEST_BYTES
    ):
        raise ValueError("synthetic fixture manifest type or byte budget is invalid")
    manifest = json.loads(
        _read_exact_regular(
            root / _MANIFEST, manifest_identity, maximum=_MAX_MANIFEST_BYTES
        )
    )
    if manifest["format"] != _FORMAT:
        raise ValueError("foreign synthetic fixture shape")
    folder = root / "1000000"
    if not stat.S_ISDIR(root_entries["1000000"].stat(follow_symlinks=False).st_mode):
        raise ValueError("synthetic gallery must be a real directory")
    expected = manifest["records"]
    if not isinstance(expected, list) or not 2 <= len(expected) <= 4097:
        raise ValueError("synthetic fixture row budget exceeded")
    expected_names = set()
    image_headers = []
    for record in expected:
        if not isinstance(record, dict) or set(record) != {"name", "size", "sha256"}:
            raise ValueError("synthetic fixture manifest has an invalid record")
        name, size = record["name"], record["size"]
        if (
            not isinstance(name, str)
            or Path(name).name != name
            or name in {"", ".", ".."}
            or name in expected_names
        ):
            raise ValueError("synthetic fixture manifest has an invalid member name")
        if type(size) is not int or not 1 <= size <= _MAX_PAGE_BYTES:
            raise ValueError("synthetic fixture manifest member byte budget exceeded")
        digest = record["sha256"]
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)
        ):
            raise ValueError("synthetic fixture manifest has an invalid digest")
        expected_names.add(name)
    if "galleryinfo.txt" not in expected_names or [
        record["name"] for record in expected
    ] != sorted(expected_names):
        raise ValueError(
            "synthetic fixture manifest is not a complete canonical inventory"
        )
    if sum(record["size"] for record in expected) > _MAX_BYTES:
        raise ValueError("synthetic fixture aggregate byte budget exceeded")
    entries = _bounded_entries(folder, expected_names)
    identities = {}
    actual_size = 0
    # Admit the complete bounded inventory before opening any content member.
    for record in expected:
        identity = entries[record["name"]].stat(follow_symlinks=False)
        if not stat.S_ISREG(identity.st_mode) or identity.st_nlink != 1:
            raise ValueError("synthetic fixture member is not a private regular file")
        if (
            not 1 <= identity.st_size <= _MAX_PAGE_BYTES
            or identity.st_size != record["size"]
        ):
            raise ValueError("synthetic fixture member byte budget exceeded")
        actual_size += identity.st_size
        if actual_size > _MAX_BYTES:
            raise ValueError("synthetic fixture aggregate byte budget exceeded")
        identities[record["name"]] = identity
    for record in expected:
        digest = _hash_exact_regular(
            folder / record["name"], identities[record["name"]]
        )
        if digest != record["sha256"]:
            raise ValueError("synthetic fixture differs from its exact byte manifest")
        if record["name"] == "galleryinfo.txt":
            if _read_exact_regular(
                folder / record["name"],
                identities[record["name"]],
                maximum=_MAX_PAGE_BYTES,
            ) != _IO["_metadata"](1000000):
                raise ValueError("synthetic fixture metadata differs from its oracle")
        elif inspect_images:
            content = _read_exact_regular(
                folder / record["name"],
                identities[record["name"]],
                maximum=_MAX_PAGE_BYTES,
            )
            if sha256(content).hexdigest() != digest:
                raise ValueError("synthetic fixture changed before image header read")
            with Image.open(BytesIO(content)) as image:
                image_headers.append(
                    {**record, "codec": image.format, "dimensions": list(image.size)}
                )
    oracle = _admitted_oracle(expected, identities)
    oracle["qualification"] = {
        **manifest["expected_qualification"],
        "source_name": None
        if manifest["expected_qualification"]["source_name"] is None
        else bytes.fromhex(manifest["expected_qualification"]["source_name"]),
    }
    for name, identity in identities.items():
        if _stat_identity(
            (folder / name).stat(follow_symlinks=False)
        ) != _stat_identity(identity):
            raise ValueError("synthetic fixture changed after its admitted read")
    return manifest, oracle, image_headers


def _admitted_oracle(
    records: list[dict[str, Any]], identities: dict[str, os.stat_result]
) -> dict[str, Any]:
    """Build the independent known-fixture oracle without reopening its paths.

    Every digest above came from an exact identity-bound, bounded read. Metadata
    bytes are independently equal to the fixed fixture producer's payload.
    """
    files, directories = [], []
    for record in records:
        name, identity = record["name"], identities[record["name"]]
        common = {
            "name_bytes": os.fsencode(name),
            "device": identity.st_dev,
            "inode": identity.st_ino,
            "modified_ns": identity.st_mtime_ns,
            "changed_ns": identity.st_ctime_ns,
            "size_bytes": record["size"],
        }
        files.append(
            {
                **common,
                "sha256": record["sha256"],
                "artifact_role": "metadata" if name == "galleryinfo.txt" else "page",
            }
        )
        directories.append({**common, "file_type": 0})
    marker = next(row for row in files if row["name_bytes"] == b"galleryinfo.txt")
    accepted = {"accepted": True, "reason_code": None, "source_name": None}
    return {
        "locator": ("1000000",),
        "marker": {"file": marker, "observation_version": 3},
        "metadata": {
            "gid": 1000000,
            "title": "Source I/O probe 1000000",
            "comment": "Synthetic offline fixture",
            "upload_account": "synthetic",
            "upload_time": int(datetime(2024, 1, 2, 3, 4, tzinfo=UTC).timestamp())
            * 1_000_000,
            "download_time": int(datetime(2024, 2, 3, 4, 5, tzinfo=UTC).timestamp())
            * 1_000_000,
            "modified_time": marker["modified_ns"] // 1_000,
            "scan_observation_version": 3,
            "source_file_count": len(files),
            "page_count": len(files) - 1,
            "qualification_policy_sha256": bytes(32),
            "qualification": accepted,
        },
        "qualification": accepted,
        "files": files,
        "directories": directories,
        "tags": [
            {"namespace": "artist", "value": "synthetic-1000000"},
            {"namespace": "language", "value": "english"},
        ],
    }


def _bounded_entries(folder: Path, allowed: set[str]) -> dict[str, os.DirEntry[str]]:
    found = {}
    with os.scandir(folder) as entries:
        for entry in entries:
            if (
                entry.name not in allowed
                or entry.name in found
                or len(found) >= len(allowed)
            ):
                raise ValueError(
                    "foreign synthetic fixture entry or row budget exceeded"
                )
            found[entry.name] = entry
    if set(found) != allowed:
        raise ValueError("synthetic fixture has missing entries")
    return found


@contextmanager
def _open_exact_regular(path: Path, expected: os.stat_result) -> Iterator[BinaryIO]:
    descriptor = os.open(
        path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    )
    with os.fdopen(descriptor, "rb") as stream:
        opened = os.fstat(stream.fileno())
        if (
            _stat_identity(opened) != _stat_identity(expected)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
        ):
            raise ValueError(
                "synthetic fixture member changed before content admission"
            )
        yield stream
        if _stat_identity(os.fstat(stream.fileno())) != _stat_identity(opened):
            raise ValueError("synthetic fixture member changed during content read")


def _stat_identity(value: os.stat_result) -> tuple[int, ...]:
    # Reads can legitimately update atime; it is not source-byte authority.
    return (
        value.st_mode,
        value.st_dev,
        value.st_ino,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _read_exact_regular(path: Path, expected: os.stat_result, *, maximum: int) -> bytes:
    with _open_exact_regular(path, expected) as stream:
        content = stream.read(min(expected.st_size, maximum) + 1)
    if len(content) != expected.st_size:
        raise ValueError("synthetic fixture manifest changed size during read")
    return content


def _hash_exact_regular(path: Path, expected: os.stat_result) -> str:
    digest, remaining = sha256(), expected.st_size
    with _open_exact_regular(path, expected) as stream:
        while remaining:
            part = stream.read(min(1024 * 1024, remaining))
            if not part:
                raise ValueError("synthetic fixture member shrank during read")
            remaining -= len(part)
            digest.update(part)
        if stream.read(1):
            raise ValueError("synthetic fixture member grew during read")
    return digest.hexdigest()


class _Buffer:
    def __init__(self, stream: BinaryIO, meter: _Meter, page: int) -> None:
        self.stream, self.meter, self.page = stream, meter, page
        self.opened = monotonic_ns()
        self.closed_at: int | None = None
        self.rolled = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self.stream, name)

    def __enter__(self) -> _Buffer:
        self.stream.__enter__()
        return self

    def __exit__(self, *args: object) -> None:
        try:
            self.stream.__exit__(*args)
        finally:
            self._closed()

    def _closed(self) -> None:
        if self.closed_at is None:
            self.closed_at = monotonic_ns()

    def close(self) -> None:
        try:
            self.stream.close()
        finally:
            self._closed()

    def write(self, content: bytes) -> int:
        with self.meter.timed("spool_write", "producer", self.page):
            count = self.stream.write(content)
        self.meter.count("spool_write_bytes", count)
        self.meter.count("spool_write_calls")
        return count

    def read(self, size: int = -1) -> bytes:
        producer = _SPOOL.get() is not None
        name = "spool_readback" if producer else "decoder_buffer_read"
        with self.meter.timed(name, "producer" if producer else "worker", self.page):
            content = self.stream.read(size)
        self.meter.count(name + "_bytes", len(content))
        self.meter.count(name + "_calls")
        if not producer:
            self.meter.workers[self.page]["decoder_buffer_read_calls"] += 1
        return content


class _Hash:
    def __init__(self, value: Any, meter: _Meter, kind: str, page: int) -> None:
        self.value, self.meter, self.kind, self.page = value, meter, kind, page

    def __getattr__(self, name: str) -> Any:
        return getattr(self.value, name)

    def update(self, data: bytes) -> None:
        with self.meter.timed(self.kind, "producer", self.page):
            self.value.update(data)
        self.meter.count(self.kind + "_bytes", len(data))
        self.meter.count(self.kind + "_calls")


class _Future:
    def __init__(self, value: Any, meter: _Meter, page: int) -> None:
        self.value, self.meter, self.page = value, meter, page

    def result(self, *args: Any, **kwargs: Any) -> Any:
        with self.meter.timed("future_wait", "producer", self.page):
            return self.value.result(*args, **kwargs)


class _Meter:
    def __init__(self, oracle: dict[str, Any]) -> None:
        self.source_files = {
            (row["device"], row["inode"]): row["name_bytes"] for row in oracle["files"]
        }
        self.events: list[dict[str, Any]] = []
        self.counters: Counter[str] = Counter()
        self.workers: dict[int, dict[str, Any]] = {}
        self.buffers: list[_Buffer] = []
        self.lock = Lock()

    def count(self, name: str, value: int = 1) -> None:
        with self.lock:
            self.counters[name] += value

    def event(
        self, name: str, owner: str, page: int | None, start: int, end: int
    ) -> None:
        with self.lock:
            self.events.append(
                {
                    "name": name,
                    "owner": owner,
                    "page": page,
                    "start_ns": start,
                    "end_ns": end,
                }
            )

    @contextmanager
    def timed(self, name: str, owner: str, page: int | None = None) -> Iterator[None]:
        start = monotonic_ns()
        try:
            yield
        finally:
            self.event(name, owner, page, start, monotonic_ns())

    @contextmanager
    def instrument(self) -> Iterator[None]:
        original_read = os.read
        original_spool = qualification._spool
        original_temporary = qualification.SpooledTemporaryFile
        original_hash = qualification.sha256
        original_executor = qualification.ThreadPoolExecutor
        original_acquire = source_image._SCHEDULER.acquire
        original_header = source_image._read_header
        original_thumbnail = Image.Image.thumbnail
        original_decode = source_image._decode_pixels
        original_pipeline = source_image._load_source_image
        original_native_thumbnail = source_image.pyvips.Image.thumbnail_source
        original_image_phase = ImageWorkMeasurement.phase
        image_owners: dict[int, dict[str, Any]] = {}
        meter = self

        def read(descriptor: int, size: int) -> bytes:
            identity = os.fstat(descriptor)
            matched = self.source_files.get((identity.st_dev, identity.st_ino))
            if matched is None:
                return original_read(descriptor, size)
            spool = _SPOOL.get()
            phase = "source_read" if spool is not None else "receipt_source_read"
            with self.timed(
                phase, "producer", None if spool is None else spool["page"]
            ):
                content = original_read(descriptor, size)
            self.count(phase + "_bytes", len(content))
            self.count(phase + "_calls")
            self.count("all_source_read_bytes", len(content))
            self.count("all_source_read_calls")
            return content

        def temporary(*args: Any, **kwargs: Any) -> _Buffer:
            state = _SPOOL.get()
            if state is None:
                raise RuntimeError("qualification spool lost its page owner")
            buffer = _Buffer(original_temporary(*args, **kwargs), self, state["page"])
            self.buffers.append(buffer)
            return buffer

        def digest(*args: Any, **kwargs: Any) -> _Hash:
            state = _SPOOL.get()
            if state is None:
                raise RuntimeError("qualification hash lost its spool owner")
            state["hashes"] += 1
            if state["hashes"] > 2:
                raise RuntimeError("qualification introduced an unclassified hash pass")
            return _Hash(
                original_hash(*args, **kwargs),
                self,
                "source_hash" if state["hashes"] == 1 else "spool_hash",
                state["page"],
            )

        def spool(member: Any, position: int) -> Any:
            state = {"page": position, "hashes": 0}
            token = _SPOOL.set(state)
            try:
                with self.timed("spool", "producer", position):
                    result = original_spool(member, position)
                result.rolled = bool(result._rolled)
                self.count("spools")
                self.count("rolled_spools", int(result.rolled))
                if state["hashes"] != 2:
                    raise RuntimeError("qualification lost an exact spool hash pass")
                return result
            finally:
                _SPOOL.reset(token)

        @contextmanager
        def acquire(*, exclusive: bool) -> Iterator[None]:
            page = _PAGE.get()
            start = monotonic_ns()
            with original_acquire(exclusive=exclusive):
                granted = monotonic_ns()
                self.event("scheduler_wait", "worker", page, start, granted)
                try:
                    yield
                finally:
                    self.event(
                        "exclusive_slot" if exclusive else "regular_slot",
                        "worker",
                        page,
                        granted,
                        monotonic_ns(),
                    )

        def header(*args: Any, **kwargs: Any) -> Any:
            self.workers[_PAGE.get()]["header_calls"] += 1
            with self.timed("header", "worker", _PAGE.get()):
                return original_header(*args, **kwargs)

        def thumbnail(image: Image.Image, *args: Any, **kwargs: Any) -> Any:
            page = _PAGE.get()
            if page is not None:
                self.workers[page]["thumbnail_calls"] += 1
            return original_thumbnail(image, *args, **kwargs)

        def decode(*args: Any, **kwargs: Any) -> Any:
            self.workers[_PAGE.get()]["decode_calls"] += 1
            return original_decode(*args, **kwargs)

        def pipeline(*args: Any, **kwargs: Any) -> Any:
            self.workers[_PAGE.get()]["pipeline_calls"] += 1
            return original_pipeline(*args, **kwargs)

        def native_thumbnail(*args: Any, **kwargs: Any) -> Any:
            self.workers[_PAGE.get()]["native_thumbnail_calls"] += 1
            return original_native_thumbnail(*args, **kwargs)

        @contextmanager
        def image_phase(measured: ImageWorkMeasurement, name: Any) -> Iterator[None]:
            # Keep the production clock/phase implementation. Record each actual
            # contribution, so one missing retry/callback cannot hide behind an
            # earlier positive aggregate. Explicit ownership also covers native
            # callbacks that do not inherit the worker's context variables.
            before = measured.phases_ns.get(name, 0)
            try:
                with original_image_phase(measured, name):
                    yield
            finally:
                image_owners[id(measured)]["image_phase_samples"].append(
                    {
                        "name": name,
                        "elapsed_ns": measured.phases_ns.get(name, 0) - before,
                    }
                )

        class Executor:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                self.executor = original_executor(*args, **kwargs)

            def __enter__(self) -> Executor:
                self.executor.__enter__()
                return self

            def __exit__(self, *args: Any) -> None:
                self.executor.__exit__(*args)

            def submit(self, function: Any, *args: Any, **kwargs: Any) -> _Future:
                page = args[2]
                submitted = monotonic_ns()
                record: dict[str, Any] = {
                    "page": page,
                    "submitted_ns": submitted,
                    "header_calls": 0,
                    "thumbnail_calls": 0,
                    "decode_calls": 0,
                    "pipeline_calls": 0,
                    "native_thumbnail_calls": 0,
                    "decoder_buffer_read_calls": 0,
                    "image_phase_samples": [],
                }
                meter.workers[page] = record

                def work() -> Any:
                    started = monotonic_ns()
                    record["started_ns"] = started
                    token = _PAGE.set(page)
                    try:
                        with measure_image_work() as measured:
                            image_owners[id(measured)] = record
                            result = function(*args, **kwargs)
                        record["invalid_image"] = result is not None
                        return result
                    finally:
                        record["finished_ns"] = monotonic_ns()
                        record["phases_ns_inclusive"] = dict(measured.phases_ns)
                        record["worker_elapsed_ns"] = measured.elapsed_ns
                        record["worker_thread_cpu_ns"] = measured.thread_cpu_ns
                        record["decoder_input_bytes"] = measured.decoder_input_bytes
                        _PAGE.reset(token)

                return _Future(self.executor.submit(work), meter, page)

        with ExitStack() as stack:
            for owner, name, replacement in (
                (os, "read", read),
                (qualification, "_spool", spool),
                (qualification, "SpooledTemporaryFile", temporary),
                (qualification, "sha256", digest),
                (qualification, "ThreadPoolExecutor", Executor),
                (source_image._SCHEDULER, "acquire", acquire),
                (source_image, "_read_header", header),
                (source_image, "_decode_pixels", decode),
                (source_image, "_load_source_image", pipeline),
                (source_image.pyvips.Image, "thumbnail_source", native_thumbnail),
                (ImageWorkMeasurement, "phase", image_phase),
                (Image.Image, "thumbnail", thumbnail),
            ):
                stack.enter_context(patch.object(owner, name, replacement))
            yield

    def report(self) -> dict[str, Any]:
        totals: Counter[str] = Counter()
        calls: Counter[str] = Counter()
        per_page: dict[int, Counter[str]] = defaultdict(Counter)
        per_page_calls: dict[int, Counter[str]] = defaultdict(Counter)
        for event in self.events:
            elapsed = event["end_ns"] - event["start_ns"]
            totals[event["name"]] += elapsed
            calls[event["name"]] += 1
            if event["page"] is not None:
                per_page[event["page"]][event["name"]] += elapsed
                per_page_calls[event["page"]][event["name"]] += 1
        worker_rows = []
        for page, worker in sorted(self.workers.items()):
            phases = worker["phases_ns_inclusive"]
            attributed = sum(
                phases.get(name, 0) for name in ("decode_and_shrink", "resize")
            )
            attributed += per_page[page]["header"] + per_page[page]["scheduler_wait"]
            worker_rows.append(
                {
                    **worker,
                    "queue_wait_ns": worker["started_ns"] - worker["submitted_ns"],
                    "scheduler_wait_ns": per_page[page]["scheduler_wait"],
                    "header_ns": per_page[page]["header"],
                    "event_calls": dict(per_page_calls[page]),
                    "event_totals_ns_nonadditive": dict(per_page[page]),
                    "worker_other_ns": worker["worker_elapsed_ns"] - attributed,
                }
            )
        spool_parts = (
            "source_read",
            "spool_write",
            "spool_readback",
            "source_hash",
            "spool_hash",
        )
        spool_other = totals["spool"] - sum(totals[name] for name in spool_parts)
        return {
            "counters": dict(self.counters),
            "events": self.events,
            "event_totals_ns_nonadditive": dict(totals),
            "event_calls": dict(calls),
            "spool_other_ns": spool_other,
            "workers": worker_rows,
            "buffers": [
                {
                    "page": item.page,
                    "opened_ns": item.opened,
                    "closed_ns": item.closed_at,
                    "rolled": item.rolled,
                }
                for item in self.buffers
            ],
        }


def _scheduler_evidence(measurements: dict[str, Any]) -> dict[int, int]:
    """Validate measured slot lifetimes, including overlap across different pages."""
    if "events" in measurements:
        slots = [
            {
                "page": event["page"],
                "start_ns": event["start_ns"],
                "end_ns": event["end_ns"],
                "exclusive": event["name"] == "exclusive_slot",
            }
            for event in measurements["events"]
            if event["name"] in {"regular_slot", "exclusive_slot"}
        ]
        measured_calls = Counter(event["name"] for event in measurements["events"])
        measured_totals: Counter[str] = Counter()
        for event in measurements["events"]:
            if (
                type(event["start_ns"]) is not int
                or type(event["end_ns"]) is not int
                or event["end_ns"] < event["start_ns"]
            ):
                raise ValueError("event has an invalid monotonic interval")
            measured_totals[event["name"]] += event["end_ns"] - event["start_ns"]
        if (
            dict(measured_calls) != measurements["event_calls"]
            or dict(measured_totals) != measurements["event_totals_ns_nonadditive"]
        ):
            raise ValueError("raw events disagree with attributed counts or duration")
        measurements["scheduler_slots"] = slots
        for row in measurements["workers"]:
            events = [
                event
                for event in measurements["events"]
                if event["page"] == row["page"]
            ]
            event_calls = Counter(event["name"] for event in events)
            event_totals: Counter[str] = Counter()
            for event in events:
                event_totals[event["name"]] += event["end_ns"] - event["start_ns"]
            if (
                dict(event_calls) != row["event_calls"]
                or dict(event_totals) != row["event_totals_ns_nonadditive"]
            ):
                raise ValueError("raw events disagree with per-worker phase timing")
    else:
        # Compact reports retain every scheduler slot, even when callers omit
        # high-volume read/write events after first complete validation.
        slots = measurements["scheduler_slots"]
    page_ids = {row["page"] for row in measurements["workers"]}
    counts: Counter[int] = Counter()
    timeline = []
    for slot in slots:
        if (
            slot["page"] not in page_ids
            or type(slot["exclusive"]) is not bool
            or not 0 <= slot["start_ns"] <= slot["end_ns"]
        ):
            raise ValueError("scheduler slot lost its worker or interval")
        counts[slot["page"]] += 1
        timeline.extend(
            (
                (slot["start_ns"], 1, slot["exclusive"]),
                (slot["end_ns"], -1, slot["exclusive"]),
            )
        )
    active = exclusive = 0
    for _point, delta, is_exclusive in sorted(timeline):
        if delta > 0 and (exclusive or (is_exclusive and active)):
            raise ValueError("exclusive decoder overlapped another scheduler slot")
        active += delta
        exclusive += delta * int(is_exclusive)
        if active < 0 or exclusive < 0:
            raise ValueError("scheduler slot timeline is not conserved")
    if (
        active
        or exclusive
        or len(slots) != measurements["event_calls"].get("scheduler_wait")
    ):
        raise ValueError("scheduler acquire/slot attribution is incomplete")
    return dict(counts)


def _validate_worker_phases(row: dict[str, Any]) -> None:
    calls, totals = row["event_calls"], row["event_totals_ns_nonadditive"]
    if (
        calls.get("header") != row["header_calls"]
        or totals.get("header", 0) <= 0
        or totals["header"] != row["header_ns"]
    ):
        raise ValueError("header operations lack complete per-worker phase timing")
    if totals.get("scheduler_wait") != row["scheduler_wait_ns"]:
        raise ValueError("worker scheduler phase timing disagrees")
    phase_counts: Counter[str] = Counter()
    phase_totals: Counter[str] = Counter()
    for sample in row["image_phase_samples"]:
        if type(sample["elapsed_ns"]) is not int or sample["elapsed_ns"] <= 0:
            raise ValueError("image phase omitted a per-operation timing contribution")
        phase_counts[sample["name"]] += 1
        phase_totals[sample["name"]] += sample["elapsed_ns"]
    if dict(phase_totals) != row["phases_ns_inclusive"]:
        raise ValueError("image phase contributions disagree with production timing")
    required = {
        "decoder_pipeline": row["pipeline_calls"],
        "decode_and_shrink": row["native_thumbnail_calls"],
        "resize": row["thumbnail_calls"],
        "decoder_input_read": row["decoder_buffer_read_calls"],
    }
    if {key: value for key, value in required.items() if value} != dict(phase_counts):
        raise ValueError("actual image operations omitted complete phase timing")
    if row["pipeline_calls"] != 1:
        raise ValueError("accepted PAGE exceeds a single production loader path")
    if calls.get("decoder_buffer_read") != row["decoder_buffer_read_calls"]:
        raise ValueError("decoder buffer operations lack per-worker phase timing")


def validate_attribution(report: dict[str, Any]) -> None:
    """Fixed completeness/conservation checks; no timing threshold fits a baseline."""
    _validate_content_oracle(report)
    measurements = report["attribution"]
    counters, rows = measurements["counters"], measurements["workers"]
    totals, calls = (
        measurements["event_totals_ns_nonadditive"],
        measurements["event_calls"],
    )
    pages = report["fixture"]["page_count"]
    expected_bytes = report["fixture"]["page_bytes"]
    accepted = report["qualification"]["accepted"]
    scalar_times = [
        report[name]
        for name in (
            "qualification_wall_ns",
            "observe_gallery_wall_ns",
            "complete_observation_wall_ns",
            "process_cpu_ns",
            "qualification_owner_other_ns",
        )
    ]
    scalar_times.extend(totals.values())
    scalar_times.append(measurements["spool_other_ns"])
    for row in rows:
        scalar_times.extend(
            row[name]
            for name in (
                "worker_elapsed_ns",
                "worker_thread_cpu_ns",
                "worker_other_ns",
                "queue_wait_ns",
                "scheduler_wait_ns",
                "header_ns",
            )
        )
        scalar_times.extend(row["phases_ns_inclusive"].values())
    if any(type(value) is not int or value < 0 for value in scalar_times):
        raise ValueError("attribution duration must be a nonnegative integer")
    slot_counts = _scheduler_evidence(measurements)
    if not rows or len(rows) > pages or (accepted and len(rows) != pages):
        raise ValueError("attribution omitted a qualified PAGE worker")
    if counters.get("spools") != len(rows) or calls.get("spool") != len(rows):
        raise ValueError("attribution omitted a producer spool")
    submitted_bytes = sum(report["fixture"]["page_sizes"][row["page"]] for row in rows)
    # Actual source reads include one terminal EOF per spool. Every nonempty
    # content_parts result is hashed and written once. Readback is the fixed
    # production 1 MiB loop over these exact admitted sizes, including EOF.
    parts = counters.get("source_read_calls", 0) - len(rows)
    readback_parts = sum(
        (report["fixture"]["page_sizes"][row["page"]] + 1024**2 - 1) // 1024**2
        for row in rows
    )
    required_timing_calls = {
        "source_read": counters.get("source_read_calls", 0),
        "receipt_source_read": counters.get("receipt_source_read_calls", 0),
        "source_hash": parts,
        "spool_write": parts,
        "spool_readback": readback_parts + len(rows),
        "spool_hash": readback_parts,
    }
    for phase, operations in required_timing_calls.items():
        if (
            operations <= 0
            or calls.get(phase) != operations
            or totals.get(phase, 0) <= 0
        ):
            raise ValueError(f"actual operations lack complete phase timing: {phase}")
        if counters.get(phase + "_calls") != operations:
            raise ValueError(
                f"direct operation count differs from exact work units: {phase}"
            )
    if (
        calls.get("decoder_buffer_read", 0) <= 0
        or totals.get("decoder_buffer_read", 0) <= 0
    ):
        raise ValueError(
            "actual operations lack complete phase timing: decoder_buffer_read"
        )
    if counters.get("decoder_buffer_read_calls") != calls["decoder_buffer_read"]:
        raise ValueError("decoder buffer operations lack complete phase timing")
    for name in (
        "source_read_bytes",
        "spool_write_bytes",
        "spool_readback_bytes",
        "source_hash_bytes",
        "spool_hash_bytes",
    ):
        if counters.get(name) != submitted_bytes:
            raise ValueError(f"attribution lost byte conservation: {name}")
    if accepted and submitted_bytes != expected_bytes:
        raise ValueError("attribution lost complete source byte coverage")
    if counters.get("decoder_buffer_read_bytes") != sum(
        row["decoder_input_bytes"] for row in rows
    ):
        raise ValueError("decoder callback and spool-read byte meters disagree")
    for row in rows:
        _validate_worker_phases(row)
        phases = row["phases_ns_inclusive"]
        if row["worker_other_ns"] < 0 or row["queue_wait_ns"] < 0:
            raise ValueError("worker interval conservation failed")
        if phases.get("decoder_pipeline", 0) <= 0 or row["header_calls"] < 1:
            raise ValueError("attribution omitted decoder/header work")
        for counter, phase in (
            ("decode_calls", "decode_and_shrink"),
            ("thumbnail_calls", "resize"),
        ):
            if row[counter] and phases.get(phase, 0) <= 0:
                raise ValueError(f"attribution omitted image phase: {phase}")
        if not row["invalid_image"] and (
            not row["decode_calls"] or not row["thumbnail_calls"]
        ):
            raise ValueError("accepted PAGE was not fully decoded and resized")
        # One production loader can retry once exclusively after a native
        # failure. A second successful full load is not such a retry: it adds
        # another regular header slot and a fourth total acquisition.
        allowed_success_paths = {(1, 1, 1, 2), (2, 1, 1, 2), (2, 2, 1, 3)}
        observed_path = (
            row["header_calls"],
            row["decode_calls"],
            row["thumbnail_calls"],
            slot_counts.get(row["page"], 0),
        )
        if not row["invalid_image"] and observed_path not in allowed_success_paths:
            raise ValueError("accepted PAGE exceeds a single production loader path")
        component = row["header_ns"] + row["scheduler_wait_ns"]
        component += sum(
            phases.get(name, 0) for name in ("decode_and_shrink", "resize")
        )
        if component + row["worker_other_ns"] != row["worker_elapsed_ns"]:
            raise ValueError("worker nonoverlapping component totals disagree")
    if calls.get("future_wait") != len(rows) or calls.get(
        "scheduler_wait", 0
    ) < 2 * len(rows):
        raise ValueError("attribution omitted owner or scheduler waits")
    if measurements["spool_other_ns"] < 0 or report["qualification_owner_other_ns"] < 0:
        raise ValueError("producer interval conservation failed")
    if (
        report["qualification_wall_ns"] > report["observe_gallery_wall_ns"]
        or report["observe_gallery_wall_ns"] > report["complete_observation_wall_ns"]
    ):
        raise ValueError("nested owner wall intervals disagree")
    if (
        report["qualification_owner_other_ns"] + totals["spool"] + totals["future_wait"]
        != report["qualification_wall_ns"]
    ):
        raise ValueError("qualification owner partition does not conserve wall time")
    buffers = measurements["buffers"]
    if len(buffers) != len(rows) or any(item["closed_ns"] is None for item in buffers):
        raise ValueError("qualification lost or retained a spool")
    timeline = sorted(
        (point, change)
        for item in buffers
        for point, change in ((item["opened_ns"], 1), (item["closed_ns"], -1))
    )
    live = peak = 0
    for _point, change in timeline:
        live += change
        peak = max(peak, live)
    if live or peak > report["workers"]:
        raise ValueError("qualification exceeded its pending-spool bound")
    measurements["peak_live_spools"] = peak
    # The existing production source meter and the independent os.read boundary
    # must agree even outside qualification (FILE receipts and marker reads).
    production = report["production_source_counters"]
    if production.get("logical_bytes_read", 0) != counters.get(
        "all_source_read_bytes", 0
    ) or production.get("read_calls", 0) != counters.get("all_source_read_calls", 0):
        raise ValueError("source telemetry and independent read meter disagree")


def _validate_content_oracle(report: dict[str, Any]) -> None:
    oracle = report.get("content_oracle")
    if (
        not isinstance(oracle, dict)
        or oracle.get("status") != "verified"
        or oracle.get("scope") != "complete_observation"
    ):
        raise ValueError("complete adapter observation oracle is missing or unverified")
    values = [
        oracle.get(key)
        for key in (
            "expected_sha256",
            "observed_sha256",
            "fixture_source_oracle_sha256",
        )
    ]
    if (
        any(
            not isinstance(value, str)
            or len(value) != 64
            or any(char not in "0123456789abcdef" for char in value)
            for value in values
        )
        or len(set(values)) != 1
    ):
        raise ValueError("complete adapter observation oracle digests disagree")


def _source_snapshot() -> dict[str, Any]:
    return {
        **_COST["_provenance"](include_git=False),
        "phase_probe_sha256": sha256(Path(__file__).read_bytes()).hexdigest(),
    }


def _require_source_snapshot(expected: dict[str, Any]) -> None:
    if _source_snapshot() != expected:
        raise RuntimeError(
            "phase probe/runtime/helper source changed during measurement"
        )


def _admit_fresh_worker(workspace: Path, expected: dict[str, Any]) -> dict[str, Any]:
    if _PRELOADED_SOURCE_MODULES or not sys.flags.isolated:
        raise RuntimeError(
            "measurement worker reused preloaded runtime modules or lacks isolated startup"
        )
    binding = _COST["_worker_binding"](workspace)
    _require_source_snapshot(expected)
    if _LOADED_SOURCE_SNAPSHOT != expected:
        raise RuntimeError(
            "fresh worker imported different source from its parent snapshot"
        )
    return {
        "kind": "fresh-source-worker",
        "isolated": True,
        "preloaded_source_modules": [],
        **binding,
    }


def _verified_case(
    workspace: Path, expected: dict[str, Any], source_root: Path, **options: Any
) -> dict[str, Any]:
    execution = _admit_fresh_worker(workspace, expected)
    result = _measure_case(source_root, **options)
    _require_source_snapshot(expected)
    result["execution"] = execution
    result["status"] = "completed"
    result["provenance"]["phase_probe_source_stable"] = True
    return result


def _validate_execution(
    report: dict[str, Any], *, workspace: Path | None = None
) -> None:
    evidence = report.get("execution", {})
    if (
        evidence.get("kind") != "fresh-source-worker"
        or evidence.get("isolated") is not True
        or evidence.get("preloaded_source_modules") != []
    ):
        raise ValueError("measurement lacks verified fresh-source execution")
    if workspace is not None and (
        evidence.get("pycache_prefix") != str(workspace / "pycache")
        or evidence.get("temporary_root") != str(workspace / "temporary")
    ):
        raise ValueError("measurement belongs to a different worker workspace")


def _transport_fixture(source_root: Path, destination: Path) -> dict[str, Any]:
    """Copy one admitted member at a time, never an unbounded directory tree."""
    manifest, oracle, _headers = _admit_fixture(source_root)
    destination.mkdir()
    folder = destination / "1000000"
    folder.mkdir()
    for record, admitted in zip(manifest["records"], oracle["files"], strict=True):
        path = source_root / "1000000" / record["name"]
        identity = path.stat(follow_symlinks=False)
        observed = (
            identity.st_dev,
            identity.st_ino,
            identity.st_size,
            identity.st_mtime_ns,
            identity.st_ctime_ns,
        )
        expected = tuple(
            admitted[key]
            for key in ("device", "inode", "size_bytes", "modified_ns", "changed_ns")
        )
        if observed != expected:
            raise ValueError("fixture changed before bounded transport")
        content = _read_exact_regular(path, identity, maximum=_MAX_PAGE_BYTES)
        if sha256(content).hexdigest() != record["sha256"]:
            raise ValueError("fixture bytes changed during bounded transport")
        with (folder / record["name"]).open("xb") as stream:
            stream.write(content)
    (destination / _MANIFEST).write_text(json.dumps(manifest, sort_keys=True))
    return manifest


def _validate_fixture_binding(
    report: dict[str, Any], expected_manifest: dict[str, Any]
) -> None:
    fixture = report["fixture"]
    oracle = fixture["source_oracle"]
    observed_records = [
        {
            "name": bytes.fromhex(row["name_bytes"]).decode(),
            "size": row["size_bytes"],
            "sha256": row["sha256"],
        }
        for row in oracle["files"]
    ]
    if (
        fixture["manifest"] != expected_manifest
        or fixture["manifest_sha256"] != _COST["_canonical_digest"](expected_manifest)
        or observed_records != expected_manifest["records"]
        or report["qualification"] != expected_manifest["expected_qualification"]
        or report["content_oracle"]["fixture_source_oracle_sha256"]
        != _COST["_canonical_digest"](oracle)
    ):
        raise ValueError(
            "fresh worker observed different requested fixture bytes or qualification"
        )


def run_case(
    source_root: Path,
    *,
    workers: int,
    label: str = "case",
    instrumented: bool = True,
    collect_events: bool = False,
    inspect_images: bool = False,
    timeout_seconds: int = 300,
) -> dict[str, Any]:
    """Public supervisor: only a new isolated worker can return complete evidence.

    The caller's imported runtime never measures the case. The exact temporary
    fixture is copied with bounded identity/hash reads before worker startup.
    The timeout covers the owned worker; transport/startup is not observation time.
    """
    source_root = _temporary_root(source_root)
    if type(workers) is not int or not 1 <= workers <= MAX_PAGE_RENDER_WORKERS:
        raise ValueError("workers outside the production bound")
    if type(timeout_seconds) is not int or not 10 <= timeout_seconds <= 3600:
        raise ValueError("timeout outside 10..3600 seconds")
    _require_source_snapshot(_LOADED_SOURCE_SNAPSHOT)
    expected = _source_snapshot()
    with tempfile.TemporaryDirectory(prefix="qualification-case-") as temporary:
        workspace = Path(temporary).resolve()
        command = [
            sys.executable,
            "-I",
            __file__,
            "--case-worker",
            str(workspace),
            "--output",
            str(workspace / "unused.json"),
        ]
        command, environment = _COST["_fresh_python_environment"](command, workspace)
        fixture = workspace / "temporary" / "fixture"
        expected_manifest = _transport_fixture(source_root, fixture)
        _require_source_snapshot(expected)
        request = {
            "workers": workers,
            "label": label,
            "instrumented": instrumented,
            "collect_events": collect_events,
            "inspect_images": inspect_images,
        }
        (workspace / "case.json").write_text(json.dumps(request))
        (workspace / "expected-provenance.json").write_text(json.dumps(expected))
        completed = _COST["_bounded_worker"](
            command, timeout=timeout_seconds, workspace=workspace, env=environment
        )
        report = json.loads(completed.stdout)
        _require_source_snapshot(expected)
        _validate_execution(report, workspace=workspace)
        if (
            report["status"] != "completed"
            or report["label"] != label
            or report["workers"] != workers
            or report["instrumented"] is not instrumented
            or report["provenance"] != {**expected, "phase_probe_source_stable": True}
        ):
            raise ValueError("fresh worker case identity or source provenance differs")
        _validate_content_oracle(report)
        _validate_fixture_binding(report, expected_manifest)
        if instrumented:
            validate_attribution(report)
        return report


def _measure_case(
    source_root: Path,
    *,
    workers: int,
    label: str = "case",
    instrumented: bool = True,
    collect_events: bool = False,
    inspect_images: bool = False,
) -> dict[str, Any]:
    """Private in-process measurement/fault seam, preserving runtime exceptions.

    This cannot establish that imported Python functions match disk bytes. Its
    evidence always remains execution_unverified until an admitted fresh worker
    attests to its own import boundary. It is not a public measurement API.
    """
    if type(workers) is not int or not 1 <= workers <= MAX_PAGE_RENDER_WORKERS:
        raise ValueError("workers outside the production bound")
    if not _RUN_LOCK.acquire(blocking=False):
        raise RuntimeError("qualification probe cannot run concurrently in one process")
    try:
        _require_source_snapshot(_LOADED_SOURCE_SNAPSHOT)
        result = _run_case(
            source_root,
            workers=workers,
            label=label,
            instrumented=instrumented,
            collect_events=collect_events,
            inspect_images=inspect_images,
        )
        _require_source_snapshot(_LOADED_SOURCE_SNAPSHOT)
        return result
    finally:
        _RUN_LOCK.release()


def _run_case(
    source_root: Path,
    *,
    workers: int,
    label: str,
    instrumented: bool,
    collect_events: bool,
    inspect_images: bool,
) -> dict[str, Any]:
    source_root = _temporary_root(source_root)
    manifest, oracle, image_headers = _admit_fixture(
        source_root, inspect_images=inspect_images
    )
    _require_source_snapshot(_LOADED_SOURCE_SNAPSHOT)
    meter = _Meter(oracle)
    intervals: Counter[str] = Counter()
    performance = SourcePerformance()
    metrics: list[Any] = []

    class TimedQualifier(qualification.ImageGalleryQualifier):
        def __call__(self, *args: Any, **kwargs: Any) -> Any:
            start = monotonic_ns()
            try:
                return super().__call__(*args, **kwargs)
            finally:
                intervals["qualification_wall_ns"] += monotonic_ns() - start

    class TimedAdapter(VNextFilesystemSourceAdapter):
        def observe_gallery(self, *args: Any, **kwargs: Any) -> Any:
            start = monotonic_ns()
            try:
                return super().observe_gallery(*args, **kwargs)
            finally:
                intervals["observe_gallery_wall_ns"] += monotonic_ns() - start

    wall, cpu = monotonic_ns(), process_time_ns()
    with (
        meter.instrument() if instrumented else nullcontext(),
        performance.operation(metrics.append),
        FilesystemSource(source_root, performance=performance) as source,
    ):
        adapter = TimedAdapter(
            source,
            qualify_gallery=TimedQualifier(ArtifactRenderPolicy(), workers=workers),
            performance=performance,
        )
        _rows, evidence = _COST["_observe"](adapter, mode="first", oracle=oracle)
    intervals["complete_observation_wall_ns"] = monotonic_ns() - wall
    intervals["process_cpu_ns"] = process_time_ns() - cpu
    _require_source_snapshot(_LOADED_SOURCE_SNAPSHOT)
    if len(metrics) != 1 or metrics[0].status != "completed":
        raise RuntimeError("source operation did not produce its completed telemetry")
    if _admit_fixture(source_root)[0] != manifest:
        raise ValueError("synthetic fixture changed during measurement")
    _require_source_snapshot(_LOADED_SOURCE_SNAPSHOT)
    page_records = [
        row for row in manifest["records"] if row["name"] != "galleryinfo.txt"
    ]
    result: dict[str, Any] = {
        "format": _FORMAT,
        "status": "execution_unverified",
        "label": label,
        "workers": workers,
        "instrumented": instrumented,
        **intervals,
        "qualification": manifest["expected_qualification"],
        "fixture": {
            "page_count": len(page_records),
            "page_bytes": sum(row["size"] for row in page_records),
            "page_sizes": [row["size"] for row in page_records],
            "manifest_sha256": _COST["_canonical_digest"](manifest),
            "manifest": manifest,
            "image_headers": image_headers,
            "source_oracle": json.loads(json.dumps(oracle, default=bytes.hex)),
        },
        "content_oracle": evidence,
        "production_source_counters": {
            item.name: item.value for item in metrics[0].counters
        },
        "production_source_phases_ns_inclusive": {
            item.name: item.value for item in metrics[0].phases_ns
        },
        "provenance": {
            **_LOADED_SOURCE_SNAPSHOT,
            "phase_probe_source_stable": False,
        },
        "scope": _SCOPE,
        "future_candidate_decision_budgets": _DECISION_BUDGETS,
    }
    if instrumented:
        attribution = meter.report()
        result["attribution"] = attribution
        totals = attribution["event_totals_ns_nonadditive"]
        result["qualification_owner_other_ns"] = (
            result["qualification_wall_ns"]
            - totals.get("spool", 0)
            - totals.get("future_wait", 0)
        )
        validate_attribution(result)
        result["attribution_status"] = "complete"
        if not collect_events:
            del attribution["events"]
    else:
        result["attribution_status"] = "not_instrumented"
    return result


def _cli_matrix(
    workspace: Path, *, expected_provenance: dict[str, Any]
) -> dict[str, Any]:
    execution = _admit_fresh_worker(workspace, expected_provenance)
    if expected_provenance != _LOADED_SOURCE_SNAPSHOT:
        raise RuntimeError(
            "worker loaded different sources from the pre-launch snapshot"
        )
    cases = []
    for codec in ("JPEG", "PNG", "GIF", "WEBP"):
        for edge in (256, 1536):
            data = random.Random(47029).randbytes(edge * edge * 3)
            with Image.frombytes("RGB", (edge, edge), data) as image:
                output = BytesIO()
                image.save(output, format=codec)
            root = create_fixture(
                workspace / "temporary" / "fixtures" / f"{codec}-{edge}",
                [
                    (f"{page:03}.{codec.lower()}", output.getvalue())
                    for page in range(4)
                ],
            )
            for workers in (1, 4):
                _require_source_snapshot(expected_provenance)
                cases.append(
                    _verified_case(
                        workspace,
                        expected_provenance,
                        root,
                        workers=workers,
                        label=f"{codec}-{edge}",
                        inspect_images=True,
                    )
                )
    _require_source_snapshot(expected_provenance)
    return {
        "status": "completed",
        "format": _FORMAT,
        "cases": cases,
        "scope": _SCOPE,
        "matrix_provenance": expected_provenance,
        "execution": execution,
    }


def _validate_cli_report(
    report: dict[str, Any],
    *,
    expected_provenance: dict[str, Any] | None = None,
    workspace: Path | None = None,
) -> None:
    _validate_execution(report, workspace=workspace)
    expected_provenance = (
        _LOADED_SOURCE_SNAPSHOT if expected_provenance is None else expected_provenance
    )
    _require_source_snapshot(expected_provenance)
    if report.get("matrix_provenance") != expected_provenance:
        raise ValueError("qualification matrix lacks its pre-launch source snapshot")
    expected = {
        (f"{codec}-{edge}", workers)
        for codec in ("JPEG", "PNG", "GIF", "WEBP")
        for edge in (256, 1536)
        for workers in (1, 4)
    }
    if (
        report.get("status") != "completed"
        or report.get("format") != _FORMAT
        or len(report.get("cases", [])) != len(expected)
    ):
        raise ValueError("qualification matrix is incomplete")
    seen = set()
    fixtures: dict[str, dict[str, Any]] = {}
    for case in report["cases"]:
        _validate_execution(case, workspace=workspace)
        identity = (case["label"], case["workers"])
        if identity not in expected or identity in seen:
            raise ValueError("qualification matrix has duplicate or foreign cases")
        seen.add(identity)
        if (
            case["status"] != "completed"
            or case["format"] != _FORMAT
            or case["instrumented"] is not True
            or case["attribution_status"] != "complete"
            or {
                key: value
                for key, value in case["provenance"].items()
                if key != "phase_probe_source_stable"
            }
            != expected_provenance
            or case["provenance"]["phase_probe_source_stable"] is not True
        ):
            raise ValueError("qualification worker lacks complete stable attribution")
        for phase in (
            "source_read",
            "receipt_source_read",
            "source_hash",
            "spool_write",
            "spool_readback",
            "spool_hash",
            "decoder_buffer_read",
        ):
            operations = case["attribution"]["counters"].get(phase + "_calls")
            if type(operations) is not int or operations <= 0:
                raise ValueError("qualification worker omitted direct operation counts")
        validate_attribution(case)
        _validate_matrix_fixture(case)
        previous = fixtures.setdefault(case["label"], case["fixture"])
        if previous != case["fixture"]:
            raise ValueError(
                "qualification workers measured different fixture identities"
            )


def _validate_matrix_fixture(case: dict[str, Any]) -> None:
    fixture = case["fixture"]
    _validate_fixture_binding(case, fixture["manifest"])
    codec, edge_text = case["label"].split("-")
    edge = int(edge_text)
    records = fixture["manifest"]["records"]
    headers = fixture["image_headers"]
    oracle = fixture["source_oracle"]
    page_records = [row for row in records if row["name"] != "galleryinfo.txt"]
    if (
        fixture["page_count"] != 4
        or len(page_records) != 4
        or len(headers) != 4
        or fixture["page_sizes"] != [row["size"] for row in page_records]
        or fixture["page_bytes"] != sum(fixture["page_sizes"])
        or fixture["manifest_sha256"] != _COST["_canonical_digest"](fixture["manifest"])
        or case["content_oracle"]["fixture_source_oracle_sha256"]
        != _COST["_canonical_digest"](oracle)
        or case["qualification"]
        != {"accepted": True, "reason_code": None, "source_name": None}
    ):
        raise ValueError(
            "qualification matrix fixture lacks exact four-page content evidence"
        )
    expected_names = [f"{page:03}.{codec.lower()}" for page in range(4)]
    if [row["name"] for row in page_records] != expected_names:
        raise ValueError("qualification matrix has different fixture page names")
    for record, header in zip(page_records, headers, strict=True):
        if header != {**record, "codec": codec, "dimensions": [edge, edge]}:
            raise ValueError("qualification matrix fixture codec or dimensions differ")
    observed_records = [
        {
            "name": bytes.fromhex(row["name_bytes"]).decode(),
            "size": row["size_bytes"],
            "sha256": row["sha256"],
        }
        for row in oracle["files"]
    ]
    if records != observed_records:
        raise ValueError(
            "qualification matrix fixture headers differ from observed bytes"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=int, default=300)
    parser.add_argument("--worker", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--case-worker", type=Path, help=argparse.SUPPRESS)
    arguments = parser.parse_args()
    if not 10 <= arguments.timeout_seconds <= 3600:
        parser.error("timeout must be in 10..3600 seconds")
    if arguments.worker is not None or arguments.case_worker is not None:
        worker_root = arguments.worker or arguments.case_worker
        expected = json.loads((worker_root / "expected-provenance.json").read_text())
        _admit_fresh_worker(worker_root, expected)
        if arguments.case_worker is not None:
            options = json.loads((worker_root / "case.json").read_text())
            report = _verified_case(
                worker_root, expected, worker_root / "temporary" / "fixture", **options
            )
        else:
            report = _cli_matrix(worker_root, expected_provenance=expected)
        print(json.dumps(report))
        return 0
    if os.path.lexists(arguments.output):
        parser.error("output already exists")
    expected = _source_snapshot()
    _require_source_snapshot(_LOADED_SOURCE_SNAPSHOT)
    with tempfile.TemporaryDirectory(prefix="qualification-phases-") as temporary:
        workspace = Path(temporary).resolve()
        (workspace / "expected-provenance.json").write_text(json.dumps(expected))
        command = [
            sys.executable,
            "-I",
            __file__,
            "--worker",
            str(workspace),
            "--output",
            str(arguments.output),
        ]
        command, environment = _COST["_fresh_python_environment"](command, workspace)
        try:
            completed = _COST["_bounded_worker"](
                command,
                timeout=arguments.timeout_seconds,
                workspace=workspace,
                env=environment,
            )
            report = json.loads(completed.stdout)
            _validate_cli_report(
                report, expected_provenance=expected, workspace=workspace
            )
        except Exception as error:
            report = {
                "format": _FORMAT,
                "status": "incomplete",
                "error_type": type(error).__name__,
                "error": str(error),
            }
            if isinstance(error, subprocess.CalledProcessError):
                report["worker_stderr_tail"] = (error.stderr or "")[-8192:]
        _IO["_atomic_report"](arguments.output, report)
    return 0 if report["status"] == "completed" else 2


_LOADED_SOURCE_SNAPSHOT = _source_snapshot()


if __name__ == "__main__":
    raise SystemExit(main())
