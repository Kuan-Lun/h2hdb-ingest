__all__ = ["CBZReconciler"]

import fcntl
import json
import logging
import os
import re
import shutil
import stat
import tempfile
from collections.abc import Callable, Collection, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path, PurePosixPath
from threading import RLock
from time import monotonic, time_ns
from uuid import uuid4
from zipfile import ZIP_DEFLATED, BadZipFile, ZipFile, ZipInfo

from PIL import Image, ImageFile, ImageOps

from .config import (
    DEFAULT_CBZ_WORKERS,
    DEFAULT_STALE_TEMP_AGE_SECONDS,
    CBZGrouping,
)
from .models import CBZArtifact, DeduplicationPlan, ScannedFile, ScannedGallery
from .naming import gallery_name_to_cbz_file_name

CBZ_MANIFEST_VERSION = 1
STATE_FILE_NAME = ".h2hdb-cbz-state.json"
STATE_LOCK_FILE_NAME = ".h2hdb-cbz-state.lock"
STATE_VERSION = 1
IMAGE_SPOOL_MEMORY_LIMIT = 16 * 1024 * 1024
CBZ_PROGRESS_INTERVAL_SECONDS = 60.0
ARTIFACT_TEMP_PREFIX = ".h2hdb-ingest-artifact-"
PROJECTION_TEMP_PREFIX = ".h2hdb-ingest-projection-"
STATE_TEMP_PREFIX = ".h2hdb-ingest-state-"
_OWNED_TEMP_PATTERN_BY_PREFIX = {
    prefix: re.compile(rf"{re.escape(prefix)}[0-9a-f]{{32}}\.tmp")
    for prefix in (ARTIFACT_TEMP_PREFIX, PROJECTION_TEMP_PREFIX, STATE_TEMP_PREFIX)
}
NORMALIZED_IMAGE_SUFFIXES = frozenset(
    {".avif", ".bmp", ".jpeg", ".jpg", ".png", ".webp"}
)

Image.MAX_IMAGE_PIXELS = None
ImageFile.LOAD_TRUNCATED_IMAGES = True
logger = logging.getLogger(__name__)


@dataclass(slots=True)
class _ReconciliationState:
    owned: set[str]
    published: set[str]
    protected: set[str]
    current: dict[str, _CurrentProjection]
    current_revision: int | None = None
    pending: dict[str, str] = field(default_factory=dict)
    pending_revision: int | None = None


@dataclass(frozen=True, slots=True)
class _ProjectionSignature:
    device: int
    inode: int
    size_bytes: int
    modified_ns: int
    changed_ns: int


@dataclass(frozen=True, slots=True)
class _CurrentProjection:
    artifact_name: str
    signature: _ProjectionSignature


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as source:
        while chunk := source.read(4 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _content_addressed_name(gallery: ScannedGallery, digest: str) -> str:
    return f"{gallery.gid}-{digest}.cbz"


def _zip_info(name: str) -> ZipInfo:
    info = ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = ZIP_DEFLATED
    info.create_system = 3
    info.external_attr = 0o100644 << 16
    return info


class CBZReconciler:
    def __init__(
        self,
        *,
        artifact_store_path: Path,
        cbz_path: Path,
        max_image_short_side: int,
        grouping: CBZGrouping = CBZGrouping.flat,
        workers: int = DEFAULT_CBZ_WORKERS,
        stale_temp_age_seconds: float = DEFAULT_STALE_TEMP_AGE_SECONDS,
        event_logger: Callable[[str], None] | None = None,
        progress_interval_seconds: float = CBZ_PROGRESS_INTERVAL_SECONDS,
    ) -> None:
        self._artifact_store_path = artifact_store_path.resolve()
        self._cbz_path = cbz_path.resolve()
        if self._artifact_store_path == self._cbz_path:
            raise ValueError("artifact_store_path and cbz_path must be different")
        if self._artifact_store_path.is_relative_to(
            self._cbz_path
        ) or self._cbz_path.is_relative_to(self._artifact_store_path):
            raise ValueError(
                "artifact_store_path and cbz_path must not contain one another"
            )
        if max_image_short_side < 1:
            raise ValueError("max_image_short_side must be positive")
        if not 1 <= workers <= 32:
            raise ValueError("workers must be between 1 and 32")
        if stale_temp_age_seconds <= 0:
            raise ValueError("stale_temp_age_seconds must be positive")
        if progress_interval_seconds <= 0:
            raise ValueError("progress_interval_seconds must be positive")
        self._max_image_short_side = max_image_short_side
        self._grouping = grouping
        self._workers = workers
        self._stale_temp_age_ns = int(stale_temp_age_seconds * 1_000_000_000)
        self._event_logger = event_logger or logger.info
        self._progress_interval_seconds = progress_interval_seconds
        self._state_path = self._artifact_store_path / STATE_FILE_NAME
        self._state_lock_path = self._artifact_store_path / STATE_LOCK_FILE_NAME
        self._process_lock = RLock()
        self._state_lock_depth = 0

    @contextmanager
    def publication_guard(self) -> Iterator[None]:
        """Serialize catalog publication through projection finalization.

        Every ingest instance sharing an artifact store participates in this
        filesystem lock.  A newer owner therefore cannot commit the next catalog
        revision between an older owner's revision check and atomic projection
        swaps.
        """

        with self._locked_state():
            yield

    def prepare(self, plan: DeduplicationPlan) -> tuple[CBZArtifact, ...]:
        with self._locked_state():
            self._cbz_path.mkdir(parents=True, exist_ok=True)
            self._cleanup_stale_temporary_files()
            state = self._read_state()
            winners = plan.winners
            if len({gallery.gid for gallery in winners}) != len(winners):
                raise ValueError("CBZ preparation requires one winner per GID")
            total = len(winners)
            self._event_logger(
                "CBZ preparation started: " f"galleries={total} workers={self._workers}"
            )
            if not winners:
                self._event_logger(
                    "CBZ preparation completed: galleries=0 created=0 rebuilt=0 "
                    "reused=0 elapsed_s=0.000"
                )
                return ()

            started_at = monotonic()
            owned_snapshot = frozenset(state.owned)
            results: list[CBZArtifact | None] = [None] * total
            maximum_pending = max(1, self._workers * 2)
            gallery_iterator = iter(enumerate(winners))
            pending: dict[
                Future[CBZArtifact],
                tuple[int, ScannedGallery, float],
            ] = {}
            first_error: BaseException | None = None
            completed = 0
            created = 0
            rebuilt = 0
            next_progress_at = monotonic() + self._progress_interval_seconds

            with ThreadPoolExecutor(
                max_workers=self._workers,
                thread_name_prefix="h2hdb-cbz",
            ) as executor:

                def fill_pending() -> None:
                    while first_error is None and len(pending) < maximum_pending:
                        try:
                            index, gallery = next(gallery_iterator)
                        except StopIteration:
                            return
                        exclusions = frozenset(
                            source_file.sha256
                            for source_file in gallery.files
                            if source_file.sha256 in plan.excluded_file_sha256s
                        )
                        future = executor.submit(
                            self._ensure_cbz,
                            gallery,
                            owned_snapshot,
                            exclusions,
                        )
                        pending[future] = (index, gallery, monotonic())

                fill_pending()
                while pending:
                    timeout = max(0.0, next_progress_at - monotonic())
                    done, _not_done = wait(
                        pending,
                        timeout=timeout,
                        return_when=FIRST_COMPLETED,
                    )
                    for future in sorted(done, key=lambda item: pending[item][0]):
                        index, gallery, gallery_started_at = pending.pop(future)
                        try:
                            artifact = future.result()
                            results[index] = artifact
                            state_name = self._state_name(artifact.path)
                            if state_name not in state.owned:
                                state.owned.add(state_name)
                                self._write_state(state)
                            completed += 1
                            created += int(artifact.created)
                            rebuilt += int(artifact.rebuilt)
                            outcome = (
                                "created"
                                if artifact.created
                                else "rebuilt" if artifact.rebuilt else "reused"
                            )
                            self._event_logger(
                                "CBZ book prepared: "
                                f"index={index + 1} galleries={total} "
                                f"gallery={gallery.gallery_name!r} gid={gallery.gid} "
                                f"outcome={outcome} "
                                f"elapsed_s={monotonic() - gallery_started_at:.3f}"
                            )
                        except BaseException as error:
                            if first_error is None:
                                first_error = error
                    fill_pending()
                    if monotonic() >= next_progress_at:
                        self._event_logger(
                            "CBZ preparation in progress: "
                            f"completed={completed} galleries={total} "
                            f"in_flight={len(pending)} "
                            f"elapsed_s={monotonic() - started_at:.3f}"
                        )
                        next_progress_at = monotonic() + self._progress_interval_seconds

            if first_error is not None:
                first_error.add_note(
                    "CBZ preparation stopped after draining all submitted workers; "
                    f"completed={completed} total={total}"
                )
                raise first_error
            artifacts = tuple(artifact for artifact in results if artifact is not None)
            if len(artifacts) != total:
                raise RuntimeError("CBZ preparation completed without every result")
            self._event_logger(
                "CBZ preparation completed: "
                f"galleries={total} created={created} rebuilt={rebuilt} "
                f"reused={total - created - rebuilt} "
                f"elapsed_s={monotonic() - started_at:.3f}"
            )
            return artifacts

    def protect_for_publish(self, artifacts: tuple[CBZArtifact, ...]) -> None:
        with self._locked_state():
            selected = {self._state_name(artifact.path) for artifact in artifacts}
            state = self._read_state()
            if missing := selected - state.owned:
                raise RuntimeError(
                    "CBZ artifacts selected for publication are missing from ingest "
                    f"state: {sorted(missing)!r}"
                )
            if unavailable := {
                name
                for name in selected
                if not self._path_from_state_name(name).is_file()
            }:
                raise RuntimeError(
                    "CBZ artifacts selected for publication are unavailable: "
                    f"{sorted(unavailable)!r}"
                )
            state.protected.update(selected - state.published)
            self._write_state(state)

    def finalize_published(
        self,
        artifacts: tuple[CBZArtifact, ...],
        *,
        revision: int | None = None,
    ) -> None:
        if revision is not None and revision < 0:
            raise ValueError("Catalog revision must not be negative")
        with self._locked_state():
            self._cleanup_stale_temporary_files()
            selected = {self._state_name(artifact.path) for artifact in artifacts}
            state = self._read_state()
            if missing := selected - state.owned:
                raise RuntimeError(
                    "Published CBZ artifacts are missing from ingest state: "
                    f"{sorted(missing)!r}"
                )
            revision_floor = max(
                candidate
                for candidate in (state.current_revision, state.pending_revision, -1)
                if candidate is not None
            )
            if revision is not None and revision < revision_floor:
                raise RuntimeError(
                    "Refusing to overwrite a newer Komga projection: "
                    f"catalog revision {revision} is older than {revision_floor}"
                )

            recovered_pending = self._recoverable_pending_names(state)
            previously_managed = set(state.current) | recovered_pending
            next_current = self._plan_current_view(artifacts, previously_managed)

            # Persist the complete projection intent before touching the Komga tree.
            # A process crash can therefore recover copied files without treating
            # them as unknown operator-owned content or inventing duplicate names.
            state.pending = {
                name: self._state_name(artifact.path)
                for name, artifact in next_current.items()
            }
            state.pending_revision = revision
            self._write_state(state)

            materialized_current = self._materialize_current_view(
                next_current,
                previously_managed=previously_managed,
                current=state.current,
            )
            self._remove_stale_current_paths(
                previously_managed - set(next_current),
            )
            state.published.update(selected)
            state.protected.difference_update(selected)
            abandoned_staging = (
                state.owned - state.published - state.protected - selected
            )
            for name in sorted(abandoned_staging):
                candidate = self._path_from_state_name(name)
                try:
                    candidate.unlink()
                except FileNotFoundError:
                    pass
                else:
                    self._fsync_directory(candidate.parent)
                self._remove_empty_parents(candidate.parent)
            state.owned.difference_update(abandoned_staging)
            state.current = materialized_current
            if revision is not None:
                state.current_revision = revision
            state.pending = {}
            state.pending_revision = None
            self._write_state(state)

    def _ensure_cbz(
        self,
        gallery: ScannedGallery,
        owned: Collection[str],
        excluded_file_sha256s: frozenset[str],
    ) -> CBZArtifact:
        prior_variant = any(
            PurePosixPath(name).name.startswith(f"{gallery.gid}-") for name in owned
        )
        reusable = self._find_reusable_cbz(
            gallery,
            owned,
            excluded_file_sha256s,
        )
        write_required = reusable is None
        if reusable is None:
            target, digest = self._build_cbz(gallery, excluded_file_sha256s)
        else:
            target, digest = reusable
        stat = target.stat()
        return CBZArtifact(
            gallery=gallery,
            path=target.resolve(),
            size_bytes=stat.st_size,
            sha256=digest,
            modified_at=datetime.fromtimestamp(stat.st_mtime, tz=UTC),
            created=write_required and not prior_variant,
            rebuilt=write_required and prior_variant,
        )

    def _find_reusable_cbz(
        self,
        gallery: ScannedGallery,
        owned: Collection[str],
        excluded_file_sha256s: frozenset[str],
    ) -> tuple[Path, str] | None:
        for name in sorted(owned):
            if not PurePosixPath(name).name.startswith(
                f"{gallery.gid}-"
            ) or not name.endswith(".cbz"):
                continue
            candidate = self._path_from_state_name(name)
            if candidate.parent != self._storage_directory(gallery):
                continue
            if digest := self._matching_manifest_digest(
                candidate,
                gallery,
                excluded_file_sha256s,
            ):
                return candidate, digest
        return None

    def _matching_manifest_digest(
        self,
        path: Path,
        gallery: ScannedGallery,
        excluded_file_sha256s: frozenset[str],
    ) -> str | None:
        if not path.is_file():
            return None
        try:
            with ZipFile(path) as archive:
                manifest = json.loads(archive.comment.decode("utf-8"))
        except BadZipFile, OSError, UnicodeDecodeError, json.JSONDecodeError:
            return None
        if not isinstance(manifest, dict):
            return None
        digest = _sha256_file(path)
        if (
            manifest.get("version") == CBZ_MANIFEST_VERSION
            and manifest.get("sourceDigest") == gallery.source_digest
            and manifest.get("contentDigest") == gallery.content_digest
            and manifest.get("excludedFileSha256s") == sorted(excluded_file_sha256s)
            and manifest.get("resizePolicy") == "webtoon-short-side-no-upscale-v1"
            and manifest.get("maxImageShortSide") == self._max_image_short_side
            and path.name.startswith(f"{gallery.gid}-{digest}")
        ):
            return digest
        return None

    def _build_cbz(
        self,
        gallery: ScannedGallery,
        excluded_file_sha256s: frozenset[str],
    ) -> tuple[Path, str]:
        output_directory = self._storage_directory(gallery)
        output_directory.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = self._create_owned_temporary_file(
            output_directory,
            prefix=ARTIFACT_TEMP_PREFIX,
        )
        os.close(descriptor)
        try:
            with ZipFile(
                temporary,
                mode="w",
                compression=ZIP_DEFLATED,
                compresslevel=9,
            ) as archive:
                member_names: set[str] = set()
                for file in gallery.files:
                    if file.sha256 in excluded_file_sha256s:
                        continue
                    member_name = self._member_name(file)
                    member_name = self._unique_member_name(
                        member_name,
                        file,
                        member_names,
                    )
                    member_names.add(member_name)
                    self._write_member(archive, member_name, file)
                archive.comment = json.dumps(
                    {
                        "version": CBZ_MANIFEST_VERSION,
                        "sourceDigest": gallery.source_digest,
                        "contentDigest": gallery.content_digest,
                        "excludedFileSha256s": sorted(excluded_file_sha256s),
                        "resizePolicy": "webtoon-short-side-no-upscale-v1",
                        "maxImageShortSide": self._max_image_short_side,
                        "files": [
                            {"name": file.name, "sha256": file.sha256}
                            for file in gallery.files
                        ],
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            with temporary.open("rb") as completed:
                os.fsync(completed.fileno())
            digest = _sha256_file(temporary)
            target = output_directory / _content_addressed_name(gallery, digest)
            try:
                os.link(temporary, target)
            except FileExistsError:
                if target.is_symlink():
                    raise RuntimeError(
                        f"Refusing content-addressed artifact symlink: {target}"
                    )
                if _sha256_file(target) != digest:
                    target = output_directory / (
                        f"{gallery.gid}-{digest}-{uuid4().hex}.cbz"
                    )
                    os.link(temporary, target)
            temporary.unlink()
            self._fsync_directory(output_directory)
            return target, digest
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise

    @staticmethod
    def _member_name(file: ScannedFile) -> str:
        suffix = file.path.suffix.casefold()
        if suffix not in NORMALIZED_IMAGE_SUFFIXES and suffix != ".gif":
            return file.name
        if suffix == ".gif":
            return file.name
        return f"{file.path.stem}.jpg"

    def _write_member(
        self,
        archive: ZipFile,
        member_name: str,
        file: ScannedFile,
    ) -> None:
        suffix = file.path.suffix.casefold()
        if suffix not in NORMALIZED_IMAGE_SUFFIXES and suffix != ".gif":
            with (
                file.path.open("rb") as source,
                archive.open(_zip_info(member_name), "w") as output,
            ):
                shutil.copyfileobj(source, output, length=4 * 1024 * 1024)
            return

        # Keep at most one transformed member in memory.  Large images spill to
        # a temporary file instead of retaining the complete CBZ payload in RAM.
        with tempfile.SpooledTemporaryFile(
            max_size=IMAGE_SPOOL_MEMORY_LIMIT,
            mode="w+b",
        ) as transformed:
            self._write_normalized_image(file, transformed)
            transformed.seek(0)
            with archive.open(_zip_info(member_name), "w") as output:
                shutil.copyfileobj(transformed, output, length=4 * 1024 * 1024)

    def _write_normalized_image(
        self,
        file: ScannedFile,
        output: tempfile.SpooledTemporaryFile[bytes],
    ) -> None:
        suffix = file.path.suffix.casefold()
        with Image.open(file.path) as source:
            image = ImageOps.exif_transpose(source)
            # Bound the short side, not the long side.  This retains readable
            # long-strip/webtoon pages while
            # Pillow's thumbnail() guarantee prevents upscaling small images.
            if image.height >= image.width:
                scale = self._max_image_short_side / image.width
                bounds = (
                    self._max_image_short_side,
                    int(image.height * scale),
                )
            else:
                scale = self._max_image_short_side / image.height
                bounds = (
                    int(image.width * scale),
                    self._max_image_short_side,
                )
            image.thumbnail(bounds, Image.Resampling.LANCZOS)
            if suffix == ".gif":
                image.save(output, format="GIF")
                return
            if image.has_transparency_data:
                foreground = image.convert("RGBA")
                background = Image.new("RGBA", foreground.size, "white")
                image = Image.alpha_composite(background, foreground).convert("RGB")
            if image.mode not in {"RGB", "L"}:
                image = image.convert("RGB")
            image.save(output, format="JPEG", quality=90, optimize=True)

    @staticmethod
    def _unique_member_name(
        desired: str,
        source_file: ScannedFile,
        members: set[str],
    ) -> str:
        used = {name.casefold() for name in members}
        if desired.casefold() not in used:
            return desired
        desired_path = PurePosixPath(desired)
        source_key = sha256(source_file.name.encode("utf-8")).hexdigest()[:12]
        candidate = f"{desired_path.stem}-{source_key}{desired_path.suffix}"
        suffix = 2
        while candidate.casefold() in used:
            candidate = (
                f"{desired_path.stem}-{source_key}-{suffix}{desired_path.suffix}"
            )
            suffix += 1
        return candidate

    def _storage_directory(self, gallery: ScannedGallery) -> Path:
        return self._safe_grouped_directory(
            self._artifact_store_path,
            gallery,
            label="artifact_store_path",
        )

    def _current_directory(self, gallery: ScannedGallery) -> Path:
        return self._safe_grouped_directory(
            self._cbz_path,
            gallery,
            label="cbz_path",
        )

    def _safe_grouped_directory(
        self,
        root: Path,
        gallery: ScannedGallery,
        *,
        label: str,
    ) -> Path:
        directory = self._grouped_directory(root, gallery).resolve(strict=False)
        if not directory.is_relative_to(root):
            raise RuntimeError(f"Unsafe CBZ grouping path outside {label}: {directory}")
        return directory

    def _grouped_directory(
        self,
        root: Path,
        gallery: ScannedGallery,
    ) -> Path:
        upload_date = gallery.upload_time.date()
        match self._grouping:
            case CBZGrouping.flat:
                return root
            case CBZGrouping.date_yyyy:
                return root / f"{upload_date.year:04d}"
            case CBZGrouping.date_yyyy_mm:
                return root / f"{upload_date.year:04d}" / f"{upload_date.month:02d}"
            case CBZGrouping.date_yyyy_mm_dd:
                return (
                    root
                    / f"{upload_date.year:04d}"
                    / f"{upload_date.month:02d}"
                    / f"{upload_date.day:02d}"
                )
        raise ValueError(f"Unsupported CBZ grouping: {self._grouping}")

    def _plan_current_view(
        self,
        artifacts: tuple[CBZArtifact, ...],
        managed_current: set[str],
    ) -> dict[str, CBZArtifact]:
        planned: dict[str, CBZArtifact] = {}
        planned_keys: set[str] = set()
        for artifact in artifacts:
            attempt = 0
            while True:
                leaf = self._current_leaf(artifact, attempt)
                target = self._current_directory(artifact.gallery) / leaf
                name = self._current_state_name(target)
                key = name.casefold()
                target_exists = target.exists() or target.is_symlink()
                if key not in planned_keys and (
                    not target_exists or name in managed_current
                ):
                    planned[name] = artifact
                    planned_keys.add(key)
                    break
                attempt += 1
                if attempt > 10_000:
                    raise RuntimeError(
                        "Unable to choose a unique managed CBZ name for "
                        f"{artifact.gallery.gallery_name!r}"
                    )
        return planned

    @staticmethod
    def _current_leaf(artifact: CBZArtifact, attempt: int) -> str:
        gallery = artifact.gallery
        if attempt == 0:
            source_name = gallery.gallery_name
        elif attempt == 1:
            source_name = f"{gallery.gallery_name} [{gallery.gid}]"
        else:
            source_name = f"{gallery.gallery_name} [{gallery.gid}-{attempt}]"
        return gallery_name_to_cbz_file_name(source_name)

    def _materialize_current_view(
        self,
        planned: dict[str, CBZArtifact],
        *,
        previously_managed: set[str],
        current: dict[str, _CurrentProjection],
    ) -> dict[str, _CurrentProjection]:
        materialized: dict[str, _CurrentProjection] = {}
        for name, artifact in planned.items():
            artifact_name = self._state_name(artifact.path)
            source = self._path_from_state_name(artifact_name)
            if not source.is_file():
                raise RuntimeError(f"Published CBZ artifact is unavailable: {source}")
            target = self._current_path_from_state_name(name)
            signature = self._projection_signature(target)
            previous = current.get(name)
            if (
                previous is not None
                and previous.artifact_name == artifact_name
                and previous.signature == signature
            ):
                materialized[name] = previous
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target = self._current_path_from_state_name(name)
            if (
                target.exists() or target.is_symlink()
            ) and name not in previously_managed:
                raise RuntimeError(
                    "Refusing to replace an unmanaged Komga library path: " f"{target}"
                )
            self._atomic_copy(
                source,
                target,
                replace_managed=name in previously_managed,
            )
            signature = self._projection_signature(target)
            if signature is None:
                raise RuntimeError(
                    f"Komga projection is not a regular file after copy: {target}"
                )
            materialized[name] = _CurrentProjection(
                artifact_name=artifact_name,
                signature=signature,
            )
        return materialized

    @staticmethod
    def _projection_signature(path: Path) -> _ProjectionSignature | None:
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            return None
        if not stat.S_ISREG(metadata.st_mode):
            return None
        return _ProjectionSignature(
            device=metadata.st_dev,
            inode=metadata.st_ino,
            size_bytes=metadata.st_size,
            modified_ns=metadata.st_mtime_ns,
            changed_ns=metadata.st_ctime_ns,
        )

    def _atomic_copy(
        self,
        source: Path,
        target: Path,
        *,
        replace_managed: bool,
    ) -> None:
        descriptor, temporary = self._create_owned_temporary_file(
            target.parent,
            prefix=PROJECTION_TEMP_PREFIX,
        )
        try:
            with (
                os.fdopen(descriptor, "wb") as output,
                source.open("rb") as source_file,
            ):
                shutil.copyfileobj(source_file, output, length=4 * 1024 * 1024)
                output.flush()
                os.fsync(output.fileno())
            if replace_managed:
                temporary.replace(target)
            else:
                # Linking the fully copied temporary into the same directory gives
                # us a portable no-replace operation.  Its inode has never belonged
                # to the immutable artifact, so the Komga view remains independent.
                os.link(temporary, target)
                temporary.unlink()
            self._fsync_directory(target.parent)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise

    def _recoverable_pending_names(self, state: _ReconciliationState) -> set[str]:
        recovered: set[str] = set()
        for name, artifact_name in state.pending.items():
            if name in state.current:
                recovered.add(name)
                continue
            target = self._current_path_from_state_name(name)
            if not target.exists() and not target.is_symlink():
                recovered.add(name)
                continue
            if target.is_symlink():
                continue
            source = self._path_from_state_name(artifact_name)
            try:
                if (
                    source.is_file()
                    and target.is_file()
                    and source.stat().st_size == target.stat().st_size
                    and _sha256_file(source) == _sha256_file(target)
                ):
                    recovered.add(name)
            except OSError:
                continue
        return recovered

    def _remove_stale_current_paths(self, stale_names: set[str]) -> None:
        for name in sorted(stale_names):
            candidate = self._current_path_from_state_name(name)
            try:
                candidate.unlink()
            except FileNotFoundError:
                pass
            else:
                self._fsync_directory(candidate.parent)
            self._remove_empty_current_parents(candidate.parent)

    def _remove_empty_current_parents(self, directory: Path) -> None:
        current = directory
        while current != self._cbz_path:
            try:
                current.rmdir()
            except OSError:
                return
            current = current.parent

    def _current_state_name(self, path: Path) -> str:
        try:
            parent = path.parent.resolve(strict=False)
            if not parent.is_relative_to(self._cbz_path):
                raise ValueError
            relative = (parent / path.name).relative_to(self._cbz_path)
        except ValueError as error:
            raise RuntimeError(
                f"Komga CBZ projection is outside cbz_path: {path}"
            ) from error
        if relative == Path() or ".." in relative.parts:
            raise RuntimeError(f"Unsafe Komga CBZ projection path: {path}")
        return relative.as_posix()

    def _current_path_from_state_name(self, name: str) -> Path:
        relative = PurePosixPath(name)
        if (
            relative.is_absolute()
            or not relative.parts
            or any(part in {"", ".", ".."} for part in relative.parts)
        ):
            raise RuntimeError(f"Unsafe Komga CBZ projection target: {name!r}")
        candidate = self._cbz_path.joinpath(*relative.parts)
        resolved_parent = candidate.parent.resolve(strict=False)
        if not resolved_parent.is_relative_to(self._cbz_path):
            raise RuntimeError(
                "Unsafe Komga CBZ projection target outside cbz_path: " f"{candidate}"
            )
        return resolved_parent / candidate.name

    def _state_name(self, path: Path) -> str:
        try:
            relative = path.resolve().relative_to(self._artifact_store_path)
        except ValueError as error:
            raise RuntimeError(
                f"CBZ artifact is outside artifact store: {path}"
            ) from error
        if relative == Path() or ".." in relative.parts:
            raise RuntimeError(f"Unsafe CBZ artifact path: {path}")
        return relative.as_posix()

    def _path_from_state_name(self, name: str) -> Path:
        relative = PurePosixPath(name)
        if (
            relative.is_absolute()
            or not relative.parts
            or any(part in {"", ".", ".."} for part in relative.parts)
        ):
            raise RuntimeError(f"Unsafe CBZ reconciliation target: {name!r}")
        candidate = self._artifact_store_path.joinpath(*relative.parts)
        resolved_parent = candidate.parent.resolve(strict=False)
        if not resolved_parent.is_relative_to(self._artifact_store_path):
            raise RuntimeError(
                "Unsafe CBZ reconciliation target outside artifact store: "
                f"{candidate}"
            )
        resolved_candidate = resolved_parent / candidate.name
        if resolved_candidate.is_symlink():
            raise RuntimeError(
                f"Unsafe symlink in CBZ artifact state: {resolved_candidate}"
            )
        return resolved_candidate

    def _remove_empty_parents(self, directory: Path) -> None:
        current = directory
        while current != self._artifact_store_path:
            try:
                current.rmdir()
            except OSError:
                return
            current = current.parent

    @staticmethod
    def _create_owned_temporary_file(
        directory: Path,
        *,
        prefix: str,
    ) -> tuple[int, Path]:
        if prefix not in _OWNED_TEMP_PATTERN_BY_PREFIX:
            raise ValueError(f"Unsupported ingest temporary-file prefix: {prefix}")
        for _attempt in range(100):
            candidate = directory / f"{prefix}{uuid4().hex}.tmp"
            try:
                descriptor = os.open(
                    candidate,
                    os.O_CREAT | os.O_EXCL | os.O_RDWR,
                    0o600,
                )
            except FileExistsError:
                continue
            return descriptor, candidate
        raise RuntimeError(
            f"Unable to allocate an ingest temporary file in {directory}"
        )

    def _cleanup_stale_temporary_files(self) -> None:
        """Remove only old files in ingest's reserved temporary namespaces.

        The caller holds the artifact-store flock, so no cooperating publisher
        can own a live temporary file.  The age floor also protects recent files
        from a process using a misconfigured coordination domain.
        """

        removed_artifacts = self._cleanup_owned_temporary_files(
            self._artifact_store_path,
            prefix=ARTIFACT_TEMP_PREFIX,
            recursive=True,
        )
        removed_projections = self._cleanup_owned_temporary_files(
            self._cbz_path,
            prefix=PROJECTION_TEMP_PREFIX,
            recursive=True,
        )
        removed_states = self._cleanup_owned_temporary_files(
            self._artifact_store_path,
            prefix=STATE_TEMP_PREFIX,
            recursive=False,
        )
        if removed_artifacts or removed_projections or removed_states:
            self._event_logger(
                "Stale ingest temporary files removed: "
                f"artifact_builds={removed_artifacts} "
                f"projections={removed_projections} states={removed_states}"
            )

    def _cleanup_owned_temporary_files(
        self,
        root: Path,
        *,
        prefix: str,
        recursive: bool,
    ) -> int:
        pattern = _OWNED_TEMP_PATTERN_BY_PREFIX[prefix]
        if not root.is_dir():
            return 0
        if recursive:
            candidates: list[Path] = []
            for directory, child_directories, file_names in os.walk(
                root,
                topdown=True,
                followlinks=False,
            ):
                directory_path = Path(directory)
                child_directories[:] = [
                    name
                    for name in child_directories
                    if not (directory_path / name).is_symlink()
                ]
                try:
                    resolved_directory = directory_path.resolve(strict=True)
                except OSError:
                    continue
                if not resolved_directory.is_relative_to(root):
                    child_directories.clear()
                    continue
                candidates.extend(
                    resolved_directory / name
                    for name in file_names
                    if pattern.fullmatch(name)
                )
        else:
            try:
                candidates = [
                    candidate
                    for candidate in root.iterdir()
                    if pattern.fullmatch(candidate.name)
                ]
            except OSError as error:
                self._event_logger(
                    "Unable to inspect ingest temporary files: "
                    f"root={root} error={error!r}"
                )
                return 0

        now_ns = time_ns()
        removed = 0
        for candidate in sorted(candidates):
            try:
                metadata = candidate.lstat()
                if not stat.S_ISREG(metadata.st_mode):
                    continue
                if now_ns - metadata.st_mtime_ns < self._stale_temp_age_ns:
                    continue
                descriptor = os.open(
                    candidate,
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                )
                try:
                    opened = os.fstat(descriptor)
                    current = candidate.lstat()
                    if (
                        not stat.S_ISREG(opened.st_mode)
                        or (opened.st_dev, opened.st_ino)
                        != (current.st_dev, current.st_ino)
                        or now_ns - opened.st_mtime_ns < self._stale_temp_age_ns
                    ):
                        continue
                    candidate.unlink()
                finally:
                    os.close(descriptor)
                self._fsync_directory(candidate.parent)
                removed += 1
            except FileNotFoundError:
                continue
            except OSError as error:
                self._event_logger(
                    "Unable to remove stale ingest temporary file: "
                    f"path={candidate} error={error!r}"
                )
        return removed

    @contextmanager
    def _locked_state(self) -> Iterator[None]:
        self._artifact_store_path.mkdir(parents=True, exist_ok=True)
        with self._process_lock:
            if self._state_lock_depth:
                self._state_lock_depth += 1
                try:
                    yield
                finally:
                    self._state_lock_depth -= 1
                return
            with self._state_lock_path.open("a+b") as lock_file:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                self._state_lock_depth = 1
                try:
                    yield
                finally:
                    self._state_lock_depth = 0
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    @staticmethod
    def _fsync_directory(directory: Path) -> None:
        descriptor = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _read_state(self) -> _ReconciliationState:
        try:
            raw = json.loads(self._state_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return _ReconciliationState(set(), set(), set(), {})
        except (OSError, json.JSONDecodeError) as error:
            raise RuntimeError(
                f"Unable to read CBZ state {self._state_path}: {error}"
            ) from error
        if not isinstance(raw, dict) or raw.get("version") != STATE_VERSION:
            raise RuntimeError(f"Unsupported CBZ state version: {self._state_path}")
        names = raw.get("owned")
        if not isinstance(names, list) or not all(
            isinstance(name, str) for name in names
        ):
            raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
        owned = set(names)
        for name in owned:
            self._path_from_state_name(name)
        published_names = raw.get("published")
        if not isinstance(published_names, list) or not all(
            isinstance(name, str) for name in published_names
        ):
            raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
        published = set(published_names)
        for name in published:
            self._path_from_state_name(name)
        if not published <= owned:
            raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
        protected_names = raw.get("protected")
        if not isinstance(protected_names, list) or not all(
            isinstance(name, str) for name in protected_names
        ):
            raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
        protected = set(protected_names)
        for name in protected:
            self._path_from_state_name(name)
        if not protected <= owned or protected & published:
            raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
        current = self._read_current_projections(raw.get("current"), owned)

        current_revision = self._optional_revision(
            raw.get("currentRevision"),
            label="currentRevision",
        )
        pending_revision = self._optional_revision(
            raw.get("pendingRevision"),
            label="pendingRevision",
        )
        pending_raw = raw.get("pending")
        if not isinstance(pending_raw, dict) or not all(
            isinstance(name, str) and isinstance(artifact_name, str)
            for name, artifact_name in pending_raw.items()
        ):
            raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
        pending = dict(pending_raw)
        for name, artifact_name in pending.items():
            self._current_path_from_state_name(name)
            self._path_from_state_name(artifact_name)
        if not set(pending.values()) <= owned:
            raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
        return _ReconciliationState(
            owned=owned,
            published=published,
            protected=protected,
            current=current,
            current_revision=current_revision,
            pending=pending,
            pending_revision=pending_revision,
        )

    def _read_current_projections(
        self,
        raw: object,
        owned: set[str],
    ) -> dict[str, _CurrentProjection]:
        if not isinstance(raw, dict):
            raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
        current: dict[str, _CurrentProjection] = {}
        for name, projection_raw in raw.items():
            if not isinstance(name, str) or not isinstance(projection_raw, dict):
                raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
            artifact_name = projection_raw.get("artifact")
            signature_raw = projection_raw.get("signature")
            if not isinstance(artifact_name, str) or signature_raw is None:
                raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
            self._current_path_from_state_name(name)
            self._path_from_state_name(artifact_name)
            if artifact_name not in owned:
                raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
            current[name] = _CurrentProjection(
                artifact_name=artifact_name,
                signature=self._read_projection_signature(signature_raw),
            )
        return current

    def _read_projection_signature(self, raw: object) -> _ProjectionSignature:
        if not isinstance(raw, dict):
            raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")

        def integer(name: str, *, nonnegative: bool = False) -> int:
            value = raw.get(name)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or (nonnegative and value < 0)
            ):
                raise RuntimeError(f"Invalid CBZ state file: {self._state_path}")
            return value

        return _ProjectionSignature(
            device=integer("device", nonnegative=True),
            inode=integer("inode", nonnegative=True),
            size_bytes=integer("sizeBytes", nonnegative=True),
            modified_ns=integer("modifiedNs"),
            changed_ns=integer("changedNs"),
        )

    def _optional_revision(self, value: object, *, label: str) -> int | None:
        if value is None:
            return None
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise RuntimeError(f"Invalid {label} in CBZ state file: {self._state_path}")
        return value

    def _write_state(self, state: _ReconciliationState) -> None:
        document = json.dumps(
            {
                "version": STATE_VERSION,
                "current": {
                    name: self._current_projection_document(projection)
                    for name, projection in sorted(state.current.items())
                },
                "currentRevision": state.current_revision,
                "owned": sorted(state.owned),
                "pending": dict(sorted(state.pending.items())),
                "pendingRevision": state.pending_revision,
                "protected": sorted(state.protected),
                "published": sorted(state.published),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        descriptor, temporary = self._create_owned_temporary_file(
            self._artifact_store_path,
            prefix=STATE_TEMP_PREFIX,
        )
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as state_file:
                state_file.write(document)
                state_file.flush()
                os.fsync(state_file.fileno())
            temporary.replace(self._state_path)
            self._fsync_directory(self._artifact_store_path)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise

    def _current_projection_document(
        self,
        projection: _CurrentProjection,
    ) -> dict[str, object]:
        signature = projection.signature
        return {
            "artifact": projection.artifact_name,
            "signature": {
                "device": signature.device,
                "inode": signature.inode,
                "sizeBytes": signature.size_bytes,
                "modifiedNs": signature.modified_ns,
                "changedNs": signature.changed_ns,
            },
        }
