__all__ = [
    "CBZArtifact",
    "CBZGalleryDescriptor",
    "CBZPreparationFile",
    "CBZPreparationMetadata",
    "CBZPreparationRequest",
    "CBZPreparationSummary",
    "CBZStreamingPreparationRequest",
    "DeduplicationPlan",
    "FileHashCacheEntry",
    "FileHashCacheKey",
    "FileStatSignature",
    "FilesystemDiscoveryBatch",
    "FilesystemDiscoverySummary",
    "FilesystemGalleryDiscovery",
    "FilesystemGalleryObservation",
    "FilesystemScanBatch",
    "ScannedFile",
    "ScannedGallery",
    "ScannedGalleryChunk",
    "ScannedGalleryCompletion",
    "ScannedGalleryManifest",
    "SyncOutcome",
]

from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ScannedFile:
    path: Path
    name: str
    size_bytes: int
    sha256: str
    relative_locator: str | None = None
    signature: FileStatSignature | None = None


@dataclass(frozen=True, slots=True)
class FileStatSignature:
    """Filesystem identity used to decide whether a cached digest is reusable."""

    device: int
    inode: int
    size_bytes: int
    modified_ns: int
    changed_ns: int


@dataclass(frozen=True, slots=True)
class FileHashCacheKey:
    """A cache key whose locator remains meaningful outside this process."""

    root_locator: str
    relative_locator: str
    signature: FileStatSignature


@dataclass(frozen=True, slots=True)
class FileHashCacheEntry:
    key: FileHashCacheKey
    sha256: str


@dataclass(frozen=True, slots=True)
class FilesystemGalleryDiscovery:
    """One gallery found during the metadata-only discovery pass."""

    relative_folder: str
    gallery_name: str
    metadata_signature: FileStatSignature

    def __post_init__(self) -> None:
        if not self.relative_folder:
            raise ValueError("relative_folder must not be blank")
        if not self.gallery_name:
            raise ValueError("gallery_name must not be blank")


@dataclass(frozen=True, slots=True)
class FilesystemDiscoveryBatch:
    """A bounded set of galleries to durably declare before source scanning."""

    galleries: tuple[FilesystemGalleryDiscovery, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "galleries", tuple(self.galleries))
        if not self.galleries:
            raise ValueError("a discovery batch must not be empty")


@dataclass(frozen=True, slots=True)
class FilesystemDiscoverySummary:
    scan_attempt: str
    gallery_count: int
    tree_observation_sha256: str

    def __post_init__(self) -> None:
        if not self.scan_attempt:
            raise ValueError("scan_attempt must not be blank")
        if self.gallery_count < 0:
            raise ValueError("gallery_count must not be negative")
        if len(self.tree_observation_sha256) != 64:
            raise ValueError("tree_observation_sha256 must be a SHA-256 digest")
        try:
            bytes.fromhex(self.tree_observation_sha256)
        except ValueError as error:
            raise ValueError("tree_observation_sha256 must be hexadecimal") from error


@dataclass(frozen=True, slots=True)
class FilesystemGalleryObservation:
    relative_folder: str
    directory_entry_count: int
    directory_observation_sha256: str

    def __post_init__(self) -> None:
        if not self.relative_folder:
            raise ValueError("relative_folder must not be blank")
        if self.directory_entry_count < 0:
            raise ValueError("directory_entry_count must not be negative")
        if len(self.directory_observation_sha256) != 64:
            raise ValueError("directory_observation_sha256 must be a SHA-256 digest")
        try:
            bytes.fromhex(self.directory_observation_sha256)
        except ValueError as error:
            raise ValueError(
                "directory_observation_sha256 must be hexadecimal"
            ) from error


@dataclass(frozen=True, slots=True)
class ScannedGalleryManifest:
    """Gallery metadata shared by every bounded file chunk in one attempt."""

    gallery_attempt: str
    folder: Path
    relative_folder: str
    gallery_name: str
    gid: int
    title: str
    summary: str
    upload_account: str
    upload_time: datetime
    download_time: datetime
    modified_time: datetime
    tags: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class ScannedGalleryCompletion:
    """Final observation needed to durably seal and later revalidate a gallery."""

    scan_observation_version: int
    metadata_sha256: str
    scan_observation_sha256: str
    canonical_source_manifest_sha256: str
    canonical_source_manifest_version: int
    raw_content_sha256: str | None
    source_file_count: int
    pages: int
    directory_entry_count: int
    directory_observation_sha256: str

    def __post_init__(self) -> None:
        if self.scan_observation_version <= 0:
            raise ValueError("scan_observation_version must be positive")
        if self.canonical_source_manifest_version <= 0:
            raise ValueError("canonical_source_manifest_version must be positive")
        if (
            min(
                self.source_file_count,
                self.pages,
                self.directory_entry_count,
            )
            < 0
        ):
            raise ValueError("gallery completion counts must not be negative")
        for label, value in (
            ("metadata_sha256", self.metadata_sha256),
            ("scan_observation_sha256", self.scan_observation_sha256),
            (
                "canonical_source_manifest_sha256",
                self.canonical_source_manifest_sha256,
            ),
            ("directory_observation_sha256", self.directory_observation_sha256),
        ):
            if len(value) != 64:
                raise ValueError(f"{label} must be a SHA-256 hexadecimal digest")
            try:
                bytes.fromhex(value)
            except ValueError as error:
                raise ValueError(f"{label} must be hexadecimal") from error
        if self.raw_content_sha256 is not None:
            if len(self.raw_content_sha256) != 64:
                raise ValueError(
                    "raw_content_sha256 must be a SHA-256 hexadecimal digest"
                )
            try:
                bytes.fromhex(self.raw_content_sha256)
            except ValueError as error:
                raise ValueError("raw_content_sha256 must be hexadecimal") from error


@dataclass(frozen=True, slots=True)
class ScannedGalleryChunk:
    """A bounded piece of a gallery; only its final piece seals the manifest."""

    manifest: ScannedGalleryManifest
    chunk_index: int
    files: tuple[ScannedFile, ...]
    completion: ScannedGalleryCompletion | None = None

    def __post_init__(self) -> None:
        if self.chunk_index < 0:
            raise ValueError("chunk_index must not be negative")

    @property
    def complete(self) -> bool:
        return self.completion is not None


@dataclass(frozen=True, slots=True)
class FilesystemScanBatch:
    """A durable-write-sized collection of gallery chunks."""

    chunks: tuple[ScannedGalleryChunk, ...]

    @property
    def gallery_count(self) -> int:
        return len({chunk.manifest.gallery_attempt for chunk in self.chunks})

    @property
    def file_count(self) -> int:
        return sum(len(chunk.files) for chunk in self.chunks)


@dataclass(frozen=True, slots=True)
class ScannedGallery:
    folder: Path
    gallery_name: str
    gid: int
    title: str
    summary: str
    upload_account: str
    upload_time: datetime
    download_time: datetime
    modified_time: datetime
    pages: int
    tags: tuple[tuple[str, str], ...]
    files: tuple[ScannedFile, ...]
    metadata_sha256: str
    source_digest: str
    content_digest: str | None

    @property
    def publication_id(self) -> str:
        return f"urn:h2h:gallery:{self.gid}"

    @property
    def language(self) -> str:
        languages = [value for name, value in self.tags if name == "language" and value]
        return languages[0] if languages else "und"


@dataclass(frozen=True, slots=True)
class DeduplicationPlan:
    canonical_galleries: tuple[ScannedGallery, ...]
    winners: tuple[ScannedGallery, ...]
    losers: tuple[ScannedGallery, ...]
    duplicate_of_by_gallery_name: tuple[tuple[str, str], ...] = field(
        default_factory=tuple
    )
    excluded_file_sha256s: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True, slots=True)
class CBZGalleryDescriptor:
    """The gallery identity needed after a CBZ has been prepared.

    Prepared artifacts deliberately retain no source-file collection.  The
    complete :class:`ScannedGallery` is needed only while a worker builds or
    verifies the artifact.
    """

    gallery_name: str
    gid: int
    upload_time: datetime

    @classmethod
    def from_scanned_gallery(cls, gallery: ScannedGallery) -> CBZGalleryDescriptor:
        return cls(
            gallery_name=gallery.gallery_name,
            gid=gallery.gid,
            upload_time=gallery.upload_time,
        )


@dataclass(frozen=True, slots=True)
class CBZPreparationRequest:
    """One bounded unit of CBZ preparation work."""

    gallery: ScannedGallery
    excluded_file_sha256s: frozenset[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "excluded_file_sha256s",
            frozenset(self.excluded_file_sha256s),
        )


@dataclass(frozen=True, slots=True)
class CBZPreparationMetadata:
    """Gallery metadata sufficient to prepare or reuse one streamed CBZ.

    Unlike :class:`ScannedGallery`, this record deliberately has no source-file
    collection.  A worker obtains source rows from the request's page-backed
    iterator only while it is building the archive.
    """

    gallery: CBZGalleryDescriptor
    source_digest: str
    content_digest: str | None

    def __post_init__(self) -> None:
        for label, value in (
            ("Source SHA-256", self.source_digest),
            ("Content SHA-256", self.content_digest),
        ):
            if value is None:
                continue
            if len(value) != 64:
                raise ValueError(f"{label} must contain 64 hexadecimal characters")
            try:
                bytes.fromhex(value)
            except ValueError as error:
                raise ValueError(f"{label} is not hexadecimal") from error


@dataclass(frozen=True, slots=True)
class CBZPreparationFile:
    """One streamed archive member decision.

    ``excluded`` travels with the file row so streamed preparation never needs
    to materialize a gallery-sized set of excluded hashes.
    """

    file_key: str
    file: ScannedFile
    excluded: bool = False

    def __post_init__(self) -> None:
        if not self.file_key:
            raise ValueError("CBZ preparation file key must not be blank")
        if self.file.signature is None:
            raise ValueError(
                "Streamed CBZ preparation requires a complete file stat signature"
            )
        if self.file.signature.size_bytes != self.file.size_bytes:
            raise ValueError(
                "CBZ preparation file signature size must match the staged size"
            )


@dataclass(frozen=True, slots=True)
class CBZStreamingPreparationRequest:
    """One worker-local, page-backed CBZ preparation request.

    ``open_files`` is invoked exactly once by a CBZ worker thread when a rebuild
    is required.  Implementations backed by a database must therefore create a
    fresh read transaction for each page call instead of sharing a connection or
    cursor owned by the orchestration thread.
    """

    metadata: CBZPreparationMetadata
    open_files: Callable[[], Iterator[CBZPreparationFile]]

    def __post_init__(self) -> None:
        if not callable(self.open_files):
            raise ValueError("open_files must be callable")


@dataclass(frozen=True, slots=True)
class CBZPreparationSummary:
    prepared: int
    created: int
    rebuilt: int

    @property
    def reused(self) -> int:
        return self.prepared - self.created - self.rebuilt


@dataclass(frozen=True, slots=True)
class CBZArtifact:
    gallery: CBZGalleryDescriptor
    path: Path
    size_bytes: int
    sha256: str
    modified_at: datetime
    created: bool
    rebuilt: bool


@dataclass(frozen=True, slots=True)
class SyncOutcome:
    revision: int
    scanned: int
    published: int
    new: int
    changed: int
    removed: int
    duplicate_losers: int
    cbz_created: int
    cbz_rebuilt: int
    immediate_rescan_required: bool | None = None

    @property
    def has_changes(self) -> bool:
        return bool(self.new or self.changed or self.removed)

    @property
    def needs_immediate_rescan(self) -> bool:
        if self.immediate_rescan_required is not None:
            return self.immediate_rescan_required
        return bool(self.new or self.changed)

    @property
    def maintenance_work(self) -> int:
        return self.changed + self.removed
