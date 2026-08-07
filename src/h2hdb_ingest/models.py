__all__ = [
    "CBZArtifact",
    "DeduplicationPlan",
    "ScannedFile",
    "ScannedGallery",
    "SyncOutcome",
]

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ScannedFile:
    path: Path
    name: str
    size_bytes: int
    sha256: str


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
class CBZArtifact:
    gallery: ScannedGallery
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

    @property
    def has_changes(self) -> bool:
        return bool(self.new or self.changed or self.removed)

    @property
    def needs_immediate_rescan(self) -> bool:
        return bool(self.new or self.changed)

    @property
    def maintenance_work(self) -> int:
        return self.changed + self.removed
