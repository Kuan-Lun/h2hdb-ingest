__all__ = [
    "DeduplicationCandidate",
    "DeduplicationPolicy",
    "effective_content_digest",
    "is_cross_artist_spam",
    "select_content_owner",
    "select_gid_winner",
]

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from hashlib import sha256

from .models import DeduplicationPlan, ScannedGallery

ALREADY_UPLOADED_TAG_VALUE = "already uploaded"
ARTIST_TAG_NAME = "artist"
SPAM_FILE_MINIMUM_OCCURRENCES = 3
SPAM_ARTIST_RATIO_THRESHOLD = 2


@dataclass(frozen=True, slots=True)
class DeduplicationCandidate:
    """Compact input used by the durable, staged deduplication path.

    The staged planner can stream these records from the database without
    hydrating every source-file row.  Keeping the selection primitives here
    preserves one ingest-owned policy for both the legacy in-memory path and
    the bounded build path.
    """

    gallery_name: str
    gid: int
    title: str
    download_time: datetime
    content_digest: str | None
    already_uploaded: bool


def _candidate(gallery: ScannedGallery) -> DeduplicationCandidate:
    return DeduplicationCandidate(
        gallery_name=gallery.gallery_name,
        gid=gallery.gid,
        title=gallery.title,
        download_time=gallery.download_time,
        content_digest=gallery.content_digest,
        already_uploaded=any(
            value.casefold() == ALREADY_UPLOADED_TAG_VALUE
            for _name, value in gallery.tags
        ),
    )


def _priority(
    candidate: DeduplicationCandidate,
) -> tuple[bool, int, datetime]:
    return (
        not candidate.already_uploaded,
        len(candidate.title),
        candidate.download_time,
    )


def select_content_owner(
    candidates: Iterable[DeduplicationCandidate],
    *,
    incumbent_gallery_name: str | None = None,
) -> DeduplicationCandidate:
    """Choose the final owner for one non-null content-digest group.

    The reducer intentionally retains only the current winner so a pathological
    cross-gallery duplicate group does not become another corpus-sized list.
    """

    iterator = iter(candidates)
    try:
        winner = next(iterator)
    except StopIteration:
        raise ValueError("A content group must contain at least one gallery")
    digest = winner.content_digest
    if digest is None:
        raise ValueError("A content group must share one non-null content digest")
    winner_priority = _priority(winner)
    for candidate in iterator:
        if candidate.content_digest != digest:
            raise ValueError("A content group must share one non-null content digest")
        candidate_priority = _priority(candidate)
        if candidate_priority > winner_priority:
            winner = candidate
            winner_priority = candidate_priority
            continue
        if candidate_priority < winner_priority:
            continue
        candidate_is_incumbent = candidate.gallery_name == incumbent_gallery_name
        winner_is_incumbent = winner.gallery_name == incumbent_gallery_name
        if candidate_is_incumbent or (
            not winner_is_incumbent
            and (candidate.gid, candidate.gallery_name)
            > (winner.gid, winner.gallery_name)
        ):
            winner = candidate
    return winner


def select_gid_winner(
    candidates: Iterable[DeduplicationCandidate],
    *,
    incumbent_gallery_name: str | None = None,
) -> DeduplicationCandidate:
    """Choose the publication winner for one GID group of content owners."""

    iterator = iter(candidates)
    try:
        winner = next(iterator)
    except StopIteration:
        raise ValueError("A GID group must contain at least one gallery")
    gid = winner.gid
    winner_priority = _priority(winner)
    for candidate in iterator:
        if candidate.gid != gid:
            raise ValueError("A GID group must share one GID")
        candidate_priority = _priority(candidate)
        if candidate_priority > winner_priority:
            winner = candidate
            winner_priority = candidate_priority
            continue
        if candidate_priority < winner_priority:
            continue
        candidate_is_incumbent = candidate.gallery_name == incumbent_gallery_name
        winner_is_incumbent = winner.gallery_name == incumbent_gallery_name
        if candidate_is_incumbent or (
            not winner_is_incumbent and candidate.gallery_name > winner.gallery_name
        ):
            winner = candidate
    return winner


def is_cross_artist_spam(artist_sets: Iterable[set[str]]) -> bool:
    """Return the existing spam-policy decision for one repeated file hash."""

    distinct_artists: set[str] = set()
    maximum_gallery_artists = 0
    for artists in artist_sets:
        if not artists:
            continue
        distinct_artists.update(artists)
        maximum_gallery_artists = max(maximum_gallery_artists, len(artists))
    if not maximum_gallery_artists:
        return False
    return len(distinct_artists) / maximum_gallery_artists > SPAM_ARTIST_RATIO_THRESHOLD


def _excluded_spam_hashes(
    galleries: tuple[ScannedGallery, ...],
) -> frozenset[str]:
    occurrences = Counter(
        source_file.sha256
        for gallery in galleries
        for source_file in gallery.files
        if source_file.name != "galleryinfo.txt"
    )
    candidates = {
        digest
        for digest, count in occurrences.items()
        if count >= SPAM_FILE_MINIMUM_OCCURRENCES
    }
    excluded: set[str] = set()
    for digest in candidates:
        artist_sets = [
            {value for name, value in gallery.tags if name == ARTIST_TAG_NAME}
            for gallery in galleries
            if any(source_file.sha256 == digest for source_file in gallery.files)
        ]
        if is_cross_artist_spam(artist_sets):
            excluded.add(digest)
    return frozenset(excluded)


def effective_content_digest(
    hashes: Iterable[str],
) -> str | None:
    """Hash a gallery's sorted raw file digests, preserving duplicates."""

    ordered = sorted(bytes.fromhex(digest) for digest in hashes)
    return sha256(b"".join(ordered)).hexdigest() if ordered else None


def _effective_content_digest(
    gallery: ScannedGallery,
    excluded_file_sha256s: frozenset[str],
) -> str | None:
    return effective_content_digest(
        source_file.sha256
        for source_file in gallery.files
        if source_file.name != "galleryinfo.txt"
        and source_file.sha256 not in excluded_file_sha256s
    )


class DeduplicationPolicy:
    def select(
        self,
        galleries: Iterable[ScannedGallery],
        *,
        incumbent_gallery_name_by_content_sha256: Mapping[str, str] | None = None,
        incumbent_gallery_name_by_gid: Mapping[int, str] | None = None,
    ) -> DeduplicationPlan:
        content_incumbents = incumbent_gallery_name_by_content_sha256 or {}
        gid_incumbents = incumbent_gallery_name_by_gid or {}
        scanned_galleries = tuple(galleries)
        excluded_file_sha256s = _excluded_spam_hashes(scanned_galleries)
        effective_galleries = tuple(
            replace(
                gallery,
                content_digest=_effective_content_digest(
                    gallery,
                    excluded_file_sha256s,
                ),
            )
            for gallery in scanned_galleries
        )
        losers: list[ScannedGallery] = []
        duplicate_of_by_gallery_name: dict[str, str] = {}

        by_content: dict[str, list[ScannedGallery]] = defaultdict(list)
        for gallery in effective_galleries:
            if gallery.content_digest is not None:
                by_content[gallery.content_digest].append(gallery)

        content_winners = [
            gallery for gallery in effective_galleries if gallery.content_digest is None
        ]
        for content_digest, same_content in by_content.items():
            owner_name = select_content_owner(
                (_candidate(gallery) for gallery in same_content),
                incumbent_gallery_name=content_incumbents.get(content_digest),
            ).gallery_name
            owner = next(
                gallery
                for gallery in same_content
                if gallery.gallery_name == owner_name
            )
            content_winners.append(owner)
            content_losers = [
                gallery for gallery in same_content if gallery is not owner
            ]
            losers.extend(content_losers)
            duplicate_of_by_gallery_name.update(
                (loser.gallery_name, owner.gallery_name) for loser in content_losers
            )

        by_gid: dict[int, list[ScannedGallery]] = defaultdict(list)
        for gallery in content_winners:
            by_gid[gallery.gid].append(gallery)
        winners: list[ScannedGallery] = []
        for gid, same_gid in by_gid.items():
            winner_name = select_gid_winner(
                (_candidate(gallery) for gallery in same_gid),
                incumbent_gallery_name=gid_incumbents.get(gid),
            ).gallery_name
            winner = next(
                gallery for gallery in same_gid if gallery.gallery_name == winner_name
            )
            winners.append(winner)
            losers.extend(gallery for gallery in same_gid if gallery is not winner)

        return DeduplicationPlan(
            canonical_galleries=tuple(
                sorted(effective_galleries, key=lambda gallery: gallery.gallery_name)
            ),
            winners=tuple(sorted(winners, key=lambda gallery: gallery.gid)),
            losers=tuple(
                sorted(losers, key=lambda gallery: (gallery.gid, gallery.gallery_name))
            ),
            duplicate_of_by_gallery_name=tuple(
                sorted(duplicate_of_by_gallery_name.items())
            ),
            excluded_file_sha256s=excluded_file_sha256s,
        )
