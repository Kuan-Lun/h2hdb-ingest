__all__ = ["DeduplicationPolicy"]

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import replace
from datetime import datetime
from hashlib import sha256

from .models import DeduplicationPlan, ScannedGallery

ALREADY_UPLOADED_TAG_VALUE = "already uploaded"
ARTIST_TAG_NAME = "artist"
SPAM_FILE_MINIMUM_OCCURRENCES = 3
SPAM_ARTIST_RATIO_THRESHOLD = 2


def _priority(gallery: ScannedGallery) -> tuple[bool, int, datetime]:
    already_uploaded = any(
        value.casefold() == ALREADY_UPLOADED_TAG_VALUE for _name, value in gallery.tags
    )
    return (not already_uploaded, len(gallery.title), gallery.download_time)


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
        artist_sets = [artists for artists in artist_sets if artists]
        if not artist_sets:
            continue
        distinct_artists = set().union(*artist_sets)
        maximum_gallery_artists = max(map(len, artist_sets))
        if (
            len(distinct_artists) / maximum_gallery_artists
            > SPAM_ARTIST_RATIO_THRESHOLD
        ):
            excluded.add(digest)
    return frozenset(excluded)


def _effective_content_digest(
    gallery: ScannedGallery,
    excluded_file_sha256s: frozenset[str],
) -> str | None:
    hashes = sorted(
        bytes.fromhex(source_file.sha256)
        for source_file in gallery.files
        if source_file.name != "galleryinfo.txt"
        and source_file.sha256 not in excluded_file_sha256s
    )
    return sha256(b"".join(hashes)).hexdigest() if hashes else None


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
            highest_priority = max(_priority(gallery) for gallery in same_content)
            top = [
                gallery
                for gallery in same_content
                if _priority(gallery) == highest_priority
            ]
            incumbents = [
                gallery
                for gallery in top
                if gallery.gallery_name == content_incumbents.get(content_digest)
            ]
            owner = max(
                incumbents or top,
                key=lambda gallery: (gallery.gid, gallery.gallery_name),
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
            highest_priority = max(_priority(gallery) for gallery in same_gid)
            top = [
                gallery
                for gallery in same_gid
                if _priority(gallery) == highest_priority
            ]
            incumbents = [
                gallery
                for gallery in top
                if gallery.gallery_name == gid_incumbents.get(gid)
            ]
            winner = max(
                incumbents or top,
                key=lambda gallery: gallery.gallery_name,
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
