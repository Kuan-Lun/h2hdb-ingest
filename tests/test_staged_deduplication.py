from datetime import datetime
from hashlib import sha256

import pytest

from h2hdb_ingest.deduplication import (
    DeduplicationCandidate,
    effective_content_digest,
    is_cross_artist_spam,
    select_content_owner,
    select_gid_winner,
)


def _digest(label: str) -> str:
    return sha256(label.encode()).hexdigest()


def _candidate(
    name: str,
    gid: int,
    *,
    digest: str | None,
    title: str = "title",
    downloaded: int = 1,
    already_uploaded: bool = False,
) -> DeduplicationCandidate:
    return DeduplicationCandidate(
        gallery_name=name,
        gid=gid,
        title=title,
        download_time=datetime(2025, 1, downloaded),
        content_digest=digest,
        already_uploaded=already_uploaded,
    )


def test_staged_content_owner_uses_priority_then_incumbent_then_identity() -> None:
    digest = _digest("content")
    low = _candidate(
        "low",
        99,
        digest=digest,
        title="a very long title",
        already_uploaded=True,
    )
    incumbent = _candidate("incumbent", 1, digest=digest)
    challenger = _candidate("challenger", 2, digest=digest)

    assert (
        select_content_owner((low, incumbent, challenger)).gallery_name == "challenger"
    )
    assert (
        select_content_owner(
            (low, incumbent, challenger),
            incumbent_gallery_name="incumbent",
        ).gallery_name
        == "incumbent"
    )


def test_staged_gid_winner_preserves_incumbent_only_at_top_priority() -> None:
    first = _candidate("a", 10, digest=_digest("a"))
    second = _candidate("b", 10, digest=_digest("b"))
    lower = _candidate(
        "old",
        10,
        digest=_digest("old"),
        already_uploaded=True,
    )

    assert select_gid_winner((first, second, lower)).gallery_name == "b"
    assert (
        select_gid_winner(
            (first, second, lower), incumbent_gallery_name="a"
        ).gallery_name
        == "a"
    )
    assert (
        select_gid_winner(
            (first, second, lower), incumbent_gallery_name="old"
        ).gallery_name
        == "b"
    )


def test_staged_spam_rule_and_effective_digest_preserve_exact_legacy_semantics() -> (
    None
):
    assert is_cross_artist_spam(({"a"}, {"b"}, {"c"}))
    assert not is_cross_artist_spam(({"a", "b"}, {"b", "c"}, {"c"}))
    assert not is_cross_artist_spam((set(), set()))

    first = _digest("first")
    second = _digest("second")
    expected = sha256(
        b"".join(
            sorted((bytes.fromhex(first), bytes.fromhex(first), bytes.fromhex(second)))
        )
    ).hexdigest()
    assert effective_content_digest((second, first, first)) == expected
    assert effective_content_digest(()) is None


def test_staged_group_reducers_reject_mixed_or_empty_groups() -> None:
    with pytest.raises(ValueError, match="at least one"):
        select_content_owner(())
    with pytest.raises(ValueError, match="content digest"):
        select_content_owner(
            (
                _candidate("a", 1, digest=_digest("a")),
                _candidate("b", 2, digest=_digest("b")),
            )
        )
    with pytest.raises(ValueError, match="one GID"):
        select_gid_winner(
            (
                _candidate("a", 1, digest=_digest("a")),
                _candidate("b", 2, digest=_digest("b")),
            )
        )
