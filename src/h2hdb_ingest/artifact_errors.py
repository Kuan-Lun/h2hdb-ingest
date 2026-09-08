"""Bounded artifact diagnostics with exact worker-owned source identity."""

from __future__ import annotations

__all__ = [
    "PageFailureContext",
    "attach_image_dimensions",
    "attach_page_failure_context",
    "format_artifact_failure",
    "get_page_failure_context",
]

import json
from dataclasses import dataclass
from unicodedata import category

from h2hdb import ArtifactFailureContext, get_artifact_failure_context

_MAXIMUM_ERROR_CHAIN = 32
_MAXIMUM_FIELD_CHARACTERS = 4096
_PAGE_CONTEXT_ATTRIBUTE = "_h2hdb_ingest_page_failure_context"
_DIMENSIONS_ATTRIBUTE = "_h2hdb_ingest_image_dimensions"


@dataclass(frozen=True, slots=True)
class PageFailureContext:
    """Exact source member selected by the failing worker, never a shared cursor."""

    source_position: int
    source_name: bytes
    expected_size_bytes: int
    width: int | None = None
    height: int | None = None
    source_kind: str | None = None


@dataclass(frozen=True, slots=True)
class _ImageDimensions:
    width: int
    height: int
    source_kind: str | None


def _error_chain(error: BaseException) -> tuple[BaseException, ...]:
    chain: list[BaseException] = []
    current: BaseException | None = error
    seen: set[int] = set()
    for _ in range(_MAXIMUM_ERROR_CHAIN):
        if current is None or id(current) in seen:
            break
        seen.add(id(current))
        chain.append(current)
        current = current.__cause__
    return tuple(chain)


def attach_image_dimensions(
    error: BaseException, *, width: int, height: int, source_kind: str | None = None
) -> None:
    """Preserve decoded header dimensions before the page worker adds its identity."""

    setattr(error, _DIMENSIONS_ATTRIBUTE, _ImageDimensions(width, height, source_kind))


def attach_page_failure_context(
    error: BaseException,
    *,
    source_position: int,
    source_name: bytes,
    expected_size_bytes: int,
    width: int | None = None,
    height: int | None = None,
    source_kind: str | None = None,
) -> None:
    """Attach context to the original error without changing its retry semantics."""

    if get_page_failure_context(error) is not None:
        return
    if width is None and height is None:
        for cause in _error_chain(error):
            dimensions = getattr(cause, _DIMENSIONS_ATTRIBUTE, None)
            if isinstance(dimensions, _ImageDimensions):
                width, height = dimensions.width, dimensions.height
                if source_kind is None:
                    source_kind = dimensions.source_kind
                break
    context = PageFailureContext(
        source_position, source_name, expected_size_bytes, width, height, source_kind
    )
    setattr(error, _PAGE_CONTEXT_ATTRIBUTE, context)
    error.add_note(
        "Artifact page source: "
        f"file={_quoted(source_name.decode('utf-8', errors='backslashreplace'))} "
        f"source_bytes={expected_size_bytes}"
    )


def get_page_failure_context(error: BaseException) -> PageFailureContext | None:
    """Read exact page diagnostics through a bounded explicit cause chain."""

    for cause in _error_chain(error):
        context = getattr(cause, _PAGE_CONTEXT_ATTRIBUTE, None)
        if isinstance(context, PageFailureContext):
            return context
    return None


def _quoted(value: str) -> str:
    if len(value) > _MAXIMUM_FIELD_CHARACTERS:
        value = value[:_MAXIMUM_FIELD_CHARACTERS] + "...[truncated]"
    # Keep human-readable Unicode names, but escape all control, directionality
    # and line-separator characters so source names cannot forge log lines.
    encoded = json.dumps(value, ensure_ascii=False)
    return "".join(
        json.dumps(character, ensure_ascii=True)[1:-1]
        if category(character) in {"Cc", "Cf", "Zl", "Zp", "Cs"}
        else character
        for character in encoded
    )


def format_artifact_failure(
    error: BaseException,
    *,
    event: str = "artifact_failed",
    context: ArtifactFailureContext | None = None,
) -> str | None:
    """Render one complete diagnostic line; never guess a gallery from a filename."""

    source = get_artifact_failure_context(error) if context is None else context
    if source is None:
        return None
    page = get_page_failure_context(error)
    source_name = source.source_name if page is None else page.source_name
    source_size = (
        source.expected_size_bytes if page is None else page.expected_size_bytes
    )
    cause = _error_chain(error)[-1]
    fields = [
        f"event={_quoted(event)}",
        f"gid={source.gid}",
        "gallery_folder="
        + _quoted(
            "/"
            + "/".join(
                (*source.source_root_components, *source.gallery_locator_components)
            )
        ),
    ]
    if source_name is not None:
        fields.append(
            "file=" + _quoted(source_name.decode("utf-8", errors="backslashreplace"))
        )
    if source_size is not None:
        fields.append(f"source_bytes={source_size}")
    if page is not None:
        fields.append(f"source_position={page.source_position}")
        if page.source_kind is not None:
            fields.append(f"source_kind={_quoted(page.source_kind)}")
        if page.width is not None and page.height is not None:
            fields.extend(
                (
                    f"image_width={page.width}",
                    f"image_height={page.height}",
                    f"image_pixels={page.width * page.height}",
                )
            )
    fields.extend(
        (
            f"error_type={_quoted(type(cause).__name__)}",
            f"reason={_quoted(str(cause))}",
        )
    )
    return " ".join(fields)
