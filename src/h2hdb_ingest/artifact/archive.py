"""Canonical CBZ framing, bounded structural inspection, and member byte extents."""

from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass
from typing import BinaryIO
from zipfile import (
    ZIP_DEFLATED,
    ZIP_STORED,
    ZipInfo,
)

from .._limits import MAX_METADATA_BYTES
from ._streams import (
    _read_extent,
    _stream_digest,
)
from .images import (
    _verify_canonical_jpeg,
)
from .model import (
    MAX_ARCHIVE_SIZE_BYTES,
    MAX_ENCODED_PAGE_BYTES,
    MAX_PAGE_COUNT,
    PreparedPageEvidence,
    PreparedPresentationEvidence,
    PresentationImageError,
)

_LOCAL_FILE_HEADER = struct.Struct("<IHHHHHIIIHH")
_LOCAL_FILE_HEADER_SIGNATURE = 0x04034B50
_CENTRAL_DIRECTORY_HEADER = struct.Struct("<I6H3I5H2I")
_CENTRAL_DIRECTORY_HEADER_SIGNATURE = 0x02014B50
_END_OF_CENTRAL_DIRECTORY = struct.Struct("<4sHHHHIIH")
_END_OF_CENTRAL_DIRECTORY_SIGNATURE = b"PK\x05\x06"
_CENTRAL_DIRECTORY_HEADER_BYTES = _CENTRAL_DIRECTORY_HEADER.size
_METADATA_MEMBER_NAME = "galleryinfo.txt"
_CANONICAL_ZIP_DATE_TIME = (1980, 1, 1, 0, 0, 0)
_CANONICAL_CREATE_SYSTEM = 3
_CANONICAL_ZIP_VERSION = 20
_CANONICAL_EXTERNAL_ATTR = 0o100644 << 16


@dataclass(frozen=True, slots=True)
class _CanonicalMember:
    name: bytes
    compression: int
    crc32: int
    compressed_size: int
    uncompressed_size: int
    local_offset: int

    @property
    def data_offset(self) -> int:
        return self.local_offset + _LOCAL_FILE_HEADER.size + len(self.name)


def _canonical_zip_info(
    name: str,
    *,
    compression: int,
    file_size: int,
) -> ZipInfo:
    info = ZipInfo(name, date_time=_CANONICAL_ZIP_DATE_TIME)
    info.compress_type = compression
    info.create_system = _CANONICAL_CREATE_SYSTEM
    info.create_version = _CANONICAL_ZIP_VERSION
    info.extract_version = _CANONICAL_ZIP_VERSION
    info.external_attr = _CANONICAL_EXTERNAL_ATTR
    info.internal_attr = 0
    info.flag_bits = 0
    info.file_size = file_size
    return info


def _deflate_worst_case(size: int) -> int:
    # zlib's public compressBound formula; raw DEFLATE cannot exceed this.
    return size + (size >> 12) + (size >> 14) + (size >> 25) + 13


def _require_projected_archive_size(
    current_size: int,
    existing_names: list[str],
    next_name: str,
    next_encoded_size: int,
) -> None:
    names = (*existing_names, next_name)
    projected = (
        current_size
        + _LOCAL_FILE_HEADER.size
        + len(next_name.encode("ascii", errors="strict"))
        + next_encoded_size
        + sum(
            _CENTRAL_DIRECTORY_HEADER_BYTES + len(name.encode("ascii", errors="strict"))
            for name in names
        )
        + _END_OF_CENTRAL_DIRECTORY.size
    )
    if projected > MAX_ARCHIVE_SIZE_BYTES:
        raise PresentationImageError(
            "presentation archive would require ZIP64 before member write"
        )


def inspect_presentation_archive(
    archive: BinaryIO,
    page_member_names: tuple[str, ...],
) -> PreparedPresentationEvidence:
    """Verify exact stored JPEG extents in one canonical acquisition.

    ``page_member_names`` contains opaque locators previously emitted by this
    adapter's archive renderer. ZIP parsing happens once during ingest preparation;
    OPDS receives byte extents and never parses or decompresses the archive at
    request time.
    """

    if not hasattr(archive, "seek") or not hasattr(archive, "read"):
        raise TypeError("archive must be a seekable binary stream")
    names = tuple(page_member_names)
    if len(names) > MAX_PAGE_COUNT:
        raise PresentationImageError("presentation exceeds 4096 pages")
    if any(
        name != canonical_page_member_name(index) for index, name in enumerate(names)
    ):
        raise PresentationImageError("presentation page names are not canonical")

    archive.seek(0, 2)
    archive_size = archive.tell()
    if archive_size < 1:
        raise PresentationImageError("presentation archive is empty")
    if archive_size > MAX_ARCHIVE_SIZE_BYTES:
        raise PresentationImageError("presentation archive exceeds the v2 size cap")
    archive.seek(0)
    archive_digest = _stream_digest(archive, archive_size)
    archive.seek(0)
    members = _read_canonical_members(
        archive,
        archive_size=archive_size,
        expected_member_names=(_METADATA_MEMBER_NAME, *names),
    )
    _validate_metadata_member(archive, members[0])
    pages = tuple(
        _inspect_page_extent(archive, member, page_index=index)
        for index, member in enumerate(members[1:])
    )

    archive.seek(0)
    return PreparedPresentationEvidence(
        archive_sha256=archive_digest,
        archive_size_bytes=archive_size,
        pages=pages,
    )


def canonical_page_member_name(page_index: int) -> str:
    """Return the only page-member spelling accepted by presentation-v2."""

    if type(page_index) is not int or not 0 <= page_index < MAX_PAGE_COUNT:
        raise ValueError("page_index is outside presentation policy")
    return f"pages/{page_index:04d}.jpg"


def _read_canonical_members(
    archive: BinaryIO,
    *,
    archive_size: int,
    expected_member_names: tuple[str, ...],
) -> tuple[_CanonicalMember, ...]:
    """Validate one bounded, closed-world central/local member authority."""

    eocd_offset = archive_size - _END_OF_CENTRAL_DIRECTORY.size
    if eocd_offset < 0:
        raise PresentationImageError("archive lacks a bounded ZIP central directory")
    archive.seek(eocd_offset)
    header = archive.read(_END_OF_CENTRAL_DIRECTORY.size)
    if len(header) != _END_OF_CENTRAL_DIRECTORY.size:
        raise PresentationImageError("archive central directory is truncated")
    (
        signature,
        disk_number,
        central_disk,
        disk_entries,
        total_entries,
        central_size,
        central_offset,
        comment_size,
    ) = _END_OF_CENTRAL_DIRECTORY.unpack(header)
    if signature != _END_OF_CENTRAL_DIRECTORY_SIGNATURE:
        raise PresentationImageError("archive lacks a bounded ZIP central directory")
    if disk_number != 0 or central_disk != 0 or disk_entries != total_entries:
        raise PresentationImageError("multi-disk presentation ZIP is unsupported")
    if total_entries > MAX_PAGE_COUNT + 1:
        raise PresentationImageError("archive has too many members")
    if total_entries != len(expected_member_names):
        raise PresentationImageError("archive member count is not presentation-closed")
    if comment_size != 0:
        raise PresentationImageError("presentation ZIP comment must be empty")
    expected_central_size = sum(
        _CENTRAL_DIRECTORY_HEADER_BYTES + len(name.encode("ascii", errors="strict"))
        for name in expected_member_names
    )
    if central_size != expected_central_size:
        raise PresentationImageError("archive central directory size is not canonical")
    if central_offset + central_size != eocd_offset:
        raise PresentationImageError("archive central directory is not canonical")
    members = _read_central_directory(
        archive,
        central_offset=central_offset,
        central_size=central_size,
        expected_member_names=expected_member_names,
    )
    archive.seek(0)
    return members


def _read_central_directory(
    archive: BinaryIO,
    *,
    central_offset: int,
    central_size: int,
    expected_member_names: tuple[str, ...],
) -> tuple[_CanonicalMember, ...]:
    """Validate every raw central field without normalization by ``zipfile``."""

    archive.seek(central_offset)
    expected_local_offset = 0
    members: list[_CanonicalMember] = []
    for index, expected_name_text in enumerate(expected_member_names):
        header = archive.read(_CENTRAL_DIRECTORY_HEADER.size)
        if len(header) != _CENTRAL_DIRECTORY_HEADER.size:
            raise PresentationImageError("archive central directory is truncated")
        (
            signature,
            create_version,
            extract_version,
            flag_bits,
            compression,
            modified_time,
            modified_date,
            _crc32_value,
            compressed_size,
            uncompressed_size,
            name_size,
            extra_size,
            comment_size,
            disk_start,
            internal_attr,
            external_attr,
            local_offset,
        ) = _CENTRAL_DIRECTORY_HEADER.unpack(header)
        if signature != _CENTRAL_DIRECTORY_HEADER_SIGNATURE:
            raise PresentationImageError(
                "member central-directory signature is invalid"
            )
        if (
            create_version != ((_CANONICAL_CREATE_SYSTEM << 8) | _CANONICAL_ZIP_VERSION)
            or extract_version != _CANONICAL_ZIP_VERSION
        ):
            raise PresentationImageError(
                "member central-directory attributes disagree: version is not canonical"
            )
        if flag_bits != 0:
            raise PresentationImageError(
                "member central-directory flags are not canonical"
            )
        expected_compression = ZIP_DEFLATED if index == 0 else ZIP_STORED
        if compression != expected_compression:
            if index == 0:
                raise PresentationImageError(
                    "presentation metadata must use ZIP_DEFLATED"
                )
            raise PresentationImageError(
                "presentation page members must use ZIP_STORED"
            )
        if modified_time != 0 or modified_date != 33:
            raise PresentationImageError(
                "member central-directory timestamp is not canonical"
            )
        if index == 0:
            if not 1 <= uncompressed_size <= MAX_METADATA_BYTES:
                raise PresentationImageError(
                    "presentation metadata size is outside policy"
                )
            if not 1 <= compressed_size <= _deflate_worst_case(MAX_METADATA_BYTES):
                raise PresentationImageError(
                    "presentation metadata compressed size is outside policy"
                )
        elif (
            compressed_size != uncompressed_size
            or not 1 <= uncompressed_size <= MAX_ENCODED_PAGE_BYTES
        ):
            raise PresentationImageError(
                "presentation page central-directory sizes are not canonical"
            )
        try:
            expected_name = expected_name_text.encode("ascii", errors="strict")
        except UnicodeEncodeError as error:  # pragma: no cover - caller prevalidates
            raise PresentationImageError(
                "member filename is not canonical ASCII"
            ) from error
        if name_size != len(expected_name):
            raise PresentationImageError(
                "member central-directory filename length disagrees"
            )
        if extra_size != 0 or comment_size != 0:
            raise PresentationImageError(
                "member central-directory extra data is not canonical"
            )
        if disk_start != 0 or internal_attr != 0:
            raise PresentationImageError("member central-directory attributes disagree")
        if external_attr != _CANONICAL_EXTERNAL_ATTR:
            raise PresentationImageError("member central-directory attributes disagree")
        if local_offset != expected_local_offset:
            raise PresentationImageError(
                "member central-directory local offset is not canonical"
            )
        observed_name = archive.read(name_size)
        if observed_name != expected_name:
            raise PresentationImageError(
                "archive member order or coverage is not canonical: "
                "central-directory filename disagrees"
            )
        members.append(
            _CanonicalMember(
                expected_name,
                compression,
                _crc32_value,
                compressed_size,
                uncompressed_size,
                local_offset,
            )
        )
        expected_local_offset = (
            local_offset + _LOCAL_FILE_HEADER.size + name_size + compressed_size
        )
        if expected_local_offset > central_offset:
            raise PresentationImageError(
                "member central-directory local extent is outside the archive"
            )
    if archive.tell() != central_offset + central_size:
        raise PresentationImageError("archive central directory is not canonical")
    if expected_local_offset != central_offset:
        raise PresentationImageError(
            "member local data does not end at the central directory"
        )
    result = tuple(members)
    _validate_local_members(archive, result)
    return result


def _validate_local_members(
    archive: BinaryIO,
    members: tuple[_CanonicalMember, ...],
) -> None:
    """Compare every raw local field with the bounded central authority."""

    for member in members:
        archive.seek(member.local_offset)
        header = archive.read(_LOCAL_FILE_HEADER.size)
        if len(header) != _LOCAL_FILE_HEADER.size:
            raise PresentationImageError("member local header is truncated")
        (
            signature,
            extract_version,
            flag_bits,
            compression,
            modified_time,
            modified_date,
            crc32_value,
            compressed_size,
            uncompressed_size,
            name_size,
            extra_size,
        ) = _LOCAL_FILE_HEADER.unpack(header)
        if signature != _LOCAL_FILE_HEADER_SIGNATURE:
            raise PresentationImageError("member local header signature is invalid")
        if extract_version != _CANONICAL_ZIP_VERSION:
            raise PresentationImageError("member local extraction version disagrees")
        if flag_bits != 0:
            raise PresentationImageError("member ZIP flags are not canonical")
        if compression != member.compression:
            raise PresentationImageError("member local compression disagrees")
        if modified_time != 0 or modified_date != 33:
            raise PresentationImageError("member local timestamp disagrees")
        if crc32_value != member.crc32:
            raise PresentationImageError("member local CRC disagrees")
        if (
            compressed_size != member.compressed_size
            or uncompressed_size != member.uncompressed_size
        ):
            raise PresentationImageError("member local sizes disagree")
        if name_size != len(member.name):
            raise PresentationImageError("member local filename length disagrees")
        if extra_size != 0:
            raise PresentationImageError("member ZIP extra data is not canonical")
        if archive.read(name_size) != member.name:
            raise PresentationImageError("member local filename disagrees")


def _validate_metadata_member(archive: BinaryIO, member: _CanonicalMember) -> None:
    compressed = _read_extent(
        archive, offset=member.data_offset, size=member.compressed_size
    )
    decoder = zlib.decompressobj(-zlib.MAX_WBITS)
    try:
        # One extra byte detects false declared sizes without unbounded inflation.
        content = decoder.decompress(compressed, member.uncompressed_size + 1)
    except zlib.error as error:
        raise PresentationImageError(
            "presentation metadata CRC does not match"
        ) from error
    if len(content) != member.uncompressed_size:
        raise PresentationImageError("presentation metadata size does not match")
    if not decoder.eof or decoder.unconsumed_tail or decoder.unused_data:
        raise PresentationImageError(
            "presentation metadata DEFLATE stream is not complete and exact"
        )
    if zlib.crc32(content) & 0xFFFFFFFF != member.crc32:
        raise PresentationImageError("presentation metadata CRC does not match")


def _inspect_page_extent(
    archive: BinaryIO,
    member: _CanonicalMember,
    *,
    page_index: int,
) -> PreparedPageEvidence:
    content = _read_extent(
        archive, offset=member.data_offset, size=member.uncompressed_size
    )
    if zlib.crc32(content) & 0xFFFFFFFF != member.crc32:
        raise PresentationImageError("presentation page CRC does not match")
    return PreparedPageEvidence(
        page_index=page_index,
        member_name=member.name.decode("ascii"),
        byte_offset=member.data_offset,
        image=_verify_canonical_jpeg(content),
    )
