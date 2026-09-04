"""Durable identity contract for one managed filesystem library."""

from __future__ import annotations

__all__ = ["LibraryStorageIdentity", "LibraryStorageIdentityProvider"]

from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class LibraryStorageIdentity:
    """Immutable UUIDv4 identity persisted by one library root."""

    storage_instance_uuid: bytes

    def __post_init__(self) -> None:
        value = self.storage_instance_uuid
        if type(value) is not bytes or len(value) != 16:
            raise ValueError("library storage instance UUID must be exactly 16 bytes")
        if value[6] >> 4 != 4 or value[8] & 0xC0 != 0x80:
            raise ValueError("library storage instance UUID must be UUIDv4")


@runtime_checkable
class LibraryStorageIdentityProvider(Protocol):
    """Provide the immutable identity owned by one durable library root."""

    def ensure_storage_identity(self) -> LibraryStorageIdentity:
        """Create or replay the root's durable identity."""
