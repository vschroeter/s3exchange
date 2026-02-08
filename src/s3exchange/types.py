"""Type definitions for s3exchange."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol, TypedDict, Union

from botocore.response import StreamingBody


# Manifest entry types
class FileEntry(TypedDict, total=False):
    """Entry for a single S3 object (loose file)."""

    kind: str  # "file"
    key: str
    id: str | None
    meta: dict[str, Any] | None
    content_type: str | None
    size_bytes: int | None
    etag: str | None


class ShardEntry(TypedDict, total=False):
    """Entry for a shard archive object."""

    kind: str  # "shard"
    archive_key: str
    format: str  # "tar"
    compression: str | None  # "gzip", None, etc.
    internal_manifest_path: str
    count: int | None
    size_bytes: int | None
    meta: dict[str, Any] | None


class ManifestRefEntry(TypedDict):
    """Entry referencing another manifest file."""

    kind: str  # "manifest_ref"
    key: str


ManifestEntry = Union[FileEntry, ShardEntry, ManifestRefEntry]

# Manifest input can be a key (str) or already-parsed entries
ManifestRef = Union[str, Iterable[ManifestEntry]]


# Shard item for building shards
class ShardItem(TypedDict, total=False):
    """Item to be included in a shard archive."""

    source: bytes | Path | str  # data source: bytes, file path, or S3 key
    member_path: str  # path inside the archive
    id: str | None
    meta: dict[str, Any] | None
    size_bytes: int | None  # if known, helps with byte-based sharding


# Shard size policy
@dataclass
class ShardSizePolicy:
    """Policy for shard size limits.

    At least one of max_entries or max_bytes must be set.
    Both can be set to enforce both limits.
    """

    max_entries: int | None = None
    max_bytes: int | None = None

    def __post_init__(self) -> None:
        """Validate that at least one limit is set."""
        if self.max_entries is None and self.max_bytes is None:
            raise ValueError("At least one of max_entries or max_bytes must be set")


# Manifest writer configuration
class ManifestWriterConfig(TypedDict, total=False):
    """Configuration for ManifestWriter."""

    mode: Literal["overwrite", "append_parts"]  # default: "append_parts"
    part_max_entries: int  # default: 50_000
    part_max_bytes: int  # default: 50 * 1024 * 1024
    publish_on_error: bool  # default: False
    dedupe_virtual_keys: bool  # default: False


# StreamingBody-like protocol
class StreamingBodyLike(Protocol):
    """Protocol for file-like objects compatible with StreamingBody."""

    def read(self, amt: int | None = None) -> bytes: ...
    def close(self) -> None: ...
    def iter_lines(self, chunk_size: int | None = None, limit: int | None = None) -> Iterator[bytes]: ...
    def readinto(self, b: bytearray) -> int | None: ...


# Delete report
class DeleteReport(TypedDict, total=False):
    """Report from delete operations."""

    deleted_object_count: int
    deleted_archive_count: int
    deleted_manifest_count: int
    errors: dict[str, str]  # key -> error message


# Compact report
class CompactReport(TypedDict, total=False):
    """Report from manifest compaction."""

    total_entries: int
    removed_entries: int
    resolved_refs: int
    expanded_shards: int
