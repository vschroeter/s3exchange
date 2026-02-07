"""S3 Exchange Library - Service-wise namespace for S3 artifact storage and exchange."""

from __future__ import annotations

from .store import S3ExchangeStore, ScopedStore
from .types import (
    ManifestEntry,
    ManifestRef,
    FileEntry,
    ShardEntry,
    ManifestRefEntry,
    ShardItem,
    ShardSizePolicy,
    ManifestWriterConfig,
    StreamingBodyLike,
    DeleteReport,
    CompactReport,
)
from .writer import ManifestWriter
from .exceptions import (
    S3ExchangeError,
    MissingPlaceholderError,
    InvalidManifestError,
    ShardReadError,
    ObjectNotFoundError,
    ManifestNotFoundError,
)
from .manifest import (
    iter_manifest_entries,
    write_manifest_to_s3,
    generate_part_manifest_key,
)
from .shard import (
    ArchiveMemberBody,
    read_shard_internal_manifest,
    iter_shard_members,
    create_shard_archive,
    create_shards,
)
from .utils import (
    normalize_key,
    resolve_template,
    infer_id_from_key,
)

__all__ = [
    # Main classes
    "S3ExchangeStore",
    "ScopedStore",
    "ManifestWriter",
    # Types
    "ManifestEntry",
    "ManifestRef",
    "FileEntry",
    "ShardEntry",
    "ManifestRefEntry",
    "ShardItem",
    "ShardSizePolicy",
    "ManifestWriterConfig",
    "StreamingBodyLike",
    "DeleteReport",
    "CompactReport",
    # Exceptions
    "S3ExchangeError",
    "MissingPlaceholderError",
    "InvalidManifestError",
    "ShardReadError",
    "ObjectNotFoundError",
    "ManifestNotFoundError",
    # Manifest utilities
    "iter_manifest_entries",
    "write_manifest_to_s3",
    "generate_part_manifest_key",
    # Shard utilities
    "ArchiveMemberBody",
    "read_shard_internal_manifest",
    "iter_shard_members",
    "create_shard_archive",
    "create_shards",
    # Utils
    "normalize_key",
    "resolve_template",
    "infer_id_from_key",
]
