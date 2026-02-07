"""S3 Exchange Library - Service-wise namespace for S3 artifact storage and exchange."""

from __future__ import annotations

from .exceptions import (
    InvalidManifestError,
    ManifestNotFoundError,
    MissingPlaceholderError,
    ObjectNotFoundError,
    S3ExchangeError,
    ShardReadError,
)
from .manifest import (
    generate_part_manifest_key,
    iter_manifest_entries,
    write_manifest_to_s3,
)
from .settings import S3Settings
from .shard import (
    ArchiveMemberBody,
    create_shard_archive,
    create_shards,
    iter_shard_members,
    read_shard_internal_manifest,
)
from .store import S3ExchangeStore, ScopedStore
from .types import (
    CompactReport,
    DeleteReport,
    FileEntry,
    ManifestEntry,
    ManifestRef,
    ManifestRefEntry,
    ManifestWriterConfig,
    ShardEntry,
    ShardItem,
    ShardSizePolicy,
    StreamingBodyLike,
)
from .utils import (
    infer_id_from_key,
    normalize_key,
    resolve_template,
)
from .writer import ManifestWriter

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
