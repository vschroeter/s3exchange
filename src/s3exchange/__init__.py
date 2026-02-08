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
from .manifest import Manifest
from .settings import S3Settings
from .shard import ArchiveMemberBody, Shard
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
    "Manifest",
    "Shard",
    "ManifestWriter",
    "ArchiveMemberBody",
    # Settings
    "S3Settings",
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
    # Utils
    "normalize_key",
    "resolve_template",
    "infer_id_from_key",
]
