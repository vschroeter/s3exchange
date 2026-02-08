"""ManifestWriter for incremental manifest writing with part and shard support."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO, Literal

from s3_exchange.manifest import Manifest
from s3_exchange.types import (
    FileEntry,
    ManifestEntry,
    ManifestRefEntry,
    ShardEntry,
    ShardItem,
    ShardSizePolicy,
)

if TYPE_CHECKING:
    from s3_exchange.store import S3ExchangeStore


class ManifestWriter:
    """Writer for incrementally building manifests with part and shard support.

    Writes manifest entries incrementally to a local temp buffer, periodically
    flushing as part manifests to S3. On close(), updates the root manifest
    by appending manifest_ref entries for new parts.

    Supports both loose file entries and shard archive entries in the same manifest.
    """

    def __init__(
        self,
        store: S3ExchangeStore,
        manifest_key: str,
        *,
        mode: Literal["overwrite", "append_parts"] = "append_parts",
        part_max_entries: int = 50_000,
        part_max_bytes: int = 50 * 1024 * 1024,
        shard_size: ShardSizePolicy | None = None,
        shard_prefix: str | None = None,
        shard_format: Literal["tar", "gzip"] | None = "tar",
        shard_compression: str | None = None,
        internal_manifest_path: str = "__manifest__.jsonl",
        publish_on_error: bool = False,
    ) -> None:
        """Initialize ManifestWriter.

        Parameters
        ----------
        store : S3ExchangeStore
            S3ExchangeStore instance.
        manifest_key : str
            Root manifest key.
        mode : ``"overwrite"`` | ``"append_parts"``
            Write mode.
        part_max_entries : int
            Max entries per part before flush.
        part_max_bytes : int
            Max bytes per part before flush.
        shard_size : ShardSizePolicy | None
            Shard buffering policy (``None`` disables shard buffering).
        shard_prefix : str | None
            Prefix for shard archives (derived from *manifest_key* when *None*).
        shard_format : str
            Archive format.
        shard_compression : str | None
            Compression type. Default is no `tar`, so no compression.
        internal_manifest_path : str
            Internal manifest path inside archives. Default is "__manifest__.jsonl".
        publish_on_error : bool
            If *True*, publish root manifest even on exception.
        """
        self.store: S3ExchangeStore = store
        self.manifest_key = manifest_key
        self.mode = mode
        self.part_max_entries = part_max_entries
        self.part_max_bytes = part_max_bytes
        self.shard_size = shard_size
        self.shard_format = shard_format
        self.shard_compression = shard_compression
        self.internal_manifest_path = internal_manifest_path
        self.publish_on_error = publish_on_error

        # Determine shard prefix
        if shard_prefix is None:
            if "/" in manifest_key:
                parent = "/".join(manifest_key.split("/")[:-1])
                self.shard_prefix = f"{parent}/shards"
            else:
                self.shard_prefix = "shards"
        else:
            self.shard_prefix = shard_prefix

        # Part buffer state
        self._part_buffer: BinaryIO | None = None
        self._part_entries_count = 0
        self._part_bytes_count = 0
        self._new_part_keys: list[str] = []

        # Shard buffer state
        self._shard_items: list[ShardItem] = []
        self._shard_entry_count = 0
        self._shard_byte_count = 0
        self._shard_sequence = 0

        # Writer state
        self._closed = False

        # Initialize part buffer
        self._part_buffer = tempfile.SpooledTemporaryFile(
            max_size=10 * 1024 * 1024,
            mode="w+b",
        )

    def __enter__(self) -> ManifestWriter:
        """Context manager entry."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> None:
        """Context manager exit - publishes on success or when *publish_on_error*."""
        if exc is None:
            self.close()
        elif self.publish_on_error:
            try:
                self.close()
            except Exception:
                pass
        self._cleanup()

    def _cleanup(self) -> None:
        """Clean up local resources."""
        if self._part_buffer is not None:
            try:
                self._part_buffer.close()
            except Exception:
                pass
            self._part_buffer = None

    # ------------------------------------------------------------------ #
    #  Entry helpers                                                      #
    # ------------------------------------------------------------------ #

    def add_entry(self, entry: ManifestEntry) -> None:
        """Add a manifest entry to the current part buffer.

        Parameters
        ----------
        entry : ManifestEntry
            Entry to add.

        Raises
        ------
        RuntimeError
            If writer is closed.
        """
        if self._closed:
            raise RuntimeError("ManifestWriter is closed")

        line_str = Manifest.serialize_entry(entry)
        line_bytes = (line_str + "\n").encode("utf-8")

        assert self._part_buffer is not None
        self._part_buffer.write(line_bytes)

        self._part_entries_count += 1
        self._part_bytes_count += len(line_bytes)

        if self.mode == "append_parts":
            if self._part_entries_count >= self.part_max_entries or self._part_bytes_count >= self.part_max_bytes:
                self.flush_part()

    def add_file(
        self,
        key: str,
        *,
        id: str | None = None,
        meta: dict[str, Any] | None = None,
        content_type: str | None = None,
        size_bytes: int | None = None,
        etag: str | None = None,
    ) -> FileEntry:
        """Add a file entry (assumes the object already exists in S3).

        Parameters
        ----------
        key : str
            S3 object key.
        id : str | None
            Optional ID (inferred from *key* when *None*).
        meta : dict[str, Any] | None
            Optional metadata.
        content_type : str | None
            Optional content type.
        size_bytes : int | None
            Optional size.
        etag : str | None
            Optional ETag.

        Returns
        -------
        FileEntry
        """
        if id is None:
            id = self.store.infer_id(key)

        entry: FileEntry = {
            "kind": "file",
            "key": key,
            "id": id,
            "meta": meta,
            "content_type": content_type,
            "size_bytes": size_bytes,
            "etag": etag,
        }

        self.add_entry(entry)
        return entry

    def add_manifest(self, key: str) -> ManifestRefEntry:
        """Add a manifest reference entry.

        Parameters
        ----------
        key : str
            S3 manifest key to reference.

        Returns
        -------
        ManifestRefEntry
        """
        entry: ManifestRefEntry = {
            "kind": "manifest_ref",
            "key": key,
        }

        self.add_entry(entry)
        return entry

    def put_object(
        self,
        key: str,
        data: bytes | Any,
        *,
        id: str | None = None,
        meta: dict[str, Any] | None = None,
        content_type: str | None = None,
    ) -> FileEntry:
        """Upload an object to S3 and add its file entry to the manifest.

        Parameters
        ----------
        key : str
            S3 object key.
        data : bytes | Any
            Object data.
        id : str | None
            Optional ID.
        meta : dict[str, Any] | None
            Optional metadata.
        content_type : str | None
            Optional content type.

        Returns
        -------
        FileEntry
        """
        entry = self.store.put_object(
            key,
            data,
            id=id,
            meta=meta,
            content_type=content_type,
        )
        self.add_entry(entry)
        return entry

    # ------------------------------------------------------------------ #
    #  Part flushing                                                      #
    # ------------------------------------------------------------------ #

    def flush_part(self) -> str | None:
        """Flush current part buffer to S3 as a part manifest.

        Returns
        -------
        str | None
            Uploaded part key, or *None* if empty / overwrite mode.
        """
        if self._closed:
            raise RuntimeError("ManifestWriter is closed")

        if self.mode == "overwrite":
            return None

        assert self._part_buffer is not None
        if self._part_entries_count == 0:
            return None

        part_key = Manifest.generate_part_key(self.manifest_key)

        self._part_buffer.seek(0)
        content_bytes = self._part_buffer.read()
        content_str = content_bytes.decode("utf-8")

        entries: list[ManifestEntry] = []
        for line in content_str.splitlines():
            line = line.strip()
            if line:
                entry: ManifestEntry = json.loads(line)
                entries.append(entry)

        Manifest.write_to_s3(
            self.store.s3_client,
            self.store.bucket,
            part_key,
            entries,
        )

        self._new_part_keys.append(part_key)

        # Reset buffer
        self._part_buffer.seek(0)
        self._part_buffer.truncate(0)
        self._part_entries_count = 0
        self._part_bytes_count = 0

        return part_key

    # ------------------------------------------------------------------ #
    #  Shard buffering                                                    #
    # ------------------------------------------------------------------ #

    def add_shard_item(self, item: ShardItem) -> None:
        """Add an item to the shard buffer.

        Parameters
        ----------
        item : ShardItem
            Shard item to buffer.

        Raises
        ------
        RuntimeError
            If writer is closed or shard buffering is not enabled.
        """
        if self._closed:
            raise RuntimeError("ManifestWriter is closed")
        if self.shard_size is None:
            raise RuntimeError("Shard buffering not enabled (shard_size not set)")

        self._shard_items.append(item)
        self._shard_entry_count += 1

        item_size = item.get("size_bytes")
        if item_size is None:
            source = item.get("source")
            if isinstance(source, (str, Path)):
                try:
                    item_size = os.path.getsize(source)
                except OSError:
                    item_size = None
            elif isinstance(source, bytes):
                item_size = len(source)

        if item_size is not None:
            self._shard_byte_count += item_size

        should_flush = False
        if self.shard_size.max_entries is not None:
            if self._shard_entry_count >= self.shard_size.max_entries:
                should_flush = True
        if self.shard_size.max_bytes is not None:
            if self._shard_byte_count >= self.shard_size.max_bytes:
                should_flush = True

        if should_flush:
            self.flush_shard()

    def add_to_shard(
        self,
        member_path: str,
        source: bytes | Path | BinaryIO,
        *,
        id: str | None = None,
        meta: dict[str, Any] | None = None,
        size_bytes: int | None = None,
        content_type: str | None = None,
    ) -> None:
        """Add an item to the shard buffer (convenience method).

        Parameters
        ----------
        member_path : str
            Path inside the archive.
        source : bytes | Path | BinaryIO
            Data source.
        id : str | None
            Optional ID.
        meta : dict[str, Any] | None
            Optional metadata.
        size_bytes : int | None
            Size in bytes (required for BinaryIO sources).
        content_type : str | None
            Optional content type (stored in meta).

        Raises
        ------
        ValueError
            If source is BinaryIO and *size_bytes* is not provided.
        """
        if not isinstance(source, (bytes, str, Path)):
            if size_bytes is None:
                raise ValueError("size_bytes required for BinaryIO/file-like sources")
            source = source.read()  # type: ignore[assignment]
        elif isinstance(source, bytes):
            if size_bytes is None:
                size_bytes = len(source)

        item_meta = meta or {}
        if content_type:
            item_meta["content_type"] = content_type

        item: ShardItem = {
            "source": source,  # type: ignore[assignment]
            "member_path": member_path,
            "id": id,
            "meta": item_meta if item_meta else None,
            "size_bytes": size_bytes,
        }

        self.add_shard_item(item)

    def flush_shard(self) -> ShardEntry | None:
        """Flush the shard buffer to S3.

        Returns
        -------
        ShardEntry | None
            The created shard entry, or *None* if buffer was empty.
        """
        if self._closed:
            raise RuntimeError("ManifestWriter is closed")
        if not self._shard_items:
            return None

        self._shard_sequence += 1
        archive_suffix = ".tar"
        if self.shard_compression == "gzip":
            archive_suffix += ".gz"
        archive_key = f"{self.shard_prefix}/shard-{self._shard_sequence:06d}{archive_suffix}"

        shard = self.store.put_shard_archive(
            archive_key,
            self._shard_items,
            format=self.shard_format,
            compression=self.shard_compression,
            internal_manifest_path=self.internal_manifest_path,
        )

        # Add the shard entry to the manifest buffer
        self.add_entry(shard.entry)

        self._shard_items = []
        self._shard_entry_count = 0
        self._shard_byte_count = 0

        return shard.entry

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                          #
    # ------------------------------------------------------------------ #

    def close(self) -> None:
        """Close the writer and publish the root manifest.

        Flushes any pending shard buffer and part buffer, then updates the
        root manifest with new part references (in ``append_parts`` mode).

        Raises
        ------
        RuntimeError
            If writer is already closed.
        """
        if self._closed:
            raise RuntimeError("ManifestWriter is already closed")

        try:
            # Flush pending shard buffer
            if self.shard_size is not None:
                self.flush_shard()

            if self.mode == "overwrite":
                assert self._part_buffer is not None
                if self._part_entries_count > 0:
                    self._part_buffer.seek(0)
                    content_bytes = self._part_buffer.read()
                    content_str = content_bytes.decode("utf-8")

                    entries: list[ManifestEntry] = []
                    for line in content_str.splitlines():
                        line = line.strip()
                        if line:
                            entry: ManifestEntry = json.loads(line)
                            entries.append(entry)

                    Manifest.write_to_s3(
                        self.store.s3_client,
                        self.store.bucket,
                        self.manifest_key,
                        entries,
                    )
            else:
                # append_parts - flush last part
                self.flush_part()

                if self._new_part_keys:
                    root_entries: list[ManifestEntry] = []
                    if self.store.exists(self.manifest_key):
                        try:
                            root_entries = list(
                                self.store.iter_manifest_entries(
                                    self.manifest_key,
                                    resolve_refs=False,
                                )
                            )
                        except Exception:
                            root_entries = []

                    for part_key in self._new_part_keys:
                        ref_entry: ManifestRefEntry = {
                            "kind": "manifest_ref",
                            "key": part_key,
                        }
                        root_entries.append(ref_entry)

                    Manifest.write_to_s3(
                        self.store.s3_client,
                        self.store.bucket,
                        self.manifest_key,
                        root_entries,
                    )
        finally:
            self._closed = True
            self._cleanup()

    @property
    def is_closed(self) -> bool:
        """Whether the writer has been closed."""
        return self._closed

    def stats(self) -> dict[str, Any]:
        """Return writer statistics.

        Returns
        -------
        dict[str, Any]
        """
        return {
            "part_entries": self._part_entries_count,
            "part_bytes": self._part_bytes_count,
            "parts_uploaded": len(self._new_part_keys),
            "shard_entries": self._shard_entry_count,
            "shard_bytes": self._shard_byte_count,
            "shards_created": self._shard_sequence,
            "is_closed": self._closed,
        }
