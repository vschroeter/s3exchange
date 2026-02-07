"""ManifestWriter for incremental manifest writing with part and shard support."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, BinaryIO, Literal

from .manifest import generate_part_manifest_key, serialize_manifest_entry, write_manifest_to_s3
from .shard import create_shard_archive
from .types import (
    FileEntry,
    ManifestEntry,
    ManifestRefEntry,
    ShardEntry,
    ShardItem,
    ShardSizePolicy,
)


class ManifestWriter:
    """Writer for incrementally building manifests with part and shard support.

    Writes manifest entries incrementally to a local temp buffer, periodically
    flushing as part manifests to S3. On close(), updates the root manifest
    by appending manifest_ref entries for new parts.

    Supports both loose file entries and shard archive entries in the same manifest.
    """

    def __init__(
        self,
        store: Any,  # S3ExchangeStore
        manifest_key: str,
        *,
        mode: Literal["overwrite", "append_parts"] = "append_parts",
        part_max_entries: int = 50_000,
        part_max_bytes: int = 50 * 1024 * 1024,
        shard_size: ShardSizePolicy | None = None,
        shard_prefix: str | None = None,
        shard_format: str = "tar",
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
        mode : Literal["overwrite", "append_parts"], optional
            Write mode. "overwrite" writes single buffer on close.
            "append_parts" flushes parts incrementally. Defaults to "append_parts".
        part_max_entries : int, optional
            Maximum entries per part before flush. Defaults to 50_000.
        part_max_bytes : int, optional
            Maximum bytes per part before flush. Defaults to 50MB.
        shard_size : ShardSizePolicy | None, optional
            If set, enables shard buffering with size limits. Defaults to None.
        shard_prefix : str | None, optional
            Prefix for shard archives. If None, derived from manifest_key.
            Defaults to None.
        shard_format : str, optional
            Archive format. Defaults to "tar".
        shard_compression : str | None, optional
            Compression type, one of ["gzip"]. Defaults to None.
        internal_manifest_path : str, optional
            Path to internal manifest inside archives. Defaults to "__manifest__.jsonl".
        publish_on_error : bool, optional
            If True, publish root manifest even on exception. Defaults to False.
        """
        self.store = store
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
            # Derive from manifest_key: training/.../manifest.jsonl -> training/.../shards/
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
            max_size=10 * 1024 * 1024,  # 10MB before switching to disk
            mode="w+b",
        )

    def __enter__(self) -> ManifestWriter:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: Any) -> None:
        """Context manager exit.

        If no exception and publish_on_error is False, calls close().
        If exception and publish_on_error is True, calls close().
        Otherwise, does not publish root manifest.
        """
        if exc is None:
            # Normal exit - always close
            self.close()
        elif self.publish_on_error:
            # Exception but publish_on_error=True - still close
            try:
                self.close()
            except Exception:
                pass  # Suppress errors during error handling
        # Otherwise: exception and publish_on_error=False - don't publish

        # Always clean up resources
        self._cleanup()

    def _cleanup(self) -> None:
        """Clean up local resources."""
        if self._part_buffer is not None:
            try:
                self._part_buffer.close()
            except Exception:
                pass
            self._part_buffer = None

    def add_entry(self, entry: ManifestEntry) -> None:
        """Add a manifest entry to the current part buffer.

        Parameters
        ----------
        entry : ManifestEntry
            Manifest entry to add.

        Raises
        ------
        RuntimeError
            If writer is closed.
        """
        if self._closed:
            raise RuntimeError("ManifestWriter is closed")

        # Serialize entry
        line_str = serialize_manifest_entry(entry)
        line_bytes = (line_str + "\n").encode("utf-8")

        # Write to buffer
        assert self._part_buffer is not None
        self._part_buffer.write(line_bytes)

        # Update counters
        self._part_entries_count += 1
        self._part_bytes_count += len(line_bytes)

        # Check if we need to flush
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
        """Add a file entry (assumes object already exists in S3).

        Parameters
        ----------
        key : str
            S3 object key.
        id : str | None, optional
            Optional ID. If None, inferred from key.
        meta : dict[str, Any] | None, optional
            Optional metadata.
        content_type : str | None, optional
            Optional content type.
        size_bytes : int | None, optional
            Optional size in bytes.
        etag : str | None, optional
            Optional ETag.

        Returns
        -------
        FileEntry
            Created file entry.
        """
        # Infer ID if not provided
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

    def put_object(
        self,
        key: str,
        data: bytes | Any,  # bytes, BinaryIO, or PathLike
        *,
        id: str | None = None,
        meta: dict[str, Any] | None = None,
        content_type: str | None = None,
    ) -> FileEntry:
        """Put an object to S3 and add file entry to manifest.

        Parameters
        ----------
        key : str
            S3 object key.
        data : bytes | Any
            Object data (bytes, file-like, or path).
        id : str | None, optional
            Optional ID. If None, inferred from key.
        meta : dict[str, Any] | None, optional
            Optional metadata.
        content_type : str | None, optional
            Optional content type.

        Returns
        -------
        FileEntry
            Created file entry.
        """
        # Upload object
        entry = self.store.put_object(key, data, id=id, meta=meta, content_type=content_type)

        # Add to manifest
        self.add_entry(entry)
        return entry

    def flush_part(self) -> str | None:
        """Flush current part buffer to S3 as a part manifest.

        Returns
        -------
        str | None
            Uploaded part key, or None if buffer was empty or mode is overwrite.
        """
        if self._closed:
            raise RuntimeError("ManifestWriter is closed")

        # In overwrite mode, don't flush parts
        if self.mode == "overwrite":
            return None

        # Check if buffer has content
        assert self._part_buffer is not None
        if self._part_entries_count == 0:
            return None

        # Generate part key
        part_key = generate_part_manifest_key(self.manifest_key)

        # Rewind buffer and read entries
        self._part_buffer.seek(0)

        # Read all content and parse entries
        content_bytes = self._part_buffer.read()
        content_str = content_bytes.decode("utf-8")

        entries: list[ManifestEntry] = []
        for line in content_str.splitlines():
            line = line.strip()
            if line:
                entry: ManifestEntry = json.loads(line)
                entries.append(entry)

        # Upload part manifest
        write_manifest_to_s3(
            self.store.s3_client,
            self.store.bucket,
            part_key,
            entries,
        )

        # Record part key
        self._new_part_keys.append(part_key)

        # Reset buffer
        self._part_buffer.seek(0)
        self._part_buffer.truncate(0)
        self._part_entries_count = 0
        self._part_bytes_count = 0

        return part_key

    def add_shard_item(self, item: ShardItem) -> None:
        """Add an item to the shard buffer.

        Parameters
        ----------
        item : ShardItem
            Shard item to add.

        Raises
        ------
        RuntimeError
            If writer is closed or shard_size not configured.
        """
        if self._closed:
            raise RuntimeError("ManifestWriter is closed")

        if self.shard_size is None:
            raise RuntimeError("Shard buffering not enabled (shard_size not set)")

        # Add to buffer
        self._shard_items.append(item)
        self._shard_entry_count += 1

        # Update byte count
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

        # Check if we need to flush
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
        id : str | None, optional
            Optional ID.
        meta : dict[str, Any] | None, optional
            Optional metadata.
        size_bytes : int | None, optional
            Size in bytes. Required for BinaryIO sources.
        content_type : str | None, optional
            Optional content type (stored in meta).

        Raises
        ------
        ValueError
            If source is BinaryIO and size_bytes is not provided.
        """
        # Handle BinaryIO by reading into bytes
        if not isinstance(source, (bytes, str, Path)):
            # BinaryIO or other file-like - read into bytes
            if size_bytes is None:
                raise ValueError("size_bytes required for BinaryIO/file-like sources")
            # Read the data
            source_bytes = source.read()
            source = source_bytes
            # size_bytes was already validated above, keep it
        elif isinstance(source, bytes):
            # Can get from len
            if size_bytes is None:
                size_bytes = len(source)
        # For str/Path, size_bytes will be determined in add_shard_item if needed

        # Build meta dict
        item_meta = meta or {}
        if content_type:
            item_meta["content_type"] = content_type

        item: ShardItem = {
            "source": source,  # type: ignore[assignment]  # Now guaranteed to be bytes | Path | str
            "member_path": member_path,
            "id": id,
            "meta": item_meta if item_meta else None,
            "size_bytes": size_bytes,
        }

        self.add_shard_item(item)

    def flush_shard(self) -> ShardEntry | None:
        """Flush current shard buffer to S3 as a shard archive.

        Returns
        -------
        ShardEntry | None
            Created shard entry, or None if buffer was empty.
        """
        if self._closed:
            raise RuntimeError("ManifestWriter is closed")

        if not self._shard_items:
            return None

        # Generate archive key
        self._shard_sequence += 1
        archive_suffix = ".tar"
        if self.shard_compression == "gzip":
            archive_suffix += ".gz"
        archive_key = f"{self.shard_prefix}/shard-{self._shard_sequence:06d}{archive_suffix}"

        # Create and upload archive
        shard_entry = self.store.put_shard_archive(
            archive_key,
            self._shard_items,
            format=self.shard_format,
            compression=self.shard_compression,
            internal_manifest_path=self.internal_manifest_path,
        )

        # Add shard entry to manifest
        self.add_entry(shard_entry)

        # Clear buffer
        self._shard_items = []
        self._shard_entry_count = 0
        self._shard_byte_count = 0

        return shard_entry

    def close(self) -> None:
        """Close the writer and publish root manifest.

        Flushes any pending shard buffer, flushes current part buffer,
        and updates root manifest with new part references (in append_parts mode).

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

            # Flush current part buffer
            if self.mode == "overwrite":
                # Upload entire buffer as root manifest
                assert self._part_buffer is not None
                if self._part_entries_count > 0:
                    self._part_buffer.seek(0)

                    # Read entries from buffer
                    self._part_buffer.seek(0)
                    content_bytes = self._part_buffer.read()
                    content_str = content_bytes.decode("utf-8")

                    entries: list[ManifestEntry] = []
                    for line in content_str.splitlines():
                        line = line.strip()
                        if line:
                            entry: ManifestEntry = json.loads(line)
                            entries.append(entry)

                    # Upload root manifest
                    write_manifest_to_s3(
                        self.store.s3_client,
                        self.store.bucket,
                        self.manifest_key,
                        entries,
                    )
            else:
                # append_parts mode - flush last part if any
                self.flush_part()

                # Update root manifest with new part references
                if self._new_part_keys:
                    # Read existing root manifest entries (without resolving refs)
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
                            # If reading fails, start fresh
                            root_entries = []

                    # Append new part references
                    for part_key in self._new_part_keys:
                        ref_entry: ManifestRefEntry = {
                            "kind": "manifest_ref",
                            "key": part_key,
                        }
                        root_entries.append(ref_entry)

                    # Write updated root manifest
                    write_manifest_to_s3(
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
        """Check if writer is closed."""
        return self._closed

    def stats(self) -> dict[str, Any]:
        """Get writer statistics.

        Returns
        -------
        dict[str, Any]
            Statistics dict with counts and sizes.
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
