"""Main S3ExchangeStore and ScopedStore classes."""

from __future__ import annotations

import os
import re
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from typing import Any, Callable, Literal

from botocore.response import StreamingBody

from .exceptions import (
    MissingPlaceholderError,
    ObjectNotFoundError,
)
from .manifest import (
    generate_part_manifest_key,
    iter_manifest_entries,
    serialize_manifest_entry,
    write_manifest_to_s3,
)
from .shard import (
    ArchiveMemberBody,
    create_shard_archive,
    create_shards,
    get_shard_member,
    iter_shard_members,
    read_shard_internal_manifest,
)
from .types import (
    CompactReport,
    DeleteReport,
    FileEntry,
    ManifestEntry,
    ManifestRef,
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


class ScopedStore:
    """Convenience wrapper for scoped key resolution."""

    def __init__(
        self,
        store: S3ExchangeStore,
        vars: dict[str, Any],
    ) -> None:
        """Initialize scoped store.

        Parameters
        ----------
        store : S3ExchangeStore
            Parent S3ExchangeStore instance.
        vars : dict[str, Any]
            Variables to pre-fill for templating.
        """
        self._store = store
        self._vars = vars

    def key(self, template: str, **vars: Any) -> str:
        """Resolve a key template with scoped and provided variables.

        Parameters
        ----------
        template : str
            Key template string.
        **vars : Any
            Additional variables (merged with scoped vars).

        Returns
        -------
        str
            Resolved and normalized key.
        """
        all_vars = {**self._vars, **vars}
        return self._store._resolve_key(template, all_vars)

    def manifest_key(self, name: str) -> str:
        """Generate a standard manifest key name.

        Parameters
        ----------
        name : str
            Manifest name (e.g., "samples").

        Returns
        -------
        str
            Resolved manifest key.
        """
        return self.key(f"{{prefix}}/{name}/manifest.jsonl")

    def shard_prefix(self, name: str) -> str:
        """Generate a standard shard prefix.

        Parameters
        ----------
        name : str
            Shard collection name.

        Returns
        -------
        str
            Resolved shard prefix.
        """
        return self.key(f"{{prefix}}/{name}/shards")


class S3ExchangeStore:
    """Main store class for S3 exchange operations."""

    def __init__(
        self,
        s3_client: Any,
        bucket: str,
        *,
        base_prefix: str = "",
        default_vars: dict[str, Any] | None = None,
        infer_id: Callable[[str], str] | None = None,
    ) -> None:
        """Initialize S3 exchange store.

        Parameters
        ----------
        s3_client : Any
            Boto3 S3 client (configured for Garage endpoint/path-style).
        bucket : str
            Target S3 bucket name.
        base_prefix : str, optional
            Optional prefix applied to all keys. Defaults to "".
        default_vars : dict[str, Any] | None, optional
            Placeholders available by default for templating.
        infer_id : Callable[[str], str] | None, optional
            Function to infer ID from key if missing (default: basename without extension).
        """
        self.s3_client = s3_client
        self.bucket = bucket
        self.base_prefix = base_prefix
        self.default_vars = default_vars or {}
        self.infer_id = infer_id or (lambda k: infer_id_from_key(k, remove_extension=True))

    def _resolve_key(self, template: str, vars: dict[str, Any] | None = None) -> str:
        """Internal method to resolve a key template.

        Parameters
        ----------
        template : str
            Key template string.
        vars : dict[str, Any] | None, optional
            Variables for resolution.

        Returns
        -------
        str
            Resolved and normalized key.
        """
        all_vars = {**self.default_vars, **(vars or {})}
        resolved = resolve_template(template, all_vars, self.default_vars)

        # Apply base prefix if set
        if self.base_prefix:
            resolved = normalize_key(f"{self.base_prefix}/{resolved}")

        return resolved

    def scope(self, **vars: Any) -> ScopedStore:
        """Create a scoped store with pre-filled variables.

        Parameters
        ----------
        **vars : Any
            Variables to pre-fill.

        Returns
        -------
        ScopedStore
            ScopedStore instance.
        """
        return ScopedStore(self, vars)

    # Read APIs

    def _infer_archive_format(self, archive_key: str) -> tuple[str, str | None]:
        """Infer archive format and compression from archive key.

        Parameters
        ----------
        archive_key : str
            Archive key.

        Returns
        -------
        tuple[str, str | None]
            Tuple of (format, compression).
        """
        format = "tar"
        compression: str | None = None

        if archive_key.endswith(".tar.gz") or archive_key.endswith(".tgz"):
            compression = "gzip"
        elif archive_key.endswith(".tar.bz2"):
            compression = "bzip2"
        elif archive_key.endswith(".tar.xz"):
            compression = "xz"
        elif archive_key.endswith(".tar"):
            compression = None
        # Default is already set to tar with no compression

        return format, compression

    def get_object(
        self,
        key: str,
        *,
        version_id: str | None = None,
        resolve_virtual_keys: bool = True,
    ) -> StreamingBodyLike:
        """Get an S3 object.

        Parameters
        ----------
        key : str
            S3 object key. If it contains '#', it is treated as a virtual key
            referencing a file inside a shard archive (format: "archive_key#member_path").
        version_id : str | None, optional
            Optional version ID.
        resolve_virtual_keys : bool, optional
            If True (default), automatically resolve virtual keys to extract files
            from shard archives. If False, treat virtual keys as literal S3 keys.

        Returns
        -------
        StreamingBody
            StreamingBody of the object (or ArchiveMemberBody for shard members).

        Raises
        ------
        ObjectNotFoundError
            If object doesn't exist.
        """
        # Check if this is a virtual key (contains '#')
        if resolve_virtual_keys and "#" in key:
            # Split into archive_key and member_path
            parts = key.split("#", 1)
            if len(parts) == 2:
                archive_key, member_path = parts

                # Try to get the archive first (without resolving virtual keys to avoid recursion)
                try:
                    archive_stream = self.get_object(archive_key, resolve_virtual_keys=False)
                except ObjectNotFoundError:
                    # If archive doesn't exist, try the original key as-is
                    # (in case someone actually has a key with '#' in it)
                    # Fall through to regular S3 lookup below
                    pass
                else:
                    # Infer format and compression from archive key
                    format, compression = self._infer_archive_format(archive_key)

                    # Extract the member from the archive
                    try:
                        return get_shard_member(
                            archive_stream,
                            member_path,
                            format=format,
                            compression=compression,
                        )
                    except ObjectNotFoundError:
                        # Member not found in archive
                        raise ObjectNotFoundError(key)
                    except Exception as e:
                        # Re-raise as ObjectNotFoundError for consistency
                        raise ObjectNotFoundError(f"{key}: {e}") from e

        # Regular S3 object or virtual key resolution disabled
        try:
            kwargs: dict[str, Any] = {"Bucket": self.bucket, "Key": key}
            if version_id:
                kwargs["VersionId"] = version_id
            resp = self.s3_client.get_object(**kwargs)
            return resp["Body"]
        except self.s3_client.exceptions.NoSuchKey:
            raise ObjectNotFoundError(key)
        except Exception as e:
            if "NoSuchKey" in str(e) or "404" in str(e):
                raise ObjectNotFoundError(key) from e
            raise

    def head_object(self, key: str) -> dict[str, Any]:
        """Get object metadata without downloading body.

        Parameters
        ----------
        key : str
            S3 object key.

        Returns
        -------
        dict[str, Any]
            Head object response dict.

        Raises
        ------
        ObjectNotFoundError
            If object doesn't exist.
        """
        try:
            return self.s3_client.head_object(Bucket=self.bucket, Key=key)
        except self.s3_client.exceptions.NoSuchKey:
            raise ObjectNotFoundError(key)
        except Exception as e:
            if "NoSuchKey" in str(e) or "404" in str(e):
                raise ObjectNotFoundError(key) from e
            raise

    def exists(self, key: str) -> bool:
        """Check if an object exists.

        Parameters
        ----------
        key : str
            S3 object key.

        Returns
        -------
        bool
            True if object exists, False otherwise.
        """
        try:
            self.head_object(key)
            return True
        except ObjectNotFoundError:
            return False

    def iter_manifest_entries(
        self,
        manifest: ManifestRef,
        *,
        resolve_refs: bool = True,
    ) -> Iterator[ManifestEntry]:
        """Iterate over manifest entries, optionally resolving manifest references.

        Parameters
        ----------
        manifest : ManifestRef
            Either a key (str) or iterable of entries.
        resolve_refs : bool, optional
            If True, recursively resolve manifest_ref entries. Defaults to True.

        Yields
        ------
        ManifestEntry
            Manifest entries.
        """
        yield from iter_manifest_entries(
            manifest,
            self.s3_client,
            self.bucket,
            resolve_refs=resolve_refs,
        )

    def iter_objects(
        self,
        manifest: ManifestRef,
        *,
        expand_shards: bool = True,
    ) -> Iterator[tuple[StreamingBodyLike, ManifestEntry]]:
        """Iterate over objects referenced by manifest, yielding (stream, entry) pairs.

        Parameters
        ----------
        manifest : ManifestRef
            Either a key (str) or iterable of entries.
        expand_shards : bool, optional
            If True, expand shard entries into individual file streams. Defaults to True.

        Yields
        ------
        tuple[StreamingBodyLike, ManifestEntry]
            Tuples of (StreamingBodyLike, ManifestEntry).
        """
        for entry in self.iter_manifest_entries(manifest, resolve_refs=True):
            kind = entry.get("kind")

            if kind == "file":
                # Loose file - get object stream
                key = entry["key"]
                stream = self.get_object(key)
                yield stream, entry

            elif kind == "shard" and expand_shards:
                # Shard entry - expand into members
                archive_key = entry["archive_key"]
                format = entry.get("format", "tar")
                compression = entry.get("compression", "gzip")
                internal_manifest_path = entry.get("internal_manifest_path", "__manifest__.jsonl")

                # Get archive stream and read internal manifest
                # Note: read_shard_internal_manifest reads the entire stream into memory and closes it
                archive_stream = self.get_object(archive_key)
                internal_manifest = read_shard_internal_manifest(
                    archive_stream,
                    internal_manifest_path,
                    format,
                    compression,
                    archive_key=archive_key,
                )

                # Get a new archive stream for member iteration
                # Note: iter_shard_members reads the entire stream into memory and closes it
                archive_stream = self.get_object(archive_key)
                for member_stream, member_entry in iter_shard_members(
                    archive_stream,
                    internal_manifest,
                    internal_manifest_path,
                    format,
                    compression,
                ):
                    # Enrich entry with archive context
                    enriched_entry: FileEntry = {
                        **member_entry,
                        "archive_key": archive_key,
                        "member_path": member_entry.get("member_path", ""),
                    }
                    # Create virtual key
                    if "key" not in enriched_entry:
                        enriched_entry["key"] = f"{archive_key}#{member_entry.get('member_path', '')}"

                    yield member_stream, enriched_entry

            elif kind == "shard" and not expand_shards:
                # Shard entry but not expanding - yield archive stream
                archive_key = entry["archive_key"]
                stream = self.get_object(archive_key)
                yield stream, entry

            # Skip manifest_ref entries (already resolved by iter_manifest_entries)

    def get_objects(
        self,
        manifest: ManifestRef,
    ) -> Iterator[tuple[StreamingBodyLike, ManifestEntry]]:
        """Alias for iter_objects()."""
        return self.iter_objects(manifest)

    # Write APIs

    def put_object(
        self,
        key: str,
        data: bytes | Any,  # bytes, BinaryIO, or PathLike
        *,
        id: str | None = None,
        meta: dict[str, Any] | None = None,
        content_type: str | None = None,
    ) -> FileEntry:
        """Put a single object to S3 and return a manifest entry.

        Parameters
        ----------
        key : str
            S3 object key.
        data : bytes | Any
            Object data (bytes, file-like, or path).
        id : str | None, optional
            Optional ID for manifest entry.
        meta : dict[str, Any] | None, optional
            Optional metadata dict.
        content_type : str | None, optional
            Optional content type.

        Returns
        -------
        FileEntry
            FileEntry manifest entry.
        """
        # Determine upload method
        if isinstance(data, (str, Path)):
            # Upload from file path
            self.s3_client.upload_file(
                str(data),
                self.bucket,
                key,
                ExtraArgs={"ContentType": content_type} if content_type else {},
            )
            # Get size and etag
            head = self.head_object(key)
            size_bytes = head.get("ContentLength")
            etag = head.get("ETag", "").strip('"')
        elif isinstance(data, bytes):
            # Upload bytes
            extra_args: dict[str, Any] = {}
            if content_type:
                extra_args["ContentType"] = content_type
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=data,
                **extra_args,
            )
            size_bytes = len(data)
            # Get etag from head
            head = self.head_object(key)
            etag = head.get("ETag", "").strip('"')
        else:
            # Assume file-like object
            extra_args: dict[str, Any] = {}
            if content_type:
                extra_args["ContentType"] = content_type
            self.s3_client.upload_fileobj(
                data,
                self.bucket,
                key,
                ExtraArgs=extra_args,
            )
            # Get size and etag
            head = self.head_object(key)
            size_bytes = head.get("ContentLength")
            etag = head.get("ETag", "").strip('"')

        # Infer ID if not provided
        if id is None:
            id = self.infer_id(key)

        return FileEntry(
            kind="file",
            key=key,
            id=id,
            meta=meta,
            content_type=content_type,
            size_bytes=size_bytes,
            etag=etag,
        )

    def write_manifest(
        self,
        key: str,
        entries: Iterable[ManifestEntry],
        *,
        mode: Literal["overwrite", "append_parts"] = "overwrite",
    ) -> None:
        """Write manifest entries to S3.

        Parameters
        ----------
        key : str
            Manifest key.
        entries : Iterable[ManifestEntry]
            Iterable of manifest entries.
        mode : Literal["overwrite", "append_parts"], optional
            Write mode - "overwrite" or "append_parts". Defaults to "overwrite".
        """
        if mode == "overwrite":
            # Simple overwrite
            write_manifest_to_s3(self.s3_client, self.bucket, key, entries)

        elif mode == "append_parts":
            # Append via parts
            # Convert entries to list (we need to iterate twice)
            entries_list = list(entries)

            # Write new part
            part_key = generate_part_manifest_key(key)
            write_manifest_to_s3(self.s3_client, self.bucket, part_key, entries_list)

            # Read existing root manifest if it exists
            root_entries: list[ManifestEntry] = []
            if self.exists(key):
                try:
                    root_entries = list(self.iter_manifest_entries(key, resolve_refs=False))
                except Exception:
                    # If reading fails, start fresh
                    root_entries = []

            # Add new part reference
            from .types import ManifestRefEntry

            new_ref: ManifestRefEntry = {
                "kind": "manifest_ref",
                "key": part_key,
            }
            root_entries.append(new_ref)

            # Write updated root manifest
            write_manifest_to_s3(self.s3_client, self.bucket, key, root_entries)

        else:
            raise ValueError(f"Invalid mode: {mode}")

    def put_shard_archive(
        self,
        archive_key: str,
        shard_items: Sequence[ShardItem],
        *,
        format: str = "tar",
        compression: str | None = "gzip",
        internal_manifest_path: str = "__manifest__.jsonl",
    ) -> ShardEntry:
        """Create and upload a shard archive.

        Parameters
        ----------
        archive_key : str
            S3 key for the archive.
        shard_items : Sequence[ShardItem]
            Items to include in the archive.
        format : str, optional
            Archive format. Defaults to "tar".
        compression : str | None, optional
            Compression type ("gzip", None, etc.). Defaults to "gzip".
        internal_manifest_path : str, optional
            Path to internal manifest inside archive. Defaults to "__manifest__.jsonl".

        Returns
        -------
        ShardEntry
            ShardEntry for root manifest.
        """
        # Create archive
        archive_data_or_path, total_size = create_shard_archive(
            shard_items,
            archive_key,
            format=format,
            compression=compression,
            internal_manifest_path=internal_manifest_path,
        )

        # Upload archive
        try:
            if isinstance(archive_data_or_path, Path):
                # Upload from file path
                self.s3_client.upload_file(
                    str(archive_data_or_path),
                    self.bucket,
                    archive_key,
                )
                # Clean up temp file
                archive_data_or_path.unlink()
            else:
                # Upload bytes
                self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=archive_key,
                    Body=archive_data_or_path,
                )
        except Exception:
            # Clean up temp file on error
            if isinstance(archive_data_or_path, Path) and archive_data_or_path.exists():
                try:
                    archive_data_or_path.unlink()
                except OSError:
                    pass
            raise

        # Get etag
        head = self.head_object(archive_key)
        etag = head.get("ETag", "").strip('"')

        return ShardEntry(
            kind="shard",
            archive_key=archive_key,
            format=format,
            compression=compression,
            internal_manifest_path=internal_manifest_path,
            count=len(shard_items),
            size_bytes=total_size,
            meta={"etag": etag},
        )

    def put_sharded(
        self,
        manifest_key: str,
        shard_entries: Iterable[ShardEntry],
        *,
        update_mode: Literal["overwrite", "append_parts"] = "append_parts",
    ) -> None:
        """Write shard entries to a manifest.

        Parameters
        ----------
        manifest_key : str
            Manifest key.
        shard_entries : Iterable[ShardEntry]
            Iterable of shard entries.
        update_mode : Literal["overwrite", "append_parts"], optional
            Update mode - "overwrite" or "append_parts". Defaults to "append_parts".
        """
        self.write_manifest(manifest_key, shard_entries, mode=update_mode)

    def open_manifest_writer(
        self,
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
    ) -> ManifestWriter:
        """Open a ManifestWriter for incremental manifest writing.

        Parameters
        ----------
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

        Returns
        -------
        ManifestWriter
            ManifestWriter instance (use as context manager).

        Notes
        -----
        The writer does not write anything to the root manifest until `close()`
        succeeds. Part manifests and shard archives may be uploaded during the
        run, but they won't be discoverable until the root manifest is updated.
        """
        return ManifestWriter(
            store=self,
            manifest_key=manifest_key,
            mode=mode,
            part_max_entries=part_max_entries,
            part_max_bytes=part_max_bytes,
            shard_size=shard_size,
            shard_prefix=shard_prefix,
            shard_format=shard_format,
            shard_compression=shard_compression,
            internal_manifest_path=internal_manifest_path,
            publish_on_error=publish_on_error,
        )

    # Delete APIs

    def delete_key(self, key: str) -> None:
        """Delete a single object key.

        Parameters
        ----------
        key : str
            S3 object key.
        """
        self.s3_client.delete_object(Bucket=self.bucket, Key=key)

    def delete_prefix(
        self,
        prefix: str,
        *,
        regex: str | None = None,
        batch_size: int = 1000,
    ) -> int:
        """Delete objects by prefix, optionally filtered by regex.

        Parameters
        ----------
        prefix : str
            S3 prefix.
        regex : str | None, optional
            Optional regex pattern to filter keys.
        batch_size : int, optional
            Batch size for delete_objects (max 1000). Defaults to 1000.

        Returns
        -------
        int
            Number of objects deleted.
        """
        if batch_size > 1000:
            batch_size = 1000

        pattern = re.compile(regex) if regex else None

        deleted_count = 0

        # List and delete in batches
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            if "Contents" not in page:
                continue

            keys_to_delete: list[dict[str, str]] = []
            for obj in page["Contents"]:
                key = obj["Key"]
                if pattern is None or pattern.search(key):
                    keys_to_delete.append({"Key": key})

            # Delete in batches
            while keys_to_delete:
                batch = keys_to_delete[:batch_size]
                keys_to_delete = keys_to_delete[batch_size:]

                if batch:
                    self.s3_client.delete_objects(
                        Bucket=self.bucket,
                        Delete={"Objects": batch},
                    )
                    deleted_count += len(batch)

        return deleted_count

    def delete_manifest(
        self,
        manifest: ManifestRef,
        *,
        delete_manifests: bool = True,
        dedupe: bool = True,
    ) -> DeleteReport:
        """Delete objects referenced by manifest (recursive by default).

        Parameters
        ----------
        manifest : ManifestRef
            Manifest to process.
        delete_manifests : bool, optional
            If True, also delete manifest files themselves. Defaults to True.
        dedupe : bool, optional
            If True, deduplicate keys to avoid double-delete. Defaults to True.

        Returns
        -------
        DeleteReport
            DeleteReport with counts and errors.
        """
        deleted_objects: set[str] = set()
        deleted_archives: set[str] = set()
        deleted_manifests: set[str] = set()
        errors: dict[str, str] = {}

        # Track manifest keys to delete
        manifest_keys_to_delete: set[str] = set()
        if isinstance(manifest, str):
            manifest_keys_to_delete.add(manifest)

        # Resolve all entries
        try:
            entries = list(self.iter_manifest_entries(manifest, resolve_refs=True))
        except Exception as e:
            errors[str(manifest)] = str(e)
            entries = []

        # Process entries
        for entry in entries:
            kind = entry.get("kind")

            if kind == "file":
                key = entry.get("key")
                if key and (not dedupe or key not in deleted_objects):
                    try:
                        self.delete_key(key)
                        deleted_objects.add(key)
                    except Exception as e:
                        errors[key] = str(e)

            elif kind == "shard":
                archive_key = entry.get("archive_key")
                if archive_key and (not dedupe or archive_key not in deleted_archives):
                    try:
                        self.delete_key(archive_key)
                        deleted_archives.add(archive_key)
                    except Exception as e:
                        errors[archive_key] = str(e)

            elif kind == "manifest_ref":
                ref_key = entry.get("key")
                if ref_key:
                    manifest_keys_to_delete.add(ref_key)

        # Delete manifest files if requested
        if delete_manifests:
            for manifest_key in manifest_keys_to_delete:
                if not dedupe or manifest_key not in deleted_manifests:
                    try:
                        self.delete_key(manifest_key)
                        deleted_manifests.add(manifest_key)
                    except Exception as e:
                        errors[manifest_key] = str(e)

        return DeleteReport(
            deleted_object_count=len(deleted_objects),
            deleted_archive_count=len(deleted_archives),
            deleted_manifest_count=len(deleted_manifests),
            errors=errors,
        )

    # List APIs

    def list_keys(self, prefix: str) -> Iterator[str]:
        """List S3 keys with given prefix.

        Parameters
        ----------
        prefix : str
            S3 prefix.

        Yields
        ------
        str
            Object keys.
        """
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            if "Contents" not in page:
                continue
            for obj in page["Contents"]:
                yield obj["Key"]

    def list_manifest_files(
        self,
        manifest: ManifestRef,
        *,
        include_shards: bool = True,
    ) -> Iterator[ManifestEntry]:
        """List file-like entries represented by manifest.

        Parameters
        ----------
        manifest : ManifestRef
            Manifest to process.
        include_shards : bool, optional
            If True, expand shard entries into file entries. Defaults to True.

        Yields
        ------
        ManifestEntry
            File-like manifest entries.
        """
        for entry in self.iter_manifest_entries(manifest, resolve_refs=True):
            kind = entry.get("kind")

            if kind == "file":
                yield entry

            elif kind == "shard" and include_shards:
                # Expand shard
                archive_key = entry.get("archive_key", "")
                format = entry.get("format", "tar")
                compression = entry.get("compression", "gzip")
                internal_manifest_path = entry.get("internal_manifest_path", "__manifest__.jsonl")

                try:
                    archive_stream = self.get_object(archive_key)
                    internal_manifest = read_shard_internal_manifest(
                        archive_stream,
                        internal_manifest_path,
                        format,
                        compression,
                        archive_key=archive_key,
                    )
                    archive_stream.close()

                    # Yield enriched entries
                    for member_entry in internal_manifest:
                        enriched: FileEntry = {
                            **member_entry,
                            "archive_key": archive_key,
                            "member_path": member_entry.get("member_path", ""),
                        }
                        if "key" not in enriched:
                            enriched["key"] = f"{archive_key}#{member_entry.get('member_path', '')}"
                        yield enriched
                except Exception:
                    # Skip shard if we can't read it
                    pass

            elif kind == "shard" and not include_shards:
                # Yield shard entry itself
                yield entry

    def list_by_manifest_prefix(
        self,
        manifest: ManifestRef,
        prefix_filter: str,
    ) -> Iterator[ManifestEntry]:
        """Filter manifest entries by prefix (works for shard members too).

        Parameters
        ----------
        manifest : ManifestRef
            Manifest to process.
        prefix_filter : str
            Prefix to filter by.

        Yields
        ------
        ManifestEntry
            Filtered manifest entries.
        """
        for entry in self.list_manifest_files(manifest, include_shards=True):
            # Check if entry matches prefix
            key = entry.get("key", "")
            member_path = entry.get("member_path", "")

            # Use virtual key or member_path for shard members
            check_key = key if key else f"{entry.get('archive_key', '')}#{member_path}"

            if check_key.startswith(prefix_filter):
                yield entry

    def compact_manifest(
        self,
        src_manifest_key: str,
        dst_manifest_key: str,
        *,
        resolve_refs: bool = True,
        apply_deletes: bool = True,
        expand_shards: bool = False,
    ) -> CompactReport:
        """Compact a manifest by flattening refs and optionally expanding shards.

        Parameters
        ----------
        src_manifest_key : str
            Source manifest key.
        dst_manifest_key : str
            Destination manifest key.
        resolve_refs : bool, optional
            If True, resolve all manifest_ref entries. Defaults to True.
        apply_deletes : bool, optional
            If True, filter out delete entries (if implemented). Defaults to True.
        expand_shards : bool, optional
            If True, expand shard entries into file entries. Defaults to False.

        Returns
        -------
        CompactReport
            CompactReport with statistics.
        """
        total_entries = 0
        removed_entries = 0
        resolved_refs = 0
        expanded_shards = 0

        # Collect all entries
        all_entries: list[ManifestEntry] = []

        for entry in self.iter_manifest_entries(src_manifest_key, resolve_refs=resolve_refs):
            total_entries += 1
            kind = entry.get("kind")

            if kind == "manifest_ref":
                resolved_refs += 1
                # Already resolved by iter_manifest_entries
                continue

            if kind == "shard" and expand_shards:
                # Expand shard
                archive_key = entry.get("archive_key", "")
                format = entry.get("format", "tar")
                compression = entry.get("compression", "gzip")
                internal_manifest_path = entry.get("internal_manifest_path", "__manifest__.jsonl")

                try:
                    archive_stream = self.get_object(archive_key)
                    internal_manifest = read_shard_internal_manifest(
                        archive_stream,
                        internal_manifest_path,
                        format,
                        compression,
                        archive_key=archive_key,
                    )
                    archive_stream.close()

                    # Add enriched file entries
                    for member_entry in internal_manifest:
                        enriched: FileEntry = {
                            **member_entry,
                            "archive_key": archive_key,
                            "member_path": member_entry.get("member_path", ""),
                        }
                        if "key" not in enriched:
                            enriched["key"] = f"{archive_key}#{member_entry.get('member_path', '')}"
                        all_entries.append(enriched)
                        expanded_shards += 1
                except Exception:
                    # Keep shard entry if expansion fails
                    all_entries.append(entry)
            else:
                all_entries.append(entry)

        # Write compacted manifest
        self.write_manifest(dst_manifest_key, all_entries, mode="overwrite")

        return CompactReport(
            total_entries=total_entries,
            removed_entries=removed_entries,
            resolved_refs=resolved_refs,
            expanded_shards=expanded_shards,
        )
