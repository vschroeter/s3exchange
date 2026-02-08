"""Main S3ExchangeStore and ScopedStore classes."""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Literal

from botocore.client import BaseClient

from s3exchange.settings import S3Settings

if TYPE_CHECKING:
    import boto3

from .exceptions import ObjectNotFoundError, ShardReadError
from .manifest import Manifest
from .shard import ArchiveMemberBody, Shard
from .types import (
    CompactReport,
    DeleteReport,
    FileEntry,
    ManifestEntry,
    ManifestRef,
    ManifestRefEntry,
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
    """Main store class for S3 exchange operations.

    All higher-level concepts (:class:`Manifest`, :class:`Shard`) keep a
    reference back to the store so that S3 credentials, bucket, and prefix
    are never passed around redundantly.
    """

    def __init__(
        self,
        s3_client_or_settings: BaseClient | S3Settings,
        bucket: str | None = None,
        *,
        base_prefix: str = "",
        default_vars: dict[str, Any] | None = None,
        infer_id: Callable[[str], str] | None = None,
    ) -> None:
        """Initialize S3 exchange store.

        Parameters
        ----------
        s3_client_or_settings : BaseClient | S3Settings
            Boto3 S3 client or :class:`S3Settings` (which creates one).
        bucket : str | None
            Target S3 bucket.  Inferred from *S3Settings* when *None*.
        base_prefix : str
            Optional prefix applied to all keys.
        default_vars : dict[str, Any] | None
            Default template variables.
        infer_id : Callable[[str], str] | None
            Custom ID inference function.
        """
        if isinstance(s3_client_or_settings, S3Settings):
            s3_client = s3_client_or_settings.create_client()
            if bucket is None:
                bucket = s3_client_or_settings.bucket
        elif not isinstance(s3_client_or_settings, BaseClient):
            raise ValueError("s3_client must be a boto3.client or S3Settings instance")
        else:
            s3_client = s3_client_or_settings

        self.s3_client: BaseClient = s3_client
        self.bucket = bucket or "default-bucket"
        self.base_prefix = base_prefix
        self.default_vars = default_vars or {}
        self.infer_id = infer_id or (lambda k: infer_id_from_key(k, remove_extension=True))

    # ------------------------------------------------------------------ #
    #  Key resolution                                                     #
    # ------------------------------------------------------------------ #

    def _resolve_key(self, template: str, vars: dict[str, Any] | None = None) -> str:
        """Resolve a key template.

        Parameters
        ----------
        template : str
            Key template string.
        vars : dict[str, Any] | None
            Variables for resolution.

        Returns
        -------
        str
        """
        all_vars = {**self.default_vars, **(vars or {})}
        resolved = resolve_template(template, all_vars, self.default_vars)
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
        """
        return ScopedStore(self, vars)

    # ------------------------------------------------------------------ #
    #  Type detection helpers                                             #
    # ------------------------------------------------------------------ #

    def is_manifest(self, key: str) -> bool:
        """Check whether *key* looks like a manifest file.

        Parameters
        ----------
        key : str
            S3 object key.

        Returns
        -------
        bool
        """
        return key.endswith(".jsonl")

    def is_shard(self, key: str) -> bool:
        """Check whether *key* looks like a shard archive.

        Parameters
        ----------
        key : str
            S3 object key.

        Returns
        -------
        bool
        """
        return key.endswith(".tar") or key.endswith(".tar.gz") or key.endswith(".tgz") or key.endswith(".tar.bz2") or key.endswith(".tar.xz")

    # ------------------------------------------------------------------ #
    #  Object-oriented accessors                                          #
    # ------------------------------------------------------------------ #

    def get_manifest(self, key: str) -> Manifest:
        """Get a :class:`Manifest` object for the given key.

        If *key* points to a shard archive the shard's internal manifest is
        returned instead.

        Parameters
        ----------
        key : str
            S3 key - manifest or shard archive.

        Returns
        -------
        Manifest
        """
        if self.is_shard(key):
            return self.get_shard(key).get_manifest()
        return Manifest.from_key(self, key)

    def get_shard(self, key: str) -> Shard:
        """Get a :class:`Shard` object for the given archive key.

        Parameters
        ----------
        key : str
            S3 key of the shard archive.

        Returns
        -------
        Shard
        """
        return Shard.from_key(self, key)

    # ------------------------------------------------------------------ #
    #  Read APIs                                                          #
    # ------------------------------------------------------------------ #

    def get_object(
        self,
        key: str,
        *,
        version_id: str | None = None,
        resolve_virtual_keys: bool = True,
    ) -> StreamingBodyLike:
        """Get an S3 object.

        Keys containing ``#`` are treated as **virtual keys** referencing a
        file inside a shard archive (``"archive_key#member_path"``).

        Parameters
        ----------
        key : str
            S3 object key (or virtual key).
        version_id : str | None
            Optional S3 version ID.
        resolve_virtual_keys : bool
            Resolve virtual keys automatically.

        Returns
        -------
        StreamingBodyLike

        Raises
        ------
        ObjectNotFoundError
            If the object (or archive member) does not exist.
        """
        if resolve_virtual_keys and "#" in key:
            parts = key.split("#", 1)
            if len(parts) == 2:
                archive_key, member_path = parts
                try:
                    archive_stream = self.get_object(
                        archive_key,
                        resolve_virtual_keys=False,
                    )
                except ObjectNotFoundError:
                    pass  # fall through to regular lookup
                else:
                    fmt, compression = Shard.infer_archive_format(archive_key)
                    try:
                        # Build a one-shot Shard from the stream data
                        archive_data = archive_stream.read()
                        archive_stream.close()
                        shard_entry = ShardEntry(
                            kind="shard",
                            archive_key=archive_key,
                            format=fmt,
                            compression=compression,
                        )
                        shard = Shard(self, shard_entry)
                        shard._cached_archive_data = archive_data  # avoid re-download
                        return shard.get_member(member_path)
                    except ObjectNotFoundError:
                        raise ObjectNotFoundError(key)
                    except Exception as e:
                        raise ObjectNotFoundError(f"{key}: {e}") from e

        # Regular S3 object
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

    def get_object_metadata(self, key: str) -> dict[str, Any]:
        """Get object metadata without downloading the body.

        Parameters
        ----------
        key : str
            S3 object key.

        Returns
        -------
        dict[str, Any]

        Raises
        ------
        ObjectNotFoundError
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
        """
        try:
            self.get_object_metadata(key)
            return True
        except ObjectNotFoundError:
            return False

    def download_object(
        self,
        key: str,
        local_path: Path | str,
    ) -> Path:
        """Download a single object from S3 to a local file path.

        Parameters
        ----------
        key : str
            S3 object key (or virtual key).
        local_path : Path | str
            Local file path where the object will be saved.

        Returns
        -------
        Path
            The local path where the file was saved.

        Raises
        ------
        ObjectNotFoundError
            If the object does not exist.
        """
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        stream = self.get_object(key)
        try:
            with open(local_path, "wb") as f:
                while True:
                    chunk = stream.read(8192)
                    if not chunk:
                        break
                    f.write(chunk)
        finally:
            stream.close()

        return local_path

    def download_objects(
        self,
        keys: Iterable[str],
        local_dir: Path | str,
    ) -> dict[str, Path]:
        """Download multiple objects from a list of keys to a local directory.

        Objects are saved using their key name (or member_path for virtual keys)
        as the filename. If multiple objects would map to the same filename,
        later objects will overwrite earlier ones.

        Parameters
        ----------
        keys : Iterable[str]
            S3 object keys (regular or virtual keys).
        local_dir : Path | str
            Local directory where objects will be saved.

        Returns
        -------
        dict[str, Path]
            Mapping from S3 key to local file path for each downloaded object.

        Examples
        --------
        >>> store = S3ExchangeStore(...)
        >>> keys = ["data/file1.txt", "shards/archive.tar.gz#member1.txt"]
        >>> paths = store.download_objects(keys, "/tmp/downloads")
        >>> # Files saved to: /tmp/downloads/file1.txt, /tmp/downloads/member1.txt
        """
        local_dir = Path(local_dir)
        local_dir.mkdir(parents=True, exist_ok=True)

        downloaded: dict[str, Path] = {}

        for stream, entry in self.iter_objects_from_keys(keys):
            # Determine filename from entry
            if entry.get("member_path"):
                # Virtual key - use member_path as filename
                filename = Path(entry["member_path"]).name
            else:
                # Regular key - use key name as filename
                key = entry["key"]
                filename = Path(key).name

            local_path = local_dir / filename
            try:
                with open(local_path, "wb") as f:
                    while True:
                        chunk = stream.read(8192)
                        if not chunk:
                            break
                        f.write(chunk)
                downloaded[entry["key"]] = local_path
            finally:
                stream.close()

        return downloaded

    # ------------------------------------------------------------------ #
    #  Manifest convenience wrappers (delegate to Manifest)               #
    # ------------------------------------------------------------------ #

    def iter_manifest_entries(
        self,
        manifest: ManifestRef,
        *,
        resolve_refs: bool = True,
        limit: int | None = None,
    ) -> Iterator[ManifestEntry]:
        """Iterate over manifest entries.

        Convenience wrapper around :meth:`Manifest.iter_entries`.

        Parameters
        ----------
        manifest : ManifestRef
            Key or iterable of entries.
        resolve_refs : bool
            Resolve ``manifest_ref`` entries recursively.
        limit : int | None
            Maximum number of entries to yield. If None, yields all entries.

        Yields
        ------
        ManifestEntry
        """
        m = Manifest.from_ref(self, manifest)
        yield from m.iter_entries(resolve_refs=resolve_refs, limit=limit)

    def iter_objects(
        self,
        manifest: ManifestRef,
        *,
        expand_shards: bool = True,
        limit: int | None = None,
    ) -> Iterator[tuple[StreamingBodyLike, ManifestEntry]]:
        """Iterate over objects in a manifest, yielding ``(stream, entry)`` pairs.

        Parameters
        ----------
        manifest : ManifestRef
            Key or iterable of entries.
        expand_shards : bool
            Expand shard entries into individual streams.
        limit : int | None
            Maximum number of objects to yield. If None, yields all objects.

        Yields
        ------
        tuple[StreamingBodyLike, ManifestEntry]
        """
        m = Manifest.from_ref(self, manifest)
        yield from m.iter_objects(expand_shards=expand_shards, limit=limit)

    def get_objects(
        self,
        manifest: ManifestRef,
    ) -> Iterator[tuple[StreamingBodyLike, ManifestEntry]]:
        """Alias for :meth:`iter_objects`."""
        return self.iter_objects(manifest)

    def iter_objects_from_keys(
        self,
        keys: Iterable[str],
        *,
        limit: int | None = None,
    ) -> Iterator[tuple[StreamingBodyLike, FileEntry]]:
        """Iterate objects from a list of keys, optimizing shard access.

        Groups virtual keys (``archive_key#member_path``) by archive and opens
        each shard only once, yielding all requested members before moving to
        the next shard. Regular S3 keys are processed immediately.

        Order of yielded objects is not guaranteed and may differ from input
        order.

        Parameters
        ----------
        keys : Iterable[str]
            S3 object keys (regular or virtual keys).
        limit : int | None
            Maximum number of objects to yield. If None, yields all objects.

        Yields
        ------
        tuple[StreamingBodyLike, FileEntry]
            ``(stream, entry)`` pairs for each object.

        Examples
        --------
        >>> store = S3ExchangeStore(...)
        >>> keys = [
        ...     "data/file1.txt",
        ...     "shards/archive.tar.gz#member1.txt",
        ...     "shards/archive.tar.gz#member2.txt",  # Same archive
        ...     "data/file2.txt",
        ... ]
        >>> for stream, entry in store.iter_objects_from_keys(keys):
        ...     # Archive is opened once, both members yielded together
        ...     print(entry["key"])
        """
        count = 0
        # Separate regular keys from virtual keys
        regular_keys: list[str] = []
        virtual_by_archive: dict[str, list[str]] = defaultdict(list)

        for key in keys:
            if "#" in key:
                parts = key.split("#", 1)
                if len(parts) == 2:
                    archive_key, member_path = parts
                    virtual_by_archive[archive_key].append((key, member_path))
                else:
                    # Malformed virtual key, treat as regular
                    regular_keys.append(key)
            else:
                regular_keys.append(key)

        # Process regular keys immediately
        for key in regular_keys:
            if limit is not None and count >= limit:
                return
            try:
                stream = self.get_object(key, resolve_virtual_keys=False)
                # Try to get metadata for a richer entry
                try:
                    metadata = self.get_object_metadata(key)
                    entry: FileEntry = {
                        "kind": "file",
                        "key": key,
                        "id": self.infer_id(key),
                        "size_bytes": metadata.get("ContentLength"),
                        "etag": metadata.get("ETag", "").strip('"'),
                        "content_type": metadata.get("ContentType"),
                    }
                except Exception:
                    # Fallback if metadata fetch fails
                    entry = {
                        "kind": "file",
                        "key": key,
                        "id": self.infer_id(key),
                    }
                yield stream, entry
                count += 1
            except ObjectNotFoundError:
                # Skip missing keys
                continue

        # Process virtual keys grouped by archive
        for archive_key, member_requests in virtual_by_archive.items():
            if limit is not None and count >= limit:
                return
            try:
                shard = self.get_shard(archive_key)
                # Get the internal manifest to enrich entries
                try:
                    internal_manifest = shard.get_manifest()
                    # Build lookup: member_path -> FileEntry from manifest
                    manifest_map: dict[str, FileEntry] = {}
                    for entry in internal_manifest.iter_entries(resolve_refs=False):
                        if entry.get("kind") == "file":
                            mp = entry.get("member_path", "")
                            if mp:
                                manifest_map[mp] = entry  # type: ignore[assignment]
                except Exception:
                    manifest_map = {}

                # Yield all requested members from this shard
                for virtual_key, member_path in member_requests:
                    if limit is not None and count >= limit:
                        return
                    try:
                        stream = shard.get_member(member_path)
                        # Use manifest entry if available, otherwise create minimal entry
                        if member_path in manifest_map:
                            base_entry = manifest_map[member_path]
                            entry: FileEntry = {
                                **base_entry,
                                "archive_key": archive_key,
                                "member_path": member_path,
                                "key": virtual_key,  # Ensure virtual key is set
                            }
                        else:
                            entry = {
                                "kind": "file",
                                "key": virtual_key,
                                "archive_key": archive_key,
                                "member_path": member_path,
                                "id": self.infer_id(member_path),
                            }
                        yield stream, entry
                        count += 1
                    except ObjectNotFoundError:
                        # Skip missing members
                        continue
            except Exception:
                # Skip archives that can't be opened
                continue

    def list_manifest_files(
        self,
        manifest: ManifestRef,
        *,
        include_shards: bool = True,
    ) -> Iterator[ManifestEntry]:
        """List file-like entries in a manifest.

        Parameters
        ----------
        manifest : ManifestRef
            Key or iterable of entries.
        include_shards : bool
            Expand shard entries into file entries.

        Yields
        ------
        ManifestEntry
        """
        m = Manifest.from_ref(self, manifest)
        yield from m.list_files(include_shards=include_shards)

    def list_by_manifest_prefix(
        self,
        manifest: ManifestRef,
        prefix_filter: str,
    ) -> Iterator[ManifestEntry]:
        """Filter manifest entries by key prefix.

        Parameters
        ----------
        manifest : ManifestRef
            Key or iterable of entries.
        prefix_filter : str
            Prefix to match.

        Yields
        ------
        ManifestEntry
        """
        m = Manifest.from_ref(self, manifest)
        yield from m.filter_by_prefix(prefix_filter)

    # ------------------------------------------------------------------ #
    #  Write APIs                                                         #
    # ------------------------------------------------------------------ #

    def put_object(
        self,
        key: str,
        data: bytes | Any,
        *,
        id: str | None = None,
        meta: dict[str, Any] | None = None,
        content_type: str | None = None,
    ) -> FileEntry:
        """Put a single object to S3 and return a :class:`FileEntry`.

        Parameters
        ----------
        key : str
            S3 object key.
        data : bytes | Any
            Object data (bytes, file-like, or path).
        id : str | None
            Optional entry ID.
        meta : dict[str, Any] | None
            Optional metadata.
        content_type : str | None
            Optional content type.

        Returns
        -------
        FileEntry
        """
        if isinstance(data, (str, Path)):
            self.s3_client.upload_file(
                str(data),
                self.bucket,
                key,
                ExtraArgs={"ContentType": content_type} if content_type else {},
            )
            head = self.get_object_metadata(key)
            size_bytes = head.get("ContentLength")
            etag = head.get("ETag", "").strip('"')
        elif isinstance(data, bytes):
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
            head = self.get_object_metadata(key)
            etag = head.get("ETag", "").strip('"')
        else:
            extra_args = {}
            if content_type:
                extra_args["ContentType"] = content_type
            self.s3_client.upload_fileobj(
                data,
                self.bucket,
                key,
                ExtraArgs=extra_args,
            )
            head = self.get_object_metadata(key)
            size_bytes = head.get("ContentLength")
            etag = head.get("ETag", "").strip('"')

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
            Entries to write.
        mode : ``"overwrite"`` | ``"append_parts"``
            Write mode.
        """
        m = Manifest(self, key=key)
        m.save(entries, mode=mode)

    def put_shard_archive(
        self,
        archive_key: str,
        shard_items: Sequence[ShardItem],
        *,
        format: str = "tar",
        compression: str | None = "gzip",
        internal_manifest_path: str = "__manifest__.jsonl",
    ) -> Shard:
        """Create and upload a shard archive.

        Parameters
        ----------
        archive_key : str
            S3 key for the archive.
        shard_items : Sequence[ShardItem]
            Items to include.
        format : str
            Archive format.
        compression : str | None
            Compression type.
        internal_manifest_path : str
            Internal manifest path.

        Returns
        -------
        Shard
            Uploaded :class:`Shard` object (with populated :attr:`Shard.entry`).
        """
        archive_data_or_path, total_size = Shard.create_archive(
            shard_items,
            archive_key,
            format=format,
            compression=compression,
            internal_manifest_path=internal_manifest_path,
        )

        try:
            if isinstance(archive_data_or_path, Path):
                self.s3_client.upload_file(
                    str(archive_data_or_path),
                    self.bucket,
                    archive_key,
                )
                archive_data_or_path.unlink()
            else:
                self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=archive_key,
                    Body=archive_data_or_path,
                )
        except Exception:
            if isinstance(archive_data_or_path, Path) and archive_data_or_path.exists():
                try:
                    archive_data_or_path.unlink()
                except OSError:
                    pass
            raise

        head = self.get_object_metadata(archive_key)
        etag = head.get("ETag", "").strip('"')

        entry = ShardEntry(
            kind="shard",
            archive_key=archive_key,
            format=format,
            compression=compression,
            internal_manifest_path=internal_manifest_path,
            count=len(shard_items),
            size_bytes=total_size,
            meta={"etag": etag},
        )
        return Shard(self, entry)

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
            Shard entries.
        update_mode : ``"overwrite"`` | ``"append_parts"``
            Update mode.
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
        """Open a :class:`ManifestWriter` for incremental manifest writing.

        Parameters
        ----------
        manifest_key : str
            Root manifest key.
        mode : ``"overwrite"`` | ``"append_parts"``
            Write mode.
        part_max_entries : int
            Max entries per part before flush.
        part_max_bytes : int
            Max bytes per part before flush.
        shard_size : ShardSizePolicy | None
            Shard buffering policy.
        shard_prefix : str | None
            Prefix for shard archives.
        shard_format : str
            Archive format.
        shard_compression : str | None
            Compression type.
        internal_manifest_path : str
            Internal manifest path.
        publish_on_error : bool
            Publish root manifest even on exception.

        Returns
        -------
        ManifestWriter
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

    # ------------------------------------------------------------------ #
    #  Delete APIs                                                        #
    # ------------------------------------------------------------------ #

    def delete_key(self, key: str) -> None:
        """Delete a single object.

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
        regex : str | None
            Optional regex filter.
        batch_size : int
            Delete batch size (max 1000).

        Returns
        -------
        int
            Number of deleted objects.
        """
        if batch_size > 1000:
            batch_size = 1000

        pattern = re.compile(regex) if regex else None
        deleted_count = 0

        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            if "Contents" not in page:
                continue

            keys_to_delete: list[dict[str, str]] = []
            for obj in page["Contents"]:
                key = obj["Key"]
                if pattern is None or pattern.search(key):
                    keys_to_delete.append({"Key": key})

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
        """Delete objects referenced by a manifest.

        Convenience wrapper around :meth:`Manifest.delete`.

        Parameters
        ----------
        manifest : ManifestRef
            Manifest to process.
        delete_manifests : bool
            Also delete manifest files.
        dedupe : bool
            Deduplicate keys.

        Returns
        -------
        DeleteReport
        """
        m = Manifest.from_ref(self, manifest)
        return m.delete(delete_manifests=delete_manifests, dedupe=dedupe)

    # ------------------------------------------------------------------ #
    #  List APIs                                                          #
    # ------------------------------------------------------------------ #

    def list_keys(self, prefix: str) -> Iterator[str]:
        """List S3 keys with given prefix.

        Parameters
        ----------
        prefix : str
            S3 prefix.

        Yields
        ------
        str
        """
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            if "Contents" not in page:
                continue
            for obj in page["Contents"]:
                yield obj["Key"]

    # ------------------------------------------------------------------ #
    #  Compact                                                            #
    # ------------------------------------------------------------------ #

    def compact_manifest(
        self,
        src_manifest_key: str,
        dst_manifest_key: str,
        *,
        resolve_refs: bool = True,
        apply_deletes: bool = True,
        expand_shards: bool = False,
    ) -> CompactReport:
        """Compact a manifest by flattening refs / expanding shards.

        Convenience wrapper around :meth:`Manifest.compact`.

        Parameters
        ----------
        src_manifest_key : str
            Source manifest key.
        dst_manifest_key : str
            Destination manifest key.
        resolve_refs : bool
            Resolve ``manifest_ref`` entries.
        apply_deletes : bool
            Reserved for future use.
        expand_shards : bool
            Expand shard entries into file entries.

        Returns
        -------
        CompactReport
        """
        m = Manifest.from_key(self, src_manifest_key)
        return m.compact(
            dst_manifest_key,
            resolve_refs=resolve_refs,
            expand_shards=expand_shards,
        )
