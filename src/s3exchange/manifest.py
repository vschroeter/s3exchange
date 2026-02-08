"""Manifest class - parsing, writing, iteration, and lifecycle management.

Every manifest-related operation lives here.  Raw helper functions have been
folded into :class:`Manifest` as **static methods** (parsing / serialisation)
or **instance methods** (store-backed operations).
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterable, Iterator
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal

from botocore.response import StreamingBody

from .exceptions import InvalidManifestError, ManifestNotFoundError
from .types import (
    CompactReport,
    DeleteReport,
    FileEntry,
    ManifestEntry,
    ManifestRef,
    ManifestRefEntry,
    ShardEntry,
    StreamingBodyLike,
)

if TYPE_CHECKING:
    from .store import S3ExchangeStore


class Manifest:
    """A manifest - either backed by an S3 key or by in-memory entries.

    Create instances via the factory helpers or through the store::

        manifest = Manifest.from_key(store, "training/manifest.jsonl")
        manifest = Manifest.from_entries(store, entries)
        manifest = store.get_manifest("training/manifest.jsonl")
    """

    # ------------------------------------------------------------------ #
    #  Construction                                                       #
    # ------------------------------------------------------------------ #

    def __init__(
        self,
        store: S3ExchangeStore,
        *,
        key: str | None = None,
        entries: list[ManifestEntry] | None = None,
    ) -> None:
        """Initialise a Manifest.

        Parameters
        ----------
        store : S3ExchangeStore
            Parent store instance (used for S3 access).
        key : str | None
            S3 key of the manifest file.  Mutually exclusive with *entries*
            only in the sense that at least one must be provided.
        entries : list[ManifestEntry] | None
            Pre-loaded entries (e.g. from a shard's internal manifest).
        """
        if key is None and entries is None:
            raise ValueError("Either 'key' or 'entries' must be provided")
        self._store = store
        self._key = key
        self._entries = entries

    @classmethod
    def from_key(cls, store: S3ExchangeStore, key: str) -> Manifest:
        """Create a Manifest backed by an S3 manifest file."""
        return cls(store, key=key)

    @classmethod
    def from_entries(
        cls,
        store: S3ExchangeStore,
        entries: list[ManifestEntry],
    ) -> Manifest:
        """Create a Manifest from pre-loaded entries."""
        return cls(store, entries=entries)

    @classmethod
    def from_ref(cls, store: S3ExchangeStore, ref: ManifestRef) -> Manifest:
        """Create a Manifest from a :data:`ManifestRef` (key *or* iterable)."""
        if isinstance(ref, str):
            return cls.from_key(store, ref)
        return cls.from_entries(store, list(ref))

    # ------------------------------------------------------------------ #
    #  Properties                                                         #
    # ------------------------------------------------------------------ #

    @property
    def key(self) -> str | None:
        """S3 key of the manifest, or ``None`` for in-memory manifests."""
        return self._key

    @property
    def store(self) -> S3ExchangeStore:
        """Parent :class:`S3ExchangeStore`."""
        return self._store

    # ------------------------------------------------------------------ #
    #  Iteration                                                          #
    # ------------------------------------------------------------------ #

    def _as_ref(self) -> ManifestRef:
        """Return a :data:`ManifestRef` representation of this manifest."""
        if self._key is not None:
            return self._key
        if self._entries is not None:
            return self._entries
        raise ValueError("Manifest has neither key nor entries")  # pragma: no cover

    def iter_entries(
        self,
        *,
        resolve_refs: bool = True,
        limit: int | None = None,
    ) -> Iterator[ManifestEntry]:
        """Iterate over manifest entries, optionally resolving ``manifest_ref``.
        This method does not resolve shard entries. Use :meth:`iter_objects` to iterate over shard entries.

        Parameters
        ----------
        resolve_refs : bool
            If *True* (default), recursively inline referenced manifests.
        limit : int | None
            Maximum number of entries to yield. If None, yields all entries.

        Yields
        ------
        ManifestEntry
        """
        visited: set[str] = set()
        yield from self._iter_recursive(resolve_refs=resolve_refs, visited=visited, limit=limit)

    def _iter_recursive(
        self,
        *,
        resolve_refs: bool,
        visited: set[str],
        limit: int | None = None,
    ) -> Iterator[ManifestEntry]:
        if limit is not None and limit <= 0:
            return
        if self._key is not None:
            if self._key in visited:
                return
            visited.add(self._key)
            raw: Iterator[ManifestEntry] = Manifest.read_from_s3(
                self._store.s3_client,
                self._store.bucket,
                self._key,
            )
        elif self._entries is not None:
            raw = iter(self._entries)
        else:
            return

        count = 0
        for entry in raw:
            if limit is not None and count >= limit:
                break
            if resolve_refs and entry.get("kind") == "manifest_ref":
                ref_key = entry["key"]
                child = Manifest(self._store, key=ref_key)
                remaining_limit = None if limit is None else limit - count
                for child_entry in child._iter_recursive(
                    resolve_refs=True,
                    visited=visited,
                    limit=remaining_limit,
                ):
                    if limit is not None and count >= limit:
                        break
                    yield child_entry
                    count += 1
            else:
                yield entry
                count += 1

    def iter_objects(
        self,
        *,
        expand_shards: bool = True,
        limit: int | None = None,
    ) -> Iterator[tuple[StreamingBodyLike, ManifestEntry]]:
        """Iterate objects, yielding ``(stream, entry)`` pairs.

        Parameters
        ----------
        expand_shards : bool
            If *True* (default), shard entries are expanded into individual
            member streams.  When *False* the raw archive stream is yielded.
        limit : int | None
            Maximum number of objects to yield. If None, yields all objects.

        Yields
        ------
        tuple[StreamingBodyLike, ManifestEntry]
        """
        from .shard import Shard  # lazy - avoids circular import

        count = 0
        for entry in self.iter_entries(resolve_refs=True):
            if limit is not None and count >= limit:
                break
            kind = entry.get("kind")

            if kind == "file":
                if limit is not None and count >= limit:
                    break
                stream = self._store.get_object(entry["key"])
                yield stream, entry
                count += 1

            elif kind == "shard" and expand_shards:
                shard = Shard.from_entry(self._store, entry)
                remaining_limit = None if limit is None else limit - count
                for stream, shard_entry in shard.iter_entries(limit=remaining_limit):
                    if limit is not None and count >= limit:
                        break
                    yield stream, shard_entry
                    count += 1

            elif kind == "shard" and not expand_shards:
                if limit is not None and count >= limit:
                    break
                archive_key = entry["archive_key"]
                stream = self._store.get_object(archive_key)
                yield stream, entry
                count += 1
            # manifest_ref entries already resolved by iter_entries

    def get_keys(
        self,
        *,
        expand_shards: bool = True,
        resolve_refs: bool = True,
        limit: int | None = None,
    ) -> Iterator[str]:
        """Get all keys from entries in the manifest.

        Parameters
        ----------
        expand_shards : bool
            If *True* (default), shard entries are expanded into individual
            member keys.  When *False* the archive key is returned.
        resolve_refs : bool
            If *True* (default), automatically detect if a file is a manifest
            and resolve its contents. When *False*, manifest_ref entries will
            yield the manifest key itself rather than resolving it.
        limit : int | None
            Maximum number of keys to yield. If None, yields all keys.
        Yields
        ------
        str
            S3 keys (or virtual keys for shard members when expanded).
        """
        from .shard import Shard  # lazy - avoids circular import

        count = 0

        for entry in self.iter_entries(resolve_refs=resolve_refs):
            kind = entry.get("kind")
            if limit is not None and count >= limit:
                break

            if kind == "file":
                count += 1
                yield entry["key"]

            elif kind == "shard" and expand_shards:
                shard = Shard.from_entry(self._store, entry)
                try:
                    internal = shard.get_manifest()
                    archive_key = shard.archive_key
                    for member_entry in internal.iter_entries(resolve_refs=False):
                        # Extract key from member entry, or construct virtual key
                        if "key" in member_entry:
                            count += 1
                            yield member_entry["key"]
                        else:
                            member_path = member_entry.get("member_path", "")
                            yield f"{archive_key}#{member_path}"
                except Exception:
                    pass  # skip unreadable shards

            elif kind == "shard" and not expand_shards:
                count += 1
                yield entry["archive_key"]

            elif kind == "manifest_ref" and not resolve_refs:
                # If not resolving refs, yield the manifest key itself
                count += 1
                yield entry["key"]

    def list_files(
        self,
        *,
        include_shards: bool = True,
    ) -> Iterator[ManifestEntry]:
        """List file-like entries (metadata only - no download).

        Parameters
        ----------
        include_shards : bool
            If *True*, expand shard entries into virtual file entries.

        Yields
        ------
        ManifestEntry
        """
        from .shard import Shard

        for entry in self.iter_entries(resolve_refs=True):
            kind = entry.get("kind")

            if kind == "file":
                yield entry

            elif kind == "shard" and include_shards:
                shard = Shard.from_entry(self._store, entry)
                try:
                    internal = shard.get_manifest()
                    for member_entry in internal.iter_entries(resolve_refs=False):
                        archive_key = shard.archive_key
                        enriched: FileEntry = {
                            **member_entry,
                            "archive_key": archive_key,
                            "member_path": member_entry.get("member_path", ""),
                        }
                        if "key" not in enriched:
                            enriched["key"] = f"{archive_key}#{member_entry.get('member_path', '')}"
                        yield enriched
                except Exception:
                    pass  # skip unreadable shards

            elif kind == "shard" and not include_shards:
                yield entry

    def filter_by_prefix(self, prefix: str) -> Iterator[ManifestEntry]:
        """Filter entries whose key starts with *prefix*.

        Works transparently for shard virtual keys.

        Parameters
        ----------
        prefix : str
            Key prefix to match.

        Yields
        ------
        ManifestEntry
        """
        for entry in self.list_files(include_shards=True):
            key = entry.get("key", "")
            member_path = entry.get("member_path", "")
            check_key = key or f"{entry.get('archive_key', '')}#{member_path}"
            if check_key.startswith(prefix):
                yield entry

    # ------------------------------------------------------------------ #
    #  Mutating operations                                                #
    # ------------------------------------------------------------------ #

    def save(
        self,
        entries: Iterable[ManifestEntry] | None = None,
        *,
        mode: Literal["overwrite", "append_parts"] = "overwrite",
    ) -> None:
        """Write (or update) this manifest in S3.

        Parameters
        ----------
        entries : Iterable[ManifestEntry] | None
            Entries to write.  Falls back to the in-memory entries when *None*.
        mode : ``"overwrite"`` | ``"append_parts"``
            Write strategy.
        """
        if self._key is None:
            raise ValueError("Cannot save a manifest without a key")

        target = entries if entries is not None else (self._entries or [])

        if mode == "overwrite":
            Manifest.write_to_s3(
                self._store.s3_client,
                self._store.bucket,
                self._key,
                target,
            )

        elif mode == "append_parts":
            entries_list = list(target)
            part_key = Manifest.generate_part_key(self._key)
            Manifest.write_to_s3(
                self._store.s3_client,
                self._store.bucket,
                part_key,
                entries_list,
            )

            # Read existing root manifest entries (un-resolved)
            root_entries: list[ManifestEntry] = []
            if self._store.exists(self._key):
                try:
                    root_entries = list(self.iter_entries(resolve_refs=False))
                except Exception:
                    root_entries = []

            new_ref: ManifestRefEntry = {"kind": "manifest_ref", "key": part_key}
            root_entries.append(new_ref)

            Manifest.write_to_s3(
                self._store.s3_client,
                self._store.bucket,
                self._key,
                root_entries,
            )
        else:
            raise ValueError(f"Invalid mode: {mode}")

    def delete(
        self,
        *,
        delete_manifests: bool = True,
        dedupe: bool = True,
    ) -> DeleteReport:
        """Delete every object referenced by this manifest.

        Parameters
        ----------
        delete_manifests : bool
            Also delete the manifest files themselves.
        dedupe : bool
            Deduplicate keys to avoid double-deletes.

        Returns
        -------
        DeleteReport
        """
        deleted_objects: set[str] = set()
        deleted_archives: set[str] = set()
        deleted_manifest_keys: set[str] = set()
        errors: dict[str, str] = {}

        manifest_keys_to_delete: set[str] = set()
        if self._key is not None:
            manifest_keys_to_delete.add(self._key)

        try:
            entries = list(self.iter_entries(resolve_refs=True))
        except Exception as e:
            errors[str(self._as_ref())] = str(e)
            entries = []

        for entry in entries:
            kind = entry.get("kind")

            if kind == "file":
                key = entry.get("key")
                if key and (not dedupe or key not in deleted_objects):
                    try:
                        self._store.delete_key(key)
                        deleted_objects.add(key)
                    except Exception as exc:
                        errors[key] = str(exc)

            elif kind == "shard":
                archive_key = entry.get("archive_key")
                if archive_key and (not dedupe or archive_key not in deleted_archives):
                    try:
                        self._store.delete_key(archive_key)
                        deleted_archives.add(archive_key)
                    except Exception as exc:
                        errors[archive_key] = str(exc)

            elif kind == "manifest_ref":
                ref_key = entry.get("key")
                if ref_key:
                    manifest_keys_to_delete.add(ref_key)

        if delete_manifests:
            for mk in manifest_keys_to_delete:
                if not dedupe or mk not in deleted_manifest_keys:
                    try:
                        self._store.delete_key(mk)
                        deleted_manifest_keys.add(mk)
                    except Exception as exc:
                        errors[mk] = str(exc)

        return DeleteReport(
            deleted_object_count=len(deleted_objects),
            deleted_archive_count=len(deleted_archives),
            deleted_manifest_count=len(deleted_manifest_keys),
            errors=errors,
        )

    def compact(
        self,
        dst_key: str,
        *,
        resolve_refs: bool = True,
        expand_shards: bool = False,
    ) -> CompactReport:
        """Compact this manifest into a new flat manifest at *dst_key*.

        Parameters
        ----------
        dst_key : str
            Destination manifest key.
        resolve_refs : bool
            Resolve ``manifest_ref`` entries.
        expand_shards : bool
            Expand shard entries into file entries.

        Returns
        -------
        CompactReport
        """
        from .shard import Shard

        total_entries = 0
        removed_entries = 0
        resolved_refs_count = 0
        expanded_shards_count = 0

        all_entries: list[ManifestEntry] = []

        for entry in self.iter_entries(resolve_refs=resolve_refs):
            total_entries += 1
            kind = entry.get("kind")

            if kind == "manifest_ref":
                resolved_refs_count += 1
                continue

            if kind == "shard" and expand_shards:
                shard = Shard.from_entry(self._store, entry)
                try:
                    internal = shard.get_manifest()
                    for member_entry in internal.iter_entries(resolve_refs=False):
                        archive_key = shard.archive_key
                        enriched: FileEntry = {
                            **member_entry,
                            "archive_key": archive_key,
                            "member_path": member_entry.get("member_path", ""),
                        }
                        if "key" not in enriched:
                            enriched["key"] = f"{archive_key}#{member_entry.get('member_path', '')}"
                        all_entries.append(enriched)
                        expanded_shards_count += 1
                except Exception:
                    all_entries.append(entry)
            else:
                all_entries.append(entry)

        dst = Manifest(self._store, key=dst_key)
        dst.save(all_entries, mode="overwrite")

        return CompactReport(
            total_entries=total_entries,
            removed_entries=removed_entries,
            resolved_refs=resolved_refs_count,
            expanded_shards=expanded_shards_count,
        )

    # ------------------------------------------------------------------ #
    #  Static / class-level utilities                                     #
    # ------------------------------------------------------------------ #

    @staticmethod
    def parse_line(line: str, line_number: int | None = None) -> ManifestEntry:
        """Parse a single JSONL line into a :data:`ManifestEntry`.

        Parameters
        ----------
        line : str
            JSON line string.
        line_number : int | None
            Optional line number for error reporting.

        Returns
        -------
        ManifestEntry

        Raises
        ------
        InvalidManifestError
        """
        line = line.strip()
        if not line:
            raise InvalidManifestError("Empty line", line_number)

        try:
            entry: dict[str, Any] = json.loads(line)
        except json.JSONDecodeError as e:
            raise InvalidManifestError(f"Invalid JSON: {e}", line_number) from e

        if "kind" not in entry:
            raise InvalidManifestError("Missing 'kind' field", line_number)

        kind = entry["kind"]
        if kind == "file":
            if "key" not in entry:
                raise InvalidManifestError("File entry missing 'key'", line_number)
        elif kind == "shard":
            if "archive_key" not in entry:
                raise InvalidManifestError("Shard entry missing 'archive_key'", line_number)
        elif kind == "manifest_ref":
            if "key" not in entry:
                raise InvalidManifestError("Manifest ref entry missing 'key'", line_number)
        else:
            raise InvalidManifestError(f"Unknown entry kind: {kind}", line_number)

        return entry

    @staticmethod
    def iter_lines(stream: StreamingBody | Any, limit: int | None = None) -> Iterator[str]:
        """Iterate over lines in a StreamingBody or file-like object.

        Handles both binary and text streams transparently.

        Parameters
        ----------
        stream : StreamingBody | Any
            StreamingBody from S3, tarfile member, or any file-like object.
        limit : int | None
            Maximum number of lines to yield. If None, yields all lines.

        Yields
        ------
        str
        """
        count = 0
        if hasattr(stream, "iter_lines"):
            try:
                # Try calling with limit parameter (for our custom classes)
                for raw_line in stream.iter_lines(chunk_size=8192, limit=limit):
                    if limit is not None and count >= limit:
                        break
                    yield raw_line.decode("utf-8")
                    count += 1
            except TypeError:
                # Fall back to calling without limit and applying limit manually
                for raw_line in stream.iter_lines(chunk_size=8192):
                    if limit is not None and count >= limit:
                        break
                    yield raw_line.decode("utf-8")
                    count += 1
        else:
            buffer = b""
            chunk_size = 8192
            while True:
                if limit is not None and count >= limit:
                    break
                chunk = stream.read(chunk_size)
                if not chunk:
                    break
                buffer += chunk
                while b"\n" in buffer:
                    if limit is not None and count >= limit:
                        break
                    line_bytes, buffer = buffer.split(b"\n", 1)
                    yield line_bytes.decode("utf-8")
                    count += 1
            if buffer and (limit is None or count < limit):
                yield buffer.decode("utf-8")

    @staticmethod
    def serialize_entry(entry: ManifestEntry) -> str:
        """Serialize a :data:`ManifestEntry` to a compact JSON line.

        Parameters
        ----------
        entry : ManifestEntry

        Returns
        -------
        str
        """
        return json.dumps(entry, separators=(",", ":"))

    @staticmethod
    def write_to_s3(
        s3_client: Any,
        bucket: str,
        key: str,
        entries: Iterable[ManifestEntry],
    ) -> None:
        """Write manifest entries to S3 as JSONL.

        Parameters
        ----------
        s3_client : Any
            Boto3 S3 client.
        bucket : str
            S3 bucket name.
        key : str
            Manifest S3 key.
        entries : Iterable[ManifestEntry]
            Entries to serialise.
        """
        lines = [Manifest.serialize_entry(e) for e in entries]
        content = "\n".join(lines) + "\n"
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=content.encode("utf-8"),
            ContentType="application/x-ndjson",
        )

    @staticmethod
    def read_from_s3(
        s3_client: Any,
        bucket: str,
        key: str,
    ) -> Iterator[ManifestEntry]:
        """Read and parse a manifest JSONL file from S3.

        Parameters
        ----------
        s3_client : Any
            Boto3 S3 client.
        bucket : str
            S3 bucket name.
        key : str
            Manifest S3 key.

        Yields
        ------
        ManifestEntry

        Raises
        ------
        ManifestNotFoundError
            If the manifest key does not exist.
        InvalidManifestError
            If a line cannot be parsed.
        """
        try:
            resp = s3_client.get_object(Bucket=bucket, Key=key)
            stream = resp["Body"]
        except s3_client.exceptions.NoSuchKey:
            raise ManifestNotFoundError(key)
        except Exception as e:
            raise InvalidManifestError(f"Failed to read manifest: {e}") from e

        line_number = 0
        try:
            for line in Manifest.iter_lines(stream):
                line_number += 1
                if line.strip():
                    yield Manifest.parse_line(line, line_number)
        finally:
            stream.close()

    @staticmethod
    def generate_part_key(base_key: str) -> str:
        """Generate a unique part manifest key from a root manifest key.

        Parameters
        ----------
        base_key : str
            Root manifest key (e.g. ``"training/123/samples/manifest.jsonl"``).

        Returns
        -------
        str
            Part key (e.g. ``"training/123/samples/manifests/part-20260207-abc123.jsonl"``).
        """
        if "/" in base_key:
            dir_path = "/".join(base_key.split("/")[:-1])
        else:
            dir_path = ""

        manifests_dir = f"{dir_path}/manifests" if dir_path else "manifests"
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"{manifests_dir}/part-{timestamp}-{unique_id}.jsonl"

    # ------------------------------------------------------------------ #
    #  Dunder helpers                                                     #
    # ------------------------------------------------------------------ #

    def __repr__(self) -> str:
        if self._key:
            return f"Manifest(key={self._key!r})"
        count = len(self._entries) if self._entries else 0
        return f"Manifest(entries=[{count} entries])"

    def __iter__(self) -> Iterator[ManifestEntry]:
        """Shortcut for ``iter_entries(resolve_refs=True)``."""
        return self.iter_entries()
