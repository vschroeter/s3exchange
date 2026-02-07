"""Shard class - archive reading, writing, member access, and lifecycle.

Every shard-related operation lives here.  :class:`ArchiveMemberBody` provides
a StreamingBody-compatible wrapper for archive members.
"""

from __future__ import annotations

import io
import json
import os
import tarfile
import tempfile
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .exceptions import InvalidManifestError, ObjectNotFoundError, ShardReadError
from .manifest import Manifest
from .types import (
    FileEntry,
    ManifestEntry,
    ShardEntry,
    ShardItem,
    StreamingBodyLike,
)

if TYPE_CHECKING:
    from botocore.response import StreamingBody

    from .store import S3ExchangeStore


# ------------------------------------------------------------------ #
#  StreamingBody-compatible wrapper                                   #
# ------------------------------------------------------------------ #


class ArchiveMemberBody:
    """StreamingBody-compatible wrapper for archive member file-like objects."""

    def __init__(self, fileobj: Any, archive_stream: Any | None = None) -> None:
        """Initialise archive member body.

        Parameters
        ----------
        fileobj : Any
            File-like object (e.g. ``io.BytesIO``).
        archive_stream : Any | None
            Optional reference kept for cleanup.
        """
        self._fileobj = fileobj
        self._archive_stream = archive_stream

    def read(self, amt: int | None = None) -> bytes:
        """Read bytes from the archive member.

        Parameters
        ----------
        amt : int | None
            Bytes to read.  *None* means read everything.

        Returns
        -------
        bytes
        """
        if amt is None:
            return self._fileobj.read()
        return self._fileobj.read(amt)

    def close(self) -> None:
        """Close the underlying file object."""
        if self._fileobj:
            self._fileobj.close()

    def iter_lines(self, chunk_size: int | None = None) -> Iterator[bytes]:
        """Iterate over lines.

        Parameters
        ----------
        chunk_size : int | None
            Read chunk size.  Defaults to 8192.

        Yields
        ------
        bytes
        """
        if chunk_size is None:
            chunk_size = 8192
        buffer = b""
        while True:
            chunk = self.read(chunk_size)
            if not chunk:
                if buffer:
                    yield buffer
                break
            buffer += chunk
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                yield line + b"\n"

    def readinto(self, b: bytearray) -> int | None:
        """Read bytes into a buffer.

        Parameters
        ----------
        b : bytearray
            Target buffer.

        Returns
        -------
        int | None
        """
        data = self.read(len(b))
        if not data:
            return None
        b[: len(data)] = data
        return len(data)

    def __enter__(self) -> ArchiveMemberBody:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


# ------------------------------------------------------------------ #
#  Shard class                                                        #
# ------------------------------------------------------------------ #


class Shard:
    """A shard archive stored in S3.

    A shard is a tar archive containing data files plus an internal
    ``__manifest__.jsonl`` describing them.

    Create instances via the factories or through the store::

        shard = Shard.from_key(store, "data/shards/shard-000001.tar.gz")
        shard = Shard.from_entry(store, shard_entry_dict)
        shard = store.get_shard("data/shards/shard-000001.tar.gz")

    The archive bytes are lazily downloaded and cached so that repeated
    calls to :meth:`get_manifest`, :meth:`iter_entries`, etc. do **not**
    trigger additional S3 downloads.
    """

    # ------------------------------------------------------------------ #
    #  Construction                                                       #
    # ------------------------------------------------------------------ #

    def __init__(
        self,
        store: S3ExchangeStore,
        entry: ShardEntry,
    ) -> None:
        """Initialise a Shard.

        Parameters
        ----------
        store : S3ExchangeStore
            Parent store.
        entry : ShardEntry
            Shard metadata dict.
        """
        self._store = store
        self._entry = entry
        self._cached_archive_data: bytes | None = None

    @classmethod
    def from_key(cls, store: S3ExchangeStore, key: str) -> Shard:
        """Create a Shard from an S3 archive key (format inferred from extension)."""
        fmt, compression = Shard.infer_archive_format(key)
        entry = ShardEntry(
            kind="shard",
            archive_key=key,
            format=fmt,
            compression=compression,
        )
        return cls(store, entry)

    @classmethod
    def from_entry(cls, store: S3ExchangeStore, entry: ShardEntry) -> Shard:
        """Create a Shard from an existing :class:`ShardEntry` dict."""
        return cls(store, entry)

    # ------------------------------------------------------------------ #
    #  Properties                                                         #
    # ------------------------------------------------------------------ #

    @property
    def archive_key(self) -> str:
        """S3 key of the archive."""
        return self._entry["archive_key"]

    @property
    def format(self) -> str:
        """Archive format (currently always ``"tar"``)."""
        return self._entry.get("format", "tar")

    @property
    def compression(self) -> str | None:
        """Compression algorithm (``"gzip"``, ``None``, …)."""
        return self._entry.get("compression", "gzip")

    @property
    def internal_manifest_path(self) -> str:
        """Path of the internal manifest inside the archive."""
        return self._entry.get("internal_manifest_path", "__manifest__.jsonl")

    @property
    def entry(self) -> ShardEntry:
        """The underlying :class:`ShardEntry` dict."""
        return self._entry

    @property
    def store(self) -> S3ExchangeStore:
        """Parent :class:`S3ExchangeStore`."""
        return self._store

    # ------------------------------------------------------------------ #
    #  Archive data access (lazy & cached)                                #
    # ------------------------------------------------------------------ #

    def _get_archive_data(self) -> bytes:
        """Download (and cache) the raw archive bytes from S3."""
        if self._cached_archive_data is None:
            stream = self._store.get_object(self.archive_key)
            try:
                self._cached_archive_data = stream.read()
            finally:
                stream.close()
        return self._cached_archive_data

    def _open_tar(self) -> tuple[io.BytesIO, str]:
        """Return ``(BytesIO buffer, tar open mode)`` for the cached archive."""
        data = self._get_archive_data()
        buf = io.BytesIO(data)
        mode = Shard._determine_tar_mode(self.compression, read=True)
        return buf, mode

    # ------------------------------------------------------------------ #
    #  Instance methods                                                   #
    # ------------------------------------------------------------------ #

    def get_manifest(self) -> Manifest:
        """Return the internal manifest as a :class:`Manifest` object.

        Returns
        -------
        Manifest
            Manifest wrapping the shard's internal file entries.
        """
        entries = self._read_internal_manifest()
        return Manifest.from_entries(self._store, entries)

    def _read_internal_manifest(self) -> list[FileEntry]:
        """Parse the ``__manifest__.jsonl`` inside the archive."""
        buf, mode = self._open_tar()
        try:
            with tarfile.open(fileobj=buf, mode=mode) as tar:
                try:
                    manifest_member = tar.getmember(self.internal_manifest_path)
                except KeyError:
                    raise ShardReadError(f"Internal manifest not found: {self.internal_manifest_path}")

                manifest_file = tar.extractfile(manifest_member)
                if manifest_file is None:
                    raise ShardReadError(f"Failed to extract internal manifest: {self.internal_manifest_path}")

                entries: list[FileEntry] = []
                line_number = 0
                try:
                    for line in Manifest.iter_lines(manifest_file):
                        line_number += 1
                        if not line.strip():
                            continue

                        try:
                            entry = Manifest.parse_line(line, line_number)
                        except InvalidManifestError as e:
                            # Backward compat: synthesise key from member_path
                            if "missing 'key'" in str(e).lower() and self.archive_key:
                                entry_dict: dict[str, Any] = json.loads(line.strip())
                                if entry_dict.get("kind") == "file" and "key" not in entry_dict:
                                    mp = entry_dict.get("member_path", "")
                                    if mp:
                                        entry_dict["key"] = f"{self.archive_key}#{mp}"
                                    else:
                                        raise
                                entry = entry_dict
                            else:
                                raise

                        if entry.get("kind") != "file":
                            raise ShardReadError(f"Internal manifest entry must be 'file', got: {entry.get('kind')}")

                        # Ensure a virtual key exists
                        if self.archive_key and "key" not in entry:
                            mp = entry.get("member_path", "")
                            if mp:
                                entry["key"] = f"{self.archive_key}#{mp}"

                        entries.append(entry)
                finally:
                    manifest_file.close()

            return entries
        except ShardReadError:
            raise
        except Exception as e:
            raise ShardReadError(f"Failed to read shard archive: {e}") from e

    def iter_entries(self) -> Iterator[tuple[ArchiveMemberBody, FileEntry]]:
        """Iterate over data files (excludes the internal manifest).

        Yields
        ------
        tuple[ArchiveMemberBody, FileEntry]
            ``(stream, enriched_entry)`` for each data member.
        """
        internal_manifest = self._read_internal_manifest()
        buf, mode = self._open_tar()

        try:
            with tarfile.open(fileobj=buf, mode=mode) as tar:
                # Build lookup: member_path -> FileEntry
                member_map: dict[str, FileEntry] = {}
                for fe in internal_manifest:
                    mp = fe.get("member_path", fe.get("key", ""))
                    if mp:
                        member_map[mp] = fe

                for member in tar:
                    if not member.isfile():
                        continue
                    if member.name == self.internal_manifest_path:
                        continue

                    if member.name in member_map:
                        entry = member_map[member.name]
                    else:
                        entry = FileEntry(
                            kind="file",
                            member_path=member.name,
                            size_bytes=member.size if hasattr(member, "size") else None,
                        )

                    member_file = tar.extractfile(member)
                    if member_file is None:
                        continue

                    member_data = member_file.read()
                    member_file.close()

                    # Enrich with archive context
                    enriched: FileEntry = {
                        **entry,
                        "archive_key": self.archive_key,
                        "member_path": entry.get("member_path", member.name),
                    }
                    if "key" not in enriched:
                        enriched["key"] = f"{self.archive_key}#{member.name}"

                    yield ArchiveMemberBody(io.BytesIO(member_data)), enriched
        except ShardReadError:
            raise
        except Exception as e:
            raise ShardReadError(f"Failed to iterate shard members: {e}") from e

    def iter_objects(self) -> Iterator[tuple[ArchiveMemberBody, FileEntry]]:
        """Iterate over **all** archive members including the internal manifest.

        Yields
        ------
        tuple[ArchiveMemberBody, FileEntry]
        """
        buf, mode = self._open_tar()
        try:
            with tarfile.open(fileobj=buf, mode=mode) as tar:
                for member in tar:
                    if not member.isfile():
                        continue
                    member_file = tar.extractfile(member)
                    if member_file is None:
                        continue

                    data = member_file.read()
                    member_file.close()

                    entry: FileEntry = {
                        "kind": "file",
                        "key": f"{self.archive_key}#{member.name}",
                        "member_path": member.name,
                        "size_bytes": member.size if hasattr(member, "size") else None,
                    }
                    yield ArchiveMemberBody(io.BytesIO(data)), entry
        except Exception as e:
            if isinstance(e, ShardReadError):
                raise
            raise ShardReadError(f"Failed to iterate shard objects: {e}") from e

    def get_member(self, member_path: str) -> ArchiveMemberBody:
        """Extract a single member from the archive.

        Parameters
        ----------
        member_path : str
            Path inside the tar archive.

        Returns
        -------
        ArchiveMemberBody

        Raises
        ------
        ObjectNotFoundError
            If the member does not exist.
        ShardReadError
            If extraction fails.
        """
        buf, mode = self._open_tar()
        try:
            with tarfile.open(fileobj=buf, mode=mode) as tar:
                try:
                    member = tar.getmember(member_path)
                except KeyError:
                    raise ObjectNotFoundError(f"{member_path} not found in archive")

                if not member.isfile():
                    raise ShardReadError(f"Member {member_path} is not a file")

                member_file = tar.extractfile(member)
                if member_file is None:
                    raise ShardReadError(f"Failed to extract member: {member_path}")

                data = member_file.read()
                member_file.close()
                return ArchiveMemberBody(io.BytesIO(data))
        except (ShardReadError, ObjectNotFoundError):
            raise
        except Exception as e:
            raise ShardReadError(f"Failed to extract shard member: {e}") from e

    def delete(self) -> None:
        """Delete the shard archive from S3."""
        self._store.delete_key(self.archive_key)

    # ------------------------------------------------------------------ #
    #  Static / class-level utilities                                     #
    # ------------------------------------------------------------------ #

    @staticmethod
    def infer_archive_format(archive_key: str) -> tuple[str, str | None]:
        """Infer ``(format, compression)`` from an archive key extension.

        Parameters
        ----------
        archive_key : str

        Returns
        -------
        tuple[str, str | None]
            ``(format, compression)`` - e.g. ``("tar", "gzip")``.
        """
        fmt = "tar"
        compression: str | None = None
        if archive_key.endswith(".tar.gz") or archive_key.endswith(".tgz"):
            compression = "gzip"
        elif archive_key.endswith(".tar.bz2"):
            compression = "bzip2"
        elif archive_key.endswith(".tar.xz"):
            compression = "xz"
        return fmt, compression

    @staticmethod
    def _determine_tar_mode(
        compression: str | None,
        *,
        read: bool = True,
        streaming: bool = False,
    ) -> str:
        """Build a ``tarfile.open`` mode string.

        Parameters
        ----------
        compression : str | None
            Compression algorithm.
        read : bool
            *True* for reading, *False* for writing.
        streaming : bool
            Use streaming (``|``) separator (write-only).

        Returns
        -------
        str
        """
        if read:
            mode = "r:"
        elif streaming:
            mode = "w|"
        else:
            mode = "w:"

        if compression == "gzip":
            mode += "gz"
        elif compression:
            mode += compression
        return mode

    @staticmethod
    def create_archive(
        shard_items: Sequence[ShardItem],
        archive_key: str,
        *,
        format: str = "tar",
        compression: str | None = "gzip",
        internal_manifest_path: str = "__manifest__.jsonl",
        use_temp_file: bool = True,
        temp_file_threshold: int = 100 * 1024 * 1024,
    ) -> tuple[bytes | Path, int]:
        """Create a shard tar archive from *shard_items*.

        Parameters
        ----------
        shard_items : Sequence[ShardItem]
            Items to include in the archive.
        archive_key : str
            S3 key for the archive (used for virtual key generation).
        format : str
            Archive format (only ``"tar"`` is supported).
        compression : str | None
            Compression algorithm (``"gzip"``, ``None``, …).
        internal_manifest_path : str
            Path for the internal manifest inside the archive.
        use_temp_file : bool
            Use a temp file for large archives.
        temp_file_threshold : int
            Byte threshold for switching to temp-file mode.

        Returns
        -------
        tuple[bytes | Path, int]
            ``(archive_data_or_path, total_size_bytes)``
        """
        if format != "tar":
            raise ValueError(f"Unsupported archive format: {format}")

        mode = Shard._determine_tar_mode(compression, read=False, streaming=True)

        # Estimate total size to decide temp-file vs in-memory
        total_size_estimate = sum(item.get("size_bytes", 0) or 0 for item in shard_items)
        use_temp = use_temp_file and total_size_estimate > temp_file_threshold

        # Build internal manifest entries
        internal_entries: list[FileEntry] = []
        for item in shard_items:
            member_path = item["member_path"]
            size_bytes = item.get("size_bytes")
            if size_bytes is None:
                source = item.get("source")
                if isinstance(source, (str, Path)):
                    try:
                        size_bytes = os.path.getsize(source)
                    except OSError:
                        pass

            internal_entries.append(
                FileEntry(
                    kind="file",
                    key=f"{archive_key}#{member_path}",
                    member_path=member_path,
                    id=item.get("id"),
                    meta=item.get("meta"),
                    size_bytes=size_bytes,
                )
            )

        manifest_content = "\n".join(Manifest.serialize_entry(e) for e in internal_entries) + "\n"
        manifest_bytes = manifest_content.encode("utf-8")

        # ---- temp-file path ---- #
        if use_temp:
            fd, temp_path = tempfile.mkstemp(
                suffix=".tar.gz" if compression == "gzip" else ".tar",
            )
            try:
                with open(fd, "wb") as f:
                    with tarfile.open(fileobj=f, mode=mode) as tar:
                        # Internal manifest
                        info = tarfile.TarInfo(name=internal_manifest_path)
                        info.size = len(manifest_bytes)
                        tar.addfile(info, io.BytesIO(manifest_bytes))

                        for item in shard_items:
                            source = item["source"]
                            mp = item["member_path"]
                            if isinstance(source, (str, Path)):
                                tar.add(str(source), arcname=mp)
                            elif isinstance(source, bytes):
                                ti = tarfile.TarInfo(name=mp)
                                ti.size = len(source)
                                tar.addfile(ti, io.BytesIO(source))
                            else:
                                raise ValueError(f"Unsupported source type: {type(source)}")

                total_size = os.path.getsize(temp_path)
                return Path(temp_path), total_size
            except Exception:
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass
                raise

        # ---- in-memory path ---- #
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode=mode) as tar:
            info = tarfile.TarInfo(name=internal_manifest_path)
            info.size = len(manifest_bytes)
            tar.addfile(info, io.BytesIO(manifest_bytes))

            for item in shard_items:
                source = item["source"]
                mp = item["member_path"]
                if isinstance(source, (str, Path)):
                    with open(str(source), "rb") as f:
                        file_data = f.read()
                    ti = tarfile.TarInfo(name=mp)
                    ti.size = len(file_data)
                    tar.addfile(ti, io.BytesIO(file_data))
                elif isinstance(source, bytes):
                    ti = tarfile.TarInfo(name=mp)
                    ti.size = len(source)
                    tar.addfile(ti, io.BytesIO(source))
                else:
                    raise ValueError(f"Unsupported source type: {type(source)}")

        buffer.seek(0)
        data = buffer.getvalue()
        return data, len(data)

    @staticmethod
    def split_items(
        items: Iterable[ShardItem],
        *,
        max_entries: int | None = None,
        max_bytes: int | None = None,
    ) -> Iterator[list[ShardItem]]:
        """Split items into shard-sized batches.

        Parameters
        ----------
        items : Iterable[ShardItem]
            Items to partition.
        max_entries : int | None
            Max entries per batch.
        max_bytes : int | None
            Max bytes per batch.

        Yields
        ------
        list[ShardItem]
        """
        if max_entries is None and max_bytes is None:
            yield list(items)
            return

        current_shard: list[ShardItem] = []
        current_bytes = 0
        current_count = 0

        for item in items:
            item_size = item.get("size_bytes")
            if item_size is None:
                source = item.get("source")
                if isinstance(source, (str, Path)):
                    try:
                        item_size = os.path.getsize(source)
                    except OSError:
                        item_size = None

            would_exceed_entries = max_entries is not None and current_count >= max_entries
            would_exceed_bytes = max_bytes is not None and item_size is not None and current_bytes + item_size > max_bytes

            if current_shard and (would_exceed_entries or would_exceed_bytes):
                yield current_shard
                current_shard = []
                current_bytes = 0
                current_count = 0

            current_shard.append(item)
            current_count += 1
            if item_size is not None:
                current_bytes += item_size

        if current_shard:
            yield current_shard

    # ------------------------------------------------------------------ #
    #  Dunder helpers                                                     #
    # ------------------------------------------------------------------ #

    def __repr__(self) -> str:
        return f"Shard(archive_key={self.archive_key!r})"
