"""Shard archive reading, writing, and internal manifest handling."""

from __future__ import annotations

import io
import json
import os
import tarfile
import tempfile
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from typing import Any

from botocore.response import StreamingBody

from .exceptions import InvalidManifestError, ObjectNotFoundError, ShardReadError
from .manifest import (
    iter_manifest_lines,
    parse_manifest_line,
    serialize_manifest_entry,
)
from .types import (
    FileEntry,
    ManifestEntry,
    ShardEntry,
    ShardItem,
    StreamingBodyLike,
)


class ArchiveMemberBody:
    """StreamingBody-compatible wrapper for archive member file-like objects."""

    def __init__(self, fileobj: Any, archive_stream: Any | None = None) -> None:
        """Initialize archive member body.

        Parameters
        ----------
        fileobj : Any
            File-like object from tarfile.extractfile()
        archive_stream : Any | None
            Optional reference to archive stream (for cleanup)
        """

        self._fileobj = fileobj
        self._archive_stream = archive_stream

    def read(self, amt: int | None = None) -> bytes:
        """Read bytes from the archive member.

        Parameters
        ----------
        amt : int | None, optional
            Number of bytes to read. If None, read all remaining bytes.

        Returns
        -------
        bytes
            Read bytes from the archive member.
        """
        if amt is None:
            return self._fileobj.read()
        return self._fileobj.read(amt)

    def close(self) -> None:
        """Close the archive member."""
        if self._fileobj:
            self._fileobj.close()

    def iter_lines(self, chunk_size: int | None = None) -> Iterator[bytes]:
        """Iterate over lines in the archive member.

        Parameters
        ----------
        chunk_size : int | None, optional
            Size of chunks to read. Defaults to 8192.

        Yields
        ------
        bytes
            Lines from the archive member.
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
            Buffer to read into.

        Returns
        -------
        int | None
            Number of bytes read, or None if no data available.
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


def read_shard_internal_manifest(
    archive_stream: StreamingBody,
    internal_manifest_path: str = "__manifest__.jsonl",
    format: str = "tar",
    compression: str | None = "gzip",
    archive_key: str | None = None,
) -> list[FileEntry]:
    """Read the internal manifest from a shard archive.

    Parameters
    ----------
    archive_stream : StreamingBody
        StreamingBody of the archive. Will be read completely into memory.
    internal_manifest_path : str, optional
        Path to internal manifest inside archive. Defaults to "__manifest__.jsonl".
    format : str, optional
        Archive format. Defaults to "tar".
    compression : str | None, optional
        Compression type ("gzip", None, etc.). Defaults to "gzip".
    archive_key : str | None, optional
        Archive key for generating virtual keys if entries are missing 'key' attribute.
        Used for backward compatibility with old shards.

    Returns
    -------
    list[FileEntry]
        List of file entries from internal manifest.

    Raises
    ------
    ShardReadError
        If archive is corrupt or manifest is missing.
    """
    # Read entire archive into memory to avoid streaming issues
    try:
        archive_data = archive_stream.read()
    except Exception as e:
        raise ShardReadError(f"Failed to read archive stream: {e}") from e
    finally:
        archive_stream.close()

    # Create BytesIO buffer from archive data
    archive_buffer = io.BytesIO(archive_data)

    # Determine tar mode
    mode = "r:"
    if compression == "gzip":
        mode += "gz"
    elif compression:
        mode += compression
    else:
        mode += ""

    try:
        # Open tar stream from buffer
        with tarfile.open(fileobj=archive_buffer, mode=mode) as tar:
            # Find and read internal manifest
            try:
                manifest_member = tar.getmember(internal_manifest_path)
            except KeyError:
                raise ShardReadError(f"Internal manifest not found: {internal_manifest_path}")

            manifest_file = tar.extractfile(manifest_member)
            if manifest_file is None:
                raise ShardReadError(f"Failed to extract internal manifest: {internal_manifest_path}")

            # Parse internal manifest
            entries: list[FileEntry] = []
            line_number = 0
            try:
                for line in iter_manifest_lines(manifest_file):
                    line_number += 1
                    if line.strip():
                        # Try to parse entry
                        try:
                            entry = parse_manifest_line(line, line_number)
                        except InvalidManifestError as e:
                            # If parsing fails due to missing 'key', try to handle it
                            if "missing 'key'" in str(e).lower() and archive_key:
                                # Parse JSON manually to add key
                                entry_dict: dict[str, Any] = json.loads(line.strip())
                                if entry_dict.get("kind") == "file" and "key" not in entry_dict:
                                    member_path = entry_dict.get("member_path", "")
                                    if member_path:
                                        entry_dict["key"] = f"{archive_key}#{member_path}"
                                    else:
                                        raise  # Re-raise if we can't generate key
                                entry = entry_dict
                            else:
                                raise
                        
                        if entry.get("kind") != "file":
                            raise ShardReadError(f"Internal manifest entry must be 'file', got: {entry.get('kind')}")
                        
                        # Ensure key is present (for backward compatibility)
                        if archive_key and "key" not in entry:
                            member_path = entry.get("member_path", "")
                            if member_path:
                                entry["key"] = f"{archive_key}#{member_path}"
                        
                        entries.append(entry)
            finally:
                manifest_file.close()

        return entries
    except Exception as e:
        if isinstance(e, ShardReadError):
            raise
        raise ShardReadError(f"Failed to read shard archive: {e}") from e


def iter_shard_members(
    archive_stream: StreamingBody,
    internal_manifest: list[FileEntry],
    internal_manifest_path: str = "__manifest__.jsonl",
    format: str = "tar",
    compression: str | None = "gzip",
) -> Iterator[tuple[ArchiveMemberBody, FileEntry]]:
    """Iterate over members in a shard archive, yielding (stream, entry) pairs.

    Parameters
    ----------
    archive_stream : StreamingBody
        StreamingBody of the archive. Will be read completely into memory.
    internal_manifest : list[FileEntry]
        Pre-parsed internal manifest (required).
    internal_manifest_path : str, optional
        Path to internal manifest inside archive. Defaults to "__manifest__.jsonl".
    format : str, optional
        Archive format. Defaults to "tar".
    compression : str | None, optional
        Compression type ("gzip", None, etc.). Defaults to "gzip".

    Yields
    ------
    tuple[ArchiveMemberBody, FileEntry]
        Tuples of (ArchiveMemberBody, FileEntry) for each file member.
    """
    # Read entire archive into memory to avoid streaming issues
    try:
        archive_data = archive_stream.read()
    except Exception as e:
        raise ShardReadError(f"Failed to read archive stream: {e}") from e
    finally:
        archive_stream.close()

    # Create BytesIO buffer from archive data
    archive_buffer = io.BytesIO(archive_data)

    # Determine tar mode
    mode = "r:"
    if compression == "gzip":
        mode += "gz"
    elif compression:
        mode += compression
    else:
        mode += ""

    try:
        # Open tar stream from buffer
        with tarfile.open(fileobj=archive_buffer, mode=mode) as tar:
            # Build member path -> entry mapping
            member_map: dict[str, FileEntry] = {}
            if internal_manifest:
                for entry in internal_manifest:
                    member_path = entry.get("member_path", entry.get("key", ""))
                    if member_path:
                        member_map[member_path] = entry

            # Iterate over members
            for member in tar:
                if member.isfile():
                    member_path = member.name

                    # Skip internal manifest itself
                    if member_path == internal_manifest_path:
                        continue

                    # Get or create entry
                    if member_path in member_map:
                        entry = member_map[member_path]
                    else:
                        # Create entry from member info
                        entry = FileEntry(
                            kind="file",
                            member_path=member_path,
                            size_bytes=member.size if hasattr(member, "size") else None,
                        )

                    # Extract file
                    member_file = tar.extractfile(member)
                    if member_file is None:
                        continue

                    # Create a BytesIO buffer for the member file data
                    # Read all data from the member file into memory
                    member_data = member_file.read()
                    member_file.close()
                    member_buffer = io.BytesIO(member_data)

                    yield ArchiveMemberBody(member_buffer, None), entry
    except Exception as e:
        raise ShardReadError(f"Failed to iterate shard members: {e}") from e


def get_shard_member(
    archive_stream: StreamingBody,
    member_path: str,
    format: str = "tar",
    compression: str | None = "gzip",
    internal_manifest_path: str = "__manifest__.jsonl",
) -> ArchiveMemberBody:
    """Extract a specific member from a shard archive.

    Parameters
    ----------
    archive_stream : StreamingBody
        StreamingBody of the archive. Will be read completely into memory.
    member_path : str
        Path of the member file inside the archive.
    format : str, optional
        Archive format. Defaults to "tar".
    compression : str | None, optional
        Compression type ("gzip", None, etc.). Defaults to "gzip".
    internal_manifest_path : str, optional
        Path to internal manifest inside archive. Defaults to "__manifest__.jsonl".

    Returns
    -------
    ArchiveMemberBody
        StreamingBody-like object for the member file.

    Raises
    ------
    ShardReadError
        If archive is corrupt or member is not found.
    ObjectNotFoundError
        If member file is not found in the archive.
    """
    # Read entire archive into memory to avoid streaming issues
    try:
        archive_data = archive_stream.read()
    except Exception as e:
        raise ShardReadError(f"Failed to read archive stream: {e}") from e
    finally:
        archive_stream.close()

    # Create BytesIO buffer from archive data
    archive_buffer = io.BytesIO(archive_data)

    # Determine tar mode
    mode = "r:"
    if compression == "gzip":
        mode += "gz"
    elif compression:
        mode += compression
    else:
        mode += ""

    try:
        # Open tar stream from buffer
        with tarfile.open(fileobj=archive_buffer, mode=mode) as tar:
            # Find the member
            try:
                member = tar.getmember(member_path)
            except KeyError:
                raise ObjectNotFoundError(f"{member_path} not found in archive")

            if not member.isfile():
                raise ShardReadError(f"Member {member_path} is not a file")

            # Extract file
            member_file = tar.extractfile(member)
            if member_file is None:
                raise ShardReadError(f"Failed to extract member: {member_path}")

            # Create a BytesIO buffer for the member file data
            # Read all data from the member file into memory
            member_data = member_file.read()
            member_file.close()
            member_buffer = io.BytesIO(member_data)

            return ArchiveMemberBody(member_buffer, None)
    except Exception as e:
        if isinstance(e, (ShardReadError, ObjectNotFoundError)):
            raise
        raise ShardReadError(f"Failed to extract shard member: {e}") from e


def create_shard_archive(
    shard_items: Sequence[ShardItem],
    archive_key: str,
    format: str = "tar",
    compression: str | None = "gzip",
    internal_manifest_path: str = "__manifest__.jsonl",
    use_temp_file: bool = True,
    temp_file_threshold: int = 100 * 1024 * 1024,  # 100 MB
) -> tuple[bytes | Path, int]:
    """Create a shard archive from items.

    Parameters
    ----------
    shard_items : Sequence[ShardItem]
        Items to include in the archive.
    archive_key : str
        S3 key for the archive (used to generate virtual keys for internal manifest entries).
    format : str, optional
        Archive format. Defaults to "tar".
    compression : str | None, optional
        Compression type ("gzip", None, etc.). Defaults to "gzip".
    internal_manifest_path : str, optional
        Path for internal manifest inside archive. Defaults to "__manifest__.jsonl".
    use_temp_file : bool, optional
        If True, use temp file; if False, use in-memory buffer. Defaults to True.
    temp_file_threshold : int, optional
        Size threshold (bytes) for using temp file vs memory. Defaults to 100 MB.

    Returns
    -------
    tuple[bytes | Path, int]
        Tuple of (archive_data or Path, total_size_bytes).
    """
    if format != "tar":
        raise ValueError(f"Unsupported archive format: {format}")

    # Determine tar mode
    mode = "w|"
    if compression == "gzip":
        mode += "gz"
    elif compression:
        mode += compression
    else:
        mode += ""

    # Estimate total size to decide on temp file
    total_size_estimate = sum(item.get("size_bytes", 0) or 0 for item in shard_items)
    use_temp = use_temp_file and total_size_estimate > temp_file_threshold

    # Build internal manifest entries
    internal_entries: list[FileEntry] = []
    for item in shard_items:
        member_path = item["member_path"]
        size_bytes = item.get("size_bytes")

        # Try to get size from source if not provided
        if size_bytes is None:
            source = item.get("source")
            if isinstance(source, (str, Path)):
                try:
                    size_bytes = os.path.getsize(source)
                except OSError:
                    pass

        entry: FileEntry = {
            "kind": "file",
            "key": f"{archive_key}#{member_path}",
            "member_path": member_path,
            "id": item.get("id"),
            "meta": item.get("meta"),
            "size_bytes": size_bytes,
        }
        internal_entries.append(entry)

    # Create archive
    if use_temp:
        # Use temporary file
        fd, temp_path = tempfile.mkstemp(suffix=".tar.gz" if compression == "gzip" else ".tar")
        try:
            with open(fd, "wb") as f:
                with tarfile.open(fileobj=f, mode=mode) as tar:
                    # Write internal manifest first
                    manifest_content = "\n".join(serialize_manifest_entry(entry) for entry in internal_entries) + "\n"
                    manifest_info = tarfile.TarInfo(name=internal_manifest_path)
                    manifest_info.size = len(manifest_content.encode("utf-8"))
                    tar.addfile(manifest_info, io.BytesIO(manifest_content.encode("utf-8")))

                    # Add file members
                    for item in shard_items:
                        source = item["source"]
                        member_path = item["member_path"]

                        if isinstance(source, (str, Path)):
                            # Add file from path
                            tar.add(source, arcname=member_path)
                        elif isinstance(source, bytes):
                            # Add from bytes
                            info = tarfile.TarInfo(name=member_path)
                            info.size = len(source)
                            tar.addfile(info, io.BytesIO(source))
                        else:
                            raise ValueError(f"Unsupported source type: {type(source)}")

            total_size = os.path.getsize(temp_path)
            return Path(temp_path), total_size
        except Exception:
            # Clean up temp file on error
            try:
                os.unlink(temp_path)
            except OSError:
                pass
            raise
    else:
        # Use in-memory buffer
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode=mode) as tar:
            # Write internal manifest first
            manifest_content = "\n".join(serialize_manifest_entry(entry) for entry in internal_entries) + "\n"
            manifest_info = tarfile.TarInfo(name=internal_manifest_path)
            manifest_info.size = len(manifest_content.encode("utf-8"))
            tar.addfile(manifest_info, io.BytesIO(manifest_content.encode("utf-8")))

            # Add file members
            for item in shard_items:
                source = item["source"]
                member_path = item["member_path"]

                if isinstance(source, (str, Path)):
                    # Read file and add
                    with open(source, "rb") as f:
                        file_data = f.read()
                    info = tarfile.TarInfo(name=member_path)
                    info.size = len(file_data)
                    tar.addfile(info, io.BytesIO(file_data))
                elif isinstance(source, bytes):
                    # Add from bytes
                    info = tarfile.TarInfo(name=member_path)
                    info.size = len(source)
                    tar.addfile(info, io.BytesIO(source))
                else:
                    raise ValueError(f"Unsupported source type: {type(source)}")

        buffer.seek(0)
        data = buffer.getvalue()
        return data, len(data)


def create_shards(
    items: Iterable[ShardItem],
    *,
    max_entries: int | None = None,
    max_bytes: int | None = None,
) -> Iterator[list[ShardItem]]:
    """Split items into shard batches based on entry count or byte size limits.

    Parameters
    ----------
    items : Iterable[ShardItem]
        Iterable of shard items.
    max_entries : int | None, optional
        Maximum entries per shard. If None, no limit.
    max_bytes : int | None, optional
        Maximum bytes per shard. If None, no limit.

    Yields
    ------
    list[ShardItem]
        Lists of ShardItem for each shard batch.
    """
    if max_entries is None and max_bytes is None:
        # No limits - yield all items as single shard
        yield list(items)
        return

    current_shard: list[ShardItem] = []
    current_bytes = 0
    current_count = 0

    for item in items:
        # Get item size
        item_size = item.get("size_bytes")
        if item_size is None:
            source = item.get("source")
            if isinstance(source, (str, Path)):
                try:
                    item_size = os.path.getsize(source)
                except OSError:
                    item_size = None

        # Check if adding this item would exceed limits
        would_exceed_entries = max_entries is not None and current_count >= max_entries
        would_exceed_bytes = max_bytes is not None and item_size is not None and current_bytes + item_size > max_bytes

        # If current shard is non-empty and would exceed limits, yield it
        if current_shard and (would_exceed_entries or would_exceed_bytes):
            yield current_shard
            current_shard = []
            current_bytes = 0
            current_count = 0

        # Add item to current shard
        current_shard.append(item)
        current_count += 1
        if item_size is not None:
            current_bytes += item_size

    # Yield final shard if non-empty
    if current_shard:
        yield current_shard
