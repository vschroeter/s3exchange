"""Manifest parsing, writing, and resolution."""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterable, Iterator
from datetime import datetime
from typing import Any

from botocore.response import StreamingBody

from .exceptions import InvalidManifestError, ManifestNotFoundError
from .types import FileEntry, ManifestEntry, ManifestRef, ManifestRefEntry, ShardEntry


def parse_manifest_line(line: str, line_number: int | None = None) -> ManifestEntry:
    """Parse a single JSONL line into a manifest entry.

    Parameters
    ----------
    line : str
        JSON line string.
    line_number : int | None, optional
        Optional line number for error reporting.

    Returns
    -------
    ManifestEntry
        Parsed manifest entry dict.

    Raises
    ------
    InvalidManifestError
        If parsing fails or entry is invalid.
    """
    line = line.strip()
    if not line:
        raise InvalidManifestError("Empty line", line_number)

    try:
        entry: dict[str, Any] = json.loads(line)
    except json.JSONDecodeError as e:
        raise InvalidManifestError(f"Invalid JSON: {e}", line_number) from e

    # Validate entry has a kind
    if "kind" not in entry:
        raise InvalidManifestError("Missing 'kind' field", line_number)

    kind = entry["kind"]

    # Validate based on kind
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


def iter_manifest_lines(stream: StreamingBody | Any) -> Iterator[str]:
    """Iterate over lines in a StreamingBody or file-like object, handling both text and binary modes.

    Parameters
    ----------
    stream : StreamingBody | Any
        StreamingBody from S3 or file-like object (e.g., tarfile file object).

    Yields
    ------
    str
        Line strings.
    """
    # StreamingBody has iter_lines method
    if hasattr(stream, "iter_lines"):
        for line in stream.iter_lines(chunk_size=8192):
            yield line.decode("utf-8")
    else:
        # Fallback: read in chunks and split lines
        # This works for tarfile file objects and other file-like objects
        buffer = b""
        chunk_size = 8192

        print(stream, type(stream))

        while True:
            chunk = stream.read(chunk_size)
            if not chunk:
                break
            buffer += chunk
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                yield line.decode("utf-8")

        # Yield any remaining data in buffer
        if buffer:
            yield buffer.decode("utf-8")


def read_manifest_from_s3(s3_client: Any, bucket: str, key: str) -> Iterator[ManifestEntry]:
    """Read and parse a manifest file from S3.

    Parameters
    ----------
    s3_client : Any
        Boto3 S3 client.
    bucket : str
        S3 bucket name.
    key : str
        Manifest key.

    Yields
    ------
    ManifestEntry
        Parsed manifest entries.

    Raises
    ------
    ManifestNotFoundError
        If manifest doesn't exist.
    InvalidManifestError
        If parsing fails.
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
        for line in iter_manifest_lines(stream):
            line_number += 1
            if line.strip():  # Skip empty lines
                yield parse_manifest_line(line, line_number)
    finally:
        stream.close()


def iter_manifest_entries(
    manifest: ManifestRef,
    s3_client: Any | None = None,
    bucket: str | None = None,
    *,
    resolve_refs: bool = True,
    _visited: set[str] | None = None,
) -> Iterator[ManifestEntry]:
    """Iterate over manifest entries, optionally resolving manifest references.

    Parameters
    ----------
    manifest : ManifestRef
        Either a key (str) or iterable of entries.
    s3_client : Any | None, optional
        Boto3 S3 client (required if manifest is a key).
    bucket : str | None, optional
        S3 bucket (required if manifest is a key).
    resolve_refs : bool, optional
        If True, recursively resolve manifest_ref entries. Defaults to True.
    _visited : set[str] | None, optional
        Internal set to prevent circular references.

    Yields
    ------
    ManifestEntry
        Manifest entries.

    Raises
    ------
    ManifestNotFoundError
        If manifest key doesn't exist.
    InvalidManifestError
        If parsing fails.
    """
    if _visited is None:
        _visited = set()

    if isinstance(manifest, str):
        # Manifest is a key - read from S3
        if manifest in _visited:
            return  # Prevent circular references
        _visited.add(manifest)

        if s3_client is None or bucket is None:
            raise ValueError("s3_client and bucket required when manifest is a key")

        for entry in read_manifest_from_s3(s3_client, bucket, manifest):
            if resolve_refs and entry.get("kind") == "manifest_ref":
                # Recursively resolve manifest reference
                ref_key = entry["key"]
                yield from iter_manifest_entries(ref_key, s3_client, bucket, resolve_refs=True, _visited=_visited)
            else:
                yield entry
    else:
        # Manifest is already parsed entries
        for entry in manifest:
            if resolve_refs and entry.get("kind") == "manifest_ref":
                if s3_client is None or bucket is None:
                    raise ValueError("s3_client and bucket required to resolve manifest_ref")
                ref_key = entry["key"]
                if ref_key in _visited:
                    continue  # Prevent circular references
                _visited.add(ref_key)
                yield from iter_manifest_entries(ref_key, s3_client, bucket, resolve_refs=True, _visited=_visited)
            else:
                yield entry


def serialize_manifest_entry(entry: ManifestEntry) -> str:
    """Serialize a manifest entry to a JSONL line.

    Parameters
    ----------
    entry : ManifestEntry
        Manifest entry dict.

    Returns
    -------
    str
        JSON line string.
    """
    return json.dumps(entry, separators=(",", ":"))


def write_manifest_to_s3(
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
        Manifest key.
    entries : Iterable[ManifestEntry]
        Iterable of manifest entries.
    """
    import io

    # Build JSONL content
    lines = [serialize_manifest_entry(entry) for entry in entries]
    content = "\n".join(lines) + "\n"

    # Upload to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=content.encode("utf-8"),
        ContentType="application/x-ndjson",
    )


def generate_part_manifest_key(base_key: str) -> str:
    """Generate a unique part manifest key.

    Parameters
    ----------
    base_key : str
        Base manifest key (e.g., "training/123/samples/manifest.jsonl").

    Returns
    -------
    str
        Part manifest key (e.g., "training/123/samples/manifests/part-20260207-abc123.jsonl").
    """
    # Extract directory and base name
    if "/" in base_key:
        dir_path = "/".join(base_key.split("/")[:-1])
        base_name = base_key.split("/")[-1]
    else:
        dir_path = ""
        base_name = base_key

    # Create manifests subdirectory
    manifests_dir = f"{dir_path}/manifests" if dir_path else "manifests"

    # Generate unique part name
    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    unique_id = str(uuid.uuid4())[:8]
    part_name = f"part-{timestamp}-{unique_id}.jsonl"

    return f"{manifests_dir}/{part_name}"
