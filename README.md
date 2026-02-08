# s3exchange

A Python library for service-wise namespace management of S3 artifacts with manifest-based data exchange, supporting both loose objects and shard archives.

## Installation

```bash
uv add s3exchange
```

## Quick Start

```python
import boto3
from s3exchange import S3ExchangeStore

# Initialize store
s3_client = boto3.client('s3', endpoint_url='http://garage:3900')
store = S3ExchangeStore(
    s3_client=s3_client,
    bucket='my-bucket',
    base_prefix='prod',  # Optional: prefix all keys
    default_vars={'service_name': 'training-service'},
)
```

## Key Features

### Reading Objects

#### Read a single object:
```python
stream = store.get_object("training/123/samples/file.wav")
data = stream.read()
stream.close()
```

#### Read from a manifest (lazy iteration):
```python
# Iterate over all objects in a manifest
for stream, entry in store.iter_objects("training/123/samples/manifest.jsonl"):
    print(f"Reading {entry['key']}")
    data = stream.read()
    # Process data...
    stream.close()
```

#### Read manifest entries without fetching objects:
```python
for entry in store.iter_manifest_entries("training/123/samples/manifest.jsonl"):
    print(f"Entry: {entry['kind']}, Key: {entry.get('key', entry.get('archive_key'))}")
```

### Writing Objects

#### Put a single object:
```python
entry = store.put_object(
    key="training/123/samples/file.wav",
    data=b"audio data...",
    id="file-001",
    meta={"sr": 16000, "length": 15342},
    content_type="audio/wav",
)
# Returns a FileEntry for the manifest
```

#### Put from file path:
```python
entry = store.put_object(
    key="training/123/samples/file.wav",
    data="/path/to/file.wav",  # Path string or Path object
    id="file-001",
)
```

### Writing Manifests

#### Overwrite mode (simple):
```python
entries = [
    {"kind": "file", "key": "training/123/samples/file1.wav", "id": "001"},
    {"kind": "file", "key": "training/123/samples/file2.wav", "id": "002"},
]
store.write_manifest(
    key="training/123/samples/manifest.jsonl",
    entries=entries,
    mode="overwrite",
)
```

#### Append parts mode (recommended for updates):
```python
# Add new entries without rewriting entire manifest
new_entries = [
    {"kind": "file", "key": "training/123/samples/file3.wav", "id": "003"},
]
store.write_manifest(
    key="training/123/samples/manifest.jsonl",
    entries=new_entries,
    mode="append_parts",  # Creates a part file and updates root manifest
)
```

### Shard Archives

Shard archives are tar/tar.gz files containing multiple files with an internal manifest.

#### Create and upload shards:
```python
from s3exchange import Shard

# Prepare items for sharding
items = [
    {
        "source": "/path/to/file1.wav",
        "member_path": "0001.wav",
        "id": "0001",
        "meta": {"sr": 16000},
        "size_bytes": 123456,
    },
    {
        "source": "/path/to/file2.wav",
        "member_path": "0002.wav",
        "id": "0002",
        "meta": {"sr": 16000},
        "size_bytes": 234567,
    },
    # ... more items
]

# Split into shards (max 10000 entries or 1GB per shard)
shard_batches = Shard.split_items(
    items,
    max_entries=10000,
    max_bytes=1024 * 1024 * 1024,  # 1 GB
)

# Upload each shard
shard_entries = []
for i, batch in enumerate(shard_batches):
    archive_key = f"training/123/samples/shards/shard-{i:05d}.tar.gz"
    shard = store.put_shard_archive(
        archive_key=archive_key,
        shard_items=batch,
        format="tar",
        compression="gzip",
    )
    shard_entries.append(shard.entry)  # Extract ShardEntry from Shard object

# Write shard entries to manifest
store.put_sharded(
    manifest_key="training/123/samples/manifest.jsonl",
    shard_entries=shard_entries,
    update_mode="append_parts",
)
```

#### Read from shards (automatically expanded):
```python
# Shards are automatically expanded when iterating objects
for stream, entry in store.iter_objects("training/123/samples/manifest.jsonl"):
    # For shard members, entry includes:
    # - entry['archive_key']: The shard archive key
    # - entry['member_path']: Path inside the archive
    # - entry['key']: Virtual key like "archive.tar.gz#member.wav"
    print(f"Reading from shard: {entry['archive_key']}, member: {entry['member_path']}")
    data = stream.read()
    stream.close()
```

### Deletion Operations

#### Delete a single object:
```python
store.delete_key("training/123/samples/file.wav")
```

#### Delete by prefix:
```python
# Delete all objects with prefix, optionally filtered by regex
count = store.delete_prefix(
    prefix="training/123/samples/",
    regex=r".*\.wav$",  # Only delete .wav files
)
print(f"Deleted {count} objects")
```

#### Delete by manifest (recursive):
```python
# Deletes all objects/shards referenced in manifest, plus manifest files themselves
report = store.delete_manifest(
    manifest="training/123/samples/manifest.jsonl",
    delete_manifests=True,  # Also delete manifest files
    dedupe=True,  # Avoid double-deletion
)
print(f"Deleted {report['deleted_object_count']} objects")
print(f"Deleted {report['deleted_archive_count']} archives")
print(f"Deleted {report['deleted_manifest_count']} manifests")
```

### Listing Operations

#### List S3 keys:
```python
for key in store.list_keys(prefix="training/123/"):
    print(key)
```

#### List files in manifest:
```python
# List all file entries (including shard members if include_shards=True)
for entry in store.list_manifest_files(
    manifest="training/123/samples/manifest.jsonl",
    include_shards=True,
):
    print(f"File: {entry.get('key', entry.get('member_path'))}")
```

#### Filter by prefix:
```python
# Filter manifest entries by prefix (works for shard members too)
for entry in store.list_by_manifest_prefix(
    manifest="training/123/samples/manifest.jsonl",
    prefix_filter="training/123/samples/000",
):
    print(entry['key'])
```

### Manifest Compaction

Flatten a manifest with many parts into a single clean manifest:

```python
report = store.compact_manifest(
    src_manifest_key="training/123/samples/manifest.jsonl",
    dst_manifest_key="training/123/samples/manifest-compact.jsonl",
    resolve_refs=True,  # Resolve all manifest_ref entries
    expand_shards=False,  # Keep shard entries (set True to expand into files)
)
print(f"Compacted {report['total_entries']} entries")
```

## Manifest Format

Manifests are JSONL files (one JSON object per line):

### File Entry
```json
{"kind":"file","key":"training/123/samples/0001.wav","id":"0001","meta":{"sr":16000},"size_bytes":123456}
```

### Shard Entry
```json
{"kind":"shard","archive_key":"training/123/samples/shards/shard-00001.tar.gz","format":"tar","compression":"gzip","internal_manifest_path":"__manifest__.jsonl","count":10000,"size_bytes":987654321}
```

### Manifest Reference Entry
```json
{"kind":"manifest_ref","key":"training/123/samples/manifests/part-00001.jsonl"}
```

## Error Handling

The library provides domain-specific exceptions:

```python
from s3exchange import (
    ObjectNotFoundError,
    ManifestNotFoundError,
    MissingPlaceholderError,
    InvalidManifestError,
    ShardReadError,
)

try:
    stream = store.get_object("nonexistent.wav")
except ObjectNotFoundError as e:
    print(f"Object not found: {e.key}")

try:
    key = store._resolve_key("training/{job_id}/samples", {})  # Missing job_id
except MissingPlaceholderError as e:
    print(f"Missing placeholder: {e.placeholder}")
```

## Type Hints

The library is fully typed with Python 3.12 type annotations. All types are available for import:

```python
from s3exchange import (
    ManifestEntry,
    FileEntry,
    ShardEntry,
    ShardItem,
    DeleteReport,
    CompactReport,
)
```
