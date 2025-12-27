# jdb_val - WAL Value Storage

- [Introduction](#introduction)
- [Features](#features)
- [Usage](#usage)
- [WAL File Format](#wal-file-format)
- [Configuration](#configuration)
- [File Rotation](#file-rotation)
- [Storage Modes](#storage-modes)
- [API Overview](#api-overview)
- [GC / Garbage Collection](#gc--garbage-collection)
- [Fork Database Design](#fork-database-design)
- [Tech Stack](#tech-stack)

## Introduction

`jdb_val` is a high-performance WAL (Write-Ahead Log) value storage library for embedded key-value databases. It provides efficient storage with automatic mode selection based on data size, LRU caching, and file rotation.

## Features

- **Header Validation**: 12-byte header with version redundancy and CRC32
- **Flexible Configuration**: RocksDB-style configuration via `Conf` enum
- **Auto Mode Selection**: Automatically chooses optimal storage mode (inline/infile/file) based on data size
- **LRU Caching**: Head, data, and file handle caches for read performance
- **File Rotation**: Automatic rotation when file size exceeds limit
- **CRC32 Verification**: Data integrity check for file-stored values
- **Async I/O**: Built on compio for efficient single-threaded async operations
- **Streaming API**: Support for large value streaming read/write

## Usage

```rust
use jdb_val::{Wal, Pos, Head};

#[compio::main]
async fn main() -> jdb_val::Result<()> {
  let mut wal = Wal::new("data", &[]);
  wal.open().await?;

  // 1. Write: put() returns Pos (file_id + offset)
  let loc: Pos = wal.put(b"key", b"value").await?;

  // 2. Read head: use Pos to get Head (64B metadata)
  let head: Head = wal.read_head(loc).await?;

  // 3. Get data: use Head to retrieve key/value
  let key: Vec<u8> = wal.head_key(&head).await?;
  let val: Vec<u8> = wal.head_val(&head).await?;

  Ok(())
}
```

### Call Flow

```
put(key, val) → Pos
                 ↓
read_head(loc) → Head
                 ↓
head_key(&head) → Vec<u8>
head_val(&head) → Vec<u8>
```

- **Pos**: Position in WAL (wal_id + offset), 16 bytes, store this in your index
- **Head**: 64-byte metadata containing flags, lengths, inline data or pointers
- **head_key/head_val**: Reads actual data based on storage mode (inline/infile/file)

Directory structure after `Wal::new("data", &[])`:
```
data/
├── wal/    # WAL files (base32 encoded IDs)
└── bin/    # Large value files (>1MB)
```

## WAL File Format

### File Header (12 bytes)

```
+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
| Version (4B)                      | Version Copy (4B)                 | CRC32 (4B)                        |
| u32 little-endian                 | same as first                     | checksum of [0..4]                |
+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
```

### Entry Format

```
+------------------+------------------+------------------+
| Head (64B)       | Infile Data (var)| End Marker (12B) |
+------------------+------------------+------------------+
```

- **Head** (64 bytes): Fixed-size metadata header with CRC32
- **Infile Data** (variable): Key/value data if stored in same file
- **End Marker** (12 bytes): `[head_offset: u64 LE] [magic: 0xEDEDEDED]`

### Header Validation & Repair

On open, files are validated:
1. File < 12 bytes → skip
2. Version1 + CRC32 valid → use Version1 to fix Version2 if needed
3. Version2 + CRC32 valid → use Version2 to fix Version1
4. Version1 == Version2 but CRC mismatch → recalculate CRC32
5. All checks fail → fallback to scan recovery using magic bytes

## Configuration

```rust
use jdb_val::{Conf, Wal};

let mut wal = Wal::new("data", &[
  Conf::MaxSize(512 * 1024 * 1024),  // 512MB
  Conf::HeadLru(16384),
]);
```

### Default Values

| Config | Default | Description |
|--------|---------|-------------|
| `MaxSize` | 256MB | Max WAL file size before rotation |
| `HeadLru` | 8192 | LRU cache capacity for heads |
| `DataLru` | 1024 | LRU cache capacity for infile data |
| `FileLru` | 64 | LRU cache capacity for file handles |

## File Rotation

WAL files rotate automatically when:

```
cur_pos + data_len > max_size
```

On rotation:
1. Generate new file ID (timestamp-based)
2. Create new file with 12-byte header
3. Reset position to 12 (after header)

## Storage Modes

| Mode | Condition | Storage |
|------|-----------|---------|
| INLINE | key+val ≤ 50B | Embedded in Head |
| INFILE | data ≤ 1MB | Same WAL file |
| FILE | data > 1MB | Separate bin file |

Mode selection is automatic based on key/value sizes.

### Head Data Layout (50 bytes)

| Layout | Condition | Structure |
|--------|-----------|-----------|
| INLINE+INLINE | key+val ≤ 50B | `[key][val]` |
| INLINE+FILE | key ≤ 30B | `[key(30B)][val_pos(16B)][val_crc(4B)]` |
| FILE+INLINE | val ≤ 34B | `[key_pos(16B)][val(34B)]` |
| FILE+FILE | both large | `[key_pos(16B)][val_pos(16B)][..][val_crc(4B)]` |

## API Overview

### Core Types

```rust
// Position in WAL file (16 bytes) - store this in your index
pub struct Pos {
  wal_id: u64,  // WAL file ID
  offset: u64,  // Byte offset in file
}

// Metadata header (64 bytes)
pub struct Head {
  key_len: u32,
  val_len: u32,
  key_flag: Flag,  // INLINE / INFILE / FILE
  val_flag: Flag,
  data: [u8; 50],  // Inline data or Pos pointers
  head_crc32: u32,
}
```

### Wal Methods

| Method | Input | Output | Description |
|--------|-------|--------|-------------|
| `new(dir, conf)` | path, config | `Wal` | Create WAL manager |
| `open()` | - | `Result<()>` | Open/create WAL file |
| `put(key, val)` | `&[u8], &[u8]` | `Result<Pos>` | Write KV, return position |
| `put_stream(key, iter)` | key, chunk iterator | `Result<Pos>` | Stream write large value |
| `read_head(loc)` | `Pos` | `Result<Head>` | Read metadata at position |
| `head_key(&head)` | `&Head` | `Result<Vec<u8>>` | Get key data |
| `head_val(&head)` | `&Head` | `Result<Vec<u8>>` | Get value data (CRC checked for FILE) |
| `head_key_stream(&head)` | `&Head` | `Result<DataStream>` | Stream read key |
| `head_val_stream(&head)` | `&Head` | `Result<DataStream>` | Stream read value |
| `scan(id, f)` | file_id, callback | `Result<()>` | Iterate all entries |
| `iter_entries(id)` | file_id | `Result<LogIter>` | Get entry iterator |
| `sync_data()` | - | `Result<()>` | Flush data to disk |
| `sync_all()` | - | `Result<()>` | Flush data + metadata |

## GC / Garbage Collection

Uses `Gcable` trait callback to check if key is deleted.

```rust
use jdb_val::{Gcable, GcState, PosMap, Wal};

struct MyChecker { /* your index */ }

impl Gcable for MyChecker {
  async fn is_rm(&self, key: &[u8]) -> bool {
    // Query index to check if key is deleted
    false
  }
  
  async fn batch_update(&self, mapping: impl IntoIterator<Item = PosMap>) -> bool {
    for m in mapping {
      // Update index: m.key, m.old -> m.new
    }
    true
  }
}

async fn do_gc(wal: &mut Wal, checker: &MyChecker) {
  let mut state = GcState::new("data", 256 * 1024 * 1024);
  
  // Auto GC: merge oldest 75% WAL files
  wal.gc_auto(checker, &mut state).await.unwrap();
}
```

### GC Flow

1. Acquire file locks for target WAL files
2. Scan entries, filter deleted keys via `Gcable::is_rm()`
3. Rewrite live entries to new WAL
4. Call `Gcable::batch_update()` to update index
5. Delete old WAL files after success

## Fork Database Design

`jdb_val` is designed to support **Copy-on-Write fork databases** with shared value storage.

### Architecture

```
┌─────────────────┐     ┌─────────────────┐
│  DB1 (db_id=1)  │     │  DB2 (db_id=2)  │  ← fork from DB1
│  key_index.db   │     │  key_index.db   │  ← file copy
│  (any KV engine)│     │  (any KV engine)│
└────────┬────────┘     └────────┬────────┘
         │                       │
         │  key -> val_pos       │  key -> val_pos
         │                       │
         └───────────┬───────────┘
                     ▼
            ┌─────────────────┐
            │    val_log      │  ← shared jdb_val
            │ (key:db_id:ver) │
            │      -> val     │
            └─────────────────┘
```

### Key Concepts

- **Key-Value Separation**: Key index stored separately, values in shared val_log
- **val_log Key Format**: `user_key:db_id:version` → user_key first for range scan efficiency
- **Fork = File Copy**: Fork a database by copying the key_index file, sharing val_log
- **Independent Writes**: Each fork writes with its own db_id, no conflicts

### Fork Flow

```
1. Copy key_index.db file
2. Assign new db_id to forked DB
3. Open forked DB, share same val_log
4. Reads: use existing val_pos pointers (still valid)
5. Writes: new entries use new db_id in val_log key
```

### GC with Forks

To garbage collect val_log entries:

1. Build **db_id family tree** (parent → children relationships)
2. For each val_log entry with `db_id=X`:
   - Check if X and ALL descendants (forks of forks) are deleted
   - Only reclaim if entire family tree is gone
3. This ensures forked DBs can still read inherited values

```
         db_id=1 (original)
            │
      ┌─────┴─────┐
      ▼           ▼
   db_id=2     db_id=3
      │
      ▼
   db_id=4

To GC db_id=1 entries: must ensure 1,2,3,4 all deleted
```

### Benefits

- **Space Efficient**: Forks share unchanged values
- **Fast Fork**: Only copy small key_index, not large values
- **Independent**: Each fork can read/write independently
- **Consistent GC**: Family tree tracking ensures no dangling references

## Tech Stack

- **compio**: Single-threaded async I/O
- **zerocopy**: Zero-copy serialization
- **crc32fast**: SIMD-accelerated checksums
- **hashlink**: LRU cache implementation
- **fast32**: Base32 encoding for file names
- **memchr**: SIMD-accelerated magic byte search
- **coarsetime**: Fast timestamp for ID generation
