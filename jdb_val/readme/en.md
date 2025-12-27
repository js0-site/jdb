# jdb_val - WAL Value Storage

- [Introduction](#introduction)
- [Features](#features)
- [Usage](#usage)
- [WAL File Format](#wal-file-format)
- [Configuration](#configuration)
- [File Rotation](#file-rotation)
- [Storage Modes](#storage-modes)
- [API Overview](#api-overview)
- [Tech Stack](#tech-stack)

## Introduction

`jdb_val` is a high-performance WAL (Write-Ahead Log) value storage library for embedded key-value databases. It provides efficient storage with automatic mode selection based on data size, LRU caching, and file rotation.

## Features

- **Magic Byte Validation**: 8-byte header with magic `JDB`, version, and CRC32
- **Flexible Configuration**: RocksDB-style configuration via `Conf` enum
- **Auto Mode Selection**: Automatically chooses optimal storage mode (inline/infile/file) based on data size
- **LRU Caching**: Head, data, and file handle caches for read performance
- **File Rotation**: Automatic rotation when file size exceeds limit
- **CRC32 Verification**: Data integrity check for non-inline values
- **Async I/O**: Built on compio for efficient single-threaded async operations

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
  let key: Vec<u8> = wal.get_key(&head).await?;
  let val: Vec<u8> = wal.get_val(&head).await?;

  Ok(())
}
```

### Call Flow

```
put(key, val) → Pos
                 ↓
read_head(loc) → Head
                 ↓
get_key(&head) → Vec<u8>
get_val(&head) → Vec<u8>
```

- **Pos**: Position in WAL (file_id + offset), 16 bytes, store this in your index
- **Head**: 64-byte metadata containing flags, lengths, inline data or pointers
- **get_key/get_val**: Reads actual data based on storage mode (inline/infile/file)

Directory structure after `Wal::new("data", &[])`:
```
data/
├── wal/    # WAL files
└── bin/    # Large value files (>64KB)
```

## WAL File Format

```
+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
| Version (4B)                      | Version Copy (4B)                 | CRC32 (4B)                        |
| u32 little-endian                 | same as first                     | checksum of [0..4]                |
+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
| Head 1 (64B)    | Data 1 (var)    | Head 2 (64B)    | ...                                                 |
+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
```

- **Version** (4 bytes): Format version (currently 1), stored twice for redundancy
- **CRC32** (4 bytes): Checksum of first 4 bytes (first version)
- **Head** (64 bytes): Fixed-size metadata header
- **Data** (variable): Infile data follows its head

### Header Validation & Repair

On open, files are validated (12-byte header):
1. File < 12 bytes → skip
2. Version1 + CRC32 valid → use Version1 to fix Version2 if needed
3. Version2 + CRC32 valid → use Version2 to fix Version1
4. Version1 == Version2 but CRC mismatch → recalculate CRC32
5. All checks fail → cannot repair, skip

## Configuration

```rust
use jdb_val::{Conf, Wal};

let mut wal = Wal::new("data", &[
  Conf::MaxSize(512 * 1024 * 1024),  // 512MB
  Conf::HeadCacheCap(16384),
]);
```

### Default Values

| Config | Default | Description |
|--------|---------|-------------|
| `MaxSize` | 256MB | Max WAL file size before rotation |
| `HeadCacheCap` | 8192 | LRU cache capacity for heads |
| `DataCacheCap` | 1024 | LRU cache capacity for infile data |
| `FileCacheCap` | 64 | LRU cache capacity for file handles |

Default values reference RocksDB configurations.

## File Rotation

WAL files rotate automatically when:

```
cur_pos + data_len > max_size
```

Rotation triggers:
1. `write_head()`: When head write would exceed limit
2. `write_data()`: When infile data write would exceed limit

On rotation:
1. Increment file ID
2. Create new file with 8-byte header
3. Reset position to 8 (after header)

## Storage Modes

| Mode | Condition | Storage |
|------|-----------|---------|
| INLINE | key+val ≤ 50B | Embedded in Head |
| INFILE | data ≤ 64KB | Same WAL file |
| FILE | data > 64KB | Separate file |

Mode selection is automatic based on key/value sizes.

## API Overview

### Core Types

```rust
// Position in WAL file (16 bytes) - store this in your index
pub struct Pos {
  bin_id: u64,  // WAL file ID
  offset: u64,  // Byte offset in file
}

// Metadata header (64 bytes) - contains flags, lengths, inline data or pointers
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
| `read_head(loc)` | `Pos` | `Result<Head>` | Read metadata at position |
| `get_key(&head)` | `&Head` | `Result<Vec<u8>>` | Get key data |
| `get_val(&head)` | `&Head` | `Result<Vec<u8>>` | Get value data (CRC checked) |
| `scan(id, f)` | file_id, callback | `Result<()>` | Iterate all entries |
| `sync_data()` | - | `Result<()>` | Flush data to disk |
| `sync_all()` | - | `Result<()>` | Flush data + metadata |

### Conf

```rust
pub enum Conf {
  MaxSize(u64),       // Max file size before rotation
  HeadCacheCap(usize),// Head LRU cache size
  DataCacheCap(usize),// Infile data cache size
  FileCacheCap(usize),// File handle cache size
}
```

## GC / Garbage Collection

Uses `Gc` trait callback to check if key is deleted.

```rust
use jdb_val::{Gc, GcState, PosMap, Wal};

struct MyChecker { /* your index */ }

impl Gc for MyChecker {
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
  let mut state = GcState::new("data");
  
  // Auto GC: randomly pick oldest unGC'd file, continue if reclaim > 25%
  wal.gc_auto(checker, &mut state).await.unwrap();
}
```

### GC Strategy

Redis-like expiration strategy:
1. Randomly pick oldest unGC'd file (from oldest 25%)
2. Execute GC, record time to `gc.log`
3. Continue if reclaim ratio > threshold (default 25%)
4. Max 16 iterations

### GC Flow

1. Scan old WAL file, collect all Head entries
2. Filter: use `Gc::is_rm()` to check if key is deleted
3. Rewrite live entries to current active WAL via `put()`
4. Call `Gc::batch_update()` to update index
5. Delete old WAL file after update success

Note: Data is appended to current WAL, not creating a replacement file.

## Tech Stack

- **compio**: Single-threaded async I/O
- **zerocopy**: Zero-copy serialization
- **crc32fast**: SIMD-accelerated checksums
- **hashlink**: LRU cache implementation
- **fast32**: Base32 encoding for file names
