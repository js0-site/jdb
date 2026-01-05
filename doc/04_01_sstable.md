# SSTable with PGM-index Design
# 使用 PGM 索引的 SSTable 设计

## Overview / 概述

SSTable (Sorted String Table) is the on-disk storage format for LSM-Tree.
This implementation uses PGM-index (Piecewise Geometric Model) to replace traditional sparse block index for faster lookups.

SSTable（有序字符串表）是 LSM-Tree 的磁盘存储格式。
本实现使用 PGM 索引替换传统稀疏块索引，实现更快的查找。

## File Layout / 文件布局

```
┌─────────────────────────────────────────┐
│           Data Blocks                    │
│  ┌─────────────────────────────────┐    │
│  │ Block 0 (prefix compressed)     │    │
│  └─────────────────────────────────┘    │
│  ┌─────────────────────────────────┐    │
│  │ Block 1 ...                     │    │
│  └─────────────────────────────────┘    │
├─────────────────────────────────────────┤
│           Filter Block                   │
│  BinaryFuse8 filter (bitcode encoded)   │
├─────────────────────────────────────────┤
│           Offset Array                   │
│  Vec<u64> block offsets (bitcode)       │
├─────────────────────────────────────────┤
│           PGM Index                      │
│  PGMIndex<u64> (bitcode encoded)        │
├─────────────────────────────────────────┤
│           Footer (44 bytes)              │
│  filter_offset: u64                     │
│  filter_size: u32                       │
│  offsets_offset: u64                    │
│  offsets_size: u32                      │
│  pgm_offset: u64                        │
│  pgm_size: u32                          │
│  block_count: u32                       │
│  checksum: u32 (CRC32)                  │
└─────────────────────────────────────────┘
```

## Key Components / 核心组件

### 1. Footer (44 bytes)

```rust
#[repr(C, packed)]
pub struct Footer {
    filter_offset: u64,   // BinaryFuse8 filter offset
    filter_size: u32,     // BinaryFuse8 filter size
    offsets_offset: u64,  // Block offset array position
    offsets_size: u32,    // Block offset array size
    pgm_offset: u64,      // PGM index offset
    pgm_size: u32,        // PGM index size
    block_count: u32,     // Number of data blocks
    checksum: u32,        // CRC32 checksum
}
```

### 2. PGM Index

- Uses `jdb_pgm` crate (local workspace)
- Epsilon: 32 (error bound for prediction)
- Key prefix: first 8 bytes converted to u64 (big-endian)
- Predicts block_id from key prefix

```rust
// Key to u64 conversion
// 键转 u64 转换
pub fn key_to_u64(key: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    let len = key.len().min(8);
    buf[..len].copy_from_slice(&key[..len]);
    u64::from_be_bytes(buf)
}
```

### 3. BinaryFuse8 Filter

- Uses `jdb_xorf` crate for bloom filtering
- Hash function: gxhash64
- No false negatives guaranteed

## Lookup Flow / 查找流程

```
1. Check filter: may_contain(key)
   检查过滤器：may_contain(key)
   
2. Convert key to u64 prefix
   将键转换为 u64 前缀
   
3. PGM prediction: pgm.get(prefix) -> approx_block_id
   PGM 预测：pgm.get(prefix) -> 近似块ID
   
4. Refine with epsilon range: [approx - ε, approx + ε]
   使用误差范围精确定位：[approx - ε, approx + ε]
   
5. Binary search in first_keys array
   在首键数组中二分查找
   
6. Read block at offsets[block_id]
   读取 offsets[block_id] 处的块
   
7. Linear scan within block
   块内线性扫描
```

## Writer Flow / 写入流程

```
1. Add key-value pairs (must be sorted)
   添加键值对（必须有序）
   
2. Flush block when size >= 4KB
   块大小 >= 4KB 时刷新
   
3. Record first key prefix for each block
   记录每个块的首键前缀
   
4. Build BinaryFuse8 filter from key hashes
   从键哈希构建 BinaryFuse8 过滤器
   
5. Build PGM index from prefixes
   从前缀构建 PGM 索引
   
6. Write: blocks -> filter -> offsets -> pgm -> footer
   写入：块 -> 过滤器 -> 偏移数组 -> PGM -> 尾部
```

## Error Types / 错误类型

```rust
pub enum Error {
    SstTooSmall { size: u64 },           // File too small
    InvalidFooter,                        // Footer parse failed
    ChecksumMismatch { expected, actual }, // CRC32 mismatch
    InvalidFilter,                        // Filter decode failed
    InvalidOffsets,                       // Offsets decode failed
    InvalidBlock { offset: u64 },         // Block parse failed
    FilterBuildFailed,                    // Filter build failed
}
```

## Performance Characteristics / 性能特点

| Operation | Complexity | Notes |
|-----------|------------|-------|
| Point lookup | O(log n) | PGM + block scan |
| Range scan | O(k) | k = result count |
| Write | O(1) amortized | Per key-value |
| Filter check | O(1) | BinaryFuse8 |

## Memory Usage / 内存使用

- Footer: 44 bytes (fixed)
- PGM index: ~few KB (depends on block count)
- Offset array: 8 bytes per block
- Filter: ~9 bits per key
- First keys: loaded on open for range refinement

## Files / 文件

- `jdb/src/sstable/mod.rs` - Module exports, key_to_u64
- `jdb/src/sstable/footer.rs` - Footer struct and builder
- `jdb/src/sstable/writer.rs` - SSTable writer
- `jdb/src/sstable/reader.rs` - SSTable reader and iterator
- `jdb/src/sstable/meta.rs` - TableMeta struct
