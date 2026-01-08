# jdb_level - LSM-tree Level Management
# jdb_level - LSM-tree 层级管理

## Overview 概述

LSM-tree level management with dynamic level bytes calculation (RocksDB style).
带动态层级字节计算的 LSM-tree 层级管理（RocksDB 风格）。

## Core Components 核心组件

### 1. Level (`level.rs`)

Single level with PGM index for fast table lookup.
带 PGM 索引的单层，用于快速表查找。

- L0: sorted by table id (tables may overlap)
- L1+: sorted by min_key (tables are disjoint), PGM-accelerated lookup

Features:
- `find_table`: O(1) fast path for out-of-range keys, PGM lookup for L1+
- `overlapping`: binary search for L1+, linear scan for L0
- `overlapping_size`: calculate total size of overlapping tables (for grandparent check)
- `no_overlap`: check if table can trivial move

### 2. Levels (`levels.rs`)

Multi-level manager with dynamic level bytes.
带动态层级字节的多层管理器。

- Dynamic base_level calculation (RocksDB style)
- Compaction score based prioritization
- Trivial move with grandparent overlap check

### 3. Calc (`calc.rs`)

Dynamic level limits calculation.
动态层级限制计算。

- `calc`: calculate level limits based on total size
- `score`: compaction score (score > SCORE_SCALE means needs compaction)
- `needs_compact`: check if level needs compaction
- `target_level`: get target level for compaction

### 4. Conf (`conf.rs`)

Configuration for level manager.
层级管理器配置。

| Config | Default | Description |
|--------|---------|-------------|
| L0Limit | 4 | L0 file count threshold |
| BaseMb | 256 | Base level size (MB) |
| Ratio | 10 | Size ratio between levels |
| GpLimitMb | 640 | Max overlap with grandparent (MB) |

## Key Optimizations 关键优化

### 1. Grandparent Overlap Check 祖父层重叠检查

Prevents write amplification cascade by limiting overlap with grandparent level during trivial move.
通过限制 trivial move 时与祖父层的重叠来防止写放大雪崩。

```
L(n) -> L(n+1): check overlap with L(n+2) <= gp_limit
```

### 2. Fast Path for Point Lookup 点查快速路径

L1+ `find_table` checks global bounds before PGM/binary search.
L1+ 的 `find_table` 在 PGM/二分查找前先检查全局边界。

```rust
if key < first.min_key() || key > last.max_key() {
  return None; // O(1)
}
```

### 3. SCORE_SCALE Constant 评分缩放常量

Extracted magic number 100 to `SCORE_SCALE` for maintainability.
将魔术数字 100 提取为 `SCORE_SCALE` 常量以提高可维护性。

## Future Improvements 未来改进

1. **Compaction Picker**: Return specific files instead of just level ID
2. **Round Robin**: Track last compaction key to prevent file starvation
3. **Compensated Size**: Prioritize files with many tombstones
4. **Intra-L0 Compaction**: Compact within L0 when L1 is busy
