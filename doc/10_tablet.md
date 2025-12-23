# jdb_tablet 模块设计 / Module Design

## 概述 / Overview

`jdb_tablet` 是 L4 内核层的核心模块，实现 VNode 存储单元。组合 WAL、BTree、Vlog、TagIndex 成为一个原子存储实体。

`jdb_tablet` is the core module of L4 kernel layer, implementing VNode storage unit. Combines WAL, BTree, Vlog, TagIndex into an atomic storage entity.

## 依赖 / Dependencies

```
jdb_tablet
├── jdb_comm    (types, errors)
├── jdb_layout  (WalEntry)
├── jdb_wal     (WAL writer/reader)
├── jdb_vlog    (blob storage)
├── jdb_index   (B+ tree)
└── jdb_tag     (inverted index)
```

## 核心结构 / Core Structures

### Tablet

```rust
pub struct Tablet {
  vnode: VNodeID,       // VNode 标识 / VNode identifier
  dir: PathBuf,         // 数据目录 / Data directory
  wal: Option<WalWriter>,   // 预写日志 / Write-ahead log
  vlog: Option<VlogWriter>, // 大对象存储 / Blob storage
  index: BTree,         // B+ 树索引 / B+ tree index
  tags: TagIndex,       // 标签索引 / Tag index
}
```

## 核心接口 / Core APIs

### 生命周期 / Lifecycle

```rust
// 创建新 tablet / Create new tablet
pub async fn create(dir: impl AsRef<Path>, vnode: VNodeID) -> JdbResult<Self>

// 打开已有 tablet（含 WAL 恢复）/ Open existing tablet (with WAL recovery)
pub async fn open(dir: impl AsRef<Path>, vnode: VNodeID) -> JdbResult<Self>
```

### 读写操作 / Read/Write Operations

```rust
// 写入键值（默认不 fsync，与 RocksDB 一致）
// Put key-value (default no fsync, same as RocksDB)
pub async fn put(&mut self, table: TableID, key: Vec<u8>, val: Vec<u8>) -> JdbResult<()>

// 写入键值并同步（强制 fsync）
// Put key-value with sync (force fsync)
pub async fn put_sync(&mut self, table: TableID, key: Vec<u8>, val: Vec<u8>) -> JdbResult<()>

// 读取值 / Get value
pub fn get(&self, key: &[u8]) -> Option<Vec<u8>>

// 删除键（默认不 fsync）/ Delete key (default no fsync)
pub async fn delete(&mut self, table: TableID, key: &[u8]) -> JdbResult<bool>

// 删除键并同步 / Delete key with sync
pub async fn delete_sync(&mut self, table: TableID, key: &[u8]) -> JdbResult<bool>

// 范围扫描 / Range scan
pub fn range(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)>
```

### 标签操作 / Tag Operations

```rust
// 添加标签 / Add tag
pub fn add_tag(&mut self, id: u32, key: &[u8], val: &[u8])

// 标签查询（AND）/ Query by tags (AND)
pub fn query_tags(&self, tags: &[(&[u8], &[u8])]) -> Vec<u32>
```

### 持久化 / Persistence

```rust
// 刷新 WAL 和 Vlog / Flush WAL and Vlog
pub async fn flush(&mut self) -> JdbResult<()>
```

## 落盘策略 / Sync Policy

与 RocksDB 默认行为保持一致：

| 操作 | 默认行为 | 说明 |
|------|---------|------|
| `put()` | 写入 OS page cache，不 fsync | 高性能，依赖 OS 刷盘 |
| `put_sync()` | 写入后立即 fsync | 强一致，每次写入都持久化 |
| `delete()` | 写入 OS page cache，不 fsync | 高性能 |
| `delete_sync()` | 写入后立即 fsync | 强一致 |
| `flush()` | 强制 fsync WAL 和 Vlog | 手动触发持久化 |

**RocksDB 对比 / RocksDB Comparison:**

| RocksDB | JDB | 说明 |
|---------|-----|------|
| `WriteOptions.sync = false` (默认) | `put()` | 不等待 fsync |
| `WriteOptions.sync = true` | `put_sync()` | 每次写入 fsync |
| `DB::FlushWAL()` | `flush()` | 手动刷盘 |

**注意 / Note:**
- 默认模式下，进程崩溃可能丢失最近未刷盘的写入
- 如需强一致性，使用 `put_sync()` 或定期调用 `flush()`

## 写入路径 / Write Path

```
put(table, key, val)           put_sync(table, key, val)
    │                              │
    ├─► WAL.append()               ├─► WAL.append_sync()
    │   (buffer only)              │   (buffer + fsync)
    │                              │
    └─► BTree.insert()             └─► BTree.insert()
```

## 恢复流程 / Recovery Flow

```
open(dir, vnode)
    │
    ├─► WalReader.open()
    │
    ├─► 遍历 WAL 条目 / Iterate WAL entries
    │   ├─► Put  → BTree.insert()
    │   └─► Delete → BTree.delete()
    │
    └─► 打开 WAL/Vlog writer / Open WAL/Vlog writers
```

## 设计约束 / Design Constraints

1. **!Send + !Sync**: Tablet 只能在创建线程使用 / Tablet can only be used in creation thread
2. **WAL 优先**: 所有写操作先写 WAL / All writes go to WAL first
3. **二进制键值**: key/val 支持任意二进制 / key/val support arbitrary binary
4. **默认不 fsync**: 与 RocksDB 一致，追求高性能 / Default no fsync, same as RocksDB for performance

## 文件布局 / File Layout

```
{tablet_dir}/
├── wal.log     # 预写日志 / Write-ahead log
└── vlog.dat    # 大对象存储 / Blob storage
```

## 测试覆盖 / Test Coverage

- `test_tablet_put_get`: 基本读写 / Basic read/write
- `test_tablet_delete`: 删除操作 / Delete operation
- `test_tablet_range`: 范围扫描 / Range scan
- `test_tablet_recovery`: WAL 恢复 / WAL recovery
- `test_tablet_tags`: 标签索引 / Tag index
