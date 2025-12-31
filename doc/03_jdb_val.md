# jdb_val 设计文档
# jdb_val Design Document

## 概述 / Overview

`jdb_val` 是一个高性能 WAL 值存储库，专为 KV 分离架构设计。支持异步 IO、LRU 缓存、LZ4 压缩和 GC。

## 核心类型 / Core Types

| Type | Size | Description |
|------|------|-------------|
| `Wal` | - | WAL 管理器（带缓存） |
| `Pos` | 24B | 值位置（INFILE/FILE 模式） |
| `Flag` | 1B | 存储标志 |
| `Val` | - | 缓存数据 `Rc<[u8]>` |

## 配置 / Configuration

```rust
pub enum Conf {
  MaxSize(u64),      // WAL 文件最大大小
  CacheSize(u64),    // 缓存大小
}
```

## 核心接口 / Core API

### 初始化 / Initialize

```rust
let mut wal = Wal::new("./data", &[Conf::CacheSize(64 * 1024 * 1024)]);
wal.open().await?;
```

### 写入 / Write

```rust
let pos = wal.put(key, val).await?;   // 写入 KV
let pos = wal.del(key).await?;        // 删除
wal.sync_all().await?;                // 同步
```

### 读取 / Read

```rust
let data: Val = wal.val(pos).await?;  // 按位置读取
let data = wal.val_cached(&pos);      // 缓存读取（无 IO）
```

### GC

```rust
let mut state = GcState::new(&dir);
let mut gc = DefaultGc;
let index = MyIndex::new();

let (reclaimed, total) = wal.gc(&ids, &checker, &mut state, &mut gc, &index).await?;
```

## GC Traits

```rust
// 检查 key 是否已删除
// Check if key is deleted
pub trait Gcable {
  fn is_rm(&self, key: &[u8]) -> impl Future<Output = bool>;
}

// 批量更新索引
// Batch update index
pub trait IndexUpdate: Send + Sync {
  fn update(&self, mapping: &[PosMap]);
}

// GC 压缩处理
// GC compression processing
pub trait GcTrait {
  fn process(&mut self, store: Flag, data: &[u8], buf: &mut Vec<u8>) -> (Flag, Option<usize>);
}

// 位置映射
// Position mapping
pub struct PosMap {
  pub key: HipByt<'static>,
  pub new: Pos,
}
```

## Pos 结构 / Pos Structure

```rust
impl Pos {
  pub fn is_infile(&self) -> bool;  // 是否内联
  pub fn id(&self) -> u64;          // WAL 文件 ID
  pub fn len(&self) -> u32;         // 值长度
}
```

## Flag 值 / Flag Values

| Value | Name | Description |
|-------|------|-------------|
| 0 | INFILE | 同 WAL 文件，无压缩 |
| 1 | INFILE_LZ4 | LZ4 压缩 |
| 2 | INFILE_ZSTD | ZSTD 压缩 |
| 3 | INFILE_PROBED | 已探测不可压缩 |
| 4 | FILE | 独立文件 |
| 5 | FILE_LZ4 | 独立文件 + LZ4 |
| 6 | FILE_ZSTD | 独立文件 + ZSTD |
| 7 | FILE_PROBED | 已探测不可压缩 |
| 8 | TOMBSTONE | 删除标记 |

## 使用示例 / Usage Example

```rust
use jdb_val::{Conf, DefaultGc, GcState, Gcable, IndexUpdate, PosMap, Pos, Wal};
use gxhash::HashMap;
use hipstr::HipByt;
use std::future::Future;

// 实现 Gcable
// Implement Gcable
struct MyChecker { deleted: HashSet<Vec<u8>> }

impl Gcable for MyChecker {
  fn is_rm(&self, key: &[u8]) -> impl Future<Output = bool> {
    let found = self.deleted.contains(key);
    async move { found }
  }
}

// 实现 IndexUpdate
// Implement IndexUpdate
struct MyIndex { map: RwLock<HashMap<HipByt<'static>, Pos>> }

impl IndexUpdate for MyIndex {
  fn update(&self, mapping: &[PosMap]) {
    let mut map = self.map.write();
    for m in mapping {
      map.insert(m.key.clone(), m.new);
    }
  }
}

// 使用
// Usage
let mut wal = Wal::new("./data", &[Conf::CacheSize(64 << 20)]);
wal.open().await?;

// 写入
let pos = wal.put(b"key1", b"value1").await?;

// 读取
let data = wal.val(pos).await?;

// GC
let mut state = GcState::new("./data");
let mut gc = DefaultGc;
let checker = MyChecker::new();
let index = MyIndex::new();

let ids: Vec<_> = wal.iter().filter(|&id| id < wal.cur_id()).collect();
let (reclaimed, total) = wal.gc(&ids, &checker, &mut state, &mut gc, &index).await?;

wal.sync_all().await?;
```

## 限制 / Limits

| Item | Limit |
|------|-------|
| Key 最大 | 64 KB |
| INFILE 最大 | 4 MB |
