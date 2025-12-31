# jdb_val

本文档仅记录暴露给上层 KV 分离数据库的公开接口、结构体和 trait。

## 概述

高性能 WAL 值存储库，专为 KV 分离架构设计。支持异步 IO（compio）、LHD 缓存、LZ4 压缩和 GC。

## 类型

| 类型 | 大小 | 说明 |
|------|------|------|
| `Wal` | - | WAL 管理器 |
| `Pos` | 24B | 值位置 |
| `Record` | 16B | 记录位置 |
| `Flag` | 1B | 存储标志 |
| `Val` | - | 缓存值 `Rc<[u8]>` |
| `Conf` | - | 配置枚举 |
| `Error`, `Result` | - | 错误类型 |

## 常量

| 常量 | 值 | 说明 |
|------|-----|------|
| `KEY_MAX` | 64 KB | Key 最大长度 |
| `INFILE_MAX` | 4 MB | INFILE 模式最大值长度 |

## 配置

```rust
pub enum Conf {
  MaxSize(u64),     // WAL 文件最大大小（默认 512MB）
  CacheSize(u64),   // 缓存大小（默认 8MB）
  FileLru(usize),   // 文件句柄缓存容量
  WriteChan(usize), // 写入队列容量
  SlotMax(usize),   // 等待前的最大槽大小（默认 8MB）
}
```

## Traits

```rust
// GC 压缩处理
pub trait Gc: Default {
  fn process(&mut self, flag: Flag, data: &[u8], buf: &mut Vec<u8>) -> (Flag, Option<usize>);
}

// 检查 key 是否已删除
pub trait Gcable {
  fn is_rm(&self, key: &[u8]) -> impl Future<Output = bool> + Send;
}

// 批量更新索引
pub trait IndexUpdate: Send + Sync {
  fn update(&self, mapping: &[PosMap]);
}
```

## Wal 方法

```rust
// 创建与打开
fn new(dir: impl Into<PathBuf>, conf: &[Conf]) -> Self;
async fn open(&mut self) -> Result<()>;

// 写入
async fn put(&mut self, key: impl Bin, val: impl Bin) -> Result<Pos>;
async fn del(&mut self, key: impl Bin) -> Result<Pos>;

// 读取
async fn val(&mut self, pos: Pos) -> Result<Val>;
fn val_cached(&mut self, pos: &Pos) -> Option<Val>;

// 迭代
fn iter(&self) -> impl Iterator<Item = u64>;  // WAL 文件 ID
async fn scan<F>(&self, id: u64, f: F) -> Result<()>
  where F: FnMut(u64, &Head, &[u8]) -> bool;

// 同步
async fn flush(&mut self) -> Result<()>;
async fn sync_all(&mut self) -> Result<()>;
fn cur_id(&self) -> u64;

// GC
async fn gc<T, M>(&mut self, ids: &[u64], checker: &T, index: &M) -> Result<(usize, usize)>
where T: Gcable, M: IndexUpdate;
```

## 示例

```rust
use jdb_val::{Conf, Gcable, IndexUpdate, PosMap, Wal};

async fn example() -> jdb_val::Result<()> {
  let mut wal = Wal::new("./data", &[Conf::CacheSize(64 << 20)]);
  wal.open().await?;

  // 写入
  let pos = wal.put(b"key", b"val").await?;

  // 读取
  let _data = wal.val(pos).await?;

  // GC
  let ids: Vec<_> = wal.iter().filter(|&id| id < wal.cur_id()).collect();
  wal.gc(&ids, &checker, &index).await?;

  wal.sync_all().await
}
```

## Flag 值

| 值 | 名称 | 说明 |
|----|------|------|
| 0 | Infile | 同 WAL 文件，无压缩 |
| 1 | InfileLz4 | LZ4 压缩 |
| 2 | InfileZstd | ZSTD 压缩 |
| 3 | InfileProbed | 已探测不可压缩 |
| 4 | File | 独立文件 |
| 5 | FileLz4 | 独立文件 + LZ4 |
| 6 | FileZstd | 独立文件 + ZSTD |
| 7 | FileProbed | 已探测不可压缩 |
| 8 | Tombstone | 删除标记 |
