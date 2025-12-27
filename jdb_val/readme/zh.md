# jdb_val - WAL 值存储

- [项目简介](#项目简介)
- [核心特性](#核心特性)
- [使用示例](#使用示例)
- [WAL 文件格式](#wal-文件格式)
- [配置参数](#配置参数)
- [文件轮转](#文件轮转)
- [存储模式](#存储模式)
- [API 概览](#api-概览)
- [技术栈](#技术栈)

## 项目简介

`jdb_val` 是一个高性能的 WAL（预写日志）值存储库，用于嵌入式键值数据库。支持根据数据大小自动选择存储模式、LRU 缓存和文件轮转。

## 核心特性

- **魔法数字校验**：8 字节文件头，包含 `JDB` 魔法数字、版本号和 CRC32
- **灵活配置**：RocksDB 风格的 `Conf` 枚举配置
- **自动模式选择**：根据数据大小自动选择最优存储模式（内联/文件内/独立文件）
- **LRU 缓存**：头部、数据、文件句柄三级缓存提升读取性能
- **文件轮转**：文件大小超限时自动轮转
- **CRC32 校验**：非内联值的数据完整性校验
- **异步 I/O**：基于 compio 的高效单线程异步操作

## 使用示例

```rust
use jdb_val::{Wal, Pos, Head};

#[compio::main]
async fn main() -> jdb_val::Result<()> {
  let mut wal = Wal::new("data", &[]);
  wal.open().await?;

  // 1. 写入：put() 返回 Pos（文件ID + 偏移量）
  let loc: Pos = wal.put(b"key", b"value").await?;

  // 2. 读取头：用 Pos 获取 Head（64字节元数据）
  let head: Head = wal.read_head(loc).await?;

  // 3. 获取数据：用 Head 获取 key/value
  let key: Vec<u8> = wal.get_key(&head).await?;
  let val: Vec<u8> = wal.get_val(&head).await?;

  Ok(())
}
```

### 调用流程

```
put(key, val) → Pos
                 ↓
read_head(loc) → Head
                 ↓
get_key(&head) → Vec<u8>
get_val(&head) → Vec<u8>
```

- **Pos**：WAL 中的位置（文件ID + 偏移量），16 字节，存入你的索引
- **Head**：64 字节元数据，包含标志、长度、内联数据或指针
- **get_key/get_val**：根据存储模式（内联/文件内/独立文件）读取实际数据

`Wal::new("data", &[])` 后的目录结构：
```
data/
├── wal/    # WAL 文件
└── bin/    # 大值文件 (>64KB)
```

## WAL 文件格式

```
+--------+--------+--------+--------+--------+--------+--------+--------+
| Magic (3B)      | Ver(1B)| CRC32 (4B)                                 |
| 0x4A 0x44 0x42  | 0x01   | 前4字节的校验和                              |
+--------+--------+--------+--------+--------+--------+--------+--------+
| Head 1 (64B)    | Data 1 (变长)   | Head 2 (64B)    | ...             |
+--------+--------+--------+--------+--------+--------+--------+--------+
```

- **Magic** (3 字节)：`JDB` (0x4A 0x44 0x42) - 标识 WAL 格式
- **Version** (1 字节)：格式版本（当前 0x01）
- **CRC32** (4 字节)：前 4 字节（magic + version）的校验和
- **Head** (64 字节)：固定大小的元数据头
- **Data** (变长)：文件内数据紧跟其 head

### 文件头校验与修复

打开时校验文件：
1. 文件 < 8 字节 → 跳过
2. Magic 损坏但 CRC32 正确 → 用 CRC32 验证后恢复 Magic
3. Magic 正确但 CRC32 损坏 → 重算 CRC32 修复
4. Magic 和 CRC32 都损坏 → 无法修复，跳过

修复原理：假设 Magic 应为 `JDB`，用 `[JDB, version]` 重算 CRC32，若与存储的 CRC32 匹配，则 Magic 可恢复。

## 配置参数

```rust
use jdb_val::{Conf, Wal};

let mut wal = Wal::new("data", &[
  Conf::MaxSize(512 * 1024 * 1024),  // 512MB
  Conf::HeadCacheCap(16384),
]);
```

### 默认值

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `MaxSize` | 256MB | 轮转前的最大文件大小 |
| `HeadCacheCap` | 8192 | Head LRU 缓存容量 |
| `DataCacheCap` | 1024 | 文件内数据 LRU 缓存容量 |
| `FileCacheCap` | 64 | 文件句柄 LRU 缓存容量 |

默认值参考 RocksDB 配置。

## 文件轮转

WAL 文件在以下条件时自动轮转：

```
cur_pos + data_len > max_size
```

触发轮转的操作：
1. `write_head()`：写入 head 将超限时
2. `write_data()`：写入文件内数据将超限时

轮转流程：
1. 递增文件 ID
2. 创建新文件并写入 8 字节头
3. 重置位置为 8（文件头之后）

## 存储模式

| 模式 | 条件 | 存储位置 |
|------|------|----------|
| INLINE | key+val ≤ 50B | 内嵌在 Head 中 |
| INFILE | data ≤ 64KB | 同一 WAL 文件 |
| FILE | data > 64KB | 独立文件 |

模式根据 key/value 大小自动选择。

## API 概览

### 核心类型

```rust
// WAL 文件中的位置（16 字节）- 存入你的索引
pub struct Pos {
  bin_id: u64,  // WAL 文件 ID
  offset: u64,  // 文件内字节偏移
}

// 元数据头（64 字节）- 包含标志、长度、内联数据或指针
pub struct Head {
  key_len: u32,
  val_len: u32,
  key_flag: Flag,  // INLINE / INFILE / FILE
  val_flag: Flag,
  data: [u8; 50],  // 内联数据或 Pos 指针
  head_crc32: u32,
}
```

### Wal 方法

| 方法 | 输入 | 输出 | 说明 |
|------|------|------|------|
| `new(dir, conf)` | 路径, 配置 | `Wal` | 创建 WAL 管理器 |
| `open()` | - | `Result<()>` | 打开/创建 WAL 文件 |
| `put(key, val)` | `&[u8], &[u8]` | `Result<Pos>` | 写入 KV，返回位置 |
| `read_head(loc)` | `Pos` | `Result<Head>` | 读取指定位置的元数据 |
| `get_key(&head)` | `&Head` | `Result<Vec<u8>>` | 获取 key 数据 |
| `get_val(&head)` | `&Head` | `Result<Vec<u8>>` | 获取 value 数据（含 CRC 校验） |
| `scan(id, f)` | 文件ID, 回调 | `Result<()>` | 遍历所有条目 |
| `sync_data()` | - | `Result<()>` | 刷数据到磁盘 |
| `sync_all()` | - | `Result<()>` | 刷数据+元数据 |

### Conf

```rust
pub enum Conf {
  MaxSize(u64),       // 轮转前最大文件大小
  HeadCacheCap(usize),// Head LRU 缓存大小
  DataCacheCap(usize),// 文件内数据缓存大小
  FileCacheCap(usize),// 文件句柄缓存大小
}
```

## GC / 垃圾回收

使用 `Gc` trait 回调判断 key 是否已删除。

```rust
use jdb_val::{Gc, GcState, PosMap, Wal};

struct MyChecker { /* 你的索引 */ }

impl Gc for MyChecker {
  async fn is_rm(&self, key: &[u8]) -> bool {
    // 查询索引判断 key 是否已删除
    false
  }
  
  async fn batch_update(&self, mapping: impl IntoIterator<Item = PosMap>) -> bool {
    for m in mapping {
      // 更新索引: m.key, m.old -> m.new
    }
    true
  }
}

async fn do_gc(wal: &mut Wal, checker: &MyChecker) {
  let mut state = GcState::new("data");
  
  // 自动 GC：随机选最久未 GC 的文件，回收率超 25% 继续
  wal.gc_auto(checker, &mut state).await.unwrap();
}
```

### GC 策略

类 Redis 过期删除策略：
1. 随机选最久未 GC 的文件（从最旧 25% 中随机）
2. 执行 GC，记录时间到 `gc.log`
3. 回收率 > 阈值（默认 25%）则继续
4. 最多迭代 16 次

### GC 流程

1. 扫描旧 WAL 文件，收集所有 Head 条目
2. 过滤：用 `Gc::is_rm()` 检查 key 是否已删除
3. 将有效条目通过 `put()` 重写到当前活跃 WAL
4. 调用 `Gc::batch_update()` 更新索引
5. 更新成功后删除旧 WAL 文件

注意：数据追加到当前 WAL，不是创建替换文件。

## 技术栈

- **compio**：单线程异步 I/O
- **zerocopy**：零拷贝序列化
- **crc32fast**：SIMD 加速校验
- **hashlink**：LRU 缓存实现
- **fast32**：Base32 编码文件名
