# jdb_val - WAL 值存储

- [项目简介](#项目简介)
- [核心特性](#核心特性)
- [使用示例](#使用示例)
- [WAL 文件格式](#wal-文件格式)
- [配置参数](#配置参数)
- [文件轮转](#文件轮转)
- [存储模式](#存储模式)
- [API 概览](#api-概览)
- [GC 垃圾回收](#gc-垃圾回收)
- [Fork 数据库设计](#fork-数据库设计)
- [技术栈](#技术栈)

## 项目简介

`jdb_val` 是一个高性能的 WAL（预写日志）值存储库，用于嵌入式键值数据库。支持根据数据大小自动选择存储模式、LRU 缓存和文件轮转。

## 核心特性

- **文件头校验**：12 字节文件头，版本冗余存储 + CRC32
- **灵活配置**：RocksDB 风格的 `Conf` 枚举配置
- **自动模式选择**：根据数据大小自动选择最优存储模式（内联/文件内/独立文件）
- **LRU 缓存**：头部、数据、文件句柄三级缓存提升读取性能
- **文件轮转**：文件大小超限时自动轮转
- **CRC32 校验**：独立文件存储的值进行完整性校验
- **异步 I/O**：基于 compio 的高效单线程异步操作
- **流式 API**：支持大值的流式读写

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
  let key: Vec<u8> = wal.head_key(&head).await?;
  let val: Vec<u8> = wal.head_val(&head).await?;

  Ok(())
}
```

### 调用流程

```
put(key, val) -> Pos
                 |
read_head(loc) -> Head
                 |
head_key(&head) -> Vec<u8>
head_val(&head) -> Vec<u8>
```

- **Pos**：WAL 中的位置（wal_id + 偏移量），16 字节，存入你的索引
- **Head**：64 字节元数据，包含标志、长度、内联数据或指针
- **head_key/head_val**：根据存储模式（内联/文件内/独立文件）读取实际数据

`Wal::new("data", &[])` 后的目录结构：
```
data/
├── wal/    # WAL 文件（base32 编码 ID）
└── bin/    # 大值文件 (>1MB)
```

## WAL 文件格式

### 文件头（12 字节）

```
+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
| Version (4B)                      | Version Copy (4B)                 | CRC32 (4B)                        |
| u32 小端序                         | 与第一个相同                        | 前4字节的校验和                      |
+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
```

### 条目格式

```
+------------------+------------------+------------------+
| Head (64B)       | Infile Data (变长)| End Marker (12B) |
+------------------+------------------+------------------+
```

- **Head** (64 字节)：固定大小的元数据头，含 CRC32
- **Infile Data** (变长)：同文件存储的 key/value 数据
- **End Marker** (12 字节)：`[head_offset: u64 小端] [magic: 0xEDEDEDED]`

### 文件头校验与修复

打开时校验文件：
1. 文件 < 12 字节 -> 跳过
2. Version1 + CRC32 正确 -> 用 Version1 修复 Version2
3. Version2 + CRC32 正确 -> 用 Version2 修复 Version1
4. Version1 == Version2 但 CRC 不匹配 -> 重算 CRC32
5. 全部失败 -> 回退到魔数扫描恢复

## 配置参数

```rust
use jdb_val::{Conf, Wal};

let mut wal = Wal::new("data", &[
  Conf::MaxSize(512 * 1024 * 1024),  // 512MB
  Conf::HeadLru(16384),
]);
```

### 默认值

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `MaxSize` | 256MB | 轮转前的最大文件大小 |
| `HeadLru` | 8192 | Head LRU 缓存容量 |
| `DataLru` | 1024 | 文件内数据 LRU 缓存容量 |
| `FileLru` | 64 | 文件句柄 LRU 缓存容量 |

## 文件轮转

WAL 文件在以下条件时自动轮转：

```
cur_pos + data_len > max_size
```

轮转流程：
1. 生成新文件 ID（基于时间戳）
2. 创建新文件并写入 12 字节头
3. 重置位置为 12（文件头之后）

## 存储模式

| 模式 | 条件 | 存储位置 |
|------|------|----------|
| INLINE | key+val <= 50B | 内嵌在 Head 中 |
| INFILE | data <= 1MB | 同一 WAL 文件 |
| FILE | data > 1MB | 独立 bin 文件 |

模式根据 key/value 大小自动选择。

### Head 数据区布局（50 字节）

| 布局 | 条件 | 结构 |
|------|------|------|
| INLINE+INLINE | key+val <= 50B | `[key][val]` |
| INLINE+FILE | key <= 30B | `[key(30B)][val_pos(16B)][val_crc(4B)]` |
| FILE+INLINE | val <= 34B | `[key_pos(16B)][val(34B)]` |
| FILE+FILE | 都很大 | `[key_pos(16B)][val_pos(16B)][..][val_crc(4B)]` |

## API 概览

### 核心类型

```rust
// WAL 文件中的位置（16 字节）- 存入你的索引
pub struct Pos {
  wal_id: u64,  // WAL 文件 ID
  offset: u64,  // 文件内字节偏移
}

// 元数据头（64 字节）
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
| `put_stream(key, iter)` | key, chunk 迭代器 | `Result<Pos>` | 流式写入大值 |
| `read_head(loc)` | `Pos` | `Result<Head>` | 读取指定位置的元数据 |
| `head_key(&head)` | `&Head` | `Result<Vec<u8>>` | 获取 key 数据 |
| `head_val(&head)` | `&Head` | `Result<Vec<u8>>` | 获取 value 数据（FILE 模式含 CRC 校验） |
| `head_key_stream(&head)` | `&Head` | `Result<DataStream>` | 流式读取 key |
| `head_val_stream(&head)` | `&Head` | `Result<DataStream>` | 流式读取 value |
| `scan(id, f)` | 文件ID, 回调 | `Result<()>` | 遍历所有条目 |
| `iter_entries(id)` | 文件ID | `Result<LogIter>` | 获取条目迭代器 |
| `sync_data()` | - | `Result<()>` | 刷数据到磁盘 |
| `sync_all()` | - | `Result<()>` | 刷数据+元数据 |

## GC 垃圾回收

使用 `Gcable` trait 回调判断 key 是否已删除。

```rust
use jdb_val::{Gcable, GcState, PosMap, Wal};

struct MyChecker { /* 你的索引 */ }

impl Gcable for MyChecker {
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
  
  // 合并指定的 WAL 文件
  let ids = vec![1, 2, 3]; // 要合并的 WAL 文件 ID
  wal.gc_merge(&ids, checker, &mut state).await.unwrap();
}
```

### GC 流程

1. 获取目标 WAL 文件的文件锁
2. 扫描条目，通过 `Gcable::is_rm()` 过滤已删除的 key
3. 将有效条目重写到新 WAL
4. 调用 `Gcable::batch_update()` 更新索引
5. 成功后删除旧 WAL 文件

## Fork 数据库设计

`jdb_val` 设计支持**写时复制（COW）的 Fork 数据库**，共享值存储。

### 架构图

```
+------------------+     +------------------+
|  DB1 (db_id=1)   |     |  DB2 (db_id=2)   |  <- fork from DB1
|  key_index.db    |     |  key_index.db    |  <- 文件复制
|  (任意 KV 引擎)   |     |  (任意 KV 引擎)   |
+---------+--------+     +---------+--------+
          |                        |
          |  key -> val_pos        |  key -> val_pos
          |                        |
          +------------+-----------+
                       |
              +--------v--------+
              |    val_log      |  <- 共享的 jdb_val
              | (key:db_id:ver) |
              |     -> val      |
              +-----------------+
```

### 核心概念

- **Key-Value 分离**：Key 索引独立存储，Value 存入共享的 val_log
- **val_log Key 格式**：`user_key:db_id:version` -> user_key 在前便于范围扫描
- **Fork = 文件复制**：Fork 数据库只需复制 key_index 文件，共享 val_log
- **独立写入**：每个 fork 用自己的 db_id 写入，互不冲突

### Fork 流程

```
1. 复制 key_index.db 文件
2. 为 fork 的 DB 分配新 db_id
3. 打开 fork 的 DB，共享同一个 val_log
4. 读取：使用现有的 val_pos 指针（仍然有效）
5. 写入：新条目在 val_log key 中使用新 db_id
```

### Fork 场景下的 GC

回收 val_log 条目时：

1. 构建 **db_id 家族树**（父 -> 子关系）
2. 对于 val_log 中 `db_id=X` 的条目：
   - 检查 X 及其所有后代（fork 的 fork）是否都已删除
   - 只有整个家族树都消失才能回收
3. 这确保 fork 的 DB 仍能读取继承的值

```
         db_id=1 (原始)
            |
      +-----+-----+
      v           v
   db_id=2     db_id=3
      |
      v
   db_id=4

要 GC db_id=1 的条目：必须确保 1,2,3,4 都已删除
```

### 优势

- **空间高效**：Fork 共享未修改的值
- **Fork 快速**：只复制小的 key_index，不复制大的值
- **相互独立**：每个 fork 可以独立读写
- **GC 一致性**：家族树追踪确保无悬空引用

## 技术栈

- **compio**：单线程异步 I/O
- **zerocopy**：零拷贝序列化
- **crc32fast**：SIMD 加速校验
- **hashlink**：LRU 缓存实现
- **fast32**：Base32 编码文件名
- **memchr**：SIMD 加速魔数搜索
- **coarsetime**：快速时间戳用于 ID 生成
