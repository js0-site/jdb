# JDB 设计文档 / Design Document

基于 compio 的单线程异步 CoW B+ Tree + KV 分离存储引擎

---

## 1. 架构总览 / Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Jdb (顶层入口)                           │
│  open / db / fork / scan                                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     Db (子库)                                │
│  put / get / rm / val / scan / history / fork / pipeline    │
└─────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
┌─────────────────────────┐     ┌─────────────────────────────┐
│   CoW B+ Tree (索引)     │     │      VLog (值存储)          │
│   Key -> ValRef          │     │   ValRef -> Bytes           │
└─────────────────────────┘     └─────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────┐
│                   PageStore (物理存储)                       │
│                   Direct I/O + compio                        │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. 核心类型 / Core Types

```rust
// 排序方向 / Sort order
pub enum Order { Asc, Desc }

// 键值对 / Key-value pair
pub type Kv = (Bytes, Bytes);
pub type Sec = u64;   // 时间戳 (秒)
pub type DbId = u64;  // 子库 ID

// 值引用 (含前驱指针 + tombstone 标记)
// Value reference with prev pointer and tombstone flag
pub struct ValRef {
  pub file_id: u64,
  pub offset: u64,         // 最高位=1 表示 tombstone
  pub prev_file_id: u64,   // 0 = 无前驱
  pub prev_offset: u64,
}

impl ValRef {
  fn is_tombstone(&self) -> bool;  // offset 最高位
  fn real_offset(&self) -> u64;    // 去掉标记位
  fn has_prev(&self) -> bool;      // prev_file_id != 0
}

// Pipeline 操作结果 / Pipeline result
pub enum Rt {
  Get(Option<Bytes>),
  Put(Option<Prev>),  // Prev = ValRef
  Rm(bool),
}
```

ValRef 包含前驱指针，用于 `history()` 遍历同一 key 的历史版本链。
`offset` 最高位标记 tombstone，删除操作也写入 VLog 保持历史链完整。

---

## 3. Db trait 实现说明 / Db Implementation

### 3.1 put(key, val) -> Option<Prev>

写入键值对，返回旧值引用（如有）。

```
实现流程:
1. B+ Tree 查找 key -> 得到 old_ref: Option<ValRef>
2. val 追加写入 VLog (prev = old_ref) -> 得到 new_ref: ValRef
3. B+ Tree CoW 插入 (key, new_ref) -> 新 root
4. 创建新 Commit(ts, new_root, parent)
5. 返回 old_ref
```

### 3.2 get(key) -> Option<Bytes>

读取键对应的值。

```
实现流程:
1. B+ Tree 查找 key -> ValRef
2. 若无，返回 None
3. 若 ValRef.is_tombstone()，返回 None
4. VLog 读取 ValRef -> Bytes
5. 返回 Some(bytes)
```

### 3.3 rm(key)

删除键（写入 tombstone 保持历史链）。

```
实现流程:
1. B+ Tree 查找 key -> 得到 old_ref: Option<ValRef>
2. VLog 写入 tombstone (prev = old_ref) -> tomb_ref
3. B+ Tree CoW 更新 (key, tomb_ref) -> 新 root
4. 创建新 Commit(ts, new_root, parent)
```

### 3.4 val(ValRef) -> Option<Bytes>

根据值引用读取值（用于历史版本）。

```
实现流程:
1. VLog 读取 ValRef.file_id 文件的 ValRef.offset 位置
2. 解码记录，返回 value 部分
```

### 3.5 scan(key, order) -> Stream<Kv>

从 key 开始扫描。

```
实现流程:
1. B+ Tree 定位 key 所在叶子节点
2. 根据 order 决定遍历方向:
   - Asc: 向后遍历 (next 指针)
   - Desc: 向前遍历 (prev 指针)
3. 对每个 (key, ValRef)，读取 VLog 得到 value
4. yield (key, value)
```

### 3.6 history(key) -> Stream<ValRef>

获取 key 的所有历史版本。

```
实现流程:
1. B+ Tree 查找 key -> 得到当前 ValRef
2. 沿 ValRef.prev_file_id/prev_offset 链遍历
3. yield 每个 ValRef
4. 直到 prev_file_id == 0
```

### 3.7 fork(ts, order) -> Option<Db>

时间旅行，创建指向历史版本的 Db。

```
实现流程:
1. 从当前 Commit 开始遍历 Commit Chain
2. 根据 order:
   - Asc: 找到 commit.ts <= ts 的最新 Commit
   - Desc: 找到 commit.ts >= ts 的最早 Commit
3. 若找到，创建新 Db 指向该 Commit
4. 返回 Some(new_db) 或 None
```

### 3.8 last_ts() -> Sec

获取最后提交的时间戳。

```
实现流程:
1. 返回当前 Commit.ts
```

### 3.9 pipeline() -> Pipeline

创建批量操作管道。

```
Pipeline 方法:
- put(key, val): 添加写入操作
- get(key): 添加读取操作
- rm(key): 添加删除操作
- exec() -> Stream<Rt>: 执行所有操作，返回结果流

实现流程:
1. 收集所有操作到 Vec<Op>
2. exec() 时批量执行:
   - 所有 put 的 val 批量写入 VLog
   - B+ Tree 批量更新 (单次 CoW)
   - 创建一个 Commit
3. 按操作顺序 yield 结果
```

---

## 4. Jdb trait 实现说明 / Jdb Implementation

### 4.1 open(conf) -> Jdb

打开数据库。

```
实现流程:
1. 解析配置 (数据目录、页大小等)
2. 打开/创建 PageStore
3. 打开/创建 VLog
4. 加载 Meta (子库列表)
5. 返回 Jdb 实例
```

### 4.2 db(id, conf) -> Db

获取或创建子库。

```
实现流程:
1. 查找 Meta 中是否存在 id
2. 若存在，加载 Db (读取 Commit Head)
3. 若不存在，创建新 Db:
   - 创建空 B+ Tree (root = null)
   - 创建初始 Commit(ts=0, root, parent=null)
   - 更新 Meta
4. 返回 Db
```

### 4.3 fork(id) -> Option<Db>

Fork 子库（创建副本）。

```
实现流程:
1. 查找 id 对应的 Db
2. 若不存在，返回 None
3. 分配新 DbId
4. 创建新 Db，指向同一个 Commit Head
5. 更新 Meta
6. 返回 Some(new_db)
```

### 4.4 scan(start, order) -> Stream<Db>

遍历子库。

```
实现流程:
1. 从 Meta 中获取所有 DbId >= start
2. 根据 order 排序
3. 对每个 DbId，加载 Db
4. yield Db
```

---

## 5. 存储结构 / Storage Layout

### 5.1 目录结构

```
data/
├── meta.jdb       # 元数据 (子库列表)
├── pages.jdb      # B+ Tree 页存储
└── vlog/
    ├── 0001.vlog  # Value Log 文件
    ├── 0002.vlog
    └── ...
```

### 5.2 Commit 结构

```rust
struct Commit {
  ts: u64,              // 时间戳 (秒)
  root: PageId,         // B+ Tree 根页
  parent: Option<PageId>, // 父 Commit
}
```

### 5.3 B+ Tree 节点

```rust
// 内部节点 / Internal node
struct Internal {
  keys: Vec<Bytes>,
  children: Vec<PageId>,
}

// 叶子节点 (前缀压缩) / Leaf node (prefix compressed)
struct Leaf {
  prefix: Bytes,          // 公共前缀
  suffixes: Vec<Bytes>,   // 后缀列表
  vals: Vec<ValRef>,
  prev: Option<PageId>,
  next: Option<PageId>,
}
```

### 5.4 VLog 记录格式

```
┌──────────┬──────────┬───────┬──────────┬──────────┐
│ len (8B) │ crc (4B) │ flag  │ key_len  │ key      │
├──────────┴──────────┴───────┴──────────┴──────────┤
│ value (flag=0 时有，flag=1 tombstone 时无)         │
└──────────────────────────────────────────────────┘

flag: 0 = 正常值, 1 = tombstone
```

---

## 6. 前缀压缩 / Prefix Compression

叶子节点内的 keys 通常有共同前缀，提取公共前缀只存一次。

### 6.1 压缩示例

```
压缩前:
keys: ["user:1001", "user:1002", "user:1003"]
总字节: 9 * 3 = 27

压缩后:
prefix: "user:100"
suffixes: ["1", "2", "3"]
总字节: 8 + 3 = 11
```

### 6.2 LeafNode 实现

```rust
impl Leaf {
  // 恢复完整 key / Restore full key
  fn key(&self, idx: usize) -> Bytes;

  // 查找 (先检查前缀) / Find with prefix check
  fn find(&self, key: &[u8]) -> Option<ValRef>;

  // 插入并重算前缀 / Insert and recompute prefix
  fn insert(&mut self, key: &[u8], val: ValRef);

  // 分裂时各自重算前缀 / Split with prefix recompute
  fn split(&mut self) -> (Bytes, Leaf);

  // 计算 LCP / Compute longest common prefix
  fn recompute_prefix(&mut self);
}
```

### 6.3 序列化格式

```
┌────────────────┬────────────────┬────────────────┐
│ prefix_len (2) │ prefix (var)   │ count (2)      │
├────────────────┼────────────────┼────────────────┤
│ suffix_len (2) │ suffix (var)   │ ValRef (32)    │
├────────────────┴────────────────┴────────────────┤
│ ... repeat count times ...                       │
├────────────────┬────────────────┬────────────────┤
│ prev (8)       │ next (8)       │                │
└────────────────┴────────────────┴────────────────┘
```

---

## 7. CoW 机制 / Copy-on-Write

写入时只复制修改路径，未修改的子树共享。

```
插入 key=5:

Before:
        [3, 7]           <- root (Page 100)
       /   |   \
    [1,2] [4,6] [8,9]

After (CoW):
        [3, 7]'          <- new root (Page 200)
       /   |   \
    [1,2] [4,5,6]' [8,9] <- new leaf (Page 201)

旧页 100 保持不变，供历史版本访问
```

---

## 8. Fork 与时间旅行 / Fork & Time Travel

Fork 只是复制 Commit 指针，O(1) 操作。

```
fork(ts=T2, Asc):

Commit Chain:
  C4(T4) -> C3(T3) -> C2(T2) -> C1(T1)
                        ^
                        | fork 指向这里

新 Db 和原 Db 共享 C2 及之前的所有数据
后续写入各自产生新的 Commit 分支
```

---

## 9. 历史版本链 / History Chain

通过 ValRef 的 prev 指针遍历同一 key 的历史版本。

```
key="user:1" 的历史:

ValRef_v3 (当前)
  ├─ file_id: 2, offset: 1000
  └─ prev_file_id: 1, prev_offset: 500
        │
        ▼
ValRef_v2
  ├─ file_id: 1, offset: 500
  └─ prev_file_id: 1, prev_offset: 100
        │
        ▼
ValRef_v1
  ├─ file_id: 1, offset: 100
  └─ prev_file_id: 0 (无前驱)
```

`history(key)` 沿此链遍历，返回所有历史 ValRef。
