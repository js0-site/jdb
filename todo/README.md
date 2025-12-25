# JDB 开发计划 / Development Roadmap

基于 compio 的单线程异步 CoW B+ Tree + KV 分离存储引擎

---

## Phase 1: 核心存储 / Core Storage

### 1.1 jdb_page - 页存储 (1-2 周)

物理页管理，Direct I/O 读写。

```
依赖: jdb_alloc, jdb_fs
```

### 1.2 jdb_vlog - Value Log (1 周)

KV 分离的值存储层，实现 `ValRef` 读写。

```
核心接口:
- append(key, val) -> ValRef
- get(ValRef) -> Bytes

依赖: jdb_fs
```

### 1.3 jdb_tree - CoW B+ Tree (2-3 周)

核心索引结构，Key -> ValRef 映射。

```
依赖: jdb_page
```

---

## Phase 2: 数据库实现 / Database Implementation

### 2.1 jdb_db - 实现 Db trait (2 周)

实现 `jdb_trait::Db`。

```rust
trait Db {
  fn put(key, val) -> Option<Prev>;   // 写入，返回旧值引用
  fn get(key) -> Option<Bytes>;       // 读取
  fn rm(key);                         // 删除
  fn val(ValRef) -> Option<Bytes>;    // 根据引用读值
  fn scan(key, order) -> Stream<Kv>;  // 范围扫描
  fn history(key) -> Stream<ValRef>;  // 历史版本
  fn fork(ts, order) -> Option<Db>;   // 时间旅行 Fork
  fn last_ts() -> Sec;                // 最后提交时间戳
  fn pipeline() -> Pipeline;          // 批量操作
}

依赖: jdb_tree, jdb_vlog
```

### 2.2 jdb_core - 实现 Jdb trait (1 周)

实现 `jdb_trait::Jdb`。

```rust
trait Jdb {
  fn open(conf) -> Jdb;               // 打开数据库
  fn db(id, conf) -> Db;              // 获取/创建子库
  fn fork(id) -> Option<Db>;          // Fork 子库
  fn scan(start, order) -> Stream<Db>; // 遍历子库
}

依赖: jdb_db
```

---

## Phase 3: 生产就绪 / Production Ready

### 3.1 jdb_wal - Write-Ahead Log (1 周)

崩溃恢复保障。

### 3.2 jdb_gc - 垃圾回收 (1 周)

Page GC + VLog GC。

### 3.3 jdb_bench - 性能测试

```
对比: RocksDB, fjall 3.0.0-rc.6, jdb_core
```

---

## 里程碑 / Milestones

| 里程碑 | 目标 | 预计时间 |
|--------|------|----------|
| M1 | jdb_page + jdb_tree 可用 | 4 周 |
| M2 | Db trait 实现 | 6 周 |
| M3 | Jdb trait 实现 | 7 周 |
| M4 | 生产就绪 (WAL + GC) | 9 周 |

---

## 当前状态 / Current Status

✅ 已完成:
- jdb_alloc: 对齐内存分配
- jdb_fs: 异步 Direct I/O
- jdb_trait: 接口定义

🚧 下一步:
- jdb_page: 页存储实现
