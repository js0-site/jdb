# JDB 开发计划 / Development Roadmap

基于 compio 的单线程异步 CoW B+ Tree + KV 分离存储引擎

---

## Phase 1: 核心存储 / Core Storage ✅

### 1.1 jdb_page - 页存储 ✅

物理页管理，Direct I/O 读写，CRC32 校验。

### 1.2 jdb_vlog - Value Log ✅

KV 分离的值存储层，支持 tombstone 和历史链。

### 1.3 jdb_tree - CoW B+ Tree ✅

核心索引结构，前缀压缩，CoW 路径复制。

---

## Phase 2: 数据库实现 / Database Implementation

### 2.1 jdb_db - 数据库 ✅

整合 B+ Tree 和 VLog，实现 put/get/rm/scan/history。

### 2.2 jdb_core - 实现 Jdb trait ✅

实现 `jdb_trait::Jdb`，多子库管理。

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

### 3.1 jdb_wal - Write-Ahead Log ✅

崩溃恢复保障。

### 3.2 jdb_gc - 垃圾回收 ✅

Page GC + VLog GC。

### 3.3 jdb_bench - 性能测试

```
对比: RocksDB, fjall 3.0.0-rc.6, jdb_core
```

---

## 里程碑 / Milestones

| 里程碑 | 目标 | 状态 |
|--------|------|------|
| M1 | jdb_page + jdb_vlog + jdb_tree | ✅ 完成 |
| M2 | jdb_db 实现 | ✅ 完成 |
| M3 | jdb_core (Jdb trait) | ✅ 完成 |
| M4 | 生产就绪 (WAL + GC) | ✅ 完成 |

---

## 当前状态 / Current Status

✅ 已完成:
- jdb_alloc: 对齐内存分配
- jdb_fs: 异步 Direct I/O
- jdb_trait: 接口定义 (含 ValRef 历史链)
- jdb_page: 页存储 (CRC32 校验)
- jdb_vlog: 值日志 (tombstone + prev 指针)
- jdb_tree: CoW B+ Tree (前缀压缩)
- jdb_db: 数据库 (put/get/rm/scan/history)
- jdb_core: 多子库管理 (Rc 单线程架构)
- jdb_wal: 预写日志 (崩溃恢复)
- jdb_gc: 垃圾回收 (Page GC + VLog GC)

🚧 下一步:
- jdb_bench: 性能测试 (对比 RocksDB, fjall)
