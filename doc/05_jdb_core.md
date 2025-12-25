# jdb_core - 多子库管理 / Multi-Database Management

## 概述 / Overview

jdb_core 是顶层数据库管理器，管理多个子数据库，共享 VLog。

## 架构 / Architecture

```
┌─────────────────────────────────────────┐
│                 Jdb                     │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐   │
│  │  Db 1   │ │  Db 2   │ │  Db N   │   │
│  │ (BTree) │ │ (BTree) │ │ (BTree) │   │
│  └────┬────┘ └────┬────┘ └────┬────┘   │
│       │           │           │         │
│       └───────────┼───────────┘         │
│                   ▼                     │
│           ┌─────────────┐               │
│           │ Shared VLog │               │
│           │  (Rc<VLog>) │               │
│           └─────────────┘               │
└─────────────────────────────────────────┘
```

## 单线程设计 / Single-Threaded Design

- 使用 `Rc<VLog>` 共享 VLog（非 Arc）
- VLog 内部用 `RefCell` 管理状态（非 tokio::Mutex）
- 适用于 compio 单线程异步运行时

## 核心结构 / Core Structures

```rust
pub struct Jdb {
  dir: PathBuf,
  vlog: Rc<VLog>,
  dbs: BTreeMap<DbId, DbMeta>,
  next_id: DbId,
}

struct DbMeta {
  root: u64,  // B+ Tree root page
  rev: u64,   // revision
}
```

## API

| 方法 | 说明 |
|------|------|
| `open(dir)` | 打开或创建 Jdb |
| `db(id)` | 获取或创建子库 |
| `fork(id)` | Fork 子库（CoW 复制） |
| `scan(start, order)` | 遍历子库 ID |
| `commit_db(id, db)` | 提交子库变更 |
| `sync()` | 同步 VLog |

## 文件布局 / File Layout

```
<dir>/
├── vlog/           # 共享值日志
│   ├── 00000001.vlog
│   └── ...
├── db_00000001.jdb # 子库 1 页文件
├── db_00000002.jdb # 子库 2 页文件
└── meta.jdb        # 元数据 (TODO)
```

## Fork 机制 / Fork Mechanism

1. 复制源库页文件
2. 分配新 DbId
3. 共享同一 VLog
4. CoW 语义：修改 fork 不影响原库

## 依赖 / Dependencies

- jdb_db: 单库实现
- jdb_vlog: 值日志
- jdb_page: 页存储
- jdb_tree: B+ Tree
