# jdb_db 模块设计 / Module Design

数据库实现，整合 B+ Tree 和 VLog / Database implementation integrating B+ Tree and VLog

## 数据结构 / Data Structures

```rust
/// 提交记录 / Commit record
pub struct Commit {
  pub rev: Rev,       // 修订号
  pub root: PageId,   // B+ Tree 根页
}

/// 数据库 / Database
pub struct Db {
  tree: BTree,        // B+ Tree 索引
  vlog: Rc<VLog>,     // 值日志 (单线程共享)
  commit: Commit,     // 当前提交
}
```

## API

```rust
impl Db {
  /// 打开或创建 / Open or create
  pub async fn open(dir: impl AsRef<Path>) -> Result<Self>;

  /// 写入 / Put
  pub async fn put(&mut self, key, val) -> Result<Option<ValRef>>;

  /// 读取 / Get
  pub async fn get(&self, key) -> Result<Option<Bytes>>;

  /// 删除 / Remove (tombstone)
  pub async fn rm(&mut self, key) -> Result<()>;

  /// 根据引用读值 / Get value by ref
  pub async fn val(&self, vref: ValRef) -> Result<Option<Bytes>>;

  /// 历史版本 / History
  pub async fn history(&self, key) -> Result<Vec<ValRef>>;

  /// 范围扫描 / Scan
  pub async fn scan(&self, start, order) -> Result<Vec<(Bytes, Bytes)>>;

  /// 同步 / Sync
  pub async fn sync(&self) -> Result<()>;
}
```

## 写入流程 / Write Flow

```
put(key, val):
1. tree.get(key) -> old_ref
2. vlog.append(key, val, old_ref) -> new_ref
3. tree.put(key, new_ref) -> new_root (CoW)
4. commit.rev++, commit.root = new_root
5. return old_ref
```

## 删除流程 / Delete Flow

```
rm(key):
1. tree.get(key) -> old_ref
2. vlog.append_tombstone(key, old_ref) -> tomb_ref
3. tree.put(key, tomb_ref) -> new_root (CoW)
4. commit.rev++, commit.root = new_root
```

## 历史链 / History Chain

通过 ValRef 的 prev 指针遍历：

```
v3 (current) -> v2 -> v1 (no prev)
     │              │
     └─ prev_file_id/prev_offset
```

## 目录结构 / Directory Layout

```
db_dir/
├── pages.jdb    # B+ Tree 页存储
└── vlog/
    ├── 00000001.vlog
    └── ...
```

## 依赖 / Dependencies

- jdb_tree: CoW B+ Tree
- jdb_vlog: Value Log
- jdb_page: Page Store
- jdb_trait: ValRef, Order
