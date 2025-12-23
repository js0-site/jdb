# jdb_index - B+ 树索引模块

定位: 面向磁盘的 B+ 树实现，集成 Binary Fuse Filter。

## 依赖

```toml
jdb_trait = { path = "../jdb_trait" }
jdb_layout = { path = "../jdb_layout" }
jdb_page = { path = "../jdb_page" }
jdb_filter = { path = "../jdb_filter" }
```

## 模块结构

```
jdb_index/src/
├── lib.rs      # 模块导出
├── node.rs     # B+ 树节点
├── btree.rs    # B+ 树实现
├── cursor.rs   # 游标迭代器
└── filter.rs   # 过滤器集成
```

## 设计背景

### 为什么选择 B+ 树？

| 方案 | 读性能 | 写性能 | 适用场景 |
|------|--------|--------|----------|
| B+ Tree | ⭐⭐⭐ | ⭐⭐ | 读多写少，点查 |
| LSM Tree | ⭐⭐ | ⭐⭐⭐ | 写多读少 |

NVMe 随机读性能好，B+ 树单次 IO 定位，延迟可预测。

## 核心类型

### IndexKey

```rust
pub type IndexKey = Vec<Val>;
pub fn cmp_key(a: &[Val], b: &[Val]) -> Ordering;
```

### Node

```rust
pub struct Node {
  page_id: u32,
  is_leaf: bool,
  keys: Vec<IndexKey>,
  children: Vec<u32>,   // Internal: 子页 ID
  row_ids: Vec<u64>,    // Leaf: 行 ID
  next: Option<u32>,    // Leaf: 下一叶子
}
```

### BTree

```rust
pub struct BTree {
  pool: BufferPool,
  root: Option<u32>,
  filter: Option<Filter>,
  unique: bool,
  fanout: usize,
}

impl BTree {
  pub async fn create(pool: BufferPool, unique: bool) -> R<Self>;
  pub async fn insert(&mut self, key: IndexKey, row_id: u64) -> R<()>;
  pub async fn get(&mut self, key: &[Val]) -> R<Option<u64>>;
  pub async fn range(&mut self, start: Option<&[Val]>, end: Option<&[Val]>, order: Order) -> R<Cursor>;
  pub async fn delete(&mut self, key: &[Val], row_id: u64) -> R<bool>;
  pub async fn rebuild_filter(&mut self) -> R<()>;
}
```

### Cursor

```rust
pub struct Cursor<'a> {
  btree: &'a mut BTree,
  current_page: u32,
  current_idx: usize,
  end_key: Option<IndexKey>,
  order: Order,
}

impl<'a> Cursor<'a> {
  pub async fn next(&mut self) -> R<Option<(IndexKey, u64)>>;
  pub async fn next_batch(&mut self, limit: usize) -> R<Vec<(IndexKey, u64)>>;
}
```

## 查询流程

```
get(key)
  ├─► Filter.may_contain(hash(key))
  │     └─► false → return None
  ├─► 从 root 向下查找
  └─► 到达叶子节点 → 二分查找 key
```

## 设计要点

1. **磁盘存储**: 索引存储在磁盘，按需加载
2. **Binary Fuse Filter**: 快速排除不存在的 key
3. **唯一约束**: 支持唯一索引
4. **多列索引**: 支持多列组合索引
