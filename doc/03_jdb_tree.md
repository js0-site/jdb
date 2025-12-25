# jdb_tree 模块设计 / Module Design

CoW B+ Tree with prefix compression / 前缀压缩的 CoW B+ 树

## 数据结构 / Data Structures

### Node Types / 节点类型

```rust
/// 内部节点 / Internal Node
pub struct Internal {
  pub keys: Vec<Bytes>,      // 分隔键
  pub children: Vec<PageId>, // 子节点页 ID
}

/// 叶子节点 (前缀压缩) / Leaf Node (prefix compressed)
pub struct Leaf {
  pub prefix: Bytes,         // 公共前缀
  pub suffixes: Vec<Bytes>,  // 后缀列表
  pub vals: Vec<ValRef>,     // 值引用
  pub prev: PageId,          // 前驱叶子
  pub next: PageId,          // 后继叶子
}

/// B+ Tree
pub struct BTree {
  store: PageStore,
  root: PageId,
}
```

## 序列化格式 / Serialization Format

### Internal Node

```
┌──────────┬──────────┬──────────────────────────────┐
│ type (1) │ count(2) │ reserved (1)                 │
├──────────┴──────────┴──────────────────────────────┤
│ key_len(2) + key(var) ... (count times)            │
├────────────────────────────────────────────────────┤
│ child(8) ... (count+1 times)                       │
└────────────────────────────────────────────────────┘
```

### Leaf Node

```
┌──────────┬──────────┬──────────────────────────────┐
│ type (1) │ count(2) │ reserved (1)                 │
├──────────┴──────────┴──────────────────────────────┤
│ prefix_len(2) + prefix(var)                        │
├────────────────────────────────────────────────────┤
│ suffix_len(2) + suffix(var) + ValRef(32) ... ×count│
├────────────────────────────────────────────────────┤
│ prev(8) + next(8)                                  │
└────────────────────────────────────────────────────┘
```

## 前缀压缩 / Prefix Compression

叶子节点内 keys 提取公共前缀，减少存储空间。

### 示例 / Example

```
压缩前: ["user:1001", "user:1002", "user:1003"]  = 27 bytes
压缩后: prefix="user:100", suffixes=["1","2","3"] = 11 bytes
```

### 实现 / Implementation

- `insert`: 恢复所有完整 key → 插入 → 重算前缀
- `delete`: 恢复所有完整 key → 删除 → 重算前缀
- `split`: 恢复所有完整 key → 分裂 → 各自重算前缀
- `find`: 检查前缀匹配 → 二分查找后缀

## CoW 机制 / Copy-on-Write

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

## API

```rust
impl BTree {
  /// 创建新树 / Create new tree
  pub async fn new(store: PageStore) -> Result<Self>;

  /// 打开已有树 / Open existing tree
  pub fn open(store: PageStore, root: PageId) -> Self;

  /// 获取 / Get
  pub async fn get(&self, key: &[u8]) -> Result<Option<ValRef>>;

  /// 插入 (CoW) / Insert with CoW
  pub async fn put(&mut self, key: &[u8], val: ValRef) -> Result<PageId>;

  /// 删除 (CoW) / Delete with CoW
  pub async fn del(&mut self, key: &[u8]) -> Result<(PageId, Option<ValRef>)>;

  /// 查找叶子 / Find leaf
  pub async fn find_leaf(&self, key: &[u8]) -> Result<(PageId, Leaf)>;

  /// 获取根 / Get root
  pub fn root(&self) -> PageId;

  /// 同步 / Sync
  pub async fn sync(&self) -> Result<()>;
}
```

## 依赖 / Dependencies

- jdb_page: 页存储
- jdb_alloc: 对齐缓冲区
- jdb_trait: ValRef 类型
