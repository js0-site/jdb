# jdb_tree 开发计划

## 目标 / Goal

CoW B+ Tree 索引结构，支持路径复制、结构共享、前缀压缩。

---

## 数据结构 / Data Structures

```rust
/// 节点类型 / Node Type
pub enum Node {
    Internal(Internal),
    Leaf(Leaf),
}

/// 内部节点 / Internal Node
pub struct Internal {
    pub keys: Vec<Bytes>,      // 分隔键
    pub children: Vec<PageId>, // 子节点
}

/// 叶子节点 (前缀压缩) / Leaf Node (prefix compressed)
pub struct Leaf {
    pub prefix: Bytes,         // 公共前缀
    pub suffixes: Vec<Bytes>,  // 后缀列表
    pub vals: Vec<ValRef>,     // 值引用 (KV 分离)
    pub prev: Option<PageId>,  // 前驱叶子
    pub next: Option<PageId>,  // 后继叶子
}

/// B+ Tree
pub struct BTree {
    root: PageId,
    store: PageStore,
}
```

---

## 接口设计 / API Design

```rust
impl BTree {
    /// 打开 / Open
    pub async fn open(store: PageStore, root: PageId) -> Result<Self>;
    
    /// 查找 / Get
    pub async fn get(&self, key: &[u8]) -> Result<Option<ValRef>>;
    
    /// 插入 (CoW) / Insert with CoW
    pub async fn put(&mut self, key: &[u8], val: ValRef) -> Result<PageId>;
    
    /// 删除 (CoW) / Delete with CoW
    pub async fn del(&mut self, key: &[u8]) -> Result<(PageId, Option<ValRef>)>;
    
    /// 范围扫描 / Range scan
    pub fn scan(&self, start: &[u8], order: Order) -> impl Stream<Item = (Bytes, ValRef)>;
    
    /// 获取新根 / Get new root
    pub fn root(&self) -> PageId;
}
```

---

## 前缀压缩 / Prefix Compression

叶子节点内 keys 提取公共前缀，减少存储空间。

### 压缩示例

```
压缩前: ["user:1001", "user:1002", "user:1003"]  = 27 bytes
压缩后: prefix="user:100", suffixes=["1","2","3"] = 11 bytes
```

### Leaf 方法

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

### 序列化格式

```
┌────────────────┬────────────────┬────────────────┐
│ prefix_len (2) │ prefix (var)   │ count (2)      │
├────────────────┼────────────────┼────────────────┤
│ suffix_len (2) │ suffix (var)   │ ValRef (16)    │
├────────────────┴────────────────┴────────────────┤
│ ... repeat count times ...                       │
├────────────────┬────────────────┬────────────────┤
│ prev (8)       │ next (8)       │                │
└────────────────┴────────────────┴────────────────┘
```

---

## CoW 实现 / CoW Implementation

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

## 实现步骤 / Implementation Steps

### Step 1: 节点结构

- [ ] `Internal` 结构
- [ ] `Leaf` 结构 (含前缀压缩)
- [ ] 序列化/反序列化

### Step 2: 前缀压缩

- [ ] `Leaf::key()` 恢复完整 key
- [ ] `Leaf::find()` 前缀检查 + 二分查找
- [ ] `Leaf::recompute_prefix()` 计算 LCP
- [ ] `Leaf::insert()` 插入并重算

### Step 3: 查找

- [ ] `BTree::get()` 实现
- [ ] 从根到叶遍历
- [ ] 节点加载

### Step 4: 插入 (CoW)

- [ ] `BTree::put()` 实现
- [ ] 路径复制
- [ ] `Leaf::split()` 节点分裂
- [ ] 返回新根

### Step 5: 删除 (CoW)

- [ ] `BTree::del()` 实现
- [ ] 路径复制
- [ ] 节点合并/借用

### Step 6: 扫描

- [ ] `BTree::scan()` 实现
- [ ] 叶子链遍历 (prev/next)
- [ ] 支持 Asc/Desc

---

## 测试用例 / Test Cases

- [ ] 基础 CRUD
- [ ] 前缀压缩验证
- [ ] 大量插入 (分裂测试)
- [ ] 大量删除 (合并测试)
- [ ] 范围扫描 (Asc/Desc)
- [ ] CoW 验证 (旧根仍可访问)

---

## 依赖 / Dependencies

- jdb_page
- jdb_alloc

---

## 预计时间 / Estimated Time

2-3 周
