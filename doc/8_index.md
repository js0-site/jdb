# jdb_index - B+ 树索引模块

定位: 面向磁盘的 B+ 树实现。

## 依赖

```toml
jdb_comm = { path = "../jdb_comm" }
jdb_layout = { path = "../jdb_layout" }
jdb_page = { path = "../jdb_page" }
```

## 模块结构

```
jdb_index/src/
├── lib.rs    # 模块导出
├── node.rs   # B+ 树节点
└── btree.rs  # B+ 树实现
```

## 核心类型

### Node

```rust
pub struct Node {
    pub header: PageHeader,
    pub keys: Vec<Vec<u8>>,
    pub children: Vec<PageID>,  // Internal node
    pub values: Vec<Vec<u8>>,   // Leaf node
    pub next: Option<PageID>,   // Leaf: next leaf pointer
}

impl Node {
    pub fn leaf(id: PageID) -> Self;
    pub fn internal(id: PageID) -> Self;
    pub fn is_leaf(&self) -> bool;
    pub fn is_full(&self) -> bool;
    pub fn find_key(&self, key: &[u8]) -> Result<usize, usize>;
    pub fn insert_leaf(&mut self, key: Vec<u8>, val: Vec<u8>);
    pub fn get_leaf(&self, key: &[u8]) -> Option<&[u8]>;
    pub fn delete_leaf(&mut self, key: &[u8]) -> bool;
    pub fn find_child(&self, key: &[u8]) -> PageID;
    pub fn serialize(&self) -> Vec<u8>;
    pub fn deserialize(buf: &[u8]) -> Self;
}
```

### BTree

```rust
pub struct BTree {
    root: Option<PageID>,
    next_page: u32,
    nodes: HashMap<u32, Node>,
}

impl BTree {
    pub fn new() -> Self;
    pub fn insert(&mut self, key: Vec<u8>, val: Vec<u8>);
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>>;
    pub fn delete(&mut self, key: &[u8]) -> bool;
    pub fn range(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)>;
}
```

## 设计要点

- 二进制键：支持任意 `&[u8]` 作为键
- 叶子链表：叶子节点通过 next 指针链接，支持高效范围扫描
- 分裂策略：节点满时分裂，向上传播
- 序列化：支持持久化到页面
