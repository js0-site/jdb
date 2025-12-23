# jdb_tag - 标签倒排索引模块

定位: 基于 RoaringBitmap 的标签倒排索引。

## 依赖

```toml
jdb_comm = { path = "../jdb_comm" }
roaring = "0.11"
```

## 模块结构

```
jdb_tag/src/
├── lib.rs    # 模块导出
└── index.rs  # 标签索引实现
```

## 核心类型

### TagIndex

```rust
pub struct TagIndex {
    index: HashMap<Vec<u8>, RoaringBitmap>,
}

impl TagIndex {
    pub fn new() -> Self;
    
    // 基本操作
    pub fn add(&mut self, id: u32, key: &[u8], val: &[u8]);
    pub fn remove(&mut self, id: u32, key: &[u8], val: &[u8]);
    pub fn get(&self, key: &[u8], val: &[u8]) -> Option<&RoaringBitmap>;
    pub fn count(&self, key: &[u8], val: &[u8]) -> u64;
    
    // 集合运算
    pub fn and(&self, tags: &[(&[u8], &[u8])]) -> RoaringBitmap;
    pub fn or(&self, tags: &[(&[u8], &[u8])]) -> RoaringBitmap;
    pub fn not(&self, base: &RoaringBitmap, key: &[u8], val: &[u8]) -> RoaringBitmap;
}
```

## 存储格式

标签键格式：`key:value`（二进制拼接）

## 设计要点

- RoaringBitmap：高效压缩位图，支持快速集合运算
- 二进制标签：key/value 支持任意 `&[u8]`
- 集合运算：AND/OR/NOT 毫秒级定位 ID 集合
- 后续可实现 LSM 持久化
