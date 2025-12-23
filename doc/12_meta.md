# jdb_meta - 元数据模块

定位: Schema 模板持久化，子表元数据索引，支持亿级子表。

## 依赖

```toml
jdb_trait = { path = "../jdb_trait" }
jdb_comm = { path = "../jdb_comm" }
jdb_fs = { path = "../jdb_fs" }
jdb_layout = { path = "../jdb_layout" }
jdb_filter = { path = "../jdb_filter" }
bitcode = "0.6"
```

## 模块结构

```
jdb_meta/src/
├── lib.rs        # 模块导出
├── schema.rs     # Schema 持久化
├── sub_meta.rs   # 子表元数据
├── router.rs     # 子表路由
└── filter.rs     # 子表过滤器
```

## 设计背景

### 亿级子表挑战

| 子表数量 | 元数据大小 | 内存占用 |
|---------|-----------|---------|
| 1M      | 100MB     | 200MB   |
| 100M    | 10GB      | 2GB (摘要) |
| 1B      | 100GB     | 20GB (摘要) |

### 解决方案

1. **三层元数据**: L0 内存 → L1 缓存 → L2 磁盘
2. **稀疏索引**: 按分区索引，不索引每个子表
3. **Binary Fuse Filter**: 快速判定子表是否存在

## 核心类型

### SchemaStore

```rust
pub struct SchemaStore {
  dir: PathBuf,
  schemas: Vec<Schema>,
}

impl SchemaStore {
  pub async fn open(dir: &Path) -> R<Self>;
  pub fn current(&self) -> &Schema;
  pub fn get(&self, ver: Ver) -> Option<&Schema>;
  pub async fn add(&mut self, schema: Schema) -> R<()>;
}
```

### SubMeta

```rust
pub struct SubMeta {
  pub key_hash: u64,
  pub key: SubTableKey,
  pub create_time: u64,
  pub update_time: u64,
  pub row_count: u64,
  pub data_size: u64,
  pub dir_offset: u64,
}
```

### SubMetaIndex

```rust
pub struct SubMetaIndex {
  dir: PathBuf,
  partitions: u32,          // 分区数 (1024)
  counts: Vec<u64>,
  filter: Option<Filter>,
  cache: LruCache<u64, SubMeta>,
}

impl SubMetaIndex {
  pub async fn open(dir: &Path, cache_size: usize) -> R<Self>;
  pub fn exists(&self, key_hash: u64) -> bool;
  pub async fn get(&mut self, key_hash: u64) -> R<Option<SubMeta>>;
  pub async fn add(&mut self, meta: SubMeta) -> R<()>;
  pub async fn remove(&mut self, key_hash: u64) -> R<()>;
}
```

### SubRouter

```rust
pub struct SubRouter {
  index: SubMetaIndex,
}

impl SubRouter {
  #[inline]
  pub fn partition(&self, key_hash: u64) -> u32 {
    (key_hash % self.index.partitions as u64) as u32
  }

  pub fn sub_dir(&self, key_hash: u64) -> PathBuf;

  #[inline]
  pub fn may_exist(&self, key_hash: u64) -> bool {
    self.index.exists(key_hash)
  }
}
```

## 设计要点

1. **分区存储**: 1024 分区，每分区独立文件
2. **Binary Fuse Filter**: 快速排除不存在的子表
3. **LRU 缓存**: 热子表元数据缓存
4. **增量更新**: 支持增量添加/删除子表
