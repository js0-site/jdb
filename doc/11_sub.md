# jdb_sub - 子表模块

定位: SubTable 实现，组合 WAL/Index/Page/VLog，实现 MVCC。

## 依赖

```toml
jdb_trait = { path = "../jdb_trait" }
jdb_layout = { path = "../jdb_layout" }
jdb_wal = { path = "../jdb_wal" }
jdb_page = { path = "../jdb_page" }
jdb_vlog = { path = "../jdb_vlog" }
jdb_index = { path = "../jdb_index" }
parking_lot = "0.12"
```

## 模块结构

```
jdb_sub/src/
├── lib.rs       # 模块导出
├── sub.rs       # SubTable 实现
├── data.rs      # 数据页管理
├── mvcc.rs      # 版本管理
└── compact.rs   # Compaction
```

## 核心类型

### Sub

```rust
use jdb_trait::{SubTable, AsyncRow, Id, Order, Query, Row, SubTableKey};

pub struct Sub {
  dir: PathBuf,
  key: SubTableKey,
  schema: Schema,
  wal: WalWriter,
  data_pool: BufferPool,
  vlog_writer: VLogWriter,
  vlog_reader: VLogReader,
  indexes: Vec<BTree>,
  ver: u32,
}

impl Sub {
  pub async fn create(dir: impl AsRef<Path>, key: SubTableKey, schema: &Schema) -> R<Self>;
  pub async fn open(dir: impl AsRef<Path>, key: SubTableKey) -> R<Self>;
  pub async fn close(&mut self) -> R<()>;
}

impl SubTable for Sub {
  type Error = Err;
  type AsyncRow = AsyncRowImpl;
  type Stream = impl Stream<Item = Result<(Id, Self::AsyncRow), Self::Error>> + Send;

  async fn put(&self, data: &[Row]) -> Result<Vec<Id>, Self::Error>;
  
  async fn get(&self, id: Id) -> Result<Option<(Id, Self::AsyncRow)>, Self::Error>;
  
  async fn select(&self, q: &Query) -> Self::Stream;
  
  async fn scan(&self, begin_id: u64, order: Order) -> Self::Stream;
  
  async fn history(&self, id: Id, offset: usize) -> Self::Stream;
  
  async fn rm(&self, q: &Query) -> Result<u64, Self::Error>;
  
  fn key(&self) -> &SubTableKey;
  
  async fn get_or_insert_with<F>(
    &self,
    query: &Query,
    f: F,
  ) -> Result<(Id, Self::AsyncRow), Self::Error>
  where
    F: FnOnce() -> Row + Send;
}
```

### AsyncRowImpl

```rust
use jdb_trait::{AsyncRow, Row};

pub struct AsyncRowImpl {
  row: Row,
  blob_cols: Vec<(usize, BlobPtr)>,
  vlog: Arc<VLogReader>,
}

impl AsyncRow for AsyncRowImpl {
  type Error = Err;
  async fn row(&self) -> Result<Row, Self::Error>;
}
```

## 写入路径

```
put(rows)
  ├─► IdGen.get() → 生成 ID
  ├─► WAL.append(Put { sub_key, id, row })
  ├─► 对每列:
  │     ├─► len < 512B → 直接写入数据页
  │     └─► len ≥ 512B → VLog.append() → 数据页存 BlobPtr
  ├─► 更新索引 Index.insert(key, id)
  └─► ver++
```

## 查询路径

```
select(query)
  ├─► 解析 val_filter，提取索引条件
  ├─► 选择索引:
  │     ├─► 有匹配索引 → Index.range()
  │     └─► 无匹配索引 → 全表扫描
  ├─► 读取数据:
  │     ├─► 数据页 → 小值 (立即返回)
  │     └─► BlobPtr → AsyncRowImpl (延迟加载)
  └─► 返回 Stream<(Id, AsyncRowImpl)>
```

## 文件布局

```
{sub_dir}/
├── meta.jdb          # 元数据
├── wal.log           # 预写日志
├── data.jdb          # 数据页文件
├── vlog/
│   ├── 0001.vlog
│   └── 0002.vlog
└── idx/
    ├── 0.idx
    └── 1.idx
```

## 设计要点

1. **WAL 优先**: 所有写操作先写 WAL
2. **KV 分离**: 大值存 VLog，减少写放大
3. **AsyncRow**: 大值延迟加载
4. **MVCC**: 支持历史版本查询
