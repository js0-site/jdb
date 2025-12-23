# jdb_api - API 模块

定位: Engine/Table 实现，对外 API。

## 依赖

```toml
jdb_trait = { path = "../jdb_trait" }
jdb_sub = { path = "../jdb_sub" }
jdb_meta = { path = "../jdb_meta" }
papaya = "0.2"
parking_lot = "0.12"
```

## 模块结构

```
jdb_api/src/
├── lib.rs      # 模块导出
├── engine.rs   # Engine 实现
├── table.rs    # Table 实现
└── id_gen.rs   # ID 生成器
```

## 核心类型

### Jdb (Engine 实现)

```rust
use jdb_trait::{Engine, IdGen, Table, Schema, AsyncItem};

pub struct Jdb {
  dir: PathBuf,
  config: Config,
  tables: papaya::HashMap<u64, JdbTable>,
  id_gen: AtomicIdGen,
}

impl Jdb {
  pub async fn open(dir: impl AsRef<Path>) -> R<Self>;
  pub async fn open_with(dir: impl AsRef<Path>, config: Config) -> R<Self>;
  pub async fn close(&self) -> R<()>;
}

impl Engine for Jdb {
  type Error = Err;
  type Gen = AtomicIdGen;
  type Table = JdbTable;

  fn id_gen(&self) -> &Self::Gen;
  
  async fn open<F, Fut>(
    &self,
    name: &[u8],
    create: F,
  ) -> Result<Self::Table, Self::Error>
  where
    F: FnOnce() -> Fut + Send,
    Fut: Future<Output = Schema> + Send;
}
```

### JdbTable (Table 实现)

```rust
use jdb_trait::{Table, SubTable, AsyncRow, AsyncItem, Id, Order, Query, Row, SubTableKey, Schema};

pub struct JdbTable {
  name_hash: u64,
  dir: PathBuf,
  schema: RwLock<Schema>,
  schema_history: Vec<Schema>,
  subs: papaya::HashMap<u64, Arc<Sub>>,
  sub_cache: LruCache<u64, Arc<Sub>>,
}

impl Table for JdbTable {
  type Error = Err;
  type SubTable = Sub;
  type AsyncRow = AsyncRowImpl;
  type Stream = impl Stream<Item = Result<AsyncItem<Self::AsyncRow>, Self::Error>> + Send;

  async fn schema(&self) -> Schema;
  
  async fn put(&self, key: &SubTableKey, data: &[Row]) -> Result<Vec<Id>, Self::Error>;
  
  async fn get(&self, key: &SubTableKey, id: Id) -> Result<Option<AsyncItem<Self::AsyncRow>>, Self::Error>;
  
  async fn get_or_insert_with<F>(
    &self,
    key: &SubTableKey,
    query: &Query,
    f: F,
  ) -> Result<AsyncItem<Self::AsyncRow>, Self::Error>
  where
    F: FnOnce() -> Row + Send;
  
  async fn compact(&self) -> Result<(), Self::Error>;
  
  async fn select(&self, q: &Query) -> Self::Stream;
  
  async fn scan(&self, begin_id: u64, order: Order) -> Self::Stream;
  
  async fn history(&self, key: &SubTableKey, id: Id, offset: usize) -> Self::Stream;
  
  async fn rm(&self, q: &Query) -> Result<u64, Self::Error>;
  
  async fn sub_exists(&self, key: &SubTableKey) -> bool;
  
  async fn sub(&self, key: &SubTableKey) -> Option<Self::SubTable>;
}
```

### AtomicIdGen

```rust
use jdb_trait::{IdGen, Id};

pub struct AtomicIdGen {
  next: AtomicU64,
}

impl IdGen for AtomicIdGen {
  type Error = Err;
  async fn get(&self) -> Result<Id, Self::Error> {
    Ok(self.next.fetch_add(1, Ordering::Relaxed))
  }
}
```

## 文件布局

```
{db_dir}/
├── meta.jdb              # 全局元数据
├── id_gen.jdb            # ID 生成器状态
└── tables/
    └── {table_hash}/
        ├── schema.jdb
        └── sub/
            └── {sub_key_hash}/
                ├── meta.jdb
                ├── wal.log
                ├── data.jdb
                ├── vlog/
                └── idx/
```

## 使用示例

```rust
use jdb_trait::{Schema, Field, Val, SubTableKey, Row};
use jdb_api::Jdb;

let db = Jdb::open("/tmp/mydb").await?;

let table = db.open(b"users", || async {
  Schema {
    name: b"users".into(),
    col_li: vec![
      Field { name: b"name".into(), default: Val::Str("".into()) },
    ],
    sub_table_key_li: vec![
      Field { name: b"tenant".into(), default: Val::U64(0) },
    ],
    ..Default::default()
  }
}).await?;

let sub_key: SubTableKey = vec![Val::U64(1)];
let row: Row = vec![Val::Str("user_name".into())];
let ids = table.put(&sub_key, &[row]).await?;
```

## 设计要点

1. **并发安全**: 使用 papaya 并发 HashMap
2. **延迟创建**: SubTable 按需创建
3. **子表缓存**: LRU 缓存活跃子表
4. **原子 ID**: 全局唯一 ID 生成
