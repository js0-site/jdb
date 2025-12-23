# jdb_api - API 模块

定位: Engine/Table 实现，对外 API。

## 依赖

```toml
jdb_trait = { path = "../jdb_trait" }
jdb_comm = { path = "../jdb_comm" }
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
```

### JdbTable (Table 实现)

```rust
pub struct JdbTable {
  name_hash: u64,
  dir: PathBuf,
  schema: RwLock<Schema>,
  schema_history: Vec<Schema>,
  subs: papaya::HashMap<u64, Arc<Sub>>,
  sub_cache: LruCache<u64, Arc<Sub>>,
}
```

### AtomicIdGen

```rust
pub struct AtomicIdGen {
  next: AtomicU64,
}

impl IdGen for AtomicIdGen {
  type Error = Err;
  async fn get(&self) -> R<Id> {
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
let db = Jdb::open("/tmp/mydb").await?;

let table = db.open(b"users", || async {
  Schema {
    name: b"users".into(),
    ver: 1,
    col_li: vec![
      Field { name: b"name".into(), default: Val::Str("".into()) },
    ],
    sub_table_key_li: vec![
      Field { name: b"tenant".into(), default: Val::U64(0) },
    ],
    ..Default::default()
  }
}).await?;

let sub_key = vec![Val::U64(1)];
let ids = table.put(&sub_key, &[row]).await?;
```

## 设计要点

1. **并发安全**: 使用 papaya 并发 HashMap
2. **延迟创建**: SubTable 按需创建
3. **子表缓存**: LRU 缓存活跃子表
4. **原子 ID**: 全局唯一 ID 生成
