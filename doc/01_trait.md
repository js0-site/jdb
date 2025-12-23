# jdb_trait - 接口定义模块

定位: 定义核心 trait 和类型，所有模块的契约层。

## 依赖

```toml
hipstr = "0.6"
ordered-float = "5"
gxhash = "4"
futures-core = "0.3"
```

## 模块结构

```
jdb_trait/src/
├── lib.rs       # 模块导出与核心类型
├── val.rs       # 原子值类型
├── row.rs       # 行数据与异步行
├── schema.rs    # Schema 定义
├── expr.rs      # 表达式与操作符
├── query.rs     # 查询结构
├── table.rs     # Table trait (超级表)
└── sub_table.rs # SubTable trait (子表)
```

## 核心类型

### lib.rs - 基础类型

```rust
/// 全局 ID 类型 Global ID type
pub type Id = u64;

/// 列名 Column name
pub type Col = HipByt<'static>;

/// 列偏移量 Column offset in Row
pub type ColIdx = u16;

/// 子表键 (用于路由定位子表)
/// SubTable key for routing
pub type SubTableKey = Row;

/// 异步记录 Async record
pub struct AsyncItem<R: AsyncRow> {
  pub sub_table: SubTableKey,
  pub id: Id,
  pub row: R,
}
```

### val.rs - 原子值

```rust
/// 数据库原子值 Database atomic value
pub enum Val {
  Bool(bool),
  I8(i8), I16(i16), I32(i32), I64(i64), I128(i128),
  U8(u8), U16(u16), U32(u32), U64(u64), U128(u128),
  F32(OrderedFloat<f32>),
  F64(OrderedFloat<f64>),
  Str(HipStr<'static>),
  Bin(HipByt<'static>),
}
```

- 支持全部数值类型
- 浮点使用 OrderedFloat 支持 Ord/Hash
- 字符串使用 HipStr/HipByt 优化内存 (SSO)

### row.rs - 行数据

```rust
/// 同步行数据 Synchronous row data
pub type Row = Vec<Val>;

/// 异步行数据 trait，用于键值分离场景
/// Async row trait for KV separation
pub trait AsyncRow: Send + Sync + Debug {
  type Error: Debug + Send + Sync;
  fn row(&self) -> impl Future<Output = Result<Row, Self::Error>> + Send;
}
```

### schema.rs - Schema 定义

```rust
/// 字段定义 Field definition
pub struct Field {
  pub name: Col,
  pub default: Val,
}

/// 索引定义 Index definition
pub struct Index {
  pub cols: Vec<ColIdx>,
  pub unique: bool,
}

/// Schema 定义
pub struct Schema {
  pub name: HipByt<'static>,
  pub col_li: Vec<Field>,
  pub sub_table_key_li: Vec<Field>,
  pub index_li: Vec<Index>,
  pub max_depth: Option<usize>,
  pub ttl: Option<Duration>,
}
```

### expr.rs - 表达式

```rust
/// 排序方向 Order direction
pub enum Order { Asc, Desc }

/// 操作符 Operator
pub enum Op {
  Eq(Val),
  In(HashSet<Val>),
  Range(Val, Val),
  RangeInclusive(Val, Val),
  RangeFrom(Val),
  RangeTo(Val),
  RangeToInclusive(Val),
}

/// 表达式 Expression
pub enum Expr {
  KeyCol(ColIdx, Op),
  ValCol(ColIdx, Op),
  And(Box<Expr>, Box<Expr>),
  Or(Box<Expr>, Box<Expr>),
  Not(Box<Expr>),
}
```

### query.rs - 查询

```rust
pub struct Query {
  pub sub_table_filter: Option<Expr>,
  pub val_filter: Option<Expr>,
  pub limit: Option<usize>,
  pub offset: Option<usize>,
  pub order: Order,
}
```

## 核心 Trait

### IdGen - ID 生成器

```rust
pub trait IdGen: Send + Sync {
  type Error: Debug + Send + Sync;
  fn get(&self) -> impl Future<Output = Result<Id, Self::Error>> + Send;
}
```

### Engine - 数据库引擎

```rust
pub trait Engine: Sized + Send + Sync {
  type Error: Debug + Send + Sync;
  type Gen: IdGen;
  type Table: Table;

  fn id_gen(&self) -> &Self::Gen;

  fn open<F, Fut>(
    &self, name: &[u8], create: F,
  ) -> impl Future<Output = Result<Self::Table, Self::Error>> + Send
  where
    F: FnOnce() -> Fut + Send,
    Fut: Future<Output = Schema> + Send;
}
```

### Table - 超级表

```rust
pub trait Table: Sized + Send + Sync {
  type Error: Debug + Send + Sync;
  type SubTable: SubTable;
  type AsyncRow: AsyncRow;
  type Stream: Stream<Item = Result<AsyncItem<Self::AsyncRow>, Self::Error>> + Send;

  fn schema(&self) -> impl Future<Output = Schema> + Send;

  fn put(&self, key: &SubTableKey, data: &[Row]) 
    -> impl Future<Output = Result<Vec<Id>, Self::Error>> + Send;

  fn get(&self, key: &SubTableKey, id: Id)
    -> impl Future<Output = Result<Option<AsyncItem<Self::AsyncRow>>, Self::Error>> + Send;

  fn select(&self, q: &Query) -> impl Future<Output = Self::Stream> + Send;
  fn scan(&self, begin_id: u64, order: Order) -> impl Future<Output = Self::Stream> + Send;
  fn history(&self, key: &SubTableKey, id: Id, offset: usize) -> impl Future<Output = Self::Stream> + Send;
  fn rm(&self, q: &Query) -> impl Future<Output = Result<u64, Self::Error>> + Send;

  fn sub_exists(&self, key: &SubTableKey) -> impl Future<Output = bool> + Send;
  fn sub(&self, key: &SubTableKey) -> impl Future<Output = Option<Self::SubTable>> + Send;
}
```

### SubTable - 子表

```rust
pub trait SubTable: Send + Sync {
  type Error: Debug + Send + Sync;
  type AsyncRow: AsyncRow;
  type Stream: Stream<Item = Result<(Id, Self::AsyncRow), Self::Error>> + Send;

  fn put(&self, data: &[Row]) -> impl Future<Output = Result<Vec<Id>, Self::Error>> + Send;
  fn get(&self, id: Id) -> impl Future<Output = Result<Option<(Id, Self::AsyncRow)>, Self::Error>> + Send;
  fn select(&self, q: &Query) -> impl Future<Output = Self::Stream> + Send;
  fn scan(&self, begin_id: u64, order: Order) -> impl Future<Output = Self::Stream> + Send;
  fn history(&self, id: Id, offset: usize) -> impl Future<Output = Self::Stream> + Send;
  fn rm(&self, q: &Query) -> impl Future<Output = Result<u64, Self::Error>> + Send;
  fn key(&self) -> &SubTableKey;
}
```

## 设计要点

1. **超级表-子表模型**: Table 是 Schema 模板，SubTable 是实际存储单元
2. **异步行 (AsyncRow)**: 支持键值分离，大值延迟加载
3. **泛型 Error**: 每个 trait 定义自己的 Error 类型
4. **流式返回**: 查询返回 Stream，支持亿级结果集
5. **零拷贝**: 使用 HipStr/HipByt 优化字符串 (SSO)
