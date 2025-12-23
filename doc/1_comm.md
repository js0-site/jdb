# jdb_comm - 基础设施模块

定位: 零依赖的基础设施库，定义全系统的通用语言。

原则: 纯逻辑，无 IO，无副作用，高内联优化。

## 依赖

```toml
thiserror = "2.0"
gxhash = "3.0"
serde = { version = "1.0", features = ["derive"] }
bitcode = "0.6"
```

## 模块结构

```
jdb_comm/src/
├── lib.rs      # 模块导出
├── consts.rs   # 全局常量
├── types.rs    # NewType 核心类型
├── error.rs    # 统一错误定义
├── config.rs   # 系统配置
└── hash.rs     # 哈希算法封装
```

## 核心类型

### types.rs

```rust
/// 64 位表 ID（由二进制名称哈希生成）
#[repr(transparent)]
pub struct TableID(pub u64);

impl TableID {
    pub const fn new(id: u64) -> Self;
    pub fn from_name(name: &[u8]) -> Self;  // 支持二进制名称
}

/// 32 位物理页号（最大支持 16TB 单文件）
#[repr(transparent)]
pub struct PageID(pub u32);

/// 16 位虚拟节点 ID（分片路由）
#[repr(transparent)]
pub struct VNodeID(pub u16);

/// 64 位秒级时间戳
#[repr(transparent)]
pub struct Timestamp(pub u64);

/// 日志序列号（WAL 序列）
#[repr(transparent)]
pub struct Lsn(pub u64);
```

### error.rs

```rust
pub type JdbResult<T> = Result<T, JdbError>;

#[derive(Error, Debug)]
pub enum JdbError {
    Io(#[from] std::io::Error),
    Serialize(String),
    Checksum { expected: u32, actual: u32 },
    PageNotFound(PageID),
    WalFull,
    Internal(String),
}
```

### consts.rs

```rust
pub const PAGE_SIZE: usize = 4096;
pub const PAGE_HEADER_SIZE: usize = 32;
pub const FILE_MAGIC: u64 = 0x4A_44_42_5F_46_49_4C_45;
pub const INVALID_PAGE_ID: u32 = u32::MAX;
```

### hash.rs

```rust
pub fn fast_hash64(data: &[u8]) -> u64;
pub fn fast_hash128(data: &[u8]) -> u128;
pub fn route_to_vnode(key_hash: u64, total: u16) -> VNodeID;
```

### config.rs

```rust
pub struct KernelConfig {
    pub data_dir: PathBuf,
    pub wal_dir: PathBuf,
    pub vnode_count: u16,
    pub worker_threads: usize,
    pub io_depth: u32,
    pub block_cache_size: u64,
}
```

## 导出

```rust
pub use config::KernelConfig;
pub use consts::{FILE_MAGIC, INVALID_PAGE_ID, PAGE_HEADER_SIZE, PAGE_SIZE};
pub use error::{JdbError, JdbResult};
pub use hash::{fast_hash128, fast_hash64, route_to_vnode};
pub use types::{Lsn, PageID, TableID, Timestamp, VNodeID};
```
