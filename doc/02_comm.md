# jdb_comm - 基础设施模块

定位: 全系统通用基础设施，纯逻辑，无 IO。

## 依赖

```toml
thiserror = "2"
gxhash = "3"
coarsetime = "0.1"
```

## 模块结构

```
jdb_comm/src/
├── lib.rs      # 模块导出
├── consts.rs   # 全局常量
├── err.rs      # 统一错误
└── hash.rs     # 哈希封装
```

## 核心类型

### consts.rs

```rust
/// 页大小 4KB (Direct I/O)
pub const PAGE_SIZE: usize = 4096;

/// 页头大小
pub const PAGE_HEADER_SIZE: usize = 32;

/// 文件魔数 "JDB_FILE"
pub const FILE_MAGIC: u64 = 0x4A_44_42_5F_46_49_4C_45;

/// 无效页 ID
pub const INVALID_PAGE_ID: u32 = u32::MAX;

/// KV 分离阈值
pub const BLOB_THRESHOLD: usize = 512;
```

### err.rs

```rust
pub type R<T> = Result<T, E>;

#[derive(Error, Debug)]
pub enum E {
  #[error("io: {0}")]
  Io(#[from] std::io::Error),

  #[error("checksum: expect {0:#x}, got {1:#x}")]
  Checksum(u32, u32),

  #[error("page not found: {0}")]
  PageNotFound(u32),

  #[error("wal corrupted at {0}")]
  WalCorrupt(u64),

  #[error("not found")]
  NotFound,

  #[error("duplicate")]
  Duplicate,

  #[error("full")]
  Full,

  #[error("{0}")]
  Other(Box<str>),
}
```

### hash.rs

```rust
use gxhash::{gxhash64, gxhash128};

#[inline(always)]
pub fn hash64(data: &[u8]) -> u64 {
  gxhash64(data, 0)
}

#[inline(always)]
pub fn hash128(data: &[u8]) -> u128 {
  gxhash128(data, 0)
}

/// 当前秒级时间戳 Current timestamp in seconds
#[inline]
pub fn now_sec() -> u64 {
  coarsetime::Clock::now_since_epoch().as_secs()
}
```

## 导出

```rust
pub use consts::*;
pub use err::{E, R};
pub use hash::{hash64, hash128, now_sec};
```

## 设计要点

1. **极简**: 只保留必要功能
2. **内联**: 热路径函数 `#[inline(always)]`
3. **错误精简**: 枚举变体最小化
4. **无 NewType**: 直接用原生类型，减少包装开销
