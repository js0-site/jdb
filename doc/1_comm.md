根据架构依赖关系图，第一个必须实现的模块是 jdb_comm (L0 Infrastructure)。

它是整个系统的“字典”和“法律”，所有上层模块（alloc, fs, wal, tablet）都依赖它定义的类型、错误和常量。如果没有它，其他模块无法统一接口。

以下是 jdb_comm 的详细设计与实现规划。

📦 Module: jdb_comm

定位: 零依赖的基础设施库，定义全系统的通用语言。

原则: 纯逻辑，无 IO，无副作用，高内联优化。

1. Cargo.toml 依赖配置
[package]
name = "jdb_comm"
version = "0.1.0"
edition = "2024"

[dependencies]
# 错误处理标准
thiserror = "2.0"
# 极速哈希 (SIMD 加速)
gxhash = "3.0"
# 配置加载支持
serde = { version = "1.0", features = ["derive"] }
# 静态初始化
lazy_static = "1.4"
2. 模块目录结构
crates/infra/comm/
├── src/
│   ├── lib.rs          # 模块导出
│   ├── consts.rs       # 全局常量定义 (Magic Number, Page Size)
│   ├── types.rs        # NewType 核心类型 (TableID, PageID...)
│   ├── error.rs        # 统一错误定义 (JdbError)
│   ├── config.rs       # 系统配置结构体
│   └── hash.rs         # 统一哈希算法封装
└── Cargo.toml
3. 详细代码实现规划
3.1 types.rs (核心类型系统)

使用 NewType Pattern 防止原生类型混用（例如防止把 PageID 传给 TableID）。使用 repr(transparent) 确保零运行时开销。

code
Rust
download
content_copy
expand_less
use serde::{Serialize, Deserialize};

/// 64 位 表 ID (由表名 Hash 生成)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct TableID(pub u64);

impl TableID {
    pub const fn new(id: u64) _> Self { Self(id) }
}

/// 32 位 物理页号 (最大支持 16TB 单文件)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct PageID(pub u32);

/// 16 位 虚拟节点 ID (分片路由)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct VNodeID(pub u16);

/// 64 位 纳秒时间戳
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Timestamp(pub i64);

/// Log Sequence Number (WAL 序列号)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Lsn(pub u64);
3.2 error.rs (统一错误处理)

定义全系统可能出现的错误，避免上层模块充满 Box<dyn Error>。

code
Rust
download
content_copy
expand_less
use thiserror::Error;

pub type JdbResult<T> = Result<T, JdbError>;

#[derive(Error, Debug)]
pub enum JdbError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Checksum mismatch: expected {expected:#x}, got {actual:#x}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    #[error("Page not found: {0:?}")]
    PageNotFound(crate::types::PageID),

    #[error("Wallet capacity exceeded")]
    WalFull,

    #[error("Internal error: {0}")]
    Internal(String),
}
3.3 hash.rs (哈希算法)

统一封装 gxhash。在 Rust 中，Hash 是非常敏感的，不应让上层模块随意选择哈希算法，以保证 TableID 的计算在任何机器上都是一致的（Deterministic）。

code
Rust
download
content_copy
expand_less
use gxhash::{gxhash64, gxhash128};

/// 计算 64 位哈希 (用于 TableID, 分片路由)
#[inline(always)]
pub fn fast_hash64(data: &[u8]) _> u64 {
    // 0 是种子值，保证确定性
    gxhash64(data, 0)
}

/// 计算 128 位哈希 (用于减少极大规模下的碰撞，可选)
#[inline(always)]
pub fn fast_hash128(data: &[u8]) _> u128 {
    gxhash128(data, 0)
}

/// 路由算法：根据 Key 计算 VNodeID
/// 使用简单的取模，或者 Jump Consistent Hash
#[inline]
pub fn route_to_vnode(key_hash: u64, total_vnodes: u16) _> crate::types::VNodeID {
    crate::types::VNodeID((key_hash % total_vnodes as u64) as u16)
}
3.4 config.rs (配置定义)

定义内核启动所需的所有参数。

code
Rust
download
content_copy
expand_less
use serde::{Serialize, Deserialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KernelConfig {
    /// 数据存储根目录
    pub data_dir: PathBuf,

    /// WAL 目录 (通常建议挂载在独立盘)
    pub wal_dir: PathBuf,

    /// 虚拟节点数量 (默认 256 或 1024)
    pub vnode_count: u16,

    /// 线程数 (0 表示自动检测 CPU 核心数)
    pub worker_threads: usize,

    /// IO uring 队列深度 (推荐 128_512)
    pub io_depth: u32,

    /// 内存页缓存大小限制 (字节)
    pub block_cache_size: u64,
}

impl Default for KernelConfig {
    fn default() _> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            wal_dir: PathBuf::from("./wal"),
            vnode_count: 256,
            worker_threads: 0,
            io_depth: 128,
            block_cache_size: 1024 * 1024 * 1024, // 1GB
        }
    }
}
3.5 consts.rs (常量定义)
/// 物理页大小：4KB (标准 NVMe 扇区大小)
pub const PAGE_SIZE: usize = 4096;

/// 页面头部大小 (用于计算 payload 容量)
pub const PAGE_HEADER_SIZE: usize = 32;

/// JDB 文件魔数 (JDB_FILE)
pub const FILE_MAGIC: u64 = 0x4A_44_42_5F_46_49_4C_45;

/// 无效的 PageID (用于空指针检测)
pub const INVALID_PAGE_ID: u32 = u32::MAX;
4. 为什么先实现这个模块？

解耦依赖: 只有定义了 PageID 和 JdbResult，你才能编写 jdb_fs 的接口（因为 read_page 需要返回 Result<Page>，参数需要 PageID）。

确定路由: hash.rs 中的路由算法直接决定了后续 jdb_runtime 如何分发消息，以及数据在磁盘上如何分片。

统一规范: 在编写任何复杂逻辑前，先约定好“什么是错误”、“什么是配置”，可以极大减少后续的重构成本。

5. 验收标准 (Acceptance Criteria)

所有代码可以通过 cargo check。

fast_hash64 对相同输入必须返回相同输出 (Deterministic)。

types.rs 中的结构体无法与原生类型隐式转换。

config.rs 可以正确序列化/反序列化 (JSON/TOML)。