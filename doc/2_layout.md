# jdb_layout - 磁盘布局模块

定位: 定义磁盘数据结构的物理布局（Protocol），不含 IO 逻辑。

## 依赖

```toml
jdb_comm = { path = "../jdb_comm" }
bitcode = "0.6"
bytes = "1.0"
crc-fast = "1.8"
```

## 模块结构

```
jdb_layout/src/
├── lib.rs       # 模块导出
├── page.rs      # 页面协议
├── wal.rs       # WAL 条目协议
├── blob.rs      # 大对象协议
└── checksum.rs  # 校验和封装
```

## 核心类型

### page.rs - 页面协议

```rust
/// 页头（32 字节）
#[repr(C)]
pub struct PageHeader {
    pub id: u32,
    pub typ: u8,
    pub flags: u8,
    pub _reserved: u16,
    pub lsn: u64,
    pub count: u16,
    pub free_off: u16,
    pub checksum: u32,
    pub _pad: [u8; 8],
}

pub mod page_type {
    pub const LEAF: u8 = 1;
    pub const INTERNAL: u8 = 2;
    pub const OVERFLOW: u8 = 3;
}

impl PageHeader {
    pub const PAYLOAD_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE;
    pub fn read(buf: &[u8]) -> Self;
    pub fn write(&self, buf: &mut [u8]);
    pub fn new(id: PageID, typ: u8, lsn: Lsn) -> Self;
}
```

### wal.rs - WAL 条目协议

```rust
#[derive(Encode, Decode)]
pub enum WalEntry {
    Put { table: TableID, ts: Timestamp, key: Vec<u8>, val: Vec<u8> },
    Delete { table: TableID, ts: Timestamp, key: Vec<u8> },
    Barrier { lsn: Lsn },
}

pub fn encode(entry: &WalEntry) -> Vec<u8>;
pub fn decode(data: &[u8]) -> Result<WalEntry, bitcode::Error>;
```

### blob.rs - 大对象协议

```rust
pub const BLOB_HEADER_SIZE: usize = 16;

/// 块头
#[repr(C)]
pub struct BlobHeader {
    pub len: u32,
    pub checksum: u32,
    pub ts: u64,
}

/// 大对象指针（存储在 B+ 树叶子节点）
#[repr(C)]
pub struct BlobPtr {
    pub file_id: u32,
    pub offset: u64,
    pub len: u32,
}

impl BlobPtr {
    pub const SIZE: usize = 16;
}
```

### checksum.rs - 校验和

```rust
pub fn crc32(data: &[u8]) -> u32;
pub fn verify(data: &[u8], expected: u32) -> bool;
```

## 导出

```rust
pub use blob::{BlobHeader, BlobPtr, BLOB_HEADER_SIZE};
pub use checksum::{crc32, verify};
pub use page::{page_type, PageHeader};
pub use wal::{decode, encode, WalEntry};
```
