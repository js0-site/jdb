# jdb_layout - 磁盘布局模块

定位: 定义磁盘数据结构的物理布局，不含 IO 逻辑。

## 依赖

```toml
jdb_comm = { path = "../jdb_comm" }
crc32fast = "1"
```

## 模块结构

```
jdb_layout/src/
├── lib.rs    # 模块导出
├── page.rs   # 页头
├── ptr.rs    # BlobPtr
└── crc.rs    # CRC32 校验
```

## 核心类型

### page.rs - 页头

```rust
/// 页类型
pub mod page_type {
  pub const DATA: u8 = 1;
  pub const INDEX_LEAF: u8 = 2;
  pub const INDEX_INTERNAL: u8 = 3;
  pub const OVERFLOW: u8 = 4;
  pub const META: u8 = 5;
}

/// 页头 32 字节
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct PageHeader {
  pub magic: u32,      // 魔数
  pub page_id: u32,    // 页 ID
  pub typ: u8,         // 页类型
  pub flags: u8,       // 标志
  pub count: u16,      // 记录数
  pub free_start: u16, // 空闲起始
  pub free_end: u16,   // 空闲结束
  pub next: u32,       // 下一页
  pub checksum: u32,   // CRC32
  pub _pad: [u8; 4],   // 填充
}
```

### ptr.rs - BlobPtr

```rust
/// Blob 指针 16 字节
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlobPtr {
  pub file_id: u32,
  pub offset: u64,
  pub len: u32,
}
```

### crc.rs - 校验和

```rust
#[inline(always)]
pub fn crc32(data: &[u8]) -> u32;

#[inline(always)]
pub fn verify(data: &[u8], expected: u32) -> bool;
```

## 设计要点

1. **定长结构**: PageHeader 32 字节，BlobPtr 16 字节
2. **#[repr(C)]**: 保证内存布局确定
3. **CRC32**: 使用 crc32fast 硬件加速
