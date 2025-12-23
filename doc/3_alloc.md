# jdb_alloc - 内存分配模块

定位: Direct I/O 对齐内存分配器，4KB 对齐。

## 依赖

```toml
jdb_comm = { path = "../jdb_comm" }
compio-buf = "0.7"
```

## 模块结构

```
jdb_alloc/src/
└── lib.rs    # AlignedBuf 实现
```

## 核心类型

### AlignedBuf

```rust
/// Direct I/O 对齐缓冲区（4KB 对齐）
pub struct AlignedBuf {
    ptr: NonNull<u8>,
    len: usize,
    cap: usize,
}

impl AlignedBuf {
    pub fn with_cap(cap: usize) -> Self;
    pub fn zeroed(size: usize) -> Self;
    pub fn page() -> Self;  // 创建单页
    
    pub fn len(&self) -> usize;
    pub fn cap(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    
    pub unsafe fn set_len(&mut self, len: usize);
    pub fn as_ptr(&self) -> *const u8;
    pub fn as_mut_ptr(&mut self) -> *mut u8;
    
    pub fn clear(&mut self);
    pub fn extend(&mut self, data: &[u8]);
}

// Trait 实现
impl Deref for AlignedBuf { type Target = [u8]; }
impl DerefMut for AlignedBuf {}
impl AsRef<[u8]> for AlignedBuf {}
impl AsMut<[u8]> for AlignedBuf {}
impl Clone for AlignedBuf {}

// compio 兼容
unsafe impl IoBuf for AlignedBuf {}
unsafe impl IoBufMut for AlignedBuf {}
impl SetBufInit for AlignedBuf {}
```

## 设计要点

- 强制 4096 字节对齐，满足 Direct I/O 要求
- 实现 compio 的 IoBuf/IoBufMut trait，可直接用于异步 IO
- Send + Sync，支持跨线程传递
