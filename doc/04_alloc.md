# jdb_alloc - 内存分配模块

定位: Direct I/O 对齐内存分配器，4KB 对齐。

## 依赖

```toml
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
/// Direct I/O 对齐缓冲区 (4KB 对齐)
pub struct AlignedBuf {
  ptr: NonNull<u8>,
  len: usize,
  cap: usize,
}

impl AlignedBuf {
  pub fn with_cap(cap: usize) -> Self;
  pub fn zeroed(size: usize) -> Self;
  pub fn page() -> Self;
  pub fn pages(n: usize) -> Self;

  pub fn len(&self) -> usize;
  pub fn cap(&self) -> usize;
  pub fn is_empty(&self) -> bool;

  pub unsafe fn set_len(&mut self, len: usize);
  pub fn as_ptr(&self) -> *const u8;
  pub fn as_mut_ptr(&mut self) -> *mut u8;

  pub fn clear(&mut self);
  pub fn extend(&mut self, data: &[u8]);
  pub fn resize(&mut self, new_len: usize, val: u8);
}

// 标准 trait 实现
impl Deref for AlignedBuf { type Target = [u8]; }
impl DerefMut for AlignedBuf {}

// compio 兼容
unsafe impl IoBuf for AlignedBuf {}
unsafe impl IoBufMut for AlignedBuf {}

// 线程安全
unsafe impl Send for AlignedBuf {}
unsafe impl Sync for AlignedBuf {}
```

## 内存布局

```
┌─────────────────────────────────────┐
│ 4KB 对齐边界                         │
├─────────────────────────────────────┤
│ ptr ──────────────────────────────► │
│ [data: len bytes]                   │
│ [unused: cap - len bytes]           │
├─────────────────────────────────────┤
│ 4KB 对齐边界                         │
└─────────────────────────────────────┘
```

## 设计要点

1. **强制 4096 字节对齐**: 满足 Direct I/O 要求
2. **compio 兼容**: 实现 IoBuf/IoBufMut trait
3. **Send + Sync**: 支持跨线程传递
4. **零拷贝**: 避免不必要的内存复制
