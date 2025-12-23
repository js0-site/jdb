# jdb_alloc - 内存分配模块

定位: Direct I/O 对齐内存分配器，4KB 对齐。提供拥有所有权的 `AlignedBuf` 和无所有权的 `RawIoBuf` 两种缓冲区类型。

## 依赖

```toml
compio-buf = "0.7"
thiserror = "2"
```

## 模块结构

```
jdb_alloc/src/
├── lib.rs      # AlignedBuf 和 RawIoBuf 实现
└── error.rs    # 错误类型定义
```

## 常量

```rust
/// 页大小 4KB / 页大小
pub const PAGE_SIZE: usize = 4096;

/// Direct I/O 对齐要求（必须是2的幂）
pub const ALIGNMENT: usize = 4096;
```

## 错误类型

```rust
#[derive(Error, Debug)]
pub enum Error {
  #[error("io: {0}")]
  Io(#[from] std::io::Error),

  #[error("invalid layout: {0}")]
  InvalidLayout(#[from] LayoutError),

  #[error("alloc failed")]
  AllocFailed,

  #[error("overflow: {0}/{1}")]
  Overflow(usize, usize),

  #[error("{0}")]
  Other(Box<str>),
}

pub type Result<T> = std::result::Result<T, Error>;
```

## 核心类型

### AlignedBuf - 拥有所有权的对齐缓冲区

```rust
/// 拥有所有权的对齐缓冲区，Drop 时释放内存
pub struct AlignedBuf {
  ptr: NonNull<u8>,
  len: usize,
  cap: usize,
}

impl AlignedBuf {
  // 构造函数
  pub fn with_cap(cap: usize) -> Result<Self>;
  pub fn zeroed(size: usize) -> Result<Self>;
  pub fn page() -> Result<Self>;

  // RawIoBuf 转换
  pub unsafe fn as_raw(&mut self) -> RawIoBuf;
  pub unsafe fn as_raw_view(&self) -> RawIoBuf;
  pub unsafe fn slice_into_raws(&self, chunk: usize) -> impl Iterator<Item = RawIoBuf> + '_;

  // 原始操作
  pub fn into_raw_parts(self) -> (NonNull<u8>, usize, usize);
  pub unsafe fn from_raw_parts(ptr: NonNull<u8>, len: usize, cap: usize) -> Self;

  // 属性访问
  pub fn len(&self) -> usize;
  pub fn cap(&self) -> usize;
  pub fn is_empty(&self) -> bool;
  pub fn as_ptr(&self) -> *const u8;
  pub fn as_mut_ptr(&mut self) -> *mut u8;

  // 修改操作
  pub fn clear(&mut self);
  pub fn truncate(&mut self, len: usize);
  pub unsafe fn set_len(&mut self, len: usize);
  pub fn extend(&mut self, data: &[u8]) -> Result<()>;
  pub fn try_clone(&self) -> Result<Self>;
}

// 标准 trait 实现
impl Deref for AlignedBuf { type Target = [u8]; }
impl DerefMut for AlignedBuf {}
impl Clone for AlignedBuf {}

// compio 兼容
unsafe impl IoBuf for AlignedBuf {}
unsafe impl IoBufMut for AlignedBuf {}
impl SetBufInit for AlignedBuf {}

// 线程安全
unsafe impl Send for AlignedBuf {}
unsafe impl Sync for AlignedBuf {}
```

### RawIoBuf - 无所有权的原始缓冲区

```rust
/// 原始 I/O 缓冲区（不持有所有权，可复制）
/// 用于 Buffer Pool Arena 模式，实现 `Copy` 便于异步 IO
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RawIoBuf {
  ptr: *mut u8,
  len: usize,
  cap: usize,
}

impl RawIoBuf {
  // 构造函数
  pub const unsafe fn new(ptr: *mut u8, cap: usize) -> Self;
  pub const fn with_len(self, len: usize) -> Self;
  pub fn from_slice(slice: &mut [u8]) -> Self;

  // 切片操作
  pub unsafe fn slice(self, offset: usize, len: usize) -> Self;
  pub unsafe fn slice_data(self, offset: usize, len: usize) -> Self;
  pub unsafe fn slice_unchecked(self, offset: usize, len: usize) -> Self;

  // 属性访问
  pub const fn len(&self) -> usize;
  pub const fn cap(&self) -> usize;
  pub const fn is_empty(&self) -> bool;
  pub const fn as_ptr(&self) -> *const u8;
  pub const fn as_mut_ptr(&self) -> *mut u8;

  // 数据访问
  pub fn as_slice(&self) -> &[u8];
  pub fn as_mut_slice(&mut self) -> &mut [u8];
  pub unsafe fn set_len(&mut self, len: usize);
}

// compio 兼容
unsafe impl IoBuf for RawIoBuf {}
unsafe impl IoBufMut for RawIoBuf {}
impl SetBufInit for RawIoBuf {}

// 线程安全（需调用者确保同步）
unsafe impl Send for RawIoBuf {}
unsafe impl Sync for RawIoBuf {}
```

## 内存布局

```
AlignedBuf (拥有所有权)
┌─────────────────────────────────────┐
│ ptr: NonNull<u8>  (4KB 对齐)        │
│ len: usize        (已初始化)        │
│ cap: usize        (已分配)          │
└─────────────────────────────────────┘
           │
           ▼
RawIoBuf (无所有权, Copy)
┌─────────────────────────────────────┐
│ ptr: *mut u8      (借用)            │
│ len: usize                          │
│ cap: usize                          │
└─────────────────────────────────────┘
```

## 使用模式

### Buffer Pool Arena 模式

```rust
// 分配大型 arena
let arena = AlignedBuf::zeroed(PAGE_SIZE * 8).unwrap();

// 切分为多个 RawIoBuf 帧
let frames: Vec<RawIoBuf> = unsafe {
  arena.slice_into_raws(PAGE_SIZE).collect()
};

// RawIoBuf 可复制，传递给异步 I/O
let frame = frames[0];
```

### 零拷贝 I/O 模式

```rust
let mut buf = AlignedBuf::zeroed(4096).unwrap();

// 转换为 RawIoBuf 用于异步 I/O
let raw = unsafe { buf.as_raw() };

// RawIoBuf 是 Copy 的，可传递给异步操作
```

## 设计要点

1. **强制 4096 字节对齐**: 满足 Direct I/O 要求
2. **双缓冲区设计**: `AlignedBuf` 拥有内存，`RawIoBuf` 用于零拷贝传递
3. **compio 完全兼容**: 实现 IoBuf/IoBufMut/SetBufInit trait
4. **Arena 模式支持**: 通过 `slice_into_raws` 实现 Buffer Pool
5. **错误处理**: 使用 thiserror 提供结构化错误
6. **线程安全**: 两种类型都支持 Send + Sync
7. **零拷贝**: RawIoBuf 的 Copy 语义避免不必要复制
