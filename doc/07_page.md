# jdb_page - 缓冲区管理模块

定位: Buffer Manager，用户态页缓存，支持 Page Grouping。

参考: TreeLine (VLDB 2022)

## 依赖

```toml
jdb_alloc = { path = "../jdb_alloc" }
jdb_fs = { path = "../jdb_fs" }
jdb_layout = { path = "../jdb_layout" }
parking_lot = "0.12"
```

## 模块结构

```
jdb_page/src/
├── lib.rs       # 模块导出
├── page.rs      # 页面抽象
├── pool.rs      # 缓冲池
└── lru.rs       # LRU 驱逐策略
```

## 核心类型

### PageState

```rust
#[derive(Clone, Copy, PartialEq)]
pub enum PageState { Clean, Dirty }
```

### Page

```rust
pub struct Page {
  id: u32,
  state: PageState,
  buf: AlignedBuf,
  pin_count: u32,
}

impl Page {
  pub fn new(id: u32) -> Self;
  pub fn from_buf(id: u32, buf: AlignedBuf) -> Self;
  pub fn id(&self) -> u32;
  pub fn header(&self) -> PageHeader;
  pub fn mark_dirty(&mut self);
  pub fn is_dirty(&self) -> bool;
  pub fn pin(&mut self);
  pub fn unpin(&mut self);
  pub fn is_pinned(&self) -> bool;
  pub fn data(&self) -> &[u8];
  pub fn data_mut(&mut self) -> &mut [u8];
}
```

### BufferPool

```rust
pub struct BufferPool {
  file: File,
  pages: HashMap<u32, Page>,
  lru: LruList,
  cap: usize,
  next_page_id: u32,
}

impl BufferPool {
  pub async fn open(file: File, cap: usize) -> R<Self>;
  pub async fn get(&mut self, id: u32) -> R<&mut Page>;
  pub fn alloc(&mut self) -> R<&mut Page>;
  pub async fn flush_page(&mut self, id: u32) -> R<()>;
  pub async fn flush_all(&mut self) -> R<()>;
  pub async fn sync(&mut self) -> R<()>;
}
```

## 页面布局

### 数据页布局

```
┌─────────────────────────────────────┐
│ PageHeader (32 bytes)               │
├─────────────────────────────────────┤
│ slot_count: u16                     │
│ free_start: u16                     │
│ free_end: u16                       │
├─────────────────────────────────────┤
│ Slot Directory                      │
│   [offset:u16, len:u16] × N         │
├─────────────────────────────────────┤
│ ... Free Space ...                  │
├─────────────────────────────────────┤
│ Row Data (从页尾向前增长)            │
└─────────────────────────────────────┘
```

## 设计要点

1. **LRU 驱逐**: 满时驱逐未固定的冷页面
2. **Pin/Unpin**: 固定页面防止被驱逐
3. **Dirty 跟踪**: 只刷新修改过的页面
4. **Direct I/O**: 使用 AlignedBuf 确保对齐
