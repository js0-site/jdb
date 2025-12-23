# jdb_page - 缓冲区管理模块

定位: Buffer Manager，用户态页缓存。

## 依赖

```toml
jdb_comm = { path = "../jdb_comm" }
jdb_alloc = { path = "../jdb_alloc" }
jdb_fs = { path = "../jdb_fs" }
jdb_layout = { path = "../jdb_layout" }
```

## 模块结构

```
jdb_page/src/
├── lib.rs    # 模块导出
├── page.rs   # 页面抽象
└── cache.rs  # 缓冲池
```

## 核心类型

### PageState

```rust
pub enum PageState {
    Clean,  // 干净，在内存中
    Dirty,  // 已修改，需要刷盘
}
```

### Page

```rust
pub struct Page {
    pub id: PageID,
    pub state: PageState,
    pub buf: AlignedBuf,
    pub pin_count: u32,
}

impl Page {
    pub fn new(id: PageID) -> Self;
    pub fn from_buf(id: PageID, buf: AlignedBuf) -> Self;
    pub fn header(&self) -> PageHeader;
    pub fn mark_dirty(&mut self);
    pub fn is_dirty(&self) -> bool;
    pub fn pin(&mut self);
    pub fn unpin(&mut self);
    pub fn is_pinned(&self) -> bool;
}
```

### BufferPool

```rust
pub struct BufferPool {
    file: File,
    pages: HashMap<u32, Page>,
    cap: usize,
}

impl BufferPool {
    pub fn new(file: File, cap: usize) -> Self;
    pub async fn get(&mut self, id: PageID) -> JdbResult<&mut Page>;
    pub fn alloc(&mut self, id: PageID) -> &mut Page;
    pub async fn flush(&mut self, id: PageID) -> JdbResult<()>;
    pub async fn flush_all(&mut self) -> JdbResult<()>;
    pub async fn sync(&mut self) -> JdbResult<()>;
}
```

## 设计要点

- Pin/Unpin：固定页面防止被驱逐
- Dirty 跟踪：只刷新修改过的页面
- LRU 驱逐：满时驱逐未固定的页面
- Direct I/O：使用 AlignedBuf 确保对齐
