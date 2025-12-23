# jdb_fs - 文件系统模块

定位: compio 异步文件系统封装。

## 依赖

```toml
jdb_comm = { path = "../jdb_comm" }
jdb_alloc = { path = "../jdb_alloc" }
compio = { version = "0.17", features = ["runtime"] }
```

## 模块结构

```
jdb_fs/src/
├── lib.rs     # 模块导出
└── file.rs    # 异步文件操作
```

## 核心类型

### File

```rust
/// 异步文件封装
pub struct File {
    inner: compio::fs::File,
}

impl File {
    // 打开方式
    pub async fn open(path: impl AsRef<Path>) -> JdbResult<Self>;
    pub async fn create(path: impl AsRef<Path>) -> JdbResult<Self>;
    pub async fn open_rw(path: impl AsRef<Path>) -> JdbResult<Self>;
    
    // 读操作
    pub async fn read_at(&self, offset: u64, len: usize) -> JdbResult<AlignedBuf>;
    pub async fn read_page(&self, page_no: u32) -> JdbResult<AlignedBuf>;
    
    // 写操作（需要 &mut self）
    pub async fn write_at(&mut self, offset: u64, buf: AlignedBuf) -> JdbResult<AlignedBuf>;
    pub async fn write_page(&mut self, page_no: u32, buf: AlignedBuf) -> JdbResult<AlignedBuf>;
    
    // 同步
    pub async fn sync(&mut self) -> JdbResult<()>;
    pub async fn size(&self) -> JdbResult<u64>;
}
```

## 设计要点

- 基于 compio 实现全异步 IO
- 使用 AlignedBuf 确保 Direct I/O 兼容
- 页级别读写接口，简化上层使用
- 写操作返回 buffer 所有权（compio 模型）
