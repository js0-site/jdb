# jdb_wal - 预写日志模块

定位: Write Ahead Log，保证原子性与持久性。

## 依赖

```toml
jdb_comm = { path = "../jdb_comm" }
jdb_alloc = { path = "../jdb_alloc" }
jdb_fs = { path = "../jdb_fs" }
jdb_layout = { path = "../jdb_layout" }
```

## 模块结构

```
jdb_wal/src/
├── lib.rs      # 模块导出
└── writer.rs   # WAL 读写器
```

## 核心类型

### WalWriter

```rust
pub struct WalWriter {
    file: File,
    buf: AlignedBuf,
    lsn: Lsn,
    offset: u64,
}

impl WalWriter {
    pub async fn create(path: impl AsRef<Path>) -> JdbResult<Self>;
    pub async fn open(path: impl AsRef<Path>) -> JdbResult<Self>;
    
    pub fn lsn(&self) -> Lsn;
    pub fn append(&mut self, entry: &WalEntry) -> JdbResult<Lsn>;
    pub async fn flush(&mut self) -> JdbResult<()>;
    pub async fn append_sync(&mut self, entry: &WalEntry) -> JdbResult<Lsn>;
}
```

### WalReader

```rust
pub struct WalReader {
    data: Vec<u8>,
    pos: usize,
}

impl WalReader {
    pub async fn open(path: impl AsRef<Path>) -> JdbResult<Self>;
    pub fn next(&mut self) -> JdbResult<Option<WalEntry>>;
}
```

## 存储格式

每条 WAL 条目：
```
+--------+--------+------------------+
| len:u32| crc:u32|    data: [u8]    |
+--------+--------+------------------+
   4B       4B         len bytes
```

- 写入时缓冲，flush 时填充到页边界
- 零长度表示 padding，读取时跳过
- CRC32 校验数据完整性

## 设计要点

- 批量写入：append 只写缓冲区，flush 时批量落盘
- 页对齐：写入时填充到 4KB 边界
- 崩溃恢复：WalReader 按页读取，解析条目
