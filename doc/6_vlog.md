# jdb_vlog - 值日志模块

定位: Value Log，KV 分离存储大对象。

## 依赖

```toml
jdb_comm = { path = "../jdb_comm" }
jdb_alloc = { path = "../jdb_alloc" }
jdb_fs = { path = "../jdb_fs" }
jdb_layout = { path = "../jdb_layout" }
```

## 模块结构

```
jdb_vlog/src/
├── lib.rs      # 模块导出
└── writer.rs   # Vlog 读写器
```

## 核心类型

### VlogWriter

```rust
pub struct VlogWriter {
    file: File,
    file_id: u32,
    offset: u64,
}

impl VlogWriter {
    pub async fn create(path: impl AsRef<Path>, file_id: u32) -> JdbResult<Self>;
    pub async fn open(path: impl AsRef<Path>, file_id: u32) -> JdbResult<Self>;
    
    pub fn offset(&self) -> u64;
    pub fn file_id(&self) -> u32;
    
    pub async fn append(&mut self, data: &[u8], ts: u64) -> JdbResult<BlobPtr>;
    pub async fn sync(&mut self) -> JdbResult<()>;
}
```

### VlogReader

```rust
pub struct VlogReader {
    file: File,
}

impl VlogReader {
    pub async fn open(path: impl AsRef<Path>) -> JdbResult<Self>;
    pub async fn read(&self, ptr: &BlobPtr) -> JdbResult<Vec<u8>>;
}
```

## 存储格式

每个 Blob：
```
+--------+--------+--------+------------------+--------+
| len:u32| crc:u32| ts:u64 |    data: [u8]    | padding|
+--------+--------+--------+------------------+--------+
   4B       4B       8B        len bytes       to 4KB
```

- Append-only 追加写入
- 返回 BlobPtr (file_id, offset, len) 供索引存储
- 页对齐写入，支持 Direct I/O
- CRC32 校验数据完整性

## 设计要点

- KV 分离：大对象存 Vlog，索引只存 BlobPtr
- 追加写：无随机写，适合 SSD
- 后续可实现 GC（标记-整理）回收空间
