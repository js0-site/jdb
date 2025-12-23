# jdb_vlog - 值日志模块

定位: KV 分离的大值存储，参考 Titan/BlobDB 设计。

## 依赖

```toml
jdb_alloc = { path = "../jdb_alloc" }
jdb_fs = { path = "../jdb_fs" }
jdb_layout = { path = "../jdb_layout" }
jdb_compress = { path = "../jdb_compress" }
```

## 模块结构

```
jdb_vlog/src/
├── lib.rs      # 模块导出
├── writer.rs   # VLog 写入器
├── reader.rs   # VLog 读取器
└── meta.rs     # 文件元数据 (GC 用)
```

## 设计背景

### 写放大对比

| 值大小 | 无分离 | 有分离 |
|-------|-------|-------|
| 100B  | 1x    | 1x    |
| 1KB   | 10x   | 1.5x  |
| 10KB  | 100x  | 1.5x  |

## 核心类型

### VLogWriter

```rust
pub struct VLogWriter {
  dir: PathBuf,
  file: File,
  file_id: u32,
  offset: u64,
  buf: AlignedBuf,
}

impl VLogWriter {
  pub async fn create(dir: &Path, file_id: u32) -> R<Self>;
  pub async fn append(&mut self, data: &[u8]) -> R<BlobPtr>;
  pub async fn flush(&mut self) -> R<()>;
  pub async fn roll(&mut self) -> R<()>;
}
```

### VLogReader

```rust
pub struct VLogReader {
  dir: PathBuf,
  cache: LruCache<u32, File>,
}

impl VLogReader {
  pub fn new(dir: &Path, cache_size: usize) -> Self;
  pub async fn read(&mut self, ptr: &BlobPtr) -> R<Vec<u8>>;
  pub async fn read_batch(&mut self, ptrs: &[BlobPtr]) -> R<Vec<Vec<u8>>>;
}
```

### VLogMeta

```rust
pub struct VLogMeta {
  pub file_id: u32,
  pub size: u64,
  pub valid_size: u64,
  pub create_time: u64,
}

impl VLogMeta {
  #[inline]
  pub fn garbage_ratio(&self) -> f64 {
    1.0 - (self.valid_size as f64 / self.size as f64)
  }
}
```

## GC 策略: Level Merge

Compaction 时顺带处理：

```
Compaction 流程:
  ├─► 遍历数据页中的 BlobPtr
  ├─► 检查 BlobPtr 是否有效
  ├─► 有效 → 从旧 VLog 读取，写入新 VLog
  ├─► 更新数据页中的 BlobPtr
  └─► 所有引用清零的旧 VLog 可删除
```

## 设计要点

1. **Append-only**: VLog 文件只追加写
2. **Level Merge GC**: Compaction 时顺带 GC
3. **CRC 校验**: 每条记录独立校验
4. **文件滚动**: 达到 max_file_size 时滚动
5. **压缩支持**: 可选 LZ4/Zstd 压缩
