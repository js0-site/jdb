# jdb_wal - 预写日志模块

定位: Write Ahead Log，保证原子性与持久性。

## 依赖

```toml
jdb_alloc = { path = "../jdb_alloc" }
jdb_fs = { path = "../jdb_fs" }
jdb_layout = { path = "../jdb_layout" }
```

## 模块结构

```
jdb_wal/src/
├── lib.rs      # 模块导出 + Writer/Reader 实现
```

## 核心类型

### Record 格式 (16 + data bytes)

```
+--------+--------+--------+------------------+
| len:u32| crc:u32| lsn:u64|    data: [u8]    |
+--------+--------+--------+------------------+
   4B       4B       8B         len bytes

- len: 数据长度 (不含 header)
- crc: data 的 CRC32
- lsn: 日志序列号
- data: 原始数据
```

### Writer

```rust
pub struct Writer {
  file: File,
  buf: AlignedBuf,
  pos: usize,      // buf 内写入位置
  offset: u64,     // 文件写入偏移
  lsn: u64,        // 下一个 LSN
}

impl Writer {
  pub async fn create(path: impl AsRef<Path>) -> R<Self>;
  pub async fn open(path: impl AsRef<Path>) -> R<Self>;
  pub fn lsn(&self) -> u64;
  pub fn append(&mut self, data: &[u8]) -> R<u64>;
  pub async fn flush(&mut self) -> R<()>;
  pub async fn sync(&mut self) -> R<()>;
}
```

### Reader

```rust
pub struct Reader {
  file: File,
  buf: AlignedBuf,
  pos: usize,      // buf 内读取位置
  offset: u64,     // 文件读取偏移
  file_size: u64,
}

impl Reader {
  pub async fn open(path: impl AsRef<Path>) -> R<Self>;
  pub async fn next(&mut self) -> R<Option<(u64, Vec<u8>)>>;
}
```

## 写入流程

```
append(data)
  ├─► 计算 record_len = 16 + data.len()
  ├─► buf 空间不足 → flush()
  ├─► 写入 header: [len, crc32(data), lsn]
  ├─► 写入 data
  └─► lsn++, 返回当前 lsn

flush()
  ├─► pos == 0 → 直接返回
  ├─► 填充 buf 到 PAGE_SIZE 边界
  ├─► file.write_at(offset, buf)
  └─► offset += written, pos = 0

sync()
  ├─► flush()
  └─► file.sync()
```

## 恢复流程

```
next()
  ├─► 读取 header (16 bytes)
  │     ├─► len == 0 → 跳过 padding, 继续
  │     └─► EOF → 返回 None
  ├─► 读取 data (len bytes)
  ├─► CRC 校验失败 → 返回 None (截断)
  └─► 返回 Some((lsn, data))
```

## 设计要点

1. **批量写入**: append 只写缓冲区，flush 时批量落盘
2. **页对齐**: flush 时填充到 PAGE_SIZE 边界
3. **CRC 校验**: 每条记录独立校验，恢复时检测损坏
4. **LSN 递增**: 全局单调递增，用于恢复点定位
5. **简洁接口**: 只接收 `&[u8]`，序列化由上层负责
