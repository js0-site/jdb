# jdb_wal - Write-Ahead Log / 预写日志

## 概述 / Overview

WAL 用于崩溃恢复，在数据写入 B+ Tree 和 VLog 前先记录到 WAL。

## 记录格式 / Record Format

```
┌─────────┬─────────┬──────┬─────────┬─────────┬─────┬─────────┬─────┐
│ len(4B) │ crc(4B) │ type │ db_id   │ key_len │ key │ val_len │ val │
│         │         │ (1B) │  (8B)   │  (4B)   │     │  (4B)   │     │
└─────────┴─────────┴──────┴─────────┴─────────┴─────┴─────────┴─────┘
```

- 对齐到 PAGE_SIZE (4KB)
- CRC32 校验 type 到 val 部分

## 记录类型 / Record Types

| Type | Value | Description |
|------|-------|-------------|
| Put | 1 | 写入操作 |
| Del | 2 | 删除操作 |
| Commit | 3 | 提交标记 |

## API

```rust
impl Wal {
  async fn open(dir) -> Result<Self>;
  async fn append(&self, rec: &Record) -> Result<()>;
  async fn recover(&self) -> Result<Vec<Record>>;
  async fn sync(&self) -> Result<()>;
  async fn rotate(&self) -> Result<()>;
  async fn clear(&self) -> Result<()>;
}
```

## 恢复流程 / Recovery Flow

1. 读取所有 WAL 文件（按 ID 排序）
2. 解析每条记录，校验 CRC
3. 遇到 Incomplete 停止（部分写入）
4. 返回有效记录列表
5. 重放到数据库

## 文件布局 / File Layout

```
<dir>/
├── 00000001.wal
├── 00000002.wal
└── ...
```

- 单文件最大 64MB，超过后轮转
- 文件名为 8 位数字 ID

## 依赖 / Dependencies

- jdb_alloc: 对齐内存
- jdb_fs: 异步 Direct I/O
