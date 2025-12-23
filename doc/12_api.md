# jdb_api 模块设计 / Module Design

## 概述 / Overview

`jdb_api` 是 L5 层的高级 Rust SDK，为开发者提供简洁的异步接口。封装 Runtime 细节，提供类似 KV 存储的 API。

`jdb_api` is the high-level Rust SDK of L5 layer, providing simple async interface for developers. Encapsulates Runtime details, provides KV-store-like API.

## 依赖 / Dependencies

```
jdb_api
├── jdb_runtime  (dispatcher)
└── jdb_comm     (types)
```

## 核心结构 / Core Structures

### JdbClient

```rust
pub struct JdbClient {
  rt: Runtime,  // 内部运行时 / Internal runtime
}
```

## 核心接口 / Core APIs

### 生命周期 / Lifecycle

```rust
// 打开数据库 / Open database
pub fn open(path: impl AsRef<Path>) -> Result<Self>

// 指定 worker 数量打开 / Open with worker count
pub fn open_with_workers(path: impl AsRef<Path>, workers: usize) -> Result<Self>

// 关闭客户端 / Close client
pub fn close(mut self)
```

### 数据操作 / Data Operations

```rust
// 写入键值 / Put key-value
pub async fn put(&self, table: &[u8], key: &[u8], val: &[u8]) -> Result<()>

// 读取值 / Get value
pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>

// 删除键 / Delete key
pub async fn delete(&self, table: &[u8], key: &[u8]) -> Result<bool>

// 范围扫描 / Range scan
pub async fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>

// 刷新到磁盘 / Flush to disk
pub async fn flush(&self) -> Result<()>
```

## 使用示例 / Usage Example

```rust
use jdb_api::JdbClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 打开数据库 / Open database
    let client = JdbClient::open("/tmp/mydb")?;

    // 写入 / Put
    client.put(b"users", b"user:1", b"Alice").await?;

    // 读取 / Get
    if let Some(val) = client.get(b"user:1").await? {
        println!("user:1 = {}", String::from_utf8_lossy(&val));
    }

    // 范围扫描 / Range scan
    let users = client.range(b"user:", b"user:\xff").await?;
    for (k, v) in users {
        println!("{} = {}", 
            String::from_utf8_lossy(&k),
            String::from_utf8_lossy(&v));
    }

    // 删除 / Delete
    client.delete(b"users", b"user:1").await?;

    // 刷新 / Flush
    client.flush().await?;

    // 关闭 / Close
    client.close();
    Ok(())
}
```

## 错误类型 / Error Types

```rust
pub enum ApiError {
  NotConnected,
  Runtime(RuntimeError),
  InvalidKey,
  InvalidValue,
}
```

## 设计特点 / Design Features

1. **二进制安全**: table/key/val 都支持任意二进制 / Binary-safe: table/key/val support arbitrary binary
2. **异步接口**: 所有 IO 操作都是 async / Async interface: all IO operations are async
3. **简洁 API**: 隐藏 Runtime 复杂性 / Simple API: hides Runtime complexity
4. **零配置**: 默认配置即可使用 / Zero-config: works with defaults

## 测试覆盖 / Test Coverage

- `test_client_basic`: 基本读写删除 / Basic read/write/delete
- `test_client_range`: 范围扫描 / Range scan
- `test_client_flush`: 刷新操作 / Flush operation
