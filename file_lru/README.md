[English](#en) | [中文](#zh)

---

<a id="en"></a>

# file_lru : Efficient file handle caching for WAL random reads

## Table of Contents

- [Project Overview](#project-overview)
- [Usage](#usage)
- [Features](#features)
- [Design](#design)
- [Tech Stack](#tech-stack)
- [Directory Structure](#directory-structure)
- [API Reference](#api-reference)
- [Historical Context](#historical-context)

---

## Project Overview

file_lru provides efficient file handle caching for WAL (Write-Ahead Log) random reads. It implements LRU (Least Recently Used) cache strategy to manage file handles, reducing I/O overhead and improving read performance.

The library integrates with compio async runtime, offering zero-copy data reading capabilities. It maintains file handles in memory cache, automatically opening files on demand and evicting least recently used files when cache reaches capacity.

---

## Usage

```rust
use file_lru::FileLru;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> std::io::Result<()> {
  // Create FileLru with directory and cache size
  let mut file_lru = FileLru::new("/path/to/wal/dir", 100);

  // Read data from file_id at offset into buffer (zero-copy)
  let mut buffer = vec![0u8; 4096];
  file_lru.read_into(12345, buffer, 1024).await?;

  // Evict file from cache (keeps file on disk)
  file_lru.evict(12345);

  // Remove file from cache and delete from disk
  file_lru.rm(12345);

  Ok(())
}
```

---

## Features

- **LRU Cache**: Automatically manages file handles using least recently used eviction policy
- **Zero-Copy Reading**: Direct data transfer to caller buffer without intermediate copies
- **Async I/O**: Built on compio async runtime for efficient non-blocking operations
- **Background Deletion**: File deletion runs in background tasks to avoid blocking
- **Minimum Cache Protection**: Enforces minimum cache size of 16 to prevent thrashing

---

## Design

### Cache Workflow

```mermaid
graph TD
  A[Read Request] --> B{File in Cache?}
  B -->|Yes| C[Use Cached Handle]
  B -->|No| D[Open File]
  D --> E[Insert into Cache]
  E --> F{Cache Full?}
  F -->|Yes| G[Evict LRU Entry]
  F -->|No| H[Proceed]
  G --> H
  C --> H
  H --> I[Read Data]
  I --> J[Return Buffer]
```

### Module Flow

When read request arrives:

1. Check if file handle exists in cache
2. If cached, reuse handle directly
3. If not cached, open file and insert into cache
4. If cache full, evict least recently used entry
5. Perform zero-copy read into caller buffer
6. Return buffer to caller

File removal operations:

- `evict()`: Removes handle from cache, disk file remains
- `rm()`: Removes handle from cache, spawns background task to delete disk file

---

## Tech Stack

### Core Dependencies

- **compio** (0.17.0): Async I/O runtime with zero-copy support
- **compio-fs** (0.10.0): Async file system operations
- **hashlink** (0.11.0): LRU cache implementation
- **ider** (0.1.7): Path generation utilities
- **jdb_lru** (0.1.8): LRU data structure
- **log** (0.4.29): Logging framework

### Development Dependencies

- **aok** (0.1.18): Error handling utilities
- **log_init** (0.1.34): Logging initialization
- **static_init** (1.0.4): Static initialization

---

## Directory Structure

```
file_lru/
├── src/
│   └── lib.rs          # Core FileLru implementation
├── tests/
│   └── main.rs         # Test cases
├── readme/
│   ├── en.md           # English documentation
│   └── zh.md           # Chinese documentation
├── Cargo.toml          # Package configuration
├── README.mdt          # Documentation template
└── test.sh             # Test script
```

---

## API Reference

### FileLru

WAL block cache with file handle cache.

#### Fields

- `dir: PathBuf` - Directory path for WAL files
- `cache: Lru<u64, File>` - LRU cache mapping file IDs to file handles

#### Methods

##### `new(dir: impl Into<PathBuf>, cache_size: usize) -> Self`

Create FileLru instance from directory path and cache size.

**Parameters:**
- `dir`: Directory path containing WAL files
- `cache_size`: Maximum number of file handles to cache (minimum 16)

**Returns:**
- `FileLru` instance

##### `async fn read_into<B: IoBufMut>(&mut self, file_id: u64, buf: B, offset: u64) -> std::io::Result<B>`

Read data at offset into caller buffer with zero-copy.

**Parameters:**
- `file_id`: Unique file identifier
- `buf`: Caller-provided buffer for data
- `offset`: Byte offset in file to start reading

**Returns:**
- `Result<B>`: Filled buffer or I/O error

##### `fn evict(&mut self, file_id: u64)`

Remove file handle from cache without deleting disk file.

**Parameters:**
- `file_id`: File identifier to evict

##### `fn rm(&mut self, file_id: u64)`

Remove file handle from cache and delete from disk in background.

**Parameters:**
- `file_id`: File identifier to remove

---

## Historical Context

The concept of LRU caching traces back to 1960s when computer scientists developed algorithms to manage limited memory resources efficiently. The LRU algorithm was formalized in 1965 by Peter J. Denning in his work on virtual memory systems.

Write-Ahead Logging (WAL) became prominent in database systems during 1970s and 1980s. The technique ensures data integrity by writing changes to log before applying to main storage. Modern databases like PostgreSQL, SQLite, and MySQL all rely on WAL for durability and crash recovery.

The combination of LRU caching with WAL optimization represents decades of evolution in storage systems. Early implementations used simple file handle pools, while modern solutions leverage async I/O and zero-copy techniques to maximize throughput on NVMe SSDs and high-performance storage devices.

Rust's ownership model and async capabilities make it ideal for building such performance-critical systems, providing memory safety without runtime overhead.

---

## About

This project is an open-source component of [js0.site ⋅ Refactoring the Internet Plan](https://js0.site).

We are redefining the development paradigm of the Internet in a componentized way. Welcome to follow us:

* [Google Group](https://groups.google.com/g/js0-site)
* [js0site.bsky.social](https://bsky.app/profile/js0site.bsky.social)

---

<a id="zh"></a>

# file_lru : 高效 WAL 随机读取的文件句柄缓存

## 目录

- [项目概述](#项目概述)
- [使用示例](#使用示例)
- [特性](#特性)
- [设计思路](#设计思路)
- [技术栈](#技术栈)
- [目录结构](#目录结构)
- [API 说明](#api-说明)
- [历史背景](#历史背景)

---

## 项目概述

file_lru 为 WAL（预写日志）随机读取提供高效文件句柄缓存。它采用 LRU（最近最少使用）缓存策略管理文件句柄，降低 I/O 开销，提升读取性能。

该库集成 compio 异步运行时，支持零拷贝数据读取。它在内存中维护文件句柄缓存，按需自动打开文件，当缓存达到容量上限时淘汰最近最少使用的文件。

---

## 使用示例

```rust
use file_lru::FileLru;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> std::io::Result<()> {
  // 创建 FileLru 实例，指定目录和缓存大小
  let mut file_lru = FileLru::new("/path/to/wal/dir", 100);

  // 从 file_id 的 offset 处读取数据到缓冲区（零拷贝）
  let mut buffer = vec![0u8; 4096];
  file_lru.read_into(12345, buffer, 1024).await?;

  // 从缓存移除文件（保留磁盘文件）
  file_lru.evict(12345);

  // 从缓存移除文件并从磁盘删除
  file_lru.rm(12345);

  Ok(())
}
```

---

## 特性

- **LRU 缓存**：采用最近最少使用淘汰策略自动管理文件句柄
- **零拷贝读取**：直接将数据传输到调用者缓冲区，无需中间拷贝
- **异步 I/O**：基于 compio 异步运行时，高效非阻塞操作
- **后台删除**：文件删除在后台任务中执行，避免阻塞
- **最小缓存保护**：强制最小缓存大小 16，防止缓存颠簸

---

## 设计思路

### 缓存流程

```mermaid
graph TD
  A[读取请求] --> B{文件在缓存中?}
  B -->|是| C[使用缓存句柄]
  B -->|否| D[打开文件]
  D --> E[插入缓存]
  E --> F{缓存已满?}
  F -->|是| G[淘汰 LRU 条目]
  F -->|否| H[继续]
  G --> H
  C --> H
  H --> I[读取数据]
  I --> J[返回缓冲区]
```

### 模块调用流程

读取请求到达时：

1. 检查文件句柄是否存在于缓存
2. 若已缓存，直接复用句柄
3. 若未缓存，打开文件并插入缓存
4. 若缓存已满，淘汰最近最少使用的条目
5. 执行零拷贝读取到调用者缓冲区
6. 返回缓冲区给调用者

文件移除操作：

- `evict()`：从缓存移除句柄，磁盘文件保留
- `rm()`：从缓存移除句柄，启动后台任务删除磁盘文件

---

## 技术栈

### 核心依赖

- **compio** (0.17.0)：支持零拷贝的异步 I/O 运行时
- **compio-fs** (0.10.0)：异步文件系统操作
- **hashlink** (0.11.0)：LRU 缓存实现
- **ider** (0.1.7)：路径生成工具
- **jdb_lru** (0.1.8)：LRU 数据结构
- **log** (0.4.29)：日志框架

### 开发依赖

- **aok** (0.1.18)：错误处理工具
- **log_init** (0.1.34)：日志初始化
- **static_init** (1.0.4)：静态初始化

---

## 目录结构

```
file_lru/
├── src/
│   └── lib.rs          # FileLru 核心实现
├── tests/
│   └── main.rs         # 测试用例
├── readme/
│   ├── en.md           # 英文文档
│   └── zh.md           # 中文文档
├── Cargo.toml          # 包配置
├── README.mdt          # 文档模板
└── test.sh             # 测试脚本
```

---

## API 说明

### FileLru

WAL 块缓存（含文件句柄缓存）。

#### 字段

- `dir: PathBuf` - WAL 文件目录路径
- `cache: Lru<u64, File>` - LRU 缓存，映射文件 ID 到文件句柄

#### 方法

##### `new(dir: impl Into<PathBuf>, cache_size: usize) -> Self`

从目录路径和缓存大小创建 FileLru 实例。

**参数：**
- `dir`：包含 WAL 文件的目录路径
- `cache_size`：最大缓存文件句柄数量（最小 16）

**返回：**
- `FileLru` 实例

##### `async fn read_into<B: IoBufMut>(&mut self, file_id: u64, buf: B, offset: u64) -> std::io::Result<B>`

在偏移处读取数据到调用者缓冲区（零拷贝）。

**参数：**
- `file_id`：唯一文件标识符
- `buf`：调用者提供的数据缓冲区
- `offset`：文件中开始读取的字节偏移

**返回：**
- `Result<B>`：填充后的缓冲区或 I/O 错误

##### `fn evict(&mut self, file_id: u64)`

从缓存移除文件句柄，不删除磁盘文件。

**参数：**
- `file_id`：要淘汰的文件标识符

##### `fn rm(&mut self, file_id: u64)`

从缓存移除文件句柄，并在后台删除磁盘文件。

**参数：**
- `file_id`：要移除的文件标识符

---

## 历史背景

LRU 缓存概念可追溯至 1960 年代，当时计算机科学家开发算法以高效管理有限内存资源。LRU 算法由 Peter J. Denning 于 1965 年在其虚拟内存系统研究中正式提出。

预写日志（WAL）在 1970 年代和 1980 年代数据库系统中变得重要。该技术通过在写入主存储前先将更改记录到日志，确保数据完整性。现代数据库如 PostgreSQL、SQLite 和 MySQL 均依赖 WAL 实现持久性和崩溃恢复。

LRU 缓存与 WAL 优化的结合代表了存储系统数十年的演进。早期实现使用简单文件句柄池，现代方案则利用异步 I/O 和零拷贝技术，在 NVMe SSD 和高性能存储设备上最大化吞吐量。

Rust 的所有权模型和异步能力使其成为构建此类性能关键系统的理想选择，在无运行时开销的前提下提供内存安全。

---

## 关于

本项目为 [js0.site ⋅ 重构互联网计划](https://js0.site) 的开源组件。

我们正在以组件化的方式重新定义互联网的开发范式，欢迎关注：

* [谷歌邮件列表](https://groups.google.com/g/js0-site)
* [js0site.bsky.social](https://bsky.app/profile/js0site.bsky.social)
