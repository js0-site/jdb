[English](#en) | [中文](#zh)

---

<a id="en"></a>

# zbin: Zero-Copy Binary Utility for Async IO

**zbin** is a lightweight compatibility layer designed for modern asynchronous I/O runtimes like `compio`. It bridges the gap between various data types (references, copy-on-write, ref-counted) and the strict ownership requirements of completion-based I/O (io_uring/IOCP).

## Table of Contents

- [Features](#features)
- [Usage](#usage)
- [Design](#design)
- [Tech Stack](#tech-stack)
- [Directory Structure](#directory-structure)
- [API Reference](#api-reference)
- [History](#history)

## Features

- **Unified Interface**: Treat `Vec<u8>`, `String`, `&[u8]`, `Bytes`, `Rc`, and `Arc` uniformly.
- **Zero-Copy Optimization**: Automatically leverages zero-copy paths for owned types (`Vec`, `String`) and reference-counted types (`Rc`, `Arc`).
- **Flexible Ownership**: Seamlessly handles ownership transfer required by `io_uring`.
- **Compio Integration**: Native support for `compio::buf::IoBuf`.

## Usage

```rust
use zbin::Bin;
use compio::io::AsyncWriteAtExt;
use compio::fs::File;

#[compio::main]
async fn main() {
    let file = File::create("/tmp/test.txt").await.unwrap();
    
    // 1. Zero-copy write (Vec moves ownership)
    let data = vec![1, 2, 3];
    file.write_all_at(data.io(), 0).await.unwrap();

    // 2. Referenced write (Automatically clones or copies)
    let static_data = b"hello";
    file.write_all_at(static_data.io(), 3).await.unwrap();
}
```

## Design

The core challenge in modern async I/O (specifically `io_uring` and IOCP) is that the kernel requires ownership of the memory buffer while the I/O operation is in flight. Standard references (`&[u8]`) cannot be safely passed to the kernel without ensuring the data remains valid indefinitely.

**zbin** solves this by defining a `Bin` trait that standardizes the conversion to an "Owned Buffer" (`IoBuf`).

### Conversion Flow

```mermaid
graph TD
    Input[Input Data] -->|Bin::io| Check{Type Check}
    
    Check -->|Vec/String| Move[Move Ownership]
    Check -->|Rc/Arc| RefCnt[Clone Ref Count]
    Check -->|"&[u8]/&str"| Copy[Deep Copy to Box]
    Check -->|Cow| CowChk{Is Owned?}
    
    CowChk -->|Yes| Move
    CowChk -->|No| Copy
    
    Move --> IoBuf[IoBuf Ready]
    RefCnt --> IoBuf
    Copy --> IoBuf
    
    IoBuf --> Kernel[Async Runtime / Kernel]
```

## Tech Stack

- **Rust**: Language of choice for memory safety.
- **Compio**: The target async runtime.
- **Hipstr**: Support for efficient string handling.
- **Bytes**: Optional integration with the `bytes` crate.

## Directory Structure

```text
zbin/
├── src/
│   └── lib.rs      # Core trait definition and implementations
├── tests/          # Integration tests
└── Cargo.toml      # Dependency management
```

## API Reference

### `Bin<'a>` Trait

- `as_slice(&self) -> &[u8]`: accessing data synchronously (e.g., for CRC calculation).
- `io(self) -> Self::Io`: Consumes the value and returns a type implementing `IoBuf`.

| Input Type | Conversion Strategy | Overhead |
|------------|---------------------|----------|
| `Vec<u8>` | Move | Zero |
| `String` | Move (`into_bytes`) | Zero |
| `Rc<[u8]>` | Clone Ptr | O(1) |
| `&[u8]` | Allocate `Box<[u8]>` | O(N) |

## History

In the early days of gigabit networking, a common rule of thumb was "1 Hz per bit per second" — meaning a 1 GHz CPU was needed just to saturate a 1 Gbps link, largely due to the overhead of copying data between kernel and user space. This "copy problem" plagued operating systems for decades. Initial solutions like `sendfile` helped static file serving, but general-purpose I/O remained copy-heavy. 

The introduction of `io_uring` in Linux 5.1 marked a paradigm shift, enabling true zero-copy submission queues. However, this required a new memory model where the kernel "owns" user-space buffers during execution. **zbin** was born from this necessity, creating a bridge to allow ergonomic Rust types to interact safe and efficiently with this strict ownership model.

---

## About

This project is an open-source component of [js0.site ⋅ Refactoring the Internet Plan](https://js0.site).

We are redefining the development paradigm of the Internet in a componentized way. Welcome to follow us:

* [Google Group](https://groups.google.com/g/js0-site)
* [js0site.bsky.social](https://bsky.app/profile/js0site.bsky.social)

---

<a id="zh"></a>

# zbin: 适用于异步 IO 的零拷贝二进制工具

**zbin** 是一个为现代异步 I/O 运行时（如 `compio`）设计的轻量级兼容层。它桥接了各种数据类型（引用、写时复制、引用计数）与基于完成的 I/O 模型（io_uring/IOCP）之间严格的所有权要求。

## 目录

- [功能特性](#功能特性)
- [使用演示](#使用演示)
- [设计思路](#设计思路)
- [技术堆栈](#技术堆栈)
- [目录结构](#目录结构)
- [API 参考](#api-参考)
- [历史轶事](#历史轶事)

## 功能特性

- **统一接口**：统一处理 `Vec<u8>`、`String`、`&[u8]`、`Bytes`、`Rc` 和 `Arc`。
- **零拷贝优化**：自动利用所有权类型（`Vec`、`String`）和引用计数类型（`Rc`、`Arc`）的零拷贝路径。
- **灵活的权属**：无缝处理 `io_uring` 所需的所有权转移。
- **Compio 集成**：原生支持 `compio::buf::IoBuf`。

## 使用演示

```rust
use zbin::Bin;
use compio::io::AsyncWriteAtExt;
use compio::fs::File;

#[compio::main]
async fn main() {
    let file = File::create("/tmp/test.txt").await.unwrap();
    
    // 1. 零拷贝写入 (Vec 所有权转移)
    let data = vec![1, 2, 3];
    file.write_all_at(data.io(), 0).await.unwrap();

    // 2. 引用写入 (自动进行 Clone 或 Copy)
    let static_data = b"hello";
    file.write_all_at(static_data.io(), 3).await.unwrap();
}
```

## 设计思路

现代异步 I/O（特别是 `io_uring` 和 IOCP）的核心挑战在于，内核要求在 I/O 操作进行期间拥有内存缓冲区的所有权。标准的引用（`&[u8]`）不能安全地传递给内核，因为无法通过编译器保证数据在异步操作完成前一直有效。

**zbin** 通过定义 `Bin` trait 解决了这个问题，标准化了向“所有权缓冲区”（`IoBuf`）的转换。

### 转换流程

```mermaid
graph TD
    Input[输入数据] -->|Bin::io| Check{类型检查}
    
    Check -->|Vec/String| Move[移动所有权]
    Check -->|Rc/Arc| RefCnt[克隆引用计数]
    Check -->|"&[u8]/&str"| Copy[深拷贝到 Box]
    Check -->|Cow| CowChk{拥有所有权?}
    
    CowChk -->|是| Move
    CowChk -->|否| Copy
    
    Move --> IoBuf[IoBuf 就绪]
    RefCnt --> IoBuf
    Copy --> IoBuf
    
    IoBuf --> Kernel[异步运行时 / 内核]
```

## 技术堆栈

- **Rust**：内存安全的首选语言。
- **Compio**：目标异步运行时。
- **Hipstr**：支持高效的字符串处理。
- **Bytes**：可选集成 `bytes` crate。

## 目录结构

```text
zbin/
├── src/
│   └── lib.rs      # 核心 trait 定义与实现
├── tests/          # 集成测试
└── Cargo.toml      # 依赖管理
```

## API 参考

### `Bin<'a>` Trait

- `as_slice(&self) -> &[u8]`：同步访问数据（例如用于 CRC 计算）。
- `io(self) -> Self::Io`：消费值并返回实现 `IoBuf` 的类型。

| 输入类型 | 转换策略 | 开销 |
|----------|----------|------|
| `Vec<u8>` | 移动 (Move) | 零 |
| `String` | 移动 (`into_bytes`) | 零 |
| `Rc<[u8]>` | 克隆指针 | O(1) |
| `&[u8]` | 分配 `Box<[u8]>` | O(N) |

## 历史轶事

在千兆网络的早期，“1 Hz 对应 1 bit 每秒”是一个通用的经验法则——意味着仅仅是为了跑满 1 Gbps 的链路，就需要 1 GHz 的 CPU，这主要是由于数据在内核空间和用户空间之间复制的开销。这个“复制问题”困扰了操作系统几十年。最初的解决方案如 `sendfile` 帮助了静态文件服务，但通用 I/O 仍然充斥着繁重的内存复制。

Linux 5.1 中 `io_uring` 的引入标志着范式的转变，实现了真正的零拷贝提交队列。然而，这通过一个新的内存模型来实现：内核在执行期间“拥有”用户空间的缓冲区。**zbin** 正是诞生于这种需求，它建立了一座桥梁，允许符合人体工程学的 Rust 类型安全且高效地与这种严格的所有权模型进行交互。

---

## 关于

本项目为 [js0.site ⋅ 重构互联网计划](https://js0.site) 的开源组件。

我们正在以组件化的方式重新定义互联网的开发范式，欢迎关注：

* [谷歌邮件列表](https://groups.google.com/g/js0-site)
* [js0site.bsky.social](https://bsky.app/profile/js0site.bsky.social)
