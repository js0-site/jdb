# JDB FSST 库

这是一个用 Rust 实现的 FSST (Fast Static Symbol Table，快速静态符号表) 压缩算法库。它是为了高效地压缩和解压缩字符串数据而设计的，特别适用于列式存储（如 JDB 中的用法）。

## 简介

FSST 是一种轻量级的压缩方案，它构建一个静态符号表，将频繁出现的字节序列（符号）映射到较短的代码（通常是单字节）。这允许在保持解压缩速度极快的同时实现良好的压缩比。

## 主要特性

- **高性能**: 针对现代处理器进行了优化，并在解压缩过程中使用了 SIMD 指令（如果适用）。
- **字符串优化**: 专门针对字符串数据进行设计，支持带有偏移量（Offsets）的字符串数组压缩，类似于 Apache Arrow 的内存布局。
- **随机访问**: 压缩格式和 API 设计支持高效的字符串处理。
- **双模式**: 当输入数据太小或不适合 FSST 压缩时，自动回退到无压缩模式（仅复制），确保即使在最坏情况下也能正常工作。

## 公共 API

主要有两个公共函数：

### `compress`

```rust
pub fn compress<T: OffsetSizeTrait>(
  symbol_table: &mut [u8],
  in_buf: &[u8],
  in_offsets_buf: &[T],
  out_buf: &mut Vec<u8>,
  out_offsets_buf: &mut Vec<T>,
) -> io::Result<()>
```

- **用途**: 压缩给定的字符串数据。
- **参数**:
  - `symbol_table`: 用于存储生成的符号表的缓冲区。
  - `in_buf`: 输入的所有字符串连接成的字节切片。
  - `in_offsets_buf`: 输入字符串的偏移量数组（支持 `i32` 或 `i64`）。
  - `out_buf`: 用于存储压缩输出数据的向量。
  - `out_offsets_buf`: 用于存储输出偏移量的向量。

### `decompress`

```rust
pub fn decompress<T: OffsetSizeTrait>(
  symbol_table: &[u8],
  in_buf: &[u8],
  in_offsets_buf: &[T],
  out_buf: &mut Vec<u8>,
  out_offsets_buf: &mut Vec<T>,
) -> io::Result<()>
```

- **用途**: 解压缩 FSST 压缩的数据。
- **参数**:
  - `symbol_table`: 压缩时生成的符号表。
  - `in_buf`: 压缩后的数据。
  - `in_offsets_buf`: 压缩数据的偏移量。
  - `out_buf`: 用于存储解压后原始数据的向量。
  - `out_offsets_buf`: 用于存储解压后偏移量的向量。

## 内部实现细节

### 符号表构建
算法首先对输入数据进行采样，统计字节和字节对的频率，然后迭代构建符号表。它使用启发式方法（如增益计算）来选择能够最大化压缩率的符号。

### 编码格式
压缩后的数据包含一个头部，其中包含：
- 魔数 (`jdbFsst1`)
- 编码器开关状态
- 符号表元数据（后缀限制、终止符、符号数量）

数据流中包含转义码（ESC），用于处理未在符号表中表示的原始字节。

## 测试
本项目包含一套并在 `tests` 模块中定义的测试用例，涵盖了基本的压缩/解压正确性以及 64 位偏移量的支持。