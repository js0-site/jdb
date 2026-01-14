# JDB FSST 库

这是一个高性能的 Rust 实现的 FSST (Fast Static Symbol Table，快速静态符号表) 压缩算法库。该库专门为字符串数据的高效压缩和解压缩而设计，特别适用于列式存储系统（如 JDB）。

## 概述

FSST 是一种轻量级的压缩方案，它构建一个静态符号表，将频繁出现的字节序列（符号）映射到较短的代码（通常是单字节）。这允许在保持极快解压缩速度的同时实现良好的压缩比。

## 主要特性

- **高性能**: 针对现代处理器进行了优化，在解压缩过程中使用 SIMD 风格的批处理。
- **字符串优化**: 专门针对字符串数据进行设计，支持带有偏移量数组的字符串压缩，类似于 Apache Arrow 的内存布局。
- **随机访问**: 压缩格式和 API 设计支持高效的字符串随机访问。
- **自动回退**: 当输入数据太小或不适合 FSST 压缩时，自动回退到无压缩模式（直接复制）。
- **批量操作**: 高效地在单个操作中压缩/解压缩多个字符串。
- **多语言支持**: 完美支持英文和中文文本数据。

## 公共 API

### `encode`

```rust
pub fn encode(
  symbol_table: &mut [u8],
  in_buf: &[u8],
  in_offsets_buf: &[usize],
  out_buf: &mut Vec<u8>,
  out_offsets_buf: &mut Vec<usize>,
) -> io::Result<()>
```

使用 FSST 压缩字符串数据。

**参数:**
- `symbol_table`: 用于存储生成的符号表的缓冲区（必须为 `SYMBOL_TABLE_SIZE` 字节）
- `in_buf`: 所有输入字符串连接成的字节切片
- `in_offsets_buf`: 字符串偏移量数组（每个偏移量指向 `in_buf` 中字符串的起始位置）
- `out_buf`: 用于存储压缩输出数据的向量
- `out_offsets_buf`: 用于存储输出偏移量的向量

**行为:**
- 如果输入大小 < `LEAST_INPUT_SIZE` (32KB)，数据将被直接复制而不进行压缩
- 否则，从采样的输入数据构建符号表并压缩数据

### `decode`

```rust
pub fn decode(
  symbol_table: &[u8],
  in_buf: &[u8],
  in_offsets_buf: &[usize],
  out_buf: &mut Vec<u8>,
  out_offsets_buf: &mut Vec<usize>,
) -> io::Result<()>
```

解压缩 FSST 压缩的数据。

**参数:**
- `symbol_table`: 压缩时生成的符号表
- `in_buf`: 压缩后的数据
- `in_offsets_buf`: 压缩数据的偏移量
- `out_buf`: 用于存储解压后原始数据的向量
- `out_offsets_buf`: 用于存储解压后偏移量的向量

**行为:**
- 读取头部以确定数据是否被压缩
- 如果未压缩，直接复制数据
- 如果已压缩，使用符号表解码数据

## 实现细节

### 符号表构建

算法首先对输入数据进行采样，构建字节和字节对的频率直方图，然后迭代构建符号表。它使用启发式方法（如增益计算）来选择能够最大化压缩率的符号。

### 编码格式

符号表头部包含：
- 魔数（未显式存储，但头部格式固定）
- 编码器开关标志（第 24 位的 1 位）
- 后缀限制（8 位）
- 终止符字节（8 位）
- 符号数量（8 位）

压缩数据流使用转义码（ESC = 255）来处理符号表中未表示的原始字节。

### 优化技术

- **符号重排序**: 构建后，符号按长度重新排序（2, 3, 4, 5, 6, 7, 8, 1）以提高压缩性能
- **后缀限制**: 双字节代码被分成两个部分，以便在压缩期间实现提前退出优化
- **SIMD 风格解码**: 使用位掩码进行转义检测，一次处理 4 个字节
- **短代码查找**: 使用 64K 查找表快速检测 2 字节符号

## 常量

- `SYMBOL_TABLE_SIZE`: 2304 字节（8 头部 + 256*8 符号 + 256 长度）
- `LEAST_INPUT_SIZE`: 32KB - 触发压缩的最小输入大小
- `LEAST_INPUT_MAX_LEN`: 5 - FSST 有效的最小字符串长度
- `MAX_SYMBOL_LEN`: 8 - 表中符号的最大长度
- `CODE_BITS`: 9 - 编码使用的位数（最多支持 512 个符号）
- `CODE_BASE`: 256 - 前 256 个编码代表单字节

## 使用示例

```rust
use jdb_fsst::{encode, decode, SYMBOL_TABLE_SIZE};

// 准备输入数据（多个字符串）
let text = "你好\n 世界\nRust\nFSST\n 压缩";
let lines: Vec<&str> = text.lines().collect();

// 转换为缓冲区和偏移量
let mut in_buf = Vec::new();
let mut in_offsets = vec![0usize];
for line in lines {
    in_buf.extend_from_slice(line.as_bytes());
    in_offsets.push(in_buf.len());
}

// 压缩
let mut symbol_table = [0u8; SYMBOL_TABLE_SIZE];
let mut out_buf = vec![0u8; in_buf.len()];
let mut out_offsets = vec![0usize; in_offsets.len()];

encode(
    &mut symbol_table,
    &in_buf,
    &in_offsets,
    &mut out_buf,
    &mut out_offsets,
)?;

// 计算压缩率
let original_size = in_buf.len();
let compressed_size = out_offsets.last().copied().unwrap_or(0);
println!("原始大小: {} 字节, 压缩后: {} 字节, 压缩率: {:.2}%",
         original_size, compressed_size,
         (compressed_size as f64 / original_size as f64) * 100.0);

// 解压缩
let mut decode_buf = vec![0u8; out_buf.len() * 8];
let mut decode_offsets = vec![0usize; out_offsets.len()];

decode(
    &symbol_table,
    &out_buf,
    &out_offsets,
    &mut decode_buf,
    &mut decode_offsets,
)?;

// 验证解压缩结果
assert_eq!(in_buf, decode_buf[..in_buf.len()]);
```

## 测试

运行测试套件：

```bash
cargo test
```

项目包含全面的测试用例，验证：
- 压缩/解压缩循环的正确性
- 支持英文和中文文本数据
- 各种输入大小（1MB 和 2MB 测试）
- 压缩率验证