[English](n) | [中文](#zh)

---

<a id="en"></a>


# jdb_pgm : Ultra-fast Learned Index for Sorted Keys

> A highly optimized, single-threaded Rust implementation of the Pgm-index (Piecewise Geometric Model index), designed for ultra-low latency lookups and minimal memory overhead.

![Benchmark](https://raw.githubusercontent.com/js0-site/jdb/refs/heads/main/jdb_pgm/svg/en.svg)

- [Introduction](#introduction)
- [Usage](#usage)
- [Performance](#performance)
- [Features](#features)
- [Design](#design)
- [Technology Stack](#technology-stack)
- [Directory Structure](#directory-structure)
- [API Reference](#api-reference)
- [History](#history)

---

## Introduction

`jdb_pgm` is a specialized reimplementation of the Pgm-index data structure. It approximates the distribution of sorted keys using piecewise linear models, enabling search operations with **O(log ε)** complexity.

This crate focuses on **single-threaded performance**, preparing for a "one thread per CPU" architecture. By removing concurrency overhead and optimizing memory layout (e.g., SIMD-friendly loops), it achieves statistically significant speedups over standard binary search and traditional tree-based indexes.

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
jdb_pgm = "0.3"
```

### Two Modes

**`Pgm<K>`** - Core index without data ownership (ideal for SSTable, mmap scenarios)

```rust
use jdb_pgm::Pgm;

fn main() {
  let data: Vec<u64> = (0..1_000_000).collect();

  // Build index from data reference
  let pgm = Pgm::new(&data, 32, true).unwrap();

  // Get predicted search range
  let (start, end) = pgm.predict_range(123_456);

  // Search in your own data store
  if let Ok(pos) = data[start..end].binary_search(&123_456) {
    println!("Found at index: {}", start + pos);
  }
}
```

**`PgmData<K>`** - Index with data ownership (convenient for in-memory use)

```rust
use jdb_pgm::PgmData;

fn main() {
  let data: Vec<u64> = (0..1_000_000).collect();

  // Build index and take ownership of data
  let index = PgmData::load(data, 32, true).unwrap();

  // Direct lookup
  if let Some(pos) = index.get(123_456) {
    println!("Found at index: {}", pos);
  }
}
```

### Feature Flags

- `data` (default): Enables `PgmData` struct with data ownership
- `bitcode`: Enables serialization via bitcode
- `key_to_u64`: Enables `key_to_u64()` helper for byte keys

## Performance

Based on internal benchmarks with 1,000,000 `u64` keys (jdb_pgm's Pgm does not own data, memory is index-only):

*   **~2.3x Faster** than standard Binary Search (17.85ns vs 40.89ns).
*   **~1.1x - 1.3x Faster** than [pgm_index](https://crates.io/crates/pgm_index) (17.85ns vs 20.13ns).
*   **~4.7x Faster** than BTreeMap (17.85ns vs 84.21ns).
*   **~2.2x Faster** than HashMap (17.85ns vs 39.99ns).
*   **1.01 MB Index Memory** for `ε=32` (pgm_index uses 8.35 MB).
*   Prediction Accuracy: jdb_pgm max error equals ε exactly, pgm_index max error is 8ε.

## 🆚 Comparison with `pgm_index`

This crate (`jdb_pgm`) is a specialized fork/rewrite of the original concept found in [`pgm_index`](https://crates.io/crates/pgm_index). While the original library aims for general-purpose usage with multi-threading support (Rayon), `jdb_pgm` takes a different approach:

### Key Differences Summary

| Feature | jdb_pgm | pgm_index |
|---------|---------------|-----------|
| Threading | Single-threaded | Multi-threaded (Rayon) |
| Segment Building | Shrinking Cone O(N) | Parallel Least Squares |
| Prediction Model | `slope * key + intercept` | `(key - intercept) / slope` |
| Prediction Accuracy | ε-bounded (guaranteed) | Heuristic (not guaranteed) |
| Memory | Arc-free, zero-copy | Arc<Vec<K>> wrapper |
| Data Ownership | Optional (`Pgm` vs `PgmData`) | Always owns data |
| Dependencies | Minimal | rayon, num_cpus, num-traits |

### Architectural Shift: Single-Threaded by Design

The original `pgm_index` introduces Rayon for parallel processing. However, in modern high-performance databases (like ScyllaDB or specialized engines), the **thread-per-core** architecture is often superior.

*   **One Thread, One CPU**: Removed all locking, synchronization, and thread-pool overhead.
*   **Deterministic Latency**: Without thread scheduling jitter, p99 latencies are significantly more stable.

### Segment Building Algorithm

**jdb_pgm: Shrinking Cone (Optimal PLA)**

The streaming algorithm guarantees that prediction error never exceeds ε, while least squares fitting provides no such guarantee.

```rust
// O(N) streaming algorithm with guaranteed ε-bound
while end < n {
  slope_lo = (idx - first_idx - ε) / dx
  slope_hi = (idx - first_idx + ε) / dx
  if min_slope > max_slope: break  // cone collapsed
  // Update shrinking cone bounds
}
slope = (min_slope + max_slope) / 2
```

**pgm_index: Parallel Least Squares**

```rust
// Divides data into fixed chunks, fits each with least squares
target_segments = optimal_segment_count_adaptive(data, epsilon)
segments = (0..target_segments).par_iter().map(|i| {
  fit_segment(&data[start..end], start)  // least squares fit
}).collect()
```

### Prediction Formula

**jdb_pgm**: `pos = slope * key + intercept`
- Direct forward prediction
- Uses FMA (Fused Multiply-Add) for precision

**pgm_index**: `pos = (key - intercept) / slope`
- Inverse formula (solving for x given y)
- Division is slower than multiplication
- Risk of division by zero when slope ≈ 0

### Core Implementation Upgrades

While based on the same Pgm theory, implementation details are significantly more aggressive:

*   **Eliminating Float Overhead**: Replaced expensive floating-point rounding operations (`round/floor`) with bitwise-based integer casting (`as isize + 0.5`).
*   **Transparent to Compiler**: Core loops refactored to remove dependencies that block LLVM's auto-vectorization, generating AVX2/AVX-512 instructions.
*   **Reducing Branch Misprediction**: Rewrote `predict` and `search` phases with manual clamping and branchless logic, drastically reducing pipeline stalls.

### Allocation Strategy

*   **Heuristic Pre-allocation**: Build process estimates segment count `(N / 2ε)` ahead of time, effectively eliminating vector reallocations during construction.
*   **Zero-Copy**: Keys (especially integers) are handled without unnecessary cloning.

## Features

*   **Single-Threaded Optimization**: Tuned for maximum throughput on a dedicated core.
*   **Zero-Copy Key Support**: Supports `u8`, `u16`, `u32`, `u64`, `i8`, `i16`, `i32`, `i64`.
*   **Predictable Error Bounds**: The `epsilon` parameter strictly controls the search range.
*   **Vectorized Sorting Check**: Uses SIMD-friendly sliding windows for validation.
*   **Flexible Data Ownership**: `Pgm` for external data, `PgmData` for owned data.

## Design

The index construction and lookup process allows for extremely fast predictions of key positions.

```mermaid
graph TD
    subgraph Construction [Construction Phase]
    A[Sorted Data] -->|build_segments| B[Linear Segments]
    B -->|build_lut| C[Look-up Table]
    end

    subgraph Query [Query Phase]
    Q[Search Key] -->|find_seg| S[Select Segment]
    S -->|predict| P[Approximate Pos]
    P -->|binary_search| F[Final Position]
    end

    C -.-> S
    B -.-> S
```

### Construction Phase

The dataset is scanned to create Piecewise Linear Models (segments) that approximate the key distribution within an error `ε`. Each segment stores:
- `min_key`, `max_key`: Key range boundaries
- `slope`, `intercept`: Linear model parameters
- `start_idx`, `end_idx`: Data position range

A secondary lookup table (LUT) enables O(1) segment selection by mapping key ranges to segment indices.

### Query Phase

1. **Segment Selection**: Use the lookup table to find the appropriate segment for the query key.
2. **Position Prediction**: Apply the linear model `pos = slope * key + intercept` to get an approximate position.
3. **Refined Search**: Perform binary search within the bounded range `[pos - ε, pos + ε]` for exact match.

This design ensures that the binary search operates on a tiny window (typically < 64 elements) regardless of dataset size, achieving near-constant lookup time.

## Technology Stack

*   **Core**: Rust (Edition 2024)
*   **Algorithm**: Pgm-Index (Piecewise Geometric Model)
*   **Testing**: `aok`, `static_init`, `criterion` (for benchmarks)
*   **Memory**: tikv-jemalloc for precise memory measurement

## Directory Structure

```text
jdb_pgm/
├── src/
│   ├── lib.rs          # Exports and entry point
│   ├── pgm.rs          # Core Pgm struct (no data ownership)
│   ├── data.rs         # PgmData struct (with data ownership)
│   ├── build.rs        # Segment building algorithm
│   ├── types.rs        # Key trait, Segment, PgmStats
│   ├── consts.rs       # Constants
│   └── error.rs        # Error types
├── tests/
│   ├── pgm.rs          # Integration tests for Pgm
│   └── data.rs         # Integration tests for PgmData
├── benches/
│   ├── main.rs         # Criterion benchmark suite
│   └── bench_*.rs      # Individual benchmark files
├── examples/
│   ├── simple_benchmark.rs
│   └── test_bitcode.rs
└── readme/
    ├── en.md
    └── zh.md
```

## API Reference

### Core Types

#### `Pgm<K>` (Core, no data ownership)

The primary index structure that holds only the index metadata, not the data itself. Ideal for scenarios where data is stored externally (SSTable, memory-mapped files).

**Construction**

```rust
pub fn new(data: &[K], epsilon: usize, check_sorted: bool) -> Result<Self>
```

Builds the index from a sorted data slice.

- `data`: Reference to sorted key array
- `epsilon`: Maximum prediction error (controls segment granularity)
- `check_sorted`: If true, validates data is sorted before building
- Returns: `Result<Pgm<K>>` with potential `PgmError`

**Prediction Methods**

```rust
pub fn predict(key: K) -> usize
```

Returns the predicted position for a key using the linear model.

```rust
pub fn predict_range(key: K) -> (usize, usize)
```

Returns the search range `[start, end)` for a key, bounded by epsilon.

**Search Methods**

```rust
pub fn find<'a, Q, F>(&self, key: &Q, get_key: F) -> usize
where
    Q: ToKey<K> + ?Sized,
    F: Fn(usize) -> Option<&'a [u8]>,
```

Finds the insertion point for a key using byte comparison. Returns the index where the key would be inserted.

```rust
pub fn find_key<F>(&self, key: K, get_key: F) -> usize
where
    F: Fn(usize) -> Option<K>,
```

Finds the insertion point using Key type comparison.

**Metadata Methods**

```rust
pub fn segment_count() -> usize
```

Returns the number of segments in the index.

```rust
pub fn avg_segment_size() -> f64
```

Returns the average number of keys per segment.

```rust
pub fn mem_usage() -> usize
```

Returns memory usage of the index (excluding data).

```rust
pub fn len() -> usize
pub fn is_empty() -> bool
```

Standard collection methods.

#### `PgmData<K>` (With data ownership, requires `data` feature)

Convenient wrapper that owns both the index and the data, providing direct lookup methods.

**Construction**

```rust
pub fn load(data: Vec<K>, epsilon: usize, check_sorted: bool) -> Result<Self>
```

Builds the index and takes ownership of data.

**Lookup Methods**

```rust
pub fn get(key: K) -> Option<usize>
```

Returns the index of the key if found, or `None`.

```rust
pub fn get_many<'a, I>(&'a self, keys: I) -> impl Iterator<Item = Option<usize>> + 'a
where
    I: IntoIterator<Item = K> + 'a,
```

Batch lookup returning an iterator of results.

```rust
pub fn count_hits<I>(&self, keys: I) -> usize
where
    I: IntoIterator<Item = K>,
```

Counts how many keys from the iterator exist in the index.

**Metadata Methods**

```rust
pub fn data() -> &[K]
```

Returns reference to underlying data.

```rust
pub fn memory_usage() -> usize
```

Returns total memory usage (data + index).

```rust
pub fn stats() -> PgmStats
```

Returns comprehensive statistics including segment count, average segment size, and memory usage.

#### `Segment<K>`

Represents a single linear segment in the index.

```rust
pub struct Segment<K: Key> {
    pub min_key: K,      // Minimum key in this segment
    pub max_key: K,      // Maximum key in this segment
    pub slope: f64,      // Linear model slope
    pub intercept: f64,  // Linear model intercept
    pub start_idx: u32,  // Starting data index
    pub end_idx: u32,    // Ending data index (exclusive)
}
```

#### `PgmStats`

Index statistics structure.

```rust
pub struct PgmStats {
    pub segments: usize,           // Number of segments
    pub avg_segment_size: f64,     // Average keys per segment
    pub memory_bytes: usize,       // Total memory usage
}
```

#### `Key` Trait

Trait defining requirements for indexable key types.

```rust
pub trait Key: Copy + Send + Sync + Ord + Debug + 'static {
    fn as_f64(self) -> f64;
}
```

Implemented for: `u8`, `i8`, `u16`, `i16`, `u32`, `i32`, `u64`, `i64`, `u128`, `i128`, `usize`, `isize`.

#### `ToKey<K>` Trait

Trait for types that can be converted to Key and provide byte reference.

```rust
pub trait ToKey<K: Key> {
    fn to_key(&self) -> K;
    fn as_bytes(&self) -> &[u8];
}
```

Implemented for: `[u8]`, `&[u8]`, `Vec<u8>`, `Box<[u8]>`, `[u8; N]`.

#### `PgmError`

Error types for index operations.

```rust
pub enum PgmError {
    EmptyData,              // Data cannot be empty
    NotSorted,              // Data must be sorted
    InvalidEpsilon {        // Epsilon must be >= MIN_EPSILON
        provided: usize,
        min: usize,
    },
}
```

### Helper Functions

```rust
pub fn key_to_u64(key: &[u8]) -> u64  // Requires `key_to_u64` feature
```

Converts key bytes to u64 prefix (big-endian, pad with 0).

```rust
pub fn build_segments<K: Key>(data: &[K], epsilon: usize) -> Vec<Segment<K>>
```

Low-level function to build segments using the shrinking cone algorithm.

```rust
pub fn build_lut<K: Key>(data: &[K], segments: &[Segment<K>]) -> (Vec<u32>, f64, f64)
```

Low-level function to build the lookup table.

## History

In the era of "Big Data," traditional B-Trees became a bottleneck due to their memory consumption and cache inefficiency. Each node in a B-Tree stores multiple keys and pointers, leading to poor cache locality and high memory overhead.

The breakthrough came in 2020 when Paolo Ferragina and Giorgio Vinciguerra introduced the **Piecewise Geometric Model (Pgm) index** in their paper "The PGM-index: a fully-dynamic compressed learned index with provable worst-case bounds." Their key insight was simple yet revolutionary: why store every key when the data's distribution often follows a predictable pattern?

By treating the index as a machine learning problem—learning the Cumulative Distribution Function (CDF) of the data—they reduced the index size by orders of magnitude while maintaining O(log N) worst-case performance. The Pgm-index approximates the key distribution using piecewise linear functions, where each segment guarantees that the prediction error never exceeds a specified epsilon.

Before learned indexes, the field was dominated by heuristic approaches like B-Trees (1970s), Skip Lists (1989), and various hash-based structures. These all relied on predetermined structural properties rather than learning from the data itself. The Pgm-index pioneered the concept of "learned indexes" that adapt to data characteristics, opening a new research direction at the intersection of databases and machine learning.

This project, `jdb_pgm`, takes that concept and strips it down to its bare metal essentials for Rust. By focusing on single-threaded performance and eliminating overhead, it prioritizes raw speed on modern CPUs where every nanosecond counts—exactly what high-performance databases need in the era of thread-per-core architectures.

## Bench

## Pgm-Index Benchmark

Performance comparison of Pgm-Index vs Binary Search with different epsilon values.

### Data Size: 1,000,000

| Algorithm | Epsilon | Mean Time | Std Dev | Throughput | Memory |
|-----------|---------|-----------|---------|------------|--------|
| jdb_pgm | 32 | 17.85ns | 58.01ns | 56.01M/s | 1.01 MB |
| jdb_pgm | 64 | 17.91ns | 56.67ns | 55.83M/s | 512.00 KB |
| pgm_index | 32 | 20.13ns | 54.58ns | 49.67M/s | 8.35 MB |
| pgm_index | 64 | 23.16ns | 66.31ns | 43.18M/s | 8.38 MB |
| pgm_index | 128 | 25.91ns | 62.66ns | 38.60M/s | 8.02 MB |
| jdb_pgm | 128 | 26.15ns | 96.65ns | 38.25M/s | 256.00 KB |
| HashMap | null | 39.99ns | 130.55ns | 25.00M/s | 40.00 MB |
| Binary Search | null | 40.89ns | 79.06ns | 24.46M/s | - |
| BTreeMap | null | 84.21ns | 99.32ns | 11.87M/s | 16.83 MB |

### Accuracy Comparison: jdb_pgm vs pgm_index

| Data Size | Epsilon | jdb_pgm (Max) | jdb_pgm (Avg) | pgm_index (Max) | pgm_index (Avg) |
|-----------|---------|---------------|---------------|-----------------|------------------|
| 1,000,000 | 128 | 128 | 46.80 | 1024 | 511.28 |
| 1,000,000 | 32 | 32 | 11.35 | 256 | 127.48 |
| 1,000,000 | 64 | 64 | 22.59 | 512 | 255.39 |
### Build Time Comparison: jdb_pgm vs pgm_index

| Data Size | Epsilon | jdb_pgm (Time) | pgm_index (Time) | Speedup |
|-----------|---------|---------------------|-----------------|---------|
| 1,000,000 | 128 | 1.28ms | 1.26ms | 0.98x |
| 1,000,000 | 32 | 1.28ms | 1.27ms | 0.99x |
| 1,000,000 | 64 | 1.28ms | 1.20ms | 0.94x |
### Configuration
Query Count: 1500000
Data Sizes: 10,000, 100,000, 1,000,000
Epsilon Values: 32, 64, 128



---

### Epsilon (ε) Explained

*Epsilon (ε) controls the accuracy-speed trade-off:*

*Mathematical definition: ε defines the maximum absolute error between the predicted position and the actual position in the data array. When calling `load(data, epsilon, ...)`, ε guarantees |pred - actual| ≤ ε, where positions are indices within the data array of length `data.len()`.*

*Example: For 1M elements with ε=32, if the actual key is at position 1000:*
- ε=32 predicts position between 968-1032, then checks up to 64 elements
- ε=128 predicts position between 872-1128, then checks up to 256 elements


### Notes
#### What is Pgm-Index?
Pgm-Index (Piecewise Geometric Model Index) is a learned index structure that approximates the distribution of keys with piecewise linear models.
It provides O(log ε) search time with guaranteed error bounds, where ε controls the trade-off between memory and speed.

#### Why Compare with Binary Search?
Binary search is the baseline for sorted array lookup. Pgm-Index aims to:
- Match or exceed binary search performance
- Reduce memory overhead compared to traditional indexes
- Provide better cache locality for large datasets

#### Environment
- OS: macOS 26.1 (arm64)
- CPU: Apple M2 Max
- Cores: 12
- Memory: 64.0GB
- Rust: rustc 1.94.0-nightly (8d670b93d 2025-12-31)

#### References
- [Pgm-Index Paper](https://doi.org/10.1145/3373718.3394764)
- [Official Pgm-Index Site](https://pgm.di.unipi.it/)
- [Learned Indexes](https://arxiv.org/abs/1712.01208)

---

## About

This project is an open-source component of [js0.site ⋅ Refactoring the Internet Plan](https://js0.site).

We are redefining the development paradigm of the Internet in a componentized way. Welcome to follow us:

* [Google Group](https://groups.google.com/g/js0-site)
* [js0site.bsky.social](https://bsky.app/profile/js0site.bsky.social)

---

<a id="zh"></a>

# jdb_pgm : 面向排序键的超快学习型索引

> 经过高度优化的 Rust 版 Pgm 索引（分段几何模型索引）单线程实现，专为超低延迟查找和极小内存开销而设计。

![性能评测](https://raw.githubusercontent.com/js0-site/jdb/refs/heads/main/jdb_pgm/svg/zh.svg)

- [简介](#简介)
- [使用方法](#使用方法)
- [性能](#性能)
- [特性](#特性)
- [设计](#设计)
- [技术栈](#技术栈)
- [目录结构](#目录结构)
- [API 参考](#api-参考)
- [历史背景](#历史背景)

---

## 简介

`jdb_pgm` 是 Pgm-index 数据结构的专用重构版本。它使用分段线性模型近似排序键的分布，从而实现 **O(log ε)** 复杂度的搜索操作。

本 crate 专注于 **单线程性能**，为"一线程一核 (One Thread Per CPU)"的架构做准备。通过移除并发开销并优化内存布局（如 SIMD 友好的循环），与标准二分查找和传统树状索引相比，它实现了具有统计意义的显著速度提升。

## 使用方法

在 `Cargo.toml` 中添加依赖：

```toml
[dependencies]
jdb_pgm = "0.3"
```

### 两种模式

**`Pgm<K>`** - 核心索引，不持有数据（适用于 SSTable、mmap 场景）

```rust
use jdb_pgm::Pgm;

fn main() {
  let data: Vec<u64> = (0..1_000_000).collect();

  // 从数据引用构建索引
  let pgm = Pgm::new(&data, 32, true).unwrap();

  // 获取预测的搜索范围
  let (start, end) = pgm.predict_range(123_456);

  // 在自己的数据存储中搜索
  if let Ok(pos) = data[start..end].binary_search(&123_456) {
    println!("Found at index: {}", start + pos);
  }
}
```

**`PgmData<K>`** - 持有数据的索引（适用于内存使用场景）

```rust
use jdb_pgm::PgmData;

fn main() {
  let data: Vec<u64> = (0..1_000_000).collect();

  // 构建索引并获取数据所有权
  let index = PgmData::load(data, 32, true).unwrap();

  // 直接查找
  if let Some(pos) = index.get(123_456) {
    println!("Found at index: {}", pos);
  }
}
```

### Feature 标志

- `data`（默认）：启用持有数据的 `PgmData` 结构体
- `bitcode`：启用 bitcode 序列化
- `key_to_u64`：启用 `key_to_u64()` 辅助函数用于字节键

## 性能

基于 1,000,000 个 `u64` 键的内部基准测试（jdb_pgm 的 Pgm 不持有数据，仅统计索引内存）：

*   比标准二分查找 **快 ~2.3 倍**（17.85ns vs 40.89ns）。
*   比 [pgm_index](https://crates.io/crates/pgm_index) **快 ~1.1 - 1.3 倍**（17.85ns vs 20.13ns）。
*   比 BTreeMap **快 ~4.7 倍**（17.85ns vs 84.21ns）。
*   比 HashMap **快 ~2.2 倍**（17.85ns vs 39.99ns）。
*   在 `ε=32` 时，索引内存仅 **1.01 MB**（pgm_index 为 8.35 MB）。
*   预测精度：jdb_pgm 最大误差严格等于 ε，pgm_index 最大误差为 8ε。

## 🆚 与 `pgm_index` 的对比

本 crate (`jdb_pgm`) 是原版 [`pgm_index`](https://crates.io/crates/pgm_index) 概念的专用分叉/重写版本。原版库旨在通用并支持多线程（Rayon），而 `jdb_pgm` 采取了截然不同的优化路径：

### 核心差异总结

| 特性 | jdb_pgm | pgm_index |
|------|---------------|-----------|
| 线程模型 | 单线程 | 多线程 (Rayon) |
| 段构建算法 | 收缩锥 O(N) | 并行最小二乘法 |
| 预测公式 | `slope * key + intercept` | `(key - intercept) / slope` |
| 预测精度 | ε 有界（保证） | 启发式（无保证） |
| 内存 | 无 Arc，零拷贝 | Arc<Vec<K>> 包装 |
| 数据所有权 | 可选（`Pgm` vs `PgmData`） | 始终持有数据 |
| 依赖 | 最小化 | rayon, num_cpus, num-traits |

### 架构转型：原生单线程设计

原版 `pgm_index` 引入了 Rayon 进行并行处理。然而，在现代高性能数据库（如 ScyllaDB 或专用引擎）中，**线程绑定核心 (Thread-per-Core)** 架构往往更具优势。

*   **一线程一 CPU**：移除了所有的锁、同步原语和线程池开销。
*   **确定的延迟**：没有了线程调度的抖动，p99 延迟显著更加稳定。

### 段构建算法

**jdb_pgm: 收缩锥算法 (Optimal PLA)**

流式算法保证预测误差永远不超过 ε，而最小二乘拟合无法提供这种保证。

```rust
// O(N) 流式算法，保证 ε 有界
while end < n {
  slope_lo = (idx - first_idx - ε) / dx
  slope_hi = (idx - first_idx + ε) / dx
  if min_slope > max_slope: break  // 锥体收缩至崩塌
  // 更新收缩锥边界
}
slope = (min_slope + max_slope) / 2
```

**pgm_index: 并行最小二乘法**

```rust
// 将数据分成固定块，对每块进行最小二乘拟合
target_segments = optimal_segment_count_adaptive(data, epsilon)
segments = (0..target_segments).par_iter().map(|i| {
  fit_segment(&data[start..end], start)  // 最小二乘拟合
}).collect()
```

### 预测公式

**jdb_pgm**: `pos = slope * key + intercept`
- 直接正向预测
- 使用 FMA（融合乘加）提高精度

**pgm_index**: `pos = (key - intercept) / slope`
- 逆向公式（给定 y 求 x）
- 除法比乘法慢
- 当 slope ≈ 0 时有除零风险

### 核心算法实现升级

虽然基于相同的 Pgm 理论，但在**具体代码实现**层面上，算法更加激进：

*   **消除浮点开销**：将所有昂贵的浮点取整操作 (`round/floor`) 替换为基于位操作的整数转换 (`as isize + 0.5`)，在指令周期层面带来质的飞跃。
*   **对编译器透明**：核心循环结构经过重构，移除了阻碍 LLVM 自动向量化的依赖，无需编写 `intrinsic` 代码即可生成 AVX2/AVX-512 指令。
*   **减少分支预测失败**：通过手动 clamp 和无分支逻辑重写了 `predict` 和 `search` 阶段，大幅降低流水线停顿。

### 分配策略

*   **启发式预分配**：构建过程会提前估算段的数量 `(N / 2ε)`，有效消除构建过程中的向量重分配 (Reallocation)。
*   **零拷贝**：键（尤其是整数）的处理避免了不必要的克隆。

## 特性

*   **单线程优化**：针对专用核心的吞吐量进行了极致调优。
*   **零拷贝支持**：支持 `u8`, `u16`, `u32`, `u64`, `i8`, `i16`, `i32`, `i64`。
*   **可预测的误差界限**：`epsilon` 参数严格控制搜索范围。
*   **向量化排序检查**：使用 SIMD 友好的滑动窗口进行验证。
*   **灵活的数据所有权**：`Pgm` 用于外部数据，`PgmData` 用于持有数据。

## 设计

索引构建和查找过程允许极快地预测键的位置。

```mermaid
graph TD
    subgraph Construction [构建阶段]
    A[已排序数据] -->|build_segments| B[线性段模型]
    B -->|build_lut| C[查找表 LUT]
    end

    subgraph Query [查询阶段]
    Q[搜索键] -->|find_seg| S[选择段]
    S -->|predict| P[近似位置]
    P -->|binary_search| F[最终位置]
    end

    C -.-> S
    B -.-> S
```

### 构建阶段

扫描数据集以创建分段线性模型（Segments），在误差 `ε` 内近似键的分布。每个段存储：
- `min_key`, `max_key`：键范围边界
- `slope`, `intercept`：线性模型参数
- `start_idx`, `end_idx`：数据位置范围

辅助查找表（LUT）通过将键范围映射到段索引，实现 O(1) 的段选择。

### 查询阶段

1. **段选择**：使用查找表找到查询键对应的段。
2. **位置预测**：应用线性模型 `pos = slope * key + intercept` 获取近似位置。
3. **精确搜索**：在有界范围 `[pos - ε, pos + ε]` 内执行二分查找以精确匹配。

此设计确保二分查找在极小窗口（通常 < 64 个元素）内操作，无论数据集大小如何，均实现近似常量的查找时间。

## 技术栈

*   **核心**: Rust (Edition 2024)
*   **算法**: Pgm-Index (分段几何模型)
*   **测试**: `aok`, `static_init`, `criterion` (用于基准测试)
*   **内存**: tikv-jemalloc 用于精确内存测量

## 目录结构

```text
jdb_pgm/
├── src/
│   ├── lib.rs          # 导出和入口点
│   ├── pgm.rs          # 核心 Pgm 结构体（不持有数据）
│   ├── data.rs         # PgmData 结构体（持有数据）
│   ├── build.rs        # 段构建算法
│   ├── types.rs        # Key trait, Segment, PgmStats
│   ├── consts.rs       # 常量
│   └── error.rs        # 错误类型
├── tests/
│   ├── pgm.rs          # Pgm 集成测试
│   └── data.rs         # PgmData 集成测试
├── benches/
│   ├── main.rs         # Criterion 基准测试套件
│   └── bench_*.rs      # 各个基准测试文件
├── examples/
│   ├── simple_benchmark.rs
│   └── test_bitcode.rs
└── readme/
    ├── en.md
    └── zh.md
```

## API 参考

### 核心类型

#### `Pgm<K>`（核心，不持有数据）

主要索引结构，仅保存索引元数据，不保存数据本身。适用于数据外部存储的场景（SSTable、内存映射文件）。

**构建**

```rust
pub fn new(data: &[K], epsilon: usize, check_sorted: bool) -> Result<Self>
```

从已排序数据切片构建索引。

- `data`：已排序键数组的引用
- `epsilon`：最大预测误差（控制段粒度）
- `check_sorted`：若为 true，构建前验证数据已排序
- 返回：`Result<Pgm<K>>`，可能包含 `PgmError`

**预测方法**

```rust
pub fn predict(key: K) -> usize
```

使用线性模型返回键的预测位置。

```rust
pub fn predict_range(key: K) -> (usize, usize)
```

返回键的搜索范围 `[start, end)`，由 epsilon 限定。

**搜索方法**

```rust
pub fn find<'a, Q, F>(&self, key: &Q, get_key: F) -> usize
where
    Q: ToKey<K> + ?Sized,
    F: Fn(usize) -> Option<&'a [u8]>,
```

使用字节比较查找键的插入点。返回键应插入的索引。

```rust
pub fn find_key<F>(&self, key: K, get_key: F) -> usize
where
    F: Fn(usize) -> Option<K>,
```

使用 Key 类型比较查找插入点。

**元数据方法**

```rust
pub fn segment_count() -> usize
```

返回索引中的段数量。

```rust
pub fn avg_segment_size() -> f64
```

返回每段的平均键数量。

```rust
pub fn mem_usage() -> usize
```

返回索引的内存使用量（不含数据）。

```rust
pub fn len() -> usize
pub fn is_empty() -> bool
```

标准集合方法。

#### `PgmData<K>`（持有数据，需要 `data` feature）

便捷包装器，同时拥有索引和数据，提供直接查找方法。

**构建**

```rust
pub fn load(data: Vec<K>, epsilon: usize, check_sorted: bool) -> Result<Self>
```

构建索引并获取数据所有权。

**查找方法**

```rust
pub fn get(key: K) -> Option<usize>
```

如果找到，返回键的索引；否则返回 `None`。

```rust
pub fn get_many<'a, I>(&'a self, keys: I) -> impl Iterator<Item = Option<usize>> + 'a
where
    I: IntoIterator<Item = K> + 'a,
```

批量查找，返回结果迭代器。

```rust
pub fn count_hits<I>(&self, keys: I) -> usize
where
    I: IntoIterator<Item = K>,
```

统计迭代器中有多少键存在于索引中。

**元数据方法**

```rust
pub fn data() -> &[K]
```

返回底层数据引用。

```rust
pub fn memory_usage() -> usize
```

返回总内存使用量（数据 + 索引）。

```rust
pub fn stats() -> PgmStats
```

返回综合统计信息，包括段数、平均段大小和内存使用量。

#### `Segment<K>`

表示索引中的单个线性段。

```rust
pub struct Segment<K: Key> {
    pub min_key: K,      // 段内最小键
    pub max_key: K,      // 段内最大键
    pub slope: f64,      // 线性模型斜率
    pub intercept: f64,  // 线性模型截距
    pub start_idx: u32,  // 起始数据索引
    pub end_idx: u32,    // 结束数据索引（不包含）
}
```

#### `PgmStats`

索引统计信息结构。

```rust
pub struct PgmStats {
    pub segments: usize,           // 段数量
    pub avg_segment_size: f64,     // 每段平均键数
    pub memory_bytes: usize,       // 总内存使用量
}
```

#### `Key` Trait

定义可索引键类型需求的 trait。

```rust
pub trait Key: Copy + Send + Sync + Ord + Debug + 'static {
    fn as_f64(self) -> f64;
}
```

已实现类型：`u8`, `i8`, `u16`, `i16`, `u32`, `i32`, `u64`, `i64`, `u128`, `i128`, `usize`, `isize`。

#### `ToKey<K>` Trait

可转换为 Key 并提供字节引用的类型 trait。

```rust
pub trait ToKey<K: Key> {
    fn to_key(&self) -> K;
    fn as_bytes(&self) -> &[u8];
}
```

已实现类型：`[u8]`, `&[u8]`, `Vec<u8>`, `Box<[u8]>`, `[u8; N]`。

#### `PgmError`

索引操作的错误类型。

```rust
pub enum PgmError {
    EmptyData,              // 数据不能为空
    NotSorted,              // 数据必须已排序
    InvalidEpsilon {        // Epsilon 必须 >= MIN_EPSILON
        provided: usize,
        min: usize,
    },
}
```

### 辅助函数

```rust
pub fn key_to_u64(key: &[u8]) -> u64  // 需要 `key_to_u64` feature
```

将键字节转换为 u64 前缀（大端序，不足补0）。

```rust
pub fn build_segments<K: Key>(data: &[K], epsilon: usize) -> Vec<Segment<K>>
```

底层函数，使用收缩锥算法构建段。

```rust
pub fn build_lut<K: Key>(data: &[K], segments: &[Segment<K>]) -> (Vec<u32>, f64, f64)
```

底层函数，构建查找表。

## 历史背景

在"大数据"时代，传统的 B-Tree 由于其内存消耗和缓存效率低逐渐成为瓶颈。B-Tree 的每个节点存储多个键和指针，导致缓存局部性差和内存开销高。

突破性进展出现在 2020 年，Paolo Ferragina 和 Giorgio Vinciguerra 在论文"The PGM-index: a fully-dynamic compressed learned index with provable worst-case bounds"中提出了 **分段几何模型 (Pgm) 索引**。他们的核心见解简单而具有革命性：如果数据分布通常遵循可预测的模式，为什么还要存储每个键呢？

通过将索引视为机器学习问题——学习数据的累积分布函数（CDF）——他们在保持 O(log N) 最坏情况性能的同时，将索引大小减少了几个数量级。Pgm-index 使用分段线性函数近似键分布，其中每个段保证预测误差永远不会超过指定的 epsilon。

在 Pgm-index 出现之前，该领域由启发式方法主导，如 B-Tree（1970年代）、Skip List（1989年）和各种基于哈希的结构。这些都依赖于预定的结构属性，而不是从数据本身学习。Pgm-index 开创了"学习型索引"的概念，根据数据特征自适应调整，开启了数据库和机器学习交叉领域的新研究方向。

本项目 `jdb_pgm` 借鉴了这一概念，并将其剥离至最本质的 Rust 实现。通过专注于单线程性能和消除开销，它在每一纳秒都至关重要的现代 CPU 上优先考虑原始速度——这正是高性能数据库在线程绑定核心架构时代所需要的。

## 评测

## Pgm 索引评测

Pgm-Index 与二分查找在不同 epsilon 值下的性能对比。

### 数据大小: 1,000,000

| 算法 | Epsilon | 平均时间 | 标准差 | 吞吐量 | 内存 |
|------|---------|----------|--------|--------|------|
| jdb_pgm | 32 | 17.85ns | 58.01ns | 56.01M/s | 1.01 MB |
| jdb_pgm | 64 | 17.91ns | 56.67ns | 55.83M/s | 512.00 KB |
| pgm_index | 32 | 20.13ns | 54.58ns | 49.67M/s | 8.35 MB |
| pgm_index | 64 | 23.16ns | 66.31ns | 43.18M/s | 8.38 MB |
| pgm_index | 128 | 25.91ns | 62.66ns | 38.60M/s | 8.02 MB |
| jdb_pgm | 128 | 26.15ns | 96.65ns | 38.25M/s | 256.00 KB |
| HashMap | null | 39.99ns | 130.55ns | 25.00M/s | 40.00 MB |
| 二分查找 | null | 40.89ns | 79.06ns | 24.46M/s | - |
| BTreeMap | null | 84.21ns | 99.32ns | 11.87M/s | 16.83 MB |

### 精度对比: jdb_pgm vs pgm_index

| 数据大小 | Epsilon | jdb_pgm (最大) | jdb_pgm (平均) | pgm_index (最大) | pgm_index (平均) |
|----------|---------|----------------|----------------|------------------|-------------------|
| 1,000,000 | 128 | 128 | 46.80 | 1024 | 511.28 |
| 1,000,000 | 32 | 32 | 11.35 | 256 | 127.48 |
| 1,000,000 | 64 | 64 | 22.59 | 512 | 255.39 |
### 构建时间对比: jdb_pgm vs pgm_index

| 数据大小 | Epsilon | jdb_pgm (时间) | pgm_index (时间) | 加速比 |
|----------|---------|---------------------|-----------------|--------|
| 1,000,000 | 128 | 1.28ms | 1.26ms | 0.98x |
| 1,000,000 | 32 | 1.28ms | 1.27ms | 0.99x |
| 1,000,000 | 64 | 1.28ms | 1.20ms | 0.94x |
### 配置
查询次数: 1500000
数据大小: 10,000, 100,000, 1,000,000
Epsilon 值: 32, 64, 128



---

### Epsilon (ε) 说明

*Epsilon (ε) 控制精度与速度的权衡：*

*数学定义：ε 定义了预测位置与实际位置在数据数组中的最大绝对误差。调用 `load(data, epsilon, ...)` 时，ε 保证 |pred - actual| ≤ ε，其中位置是长度为 `data.len()` 的数据数组中的索引。*

*举例说明：对于 100 万个元素，ε=32 时，如果实际键在位置 1000：*
- ε=32 预测位置在 968-1032 之间，然后检查最多 64 个元素
- ε=128 预测位置在 872-1128 之间，然后检查最多 256 个元素


### 备注
#### 什么是 Pgm-Index?
Pgm-Index（分段几何模型索引）是一种学习型索引结构，使用分段线性模型近似键的分布。
它提供 O(log ε) 的搜索时间，并保证误差边界，其中 ε 控制内存和速度之间的权衡。

#### 为什么与二分查找对比?
二分查找是已排序数组查找的基准。Pgm-Index 旨在：
- 匹配或超过二分查找的性能
- 相比传统索引减少内存开销
- 为大数据集提供更好的缓存局部性

#### 环境
- 系统: macOS 26.1 (arm64)
- CPU: Apple M2 Max
- 核心数: 12
- 内存: 64.0GB
- Rust版本: rustc 1.94.0-nightly (8d670b93d 2025-12-31)

#### 参考
- [Pgm-Index 论文](https://doi.org/10.1145/3373718.3394764)
- [Pgm-Index 官方网站](https://pgm.di.unipi.it/)
- [学习型索引](https://arxiv.org/abs/1712.01208)

---

## 关于

本项目为 [js0.site ⋅ 重构互联网计划](https://js0.site) 的开源组件。

我们正在以组件化的方式重新定义互联网的开发范式，欢迎关注：

* [谷歌邮件列表](https://groups.google.com/g/js0-site)
* [js0site.bsky.social](https://bsky.app/profile/js0site.bsky.social)
