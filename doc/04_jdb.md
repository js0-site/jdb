这份需求文档旨在明确 **基于 Rust 和 Compio 的高性能嵌入式 KV 分离数据库** 的设计目标与约束。

该文档强调极致性能、海量多租户支持以及与 `wlog` 组件的集成。

实现请写在 jdb 模块中

---

# 需求规格说明书：高性能嵌入式 NVMe KV 数据库

## 1. 项目概述
本项目旨在开发一款嵌入式（Library form）键值数据库。核心设计思想是 **KV 分离（Key-Value Separation）**，利用现代 NVMe SSD 的并行特性，基于 Rust 语言和 `compio` 异步运行时（单线程 /Thread-per-core 模型），实现极致的 I/O 性能。

## 2. 核心技术栈与约束
*   **编程语言**: Rust (Stable/Nightly)
*   **异步运行时**: `compio` (基于 IOCP/io_uring 的完成式 I/O 模型，单线程异步设计)
*   **存储架构**: KV 分离 (LSM-Tree 用于 Key 索引，Blob Log 用于 Value 存储)
*   **已有组件**: `wlog` (负责 Value Log 的管理与读写)
*   **硬件依赖**: 针对 NVMe 优化，但 **不绑定硬件** (需支持普通文件系统/VPS 环境，不强制依赖 SPDK)

## 3. 功能性需求

### 3.1 数据操作 (Data Operations)
*   **点查 (Point Lookup)**: 低延迟获取单个 Key 对应的 Value。
*   **范围查询 (Range Query)**:
    *   支持基于 Key 的字典序扫描。
    *   **正向扫描 (Forward Scan)**: `Iterator::next()`
    *   **反向扫描 (Backward Scan)**: `Iterator::prev()`，要求反向遍历性能与正向接近。
*   **写入/更新 (Put)**: 高吞吐量的键值写入。
*   **删除 (Delete)**: 支持逻辑删除（Tombstone）。
*  支持批量操作

### 3.2 索引与存储 (Index & Storage)
*   **前缀压缩 (Prefix Compression)**: 索引部分（LSM-Tree 的 SSTable 或 MemTable）必须实现高效的 Key 前缀压缩，以减少内存占用和磁盘 I/O。
*   **KV 分离**: Key 存储在索引引擎中，Value 的物理地址（VLog Pointer）存储为索引的值；实际 Value 存储在 `wlog` 中。


## 4. 非功能性需求 (性能与质量)

### 4.1 极致性能 (Extreme Performance)
*   **I/O 模型**: 充分利用 `compio` 的完成式 I/O 特性，消除不必要的系统调用上下文切换。
*   **CPU 效率**: 关键路径零拷贝（Zero-copy where possible），最小化锁竞争（鉴于单线程异步模型，主要关注 RefCell/RC 开销及内存分配开销）。
*   **NVMe 亲和性**: 针对 NVMe 进行优化，但在普通 SSD/HDD 上也能正确运行（性能降级但功能可用）。

### 4.2 资源效率
*   **内存占用**: 在海量空闲 Namespace 场景下，常驻内存（RSS）应极低。
*   **写放大 (Write Amplification)**: 利用 KV 分离特性，显著降低 LSM 的写放大，延长 SSD 寿命。

## 5. 架构设计关键点 (初步构想)

为了满足上述需求，设计时需重点考虑以下模块的交互：

3.  **Compio Adapter**:
    *   封装文件 I/O，自动选择 `io_uring` (Linux) 或 IOCP (Windows)，实现全链路异步。
4.  **Index Engine**:
    *   自定义或高度优化的 LSM 实现，重点在于 Block 格式设计以支持**前缀压缩**和**双向迭代**。

## 其他

1. 用 BinaryFuse8 过滤器 (jdb_xorf) 做查询过滤，比布谷鸟过滤器更高效

2. 写入索引之后，会写检查点，启动用 wlog 从检查点开始加载数据，写入索引，这个要保证原子性（不会重放多了，不会漏掉）

3. 请参考 ./code 下面 的 fjall  lsm-tree 源代码来规划如何实现（一定要学习，不要全部自己写），注意，我们是单线程异步，不用那些并发数据结构