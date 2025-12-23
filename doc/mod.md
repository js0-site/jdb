JDB Kernel 模块架构设计文档

项目代号: JDB (Jet Data Base)
设计目标: 面向 NVMe 的嵌入式、全异步、Shared_Nothing 架构时序数据库内核。

1. 核心技术栈选型 (2025 Performance Stack)

| 组件领域 | 选型库 | 核心理由 |
|---------|--------|---------|
| 异步运行时 | compio | 基于 io_uring/IOCP 的全异步 IO |
| 哈希算法 | gxhash | SIMD 加速非加密哈希 |
| 校验和 | crc_fast | PCLMULQDQ 指令集加速 |
| 序列化(变长) | bitcode | 极速二进制序列化 |
| 序列化(定长) | bytes | 零拷贝读写 |
| 位图索引 | roaring | SIMD 加速集合运算 |

2. 模块层级架构

```
jdb_kernel/
├── [L0] Infra: 基础类型与协议
│   ├── jdb_comm    ✓  (Types, Errors, Hash, Config)
│   └── jdb_layout  ✓  (On_Disk Formats, Serialization)
├── [L1] IO Base: 内存与文件系统
│   ├── jdb_alloc   ✓  (Aligned Memory Allocator)
│   └── jdb_fs      ✓  (Compio FS Wrapper)
├── [L2] Engines: 存储引擎组件
│   ├── jdb_wal     ✓  (Write Ahead Log, Group Commit)
│   ├── jdb_vlog    ✓  (Blob Store, KV Separation)
│   └── jdb_page    ✓  (Buffer Manager, Page State Machine)
├── [L3] Indexing: 索引算法
│   ├── jdb_index   ✓  (B+ Tree Implementation)
│   └── jdb_tag     ✓  (Inverted Index / RoaringBitmap)
├── [L4] Core: 内核胶水层
│   └── jdb_tablet  ✓  (VNode Entity, MVCC, ACID)
└── [L5] Runtime: 运行时与接口
    ├── jdb_runtime ✓  (Thread_per_Core Dispatcher)
    └── jdb_api     ✓  (Rust SDK)
```

3. 设计约束

- Direct I/O: 所有磁盘 IO 必须使用 AlignedBuf（4KB 对齐）
- Thread_per_Core: Tablet 及下层组件为 !Send + !Sync
- 二进制名称: 数据库名、表名支持任意二进制（&[u8]）
- 时间戳: 秒级精度 u64
- 落盘策略: 默认与 RocksDB 一致（sync=false）

3.1 落盘策略 (Sync Policy)

与 RocksDB 默认行为保持一致，追求高性能：

| 方法 | 行为 | 说明 |
|------|------|------|
| `put()` | 写入 OS page cache | 不等待 fsync，高吞吐 |
| `put_sync()` | 写入后 fsync | 强一致，每次持久化 |
| `flush()` | 强制 fsync | 手动触发刷盘 |

**对比 RocksDB:**
- `WriteOptions.sync = false` (默认) → JDB `put()`
- `WriteOptions.sync = true` → JDB `put_sync()`
- `DB::FlushWAL()` → JDB `flush()`

4. 详细模块定义
L0: Infrastructure (基础设施层)
📦 jdb_comm

路径: crates/infra/comm

用途: 全系统通用的类型定义与工具，打破循环依赖。

依赖: thiserror, gxhash, lazy_static

核心内容:

TableID, PageID, VNodeID, Timestamp (NewType 模式)。

JdbError, JdbResult (统一错误处理)。

Hasher: 封装 gxhash，确保全系统 Hash 行为一致。

Config: 定义 Buffer Size, IO Depth, Paths 等配置。

📦 jdb_layout

路径: crates/infra/layout

用途: 定义磁盘数据结构的物理布局（Protocol）。不含 IO 逻辑。

依赖: jdb_comm, bitcode, bytes, crc_fast

核心内容:

WAL Protocol: 使用 bitcode 定义 WalEntry { Put, Delete, Barrier }。

Page Protocol: 使用 bytes 手动定义 4KB 页面的 Header 和 Trailer。

Blob Protocol: 定义大对象文件的 Block Header。

Checksum: 封装 crc_fast 的计算逻辑。

L1: IO & Memory Foundation (系统抽象层)
📦 jdb_alloc

路径: crates/base/alloc

用途: 内存分配基座。Direct I/O 要求内存地址必须与扇区对齐。

依赖: std::alloc, libc

核心内容:

AlignedVec: 类似于 Vec<u8>，但强制 4096 字节对齐。

HugePage: (可选) 尝试申请 Linux HugePages (2MB) 以减少 TLB Miss。

📦 jdb_fs

路径: crates/base/fs

用途: compio 的封装与 Buffer 管理。解决异步 IO 的所有权问题。

依赖: compio, jdb_alloc, jdb_comm

核心内容:

FileOp: 封装 O_DIRECT 标志的文件打开与读写。

Buffer Recycler: 实现 Thread_Local 的 AlignedVec 对象池，避免高频 IO 下的 malloc 开销。

IoMetrics: 暴露 IOPS、延迟、带宽监控指标。

L2: Storage Components (存储组件层)
📦 jdb_wal

路径: crates/store/wal

用途: 预写日志管理，保证原子性 (Atomicity) 与持久性 (Durability)。

依赖: jdb_fs, jdb_layout

核心内容:

LogWriter: 利用 io_uring 的 Link/Batch 特性实现 Group Commit。

LogReplayer: 启动时的崩溃恢复逻辑。

📦 jdb_vlog

路径: crates/store/vlog

用途: 大对象存储 (WiscKey 模型)。KV 分离的核心。

依赖: jdb_fs, jdb_layout

核心内容:

BlobWriter: Append_only 写入，返回 (FileID, Offset, Len)。

BlobReader: 配合 compio 实现流式预读 (Prefetching)，支持 Zero_Copy Stream 返回。

GC: 后台垃圾回收逻辑（标记_整理）。

📦 jdb_page

路径: crates/store/page

用途: 用户态页缓存 (Buffer Manager)。Direct I/O 必须自己管理缓存。

依赖: jdb_fs, jdb_layout, lru (仅用于算法逻辑)

核心内容:

Page State Machine: Resident (内存中), Loading (IO 中), Dirty (需刷盘)。

PageGuard: RAII 句柄，持有期间页面被锁定在内存，不可被驱逐。

Swizzling: 内存中直接持有指针，磁盘上持有 PageID。

L3: Indexing (索引层)
📦 jdb_index

路径: crates/index/btree

用途: 面向磁盘的 B+ 树实现。

依赖: jdb_page, jdb_layout

核心内容:

BTree: 实现 Insert, Split, Merge, Range Scan 逻辑。

Leaf Inlining: 小 Value 直接存叶子节点，大 Value 存 BlobPtr。

此模块不直接操作文件，而是操作 jdb_page 提供的 PageGuard。

📦 jdb_tag

路径: crates/index/tag

用途: 标签倒排索引。

依赖: roaring, jdb_fs, jdb_comm

核心内容:

LsmBitmap: 使用类似 LSM_Tree 的结构持久化 RoaringBitmap。

TagMatcher: 支持 AND, OR, NOT 的集合运算，毫秒级定位 ID 集合。

L4: Engine Core (内核层)
📦 jdb_tablet

路径: crates/engine/tablet

用途: 分片 (VNode) 的实体。事务与 MVCC 的边界。

依赖: jdb_wal, jdb_index, jdb_vlog, jdb_tag

核心内容:

Tablet: 组合 WAL, BTree, Vlog 成为一个原子存储单元。

WritePath: 协调写入顺序：WAL _> MemTable _> Flush (BTree/Vlog)。

ReadPath: 融合 MemTable 与 Disk Index 的视图，处理 MVCC 版本可见性。

Lock_Free: 由于 Thread_per_Core，Tablet 内部无锁 (RefCell 即可)。

L5: Runtime & Interface (运行时与接口)
📦 jdb_runtime

路径: crates/runtime

用途: 线程模型与调度器。

依赖: compio, crossfire, jdb_tablet, core_affinity

核心内容:

Bootstrap: 启动 N 个线程，使用 core_affinity 绑定 CPU 核心。

Dispatcher: 维护 HashMap<VNodeID, Channel>，使用 crossfire 将外部请求极速路由到指定 Core。

Reactor: 每个线程的主循环，运行 compio::block_on 处理 IO 事件和 Channel 消息。

📦 jdb_api

路径: crates/api

用途: Rust 开发者使用的 High_Level SDK。

依赖: jdb_runtime, jdb_comm

核心内容:

JdbClient: 提供 put, get, scan 等 Async 接口。

BlobStream: 将底层 Vlog 的读取封装为 futures::Stream。

📦 jdb_capi

路径: crates/capi

用途: C/C++ FFI 导出。

依赖: jdb_api

核心内容:

libjdb.so: 导出 jdb_open, jdb_put 等符号。

Safety: 处理 Panic 捕捉与 Error Code 转换。

4. 关键设计约束 (Design Constraints)

Direct I/O 强制性:

所有涉及磁盘 IO 的模块 (fs, wal, vlog, page) 必须使用 jdb_alloc 分配的 AlignedVec，严禁使用普通的 Vec<u8>，否则会导致内核写入失败 (EINVAL)。

Thread_per_Core 隔离性:

jdb_tablet 及其下层组件 (wal, index) 必须是 ! Send 和 ! Sync 的。

它们只能存在于创建它们的那个线程中，严禁跨线程共享。跨线程交互必须通过 jdb_runtime 的 Channel 进行。

Compio 所有权模型:

调用 IO 接口时，Buffer 的所有权必须移交给 jdb_fs。IO 完成后，jdb_fs 会归还 Buffer。

上层逻辑（如 jdb_page）必须处理这种“Buffer 暂时消失”的状态（State: Loading/Flushing）。