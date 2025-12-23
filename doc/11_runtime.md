# jdb_runtime 模块设计 / Module Design

## 概述 / Overview

`jdb_runtime` 是 L5 运行时层的核心模块，实现 Thread-per-Core 调度器。每个 CPU 核心运行一个 Worker 线程，通过 Channel 路由请求到对应的 VNode。

`jdb_runtime` is the core module of L5 runtime layer, implementing Thread-per-Core dispatcher. Each CPU core runs a Worker thread, routing requests to corresponding VNode via Channel.

## 依赖 / Dependencies

```
jdb_runtime
├── jdb_comm      (types, errors)
├── jdb_tablet    (storage unit)
├── compio        (async runtime)
├── core_affinity (CPU binding)
├── tokio         (oneshot channel)
└── parking_lot   (locks)
```

## 核心结构 / Core Structures

### RuntimeConfig

```rust
pub struct RuntimeConfig {
  pub workers: usize,           // Worker 数量 / Number of workers
  pub bind_cores: bool,         // 绑定 CPU 核心 / Bind to CPU cores
  pub data_dir: PathBuf,        // 数据目录 / Data directory
}
```

### Runtime

```rust
pub struct Runtime {
  workers: Vec<Worker>,                      // Worker 列表 / Worker list
  vnode_map: Arc<RwLock<HashMap<u16, usize>>>, // VNode 路由表 / VNode routing table
  started: bool,
}
```

### Worker

```rust
pub struct Worker {
  pub id: usize,
  pub tx: Sender<Request>,      // 请求发送端 / Request sender
  pub handle: Option<JoinHandle<()>>,
}
```

## 核心接口 / Core APIs

### 生命周期 / Lifecycle

```rust
// 创建运行时 / Create runtime
pub fn new() -> Self

// 启动运行时 / Start runtime
pub fn start(&mut self, cfg: RuntimeConfig) -> Result<()>

// 关闭运行时 / Shutdown runtime
pub fn shutdown(&mut self)
```

### 数据操作 / Data Operations

```rust
// 写入键值 / Put key-value
pub async fn put(&self, table: TableID, key: Vec<u8>, val: Vec<u8>) -> Result<()>

// 读取值 / Get value
pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>

// 删除键 / Delete key
pub async fn delete(&self, table: TableID, key: &[u8]) -> Result<bool>

// 范围扫描 / Range scan
pub async fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>

// 刷新 / Flush
pub async fn flush(&self) -> Result<()>
```

## 架构设计 / Architecture

```
┌─────────────────────────────────────────────────┐
│                   Runtime                        │
│  ┌─────────────────────────────────────────┐    │
│  │           VNode Routing Table            │    │
│  │     HashMap<VNodeID, WorkerIndex>        │    │
│  └─────────────────────────────────────────┘    │
│                      │                           │
│    ┌─────────────────┼─────────────────┐        │
│    ▼                 ▼                 ▼        │
│ ┌──────┐         ┌──────┐         ┌──────┐     │
│ │Worker│         │Worker│         │Worker│     │
│ │  0   │         │  1   │         │  N   │     │
│ │ CPU0 │         │ CPU1 │         │ CPUN │     │
│ └──┬───┘         └──┬───┘         └──┬───┘     │
│    │                │                │          │
│    ▼                ▼                ▼          │
│ ┌──────┐         ┌──────┐         ┌──────┐     │
│ │Tablet│         │Tablet│         │Tablet│     │
│ │VNode0│         │VNode1│         │VNodeN│     │
│ └──────┘         └──────┘         └──────┘     │
└─────────────────────────────────────────────────┘
```

## 请求流程 / Request Flow

```
Client
   │
   ├─► Runtime.put(table, key, val)
   │       │
   │       ├─► default_worker()
   │       │
   │       ├─► oneshot::channel()
   │       │
   │       ├─► worker.send(Request::Put{...})
   │       │
   │       └─► rx.await  ◄─────────────────┐
   │                                        │
   │   Worker Thread (compio runtime)       │
   │       │                                │
   │       ├─► rx.recv()                    │
   │       │                                │
   │       ├─► tablet.put(...)              │
   │       │                                │
   │       └─► tx.send(Response::Ok) ───────┘
   │
   └─► Ok(())
```

## 设计约束 / Design Constraints

1. **Thread-per-Core**: 每个 Worker 绑定一个 CPU 核心 / Each Worker binds to one CPU core
2. **!Send + !Sync**: Tablet 只在 Worker 线程内使用 / Tablet only used within Worker thread
3. **Channel 通信**: 跨线程通过 Channel 通信 / Cross-thread communication via Channel
4. **compio Runtime**: Worker 内部使用 compio 异步运行时 / Worker uses compio async runtime internally

## 错误类型 / Error Types

```rust
pub enum RuntimeError {
  VNodeNotFound(u64),
  WorkerNotFound(usize),
  SendFailed,
  RecvFailed,
  NotStarted,
  AlreadyStarted,
  Tablet(JdbError),
}
```

## 测试覆盖 / Test Coverage

- `test_runtime_basic`: 基本读写删除 / Basic read/write/delete
- `test_runtime_range`: 范围扫描 / Range scan
- `test_runtime_flush`: 刷新操作 / Flush operation
