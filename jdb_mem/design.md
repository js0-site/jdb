# jdb_mem 设计文档

## 需求概述 (Requirements)

本模块旨在实现高效的内存表 (MemTable) 管理，并将其无缝集成到 LSM-Tree 的刷盘 (Flush) 流程中。

核心需求如下：
1. **接口标准化**: 定义清晰的 `Flush` 和 `Discard` 接口，配合 `jdb_base`。
2. **自动化轮转 (Rotation)**: 当内存表达到阈值时，自动冻结并触发刷盘，无需外部干预 `put` 流程。
3. **异步与并发控制**:
   - 刷盘操作应在后台异步执行，不阻塞写入。
   - 采用 **串行化刷盘** 策略：任一时刻只有一个刷盘任务在运行，前一个完成后才触发下一个。
   - 使用 `oneshot` 通道在任务完成时通知 `Mem` 并归还资源（`Flush`/`Discard` 句柄）。
4. **单线程友好**:
   - 针对单线程异步运行时 (如 `compio`) 设计。
   - 避免使用 `Arc` 和 `Send`/`Sync` 约束，使用 `Rc` 进行轻量级对象共享。
   - 避免使用 `Box<dyn Future>`，通过泛型注入 `Spawner` 闭包来创建和调度任务。
5. **ID 管理**: 使用 `ider::id()` 为生成的 SSTable 分配唯一标识。

## 架构设计 (Architecture)

### 1. 核心组件

- **`Mem<F, D, S>`**: 核心结构体。
  - **泛型**:
    - `F: Flush`: 处理将 `Map` 转换为 SST。
    - `D: Discard`: 处理被丢弃的数据 (如旧版本/墓碑)。
    - `S: Fn(...)`: 任务生成器 (Spawner)，负责将刷盘逻辑提交给 Runtime。
  - **状态管理**:
    - `old`: 等待刷盘的 `Vec<Rc<Map>>` 队列。
    - `disk`: `Option<Disk<F, D>>`，封装 Flush 和 Discard 句柄。
    - `recv`: `Option<Receiver<Disk<F, D>>>`，持有当前运行任务的通知句柄。

### 2. 工作流程 (Workflow)

#### 写入与轮转
1. 用户调用 `Mem::put`。
2. 检查 `self.size >= self.rotate_size`。
3. 若满足，调用 `self.rotate()`:
   - 将 `now` 包装为 `Rc<Map>` 移入 `old` 队列。
   - 重置 `now` 和 `size`。
   - 触发 `self.try_trigger_flush()`。

#### 异步刷盘状态机 (`State::flush`)
该方法封装在 `State` 内部，确保 `old` 队列中的任务被逐个处理：

1. **检查当前任务**:
   - **Running**: 检查 `recv`。
     - **完成 (`Ok`)**: 状态变更为 `Idle(Disk)`，从 `old` 队列移除已完成 Map。
     - **进行中**: 返回。
     - **断开**: Panic。

2. **发起新任务**:
   - **Idle**: 若 `old` 为空则返回。
   - **Transition**: 取出 `Disk`，状态暂时变更为 `Running` (placeholder)。
   - **Spawn**: 启动异步任务。
   - **更新状态**: `*self = State::Running(rx)`.

`Mem::try_trigger_flush` 简化为调用 `self.state.flush(&mut self.old)`。

### 3. 并发模型
- **Compio 直接集成**: `Mem` 直接依赖 `compio` 库，使用 `spawn` 启动后台任务。
- **资源回收**: 通过 `oneshot` 通道在任务结束时归还 `Flush` 和 `Discard` 句柄，确保串行化刷盘（资源锁）。

## API 变更总结

### `jdb_base`
- **`Flush` trait**: `flush` 方法增加 `id: u64` 参数。

### `jdb_mem`
- **`Mem` struct**:
  - 增加了泛型 `F, D`。
  - 字段 `old` 类型变更为 `Vec<Rc<Map>>`。
  - `new` 方法增加 `flush`, `discard` 参数 (移除 `spawner`)。
