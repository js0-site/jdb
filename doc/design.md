# JDB Slab 存储引擎设计文档 / Design Document

基于 compio 的单线程异步 Direct I/O Slab 存储引擎

---

## 1. 架构总览 / Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Engine (顶层入口)                        │
│  Rc<RefCell<SlabClass>> 容器管理                             │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   SlabClass (分层存储)                       │
│  compio::fs::File + O_DIRECT + RoaringBitmap                │
└─────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│  SlabStream     │ │  HeatTracker    │ │  GcWorker       │
│  流式读取        │ │  热度追踪        │ │  压缩回收        │
└─────────────────┘ └─────────────────┘ └─────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   AlignedBuf / RawIoBuf                      │
│                   Direct I/O 对齐内存                         │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. 核心类型 / Core Types

```rust
// Slab 配置 / Slab configuration
pub struct SlabConfig {
  pub class_size_li: Vec<usize>,  // 分层大小 [4KB, 16KB, 64KB, ...]
  pub base_path: PathBuf,         // 数据目录
}

// Slot ID (文件内位置)
pub type SlotId = u64;

// 物理帧头 / Physical frame header
pub struct Header {
  pub crc32: u32,       // CRC32 校验
  pub payload_len: u32, // 有效载荷长度
  pub flags: u8,        // 标志位 (压缩等)
}

// 热度统计 / Heat statistics
pub struct HeatStat {
  pub access_count: u32,
  pub last_access: u32,  // 相对时间戳
}
```

---

## 3. SlabClass 设计 / SlabClass Design

### 3.1 结构定义

```rust
pub struct SlabClass {
  file: compio::fs::File,       // O_DIRECT 文件
  class_size: usize,            // 本层 Slot 大小
  free_map: RoaringBitmap,      // 空闲 Slot 位图
  access_stat_li: Vec<u32>,     // 热度统计
  slot_count: u64,              // 总 Slot 数
}
```

### 3.2 put(data) -> SlotId

写入数据，返回 Slot ID。

```
实现流程:
1. 计算 Header (CRC32 + payload_len + flags)
2. 组装 Header + Payload 到 AlignedBuffer
3. 从 free_map 获取空闲 SlotId (或追加新 Slot)
4. pwrite 到 slot_offset = slot_id * class_size
5. 从 free_map 移除该 SlotId
6. 返回 SlotId
```

### 3.3 get(slot_id) -> Bytes

读取数据。

```
实现流程:
1. 计算 offset = slot_id * class_size
2. pread 到 AlignedBuffer
3. 解析 Header，校验 CRC32
4. 若压缩，解压 Payload
5. 更新 access_stat_li[slot_id]
6. 返回 Payload
```

### 3.4 del(slot_id)

删除数据（逻辑删除）。

```
实现流程:
1. free_map.insert(slot_id)
2. 清零 access_stat_li[slot_id]
```

---

## 4. 流式读取 / Streaming Read

### 4.1 SlabStream 结构

```rust
pub struct SlabStream {
  slab: Rc<RefCell<SlabClass>>,
  slot_id: SlotId,
  current_offset: u64,
  total_len: u64,
  chunk_size: usize,  // 4KB/16KB/64KB
}
```

### 4.2 next_chunk() -> Option<Bytes>

```
实现流程:
1. 若 current_offset >= total_len，返回 None
2. 分配临时 AlignedBuffer (chunk_size)
3. pread 从 current_offset 读取
4. current_offset += 实际读取长度
5. 返回 Some(chunk)
```

**场景**: 读取 1GB 超大 Value 而不消耗 1GB 内存

---

## 5. 元数据快照 / Metadata Snapshot

### 5.1 sync_meta()

```
实现流程:
1. 序列化 free_map (roaring::serialize_into)
2. 写入 {class_size}.roaring 文件
3. 序列化 access_stat_li
4. 写入 {class_size}.heat 文件
```

### 5.2 recovery()

```
实现流程:
1. 检查 .roaring 文件是否存在
2. 若存在，反序列化恢复 free_map
3. 若不存在，扫描文件重建
4. 同理恢复 access_stat_li
```

---

## 6. 热度追踪 / Heat Tracking

### 6.1 单线程优势

- 无需 Atomic 或 DashMap
- 直接读写 Vec<u32>，零开销
- 索引与 SlotId 一一对应

### 6.2 热度衰减

```rust
fn decay(&mut self) {
  for stat in &mut self.access_stat_li {
    *stat >>= 1;  // 右移衰减
  }
}
```

**目的**: 防止数值溢出，老化历史热度

---

## 7. 物理帧格式 / Physical Frame Format

```
┌──────────────┬──────────────┬───────────┬──────────┐
│ CRC32 (4B)   │ Payload_Len  │ Flags (1B)│ Reserved │
│              │ (4B)         │           │ (3B)     │
├──────────────┴──────────────┴───────────┴──────────┤
│ Payload (变长，对齐到 class_size)                    │
└───────────────────────────────────────────────────┘

Flags:
  bit 0: 是否压缩 (lz4)
  bit 1-7: 保留
```

---

## 8. GC 与压缩 / GC & Compaction

### 8.1 冷数据识别

```rust
fn scan_cold_data(&self, threshold: u32) -> Vec<SlotId> {
  self.access_stat_li
    .iter()
    .enumerate()
    .filter(|(_, &stat)| stat < threshold)
    .map(|(id, _)| id as SlotId)
    .collect()
}
```

### 8.2 压缩迁移

```
实现流程:
1. 读取冷 Slot 数据
2. lz4_flex 压缩
3. 若压缩率达标，写入更小 SlabClass
4. 原 Slot 标记为空闲
5. 返回 (old_id, new_id) 变更列表
```

---

## 9. 存储布局 / Storage Layout

```
data/
├── 4096.slab      # 4KB SlabClass
├── 4096.roaring   # 4KB 空闲位图
├── 4096.heat      # 4KB 热度统计
├── 16384.slab     # 16KB SlabClass
├── 16384.roaring
├── 16384.heat
├── 65536.slab     # 64KB SlabClass
├── 65536.roaring
├── 65536.heat
└── ...
```

---

## 10. 模块规划 / Module Planning

**已完成 / Completed**:
- jdb_alloc: AlignedBuf / RawIoBuf 对齐内存
- jdb_fs: compio Direct I/O 封装
- jdb_slab: SlabClass + SlabReader/SlabWriter + HeatTracker + GC ✓

---

## 11. 模块依赖关系 / Module Dependencies

```
┌─────────────────────────────────────────────────────────────┐
│                        jdb_slab                              │
│  Engine, SlabClass, SlabReader, SlabWriter, GcWorker        │
└─────────────────────────────────────────────────────────────┘
                    │                   │
                    ▼                   ▼
┌─────────────────────────┐   ┌─────────────────────────┐
│       jdb_alloc         │   │        jdb_fs           │
│  AlignedBuf, RawIoBuf   │   │  File (Direct I/O)      │
└─────────────────────────┘   └─────────────────────────┘
```

### jdb_slab 依赖 / Dependencies

| 依赖 / Dependency | 用途 / Purpose |
|-------------------|----------------|
| jdb_alloc | Direct I/O 对齐内存分配 |
| jdb_fs | compio Direct I/O 文件操作 |
| roaring | RoaringBitmap 空闲位图 |
| crc32fast | CRC32 校验 |
| lz4_flex | LZ4 快速压缩 (温数据) |
| zstd | Zstd 高压缩比 (冷数据) |
| thiserror | 错误类型定义 |

### jdb_slab 导出 / Exports

```rust
// 核心类型 / Core types
pub use Engine;           // 顶层入口
pub use SlabClass;        // 分层存储
pub use SlabConfig;       // 配置

// 流式接口 / Streaming
pub use SlabReader;       // 流式读取
pub use SlabWriter;       // 流式写入
pub use pipe;             // 管道传输
pub use pipe_with;        // 带缓冲管道

// 热度与 GC / Heat & GC
pub use HeatTracker;      // 热度追踪
pub use GcWorker;         // 垃圾回收
pub use Migration;        // 迁移结果

// 帧格式 / Frame format
pub use Header;           // 物理帧头
pub use Compress;         // 压缩类型

// 错误处理 / Error handling
pub use Error;            // 错误类型
pub use Result;           // 结果类型
pub use SlotId;           // 槽位 ID
```
