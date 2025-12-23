# jdb_index - 高性能 NVMe B+ 树索引

版本: 2.0 (Async/Zero-Copy)

定位: 面向 NVMe SSD 的生产级异步 B+ 树，吞吐量优先，低延迟。

## 核心特性

| 特性 | 实现方式 | 收益 |
|------|----------|------|
| I/O 模型 | compio (io_uring) | 队列深度打满，NVMe 吞吐最大化 |
| Direct I/O | O_DIRECT + AlignedBuf | 避免 OS Page Cache 双重拷贝 |
| 内存布局 | 零拷贝视图 NodeView | 消除 Serde 开销，CPU 占用降 90% |
| 并发控制 | 乐观锁 | 读操作无锁，写操作子表内串行 |

参考:
- TreeLine (VLDB 2022) - NVMe 优化
- Blink-Tree - 并发分裂策略

## 依赖

```toml
jdb_layout = { path = "../jdb_layout" }
jdb_page = { path = "../jdb_page" }
jdb_fs = { path = "../jdb_fs" }
parking_lot = "0.12"
```

## 模块结构

```
jdb_index/src/
├── lib.rs       # 模块导出
├── key.rs       # 键编码 (可比较字节序)
├── view.rs      # 零拷贝节点视图
├── tree.rs      # B+ 树核心
└── cursor.rs    # 范围扫描游标
```

## 架构设计

采用 "Sync Logic, Async I/O" 分层架构。

```
┌─────────────────────────────────────────────────────────┐
│                    User API (Async)                     │
│                   get / insert / range                  │
└─────────────────────┬───────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────┐
│                  B+ Tree Logic Layer                    │
│  ┌─────────────┐  ┌─────────────┐                      │
│  │ Optimistic  │  │ Zero-Copy   │                      │
│  │ Locking     │  │ Node View   │                      │
│  └─────────────┘  └─────────────┘                      │
└─────────────────────┬───────────────────────────────────┘
                      │ Page Read/Write
┌─────────────────────▼───────────────────────────────────┐
│                    Buffer Pool Layer                    │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│  │ Frame Table │  │ Eviction    │  │ Aligned     │     │
│  │ (papaya)    │  │ (Clock)     │  │ Allocator   │     │
│  └─────────────┘  └─────────────┘  └─────────────┘     │
└─────────────────────┬───────────────────────────────────┘
                      │ Submit Op
┌─────────────────────▼───────────────────────────────────┐
│                   Compio Runtime                        │
│  ┌─────────────┐  ┌─────────────┐                      │
│  │ io_uring    │  │ DMA         │                      │
│  │ / IOCP      │  │ Transfer    │                      │
│  └─────────────┘  └─────────────┘                      │
└─────────────────────┬───────────────────────────────────┘
                      │ Direct I/O
                ┌─────▼─────┐
                │ NVMe SSD  │
                └───────────┘
```

## 核心数据结构

### 1. 页头 (PageHeader)

```rust
/// 页头 32 bytes (4KB 对齐页)
/// Page header 32 bytes (4KB aligned page)
#[repr(C)]
pub struct PageHeader {
  pub version: u64,      // 乐观锁版本 Optimistic lock version
  pub page_id: u32,      // 自身 ID Self ID
  pub page_type: u8,     // 1=Internal, 2=Leaf
  pub level: u8,         // 层级 Level (叶子=0)
  pub item_count: u16,   // 元素数量 Item count
  pub free_begin: u16,   // 空闲区开始 Free space begin
  pub free_end: u16,     // 空闲区结束 Free space end
  pub next_page: u32,    // 叶子链表 Leaf chain
  pub prev_page: u32,    // 双向链表 Double linked
}
```

### 2. 零拷贝节点视图 (NodeView)

```rust
/// 内部节点视图 (不持有数据，只借用)
/// Internal node view (borrows data, no ownership)
pub struct InternalView<'a> {
  data: &'a [u8],
}

impl<'a> InternalView<'a> {
  /// O(1) 获取键，直接返回 slice
  /// O(1) get key, returns slice directly
  #[inline]
  pub fn key(&self, idx: usize) -> &[u8] {
    let slot = self.slot(idx);
    &self.data[slot.key_off..slot.key_off + slot.key_len]
  }

  /// 二分查找子节点
  /// Binary search for child
  pub fn find_child(&self, key: &[u8]) -> usize {
    let n = self.count();
    if n == 0 { return 0; }
    let mut lo = 0;
    let mut hi = n;
    while lo < hi {
      let mid = lo + (hi - lo) / 2;
      if self.key(mid) <= key {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    lo
  }
}
```

## 页面布局

### 叶子节点 (Leaf Node)

```
┌─────────────────────────────────────────┐
│ PageHeader (32 bytes)                   │
│   version, page_id, type, level, count  │
│   free_begin, free_end, next, prev      │
├─────────────────────────────────────────┤
│ Slot Directory (从前向后增长)           │
│   [key_off:u16, key_len:u16, val:u64]   │
│   × item_count                          │
├─────────────────────────────────────────┤
│ ... Free Space ...                      │
├─────────────────────────────────────────┤
│ Key Data (从后向前增长)                 │
└─────────────────────────────────────────┘
```

### 内部节点 (Internal Node)

```
┌─────────────────────────────────────────┐
│ PageHeader (32 bytes)                   │
├─────────────────────────────────────────┤
│ first_child: u32 (第一个子指针)         │
│ Slot[0]: key_off, key_len, child_id     │
│ Slot[1]: key_off, key_len, child_id     │
│ ...                                     │
├─────────────────────────────────────────┤
│ ... Free Space ...                      │
├─────────────────────────────────────────┤
│ Key Data (从后向前增长)                 │
└─────────────────────────────────────────┘
```

## 并发控制

采用乐观锁 + 子表内写串行化：

```
get(key):
  1. 乐观读：记录版本号 → 读取 → 验证版本
  2. 版本冲突则重试
  3. 重试超限则降级为悲观读

insert(key, value):
  1. 子表内写操作串行化 (由上层保证)
  2. 找到叶子节点路径
  3. 尝试插入，空间不足则分裂
  4. 分裂时锁住节点 (version bit 63)
```

## 性能对比

| 操作 | 传统设计 | 优化后 | 提升 |
|------|----------|--------|------|
| 节点解码 | 1-5 μs (Vec alloc) | 10-50 ns (view) | 20-100x |
| 并发读取 | RwLock 竞争 | 无锁乐观读 | 5-10x |
| I/O 吞吐 | tokio (epoll) | compio (io_uring) | 2-3x |

## 开发状态

- [x] 键编码 (可比较字节序)
- [x] 零拷贝节点视图
- [x] 乐观锁版本控制
- [x] B+ 树 CRUD
- [x] 范围扫描游标
- [x] 节点分裂
