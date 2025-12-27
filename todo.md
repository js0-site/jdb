# jdb_slab 性能优化方案

## 当前性能基线

| Engine | Small W | Medium W | Large W | Disk B/rec | Mem |
|--------|---------|----------|---------|------------|-----|
| jdb_slab | ~4.6K | ~25K | ~9.7K | 374KB | 9MB |
| fjall | ~629K | ~150K | ~6.5K | 9.4KB | 15MB |
| rocksdb | ~329K | ~143K | ~6.5K | 21B | 17MB |

**问题**: jdb_slab 小数据写入性能差 100 倍+

## 根因分析

1. **Direct I/O 对齐开销**: 写 8B 数据需要写 16KB 对齐块
2. **每次写入都落盘**: 无缓冲，每次 put 都是系统调用
3. **无批量写入**: 不支持 batch write

## 优化方案

### Phase 1: WAL 小数据缓冲 (预期 10x 提升)

- [x] 创建 `wal.rs` 模块
- [x] 创建 `path.rs` 路径编码复用
- [ ] 实现 `WalManager` 管理多个 WAL
- [ ] Engine 集成 WAL：小数据 (< 4KB) 走 WAL
- [ ] SlotId 编码支持 WAL 标识
- [ ] 读取时先查 WAL，再查 slab

**配置参数**:
```rust
pub struct WalConfig {
  threshold: usize,    // 4KB - 小于此值走 WAL
  max_size: u64,       // 16MB - 单 WAL 大小上限
  compact_ratio: f64,  // 0.5 - 死空间比例触发 compact
}
```

### Phase 2: WAL Compact (空间回收)

- [ ] 单 WAL compact: dead_ratio > 50%
- [ ] 多 WAL merge: sealed WAL > 4 个时合并
- [ ] 后台 compact 任务

### Phase 3: 读缓存 (预期 2x 读提升)

- [ ] LRU 读缓存 (可选)
- [ ] 热数据内存缓存

### Phase 4: 批量写入 API

- [ ] `put_batch()` 批量写入接口
- [ ] 减少系统调用次数

## 验证步骤

```bash
# 优化前基线
./jdb_bench/bench.sh

# 每完成一个 Phase 后
./test.sh
./sh/clippy.sh
./jdb_bench/bench.sh
```

## 目标性能

| Engine | Small W | Medium W | Large W |
|--------|---------|----------|---------|
| jdb_slab | 50K+ | 50K+ | 10K+ |

小数据写入目标: 从 4.6K 提升到 50K+ ops/s (10x+)

## 实现顺序

1. `WalManager` - 管理 active + sealed WALs
2. `slot.rs` - 扩展 SlotId 编码支持 WAL
3. `engine.rs` - 集成 WAL 路由
4. 测试验证
5. Compact 实现
6. 性能调优
