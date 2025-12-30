# LRU Cache Benchmark Design / LRU 缓存基准测试设计

## Reference / 参考文献

Based on real-world cache datasets and research papers:

基于真实缓存数据集和研究论文：

1. **cache_dataset** (https://github.com/cacheMon/cache_dataset)
   - Twitter Twemcache: 54 clusters, 195B requests, 7-day traces
   - Meta KV Cache: 500-8000 hosts, 42GB DRAM + 930GB SSD per host
   - Meta CDN: Edge clusters with 40GB DRAM + 1.8TB SSD per host

2. **OSDI'20**: "A large scale analysis of hundreds of in-memory cache clusters at Twitter"

3. **FAST'20**: "Characterizing, Modeling, and Benchmarking RocksDB Key-Value Workloads at Facebook"

## Real-World Composite Workload Strategy / 真实世界混合工作负载策略

Based on Facebook USR/APP/VAR pool characteristics.

基于 Facebook USR/APP/VAR 池混合特征。

### Key Generation / Key 生成策略

| Parameter | Value | Description |
|-----------|-------|-------------|
| Pattern | `{prefix}_{id:04x}` | Namespace pattern / 命名空间模式 |
| Average Size | 32-64 Bytes | Matches Facebook APP pool median (53B) |
| Access Distribution | Zipf (α=1.2) | 20% keys → 80% accesses |

### Value Distribution Tiers / Value 分布层级

| Tier | Name | Weight % | Size Range | Content Type | Purpose |
|------|------|----------|------------|--------------|---------|
| 1 | Tiny Metadata | 40% | 16-100B | Binary packed | USR pool, Bloom filter test |
| 2 | Small Structs | 50% | 512B-2KB | JSON/Protobuf | APP pool, baseline perf |
| 3 | Medium Content | 9% | 10-50KB | HTML/Text | Bandwidth, compaction test |
| 4 | Huge Blobs | 1% | 500KB-1MB | Random bytes | VAR pool, latency spikes |

### Workload Mix / 工作负载混合

Based on Twitter/Facebook cluster characteristics:

基于 Twitter/Facebook 集群特征：

| Operation | Ratio | Description |
|-----------|-------|-------------|
| Read (GET) | 90% | Cache lookups / 缓存查找 |
| Write (SET) | 7% | Cache updates / 缓存更新 |
| Delete | 3% | Cache invalidation / 缓存失效 |

### Real Miss Rate / 真实 Miss 率

- 5% of reads are for keys that never existed
- 5% 的读取是针对从未存在的键
- Simulates cache penetration / negative lookups (e.g., 404s, security scans, deleted content)
- 模拟缓存穿透/空查询（如 404、安全扫描、已删除内容）
- Tests the efficiency of "miss path" handling (Bloom filters, hash lookups)
- 测试“未命中路径”的处理效率（布隆过滤器、哈希查找）

## Implementation Notes / 实现说明

### Data Generation / 数据生成

1. Use real text files as source (for compression testing)
   使用真实文本文件作为源（用于压缩测试）

2. Generate slices matching tier size distributions
   生成符合层级大小分布的切片

3. Data is shuffled after generation to mix sizes
   数据生成后打乱以混合大小

### Capacity Calculation / 容量计算

For target hit rate H with Zipf parameter s and N items:

对于目标命中率 H、Zipf 参数 s 和 N 个条目：

1. Find k such that cumulative Zipf probability >= H
   找到 k 使得累积 Zipf 概率 >= H

2. Apply compensation factor for real-world effects:
   应用真实场景补偿系数：
   - Delete ops remove hot items / 删除操作移除热条目
   - Size variance causes uneven eviction / 大小方差导致不均匀驱逐
   - Write churn affects stability / 写入扰动影响稳定性

3. `weight_capacity = avg_size × k × factor`

### Effective OPS Calculation / 有效 OPS 计算

```
effective_ops = 1 / (hit_time + miss_rate × miss_latency)
```

Where / 其中：
- `hit_time = 1 / raw_ops_per_second`
- `miss_rate = 1 - hit_rate`
- `miss_latency` = measured NVMe 4KB random read latency
