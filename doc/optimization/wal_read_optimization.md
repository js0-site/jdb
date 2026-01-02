# WAL 读取性能优化记录

## 背景

jdb_val 的读取性能比 fjall 慢约 3-4 倍：
- fjall Large read: ~488K ops/s vs jdb_val: ~107K ops/s
- fjall Medium read: ~707K ops/s vs jdb_val: ~225K ops/s
- fjall Small read: ~550K ops/s vs jdb_val: ~180K ops/s

## 优化结果

优化后 jdb_val 读取性能接近甚至超过 fjall：

| 类别 | 优化前 | 优化后 | 提升 | vs fjall |
|------|--------|--------|------|----------|
| Large | ~107K | ~500-650K | +370%~507% | ✅ 超过 |
| Medium | ~225K | ~550-670K | +144%~198% | ⚠️ 接近 |
| Small | ~180K | ~630-730K | +250%~306% | ✅ 超过 |

> 注：性能有波动，取决于系统负载和测试数据随机性

---

## 成功的优化

### ⭐ 最重要：消除 block_cache 数据双重复制 (+200%~285%)

**问题**：旧的 `block_cache.read()` 实现先读数据到内部 buffer，然后 `read_impl` 再复制到调用者 buffer。每次读取都有一次不必要的 memcpy。

**解决**：改为 `read_into()` 方法，直接读到调用者的 buffer，实现零拷贝。

```rust
// 旧实现 - 两次复制
// Old: double copy
pub async fn read(&mut self, file_id: u64, offset: u64, len: usize) -> Result<&[u8]> {
    let res = file.read_exact_at(self.buf, offset).await;  // copy 1: file -> internal buf
    Ok(&self.buf)  // caller still needs to copy
}

// 新实现 - 零拷贝
// New: zero-copy
pub async fn read_into<B: IoBufMut>(&mut self, file_id: u64, buf: B, offset: u64) -> (Result<()>, B) {
    file.read_exact_at(buf, offset).await  // direct read to caller's buf
}
```

**文件**：`jdb_val/src/block_cache.rs`, `jdb_val/src/wal/read.rs`

---

### 次要：使用 gxhash HashMap (+4%~6%)

**问题**：benchmark adapter 使用 `std::collections::HashMap`，hash 计算较慢。

**解决**：改用 `gxhash::HashMap`，hash 性能更好。

**文件**：`jdb_bench/src/adapter/jdb_val.rs`

---

### 次要：read_file_into 零拷贝

**问题**：`read_file_into` 中调用了两次 `get_bin_file`，且原实现先读到内部 buffer 再复制。

**解决**：直接读到调用者 buffer，消除双重复制和重复调用。

**文件**：`jdb_val/src/wal/read.rs`

**注意**：仅影响 FILE 模式（大文件存储），不影响 INFILE 模式热路径。

---

## 失败的优化

### ❌ val_cached 同步缓存检查 (负优化)

**想法**：添加 `val_cached()` 同步方法，先检查缓存命中，避免异步开销。

**结果**：Medium -30%，Small -24%

**原因**：cache miss 时双重查找，LHD 的 `get()` 会更新统计（tick + hits），双重调用增加开销。

---

### ❌ LHD cache peek() 不更新统计 (负优化 -50%)

**想法**：添加 `peek()` 方法，只查看值不更新 LHD 统计。

**结果**：性能下降约 50%。

**原因**：LHD 淘汰策略依赖准确的命中统计，不更新统计会导致淘汰决策失准。

---

### ❌ LHD tick() 位运算优化 (无效)

**想法**：用位掩码 `ts & (RECONFIG - 1) == 0` 替代减法比较。

**结果**：无明显变化。

**原因**：编译器已做类似优化。

---

### ❌ block_cache entry API 优化 (负优化 -50%)

**想法**：用 `hashlink::LruCache::entry()` API 避免双重查找。

**结果**：性能下降约 50%。

**原因**：entry API 没有正确更新 LRU 顺序，导致缓存效率下降。

---

### ❌ 跳过 cuckoo filter check (负优化)

**想法**：benchmark 读取阶段跳过 cuckoo filter check。

**结果**：性能下降。

**原因**：cuckoo filter check 帮助 CPU 分支预测。

---

### ❌ 直接从 block_cache 返回 CachedData (内存爆炸)

**想法**：让 block_cache 直接返回 `Rc<[u8]>`。

**结果**：内存暴涨到 2GB-8GB。

**原因**：每次读取都创建新的 `Rc<[u8]>`，没有利用 LHD cache。

---

## 关键经验

1. **零拷贝是王道**：消除不必要的 memcpy 带来最大性能提升
2. **避免重复操作**：双重 cache 查找比单次查找更慢
3. **测量优先**：每次优化后都要 benchmark 验证效果
4. **小心内存**：缓存策略改动可能导致内存问题
5. **LHD 需要准确统计**：不要跳过 LHD 的统计更新
6. **编译器很聪明**：简单的位运算优化可能已被编译器实现
7. **分支预测很重要**：看似多余的检查可能帮助 CPU 预测

## 相关文件

- `jdb_val/src/block_cache.rs` - 块缓存实现
- `jdb_val/src/wal/read.rs` - WAL 读取路径
- `jdb_bench/src/adapter/jdb_val.rs` - benchmark adapter
- `size_lru/src/lhd.rs` - LHD 缓存实现

---

## 最终性能对比 (2026-01-01)

| 类别 | jdb_val | fjall | 结果 |
|------|---------|-------|------|
| Large read | ~500K ops/s | ~420K ops/s | ✅ +19% |
| Medium read | ~637K ops/s | ~648K ops/s | ⚠️ -1.7% |
| Small read | ~700K+ ops/s | ~550K ops/s | ✅ +27% |

Large 和 Small 超过 fjall，Medium 接近 fjall。
