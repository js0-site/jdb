# Snapshot Design for Safe Iteration
# 安全遍历的快照设计

## Problem / 问题

During range iteration, background operations may invalidate data:
遍历期间，后台操作可能使数据失效：

1. MemTable flush → data moves to SSTable
2. SSTable compaction → files merged/deleted  
3. WAL GC → WAL files deleted

## Solution: Snapshot + Reference Counting
## 解决方案：快照 + 引用计数

### Architecture / 架构

```
Jdb
 ├── NsIndex (per namespace)
 │    ├── active_mem: Arc<Mem>           // Current writable memtable
 │    ├── frozen_mems: Vec<Arc<Mem>>     // Immutable, pending flush
 │    └── sstables: Vec<Arc<TableInfo>>  // SSTable references
 │
 └── Wal
      └── wal_refs: HashMap<u64, usize>  // WAL ID → ref count
```

### Snapshot Structure / 快照结构

```rust
pub struct Snapshot {
    // Memtable snapshots (Arc ensures data lives)
    // 内存表快照（Arc 保证数据存活）
    mems: Vec<Arc<Mem>>,
    
    // SSTable snapshots (Arc prevents deletion)
    // SSTable 快照（Arc 阻止删除）
    sstables: Vec<Arc<TableInfo>>,
    
    // WAL IDs referenced (increment ref count on create)
    // 引用的 WAL ID（创建时增加引用计数）
    wal_ids: Vec<u64>,
    
    // Sequence number for MVCC
    // MVCC 序列号
    seq: u64,
}
```

### Lifecycle / 生命周期

```
1. Create Snapshot
   创建快照
   ├── Clone Arc<Mem> for all memtables
   │   克隆所有内存表的 Arc
   ├── Clone Arc<TableInfo> for all sstables
   │   克隆所有 SSTable 的 Arc
   └── Increment WAL ref counts
       增加 WAL 引用计数

2. Iterate with Snapshot
   使用快照遍历
   └── MergeStream uses snapshot's sources
       MergeStream 使用快照的数据源

3. Drop Snapshot
   释放快照
   ├── Arc<Mem> dropped (Mem freed if last ref)
   │   Arc<Mem> 释放（如果是最后引用则释放 Mem）
   ├── Arc<TableInfo> dropped (file deletable if last ref)
   │   Arc<TableInfo> 释放（如果是最后引用则可删除文件）
   └── Decrement WAL ref counts (GC can proceed if 0)
       减少 WAL 引用计数（如果为 0 则 GC 可继续）
```

### GC Safety Rules / GC 安全规则

```rust
// Before GC, check ref count
// GC 前检查引用计数
impl Wal {
    fn can_gc(&self, id: u64) -> bool {
        self.wal_refs.get(&id).map_or(true, |&c| c == 0)
    }
}

// Before SSTable deletion, check Arc strong count
// 删除 SSTable 前检查 Arc 强引用计数
impl NsIndex {
    fn can_delete_sstable(&self, table: &Arc<TableInfo>) -> bool {
        Arc::strong_count(table) == 1  // Only held by index
    }
}
```

### MemTable Flush Safety / 内存表 Flush 安全

```rust
impl NsIndex {
    async fn flush(&mut self) {
        // 1. Freeze current memtable (move to frozen list)
        //    冻结当前内存表（移到冻结列表）
        let frozen = std::mem::replace(&mut self.active_mem, Arc::new(Mem::new()));
        self.frozen_mems.push(frozen.clone());
        
        // 2. Write to SSTable
        //    写入 SSTable
        let sstable = write_sstable(&frozen).await;
        self.sstables.push(Arc::new(sstable));
        
        // 3. Remove from frozen list (snapshots may still hold Arc)
        //    从冻结列表移除（快照可能仍持有 Arc）
        self.frozen_mems.retain(|m| !Arc::ptr_eq(m, &frozen));
        // frozen's Arc dropped here, but snapshot's Arc keeps data alive
        // frozen 的 Arc 在此释放，但快照的 Arc 保持数据存活
    }
}
```

## API Design / API 设计

```rust
impl Jdb {
    /// Create snapshot for iteration
    /// 创建用于遍历的快照
    pub fn snapshot(&self, ns_id: NsId) -> Snapshot;
    
    /// Range query with snapshot
    /// 使用快照进行范围查询
    pub fn range<'a>(
        &'a self,
        snapshot: &'a Snapshot,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> impl Stream<Item = (HipByt, Val)> + 'a;
}

impl Snapshot {
    /// Create merge stream from snapshot sources
    /// 从快照源创建合并流
    pub fn merge_stream(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> MergeStream<...>;
}
```

## Implementation Priority / 实现优先级

1. **Phase 1**: Arc<Mem> for memtable (simple, high impact)
   阶段1：内存表使用 Arc<Mem>（简单，影响大）

2. **Phase 2**: Arc<TableInfo> for SSTable
   阶段2：SSTable 使用 Arc<TableInfo>

3. **Phase 3**: WAL ref counting
   阶段3：WAL 引用计数

4. **Phase 4**: Full Snapshot API
   阶段4：完整快照 API
