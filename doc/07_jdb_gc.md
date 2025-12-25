# jdb_gc - 垃圾回收 / Garbage Collection

## 概述 / Overview

jdb_gc 提供 Page GC 和 VLog GC，回收不可达的页和旧值文件。

## Page GC

回收 B+ Tree 中不可达的页（CoW 产生的旧页）。

```rust
let mut gc = PageGc::new();

// Mark reachable pages (traverse tree) / 标记可达页（遍历树）
gc.mark(root_page);
gc.mark_all(child_pages);

// Sweep / 清扫
let freed = gc.sweep(&mut store);
```

### 流程 / Flow

1. 从根页开始遍历 B+ Tree
2. 标记所有可达页
3. 未标记的页加入 free_list

## VLog GC

回收不再被引用的旧 VLog 文件。

```rust
let mut gc = VlogGc::new();

// Mark live refs (from tree leaves) / 标记存活引用（从树叶子）
gc.mark(&vref);

// Find deletable files / 查找可删除文件
let deletable = gc.deletable_files(&all_files);

// Delete / 删除
VlogGc::delete_files(vlog_dir, &deletable)?;
```

### 策略 / Strategy

- 只删除整个文件（无文件内 compaction）
- 文件内所有引用都失效后才可删除
- 保留活跃文件（正在写入的）

## GcStats

```rust
pub struct GcStats {
  pub total: u64,     // 总页数
  pub reachable: u64, // 可达页数
  pub garbage: u64,   // 垃圾页数
}

// 垃圾比例
stats.ratio() -> f64
```

## 依赖 / Dependencies

- jdb_page: 页存储
- jdb_trait: ValRef
