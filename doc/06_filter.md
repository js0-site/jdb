# jdb_filter - 概率过滤器模块

定位: 封装 Binary Fuse Filter，用于快速判定 key 是否存在。

## 依赖

```toml
xorf = "0.12"
```

## 模块结构

```
jdb_filter/src/
└── lib.rs    # Filter 实现
```

## 设计背景

### 过滤器对比

| 过滤器 | 空间开销 | 查询 | 支持删除 |
|-------|---------|------|---------|
| Bloom | 44% | O(k) | ❌ |
| Cuckoo | 23% | O(1) | ✅ |
| **Binary Fuse** | **13%** | **O(1)** | ❌ |

Binary Fuse Filter 空间效率最高，适合只读场景。

## 核心类型

```rust
use xorf::{BinaryFuse8, Filter as XorfFilter};

/// Binary Fuse Filter 封装
pub struct Filter(BinaryFuse8);

impl Filter {
  /// 从 u64 key 列表构建 Build from u64 keys
  pub fn new(keys: &[u64]) -> Option<Self>;

  /// 检查 key 是否可能存在 Check if key may exist
  #[inline]
  pub fn may_contain(&self, key: u64) -> bool;

  /// 内存占用 Memory usage in bytes
  pub fn size(&self) -> usize;
}
```

## 使用示例

```rust
use jdb_filter::Filter;

let keys = vec![1u64, 2, 3, 100, 200];
let filter = Filter::new(&keys).unwrap();

assert!(filter.may_contain(1));
assert!(filter.may_contain(100));
assert!(!filter.may_contain(999));  // 大概率 false
```

## 性能特性

- 构建: O(n)
- 查询: O(1)，3 次内存访问
- 空间: 每 key 约 9 bits (1.125 bytes)
- 误判率: ~0.4%

## 设计要点

1. **极简封装**: 仅包装 xorf::BinaryFuse8
2. **零依赖**: 不依赖 jdb_comm，上层自行 hash
3. **零拷贝查询**: contains 无内存分配
