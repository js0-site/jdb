# KV 分离数据库设计
# KV Separation Database Design

## 架构 / Architecture

```
┌─────────────────────────────────────────────────┐
│                    jdb (DB)                      │
├─────────────────────────────────────────────────┤
│  Index: HashMap<Key, Pos> + CuckooFilter        │
├─────────────────────────────────────────────────┤
│  Value: jdb_val::Wal                            │
├─────────────────────────────────────────────────┤
│  Storage: wal/ + bin/                           │
└─────────────────────────────────────────────────┘
```

## 核心组件 / Core Components

### Index 索引

```rust
use papaya::HashMap;
use autoscale_cuckoo_filter::CuckooFilter;
use jdb_val::Pos;

pub struct Index {
  map: HashMap<HipByt<'static>, Pos>,
  filter: CuckooFilter<[u8]>,  // fast miss
}
```

### Value 值存储

直接使用 `jdb_val::Wal`。

## 读写流程 / Read/Write Flow

### Write

```
put(key, val)
  → wal.put(key, val) → Pos
  → index.insert(key, pos)
  → filter.add(key)
```

### Read

```
get(key)
  → filter.contains(key)?     // fast miss
  → index.get(key) → Pos?
  → wal.val(pos) → data
```

### Delete

```
del(key)
  → wal.del(key)
  → index.remove(key)
  → filter.remove(key)
```

## GC 流程 / GC Flow

```rust
impl Gcable for Index {
  async fn is_rm(&self, key: &[u8]) -> bool {
    !self.map.contains_key(key)
  }
  
  async fn batch_update(&self, mapping: impl IntoIterator<Item = PosMap>) -> bool {
    for m in mapping {
      self.map.insert(m.key, m.new);
    }
    true
  }
}

// 执行 GC
let ids = wal.iter().filter(|&id| id < wal.cur_id()).collect();
wal.gc_merge_compress(&ids, &index, &mut state, &mut DefaultGc, &index).await?;
```

## 配置 / Configuration

```rust
pub struct DbConf {
  pub cache_size: u64,      // 缓存大小 (default: 64MB)
  pub wal_max_size: u64,    // WAL 最大 (default: 256MB)
  pub filter_fpr: f64,      // 过滤器假阳性率 (default: 0.001)
}
```

## 使用示例 / Usage

```rust
let mut db = Db::open("./mydb", DbConf::default()).await?;

db.put(b"user:1", b"Alice").await?;

if let Some(val) = db.get(b"user:1").await? {
  println!("{}", String::from_utf8_lossy(&val));
}

db.del(b"user:1").await?;
db.sync().await?;
```

## 扩展方向 / Extensions

- SSTable 持久化索引
- Compaction 压缩
- Snapshot 快照
- Range scan 范围扫描
