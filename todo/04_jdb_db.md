# jdb_db 开发计划

## 目标 / Goal

实现 `jdb_trait::Db`，单个子库的完整功能。

---

## 数据结构 / Data Structures

```rust
/// 子库 / Database
pub struct Db {
    id: DbId,
    tree: BTree,
    vlog: Arc<VLog>,
    head: Commit,        // 当前 Commit
    commits: CommitChain, // 版本链
}

/// 提交 / Commit
pub struct Commit {
    pub ts: u64,              // 时间戳
    pub root: PageId,         // B+ Tree 根
    pub parent: Option<PageId>, // 父 Commit
}

/// Pipeline 批量操作
pub struct DbPipeline {
    ops: Vec<Op>,
    db: Db,
}

enum Op {
    Put(Bytes, Bytes),
    Get(Bytes),
    Rm(Bytes),
}
```

---

## 接口实现 / API Implementation

```rust
impl Db for Db {
    async fn put(&self, key, val) -> Option<Prev> {
        // 1. val 写入 VLog -> ValRef
        // 2. (key, ValRef) 插入 BTree (CoW)
        // 3. 创建新 Commit
        // 4. 返回旧 ValRef (如有)
    }
    
    async fn get(&self, key) -> Option<Bytes> {
        // 1. BTree 查找 key -> ValRef
        // 2. VLog 读取 ValRef -> Bytes
    }
    
    async fn rm(&self, key) {
        // 1. BTree 删除 key (CoW)
        // 2. 创建新 Commit
    }
    
    async fn scan(&self, start, order) -> Stream<Kv> {
        // 1. BTree scan -> Stream<(key, ValRef)>
        // 2. 并行读取 VLog
    }
    
    async fn history(&self, key) -> Stream<ValRef> {
        // 遍历 Commit Chain，查找 key 的所有历史值
    }
    
    async fn fork(&self, ts, order) -> Option<Db> {
        // 1. 找到 ts 对应的 Commit
        // 2. 创建新 Db，共享 Commit
    }
    
    fn pipeline(&self) -> Pipeline {
        DbPipeline::new(self.clone())
    }
}
```

---

## 实现步骤 / Implementation Steps

### Step 1: 基础结构

- [ ] `Db` 结构
- [ ] `Commit` 结构
- [ ] 打开/关闭

### Step 2: 写入

- [ ] `put()` 实现
- [ ] `rm()` 实现
- [ ] Commit 创建

### Step 3: 读取

- [ ] `get()` 实现
- [ ] `scan()` 实现
- [ ] `val()` 实现

### Step 4: 版本控制

- [ ] `history()` 实现
- [ ] `fork()` 实现
- [ ] `last_ts()` 实现

### Step 5: Pipeline

- [ ] `DbPipeline` 结构
- [ ] 批量执行
- [ ] 结果 Stream

---

## 测试用例 / Test Cases

- [ ] 基础 CRUD
- [ ] 范围扫描
- [ ] 历史查询
- [ ] Fork 测试
- [ ] Pipeline 测试
- [ ] 并发读写

---

## 依赖 / Dependencies

- jdb_tree
- jdb_vlog
- jdb_trait

---

## 预计时间 / Estimated Time

1-2 周
