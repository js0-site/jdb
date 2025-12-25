# jdb_vlog 开发计划

## 目标 / Goal

KV 分离的值存储层，append-only 日志。

---

## ValRef 结构 / ValRef Structure

```rust
pub struct ValRef {
  pub file_id: u64,
  pub offset: u64,
  pub prev_file_id: u64, // 0 = 无前驱
  pub prev_offset: u64,
}
```

ValRef 包含前驱指针，用于 `history()` 遍历同一 key 的历史版本。

---

## 数据结构 / Data Structures

```rust
pub struct VLog {
    dir: PathBuf,
    active: VLogFile,
    file_li: Vec<VLogFile>,
}

struct VLogFile {
    id: u64,
    file: jdb_fs::File,
    size: u64,
}
```

---

## 记录格式 / Record Format

```
┌──────────┬──────────┬───────┬──────────┬──────────┐
│ len (8B) │ crc (4B) │ flag  │ key_len  │ key      │
├──────────┴──────────┴───────┴──────────┴──────────┤
│ value (flag=0 时有，flag=1 tombstone 时无)         │
└──────────────────────────────────────────────────┘

flag: 0 = 正常值, 1 = tombstone
```

---

## 接口设计 / API Design

```rust
impl VLog {
    pub async fn open(dir: &Path) -> Result<Self>;
    
    /// 追加值 / Append value
    pub async fn append(&mut self, key: &[u8], val: &[u8], prev: Option<&ValRef>) -> Result<ValRef>;
    
    /// 追加 tombstone / Append tombstone
    pub async fn append_tombstone(&mut self, key: &[u8], prev: Option<&ValRef>) -> Result<ValRef>;
    
    pub async fn get(&self, vref: &ValRef) -> Result<Bytes>;
    pub async fn sync(&self) -> Result<()>;
    pub async fn rotate(&mut self) -> Result<()>;
    pub fn mark(&mut self, vref: &ValRef);
    pub async fn gc(&mut self) -> Result<u64>;
}
```

---

## 实现步骤 / Implementation Steps

### Step 1: 基础结构

- [ ] `VLogFile` 封装
- [ ] `VLog` 骨架

### Step 2: 写入

- [ ] `append()`: 写入值到 VLog
- [ ] `append_tombstone()`: 写入 tombstone (flag=1, 无 value)
- [ ] 记录编码 + CRC
- [ ] 文件轮转

### Step 3: 读取

- [ ] `get()`: 读取 VLog (检查 tombstone 返回空)
- [ ] 记录解码 + CRC 校验

### Step 4: GC

- [ ] `mark()` 标记
- [ ] `gc()` 回收
- [ ] 文件删除

---

## 测试用例 / Test Cases

- [ ] 基础读写
- [ ] 大 value 测试
- [ ] tombstone 写入和读取
- [ ] 历史链遍历 (prev 指针，含 tombstone)
- [ ] 文件轮转
- [ ] GC 测试

---

## 依赖 / Dependencies

- jdb_fs
- jdb_alloc

---

## 预计时间 / Estimated Time

1 周
