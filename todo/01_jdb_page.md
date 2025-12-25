# jdb_page 开发计划

## 目标 / Goal

物理页存储管理，支持 Direct I/O 读写。

---

## 数据结构 / Data Structures

```rust
/// 页 ID / Page ID
pub struct PageId(pub u64);

/// 页头 (每页开头 16 字节) / Page Header
#[repr(C)]
pub struct PageHeader {
    pub checksum: u32,  // CRC32
    pub flags: u16,     // 页类型标志
    pub _pad: [u8; 10], // 对齐填充
}

/// 页存储 / Page Store
pub struct PageStore {
    file: jdb_fs::File,
    alloc: BitMap,      // 空闲页位图
    cap: u64,           // 总页数
}
```

---

## 接口设计 / API Design

```rust
impl PageStore {
    /// 打开/创建 / Open or create
    pub async fn open(path: &Path) -> Result<Self>;
    
    /// 分配新页 / Allocate new page
    pub async fn alloc(&mut self) -> Result<PageId>;
    
    /// 释放页 / Free page
    pub fn free(&mut self, id: PageId);
    
    /// 读取页 / Read page
    pub async fn read(&self, id: PageId) -> Result<Page>;
    
    /// 写入页 / Write page
    pub async fn write(&self, id: PageId, page: &Page) -> Result<()>;
    
    /// 同步 / Sync
    pub async fn sync(&self) -> Result<()>;
}
```

---

## 实现步骤 / Implementation Steps

### Step 1: 基础结构

- [ ] `PageId` 类型定义
- [ ] `PageHeader` 结构
- [ ] `PageStore` 骨架

### Step 2: 位图分配器

- [ ] `BitMap` 结构 (基于 `roaring` 或自实现)
- [ ] `alloc()` 分配
- [ ] `free()` 释放
- [ ] 持久化位图

### Step 3: 读写操作

- [ ] `read()` 实现 (使用 jdb_buf::Page)
- [ ] `write()` 实现
- [ ] checksum 校验

### Step 4: 文件管理

- [ ] 文件头 (magic, version, page_count)
- [ ] 自动扩容
- [ ] `sync()` 实现

---

## 测试用例 / Test Cases

- [ ] 基础读写
- [ ] 分配/释放
- [ ] checksum 校验
- [ ] 扩容测试
- [ ] 崩溃恢复 (位图一致性)

---

## 依赖 / Dependencies

- jdb_alloc
- jdb_buf
- jdb_fs

---

## 预计时间 / Estimated Time

1-2 周
