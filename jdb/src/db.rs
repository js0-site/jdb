//! Database core structure
//! 数据库核心结构

use std::{cell::RefCell, ops::Bound, path::PathBuf, rc::Rc};

use jdb_base::{
  Pos,
  table::{SsTable, Table as _, TableMut, prefix_end},
};
use jdb_ckp::Ckp;
use jdb_fs::fs_id::id_path;
use jdb_index::{Merge, MergeAsc, MergeDesc};
use jdb_mem::{Mem, MemIter, MemRevIter};
use jdb_sstable::{MultiAsc, MultiDesc, Read, Table};
use wlog::Wal;
use zbin::Bin;

use crate::{Conf, Result, load::SST_DIR};

// Default memory table threshold: 64MB
// 默认内存表阈值：64MB
const DEFAULT_MEM_THRESHOLD: u64 = 64 * 1024 * 1024;

// Default file LRU capacity
// 默认文件 LRU 容量
const DEFAULT_FILE_CAP: usize = 16;

/// Database core structure
/// 数据库核心结构
pub struct Db {
  /// Data directory / 数据目录
  pub dir: PathBuf,
  /// Active memtable / 活跃内存表
  pub mem: Mem,
  /// Frozen memtables / 冻结内存表
  pub frozen: Vec<Mem>,
  /// WAL manager / WAL 管理器
  pub wal: Wal,
  /// Checkpoint manager / 检查点管理器
  pub ckp: Rc<RefCell<Ckp>>,
  /// SSTable read manager / SSTable 读取管理器
  pub sst: Read,
  /// Memory table size threshold / 内存表大小阈值
  pub mem_threshold: u64,
  /// Compaction in progress flag / 压缩进行中标志
  compacting: bool,
}

impl Db {
  /// Open database at path
  /// 打开数据库
  pub async fn open(path: impl Into<PathBuf>, conf: &[Conf]) -> Result<Self> {
    let dir = path.into();

    // Extract configurations in a single pass to avoid redundant iteration
    // 单次遍历提取配置，避免重复迭代
    let mut mem_threshold = DEFAULT_MEM_THRESHOLD;
    let mut file_cap = DEFAULT_FILE_CAP;
    let mut ckp_conf = Vec::new();
    let mut wal_conf = Vec::new();

    for c in conf {
      match c {
        Conf::MemThreshold(v) => mem_threshold = *v,
        Conf::FileCap(v) => file_cap = *v,
        Conf::Ckp(c) => ckp_conf.push(*c),
        Conf::Wal(c) => wal_conf.push(*c),
      }
    }

    // 1. Open checkpoint / 打开检查点
    let (ckp, after) = jdb_ckp::open(&dir, &ckp_conf).await?;
    let ckp = Rc::new(RefCell::new(ckp));

    // 2. Create memtable for recovery / 创建用于恢复的内存表
    let mut mem = Mem::new();

    // 3. Open WAL with recovery callback / 打开 WAL 并通过回调恢复
    let wal = Wal::open(&dir, &wal_conf, after.as_ref(), |key, pos| {
      if pos.is_tombstone() {
        mem.rm(key);
      } else {
        mem.put(key, pos);
      }
    })
    .await?;

    // 4. Load SSTable manager / 加载 SSTable 管理器
    let sst_dir = dir.join(SST_DIR);
    let sst = Read::load(&sst_dir, file_cap, Rc::clone(&ckp)).await?;

    Ok(Self::from_parts(dir, mem, wal, ckp, sst, mem_threshold))
  }

  /// Create Db from components
  /// 从组件创建 Db
  pub(crate) fn from_parts(
    dir: PathBuf,
    mem: Mem,
    wal: Wal,
    ckp: Rc<RefCell<Ckp>>,
    sst: Read,
    mem_threshold: u64,
  ) -> Self {
    Self {
      dir,
      mem,
      frozen: Vec::new(),
      wal,
      ckp,
      sst,
      mem_threshold,
      compacting: false,
    }
  }

  /// Check if flush is needed
  /// 检查是否需要 flush
  #[inline]
  pub(crate) fn need_flush(&self) -> bool {
    self.mem.size() >= self.mem_threshold
  }

  /// Write key-value pair
  /// 写入键值对
  ///
  /// 1. Write to WAL, get Pos
  /// 2. Update memtable
  /// 3. Check flush threshold
  pub async fn put<'a, 'b>(&mut self, key: impl Bin<'a>, val: impl Bin<'b>) -> Result<()> {
    let key = key.as_slice();
    let val = val.as_slice();

    // 1. Write WAL, get Pos / 写 WAL，获取 Pos
    let pos = self.wal.put(key, val).await?;

    // 2. Update memtable / 更新内存表
    self.mem.put(key, pos);

    // 3. Check flush threshold / 检查 flush 阈值
    if self.need_flush() {
      self.trigger_flush();
    }

    Ok(())
  }

  /// Delete key (write tombstone)
  /// 删除键（写入 tombstone）
  ///
  /// 1. Write tombstone to WAL
  /// 2. Update memtable
  pub async fn rm<'a>(&mut self, key: impl Bin<'a>) -> Result<()> {
    let key = key.as_slice();

    // 1. Write tombstone to WAL / 写 tombstone 到 WAL
    self.wal.rm(key).await?;

    // 2. Update memtable / 更新内存表
    self.mem.rm(key);

    Ok(())
  }

  /// Trigger flush: freeze current memtable, create new one
  /// 触发 flush：冻结当前内存表，创建新的
  fn trigger_flush(&mut self) {
    let old_mem = std::mem::replace(&mut self.mem, Mem::new());
    self.frozen.push(old_mem);
  }

  /// Flush frozen memtables to SSTable
  /// 将冻结的内存表刷写到 SSTable
  ///
  /// 1. Pop oldest frozen memtable
  /// 2. Write to SSTable
  /// 3. Load Table
  /// 4. Save checkpoint
  #[allow(clippy::await_holding_refcell_ref)]
  pub async fn flush(&mut self) -> Result<()> {
    // Pop oldest frozen memtable (FIFO order)
    // 弹出最旧的冻结内存表（FIFO 顺序）
    let Some(mem) = self.frozen.first() else {
      return Ok(());
    };

    // Skip empty memtable
    // 跳过空内存表
    if mem.is_empty() {
      self.frozen.remove(0);
      return Ok(());
    }

    // Ensure SST directory exists
    // 确保 SST 目录存在
    let sst_dir = self.sst.dir();
    if !sst_dir.exists() {
      std::fs::create_dir_all(sst_dir)?;
    }

    // Write memtable to SSTable
    // 将内存表写入 SSTable
    let meta = jdb_sstable::write(sst_dir, 0, mem, &[]).await?;

    // Load Table for new SSTable
    // 加载新 SSTable 的 Table
    let path = id_path(sst_dir, meta.id);
    let table_info = Table::load(&path, meta.id).await?;

    // Remove from frozen list after successful write
    // 写入成功后从冻结列表移除
    self.frozen.remove(0);

    // Add to SSTable manager
    // 添加到 SSTable 管理器
    self.sst.add(table_info);

    // Record SST in checkpoint
    // 在检查点中记录 SST
    self.ckp.borrow_mut().sst_add(meta.id, 0).await?;

    // Flush WAL to ensure data is persisted
    // 刷新 WAL 确保数据持久化
    self.wal.flush().await?;

    // Save checkpoint with current WAL position
    // 保存检查点，记录当前 WAL 位置
    self
      .ckp
      .borrow_mut()
      .set_wal_ptr(self.wal.cur_id(), self.wal.cur_pos())
      .await?;

    Ok(())
  }

  /// Flush all frozen memtables
  /// 刷写所有冻结的内存表
  pub async fn flush_all(&mut self) -> Result<()> {
    while !self.frozen.is_empty() {
      self.flush().await?;
    }
    Ok(())
  }

  /// Check if compaction is needed
  /// 检查是否需要压缩
  #[inline]
  pub fn need_compact(&mut self) -> bool {
    self.sst.levels_mut().needs_compaction(0)
  }

  /// Execute one round of compaction
  /// 执行一轮压缩
  ///
  /// Returns true if compaction was performed
  /// 如果执行了压缩则返回 true
  pub async fn compact(&mut self) -> Result<bool> {
    // Prevent concurrent compaction
    // 防止并发压缩
    if self.compacting {
      return Ok(false);
    }
    self.compacting = true;

    let result = self.sst.compact().await;

    self.compacting = false;
    Ok(result?)
  }

  /// Execute compaction until no more work
  /// 执行压缩直到没有更多工作
  pub async fn compact_all(&mut self) -> Result<()> {
    while self.compact().await? {}
    Ok(())
  }

  /// Flush and compact (typical maintenance operation)
  /// 刷写并压缩（典型维护操作）
  pub async fn maintain(&mut self) -> Result<()> {
    self.flush_all().await?;
    self.compact_all().await?;
    Ok(())
  }

  /// Get value by key
  /// 根据键获取值
  ///
  /// Search order: active mem → frozen mems → SSTables
  /// 查找顺序：活跃内存表 → 冻结内存表 → SSTable
  pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    // 1. Check active memtable / 查活跃内存表
    if let Some(pos) = self.mem.get(key) {
      return self.read_pos(pos).await;
    }

    // 2. Check frozen memtables (newest first) / 查冻结内存表（从新到旧）
    for mem in self.frozen.iter().rev() {
      if let Some(pos) = mem.get(key) {
        return self.read_pos(pos).await;
      }
    }

    // 3. Check SSTables via Read manager / 通过 Read 管理器查 SSTable
    if let Some(pos) = SsTable::get(&mut self.sst, key).await {
      return self.read_pos(pos).await;
    }

    Ok(None)
  }

  /// Read value from Pos
  /// 从 Pos 读取值
  async fn read_pos(&mut self, pos: Pos) -> Result<Option<Vec<u8>>> {
    if pos.is_tombstone() {
      return Ok(None);
    }
    let val = self.wal.val(pos).await?;
    Ok(Some(val.to_vec()))
  }

  /// Range query with bounds, merging Mem + Frozen + SSTable
  /// 范围查询，合并 Mem + Frozen + SSTable
  ///
  /// Returns async Stream of (key, Pos) pairs in ascending order.
  /// Tombstones are skipped by default.
  /// 返回升序的 (key, Pos) 异步流，默认跳过删除标记。
  pub fn range(
    &mut self,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> MergeAsc<MemIter<'_>, MultiAsc<'_>> {
    self.range_inner(start, end, true)
  }

  /// Range query including tombstones
  /// 范围查询（包含删除标记）
  pub fn range_with_tombstone(
    &mut self,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> MergeAsc<MemIter<'_>, MultiAsc<'_>> {
    self.range_inner(start, end, false)
  }

  /// Internal range query implementation
  /// 内部范围查询实现
  fn range_inner(
    &mut self,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
    skip_rm: bool,
  ) -> MergeAsc<MemIter<'_>, MultiAsc<'_>> {
    // Collect mem iterators: active mem first (newest), then frozen (newest to oldest)
    // 收集内存迭代器：活跃内存表优先（最新），然后是冻结表（从新到旧）
    let mut mem_iters: Vec<MemIter<'_>> = Vec::with_capacity(1 + self.frozen.len());
    mem_iters.push(self.mem.range(start, end));
    for m in self.frozen.iter().rev() {
      mem_iters.push(m.range(start, end));
    }

    // Create SSTable stream via Read manager
    // 通过 Read 管理器创建 SSTable 流
    let sst_stream = self.sst.range(start, end);

    Merge::new(mem_iters, sst_stream, skip_rm)
  }

  /// Reverse range query with bounds
  /// 反向范围查询
  ///
  /// Returns async Stream of (key, Pos) pairs in descending order.
  /// 返回降序的 (key, Pos) 异步流。
  pub fn rev_range(
    &mut self,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> MergeDesc<MemRevIter<'_>, MultiDesc<'_>> {
    self.rev_range_inner(start, end, true)
  }

  /// Reverse range query including tombstones
  /// 反向范围查询（包含删除标记）
  pub fn rev_range_with_tombstone(
    &mut self,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> MergeDesc<MemRevIter<'_>, MultiDesc<'_>> {
    self.rev_range_inner(start, end, false)
  }

  /// Internal reverse range query implementation
  /// 内部反向范围查询实现
  fn rev_range_inner(
    &mut self,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
    skip_rm: bool,
  ) -> MergeDesc<MemRevIter<'_>, MultiDesc<'_>> {
    // Collect mem iterators (reversed): active mem first, then frozen
    // 收集内存迭代器（反向）：活跃内存表优先，然后是冻结表
    let mut mem_iters: Vec<MemRevIter<'_>> = Vec::with_capacity(1 + self.frozen.len());
    mem_iters.push(self.mem.rev_range(start, end));
    for m in self.frozen.iter().rev() {
      mem_iters.push(m.rev_range(start, end));
    }

    // Create SSTable stream via Read manager
    // 通过 Read 管理器创建 SSTable 流
    let sst_stream = self.sst.rev_range(start, end);

    Merge::new(mem_iters, sst_stream, skip_rm)
  }

  /// Iterate all entries in ascending order
  /// 升序迭代所有条目
  #[inline]
  pub fn iter(&mut self) -> MergeAsc<MemIter<'_>, MultiAsc<'_>> {
    self.range(Bound::Unbounded, Bound::Unbounded)
  }

  /// Iterate all entries in descending order
  /// 降序迭代所有条目
  #[inline]
  pub fn rev_iter(&mut self) -> MergeDesc<MemRevIter<'_>, MultiDesc<'_>> {
    self.rev_range(Bound::Unbounded, Bound::Unbounded)
  }

  /// Prefix query in ascending order
  /// 前缀查询（升序）
  #[inline]
  pub fn prefix(&mut self, prefix: &[u8]) -> MergeAsc<MemIter<'_>, MultiAsc<'_>> {
    let start = Bound::Included(prefix);
    match prefix_end(prefix) {
      Some(end) => self.range(start, Bound::Excluded(end.as_ref())),
      None => self.range(start, Bound::Unbounded),
    }
  }

  /// Prefix query in descending order
  /// 前缀查询（降序）
  #[inline]
  pub fn rev_prefix(&mut self, prefix: &[u8]) -> MergeDesc<MemRevIter<'_>, MultiDesc<'_>> {
    let start = Bound::Included(prefix);
    match prefix_end(prefix) {
      Some(end) => self.rev_range(start, Bound::Excluded(end.as_ref())),
      None => self.rev_range(start, Bound::Unbounded),
    }
  }
}
