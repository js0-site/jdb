//! Database core structure
//! 数据库核心结构

use std::{cell::RefCell, ops::Bound, path::PathBuf, rc::Rc};

use jdb_base::{
  Pos,
  table::{SsTable, TableMut, prefix_end},
};
use jdb_ckp::Ckp;
use jdb_fs::fs_id::id_path;
use jdb_index::{Merge, MergeAsc, MergeDesc};
use jdb_mem::{MemIter, MemRevIter, Mems};
use jdb_sst::{MultiAsc, MultiDesc, Read, Table};
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
  dir: PathBuf,
  /// Memtable manager / 内存表管理器
  mems: Rc<RefCell<Mems>>,
  /// WAL manager / WAL 管理器
  wal: Wal,
  /// Checkpoint manager / 检查点管理器
  ckp: Rc<RefCell<Ckp>>,
  /// SSTable read manager / SSTable 读取管理器
  sst: Read,
  /// Memory table size threshold / 内存表大小阈值
  mem_threshold: u64,
  /// Compaction in progress flag / 压缩进行中标志
  compacting: bool,
}

impl Db {
  /// Open database at path
  /// 打开数据库
  pub async fn open(path: impl Into<PathBuf>, conf: &[Conf]) -> Result<Self> {
    let dir = path.into();

    // Extract configurations
    // 提取配置
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

    // 2. Create mems for recovery / 创建用于恢复的内存表管理器
    let mems = Rc::new(RefCell::new(Mems::new()));

    // 3. Open WAL with recovery callback / 打开 WAL 并通过回调恢复
    let wal = {
      let mems_ref = Rc::clone(&mems);
      Wal::open(&dir, &wal_conf, after.as_ref(), move |key, pos| {
        let mut mem = mems_ref.borrow_mut();
        if pos.is_tombstone() {
          mem.active_mut().rm(key);
        } else {
          mem.active_mut().put(key, pos);
        }
      })
      .await?
    };

    // 4. Load SSTable manager / 加载 SSTable 管理器
    let sst_dir = dir.join(SST_DIR);
    let sst = Read::load(&sst_dir, file_cap, Rc::clone(&ckp)).await?;

    Ok(Self {
      dir,
      mems,
      wal,
      ckp,
      sst,
      mem_threshold,
      compacting: false,
    })
  }

  /// Check if flush is needed
  /// 检查是否需要 flush
  #[inline]
  pub fn need_flush(&self) -> bool {
    self.mems.borrow().active_size() >= self.mem_threshold
  }

  /// Write key-value pair
  /// 写入键值对
  pub async fn put<'a, 'b>(&mut self, key: impl Bin<'a>, val: impl Bin<'b>) -> Result<()> {
    let key = key.as_slice();
    let val = val.as_slice();

    // 1. Write WAL, get Pos / 写 WAL，获取 Pos
    let pos = self.wal.put(key, val).await?;

    // 2. Update memtable / 更新内存表
    self.mems.borrow_mut().active_mut().put(key, pos);

    // 3. Check flush threshold / 检查 flush 阈值
    if self.need_flush() {
      self.mems.borrow_mut().freeze();
    }

    Ok(())
  }

  /// Delete key (write tombstone)
  /// 删除键（写入 tombstone）
  pub async fn rm<'a>(&mut self, key: impl Bin<'a>) -> Result<()> {
    let key = key.as_slice();

    // 1. Write tombstone to WAL / 写 tombstone 到 WAL
    self.wal.rm(key).await?;

    // 2. Update memtable / 更新内存表
    self.mems.borrow_mut().active_mut().rm(key);

    Ok(())
  }

  /// Flush oldest frozen memtable to SSTable
  /// 将最旧的冻结内存表刷写到 SSTable
  #[allow(clippy::await_holding_refcell_ref)]
  pub async fn flush(&mut self) -> Result<()> {
    let mems = self.mems.borrow();
    let Some(mem) = mems.oldest_frozen() else {
      return Ok(());
    };

    // Skip empty memtable
    // 跳过空内存表
    if mem.is_empty() {
      drop(mems);
      if let Some(id) = self.mems.borrow().oldest_frozen_id() {
        self.mems.borrow_mut().rm_frozen(id);
      }
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
    let meta = jdb_sst::write(sst_dir, 0, mem, &[]).await?;
    let mem_id = mem.id();
    drop(mems);

    // Load Table for new SSTable
    // 加载新 SSTable 的 Table
    let path = id_path(sst_dir, meta.id);
    let table_info = Table::load(&path, meta.id).await?;

    // Add to SSTable manager
    // 添加到 SSTable 管理器
    self.sst.add(table_info);

    // Record SST in checkpoint
    // 在检查点中记录 SST
    self.ckp.borrow_mut().sst_add(meta.id, 0).await?;

    // Remove frozen memtable
    // 移除冻结的内存表
    self.mems.borrow_mut().rm_frozen(mem_id);

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
    while self.mems.borrow().has_frozen() {
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
  pub async fn compact(&mut self) -> Result<bool> {
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
  pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    // 1. Check memtables / 查内存表
    let pos = self.mems.borrow().get(key);
    if let Some(pos) = pos {
      return self.read_pos(pos).await;
    }

    // 2. Check SSTables / 查 SSTable
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

  /// Range query with bounds
  /// 范围查询
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

  fn range_inner(
    &mut self,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
    skip_rm: bool,
  ) -> MergeAsc<MemIter<'_>, MultiAsc<'_>> {
    // SAFETY: mems borrow is held for lifetime of iterators
    // 安全：mems 借用在迭代器生命周期内保持
    let mems = unsafe { &*self.mems.as_ptr() };
    let mem_iters = mems.range_iters(start, end);
    let sst_stream = self.sst.range(start, end);
    Merge::new(mem_iters, sst_stream, skip_rm)
  }

  /// Reverse range query
  /// 反向范围查询
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

  fn rev_range_inner(
    &mut self,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
    skip_rm: bool,
  ) -> MergeDesc<MemRevIter<'_>, MultiDesc<'_>> {
    let mems = unsafe { &*self.mems.as_ptr() };
    let mem_iters = mems.rev_range_iters(start, end);
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
