//! Database core structure
//! 数据库核心结构

use std::path::PathBuf;

use jdb_base::{Pos, table::TableMut};
use jdb_ckp::Ckp;
use jdb_fs::FileLru;
use jdb_mem::Mem;
use jdb_sstable::TableInfo;
use jdb_val::Wal;
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
  pub ckp: Ckp,
  /// SSTable list (newest first) / SSTable 列表（最新在前）
  pub sstables: Vec<TableInfo>,
  /// File LRU cache / 文件 LRU 缓存
  pub file_lru: FileLru,
  /// Memory table size threshold / 内存表大小阈值
  pub mem_threshold: u64,
}

impl Db {
  /// Open database at path
  /// 打开数据库
  pub async fn open(path: impl Into<PathBuf>, conf: &[Conf]) -> Result<Self> {
    let dir = path.into();

    // Extract sub-configs in single pass
    // 单次遍历提取子配置
    let (ckp_conf, wal_conf): (Vec<_>, Vec<_>) =
      conf
        .iter()
        .fold((Vec::new(), Vec::new()), |(mut ckp, mut wal), c| {
          match c {
            Conf::Ckp(c) => ckp.push(*c),
            Conf::Wal(c) => wal.push(*c),
            _ => {}
          }
          (ckp, wal)
        });

    // 1. Open checkpoint / 打开检查点
    let (ckp, after) = jdb_ckp::open(&dir, &ckp_conf).await?;

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

    // 4. Load SSTable list / 加载 SSTable 列表
    let sst_dir = dir.join(SST_DIR);
    let sstables = jdb_sstable::load(&sst_dir).await?;

    Ok(Self::from_parts(dir, mem, wal, ckp, sstables, conf))
  }

  /// Create Db from components
  /// 从组件创建 Db
  pub(crate) fn from_parts(
    dir: PathBuf,
    mem: Mem,
    wal: Wal,
    ckp: Ckp,
    sstables: Vec<TableInfo>,
    conf: &[Conf],
  ) -> Self {
    let mut mem_threshold = DEFAULT_MEM_THRESHOLD;
    let mut file_cap = DEFAULT_FILE_CAP;

    for c in conf {
      match c {
        Conf::MemThreshold(n) => mem_threshold = *n,
        Conf::FileCap(n) => file_cap = *n,
        _ => {}
      }
    }

    Self {
      file_lru: FileLru::new(dir.join(SST_DIR), file_cap),
      dir,
      mem,
      frozen: Vec::new(),
      wal,
      ckp,
      sstables,
      mem_threshold,
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

  /// Get value by key
  /// 根据键获取值
  ///
  /// Search order: active mem → frozen mems → SSTables
  /// 查找顺序：活跃内存表 → 冻结内存表 → SSTable
  pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    use jdb_base::table::Table;

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

    // 3. Check SSTables (newest first) / 查 SSTable（从新到旧）
    for i in 0..self.sstables.len() {
      if let Some(pos) = self.sstables[i].get_pos(key, &mut self.file_lru).await? {
        return self.read_pos(pos).await;
      }
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
}
