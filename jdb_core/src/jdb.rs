//! Jdb implementation / Jdb 实现

use std::{
  cell::RefCell,
  collections::BTreeMap,
  path::{Path, PathBuf},
  rc::Rc,
};

use jdb_gc::{GcConf, GcHandle, GcStats, GcWorker};
use jdb_page::PageStore;
use jdb_table::{Commit, Conf, Keep, Table};
use jdb_trait::{Order, TableId};
use jdb_tree::BTree;
use jdb_vlog::VLog;

use crate::Result;

/// Meta file name / 元数据文件名
const META_FILE: &str = "meta.jdb";

/// Jdb - top level database / 顶层数据库
pub struct Jdb {
  dir: PathBuf,
  vlog: Rc<VLog>,
  tables: BTreeMap<TableId, TableMeta>,
  next_id: TableId,
  /// GC worker / GC 工作器
  gc_worker: RefCell<GcWorker>,
  /// GC handle for background task / 后台任务句柄
  gc_handle: Option<GcHandle>,
  /// Closed flag / 已关闭标志
  closed: RefCell<bool>,
}

/// Table metadata / 表元数据
#[derive(Debug, Clone, Copy)]
struct TableMeta {
  root: u64,
  rev: u64,
  keep: Keep,
}

impl Jdb {
  /// Open or create Jdb / 打开或创建 Jdb
  pub async fn open(dir: impl AsRef<Path>) -> Result<Self> {
    Self::open_with_gc(dir, GcConf::default()).await
  }

  /// Open with GC config / 带 GC 配置打开
  pub async fn open_with_gc(dir: impl AsRef<Path>, gc_conf: GcConf) -> Result<Self> {
    let dir = dir.as_ref().to_path_buf();

    // Create dir / 创建目录
    if !dir.exists() {
      std::fs::create_dir_all(&dir)?;
    }

    // Open shared vlog / 打开共享值日志
    let vlog_dir = dir.join("vlog");
    let vlog = Rc::new(VLog::open(&vlog_dir).await?);

    // Load meta / 加载元数据
    let meta_path = dir.join(META_FILE);
    let (tables, next_id) = if meta_path.exists() {
      Self::load_meta(&meta_path)?
    } else {
      (BTreeMap::new(), 1)
    };

    let gc_worker = RefCell::new(GcWorker::with_conf(gc_conf));

    Ok(Self {
      dir,
      vlog,
      tables,
      next_id,
      gc_worker,
      gc_handle: None,
      closed: RefCell::new(false),
    })
  }

  fn load_meta(_path: &Path) -> Result<(BTreeMap<TableId, TableMeta>, TableId)> {
    // TODO: implement meta persistence / 实现元数据持久化
    Ok((BTreeMap::new(), 1))
  }

  fn save_meta(&self) -> Result<()> {
    // TODO: implement meta persistence / 实现元数据持久化
    Ok(())
  }

  /// Get or create table / 获取或创建表
  pub async fn table(&mut self, id: TableId, conf: &[Conf]) -> Result<Table> {
    if let Some(meta) = self.tables.get(&id).copied() {
      // Open existing / 打开已有
      let page_path = self.table_page_path(id);
      let store = PageStore::open(&page_path).await?;
      let tree = BTree::open(store, meta.root);
      let commit = Commit {
        rev: meta.rev,
        root: meta.root,
      };
      let mut tbl = Table::with_keep(tree, Rc::clone(&self.vlog), commit, meta.keep);
      tbl.conf(conf);
      Ok(tbl)
    } else {
      self.create_table(id, conf).await
    }
  }

  async fn create_table(&mut self, id: TableId, conf: &[Conf]) -> Result<Table> {
    let page_path = self.table_page_path(id);
    let store = PageStore::open(&page_path).await?;
    let tree = BTree::new(store).await?;

    // Parse keep from conf / 从配置解析保留策略
    let mut keep = Keep::default();
    for c in conf {
      let Conf::Keep(k) = c;
      keep = *k;
    }

    let meta = TableMeta {
      root: tree.root(),
      rev: 0,
      keep,
    };
    self.tables.insert(id, meta);

    if id >= self.next_id {
      self.next_id = id + 1;
    }

    self.save_meta()?;

    let commit = Commit {
      rev: meta.rev,
      root: meta.root,
    };
    Ok(Table::with_keep(tree, Rc::clone(&self.vlog), commit, keep))
  }

  fn table_page_path(&self, id: TableId) -> PathBuf {
    self.dir.join(format!("tbl_{id:08}.jdb"))
  }

  /// Fork table / Fork 表
  pub async fn fork(&mut self, id: TableId) -> Result<Option<Table>> {
    let meta = match self.tables.get(&id).copied() {
      Some(m) => m,
      None => return Ok(None),
    };

    // Allocate new id / 分配新 ID
    let new_id = self.next_id;
    self.next_id += 1;

    // Copy page file / 复制页文件
    let src_path = self.table_page_path(id);
    let dst_path = self.table_page_path(new_id);
    std::fs::copy(&src_path, &dst_path)?;

    // Create new meta / 创建新元数据
    let new_meta = TableMeta {
      root: meta.root,
      rev: meta.rev,
      keep: meta.keep,
    };
    self.tables.insert(new_id, new_meta);
    self.save_meta()?;

    // Open new table / 打开新表
    let store = PageStore::open(&dst_path).await?;
    let tree = BTree::open(store, new_meta.root);
    let commit = Commit {
      rev: new_meta.rev,
      root: new_meta.root,
    };
    Ok(Some(Table::with_keep(
      tree,
      Rc::clone(&self.vlog),
      commit,
      new_meta.keep,
    )))
  }

  /// Scan tables / 扫描表
  pub fn scan(&self, start: TableId, order: Order) -> Vec<TableId> {
    match order {
      Order::Asc => self
        .tables
        .keys()
        .filter(|&&id| id >= start)
        .copied()
        .collect(),
      Order::Desc => self
        .tables
        .keys()
        .rev()
        .filter(|&&id| id <= start)
        .copied()
        .collect(),
    }
  }

  /// Get all table ids / 获取所有表 ID
  pub fn table_ids(&self) -> Vec<TableId> {
    self.tables.keys().copied().collect()
  }

  /// Get table count / 获取表数量
  pub fn table_count(&self) -> usize {
    self.tables.len()
  }

  /// Get table keep policy / 获取表保留策略
  pub fn table_keep(&self, id: TableId) -> Option<Keep> {
    self.tables.get(&id).map(|m| m.keep)
  }

  /// Get next available id / 获取下一个可用 ID
  pub fn next_id(&self) -> TableId {
    self.next_id
  }

  /// Sync all / 同步全部
  pub async fn sync(&self) -> Result<()> {
    self.vlog.sync().await?;
    Ok(())
  }

  /// Commit table changes / 提交表变更
  pub fn commit_table(&mut self, id: TableId, tbl: &Table) {
    self.tables.insert(
      id,
      TableMeta {
        root: tbl.root(),
        rev: tbl.rev(),
        keep: tbl.keep(),
      },
    );
    let _ = self.save_meta();
  }

  // ==================== GC API ====================

  /// Start background GC task / 启动后台 GC 任务
  pub fn start_gc(&mut self) -> GcHandle {
    let handle = GcHandle::new();
    self.gc_handle = Some(GcHandle::new());
    // TODO: spawn background task / 启动后台任务
    // 需要在 Jdb 外部调用 spawn，因为 Jdb 不持有 runtime
    handle
  }

  /// Stop background GC / 停止后台 GC
  pub fn stop_gc(&mut self) {
    if let Some(handle) = &self.gc_handle {
      handle.stop();
    }
    self.gc_handle = None;
  }

  /// Execute one GC step / 执行一步 GC
  pub async fn gc_step(&self) -> Result<bool> {
    let mut worker = self.gc_worker.borrow_mut();

    if worker.is_idle() {
      worker.start();
    }

    // TODO: implement actual GC logic / 实现实际 GC 逻辑
    // 1. Marking: iterate tables, collect live refs
    // 2. Sweeping: delete/compact vlog files

    Ok(worker.is_done())
  }

  /// Execute full GC / 执行完整 GC
  pub async fn gc(&self) -> Result<GcStats> {
    {
      let mut worker = self.gc_worker.borrow_mut();
      worker.start();
    }

    loop {
      if self.gc_step().await? {
        break;
      }
      // GC step already has IO await points, no need for explicit yield
      // GC 步骤已有 IO await 点，无需显式 yield
    }

    Ok(*self.gc_worker.borrow().stats())
  }

  /// Get GC statistics / 获取 GC 统计
  pub fn gc_stats(&self) -> GcStats {
    *self.gc_worker.borrow().stats()
  }

  /// Check if GC is running / 检查 GC 是否运行中
  pub fn gc_running(&self) -> bool {
    !self.gc_worker.borrow().is_idle() && !self.gc_worker.borrow().is_done()
  }

  /// Get VLog reference / 获取 VLog 引用
  pub fn vlog(&self) -> &Rc<VLog> {
    &self.vlog
  }

  /// Get directory / 获取目录
  pub fn dir(&self) -> &Path {
    &self.dir
  }

  /// Close database / 关闭数据库
  pub async fn close(&mut self) -> Result<()> {
    if *self.closed.borrow() {
      return Ok(());
    }

    // Stop GC task / 停止 GC 任务
    self.stop_gc();

    // Sync vlog / 同步值日志
    self.vlog.sync().await?;

    // Save meta / 保存元数据
    self.save_meta()?;

    *self.closed.borrow_mut() = true;
    Ok(())
  }

  /// Check if closed / 检查是否已关闭
  pub fn is_closed(&self) -> bool {
    *self.closed.borrow()
  }
}

impl Drop for Jdb {
  fn drop(&mut self) {
    if *self.closed.borrow() {
      return;
    }

    // Stop GC task / 停止 GC 任务
    self.stop_gc();

    // Save meta (sync, can't await in drop) / 保存元数据（同步，drop 中不能 await）
    let _ = self.save_meta();
  }
}
