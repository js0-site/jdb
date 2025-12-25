//! Jdb implementation / Jdb 实现

#![allow(clippy::await_holding_refcell_ref)] // compio single-thread runtime / compio 单线程运行时

use std::{
  cell::RefCell,
  collections::BTreeMap,
  path::{Path, PathBuf},
  rc::Rc,
  time::Duration,
};

use jdb_gc::{GcConf, GcHandle, GcStats, GcWorker};
use jdb_page::PageStore;
use jdb_table::{Commit, Conf, Keep, Table};
use jdb_trait::{Order, TableId};
use jdb_tree::BTree;
use jdb_vlog::VLog;
use log::debug;

use crate::Result;

/// Meta file name / 元数据文件名
const META_FILE: &str = "meta.jdb";

/// Table metadata / 表元数据
#[derive(Debug, Clone, Copy)]
struct TableMeta {
  root: u64,
  rev: u64,
  keep: Keep,
}

/// Inner state / 内部状态
struct JdbInner {
  dir: PathBuf,
  vlog: Rc<VLog>,
  tables: BTreeMap<TableId, TableMeta>,
  next_id: TableId,
  gc_worker: GcWorker,
  gc_handle: Option<GcHandle>,
  closed: bool,
}

/// Jdb - multi-table database / 多表数据库
#[derive(Clone)]
pub struct Jdb {
  inner: Rc<RefCell<JdbInner>>,
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
      jdb_fs::mkdir(&dir).await?;
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

    let inner = JdbInner {
      dir,
      vlog,
      tables,
      next_id,
      gc_worker: GcWorker::with_conf(gc_conf),
      gc_handle: None,
      closed: false,
    };

    Ok(Self {
      inner: Rc::new(RefCell::new(inner)),
    })
  }

  fn load_meta(_path: &Path) -> Result<(BTreeMap<TableId, TableMeta>, TableId)> {
    // TODO: implement meta persistence / 实现元数据持久化
    Ok((BTreeMap::new(), 1))
  }

  fn save_meta_inner(inner: &JdbInner) -> Result<()> {
    // TODO: implement meta persistence / 实现元数据持久化
    let _ = inner;
    Ok(())
  }

  /// Get or create table / 获取或创建表
  pub async fn table(&self, id: TableId, conf: &[Conf]) -> Result<Table> {
    let inner = self.inner.borrow();
    if let Some(meta) = inner.tables.get(&id) {
      // Open existing / 打开已有
      let page_path = inner.dir.join(format!("{id}.pages"));
      let vlog = Rc::clone(&inner.vlog);
      let meta = *meta;
      drop(inner);

      let store = PageStore::open(&page_path).await?;
      let tree = BTree::open(store, meta.root);
      let commit = Commit {
        rev: meta.rev,
        root: meta.root,
      };
      let mut tbl = Table::with_keep(tree, vlog, commit, meta.keep);
      tbl.conf(conf);
      Ok(tbl)
    } else {
      drop(inner);
      self.create_table(id, conf).await
    }
  }

  async fn create_table(&self, id: TableId, conf: &[Conf]) -> Result<Table> {
    let inner = self.inner.borrow();
    let page_path = inner.dir.join(format!("{id}.pages"));
    let vlog = Rc::clone(&inner.vlog);
    drop(inner);

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

    let mut inner = self.inner.borrow_mut();
    inner.tables.insert(id, meta);
    if id >= inner.next_id {
      inner.next_id = id + 1;
    }
    Self::save_meta_inner(&inner)?;
    drop(inner);

    let commit = Commit {
      rev: meta.rev,
      root: meta.root,
    };
    Ok(Table::with_keep(tree, vlog, commit, keep))
  }

  /// Fork table / Fork 表
  pub async fn fork(&self, src_id: TableId) -> Result<Option<Table>> {
    let inner = self.inner.borrow();
    let meta = match inner.tables.get(&src_id) {
      Some(m) => *m,
      None => return Ok(None),
    };

    let new_id = inner.next_id;
    let src_path = inner.dir.join(format!("{src_id}.pages"));
    let dst_path = inner.dir.join(format!("{new_id}.pages"));
    let vlog = Rc::clone(&inner.vlog);
    drop(inner);

    // Copy page file / 复制页文件
    std::fs::copy(&src_path, &dst_path)?;

    // Create new meta / 创建新元数据
    let new_meta = TableMeta {
      root: meta.root,
      rev: meta.rev,
      keep: meta.keep,
    };

    let mut inner = self.inner.borrow_mut();
    inner.tables.insert(new_id, new_meta);
    inner.next_id = new_id + 1;
    Self::save_meta_inner(&inner)?;
    drop(inner);

    // Open new table / 打开新表
    let store = PageStore::open(&dst_path).await?;
    let tree = BTree::open(store, new_meta.root);
    let commit = Commit {
      rev: new_meta.rev,
      root: new_meta.root,
    };
    Ok(Some(Table::with_keep(tree, vlog, commit, new_meta.keep)))
  }

  /// Scan tables / 扫描表
  pub fn scan(&self, start: TableId, order: Order) -> Vec<TableId> {
    let inner = self.inner.borrow();
    match order {
      Order::Asc => inner
        .tables
        .keys()
        .filter(|&&id| id >= start)
        .copied()
        .collect(),
      Order::Desc => inner
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
    self.inner.borrow().tables.keys().copied().collect()
  }

  /// Get table count / 获取表数量
  pub fn table_count(&self) -> usize {
    self.inner.borrow().tables.len()
  }

  /// Get table keep policy / 获取表保留策略
  pub fn table_keep(&self, id: TableId) -> Option<Keep> {
    self.inner.borrow().tables.get(&id).map(|m| m.keep)
  }

  /// Get next available id / 获取下一个可用 ID
  pub fn next_id(&self) -> TableId {
    self.inner.borrow().next_id
  }

  /// Sync all / 同步全部
  pub async fn sync(&self) -> Result<()> {
    let inner = self.inner.borrow();
    inner.vlog.sync().await?;
    Ok(())
  }

  /// Commit table changes / 提交表变更
  pub fn commit(&self, id: TableId, tbl: &Table) {
    let mut inner = self.inner.borrow_mut();
    inner.tables.insert(
      id,
      TableMeta {
        root: tbl.root(),
        rev: tbl.rev(),
        keep: tbl.keep(),
      },
    );
    let _ = Self::save_meta_inner(&inner);
  }

  /// Get directory / 获取目录
  pub fn dir(&self) -> PathBuf {
    self.inner.borrow().dir.clone()
  }

  // ======================== GC ========================

  /// Start background GC task / 启动后台 GC 任务
  pub fn start_gc(&self) {
    let handle = GcHandle::new();
    let stop_flag = handle.stop_flag();

    {
      let mut inner = self.inner.borrow_mut();
      inner.gc_handle = Some(handle);
    }

    let jdb = self.clone();
    let interval = jdb.inner.borrow().gc_worker.conf().interval_secs;

    compio::runtime::spawn(async move {
      loop {
        // Check closed / 检查关闭
        if jdb.inner.borrow().closed {
          break;
        }

        // Wait interval / 等待间隔
        compio::time::sleep(Duration::from_secs(interval)).await;

        // Check stop flag / 检查停止标志
        if *stop_flag.borrow() {
          break;
        }

        // Execute GC step / 执行 GC 步骤
        let _ = jdb.gc_step().await;
      }
      debug!("GC task stopped / GC 任务已停止");
    })
    .detach();
  }

  /// Stop background GC task / 停止后台 GC 任务
  pub fn stop_gc(&self) {
    let mut inner = self.inner.borrow_mut();
    if let Some(handle) = inner.gc_handle.take() {
      handle.stop();
    }
  }

  /// Execute one GC step / 执行一步 GC
  pub async fn gc_step(&self) -> Result<bool> {
    let mut inner = self.inner.borrow_mut();

    if inner.gc_worker.is_idle() {
      inner.gc_worker.start();
    }

    if inner.gc_worker.is_done() {
      return Ok(true);
    }

    // Get current state / 获取当前状态
    let state = inner.gc_worker.state().clone();
    drop(inner);

    match state {
      jdb_gc::GcState::Marking { .. } => {
        self.gc_mark_step().await?;
      }
      jdb_gc::GcState::Sweeping { .. } => {
        self.gc_sweep_step().await?;
      }
      _ => {}
    }

    Ok(self.inner.borrow().gc_worker.is_done())
  }

  /// Execute one marking step / 执行一步标记
  async fn gc_mark_step(&self) -> Result<()> {
    let inner = self.inner.borrow();
    let state = inner.gc_worker.state().clone();
    let mark_batch = inner.gc_worker.conf().mark_batch;
    let table_ids: Vec<TableId> = inner.tables.keys().copied().collect();
    drop(inner);

    let (table_idx, key_cursor) = match state {
      jdb_gc::GcState::Marking {
        table_idx,
        key_cursor,
      } => (table_idx, key_cursor),
      _ => return Ok(()),
    };

    // Check if all tables scanned / 检查是否所有表已扫描
    if table_idx >= table_ids.len() {
      let mut inner = self.inner.borrow_mut();
      inner.gc_worker.start_sweep();
      return Ok(());
    }

    // Get current table / 获取当前表
    let table_id = table_ids[table_idx];
    let tbl = self.table(table_id, &[]).await?;
    let keep = self.table_keep(table_id).unwrap_or_default();

    // Scan keys from cursor / 从游标扫描键
    let (keys, vrefs, next_cursor) = tbl
      .scan_keys_for_gc(key_cursor.as_ref(), mark_batch)
      .await?;

    let keys_count = keys.len() as u64;

    // Mark each ValRef / 标记每个 ValRef
    let mut inner = self.inner.borrow_mut();
    let now_ms = coarsetime::Clock::now_since_epoch().as_millis();

    for (i, vref) in vrefs.iter().enumerate() {
      // Mark current value / 标记当前值
      inner.gc_worker.live_tracker_mut().mark(vref);

      // Mark bin if external / 如果是外部值则标记 bin
      if vref.is_external() {
        inner.gc_worker.live_tracker_mut().mark_bin(vref.file_id);
      }

      // Mark history based on Keep policy / 根据 Keep 策略标记历史
      if keep != Keep::Current && vref.has_prev() {
        // Get history chain / 获取历史链
        drop(inner);
        let history = tbl.history(&keys[i]).await?;
        inner = self.inner.borrow_mut();

        // Get timestamps (simplified - use current time for all)
        // 获取时间戳（简化 - 对所有使用当前时间）
        let timestamps: Vec<u64> = vec![now_ms / 1000; history.len()];
        inner
          .gc_worker
          .live_tracker_mut()
          .mark_history(&history, keep, now_ms, &timestamps);
      }
    }

    // Update stats / 更新统计
    inner.gc_worker.inc_keys(keys_count);

    // Update state / 更新状态
    if next_cursor.is_some() {
      // More keys in current table / 当前表还有更多键
      inner.gc_worker.update_marking(table_idx, next_cursor);
    } else {
      // Move to next table / 移动到下一个表
      inner.gc_worker.inc_tables();
      inner.gc_worker.update_marking(table_idx + 1, None);
    }

    Ok(())
  }

  /// Execute one sweeping step / 执行一步清扫
  async fn gc_sweep_step(&self) -> Result<()> {
    let inner = self.inner.borrow();
    let state = inner.gc_worker.state().clone();
    let compact_threshold = inner.gc_worker.conf().compact_threshold;
    let vlog = Rc::clone(&inner.vlog);
    drop(inner);

    let file_idx = match state {
      jdb_gc::GcState::Sweeping { file_idx } => file_idx,
      _ => return Ok(()),
    };

    // Get vlog file list / 获取 vlog 文件列表
    let file_ids = vlog.file_ids().await?;
    let active_id = vlog.active_id();

    // Check if all files processed / 检查是否所有文件已处理
    if file_idx >= file_ids.len() {
      let mut inner = self.inner.borrow_mut();
      inner.gc_worker.finish();
      return Ok(());
    }

    let file_id = file_ids[file_idx];

    // Skip active file / 跳过活跃文件
    if file_id == active_id {
      let mut inner = self.inner.borrow_mut();
      inner.gc_worker.update_sweeping(file_idx + 1);
      return Ok(());
    }

    // Calculate FileStat / 计算 FileStat
    let inner = self.inner.borrow();
    let live_count = inner.gc_worker.live_tracker().live_count(file_id);
    drop(inner);

    let file_size = vlog.file_size(file_id).await.unwrap_or(0);

    // Estimate total records (assume average record size = PAGE_SIZE)
    // 估算总记录数（假设平均记录大小 = PAGE_SIZE）
    let total = file_size / 4096;

    let stat = jdb_gc::FileStat {
      total,
      live: live_count,
      size: file_size,
    };

    // Decide action based on live_count / 根据 live_count 决定操作
    let mut inner = self.inner.borrow_mut();

    if stat.live == 0 {
      // Delete file / 删除文件
      drop(inner);
      if let Err(e) = vlog.delete_file(file_id).await {
        // Log error and continue / 记录错误并继续
        debug!("Failed to delete vlog file {file_id}: {e}");
      } else {
        let mut inner = self.inner.borrow_mut();
        inner.gc_worker.inc_files_deleted(file_size);
      }
      inner = self.inner.borrow_mut();
    } else if stat.garbage_ratio() >= compact_threshold {
      // Mark for compaction (not implemented yet) / 标记待压缩（暂未实现）
      inner.gc_worker.inc_files_compacted();
    }

    // Update state / 更新状态
    inner.gc_worker.update_sweeping(file_idx + 1);

    Ok(())
  }

  /// Run full GC / 运行完整 GC
  pub async fn gc(&self) -> Result<GcStats> {
    {
      let mut inner = self.inner.borrow_mut();
      inner.gc_worker.start();
    }

    loop {
      if self.gc_step().await? {
        break;
      }
    }

    Ok(self.gc_stats())
  }

  /// Get GC statistics / 获取 GC 统计
  pub fn gc_stats(&self) -> GcStats {
    *self.inner.borrow().gc_worker.stats()
  }

  /// Check if GC is running / 检查 GC 是否运行中
  pub fn gc_running(&self) -> bool {
    let inner = self.inner.borrow();
    !inner.gc_worker.is_idle() && !inner.gc_worker.is_done()
  }

  // ======================== Close ========================

  /// Close database / 关闭数据库
  pub async fn close(&self) -> Result<()> {
    let inner = self.inner.borrow();
    if inner.closed {
      return Ok(());
    }
    drop(inner);

    // Stop GC task / 停止 GC 任务
    self.stop_gc();

    // Sync vlog / 同步值日志
    {
      let inner = self.inner.borrow();
      inner.vlog.sync().await?;
    }

    // Save meta and mark closed / 保存元数据并标记关闭
    {
      let mut inner = self.inner.borrow_mut();
      Self::save_meta_inner(&inner)?;
      inner.closed = true;
    }

    Ok(())
  }

  /// Check if closed / 检查是否已关闭
  pub fn is_closed(&self) -> bool {
    self.inner.borrow().closed
  }
}

impl Drop for Jdb {
  fn drop(&mut self) {
    // Only cleanup if this is the last reference / 只有最后一个引用时才清理
    if Rc::strong_count(&self.inner) == 1 {
      let mut inner = self.inner.borrow_mut();
      if inner.closed {
        return;
      }

      // Stop GC task / 停止 GC 任务
      if let Some(handle) = inner.gc_handle.take() {
        handle.stop();
      }

      // Save meta (sync, can't await in drop) / 保存元数据
      let _ = Self::save_meta_inner(&inner);
      inner.closed = true;
    }
  }
}
