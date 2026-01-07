//! Compactor - SSTable compaction with background thread
//! SSTable 压缩（后台线程）

use std::{
  cell::RefCell,
  io,
  ops::Bound,
  path::{Path, PathBuf},
  rc::Rc,
};

use compio::fs;
use jdb_base::Compact;
use jdb_fs::{FileLru, fs_id::id_path};
use oneshot::Receiver;

use crate::{Conf, Error, Meta, MultiAsc, Result, Table, gen_id, write_stream};

type Lru = Rc<RefCell<FileLru>>;

/// Default LRU capacity for background thread
/// 后台线程默认 LRU 容量
const BG_LRU_CAP: usize = 8;

/// Compactor for SSTable compaction
/// SSTable 压缩器
pub struct Compactor<'a> {
  pub dir: &'a Path,
  pub lru: Lru,
  pub conf: &'a [Conf],
  /// Background merge receiver (L1+ only)
  /// 后台合并接收器（仅 L1+）
  bg_rx: Option<Receiver<io::Result<Meta>>>,
}

impl<'a> Compactor<'a> {
  #[inline]
  pub fn new(dir: &'a Path, lru: Lru, conf: &'a [Conf]) -> Self {
    Self {
      dir,
      lru,
      conf,
      bg_rx: None,
    }
  }

  /// Load table by id
  /// 按 id 加载表
  #[inline]
  async fn load(&self, id: u64) -> Result<Table> {
    Table::load(&id_path(self.dir, id), id).await
  }

  /// Merge tables and write new SSTable
  /// 合并表并写入新 SSTable
  async fn do_merge(&self, tables: &[Table], level: u8, id: u64) -> Result<Meta> {
    if tables.is_empty() {
      return Ok(Meta::default());
    }

    let stream = MultiAsc::new(
      tables,
      Rc::clone(&self.lru),
      Bound::Unbounded,
      Bound::Unbounded,
    );

    write_stream(self.dir, level, stream, self.conf, id).await
  }

  /// Spawn background merge (for L1+)
  /// 启动后台合并（用于 L1+）
  fn spawn_merge(&mut self, ids: Vec<u64>, level: u8, new_id: u64) {
    let dir = self.dir.to_path_buf();
    let conf: Vec<Conf> = self.conf.to_vec();

    let (tx, rx) = oneshot::channel();
    self.bg_rx = Some(rx);

    cpu_bind::spawn(move |rt| {
      let result = rt.block_on(bg_merge(dir, ids, level, new_id, conf));
      let _ = tx.send(result);
    });
  }

  /// Wait for background merge to complete (async)
  /// 异步等待后台合并完成
  async fn wait_bg(&mut self) -> Result<Option<Meta>> {
    let Some(rx) = self.bg_rx.take() else {
      return Ok(None);
    };

    match rx.await {
      Ok(Ok(meta)) => Ok(Some(meta)),
      Ok(Err(e)) => Err(Error::Io(e)),
      Err(_) => Err(Error::Compact),
    }
  }
}

/// Background merge task
/// 后台合并任务
async fn bg_merge(
  dir: PathBuf,
  ids: Vec<u64>,
  level: u8,
  new_id: u64,
  conf: Vec<Conf>,
) -> io::Result<Meta> {
  // Load tables
  // 加载表
  let mut tables = Vec::with_capacity(ids.len());
  for id in ids {
    tables.push(
      Table::load(&id_path(&dir, id), id)
        .await
        .map_err(io::Error::other)?,
    );
  }

  if tables.is_empty() {
    return Ok(Meta::default());
  }

  let lru = Rc::new(RefCell::new(FileLru::new(&dir, BG_LRU_CAP)));
  let stream = MultiAsc::new(&tables, lru, Bound::Unbounded, Bound::Unbounded);

  write_stream(&dir, level, stream, &conf, new_id)
    .await
    .map_err(io::Error::other)
}

impl Compact<Table> for Compactor<'_> {
  type Error = Error;

  async fn merge_l0(
    &mut self,
    src_ids: &[u64],
    dst_ids: &[u64],
    dst_level: u8,
  ) -> Result<Vec<Table>> {
    // L0: merge in current thread
    // L0：在当前线程合并
    let new_id = gen_id(self.dir);

    let mut tables = Vec::with_capacity(src_ids.len() + dst_ids.len());
    for &id in src_ids.iter().chain(dst_ids) {
      tables.push(self.load(id).await?);
    }

    let meta = self.do_merge(&tables, dst_level, new_id).await?;

    if meta.item_count == 0 {
      return Ok(Vec::new());
    }

    Ok(vec![self.load(meta.id).await?])
  }

  async fn merge(
    &mut self,
    _src_level: u8,
    src_id: u64,
    dst_ids: &[u64],
    dst_level: u8,
  ) -> Result<Vec<Table>> {
    // L1+: merge in background thread
    // L1+：在后台线程合并

    // Wait for any previous background merge
    // 等待之前的后台合并
    self.wait_bg().await?;

    let new_id = gen_id(self.dir);

    let mut ids = Vec::with_capacity(1 + dst_ids.len());
    ids.push(src_id);
    ids.extend_from_slice(dst_ids);

    self.spawn_merge(ids, dst_level, new_id);

    let meta = self.wait_bg().await?.unwrap_or_default();

    if meta.item_count == 0 {
      return Ok(Vec::new());
    }

    Ok(vec![self.load(meta.id).await?])
  }

  async fn rm(&mut self, id: u64) -> Result<()> {
    self.lru.borrow_mut().rm(id);
    fs::remove_file(id_path(self.dir, id)).await?;
    Ok(())
  }
}
