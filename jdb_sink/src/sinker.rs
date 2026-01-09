//! Sinker - SSTable sink (compaction) with background thread
//! SSTable 下沉器（后台线程）

use std::{
  cell::RefCell,
  io,
  ops::Bound,
  path::{Path, PathBuf},
  rc::Rc,
};

use jdb_fs::{FileLru, fs_id::id_path};
use jdb_sst::{Conf, Meta, NoDiscard};
use jdb_sst_read::Table;
use jdb_sst_write::write_stream;
use oneshot::Receiver;

use crate::{Error, Result, multi::new_asc};

type Lru = Rc<RefCell<FileLru>>;

/// Default LRU capacity for background thread
/// 后台线程默认 LRU 容量
const BG_LRU_CAP: usize = 8;

/// Sinker for SSTable sink (compaction)
/// SSTable 下沉器
pub struct Sinker<'a> {
  dir: &'a Path,
  lru: Lru,
  conf: &'a [Conf],
  /// Background merge receiver (L1+ only)
  /// 后台合并接收器（仅 L1+）
  bg_rx: Option<Receiver<io::Result<Meta>>>,
}

impl<'a> Sinker<'a> {
  #[inline]
  pub fn new(dir: &'a Path, lru: Lru, conf: &'a [Conf]) -> Self {
    Self {
      dir,
      lru,
      conf,
      bg_rx: None,
    }
  }

  /// Merge tables and write new SSTable
  /// 合并表并写入新 SSTable
  async fn do_merge(&self, tables: &[Table], level: u8) -> Result<Meta> {
    if tables.is_empty() {
      return Ok(Meta::default());
    }

    let stream = new_asc(
      tables,
      Rc::clone(&self.lru),
      Bound::Unbounded,
      Bound::Unbounded,
      NoDiscard,
      false,
    );

    Ok(write_stream(self.dir, level, stream, self.conf).await?)
  }

  /// Spawn background merge (for L1+)
  /// 启动后台合并（用于 L1+）
  fn spawn_merge(&mut self, ids: Vec<u64>, level: u8) {
    let dir = self.dir.to_path_buf();
    let conf: Vec<Conf> = self.conf.to_vec();

    let (tx, rx) = oneshot::channel();
    self.bg_rx = Some(rx);

    cpu_bind::spawn(move |rt| {
      let result = rt.block_on(bg_merge(dir, ids, level, conf));
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
      Err(_) => Err(Error::Sink),
    }
  }
}

/// Background merge task
/// 后台合并任务
async fn bg_merge(dir: PathBuf, ids: Vec<u64>, level: u8, conf: Vec<Conf>) -> io::Result<Meta> {
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
  let stream = new_asc(
    &tables,
    lru,
    Bound::Unbounded,
    Bound::Unbounded,
    NoDiscard,
    false,
  );

  write_stream(&dir, level, stream, &conf)
    .await
    .map_err(io::Error::other)
}

impl Sinker<'_> {
  /// Merge L0 tables (multiple src files, may overlap)
  /// 合并 L0 表（多个源文件，可能重叠）
  pub async fn sink_l0(
    &mut self,
    src_ids: &[u64],
    dst_ids: &[u64],
    dst_level: u8,
  ) -> Result<Vec<Meta>> {
    let mut tables = Vec::with_capacity(src_ids.len() + dst_ids.len());
    for &id in src_ids.iter().chain(dst_ids) {
      let table = Table::load(&id_path(self.dir, id), id).await?;
      tables.push(table);
    }

    let meta = self.do_merge(&tables, dst_level).await?;

    if meta.item_count == 0 {
      return Ok(Vec::new());
    }

    Ok(vec![meta])
  }

  /// Merge single table from L1+ (one src file)
  /// 合并 L1+ 的单个表（一个源文件）
  pub async fn sink(
    &mut self,
    _src_level: u8,
    src_id: u64,
    dst_ids: &[u64],
    dst_level: u8,
  ) -> Result<Vec<Meta>> {
    self.wait_bg().await?;

    let mut ids = Vec::with_capacity(1 + dst_ids.len());
    ids.push(src_id);
    ids.extend_from_slice(dst_ids);

    self.spawn_merge(ids, dst_level);

    let meta = self.wait_bg().await?.unwrap_or_default();

    if meta.item_count == 0 {
      return Ok(Vec::new());
    }

    Ok(vec![meta])
  }
}
