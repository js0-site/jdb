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
use jdb_sst::{Conf, Meta};
use jdb_sst_read::Table;
use jdb_sst_write::write_stream;
use oneshot::Receiver;

use crate::{Error, Result, SinkLog, flush_positions, multi::new_asc};

type Lru = Rc<RefCell<FileLru>>;

/// Default LRU capacity for background thread
/// 后台线程默认 LRU 容量
const BG_LRU_CAP: usize = 8;

/// Merge result
/// 合并结果
pub struct MergeResult {
  pub meta: Meta,
  pub log: SinkLog,
}

/// Sinker for SSTable sink (compaction)
/// SSTable 下沉器
pub struct Sinker<'a> {
  dir: &'a Path,
  lru: Lru,
  conf: &'a [Conf],
  /// Background merge receiver (L1+ only)
  /// 后台合并接收器（仅 L1+）
  bg_rx: Option<Receiver<io::Result<MergeResult>>>,
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
  async fn do_merge(&self, tables: &[Table], level: u8, bottom: bool) -> Result<MergeResult> {
    if tables.is_empty() {
      return Ok(MergeResult {
        meta: Meta::default(),
        log: SinkLog::new(),
      });
    }

    let mut log = SinkLog::new();
    let stream = new_asc(
      tables,
      Rc::clone(&self.lru),
      Bound::Unbounded,
      Bound::Unbounded,
      &mut log,
      bottom,
    );

    let meta = write_stream(self.dir, level, stream, self.conf).await?;

    // Flush discard positions
    // 刷盘丢弃位置
    flush_positions(self.dir, &log).await?;

    Ok(MergeResult { meta, log })
  }

  /// Spawn background merge (for L1+)
  /// 启动后台合并（用于 L1+）
  fn spawn_merge(&mut self, ids: Vec<u64>, level: u8, bottom: bool) {
    let dir = self.dir.to_path_buf();
    let conf: Vec<Conf> = self.conf.to_vec();

    let (tx, rx) = oneshot::channel();
    self.bg_rx = Some(rx);

    cpu_bind::spawn(move |rt| {
      let result = rt.block_on(bg_merge(dir, ids, level, conf, bottom));
      let _ = tx.send(result);
    });
  }

  /// Wait for background merge to complete (async)
  /// 异步等待后台合并完成
  async fn wait_bg(&mut self) -> Result<Option<MergeResult>> {
    let Some(rx) = self.bg_rx.take() else {
      return Ok(None);
    };

    match rx.await {
      Ok(Ok(r)) => Ok(Some(r)),
      Ok(Err(e)) => Err(Error::Io(e)),
      Err(_) => Err(Error::Sink),
    }
  }
}

/// Background merge task
/// 后台合并任务
async fn bg_merge(
  dir: PathBuf,
  ids: Vec<u64>,
  level: u8,
  conf: Vec<Conf>,
  bottom: bool,
) -> io::Result<MergeResult> {
  let mut tables = Vec::with_capacity(ids.len());
  for id in ids {
    tables.push(
      Table::load(&id_path(&dir, id), id)
        .await
        .map_err(io::Error::other)?,
    );
  }

  if tables.is_empty() {
    return Ok(MergeResult {
      meta: Meta::default(),
      log: SinkLog::new(),
    });
  }

  let lru = Rc::new(RefCell::new(FileLru::new(&dir, BG_LRU_CAP)));
  let mut log = SinkLog::new();
  let stream = new_asc(
    &tables,
    lru,
    Bound::Unbounded,
    Bound::Unbounded,
    &mut log,
    bottom,
  );

  let meta = write_stream(&dir, level, stream, &conf)
    .await
    .map_err(io::Error::other)?;

  // Flush discard positions
  // 刷盘丢弃位置
  flush_positions(&dir, &log).await?;

  Ok(MergeResult { meta, log })
}

impl Sinker<'_> {
  /// Merge L0 tables (multiple src files, may overlap)
  /// 合并 L0 表（多个源文件，可能重叠）
  pub async fn sink_l0(
    &mut self,
    src_ids: &[u64],
    dst_ids: &[u64],
    dst_level: u8,
    bottom: bool,
  ) -> Result<MergeResult> {
    let mut tables = Vec::with_capacity(src_ids.len() + dst_ids.len());
    for &id in src_ids.iter().chain(dst_ids) {
      let table = Table::load(&id_path(self.dir, id), id).await?;
      tables.push(table);
    }

    self.do_merge(&tables, dst_level, bottom).await
  }

  /// Merge single table from L1+ (one src file)
  /// 合并 L1+ 的单个表（一个源文件）
  pub async fn sink(
    &mut self,
    _src_level: u8,
    src_id: u64,
    dst_ids: &[u64],
    dst_level: u8,
    bottom: bool,
  ) -> Result<MergeResult> {
    self.wait_bg().await?;

    let mut ids = Vec::with_capacity(1 + dst_ids.len());
    ids.push(src_id);
    ids.extend_from_slice(dst_ids);

    self.spawn_merge(ids, dst_level, bottom);

    Ok(self.wait_bg().await?.unwrap_or(MergeResult {
      meta: Meta::default(),
      log: SinkLog::new(),
    }))
  }
}
