//! Multi-level manager with dynamic level bytes
//! 带动态层级字节的多层管理器

use std::{cell::RefCell, mem, ops::Bound, rc::Rc};

use jdb_base::{Compact, table::Meta};
use jdb_ckp::{Ckp, Op};
use jdb_rm::{Kind, Ver};
use log::error;

use crate::{
  Conf, Level, Limits, MAX_LEVEL, ParsedConf,
  calc::{SCORE_SCALE, SCORE_URGENT, calc, needs_compact, score, target_level},
};

/// Reusable buffers for compaction
/// 压缩复用缓冲区
#[derive(Default)]
struct Buf {
  src_idx: Vec<usize>,
  dst_idx: Vec<usize>,
  src_ids: Vec<u64>,
  dst_ids: Vec<u64>,
  ops: Vec<Op>,
}

/// Multi-level manager
/// 多层管理器
pub struct Levels<T> {
  pub levels: Vec<Level<T>>,
  conf: ParsedConf,
  limits: Limits,
  dirty: bool,
  ckp: Rc<RefCell<Ckp>>,
  buf: Buf,
  ver: Ver,
}

impl<T: Meta> Levels<T> {
  pub fn new(conf: &[Conf], ckp: Rc<RefCell<Ckp>>, ver: Ver) -> Self {
    let c = ParsedConf::new(conf);
    let levels = (0..=MAX_LEVEL).map(Level::new).collect();
    let limits = calc(0, c.base_size, c.ratio);
    Self {
      levels,
      conf: c,
      limits,
      dirty: false,
      ckp,
      buf: Buf::default(),
      ver,
    }
  }

  #[inline]
  pub fn ver(&self) -> u64 {
    self.ver.get()
  }

  #[inline]
  pub fn rm(&self, kind: Kind) {
    self.ver.rm(kind);
  }

  #[inline]
  pub fn rm_batch(&self, kinds: impl IntoIterator<Item = Kind>) {
    self.ver.rm_batch(kinds);
  }

  fn recalc(&mut self) {
    if !self.dirty {
      return;
    }
    self.dirty = false;
    let total: u64 = self.levels[1..].iter().map(|l| l.size()).sum();
    self.limits = calc(total, self.conf.base_size, self.conf.ratio);
  }

  #[inline]
  pub fn max_level(&self) -> u8 {
    MAX_LEVEL
  }

  #[inline]
  pub fn l0_limit(&self) -> usize {
    self.conf.l0_limit
  }

  #[inline]
  pub fn base_size(&self) -> u64 {
    self.conf.base_size
  }

  #[inline]
  pub fn ratio(&self) -> u64 {
    self.conf.ratio
  }

  #[inline]
  pub fn mark_dirty(&mut self) {
    self.dirty = true;
  }

  #[inline]
  pub fn size_limit(&mut self, level: u8) -> u64 {
    self.recalc();
    self.limits.limits[level as usize]
  }

  #[inline]
  pub fn base_level(&mut self) -> u8 {
    self.recalc();
    self.limits.base_level
  }

  #[inline]
  pub fn needs_compaction(&mut self, level: u8) -> bool {
    self.recalc();
    let i = level as usize;
    if i >= self.levels.len() {
      return false;
    }
    let l = &self.levels[i];
    needs_compact(
      level,
      l.len(),
      l.size(),
      self.conf.l0_limit,
      self.limits.base_level,
      self.limits.limits[i],
    )
  }

  #[allow(clippy::await_holding_refcell_ref)]
  pub async fn compact<C>(&mut self, compactor: &mut C) -> Result<bool, C::Error>
  where
    C: Compact<T>,
    C::Error: std::fmt::Debug,
  {
    self.recalc();

    for level in 1..MAX_LEVEL {
      let dst = target_level(level, self.limits.base_level);
      self.buf.src_idx.clear();
      self.find_trivial_candidates(level, dst);
      if !self.buf.src_idx.is_empty() && self.trivial_move(level, dst).await {
        self.recalc();
      }
    }

    let mut best_level = None;
    let mut best_score: u32 = SCORE_SCALE;

    for level in 0..MAX_LEVEL {
      let i = level as usize;
      let l = &self.levels[i];
      let s = if level == 0 {
        score(level, l.len(), l.size(), self.conf.l0_limit, 0)
      } else if level < self.limits.base_level {
        if !l.is_empty() { SCORE_URGENT } else { 0 }
      } else {
        score(level, l.len(), l.size(), self.conf.l0_limit, self.limits.limits[i])
      };
      if s > best_score {
        best_score = s;
        best_level = Some(level);
      }
    }

    let Some(src) = best_level else {
      return Ok(false);
    };

    let dst = target_level(src, self.limits.base_level);
    self.buf.src_idx.clear();
    self.pick_files(src);

    if self.buf.src_idx.is_empty() {
      return Ok(false);
    }

    let l = &self.levels[src as usize];
    let first = l.get(self.buf.src_idx[0]).expect("index valid");
    let mut min_key = first.min_key();
    let mut max_key = first.max_key();
    for &i in &self.buf.src_idx[1..] {
      if let Some(t) = l.get(i) {
        if t.min_key() < min_key {
          min_key = t.min_key();
        }
        if t.max_key() > max_key {
          max_key = t.max_key();
        }
      }
    }

    self.buf.dst_idx.clear();
    self.buf.dst_idx.extend(
      self.levels[dst as usize].overlapping(Bound::Included(min_key), Bound::Included(max_key)),
    );

    self.buf.src_ids.clear();
    self.buf.src_ids.extend(
      self.buf.src_idx.iter().filter_map(|&i| self.levels[src as usize].get(i).map(|t| t.id())),
    );
    self.buf.dst_ids.clear();
    self.buf.dst_ids.extend(
      self.buf.dst_idx.iter().filter_map(|&i| self.levels[dst as usize].get(i).map(|t| t.id())),
    );

    let new_tables = if src == 0 {
      compactor.merge_l0(&self.buf.src_ids, &self.buf.dst_ids, dst).await?
    } else {
      compactor.merge(src, self.buf.src_ids[0], &self.buf.dst_ids, dst).await?
    };

    self.buf.ops.clear();
    self.buf.ops.extend(
      self.buf.src_ids.iter().chain(self.buf.dst_ids.iter()).map(|&id| Op::SstRm(id)),
    );
    self.buf.ops.extend(new_tables.iter().map(|t| Op::SstAdd(t.id(), dst)));

    if let Err(e) = self.ckp.borrow_mut().batch(mem::take(&mut self.buf.ops)).await {
      error!("ckp batch failed: {e:?}");
      return Ok(false);
    }

    self.levels[src as usize].drain(&self.buf.src_idx);
    self.levels[dst as usize].drain(&self.buf.dst_idx);
    for t in new_tables {
      self.levels[dst as usize].add(t);
    }

    self.ver.rm_batch(
      self.buf.src_ids.iter().chain(self.buf.dst_ids.iter()).map(|&id| Kind::Sst(id)),
    );

    self.dirty = true;
    Ok(true)
  }

  fn pick_files(&mut self, level: u8) {
    let l = &mut self.levels[level as usize];
    if level == 0 {
      if let Some(seed) = l.pick_file() {
        self.buf.src_idx.extend(l.pick_l0_files(seed));
      }
    } else if let Some(i) = l.pick_file() {
      self.buf.src_idx.push(i);
    }
  }

  fn find_trivial_candidates(&mut self, src: u8, dst: u8) {
    if src == 0 || dst <= src || dst as usize >= self.levels.len() {
      return;
    }
    let src_level = &self.levels[src as usize];
    let dst_level = &self.levels[dst as usize];
    let gp_level = if (dst as usize + 1) < self.levels.len() {
      Some(&self.levels[dst as usize + 1])
    } else {
      None
    };
    let gp_limit = self.conf.gp_limit;

    for (i, t) in src_level.iter().enumerate() {
      if !dst_level.no_overlap(t.min_key(), t.max_key()) {
        continue;
      }
      if let Some(gp) = gp_level
        && gp.overlapping_exceeds(t.min_key(), t.max_key(), gp_limit)
      {
        continue;
      }
      self.buf.src_idx.push(i);
    }
  }

  #[inline]
  pub fn target_level(&mut self, src: u8) -> u8 {
    self.recalc();
    target_level(src, self.limits.base_level)
  }

  #[inline]
  pub fn table_count(&self) -> usize {
    self.levels.iter().map(|l| l.len()).sum()
  }

  #[inline]
  pub fn total_size(&self) -> u64 {
    self.levels.iter().map(|l| l.size()).sum()
  }

  #[inline]
  fn can_trivial_move(&self, src: u8, dst: u8, indices: &[usize]) -> bool {
    if src == 0 || dst <= src || dst as usize >= self.levels.len() {
      return false;
    }
    let src_level = &self.levels[src as usize];
    let dst_level = &self.levels[dst as usize];
    dst_level.no_overlap_all(indices.iter().filter_map(|&i| src_level.get(i)))
  }

  #[allow(clippy::await_holding_refcell_ref)]
  async fn trivial_move(&mut self, src: u8, dst: u8) -> bool {
    let indices = &self.buf.src_idx;
    if indices.is_empty() || !self.can_trivial_move(src, dst, indices) {
      return false;
    }

    self.buf.src_ids.clear();
    self.buf.src_ids.extend(
      indices.iter().filter_map(|&i| self.levels[src as usize].get(i).map(|t| t.id())),
    );

    self.buf.ops.clear();
    self.buf.ops.extend(self.buf.src_ids.iter().map(|&id| Op::SstAdd(id, dst)));

    if let Err(e) = self.ckp.borrow_mut().batch(mem::take(&mut self.buf.ops)).await {
      error!("trivial move ckp failed: {e:?}");
      return false;
    }

    let tables = self.levels[src as usize].drain(indices);
    for t in tables {
      self.levels[dst as usize].add_rc(t);
    }
    self.dirty = true;
    true
  }
}

#[inline]
pub fn new<T: Meta>(ckp: Rc<RefCell<Ckp>>, ver: Ver) -> Levels<T> {
  Levels::new(&[], ckp, ver)
}

#[inline]
pub fn conf<T: Meta>(conf: &[Conf], ckp: Rc<RefCell<Ckp>>, ver: Ver) -> Levels<T> {
  Levels::new(conf, ckp, ver)
}
