//! Multi-level manager with dynamic level bytes
//! 带动态层级字节的多层管理器

use jdb_base::table::Meta;

use crate::{
  Conf, Level, Limits, MAX_LEVEL, ParsedConf,
  calc::{calc, needs_compact, target_level},
};

/// Multi-level manager with dynamic level bytes
/// 带动态层级字节的多层管理器
pub struct Levels<T> {
  pub levels: Vec<Level<T>>,
  conf: ParsedConf,
  limits: Limits,
  dirty: bool,
}

impl<T: Meta> Levels<T> {
  pub fn new(conf: &[Conf]) -> Self {
    let c = ParsedConf::new(conf);
    let levels = (0..=MAX_LEVEL).map(Level::new).collect();
    let limits = calc(0, c.base_size, c.ratio);
    Self {
      levels,
      conf: c,
      limits,
      dirty: false,
    }
  }

  /// Recalculate limits if dirty
  /// 如果脏则重算限制
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

  #[inline]
  pub fn next_compaction(&mut self) -> Option<u8> {
    self.recalc();
    for level in 0..=MAX_LEVEL {
      let i = level as usize;
      let l = &self.levels[i];
      if needs_compact(
        level,
        l.len(),
        l.size(),
        self.conf.l0_limit,
        self.limits.base_level,
        self.limits.limits[i],
      ) {
        return Some(level);
      }
    }
    None
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
}

#[inline]
pub fn new_levels<T: Meta>() -> Levels<T> {
  Levels::new(&[])
}

#[inline]
pub fn new_levels_conf<T: Meta>(conf: &[Conf]) -> Levels<T> {
  Levels::new(conf)
}
