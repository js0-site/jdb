use jdb_base::{ckp::sst::Sst, sst::Level};

use super::Score;
use crate::Id;

impl Score {
  pub fn new(iter: impl IntoIterator<Item = (Id, Sst)>) -> Self {
    let mut score = Self {
      level_target_size: Default::default(),
      score: Default::default(),
      total_size: 0,
      l0_cnt: 0,
      level_size: Default::default(),
      base_level: Level::L6,
      level_files: Default::default(),
      id_sst: Default::default(),
      dirty: true,
    };
    score.push_iter(iter);
    score
  }

  #[inline]
  pub fn push(&mut self, id: Id, sst: Sst) {
    self.add(id, &sst);
    self.id_sst.insert(id, sst);
  }

  #[inline]
  pub fn rm<'a>(&mut self, iter: impl IntoIterator<Item = (Level, &'a [Id])>) {
    for (level, ids) in iter {
      for &id in ids {
        if let Some(sst) = self.id_sst.remove(&id) {
          debug_assert_eq!(sst.level, level);
          self.rm_sst(id, &sst);
        }
      }
    }
  }

  #[inline]
  pub fn sink<'a>(
    &mut self,
    add: impl IntoIterator<Item = (Id, Sst)>,
    rm: impl IntoIterator<Item = (Level, &'a [Id])>,
  ) {
    self.push_iter(add);
    self.rm(rm);
  }
}
