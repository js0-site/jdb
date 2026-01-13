use jdb_base::ckp::{Levels as LevelsTrait, SstOp};

use crate::Levels;

impl LevelsTrait for Levels {
  #[inline]
  fn update(&mut self, op: SstOp) {
    match op {
      SstOp::Mem2Sst { meta } => {
        self.push(meta);
      }
      SstOp::Compact { add, rm } => {
        for (level, ids) in rm {
          self.rm_ids(level, ids);
        }
        if !add.is_empty() {
          self.push_iter(add);
        }
      }
    }
  }
}
