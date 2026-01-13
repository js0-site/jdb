use jdb_base::ckp::{Levels as LevelsTrait, Op};

use crate::{Levels, Meta};

impl LevelsTrait for Levels {
  #[inline]
  fn update(&mut self, op: &Op) {
    match op {
      Op::Mem2Sst { meta } => {
        self.push(&meta.meta, meta.sst_level);
      }
      Op::Compact { add, rm } => {
        // 1. Remove rm from all levels (IDs are unique)
        // 1. 从所有层级移除 rm (ID 唯一)
        if !rm.is_empty() {
          let rm_and_mark = |m: &Meta| {
            if rm.contains(&m.id) {
              m.mark_rm();
              false
            } else {
              true
            }
          };
          self.l0.retain(rm_and_mark);
          for vec in &mut self.levels {
            vec.retain(rm_and_mark);
          }
        }

        // 2. Add add to respective levels
        // 2. 将 add 添加到对应层级
        for add in add {
          self.push(&add.meta, add.sst_level);
        }
      }
    }
  }
}
