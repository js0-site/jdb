use jdb_base::ckp::{Levels as LevelsTrait, SstOp};

use crate::Levels;

impl LevelsTrait for Levels {
  #[inline]
  fn update(&mut self, op: SstOp) {
    match op {
      SstOp::Mem2Sst { meta } => {
        // Update score and levels with new SST
        // 使用新 SST 更新分数和层级
        self.sink_score.push(meta.meta.id, meta.sst);
        self.push(meta);
      }
      SstOp::Compact { add, rm } => {
        // Update score lazily with iterators
        // 使用迭代器延迟更新分数
        self.sink_score.update(
          add.iter().map(|m| (m.meta.id, m.sst)),
          rm.iter().map(|(l, ids)| (*l, ids.as_slice())),
        );

        // Update levels state
        // 更新层级状态
        // rm: (Level, Vec<u64>)
        for (l, ids) in rm {
          self.rm_ids(l, ids);
        }
        if !add.is_empty() {
          self.push_iter(add);
        }
      }
    }
  }
}
