use jdb_base::ckp::sst::ckp::{Levels, Op};

impl Levels for crate::Levels {
  #[inline]
  fn update(&mut self, op: Op) {
    match op {
      Op::Mem2Sst { meta } => {
        // Update score and levels with new SST
        // 使用新 SST 更新分数和层级
        self.sink.push(meta.meta.id, meta.sst);
        self.push(meta);
      }
      Op::Compact { add, rm } => {
        // Update score lazily with iterators
        // 使用迭代器延迟更新分数
        self.sink.sink(
          add.iter().map(|m| (m.meta.id, m.sst)),
          rm.iter().map(|(level, ids)| (*level, ids.as_slice())),
        );

        // Update levels state
        // 更新层级状态
        for (level, ids) in rm {
          self.rm(level, ids);
        }
        if !add.is_empty() {
          self.push_iter(add);
        }
      }
    }
  }
}
