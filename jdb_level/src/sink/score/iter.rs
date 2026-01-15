use jdb_base::sst::Level;

use super::{super::Sink, LevelScore, SCALE, Score};

impl Iterator for Score {
  type Item = Sink;

  /// Get next GC target (triggers lazy computation)
  /// 获取下一个 GC 目标（触发延迟计算）
  fn next(&mut self) -> Option<Self::Item> {
    self.update();

    let (idx, &max) = self.score.iter().enumerate().max_by_key(|&(_, s)| s)?;
    if max < SCALE as LevelScore {
      return None;
    }

    // SAFETY: idx < N, Level conversion is safe
    // 安全：idx < N，Level 转换安全
    let from: Level = unsafe { std::mem::transmute(idx as u8) };

    if from == Level::L0 {
      return Some(Sink::L0(self.base_level));
    }

    let to = if from < self.base_level {
      self.base_level
    } else {
      from.next().unwrap_or(Level::L6)
    };

    // SAFETY: from is L1-L5, idx = from-1 is 0-4
    // 安全：from 是 L1-L5，idx = from-1 为 0-4
    let files = unsafe { self.level_files.get_unchecked(from as usize - 1) };
    let &(id, _) = files.first()?;

    Some(Sink::L1Plus { from, to, id })
  }
}
