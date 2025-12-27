//! ID generator with timestamp / 时间戳 ID 生成器

use coarsetime::Clock;

/// ID generator / ID 生成器
///
/// Format: (timestamp_secs << 20) | micros
/// - High 44 bits: seconds since epoch / 高44位: 秒级时间戳
/// - Low 20 bits: microseconds within second / 低20位: 秒内微秒
pub struct GenId {
  ts: u64,
  pos: u32,
}

const POS_BITS: u32 = 20;
const POS_MAX: u32 = (1 << POS_BITS) - 1; // 0xFFFFF (~1M)
const MICROS_PER_SEC: u64 = 1_000_000;

impl GenId {
  /// Create new generator / 创建新生成器
  ///
  /// Init pos with microseconds to avoid restart collision
  /// 用微秒初始化 pos 避免重启冲突
  #[inline]
  pub fn new() -> Self {
    let now = Clock::now_since_epoch();
    let micros = now.as_micros() % MICROS_PER_SEC;
    Self {
      ts: now.as_secs(),
      pos: micros as u32,
    }
  }

  /// Ensure generator ahead of last_id / 确保生成器领先于 last_id
  ///
  /// Must call after recovery to prevent ID collision
  /// 恢复后必须调用以防止 ID 碰撞
  pub fn init_last_id(&mut self, last_id: u64) {
    let last_ts = last_id >> POS_BITS;
    let last_pos = (last_id & POS_MAX as u64) as u32;

    if last_ts > self.ts {
      self.ts = last_ts;
      self.pos = last_pos + 1;
    } else if last_ts == self.ts && last_pos >= self.pos {
      self.pos = last_pos + 1;
    }

    if self.pos > POS_MAX {
      self.ts += 1;
      self.pos = 0;
    }
  }

  /// Generate next id / 生成下一个 id
  #[inline]
  pub fn next_id(&mut self) -> u64 {
    let now = Clock::now_since_epoch().as_secs();
    // Use max to ensure monotonicity even if clock moves backward
    // 使用 max 确保单调性，即使时钟回拨
    if now > self.ts {
      self.ts = now;
      self.pos = 0;
    } else if self.pos >= POS_MAX {
      // Position exhausted, borrow from future / 位置耗尽，借用未来时间
      self.ts += 1;
      self.pos = 0;
    }
    let id = (self.ts << POS_BITS) | (self.pos as u64);
    self.pos += 1;
    id
  }
}

impl Iterator for GenId {
  type Item = u64;

  #[inline]
  fn next(&mut self) -> Option<u64> {
    Some(self.next_id())
  }
}

impl Default for GenId {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_monotonic() {
    let mut g = GenId::new();
    let mut prev = g.next_id();
    for _ in 0..1000 {
      let cur = g.next_id();
      assert!(cur > prev, "IDs must be monotonic");
      prev = cur;
    }
  }

  #[test]
  fn test_init_last_id() {
    let mut g = GenId::new();
    let id1 = g.next_id();

    // Simulate recovery with future ID / 模拟用未来 ID 恢复
    let future_id = id1 + 1000;
    g.init_last_id(future_id);

    let id2 = g.next_id();
    assert!(id2 > future_id, "ID after init_last_id must be greater");
  }

  #[test]
  fn test_iterator() {
    let mut g = GenId::new();
    let ids: Vec<_> = g.by_ref().take(5).collect();
    assert_eq!(ids.len(), 5);
    for i in 1..5 {
      assert!(ids[i] > ids[i - 1]);
    }
  }

  #[test]
  fn test_pos_overflow() {
    let mut g = GenId::new();
    // Force pos to max / 强制 pos 到最大值
    g.pos = (1 << 20) - 1;
    let id1 = g.next_id();
    let id2 = g.next_id();
    assert!(id2 > id1, "Should handle pos overflow");
  }
}
