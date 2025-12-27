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

    if last_ts > self.ts || (last_ts == self.ts && last_pos >= self.pos) {
      if last_pos >= POS_MAX {
        self.ts = last_ts + 1;
        self.pos = 0;
      } else {
        self.ts = last_ts;
        self.pos = last_pos + 1;
      }
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
