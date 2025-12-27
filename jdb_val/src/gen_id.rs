//! ID generator with timestamp + offset / 时间戳+偏移量的ID生成器

use coarsetime::Clock;

/// ID generator / ID 生成器
///
/// Format: (timestamp_secs << 20) | pos
/// - High 44 bits: seconds since epoch / 高44位: 秒级时间戳
/// - Low 20 bits: position within second / 低20位: 秒内位置
pub struct GenId {
  ts: u64,
  pos: u32,
}

const POS_BITS: u32 = 20;
const POS_MAX: u32 = (1 << POS_BITS) - 1; // 0xFFFFF

impl GenId {
  /// Create new generator / 创建新生成器
  #[inline]
  pub fn new() -> Self {
    Self {
      ts: Clock::now_since_epoch().as_secs(),
      pos: 0,
    }
  }

  /// Generate next id / 生成下一个 id
  #[inline]
  pub fn next(&mut self) -> u64 {
    let now = Clock::now_since_epoch().as_secs();
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

impl Default for GenId {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}
