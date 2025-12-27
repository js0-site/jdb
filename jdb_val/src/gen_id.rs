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
}

impl Iterator for GenId {
  type Item = u64;

  #[inline]
  fn next(&mut self) -> Option<u64> {
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
    Some(id)
  }
}

impl Default for GenId {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}
