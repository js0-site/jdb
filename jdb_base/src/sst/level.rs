#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
  L0 = 0,
  L1 = 1,
  L2 = 2,
  L3 = 3,
  L4 = 4,
  L5 = 5,
  L6 = 6,
}

impl Level {
  pub const LEN: usize = 7;
  pub fn next(&self) -> Option<Self> {
    let v = *self as u8;
    if v < 6 {
      unsafe { Some(std::mem::transmute::<u8, Self>(v + 1)) }
    } else {
      None
    }
  }
}

impl From<Level> for usize {
  #[inline]
  fn from(level: Level) -> Self {
    level as usize
  }
}
