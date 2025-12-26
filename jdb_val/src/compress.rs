#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compress {
  None = 0x00,
  Lz4 = 0x10,
  Zstd = 0x20,
  Unknown = 0xF0,
}

impl From<u8> for Compress {
  fn from(byte: u8) -> Self {
    match byte & 0xF0 {
      0x00 => Compress::None,
      0x10 => Compress::Lz4,
      0x20 => Compress::Zstd,
      _ => Compress::Unknown,
    }
  }
}

impl Compress {
  #[inline]
  #[must_use]
  pub fn is_none(&self) -> bool {
    matches!(self, Compress::None)
  }

  #[inline]
  #[must_use]
  pub fn is_lz4(&self) -> bool {
    matches!(self, Compress::Lz4)
  }

  #[inline]
  #[must_use]
  pub fn is_zstd(&self) -> bool {
    matches!(self, Compress::Zstd)
  }
}
