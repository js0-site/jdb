use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct Key([u8; 64]);

impl Default for Key {
  fn default() -> Self {
    Self([0u8; 64])
  }
}

impl Key {
  #[inline]
  pub fn new_inline(&mut self, data: &[u8]) {
    self.0.fill(0);
    let len = data.len().min(64);
    self.0[..len].copy_from_slice(&data[..len]);
  }

  #[inline]
  pub fn new_ext(&mut self, prefix: &[u8], file_id: u32, offset: u64, crc: u32) {
    let len = prefix.len().min(48);
    self.0[..len].copy_from_slice(&prefix[..len]);
    if len < 48 {
      self.0[len..48].fill(0);
    }
    self.0[48..52].copy_from_slice(&file_id.to_le_bytes());
    self.0[52..60].copy_from_slice(&offset.to_le_bytes());
    self.0[60..64].copy_from_slice(&crc.to_le_bytes());
  }

  #[inline]
  pub(crate) fn external(&self) -> (&[u8], u32, u64, u32) {
    let file_id_b: [u8; 4] = self.0[48..52].try_into().unwrap();
    let offset_b: [u8; 8] = self.0[52..60].try_into().unwrap();
    let crc_b: [u8; 4] = self.0[60..64].try_into().unwrap();
    (
      &self.0[..48],
      u32::from_le_bytes(file_id_b),
      u64::from_le_bytes(offset_b),
      u32::from_le_bytes(crc_b),
    )
  }

  #[inline]
  pub(crate) fn inline(&self, len: usize) -> &[u8] {
    &self.0[..len.min(64)]
  }
}
