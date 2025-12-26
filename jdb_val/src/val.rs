use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[repr(C)]
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Immutable, KnownLayout, Default,
)]
pub struct Val([u8; 16]);

impl Val {
  #[inline]
  pub fn new_inline(&mut self, data: &[u8]) {
    self.0.fill(0);
    let len = data.len().min(16);
    self.0[..len].copy_from_slice(&data[..len]);
  }

  #[inline]
  pub fn new_ext(&mut self, len: u64, crc: u32) {
    self.0[0..8].copy_from_slice(&len.to_le_bytes());
    self.0[8..12].copy_from_slice(&crc.to_le_bytes());
    self.0[12..16].fill(0);
  }

  #[inline]
  pub(crate) fn external(&self) -> (u64, u32) {
    let b_len: [u8; 8] = self.0[0..8].try_into().unwrap();
    let b_crc: [u8; 4] = self.0[8..12].try_into().unwrap();
    (u64::from_le_bytes(b_len), u32::from_le_bytes(b_crc))
  }

  #[inline]
  pub(crate) fn inline(&self, len: usize) -> &[u8] {
    &self.0[..len.min(16)]
  }
}
