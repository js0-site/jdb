use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct Key([u8; 72]);

impl Default for Key {
  fn default() -> Self {
    Self([0u8; 72])
  }
}

impl Key {
  #[inline]
  pub fn new_inline(&mut self, data: &[u8]) {
    unsafe {
      let ptr = self.0.as_mut_ptr();
      let len = data.len().min(72);
      std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, len);
      if len < 72 {
        std::ptr::write_bytes(ptr.add(len), 0, 72 - len);
      }
    }
  }

  #[inline]
  pub fn new_ext(&mut self, hash: u64, prefix: &[u8], file_id: u32, offset: u64, crc: u32) {
    unsafe {
      let ptr = self.0.as_mut_ptr();
      (ptr as *mut u64).write_unaligned(hash.to_le());
      let len = prefix.len().min(48);
      std::ptr::copy_nonoverlapping(prefix.as_ptr(), ptr.add(8), len);
      if len < 48 {
        std::ptr::write_bytes(ptr.add(8 + len), 0, 48 - len);
      }
      (ptr.add(56) as *mut u64).write_unaligned(offset.to_le());
      (ptr.add(64) as *mut u32).write_unaligned(file_id.to_le());
      (ptr.add(68) as *mut u32).write_unaligned(crc.to_le());
    }
  }

  #[inline]
  pub(crate) fn external(&self) -> (u64, &[u8], u32, u64, u32) {
    unsafe {
      let ptr = self.0.as_ptr();
      (
        u64::from_le((ptr as *const u64).read_unaligned()),
        &self.0[8..56],
        u32::from_le((ptr.add(64) as *const u32).read_unaligned()),
        u64::from_le((ptr.add(56) as *const u64).read_unaligned()),
        u32::from_le((ptr.add(68) as *const u32).read_unaligned()),
      )
    }
  }

  #[inline]
  pub(crate) fn inline(&self, len: usize) -> &[u8] {
    &self.0[..len.min(72)]
  }
}
