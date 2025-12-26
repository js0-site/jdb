use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[repr(C)]
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Immutable, KnownLayout, Default,
)]
pub struct Val([u8; 16]);

impl Val {
  #[inline]
  pub fn new_inline(data: &[u8]) -> Self {
    let mut val = Self::default();
    unsafe {
      let ptr = val.0.as_mut_ptr();
      let len = data.len().min(16);
      std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, len);
      if len < 16 {
        std::ptr::write_bytes(ptr.add(len), 0, 16 - len);
      }
    }
    val
  }

  #[inline]
  pub fn new_ext(len: u64, crc: u32) -> Self {
    let mut val = Self::default();
    unsafe {
      let ptr = val.0.as_mut_ptr();
      (ptr as *mut u64).write_unaligned(len.to_le());
      (ptr.add(8) as *mut u32).write_unaligned(crc.to_le());
      (ptr.add(12) as *mut u32).write_unaligned(0);
    }
    val
  }

  #[inline]
  pub fn external(&self) -> (u64, u32) {
    unsafe {
      let ptr = self.0.as_ptr();
      (
        u64::from_le((ptr as *const u64).read_unaligned()),
        u32::from_le((ptr.add(8) as *const u32).read_unaligned()),
      )
    }
  }

  #[inline]
  pub fn inline(&self, len: usize) -> &[u8] {
    &self.0[..len.min(16)]
  }
}
