/// Reference to a value, either inline or external / 值的引用，可能内联或在外部存储
pub enum ValRef<'a> {
  Inline(&'a [u8]),
  External { len: u64, crc: u32 },
}

impl AsRef<[u8]> for ValRef<'_> {
  fn as_ref(&self) -> &[u8] {
    match self {
      ValRef::Inline(data) => data,
      ValRef::External { .. } => &[],
    }
  }
}
