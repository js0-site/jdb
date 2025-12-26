/// Reference to a key, either inline or internal with external storage / 键的引用，可能内联或在外部存储
pub enum KeyRef<'a> {
  Inline(&'a [u8]),
  External {
    hash: u64,
    prefix: &'a [u8],
    len: u16,
    file_id: u32,
    offset: u64,
    crc: u32,
  },
}

impl AsRef<[u8]> for KeyRef<'_> {
  fn as_ref(&self) -> &[u8] {
    match self {
      KeyRef::Inline(data) => data,
      KeyRef::External { prefix, .. } => prefix,
    }
  }
}
