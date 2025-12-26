#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
  /// Normal value stored in slab / 存储在 slab 中的普通值
  Val = 0x00,
  /// Removal record (tombstone) / 删除标记
  Rm = 0x01,
  /// Value inlined in the header / 内联在头部中的值
  Inline = 0x02,
  /// Saved as a separate file (large object) / 作为独立文件保存（大对象）
  Blob = 0x03,
  /// Reserved / 未知或保留
  Unknown = 0x0F,
}

impl From<u8> for Kind {
  fn from(byte: u8) -> Self {
    match byte & 0x0F {
      0x00 => Kind::Val,
      0x01 => Kind::Rm,
      0x02 => Kind::Inline,
      0x03 => Kind::Blob,
      _ => Kind::Unknown,
    }
  }
}
