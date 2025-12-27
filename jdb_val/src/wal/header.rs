//! WAL file header / WAL 文件头

/// WAL file header size / WAL 文件头大小
pub const HEADER_SIZE: usize = 12;
/// Current version / 当前版本
pub const WAL_VERSION: u32 = 1;

/// Header check result / 头校验结果
pub(crate) enum HeaderState {
  /// Valid / 有效
  Ok,
  /// Repaired / 已修复
  Repaired,
  /// Cannot repair / 无法修复
  Invalid,
}

/// Build WAL file header (12 bytes) / 构建 WAL 文件头
/// [Version u32] [Version u32 copy] [CRC32 of first 4B]
#[inline]
pub(crate) fn build_header() -> [u8; HEADER_SIZE] {
  let mut buf = [0u8; HEADER_SIZE];
  let ver = WAL_VERSION.to_le_bytes();
  buf[0..4].copy_from_slice(&ver);
  buf[4..8].copy_from_slice(&ver);
  let crc = crc32fast::hash(&buf[0..4]);
  buf[8..12].copy_from_slice(&crc.to_le_bytes());
  buf
}

/// Check and repair header / 校验并修复头
#[inline]
pub(crate) fn check_header(buf: &mut [u8]) -> HeaderState {
  if buf.len() < HEADER_SIZE {
    return HeaderState::Invalid;
  }

  let ver1 = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
  let ver2 = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
  let stored_crc = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
  let crc1 = crc32fast::hash(&buf[0..4]);

  // Case 1: all valid / 全部正确
  if ver1 == ver2 && crc1 == stored_crc {
    return HeaderState::Ok;
  }

  // Case 2: ver1 + crc valid, fix ver2 / ver1 + crc 正确，修复 ver2
  if crc1 == stored_crc {
    let v = ver1.to_le_bytes();
    buf[4..8].copy_from_slice(&v);
    return HeaderState::Repaired;
  }

  // Case 3: ver2 + crc valid, fix ver1 / ver2 + crc 正确，修复 ver1
  let crc2 = crc32fast::hash(&buf[4..8]);
  if crc2 == stored_crc {
    let v = ver2.to_le_bytes();
    buf[0..4].copy_from_slice(&v);
    return HeaderState::Repaired;
  }

  // Case 4: ver1 == ver2, fix crc / ver1 == ver2，修复 crc
  if ver1 == ver2 {
    buf[8..12].copy_from_slice(&crc1.to_le_bytes());
    return HeaderState::Repaired;
  }

  HeaderState::Invalid
}
