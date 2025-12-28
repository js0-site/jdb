//! WAL file header / WAL 文件头

use super::consts::{HEADER_SIZE, WAL_VERSION};

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
#[inline(always)]
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
#[inline(always)]
pub(crate) fn check_header(buf: &mut [u8]) -> HeaderState {
  // Length check enables compiler to elide bounds checks below
  // 长度检查使编译器能消除下方的边界检查
  let Some(header) = buf.get_mut(..HEADER_SIZE) else {
    return HeaderState::Invalid;
  };

  let ver1 = u32::from_le_bytes(header[0..4].try_into().unwrap());
  let ver2 = u32::from_le_bytes(header[4..8].try_into().unwrap());
  let stored_crc = u32::from_le_bytes(header[8..12].try_into().unwrap());
  let crc1 = crc32fast::hash(&header[0..4]);

  // Case 1: all valid / 全部正确
  if ver1 == ver2 && ver1 == WAL_VERSION && crc1 == stored_crc {
    return HeaderState::Ok;
  }

  // Case 2: ver1 + crc valid, fix ver2 / ver1 + crc 正确，修复 ver2
  if ver1 == WAL_VERSION && crc1 == stored_crc {
    header[4..8].copy_from_slice(&ver1.to_le_bytes());
    return HeaderState::Repaired;
  }

  // Case 3: ver2 + crc valid, fix ver1 / ver2 + crc 正确，修复 ver1
  let crc2 = crc32fast::hash(&header[4..8]);
  if ver2 == WAL_VERSION && crc2 == stored_crc {
    header[0..4].copy_from_slice(&ver2.to_le_bytes());
    return HeaderState::Repaired;
  }

  // Case 4: ver1 == ver2 == WAL_VERSION, fix crc / ver1 == ver2 == WAL_VERSION，修复 crc
  if ver1 == ver2 && ver1 == WAL_VERSION {
    header[8..12].copy_from_slice(&crc1.to_le_bytes());
    return HeaderState::Repaired;
  }

  HeaderState::Invalid
}
