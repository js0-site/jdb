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

  // SAFETY: length checked >= HEADER_SIZE (12)
  // 安全性：已检查长度 >= HEADER_SIZE (12)
  let ver1 = u32::from_le_bytes(unsafe { buf.get_unchecked(0..4).try_into().unwrap_unchecked() });
  let ver2 = u32::from_le_bytes(unsafe { buf.get_unchecked(4..8).try_into().unwrap_unchecked() });
  let stored_crc =
    u32::from_le_bytes(unsafe { buf.get_unchecked(8..12).try_into().unwrap_unchecked() });
  let crc1 = crc32fast::hash(unsafe { buf.get_unchecked(0..4) });

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
  let crc2 = crc32fast::hash(unsafe { buf.get_unchecked(4..8) });
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

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_build_header() {
    let buf = build_header();
    assert_eq!(buf.len(), HEADER_SIZE);

    let ver = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    assert_eq!(ver, WAL_VERSION);

    let ver2 = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    assert_eq!(ver2, WAL_VERSION);
  }

  #[test]
  fn test_check_header_ok() {
    let mut buf = build_header().to_vec();
    assert!(matches!(check_header(&mut buf), HeaderState::Ok));
  }

  #[test]
  fn test_check_header_repair_ver2() {
    let mut buf = build_header().to_vec();
    buf[4] = 0xFF; // corrupt ver2
    assert!(matches!(check_header(&mut buf), HeaderState::Repaired));
    // ver2 should be repaired / ver2 应该被修复
    assert_eq!(buf[4..8], buf[0..4]);
  }

  #[test]
  fn test_check_header_repair_ver1() {
    let mut buf = build_header().to_vec();
    buf[0] = 0xFF; // corrupt ver1
    assert!(matches!(check_header(&mut buf), HeaderState::Repaired));
    // ver1 should be repaired / ver1 应该被修复
    assert_eq!(buf[0..4], buf[4..8]);
  }

  #[test]
  fn test_check_header_repair_crc() {
    let mut buf = build_header().to_vec();
    buf[8] = 0xFF; // corrupt crc
    assert!(matches!(check_header(&mut buf), HeaderState::Repaired));
  }

  #[test]
  fn test_check_header_invalid() {
    let mut buf = build_header().to_vec();
    buf[0] = 0xFF;
    buf[4] = 0xFE;
    buf[8] = 0xFD;
    assert!(matches!(check_header(&mut buf), HeaderState::Invalid));
  }

  #[test]
  fn test_check_header_short() {
    let mut buf = vec![0u8; 8];
    assert!(matches!(check_header(&mut buf), HeaderState::Invalid));
  }
}
