//! Path encoding utilities / 路径编码工具
//!
//! Base32 encoded paths: xx/xx/xx (2+2+remaining)
//! Base32 编码路径：xx/xx/xx（2+2+剩余）
#![cfg_attr(docsrs, feature(doc_cfg))]

use std::path::{Path, PathBuf};

use fast32::base32::CROCKFORD_LOWER;

/// Encode id to path: base/prefix/xx/xx/xx
/// 编码 ID 为路径
#[inline]
pub fn encode(base: &Path, prefix: &str, id: u64) -> PathBuf {
  let encoded = CROCKFORD_LOWER.encode_u64(id);
  let padded = format!("{encoded:0>6}");
  let (d1, rest) = padded.split_at(2);
  let (d2, name) = rest.split_at(2);
  base.join(prefix).join(d1).join(d2).join(name)
}

/// Decode path to id / 解码路径为 ID
pub fn decode(path: &Path) -> Option<u64> {
  let name = path.file_name()?.to_str()?;
  let d2 = path.parent()?.file_name()?.to_str()?;
  let d1 = path.parent()?.parent()?.file_name()?.to_str()?;

  let encoded = format!("{d1}{d2}{name}");
  let trimmed = encoded.trim_start_matches('0');
  if trimmed.is_empty() {
    return Some(0);
  }
  CROCKFORD_LOWER.decode_u64(trimmed.as_bytes()).ok()
}
