//! PGM index builder
//! PGM 索引构建器

use jdb_pgm::{Pgm, key_to_u64};
use shared_prefix_len::shared_prefix_len;

/// Build PGM index
/// 构建 PGM 索引
pub(crate) fn build(first_keys: &[Box<[u8]>], epsilon: usize) -> (Vec<u8>, u8) {
  if first_keys.len() <= 1 {
    return (Vec::new(), 0);
  }

  let first = &first_keys[0];
  let last = &first_keys[first_keys.len() - 1];
  let prefix_len = shared_prefix_len(first, last).min(255) as u8;

  let mut data: Vec<u64> = Vec::with_capacity(first_keys.len());
  data.extend(
    first_keys
      .iter()
      .map(|k| key_to_u64(&k[prefix_len as usize..])),
  );
  data.dedup();

  if data.len() <= 1 {
    return (Vec::new(), prefix_len);
  }

  let Ok(pgm) = Pgm::new(&data, epsilon, false) else {
    return (Vec::new(), prefix_len);
  };
  (bitcode::encode(&pgm), prefix_len)
}
