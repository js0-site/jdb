// Corpus loader for benchmark data
// 基准测试语料加载器

use std::path::Path;

use crate::{Result, ZipfWorkload};

/// Large corpus: (filename, file_content bytes)
/// 大语料：(文件名, 文件内容字节)
pub type LargeCorpus = ZipfWorkload<String, Vec<u8>>;

/// Medium corpus: (filename, partial bytes)
/// 中等语料：(文件名, 部分字节)
pub type MediumCorpus = ZipfWorkload<String, Vec<u8>>;

/// Small corpus: (filename, file_size as bytes)
/// 小语料：(文件名, 文件大小字节)
pub type SmallCorpus = ZipfWorkload<String, Vec<u8>>;

/// All corpus types loaded together
/// 所有语料类型一起加载
pub struct AllCorpus {
  pub large: LargeCorpus,
  pub medium: MediumCorpus,
  pub small: SmallCorpus,
}

/// Load all corpus types in one pass
/// 一次遍历加载所有语料类型
pub fn load_all(data_dir: &Path, s: f64, seed: u64) -> Result<AllCorpus> {
  let txt_dir = data_dir.join("txt");

  let mut large_data = Vec::new();
  let mut medium_data = Vec::new();
  let mut small_data = Vec::new();

  let mut rng = fastrand::Rng::with_seed(seed);

  load_recursive(
    &txt_dir,
    &mut large_data,
    &mut medium_data,
    &mut small_data,
    &mut rng,
  )?;

  Ok(AllCorpus {
    large: ZipfWorkload::new(large_data, s, seed),
    medium: ZipfWorkload::new(medium_data, s, seed),
    small: ZipfWorkload::new(small_data, s, seed),
  })
}

fn load_recursive(
  dir: &Path,
  large: &mut Vec<(String, Vec<u8>)>,
  medium: &mut Vec<(String, Vec<u8>)>,
  small: &mut Vec<(String, Vec<u8>)>,
  rng: &mut fastrand::Rng,
) -> Result<()> {
  if !dir.is_dir() {
    return Ok(());
  }

  for entry in std::fs::read_dir(dir)? {
    let entry = entry?;
    let path = entry.path();

    if path.is_dir() {
      load_recursive(&path, large, medium, small, rng)?;
    } else if path
      .extension()
      .is_some_and(|e| e.eq_ignore_ascii_case("txt"))
    {
      let name = path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();

      let content = match std::fs::read(&path) {
        Ok(b) if !b.is_empty() => b,
        _ => continue,
      };

      let len = content.len();

      // Small: file size as u64 bytes / 小：文件大小的 u64 字节
      small.push((name.clone(), (len as u64).to_le_bytes().to_vec()));

      // Medium: random slice / 中等：随机切片
      let mid_len = if len <= 1024 {
        len
      } else {
        rng.usize(1024..65536.min(len))
      };
      let start = if len > mid_len {
        rng.usize(0..len - mid_len)
      } else {
        0
      };
      medium.push((name.clone(), content[start..start + mid_len].to_vec()));

      // Large: full content / 大：完整内容
      large.push((name, content));
    }
  }
  Ok(())
}
