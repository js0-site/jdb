// Corpus loader for benchmark data
// 基准测试语料加载器

use std::path::Path;

use crate::{Result, ZipfWorkload};

/// Large text corpus: (filename, file_content)
/// 大文本语料：(文件名, 文件内容)
pub type LargeTextCorpus = ZipfWorkload<String, String>;

/// Medium text corpus: (filename, random_lines)
/// 中等文本语料：(文件名, 随机行)
pub type MediumTextCorpus = ZipfWorkload<String, String>;

/// Small number corpus: (filename, file_size)
/// 小数字语料：(文件名, 文件大小)
pub type SmallNumCorpus = ZipfWorkload<String, u64>;

/// All corpus types loaded together
/// 所有语料类型一起加载
pub struct AllCorpus {
  pub large: LargeTextCorpus,
  pub medium: MediumTextCorpus,
  pub small: SmallNumCorpus,
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
  large: &mut Vec<(String, String)>,
  medium: &mut Vec<(String, String)>,
  small: &mut Vec<(String, u64)>,
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

      // Small: file size / 小数字：文件大小
      let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
      small.push((name.clone(), size));

      // Try read content / 尝试读取内容
      let content = match std::fs::read_to_string(&path) {
        Ok(s) if !s.is_empty() => s,
        _ => continue,
      };

      // Large: full content / 大文本：完整内容
      large.push((name.clone(), content.clone()));

      // Medium: random n lines / 中等文本：随机 n 行
      let lines: Vec<&str> = content.lines().filter(|l| !l.trim().is_empty()).collect();
      if !lines.is_empty() {
        let n = rng.usize(2..256.min(lines.len() + 1));
        let mut selected = Vec::with_capacity(n);
        for _ in 0..n {
          let idx = rng.usize(0..lines.len());
          selected.push(lines[idx]);
        }
        medium.push((name, selected.join("\n")));
      }
    }
  }
  Ok(())
}

/// Load large text corpus / 加载大文本语料
pub fn load_large_text(data_dir: &Path, s: f64, seed: u64) -> Result<LargeTextCorpus> {
  Ok(load_all(data_dir, s, seed)?.large)
}

/// Load medium text corpus / 加载中等文本语料
pub fn load_medium_text(data_dir: &Path, s: f64, seed: u64) -> Result<MediumTextCorpus> {
  Ok(load_all(data_dir, s, seed)?.medium)
}

/// Load small number corpus / 加载小数字语料
pub fn load_small_num(data_dir: &Path, s: f64, seed: u64) -> Result<SmallNumCorpus> {
  Ok(load_all(data_dir, s, seed)?.small)
}
