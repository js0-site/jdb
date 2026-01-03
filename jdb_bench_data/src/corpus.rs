// Corpus loader for benchmark data (legacy API)
// 基准测试语料加载器（旧版 API）

use std::path::Path;

use crate::{Result, WorkloadConfig, ZipfWorkload, load_workload};

/// Large corpus: (filename, file_content bytes)
/// 大语料：(文件名, 文件内容字节)
pub type LargeCorpus = ZipfWorkload<String, Vec<u8>>;

/// Medium corpus: (filename, partial bytes)
/// 中等语料：(文件名, 部分字节)
pub type MediumCorpus = ZipfWorkload<String, Vec<u8>>;

/// Small corpus: (filename, file_size as bytes)
/// 小语料：(文件名, 文件大小字节)
pub type SmallCorpus = ZipfWorkload<String, Vec<u8>>;

/// All corpus types loaded together (legacy compatibility)
/// 所有语料类型一起加载（旧版兼容）
pub struct AllCorpus {
  pub large: LargeCorpus,
  pub medium: MediumCorpus,
  pub small: SmallCorpus,
}

/// Load corpus with default Facebook USR/APP/VAR distribution
/// 加载默认 Facebook USR/APP/VAR 分布的语料
///
/// This is a legacy API. For new code, use `load_workload()` with `WorkloadConfig`.
/// 这是旧版 API。新代码请使用 `load_workload()` 配合 `WorkloadConfig`。
pub fn load_all(data_dir: &Path, s: f64, seed: u64) -> Result<AllCorpus> {
  let config = WorkloadConfig::default().with_zipf_s(s).with_seed(seed);

  let workload = load_workload(data_dir, config)?;
  let mut data: Vec<_> = workload.data().to_vec();

  if data.is_empty() {
    return Ok(AllCorpus {
      large: ZipfWorkload::new(Vec::new(), s, seed),
      medium: ZipfWorkload::new(Vec::new(), s, seed),
      small: ZipfWorkload::new(Vec::new(), s, seed),
    });
  }

  // Split into 3 parts for compatibility / 分成 3 部分以兼容
  let n = data.len();
  let third = n / 3;

  let small_data: Vec<_> = data.drain(..third).collect();
  let medium_data: Vec<_> = data.drain(..third).collect();
  let large_data = data;

  Ok(AllCorpus {
    large: ZipfWorkload::new(large_data, s, seed),
    medium: ZipfWorkload::new(medium_data, s, seed),
    small: ZipfWorkload::new(small_data, s, seed),
  })
}
