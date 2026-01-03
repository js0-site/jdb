// Configurable workload distribution for benchmarks
// 可配置的基准测试工作负载分布
//
// All data is generated from real text files for future compression testing.
// 所有数据都从真实文本文件生成，便于将来测试压缩。

use std::path::Path;

use crate::{DatasetStats, Result, ZipfWorkload};

/// Value size tier configuration / Value 大小层级配置
#[derive(Debug, Clone)]
pub struct SizeTier {
  /// Tier name / 层级名称
  pub name: &'static str,
  /// Min size in bytes / 最小字节数
  pub min: usize,
  /// Max size in bytes / 最大字节数
  pub max: usize,
  /// Weight percentage (0-100) / 权重百分比
  pub weight: u32,
}

/// Workload configuration / 工作负载配置
#[derive(Debug, Clone)]
pub struct WorkloadConfig {
  /// Size tiers / 大小层级
  pub tiers: Vec<SizeTier>,
  /// Total target size in bytes / 目标总大小（字节）
  pub total_size: usize,
  /// Zipf exponent / Zipf 指数
  pub zipf_s: f64,
  /// Random seed / 随机种子
  pub seed: u64,
}

impl Default for WorkloadConfig {
  /// Default: Facebook USR/APP/VAR pool distribution
  /// 默认：Facebook USR/APP/VAR 池分布
  /// Reference: FAST'20 Facebook RocksDB workload analysis
  /// 参考：FAST'20 Facebook RocksDB 工作负载分析
  fn default() -> Self {
    Self {
      tiers: vec![
        SizeTier {
          name: "Tiny Metadata",
          min: 16,
          max: 100,
          weight: 40,
        },
        SizeTier {
          name: "Small Structs",
          min: 100,
          max: 1024,
          weight: 35,
        },
        SizeTier {
          name: "Medium Content",
          min: 1024,
          max: 10240,
          weight: 20,
        },
        SizeTier {
          name: "Large Objects",
          min: 10240,
          max: 102400,
          weight: 4,
        },
        SizeTier {
          name: "Huge Blobs",
          min: 102400,
          max: 1048576,
          weight: 1,
        },
      ],
      total_size: 50 * 1024 * 1024, // 50MB
      zipf_s: 1.5,                  // Higher s for more realistic hot-spot access
      seed: 42,
    }
  }
}

impl WorkloadConfig {
  /// Create config with uniform size distribution (10 buckets, 10% each)
  /// 创建均匀大小分布配置（10 档，每档 10%）
  pub fn uniform_buckets() -> Self {
    Self {
      tiers: vec![
        SizeTier {
          name: "64-128B",
          min: 64,
          max: 128,
          weight: 10,
        },
        SizeTier {
          name: "128-256B",
          min: 128,
          max: 256,
          weight: 10,
        },
        SizeTier {
          name: "256-512B",
          min: 256,
          max: 512,
          weight: 10,
        },
        SizeTier {
          name: "512B-1KB",
          min: 512,
          max: 1024,
          weight: 10,
        },
        SizeTier {
          name: "1-2KB",
          min: 1024,
          max: 2048,
          weight: 10,
        },
        SizeTier {
          name: "2-4KB",
          min: 2048,
          max: 4096,
          weight: 10,
        },
        SizeTier {
          name: "4-8KB",
          min: 4096,
          max: 8192,
          weight: 10,
        },
        SizeTier {
          name: "8-16KB",
          min: 8192,
          max: 16384,
          weight: 10,
        },
        SizeTier {
          name: "16-32KB",
          min: 16384,
          max: 32768,
          weight: 10,
        },
        SizeTier {
          name: "32-64KB",
          min: 32768,
          max: 65536,
          weight: 10,
        },
      ],
      total_size: 50 * 1024 * 1024,
      zipf_s: 1.2,
      seed: 42,
    }
  }

  /// Create config for small objects only (metadata/config cache)
  /// 创建仅小对象配置（元数据/配置缓存）
  pub fn small_objects() -> Self {
    Self {
      tiers: vec![
        SizeTier {
          name: "Tiny",
          min: 16,
          max: 64,
          weight: 30,
        },
        SizeTier {
          name: "Small",
          min: 64,
          max: 256,
          weight: 40,
        },
        SizeTier {
          name: "Medium",
          min: 256,
          max: 1024,
          weight: 30,
        },
      ],
      total_size: 50 * 1024 * 1024,
      zipf_s: 1.2,
      seed: 42,
    }
  }

  /// Set total size / 设置总大小
  pub fn with_total_size(mut self, size: usize) -> Self {
    self.total_size = size;
    self
  }

  /// Set Zipf exponent / 设置 Zipf 指数
  pub fn with_zipf_s(mut self, s: f64) -> Self {
    self.zipf_s = s;
    self
  }

  /// Set random seed / 设置随机种子
  pub fn with_seed(mut self, seed: u64) -> Self {
    self.seed = seed;
    self
  }

  /// Add custom tier / 添加自定义层级
  pub fn add_tier(mut self, name: &'static str, min: usize, max: usize, weight: u32) -> Self {
    self.tiers.push(SizeTier {
      name,
      min,
      max,
      weight,
    });
    self
  }

  /// Clear all tiers / 清除所有层级
  pub fn clear_tiers(mut self) -> Self {
    self.tiers.clear();
    self
  }
}

/// Generated workload data / 生成的工作负载数据
pub struct Workload {
  data: Vec<(String, Vec<u8>)>,
  config: WorkloadConfig,
  stats: DatasetStats,
}

impl Workload {
  /// Get data slice / 获取数据切片
  pub fn data(&self) -> &[(String, Vec<u8>)] {
    &self.data
  }

  /// Get config / 获取配置
  pub fn config(&self) -> &WorkloadConfig {
    &self.config
  }

  /// Get pre-computed stats / 获取预计算的统计
  pub fn stats(&self) -> &DatasetStats {
    &self.stats
  }

  /// Convert to ZipfWorkload / 转换为 ZipfWorkload
  pub fn into_zipf(self) -> ZipfWorkload<String, Vec<u8>> {
    ZipfWorkload::new(self.data, self.config.zipf_s, self.config.seed)
  }

  /// Item count / 条目数
  pub fn len(&self) -> usize {
    self.data.len()
  }

  /// Is empty / 是否为空
  pub fn is_empty(&self) -> bool {
    self.data.is_empty()
  }

  /// Total size in bytes / 总字节数
  pub fn total_size(&self) -> usize {
    self.stats.total_size_bytes as usize
  }

  /// Average item size / 平均条目大小
  pub fn avg_size(&self) -> usize {
    self.stats.avg_item_size
  }
}

/// Load workload from text files with configured distribution
/// 从文本文件加载符合配置分布的工作负载
///
/// All values are slices of real text content for compression testing.
/// 所有值都是真实文本内容的切片，便于压缩测试。
pub fn load_workload(data_dir: &Path, config: WorkloadConfig) -> Result<Workload> {
  let txt_dir = data_dir.join("txt");

  // Collect all file contents / 收集所有文件内容
  let mut files = Vec::new();
  collect_files(&txt_dir, &mut files)?;

  if files.is_empty() {
    let stats = DatasetStats::from_kv::<String, Vec<u8>>(&[]);
    return Ok(Workload {
      data: Vec::new(),
      config,
      stats,
    });
  }

  // Concatenate all files for large slice generation / 连接所有文件用于大切片生成
  let all_content: Vec<u8> = files.iter().flat_map(|(_, c)| c.iter().copied()).collect();

  let mut rng = fastrand::Rng::with_seed(config.seed);
  let mut all_data = Vec::new();

  // Calculate target count for each tier based on weight (count distribution)
  // 根据权重计算每层目标条目数（按数量分布）
  // First pass: estimate total items to reach total_size
  // 第一遍：估算达到 total_size 需要的总条目数
  let total_weight: u32 = config.tiers.iter().map(|t| t.weight).sum();
  let avg_item_size: f64 = config
    .tiers
    .iter()
    .map(|t| (t.weight as f64 / total_weight as f64) * ((t.min + t.max) as f64 / 2.0))
    .sum();
  let est_total_items = (config.total_size as f64 / avg_item_size) as usize;

  // Generate data for each tier by COUNT (not size)
  // 按条目数量（而非大小）为每个层级生成数据
  for tier in &config.tiers {
    let target_count = est_total_items * tier.weight as usize / total_weight as usize;

    for idx in 0..target_count {
      // Pick random file / 随机选择文件
      let file_idx = rng.usize(0..files.len());
      let (name, content) = &files[file_idx];

      // Determine slice length / 确定切片长度
      let slice_len = rng.usize(tier.min..=tier.max);

      // Generate slice from real text / 从真实文本生成切片
      let val = if slice_len <= content.len() {
        // File is large enough / 文件足够大
        let start = if content.len() > slice_len {
          rng.usize(0..content.len() - slice_len)
        } else {
          0
        };
        content[start..start + slice_len].to_vec()
      } else if slice_len <= all_content.len() {
        // Use concatenated content / 使用连接的内容
        let start = rng.usize(0..all_content.len() - slice_len);
        all_content[start..start + slice_len].to_vec()
      } else {
        // Repeat content to fill / 重复内容填充
        let mut val = Vec::with_capacity(slice_len);
        while val.len() < slice_len {
          let remaining = slice_len - val.len();
          let chunk_len = remaining.min(all_content.len());
          let start = rng.usize(0..all_content.len().saturating_sub(chunk_len).max(1));
          val.extend_from_slice(&all_content[start..start + chunk_len]);
        }
        val.truncate(slice_len);
        val
      };

      let key = format!("{name}_{idx:04x}");
      all_data.push((key, val));
    }
  }

  rng.shuffle(&mut all_data);

  // Compute stats / 计算统计
  let stats = DatasetStats::from_kv(&all_data);

  Ok(Workload {
    data: all_data,
    config,
    stats,
  })
}

/// Collect all txt files recursively / 递归收集所有 txt 文件
fn collect_files(dir: &Path, files: &mut Vec<(String, Vec<u8>)>) -> Result<()> {
  if !dir.is_dir() {
    return Ok(());
  }

  for entry in std::fs::read_dir(dir)? {
    let entry = entry?;
    let path = entry.path();

    if path.is_dir() {
      collect_files(&path, files)?;
    } else if path
      .extension()
      .is_some_and(|e| e.eq_ignore_ascii_case("txt"))
    {
      let name = path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();

      if let Ok(content) = std::fs::read(&path)
        && content.len() >= 16
      {
        files.push((name, content));
      }
    }
  }
  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_default_config() {
    let cfg = WorkloadConfig::default();
    assert_eq!(cfg.tiers.len(), 5);
    let total_weight: u32 = cfg.tiers.iter().map(|t| t.weight).sum();
    assert_eq!(total_weight, 100);
  }

  #[test]
  fn test_uniform_config() {
    let cfg = WorkloadConfig::uniform_buckets();
    assert_eq!(cfg.tiers.len(), 10);
    let total_weight: u32 = cfg.tiers.iter().map(|t| t.weight).sum();
    assert_eq!(total_weight, 100);
  }

  #[test]
  fn test_config_builder() {
    let cfg = WorkloadConfig::default()
      .with_total_size(10 * 1024 * 1024)
      .with_zipf_s(1.5)
      .with_seed(123);
    assert_eq!(cfg.total_size, 10 * 1024 * 1024);
    assert!((cfg.zipf_s - 1.5).abs() < 0.001);
    assert_eq!(cfg.seed, 123);
  }
}
