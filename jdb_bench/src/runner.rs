// Benchmark runner and configuration
// 基准测试运行器和配置

use std::{io::Write, path::PathBuf, time::Instant};

use serde::{Deserialize, Serialize};

use crate::{BenchEngine, BenchMetrics, LatencyHistogram, LatencyStats, Result};

/// Workload type for benchmark / 基准测试工作负载类型
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
pub enum WorkloadType {
  /// Sequential write only / 顺序写入
  #[default]
  Sequential,
  /// Random read/write / 随机读写
  Random,
  /// Zipf distribution (hot-spot) / Zipf 分布（热点）
  Zipf { s: f64 },
  /// Mixed read/write with ratio / 混合读写
  Mixed { read_ratio: f64 },
  /// Write heavy: 90% write, 10% read / 写密集
  WriteHeavy,
  /// Read heavy: 10% write, 90% read / 读密集
  ReadHeavy,
  /// Balanced: 50% write, 50% read / 均衡
  Balanced,
}

/// Benchmark configuration / 基准测试配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchConfig {
  /// Value sizes to test (bytes) / 测试的值大小（字节）
  pub value_sizes: Vec<usize>,
  /// Operation counts / 操作数量
  pub op_counts: Vec<u64>,
  /// Workload types / 工作负载类型
  pub workloads: Vec<WorkloadType>,
  /// Output directory / 输出目录
  pub output_dir: PathBuf,
  /// Warmup iterations / 预热迭代次数
  pub warmup: u64,
}

impl Default for BenchConfig {
  fn default() -> Self {
    Self {
      value_sizes: vec![1024, 4096, 16384, 65536, 262144, 1048576], // 1KB-1MB
      op_counts: vec![1000, 10000, 100000, 1000000],                // 1K-1M
      workloads: vec![
        WorkloadType::Sequential,
        WorkloadType::WriteHeavy,
        WorkloadType::ReadHeavy,
        WorkloadType::Balanced,
      ],
      output_dir: PathBuf::from("bench_reports"),
      warmup: 100,
    }
  }
}

impl BenchConfig {
  /// Create new config with custom settings
  /// 使用自定义设置创建新配置
  pub fn new(value_sizes: Vec<usize>, op_counts: Vec<u64>, workloads: Vec<WorkloadType>) -> Self {
    Self {
      value_sizes,
      op_counts,
      workloads,
      output_dir: PathBuf::from("bench_reports"),
      warmup: 100,
    }
  }

  /// Set output directory / 设置输出目录
  pub fn output_dir(mut self, dir: PathBuf) -> Self {
    self.output_dir = dir;
    self
  }

  /// Set warmup iterations / 设置预热迭代次数
  pub fn warmup(mut self, warmup: u64) -> Self {
    self.warmup = warmup;
    self
  }
}

/// Operation type for benchmark / 基准测试操作类型
#[derive(Debug, Clone, Copy)]
pub enum OpType {
  Put,
  Get,
}

/// Benchmark runner / 基准测试运行器
pub struct BenchRunner {
  config: BenchConfig,
  rng: fastrand::Rng,
  /// Reusable value buffer / 可复用的值缓冲区
  val_buf: Vec<u8>,
}

impl BenchRunner {
  /// Create new runner with config / 使用配置创建新运行器
  pub fn new(config: BenchConfig) -> Self {
    Self {
      config,
      rng: fastrand::Rng::new(),
      val_buf: Vec::new(),
    }
  }

  /// Create runner with seed for reproducibility
  /// 使用种子创建运行器以实现可重现性
  pub fn with_seed(config: BenchConfig, seed: u64) -> Self {
    Self {
      config,
      rng: fastrand::Rng::with_seed(seed),
      val_buf: Vec::new(),
    }
  }

  /// Get config reference / 获取配置引用
  pub fn config(&self) -> &BenchConfig {
    &self.config
  }

  /// Generate key from index / 从索引生成键
  fn gen_key(&self, idx: u64) -> Vec<u8> {
    // Optimized: Direct write to Vec<u8> avoids String allocation overhead
    // 优化：直接写入 Vec<u8> 避免 String 分配开销
    let mut buf = Vec::with_capacity(20); // "key_" (4) + 16 digits
    let _ = write!(&mut buf, "key_{idx:016}");
    buf
  }

  /// Determine operation type based on workload
  /// 根据工作负载确定操作类型
  fn next_op(&mut self, workload: WorkloadType) -> OpType {
    let read_ratio = match workload {
      WorkloadType::Sequential => 0.0,
      WorkloadType::Random => 0.5,
      WorkloadType::Zipf { .. } => 0.5,
      WorkloadType::Mixed { read_ratio } => read_ratio,
      WorkloadType::WriteHeavy => 0.1,
      WorkloadType::ReadHeavy => 0.9,
      WorkloadType::Balanced => 0.5,
    };

    if self.rng.f64() < read_ratio {
      OpType::Get
    } else {
      OpType::Put
    }
  }

  /// Run single benchmark test
  /// 运行单个基准测试
  ///
  /// # Arguments
  /// - `engine`: Storage engine adapter / 存储引擎适配器
  /// - `workload`: Workload type / 工作负载类型
  /// - `value_size`: Value size in bytes / 值大小（字节）
  /// - `op_count`: Number of operations / 操作数量
  pub async fn run_single<E: BenchEngine>(
    &mut self,
    engine: &mut E,
    workload: WorkloadType,
    value_size: usize,
    op_count: u64,
  ) -> Result<BenchMetrics> {
    // Warmup phase / 预热阶段
    for i in 0..self.config.warmup {
      let key = self.gen_key(i);
      // Reuse buffer logic inline to avoid double mutable borrow or allocation
      // 内联缓冲区复用逻辑，避免双重可变借用或分配
      if self.val_buf.len() < value_size {
        self.val_buf.resize(value_size, 0);
      }
      self.rng.fill(&mut self.val_buf[..value_size]);
      engine.put(&key, &self.val_buf[..value_size]).await?;
    }

    // Reset for actual test / 重置以进行实际测试
    let mut histogram = LatencyHistogram::new()?;
    let mut write_idx = self.config.warmup;

    let start = Instant::now();

    for _ in 0..op_count {
      let op = self.next_op(workload);
      let op_start = Instant::now();

      match op {
        OpType::Put => {
          let key = self.gen_key(write_idx);
          // Zero-copy value generation using reused buffer
          // 使用复用缓冲区的零拷贝值生成
          if self.val_buf.len() < value_size {
            self.val_buf.resize(value_size, 0);
          }
          self.rng.fill(&mut self.val_buf[..value_size]);
          engine.put(&key, &self.val_buf[..value_size]).await?;
          write_idx += 1;
        }
        OpType::Get => {
          // Read from existing keys / 从已有键读取
          if write_idx > 0 {
            let idx = self.rng.u64(0..write_idx);
            let key = self.gen_key(idx);
            let _ = engine.get(&key).await?;
          }
        }
      }

      let latency_ns = op_start.elapsed().as_nanos() as u64;
      histogram.record_saturating(latency_ns);
    }

    let duration = start.elapsed();

    // Sync and collect metrics / 同步并收集指标
    engine.sync().await?;
    let disk_bytes = engine.disk_usage();
    let memory_bytes = engine.memory_usage();

    let latency_stats = LatencyStats::from(&histogram);

    Ok(BenchMetrics::new(
      op_count,
      duration,
      latency_stats,
      disk_bytes,
      memory_bytes,
    ))
  }
}
