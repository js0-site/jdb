// Benchmark runner and configuration
// 基准测试运行器和配置

use std::{io::Write, path::PathBuf, time::Instant};

use serde::{Deserialize, Serialize};

use crate::{BenchEngine, BenchMetrics, LatencyHistogram, LatencyStats, MemBaseline, Result};

/// Workload type for benchmark / 基准测试工作负载类型
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
pub enum WorkloadType {
  #[default]
  Sequential,
  Random,
  Zipf {
    s: f64,
  },
  Mixed {
    read_ratio: f64,
  },
  WriteHeavy,
  ReadHeavy,
  Balanced,
}

/// Benchmark configuration / 基准测试配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchConfig {
  pub value_sizes: Vec<usize>,
  pub op_counts: Vec<u64>,
  pub workloads: Vec<WorkloadType>,
  pub output_dir: PathBuf,
  pub warmup: u64,
}

impl Default for BenchConfig {
  fn default() -> Self {
    Self {
      value_sizes: vec![1024, 4096, 16384, 65536, 262144, 1048576],
      op_counts: vec![1000, 10000, 100000, 1000000],
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
  pub fn new(value_sizes: Vec<usize>, op_counts: Vec<u64>, workloads: Vec<WorkloadType>) -> Self {
    Self {
      value_sizes,
      op_counts,
      workloads,
      output_dir: PathBuf::from("bench_reports"),
      warmup: 100,
    }
  }

  pub fn output_dir(mut self, dir: PathBuf) -> Self {
    self.output_dir = dir;
    self
  }

  pub fn warmup(mut self, warmup: u64) -> Self {
    self.warmup = warmup;
    self
  }
}

/// Operation type / 操作类型
#[derive(Debug, Clone, Copy)]
pub enum OpType {
  Put,
  Get,
}

/// Benchmark runner / 基准测试运行器
pub struct BenchRunner {
  config: BenchConfig,
  rng: fastrand::Rng,
  val_buf: Vec<u8>,
}

impl BenchRunner {
  pub fn new(config: BenchConfig) -> Self {
    Self {
      config,
      rng: fastrand::Rng::new(),
      val_buf: Vec::new(),
    }
  }

  pub fn with_seed(config: BenchConfig, seed: u64) -> Self {
    Self {
      config,
      rng: fastrand::Rng::with_seed(seed),
      val_buf: Vec::new(),
    }
  }

  pub fn config(&self) -> &BenchConfig {
    &self.config
  }

  fn gen_key(&self, idx: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(20);
    let _ = write!(&mut buf, "key_{idx:016}");
    buf
  }

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
  /// `mem_baseline`: Memory baseline recorded after data preload, before db open
  /// `mem_baseline`: 数据预加载后、数据库打开前记录的内存基准点
  pub async fn run_single<E: BenchEngine>(
    &mut self,
    engine: &mut E,
    workload: WorkloadType,
    value_size: usize,
    op_count: u64,
    mem_baseline: &MemBaseline,
  ) -> Result<BenchMetrics> {
    // Warmup / 预热
    for i in 0..self.config.warmup {
      let key = self.gen_key(i);
      if self.val_buf.len() < value_size {
        self.val_buf.resize(value_size, 0);
      }
      self.rng.fill(&mut self.val_buf[..value_size]);
      engine.put(&key, &self.val_buf[..value_size]).await?;
    }

    let mut histogram = LatencyHistogram::new()?;
    let mut write_idx = self.config.warmup;
    let start = Instant::now();

    for _ in 0..op_count {
      let op = self.next_op(workload);
      let op_start = Instant::now();

      match op {
        OpType::Put => {
          let key = self.gen_key(write_idx);
          if self.val_buf.len() < value_size {
            self.val_buf.resize(value_size, 0);
          }
          self.rng.fill(&mut self.val_buf[..value_size]);
          engine.put(&key, &self.val_buf[..value_size]).await?;
          write_idx += 1;
        }
        OpType::Get => {
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
    engine.sync().await?;

    let disk_bytes = engine.disk_usage();
    let memory_bytes = mem_baseline.db_mem().saturating_sub(engine.sim_mem());
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
