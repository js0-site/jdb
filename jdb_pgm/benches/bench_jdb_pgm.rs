//! jdb_pgm benchmark
//! jdb_pgm 评测

use jdb_pgm::{Pgm, bench_common::Benchmarkable};

pub struct JdbPgm {
  pgm: Pgm<u64>,
}

impl Benchmarkable for JdbPgm {
  const NAME: &'static str = "jdb_pgm";

  fn build(data: &[u64], epsilon: Option<usize>) -> Self {
    let pgm = Pgm::new(data, epsilon.unwrap_or(64), false).unwrap();
    Self { pgm }
  }

  fn query(&self, data: &[u64], key: u64) -> Option<usize> {
    let (lo, hi) = self.pgm.predict_range(key);
    data[lo..hi.min(data.len())]
      .binary_search(&key)
      .ok()
      .map(|p| lo + p)
  }

  fn uses_epsilon() -> bool {
    true
  }
}
