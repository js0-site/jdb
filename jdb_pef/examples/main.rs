use jdb_pef::Pef;
// use rand::Rng; // (Removed: Zero dependency)
// Verify if we can add rand or just use a simple LCG properly.
// The user previously provided code using ONLY std.
// So we implement a simple LCG here.

struct SimpleLcg {
  state: u64,
}

impl SimpleLcg {
  fn new(seed: u64) -> Self {
    Self { state: seed }
  }

  fn next(&mut self) -> u64 {
    // A simple LCG parameters (PCG is better but we keep it minimal)
    self.state = self.state.wrapping_mul(6364136223846793005).wrapping_add(1);
    self.state
  }

  fn range(&mut self, min: u64, max: u64) -> u64 {
    let r = self.next();
    min + (r % (max - min))
  }
}

fn main() {
  let num_elements = 1_000_000;
  println!(
    "
Generating 1,000,000 simulated disk key offsets...
正在生成 1,000,000 个模拟磁盘键偏移量...
"
  );

  let mut data = Vec::with_capacity(num_elements);
  let mut current_offset = 0;
  // Simulate initial offset (e.g., file header)
  // 模拟初始偏移量（例如文件头）
  current_offset += 4096;

  let mut rng = SimpleLcg::new(12345);

  // Realistic scenario: Keys/Values stored sequentially.
  // Lengths varying between small keys (e.g. 30 bytes) to larger values (e.g. 500 bytes).
  // Let's assume average 100 bytes.
  // 现实场景：键/值顺序存储。
  // 长度在小键（例如 30 字节）到大值（例如 500 字节）之间变化。
  // 假设平均 100 字节。
  for _ in 0..num_elements {
    data.push(current_offset);
    // Next offset = current + entry_size
    // Entry size random between 32 and 256 bytes
    let entry_size = rng.range(32, 256);
    current_offset += entry_size;
  }

  println!("Max offset: {}", current_offset);

  // Benchmark Construction
  // 基准测试：构建
  let start = std::time::Instant::now();
  let pef = Pef::new_with_conf(
    &data,
    jdb_pef::conf::Conf {
      block_size: 128,
      ..Default::default()
    },
  );
  let duration = start.elapsed();

  let compressed_bytes = pef.memory_usage();
  let raw_bytes = num_elements * 8;
  let bpe = (compressed_bytes as f64 * 8.0) / num_elements as f64;
  let ratio = (compressed_bytes as f64 / raw_bytes as f64) * 100.0;

  println!(
    "
--------------------------------------------------
Benchmark Results / 基准测试结果
--------------------------------------------------
"
  );
  println!("Construction Time / 构建时间: {:.2?}", duration);
  println!("Elements / 元素数量:          {}", num_elements);
  println!(
    "Raw Size / 原始大小:          {:.2} MB",
    raw_bytes as f64 / 1024.0 / 1024.0
  );
  println!(
    "PEF Size / PEF 大小:          {:.2} MB",
    compressed_bytes as f64 / 1024.0 / 1024.0
  );
  println!("Bits/Elem / 每元素比特数:     {:.2}", bpe);
  println!(
    "Ratio / 压缩率:               {:.2}% (Compressed/Raw)",
    ratio
  );
  println!("--------------------------------------------------");

  println!(
    "
Verifying random access...
正在验证随机访问...
"
  );
  for _ in 0..1000 {
    let idx = rng.range(0, num_elements as u64) as usize;
    assert_eq!(pef.get(idx).unwrap(), data[idx]);
  }
  println!("Verification passed. / 验证通过。");
}
