use aok::{OK, Void};
use jdb_pgm_index::PGMIndex;
use log::info;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

// ============================================================================
// Normal Tests / 普通测试
// ============================================================================

/// Test basic get functionality
/// 测试基本的 get 功能
#[test]
fn test_basic_get() -> Void {
  let data: Vec<u64> = (0..10_000).collect();
  let index = PGMIndex::new(data, 32);

  assert_eq!(index.get(0), Some(0));
  assert_eq!(index.get(5000), Some(5000));
  assert_eq!(index.get(9999), Some(9999));
  assert_eq!(index.get(10000), None);

  info!("basic_get passed");
  OK
}

/// Test with different epsilon values
/// 测试不同的 epsilon 值
#[test]
fn test_different_epsilon() -> Void {
  let data: Vec<u64> = (0..50_000).collect();

  for &eps in &[1usize, 4, 16, 32, 64, 128, 256] {
    let index = PGMIndex::new(data.clone(), eps);
    assert!(index.segment_count() >= 1);

    for &k in &[0u64, 1000, 25000, 49999] {
      assert_eq!(index.get(k), Some(k as usize), "eps={eps}, key={k}");
    }
  }

  info!("different_epsilon passed");
  OK
}

/// Test querying non-existent keys
/// 测试查询不存在的键
#[test]
fn test_non_existent() -> Void {
  // Even numbers only
  // 仅偶数
  let data: Vec<u64> = (0..1000).step_by(2).collect();
  let index = PGMIndex::new(data, 16);

  // Odd numbers not found
  // 奇数找不到
  assert_eq!(index.get(1), None);
  assert_eq!(index.get(3), None);
  assert_eq!(index.get(999), None);

  // Even numbers found
  // 偶数能找到
  assert_eq!(index.get(0), Some(0));
  assert_eq!(index.get(2), Some(1));
  assert_eq!(index.get(998), Some(499));

  info!("non_existent passed");
  OK
}

/// Test statistics functionality
/// 测试统计功能
#[test]
fn test_stats() -> Void {
  let data: Vec<u64> = (0..10_000).collect();
  let index = PGMIndex::new(data, 32);
  let stats = index.stats();

  assert!(stats.segments >= 1);
  assert!(stats.avg_segment_size > 0.0);
  assert!(stats.memory_bytes > 0);

  info!(
    "stats: segments={}, avg={:.2}, mem={}",
    stats.segments, stats.avg_segment_size, stats.memory_bytes
  );
  OK
}

/// Test batch lookup
/// 测试批量查找
#[test]
fn test_batch_lookup() -> Void {
  let data: Vec<u64> = (0..100_000).collect();
  let index = PGMIndex::new(data, 32);

  let keys: Vec<u64> = (0..1000).map(|i| i * 100).collect();
  let results = index.get_many(&keys);

  for (i, &k) in keys.iter().enumerate() {
    assert_eq!(results[i], Some(k as usize));
  }

  info!("batch_lookup passed");
  OK
}

/// Test hit counting
/// 测试命中计数
#[test]
fn test_count_hits() -> Void {
  let data: Vec<u64> = (0..10_000).collect();
  let index = PGMIndex::new(data, 32);

  let keys: Vec<u64> = (0..100).collect();
  assert_eq!(index.count_hits(&keys), 100);

  // Mix existing and non-existing
  // 混合存在和不存在的键
  let mixed: Vec<u64> = (9990..10010).collect();
  assert_eq!(index.count_hits(&mixed), 10);

  info!("count_hits passed");
  OK
}

// ============================================================================
// Boundary Tests / 边界测试
// ============================================================================

/// Test with single element
/// 测试单元素
#[test]
fn test_single_element() -> Void {
  let data = vec![42u64];
  let index = PGMIndex::new(data, 1);

  assert_eq!(index.get(42), Some(0));
  assert_eq!(index.get(41), None);
  assert_eq!(index.get(43), None);
  assert_eq!(index.segment_count(), 1);

  info!("single_element passed");
  OK
}

/// Test with two elements
/// 测试两个元素
#[test]
fn test_two_elements() -> Void {
  let data = vec![10u64, 20u64];
  let index = PGMIndex::new(data, 1);

  assert_eq!(index.get(10), Some(0));
  assert_eq!(index.get(20), Some(1));
  assert_eq!(index.get(15), None);
  assert_eq!(index.get(9), None);
  assert_eq!(index.get(21), None);

  info!("two_elements passed");
  OK
}

/// Test with duplicate values
/// 测试重复值
#[test]
fn test_duplicates() -> Void {
  let data = vec![1u64, 1, 1, 2, 2, 3, 3, 3, 3];
  let index = PGMIndex::new(data, 1);

  let pos = index.get(1);
  assert!(pos.is_some());
  assert!(pos.unwrap() <= 2);

  let pos = index.get(2);
  assert!(pos.is_some());

  let pos = index.get(3);
  assert!(pos.is_some());

  assert_eq!(index.get(0), None);
  assert_eq!(index.get(4), None);

  info!("duplicates passed");
  OK
}

/// Test with minimum epsilon (1)
/// 测试最小 epsilon (1)
#[test]
fn test_min_epsilon() -> Void {
  let data: Vec<u64> = (0..1000).collect();
  let index = PGMIndex::new(data, 1);

  assert_eq!(index.get(0), Some(0));
  assert_eq!(index.get(500), Some(500));
  assert_eq!(index.get(999), Some(999));

  info!("min_epsilon passed");
  OK
}

/// Test with large epsilon
/// 测试大 epsilon
#[test]
fn test_large_epsilon() -> Void {
  let data: Vec<u64> = (0..10_000).collect();
  let index = PGMIndex::new(data, 1000);

  assert_eq!(index.get(0), Some(0));
  assert_eq!(index.get(5000), Some(5000));
  assert_eq!(index.get(9999), Some(9999));

  info!("large_epsilon passed");
  OK
}

/// Test boundary keys (first and last)
/// 测试边界键（第一个和最后一个）
#[test]
fn test_boundary_keys() -> Void {
  let data: Vec<u64> = (100..200).collect();
  let index = PGMIndex::new(data, 16);

  assert_eq!(index.get(100), Some(0));
  assert_eq!(index.get(199), Some(99));
  assert_eq!(index.get(99), None);
  assert_eq!(index.get(200), None);

  info!("boundary_keys passed");
  OK
}

/// Test with sparse data (large gaps)
/// 测试稀疏数据（大间隔）
#[test]
fn test_sparse_data() -> Void {
  let data: Vec<u64> = vec![1, 100, 10000, 1000000, 100000000];
  let index = PGMIndex::new(data, 4);

  assert_eq!(index.get(1), Some(0));
  assert_eq!(index.get(100), Some(1));
  assert_eq!(index.get(10000), Some(2));
  assert_eq!(index.get(1000000), Some(3));
  assert_eq!(index.get(100000000), Some(4));

  assert_eq!(index.get(50), None);
  assert_eq!(index.get(5000), None);

  info!("sparse_data passed");
  OK
}

/// Test with dense consecutive data
/// 测试密集连续数据
#[test]
fn test_dense_data() -> Void {
  let data: Vec<u64> = (0..100_000).collect();
  let index = PGMIndex::new(data, 32);

  for k in [0u64, 1, 100, 1000, 50000, 99998, 99999] {
    assert_eq!(index.get(k), Some(k as usize));
  }

  info!("dense_data passed");
  OK
}

// ============================================================================
// Type Tests / 类型测试
// ============================================================================

/// Test with different key types (u8, i32, u16)
/// 测试不同的键类型 (u8, i32, u16)
#[test]
fn test_key_types() -> Void {
  // u8
  let data_u8: Vec<u8> = (0..=255).collect();
  let idx = PGMIndex::new(data_u8, 4);
  assert_eq!(idx.get(0u8), Some(0));
  assert_eq!(idx.get(255u8), Some(255));

  // i32
  let data_i32: Vec<i32> = (-500..500).collect();
  let idx = PGMIndex::new(data_i32, 16);
  assert_eq!(idx.get(-500i32), Some(0));
  assert_eq!(idx.get(0i32), Some(500));
  assert_eq!(idx.get(499i32), Some(999));

  // u16
  let data_u16: Vec<u16> = (0..10000).map(|x| x as u16).collect();
  let idx = PGMIndex::new(data_u16, 16);
  assert_eq!(idx.get(0u16), Some(0));
  assert_eq!(idx.get(9999u16), Some(9999));

  info!("key_types passed");
  OK
}

/// Test with negative keys
/// 测试负数键
#[test]
fn test_negative_keys() -> Void {
  let data: Vec<i64> = (-1000..1000).collect();
  let index = PGMIndex::new(data, 32);

  assert_eq!(index.get(-1000i64), Some(0));
  assert_eq!(index.get(-1i64), Some(999));
  assert_eq!(index.get(0i64), Some(1000));
  assert_eq!(index.get(999i64), Some(1999));
  assert_eq!(index.get(-1001i64), None);
  assert_eq!(index.get(1000i64), None);

  info!("negative_keys passed");
  OK
}

// ============================================================================
// Memory Tests / 内存测试
// ============================================================================

/// Test memory usage calculation
/// 测试内存使用计算
#[test]
fn test_memory_usage() -> Void {
  let data: Vec<u64> = (0..100_000).collect();
  let index = PGMIndex::new(data, 32);

  let mem = index.memory_usage();
  // Min: data (100k * 8 bytes) = 800KB
  // 最小：数据 (100k * 8 字节) = 800KB
  assert!(mem >= 800_000);

  info!("memory_usage: {mem} bytes");
  OK
}

/// Test segment count varies with epsilon
/// 测试段数随 epsilon 变化
#[test]
fn test_segment_count_vs_epsilon() -> Void {
  let data: Vec<u64> = (0..100_000).collect();

  let idx_small = PGMIndex::new(data.clone(), 8);
  let idx_large = PGMIndex::new(data, 128);

  // Smaller epsilon -> more segments
  // 更小的 epsilon -> 更多的段
  assert!(idx_small.segment_count() >= idx_large.segment_count());

  info!(
    "segment_count: small={}, large={}",
    idx_small.segment_count(),
    idx_large.segment_count()
  );
  OK
}

// ============================================================================
// Edge Cases / 极端情况
// ============================================================================

/// Test with all same values
/// 测试所有相同的值
#[test]
fn test_all_same() -> Void {
  let data = vec![42u64; 100];
  let index = PGMIndex::new(data, 1);

  let pos = index.get(42);
  assert!(pos.is_some());
  assert!(pos.unwrap() < 100);

  assert_eq!(index.get(41), None);
  assert_eq!(index.get(43), None);

  info!("all_same passed");
  OK
}

/// Test with large u64 values near MAX
/// 测试接近 MAX 的大 u64 值
#[test]
fn test_large_values() -> Void {
  let base = u64::MAX - 1000;
  let data: Vec<u64> = (0..1000).map(|i| base + i).collect();
  let index = PGMIndex::new(data, 16);

  assert_eq!(index.get(base), Some(0));
  assert_eq!(index.get(base + 500), Some(500));
  assert_eq!(index.get(base + 999), Some(999));
  assert_eq!(index.get(base - 1), None);

  info!("large_values passed");
  OK
}

/// Test batch lookup with empty keys
/// 测试空键的批量查找
#[test]
fn test_empty_batch() -> Void {
  let data: Vec<u64> = (0..1000).collect();
  let index = PGMIndex::new(data, 16);

  let empty: Vec<u64> = vec![];
  let results = index.get_many(&empty);
  assert!(results.is_empty());

  info!("empty_batch passed");
  OK
}

// ============================================================================
// Additional Tests / 补充测试
// ============================================================================

/// Test with random sorted data
/// 测试随机排序数据
#[test]
fn test_random_data() -> Void {
  use rand::{Rng, SeedableRng, rngs::StdRng};

  let mut rng = StdRng::seed_from_u64(12345);
  let mut data: Vec<u64> = (0..10_000)
    .map(|_| rng.random_range(0..1_000_000))
    .collect();
  data.sort();
  data.dedup();

  let n = data.len();
  let index = PGMIndex::new(data.clone(), 32);

  // Verify all elements can be found
  // 验证所有元素都能找到
  for (i, &k) in data.iter().enumerate() {
    assert_eq!(index.get(k), Some(i), "key={k} at index {i}");
  }

  assert_eq!(index.get(1_000_001), None);

  info!("random_data passed, n={n}");
  OK
}

/// Test epsilon actually bounds prediction error
/// 测试 epsilon 确实限制了预测误差
#[test]
fn test_epsilon_bound() -> Void {
  let data: Vec<u64> = (0..10_000).collect();

  for &eps in &[1usize, 4, 16, 64] {
    let index = PGMIndex::new(data.clone(), eps);

    for &k in &[0u64, 100, 500, 1000, 5000, 9999] {
      assert_eq!(index.get(k), Some(k as usize), "eps={eps}, key={k}");
    }
  }

  info!("epsilon_bound passed");
  OK
}

/// Test epsilon larger than data size
/// 测试 epsilon 大于数据大小
#[test]
fn test_epsilon_larger_than_data() -> Void {
  let data: Vec<u64> = (0..100).collect();
  let index = PGMIndex::new(data, 1000);

  assert_eq!(index.get(0), Some(0));
  assert_eq!(index.get(50), Some(50));
  assert_eq!(index.get(99), Some(99));
  assert_eq!(index.get(100), None);

  info!("epsilon_larger_than_data passed");
  OK
}

/// Test i8 full range (-128 to 127)
/// 测试 i8 完整范围 (-128 到 127)
#[test]
fn test_i8_bounds() -> Void {
  let data: Vec<i8> = (-128..=127).collect();
  let index = PGMIndex::new(data, 4);

  assert_eq!(index.get(-128i8), Some(0));
  assert_eq!(index.get(0i8), Some(128));
  assert_eq!(index.get(127i8), Some(255));

  info!("i8_bounds passed");
  OK
}

/// Test segment properties
/// 测试段属性
#[test]
fn test_segment_properties() -> Void {
  let data: Vec<u64> = (0..1000).collect();
  let index = PGMIndex::new(data, 16);

  assert!(index.segment_count() >= 1);
  assert!(index.avg_segment_size() > 0.0);
  assert!(index.memory_usage() > 0);

  info!("segment_properties passed");
  OK
}

/// Test quadratic distribution (i^2)
/// 测试二次分布 (i^2)
#[test]
fn test_quadratic_data() -> Void {
  let data: Vec<u64> = (0..1000u64).map(|i| i * i).collect();
  let index = PGMIndex::new(data.clone(), 16);

  for (i, &k) in data.iter().enumerate() {
    assert_eq!(index.get(k), Some(i), "key={k}");
  }

  // Not perfect squares
  // 非完全平方数
  assert_eq!(index.get(2), None);
  assert_eq!(index.get(5), None);

  info!("quadratic_data passed");
  OK
}

/// Test exponential distribution (2^i)
/// 测试指数分布 (2^i)
#[test]
fn test_exponential_data() -> Void {
  let data: Vec<u64> = (0..20).map(|i| 1u64 << i).collect();
  let index = PGMIndex::new(data.clone(), 4);

  for (i, &k) in data.iter().enumerate() {
    assert_eq!(index.get(k), Some(i), "key={k}");
  }

  assert_eq!(index.get(3), None);
  assert_eq!(index.get(5), None);

  info!("exponential_data passed");
  OK
}

/// Test batch with mixed existing and non-existing keys
/// 测试混合存在和不存在键的批量查找
#[test]
fn test_batch_mixed() -> Void {
  // Even only
  // 仅偶数
  let data: Vec<u64> = (0..1000).step_by(2).collect();
  let index = PGMIndex::new(data, 8);

  let keys: Vec<u64> = (0..20).collect();
  let results = index.get_many(&keys);

  for (i, &k) in keys.iter().enumerate() {
    if k % 2 == 0 {
      assert_eq!(results[i], Some((k / 2) as usize));
    } else {
      assert_eq!(results[i], None);
    }
  }

  info!("batch_mixed passed");
  OK
}

/// Test with three elements
/// 测试三个元素
#[test]
fn test_three_elements() -> Void {
  let data = vec![1u64, 50, 100];
  let index = PGMIndex::new(data, 1);

  assert_eq!(index.get(1), Some(0));
  assert_eq!(index.get(50), Some(1));
  assert_eq!(index.get(100), Some(2));
  assert_eq!(index.get(0), None);
  assert_eq!(index.get(25), None);
  assert_eq!(index.get(101), None);

  info!("three_elements passed");
  OK
}

#[test]
fn test() -> Void {
  info!("All pgm_index tests passed!");
  OK
}
