//! LZ4 compression property tests
//! LZ4 压缩属性测试

use jdb_val::wal::lz4;
use proptest::prelude::*;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

/// Property 1: Compression round-trip consistency
/// 压缩往返一致性
mod prop_round_trip {
  use super::*;

  fn prop_compress_decompress_round_trip(data: Vec<u8>) {
    let mut compress_buf = Vec::new();
    let mut decompress_buf = Vec::new();

    if let Some(compressed_len) = lz4::try_compress(&data, &mut compress_buf) {
      // Compression succeeded, verify round-trip
      // 压缩成功，验证往返
      assert!(compressed_len <= compress_buf.len());
      let compressed = &compress_buf[..compressed_len];

      lz4::decompress(compressed, data.len(), &mut decompress_buf)
        .expect("decompress should succeed");

      assert_eq!(
        decompress_buf, data,
        "decompressed data should equal original"
      );
    }
    // If try_compress returns None, no round-trip to verify
    // 如果 try_compress 返回 None，无需验证往返
  }

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn test_prop_round_trip(
      // Generate data >= 1KB to ensure compression is attempted
      // 生成 >= 1KB 的数据以确保尝试压缩
      data in prop::collection::vec(any::<u8>(), 1024..32768)
    ) {
      prop_compress_decompress_round_trip(data);
    }
  }
}

/// Property 2: Small data skip compression
/// 小数据跳过压缩
mod prop_small_skip {
  use super::*;

  fn prop_small_data_skip(data: Vec<u8>) {
    let mut buf = Vec::new();
    let result = lz4::try_compress(&data, &mut buf);
    assert!(
      result.is_none(),
      "data < 1KB should skip compression, got Some({result:?})"
    );
  }

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn test_prop_small_skip(
      // Generate data < 1KB (MIN_COMPRESS_SIZE)
      // 生成 < 1KB 的数据
      data in prop::collection::vec(any::<u8>(), 0..1024)
    ) {
      prop_small_data_skip(data);
    }
  }
}

/// Property 3: Medium data compression validity
/// 中等数据压缩有效性
mod prop_medium_valid {
  use super::*;

  fn prop_medium_compression_smaller(data: Vec<u8>) {
    let original_len = data.len();
    let mut buf = Vec::new();

    if let Some(compressed_len) = lz4::try_compress(&data, &mut buf) {
      assert!(
        compressed_len < original_len,
        "compressed_len ({compressed_len}) should be < original_len ({original_len})"
      );
    }
    // If None, compression was skipped (not beneficial) - that's valid
    // 如果返回 None，压缩被跳过（无益）- 这是有效的
  }

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn test_prop_medium_valid(
      // Generate data between 1KB and 16KB
      // 生成 1KB 到 16KB 之间的数据
      data in prop::collection::vec(any::<u8>(), 1024..16384)
    ) {
      prop_medium_compression_smaller(data);
    }
  }
}

/// Property 4: 3x3 storage mode round-trip
/// 3x3 存储模式往返
mod prop_3x3_round_trip {
  use jdb_val::{INFILE_MAX, Wal};
  use proptest::prelude::*;

  /// Size category for storage mode
  /// 存储模式的大小类别
  #[derive(Debug, Clone, Copy)]
  enum SizeCategory {
    /// Small data (infile)
    /// 小数据（同文件）
    Small,
    /// Medium data (infile, may compress)
    /// 中等数据（同文件，可能压缩）
    Medium,
    /// Large data (file)
    /// 大数据（文件）
    Large,
  }

  /// Generate data for given category
  /// 根据类别生成数据
  fn gen_data(cat: SizeCategory, fill: u8) -> Vec<u8> {
    match cat {
      // Small: <= 100B
      // 小：<= 100B
      SizeCategory::Small => vec![fill; 10],
      // Medium: 2KB to trigger compression
      // 中等：2KB 触发压缩
      SizeCategory::Medium => vec![fill; 2048],
      // Large: > 1MB
      // 大：> 1MB
      SizeCategory::Large => vec![fill; INFILE_MAX + 1024],
    }
  }

  /// Property test: 3x3 storage mode round-trip with compression
  /// 属性测试：带压缩的 3x3 存储模式往返
  fn prop_3x3_round_trip(key_cat: SizeCategory, val_cat: SizeCategory, key_fill: u8, val_fill: u8) {
    compio::runtime::Runtime::new().unwrap().block_on(async {
      let dir = tempfile::tempdir().unwrap();
      let mut wal = Wal::new(dir.path(), &[]);
      wal.open().await.unwrap();

      let key = gen_data(key_cat, key_fill);
      let val = gen_data(val_cat, val_fill);

      // Write
      // 写入
      let loc = wal.put(&key, &val).await.unwrap();
      // Must flush before read_head_data
      // 读取 head_data 前必须刷新
      wal.flush().await.unwrap();

      // Read and verify
      // 读取并验证
      let head = wal.read_head(loc).await.unwrap();
      let head_data = wal.read_head_data(loc, &head).await.unwrap();
      let got_key = wal.head_key(&head, &head_data).await.unwrap();
      let got_val = wal.head_val(&head, &head_data).await.unwrap();

      assert_eq!(
        got_key, key,
        "key mismatch: key_cat={key_cat:?}, val_cat={val_cat:?}"
      );
      assert_eq!(
        got_val, val,
        "val mismatch: key_cat={key_cat:?}, val_cat={val_cat:?}"
      );

      // Verify storage mode
      // 验证存储模式
      match key_cat {
        SizeCategory::Small | SizeCategory::Medium => {
          assert!(head.key_store().is_infile(), "key should be infile");
        }
        SizeCategory::Large => {
          assert!(head.key_store().is_file(), "key should be file");
        }
      }

      match val_cat {
        SizeCategory::Small | SizeCategory::Medium => {
          assert!(head.val_store().is_infile(), "val should be infile");
        }
        SizeCategory::Large => {
          assert!(head.val_store().is_file(), "val should be file");
        }
      }
    });
  }

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn test_prop_3x3_round_trip(
      // Generate category indices (0=Small, 1=Medium, 2=Large)
      // 生成类别索引
      key_cat_idx in 0u8..3,
      val_cat_idx in 0u8..3,
      // Fill bytes for data
      // 数据填充字节
      key_fill in any::<u8>(),
      val_fill in any::<u8>()
    ) {
      let key_cat = match key_cat_idx {
        0 => SizeCategory::Small,
        1 => SizeCategory::Medium,
        _ => SizeCategory::Large,
      };
      let val_cat = match val_cat_idx {
        0 => SizeCategory::Small,
        1 => SizeCategory::Medium,
        _ => SizeCategory::Large,
      };
      prop_3x3_round_trip(key_cat, val_cat, key_fill, val_fill);
    }
  }
}
