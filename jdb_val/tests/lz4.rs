//! LZ4 compression property tests / LZ4 压缩属性测试
//!
//! **Feature: lz4-compression**
//!
//! Tests validate correctness properties from design.md

use jdb_val::wal::lz4;
use proptest::prelude::*;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

/// Property 1: Compression round-trip consistency / 压缩往返一致性
///
/// *For any* valid data, if try_compress returns compressed data,
/// then decompress(compressed, original_len) should produce the same result as original.
///
/// **Validates: Requirements 6.1**
mod prop_round_trip {
  use super::*;

  fn prop_compress_decompress_round_trip(data: Vec<u8>) {
    let mut compress_buf = Vec::new();
    let mut decompress_buf = Vec::new();

    if let Some(compressed_len) = lz4::try_compress(&data, &mut compress_buf) {
      // Compression succeeded, verify round-trip / 压缩成功，验证往返
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

    /// **Feature: lz4-compression, Property 1: Compression round-trip**
    /// **Validates: Requirements 6.1**
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

/// Property 2: Small data skip compression / 小数据跳过压缩
///
/// *For any* data with size < 1KB, try_compress should return None.
///
/// **Validates: Requirements 1.1, 2.1**
mod prop_small_skip {
  use super::*;

  fn prop_small_data_skip(data: Vec<u8>) {
    let mut buf = Vec::new();
    let result = lz4::try_compress(&data, &mut buf);
    assert!(
      result.is_none(),
      "data < 1KB should skip compression, got Some({:?})",
      result
    );
  }

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// **Feature: lz4-compression, Property 2: Small data skip**
    /// **Validates: Requirements 1.1, 2.1**
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

/// Property 3: Medium data compression validity / 中等数据压缩有效性
///
/// *For any* data with size between 1KB and 16KB, if try_compress returns Some(compressed_len),
/// then compressed_len should be less than original data length.
///
/// **Validates: Requirements 1.2, 2.2**
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

    /// **Feature: lz4-compression, Property 3: Medium data compression validity**
    /// **Validates: Requirements 1.2, 2.2**
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

/// Property 4: 3x3 storage mode round-trip / 3x3 存储模式往返
///
/// *For any* key and val size combination (covering 9 combinations of INLINE/INFILE/FILE),
/// put followed by head_key and head_val should return original data.
///
/// **Validates: Requirements 5.1, 5.2, 5.3, 5.4, 3.1, 3.2**
mod prop_3x3_round_trip {
  use jdb_val::{INFILE_MAX, Wal};
  use proptest::prelude::*;

  /// Size category for storage mode / 存储模式的大小类别
  #[derive(Debug, Clone, Copy)]
  enum SizeCategory {
    /// INLINE: small data / 小数据
    Inline,
    /// INFILE: medium data (may compress) / 中等数据（可能压缩）
    Infile,
    /// FILE: large data (may compress) / 大数据（可能压缩）
    File,
  }

  /// Generate data for given category / 根据类别生成数据
  fn gen_data(cat: SizeCategory, fill: u8) -> Vec<u8> {
    match cat {
      // INLINE: <= 30B for key, <= 50B for val / 内联：key <= 30B, val <= 50B
      SizeCategory::Inline => vec![fill; 10],
      // INFILE: 31B ~ 1MB, use 2KB to trigger compression / 中等：31B ~ 1MB，用 2KB 触发压缩
      SizeCategory::Infile => vec![fill; 2048],
      // FILE: > 1MB, use 1MB + 1KB / 大：> 1MB，用 1MB + 1KB
      SizeCategory::File => vec![fill; INFILE_MAX + 1024],
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

      // Write / 写入
      let loc = wal.put(&key, &val).await.unwrap();
      wal.sync_all().await.unwrap();

      // Read and verify / 读取并验证
      let head = wal.read_head(loc).await.unwrap();
      let got_key = wal.head_key(&head).await.unwrap();
      let got_val = wal.head_val(&head).await.unwrap();

      assert_eq!(
        got_key, key,
        "key mismatch: key_cat={key_cat:?}, val_cat={val_cat:?}"
      );
      assert_eq!(
        got_val, val,
        "val mismatch: key_cat={key_cat:?}, val_cat={val_cat:?}"
      );

      // Verify storage mode / 验证存储模式
      match key_cat {
        SizeCategory::Inline => assert!(
          head.key_flag.is_inline() || head.key_flag.is_tombstone(),
          "key should be inline"
        ),
        SizeCategory::Infile => assert!(head.key_flag.is_infile(), "key should be infile"),
        SizeCategory::File => assert!(head.key_flag.is_file(), "key should be file"),
      }

      // Val mode depends on key mode for inline / val 模式取决于 key 模式（内联情况）
      match val_cat {
        SizeCategory::Inline => {
          // Val inline only when key is also inline / val 内联仅当 key 也内联
          if matches!(key_cat, SizeCategory::Inline) {
            assert!(head.val_flag.is_inline(), "val should be inline");
          }
        }
        SizeCategory::Infile => assert!(head.val_flag.is_infile(), "val should be infile"),
        SizeCategory::File => assert!(head.val_flag.is_file(), "val should be file"),
      }
    });
  }

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// **Feature: lz4-compression, Property 4: 3x3 storage mode round-trip**
    /// **Validates: Requirements 5.1, 5.2, 5.3, 5.4, 3.1, 3.2**
    #[test]
    fn test_prop_3x3_round_trip(
      // Generate category indices (0=Inline, 1=Infile, 2=File)
      // 生成类别索引
      key_cat_idx in 0u8..3,
      val_cat_idx in 0u8..3,
      // Fill bytes for data / 数据填充字节
      key_fill in any::<u8>(),
      val_fill in any::<u8>()
    ) {
      let key_cat = match key_cat_idx {
        0 => SizeCategory::Inline,
        1 => SizeCategory::Infile,
        _ => SizeCategory::File,
      };
      let val_cat = match val_cat_idx {
        0 => SizeCategory::Inline,
        1 => SizeCategory::Infile,
        _ => SizeCategory::File,
      };
      prop_3x3_round_trip(key_cat, val_cat, key_fill, val_fill);
    }
  }
}
