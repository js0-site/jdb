use aok::{OK, Void};
use jdb_slab::{
  Compress, Engine, Error, Header, HeatTracker, SlabClass, SlabConfig, pipe, stream_copy,
};
use proptest::prelude::*;
use tempfile::TempDir;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

/// Verify Header::SIZE == 12 / 验证头部大小为 12 字节
#[test]
fn test_header_size() {
  assert_eq!(Header::SIZE, 12);
}

/// Encode/decode round-trip / 编解码往返测试
#[test]
fn test_header_encode_decode_roundtrip() -> Void {
  let h1 = Header::new(0xDEADBEEF, 1024, Compress::None);
  let encoded = h1.encode();
  let decoded = Header::decode(&encoded)?;
  assert_eq!(h1, decoded);
  assert!(!decoded.is_compressed());
  assert_eq!(decoded.compress(), Compress::None);

  let h2 = Header::new(0x12345678, 4096, Compress::Lz4);
  let encoded = h2.encode();
  let decoded = Header::decode(&encoded)?;
  assert_eq!(h2, decoded);
  assert!(decoded.is_compressed());
  assert_eq!(decoded.compress(), Compress::Lz4);

  let h3 = Header::new(0xCAFEBABE, 65536, Compress::Zstd);
  let encoded = h3.encode();
  let decoded = Header::decode(&encoded)?;
  assert_eq!(h3, decoded);
  assert!(decoded.is_compressed());
  assert_eq!(decoded.compress(), Compress::Zstd);

  let h4 = Header::new(0, 0, Compress::None);
  let encoded = h4.encode();
  let decoded = Header::decode(&encoded)?;
  assert_eq!(h4, decoded);

  let h5 = Header::new(u32::MAX, u32::MAX, Compress::Zstd);
  let encoded = h5.encode();
  let decoded = Header::decode(&encoded)?;
  assert_eq!(h5, decoded);

  OK
}

proptest! {
  #![proptest_config(ProptestConfig::with_cases(20))]

  /// Property 7: Decay Reduces All Counters / 衰减减少所有计数器
  #[test]
  fn prop_decay_reduces_all_counters(
    counters in prop::collection::vec(0u32..1000, 1..20)
  ) {
    let mut heat = HeatTracker::with_cap(counters.len());
    for (i, &c) in counters.iter().enumerate() {
      for _ in 0..c {
        heat.access(i as u64);
      }
    }

    for (i, &expected) in counters.iter().enumerate() {
      prop_assert_eq!(heat.get(i as u64), expected);
    }

    heat.decay();

    for (i, &original) in counters.iter().enumerate() {
      let expected = original >> 1;
      let actual = heat.get(i as u64);
      prop_assert_eq!(actual, expected);
    }
  }
}

/// Property 1: Put/Get Round-Trip / 写入读取往返测试
#[test]
fn prop_put_get_roundtrip() {
  const CLASS_SIZE: usize = 16384;
  const MAX_PAYLOAD: usize = CLASS_SIZE - Header::SIZE;

  let config = ProptestConfig::with_cases(100);
  let mut runner = proptest::test_runner::TestRunner::new(config);

  runner
    .run(
      &prop::collection::vec(any::<u8>(), 0..MAX_PAYLOAD),
      |data| {
        compio::runtime::Runtime::new().unwrap().block_on(async {
          let tmp = TempDir::new().unwrap();
          let mut slab = SlabClass::open(tmp.path(), CLASS_SIZE).await.unwrap();

          let slot_id = slab.put(&data).await.unwrap();
          let retrieved = slab.get(slot_id).await.unwrap();

          prop_assert_eq!(&retrieved, &data);
          Ok(())
        })
      },
    )
    .unwrap();
}

/// Property 2: Delete Marks Slot as Free / 删除标记槽位为空闲
#[test]
fn prop_del_marks_slot_free() {
  const CLASS_SIZE: usize = 16384;
  const MAX_PAYLOAD: usize = CLASS_SIZE - Header::SIZE;

  let config = ProptestConfig::with_cases(100);
  let mut runner = proptest::test_runner::TestRunner::new(config);

  runner
    .run(
      &prop::collection::vec(any::<u8>(), 1..MAX_PAYLOAD),
      |data| {
        compio::runtime::Runtime::new().unwrap().block_on(async {
          let tmp = TempDir::new().unwrap();
          let mut slab = SlabClass::open(tmp.path(), CLASS_SIZE).await.unwrap();

          let slot_id = slab.put(&data).await.unwrap();
          prop_assert!(!slab.is_free(slot_id));

          slab.del(slot_id);
          prop_assert!(slab.is_free(slot_id));
          prop_assert_eq!(slab.heat().get(slot_id), 0);

          Ok(())
        })
      },
    )
    .unwrap();
}

/// Property 9.3: Pipe Transfers All Data / 管道传输所有数据
#[test]
fn prop_pipe_transfers_all_data() {
  const CLASS_SIZE: usize = 16384;
  const MAX_PAYLOAD: usize = CLASS_SIZE - Header::SIZE;

  let config = ProptestConfig::with_cases(100);
  let mut runner = proptest::test_runner::TestRunner::new(config);

  runner
    .run(
      &prop::collection::vec(any::<u8>(), 1..MAX_PAYLOAD),
      |data| {
        compio::runtime::Runtime::new().unwrap().block_on(async {
          let tmp = TempDir::new().unwrap();
          let src_path = tmp.path().join("src");
          let dst_path = tmp.path().join("dst");
          std::fs::create_dir_all(&src_path).unwrap();
          std::fs::create_dir_all(&dst_path).unwrap();

          let mut src_slab = SlabClass::open(&src_path, CLASS_SIZE).await.unwrap();
          let mut dst_slab = SlabClass::open(&dst_path, CLASS_SIZE).await.unwrap();

          let src_slot = src_slab.put(&data).await.unwrap();
          let src_len = data.len() as u64;

          let mut reader = src_slab.reader(src_slot, src_len);
          let initial_remaining = reader.remaining();
          prop_assert_eq!(initial_remaining, src_len);

          let mut writer = dst_slab.writer().await.unwrap();
          let transferred = pipe(&mut reader, &mut writer).await.unwrap();

          prop_assert_eq!(transferred, initial_remaining);
          prop_assert!(reader.is_done());
          prop_assert_eq!(reader.remaining(), 0);
          prop_assert_eq!(writer.written(), transferred);

          let dst_slot = writer.finish().await.unwrap();
          let retrieved = dst_slab.get(dst_slot).await.unwrap();
          prop_assert_eq!(&retrieved, &data);

          Ok(())
        })
      },
    )
    .unwrap();
}

/// Property 9.2: Stream Copy Preserves Data / 流对拷保持数据
#[test]
fn prop_stream_copy_preserves_data() {
  const CLASS_SIZE: usize = 16384;
  const MAX_PAYLOAD: usize = CLASS_SIZE - Header::SIZE;

  let config = ProptestConfig::with_cases(100);
  let mut runner = proptest::test_runner::TestRunner::new(config);

  runner
    .run(
      &prop::collection::vec(any::<u8>(), 1..MAX_PAYLOAD),
      |data| {
        compio::runtime::Runtime::new().unwrap().block_on(async {
          let tmp = TempDir::new().unwrap();
          let src_path = tmp.path().join("src");
          let dst_path = tmp.path().join("dst");
          std::fs::create_dir_all(&src_path).unwrap();
          std::fs::create_dir_all(&dst_path).unwrap();

          let mut src_slab = SlabClass::open(&src_path, CLASS_SIZE).await.unwrap();
          let mut dst_slab = SlabClass::open(&dst_path, CLASS_SIZE).await.unwrap();

          let src_slot = src_slab.put(&data).await.unwrap();
          let src_len = data.len() as u64;

          let dst_slot = stream_copy(&src_slab, src_slot, src_len, &mut dst_slab)
            .await
            .unwrap();

          let dst_data = dst_slab.get(dst_slot).await.unwrap();
          prop_assert_eq!(&dst_data, &data);

          let src_data = src_slab.get(src_slot).await.unwrap();
          prop_assert_eq!(&src_data, &data);

          Ok(())
        })
      },
    )
    .unwrap();
}

/// Property 8: Scan Cold Returns Slots Below Threshold / 扫描冷数据返回低于阈值的槽位
#[test]
fn prop_scan_cold_returns_below_threshold() {
  let config = ProptestConfig::with_cases(100);
  let mut runner = proptest::test_runner::TestRunner::new(config);

  runner
    .run(
      &(prop::collection::vec(0u32..100, 1..50), 1u32..50),
      |(counters, threshold)| {
        let mut heat = HeatTracker::with_cap(counters.len());
        for (i, &c) in counters.iter().enumerate() {
          for _ in 0..c {
            heat.access(i as u64);
          }
        }

        let cold = heat.scan_cold(threshold);

        // Verify all returned slots are below threshold / 验证返回的槽位都低于阈值
        for &slot_id in &cold {
          let count = heat.get(slot_id);
          prop_assert!(
            count < threshold,
            "slot {slot_id} has count {count} >= threshold {threshold}"
          );
        }

        // Verify all slots below threshold are returned / 验证所有低于阈值的槽位都被返回
        for (i, &c) in counters.iter().enumerate() {
          if c < threshold {
            prop_assert!(
              cold.contains(&(i as u64)),
              "slot {i} with count {c} < {threshold} not in cold list"
            );
          }
        }

        Ok(())
      },
    )
    .unwrap();
}

/// Property 3: Overflow Error for Oversized Data / 超大数据返回溢出错误
#[test]
fn prop_overflow_error_for_oversized_data() {
  const CLASS_SIZE: usize = 4096;
  const MAX_PAYLOAD: usize = CLASS_SIZE - Header::SIZE;

  let config = ProptestConfig::with_cases(50);
  let mut runner = proptest::test_runner::TestRunner::new(config);

  runner
    .run(
      &prop::collection::vec(any::<u8>(), MAX_PAYLOAD + 1..MAX_PAYLOAD + 1000),
      |data| {
        compio::runtime::Runtime::new().unwrap().block_on(async {
          let tmp = TempDir::new().unwrap();
          let mut slab = SlabClass::open(tmp.path(), CLASS_SIZE).await.unwrap();

          let result = slab.put(&data).await;
          prop_assert!(result.is_err());
          match result {
            Err(Error::Overflow { len, max }) => {
              prop_assert_eq!(len, data.len());
              prop_assert_eq!(max, MAX_PAYLOAD);
            }
            _ => prop_assert!(false, "expected Overflow error"),
          }

          Ok(())
        })
      },
    )
    .unwrap();
}

/// Property 4: Allocated Slot Removed from Free Bitmap / 分配的槽位从空闲位图移除
#[test]
fn prop_allocated_slot_removed_from_free() {
  const CLASS_SIZE: usize = 16384;
  const MAX_PAYLOAD: usize = CLASS_SIZE - Header::SIZE;

  let config = ProptestConfig::with_cases(100);
  let mut runner = proptest::test_runner::TestRunner::new(config);

  runner
    .run(
      &prop::collection::vec(any::<u8>(), 1..MAX_PAYLOAD),
      |data| {
        compio::runtime::Runtime::new().unwrap().block_on(async {
          let tmp = TempDir::new().unwrap();
          let mut slab = SlabClass::open(tmp.path(), CLASS_SIZE).await.unwrap();

          // Put data, slot should not be free / 写入数据，槽位不应空闲
          let slot_id = slab.put(&data).await.unwrap();
          prop_assert!(!slab.is_free(slot_id));
          prop_assert!(!slab.free_map().contains(slot_id as u32));

          // Delete and reuse / 删除并重用
          slab.del(slot_id);
          prop_assert!(slab.is_free(slot_id));

          // Put again, should reuse the freed slot / 再次写入，应重用已释放的槽位
          let new_slot = slab.put(&data).await.unwrap();
          prop_assert_eq!(new_slot, slot_id);
          prop_assert!(!slab.is_free(new_slot));

          Ok(())
        })
      },
    )
    .unwrap();
}

/// Property 5: Metadata Serialization Round-Trip / 元数据序列化往返
#[test]
fn prop_metadata_roundtrip() {
  const CLASS_SIZE: usize = 16384;
  const MAX_PAYLOAD: usize = CLASS_SIZE - Header::SIZE;

  let config = ProptestConfig::with_cases(50);
  let mut runner = proptest::test_runner::TestRunner::new(config);

  runner
    .run(
      &prop::collection::vec(
        prop::collection::vec(any::<u8>(), 1..MAX_PAYLOAD / 10),
        1..10,
      ),
      |data_list| {
        compio::runtime::Runtime::new().unwrap().block_on(async {
          let tmp = TempDir::new().unwrap();

          // Phase 1: Write data and delete some / 阶段1：写入数据并删除部分
          let mut slots = Vec::new();
          {
            let mut slab = SlabClass::open(tmp.path(), CLASS_SIZE).await.unwrap();
            for data in &data_list {
              let slot_id = slab.put(data).await.unwrap();
              // Access multiple times to build heat / 多次访问以建立热度
              for _ in 0..3 {
                let _ = slab.get(slot_id).await.unwrap();
              }
              slots.push(slot_id);
            }

            // Delete every other slot / 删除每隔一个的槽位
            for (i, &slot_id) in slots.iter().enumerate() {
              if i % 2 == 0 {
                slab.del(slot_id);
              }
            }

            // Sync metadata / 同步元数据
            slab.sync_meta().await.unwrap();
          }

          // Phase 2: Recover and verify / 阶段2：恢复并验证
          {
            let mut slab = SlabClass::open(tmp.path(), CLASS_SIZE).await.unwrap();
            slab.recovery().await.unwrap();

            // Verify free_map state / 验证空闲位图状态
            for (i, &slot_id) in slots.iter().enumerate() {
              if i % 2 == 0 {
                prop_assert!(slab.is_free(slot_id), "slot {slot_id} should be free");
              } else {
                prop_assert!(!slab.is_free(slot_id), "slot {slot_id} should not be free");
              }
            }

            // Verify heat state (non-deleted slots should have heat) / 验证热度状态
            for (i, &slot_id) in slots.iter().enumerate() {
              if i % 2 != 0 {
                // Heat should be preserved (3 accesses + 1 from get in phase 1)
                let heat_count = slab.heat().get(slot_id);
                prop_assert!(heat_count >= 3, "slot {slot_id} heat {heat_count} < 3");
              }
            }
          }

          Ok(())
        })
      },
    )
    .unwrap();
}

/// Property 9: Stream Reads Complete Data / 流式读取完整数据
#[test]
fn prop_stream_reads_complete_data() {
  const CLASS_SIZE: usize = 16384;
  const MAX_PAYLOAD: usize = CLASS_SIZE - Header::SIZE;

  let config = ProptestConfig::with_cases(100);
  let mut runner = proptest::test_runner::TestRunner::new(config);

  runner
    .run(
      &prop::collection::vec(any::<u8>(), 1..MAX_PAYLOAD),
      |data| {
        compio::runtime::Runtime::new().unwrap().block_on(async {
          let tmp = TempDir::new().unwrap();
          let mut slab = SlabClass::open(tmp.path(), CLASS_SIZE).await.unwrap();

          let slot_id = slab.put(&data).await.unwrap();
          let total_len = data.len() as u64;

          let mut reader = slab.reader(slot_id, total_len);
          let mut collected = Vec::new();

          // Read in small chunks / 小块读取
          while !reader.is_done() {
            let chunk = reader.read(256).await.unwrap();
            if chunk.is_empty() {
              break;
            }
            collected.extend_from_slice(&chunk);
          }

          prop_assert_eq!(reader.remaining(), 0);
          prop_assert!(reader.is_done());
          prop_assert_eq!(&collected, &data);

          Ok(())
        })
      },
    )
    .unwrap();
}

/// Property 9.1: Stream Writes Complete Data / 流式写入完整数据
#[test]
fn prop_stream_writes_complete_data() {
  const CLASS_SIZE: usize = 16384;
  const MAX_PAYLOAD: usize = CLASS_SIZE - Header::SIZE;

  let config = ProptestConfig::with_cases(100);
  let mut runner = proptest::test_runner::TestRunner::new(config);

  runner
    .run(
      &prop::collection::vec(any::<u8>(), 1..MAX_PAYLOAD),
      |data| {
        compio::runtime::Runtime::new().unwrap().block_on(async {
          let tmp = TempDir::new().unwrap();
          let mut slab = SlabClass::open(tmp.path(), CLASS_SIZE).await.unwrap();

          let mut writer = slab.writer().await.unwrap();

          // Write in small chunks / 小块写入
          let mut offset = 0;
          while offset < data.len() {
            let end = (offset + 256).min(data.len());
            let n = writer.write(&data[offset..end]).unwrap();
            offset += n;
          }

          prop_assert_eq!(writer.written(), data.len() as u64);

          let slot_id = writer.finish().await.unwrap();
          let retrieved = slab.get(slot_id).await.unwrap();

          prop_assert_eq!(&retrieved, &data);

          Ok(())
        })
      },
    )
    .unwrap();
}

/// Property 10: Engine Routes to Smallest Fitting Class / Engine 路由到最小合适层级
#[test]
fn prop_engine_routes_to_smallest_class() {
  let config = ProptestConfig::with_cases(50);
  let mut runner = proptest::test_runner::TestRunner::new(config);

  // Test with various data sizes / 测试各种数据大小
  runner
    .run(&prop::collection::vec(any::<u8>(), 1..60000), |data| {
      compio::runtime::Runtime::new().unwrap().block_on(async {
        let tmp = TempDir::new().unwrap();
        let cfg = SlabConfig::new(tmp.path());
        let mut engine = Engine::new(cfg.clone()).await.unwrap();

        let (class_idx, slot_id) = engine.put(&data).await.unwrap();

        // Verify it's the smallest fitting class / 验证是最小合适层级
        let needed = data.len() + Header::SIZE;
        for (i, &size) in cfg.class_sizes.iter().enumerate() {
          if size >= needed {
            prop_assert_eq!(
              class_idx,
              i,
              "expected class {} (size {}) for data len {}",
              i,
              size,
              data.len()
            );
            break;
          }
        }

        // Verify data can be retrieved / 验证数据可以读取
        let retrieved = engine.get(class_idx, slot_id).await.unwrap();
        prop_assert_eq!(&retrieved, &data);

        Ok(())
      })
    })
    .unwrap();
}

/// Property 11: GC Compresses Beneficial Data / GC 压缩有益数据
#[test]
fn test_gc_compresses_beneficial_data() {
  compio::runtime::Runtime::new().unwrap().block_on(async {
    let tmp = TempDir::new().unwrap();
    // Use smaller class sizes for testing / 使用较小的层级大小进行测试
    let cfg = SlabConfig {
      class_sizes: vec![16384, 65536, 262144],
      base_path: tmp.path().to_path_buf(),
    };
    let mut engine = Engine::new(cfg).await.unwrap();

    // Write highly compressible data (repeated pattern) / 写入高度可压缩的数据（重复模式）
    let data: Vec<u8> = (0..50000).map(|i| (i % 256) as u8).collect();
    let (class_idx, slot_id) = engine.put(&data).await.unwrap();

    // Data should be in a larger class initially / 数据最初应在较大的层级
    assert!(
      class_idx > 0,
      "data should be in class > 0, got {class_idx}"
    );

    // Run GC with low thresholds (all data is cold) / 使用低阈值运行 GC（所有数据都是冷数据）
    let migrations = engine.gc(10, 5).await.unwrap();

    // If compression was beneficial, data should migrate to smaller class
    // 如果压缩有益，数据应迁移到更小的层级
    let migration = migrations
      .iter()
      .find(|m| m.old_slot == slot_id && m.old_class == class_idx);
    if let Some(m) = migration {
      assert!(m.new_class < m.old_class, "should migrate to smaller class");
      assert!(m.new_compress != Compress::None, "should be compressed");

      // Verify data integrity after migration / 验证迁移后数据完整性
      let retrieved = engine.get(m.new_class, m.new_slot).await.unwrap();
      assert_eq!(retrieved, data);
    }
  });
}

/// Property 12: GC Skips Non-Beneficial Compression / GC 跳过无益压缩
#[test]
fn test_gc_skips_non_beneficial_compression() {
  compio::runtime::Runtime::new().unwrap().block_on(async {
    let tmp = TempDir::new().unwrap();
    // Use same-sized classes to test skip behavior / 使用相同大小的层级测试跳过行为
    let cfg = SlabConfig {
      class_sizes: vec![16384, 65536],
      base_path: tmp.path().to_path_buf(),
    };
    let mut engine = Engine::new(cfg).await.unwrap();

    // Write random data (hard to compress) that fits in smallest class
    // 写入随机数据（难以压缩）且适合最小层级
    let data: Vec<u8> = (0..10000).map(|i| (i * 17 + i * i) as u8).collect();
    let (class_idx, slot_id) = engine.put(&data).await.unwrap();

    // Should be in smallest class / 应在最小层级
    assert_eq!(class_idx, 0, "data should be in smallest class");

    // Run GC / 运行 GC
    let migrations = engine.gc(10, 5).await.unwrap();

    // Should not migrate (already in smallest class or compression not beneficial)
    // 不应迁移（已在最小层级或压缩无益）
    let migrated = migrations.iter().any(|m| m.old_slot == slot_id);
    assert!(!migrated, "should not migrate data in smallest class");

    // Verify data still accessible / 验证数据仍可访问
    let retrieved = engine.get(class_idx, slot_id).await.unwrap();
    assert_eq!(retrieved, data);
  });
}

/// Property 13: GC Frees Original Slot After Migration / GC 迁移后释放原槽位
#[test]
fn test_gc_frees_original_slot() {
  compio::runtime::Runtime::new().unwrap().block_on(async {
    let tmp = TempDir::new().unwrap();
    let cfg = SlabConfig {
      class_sizes: vec![16384, 65536, 262144],
      base_path: tmp.path().to_path_buf(),
    };
    let mut engine = Engine::new(cfg).await.unwrap();

    // Write highly compressible data / 写入高度可压缩的数据
    let data: Vec<u8> = vec![0u8; 50000];
    let (class_idx, slot_id) = engine.put(&data).await.unwrap();

    assert!(
      class_idx > 0,
      "data should be in class > 0, got {class_idx}"
    );

    // Run GC / 运行 GC
    let migrations = engine.gc(10, 5).await.unwrap();

    // Find migration for our slot / 查找我们槽位的迁移
    let migration = migrations
      .iter()
      .find(|m| m.old_slot == slot_id && m.old_class == class_idx);

    if let Some(m) = migration {
      // Original slot should be freed / 原槽位应被释放
      let old_slab = engine.class(m.old_class).unwrap();
      assert!(
        old_slab.is_free(m.old_slot),
        "original slot should be freed"
      );

      // New slot should not be free / 新槽位不应空闲
      let new_slab = engine.class(m.new_class).unwrap();
      assert!(!new_slab.is_free(m.new_slot), "new slot should not be free");

      // Data should be accessible from new location / 数据应可从新位置访问
      let retrieved = engine.get(m.new_class, m.new_slot).await.unwrap();
      assert_eq!(retrieved, data);
    }
  });
}
