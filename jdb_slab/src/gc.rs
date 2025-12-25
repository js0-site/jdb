//! GC worker for cold data compaction and tiering / 冷数据压缩回收与分层
//!
//! Scans cold data and migrates to smaller classes with compression.
//! 扫描冷数据并压缩迁移到更小的层级。

use crate::{Compress, Engine, Result, SlotId, decode_slab, encode_slab, is_blob};

/// Migration result / 迁移结果
#[derive(Debug, Clone)]
pub struct Migration {
  /// Old slot id / 旧槽位 ID
  pub old_slot: SlotId,
  /// New slot id / 新槽位 ID
  pub new_slot: SlotId,
  /// Old compression / 旧压缩类型
  pub old_compress: Compress,
  /// New compression / 新压缩类型
  pub new_compress: Compress,
}

impl Engine {
  /// Get classes iterator / 获取层级迭代器
  pub fn classes_iter(&self) -> impl Iterator<Item = &crate::SlabClass> {
    self.classes.iter()
  }

  /// Select compression based on access count / 根据访问计数选择压缩方式
  fn select_compress(access_count: u32, warm: u32, cold: u32) -> Compress {
    if access_count < cold {
      Compress::Zstd // Cold: max compression / 冷数据：最大压缩
    } else if access_count < warm {
      Compress::Lz4 // Warm: balanced / 温数据：平衡
    } else {
      Compress::None // Hot: no compression / 热数据：无压缩
    }
  }

  /// Run garbage collection / 运行垃圾回收
  pub async fn gc(&mut self, warm_threshold: u32, cold_threshold: u32) -> Result<Vec<Migration>> {
    let mut migrations = Vec::new();

    // Collect cold slot info first / 先收集冷槽位信息
    let cold_slots: Vec<(usize, u64, u32)> = self
      .classes
      .iter()
      .enumerate()
      .flat_map(|(class_idx, slab)| {
        slab
          .heat()
          .cold_iter(warm_threshold)
          .filter(|&inner_id| !slab.is_free(inner_id))
          .map(move |inner_id| (class_idx, inner_id, slab.heat().get(inner_id)))
      })
      .collect();

    // Process each cold slot / 处理每个冷槽位
    for (class_idx, inner_id, access_count) in cold_slots {
      let slot_id = encode_slab(class_idx, inner_id);
      if let Some(migration) = self
        .compact_slot(slot_id, access_count, warm_threshold, cold_threshold)
        .await?
      {
        migrations.push(migration);
      }
    }

    Ok(migrations)
  }

  /// Compact single slot with appropriate compression / 用合适的压缩方式压缩单个槽位
  async fn compact_slot(
    &mut self,
    slot_id: SlotId,
    access_count: u32,
    warm: u32,
    cold: u32,
  ) -> Result<Option<Migration>> {
    // Skip blob slots / 跳过 blob 槽位
    if is_blob(slot_id) {
      return Ok(None);
    }

    let (old_class, _) = decode_slab(slot_id);

    // Read original data / 读取原始数据
    let data = self.get(slot_id).await?;

    // Select compression / 选择压缩方式
    let new_compress = Self::select_compress(access_count, warm, cold);

    // Try to find smaller class / 尝试找到更小的层级
    let new_slot = self.put_with(&data, new_compress).await?;

    // If migrated to blob or same/larger class, skip / 如果迁移到 blob 或相同/更大的层级，跳过
    if is_blob(new_slot) {
      self.del(new_slot);
      return Ok(None);
    }

    let (new_class, _) = decode_slab(new_slot);
    if new_class >= old_class {
      // Rollback: delete new slot / 回滚：删除新槽位
      self.del(new_slot);
      return Ok(None);
    }

    // Mark original slot as free / 标记原槽位为空闲
    self.del(slot_id);

    Ok(Some(Migration {
      old_slot: slot_id,
      new_slot,
      old_compress: Compress::None, // Original was uncompressed / 原始未压缩
      new_compress,
    }))
  }
}
