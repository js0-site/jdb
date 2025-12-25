//! GC worker for cold data compaction and tiering / 冷数据压缩回收与分层
//!
//! Scans cold data and migrates to smaller classes with compression.
//! 扫描冷数据并压缩迁移到更小的层级。

use crate::{Compress, Engine, Result, SlotId};

/// Migration result / 迁移结果
#[derive(Debug, Clone)]
pub struct Migration {
  /// Old class index / 旧层级索引
  pub old_class: usize,
  /// Old slot id / 旧槽位 ID
  pub old_slot: SlotId,
  /// New class index / 新层级索引
  pub new_class: usize,
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
    let cold_slots: Vec<(usize, SlotId, u32)> = self
      .classes
      .iter()
      .enumerate()
      .flat_map(|(class_idx, slab)| {
        slab
          .heat()
          .cold_iter(warm_threshold)
          .filter(|&slot_id| !slab.is_free(slot_id))
          .map(move |slot_id| (class_idx, slot_id, slab.heat().get(slot_id)))
      })
      .collect();

    // Process each cold slot / 处理每个冷槽位
    for (class_idx, slot_id, access_count) in cold_slots {
      if let Some(migration) = self
        .compact_slot(
          class_idx,
          slot_id,
          access_count,
          warm_threshold,
          cold_threshold,
        )
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
    class_idx: usize,
    slot_id: SlotId,
    access_count: u32,
    warm: u32,
    cold: u32,
  ) -> Result<Option<Migration>> {
    // Read original data / 读取原始数据
    let data = self.get(class_idx, slot_id).await?;

    // Select compression / 选择压缩方式
    let new_compress = Self::select_compress(access_count, warm, cold);

    // Try to find smaller class / 尝试找到更小的层级
    let (new_class, new_slot) = self.put_with(&data, new_compress).await?;

    // If migrated to same or larger class, skip / 如果迁移到相同或更大的层级，跳过
    if new_class >= class_idx {
      // Rollback: delete new slot / 回滚：删除新槽位
      self.del(new_class, new_slot);
      return Ok(None);
    }

    // Mark original slot as free / 标记原槽位为空闲
    self.del(class_idx, slot_id);

    Ok(Some(Migration {
      old_class: class_idx,
      old_slot: slot_id,
      new_class,
      new_slot,
      old_compress: Compress::None, // Original was uncompressed / 原始未压缩
      new_compress,
    }))
  }
}
