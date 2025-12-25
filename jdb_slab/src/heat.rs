//! Heat tracker for access frequency / 访问热度追踪器
//!
//! Tracks slot access counts for tiered storage decisions.
//! 追踪槽位访问计数，用于分层存储决策。

use crate::{Error, Result};

/// Slot ID type / 槽位 ID 类型
pub type SlotId = u64;

/// Access heat tracker / 访问热度追踪器
#[derive(Debug, Clone, Default)]
pub struct HeatTracker {
  /// Access count per slot / 每槽位访问计数
  stats: Vec<u32>,
}

impl HeatTracker {
  /// Create with capacity / 创建指定容量
  #[inline]
  pub fn with_cap(cap: usize) -> Self {
    Self {
      stats: vec![0; cap],
    }
  }

  /// Record access / 记录访问
  #[inline]
  pub fn access(&mut self, slot_id: SlotId) {
    let idx = slot_id as usize;
    if idx >= self.stats.len() {
      self.stats.resize(idx + 1, 0);
    }
    self.stats[idx] = self.stats[idx].saturating_add(1);
  }

  /// Get access count / 获取访问计数
  #[inline]
  pub fn get(&self, slot_id: SlotId) -> u32 {
    self.stats.get(slot_id as usize).copied().unwrap_or(0)
  }

  /// Decay all counters (right-shift by 1) / 衰减所有计数器
  #[inline]
  pub fn decay(&mut self) {
    for c in &mut self.stats {
      *c >>= 1;
    }
  }

  /// Find cold slots below threshold / 查找低于阈值的冷槽位
  pub fn scan_cold(&self, threshold: u32) -> Vec<SlotId> {
    self
      .stats
      .iter()
      .enumerate()
      .filter_map(|(i, &c)| {
        if c < threshold {
          Some(i as SlotId)
        } else {
          None
        }
      })
      .collect()
  }

  /// Clear slot stats / 清除槽位统计
  #[inline]
  pub fn clear(&mut self, slot_id: SlotId) {
    let idx = slot_id as usize;
    if idx < self.stats.len() {
      self.stats[idx] = 0;
    }
  }

  /// Current capacity / 当前容量
  #[inline]
  pub fn len(&self) -> usize {
    self.stats.len()
  }

  /// Check if empty / 是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.stats.is_empty()
  }

  /// Serialize to bytes / 序列化为字节
  /// Format: [count: u64][stats: [u32; count]]
  pub fn serialize(&self) -> Vec<u8> {
    let count = self.stats.len() as u64;
    let mut buf = Vec::with_capacity(8 + self.stats.len() * 4);
    buf.extend_from_slice(&count.to_le_bytes());
    for &c in &self.stats {
      buf.extend_from_slice(&c.to_le_bytes());
    }
    buf
  }

  /// Deserialize from bytes / 从字节反序列化
  pub fn deserialize(bytes: &[u8]) -> Result<Self> {
    if bytes.len() < 8 {
      return Err(Error::Serialize("heat data too short".into()));
    }
    let count = u64::from_le_bytes([
      bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]) as usize;
    let expected = 8 + count * 4;
    if bytes.len() < expected {
      return Err(Error::Serialize(format!(
        "heat data truncated: {len} < {expected}",
        len = bytes.len()
      )));
    }
    let mut stats = Vec::with_capacity(count);
    for i in 0..count {
      let off = 8 + i * 4;
      let c = u32::from_le_bytes([bytes[off], bytes[off + 1], bytes[off + 2], bytes[off + 3]]);
      stats.push(c);
    }
    Ok(Self { stats })
  }
}
