//! SlotId encoding/decoding / SlotId 编码解码
//!
//! Format: [1 bit blob flag][7 bits class_idx][56 bits inner_id]
//! 格式: [1位blob标记][7位class索引][56位内部ID]

/// Slot identifier / 槽位标识
pub type SlotId = u64;

/// Blob flag (high bit) / Blob 标记（最高位）
const BLOB_FLAG: u64 = 1 << 63;

/// Class index bits / 类索引位数
const CLASS_SHIFT: u32 = 56;

/// Inner id mask / 内部ID掩码
const INNER_MASK: u64 = (1 << CLASS_SHIFT) - 1;

/// Check if slot is blob / 检查是否为 blob
#[inline]
pub const fn is_blob(slot_id: SlotId) -> bool {
  slot_id & BLOB_FLAG != 0
}

/// Extract blob id / 提取 blob ID
#[inline]
pub const fn blob_id(slot_id: SlotId) -> u64 {
  slot_id & !BLOB_FLAG
}

/// Make blob slot id / 创建 blob 槽位 ID
#[inline]
pub const fn make_blob(id: u64) -> SlotId {
  id | BLOB_FLAG
}

/// Encode slab slot / 编码 slab 槽位
#[inline]
pub const fn encode_slab(class_idx: usize, inner_id: u64) -> SlotId {
  ((class_idx as u64) << CLASS_SHIFT) | (inner_id & INNER_MASK)
}

/// Decode slab slot / 解码 slab 槽位
#[inline]
pub const fn decode_slab(slot_id: SlotId) -> (usize, u64) {
  let class_idx = (slot_id >> CLASS_SHIFT) as usize;
  let inner_id = slot_id & INNER_MASK;
  (class_idx, inner_id)
}
