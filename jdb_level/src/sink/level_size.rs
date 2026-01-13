use super::{LevelSize, N};
// 基础层的目标大小，RocksDB 默认通常为 256 MB
const BASE_SIZE: u64 = 256 * 1024 * 1024;

// 每一层的放大倍数
const SCALE: u64 = 8;

#[inline]
pub fn level_size(total_size: u64) -> LevelSize {
  // 初始化所有层的大小为 0
  let mut li = [0u64; _];

  // 最后一层的目标大小就是当前的总数据量
  let mut current_target = total_size;

  // 从最后一层 (N-1) 自底向上倒推，L0 通常受文件数限制，不分配字节大小
  for level in (0..N).rev() {
    // SAFETY: level is within range 0..N, N is 6, li is size 6.
    unsafe {
      *li.get_unchecked_mut(level) = current_target;
    }

    // 截止条件：如果当前层大小小于等于基础大小 (256MB)，
    // 则上面的层全部设为 0 (数据直通 Base Level)，停止倒推。
    if current_target <= BASE_SIZE {
      break;
    }

    // 计算上一层的大小
    current_target /= SCALE;
  }

  li
}
