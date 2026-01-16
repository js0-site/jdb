use crate::{elias_fano::Ef, iter::Iter};

/// Partitioned Elias-Fano Index.
/// 分区 Elias-Fano 索引。
/// Contains two levels: Upper (Headers) and Lower (Chunks).
/// 包含两层：上层（头信息）和下层（数据块）。
#[cfg_attr(feature = "bitcode", derive(bitcode::Encode, bitcode::Decode))]
pub struct Pef {
  pub upper: Ef,
  pub chunks: Vec<Ef>,
  pub block_size: usize,
  pub num_elements: usize,
}

impl Pef {
  /// Create a new Partitioned Elias-Fano index.
  /// 创建一个新的分区 Elias-Fano 索引。
  ///
  /// `data`: Sorted sequence of integers.
  /// `data`: 排序的整数序列。
  /// `block_size`: Number of elements per chunk (recommend 128).
  /// `block_size`: 每个块的元素数量（推荐 128）。
  pub fn new(data: &[u64], block_size: usize) -> Self {
    assert!(block_size > 0, "block_size must be > 0");
    if data.is_empty() {
      return Self {
        upper: Ef::empty(),
        chunks: vec![],
        block_size,
        num_elements: 0,
      };
    }

    let mut chunks = Vec::with_capacity(data.len().div_ceil(block_size));
    let mut upper_bounds = Vec::with_capacity(chunks.capacity());

    for chunk_slice in data.chunks(block_size) {
      chunks.push(Ef::new(chunk_slice));
      // Record the max value of each chunk
      // 记录每个块的最大值
      // SAFETY: chunks are non-empty because data is non-empty and chunk_slice is from data
      unsafe {
        upper_bounds.push(*chunk_slice.last().unwrap_unchecked());
      }
    }

    // Build upper level index
    // 构建上层索引
    let upper = Ef::new(&upper_bounds);

    Self {
      upper,
      chunks,
      block_size,
      num_elements: data.len(),
    }
  }

  /// Random access: get the index-th element.
  /// 随机访问：获取第 index 个元素。
  pub fn get(&self, index: usize) -> Option<u64> {
    if index >= self.num_elements {
      return None;
    }

    // Optimization: use unchecked division/mod if we trust block_size > 0
    // 优化：如果相信 block_size > 0，可使用 unchecked 除法/取模
    let chunk_idx = index / self.block_size;
    let offset = index % self.block_size;

    // SAFETY: chunk_idx is generally safe but let's keep safe get for robustness.
    // We can use get_unchecked safely because:
    // index < num_elements
    // num_elements = sum(chunk_len)
    // so chunk_idx < chunks.len()
    unsafe { self.chunks.get_unchecked(chunk_idx).get(offset) }
  }

  /// Find first element >= target.
  /// 查找第一个 >= target 的元素。
  pub fn next_ge(&self, target: u64) -> Option<u64> {
    // 1. Search in Upper layer
    // 1. 在上层索引中搜索
    // We look for a chunk whose Max >= target
    // 我们寻找一个 Max >= target 的块
    match self.upper.next_ge(target) {
      Some((chunk_idx, _max_val)) => {
        // 2. Search in specific chunk
        // 2. 在特定块中搜索
        if chunk_idx >= self.chunks.len() {
          return None;
        }
        // SAFETY: indexing chunks is safe as checked above?
        // Wait, upper index stores MAX values.
        // If chunk_idx from upper is valid, chunks[chunk_idx] exists.
        unsafe {
          self
            .chunks
            .get_unchecked(chunk_idx)
            .next_ge(target)
            .map(|(_idx, val)| val)
        }
      }
      None => None,
    }
  }

  /// Create an iterator for efficient sequential access and skipping.
  /// 创建一个用于高效顺序访问和跳过的迭代器。
  pub fn iter(&self) -> Iter<'_> {
    Iter::new(self)
  }

  /// Memory usage in bytes.
  /// 内存使用量（字节）。
  pub fn memory_usage(&self) -> usize {
    let mut bytes = self.upper.size_in_bytes();
    for c in &self.chunks {
      bytes += c.size_in_bytes();
    }
    bytes
  }
}

// Implement IntoIterator for &Pef
// 为 &Pef 实现 IntoIterator
impl<'a> IntoIterator for &'a Pef {
  type Item = u64;
  type IntoIter = Iter<'a>;

  fn into_iter(self) -> Self::IntoIter {
    self.iter()
  }
}
