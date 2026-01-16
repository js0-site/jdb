/// A compact read-only bit vector supporting high-performance bit reading.
/// 紧凑的只读位向量，支持高性能位读取。
#[derive(Clone, Debug)]
#[cfg_attr(feature = "bitcode", derive(bitcode::Encode, bitcode::Decode))]
pub struct Bv {
  pub(crate) data: Vec<u64>,
  pub(crate) len: usize,
}

impl Bv {
  /// Create a new empty BitVector.
  /// 创建一个新的空位向量。
  pub fn new() -> Self {
    Self {
      data: Vec::new(),
      len: 0,
    }
  }
}

impl Default for Bv {
  fn default() -> Self {
    Self::new()
  }
}

impl Bv {
  /// Appends a single bit.
  /// 追加单个位。
  #[inline]
  pub fn push(&mut self, bit: bool) {
    let bit_idx = self.len & 63; // % 64
    if bit_idx == 0 {
      self.data.push(0);
    }
    if bit {
      // SAFETY: We just pushed if bit_idx == 0, so data is not empty.
      // 安全性：若 bit_idx == 0 则刚 push 元素，故 data 非空。
      unsafe {
        let last = self.data.len() - 1;
        *self.data.get_unchecked_mut(last) |= 1 << bit_idx;
      }
    }
    self.len += 1;
  }

  /// Shrinks the capacity of the data vector as much as possible.
  /// 尽可能收缩数据向量的容量。
  pub fn shrink_to_fit(&mut self) {
    self.data.shrink_to_fit();
  }

  /// Appends the lower `width` bits of `value`.
  /// 追加 `value` 的低 `width` 位。
  pub fn push_int(&mut self, mut value: u64, width: usize) {
    // Mask out unwanted upper bits
    // 掩码屏蔽不需要的高位
    if width < 64 {
      value &= (1u64 << width) - 1;
    }

    let bit_offset = self.len & 63; // % 64
    if bit_offset == 0 {
      self.data.push(value);
    } else {
      let available = 64 - bit_offset;
      // SAFETY: data is not empty because bit_offset != 0
      // 安全性：bit_offset != 0 意味着已有数据，data 非空
      unsafe {
        let last_idx = self.data.len() - 1;
        let last = self.data.get_unchecked_mut(last_idx);
        *last |= value << bit_offset;
      }

      if width > available {
        self.data.push(value >> available);
      }
    }
    self.len += width;
  }

  /// Get the bit at `index`. Panics if index is out of bounds.
  /// 获取 `index` 处的位。如果索引越界则恐慌。
  #[inline]
  pub fn get(&self, index: usize) -> bool {
    assert!(
      index < self.len,
      "Bv index out of bounds: {} >= {}",
      index,
      self.len
    );
    unsafe { self.get_unchecked(index) }
  }

  /// Get bit without bounds checking.
  /// 不进行边界检查获取位。
  ///
  /// # Safety
  /// `index` must be < `self.len`.
  /// `index` 必须小于 `self.len`。
  #[inline]
  pub unsafe fn get_unchecked(&self, index: usize) -> bool {
    let word_idx = index / 64;
    let bit_idx = index & 63; // % 64
    // SAFETY: Caller guarantees index validity
    let word = unsafe { *self.data.get_unchecked(word_idx) };
    (word >> bit_idx) & 1 == 1
  }

  /// Get an integer of `width` bits starting at `index`.
  /// 从 `index` 开始获取 `width` 位的整数。
  #[inline]
  pub fn get_int(&self, index: usize, width: usize) -> u64 {
    assert!(index + width <= self.len, "Bv range out of bounds");
    unsafe { self.get_int_unchecked(index, width) }
  }

  /// Get int without bounds checking.
  /// 不进行边界检查获取整数。
  ///
  /// # Safety
  /// `index + width` must be <= `self.len`.
  /// `index + width` 必须小于等于 `self.len`。
  #[inline]
  pub unsafe fn get_int_unchecked(&self, index: usize, width: usize) -> u64 {
    if width == 0 {
      return 0;
    }

    let word_idx = index / 64;
    let bit_offset = index & 63;

    // Load first word
    // 加载第一个字
    // SAFETY: Caller guarantees index validity
    let word = unsafe { *self.data.get_unchecked(word_idx) };
    let mut result = word >> bit_offset;

    let available = 64 - bit_offset;
    if width > available {
      // Need part of next word
      // 需要下一个字的一部分
      // SAFETY: Caller guarantees index+width validity
      let next_word = unsafe { *self.data.get_unchecked(word_idx + 1) };
      result |= next_word << available;
    }

    if width < 64 {
      result &= (1u64 << width) - 1;
    }
    result
  }

  /// Returns the length in bits.
  /// 返回位长度。
  #[inline]
  pub fn len(&self) -> usize {
    self.len
  }

  /// Returns true if empty.
  /// 若为空返回 true。
  pub fn is_empty(&self) -> bool {
    self.len == 0
  }

  /// Size in bytes (heap usage).
  /// 字节大小（堆内存使用量）。
  #[cfg(feature = "mem")]
  pub fn size_in_bytes(&self) -> usize {
    self.data.capacity() * 8
  }
}
