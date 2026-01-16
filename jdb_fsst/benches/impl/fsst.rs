use super::r#trait::FsstBench;

pub struct Fsst {
  symbol_table: [u8; fsst::fsst::FSST_SYMBOL_TABLE_SIZE],
  encode_buf: Vec<u8>,
  decode_buf: Vec<u8>,
  // Separate buffer for random decode
  // 随机解码单独的 buffer
  rand_decode_buf: Vec<u8>,
  buf: Vec<u8>,
  offsets: Vec<i64>,
  compressed_offsets: Vec<i64>,
  out_offsets: Vec<i64>,
  mini_offsets: Vec<i64>,
  mini_out_offsets: Vec<i64>,
  // Original data size for buffer allocation
  // 原始数据大小，用于分配 buffer
  total_size: usize,
}

impl Default for Fsst {
  fn default() -> Self {
    Self::new()
  }
}

impl Fsst {
  pub fn new() -> Self {
    Self {
      symbol_table: [0u8; fsst::fsst::FSST_SYMBOL_TABLE_SIZE],
      encode_buf: Vec::new(),
      decode_buf: Vec::new(),
      rand_decode_buf: Vec::new(),
      buf: Vec::new(),
      offsets: Vec::new(),
      compressed_offsets: Vec::new(),
      out_offsets: Vec::new(),
      mini_offsets: vec![0, 0],
      mini_out_offsets: vec![0, 0],
      total_size: 0,
    }
  }
}

impl FsstBench for Fsst {
  const NAME: &'static str = "fsst";

  fn prepare(&mut self, items: &[&[u8]]) {
    self.buf.clear();
    self.offsets.clear();
    self.offsets.push(0);
    for item in items {
      self.buf.extend_from_slice(item);
      self.offsets.push(self.buf.len() as i64);
    }
    self.total_size = self.buf.len();
    let n = items.len();
    self.compressed_offsets = vec![0i64; n + 1];
    self.out_offsets = vec![0i64; n + 1];
  }

  fn train_and_encode(&mut self) -> usize {
    // fsst compress needs out_buf >= in_buf, and modifies its length
    // fsst compress 需要 out_buf >= in_buf，且会修改其长度
    self.encode_buf.resize(self.total_size, 0);
    fsst::fsst::compress(
      &mut self.symbol_table,
      &self.buf,
      &self.offsets,
      &mut self.encode_buf,
      &mut self.compressed_offsets,
    )
    .expect("fsst compress failed");
    self.compressed_offsets.last().copied().unwrap_or(0) as usize
  }

  fn decode_all(&mut self) {
    // fsst decompress needs out_buf >= in_buf * 3
    // fsst 解压需要 out_buf >= in_buf * 3
    self.decode_buf.resize(self.total_size * 3, 0);
    fsst::fsst::decompress(
      &self.symbol_table,
      &self.encode_buf,
      &self.compressed_offsets,
      &mut self.decode_buf,
      &mut self.out_offsets,
    )
    .expect("fsst decode failed");
  }

  fn random_decode(&mut self, index: usize) {
    if index + 1 >= self.compressed_offsets.len() {
      return;
    }
    let start = self.compressed_offsets[index] as usize;
    let end = self.compressed_offsets[index + 1] as usize;
    if start >= self.encode_buf.len() || end > self.encode_buf.len() {
      return;
    }
    let compressed = &self.encode_buf[start..end];
    let compressed_len = end - start;

    // fsst decompress needs out_buf >= in_buf * 3
    // fsst 解压需要 out_buf >= in_buf * 3
    let need_size = compressed_len * 3;
    if self.rand_decode_buf.len() < need_size {
      self.rand_decode_buf.resize(need_size, 0);
    }

    self.mini_offsets[0] = 0;
    self.mini_offsets[1] = compressed_len as i64;
    self.mini_out_offsets[0] = 0;
    self.mini_out_offsets[1] = 0;

    fsst::fsst::decompress(
      &self.symbol_table,
      compressed,
      &self.mini_offsets,
      &mut self.rand_decode_buf,
      &mut self.mini_out_offsets,
    )
    .expect("fsst random decode failed");
  }

  fn num_items(&self) -> usize {
    self.offsets.len().saturating_sub(1)
  }
}
