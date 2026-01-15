use super::r#trait::FsstBench;

pub struct Fsst {
  symbol_table: [u8; fsst::fsst::FSST_SYMBOL_TABLE_SIZE],
  encode_buf: Vec<u8>,
  decode_buf: Vec<u8>,
  buf: Vec<u8>,
  offsets: Vec<i64>,
  compressed_offsets: Vec<i64>,
  out_offsets: Vec<i64>,
  // Reusable buffers for random decode
  // 随机解码复用的 buffer
  mini_offsets: Vec<i64>,
  mini_out_offsets: Vec<i64>,
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
      buf: Vec::new(),
      offsets: Vec::new(),
      compressed_offsets: Vec::new(),
      out_offsets: Vec::new(),
      mini_offsets: vec![0, 0],
      mini_out_offsets: vec![0, 0],
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
    // fsst compress: out_buf >= in_buf
    // fsst decompress: out_buf >= in_buf * 3
    // 预分配缓冲区
    let total = self.buf.len();
    let n = items.len();
    self.encode_buf = vec![0u8; total];
    self.decode_buf = vec![0u8; total * 3];
    self.compressed_offsets = vec![0i64; n + 1];
    self.out_offsets = vec![0i64; n + 1];
  }

  fn train_and_encode(&mut self) -> usize {
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
    let compressed = &self.encode_buf[start..end];

    // Reuse pre-allocated buffers
    // 复用预分配的 buffer
    self.mini_offsets[0] = 0;
    self.mini_offsets[1] = (end - start) as i64;
    self.mini_out_offsets[0] = 0;
    self.mini_out_offsets[1] = 0;

    fsst::fsst::decompress(
      &self.symbol_table,
      compressed,
      &self.mini_offsets,
      &mut self.decode_buf,
      &mut self.mini_out_offsets,
    )
    .expect("fsst random decode failed");
  }

  fn num_items(&self) -> usize {
    self.offsets.len().saturating_sub(1)
  }
}
