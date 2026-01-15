pub struct RefFsst {
  symbol_table: [u8; fsst::fsst::FSST_SYMBOL_TABLE_SIZE],
  encode_buf: Vec<u8>,
  decode_buf: Vec<u8>,
  buf: Vec<u8>,
  offsets: Vec<i64>,
  compressed_offsets: Vec<i64>,
}

impl Default for RefFsst {
  fn default() -> Self { Self::new() }
}

impl RefFsst {
  pub fn new() -> Self {
    Self {
      symbol_table: [0u8; fsst::fsst::FSST_SYMBOL_TABLE_SIZE],
      encode_buf: Vec::new(),
      decode_buf: Vec::new(),
      buf: Vec::new(),
      offsets: Vec::new(),
      compressed_offsets: Vec::new(),
    }
  }
}

impl FsstBench for RefFsst {
  const NAME: &'static str = "Ref";

  fn prepare(&mut self, items: &[&[u8]]) {
    self.buf.clear();
    self.offsets.clear();
    self.offsets.push(0);
    for item in items {
      self.buf.extend_from_slice(item);
      self.offsets.push(self.buf.len() as i64);
    }
  }

  fn train_and_encode(&mut self) -> usize {
    self.encode_buf.clear();
    self.encode_buf.resize(self.buf.len() * 2 + 1024, 0);
    self.compressed_offsets = vec![0i64; self.offsets.len()];
    fsst::fsst::compress(&mut self.symbol_table, &self.buf, &self.offsets, &mut self.encode_buf, &mut self.compressed_offsets).expect("fsst compress failed");
    self.compressed_offsets.last().copied().unwrap_or(0) as usize
  }

  fn decompress_all(&mut self) {
    self.decode_buf.clear();
    self.decode_buf.resize(self.buf.len() * 2 + 1024, 0);
    let mut out_offsets = vec![0i64; self.offsets.len()];
    fsst::fsst::decompress(&self.symbol_table, &self.encode_buf, &self.compressed_offsets, &mut self.decode_buf, &mut out_offsets).expect("fsst decode failed");
  }

  fn reset(&mut self) {
    self.symbol_table = [0u8; fsst::fsst::FSST_SYMBOL_TABLE_SIZE];
    self.encode_buf.clear();
    self.decode_buf.clear();
    self.buf.clear();
    self.offsets.clear();
    self.compressed_offsets.clear();
  }
}
