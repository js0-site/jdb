/// My jdb_fsst implementation benchmark
use jdb_fsst::train;

pub struct MyFsst {
  encoder: Option<jdb_fsst::encode::Encode>,
  decoder: Option<jdb_fsst::decode::Decode>,
  // Contiguous buffer + offsets (like ref impl)
  // 连续缓冲区 + 偏移量（类似参考实现）
  buf: Vec<u8>,
  offsets: Vec<usize>,
  encode_buf: Vec<u8>,
  compressed_offsets: Vec<usize>,
  decode_buf: Vec<u8>,
}

impl Default for MyFsst {
  fn default() -> Self {
    Self::new()
  }
}

impl MyFsst {
  pub fn new() -> Self {
    Self {
      encoder: None,
      decoder: None,
      buf: Vec::new(),
      offsets: Vec::new(),
      encode_buf: Vec::new(),
      compressed_offsets: Vec::new(),
      decode_buf: Vec::new(),
    }
  }
}

impl FsstBench for MyFsst {
  const NAME: &'static str = "My";

  fn prepare(&mut self, items: &[&[u8]]) {
    // Store as contiguous buffer + offsets (like ref impl)
    // 存储为连续缓冲区 + 偏移量
    self.buf.clear();
    self.offsets.clear();
    self.offsets.push(0);
    for item in items {
      self.buf.extend_from_slice(item);
      self.offsets.push(self.buf.len());
    }
  }

  fn train_and_encode(&mut self) -> usize {
    // Convert to slice refs for training
    // 转换为切片引用用于训练
    let items: Vec<&[u8]> = self.offsets.windows(2)
      .map(|w| &self.buf[w[0]..w[1]])
      .collect();
    
    self.encoder = Some(train(&items).unwrap());
    self.decoder = Some(jdb_fsst::decode::Decode::from(self.encoder.as_ref().unwrap()));
    
    self.encode_buf.clear();
    self.compressed_offsets.clear();
    self.compressed_offsets.push(0);
    
    let encoder = self.encoder.as_ref().unwrap();
    for w in self.offsets.windows(2) {
      let item = &self.buf[w[0]..w[1]];
      encoder.encode(item, &mut self.encode_buf);
      self.compressed_offsets.push(self.encode_buf.len());
    }
    
    self.encode_buf.len()
  }

  fn decompress_all(&mut self) {
    let decoder = self.decoder.as_ref().expect("Decoder not initialized");
    
    self.decode_buf.clear();
    
    for w in self.compressed_offsets.windows(2) {
      let compressed = &self.encode_buf[w[0]..w[1]];
      decoder.decode(compressed, &mut self.decode_buf);
    }
  }

  fn reset(&mut self) {
    self.encoder = None;
    self.decoder = None;
    self.buf.clear();
    self.offsets.clear();
    self.encode_buf.clear();
    self.compressed_offsets.clear();
    self.decode_buf.clear();
  }
}
