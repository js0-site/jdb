/// My jdb_fsst implementation benchmark
/// jdb_fsst 实现的基准测试
use jdb_fsst::train;

use super::r#trait::FsstBench;

pub struct JdbFsst {
  encoder: Option<jdb_fsst::encode::Encode>,
  decoder: Option<jdb_fsst::decode::Decode>,
  items: Vec<Vec<u8>>,
  encode_buf: Vec<u8>,
  compressed_offsets: Vec<usize>,
  decode_buf: Vec<u8>,
}

impl Default for JdbFsst {
  fn default() -> Self {
    Self::new()
  }
}

impl JdbFsst {
  pub fn new() -> Self {
    Self {
      encoder: None,
      decoder: None,
      items: Vec::new(),
      encode_buf: Vec::new(),
      compressed_offsets: Vec::new(),
      decode_buf: Vec::new(),
    }
  }
}

impl FsstBench for JdbFsst {
  const NAME: &'static str = "jdb_fsst";

  fn prepare(&mut self, items: &[&[u8]]) {
    self.items.clear();
    self.items.extend(items.iter().map(|&s| s.to_vec()));
    // Pre-allocate decode buffer to max item size
    // 预分配解码缓冲区到最大 item 大小
    let max_len = items.iter().map(|i| i.len()).max().unwrap_or(0);
    self.decode_buf = Vec::with_capacity(max_len);
  }

  fn train_and_encode(&mut self) -> usize {
    self.encoder = Some(train(&self.items).unwrap());
    self.decoder = Some(jdb_fsst::decode::Decode::from(
      self.encoder.as_ref().unwrap(),
    ));

    self.encode_buf.clear();
    self.compressed_offsets.clear();
    self.compressed_offsets.push(0);

    let encoder = self.encoder.as_ref().unwrap();
    for item in &self.items {
      encoder.encode(item, &mut self.encode_buf);
      self.compressed_offsets.push(self.encode_buf.len());
    }

    self.encode_buf.len()
  }

  fn decode_all(&mut self) {
    let decoder = self.decoder.as_ref().expect("Decoder not initialized");
    self.decode_buf.clear();
    if !self.encode_buf.is_empty() {
      decoder.decode(&self.encode_buf, &mut self.decode_buf);
    }
  }

  fn random_decode(&mut self, index: usize) {
    let decoder = self.decoder.as_ref().expect("Decoder not initialized");
    let start = self.compressed_offsets[index];
    let end = self.compressed_offsets[index + 1];
    let compressed = &self.encode_buf[start..end];
    // Reuse buffer, just clear length
    // 复用缓冲区，只清空长度
    self.decode_buf.clear();
    decoder.decode(compressed, &mut self.decode_buf);
  }

  fn num_items(&self) -> usize {
    self.items.len()
  }
}
