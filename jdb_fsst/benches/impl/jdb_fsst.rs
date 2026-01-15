/// My jdb_fsst implementation benchmark
use jdb_fsst::train;

use super::r#trait::FsstBench;

pub struct JdbFsst {
  encoder: Option<jdb_fsst::encode::Encode>,
  decoder: Option<jdb_fsst::decode::Decode>,
  // Store items as Vec<Vec<u8>>
  // 直接存储为 Vec<Vec<u8>>
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

  fn decompress_all(&mut self) {
    let decoder = self.decoder.as_ref().expect("Decoder not initialized");

    self.decode_buf.clear();

    for w in self.compressed_offsets.windows(2) {
      let compressed = &self.encode_buf[w[0]..w[1]];
      decoder.decode(compressed, &mut self.decode_buf);
    }
  }
}