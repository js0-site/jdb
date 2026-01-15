/// My jdb_fsst implementation benchmark
use jdb_fsst::train;

pub struct MyFsst {
  encoder: Option<jdb_fsst::encode::Encode>,
  decoder: Option<jdb_fsst::decode::Decode>,
  encode_buf: Vec<u8>,
  decode_buf: Vec<u8>,
  items: Vec<Vec<u8>>,
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
      encode_buf: Vec::new(),
      decode_buf: Vec::new(),
      items: Vec::new(),
    }
  }
}

impl FsstBench for MyFsst {
  const NAME: &'static str = "My";

  fn prepare(&mut self, items: &[&[u8]]) {
    self.items = items.iter().map(|item| item.to_vec()).collect();
  }

  fn train_and_encode(&mut self) -> usize {
    self.encoder = Some(train(&self.items).unwrap());
    self.decoder = Some(jdb_fsst::decode::Decode::from(self.encoder.as_ref().unwrap()));
    
    let mut total_size = 0;
    for item in &self.items {
      self.encode_buf.clear();
      total_size += self.encoder.as_ref().unwrap().encode(item, &mut self.encode_buf).unwrap();
    }
    total_size
  }

  fn decode(&mut self, compressed: &[u8]) -> usize {
    self.decode_buf.clear();
    self.decoder.as_ref().unwrap().decode(compressed, &mut self.decode_buf).unwrap()
  }

  fn reset(&mut self) {
    self.encoder = None;
    self.decoder = None;
    self.encode_buf.clear();
    self.decode_buf.clear();
    self.items.clear();
  }
}
