/// My jdb_fsst implementation benchmark
use jdb_fsst::encode;

pub struct MyFsst {
    output_buf: Vec<u8>,
    offset_buf: Vec<usize>,
}

impl MyFsst {
    pub fn new(buf_size: usize, offsets_len: usize) -> Self {
        Self {
            output_buf: vec![0; buf_size],
            offset_buf: vec![0; offsets_len],
        }
    }
}

impl FsstBench for MyFsst {
    const NAME: &'static str = "My";

    fn compress(&mut self, in_buf: &[u8], in_offsets: &[usize]) -> usize {
        let _ = encode(
            in_buf,
            in_offsets,
            &mut self.output_buf,
            &mut self.offset_buf,
        );
        *self.offset_buf.last().unwrap_or(&0)
    }

    fn reset(&mut self, buf_size: usize, offsets_len: usize) {
        self.output_buf.resize(buf_size, 0);
        self.offset_buf.resize(offsets_len, 0);
    }
}
