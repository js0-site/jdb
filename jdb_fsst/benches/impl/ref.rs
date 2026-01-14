pub struct RefFsst;

impl Default for RefFsst {
    fn default() -> Self {
        Self::new()
    }
}

impl RefFsst {
    pub fn new() -> Self {
        Self
    }
}

impl FsstBench for RefFsst {
    const NAME: &'static str = "Ref";

    fn compress(&mut self, in_buf: &[u8], in_offsets: &[usize]) -> usize {
        let mut symbol_table = [0u8; fsst::fsst::FSST_SYMBOL_TABLE_SIZE];
        let mut out_buf = vec![0u8; in_buf.len() * 2 + 1024];
        
        // The fsst crate requires i32 or i64 for offsets
        let in_offsets_i64: Vec<i64> = in_offsets.iter().map(|&x| x as i64).collect();
        let mut out_offsets_i64 = vec![0i64; in_offsets.len()];

        fsst::fsst::compress(
            &mut symbol_table,
            in_buf,
            &in_offsets_i64,
            &mut out_buf,
            &mut out_offsets_i64,
        ).expect("fsst compression failed");
        
        out_buf.len()
    }

    fn reset(&mut self, _buf_size: usize, _offsets_len: usize) {
        // No-op
    }
}
