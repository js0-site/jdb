use fsst::{Compressor, Decompressor};

pub struct RefFsst {
    compressor: Option<Compressor>,
}

impl Default for RefFsst {
    fn default() -> Self {
        Self::new()
    }
}

impl RefFsst {
    pub fn new() -> Self {
        Self { compressor: None }
    }
}

impl FsstBench for RefFsst {
    const NAME: &'static str = "Ref";

    fn compress(&mut self, in_buf: &[u8], in_offsets: &[usize]) -> usize {
        let n = in_offsets.len() - 1;
        let mut lengths = Vec::with_capacity(n);
        let mut ptrs = Vec::with_capacity(n);
        
        for i in 0..n {
            let start = in_offsets[i];
            let end = in_offsets[i+1];
            ptrs.push(&in_buf[start..end] as *const [u8] as *const u8);
            lengths.push(end - start);
        }

        let compressor = Compressor::train(&ptrs, &lengths);
        let mut out_buf = vec![0u8; in_buf.len() * 2 + 1024];
        let mut out_ptrs = vec![std::ptr::null_mut::<u8>(); n];
        let mut out_lengths = vec![0usize; n];

        let compressed_size = compressor.compress(
            &ptrs,
            &lengths,
            &mut out_buf,
            &mut out_ptrs,
            &mut out_lengths,
        );
        
        self.compressor = Some(compressor);
        compressed_size
    }

    fn reset(&mut self, _buf_size: usize, _offsets_len: usize) {
        self.compressor = None;
    }
}
