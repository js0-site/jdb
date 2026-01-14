/// Benchmark trait definition
/// A trait for FSST compression implementations to benchmark
pub trait FsstBench {
    /// Name of this implementation
    const NAME: &'static str;

    /// Compress data and return compressed size
    fn compress(&mut self, in_buf: &[u8], in_offsets: &[usize]) -> usize;

    /// Reset internal buffers for next run
    fn reset(&mut self, buf_size: usize, offsets_len: usize);
}
