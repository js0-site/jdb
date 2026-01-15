/// Benchmark trait definition
/// 基准测试 trait 定义
pub trait FsstBench {
  /// Name of this implementation
  const NAME: &'static str;

  /// Prepare training data (called once)
  /// 准备训练数据（只调用一次）
  fn prepare(&mut self, items: &[&[u8]]);

  /// Train and encode data, return compressed size
  /// 训练并编码数据，返回压缩后大小
  fn train_and_encode(&mut self) -> usize;

  /// Decode a single compressed item and return decompressed size
  /// 解码单个压缩项目并返回解压大小
  fn decode(&mut self, compressed: &[u8]) -> usize;

  /// Reset internal state for next benchmark run
  /// 重置内部状态以进行下一次基准测试
  fn reset(&mut self);
}
