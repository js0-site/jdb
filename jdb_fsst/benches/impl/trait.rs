/// Benchmark trait definition
/// 基准测试 trait 定义
pub trait FsstBench {
  /// Name of this implementation
  /// 实现名称
  const NAME: &'static str;

  /// Prepare training data (called once)
  /// 准备训练数据（只调用一次）
  fn prepare(&mut self, items: &[&[u8]]);

  /// Train and encode data, return compressed size
  /// 训练并编码数据，返回压缩后大小
  fn train_and_encode(&mut self) -> usize;

  /// Decompress all data that was just compressed
  /// 解压所有刚刚压缩的数据
  fn decode_all(&mut self);

  /// Decompress a single item by index
  /// 按索引解压单个项目
  fn random_decode(&mut self, index: usize);

  /// Number of items available for random decoding
  /// 可进行随机解压的项目数量
  fn num_items(&self) -> usize;
}
