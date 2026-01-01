/// Checkpoint configuration
/// 检查点配置
#[derive(Clone, Copy, Debug)]
pub enum Conf {
  /// Trigger compaction after writing this many entries
  /// 写入多少条 Item 后触发压缩
  TruncateAfter(usize),
  /// Keep this many Checkpoint (Save) points
  /// 保留多少个 Checkpoint (Save) 点
  Keep(usize),
}
