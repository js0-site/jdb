use super::SstOp;

/// Interface for updating Levels state
/// 更新 Levels 状态的接口
pub trait Levels {
  /// Update state with operation (apply only to memory)
  /// 使用操作更新状态（仅应用到内存）
  fn update(&mut self, op: SstOp);
}
