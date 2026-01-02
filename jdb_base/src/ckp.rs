/// WAL file identifier
/// WAL 文件标识符
pub type WalId = u64;

/// Offset within WAL file
/// WAL 文件内的偏移量
pub type WalOffset = u64;

#[derive(Debug, Clone)]
pub struct WalIdOffset {
  pub wal_id: WalId,
  pub offset: WalOffset,
}

/// Replay information after recovery
/// 恢复后的回放信息
#[derive(Debug, Clone)]
pub struct Ckp {
  pub wal_id: WalId,
  pub offset: WalOffset,
  /// All Rotate events that occurred after the Checkpoint
  /// Checkpoint 之后发生的所有 Rotate 事件
  pub rotate_wal_id_li: Vec<WalId>,
}
