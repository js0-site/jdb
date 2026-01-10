pub const BUF_WRITER_SIZE: usize = 512 * 1024;

pub const BUF_READ_SIZE: usize = 512 * 1024;

/// Compact operation interval (operations per compaction)
/// 压缩操作间隔（每次压缩的操作次数）
pub const COMPACT_INTERVAL: usize = 1024 * 64;
