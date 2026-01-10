/// Default buffer size for writers
/// 写入器的默认缓冲区大小
pub const BUF_WRITER_SIZE: usize = 512 * 1024;

/// Default buffer size for readers
/// 读取器的默认缓冲区大小
pub const BUF_READ_SIZE: usize = 512 * 1024;

/// Compact operation interval (operations per compaction)
/// 压缩操作间隔（每次压缩的操作次数）
pub const COMPACT_INTERVAL: usize = 1024 * 64;
