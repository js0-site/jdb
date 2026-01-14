/// JdbLevel Error
/// JdbLevel 错误
#[derive(thiserror::Error, Debug)]
pub enum Error {}

/// Result type for JdbLevel
/// JdbLevel 的 Result 类型
pub type Result<T> = std::result::Result<T, Error>;
