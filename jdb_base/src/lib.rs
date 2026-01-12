mod flag;
pub use flag::Flag;
mod mem;
pub use mem::Mem;
mod pos;
pub use pos::Pos;
pub mod order;
pub mod query;
mod sst;

/// Type alias for existence check result
/// 存在性检查结果的类型别名
pub type Exist = bool;
