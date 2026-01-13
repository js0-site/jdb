mod flag;
pub use flag::Flag;
mod mem;
pub use mem::Mem;
mod pos;
pub use pos::Pos;
mod discard;
pub mod order;
pub mod query;
pub mod sst;
pub use discard::Discard;

/// Type alias for existence check result
/// 存在性检查结果的类型别名
pub type Exist = bool;
