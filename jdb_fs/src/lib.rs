#![cfg_attr(docsrs, feature(doc_cfg))]

mod add_ext;
mod atom_write;
mod compact;
pub mod consts;
mod file;
mod try_rm;

pub use add_ext::add_ext;
pub use atom_write::AtomWrite;
pub use compact::{AutoCompact, Compact, Decoded, IncrCount};
pub use try_rm::try_rm;
