#![cfg_attr(docsrs, feature(doc_cfg))]

mod add_ext;
mod atom_write;
mod try_rm;

pub use add_ext::add_ext;
pub use atom_write::AtomWrite;
pub use try_rm::try_rm;
