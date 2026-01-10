#![cfg_attr(docsrs, feature(doc_cfg))]

mod atom_write;
mod try_rm;
pub use atom_write::AtomWrite;
pub use try_rm::try_rm;
