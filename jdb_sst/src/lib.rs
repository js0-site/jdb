#![cfg_attr(docsrs, feature(doc_cfg))]

mod compress;
mod conf;
mod foot;

pub use compress::Compress;
pub use conf::{Conf, Config};
pub use foot::{Foot, FootCrc};

pub const MAGIC_VER: u64 = u64::from_be_bytes(*b"jdbSst01");
