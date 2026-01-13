#![cfg_attr(docsrs, feature(doc_cfg))]

mod compress;
mod conf;
mod foot;

pub use compress::Compress;
pub use conf::{Conf, Config};
pub use foot::Foot;

pub const VER: u8 = 1;
