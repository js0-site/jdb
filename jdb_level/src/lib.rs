#![cfg_attr(docsrs, feature(doc_cfg))]

//! LSM-tree level management with dynamic level bytes
//! 带动态层级字节的 LSM-tree 层级管理

mod calc;
mod compact;
mod conf;
mod error;
mod handle;
mod level;
mod levels;
mod load;
mod multi;
mod read;

pub use calc::{Limits, SCORE_SCALE, SCORE_URGENT, score};
pub use compact::Compactor;
pub use conf::{Conf, MAX_LEVEL, MAX_LEVELS, ParsedConf, default};
pub use error::{Error, Result};
pub use handle::Handle;
pub use jdb_ckp::Op;
pub use level::Level;
pub use levels::{Levels, conf, new};
pub use load::load;
pub use multi::{MultiAsc, MultiDesc};
pub use read::Read;
