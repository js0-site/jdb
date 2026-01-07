#![cfg_attr(docsrs, feature(doc_cfg))]

//! LSM-tree level management with dynamic level bytes
//! 带动态层级字节的 LSM-tree 层级管理

mod calc;
mod conf;
mod level;
mod levels;
mod refcount;
mod snapshot;

pub use calc::{Limits, SCORE_SCALE, SCORE_URGENT, score};
pub use conf::{Conf, MAX_LEVEL, MAX_LEVELS, ParsedConf, default};
pub use jdb_ckp::Op;
pub use level::Level;
pub use levels::{Levels, conf, new};
pub use refcount::RefCountMap;
pub use snapshot::Snapshot;
