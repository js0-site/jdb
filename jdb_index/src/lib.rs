#![cfg_attr(docsrs, feature(doc_cfg))]

mod merge;

pub use jdb_base::table::{
  Asc, Desc, Order,
  level::{self, Conf, Level, Levels},
};
pub use merge::{Merge, MergeAsc, MergeDesc, PeekIter};
