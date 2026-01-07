#![cfg_attr(docsrs, feature(doc_cfg))]

mod merge;

pub use jdb_base::table::{Asc, Desc, Order};
pub use jdb_level::{Conf, Level, Levels};
pub use merge::{Merge, MergeAsc, MergeDesc, PeekIter};
